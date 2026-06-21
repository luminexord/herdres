"""Tests for `herdres version` and `herdres update` (Issue #13 Phase 1+2, edge).

These pin the env-safe self-update's load-bearing guarantees: source resolution
order, that ``--check``/``--dry-run`` change nothing, that an edge apply backs up
and atomically replaces the install-set while *never* touching herdres.env, that
the gateway is re-leased via disable->enable (in that order), and that any failure
during replace/restart/verify rolls the backup back.

All git/systemctl/launchctl/verify subprocesses are mocked and every install dest
root is redirected onto a tmp filesystem, so the tests are offline, deterministic,
and never touch the real ~/.local, ~/.config, or the live services.
"""

from __future__ import annotations

import json
import os
import subprocess
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
from unittest.mock import patch

import herdres


# The install-set source filenames the updater copies. Mirrors _update_files_plan.
SOURCE_FILES = {
    "herdres.py": f'#!/usr/bin/env python3\nHERDRES_VERSION = "9.9.9"\n',
    "herdres_gateway.py": "# gateway\n",
    "herdres_routing.py": "# routing\n",
    "herdr_topic_bridge.py": "# bridge\n",
    "herdres-plugin/herdr-plugin.toml": 'command = ["herdres", "event"]\n',
    "systemd/user/herdres.service": "[Service]\n",
    "systemd/user/herdres.timer": "[Timer]\n",
    "systemd/user/herdres-gateway.service": "[Service]\n",
}


def _args(**over):
    base = {
        "channel": "edge",
        "repo": "",
        "check": False,
        "rollback": False,
        "dry_run": False,
    }
    base.update(over)
    return SimpleNamespace(**base)


def _make_source(root: Path) -> Path:
    """Materialize a fake source checkout under ``root`` and return it."""
    repo = root / "src"
    for rel, text in SOURCE_FILES.items():
        path = repo / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text, encoding="utf-8")
    return repo


class _SubprocessRouter:
    """Records herdres.subprocess.run calls and returns canned CompletedProcess.

    By default every command "succeeds". Tests can flip ``version_returncode`` to
    simulate a failing verify, or inspect ``calls`` for ordering assertions.
    """

    def __init__(self):
        self.calls: list[list[str]] = []
        self.version_returncode = 0
        self.pull_returncode = 0
        self.new_version = "9.9.9"

    def __call__(self, argv, *a, **kw):  # noqa: ANN001 - mimics subprocess.run
        self.calls.append(list(argv))
        stdout = ""
        rc = 0
        if argv[0] == "git":
            sub = argv[3] if len(argv) > 3 else ""
            if sub == "pull":
                rc = self.pull_returncode
            elif sub == "rev-parse":
                stdout = "abc1234\n"
            elif sub == "fetch":
                rc = 0
        elif argv[0].endswith("herdres") or argv[0].endswith("/herdres"):
            # the installed-binary verify probes: `herdres version` / `herdres sync`
            if argv[-1] == "version":
                rc = self.version_returncode
                stdout = json.dumps({"ok": True, "version": self.new_version})
            elif argv[-1] == "sync":
                rc = 0
        elif argv[0] in {"systemctl", "launchctl"}:
            # is-enabled -> "enabled" so the unit is treated as active.
            if "is-enabled" in argv:
                stdout = "enabled\n"
            rc = 0
        return subprocess.CompletedProcess(argv, rc, stdout=stdout, stderr="")


class UpdateTestBase(unittest.TestCase):
    def setUp(self):
        tmp = Path(self.enterContext(TemporaryDirectory()))
        self.tmp = tmp
        self.bin = tmp / "bin"
        self.share = tmp / "share"
        self.systemd = tmp / "systemd"
        self.backups = self.share / "backups"
        self.marker = self.share / "source"
        self.repo = _make_source(tmp)

        # Redirect every install dest root + marker onto the tmp filesystem.
        self.enterContext(patch.object(herdres, "INSTALL_BIN_DIR", self.bin))
        self.enterContext(patch.object(herdres, "INSTALL_SHARE_DIR", self.share))
        self.enterContext(patch.object(herdres, "INSTALL_SYSTEMD_DIR", self.systemd))
        self.enterContext(patch.object(herdres, "BACKUP_DIR", self.backups))
        self.enterContext(patch.object(herdres, "SOURCE_MARKER", self.marker))
        # Force Linux/systemd path regardless of the host.
        self.enterContext(patch.object(herdres, "_platform_is_macos", lambda: False))

        self.router = _SubprocessRouter()
        self.enterContext(patch.object(herdres, "subprocess", _SubprocessShim(self.router)))

        # Keep HERDRES_SRC out of the picture unless a test sets it.
        self._saved_src = os.environ.pop("HERDRES_SRC", None)
        self.addCleanup(self._restore_src)

    def _restore_src(self):
        if self._saved_src is None:
            os.environ.pop("HERDRES_SRC", None)
        else:
            os.environ["HERDRES_SRC"] = self._saved_src

    def _seed_installed(self):
        """Write a pre-existing install set (so backups have something to copy)."""
        (self.bin).mkdir(parents=True, exist_ok=True)
        (self.share / "herdres-plugin").mkdir(parents=True, exist_ok=True)
        (self.systemd).mkdir(parents=True, exist_ok=True)
        (self.bin / "herdres").write_text('HERDRES_VERSION = "0.1.0"\n', encoding="utf-8")
        (self.bin / "herdres-gateway").write_text("# old gateway\n", encoding="utf-8")
        (self.bin / "herdres_routing.py").write_text("# old routing\n", encoding="utf-8")
        (self.share / "herdr_topic_bridge.py").write_text("# old bridge\n", encoding="utf-8")
        (self.share / "herdres-plugin" / "herdr-plugin.toml").write_text("# old plugin\n", encoding="utf-8")
        for unit in ("herdres.service", "herdres.timer", "herdres-gateway.service"):
            (self.systemd / unit).write_text("# old unit\n", encoding="utf-8")


class _SubprocessShim:
    """Wraps the stdlib subprocess module, replacing only ``run`` with the router.

    update_once references ``subprocess.run`` and ``subprocess.CompletedProcess``
    via the module global, so patching the whole module attribute keeps both.
    """

    def __init__(self, router):
        self._router = router
        self.CompletedProcess = subprocess.CompletedProcess
        self.PIPE = subprocess.PIPE
        self.STDOUT = subprocess.STDOUT

    def run(self, *a, **kw):
        return self._router(*a, **kw)


class VersionTests(unittest.TestCase):
    def test_version_returns_constant(self):
        result = herdres.version_once(_args())
        self.assertEqual(result, {"ok": True, "version": herdres.HERDRES_VERSION})


class SourceResolutionTests(UpdateTestBase):
    def test_repo_flag_wins(self):
        path = herdres._resolve_source(_args(repo=str(self.repo)))
        self.assertEqual(path, self.repo)

    def test_env_var_used_when_no_flag(self):
        os.environ["HERDRES_SRC"] = str(self.repo)
        path = herdres._resolve_source(_args())
        self.assertEqual(path, self.repo)

    def test_marker_used_when_no_flag_or_env(self):
        self.marker.parent.mkdir(parents=True, exist_ok=True)
        self.marker.write_text(str(self.repo) + "\n", encoding="utf-8")
        path = herdres._resolve_source(_args())
        self.assertEqual(path, self.repo)

    def test_flag_beats_env_and_marker(self):
        os.environ["HERDRES_SRC"] = str(self.tmp / "elsewhere")
        self.marker.parent.mkdir(parents=True, exist_ok=True)
        self.marker.write_text(str(self.tmp / "other") + "\n", encoding="utf-8")
        path = herdres._resolve_source(_args(repo=str(self.repo)))
        self.assertEqual(path, self.repo)

    def test_not_found_raises(self):
        with self.assertRaises(herdres.BridgeError) as ctx:
            herdres._resolve_source(_args())
        self.assertIn("source checkout not found", str(ctx.exception))

    def test_non_checkout_path_raises(self):
        empty = self.tmp / "empty"
        empty.mkdir()
        with self.assertRaises(herdres.BridgeError):
            herdres._resolve_source(_args(repo=str(empty)))


class CheckTests(UpdateTestBase):
    def test_check_applies_nothing(self):
        self._seed_installed()
        before = _snapshot(self.tmp)
        result = herdres.update_once(_args(repo=str(self.repo), check=True))
        self.assertTrue(result["ok"])
        self.assertEqual(result["action"], "check")
        self.assertEqual(result["current_version"], herdres.HERDRES_VERSION)
        self.assertEqual(result["available_version"], "9.9.9")
        # No file changed.
        self.assertEqual(_snapshot(self.tmp), before)
        # No service mutation was issued (only git fetch / rev-parse allowed).
        for call in self.router.calls:
            self.assertNotIn(call[0], {"systemctl", "launchctl"})
            if call[0] == "git":
                self.assertIn(call[3], {"fetch", "rev-parse"})


class DryRunTests(UpdateTestBase):
    def test_dry_run_applies_nothing(self):
        self._seed_installed()
        before = _snapshot(self.tmp)
        result = herdres.update_once(_args(repo=str(self.repo), dry_run=True))
        self.assertTrue(result["ok"])
        self.assertEqual(result["action"], "dry-run")
        self.assertEqual(result["source"], str(self.repo))
        self.assertEqual(_snapshot(self.tmp), before)
        # Plan-only: nothing shelled out at all.
        self.assertEqual(self.router.calls, [])


class EdgeApplyTests(UpdateTestBase):
    def test_edge_apply_replaces_and_backs_up(self):
        self._seed_installed()
        env_path = self.tmp / "herdres.env"
        env_path.write_text("TELEGRAM_BOT_TOKEN=secret\n", encoding="utf-8")
        with patch.object(herdres, "DEFAULT_ENV", env_path):
            result = herdres.update_once(_args(repo=str(self.repo)))

        self.assertTrue(result["ok"], result)
        self.assertEqual(result["action"], "update")
        self.assertEqual(result["version"], "9.9.9")

        # Code files were replaced from source.
        self.assertIn('HERDRES_VERSION = "9.9.9"', (self.bin / "herdres").read_text())
        self.assertEqual((self.bin / "herdres-gateway").read_text(), "# gateway\n")
        self.assertEqual((self.share / "herdr_topic_bridge.py").read_text(), "# bridge\n")

        # Plugin manifest got the absolute herdres path sed'd in.
        plugin = (self.share / "herdres-plugin" / "herdr-plugin.toml").read_text()
        self.assertIn(f'["{self.bin / "herdres"}", "event"]', plugin)

        # herdres.env is UNTOUCHED.
        self.assertEqual(env_path.read_text(), "TELEGRAM_BOT_TOKEN=secret\n")

        # A backup directory with a manifest was created.
        backup = Path(result["backup"])
        self.assertTrue(backup.is_dir())
        self.assertTrue((backup / "manifest.json").exists())

        # Installed binary has the 0755 mode.
        self.assertEqual((self.bin / "herdres").stat().st_mode & 0o777, 0o755)

    def test_gateway_restart_is_disable_then_enable(self):
        self._seed_installed()
        herdres.update_once(_args(repo=str(self.repo)))
        gateway_calls = [
            c for c in self.router.calls
            if c[0] == "systemctl" and "herdres-gateway.service" in c
        ]
        # is-enabled probe, then disable --now, then enable --now.
        actions = [c for c in gateway_calls if c[2] in {"disable", "enable"}]
        self.assertEqual([a[2] for a in actions], ["disable", "enable"])
        # Each carries --now (so the lease is actually released/reacquired).
        for a in actions:
            self.assertIn("--now", a)
        # daemon-reload happened before the gateway dance.
        flat = [" ".join(c) for c in self.router.calls]
        reload_idx = next(i for i, c in enumerate(flat) if "daemon-reload" in c)
        disable_idx = next(i for i, c in enumerate(flat) if "disable --now herdres-gateway" in c)
        enable_idx = next(i for i, c in enumerate(flat) if "enable --now herdres-gateway" in c)
        self.assertLess(reload_idx, disable_idx)
        self.assertLess(disable_idx, enable_idx)


class RollbackOnFailureTests(UpdateTestBase):
    def test_verify_failure_restores_backup(self):
        self._seed_installed()
        # Make the post-install `herdres version` probe fail -> rollback.
        self.router.version_returncode = 1
        env_path = self.tmp / "herdres.env"
        env_path.write_text("TELEGRAM_BOT_TOKEN=secret\n", encoding="utf-8")

        with patch.object(herdres, "DEFAULT_ENV", env_path):
            with self.assertRaises(herdres.BridgeError) as ctx:
                herdres.update_once(_args(repo=str(self.repo)))
        self.assertIn("rolled back", str(ctx.exception))

        # Files were restored to the pre-update content.
        self.assertEqual((self.bin / "herdres").read_text(), 'HERDRES_VERSION = "0.1.0"\n')
        self.assertEqual((self.bin / "herdres-gateway").read_text(), "# old gateway\n")
        # env still untouched.
        self.assertEqual(env_path.read_text(), "TELEGRAM_BOT_TOKEN=secret\n")

    def test_dirty_repo_pull_failure_does_not_replace(self):
        self._seed_installed()
        self.router.pull_returncode = 1
        before = _snapshot_install(self)
        with self.assertRaises(herdres.BridgeError) as ctx:
            herdres.update_once(_args(repo=str(self.repo)))
        self.assertIn("git pull", str(ctx.exception))
        # Nothing replaced (pull failed before backup/replace).
        self.assertEqual(_snapshot_install(self), before)


class RollbackCommandTests(UpdateTestBase):
    def test_rollback_restores_latest_backup(self):
        self._seed_installed()
        # First, apply an update to create a backup of the 0.1.0 set.
        herdres.update_once(_args(repo=str(self.repo)))
        self.assertIn('HERDRES_VERSION = "9.9.9"', (self.bin / "herdres").read_text())

        # Now roll back: latest backup holds the original 0.1.0 launcher.
        result = herdres.update_once(_args(rollback=True))
        self.assertTrue(result["ok"])
        self.assertEqual(result["action"], "rollback")
        self.assertEqual((self.bin / "herdres").read_text(), 'HERDRES_VERSION = "0.1.0"\n')

    def test_rollback_without_backup_raises(self):
        with self.assertRaises(herdres.BridgeError) as ctx:
            herdres.update_once(_args(rollback=True))
        self.assertIn("no backup", str(ctx.exception))


class ChannelTests(UpdateTestBase):
    def test_stable_channel_rejected(self):
        with self.assertRaises(herdres.BridgeError) as ctx:
            herdres.update_once(_args(channel="stable", repo=str(self.repo)))
        self.assertIn("Phase 3", str(ctx.exception))

    def test_unknown_channel_rejected(self):
        with self.assertRaises(herdres.BridgeError):
            herdres.update_once(_args(channel="weird", repo=str(self.repo)))


class BackupPruneTests(UpdateTestBase):
    def test_old_backups_pruned_to_keep_n(self):
        self._seed_installed()
        with patch.object(herdres, "KEEP_BACKUPS", 2):
            made = []
            for i in range(4):
                # Distinct timestamps via the suffix-on-collision path is fragile;
                # create dirs directly to control names deterministically.
                d = self.backups / f"2026010{i}T000000Z"
                d.mkdir(parents=True)
                (d / "manifest.json").write_text("{}", encoding="utf-8")
                made.append(d)
            herdres._prune_backups()
        remaining = sorted(p.name for p in self.backups.iterdir() if p.is_dir())
        self.assertEqual(remaining, ["20260102T000000Z", "20260103T000000Z"])


def _snapshot(root: Path) -> dict[str, str]:
    """Map every file under ``root`` to its content (for change detection)."""
    out: dict[str, str] = {}
    for path in sorted(root.rglob("*")):
        if path.is_file():
            try:
                out[str(path.relative_to(root))] = path.read_text(encoding="utf-8")
            except UnicodeDecodeError:
                out[str(path.relative_to(root))] = "<binary>"
    return out


def _snapshot_install(case: UpdateTestBase) -> dict[str, str]:
    out: dict[str, str] = {}
    for base in (case.bin, case.share, case.systemd):
        if base.exists():
            for path in sorted(base.rglob("*")):
                if path.is_file() and "backups" not in path.parts:
                    out[str(path)] = path.read_text(encoding="utf-8")
    return out


if __name__ == "__main__":
    unittest.main()
