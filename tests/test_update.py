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

import hashlib
import io
import json
import os
import subprocess
import tarfile
import unittest
import urllib.error
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
        "version": "",
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
        self.sync_returncode = 0
        self.new_version = "9.9.9"
        # Version reported by `git show <ref>:herdres.py` for --check; None => the
        # `git show` "fails" (no upstream), exercising the needs-upstream path.
        self.remote_version = "9.9.9"
        # `systemctl --user is-active herdres-gateway.service` result. "active" by
        # default; flip to "inactive" to simulate a silently-failed re-enable.
        self.gateway_active = "active"
        # When set, the FIRST gateway `enable --now` flips gateway_active to
        # "inactive" (one-shot), simulating a re-enable that silently fails on the
        # apply restart while leaving the rollback restart able to recover.
        self.flip_gateway_inactive_on_enable = False

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
            elif sub == "show":
                # `git show <ref>:herdres.py` -> remote source (for --check version).
                if self.remote_version is None:
                    rc = 1
                else:
                    stdout = f'HERDRES_VERSION = "{self.remote_version}"\n'
        elif argv[0].endswith("herdres") or argv[0].endswith("/herdres"):
            # the installed-binary verify probes: `herdres version` / `herdres sync`
            if argv[-1] == "version":
                rc = self.version_returncode
                stdout = json.dumps({"ok": True, "version": self.new_version})
            elif argv[-1] == "sync":
                rc = self.sync_returncode
        elif argv[0] in {"systemctl", "launchctl"}:
            # is-enabled -> "enabled" so the unit is treated as active.
            if "is-enabled" in argv:
                stdout = "enabled\n"
            elif "is-active" in argv:
                stdout = self.gateway_active + "\n"
                rc = 0 if self.gateway_active == "active" else 3
            elif (
                self.flip_gateway_inactive_on_enable
                and "enable" in argv
                and "herdres-gateway.service" in argv
            ):
                # One-shot: the apply restart's re-enable silently fails.
                self.gateway_active = "inactive"
                self.flip_gateway_inactive_on_enable = False
            # Every other systemctl/launchctl mutation "succeeds" (rc stays 0).
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
        self.router.remote_version = "9.9.9"
        before = _snapshot(self.tmp)
        result = herdres.update_once(_args(repo=str(self.repo), check=True))
        self.assertTrue(result["ok"])
        self.assertEqual(result["action"], "check")
        self.assertEqual(result["current_version"], herdres.HERDRES_VERSION)
        # available_version is read from the REMOTE source (git show), not local.
        self.assertEqual(result["available_version"], "9.9.9")
        self.assertTrue(result["update_available"])  # 9.9.9 != local HERDRES_VERSION
        self.assertFalse(result["needs_upstream"])
        # No file changed.
        self.assertEqual(_snapshot(self.tmp), before)
        # Side-effect-free except the fetch: only git fetch/rev-parse/show ran, no
        # service mutation.
        for call in self.router.calls:
            self.assertNotIn(call[0], {"systemctl", "launchctl"})
            if call[0] == "git":
                self.assertIn(call[3], {"fetch", "rev-parse", "show"})

    def test_check_reports_no_update_when_remote_matches_local(self):
        self._seed_installed()
        # Remote reports the SAME version the running binary is -> no update.
        self.router.remote_version = herdres.HERDRES_VERSION
        result = herdres.update_once(_args(repo=str(self.repo), check=True))
        self.assertEqual(result["available_version"], herdres.HERDRES_VERSION)
        self.assertFalse(result["update_available"])
        self.assertFalse(result["needs_upstream"])

    def test_check_missing_upstream_reports_unknown(self):
        self._seed_installed()
        # `git show <ref>:herdres.py` fails -> no upstream version known.
        self.router.remote_version = None
        result = herdres.update_once(_args(repo=str(self.repo), check=True))
        self.assertEqual(result["available_version"], "unknown")
        self.assertTrue(result["needs_upstream"])
        # Must NOT silently claim no update is available.
        self.assertFalse(result["update_available"])


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


class NoRestartTests(UpdateTestBase):
    def test_no_restart_skips_services_and_warns(self):
        self._seed_installed()
        env_path = self.tmp / "herdres.env"
        env_path.write_text("TELEGRAM_BOT_TOKEN=secret\n", encoding="utf-8")
        with patch.object(herdres, "DEFAULT_ENV", env_path):
            result = herdres.update_once(_args(repo=str(self.repo), no_restart=True))

        self.assertTrue(result["ok"], result)
        # Files were still replaced (the update happened)...
        self.assertIn('HERDRES_VERSION = "9.9.9"', (self.bin / "herdres").read_text())
        # ...but NO systemctl/launchctl calls were issued (restart fully skipped).
        for call in self.router.calls:
            self.assertNotIn(call[0], {"systemctl", "launchctl"})
        # The result warns the operator to restart manually.
        self.assertIn("warnings", result)
        self.assertTrue(any("no-restart" in w for w in result["warnings"]), result["warnings"])
        # herdres.env is still untouched.
        self.assertEqual(env_path.read_text(), "TELEGRAM_BOT_TOKEN=secret\n")

    def test_no_restart_via_env(self):
        self._seed_installed()
        with patch.dict(os.environ, {"HERDRES_UPDATE_SKIP_RESTART": "1"}):
            herdres.update_once(_args(repo=str(self.repo)))
        for call in self.router.calls:
            self.assertNotIn(call[0], {"systemctl", "launchctl"})


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

    def test_version_mismatch_rolls_back(self):
        """The new binary runs but reports a DIFFERENT version -> code didn't land."""
        self._seed_installed()
        # version probe exits 0 but reports a stale version != pulled 9.9.9.
        self.router.new_version = "0.1.0"
        with self.assertRaises(herdres.BridgeError) as ctx:
            herdres.update_once(_args(repo=str(self.repo)))
        self.assertIn("rolled back", str(ctx.exception))
        self.assertEqual((self.bin / "herdres").read_text(), 'HERDRES_VERSION = "0.1.0"\n')


class SoftSyncTests(UpdateTestBase):
    def test_sync_failure_warns_but_does_not_roll_back(self):
        """A post-install dry-run `herdres sync` failure is environmental -> SOFT.

        The version probe proves the code update; a nonzero sync only attaches a
        warning. The good update must STAY applied (no rollback).
        """
        self._seed_installed()
        self.router.sync_returncode = 7  # environmental failure
        result = herdres.update_once(_args(repo=str(self.repo)))
        self.assertTrue(result["ok"])
        self.assertEqual(result["action"], "update")
        self.assertEqual(result["version"], "9.9.9")
        # New code stayed in place.
        self.assertIn('HERDRES_VERSION = "9.9.9"', (self.bin / "herdres").read_text())
        # A warning was surfaced.
        self.assertIn("warnings", result)
        self.assertTrue(any("sync" in w for w in result["warnings"]))


class GatewayHealthTests(UpdateTestBase):
    def test_dead_gateway_after_restart_rolls_back(self):
        """Gateway active before but inactive after re-enable -> roll back.

        The router starts "active" (so the pre-restart snapshot sees it up) and the
        gateway `enable --now` call flips it to "inactive" (a silently-failed
        re-enable), so the post-restart health check fails and the update rolls back.
        """
        self._seed_installed()
        self.router.flip_gateway_inactive_on_enable = True
        with self.assertRaises(herdres.BridgeError) as ctx:
            herdres.update_once(_args(repo=str(self.repo)))
        self.assertIn("rolled back", str(ctx.exception))
        self.assertIn("gateway", str(ctx.exception).lower())
        # Files rolled back to the pre-update content.
        self.assertEqual((self.bin / "herdres").read_text(), 'HERDRES_VERSION = "0.1.0"\n')

    def test_gateway_inactive_before_is_not_asserted(self):
        """If the gateway was NOT active before, we never force it / never raise."""
        self._seed_installed()
        self.router.gateway_active = "inactive"
        # is-enabled still says enabled (so the disable/enable dance runs), but the
        # gateway was never active, so a still-inactive gateway must not raise.
        result = herdres.update_once(_args(repo=str(self.repo)))
        self.assertTrue(result["ok"])
        self.assertEqual(result["action"], "update")


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


class FullRollbackTests(UpdateTestBase):
    def test_rollback_deletes_newly_created_dest(self):
        """A dest that did NOT exist pre-apply is DELETED on rollback (not orphaned).

        Seed every dest except herdr_topic_bridge.py, so the apply creates it fresh.
        Rolling back must remove that newly-created file, fully reverting the apply.
        """
        self._seed_installed()
        # Remove one dest so it is genuinely absent at backup time.
        new_dest = self.share / "herdr_topic_bridge.py"
        new_dest.unlink()
        self.assertFalse(new_dest.exists())

        # Apply: this creates herdr_topic_bridge.py for the first time.
        herdres.update_once(_args(repo=str(self.repo)))
        self.assertTrue(new_dest.exists())
        self.assertEqual(new_dest.read_text(), "# bridge\n")

        # Roll back: the newly-created file must be gone, others restored.
        herdres.update_once(_args(rollback=True))
        self.assertFalse(new_dest.exists())
        self.assertEqual((self.bin / "herdres").read_text(), 'HERDRES_VERSION = "0.1.0"\n')

    def test_failed_apply_rollback_deletes_newly_created_dest(self):
        """Same, but via the inline rollback when verify fails mid-apply."""
        self._seed_installed()
        new_dest = self.share / "herdr_topic_bridge.py"
        new_dest.unlink()
        self.router.version_returncode = 1  # force verify failure -> inline rollback
        with self.assertRaises(herdres.BridgeError):
            herdres.update_once(_args(repo=str(self.repo)))
        # The file created during the partial apply was removed by rollback.
        self.assertFalse(new_dest.exists())


class EnvUntouchedTests(UpdateTestBase):
    """Make the env-untouched guarantee REAL: a real herdres.env under the patched
    config dir must be byte-identical after an edge apply AND after a rollback. If
    update_once ever wrote to herdres.env, these would fail.
    """

    ENV_BODY = "TELEGRAM_BOT_TOKEN=supersecret\nTELEGRAM_CHAT_ID=123\nHERDR_OSOV=1\n"

    def _seed_env(self) -> Path:
        # Place herdres.env under a patched config dir that update_once would write
        # to if it were buggy. Point DEFAULT_ENV at it for the whole test.
        config_dir = self.tmp / "config" / "herdres"
        config_dir.mkdir(parents=True, exist_ok=True)
        env_path = config_dir / "herdres.env"
        env_path.write_bytes(self.ENV_BODY.encode("utf-8"))
        self.enterContext(patch.object(herdres, "DEFAULT_ENV", env_path))
        return env_path

    def test_env_unchanged_after_apply_and_rollback(self):
        self._seed_installed()
        env_path = self._seed_env()
        before = env_path.read_bytes()

        # Edge apply.
        herdres.update_once(_args(repo=str(self.repo)))
        self.assertTrue(env_path.exists())
        self.assertEqual(env_path.read_bytes(), before)

        # Rollback.
        herdres.update_once(_args(rollback=True))
        self.assertTrue(env_path.exists())
        self.assertEqual(env_path.read_bytes(), before)

    def test_env_unchanged_on_failed_apply_rollback(self):
        self._seed_installed()
        env_path = self._seed_env()
        before = env_path.read_bytes()
        self.router.version_returncode = 1  # verify fails -> inline rollback
        with self.assertRaises(herdres.BridgeError):
            herdres.update_once(_args(repo=str(self.repo)))
        self.assertTrue(env_path.exists())
        self.assertEqual(env_path.read_bytes(), before)


class ChannelTests(UpdateTestBase):
    def test_unknown_channel_rejected(self):
        with self.assertRaises(herdres.BridgeError) as ctx:
            herdres.update_once(_args(channel="weird", repo=str(self.repo)))
        self.assertIn("unknown channel", str(ctx.exception))


class BackupPruneTests(UpdateTestBase):
    def test_old_backups_pruned_to_keep_n(self):
        self._seed_installed()
        with patch.object(herdres, "KEEP_BACKUPS", 2):
            made = []
            for i in range(4):
                # Distinct timestamps via the suffix-on-collision path is fragile;
                # create dirs directly to control names deterministically.
                d = self.backups / f"2026010{i}T000000.000000Z"
                d.mkdir(parents=True)
                (d / "manifest.json").write_text("{}", encoding="utf-8")
                made.append(d)
            herdres._prune_backups()
        remaining = sorted(p.name for p in self.backups.iterdir() if p.is_dir())
        self.assertEqual(
            remaining,
            ["20260102T000000.000000Z", "20260103T000000.000000Z"],
        )

    def test_latest_backup_is_chronological_not_lexical(self):
        """Microsecond-precision names make lexical order == chronological order.

        The old same-second suffixes -1..-10 sorted -10 before -2 lexically, so the
        "latest" pick was wrong. With microsecond timestamps the 10th-of-a-second
        backup is still the newest by both string sort and wall clock.
        """
        self._seed_installed()
        # Two same-second backups differing only in microseconds; the later one
        # (larger microseconds) must be picked as latest.
        earlier = self.backups / "20260101T000000.000002Z"
        later = self.backups / "20260101T000000.000010Z"
        for d in (earlier, later):
            d.mkdir(parents=True)
            (d / "manifest.json").write_text(
                json.dumps({"files": {}, "created": []}), encoding="utf-8"
            )
        self.assertEqual(herdres._latest_backup(), later)

    def test_backup_dir_name_has_microsecond_precision(self):
        self._seed_installed()
        backup = herdres._backup_install_set()
        # Name like 20260101T000000.123456Z — a dot + 6 digits before the Z.
        self.assertRegex(backup.name, r"^\d{8}T\d{6}\.\d{6}Z$")


def _build_release_tarball(prefix: str = "herdres-9.9.9", files=None) -> bytes:
    """Build an in-memory .tar.gz of the install-set source under ``prefix/``.

    Mirrors a real release tarball: every install-set source file lives under a
    single top-level prefix dir, with herdres.py carrying the release version.
    """
    members = files if files is not None else SOURCE_FILES
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w:gz") as tar:
        for rel, text in members.items():
            data = text.encode("utf-8")
            info = tarfile.TarInfo(name=f"{prefix}/{rel}")
            info.size = len(data)
            tar.addfile(info, io.BytesIO(data))
    return buf.getvalue()


class _FakeHTTP:
    """Routes herdres' urllib.request.urlopen calls for the stable channel offline.

    Serves the Releases API JSON for /releases/latest and /releases/tags/<tag>, and
    the two release assets (tarball + .sha256). ``calls`` records every URL fetched
    so tests can assert "nothing was downloaded" on --check / --dry-run. Knobs let a
    test corrupt the sha256, omit a release, or pin a specific tag.
    """

    def __init__(self, *, tag="v9.9.9", tarball=None, sha_override=None, has_release=True):
        self.tag = tag
        self.tarball = tarball if tarball is not None else _build_release_tarball()
        self.sha = sha_override if sha_override is not None else hashlib.sha256(self.tarball).hexdigest()
        self.has_release = has_release
        self.calls: list[str] = []

    def _release_json(self) -> bytes:
        # Assets are named off the literal tag (the RELEASE-ASSET contract):
        # herdres-<tag>.tar.gz + herdres-<tag>.tar.gz.sha256.
        tarball = f"herdres-{self.tag}.tar.gz"
        assets = [
            {"name": tarball, "browser_download_url": f"https://example.test/dl/{tarball}"},
            {"name": tarball + ".sha256", "browser_download_url": f"https://example.test/dl/{tarball}.sha256"},
        ]
        return json.dumps({"tag_name": self.tag, "assets": assets}).encode("utf-8")

    def __call__(self, req, *a, **kw):  # noqa: ANN001 - mimics urlopen(req)
        url = req.full_url if hasattr(req, "full_url") else str(req)
        self.calls.append(url)
        if "/releases/" in url and "/download/" not in url and ".tar.gz" not in url:
            if not self.has_release:
                raise urllib.error.HTTPError(url, 404, "Not Found", {}, None)
            body = self._release_json()
        elif url.endswith(".sha256"):
            # `sha256sum`-format line naming the real tarball asset (the verifier
            # parses the leading digest; the filename mirrors what CI publishes).
            body = (self.sha + f"  herdres-{self.tag}.tar.gz\n").encode("utf-8")
        elif url.endswith(".tar.gz"):
            body = self.tarball
        else:  # pragma: no cover - unexpected URL in a test
            raise AssertionError(f"unexpected URL: {url}")
        return _FakeResponse(body)


class _FakeResponse:
    """Minimal context-manager response with .read(), matching urlopen's contract."""

    def __init__(self, body: bytes):
        self._body = body

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def read(self):
        return self._body


class StableChannelTests(UpdateTestBase):
    """The stable channel: API resolve -> download -> sha256 verify -> extract ->
    the SAME env-safe backup/apply/restart/verify/rollback as edge.

    urllib.request.urlopen is mocked with an in-memory tarball + sha256, so the
    tests are fully offline and never hit GitHub.
    """

    def _patch_http(self, http: _FakeHTTP):
        self.enterContext(patch.object(herdres.urllib.request, "urlopen", http))
        return http

    def test_stable_check_resolves_version_downloads_nothing(self):
        self._seed_installed()
        http = self._patch_http(_FakeHTTP(tag="v9.9.9"))
        before = _snapshot(self.tmp)
        result = herdres.update_once(_args(channel="stable", check=True))
        self.assertTrue(result["ok"])
        self.assertEqual(result["action"], "check")
        self.assertEqual(result["channel"], "stable")
        self.assertEqual(result["current_version"], herdres.HERDRES_VERSION)
        self.assertEqual(result["available_version"], "9.9.9")
        self.assertEqual(result["tag"], "v9.9.9")
        self.assertTrue(result["update_available"])
        # Nothing on disk changed and no asset was downloaded (only the API JSON).
        self.assertEqual(_snapshot(self.tmp), before)
        self.assertTrue(all(".tar.gz" not in u for u in http.calls), http.calls)
        # No services were touched.
        for call in self.router.calls:
            self.assertNotIn(call[0], {"systemctl", "launchctl"})

    def test_stable_check_uses_latest_endpoint_unpinned(self):
        self._seed_installed()
        http = self._patch_http(_FakeHTTP(tag="v9.9.9"))
        herdres.update_once(_args(channel="stable", check=True))
        self.assertTrue(any(u.endswith("/releases/latest") for u in http.calls), http.calls)

    def test_stable_check_pins_tag_with_version(self):
        self._seed_installed()
        http = self._patch_http(_FakeHTTP(tag="v9.9.9"))
        herdres.update_once(_args(channel="stable", check=True, version="v9.9.9"))
        self.assertTrue(
            any(u.endswith("/releases/tags/v9.9.9") for u in http.calls), http.calls
        )

    def test_stable_dry_run_downloads_nothing(self):
        self._seed_installed()
        http = self._patch_http(_FakeHTTP(tag="v9.9.9"))
        before = _snapshot(self.tmp)
        result = herdres.update_once(_args(channel="stable", dry_run=True))
        self.assertEqual(result["action"], "dry-run")
        self.assertEqual(result["channel"], "stable")
        self.assertEqual(result["target_version"], "9.9.9")
        self.assertEqual(result["tag"], "v9.9.9")
        self.assertEqual(_snapshot(self.tmp), before)
        self.assertTrue(all(".tar.gz" not in u for u in http.calls), http.calls)

    def test_stable_apply_downloads_verifies_extracts_and_installs(self):
        self._seed_installed()
        self._patch_http(_FakeHTTP(tag="v9.9.9"))
        env_path = self.tmp / "herdres.env"
        env_path.write_text("TELEGRAM_BOT_TOKEN=secret\n", encoding="utf-8")
        with patch.object(herdres, "DEFAULT_ENV", env_path):
            result = herdres.update_once(_args(channel="stable"))

        self.assertTrue(result["ok"], result)
        self.assertEqual(result["action"], "update")
        self.assertEqual(result["channel"], "stable")
        self.assertEqual(result["version"], "9.9.9")
        self.assertEqual(result["head"], "v9.9.9")
        # The release code landed on disk.
        self.assertIn('HERDRES_VERSION = "9.9.9"', (self.bin / "herdres").read_text())
        self.assertEqual((self.bin / "herdres-gateway").read_text(), "# gateway\n")
        # Plugin manifest got the absolute path sed'd in (same transform as edge).
        plugin = (self.share / "herdres-plugin" / "herdr-plugin.toml").read_text()
        self.assertIn(f'["{self.bin / "herdres"}", "event"]', plugin)
        # A backup exists and herdres.env is UNTOUCHED.
        self.assertTrue(Path(result["backup"]).is_dir())
        self.assertEqual(env_path.read_text(), "TELEGRAM_BOT_TOKEN=secret\n")
        # Gateway was re-leased disable->enable, same as edge.
        gw = [c for c in self.router.calls
              if c[0] == "systemctl" and "herdres-gateway.service" in c and c[2] in {"disable", "enable"}]
        self.assertEqual([c[2] for c in gw], ["disable", "enable"])

    def test_stable_apply_flat_tarball_layout(self):
        """A tarball with no top-level prefix dir (herdres.py at the root) works too."""
        self._seed_installed()
        self._patch_http(_FakeHTTP(tag="v9.9.9", tarball=_build_release_tarball(prefix=".")))
        result = herdres.update_once(_args(channel="stable"))
        self.assertEqual(result["version"], "9.9.9")
        self.assertIn('HERDRES_VERSION = "9.9.9"', (self.bin / "herdres").read_text())

    def test_stable_sha256_mismatch_refuses_and_does_not_install(self):
        self._seed_installed()
        before = _snapshot_install(self)
        self._patch_http(_FakeHTTP(tag="v9.9.9", sha_override="0" * 64))
        with self.assertRaises(herdres.BridgeError) as ctx:
            herdres.update_once(_args(channel="stable"))
        self.assertIn("sha256 mismatch", str(ctx.exception))
        # Nothing was installed — the tarball never reached the apply step.
        self.assertEqual(_snapshot_install(self), before)

    def test_stable_malformed_sha256_refused(self):
        self._seed_installed()
        self._patch_http(_FakeHTTP(tag="v9.9.9", sha_override="not-a-hex-digest"))
        with self.assertRaises(herdres.BridgeError) as ctx:
            herdres.update_once(_args(channel="stable"))
        self.assertIn("sha256", str(ctx.exception))

    def test_stable_missing_release_raises(self):
        self._seed_installed()
        self._patch_http(_FakeHTTP(has_release=False))
        with self.assertRaises(herdres.BridgeError) as ctx:
            herdres.update_once(_args(channel="stable", check=True))
        self.assertIn("404", str(ctx.exception))

    def test_stable_verify_failure_rolls_back(self):
        """A failing post-install verify rolls the stable apply back, same as edge."""
        self._seed_installed()
        self._patch_http(_FakeHTTP(tag="v9.9.9"))
        self.router.version_returncode = 1  # `herdres version` probe fails -> rollback
        with self.assertRaises(herdres.BridgeError) as ctx:
            herdres.update_once(_args(channel="stable"))
        self.assertIn("rolled back", str(ctx.exception))
        self.assertEqual((self.bin / "herdres").read_text(), 'HERDRES_VERSION = "0.1.0"\n')

    def test_stable_no_restart_skips_services(self):
        self._seed_installed()
        self._patch_http(_FakeHTTP(tag="v9.9.9"))
        result = herdres.update_once(_args(channel="stable", no_restart=True))
        self.assertTrue(result["ok"])
        self.assertIn('HERDRES_VERSION = "9.9.9"', (self.bin / "herdres").read_text())
        for call in self.router.calls:
            self.assertNotIn(call[0], {"systemctl", "launchctl"})

    @staticmethod
    def _old_release(version: str):
        """An in-memory release tarball whose herdres.py reports ``version``."""
        files = dict(SOURCE_FILES)
        files["herdres.py"] = f'#!/usr/bin/env python3\nHERDRES_VERSION = "{version}"\n'
        return _build_release_tarball(prefix=f"herdres-{version}", files=files)

    def test_stable_unpinned_not_newer_is_up_to_date_no_apply(self):
        """An unpinned 'latest' that is <= the installed version applies nothing — no
        silent downgrade — even though the tarball was fetched + sha-verified."""
        self._seed_installed()
        self._patch_http(_FakeHTTP(tag="v0.1.0", tarball=self._old_release("0.1.0")))
        before = _snapshot_install(self)
        result = herdres.update_once(_args(channel="stable"))
        self.assertEqual(result["action"], "up-to-date")
        self.assertEqual(result["channel"], "stable")
        self.assertEqual(result["available_version"], "0.1.0")
        self.assertEqual(result["current_version"], herdres.HERDRES_VERSION)
        self.assertNotIn("backup", result)  # nothing was applied
        self.assertEqual(_snapshot_install(self), before)
        for call in self.router.calls:
            self.assertNotIn(call[0], {"systemctl", "launchctl"})

    def test_version_pin_implies_stable_channel(self):
        """--version routes to stable even when --channel is left at the edge default,
        so the pin is honored instead of silently running an edge git-pull."""
        self._seed_installed()
        http = self._patch_http(_FakeHTTP(tag="v9.9.9"))
        # channel defaults to 'edge'; ONLY --version is supplied.
        result = herdres.update_once(_args(version="v9.9.9"))
        self.assertEqual(result["channel"], "stable")
        self.assertEqual(result["action"], "update")
        self.assertTrue(
            any(u.endswith("/releases/tags/v9.9.9") for u in http.calls), http.calls
        )
        self.assertIn('HERDRES_VERSION = "9.9.9"', (self.bin / "herdres").read_text())

    def test_stable_pinned_downgrade_bypasses_up_to_date_guard(self):
        """A --version pin is an explicit, intentional target: it applies even when
        older than the installed version, unlike an unpinned 'latest'."""
        self._seed_installed()
        self._patch_http(_FakeHTTP(tag="v0.1.0", tarball=self._old_release("0.1.0")))
        self.router.new_version = "0.1.0"  # the freshly-installed binary now reports 0.1.0
        result = herdres.update_once(_args(version="v0.1.0"))
        self.assertEqual(result["action"], "update")
        self.assertEqual(result["version"], "0.1.0")
        self.assertIn('HERDRES_VERSION = "0.1.0"', (self.bin / "herdres").read_text())


class StableFetchHelperTests(UpdateTestBase):
    """Direct unit tests for the fetch building blocks (verify + safe-extract)."""

    def test_verify_sha256_accepts_sha256sum_line(self):
        blob = b"hello"
        digest = hashlib.sha256(blob).hexdigest()
        out = herdres._verify_sha256(blob, f"{digest}  some-file.tar.gz\n", "some-file.tar.gz")
        self.assertEqual(out, digest)

    def test_verify_sha256_accepts_bare_digest(self):
        blob = b"world"
        digest = hashlib.sha256(blob).hexdigest()
        self.assertEqual(herdres._verify_sha256(blob, digest, "x"), digest)

    def test_strip_v_drops_one_leading_v_only(self):
        self.assertEqual(herdres._strip_v("v0.3.0"), "0.3.0")
        self.assertEqual(herdres._strip_v("0.3.0"), "0.3.0")
        # Only ONE leading v, and nothing from the middle (unlike str.lstrip("v")).
        self.assertEqual(herdres._strip_v("v1.2.3-victory"), "1.2.3-victory")

    def test_version_key_orders_numerically(self):
        key = herdres._version_key
        self.assertEqual(key("0.3.0"), (0, 3, 0))
        self.assertEqual(key("v0.3.0"), (0, 3, 0))
        # Pre-release suffix stops the component; compares by released parts.
        self.assertEqual(key("0.3.0-rc1"), (0, 3, 0))
        # 0.10.0 must sort ABOVE 0.9.0 (numeric, not lexical).
        self.assertGreater(key("0.10.0"), key("0.9.0"))
        self.assertGreater(key("1.0.0"), key("0.99.0"))
        self.assertLess(key("0.2.0"), key("0.3.0"))

    def test_safe_extract_rejects_traversal(self):
        evil = io.BytesIO()
        with tarfile.open(fileobj=evil, mode="w:gz") as tar:
            data = b"pwn"
            info = tarfile.TarInfo(name="../escape.txt")
            info.size = len(data)
            tar.addfile(info, io.BytesIO(data))
        evil.seek(0)
        dest = self.tmp / "x"
        dest.mkdir()
        with tarfile.open(fileobj=evil, mode="r:gz") as tar:
            with self.assertRaises(herdres.BridgeError):
                herdres._safe_extract(tar, dest)
        self.assertFalse((self.tmp / "escape.txt").exists())

    def test_source_root_requires_herdres_py(self):
        empty = self.tmp / "empty-extract"
        empty.mkdir()
        with self.assertRaises(herdres.BridgeError):
            herdres._source_root(empty)


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
