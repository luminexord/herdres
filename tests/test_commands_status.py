"""Issue #27: `herdres status` (read-only health + roster) and `herdres commands install`
(runtime-native /herdres slash commands for Claude Code), plus the shipped command-file shapes.

Covered:
  * status_once never leaks a token/chat_id (only chat_id_set bool + allowed_users_count int),
    and degrades cleanly on absent / corrupt state;
  * `main()` routes `status` to status_once, never the Telegram probe fallback;
  * install_runtime_commands copies only our herdres*.md into ~/.claude/commands, is idempotent,
    skips an absent runtime, and honours an explicit --source over the SOURCE_MARKER;
  * the repo ships exactly the four Claude .md commands (Codex is a descoped follow-up).
"""

from __future__ import annotations

import io
import json
import re
import unittest
import unittest.mock as mock
from contextlib import redirect_stdout
from pathlib import Path
from types import SimpleNamespace

import herdres

ROOT = Path(__file__).resolve().parent.parent
COMMANDS_DIR = ROOT / "commands"
STEMS = ["herdres", "herdres-setup", "herdres-sync", "herdres-status"]


def _run_status(state, *, state_present=True, corrupt=False):
    """Drive status_once with state fully stubbed (no real systemd/launchd I/O).

    status_once reads state.json directly (read-only — it must NOT call load_state, which renames a
    corrupt file). We stub state_path().read_text() and keep normalize_state an identity so the test
    controls the exact state. `corrupt=True` simulates unparseable JSON.
    """
    text = "}{ not json" if corrupt else json.dumps(state)
    sp = SimpleNamespace(exists=lambda: state_present, read_text=lambda *a, **k: text)
    with mock.patch.object(herdres, "state_path", return_value=sp), \
         mock.patch.object(herdres, "normalize_state", side_effect=lambda d: d), \
         mock.patch.object(herdres, "_unit_is_active", return_value=False), \
         mock.patch.object(herdres, "_gateway_is_active", return_value=False), \
         mock.patch.object(herdres, "_launchd_label_loaded", return_value=False), \
         mock.patch.object(herdres, "per_agent_topics_enabled", return_value=True):
        return herdres.status_once(SimpleNamespace())


class StatusOnceTests(unittest.TestCase):
    SECRET_TOKEN = "1234567890:AAFAKEFAKEFAKEsecrettokenshouldneverleak"
    SECRET_CHAT = "-1009998887776"

    def test_no_secret_leaks_only_bools_and_counts(self) -> None:
        state = {
            "telegram": {
                "bot_token": self.SECRET_TOKEN,
                "chat_id": self.SECRET_CHAT,
                "owner_user_ids": [111, 222, 333],
                "last_preflight_ok_at": "2026-06-20T00:00:00Z",
            },
            "enabled": True,
            "plugin_event_enabled": True,
        }
        result = _run_status(state)
        blob = json.dumps(result)
        self.assertNotIn(self.SECRET_TOKEN, blob)
        self.assertNotIn(self.SECRET_CHAT, blob)
        self.assertNotIn("bot_token", blob)
        self.assertTrue(result["config"]["chat_id_set"])
        self.assertEqual(result["config"]["allowed_users_count"], 3)
        self.assertTrue(result["ok"])

    def test_never_synced_is_degraded_not_error(self) -> None:
        result = _run_status({}, state_present=False)
        self.assertTrue(result["ok"])
        self.assertFalse(result["installed"]["state_present"])
        self.assertFalse(result["config"]["chat_id_set"])
        self.assertEqual(result["config"]["allowed_users_count"], 0)
        self.assertIsNone(result["config"]["enabled"])  # no state -> unknown, not a guessed default
        self.assertEqual(result["counts"], {"panes": 0, "spaces": 0, "open_panes": 0})

    def test_corrupt_state_reports_error_but_stays_ok(self) -> None:
        result = _run_status({}, corrupt=True)
        self.assertTrue(result["ok"])  # install + service health still answerable
        self.assertIn("state_error", result)
        self.assertIn("services", result)
        self.assertEqual(result["counts"]["panes"], 0)

    def test_corrupt_state_is_not_quarantined_readonly(self) -> None:
        # Fix for the review's read-only violation: reading a corrupt state.json via `status` must
        # NOT rename it aside (load_state's repair side-effect). Use a REAL file + real helpers.
        import tempfile
        with tempfile.TemporaryDirectory() as d:
            state_file = Path(d) / "state.json"
            state_file.write_text("}{ not valid json", encoding="utf-8")
            with mock.patch.object(herdres, "state_path", return_value=state_file), \
                 mock.patch.object(herdres, "_unit_is_active", return_value=False), \
                 mock.patch.object(herdres, "_gateway_is_active", return_value=False), \
                 mock.patch.object(herdres, "_launchd_label_loaded", return_value=False), \
                 mock.patch.object(herdres, "per_agent_topics_enabled", return_value=True):
                result = herdres.status_once(SimpleNamespace())
            self.assertTrue(result["ok"])
            self.assertIn("state_error", result)
            self.assertTrue(state_file.exists(), "status must leave the corrupt state file in place")
            siblings = [p.name for p in Path(d).iterdir()]
            self.assertEqual(siblings, ["state.json"], f"status quarantined the file: {siblings}")

    def test_missing_service_tools_degrade_not_crash(self) -> None:
        # Fix: on a box without systemctl/launchctl the service probes raise FileNotFoundError;
        # the read-only health command must still answer (ok=True), reporting them as not-active.
        sp = SimpleNamespace(exists=lambda: False)
        boom = mock.Mock(side_effect=FileNotFoundError("no service manager"))
        for macos in (False, True):
            with mock.patch.object(herdres, "state_path", return_value=sp), \
                 mock.patch.object(herdres, "_platform_is_macos", return_value=macos), \
                 mock.patch.object(herdres, "_unit_is_active", return_value=False), \
                 mock.patch.object(herdres, "_gateway_is_active", boom), \
                 mock.patch.object(herdres, "_launchd_label_loaded", boom), \
                 mock.patch.object(herdres, "per_agent_topics_enabled", return_value=True):
                result = herdres.status_once(SimpleNamespace())
            self.assertTrue(result["ok"], f"macos={macos}")
            self.assertFalse(result["services"]["gateway_active"], f"macos={macos}")
            if macos:
                self.assertFalse(result["services"]["timer_active"])

    def test_roster_counts_open_panes_excluding_closed(self) -> None:
        state = {
            "panes": {
                "k1": {"pane_id": "p1", "agent": "claude", "last_known_status": "working",
                       "topic_id": "10", "space_key": "s1"},
                "k2": {"pane_id": "p2", "agent": "codex", "last_known_status": "closed",
                       "topic_id": "11", "space_key": "s1"},
                "k3": {"pane_id": "p3", "agent": "claude", "last_known_status": "idle",
                       "topic_id": "12", "space_key": "s2"},
            },
            "spaces": {
                "s1": {"space_key": "s1", "topic_id": "20", "topic_name": "Alpha",
                       "pane_keys": ["k1", "k2"]},
                "s2": {"space_key": "s2", "topic_id": "21", "topic_name": "Beta",
                       "pane_keys": ["k3"]},
            },
        }
        result = _run_status(state)
        self.assertEqual(result["counts"], {"panes": 3, "spaces": 2, "open_panes": 2})
        self.assertEqual(len(result["panes"]), 3)
        beta = next(s for s in result["spaces"] if s["space_key"] == "s2")
        self.assertEqual(beta["pane_count"], 1)
        self.assertEqual(beta["topic_name"], "Beta")


class StatusDispatchTests(unittest.TestCase):
    def test_status_routes_to_status_once_not_probe(self) -> None:
        sentinel = {"ok": True, "marker": "status-not-probe"}
        with mock.patch.object(herdres.sys, "argv", ["herdres", "status"]), \
             mock.patch.object(herdres, "status_once", return_value=sentinel) as so, \
             mock.patch.object(herdres, "probe_rich",
                               side_effect=AssertionError("probe must not run for `status`")) as probe:
            buf = io.StringIO()
            with redirect_stdout(buf):
                rc = herdres.main()
        self.assertEqual(rc, 0)
        so.assert_called_once()
        probe.assert_not_called()
        self.assertEqual(json.loads(buf.getvalue())["marker"], "status-not-probe")


class InstallRuntimeCommandsTests(unittest.TestCase):
    def _make_source(self, tmp: Path) -> Path:
        src = tmp / "checkout"
        cmds = src / "commands"
        cmds.mkdir(parents=True)
        for stem in STEMS:
            (cmds / f"{stem}.md").write_text(f"# {stem} md\n", encoding="utf-8")
            (cmds / f"{stem}.toml").write_text(f'description = "{stem}"\n', encoding="utf-8")
        # Foreign command files from other plugins must never be swept up.
        (cmds / "commit.md").write_text("# not ours\n", encoding="utf-8")
        (cmds / "deploy.toml").write_text('description = "not ours"\n', encoding="utf-8")
        return src

    def _runtime_home(self, tmp: Path, *, claude=True) -> Path:
        home = tmp / "home"
        home.mkdir()
        if claude:
            (home / ".claude").mkdir()
        return home

    def test_copies_only_herdres_md_to_claude(self) -> None:
        import tempfile
        with tempfile.TemporaryDirectory() as d:
            tmp = Path(d)
            src = self._make_source(tmp)
            home = self._runtime_home(tmp)
            with mock.patch.object(herdres.Path, "home", classmethod(lambda cls: home)):
                counts = herdres.install_runtime_commands(src)
            self.assertEqual(counts, {"claude": 4})
            claude_files = sorted(p.name for p in (home / ".claude" / "commands").glob("*"))
            self.assertEqual(claude_files, sorted(f"{s}.md" for s in STEMS))
            self.assertNotIn("commit.md", claude_files)        # foreign .md left alone
            # Codex is descoped: even a herdres*.toml in source must NOT be copied (only *.md).
            self.assertFalse(any(n.endswith(".toml") for n in claude_files))
            self.assertFalse((home / ".codex").exists())       # no Codex dir touched/created

    def test_idempotent_second_run(self) -> None:
        import tempfile
        with tempfile.TemporaryDirectory() as d:
            tmp = Path(d)
            src = self._make_source(tmp)
            home = self._runtime_home(tmp)
            with mock.patch.object(herdres.Path, "home", classmethod(lambda cls: home)):
                herdres.install_runtime_commands(src)
                herdres.install_runtime_commands(src)
            self.assertEqual(len(list((home / ".claude" / "commands").glob("*"))), 4)

    def test_skips_uninstalled_runtime(self) -> None:
        import tempfile
        with tempfile.TemporaryDirectory() as d:
            tmp = Path(d)
            src = self._make_source(tmp)
            home = self._runtime_home(tmp, claude=False)  # ~/.claude absent
            with mock.patch.object(herdres.Path, "home", classmethod(lambda cls: home)):
                counts = herdres.install_runtime_commands(src)
            self.assertEqual(counts, {"claude": 0})
            self.assertFalse((home / ".claude").exists())  # must not be created

    def test_no_source_yields_zero_counts(self) -> None:
        import tempfile
        with tempfile.TemporaryDirectory() as d:
            tmp = Path(d)
            home = self._runtime_home(tmp)
            with mock.patch.object(herdres.Path, "home", classmethod(lambda cls: home)):
                # A source dir with no commands/ subdir is a no-op (not an error).
                self.assertEqual(herdres.install_runtime_commands(tmp), {"claude": 0})

    def test_commands_once_resolves_source_marker(self) -> None:
        import tempfile
        with tempfile.TemporaryDirectory() as d:
            tmp = Path(d)
            src = self._make_source(tmp)
            home = self._runtime_home(tmp)
            marker = tmp / "source"
            marker.write_text(str(src) + "\n", encoding="utf-8")
            with mock.patch.object(herdres, "SOURCE_MARKER", marker), \
                 mock.patch.object(herdres.Path, "home", classmethod(lambda cls: home)):
                result = herdres.commands_once(SimpleNamespace(action="install", source=""))
            self.assertTrue(result["ok"])
            self.assertEqual(result["installed"], {"claude": 4})

    def test_commands_once_explicit_source_overrides_marker(self) -> None:
        # The installers pass --source explicitly (cwd-independent); it must win over the marker.
        import tempfile
        with tempfile.TemporaryDirectory() as d:
            tmp = Path(d)
            src = self._make_source(tmp)
            home = self._runtime_home(tmp)
            bad_marker = tmp / "source"
            bad_marker.write_text(str(tmp / "nonexistent") + "\n", encoding="utf-8")
            with mock.patch.object(herdres, "SOURCE_MARKER", bad_marker), \
                 mock.patch.object(herdres.Path, "home", classmethod(lambda cls: home)):
                result = herdres.commands_once(SimpleNamespace(action="install", source=str(src)))
            self.assertEqual(result["installed"], {"claude": 4})

    def test_commands_once_rejects_unknown_action(self) -> None:
        result = herdres.commands_once(SimpleNamespace(action="nuke", source=""))
        self.assertFalse(result["ok"])


class CommandFileShapeTests(unittest.TestCase):
    def test_only_claude_md_commands_shipped(self) -> None:
        # Codex .toml command files are descoped (issue #27 follow-up); the repo must ship only
        # the four Claude Code .md commands so we don't re-introduce the broken Codex format.
        shipped = sorted(p.name for p in COMMANDS_DIR.glob("*"))
        self.assertEqual(shipped, sorted(f"{s}.md" for s in STEMS))

    def test_claude_md_frontmatter_name_matches_stem(self) -> None:
        for stem in STEMS:
            path = COMMANDS_DIR / f"{stem}.md"
            self.assertTrue(path.is_file(), f"missing {path}")
            text = path.read_text(encoding="utf-8")
            m = re.match(r"^---\n(.*?)\n---\n", text, re.S)
            self.assertIsNotNone(m, f"{path.name} must open with a YAML frontmatter block")
            fm = m.group(1)
            name = re.search(r"^name\s*:\s*(\S+)\s*$", fm, re.M)
            self.assertIsNotNone(name, f"{path.name} frontmatter needs a name")
            self.assertEqual(name.group(1), stem, f"{path.name} name must equal its filename stem")
            self.assertRegex(fm, r"(?m)^description\s*:\s*\S", f"{path.name} frontmatter needs a description")


if __name__ == "__main__":
    unittest.main()
