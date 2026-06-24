"""Issue #36: the herdres Claude Code hook (herdres_decision_hook.py) records a PENDING
AskUserQuestion / ExitPlanMode to a per-session file and clears it when answered / on session end.

The hook is run as an external process by Claude Code, so these tests invoke the actual script
through a subprocess (stdin JSON in, exit code + on-disk file out) rather than importing it."""
import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

HOOK = Path(__file__).resolve().parent.parent / "herdres_decision_hook.py"


def _safe(value: str) -> str:
    cleaned = "".join(c for c in str(value) if c.isalnum() or c in "-_.")
    return cleaned[:120] or "session"


class DecisionHookTests(unittest.TestCase):
    def _run(self, payload, pending_dir, *, raw=None):
        """Run the hook with payload (or raw bytes) on stdin; return (returncode)."""
        stdin = raw if raw is not None else json.dumps(payload)
        env = dict(os.environ, HERDRES_PENDING_DIR=str(pending_dir))
        proc = subprocess.run(
            [sys.executable, str(HOOK)], input=stdin, env=env,
            capture_output=True, text=True, timeout=30,
        )
        return proc

    def _file(self, pending_dir, session_id):
        return Path(pending_dir) / f"{_safe(session_id)}.json"

    def test_pretooluse_askuserquestion_writes_pending_file(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            payload = {
                "hook_event_name": "PreToolUse",
                "tool_name": "AskUserQuestion",
                "session_id": "sess-A",
                "tool_input": {"questions": [
                    {"question": "Tea or coffee?", "header": "Pick",
                     "options": [{"label": "Tea"}, {"label": "Coffee"}], "multiSelect": False},
                ]},
            }
            proc = self._run(payload, tmp)
            self.assertEqual(proc.returncode, 0)
            self.assertEqual(proc.stdout, "")  # passive recorder: emits nothing that could alter the turn
            rec = json.loads(self._file(tmp, "sess-A").read_text())
            self.assertEqual(rec["name"], "AskUserQuestion")
            self.assertEqual(rec["session_id"], "sess-A")
            self.assertTrue(rec["tool_use_id"].startswith("hookdec-"))
            self.assertEqual(rec["input"]["questions"][0]["question"], "Tea or coffee?")
            self.assertIn("ts", rec)

    def test_pretooluse_exitplanmode_writes_pending_file(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            payload = {
                "hook_event_name": "PreToolUse", "tool_name": "ExitPlanMode",
                "session_id": "sess-P", "tool_input": {"plan": "# Plan\nstep one"},
            }
            self._run(payload, tmp)
            rec = json.loads(self._file(tmp, "sess-P").read_text())
            self.assertEqual(rec["name"], "ExitPlanMode")
            self.assertEqual(rec["input"]["plan"], "# Plan\nstep one")

    def test_synthesized_id_is_stable_across_refires(self) -> None:
        # Re-firing PreToolUse for the SAME prompt must keep the same decision_id (no duplicate buttons).
        with tempfile.TemporaryDirectory() as tmp:
            payload = {
                "hook_event_name": "PreToolUse", "tool_name": "AskUserQuestion", "session_id": "sess-S",
                "tool_input": {"questions": [{"question": "Q?", "options": [{"label": "x"}]}]},
            }
            self._run(payload, tmp)
            first = json.loads(self._file(tmp, "sess-S").read_text())["tool_use_id"]
            self._run(payload, tmp)
            second = json.loads(self._file(tmp, "sess-S").read_text())["tool_use_id"]
            self.assertEqual(first, second)

    def test_posttooluse_clears_pending_file(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            pre = {"hook_event_name": "PreToolUse", "tool_name": "AskUserQuestion",
                   "session_id": "sess-C", "tool_input": {"questions": [{"question": "Q?"}]}}
            self._run(pre, tmp)
            self.assertTrue(self._file(tmp, "sess-C").exists())
            self._run({"hook_event_name": "PostToolUse", "tool_name": "AskUserQuestion",
                       "session_id": "sess-C"}, tmp)
            self.assertFalse(self._file(tmp, "sess-C").exists())

    def test_sessionend_and_stop_clear_pending_file(self) -> None:
        for event in ("SessionEnd", "Stop"):
            with tempfile.TemporaryDirectory() as tmp:
                self._run({"hook_event_name": "PreToolUse", "tool_name": "ExitPlanMode",
                           "session_id": "sess-E", "tool_input": {"plan": "p"}}, tmp)
                self.assertTrue(self._file(tmp, "sess-E").exists())
                self._run({"hook_event_name": event, "session_id": "sess-E"}, tmp)
                self.assertFalse(self._file(tmp, "sess-E").exists(), event)

    def test_clear_is_safe_when_no_file_exists(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            proc = self._run({"hook_event_name": "PostToolUse", "session_id": "sess-none"}, tmp)
            self.assertEqual(proc.returncode, 0)  # unlink of a missing file must not error

    def test_non_decision_tool_writes_nothing(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            proc = self._run({"hook_event_name": "PreToolUse", "tool_name": "Bash",
                              "session_id": "sess-B", "tool_input": {"command": "ls"}}, tmp)
            self.assertEqual(proc.returncode, 0)
            self.assertEqual(list(Path(tmp).glob("*.json")), [])

    def test_malformed_stdin_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            proc = self._run(None, tmp, raw="this is not json{{{")
            self.assertEqual(proc.returncode, 0)  # never blocks Claude
            self.assertEqual(list(Path(tmp).glob("*.json")), [])

    def test_empty_stdin_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            proc = self._run(None, tmp, raw="")
            self.assertEqual(proc.returncode, 0)
            self.assertEqual(list(Path(tmp).glob("*.json")), [])

    def test_missing_session_id_writes_nothing(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            proc = self._run({"hook_event_name": "PreToolUse", "tool_name": "AskUserQuestion",
                              "tool_input": {"questions": []}}, tmp)
            self.assertEqual(proc.returncode, 0)
            self.assertEqual(list(Path(tmp).glob("*.json")), [])

    def test_session_id_with_path_separators_is_sanitized(self) -> None:
        # A hostile/odd session_id must not let the hook write outside the pending dir.
        with tempfile.TemporaryDirectory() as tmp:
            self._run({"hook_event_name": "PreToolUse", "tool_name": "AskUserQuestion",
                       "session_id": "../../etc/evil", "tool_input": {"questions": [{"question": "Q?"}]}}, tmp)
            written = list(Path(tmp).glob("*.json"))
            self.assertEqual(len(written), 1)
            self.assertEqual(written[0].parent, Path(tmp))  # stayed inside the pending dir


if __name__ == "__main__":
    unittest.main()
