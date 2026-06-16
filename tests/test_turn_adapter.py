import importlib.util
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch


MODULE_PATH = Path(__file__).resolve().parents[1] / "herdr_turn_adapter.py"
SPEC = importlib.util.spec_from_file_location("herdr_turn_adapter", MODULE_PATH)
adapter = importlib.util.module_from_spec(SPEC)
assert SPEC and SPEC.loader
SPEC.loader.exec_module(adapter)


def write_jsonl(path: Path, rows: list[dict]) -> None:
    path.write_text("".join(json.dumps(row) + "\n" for row in rows), encoding="utf-8")


class TurnAdapterTests(unittest.TestCase):
    def test_codex_extracts_completed_turn_from_task_complete(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "rollout-2026-06-15T00-00-00-session-1.jsonl"
            write_jsonl(
                path,
                [
                    {"type": "event_msg", "payload": {"type": "task_started", "turn_id": "turn-1", "started_at": 10}},
                    {
                        "type": "response_item",
                        "payload": {
                            "type": "message",
                            "role": "user",
                            "content": [{"type": "input_text", "text": "<environment_context>ignore</environment_context>"}],
                        },
                    },
                    {
                        "type": "response_item",
                        "payload": {
                            "type": "message",
                            "role": "user",
                            "content": [{"type": "input_text", "text": "<literal> What happened?"}],
                        },
                    },
                    {
                        "type": "response_item",
                        "payload": {
                            "type": "message",
                            "role": "assistant",
                            "content": [{"type": "output_text", "text": "progress update"}],
                        },
                    },
                    {
                        "type": "event_msg",
                        "payload": {
                            "type": "task_complete",
                            "turn_id": "turn-1",
                            "completed_at": 20,
                            "last_agent_message": "Final answer only.",
                        },
                    },
                ],
            )

            turn = adapter.extract_codex_turn(path, "pane-1", "session-1")

        self.assertTrue(turn["available"])
        self.assertTrue(turn["complete"])
        self.assertEqual(turn["turn_id"], "turn-1")
        self.assertEqual(turn["user_text"], "<literal> What happened?")
        self.assertEqual(turn["assistant_final_text"], "Final answer only.")

    def test_codex_open_turn_is_incomplete(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "rollout-2026-06-15T00-00-00-session-1.jsonl"
            write_jsonl(
                path,
                [
                    {
                        "type": "event_msg",
                        "payload": {"type": "task_started", "turn_id": "turn-2", "started_at": 30},
                    },
                    {
                        "type": "response_item",
                        "payload": {
                            "type": "message",
                            "role": "user",
                            "content": [{"type": "input_text", "text": "Still running?"}],
                        },
                    },
                ],
            )

            turn = adapter.extract_codex_turn(path, "pane-1", "session-1")

        self.assertTrue(turn["available"])
        self.assertFalse(turn["complete"])
        self.assertEqual(turn["turn_id"], "turn-2")
        self.assertEqual(turn["user_text"], "Still running?")
        self.assertEqual(turn["assistant_final_text"], "")

    def test_codex_returns_latest_complete_when_newer_user_turn_is_open(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "rollout-2026-06-15T00-00-00-session-1.jsonl"
            write_jsonl(
                path,
                [
                    {"type": "event_msg", "payload": {"type": "task_started", "turn_id": "turn-1"}},
                    {
                        "type": "response_item",
                        "payload": {
                            "type": "message",
                            "role": "user",
                            "content": [{"type": "input_text", "text": "What finished?"}],
                        },
                    },
                    {
                        "type": "event_msg",
                        "payload": {
                            "type": "task_complete",
                            "turn_id": "turn-1",
                            "last_agent_message": "The completed result.",
                        },
                    },
                    {"type": "event_msg", "payload": {"type": "task_started", "turn_id": "turn-2"}},
                    {
                        "type": "response_item",
                        "payload": {
                            "type": "message",
                            "role": "user",
                            "content": [{"type": "input_text", "text": "This newer turn is still open."}],
                        },
                    },
                ],
            )

            turn = adapter.extract_codex_turn(path, "pane-1", "session-1")

        self.assertTrue(turn["available"])
        self.assertTrue(turn["complete"])
        self.assertEqual(turn["turn_id"], "turn-1")
        self.assertEqual(turn["user_text"], "What finished?")
        self.assertEqual(turn["assistant_final_text"], "The completed result.")
        self.assertTrue(turn["has_open_turn"])
        self.assertEqual(turn["open_turn_id"], "turn-2")

    def test_claude_extracts_last_end_turn_assistant_message(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "session-1.jsonl"
            write_jsonl(
                path,
                [
                    {
                        "type": "user",
                        "uuid": "user-1",
                        "message": {"role": "user", "content": "Diagnose it."},
                    },
                    {
                        "type": "assistant",
                        "uuid": "tool-1",
                        "message": {
                            "role": "assistant",
                            "stop_reason": "tool_use",
                            "content": [{"type": "text", "text": "intermediate"}],
                        },
                    },
                    {
                        "type": "assistant",
                        "uuid": "assistant-1",
                        "timestamp": "2026-06-15T10:00:00Z",
                        "message": {
                            "role": "assistant",
                            "stop_reason": "end_turn",
                            "content": [{"type": "text", "text": "Final diagnosis."}],
                        },
                    },
                ],
            )

            turn = adapter.extract_claude_turn(path, "pane-1", "session-1")

        self.assertTrue(turn["available"])
        self.assertTrue(turn["complete"])
        self.assertEqual(turn["turn_id"], "assistant-1")
        self.assertEqual(turn["user_text"], "Diagnose it.")
        self.assertEqual(turn["assistant_final_text"], "Final diagnosis.")

    def test_claude_suppresses_internal_task_notification_user_text(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "session-1.jsonl"
            write_jsonl(
                path,
                [
                    {
                        "type": "user",
                        "uuid": "user-1",
                        "message": {
                            "role": "user",
                            "content": "<task-notification><task-id>abc</task-id></task-notification>",
                        },
                    },
                    {
                        "type": "assistant",
                        "uuid": "assistant-1",
                        "timestamp": "2026-06-15T10:00:00Z",
                        "message": {
                            "role": "assistant",
                            "stop_reason": "end_turn",
                            "content": [{"type": "text", "text": "Final diagnosis."}],
                        },
                    },
                ],
            )

            turn = adapter.extract_claude_turn(path, "pane-1", "session-1")

        self.assertTrue(turn["complete"])
        self.assertEqual(turn["user_text"], "")
        self.assertEqual(turn["assistant_final_text"], "Final diagnosis.")

    def test_claude_returns_latest_complete_when_newer_user_turn_is_open(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "session-1.jsonl"
            write_jsonl(
                path,
                [
                    {
                        "type": "user",
                        "uuid": "user-1",
                        "message": {"role": "user", "content": "What finished?"},
                    },
                    {
                        "type": "assistant",
                        "uuid": "assistant-1",
                        "timestamp": "2026-06-15T10:00:00Z",
                        "message": {
                            "role": "assistant",
                            "stop_reason": "end_turn",
                            "content": [{"type": "text", "text": "The completed result."}],
                        },
                    },
                    {
                        "type": "user",
                        "uuid": "user-2",
                        "message": {"role": "user", "content": "This newer turn is still open."},
                    },
                ],
            )

            turn = adapter.extract_claude_turn(path, "pane-1", "session-1")

        self.assertTrue(turn["available"])
        self.assertTrue(turn["complete"])
        self.assertEqual(turn["turn_id"], "assistant-1")
        self.assertEqual(turn["user_text"], "What finished?")
        self.assertEqual(turn["assistant_final_text"], "The completed result.")
        self.assertTrue(turn["has_open_turn"])
        self.assertEqual(turn["open_turn_id"], "user-2")

    def test_claude_no_session_id_can_match_unique_visible_session(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            project = root / "-tmp-project"
            project.mkdir()
            matched = project / "session-match.jsonl"
            other = project / "session-other.jsonl"
            final = (
                "Yes, done. The updater is working for Codex panes, and Claude panes were skipped "
                "because Herdr did not expose a session id. The fix is to match a unique visible session."
            )
            write_jsonl(
                matched,
                [
                    {"type": "user", "uuid": "u1", "message": {"content": "Check updater."}},
                    {
                        "type": "assistant",
                        "uuid": "a1",
                        "timestamp": "now",
                        "message": {"stop_reason": "end_turn", "content": [{"text": final}]},
                    },
                ],
            )
            write_jsonl(
                other,
                [
                    {"type": "user", "uuid": "u2", "message": {"content": "Other task."}},
                    {
                        "type": "assistant",
                        "uuid": "a2",
                        "timestamp": "now",
                        "message": {"stop_reason": "end_turn", "content": [{"text": "Different final answer."}]},
                    },
                ],
            )
            pane = {
                "pane_id": "pane-1",
                "agent": "claude",
                "cwd": "/tmp/project",
                "foreground_cwd": "/tmp/project",
                "agent_session": None,
            }
            with patch.object(adapter, "pane_from_list", Mock(return_value=pane)), patch.dict(
                adapter.os.environ,
                {"CLAUDE_PROJECTS_DIR": str(root)},
            ), patch.object(
                adapter,
                "pane_recent_text",
                Mock(return_value="prefix " + final + " suffix"),
            ):
                result = adapter.pane_turn("pane-1")

        turn = result["result"]["turn"]
        self.assertTrue(turn["available"])
        self.assertTrue(turn["complete"])
        self.assertEqual(turn["agent_session_id"], "session-match")
        self.assertEqual(turn["session_match_source"], "pane_visible_match")
        self.assertEqual(turn["assistant_final_text"], final)

    def test_claude_visible_session_fallback_fails_closed_when_ambiguous(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            project = root / "-tmp-project"
            project.mkdir()
            final = (
                "The same visible final answer appears in two candidate sessions, so the adapter "
                "must not guess which one belongs to the pane."
            )
            for name in ("session-a.jsonl", "session-b.jsonl"):
                write_jsonl(
                    project / name,
                    [
                        {"type": "user", "uuid": "u", "message": {"content": "Check updater."}},
                        {
                            "type": "assistant",
                            "uuid": name,
                            "timestamp": "now",
                            "message": {"stop_reason": "end_turn", "content": [{"text": final}]},
                        },
                    ],
                )
            pane = {
                "pane_id": "pane-1",
                "agent": "claude",
                "cwd": "/tmp/project",
                "foreground_cwd": "/tmp/project",
                "agent_session": None,
            }
            with patch.object(adapter, "pane_from_list", Mock(return_value=pane)), patch.dict(
                adapter.os.environ,
                {"CLAUDE_PROJECTS_DIR": str(root)},
            ), patch.object(
                adapter,
                "pane_recent_text",
                Mock(return_value=final),
            ):
                result = adapter.pane_turn("pane-1")

        turn = result["result"]["turn"]
        self.assertFalse(turn["available"])
        self.assertEqual(turn["reason"], "ambiguous_claude_session_match")

    def test_codex_pane_turn_requires_agent_session_id(self) -> None:
        with patch.object(
            adapter,
            "pane_from_list",
            Mock(return_value={"pane_id": "pane-1", "agent": "codex", "agent_session": None}),
        ):
            result = adapter.pane_turn("pane-1")

        turn = result["result"]["turn"]
        self.assertFalse(turn["available"])
        self.assertEqual(turn["reason"], "no_agent_session_id")

    def test_pane_turn_reports_herdr_list_failure(self) -> None:
        with patch.object(adapter, "pane_from_list", Mock(return_value={"_adapter_error": "herdr_list_failed"})):
            result = adapter.pane_turn("pane-1")

        turn = result["result"]["turn"]
        self.assertFalse(turn["available"])
        self.assertEqual(turn["reason"], "herdr_list_failed")

    def test_non_turn_commands_delegate_to_real_herdr(self) -> None:
        exec_real = Mock()

        with patch.object(adapter.sys, "argv", ["herdr_turn_adapter.py", "pane", "list"]), patch.object(
            adapter,
            "exec_real_herdr",
            exec_real,
        ):
            result = adapter.main()

        exec_real.assert_called_once_with()
        self.assertEqual(result, 127)


if __name__ == "__main__":
    unittest.main()
