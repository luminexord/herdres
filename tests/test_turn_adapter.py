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

    def test_pane_turn_requires_agent_session_id(self) -> None:
        with patch.object(
            adapter,
            "pane_from_list",
            Mock(return_value={"pane_id": "pane-1", "agent": "claude", "agent_session": None}),
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


if __name__ == "__main__":
    unittest.main()
