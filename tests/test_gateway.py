from __future__ import annotations

import importlib.util
import json
import os
import subprocess
import sys
import tempfile
import types
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

import herdres_routing


MODULE_PATH = Path(__file__).resolve().parents[1] / "herdres-gateway.py"
SPEC = importlib.util.spec_from_file_location("herdres_gateway", MODULE_PATH)
gateway = importlib.util.module_from_spec(SPEC)
assert SPEC and SPEC.loader
sys.modules[SPEC.name] = gateway
SPEC.loader.exec_module(gateway)


def object_message(**overrides):
    defaults = {
        "text": "hello",
        "caption": "",
        "document": None,
        "photo": None,
        "chat": types.SimpleNamespace(id=-1001, is_forum=True),
        "message_thread_id": 77,
        "message_id": 9,
        "from_user": types.SimpleNamespace(id=42, is_bot=False),
        "reply_to_message": types.SimpleNamespace(message_id=8),
        "edit_date": None,
    }
    defaults.update(overrides)
    return types.SimpleNamespace(**defaults)


def dict_message(**overrides):
    defaults = {
        "text": "hello",
        "caption": "",
        "chat": {"id": -1001, "is_forum": True},
        "message_thread_id": 77,
        "message_id": 9,
        "from": {"id": 42, "is_bot": False},
        "reply_to_message": {"message_id": 8},
    }
    defaults.update(overrides)
    return defaults


class GatewayTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        root = Path(self.tmp.name)
        self.state_path = root / "state.json"
        self.offset_path = root / "gateway.offset"
        self.config = gateway.GatewayConfig(
            token="TOKEN",
            state_path=self.state_path,
            script_path=Path("/bin/echo"),
            offset_path=self.offset_path,
            long_poll_seconds=0,
            error_backoff=0,
        )
        self.write_state()

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def write_state(self, *, owners=None, topic_id="77") -> None:
        state = {
            "version": 1,
            "enabled": True,
            "telegram": {
                "chat_id": "-1001",
                "general_thread_id": "1",
                "owner_user_ids": ["42"] if owners is None else owners,
            },
            "panes": {"pane": {"pane_id": "pane-1", "topic_id": topic_id}},
        }
        self.state_path.write_text(json.dumps(state), encoding="utf-8")

    def test_build_command_payload_matches_bridge_contract(self) -> None:
        expected = herdres_routing.build_command_payload_obj(object_message())
        payload = gateway.build_command_payload_dict(dict_message())

        self.assertEqual(payload, expected)
        self.assertEqual(
            list(payload.keys()),
            [
                "chat_id",
                "topic_id",
                "message_id",
                "reply_to_message_id",
                "user_id",
                "from_bot",
                "forwarded",
                "edited",
                "text",
                "caption",
                "attachment",
            ],
        )

    def test_message_in_unmapped_topic_is_ignored(self) -> None:
        with patch.object(gateway, "run_herdres") as run_herdres, patch.object(gateway, "send_message") as send_message:
            gateway.handle_message(self.config, dict_message(message_thread_id=999))

        run_herdres.assert_not_called()
        send_message.assert_not_called()

    def test_general_topic_is_ignored(self) -> None:
        message = dict_message()
        message.pop("message_thread_id")
        with patch.object(gateway, "run_herdres") as run_herdres, patch.object(gateway, "send_message") as send_message:
            gateway.handle_message(self.config, message)

        run_herdres.assert_not_called()
        send_message.assert_not_called()

    def test_owner_prefilter_drops_non_owner_messages(self) -> None:
        message = dict_message(**{"from": {"id": 99, "is_bot": False}})
        with patch.object(gateway, "run_herdres") as run_herdres, patch.object(gateway, "send_message") as send_message:
            gateway.handle_message(self.config, message)

        run_herdres.assert_not_called()
        send_message.assert_not_called()

    def test_callback_routing(self) -> None:
        query = {
            "id": "cb-1",
            "data": "herdr:c:p:1",
            "from": {"id": 42},
            "message": {"message_id": 44, "message_thread_id": 77, "chat": {"id": -1001, "is_forum": True}},
        }
        run_herdres = Mock(return_value={"handled": True, "answer": "ok", "show_alert": True})
        api = Mock(return_value={"ok": True, "result": True})

        with patch.object(gateway, "run_herdres", run_herdres), patch.object(gateway, "telegram_api", api):
            gateway.handle_callback(self.config, query)

        run_herdres.assert_called_once()
        self.assertEqual(
            run_herdres.call_args.args[2],
            {"chat_id": "-1001", "topic_id": "77", "message_id": "44", "user_id": "42", "data": "herdr:c:p:1"},
        )
        api.assert_called_once_with(
            "TOKEN",
            "answerCallbackQuery",
            {"callback_query_id": "cb-1", "text": "ok", "show_alert": "true"},
            timeout=15.0,
        )

    def test_first_start_backlog_drain(self) -> None:
        api = Mock(return_value={"ok": True, "result": [{"update_id": 5}, {"update_id": 8}]})
        with patch.object(gateway, "telegram_api", api), patch.object(gateway, "handle_update") as handle_update:
            gateway.poll_once(self.config)

        handle_update.assert_not_called()
        self.assertEqual(self.offset_path.read_text(encoding="utf-8").strip(), "9")
        api.assert_called_once()
        self.assertEqual(api.call_args.args[1], "getUpdates")
        self.assertEqual(api.call_args.args[2]["timeout"], 0)

    def test_atomic_offset_persistence(self) -> None:
        real_replace = os.replace
        calls = []

        def replacing(temp, final):
            calls.append((Path(temp), Path(final)))
            real_replace(temp, final)

        with patch.object(gateway.os, "replace", side_effect=replacing):
            gateway.write_offset_atomic(self.offset_path, 123)

        self.assertEqual(self.offset_path.read_text(encoding="utf-8").strip(), "123")
        self.assertEqual(calls[0][0].parent, self.offset_path.parent)
        self.assertEqual(calls[0][1], self.offset_path)

    def test_poll_error_preserves_offset(self) -> None:
        self.offset_path.write_text("12\n", encoding="utf-8")
        with patch.object(gateway, "get_updates", side_effect=RuntimeError("network")), patch.object(gateway.time, "sleep"):
            gateway.poll_once(self.config)

        self.assertEqual(self.offset_path.read_text(encoding="utf-8").strip(), "12")

    def test_subprocess_timeout_does_not_crash_and_advances_offset(self) -> None:
        self.offset_path.write_text("10\n", encoding="utf-8")
        update = {"update_id": 10, "message": dict_message(message_id=55)}
        api = Mock(return_value={"ok": True})
        with patch.object(gateway, "get_updates", Mock(return_value=[update])), patch.object(
            gateway.subprocess, "run", side_effect=subprocess.TimeoutExpired(["herdres", "command"], 25)
        ), patch.object(gateway, "telegram_api", api):
            gateway.poll_once(self.config)

        self.assertEqual(self.offset_path.read_text(encoding="utf-8").strip(), "11")
        self.assertEqual(api.call_args.args[1], "sendMessage")
        self.assertIn("timed out", api.call_args.args[2]["text"])

    def test_invalid_json_subprocess_output_does_not_crash_and_advances_offset(self) -> None:
        self.offset_path.write_text("20\n", encoding="utf-8")
        update = {"update_id": 20, "message": dict_message(message_id=56)}
        proc = subprocess.CompletedProcess(["herdres", "command"], 0, stdout="not json", stderr="")
        api = Mock(return_value={"ok": True})
        with patch.object(gateway, "get_updates", Mock(return_value=[update])), patch.object(
            gateway.subprocess, "run", Mock(return_value=proc)
        ), patch.object(gateway, "telegram_api", api):
            gateway.poll_once(self.config)

        self.assertEqual(self.offset_path.read_text(encoding="utf-8").strip(), "21")
        self.assertEqual(api.call_args.args[1], "sendMessage")
        self.assertIn("invalid output", api.call_args.args[2]["text"])


if __name__ == "__main__":
    unittest.main()
