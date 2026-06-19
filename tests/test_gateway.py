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


ROOT = Path(__file__).resolve().parents[1]


def load_gateway_module(filename: str, module_name: str):
    module_path = ROOT / filename
    spec = importlib.util.spec_from_file_location(module_name, module_path)
    module = importlib.util.module_from_spec(spec)
    assert spec and spec.loader
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


gateway = load_gateway_module("herdres-gateway.py", "herdres_gateway_upstream")
managed_gateway = load_gateway_module("herdres_gateway.py", "herdres_gateway_managed")


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


class GatewayManagedBotTests(unittest.TestCase):
    def test_managed_bot_tokens_reads_enabled_child_tokens(self) -> None:
        state = {
            "version": 1,
            "enabled": True,
            "telegram": {
                "managed_bots": {
                    "codex": {"token": "CODEX_TOKEN", "enabled": True},
                    "claude": {"token": "CLAUDE_TOKEN", "enabled": False},
                    "kimi": {"token": ""},
                }
            },
        }

        tokens = managed_gateway.managed_bot_tokens(state)

        self.assertEqual(len(tokens), 1)
        self.assertTrue(tokens[0][0].startswith("managed-codex-"))
        self.assertEqual(tokens[0][1], "CODEX_TOKEN")

    def test_handle_update_dispatches_managed_bot_created_message(self) -> None:
        handler = Mock()
        update = {
            "update_id": 7,
            "message": {
                "from": {"id": 42},
                "managed_bot_created": {"bot": {"id": 111, "username": "herdr_codex_bot"}},
            },
        }

        with patch.object(managed_gateway, "handle_managed_bot_update", handler):
            managed_gateway.handle_update(update, bot_token="MANAGER_TOKEN")

        handler.assert_called_once_with({"message": update["message"]})

    def test_offset_path_is_per_managed_bot(self) -> None:
        manager_path = managed_gateway.offset_path_for("manager")
        child_path = managed_gateway.offset_path_for("managed-codex-token")

        self.assertEqual(manager_path, managed_gateway.OFFSET_PATH)
        self.assertNotEqual(child_path, managed_gateway.OFFSET_PATH)
        self.assertTrue(str(child_path).endswith("gateway_offset.managed-codex-token"))

    def test_poll_timeout_plan_keeps_manager_poll_fast_with_child_bots(self) -> None:
        child_bots = [
            ("managed-codex-token", "CODEX_TOKEN"),
            ("managed-kimi-token", "KIMI_TOKEN"),
        ]

        plan = managed_gateway.poll_timeout_plan(child_bots)

        self.assertEqual(plan[0], ("manager", managed_gateway.TOKEN, 1))
        self.assertEqual(
            plan[1:],
            [
                ("managed-codex-token", "CODEX_TOKEN", 0),
                ("managed-kimi-token", "KIMI_TOKEN", 0),
            ],
        )

    def test_handle_message_dispatches_targeted_shared_topic_mention_without_child_token(self) -> None:
        state = {
            "version": 1,
            "enabled": True,
            "telegram": {
                "chat_id": "-1001",
                "general_thread_id": "1",
                "owner_user_ids": ["42"],
                "managed_bots": {
                    "claude": {"username": "herdr_claude_bot", "enabled": True}
                },
            },
            "spaces": {
                "workspace:workspace-1": {
                    "space_key": "workspace:workspace-1",
                    "topic_id": "77",
                    "pane_keys": ["pane-1", "pane-2"],
                }
            },
            "panes": {
                "pane-1": {
                    "pane_key": "pane-1",
                    "pane_id": "pane-1",
                    "space_key": "workspace:workspace-1",
                    "topic_id": "77",
                    "last_known_status": "working",
                    "agent": "codex",
                },
                "pane-2": {
                    "pane_key": "pane-2",
                    "pane_id": "pane-2",
                    "space_key": "workspace:workspace-1",
                    "topic_id": "77",
                    "last_known_status": "working",
                    "agent": "claude",
                },
            },
        }
        run_script = Mock(return_value={"handled": True, "reply": ""})
        api = Mock()

        with patch.object(managed_gateway, "load_state", Mock(return_value=state)), patch.object(
            managed_gateway, "run_script", run_script
        ), patch.object(managed_gateway, "api", api):
            managed_gateway.handle_message(
                {
                    "message_id": 4000,
                    "message_thread_id": 77,
                    "chat": {"id": -1001, "is_forum": True},
                    "from": {"id": 42, "is_bot": False},
                    "text": "@herdr_claude_bot run tests",
                },
                bot_token="MANAGER_TOKEN",
            )

        api.assert_not_called()
        run_script.assert_called_once()
        payload = run_script.call_args.args[0]
        self.assertEqual(payload["target_bot_kind"], "claude")
        self.assertEqual(payload["text"], "@herdr_claude_bot run tests")

    def test_manager_defers_reply_to_configured_child_bot(self) -> None:
        state = {
            "version": 1,
            "enabled": True,
            "telegram": {
                "chat_id": "-1001",
                "general_thread_id": "1",
                "owner_user_ids": ["42"],
                "managed_bots": {
                    "codex": {"username": "herdr_codex_bot", "token": "CODEX_TOKEN", "enabled": True}
                },
            },
            "spaces": {
                "workspace:workspace-1": {
                    "space_key": "workspace:workspace-1",
                    "topic_id": "77",
                    "pane_keys": ["pane-1"],
                }
            },
            "panes": {
                "pane-1": {
                    "pane_key": "pane-1",
                    "pane_id": "pane-1",
                    "space_key": "workspace:workspace-1",
                    "topic_id": "77",
                    "last_known_status": "idle",
                    "agent": "codex",
                },
            },
        }
        run_script = Mock(return_value={"handled": True, "reply": "Send failed"})
        api = Mock()

        with patch.object(managed_gateway, "load_state", Mock(return_value=state)), patch.object(
            managed_gateway, "run_script", run_script
        ), patch.object(managed_gateway, "api", api):
            managed_gateway.handle_message(
                {
                    "message_id": 4000,
                    "message_thread_id": 77,
                    "chat": {"id": -1001, "is_forum": True},
                    "from": {"id": 42, "is_bot": False},
                    "reply_to_message": {
                        "message_id": 3999,
                        "from": {"id": 8873456652, "is_bot": True, "username": "herdr_codex_bot"},
                    },
                    "text": "okay create PoCs",
                },
                bot_token="MANAGER_TOKEN",
                bot_key="manager",
            )

        run_script.assert_not_called()
        api.assert_not_called()

    def test_child_bot_update_sets_target_bot_kind(self) -> None:
        state = {
            "version": 1,
            "enabled": True,
            "telegram": {"chat_id": "-1001", "general_thread_id": "1", "owner_user_ids": ["42"]},
            "spaces": {
                "workspace:workspace-1": {
                    "space_key": "workspace:workspace-1",
                    "topic_id": "77",
                    "pane_keys": ["pane-1"],
                }
            },
            "panes": {
                "pane-1": {
                    "pane_key": "pane-1",
                    "pane_id": "pane-1",
                    "space_key": "workspace:workspace-1",
                    "topic_id": "77",
                    "last_known_status": "working",
                    "agent": "claude",
                }
            },
        }
        run_script = Mock(return_value={"handled": True, "reply": ""})

        with patch.object(managed_gateway, "load_state", Mock(return_value=state)), patch.object(
            managed_gateway, "run_script", run_script
        ):
            managed_gateway.handle_update(
                {
                    "update_id": 7,
                    "message": {
                        "message_id": 4000,
                        "message_thread_id": 77,
                        "chat": {"id": -1001, "is_forum": True},
                        "from": {"id": 42, "is_bot": False},
                        "text": "run tests",
                    },
                },
                bot_token="CLAUDE_TOKEN",
                bot_key="managed-claude-deadbeef",
            )

        run_script.assert_called_once()
        payload = run_script.call_args.args[0]
        self.assertEqual(payload["target_bot_kind"], "claude")


if __name__ == "__main__":
    unittest.main()
