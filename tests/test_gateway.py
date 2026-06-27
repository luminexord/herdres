from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
import threading
import time
import types
import unittest
import urllib.error
from pathlib import Path
from unittest.mock import Mock, patch
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
if str(ROOT / "tests") not in sys.path:
    sys.path.insert(0, str(ROOT / "tests"))
if "pytest" not in sys.modules:
    class _PytestMark:
        def parametrize(self, *args, **kwargs):
            return lambda func: func

    sys.modules["pytest"] = types.SimpleNamespace(
        fixture=lambda *args, **kwargs: (lambda func: func),
        mark=_PytestMark(),
    )


import conftest  # noqa: F401
import herdres
import herdres_routing
import herdres_gateway_upstream as gateway
import herdres_gateway_managed as managed_gateway


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


def managed_multi_pane_space_state() -> dict:
    return {
        "version": 1,
        "enabled": True,
        "telegram": {
            "chat_id": "-1001",
            "general_thread_id": "1",
            "owner_user_ids": ["42"],
        },
        "spaces": {
            "workspace:workspace-1": {
                "space_key": "workspace:workspace-1",
                "topic_id": "77",
                "pane_keys": ["pane-1", "pane-2"],
                "message_routes": {},
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


class GatewayUpstreamMultiPaneTests(unittest.TestCase):
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

    def write_state(self) -> None:
        state = {
            "version": 1,
            "enabled": True,
            "telegram": {
                "chat_id": "-1001",
                "general_thread_id": "1",
                "owner_user_ids": ["42"],
            },
            "spaces": {
                "workspace:workspace-1": {
                    "space_key": "workspace:workspace-1",
                    "topic_id": "77",
                    "pane_keys": ["pane-1", "pane-2"],
                    "message_routes": {},
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
        self.state_path.write_text(json.dumps(state), encoding="utf-8")

    def test_upstream_multipane_command_agents(self) -> None:
        run_herdres = Mock(return_value={"handled": True, "reply": ""})
        send_message = Mock()
        message = dict_message(text="/agents", message_id=9101, reply_to_message=None)

        with patch.object(gateway, "run_herdres", run_herdres), patch.object(gateway, "send_message", send_message):
            gateway.handle_message(self.config, message)

        run_herdres.assert_called_once()
        self.assertEqual(run_herdres.call_args.args[0], self.config)
        self.assertEqual(run_herdres.call_args.args[1], "command")
        self.assertEqual(run_herdres.call_args.args[2]["text"], "/agents")
        send_message.assert_not_called()

    def test_upstream_multipane_command_text(self) -> None:
        run_herdres = Mock(return_value={"handled": True, "reply": ""})
        send_message = Mock()
        message = dict_message(text="hello", message_id=9102, reply_to_message=None)

        with patch.object(gateway, "run_herdres", run_herdres), patch.object(gateway, "send_message", send_message):
            gateway.handle_message(self.config, message)

        run_herdres.assert_called_once()
        self.assertEqual(run_herdres.call_args.args[0], self.config)
        self.assertEqual(run_herdres.call_args.args[1], "command")
        self.assertEqual(run_herdres.call_args.args[2]["text"], "hello")
        send_message.assert_not_called()

    def test_upstream_multipane_callback_ob(self) -> None:
        run_herdres = Mock(return_value={"handled": True, "answer": "ok"})
        answer_callback = Mock()
        query = {
            "id": "cb-ob",
            "data": "herdr:ob:space-token:codex",
            "from": {"id": 42},
            "message": {"message_id": 9201, "message_thread_id": 77, "chat": {"id": -1001, "is_forum": True}},
        }

        with patch.object(gateway, "run_herdres", run_herdres), patch.object(
            gateway, "answer_callback", answer_callback
        ):
            gateway.handle_callback(self.config, query)

        run_herdres.assert_called_once()
        self.assertEqual(run_herdres.call_args.args[0], self.config)
        self.assertEqual(run_herdres.call_args.args[1], "callback")
        self.assertEqual(run_herdres.call_args.args[2]["data"], "herdr:ob:space-token:codex")
        answer_callback.assert_called_once()

    def test_upstream_multipane_callback_ag(self) -> None:
        run_herdres = Mock(return_value={"handled": True, "answer": "ok"})
        answer_callback = Mock()
        query = {
            "id": "cb-ag",
            "data": "herdr:ag:space-token:pane-token",
            "from": {"id": 42},
            "message": {"message_id": 9202, "message_thread_id": 77, "chat": {"id": -1001, "is_forum": True}},
        }

        with patch.object(gateway, "run_herdres", run_herdres), patch.object(
            gateway, "answer_callback", answer_callback
        ):
            gateway.handle_callback(self.config, query)

        run_herdres.assert_called_once()
        self.assertEqual(run_herdres.call_args.args[0], self.config)
        self.assertEqual(run_herdres.call_args.args[1], "callback")
        self.assertEqual(run_herdres.call_args.args[2]["data"], "herdr:ag:space-token:pane-token")
        answer_callback.assert_called_once()


class GatewayManagedBotTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        processed_patch = patch.object(managed_gateway, "PROCESSED_PATH", Path(self.tmp.name) / "processed.json")
        processed_patch.start()
        self.addCleanup(processed_patch.stop)
        offset_patch = patch.object(managed_gateway, "OFFSET_PATH", Path(self.tmp.name) / "gateway_offset")
        offset_patch.start()
        self.addCleanup(offset_patch.stop)
        reassembly_patch = patch.object(managed_gateway, "REASSEMBLY_PATH", Path(self.tmp.name) / "gateway_reassembly.json")
        reassembly_patch.start()
        self.addCleanup(reassembly_patch.stop)
        managed_gateway.PROCESSED_MESSAGE_KEYS = None
        managed_gateway.PROCESSED_MESSAGE_ORDER = []
        if hasattr(managed_gateway, "QUARANTINED_KEYS"):
            managed_gateway.QUARANTINED_KEYS.clear()
        self.addCleanup(setattr, managed_gateway, "PROCESSED_MESSAGE_KEYS", None)
        self.addCleanup(setattr, managed_gateway, "PROCESSED_MESSAGE_ORDER", [])
        if hasattr(managed_gateway, "QUARANTINED_KEYS"):
            self.addCleanup(managed_gateway.QUARANTINED_KEYS.clear)
        if hasattr(managed_gateway, "DISPATCH_QUEUE_SEMAPHORE"):
            old_semaphore = managed_gateway.DISPATCH_QUEUE_SEMAPHORE
            managed_gateway.DISPATCH_QUEUE_SEMAPHORE = threading.BoundedSemaphore(128)
            self.addCleanup(setattr, managed_gateway, "DISPATCH_QUEUE_SEMAPHORE", old_semaphore)

    def single_pane_space_state(self) -> dict:
        state = managed_multi_pane_space_state()
        state["spaces"]["workspace:workspace-1"]["pane_keys"] = ["pane-1"]
        state["panes"] = {"pane-1": state["panes"]["pane-1"]}
        return state

    def reassembly_message(self, message_id: int, text: str) -> dict:
        return {
            "message_id": message_id,
            "message_thread_id": 77,
            "chat": {"id": -1001, "is_forum": True},
            "from": {"id": 42, "is_bot": False},
            "text": text,
        }

    def poll_update(self, update_id: int, message: dict) -> dict:
        return {"update_id": update_id, "message": message}

    def read_reassembly_buffers(self) -> dict:
        raw = json.loads(managed_gateway.REASSEMBLY_PATH.read_text(encoding="utf-8"))
        return raw.get("buffers") or {}

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

    def test_poll_worker_specs_long_poll_each_bot_independently(self) -> None:
        child_bots = [
            ("managed-codex-token", "CODEX_TOKEN"),
            ("managed-kimi-token", "KIMI_TOKEN"),
        ]

        with patch.object(managed_gateway, "TOKEN", "MANAGER_TOKEN"), patch.object(
            managed_gateway, "LONG_POLL_SECONDS", 50
        ), patch.object(managed_gateway, "CHILD_POLL_SECONDS", 0):
            plan = managed_gateway.poll_worker_specs(child_bots)

        self.assertEqual(plan[0], ("manager", "MANAGER_TOKEN", 50))
        self.assertEqual(
            plan[1:],
            [
                ("managed-codex-token", "CODEX_TOKEN", 50),
                ("managed-kimi-token", "KIMI_TOKEN", 50),
            ],
        )

    def test_zero_children_runs_only_manager_worker(self) -> None:
        with patch.object(managed_gateway, "TOKEN", "MANAGER_TOKEN"), patch.object(managed_gateway, "LONG_POLL_SECONDS", 50):
            self.assertEqual(managed_gateway.poll_worker_specs([]), [("manager", "MANAGER_TOKEN", 50)])

    def test_allowed_updates_includes_managed_bot(self) -> None:
        self.assertIn("managed_bot", json.loads(managed_gateway.ALLOWED_UPDATES))

    def test_handle_update_routes_managed_bot_and_created(self) -> None:
        handler = Mock()
        with patch.object(managed_gateway, "handle_managed_bot_update", handler):
            managed_gateway.handle_update({"update_id": 1, "managed_bot": {"id": 1}}, bot_token="MANAGER_TOKEN")
            managed_gateway.handle_update(
                {
                    "update_id": 2,
                    "message": {
                        "from": {"id": 42},
                        "managed_bot_created": {"bot": {"id": 2, "username": "herdr_codex_bot"}},
                    },
                },
                bot_token="MANAGER_TOKEN",
            )

        self.assertEqual(handler.call_count, 2)

    def test_reconcile_starts_stops_rotates_workers(self) -> None:
        started: list[str] = []

        class FakeThread:
            def __init__(self, *, target, args, name, daemon):
                self.name = name

            def start(self):
                started.append(self.name)

        workers: dict[str, dict[str, object]] = {}
        with patch.object(managed_gateway.threading, "Thread", FakeThread):
            managed_gateway.reconcile_poll_workers(workers, [("manager", "MANAGER", 50), ("managed-codex-a", "A", 50)])
            old_stop = workers["managed-codex-a"]["stop"]
            managed_gateway.reconcile_poll_workers(workers, [("manager", "MANAGER", 50), ("managed-codex-b", "B", 50)])

        self.assertIn("manager", workers)
        self.assertIn("managed-codex-b", workers)
        self.assertNotIn("managed-codex-a", workers)
        self.assertTrue(old_stop.is_set())
        self.assertIn("herdres-gateway-managed-codex-b", started)

    def test_child_callback_answered_with_child_token(self) -> None:
        state = managed_multi_pane_space_state()
        run_script = Mock(return_value={"handled": True, "answer": "ok"})
        api = Mock(return_value={"ok": True, "result": True})

        with patch.object(managed_gateway, "load_state", Mock(return_value=state)), patch.object(
            managed_gateway, "run_script", run_script
        ), patch.object(managed_gateway, "api", api):
            managed_gateway.handle_callback(
                {
                    "id": "cb-child",
                    "from": {"id": 42},
                    "data": "herdr:mb:space-token:up",
                    "message": {
                        "message_id": 8201,
                        "message_thread_id": 77,
                        "chat": {"id": -1001, "is_forum": True},
                    },
                },
                bot_token="CHILD_TOKEN",
            )

        run_script.assert_called_once()
        api.assert_called_once()
        self.assertEqual(api.call_args.kwargs["token"], "CHILD_TOKEN")

    def test_child_message_replies_with_child_token(self) -> None:
        state = managed_multi_pane_space_state()
        state["telegram"]["managed_bots"] = {"codex": {"token": "CHILD_TOKEN", "enabled": True}}
        run_script = Mock(return_value={"handled": True, "reply": "done"})
        api = Mock(return_value={"ok": True, "result": True})

        with patch.object(managed_gateway, "load_state", Mock(return_value=state)), patch.object(
            managed_gateway, "run_script", run_script
        ), patch.object(managed_gateway, "api", api):
            managed_gateway.handle_message(
                {
                    "message_id": 4000,
                    "message_thread_id": 77,
                    "chat": {"id": -1001, "is_forum": True},
                    "from": {"id": 42, "is_bot": False},
                    "text": "@herdr_codex_bot run tests",
                },
                bot_token="CHILD_TOKEN",
                bot_key="managed-codex-deadbeef",
            )

        api.assert_called_once()
        self.assertEqual(api.call_args.kwargs["token"], "CHILD_TOKEN")

    def test_manager_path_unchanged_when_children_absent(self) -> None:
        state = managed_multi_pane_space_state()
        run_script = Mock(return_value={"handled": True, "reply": ""})

        with patch.object(managed_gateway, "load_state", Mock(return_value=state)), patch.object(
            managed_gateway, "run_script", run_script
        ), patch.object(managed_gateway, "api", Mock()):
            managed_gateway.handle_message(
                {
                    "message_id": 4100,
                    "message_thread_id": 77,
                    "chat": {"id": -1001, "is_forum": True},
                    "from": {"id": 42, "is_bot": False},
                    "text": "/new codex",
                },
                bot_token="MANAGER_TOKEN",
                bot_key="manager",
            )

        run_script.assert_called_once()
        self.assertEqual(run_script.call_args.args[0]["pane_key"], "")

    def test_handle_message_reassembles_two_split_fragments_once(self) -> None:
        state = self.single_pane_space_state()
        run_script = Mock(return_value={"handled": True, "reply": ""})
        first = "a" * 4041
        second = "b" * 2408

        with patch.object(managed_gateway, "load_state", Mock(return_value=state)), patch.object(
            managed_gateway, "run_script", run_script
        ), patch.object(managed_gateway, "api", Mock()):
            managed_gateway.handle_message(self.reassembly_message(5000, first), bot_token="MANAGER_TOKEN", bot_key="manager")
            self.assertEqual(run_script.call_count, 0)
            managed_gateway.handle_message(self.reassembly_message(5001, second), bot_token="MANAGER_TOKEN", bot_key="manager")

        run_script.assert_called_once()
        self.assertEqual(run_script.call_args.args[0]["text"], first + second)

    def test_handle_message_normal_short_dispatches_immediately_without_buffer(self) -> None:
        state = self.single_pane_space_state()
        run_script = Mock(return_value={"handled": True, "reply": ""})
        text = "x" * 3796

        with patch.object(managed_gateway, "load_state", Mock(return_value=state)), patch.object(
            managed_gateway, "run_script", run_script
        ), patch.object(managed_gateway, "api", Mock()):
            managed_gateway.handle_message(self.reassembly_message(5010, text), bot_token="MANAGER_TOKEN", bot_key="manager")

        run_script.assert_called_once()
        self.assertEqual(run_script.call_args.args[0]["text"], text)
        self.assertFalse(managed_gateway.REASSEMBLY_PATH.exists())

    def test_main_sweep_flushes_stale_buffer_with_origin_bot_routing(self) -> None:
        now = time.time()
        managed_gateway.REASSEMBLY_PATH.write_text(
            json.dumps(
                {
                    "version": 1,
                    "buffers": {
                        "-1001|77|42": {
                            "first_at": now - 30,
                            "updated_at": now - 30,
                            "fragments": [{"message_id": 5020, "text": "a" * 4041}],
                            "sample": {
                                "chat_id": "-1001",
                                "thread_id": "77",
                                "pane_key": "pane-1",
                                "user_id": "42",
                                "reply_to_message_id": "",
                                "from_bot": False,
                                "forwarded": False,
                                "edited": False,
                                "bot_token": "CHILD_TOKEN",
                                "bot_key": "managed-codex-deadbeef",
                            },
                        }
                    },
                }
            ),
            encoding="utf-8",
        )
        run_script = Mock(return_value={"handled": True, "reply": "done"})
        send_reply = Mock()

        with patch.object(managed_gateway, "_token", Mock(return_value="MANAGER_TOKEN")), patch.object(
            managed_gateway, "api", Mock(return_value={"ok": True, "result": {"url": ""}})
        ), patch.object(managed_gateway, "load_state", Mock(return_value=None)), patch.object(
            managed_gateway, "run_script", run_script
        ), patch.object(managed_gateway, "send_reply", send_reply), patch.object(
            managed_gateway.time, "sleep", Mock(side_effect=KeyboardInterrupt)
        ):
            with self.assertRaises(KeyboardInterrupt):
                managed_gateway.main()

        run_script.assert_called_once()
        self.assertEqual(run_script.call_args.args[0]["text"], "a" * 4041)
        send_reply.assert_called_once_with("CHILD_TOKEN", "-1001", "77", "done", reply_to_message_id=None)

    def test_handle_message_chain_break_flushes_old_and_processes_new(self) -> None:
        state = self.single_pane_space_state()
        run_script = Mock(return_value={"handled": True, "reply": ""})
        first = "a" * 4041
        breaker = "new prompt"

        with patch.object(managed_gateway, "load_state", Mock(return_value=state)), patch.object(
            managed_gateway, "run_script", run_script
        ), patch.object(managed_gateway, "api", Mock()):
            managed_gateway.handle_message(self.reassembly_message(5030, first), bot_token="MANAGER_TOKEN", bot_key="manager")
            managed_gateway.handle_message(self.reassembly_message(5033, breaker), bot_token="MANAGER_TOKEN", bot_key="manager")

        self.assertEqual(run_script.call_count, 2)
        self.assertEqual([call.args[0]["text"] for call in run_script.call_args_list], [first, breaker])

    def test_handle_message_out_of_order_fragments_concatenate_by_message_id(self) -> None:
        state = self.single_pane_space_state()
        run_script = Mock(return_value={"handled": True, "reply": ""})
        first = "a" * 4041
        second = "b" * 4041
        terminal = "c" * 128

        with patch.object(managed_gateway, "load_state", Mock(return_value=state)), patch.object(
            managed_gateway, "run_script", run_script
        ), patch.object(managed_gateway, "api", Mock()):
            managed_gateway.handle_message(self.reassembly_message(5041, second), bot_token="MANAGER_TOKEN", bot_key="manager")
            managed_gateway.handle_message(self.reassembly_message(5040, first), bot_token="MANAGER_TOKEN", bot_key="manager")
            self.assertEqual(run_script.call_count, 0)
            managed_gateway.handle_message(self.reassembly_message(5042, terminal), bot_token="MANAGER_TOKEN", bot_key="manager")

        run_script.assert_called_once()
        self.assertEqual(run_script.call_args.args[0]["text"], first + second + terminal)

    def test_poll_once_deletes_topic_icon_service_message(self) -> None:
        state = self.single_pane_space_state()
        icon_msg = {
            "message_id": 7000,
            "message_thread_id": 77,
            "chat": {"id": -1001, "is_forum": True},
            "forum_topic_edited": {"icon_custom_emoji_id": "5310000000000000001"},
        }
        managed_gateway.offset_path_for("manager").write_text("200\n", encoding="utf-8")
        calls = []

        def fake_api(method, params=None, timeout=30, *, token=None):
            calls.append((method, params, token))
            return {"ok": True, "result": [self.poll_update(200, icon_msg)] if method == "getUpdates" else True}

        prepare = Mock(return_value=None)
        with patch.object(managed_gateway, "clear_webhook"), patch.object(
            managed_gateway, "api", fake_api
        ), patch.object(managed_gateway, "load_state", Mock(return_value=state)), patch.object(
            managed_gateway, "prepare_message", prepare
        ):
            managed_gateway.poll_once("manager", "MANAGER_TOKEN", timeout_seconds=0)

        deletes = [c for c in calls if c[0] == "deleteMessage"]
        self.assertEqual(len(deletes), 1)
        self.assertEqual(deletes[0][1]["message_id"], "7000")
        self.assertEqual(deletes[0][2], "MANAGER_TOKEN")  # deleted with the manager's token
        prepare.assert_not_called()  # service message never reaches the dispatch path
        self.assertEqual(managed_gateway.offset_path_for("manager").read_text(encoding="utf-8").strip(), "201")

    def test_consume_icon_service_message_matrix(self) -> None:
        icon_msg = {"message_id": 1, "chat": {"id": -1001}, "forum_topic_edited": {"icon_custom_emoji_id": "5"}}
        name_msg = {"message_id": 2, "chat": {"id": -1001}, "forum_topic_edited": {"name": "Renamed"}}
        created_msg = {"message_id": 3, "chat": {"id": -1001}, "forum_topic_created": {"name": "New"}}

        # name-only edit and topic-created are NOT consumed (fall through to normal handling)
        self.assertFalse(managed_gateway.is_topic_icon_service_message(name_msg))
        self.assertFalse(managed_gateway.is_topic_icon_service_message(created_msg))
        self.assertTrue(managed_gateway.is_topic_icon_service_message(icon_msg))

        # manager deletes
        api = Mock(return_value={"ok": True, "result": True})
        with patch.object(managed_gateway, "api", api):
            self.assertTrue(managed_gateway.consume_topic_icon_service_message(icon_msg, bot_token="MANAGER_TOKEN", bot_key="manager"))
        api.assert_called_once()
        self.assertEqual(api.call_args.args[0], "deleteMessage")

        # name-only edit: not consumed, no delete
        api2 = Mock(return_value={"ok": True, "result": True})
        with patch.object(managed_gateway, "api", api2):
            self.assertFalse(managed_gateway.consume_topic_icon_service_message(name_msg, bot_token="MANAGER_TOKEN", bot_key="manager"))
        api2.assert_not_called()

        # child worker swallows but does NOT delete (only the manager deletes)
        api3 = Mock(return_value={"ok": True, "result": True})
        with patch.object(managed_gateway, "api", api3):
            self.assertTrue(managed_gateway.consume_topic_icon_service_message(icon_msg, bot_token="CHILD", bot_key="managed-codex-abc"))
        api3.assert_not_called()

        # env-gated off: consumed (suppressed) but not deleted
        api4 = Mock(return_value={"ok": True, "result": True})
        with patch.object(managed_gateway, "DELETE_ICON_SERVICE_MESSAGES", False), patch.object(managed_gateway, "api", api4):
            self.assertTrue(managed_gateway.consume_topic_icon_service_message(icon_msg, bot_token="MANAGER_TOKEN", bot_key="manager"))
        api4.assert_not_called()

        # delete failure is non-fatal (no exception escapes), still consumed
        api5 = Mock(side_effect=RuntimeError("boom"))
        with patch.object(managed_gateway, "api", api5):
            self.assertTrue(managed_gateway.consume_topic_icon_service_message(icon_msg, bot_token="MANAGER_TOKEN", bot_key="manager"))

    def test_handle_update_consumes_icon_service_message(self) -> None:
        icon_msg = {"message_id": 9, "chat": {"id": -1001}, "forum_topic_edited": {"icon_custom_emoji_id": "5"}}
        api = Mock(return_value={"ok": True, "result": True})
        handle_message = Mock()
        with patch.object(managed_gateway, "api", api), patch.object(managed_gateway, "handle_message", handle_message):
            managed_gateway.handle_update({"update_id": 1, "message": icon_msg}, bot_token="MANAGER_TOKEN", bot_key="manager")
        self.assertEqual(api.call_args.args[0], "deleteMessage")
        handle_message.assert_not_called()

    def test_terminal_before_body_deterministic(self) -> None:
        class FakeFuture:
            def __init__(self) -> None:
                self.callbacks = []
                self.done = False
                self.error = None

            def add_done_callback(self, callback):
                self.callbacks.append(callback)
                if self.done:
                    callback(self)

            def result(self):
                if self.error:
                    raise self.error

            def finish(self, error=None):
                self.error = error
                self.done = True
                for callback in list(self.callbacks):
                    callback(self)

        class TailFirstExecutor:
            def __init__(self) -> None:
                self.tasks = []

            def submit(self, fn, *args, **kwargs):
                future = FakeFuture()
                self.tasks.append((fn, args, kwargs, future))
                if len(self.tasks) == 2:
                    second = self.tasks.pop()
                    first = self.tasks.pop()
                    self.run_task(second)
                    self.run_task(first)
                return future

            def run_task(self, task):
                fn, args, kwargs, future = task
                error = None
                try:
                    fn(*args, **kwargs)
                except Exception as exc:
                    error = exc
                future.finish(error)

            def flush_remaining(self):
                while self.tasks:
                    self.run_task(self.tasks.pop(0))

        state = self.single_pane_space_state()
        executor = TailFirstExecutor()
        run_script = Mock(return_value={"handled": True, "reply": ""})
        body = "a" * 4041
        tail = "b" * 128
        updates = [
            self.poll_update(100, self.reassembly_message(6000, body)),
            self.poll_update(101, self.reassembly_message(6001, tail)),
        ]
        managed_gateway.offset_path_for("manager").write_text("100\n", encoding="utf-8")

        with patch.object(managed_gateway, "clear_webhook"), patch.object(
            managed_gateway, "api", Mock(return_value={"ok": True, "result": updates})
        ), patch.object(managed_gateway, "load_state", Mock(return_value=state)), patch.object(
            managed_gateway, "get_dispatch_executor", Mock(return_value=executor)
        ), patch.object(managed_gateway, "run_script", run_script):
            managed_gateway.poll_once("manager", "TOKEN", timeout_seconds=0)
            executor.flush_remaining()

        run_script.assert_called_once()
        self.assertEqual(run_script.call_args.args[0]["text"], body + tail)
        self.assertEqual(managed_gateway.offset_path_for("manager").read_text(encoding="utf-8").strip(), "102")
        self.assertEqual(self.read_reassembly_buffers(), {})

        managed_gateway.PROCESSED_MESSAGE_KEYS = None
        managed_gateway.PROCESSED_MESSAGE_ORDER = []
        if managed_gateway.PROCESSED_PATH.exists():
            managed_gateway.PROCESSED_PATH.unlink()
        with patch.object(managed_gateway, "load_state", Mock(return_value=state)):
            first_ready = managed_gateway.prepare_message(
                self.reassembly_message(6100, body), bot_token="MANAGER_TOKEN", bot_key="manager"
            )
            second_ready = managed_gateway.prepare_message(
                self.reassembly_message(6101, tail), bot_token="MANAGER_TOKEN", bot_key="manager"
            )
        self.assertIsNone(first_ready)
        self.assertIsNotNone(second_ready)
        self.assertEqual(second_ready.dispatch_texts, [body + tail])

    def test_normal_short_message_zero_delay_and_pooled(self) -> None:
        class QueuedExecutor:
            def __init__(self) -> None:
                self.calls = []

            def submit(self, fn, *args, **kwargs):
                future = Mock()
                future.add_done_callback = Mock()
                self.calls.append((fn, args, kwargs, future))
                return future

        state = self.single_pane_space_state()
        run_script = Mock(return_value={"handled": True, "reply": ""})
        executor = QueuedExecutor()
        text = "short prompt"

        with patch.object(managed_gateway, "load_state", Mock(return_value=state)), patch.object(
            managed_gateway, "run_script", run_script
        ), patch.object(managed_gateway, "get_dispatch_executor", Mock(return_value=executor)):
            ready = managed_gateway.prepare_message(self.reassembly_message(6200, text), bot_token="MANAGER_TOKEN", bot_key="manager")
            self.assertIsNotNone(ready)
            self.assertEqual(ready.dispatch_texts, [text])
            self.assertFalse(managed_gateway.REASSEMBLY_PATH.exists())
            managed_gateway.dispatch_ready(ready)

        self.assertEqual(len(executor.calls), 1)
        run_script.assert_not_called()

    def test_duplicate_fragment_is_noop(self) -> None:
        key = "-1001|77|42"
        sample = {"chat_id": "-1001", "thread_id": "77", "pane_key": "pane-1", "user_id": "42"}
        body = "a" * 4041
        tail = "b" * 128
        now = time.time()

        self.assertEqual(
            managed_gateway.buffer_or_assemble(key, 6300, body, is_command=False, sample=sample, now=now),
            ([], True),
        )
        self.assertEqual(
            managed_gateway.buffer_or_assemble(key, 6300, body, is_command=False, sample=sample, now=now + 1),
            ([], True),
        )
        buffers = self.read_reassembly_buffers()
        self.assertEqual(len(buffers[key]["fragments"]), 1)
        dispatch, hold = managed_gateway.buffer_or_assemble(
            key, 6301, tail, is_command=False, sample=sample, now=now + 2
        )
        self.assertFalse(hold)
        self.assertEqual(dispatch, [body + tail])

    def test_duplicate_tail_after_completion(self) -> None:
        state = self.single_pane_space_state()
        run_script = Mock(return_value={"handled": True, "reply": ""})
        body = "a" * 4041
        tail = "b" * 128
        updates = [
            self.poll_update(100, self.reassembly_message(6400, body)),
            self.poll_update(101, self.reassembly_message(6401, tail)),
        ]
        duplicate_tail = [self.poll_update(102, self.reassembly_message(6401, tail))]
        managed_gateway.offset_path_for("manager").write_text("100\n", encoding="utf-8")

        with patch.object(managed_gateway, "clear_webhook"), patch.object(
            managed_gateway, "api", Mock(return_value={"ok": True, "result": updates})
        ), patch.object(managed_gateway, "load_state", Mock(return_value=state)), patch.object(
            managed_gateway, "dispatch_ready", lambda ready: managed_gateway.execute_dispatch(ready)
        ), patch.object(managed_gateway, "run_script", run_script):
            managed_gateway.poll_once("manager", "TOKEN", timeout_seconds=0)
        with patch.object(managed_gateway, "clear_webhook"), patch.object(
            managed_gateway, "api", Mock(return_value={"ok": True, "result": duplicate_tail})
        ), patch.object(managed_gateway, "load_state", Mock(return_value=state)), patch.object(
            managed_gateway, "dispatch_ready", lambda ready: managed_gateway.execute_dispatch(ready)
        ), patch.object(managed_gateway, "run_script", run_script):
            managed_gateway.poll_once("manager", "TOKEN", timeout_seconds=0)

        run_script.assert_called_once()
        self.assertEqual(run_script.call_args.args[0]["text"], body + tail)

    def test_break_chain_emits_single_deduped_update(self) -> None:
        state = self.single_pane_space_state()
        key = "-1001|77|42"
        sample = {"chat_id": "-1001", "thread_id": "77", "pane_key": "pane-1", "user_id": "42"}
        first = "a" * 4041
        breaker = "/send now"
        managed_gateway.buffer_or_assemble(key, 6500, first, is_command=False, sample=sample, now=time.time())
        run_script = Mock(return_value={"handled": True, "reply": ""})

        with patch.object(managed_gateway, "load_state", Mock(return_value=state)), patch.object(
            managed_gateway, "reserve_message_processing", Mock(return_value=True)
        ) as reserve, patch.object(managed_gateway, "run_script", run_script):
            ready = managed_gateway.prepare_message(
                self.reassembly_message(6502, breaker), bot_token="MANAGER_TOKEN", bot_key="manager"
            )
            self.assertIsNotNone(ready)
            self.assertEqual(ready.dispatch_texts, [first, breaker])
            reserve.assert_called_once()
            managed_gateway.execute_dispatch(ready)

        self.assertEqual(run_script.call_count, 2)
        self.assertEqual([call.args[0]["text"] for call in run_script.call_args_list], [first, breaker])

    def test_held_fragment_persists_before_offset(self) -> None:
        state = self.single_pane_space_state()
        body = "a" * 4041
        updates = [self.poll_update(100, self.reassembly_message(6600, body))]
        managed_gateway.offset_path_for("manager").write_text("100\n", encoding="utf-8")
        order = []

        def write_offset(offset, key="manager"):
            order.append("offset")
            self.assertTrue(managed_gateway.REASSEMBLY_PATH.exists())
            buffers = self.read_reassembly_buffers()
            self.assertIn("-1001|77|42", buffers)

        with patch.object(managed_gateway, "clear_webhook"), patch.object(
            managed_gateway, "api", Mock(return_value={"ok": True, "result": updates})
        ), patch.object(managed_gateway, "load_state", Mock(return_value=state)), patch.object(
            managed_gateway, "dispatch_ready", Mock()
        ) as dispatch_ready, patch.object(managed_gateway, "write_offset", write_offset):
            managed_gateway.poll_once("manager", "TOKEN", timeout_seconds=0)

        dispatch_ready.assert_not_called()
        self.assertEqual(order, ["offset"])

    def test_crash_replay_held_fragment_no_dup(self) -> None:
        state = self.single_pane_space_state()
        body = "a" * 4041
        tail = "b" * 128
        first_update = [self.poll_update(100, self.reassembly_message(6700, body))]
        replay_update = [self.poll_update(100, self.reassembly_message(6700, body))]
        tail_update = [self.poll_update(101, self.reassembly_message(6701, tail))]
        run_script = Mock(return_value={"handled": True, "reply": ""})
        managed_gateway.offset_path_for("manager").write_text("100\n", encoding="utf-8")

        with patch.object(managed_gateway, "clear_webhook"), patch.object(
            managed_gateway, "api", Mock(return_value={"ok": True, "result": first_update})
        ), patch.object(managed_gateway, "load_state", Mock(return_value=state)), patch.object(
            managed_gateway, "write_offset", Mock()
        ), patch.object(managed_gateway, "dispatch_ready", Mock()):
            managed_gateway.poll_once("manager", "TOKEN", timeout_seconds=0)
        managed_gateway.PROCESSED_MESSAGE_KEYS = None
        managed_gateway.PROCESSED_MESSAGE_ORDER = []
        if managed_gateway.PROCESSED_PATH.exists():
            managed_gateway.PROCESSED_PATH.unlink()

        with patch.object(managed_gateway, "clear_webhook"), patch.object(
            managed_gateway, "api", Mock(return_value={"ok": True, "result": replay_update})
        ), patch.object(managed_gateway, "load_state", Mock(return_value=state)), patch.object(
            managed_gateway, "write_offset", Mock()
        ), patch.object(managed_gateway, "dispatch_ready", Mock()):
            managed_gateway.poll_once("manager", "TOKEN", timeout_seconds=0)
        buffers = self.read_reassembly_buffers()
        self.assertEqual(len(buffers["-1001|77|42"]["fragments"]), 1)

        managed_gateway.offset_path_for("manager").write_text("101\n", encoding="utf-8")
        with patch.object(managed_gateway, "clear_webhook"), patch.object(
            managed_gateway, "api", Mock(return_value={"ok": True, "result": tail_update})
        ), patch.object(managed_gateway, "load_state", Mock(return_value=state)), patch.object(
            managed_gateway, "dispatch_ready", lambda ready: managed_gateway.execute_dispatch(ready)
        ), patch.object(managed_gateway, "run_script", run_script):
            managed_gateway.poll_once("manager", "TOKEN", timeout_seconds=0)

        run_script.assert_called_once()
        self.assertEqual(run_script.call_args.args[0]["text"], body + tail)

    def test_attachment_skips_buffering(self) -> None:
        state = self.single_pane_space_state()
        message = self.reassembly_message(6800, "caption text")
        message["document"] = {"file_id": "file-1", "file_name": "report.txt", "mime_type": "text/plain", "file_size": 12}

        with patch.object(managed_gateway, "load_state", Mock(return_value=state)), patch.object(
            managed_gateway, "buffer_or_assemble", Mock(side_effect=AssertionError("buffered attachment"))
        ):
            ready = managed_gateway.prepare_message(message, bot_token="MANAGER_TOKEN", bot_key="manager")

        self.assertIsNotNone(ready)
        self.assertEqual(ready.dispatch_texts, ["caption text"])
        self.assertEqual(ready.attachment["kind"], "document")
        self.assertFalse(managed_gateway.REASSEMBLY_PATH.exists())

    def test_callback_and_managed_bot_bypass_prepare(self) -> None:
        updates = [
            {"update_id": 100, "callback_query": {"id": "cb-1", "data": "herdr:x"}},
            {"update_id": 101, "managed_bot": {"id": 1}},
            {"update_id": 102, "message": {"managed_bot_created": {"bot": {"id": 2}}}},
        ]
        managed_gateway.offset_path_for("manager").write_text("100\n", encoding="utf-8")

        with patch.object(managed_gateway, "clear_webhook"), patch.object(
            managed_gateway, "api", Mock(return_value={"ok": True, "result": updates})
        ), patch.object(managed_gateway, "prepare_message", Mock()) as prepare, patch.object(
            managed_gateway, "dispatch_update", Mock()
        ) as dispatch_update:
            managed_gateway.poll_once("manager", "TOKEN", timeout_seconds=0)

        prepare.assert_not_called()
        self.assertEqual(dispatch_update.call_count, 3)

    def test_non_owner_fragment_does_not_open_chain(self) -> None:
        state = self.single_pane_space_state()
        message = self.reassembly_message(6900, "a" * 4041)
        message["from"] = {"id": 99, "is_bot": False}

        with patch.object(managed_gateway, "load_state", Mock(return_value=state)):
            ready = managed_gateway.prepare_message(message, bot_token="MANAGER_TOKEN", bot_key="manager")

        self.assertIsNone(ready)
        self.assertFalse(managed_gateway.REASSEMBLY_PATH.exists())

    def test_unmapped_chat_fragment_not_buffered(self) -> None:
        state = self.single_pane_space_state()
        message = self.reassembly_message(7000, "a" * 4041)
        message["chat"] = {"id": -2002, "is_forum": True}
        message["message_thread_id"] = 999

        with patch.object(managed_gateway, "load_state", Mock(return_value=state)):
            ready = managed_gateway.prepare_message(message, bot_token="MANAGER_TOKEN", bot_key="manager")

        self.assertIsNone(ready)
        self.assertFalse(managed_gateway.REASSEMBLY_PATH.exists())

    def test_sweep_unchanged_recovers_orphan(self) -> None:
        key = "-1001|77|42"
        sample = {"chat_id": "-1001", "thread_id": "77", "pane_key": "pane-1", "user_id": "42"}
        body = "a" * 4041
        now = time.time()

        managed_gateway.buffer_or_assemble(key, 7100, body, is_command=False, sample=sample, now=now)
        flushed = managed_gateway.sweep_stale_reassembly(now + managed_gateway.REASSEMBLY_WINDOW_SECONDS + 1)

        self.assertEqual(flushed, [(key, body, sample)])
        self.assertEqual(self.read_reassembly_buffers(), {})

    def test_inline_overflow_runs_execute_on_caller(self) -> None:
        semaphore = threading.BoundedSemaphore(1)
        self.assertTrue(semaphore.acquire(blocking=False))
        ready = managed_gateway.ReadyDispatch(bot_key="manager")

        with patch.object(managed_gateway, "DISPATCH_QUEUE_SEMAPHORE", semaphore), patch.object(
            managed_gateway, "execute_dispatch_guarded", Mock()
        ) as execute_dispatch_guarded, patch.object(managed_gateway, "get_dispatch_executor", Mock()) as get_executor:
            managed_gateway.dispatch_ready(ready)

        execute_dispatch_guarded.assert_called_once_with(ready)
        get_executor.assert_not_called()

    def test_409_conflict_backs_off_and_retains_worker(self) -> None:
        managed_gateway.offset_path_for("managed-codex-a").write_text("10\n", encoding="utf-8")
        exc = urllib.error.HTTPError("url", 409, "Conflict", hdrs=None, fp=None)
        with patch.object(managed_gateway, "clear_webhook"), patch.object(
            managed_gateway, "api", Mock(side_effect=exc)
        ), patch.object(managed_gateway.time, "sleep") as sleep:
            managed_gateway.poll_once("managed-codex-a", "TOKEN", timeout_seconds=0)

        sleep.assert_called_once_with(managed_gateway.ERROR_BACKOFF)
        self.assertNotIn("managed-codex-a", managed_gateway.QUARANTINED_KEYS)

    def test_child_401_404_quarantines_worker_no_restart(self) -> None:
        managed_gateway.offset_path_for("managed-codex-a").write_text("10\n", encoding="utf-8")
        exc = urllib.error.HTTPError("url", 401, "Unauthorized", hdrs=None, fp=None)
        stop_event = threading.Event()

        with patch.object(managed_gateway, "clear_webhook"), patch.object(managed_gateway, "api", Mock(side_effect=exc)):
            managed_gateway.poll_worker("managed-codex-a", "TOKEN", 0, stop_event)

        self.assertIn("managed-codex-a", managed_gateway.QUARANTINED_KEYS)
        started: list[str] = []

        class FakeThread:
            def __init__(self, *, target, args, name, daemon):
                self.name = name

            def start(self):
                started.append(self.name)

        with patch.object(managed_gateway.threading, "Thread", FakeThread):
            managed_gateway.reconcile_poll_workers({}, [("managed-codex-a", "TOKEN", 0)])
            managed_gateway.reconcile_poll_workers({}, [("managed-codex-b", "ROTATED", 0)])

        self.assertNotIn("herdres-gateway-managed-codex-a", started)
        self.assertIn("herdres-gateway-managed-codex-b", started)

    def test_supervisor_survives_malformed_state_tick(self) -> None:
        valid = {"version": 1, "enabled": True, "telegram": {}}
        reconciles = []

        def reconcile(workers, specs):
            reconciles.append(specs)

        with patch.object(managed_gateway, "_token", Mock(return_value="MANAGER_TOKEN")), patch.object(
            managed_gateway, "api", Mock(return_value={"ok": True, "result": {"url": ""}})
        ), patch.object(managed_gateway, "load_state", Mock(side_effect=[valid, RuntimeError("bad state")])), patch.object(
            managed_gateway, "reconcile_poll_workers", reconcile
        ), patch.object(managed_gateway.time, "sleep", Mock(side_effect=[None, KeyboardInterrupt])):
            with self.assertRaises(KeyboardInterrupt):
                managed_gateway.main()

        self.assertEqual(len(reconciles), 1)

    def test_supervisor_keeps_workers_on_none_state_tick(self) -> None:
        # load_state() returns None (not raises) on a transient/partial read; that
        # tick must be skipped, NOT reconciled to an empty token set (which would
        # tear down every child poller). Exercises the real None path.
        valid = {"version": 1, "enabled": True, "telegram": {}}
        reconciles = []

        def reconcile(workers, specs):
            reconciles.append(specs)

        with patch.object(managed_gateway, "_token", Mock(return_value="MANAGER_TOKEN")), patch.object(
            managed_gateway, "api", Mock(return_value={"ok": True, "result": {"url": ""}})
        ), patch.object(managed_gateway, "load_state", Mock(side_effect=[valid, None])), patch.object(
            managed_gateway, "reconcile_poll_workers", reconcile
        ), patch.object(managed_gateway.time, "sleep", Mock(side_effect=[None, KeyboardInterrupt])):
            with self.assertRaises(KeyboardInterrupt):
                managed_gateway.main()

        self.assertEqual(len(reconciles), 1)  # the None tick must NOT reconcile

    def test_child_first_poll_drains_and_clears_webhook(self) -> None:
        calls = []

        def api(method, params=None, timeout=30, *, token=None):
            calls.append(method)
            if method == "getUpdates":
                return {"ok": True, "result": [{"update_id": 5}]}
            return {"ok": True, "result": True}

        with patch.object(managed_gateway, "api", api):
            managed_gateway.poll_once("managed-codex-a", "TOKEN", timeout_seconds=0)

        self.assertEqual(calls[:2], ["deleteWebhook", "getUpdates"])
        self.assertEqual(managed_gateway.offset_path_for("managed-codex-a").read_text(encoding="utf-8").strip(), "6")

    def test_runner_mode_subprocess_pinned_when_env_set(self) -> None:
        with patch.dict(os.environ, {"HERDRES_GATEWAY_RUNNER": "subprocess"}, clear=False):
            self.assertEqual(managed_gateway.gateway_runner_mode(), "subprocess")

    def test_poll_once_queues_update_handlers_without_waiting_for_commands(self) -> None:
        class QueuedExecutor:
            def __init__(self) -> None:
                self.calls = []

            def submit(self, fn, *args, **kwargs):
                future = Mock()
                future.add_done_callback = Mock()
                self.calls.append((fn, args, kwargs, future))
                return future

        executor = QueuedExecutor()
        managed_gateway.offset_path_for("manager").write_text("100\n", encoding="utf-8")
        updates = [
            {"update_id": 100, "message": dict_message(message_id=5100)},
            {"update_id": 101, "message": dict_message(message_id=5101)},
        ]

        with patch.object(managed_gateway, "clear_webhook"), patch.object(
            managed_gateway, "api", Mock(return_value={"ok": True, "result": updates})
        ), patch.object(managed_gateway, "get_dispatch_executor", Mock(return_value=executor)), patch.object(
            managed_gateway, "load_state", Mock(return_value=self.single_pane_space_state())
        ), patch.object(
            managed_gateway, "handle_update", Mock(side_effect=AssertionError("handler ran inline"))
        ):
            managed_gateway.poll_once("manager", "TOKEN", timeout_seconds=50)

        self.assertEqual(len(executor.calls), 2)
        self.assertEqual(managed_gateway.offset_path_for("manager").read_text(encoding="utf-8").strip(), "102")

    def test_same_pane_commands_are_serialized_under_async_dispatch(self) -> None:
        state = {
            "version": 1,
            "enabled": True,
            "telegram": {
                "chat_id": "-1001",
                "general_thread_id": "1",
                "owner_user_ids": ["42"],
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
                    "last_known_status": "working",
                    "agent": "codex",
                },
            },
        }
        active = 0
        max_active = 0
        run_count = 0
        counter_lock = threading.Lock()

        def slow_run_script(_payload, _mode):
            nonlocal active, max_active, run_count
            with counter_lock:
                active += 1
                max_active = max(max_active, active)
                run_count += 1
            time.sleep(0.03)
            with counter_lock:
                active -= 1
            return {"handled": True, "reply": ""}

        def send_message(message_id: int) -> None:
            managed_gateway.handle_message(
                {
                    "message_id": message_id,
                    "message_thread_id": 77,
                    "chat": {"id": -1001, "is_forum": True},
                    "from": {"id": 42, "is_bot": False},
                    "text": f"message {message_id}",
                },
                bot_token="MANAGER_TOKEN",
                bot_key="manager",
            )

        with patch.object(managed_gateway, "load_state", Mock(return_value=state)), patch.object(
            managed_gateway, "run_script", slow_run_script
        ), patch.object(managed_gateway, "api", Mock()):
            threads = [
                threading.Thread(target=send_message, args=(5200,)),
                threading.Thread(target=send_message, args=(5201,)),
            ]
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join()

        self.assertEqual(run_count, 2)
        self.assertEqual(max_active, 1)

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

    def test_handle_message_dispatches_new_command_in_shared_topic_without_reply(self) -> None:
        state = {
            "version": 1,
            "enabled": True,
            "telegram": {
                "chat_id": "-1001",
                "general_thread_id": "1",
                "owner_user_ids": ["42"],
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
                    "text": "/new claude",
                },
                bot_token="MANAGER_TOKEN",
            )

        api.assert_not_called()
        run_script.assert_called_once()
        payload = run_script.call_args.args[0]
        self.assertEqual(payload["pane_key"], "")
        self.assertEqual(payload["text"], "/new claude")

    def test_handle_message_dispatches_agents_in_multi_pane_topic_without_inline_ambiguous_reply(self) -> None:
        run_script = Mock(return_value={"handled": True, "reply": ""})
        api = Mock()

        with patch.object(managed_gateway, "load_state", Mock(return_value=managed_multi_pane_space_state())), patch.object(
            managed_gateway, "run_script", run_script
        ), patch.object(managed_gateway, "api", api):
            managed_gateway.handle_message(
                {
                    "message_id": 8101,
                    "message_thread_id": 77,
                    "chat": {"id": -1001, "is_forum": True},
                    "from": {"id": 42, "is_bot": False},
                    "text": "/agents",
                },
                bot_token="MANAGER_TOKEN",
            )

        api.assert_not_called()
        run_script.assert_called_once()
        payload, mode = run_script.call_args.args
        self.assertEqual(mode, "command")
        self.assertEqual(payload["pane_key"], "")
        self.assertEqual(payload["text"], "/agents")

    def test_handle_message_dispatches_plain_text_in_multi_pane_topic_without_inline_ambiguous_reply(self) -> None:
        run_script = Mock(return_value={"handled": True, "reply": ""})
        api = Mock()

        with patch.object(managed_gateway, "load_state", Mock(return_value=managed_multi_pane_space_state())), patch.object(
            managed_gateway, "run_script", run_script
        ), patch.object(managed_gateway, "api", api):
            managed_gateway.handle_message(
                {
                    "message_id": 8102,
                    "message_thread_id": 77,
                    "chat": {"id": -1001, "is_forum": True},
                    "from": {"id": 42, "is_bot": False},
                    "text": "hello",
                },
                bot_token="MANAGER_TOKEN",
            )

        api.assert_not_called()
        run_script.assert_called_once()
        payload, mode = run_script.call_args.args
        self.assertEqual(mode, "command")
        self.assertEqual(payload["pane_key"], "")
        self.assertEqual(payload["text"], "hello")

    def test_handle_callback_dispatches_space_callbacks_in_multi_pane_topic_without_route(self) -> None:
        run_script = Mock(return_value={"handled": True, "answer": "ok"})
        answer_callback_query = Mock()

        with patch.object(managed_gateway, "load_state", Mock(return_value=managed_multi_pane_space_state())), patch.object(
            managed_gateway, "run_script", run_script
        ), patch.object(managed_gateway, "answer_callback_query", answer_callback_query):
            for idx, data in enumerate(["herdr:ob:space-token:codex", "herdr:ag:space-token:pane-token"], start=1):
                managed_gateway.handle_callback(
                    {
                        "id": f"cb-{idx}",
                        "from": {"id": 42},
                        "data": data,
                        "message": {
                            "message_id": 8200 + idx,
                            "message_thread_id": 77,
                            "chat": {"id": -1001, "is_forum": True},
                        },
                    },
                    bot_token="MANAGER_TOKEN",
                )

        self.assertEqual(run_script.call_count, 2)
        self.assertEqual(answer_callback_query.call_count, 2)
        for call, data in zip(run_script.call_args_list, ["herdr:ob:space-token:codex", "herdr:ag:space-token:pane-token"]):
            payload, mode = call.args
            self.assertEqual(mode, "callback")
            self.assertEqual(payload["pane_key"], "")
            self.assertEqual(payload["data"], data)

    def test_child_bot_ignores_manager_owned_new_command(self) -> None:
        state = {
            "version": 1,
            "enabled": True,
            "telegram": {
                "chat_id": "-1001",
                "general_thread_id": "1",
                "owner_user_ids": ["42"],
                "managed_bots": {
                    "claude": {"username": "herdr_claude_bot", "token": "CLAUDE_TOKEN", "enabled": True}
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
                    "text": "/new claude",
                },
                bot_token="CLAUDE_TOKEN",
                bot_key="managed-claude-deadbeef",
            )

        run_script.assert_not_called()
        api.assert_not_called()

    def test_single_pane_plain_message_is_owned_by_manager_once(self) -> None:
        state = {
            "version": 1,
            "enabled": True,
            "telegram": {
                "chat_id": "-1001",
                "general_thread_id": "1",
                "owner_user_ids": ["42"],
                "managed_bots": {
                    "claude": {"username": "herdr_claude_bot", "token": "CLAUDE_TOKEN", "enabled": True}
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
                    "last_known_status": "working",
                    "agent": "claude",
                }
            },
        }
        manager_run_script = Mock(return_value={"handled": True, "reply": ""})
        child_run_script = Mock(return_value={"handled": True, "reply": ""})

        with patch.object(managed_gateway, "load_state", Mock(return_value=state)), patch.object(
            managed_gateway, "run_script", manager_run_script
        ), patch.object(managed_gateway, "api", Mock()):
            managed_gateway.handle_message(
                {
                    "message_id": 4000,
                    "message_thread_id": 77,
                    "chat": {"id": -1001, "is_forum": True},
                    "from": {"id": 42, "is_bot": False},
                    "text": "run tests",
                },
                bot_token="MANAGER_TOKEN",
                bot_key="manager",
            )
        with patch.object(managed_gateway, "load_state", Mock(return_value=state)), patch.object(
            managed_gateway, "run_script", child_run_script
        ), patch.object(managed_gateway, "api", Mock()):
            managed_gateway.handle_message(
                {
                    "message_id": 4000,
                    "message_thread_id": 77,
                    "chat": {"id": -1001, "is_forum": True},
                    "from": {"id": 42, "is_bot": False},
                    "text": "run tests",
                },
                bot_token="CLAUDE_TOKEN",
                bot_key="managed-claude-deadbeef",
            )

        manager_run_script.assert_called_once()
        child_run_script.assert_not_called()
        payload = manager_run_script.call_args.args[0]
        self.assertNotIn("target_bot_kind", payload)
        self.assertEqual(payload["pane_key"], "pane-1")

    def test_devin_mention_sets_target_bot_kind(self) -> None:
        state = {
            "version": 1,
            "enabled": True,
            "telegram": {
                "chat_id": "-1001",
                "general_thread_id": "1",
                "owner_user_ids": ["42"],
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
                    "last_known_status": "working",
                    "agent": "devin",
                }
            },
        }
        run_script = Mock(return_value={"handled": True, "reply": ""})

        with patch.object(managed_gateway, "load_state", Mock(return_value=state)), patch.object(
            managed_gateway, "run_script", run_script
        ):
            managed_gateway.handle_message(
                {
                    "message_id": 4000,
                    "message_thread_id": 77,
                    "chat": {"id": -1001, "is_forum": True},
                    "from": {"id": 42, "is_bot": False},
                    "text": "@herdr_devin_bot run analysis",
                },
                bot_token="MANAGER_TOKEN",
            )

        run_script.assert_called_once()
        payload = run_script.call_args.args[0]
        self.assertEqual(payload["target_bot_kind"], "devin")
        self.assertEqual(payload["text"], "@herdr_devin_bot run analysis")

    def test_manager_is_fallback_for_targeted_child_bot_mention(self) -> None:
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
        manager_run_script = Mock(return_value={"handled": True, "reply": ""})
        child_run_script = Mock(return_value={"handled": True, "reply": ""})
        message = {
            "message_id": 4000,
            "message_thread_id": 77,
            "chat": {"id": -1001, "is_forum": True},
            "from": {"id": 42, "is_bot": False},
            "text": "@herdr_codex_bot how are you now",
        }

        with patch.object(managed_gateway, "load_state", Mock(return_value=state)), patch.object(
            managed_gateway, "run_script", manager_run_script
        ), patch.object(managed_gateway, "api", Mock()):
            managed_gateway.handle_message(message, bot_token="MANAGER_TOKEN", bot_key="manager")
        with patch.object(managed_gateway, "load_state", Mock(return_value=state)), patch.object(
            managed_gateway, "run_script", child_run_script
        ), patch.object(managed_gateway, "api", Mock()):
            managed_gateway.handle_message(message, bot_token="CODEX_TOKEN", bot_key="managed-codex-deadbeef")

        manager_run_script.assert_called_once()
        child_run_script.assert_not_called()
        payload = manager_run_script.call_args.args[0]
        self.assertEqual(payload["target_bot_kind"], "codex")
        self.assertEqual(payload["pane_key"], "")

    def test_manager_is_fallback_for_reply_to_configured_child_bot(self) -> None:
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
                    "reply_to_message": {
                        "message_id": 3999,
                        "from": {"id": 8873456652, "is_bot": True, "username": "herdr_codex_bot"},
                    },
                    "text": "okay create PoCs",
                },
                bot_token="MANAGER_TOKEN",
                bot_key="manager",
            )

        run_script.assert_called_once()
        payload = run_script.call_args.args[0]
        self.assertEqual(payload["target_bot_kind"], "codex")
        self.assertEqual(payload["pane_key"], "pane-1")
        api.assert_not_called()

    def test_child_bot_update_sets_target_bot_kind(self) -> None:
        state = {
            "version": 1,
            "enabled": True,
            "telegram": {
                "chat_id": "-1001",
                "general_thread_id": "1",
                "owner_user_ids": ["42"],
                "managed_bots": {
                    "claude": {"username": "herdr_claude_bot", "token": "CLAUDE_TOKEN", "enabled": True}
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

    def test_run_script_uses_embedded_herdres_without_subprocess(self) -> None:
        calls = []

        class FakeHerdres:
            @staticmethod
            def with_lock(fn, *, blocking=False):
                calls.append(("lock", blocking))
                return fn()

            @staticmethod
            def command_reply(payload):
                calls.append(("command", payload))
                return {"handled": True, "reply": "ok"}

        payload = {"topic_id": "77", "text": "hi"}

        with patch.object(managed_gateway, "load_herdres_module", Mock(return_value=FakeHerdres)), patch.object(
            managed_gateway.subprocess,
            "run",
            Mock(side_effect=AssertionError("subprocess should not be used")),
        ), patch.dict(os.environ, {"HERDRES_GATEWAY_RUNNER": "embedded"}, clear=False):
            result = managed_gateway.run_script(payload, "command")

        self.assertEqual(result, {"handled": True, "reply": "ok"})
        self.assertEqual(calls, [("lock", True), ("command", payload)])

    def test_embedded_runner_loads_extensionless_installed_script(self) -> None:
        script_path = Path(self.tmp.name) / "herdres"
        script_path.write_text(
            "def with_lock(fn, *, blocking=False):\n"
            "    return fn()\n"
            "def command_reply(payload):\n"
            "    return {'handled': True, 'reply': payload['text']}\n",
            encoding="utf-8",
        )
        old_module = managed_gateway.HERDRES_MODULE
        old_key = managed_gateway.HERDRES_MODULE_KEY
        self.addCleanup(setattr, managed_gateway, "HERDRES_MODULE", old_module)
        self.addCleanup(setattr, managed_gateway, "HERDRES_MODULE_KEY", old_key)

        with patch.object(managed_gateway, "SCRIPT_PATH", script_path):
            managed_gateway.HERDRES_MODULE = None
            managed_gateway.HERDRES_MODULE_KEY = None
            module = managed_gateway.load_herdres_module()

        self.assertEqual(module.command_reply({"text": "ok"}), {"handled": True, "reply": "ok"})

    def test_run_script_can_use_subprocess_runner_when_configured(self) -> None:
        proc = subprocess.CompletedProcess(
            ["herdres", "command"],
            0,
            stdout=json.dumps({"handled": True, "reply": "ok"}).encode("utf-8"),
            stderr=b"",
        )
        runner = Mock(return_value=proc)

        with patch.object(managed_gateway.subprocess, "run", runner), patch.dict(
            os.environ,
            {"HERDRES_GATEWAY_RUNNER": "subprocess"},
            clear=False,
        ):
            result = managed_gateway.run_script({"topic_id": "77"}, "command")

        self.assertEqual(result, {"handled": True, "reply": "ok"})
        runner.assert_called_once()

    def test_subprocess_runner_uses_command_timeout(self) -> None:
        # A command/callback subprocess must get a forgiving timeout so a legitimate
        # send_to_pane chain or rate-limit backoff isn't killed mid-flight (was 30s).
        self.assertGreaterEqual(managed_gateway.COMMAND_TIMEOUT, 60)
        proc = subprocess.CompletedProcess(["herdres", "command"], 0, stdout=b'{"handled": true}', stderr=b"")
        runner = Mock(return_value=proc)
        with patch.object(managed_gateway.subprocess, "run", runner):
            managed_gateway.run_subprocess_herdres({"topic_id": "77"}, "command", {})
        self.assertEqual(runner.call_args.kwargs["timeout"], managed_gateway.COMMAND_TIMEOUT)

    def test_command_returns_before_command_timeout_under_all_hang(self) -> None:
        state = {
            "version": 1,
            "telegram": {"chat_id": "-1001", "owner_user_ids": ["42"]},
            "spaces": {
                "workspace:workspace-1": {
                    "space_key": "workspace:workspace-1",
                    "topic_id": "77",
                    "pane_keys": ["pane-1"],
                    "message_routes": {"1001": "pane-1"},
                }
            },
            "panes": {
                "pane-1": {
                    "pane_key": "pane-1",
                    "pane_id": "pane-1",
                    "space_key": "workspace:workspace-1",
                    "topic_id": "77",
                    "pane_root_message_id": "1001",
                    "last_known_status": "idle",
                }
            },
        }
        now = 100.0

        def monotonic() -> float:
            return now

        def sleep(seconds: float) -> None:
            nonlocal now
            now += max(0.0, float(seconds))

        def run_cmd(args: list[str], *, timeout: int = 10, input_text: str | None = None):
            sleep(timeout)
            proc = Mock()
            proc.returncode = 0
            proc.stderr = ""
            proc.stdout = ""
            if args[1:3] == ["pane", "list"]:
                proc.stdout = json.dumps({
                    "result": {
                        "panes": [{
                            "pane_id": "pane-1",
                            "workspace_id": "workspace-1",
                            "agent": "codex",
                            "agent_status": "idle",
                        }]
                    }
                })
            elif args[1:3] == ["workspace", "list"]:
                proc.stdout = json.dumps({"result": {"workspaces": []}})
            elif args[1:3] == ["pane", "read"]:
                proc.stdout = "❯ staged input"
            return proc

        payload = {
            "chat_id": "-1001",
            "topic_id": "77",
            "message_id": "4000",
            "reply_to_message_id": "1001",
            "user_id": "42",
            "text": "deploy when ready",
        }

        with patch.object(managed_gateway, "load_herdres_module", Mock(return_value=herdres)), patch.object(
            herdres,
            "with_lock",
            lambda fn, *, blocking=False: fn(),
        ), patch.object(herdres.time, "monotonic", monotonic), patch.object(
            herdres.time, "sleep", sleep
        ), patch.multiple(
            herdres,
            load_dotenv=Mock(),
            load_state=Mock(return_value=state),
            save_state=Mock(),
            run_cmd=run_cmd,
        ):
            result = managed_gateway.run_embedded_herdres(payload, "command")

        self.assertTrue(result["handled"])
        self.assertIn("reply", result)
        self.assertLess(now - 100.0, managed_gateway.COMMAND_TIMEOUT)

    def test_same_message_id_dispatches_to_command_once(self) -> None:
        state = {
            "version": 1,
            "enabled": True,
            "telegram": {
                "chat_id": "-1001",
                "general_thread_id": "1",
                "owner_user_ids": ["42"],
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
                    "last_known_status": "working",
                    "agent": "codex",
                },
            },
        }
        run_script = Mock(return_value={"handled": True, "reply": ""})
        api = Mock()
        message = {
            "message_id": 4500,
            "message_thread_id": 77,
            "chat": {"id": -1001, "is_forum": True},
            "from": {"id": 42, "is_bot": False},
            "text": "run tests",
        }

        with patch.object(managed_gateway, "load_state", Mock(return_value=state)), patch.object(
            managed_gateway, "run_script", run_script
        ), patch.object(managed_gateway, "api", api):
            managed_gateway.handle_message(message)
            managed_gateway.handle_message(dict(message))

        run_script.assert_called_once()
        api.assert_not_called()


class DirectOriginCommandMarkerTests(unittest.TestCase):
    DIRECT_ORIGIN_FIELDS = (
        "direct_origin_at",
        "direct_origin_created_at",
        "direct_origin_origin",
        "direct_origin_pane_id",
        "direct_origin_pane_key",
        "direct_origin_user_text_hash",
        "direct_origin_text_hash",
        "direct_origin_after_turn_id",
        "direct_origin_turn_id",
        "direct_origin_bound_at",
        "direct_origin_consumed_turn_id",
        "direct_origin_consumed_user_text_hash",
        "direct_origin_consumed_hash",
        "direct_origin_consumed_at",
    )

    def _state(self, *, panes: int = 1) -> dict:
        state = {
            "version": 1,
            "enabled": True,
            "telegram": {
                "chat_id": "-1001",
                "general_thread_id": "1",
                "owner_user_ids": ["42"],
            },
            "spaces": {
                "workspace:workspace-1": {
                    "space_key": "workspace:workspace-1",
                    "topic_id": "77",
                    "pane_keys": ["pane-1"],
                    "message_routes": {"1001": "pane-1"},
                }
            },
            "panes": {
                "pane-1": {
                    "pane_key": "pane-1",
                    "pane_id": "pane-1",
                    "space_key": "workspace:workspace-1",
                    "topic_id": "77",
                    "pane_root_message_id": "1001",
                    "last_known_status": "working",
                    "agent": "claude",
                    "last_turn_id": "turn-before",
                }
            },
        }
        if panes > 1:
            state["spaces"]["workspace:workspace-1"]["pane_keys"].append("pane-2")
            state["spaces"]["workspace:workspace-1"]["message_routes"]["1002"] = "pane-2"
            state["panes"]["pane-2"] = {
                "pane_key": "pane-2",
                "pane_id": "pane-2",
                "space_key": "workspace:workspace-1",
                "topic_id": "77",
                "pane_root_message_id": "1002",
                "last_known_status": "working",
                "agent": "codex",
            }
        return state

    def _payload(self, **overrides) -> dict:
        payload = {
            "chat_id": "-1001",
            "topic_id": "77",
            "message_id": "5000",
            "reply_to_message_id": "1001",
            "user_id": "42",
            "from_bot": False,
            "forwarded": False,
            "edited": False,
            "text": "/send Run direct task.",
        }
        payload.update(overrides)
        return payload

    def _assert_no_direct_origin_marker(self, entry: dict) -> None:
        for key in self.DIRECT_ORIGIN_FIELDS:
            self.assertNotIn(key, entry)

    def _command_patches(
        self,
        state: dict,
        *,
        send_to_pane: Mock | None = None,
        pane_turn: Mock | None = None,
        save_state: Mock | None = None,
    ):
        return patch.multiple(
            herdres,
            load_dotenv=Mock(),
            load_state=Mock(return_value=state),
            save_state=save_state or Mock(),
            send_to_pane=send_to_pane or Mock(return_value=(True, "")),
            pane_turn=pane_turn or Mock(return_value={"available": True, "turn_id": "turn-before"}),
        )

    def test_command_reply_successful_direct_owner_send_sets_marker(self) -> None:
        state = self._state()
        entry = state["panes"]["pane-1"]
        entry.update({
            "direct_origin_turn_id": "old-turn",
            "direct_origin_bound_at": "2026-01-01T00:00:00+00:00",
            "direct_origin_consumed_turn_id": "old-turn",
            "direct_origin_consumed_user_text_hash": "old-user-hash",
            "direct_origin_consumed_hash": "old-hash",
            "direct_origin_consumed_at": "2026-01-01T00:00:01+00:00",
        })
        send_to_pane = Mock(return_value=(True, ""))
        save_state = Mock()
        pane_turn = Mock(return_value={"available": True, "turn_id": "turn-before"})

        with self._command_patches(
            state,
            send_to_pane=send_to_pane,
            pane_turn=pane_turn,
            save_state=save_state,
        ):
            result = herdres.command_reply(self._payload())

        self.assertTrue(result["handled"])
        self.assertEqual(result["reply"], "")
        send_to_pane.assert_called_once_with("pane-1", "Run direct task.")
        self.assertIn("direct_origin_at", entry)
        self.assertEqual(entry["direct_origin_created_at"], entry["direct_origin_at"])
        self.assertEqual(entry["direct_origin_origin"], "send")
        self.assertEqual(entry["direct_origin_pane_id"], "pane-1")
        self.assertEqual(entry["direct_origin_pane_key"], "pane-1")
        self.assertEqual(entry["direct_origin_user_text_hash"], herdres.stream_text_hash("Run direct task."))
        self.assertEqual(entry["direct_origin_text_hash"], herdres.stream_text_hash("Run direct task."))
        self.assertEqual(entry["direct_origin_after_turn_id"], "turn-before")
        self.assertNotIn("direct_origin_turn_id", entry)
        self.assertNotIn("direct_origin_bound_at", entry)
        self.assertNotIn("direct_origin_consumed_turn_id", entry)
        self.assertNotIn("direct_origin_consumed_user_text_hash", entry)
        self.assertNotIn("direct_origin_consumed_hash", entry)
        self.assertNotIn("direct_origin_consumed_at", entry)
        self.assertTrue(save_state.called)

    def test_command_reply_successful_plain_reply_sets_plain_origin(self) -> None:
        state = self._state()
        entry = state["panes"]["pane-1"]
        send_to_pane = Mock(return_value=(True, ""))

        with self._command_patches(state, send_to_pane=send_to_pane):
            result = herdres.command_reply(self._payload(text="Run direct task."))

        self.assertTrue(result["handled"])
        send_to_pane.assert_called_once_with("pane-1", "Run direct task.")
        self.assertEqual(entry["direct_origin_origin"], "plain")
        self.assertEqual(entry["direct_origin_user_text_hash"], herdres.stream_text_hash("Run direct task."))

    def test_command_reply_failed_direct_send_does_not_set_marker(self) -> None:
        state = self._state()
        entry = state["panes"]["pane-1"]
        send_to_pane = Mock(return_value=(False, "permission denied"))

        with self._command_patches(state, send_to_pane=send_to_pane):
            result = herdres.command_reply(self._payload())

        self.assertTrue(result["handled"])
        self.assertIn("Send failed", result["reply"])
        send_to_pane.assert_called_once_with("pane-1", "Run direct task.")
        self._assert_no_direct_origin_marker(entry)

    def test_command_reply_rejected_messages_do_not_set_direct_marker(self) -> None:
        cases = (
            ("forwarded", self._state(), self._payload(forwarded=True)),
            ("non_owner", self._state(), self._payload(user_id="99")),
            ("bot_origin", self._state(), self._payload(from_bot=True)),
            ("edited", self._state(), self._payload(edited=True)),
            (
                "ambiguous",
                self._state(panes=2),
                self._payload(reply_to_message_id="", text="/send Run direct task."),
            ),
        )
        for name, state, payload in cases:
            with self.subTest(name=name):
                send_to_pane = Mock(return_value=(True, ""))
                pane_turn = Mock(return_value={"available": True, "turn_id": "turn-before"})

                with self._command_patches(state, send_to_pane=send_to_pane, pane_turn=pane_turn):
                    result = herdres.command_reply(payload)

                self.assertTrue(result["handled"])
                send_to_pane.assert_not_called()
                for entry in state["panes"].values():
                    self._assert_no_direct_origin_marker(entry)


class TypingActionTests(unittest.TestCase):
    """Issue #44: the gateway sends a native "typing…" chat action to each actively-working pane's
    topic (refreshed ~4s) so Telegram renders its own animated dots — no message edits."""

    def _state(self, panes, managed_bots=None):
        return {"version": 1, "enabled": True, "panes": panes, "telegram": {
            "chat_id": "-1001", "general_thread_id": "1", "managed_bots": managed_bots or {},
        }}

    def test_selects_only_working_recent_panes_with_real_topics(self):
        st = self._state({
            "a": {"last_known_status": "working", "topic_id": "77"},
            "b": {"last_known_status": "idle", "topic_id": "78"},      # not working
            "c": {"last_known_status": "running", "topic_id": "79"},   # active synonym
            "d": {"last_known_status": "working", "topic_id": ""},     # no topic
            "e": {"last_known_status": "working", "topic_id": "1"},    # General topic — never targeted
        })
        topics = sorted(t for _c, t, _tok in managed_gateway.typing_panes(st))
        self.assertEqual(topics, ["77", "79"])

    def test_busy_and_hyphenated_statuses_are_active(self):
        st = self._state({
            "a": {"last_known_status": "busy", "topic_id": "77"},
            "b": {"last_known_status": "in-progress", "topic_id": "78"},  # hyphen normalized
        })
        self.assertEqual(sorted(t for _c, t, _tok in managed_gateway.typing_panes(st)), ["77", "78"])

    def test_skips_stale_status(self):
        # A frozen "working" status (e.g. the sync timer paused) must not animate forever.
        st = self._state({
            "fresh": {"last_known_status": "working", "topic_id": "77"},  # no last_seen_at -> treated fresh
            "stale": {"last_known_status": "working", "topic_id": "78", "last_seen_at": "2020-01-01T00:00:00Z"},
        })
        self.assertEqual(sorted(t for _c, t, _tok in managed_gateway.typing_panes(st)), ["77"])

    def test_general_topic_with_empty_id_in_state_still_excluded(self):
        # If general_thread_id is present-but-empty, fall back to the env default so the real General
        # ("1") is still excluded rather than animated.
        st = self._state({"g": {"last_known_status": "working", "topic_id": "1"}})
        st["telegram"]["general_thread_id"] = ""
        self.assertEqual(managed_gateway.typing_panes(st), [])

    def test_dedupes_per_topic(self):
        st = self._state({
            "a": {"last_known_status": "working", "topic_id": "77"},
            "b": {"last_known_status": "running", "topic_id": "77"},
        })
        self.assertEqual(len(managed_gateway.typing_panes(st)), 1)

    def test_no_chat_id_returns_empty(self):
        st = self._state({"a": {"last_known_status": "working", "topic_id": "77"}})
        st["telegram"]["chat_id"] = ""
        self.assertEqual(managed_gateway.typing_panes(st), [])

    def test_flag_default_off_and_toggle(self):
        env = {k: v for k, v in os.environ.items() if k != "HERDR_TELEGRAM_TOPICS_TYPING_ACTION"}
        with patch.dict(os.environ, env, clear=True):
            self.assertFalse(managed_gateway.typing_action_enabled())
        with patch.dict(os.environ, {"HERDR_TELEGRAM_TOPICS_TYPING_ACTION": "1"}):
            self.assertTrue(managed_gateway.typing_action_enabled())

    def test_token_resolution_prefers_voice_bot_then_any_managed(self):
        # The typing token MUST be an in-group bot (the manager/poll token is often not a chat
        # member -> 400 "chat not found"). Prefer the pane's own voice bot; else any managed bot.
        st = self._state(
            {"voiced": {"last_known_status": "working", "topic_id": "77",
                        "managed_voice_active": True, "pane_root_bot_kind": "codex"},
             "manager_voiced": {"last_known_status": "working", "topic_id": "78"}},  # not managed-voice
            managed_bots={"claude": {"enabled": True, "token": "CLAUDE_TOK"},
                          "codex": {"enabled": True, "token": "CODEX_TOK"}},
        )
        by_topic = {t: tok for _c, t, tok in managed_gateway.typing_panes(st)}
        self.assertEqual(by_topic["77"], "CODEX_TOK")   # the pane's own voice bot
        self.assertEqual(by_topic["78"], "CLAUDE_TOK")  # any in-group managed bot (sorted: claude first)

    def test_token_falls_back_to_manager_when_no_managed_bots(self):
        st = self._state({"a": {"last_known_status": "working", "topic_id": "77"}})  # no managed_bots
        self.assertEqual(managed_gateway.typing_panes(st), [("-1001", "77", None)])

    def test_tick_sends_typing_per_topic_with_clean_payload(self):
        st = self._state(
            {"a": {"last_known_status": "working", "topic_id": "77"},
             "b": {"last_known_status": "working", "topic_id": "79"}},
            managed_bots={"claude": {"enabled": True, "token": "CLAUDE_TOK"}},
        )
        calls = []

        def fake_api(method, params=None, timeout=30, *, token=None):
            calls.append((method, params, token))
            return {"ok": True}

        with patch.object(managed_gateway, "api", fake_api):
            sent, backoff = managed_gateway.typing_tick(st)
        self.assertEqual((sent, backoff), (2, 0.0))
        self.assertTrue(all(m == "sendChatAction" and p["action"] == "typing" for m, p, _t in calls))
        self.assertEqual(sorted(p["message_thread_id"] for _m, p, _t in calls), ["77", "79"])
        self.assertTrue(all(tok == "CLAUDE_TOK" for _m, _p, tok in calls))  # an in-group bot, not manager
        # sendChatAction rejects extra fields — never send notify/markup
        self.assertTrue(all("disable_notification" not in p and "reply_markup" not in p for _m, p, _t in calls))

    def test_tick_resilient_to_per_topic_errors(self):
        st = self._state({
            "a": {"last_known_status": "working", "topic_id": "77"},
            "b": {"last_known_status": "working", "topic_id": "79"},
        })
        sent = []

        def boom_then_ok(method, params=None, timeout=30, *, token=None):
            if params["message_thread_id"] == "77":
                raise RuntimeError("transient")
            sent.append(params["message_thread_id"])
            return {"ok": True}

        with patch.object(managed_gateway, "api", boom_then_ok):
            count, backoff = managed_gateway.typing_tick(st)
        self.assertEqual((count, backoff), (1, 0.0))  # the failing topic didn't count; no backoff
        self.assertEqual(sent, ["79"])                # but the second topic still got its action

    def test_tick_backs_off_on_429(self):
        st = self._state({
            "a": {"last_known_status": "working", "topic_id": "77"},
            "b": {"last_known_status": "working", "topic_id": "79"},
        })
        err = urllib.error.HTTPError("u", 429, "Too Many Requests", {"Retry-After": "12"}, None)
        with patch.object(managed_gateway, "api", Mock(side_effect=err)):
            sent, backoff = managed_gateway.typing_tick(st)
        self.assertEqual(sent, 0)
        self.assertEqual(backoff, 12.0)  # honors Retry-After (capped) so the loop waits, not hammers

    def test_refresh_loop_runs_tick_only_when_enabled(self):
        for flag, expect_called in (("1", True), ("0", False)):
            tick = Mock(return_value=(1, 0.0))
            env = {k: v for k, v in os.environ.items() if k != "HERDR_TELEGRAM_TOPICS_TYPING_ACTION"}
            env["HERDR_TELEGRAM_TOPICS_TYPING_ACTION"] = flag
            with patch.dict(os.environ, env, clear=True), \
                 patch.object(managed_gateway, "load_state", Mock(return_value={"version": 1, "enabled": True, "telegram": {}})), \
                 patch.object(managed_gateway, "typing_tick", tick), \
                 patch.object(managed_gateway.time, "sleep", Mock(side_effect=KeyboardInterrupt)):
                try:
                    managed_gateway.typing_refresh_loop()
                except KeyboardInterrupt:
                    pass
            self.assertEqual(tick.called, expect_called, f"flag={flag}")

    def test_main_starts_typing_thread_only_when_enabled(self):
        # The refresh thread is started only when the flag is on at boot (so its sleep never races
        # the supervisor in the flag-off path); enabling needs a gateway restart.
        class FakeThread:
            def __init__(self, *, target=None, name="", daemon=False, args=()):
                self.name = name

            def start(self):
                started.append(self.name)

        for flag, expect_started in (("1", True), ("0", False)):
            started: list[str] = []
            env = {k: v for k, v in os.environ.items() if k != "HERDR_TELEGRAM_TOPICS_TYPING_ACTION"}
            env["HERDR_TELEGRAM_TOPICS_TYPING_ACTION"] = flag
            with patch.dict(os.environ, env, clear=True), \
                 patch.object(managed_gateway, "_token", Mock(return_value="MANAGER")), \
                 patch.object(managed_gateway, "api", Mock(return_value={"ok": True, "result": {"url": ""}})), \
                 patch.object(managed_gateway, "load_state", Mock(return_value={"version": 1, "enabled": True, "telegram": {}})), \
                 patch.object(managed_gateway, "sweep_stale_reassembly", Mock(return_value=[])), \
                 patch.object(managed_gateway, "reconcile_poll_workers", Mock()), \
                 patch.object(managed_gateway.threading, "Thread", FakeThread), \
                 patch.object(managed_gateway.time, "sleep", Mock(side_effect=KeyboardInterrupt)):
                try:
                    managed_gateway.main()
                except KeyboardInterrupt:
                    pass
            self.assertEqual("herdres-gateway-typing" in started, expect_started, f"flag={flag}")


if __name__ == "__main__":
    unittest.main()
