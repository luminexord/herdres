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

import conftest  # noqa: F401
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


if __name__ == "__main__":
    unittest.main()
