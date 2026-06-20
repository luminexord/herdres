from __future__ import annotations

import copy
import hashlib
import inspect
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

import herdres
import herdres_gateway_managed as managed_gateway


def osov_state(*, panes: int = 2) -> dict:
    pane_map = {}
    pane_keys = []
    agents = ["codex", "claude", "devin"]
    for idx in range(panes):
        key = f"pane-{idx + 1}"
        pane_keys.append(key)
        pane_map[key] = {
            "pane_key": key,
            "pane_id": key,
            "agent": agents[idx],
            "space_key": "workspace:one",
            "topic_id": "77",
            "last_known_status": "working",
        }
    return {
        "version": 1,
        "telegram": {"chat_id": "-1001", "general_thread_id": "1", "owner_user_ids": ["42"]},
        "spaces": {
            "workspace:one": {
                "space_key": "workspace:one",
                "topic_id": "77",
                "pane_keys": pane_keys,
                "message_routes": {},
            }
        },
        "panes": pane_map,
    }


def command_patches(state: dict, *, send_message: Mock | None = None, send_to_pane: Mock | None = None):
    return patch.multiple(
        herdres,
        load_dotenv=Mock(),
        load_state=Mock(return_value=state),
        save_state=Mock(),
        send_message=send_message or Mock(return_value="9001"),
        send_to_pane=send_to_pane or Mock(return_value=(True, "")),
    )


def callback_patches(state: dict, *, api: Mock | None = None, send_to_pane: Mock | None = None):
    return patch.multiple(
        herdres,
        load_dotenv=Mock(),
        load_state=Mock(return_value=state),
        save_state=Mock(),
        telegram_api=api or Mock(return_value={"ok": True, "result": True}),
        send_to_pane=send_to_pane or Mock(return_value=(True, "")),
    )


def callback_payload(data: str, *, user_id: str = "42", topic_id: str = "77", message_id: str = "555") -> dict:
    return {
        "chat_id": "-1001",
        "topic_id": topic_id,
        "user_id": user_id,
        "message_id": message_id,
        "data": data,
    }


class OneSpaceOneVoiceStateTests(unittest.TestCase):
    def test_managed_bots_env_default_is_off(self) -> None:
        self.assertFalse(herdres.MANAGED_BOTS_ENABLED)

    def test_migrate_voice_mode_sets_per_agent_for_live_bot_space(self) -> None:
        state = osov_state(panes=1)
        state["telegram"]["managed_bots"] = {"codex": {"token": "TOK", "enabled": True}}
        state["spaces"]["workspace:one"].pop("voice_mode", None)

        self.assertTrue(herdres.migrate_space_voice_mode(state))
        self.assertEqual(state["spaces"]["workspace:one"]["voice_mode"], "per_agent")

    def test_migrate_voice_mode_leaves_single_bot_space_unset(self) -> None:
        state = osov_state(panes=1)
        state["spaces"]["workspace:one"].pop("voice_mode", None)

        self.assertFalse(herdres.migrate_space_voice_mode(state))
        self.assertNotIn("voice_mode", state["spaces"]["workspace:one"])
        self.assertEqual(herdres.space_voice_mode(state, state["panes"]["pane-1"]), "shared")

    def test_migrate_voice_mode_idempotent(self) -> None:
        state = osov_state(panes=1)
        state["telegram"]["managed_bots"] = {"codex": {"token": "TOK", "enabled": True}}
        state["spaces"]["workspace:one"].pop("voice_mode", None)
        herdres.normalize_state(state)
        before = hashlib.sha1(json.dumps(state["spaces"], sort_keys=True).encode("utf-8")).hexdigest()

        self.assertFalse(herdres.migrate_space_voice_mode(state))
        after = hashlib.sha1(json.dumps(state["spaces"], sort_keys=True).encode("utf-8")).hexdigest()
        self.assertEqual(before, after)

    def test_migrate_voice_mode_handles_malformed_dicts(self) -> None:
        state = {"telegram": {"managed_bots": []}, "spaces": {"bad": "value"}, "panes": []}

        self.assertFalse(herdres.migrate_space_voice_mode(state))

    def test_migrate_voice_mode_respects_disabled_record(self) -> None:
        state = osov_state(panes=1)
        state["telegram"]["managed_bots"] = {"codex": {"token": "TOK", "enabled": False}}
        state["spaces"]["workspace:one"].pop("voice_mode", None)

        self.assertFalse(herdres.migrate_space_voice_mode(state))
        self.assertNotIn("voice_mode", state["spaces"]["workspace:one"])

    def test_normalize_state_version_unchanged(self) -> None:
        state = osov_state(panes=1)
        state["telegram"]["managed_bots"] = {"codex": {"token": "TOK", "enabled": True}}
        state["spaces"]["workspace:one"].pop("voice_mode", None)

        self.assertEqual(herdres.normalize_state(state)["version"], 1)


class OneSpaceOneVoiceGatingTests(unittest.TestCase):
    def test_new_space_seeds_shared_voice_mode(self) -> None:
        state = {"spaces": {}}
        pane = {"workspace_id": "work", "tab_id": "tab", "pane_id": "pane", "label": "Pane"}

        _key, entry, changed = herdres.ensure_space_entry(state, pane)

        self.assertTrue(changed)
        self.assertEqual(entry["voice_mode"], "shared")

    def test_voice_mode_survives_grouping_flip(self) -> None:
        state = osov_state(panes=1)
        state["telegram"]["managed_bots"] = {"codex": {"token": "TOK", "enabled": True}}
        state["spaces"]["workspace:one"]["voice_mode"] = "per_agent"
        state["panes"]["pane-1"]["workspace_id"] = "one"

        with tempfile.TemporaryDirectory() as tmp:
            state_file = str(Path(tmp) / "state.json")
            with patch.dict("os.environ", {"HERDR_TELEGRAM_TOPICS_STATE": state_file}):
                herdres.reset_topic_grouping(state, "agent", reason="test")
                _key, entry, _changed = herdres.ensure_space_entry(state, state["panes"]["pane-1"])
                herdres.save_state(state)
                loaded = herdres.load_state()

        final_entry = loaded["spaces"]["workspace:one"]
        self.assertEqual(final_entry["voice_mode"], "per_agent")
        self.assertTrue(herdres.managed_voice_enabled_for_space(loaded, entry))

    def test_managed_token_none_when_entry_voice_inactive(self) -> None:
        telegram = {"managed_bots": {"codex": {"token": "TOK", "enabled": True}}}
        entry = {"agent": "codex", "managed_voice_active": False}

        self.assertIsNone(herdres.managed_bot_token_for_entry(telegram, entry))

    def test_managed_token_returned_when_voice_active(self) -> None:
        telegram = {"managed_bots": {"codex": {"token": "TOK", "enabled": True}}}
        entry = {"agent": "codex", "managed_voice_active": True}

        self.assertEqual(herdres.managed_bot_token_for_entry(telegram, entry), "TOK")

    def test_managed_token_env_fallback_when_unstamped(self) -> None:
        telegram = {"managed_bots": {"codex": {"token": "TOK", "enabled": True}}}
        entry = {"agent": "codex"}

        with patch.object(herdres, "MANAGED_BOTS_ENABLED", True):
            self.assertEqual(herdres.managed_bot_token_for_entry(telegram, entry), "TOK")
        with patch.object(herdres, "MANAGED_BOTS_ENABLED", False):
            self.assertIsNone(herdres.managed_bot_token_for_entry(telegram, entry))

    def test_refresh_entry_managed_voice_stamps_from_space(self) -> None:
        state = osov_state(panes=1)
        entry = state["panes"]["pane-1"]
        state["spaces"]["workspace:one"]["voice_mode"] = "per_agent"

        herdres.refresh_entry_managed_voice(state, entry)
        self.assertTrue(entry["managed_voice_active"])
        state["spaces"]["workspace:one"]["voice_mode"] = "shared"
        herdres.refresh_entry_managed_voice(state, entry)
        self.assertFalse(entry["managed_voice_active"])


class OneSpaceOneVoiceManagerCommandsTests(unittest.TestCase):
    def test_ensure_manager_commands_calls_setmycommands_once(self) -> None:
        calls = []

        def api(method: str, payload: dict) -> dict:
            calls.append((method, payload))
            return {"ok": True, "result": True}

        telegram = {}
        with patch.object(herdres, "telegram_api", api):
            herdres.ensure_manager_commands(telegram)

        self.assertEqual(calls[0][0], "setMyCommands")
        self.assertIsInstance(calls[0][1]["commands"], str)
        self.assertIn("manager_commands_digest", telegram)

    def test_ensure_manager_commands_idempotent(self) -> None:
        telegram = {}
        api = Mock(return_value={"ok": True, "result": True})

        with patch.object(herdres, "telegram_api", api):
            herdres.ensure_manager_commands(telegram)
            herdres.ensure_manager_commands(telegram)

        api.assert_called_once()

    def test_ensure_manager_commands_nonfatal_on_error(self) -> None:
        telegram = {}

        with patch.object(herdres, "telegram_api", Mock(side_effect=RuntimeError("boom"))):
            herdres.ensure_manager_commands(telegram)

        self.assertIn("boom", telegram["manager_commands_error"])
        self.assertNotIn("manager_commands_digest", telegram)

    def test_manager_commands_only_registers_real_handlers(self) -> None:
        registered = {command for command, _description in herdres.MANAGER_BOT_COMMANDS}
        source = inspect.getsource(herdres.command_reply)

        self.assertEqual(
            registered,
            {"status", "report", "choices", "raw", "send", "keys", "agents", "voice", "new", "debug", "help"},
        )
        for command in registered - {"agents"}:
            self.assertIn(command, source)


class OneSpaceOneVoiceOnboardingTests(unittest.TestCase):
    def test_onboarding_card_posted_on_fresh_topic(self) -> None:
        state = {"spaces": {}, "panes": {}}
        pane = {"workspace_id": "work", "pane_id": "p1", "agent": "codex", "label": "Pane"}
        send_message = Mock(return_value="901")

        with patch.object(herdres, "create_topic", Mock(return_value="77")), patch.object(
            herdres, "send_message", send_message
        ), patch.object(herdres, "save_state", Mock()):
            space, changed = herdres.ensure_space_topic(state, "-1001", {}, pane, {"creates": 0}, 5)

        self.assertTrue(changed)
        self.assertEqual(space["onboarding_selected"], ["codex"])
        self.assertEqual(space["onboarding_status"], "pending")
        self.assertEqual(space["onboarding_message_id"], "901")
        self.assertEqual(send_message.call_args.kwargs["thread_id"], "77")

    def test_onboarding_card_not_reposted_existing_topic(self) -> None:
        state = {"spaces": {}, "panes": {}}
        pane = {"workspace_id": "work", "pane_id": "p1", "agent": "codex"}
        _key, space, _changed = herdres.ensure_space_entry(state, pane)
        space["topic_id"] = "77"
        send_message = Mock()

        result, changed = herdres.ensure_space_topic(state, "-1001", {}, pane, {"creates": 0}, 5)

        self.assertFalse(changed)
        self.assertIs(result, space)
        send_message.assert_not_called()

    def test_onboarding_card_skipped_when_max_creates_hit(self) -> None:
        state = {"spaces": {}, "panes": {}}
        pane = {"workspace_id": "work", "pane_id": "p1", "agent": "codex"}
        send_message = Mock()

        with patch.object(herdres, "send_message", send_message):
            space, changed = herdres.ensure_space_topic(state, "-1001", {}, pane, {"creates": 5}, 5)

        self.assertTrue(changed)
        self.assertNotIn("onboarding_status", space)
        send_message.assert_not_called()

    def test_onboarding_toggle_flips_selection_and_edits_markup(self) -> None:
        state = osov_state()
        space = state["spaces"]["workspace:one"]
        space["onboarding_selected"] = ["codex"]
        token = herdres._callback_id("workspace:one", "space")[:16]
        api = Mock(return_value={"ok": True, "result": True})

        with callback_patches(state, api=api):
            result = herdres.callback_reply(callback_payload(f"herdr:ob:{token}:claude"))

        self.assertEqual(result["answer"], "Updated.")
        self.assertEqual(space["onboarding_selected"], ["codex", "claude"])
        self.assertEqual(api.call_args.args[0], "editMessageReplyMarkup")
        self.assertIsInstance(api.call_args.args[1]["reply_markup"], str)

    def test_onboarding_done_commits_and_edits_text(self) -> None:
        state = osov_state()
        space = state["spaces"]["workspace:one"]
        space["onboarding_selected"] = ["codex"]
        token = herdres._callback_id("workspace:one", "space")[:16]
        api = Mock(return_value={"ok": True, "result": True})

        with callback_patches(state, api=api):
            result = herdres.callback_reply(callback_payload(f"herdr:ob:{token}:_done"))

        self.assertEqual(result["answer"], "Agents set.")
        self.assertEqual(space["onboarding_status"], "committed")
        self.assertEqual(api.call_args.args[0], "editMessageText")

    def test_onboarding_callback_multi_pane_topic_not_rejected(self) -> None:
        state = osov_state()
        token = herdres._callback_id("workspace:one", "space")[:16]

        with callback_patches(state):
            result = herdres.callback_reply(callback_payload(f"herdr:ob:{token}:codex"))

        self.assertTrue(result["handled"])
        self.assertEqual(result["answer"], "Updated.")

    def test_onboarding_callback_nonowner_denied(self) -> None:
        state = osov_state()
        token = herdres._callback_id("workspace:one", "space")[:16]

        with callback_patches(state):
            result = herdres.callback_reply(callback_payload(f"herdr:ob:{token}:codex", user_id="99"))

        self.assertEqual(result["answer"], "Not authorized.")
        self.assertTrue(result["show_alert"])

    def test_onboarding_callback_stale_space(self) -> None:
        state = osov_state()
        token = herdres._callback_id("workspace:one", "space")[:16]

        with callback_patches(state):
            result = herdres.callback_reply(callback_payload(f"herdr:ob:{token}:codex", topic_id="88"))

        self.assertEqual(result["answer"], "This space is no longer active.")
        self.assertTrue(result["show_alert"])

    def test_onboarding_card_send_failure_keeps_selection(self) -> None:
        state = {"spaces": {}, "panes": {}}
        pane = {"workspace_id": "work", "pane_id": "p1", "agent": "codex"}

        with patch.object(herdres, "create_topic", Mock(return_value="77")), patch.object(
            herdres, "send_message", Mock(side_effect=RuntimeError("send failed"))
        ), patch.object(herdres, "save_state", Mock()):
            space, _changed = herdres.ensure_space_topic(state, "-1001", {}, pane, {"creates": 0}, 5)

        self.assertEqual(space["onboarding_selected"], ["codex"])
        self.assertIn("send failed", space["onboarding_error"])


class OneSpaceOneVoiceAgentsTests(unittest.TestCase):
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
        self.addCleanup(setattr, managed_gateway, "PROCESSED_MESSAGE_KEYS", None)
        self.addCleanup(setattr, managed_gateway, "PROCESSED_MESSAGE_ORDER", [])

    def test_agents_command_multi_pane_posts_picker(self) -> None:
        state = osov_state()
        send_message = Mock(return_value="901")

        with command_patches(state, send_message=send_message):
            result = herdres.command_reply({"chat_id": "-1001", "topic_id": "77", "user_id": "42", "text": "/agents"})

        self.assertEqual(result["reply"], "")
        markup = send_message.call_args.kwargs["reply_markup"]
        self.assertEqual(len(markup["inline_keyboard"]), 2)

    def test_plain_text_multi_pane_posts_picker(self) -> None:
        state = osov_state()
        send_message = Mock(return_value="901")

        with command_patches(state, send_message=send_message):
            result = herdres.command_reply({"chat_id": "-1001", "topic_id": "77", "user_id": "42", "text": "hello"})

        self.assertEqual(result["reply"], "")
        send_message.assert_called_once()

    def test_status_command_multi_pane_keeps_ambiguous(self) -> None:
        state = osov_state()

        with command_patches(state):
            result = herdres.command_reply({"chat_id": "-1001", "topic_id": "77", "user_id": "42", "text": "/status"})

        self.assertEqual(result["reply"], herdres.AMBIGUOUS_PANE_THREAD_REPLY)

    def test_agent_pick_sets_active_pane(self) -> None:
        state = osov_state()
        space = state["spaces"]["workspace:one"]
        space_token = herdres._callback_id("workspace:one", "space")[:16]
        pane_token = herdres._callback_id("pane-2", "pane")[:24]
        api = Mock(return_value={"ok": True, "result": True})

        with callback_patches(state, api=api):
            result = herdres.callback_reply(callback_payload(f"herdr:ag:{space_token}:{pane_token}"))

        self.assertEqual(space["active_pane"]["42"]["pane_key"], "pane-2")
        self.assertEqual(result["answer"], "Sending to Claude.")
        self.assertEqual(api.call_args.args[0], "editMessageText")

    def test_plain_pick_delivers_original_message(self) -> None:
        state = osov_state()
        space = state["spaces"]["workspace:one"]
        space_token = herdres._callback_id("workspace:one", "space")[:16]
        pane_token = herdres._callback_id("pane-1", "pane")[:24]
        send_message = Mock(return_value="901")
        api = Mock(return_value={"ok": True, "result": True})
        send_to_pane = Mock(return_value=(True, "Queued for pane."))

        with command_patches(state, send_message=send_message):
            result = herdres.command_reply({"chat_id": "-1001", "topic_id": "77", "user_id": "42", "text": "hello"})

        self.assertEqual(result["reply"], "")
        self.assertEqual(space["pending_pick"]["42"]["text"], "hello")
        self.assertIn("10 min", send_message.call_args.args[1])

        with callback_patches(state, api=api, send_to_pane=send_to_pane):
            result = herdres.callback_reply(callback_payload(f"herdr:ag:{space_token}:{pane_token}"))

        send_to_pane.assert_called_once_with("pane-1", "hello")
        self.assertEqual(space["active_pane"]["42"]["pane_key"], "pane-1")
        self.assertNotIn("42", space["pending_pick"])
        self.assertEqual(result["answer"], "Sent to Codex.")
        body = api.call_args.args[1]["text"]
        self.assertIn("Sent to Codex.", body)
        self.assertIn("10 min", body)
        self.assertIn("no reply or @ needed", body)
        self.assertIn("Queued for pane.", body)

    def test_pick_delivery_send_failure_is_nonfatal(self) -> None:
        # send_to_pane shells out (subprocess timeout) and can RAISE on a stalled
        # pane. The callback must not crash: it must still set+persist the active
        # pane, consume pending, and answer the button.
        import subprocess
        state = osov_state()
        space = state["spaces"]["workspace:one"]
        space["pending_pick"] = {"42": {"text": "hello", "set_at": herdres.utc_now()}}
        space_token = herdres._callback_id("workspace:one", "space")[:16]
        pane_token = herdres._callback_id("pane-1", "pane")[:24]
        api = Mock(return_value={"ok": True, "result": True})
        send_to_pane = Mock(side_effect=subprocess.TimeoutExpired(cmd=["herdr"], timeout=8))

        with callback_patches(state, api=api, send_to_pane=send_to_pane):
            result = herdres.callback_reply(callback_payload(f"herdr:ag:{space_token}:{pane_token}"))

        self.assertIsInstance(result, dict)
        send_to_pane.assert_called_once_with("pane-1", "hello")
        self.assertEqual(space["active_pane"]["42"]["pane_key"], "pane-1")
        self.assertNotIn("42", space["pending_pick"])
        self.assertEqual(result["answer"], "Sending to Codex.")
        self.assertIn("Messages here go to Codex", api.call_args.args[1]["text"])

    def test_agents_command_pick_delivers_nothing(self) -> None:
        state = osov_state()
        space = state["spaces"]["workspace:one"]
        space_token = herdres._callback_id("workspace:one", "space")[:16]
        pane_token = herdres._callback_id("pane-1", "pane")[:24]
        send_to_pane = Mock(return_value=(True, ""))
        api = Mock(return_value={"ok": True, "result": True})

        with command_patches(state):
            result = herdres.command_reply({"chat_id": "-1001", "topic_id": "77", "user_id": "42", "text": "/agents"})

        self.assertEqual(result["reply"], "")
        self.assertNotIn("pending_pick", space)

        with callback_patches(state, api=api, send_to_pane=send_to_pane):
            result = herdres.callback_reply(callback_payload(f"herdr:ag:{space_token}:{pane_token}"))

        send_to_pane.assert_not_called()
        self.assertEqual(result["answer"], "Sending to Codex.")
        self.assertIn("Messages here go to Codex for 10 min", api.call_args.args[1]["text"])

    def test_pending_pick_expires_not_delivered(self) -> None:
        state = osov_state()
        space = state["spaces"]["workspace:one"]
        space["pending_pick"] = {"42": {"text": "hello", "set_at": "2000-01-01T00:00:00+00:00"}}
        space_token = herdres._callback_id("workspace:one", "space")[:16]
        pane_token = herdres._callback_id("pane-1", "pane")[:24]
        send_to_pane = Mock(return_value=(True, ""))
        api = Mock(return_value={"ok": True, "result": True})

        with callback_patches(state, api=api, send_to_pane=send_to_pane):
            result = herdres.callback_reply(callback_payload(f"herdr:ag:{space_token}:{pane_token}"))

        send_to_pane.assert_not_called()
        self.assertEqual(space["active_pane"]["42"]["pane_key"], "pane-1")
        self.assertNotIn("42", space["pending_pick"])
        self.assertEqual(result["answer"], "Sending to Codex.")
        self.assertIn("Messages here go to Codex for 10 min", api.call_args.args[1]["text"])

    def test_pick_closed_pane_keeps_pending(self) -> None:
        state = osov_state()
        space = state["spaces"]["workspace:one"]
        space["pending_pick"] = {"42": {"text": "hello", "set_at": herdres.utc_now()}}
        state["panes"]["pane-1"]["last_known_status"] = "closed"
        space_token = herdres._callback_id("workspace:one", "space")[:16]
        pane_token = herdres._callback_id("pane-1", "pane")[:24]
        send_to_pane = Mock(return_value=(True, ""))

        with callback_patches(state, send_to_pane=send_to_pane):
            result = herdres.callback_reply(callback_payload(f"herdr:ag:{space_token}:{pane_token}"))

        self.assertEqual(result["answer"], "That pane is no longer live.")
        self.assertTrue(result["show_alert"])
        self.assertEqual(space["pending_pick"]["42"]["text"], "hello")
        self.assertNotIn("active_pane", space)
        send_to_pane.assert_not_called()

    def test_pick_confirmation_states_window(self) -> None:
        state = osov_state()
        space_token = herdres._callback_id("workspace:one", "space")[:16]
        pane_token = herdres._callback_id("pane-2", "pane")[:24]
        api = Mock(return_value={"ok": True, "result": True})
        mins = max(1, herdres.ACTIVE_PANE_TTL_SECONDS // 60)

        with callback_patches(state, api=api):
            result = herdres.callback_reply(callback_payload(f"herdr:ag:{space_token}:{pane_token}"))

        self.assertEqual(result["answer"], "Sending to Claude.")
        body = api.call_args.args[1]["text"]
        self.assertIn(f"for {mins} min", body)
        self.assertIn("no reply or @ needed", body)

    def test_plain_after_pick_still_routes(self) -> None:
        state = osov_state()
        space_token = herdres._callback_id("workspace:one", "space")[:16]
        pane_token = herdres._callback_id("pane-2", "pane")[:24]
        send_to_pane = Mock(return_value=(True, ""))

        with callback_patches(state):
            herdres.callback_reply(callback_payload(f"herdr:ag:{space_token}:{pane_token}"))
        with command_patches(state, send_to_pane=send_to_pane):
            result = herdres.command_reply({"chat_id": "-1001", "topic_id": "77", "user_id": "42", "text": "later"})

        self.assertEqual(result["reply"], "")
        send_to_pane.assert_called_once_with("pane-2", "later")

    def test_active_pane_routes_next_plain_text(self) -> None:
        state = osov_state()
        herdres.set_active_pane(state["spaces"]["workspace:one"], "pane-2", "42")
        send_to_pane = Mock(return_value=(True, ""))

        with command_patches(state, send_to_pane=send_to_pane):
            result = herdres.command_reply({"chat_id": "-1001", "topic_id": "77", "user_id": "42", "text": "go"})

        self.assertEqual(result["reply"], "")
        send_to_pane.assert_called_once_with("pane-2", "go")

    def test_active_pane_routes_send_bang_to_active_pane(self) -> None:
        state = osov_state()
        herdres.set_active_pane(state["spaces"]["workspace:one"], "pane-2", "42")
        interrupt_and_send = Mock(return_value=(True, ""))

        with command_patches(state), patch.object(herdres, "interrupt_and_send_to_pane", interrupt_and_send):
            result = herdres.command_reply({"chat_id": "-1001", "topic_id": "77", "user_id": "42", "text": "/send! stop"})

        self.assertNotEqual(result["reply"], herdres.AMBIGUOUS_PANE_THREAD_REPLY)
        interrupt_and_send.assert_called_once_with("pane-2", "stop")

    def test_active_pane_routes_send_to_active_pane(self) -> None:
        state = osov_state()
        herdres.set_active_pane(state["spaces"]["workspace:one"], "pane-2", "42")
        send_to_pane = Mock(return_value=(True, ""))

        with command_patches(state, send_to_pane=send_to_pane):
            result = herdres.command_reply({"chat_id": "-1001", "topic_id": "77", "user_id": "42", "text": "/send hi"})

        self.assertEqual(result["reply"], "")
        send_to_pane.assert_called_once_with("pane-2", "hi")

    def test_active_pane_routes_keys_to_active_pane(self) -> None:
        state = osov_state()
        herdres.set_active_pane(state["spaces"]["workspace:one"], "pane-2", "42")
        run_cmd = Mock(return_value=Mock(returncode=0, stdout="", stderr=""))

        with command_patches(state), patch.object(herdres, "run_cmd", run_cmd):
            result = herdres.command_reply({"chat_id": "-1001", "topic_id": "77", "user_id": "42", "text": "/keys escape"})

        self.assertEqual(result["reply"], "Sent keys: escape")
        run_cmd.assert_called_once_with([herdres.herdr_bin(), "pane", "send-keys", "pane-2", "escape"], timeout=8)

    def test_active_pane_routes_status_to_active_pane(self) -> None:
        state = osov_state()
        herdres.set_active_pane(state["spaces"]["workspace:one"], "pane-2", "42")

        def latest_clean_report(entry, pane):
            self.assertIs(entry, state["panes"]["pane-2"])
            return "active pane status"

        with command_patches(state), patch.multiple(
            herdres,
            TURN_FEED_ENABLED=False,
            latest_clean_item=Mock(return_value=None),
            latest_clean_report=latest_clean_report,
            pane_by_id=Mock(return_value=None),
        ):
            result = herdres.command_reply({"chat_id": "-1001", "topic_id": "77", "user_id": "42", "text": "/status"})

        self.assertEqual(result["reply"], "active pane status")

    def test_agents_command_shows_picker_with_active_pane(self) -> None:
        state = osov_state()
        herdres.set_active_pane(state["spaces"]["workspace:one"], "pane-2", "42")
        send_message = Mock(return_value="901")
        send_to_pane = Mock(return_value=(True, ""))

        with command_patches(state, send_message=send_message, send_to_pane=send_to_pane):
            result = herdres.command_reply({"chat_id": "-1001", "topic_id": "77", "user_id": "42", "text": "/agents"})

        self.assertEqual(result["reply"], "")
        send_message.assert_called_once()
        send_to_pane.assert_not_called()

    def test_active_pane_forwarded_and_edited_messages_do_not_forward(self) -> None:
        for key, value, expected_reply in (
            ("forwarded", True, "Ignored non-direct owner message in pane topic."),
            ("edited", True, ""),
        ):
            with self.subTest(key=key):
                state = osov_state()
                herdres.set_active_pane(state["spaces"]["workspace:one"], "pane-2", "42")
                send_to_pane = Mock(return_value=(True, ""))
                results = []

                def run_script(payload, mode):
                    self.assertEqual(mode, "command")
                    result = herdres.command_reply(payload)
                    results.append(result)
                    return result

                message = {
                    "message_id": 8300 if key == "forwarded" else 8301,
                    "message_thread_id": 77,
                    "chat": {"id": -1001, "is_forum": True},
                    "from": {"id": 42, "is_bot": False},
                    "text": "go",
                }
                if key == "forwarded":
                    message["forward_date"] = 1
                else:
                    message["edit_date"] = 1

                with command_patches(state, send_to_pane=send_to_pane), patch.object(
                    managed_gateway, "load_state", Mock(return_value=state)
                ), patch.object(managed_gateway, "run_script", Mock(side_effect=run_script)), patch.object(
                    managed_gateway, "api", Mock()
                ):
                    managed_gateway.handle_message(message, bot_token="MANAGER_TOKEN")

                self.assertEqual(results[0]["reply"], expected_reply)
                send_to_pane.assert_not_called()

    def test_active_pane_expires_after_ttl(self) -> None:
        state = osov_state()
        state["spaces"]["workspace:one"]["active_pane"] = {
            "42": {"pane_key": "pane-2", "set_at": "2000-01-01T00:00:00Z"}
        }
        send_message = Mock(return_value="901")

        with command_patches(state, send_message=send_message):
            result = herdres.command_reply({"chat_id": "-1001", "topic_id": "77", "user_id": "42", "text": "go"})

        self.assertEqual(result["reply"], "")
        self.assertNotIn("42", state["spaces"]["workspace:one"]["active_pane"])
        send_message.assert_called_once()

    def test_active_pane_closed_pane_evicted(self) -> None:
        state = osov_state(panes=3)
        state["panes"]["pane-2"]["last_known_status"] = "closed"
        herdres.set_active_pane(state["spaces"]["workspace:one"], "pane-2", "42")
        send_message = Mock(return_value="901")

        with command_patches(state, send_message=send_message):
            result = herdres.command_reply({"chat_id": "-1001", "topic_id": "77", "user_id": "42", "text": "go"})

        self.assertEqual(result["reply"], "")
        self.assertNotIn("42", state["spaces"]["workspace:one"]["active_pane"])
        send_message.assert_called_once()

    def test_reply_to_overrides_active_pane(self) -> None:
        state = osov_state()
        state["spaces"]["workspace:one"]["message_routes"] = {"1002": "pane-2"}
        herdres.set_active_pane(state["spaces"]["workspace:one"], "pane-1", "42")
        send_to_pane = Mock(return_value=(True, ""))

        with command_patches(state, send_to_pane=send_to_pane):
            herdres.command_reply({
                "chat_id": "-1001",
                "topic_id": "77",
                "user_id": "42",
                "reply_to_message_id": "1002",
                "text": "reply",
            })

        send_to_pane.assert_called_once_with("pane-2", "reply")

    def test_single_live_pane_implicit_routing_unchanged(self) -> None:
        state = osov_state(panes=1)
        send_to_pane = Mock(return_value=(True, ""))

        with command_patches(state, send_to_pane=send_to_pane):
            result = herdres.command_reply({"chat_id": "-1001", "topic_id": "77", "user_id": "42", "text": "go"})

        self.assertEqual(result["reply"], "")
        send_to_pane.assert_called_once_with("pane-1", "go")

    def test_agents_command_single_live_pane_replies_without_forwarding(self) -> None:
        state = osov_state(panes=1)
        send_to_pane = Mock(return_value=(True, ""))
        results = []

        def run_script(payload, mode):
            self.assertEqual(mode, "command")
            result = herdres.command_reply(payload)
            results.append(result)
            return result

        with command_patches(state, send_to_pane=send_to_pane), patch.object(
            managed_gateway, "load_state", Mock(return_value=state)
        ), patch.object(managed_gateway, "run_script", Mock(side_effect=run_script)), patch.object(
            managed_gateway, "api", Mock()
        ):
            managed_gateway.handle_message(
                {
                    "message_id": 8400,
                    "message_thread_id": 77,
                    "chat": {"id": -1001, "is_forum": True},
                    "from": {"id": 42, "is_bot": False},
                    "text": "/agents",
                },
                bot_token="MANAGER_TOKEN",
            )

        self.assertEqual(results[0]["reply"], "Only one agent here (Codex) — your messages already route to it.")
        send_to_pane.assert_not_called()

    def test_agent_pick_stale_pane_token(self) -> None:
        state = osov_state()
        space_token = herdres._callback_id("workspace:one", "space")[:16]

        with callback_patches(state):
            result = herdres.callback_reply(callback_payload(f"herdr:ag:{space_token}:missing"))

        self.assertEqual(result["answer"], "That pane is no longer live.")
        self.assertTrue(result["show_alert"])

    def test_agent_pick_callback_multi_pane_not_rejected(self) -> None:
        state = osov_state()
        space_token = herdres._callback_id("workspace:one", "space")[:16]
        pane_token = herdres._callback_id("pane-1", "pane")[:24]

        with callback_patches(state):
            result = herdres.callback_reply(callback_payload(f"herdr:ag:{space_token}:{pane_token}"))

        self.assertTrue(result["handled"])
        self.assertEqual(result["answer"], "Sending to Codex.")


class OneSpaceOneVoiceCallbackDataTests(unittest.TestCase):
    def test_multibot_offer_callback_data_within_64_bytes(self) -> None:
        token = herdres._callback_id("workspace:one", "space")[:16]
        for action in ("up", "no"):
            self.assertLessEqual(len(f"herdr:mb:{token}:{action}".encode("utf-8")), 64)

    def test_onboarding_callback_data_within_64_bytes(self) -> None:
        markup = herdres.onboarding_reply_markup("x" * 16, list(herdres.managed_bot_specs().keys()), ["codex"])

        for row in markup["inline_keyboard"]:
            for button in row:
                self.assertLessEqual(len(button["callback_data"].encode("utf-8")), 64)


class Increment2OfferTests(unittest.TestCase):
    def test_offer_signal_recorded_on_ambiguous_multi_kind(self) -> None:
        state = osov_state()

        with command_patches(state), patch.object(herdres, "per_agent_topics_enabled", Mock(return_value=False)):
            result = herdres.command_reply({"chat_id": "-1001", "topic_id": "77", "user_id": "42", "text": "/status"})

        space = state["spaces"]["workspace:one"]
        self.assertEqual(result["reply"], herdres.AMBIGUOUS_PANE_THREAD_REPLY)
        self.assertEqual(space["multibot_offer_signal"], 1)
        self.assertIn("multibot_offer_last_signal_at", space)

    def test_offer_signal_suppressed_when_per_agent_mode_or_voice_or_dismissed_or_single_kind(self) -> None:
        for name, mutate in (
            ("per-agent topics", lambda state: None),
            ("voice", lambda state: state["spaces"]["workspace:one"].update({"voice_mode": "per_agent"})),
            ("dismissed", lambda state: state["spaces"]["workspace:one"].update({"multibot_offer_dismissed": True})),
            ("single kind", lambda state: state["panes"]["pane-2"].update({"agent": "codex"})),
        ):
            with self.subTest(name=name):
                state = osov_state()
                mutate(state)
                per_agent_topics = name == "per-agent topics"
                with command_patches(state), patch.object(
                    herdres, "per_agent_topics_enabled", Mock(return_value=per_agent_topics)
                ):
                    herdres.command_reply({"chat_id": "-1001", "topic_id": "77", "user_id": "42", "text": "/status"})
                self.assertNotIn("multibot_offer_signal", state["spaces"]["workspace:one"])

    def test_ensure_multibot_offer_message_posts_once_idempotent(self) -> None:
        state = osov_state()
        telegram = state["telegram"]
        telegram["can_manage_bots"] = True
        state["spaces"]["workspace:one"]["multibot_offer_signal"] = 1
        send_notice = Mock(return_value={"ok": True, "message_id": "9001"})
        panes = list(state["panes"].values())
        counters = {"sends": 0}

        with patch.object(herdres, "managed_bot_setup_enabled", Mock(return_value=True)), patch.object(
            herdres, "per_agent_topics_enabled", Mock(return_value=False)
        ), patch.object(herdres, "send_notice", send_notice), patch.object(herdres, "save_state", Mock()):
            self.assertTrue(herdres.ensure_multibot_offer_message(state, "-1001", telegram, counters, 5, panes))
            self.assertFalse(herdres.ensure_multibot_offer_message(state, "-1001", telegram, counters, 5, panes))

        self.assertEqual(state["spaces"]["workspace:one"]["multibot_offer_message_id"], "9001")
        self.assertEqual(send_notice.call_count, 1)
        self.assertEqual(counters["sends"], 1)

    def test_multibot_offer_failure_cooldown_suppresses_refire(self) -> None:
        # A send failure (deleted topic / kicked bot -> ok=False, no raise) must not
        # re-fire and burn a send slot every sync; the cooldown suppresses retries.
        state = osov_state()
        telegram = state["telegram"]
        telegram["can_manage_bots"] = True
        state["spaces"]["workspace:one"]["multibot_offer_signal"] = 1
        send_notice = Mock(return_value={"ok": False})
        panes = list(state["panes"].values())

        with patch.object(herdres, "managed_bot_setup_enabled", Mock(return_value=True)), patch.object(
            herdres, "per_agent_topics_enabled", Mock(return_value=False)
        ), patch.object(herdres, "send_notice", send_notice), patch.object(herdres, "save_state", Mock()):
            counters = {"sends": 0}
            self.assertTrue(herdres.ensure_multibot_offer_message(state, "-1001", telegram, counters, 5, panes))
            counters2 = {"sends": 0}
            self.assertFalse(herdres.ensure_multibot_offer_message(state, "-1001", telegram, counters2, 5, panes))

        self.assertEqual(send_notice.call_count, 1)
        self.assertEqual(counters2["sends"], 0)
        self.assertNotIn("multibot_offer_message_id", state["spaces"]["workspace:one"])
        self.assertTrue(state["spaces"]["workspace:one"].get("multibot_offer_error_at"))

    def test_ensure_multibot_offer_suppressed_cases(self) -> None:
        for name, mutate, max_sends in (
            ("cannot manage", lambda state: state["telegram"].update({"can_manage_bots": False}), 5),
            ("no signal", lambda state: state["telegram"].update({"can_manage_bots": True}), 5),
            ("single kind", lambda state: (state["telegram"].update({"can_manage_bots": True}), state["spaces"]["workspace:one"].update({"multibot_offer_signal": 1}), state["panes"]["pane-2"].update({"agent": "codex"})), 5),
            ("budget", lambda state: (state["telegram"].update({"can_manage_bots": True}), state["spaces"]["workspace:one"].update({"multibot_offer_signal": 1})), 0),
        ):
            with self.subTest(name=name):
                state = osov_state()
                mutate(state)
                send_notice = Mock(return_value={"ok": True, "message_id": "9001"})
                with patch.object(herdres, "managed_bot_setup_enabled", Mock(return_value=True)), patch.object(
                    herdres, "per_agent_topics_enabled", Mock(return_value=False)
                ), patch.object(herdres, "send_notice", send_notice), patch.object(herdres, "save_state", Mock()):
                    changed = herdres.ensure_multibot_offer_message(
                        state, "-1001", state["telegram"], {"sends": 0}, max_sends, list(state["panes"].values())
                    )
                self.assertFalse(changed)
                send_notice.assert_not_called()

    def test_mb_offer_upgrade_flips_voice_per_agent_and_emits_scoped_markup(self) -> None:
        state = osov_state(panes=2)
        state["telegram"]["bot_username"] = "manager_bot"
        state["spaces"]["workspace:two"] = {
            "space_key": "workspace:two",
            "topic_id": "88",
            "pane_keys": ["pane-x"],
        }
        state["panes"]["pane-x"] = {
            "pane_key": "pane-x",
            "pane_id": "pane-x",
            "agent": "devin",
            "space_key": "workspace:two",
            "topic_id": "88",
            "last_known_status": "working",
        }
        token = herdres._callback_id("workspace:one", "space")[:16]
        api = Mock(return_value={"ok": True, "result": True})

        with callback_patches(state, api=api):
            result = herdres.callback_reply(callback_payload(f"herdr:mb:{token}:up"))

        self.assertEqual(result["answer"], "Per-agent voice enabled.")
        self.assertEqual(state["spaces"]["workspace:one"]["voice_mode"], "per_agent")
        self.assertTrue(state["panes"]["pane-1"]["managed_voice_active"])
        self.assertTrue(state["panes"]["pane-2"]["managed_voice_active"])
        self.assertNotIn("managed_voice_active", state["panes"]["pane-x"])
        markup_call = [call for call in api.call_args_list if call.args[0] == "editMessageReplyMarkup"][0]
        markup = markup_call.args[1]["reply_markup"]
        self.assertIn("Codex", markup)
        self.assertIn("Claude", markup)
        self.assertNotIn("Devin", markup)

    def test_mb_dismiss_sets_dismissed_and_edits_card(self) -> None:
        state = osov_state()
        space = state["spaces"]["workspace:one"]
        space["multibot_offer_signal"] = 2
        token = herdres._callback_id("workspace:one", "space")[:16]
        api = Mock(return_value={"ok": True, "result": True})

        with callback_patches(state, api=api):
            result = herdres.callback_reply(callback_payload(f"herdr:mb:{token}:no"))

        self.assertEqual(result["answer"], "Dismissed.")
        self.assertTrue(space["multibot_offer_dismissed"])
        self.assertNotIn("multibot_offer_signal", space)
        self.assertEqual(api.call_args.args[0], "editMessageText")

    def test_mb_callback_nonowner_denied_and_stale_space(self) -> None:
        state = osov_state()
        token = herdres._callback_id("workspace:one", "space")[:16]

        with callback_patches(state):
            denied = herdres.callback_reply(callback_payload(f"herdr:mb:{token}:up", user_id="99"))
            stale = herdres.callback_reply(callback_payload(f"herdr:mb:{token}:up", topic_id="88"))

        self.assertEqual(denied["answer"], "Not authorized.")
        self.assertTrue(denied["show_alert"])
        self.assertEqual(stale["answer"], "This space is no longer active.")
        self.assertTrue(stale["show_alert"])

    def test_mb_offer_startgroup_only_for_captured(self) -> None:
        pane1 = {"pane_id": "pane-1", "workspace_id": "one", "agent": "codex", "agent_status": "working"}
        pane2 = {"pane_id": "pane-2", "workspace_id": "one", "agent": "claude", "agent_status": "working"}
        key1 = herdres.pane_key(pane1)
        key2 = herdres.pane_key(pane2)
        state = {
            "telegram": {
                "general_thread_id": "1",
                "managed_bots": {
                    "codex": {"username": "herdr_codex_bot", "token": "CODEX_TOKEN", "enabled": True}
                },
            },
            "panes": {
                key1: {
                    "pane_key": key1,
                    "agent": "codex",
                    "pane_root_bot_kind_retry_kind": "codex",
                    "pane_root_bot_kind_retry_at": herdres.utc_now(),
                },
                key2: {
                    "pane_key": key2,
                    "agent": "claude",
                    "pane_root_bot_kind_retry_kind": "claude",
                    "pane_root_bot_kind_retry_at": herdres.utc_now(),
                },
            },
        }
        send_notice = Mock(return_value={"ok": True, "message_id": "9001"})

        with patch.object(herdres, "send_notice", send_notice), patch.object(
            herdres, "save_state", Mock()
        ), patch.object(herdres, "MANAGED_BOTS_ENABLED", True):
            changed = herdres.ensure_managed_bot_group_access_message(
                state, "-1001", state["telegram"], {"sends": 0}, 5, [pane1, pane2]
            )

        self.assertTrue(changed)
        self.assertEqual(state["telegram"]["managed_bot_group_access_kinds"], ["codex"])
        self.assertIn("herdr_codex_bot", json.dumps(send_notice.call_args.kwargs["reply_markup"]))
        self.assertNotIn("herdr_claude_bot", json.dumps(send_notice.call_args.kwargs["reply_markup"]))

    def test_managed_bot_update_captures_token_kind_keyed(self) -> None:
        state = {"version": 1, "telegram": {}}
        payload = {
            "message": {
                "from": {"id": 42},
                "managed_bot_created": {"bot": {"id": 111, "username": "herdr_codex_bot", "first_name": "Codex"}},
            }
        }
        with patch.multiple(
            herdres,
            load_dotenv=Mock(),
            load_state=Mock(return_value=state),
            save_state=Mock(),
            telegram_api=Mock(return_value={"ok": True, "result": "CODEX_TOKEN"}),
            configure_managed_bot_profile=Mock(return_value={"ok": True}),
        ):
            result = herdres.managed_bot_update(payload)

        self.assertTrue(result["handled"])
        self.assertEqual(state["telegram"]["managed_bots"]["codex"]["token"], "CODEX_TOKEN")

    def test_per_agent_space_without_child_uses_manager_outbound(self) -> None:
        telegram = {"managed_bots": {}}
        entry = {"agent": "codex", "managed_voice_active": True}

        self.assertIsNone(herdres.managed_bot_token_for_entry(telegram, entry))

    def test_per_agent_space_without_child_manager_owns_inbound(self) -> None:
        state = osov_state(panes=1)
        state["spaces"]["workspace:one"]["voice_mode"] = "per_agent"
        entry = state["panes"]["pane-1"]

        self.assertEqual(managed_gateway.owner_for_entry(state, entry), "manager")
        owners = managed_gateway.message_owner_kinds(
            state,
            "hello",
            {
                "message_id": 1,
                "message_thread_id": 77,
                "chat": {"id": -1001, "is_forum": True},
                "from": {"id": 42, "is_bot": False},
            },
            "-1001",
            "77",
        )
        self.assertEqual(owners, {"manager"})

    def test_voice_shared_reverts_routing_to_manager_no_teardown(self) -> None:
        state = osov_state()
        state["telegram"]["managed_bots"] = {"codex": {"token": "CODEX_TOKEN", "enabled": True}}
        state["spaces"]["workspace:one"]["voice_mode"] = "per_agent"
        for entry in state["panes"].values():
            herdres.refresh_entry_managed_voice(state, entry)

        with command_patches(state):
            result = herdres.command_reply({"chat_id": "-1001", "topic_id": "77", "user_id": "42", "text": "/voice shared"})

        self.assertIn("shared manager bot", result["reply"])
        self.assertEqual(state["spaces"]["workspace:one"]["voice_mode"], "shared")
        self.assertFalse(state["panes"]["pane-1"]["managed_voice_active"])
        self.assertIn("codex", state["telegram"]["managed_bots"])

    def test_voice_mode_survives_grouping_reset(self) -> None:
        state = osov_state(panes=1)
        state["spaces"]["workspace:one"]["voice_mode"] = "per_agent"
        state["panes"]["pane-1"]["workspace_id"] = "one"

        with tempfile.TemporaryDirectory() as tmp, patch.dict(
            "os.environ", {"HERDR_TELEGRAM_TOPICS_STATE": str(Path(tmp) / "state.json")}
        ):
            herdres.reset_topic_grouping(state, "agent", reason="test")
            herdres.ensure_space_entry(state, state["panes"]["pane-1"])
            herdres.save_state(state)
            loaded = herdres.load_state()

        self.assertEqual(loaded["spaces"]["workspace:one"]["voice_mode"], "per_agent")

    def test_downgrade_one_space_does_not_remove_kind_record_or_stop_other_spaces_worker(self) -> None:
        state = osov_state()
        state["telegram"]["managed_bots"] = {"codex": {"token": "CODEX_TOKEN", "enabled": True}}
        before = managed_gateway.managed_bot_tokens(state)

        with command_patches(state):
            herdres.command_reply({"chat_id": "-1001", "topic_id": "77", "user_id": "42", "text": "/voice shared"})

        self.assertEqual(managed_gateway.managed_bot_tokens(state), before)

    def test_offer_to_upgrade_to_capture_to_poller_birth(self) -> None:
        state = osov_state()
        state["telegram"].update({"can_manage_bots": True, "bot_username": "manager_bot"})
        with command_patches(state), patch.object(herdres, "per_agent_topics_enabled", Mock(return_value=False)):
            herdres.command_reply({"chat_id": "-1001", "topic_id": "77", "user_id": "42", "text": "/status"})
        with patch.object(herdres, "managed_bot_setup_enabled", Mock(return_value=True)), patch.object(
            herdres, "per_agent_topics_enabled", Mock(return_value=False)
        ), patch.object(herdres, "send_notice", Mock(return_value={"ok": True, "message_id": "9001"})), patch.object(
            herdres, "save_state", Mock()
        ):
            self.assertTrue(
                herdres.ensure_multibot_offer_message(
                    state, "-1001", state["telegram"], {"sends": 0}, 5, list(state["panes"].values())
                )
            )
        token = herdres._callback_id("workspace:one", "space")[:16]
        with callback_patches(state):
            herdres.callback_reply(callback_payload(f"herdr:mb:{token}:up"))
        with patch.multiple(
            herdres,
            load_dotenv=Mock(),
            load_state=Mock(return_value=state),
            save_state=Mock(),
            telegram_api=Mock(return_value={"ok": True, "result": "CODEX_TOKEN"}),
            configure_managed_bot_profile=Mock(return_value={"ok": True}),
        ):
            herdres.managed_bot_update({"bot": {"id": 111, "username": "herdr_codex_bot"}})

        self.assertEqual(state["spaces"]["workspace:one"]["voice_mode"], "per_agent")
        self.assertTrue(any(key.startswith("managed-codex-") for key, _token in managed_gateway.managed_bot_tokens(state)))

    def test_agents_callback_data_within_64_bytes(self) -> None:
        live = [(f"pane-key-{idx}-" + "x" * 80, {"agent": "codex", "pane_id": "pane-" + "y" * 80}) for idx in range(12)]
        markup = herdres.agents_picker_reply_markup("x" * 16, live)

        for row in markup["inline_keyboard"]:
            for button in row:
                self.assertLessEqual(len(button["callback_data"].encode("utf-8")), 64)
