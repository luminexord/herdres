from __future__ import annotations

import unittest
from unittest.mock import Mock, patch

import herdres


class ManagedBotRoutingRepairTests(unittest.TestCase):
    def test_chat_not_found_is_child_bot_access_failure(self) -> None:
        error = herdres.BridgeError("Telegram sendMessage failed: Bad Request: chat not found")

        self.assertEqual(herdres.classify_telegram_error(error, managed_bot_context=True), "bot_access")

    def test_missing_reply_target_is_pane_root_not_found(self) -> None:
        error = herdres.BridgeError("Telegram sendMessage failed: Bad Request: message to be replied not found")

        self.assertEqual(herdres.classify_telegram_error(error), "not_found")

    def test_child_bot_chat_not_found_does_not_fallback_to_manager_by_default(self) -> None:
        send_message = Mock(
            side_effect=herdres.BridgeError("Telegram sendMessage failed: Bad Request: chat not found")
        )

        with patch.object(herdres, "send_message", send_message):
            result = herdres.send_legacy_message_result(
                "-1001",
                "hello",
                thread_id="77",
                api_token="CHILD_TOKEN",
            )

        self.assertFalse(result["ok"])
        self.assertEqual(result["kind"], "bot_access")
        send_message.assert_called_once()
        self.assertEqual(send_message.call_args.kwargs["api_token"], "CHILD_TOKEN")

    def test_existing_manager_root_is_reissued_by_child_bot(self) -> None:
        pane = {
            "pane_id": "pane-1",
            "workspace_id": "workspace-1",
            "agent": "codex",
            "label": "ChatGPT",
        }
        entry = {
            "pane_key": "pane-1",
            "pane_id": "pane-1",
            "space_key": "workspace:workspace-1",
            "topic_id": "77",
            "pane_root_message_id": "1001",
            "agent": "codex",
            "managed_voice_active": True,
        }
        state = {
            "spaces": {"workspace:workspace-1": {"message_routes": {"1001": "pane-1"}}},
            "panes": {"pane-1": entry},
        }
        telegram = {"managed_bots": {"codex": {"token": "CODEX_TOKEN", "enabled": True}}}
        send_root = Mock(return_value={"ok": True, "message_id": "2001"})

        with patch.object(herdres, "send_pane_root_message", send_root), patch.object(
            herdres, "save_state", Mock()
        ), patch.object(herdres, "PANE_ROOT_MESSAGES_ENABLED", True):
            changed, result = herdres.ensure_pane_root_message(
                state,
                "-1001",
                telegram,
                pane,
                entry,
                {"sends": 0},
                5,
            )

        self.assertTrue(changed)
        self.assertTrue(result["ok"])
        self.assertEqual(entry["pane_root_message_id"], "2001")
        self.assertEqual(entry["pane_root_bot_kind"], "codex")
        self.assertEqual(state["spaces"]["workspace:workspace-1"]["message_routes"]["2001"], "pane-1")

    def test_pane_root_waits_after_child_bot_group_access_failure(self) -> None:
        pane = {
            "pane_id": "pane-1",
            "workspace_id": "workspace-1",
            "agent": "codex",
            "label": "ChatGPT",
        }
        entry = {
            "pane_key": "pane-1",
            "pane_id": "pane-1",
            "space_key": "workspace:workspace-1",
            "topic_id": "77",
            "agent": "codex",
            "pane_root_bot_kind": "manager",
            "pane_root_bot_kind_retry_kind": "codex",
            "pane_root_bot_kind_retry_at": herdres.utc_now(),
            "managed_voice_active": True,
        }
        state = {
            "spaces": {"workspace:workspace-1": {"message_routes": {}}},
            "panes": {"pane-1": entry},
        }
        telegram = {"managed_bots": {"codex": {"token": "CODEX_TOKEN", "enabled": True}}}
        send_root = Mock(return_value={"ok": True, "message_id": "2001"})

        with patch.object(herdres, "send_pane_root_message", send_root), patch.object(
            herdres, "PANE_ROOT_MESSAGES_ENABLED", True
        ):
            changed, result = herdres.ensure_pane_root_message(
                state,
                "-1001",
                telegram,
                pane,
                entry,
                {"sends": 0},
                5,
            )

        self.assertFalse(changed)
        self.assertTrue(result["skipped"])
        self.assertEqual(result["reason"], "managed_bot_access_pending")
        send_root.assert_not_called()

    def test_existing_manager_status_marker_is_reissued_by_child_bot(self) -> None:
        pane = {
            "pane_id": "pane-1",
            "workspace_id": "workspace-1",
            "agent": "codex",
            "agent_status": "idle",
            "label": "ChatGPT",
        }
        entry = {
            "pane_key": "pane-1",
            "pane_id": "pane-1",
            "space_key": "workspace:workspace-1",
            "topic_id": "77",
            "pane_root_message_id": "1001",
            "agent": "codex",
            "status_marker_message_id": "1002",
            "status_marker_hash": herdres.status_marker_hash(pane),
            "managed_voice_active": True,
        }
        telegram = {"managed_bots": {"codex": {"token": "CODEX_TOKEN", "enabled": True}}}
        send_notice = Mock(return_value={"ok": True, "message_id": "2002"})

        with patch.object(herdres, "send_notice", send_notice), patch.object(herdres, "delete_message", Mock()):
            result = herdres.update_status_marker("-1001", entry, pane, telegram=telegram)

        self.assertTrue(result["ok"])
        self.assertTrue(result["attempted"])
        self.assertEqual(entry["status_marker_message_id"], "2002")
        self.assertEqual(entry["status_marker_bot_kind"], "codex")
        self.assertEqual(send_notice.call_args.kwargs["api_token"], "CODEX_TOKEN")

    def test_status_marker_waits_after_child_bot_group_access_failure(self) -> None:
        pane = {
            "pane_id": "pane-1",
            "workspace_id": "workspace-1",
            "agent": "codex",
            "agent_status": "idle",
            "label": "ChatGPT",
        }
        entry = {
            "pane_key": "pane-1",
            "pane_id": "pane-1",
            "space_key": "workspace:workspace-1",
            "topic_id": "77",
            "pane_root_message_id": "1001",
            "agent": "codex",
            "status_marker_bot_kind": "manager",
            "status_marker_bot_kind_retry_kind": "codex",
            "status_marker_bot_kind_retry_at": herdres.utc_now(),
            "managed_voice_active": True,
        }
        telegram = {"managed_bots": {"codex": {"token": "CODEX_TOKEN", "enabled": True}}}
        send_notice = Mock(return_value={"ok": True, "message_id": "2002"})

        with patch.object(herdres, "send_notice", send_notice):
            result = herdres.update_status_marker("-1001", entry, pane, telegram=telegram)

        self.assertTrue(result["ok"])
        self.assertFalse(result["attempted"])
        self.assertEqual(result["kind"], "managed_bot_access_pending")
        send_notice.assert_not_called()

    def test_plain_reply_forwards_without_ack_message(self) -> None:
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
                    "last_known_status": "working",
                }
            },
        }
        send_to_pane = Mock(return_value=(True, ""))

        with patch.object(herdres, "load_dotenv", Mock()), patch.object(
            herdres, "load_state", Mock(return_value=state)
        ), patch.object(
            herdres,
            "send_to_pane",
            send_to_pane,
        ), patch.object(herdres, "save_state", Mock()):
            result = herdres.command_reply(
                {
                    "chat_id": "-1001",
                    "topic_id": "77",
                    "message_id": "4000",
                    "reply_to_message_id": "1001",
                    "user_id": "42",
                    "text": "Hey",
                }
            )

        self.assertTrue(result["handled"])
        self.assertEqual(result["reply"], "")
        send_to_pane.assert_called_once_with("pane-1", "Hey")

    def test_devin_agent_uses_managed_bot_spec(self) -> None:
        self.assertEqual(herdres.managed_bot_kind_for_agent("devin"), "devin")
        self.assertEqual(herdres.managed_bot_kind_for_agent("Cognition Devin"), "devin")
        self.assertEqual(herdres.managed_bot_kinds_for_panes([{"agent": "devin"}]), ["devin"])
        self.assertEqual(herdres.pane_agent_status_label({"agent": "devin"}), "Devin")

        keyboard = herdres.managed_bot_request_keyboard(kinds=["devin"])
        first = keyboard["keyboard"][0][0]

        self.assertEqual(first["text"], "Create Devin bot")
        self.assertEqual(first["request_managed_bot"]["request_id"], 41005)
        self.assertEqual(first["request_managed_bot"]["suggested_name"], "Herdr Devin")
        self.assertEqual(first["request_managed_bot"]["suggested_username"], "herdr_devin_bot")

    def test_plain_reply_to_busy_agent_reports_queued_not_failed(self) -> None:
        # E2E through the gateway's entry point with the REAL send path: an owner
        # reply routed to a WORKING pane runs command_reply -> forward_text_to_pane_
        # response -> send_to_pane -> submit_staged_pane_input_if_needed. A busy
        # agent queues the input (box never clears), so the reply must be the
        # "Queued" note, NOT "Send failed". Only the Herdr CLI boundary is mocked.
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
                    "last_known_status": "working",
                }
            },
        }
        working_pane = {"pane_id": "pane-1", "agent": "claude", "agent_status": "working"}
        with patch.object(herdres.time, "sleep", lambda *_: None), patch.multiple(
            herdres,
            load_dotenv=Mock(),
            load_state=Mock(return_value=state),
            save_state=Mock(),
            pane_by_id=Mock(return_value=working_pane),
            clear_staged_pane_input_if_needed=Mock(return_value=(True, "")),
            run_cmd=Mock(return_value=Mock(returncode=0, stdout="", stderr="")),
            pane_input_looks_staged=Mock(return_value=True),  # busy agent: box never clears
        ):
            result = herdres.command_reply(
                {
                    "chat_id": "-1001",
                    "topic_id": "77",
                    "message_id": "4000",
                    "reply_to_message_id": "1001",
                    "user_id": "42",
                    "text": "deploy when ready",
                }
            )

        self.assertTrue(result["handled"])
        self.assertIn("Queued", result["reply"])
        self.assertNotIn("Send failed", result["reply"])

    def test_send_bang_interrupts_busy_agent_then_delivers(self) -> None:
        # E2E: "/send!" to a busy pane must halt the current turn (send Esc) and
        # then deliver immediately — not queue. Only the Herdr CLI is mocked.
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
                    "last_known_status": "working",
                }
            },
        }
        # The pane is working, so /send! must interrupt (Esc). The box is empty
        # (pane_input_looks_staged mocked False), so the message submits cleanly.
        working_pane = {"pane_id": "pane-1", "agent": "claude", "agent_status": "working"}
        calls = []

        def run_cmd(args, **kwargs):
            calls.append(args)
            return Mock(returncode=0, stdout="", stderr="")

        with patch.object(herdres.time, "sleep", lambda *_: None), patch.multiple(
            herdres,
            load_dotenv=Mock(),
            load_state=Mock(return_value=state),
            save_state=Mock(),
            pane_by_id=Mock(return_value=working_pane),
            clear_staged_pane_input_if_needed=Mock(return_value=(True, "")),
            run_cmd=run_cmd,
            pane_input_looks_staged=Mock(return_value=False),  # box clears -> delivered
        ):
            result = herdres.command_reply(
                {
                    "chat_id": "-1001",
                    "topic_id": "77",
                    "message_id": "4000",
                    "reply_to_message_id": "1001",
                    "user_id": "42",
                    "text": "/send! deploy now",
                }
            )

        # Esc was issued (interrupt) and the message was actually run into the pane.
        self.assertTrue(any("send-keys" in a and "escape" in a for a in calls), calls)
        self.assertTrue(any("run" in a for a in calls), calls)
        self.assertTrue(result["handled"])
        self.assertIn("Interrupted", result["reply"])
        self.assertNotIn("Send failed", result["reply"])
        self.assertNotIn("Queued", result["reply"])

    def test_group_access_markup_uses_child_bot_startgroup_links(self) -> None:
        telegram = {
            "managed_bots": {
                "codex": {"username": "herdr_codex_bot", "token": "CODEX_TOKEN"},
                "claude": {"token": "CLAUDE_TOKEN"},
            }
        }

        markup = herdres.managed_bot_group_access_reply_markup(telegram, ["codex", "claude"])
        buttons = [button for row in markup["inline_keyboard"] for button in row]

        self.assertEqual([button["text"] for button in buttons], ["Add Codex", "Add Claude"])
        self.assertEqual(buttons[0]["url"], "https://t.me/herdr_codex_bot?startgroup=herdres")
        self.assertEqual(buttons[1]["url"], "https://t.me/herdr_claude_bot?startgroup=herdres")

    def test_group_access_notice_is_sent_for_open_pane_child_fallback(self) -> None:
        pane = {
            "pane_id": "pane-1",
            "workspace_id": "workspace-1",
            "agent": "codex",
            "agent_status": "working",
        }
        entry = {
            "pane_key": herdres.pane_key(pane),
            "agent": "codex",
            "pane_root_bot_kind": "manager",
            "pane_root_bot_kind_retry_kind": "codex",
            "pane_root_bot_kind_retry_at": herdres.utc_now(),
        }
        state = {
            "telegram": {
                "managed_bots": {
                    "codex": {"username": "herdr_codex_bot", "token": "CODEX_TOKEN", "enabled": True},
                },
                "general_thread_id": "1",
            },
            "panes": {entry["pane_key"]: entry},
        }
        send_notice = Mock(return_value={"ok": True, "message_id": "9001"})
        counters = {"sends": 0}

        with patch.object(herdres, "send_notice", send_notice), patch.object(
            herdres, "save_state", Mock()
        ), patch.object(herdres, "MANAGED_BOTS_ENABLED", True):
            changed = herdres.ensure_managed_bot_group_access_message(
                state,
                "-1001",
                state["telegram"],
                counters,
                5,
                [pane],
            )

        self.assertTrue(changed)
        self.assertEqual(counters["sends"], 1)
        self.assertEqual(state["telegram"]["managed_bot_group_access_message_id"], "9001")
        self.assertEqual(state["telegram"]["managed_bot_group_access_kinds"], ["codex"])
        send_notice.assert_called_once()
        self.assertIn("Telegram is rejecting", send_notice.call_args.args[2])
        buttons = send_notice.call_args.kwargs["reply_markup"]["inline_keyboard"][0]
        self.assertEqual(buttons[0]["url"], "https://t.me/herdr_codex_bot?startgroup=herdres")
