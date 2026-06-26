from __future__ import annotations

import unittest
from unittest.mock import Mock, patch

import herdres


class DeliveryReliabilityBase(unittest.TestCase):
    def setUp(self) -> None:
        if hasattr(herdres, "clear_sync_caches"):
            herdres.clear_sync_caches()

    def _pane(self, *, status: str = "done") -> dict:
        return {
            "pane_id": "pane-1",
            "terminal_id": "term-1",
            "workspace_id": "workspace-1",
            "tab_id": "tab-1",
            "agent": "claude",
            "agent_status": status,
            "label": "Claude",
        }

    def _state(self, *, status: str = "done") -> tuple[dict, dict, str, dict, dict, dict]:
        pane = self._pane(status=status)
        pane_key = herdres.pane_key(pane)
        space_key = herdres.space_key(pane)
        entry = {
            "pane_key": pane_key,
            "pane_id": pane["pane_id"],
            "space_key": space_key,
            "topic_id": "77",
            "pane_root_message_id": "1001",
            "pane_thread_name": "Claude",
        }
        state = {
            "version": 1,
            "enabled": True,
            "telegram": {"chat_id": "-1001", "owner_user_ids": ["42"]},
            "spaces": {
                space_key: {
                    "space_key": space_key,
                    "topic_id": "77",
                    "pane_keys": [pane_key],
                    "message_routes": {},
                }
            },
            "panes": {pane_key: entry},
        }
        counters = {"creates": 0, "sends": 0, "feed_sends": 0, "marker_sends": 0, "verifies": 0, "renames": 0}
        caps = {"max_creates": 0, "max_sends": 10, "max_feed_sends": 10, "max_marker_sends": 0, "max_verifies": 0}
        return state, pane, pane_key, entry, counters, caps

    def _turn_item(self, turn_id: str, *, answer: str = "Stable final answer.", prompt: str = "Do the work.") -> dict:
        item = herdres.make_turn_feed_item(
            {
                "available": True,
                "complete": True,
                "has_open_turn": False,
                "turn_id": turn_id,
                "user_text": prompt,
                "assistant_final_text": answer,
            }
        )
        assert item is not None
        return item

    def _sync_item(
        self,
        state: dict,
        pane: dict,
        counters: dict,
        caps: dict,
        item: dict | None,
        send_feed_item: Mock,
    ) -> bool:
        space = state["spaces"][herdres.space_key(pane)]
        with patch.object(herdres.time, "sleep", Mock()), patch.multiple(
            herdres,
            ensure_space_topic=Mock(return_value=(space, False)),
            ensure_pane_root_message=Mock(return_value=(False, {"ok": True})),
            topic_verify_due=Mock(return_value=False),
            extract_turn_feed_item=Mock(return_value=item),
            send_pending_prompt_message=Mock(return_value={"changed": False, "topic_missing": False, "pane_root_missing": False}),
            send_feed_item=send_feed_item,
            save_state=Mock(),
            apply_api_error_warning=Mock(return_value={"topic_missing": False, "changed": False}),
            fold_superseded_turns=Mock(return_value=False),
            flush_pending_plan_doc=Mock(return_value=False),
            flush_pending_speech_reply=Mock(return_value=False),
            CLEAN_FEED_ENABLED=True,
            TURN_FEED_ENABLED=True,
            LIVE_CARD_ENABLED=False,
            STATUS_MARKER_ENABLED=False,
            STATUS_ICON_ENABLED=False,
        ):
            return herdres.sync_pane_once(state, "-1001", state["telegram"], pane, counters, caps)


class CleanFeedTransientRetryTests(DeliveryReliabilityBase):
    def test_transient_fail_then_ok_records_one_logical_delivery(self) -> None:
        state, pane, _pane_key, entry, counters, caps = self._state()
        item = self._turn_item("turn-1")
        send_feed_item = Mock(
            side_effect=[
                {"ok": False, "kind": "transient", "transient": True, "error": "timeout"},
                {"ok": True, "message_id": "2001"},
            ]
        )

        changed = self._sync_item(state, pane, counters, caps, item, send_feed_item)

        self.assertTrue(changed)
        self.assertEqual(send_feed_item.call_count, 2)
        self.assertEqual(counters["feed_sends"], 1)
        self.assertEqual(counters["sends"], 1)
        self.assertEqual(entry["last_clean_message_id"], "2001")
        self.assertEqual(entry["last_turn_id"], "turn-1")
        self.assertNotIn("last_clean_send_error", entry)

    def test_exhausted_transient_leaves_no_cursor_and_next_sync_is_not_throttled(self) -> None:
        state, pane, _pane_key, entry, counters, caps = self._state()
        item = self._turn_item("turn-1")
        mode = {"ok": False}

        def send_result(*_args, **_kwargs):
            if mode["ok"]:
                return {"ok": True, "message_id": "2002"}
            return {"ok": False, "kind": "transient", "transient": True, "error": "timeout"}

        send_feed_item = Mock(side_effect=send_result)

        self._sync_item(state, pane, counters, caps, item, send_feed_item)
        first_attempts = send_feed_item.call_count

        self.assertGreaterEqual(first_attempts, 2)
        self.assertNotIn("last_clean_hash", entry)
        self.assertNotIn("last_clean_message_id", entry)
        self.assertNotIn("last_turn_id", entry)
        self.assertNotIn("last_clean_attempt_hash", entry)
        self.assertIn("timeout", entry.get("last_clean_send_error", ""))

        mode["ok"] = True
        self._sync_item(state, pane, counters, caps, item, send_feed_item)

        self.assertEqual(send_feed_item.call_count, first_attempts + 1)
        self.assertEqual(entry["last_clean_message_id"], "2002")
        self.assertEqual(entry["last_turn_id"], "turn-1")

    def test_rate_limited_clean_feed_send_is_not_retried(self) -> None:
        state, pane, _pane_key, _entry, counters, caps = self._state()
        send_feed_item = Mock(side_effect=herdres.RateLimited(3))

        with self.assertRaises(herdres.RateLimited):
            self._sync_item(state, pane, counters, caps, self._turn_item("turn-1"), send_feed_item)

        send_feed_item.assert_called_once()

    def test_non_transient_clean_feed_failures_are_not_retried(self) -> None:
        cases = {
            "permanent": {"ok": False, "kind": "permanent", "error": "permanent failure"},
            "http400": {"ok": False, "kind": "bad_request", "error": "HTTP 400 Bad Request"},
            "bot_access": {"ok": False, "kind": "bot_access", "error": "bot was kicked"},
            "topic_not_found": {"ok": False, "kind": "topic_not_found", "topic_missing": True, "error": "topic not found"},
            "message_not_found": {"ok": False, "kind": "not_found", "not_found": True, "error": "message not found"},
        }

        for name, result in cases.items():
            with self.subTest(name=name):
                state, pane, _pane_key, entry, counters, caps = self._state()
                send_feed_item = Mock(return_value=result)

                self._sync_item(state, pane, counters, caps, self._turn_item(f"turn-{name}"), send_feed_item)

                send_feed_item.assert_called_once()
                self.assertNotIn("last_clean_hash", entry)

    def test_final_turn_sends_once_across_two_syncs(self) -> None:
        state, pane, _pane_key, entry, counters, caps = self._state()
        item = self._turn_item("turn-final")
        send_feed_item = Mock(return_value={"ok": True, "message_id": "2001"})

        first = self._sync_item(state, pane, counters, caps, item, send_feed_item)
        second = self._sync_item(state, pane, counters, caps, item, send_feed_item)

        self.assertTrue(first)
        self.assertFalse(second)
        send_feed_item.assert_called_once()
        self.assertEqual(entry["last_clean_message_id"], "2001")


class VisibleFallbackReliabilityTests(DeliveryReliabilityBase):
    def _visible_question(self, text: str, *, key: str = "prompt") -> dict:
        item = {
            "kind": "question",
            "title": "Input needed",
            "text": text,
            "turn_id": f"visible-readonly-question:{key}",
            "choice_source": "visible_readonly",
            "notify": True,
        }
        herdres.attach_visible_prompt_key(item)
        return item

    def test_stale_visible_text_equal_or_subsumed_by_recent_delivery_is_suppressed(self) -> None:
        cases = [
            ("last_clean_text", "Should I deploy now?", "Should I deploy now?"),
            ("last_prompt_text", "Please choose whether to deploy now or wait.", "deploy now or wait"),
            ("last_stream_text", "Deploy now?", "Context from the pane\n\nDeploy now?"),
        ]

        for field, previous, visible_text in cases:
            with self.subTest(field=field):
                pane = self._pane(status="blocked")
                visible = self._visible_question(visible_text, key=field)
                visible_reader = Mock(return_value=visible)
                with patch.multiple(
                    herdres,
                    pane_turn=Mock(return_value={"available": False, "reason": "no_completed_turn"}),
                    extract_visible_readonly_feed_item=visible_reader,
                    VISIBLE_CHOICE_BUTTONS_ENABLED=False,
                    VISIBLE_READONLY_PROMPTS_ENABLED=True,
                ):
                    item = herdres.extract_turn_feed_item(pane, {field: previous})

                visible_reader.assert_called_once()
                self.assertIsNone(item)

    def test_genuine_blocked_awaiting_visible_prompt_posts_once(self) -> None:
        state, pane, _pane_key, entry, counters, caps = self._state(status="blocked")
        turn = {
            "available": True,
            "complete": True,
            "has_open_turn": True,
            "awaiting_input": True,
            "turn_id": "completed-before-question",
            "open_turn_id": "awaiting-visible-question",
            "assistant_final_text": "I need one decision before continuing.",
        }
        visible = self._visible_question("Should I deploy now?", key="deploy")
        send_feed_item = Mock(return_value={"ok": True, "message_id": "3001"})
        space = state["spaces"][herdres.space_key(pane)]

        with patch.object(herdres.time, "sleep", Mock()), patch.multiple(
            herdres,
            ensure_space_topic=Mock(return_value=(space, False)),
            ensure_pane_root_message=Mock(return_value=(False, {"ok": True})),
            topic_verify_due=Mock(return_value=False),
            pane_turn=Mock(return_value=turn),
            extract_visible_readonly_feed_item=Mock(return_value=visible),
            VISIBLE_CHOICE_BUTTONS_ENABLED=False,
            VISIBLE_READONLY_PROMPTS_ENABLED=True,
            send_pending_prompt_message=Mock(return_value={"changed": False, "topic_missing": False, "pane_root_missing": False}),
            send_feed_item=send_feed_item,
            save_state=Mock(),
            apply_api_error_warning=Mock(return_value={"topic_missing": False, "changed": False}),
            fold_superseded_turns=Mock(return_value=False),
            flush_pending_plan_doc=Mock(return_value=False),
            flush_pending_speech_reply=Mock(return_value=False),
            CLEAN_FEED_ENABLED=True,
            TURN_FEED_ENABLED=True,
            LIVE_CARD_ENABLED=False,
            STATUS_MARKER_ENABLED=False,
            STATUS_ICON_ENABLED=False,
        ):
            first = herdres.sync_pane_once(state, "-1001", state["telegram"], pane, counters, caps)
            herdres.clear_sync_caches()
            second = herdres.sync_pane_once(state, "-1001", state["telegram"], pane, counters, caps)

        self.assertTrue(first)
        self.assertFalse(second)
        send_feed_item.assert_called_once()
        self.assertEqual(entry["last_clean_message_id"], "3001")
        self.assertEqual(entry["last_visible_prompt_key"], visible["visible_prompt_key"])

    def test_visible_prompt_key_uses_normalized_prompt_and_options(self) -> None:
        first = {
            "kind": "choices",
            "summary": "  Should I deploy now?  ",
            "detail": herdres.visible_readonly_prompt_note(),
            "options": [{"number": "1", "label": "Yes, deploy"}, {"number": "2", "label": "Wait"}],
        }
        second = {
            "kind": "choices",
            "summary": "should   i   deploy now?",
            "options": [{"number": "1", "label": "yes,   deploy"}, {"number": "2", "label": "wait"}],
        }

        self.assertEqual(herdres.visible_prompt_key(first), herdres.visible_prompt_key(second))

    def test_active_panes_never_scrape_visible_fallback(self) -> None:
        for status in sorted(herdres.ACTIVE_AGENT_STATUSES):
            with self.subTest(status=status):
                pane = self._pane(status=status)
                choice_reader = Mock(return_value=self._visible_question("Pick one", key=status))
                readonly_reader = Mock(return_value=self._visible_question("Should I continue?", key=status))
                with patch.multiple(
                    herdres,
                    pane_turn=Mock(return_value={"available": False, "reason": "no_completed_turn"}),
                    extract_visible_choice_feed_item=choice_reader,
                    extract_visible_readonly_feed_item=readonly_reader,
                    VISIBLE_CHOICE_BUTTONS_ENABLED=True,
                    VISIBLE_READONLY_PROMPTS_ENABLED=True,
                ):
                    item = herdres.extract_turn_feed_item(pane, {})

                self.assertIsNone(item)
                choice_reader.assert_not_called()
                readonly_reader.assert_not_called()


class ContentIdentityReliabilityTests(DeliveryReliabilityBase):
    def test_same_delivered_content_matches_legacy_partial_and_council_items(self) -> None:
        current = self._turn_item("volatile-current", answer="Stable answer.")
        legacy_text = herdres.item_plain_text(current)
        partial_previous = self._turn_item("previous-partial", answer="Stable answer.")
        council_previous = self._turn_item("gitmoot:root/delegation/d1@old:abc", answer="Council answer.", prompt="")
        council_current = self._turn_item("gitmoot:root/delegation/d1@new:def", answer="Council answer.", prompt="")
        council_current["_council_job_ref"] = "root/delegation/d1:def"

        cases = [
            ({"last_clean_text": legacy_text, "last_turn_id": "legacy-turn-id"}, current),
            ({"last_clean_item": partial_previous}, current),
            ({"last_clean_item": council_previous, "last_council_job_ref": "root/delegation/d1:abc"}, council_current),
        ]

        for entry, item in cases:
            with self.subTest(entry=entry):
                semantic_hash = herdres.clean_feed_hash(item, include_render_version=False)
                self.assertTrue(herdres.same_delivered_content(entry, item, semantic_hash))

    def test_completed_turn_volatile_identity_does_not_resend_but_content_change_does(self) -> None:
        state, pane, _pane_key, entry, counters, caps = self._state()
        first = self._turn_item("volatile-turn-a", answer="The stable answer.")
        same_content = self._turn_item("volatile-turn-b", answer="The stable answer.")
        same_content["updated_at"] = "2026-06-26T10:00:00Z"
        same_content["completed_at"] = "2026-06-26T10:01:00Z"
        same_content["partial"] = {"revision": "old adapter state"}
        changed_content = self._turn_item("volatile-turn-c", answer="The changed answer.")
        send_feed_item = Mock(return_value={"ok": True, "message_id": "2001"})

        self._sync_item(state, pane, counters, caps, first, send_feed_item)
        self._sync_item(state, pane, counters, caps, same_content, send_feed_item)
        self._sync_item(state, pane, counters, caps, changed_content, send_feed_item)

        self.assertEqual(send_feed_item.call_count, 2)
        self.assertIn("The changed answer.", entry["last_clean_text"])
        self.assertEqual(entry["last_turn_id"], "volatile-turn-c")

    def test_visible_scrape_delivery_does_not_replace_real_turn_cursor(self) -> None:
        entry = {"last_turn_id": "real-turn-1"}
        visible = {
            "kind": "question",
            "title": "Input needed",
            "text": "Should I continue?",
            "turn_id": "visible-readonly-question:prompt",
            "choice_source": "visible_readonly",
        }
        herdres.attach_visible_prompt_key(visible)

        herdres.record_delivered_feed_item(
            entry,
            visible,
            {"ok": True, "message_id": "3001"},
            pending_active_prompt=None,
            clear_active_prompt=True,
        )

        self.assertEqual(entry["last_turn_id"], "real-turn-1")
        self.assertEqual(entry["last_visible_prompt_key"], visible["visible_prompt_key"])

        real = self._turn_item("real-turn-2", answer="Finished after the prompt.")
        herdres.record_delivered_feed_item(
            entry,
            real,
            {"ok": True, "message_id": "3002"},
            pending_active_prompt=None,
            clear_active_prompt=False,
        )

        self.assertEqual(entry["last_turn_id"], "real-turn-2")


if __name__ == "__main__":
    unittest.main()
