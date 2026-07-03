from __future__ import annotations

import json
import os
import unittest
from unittest.mock import Mock, patch

import herdres
from conftest import make_pane
from herdres_connector import formatter


def _sanitize(text: str, limit: int = 300) -> str:
    return str(text)[:limit]


class SourceV2TelegramLayoutTests(unittest.TestCase):
    def source_v2_env(self, **extra: str):
        env = {"HERDRES_TELEGRAM_LAYOUT": "source_v2"}
        env.update(extra)
        return patch.dict(os.environ, env, clear=False)

    def test_multiple_spaces_render_as_separate_status_boards(self) -> None:
        alpha = make_pane(
            "Codex",
            "working",
            agent="codex",
            source="tendwire",
            entry_type="worker",
            worker_id="worker-alpha-codex",
            space_id="alpha",
        )
        beta = make_pane(
            "Claude",
            "idle",
            agent="claude",
            source="tendwire",
            entry_type="worker",
            worker_id="worker-beta-claude",
            space_id="beta",
        )
        grouped = herdres.open_panes_by_space([alpha, beta])

        with self.source_v2_env():
            alpha_board = herdres.render_pinned_status({"panes": {}}, grouped["space:alpha"])
            beta_board = herdres.render_pinned_status({"panes": {}}, grouped["space:beta"])

        self.assertIn("Codex 🟡", alpha_board)
        self.assertNotIn("Claude", alpha_board)
        self.assertIn("Claude 🟢", beta_board)
        self.assertNotIn("Codex", beta_board)

    def test_multiple_workers_per_space_render_as_compact_board_rows(self) -> None:
        codex = make_pane("Codex", "idle", agent="codex", space_id="alpha")
        claude = make_pane("Claude", "working", agent="claude", space_id="alpha")
        kimi = make_pane("Kimi", "blocked", agent="kimi", space_id="alpha")

        with self.source_v2_env():
            board = herdres.render_pinned_status({"panes": {}}, [codex, claude, kimi])

        self.assertEqual(board.splitlines(), ["Kimi 🔴", "Claude 🟡", "Codex 🟢"])

    def test_source_v2_enables_per_space_status_board_without_global_dashboard(self) -> None:
        pane = make_pane("Codex", "working", agent="codex", space_id="alpha")
        state = {
            "spaces": {"space:alpha": {"space_key": "space:alpha", "topic_id": "77"}},
            "panes": {},
        }
        send_status = Mock(return_value={"ok": True, "message_id": "501"})
        pin_status = Mock(return_value={"ok": True})

        with self.source_v2_env(HERDR_TELEGRAM_TOPICS_PINNED_STATUS="0"), patch.multiple(
            herdres,
            PINNED_STATUS_ENABLED=False,
            send_legacy_message_result=send_status,
            pin_chat_message=pin_status,
        ):
            result = herdres.sync_space_pinned_statuses(
                state,
                "-1001",
                [pane],
                {"sends": 0},
                8,
            )

        self.assertTrue(result["changed"])
        self.assertEqual(result["updated"], 1)
        send_status.assert_called_once()
        self.assertEqual(send_status.call_args.args[:2], ("-1001", "Codex 🟡"))
        self.assertEqual(send_status.call_args.kwargs["thread_id"], "77")
        pin_status.assert_called_once_with("-1001", "501")

    def test_source_v2_enables_edited_live_card_per_worker(self) -> None:
        pane = make_pane("Codex", "working", agent="codex", workspace_id="alpha", tab_id="tab-alpha")
        state = {"version": 1, "telegram": {"chat_id": "-1001"}, "spaces": {}, "panes": {}}
        counters = {"creates": 0, "sends": 0, "feed_sends": 0, "marker_sends": 0, "verifies": 0, "renames": 0}
        caps = {"max_creates": 5, "max_sends": 8, "max_feed_sends": 0, "max_marker_sends": 0, "max_verifies": 0}
        update_live_card = Mock(return_value={"ok": True, "attempted": True, "message_id": "601"})

        with self.source_v2_env(), patch.multiple(
            herdres,
            LIVE_CARD_ENABLED=False,
            STATUS_MARKER_ENABLED=False,
            TURN_FEED_ENABLED=False,
            CLEAN_FEED_ENABLED=False,
            STATUS_ICON_ENABLED=False,
            create_topic=Mock(return_value="77"),
            update_live_card=update_live_card,
            save_state=Mock(),
            apply_api_error_warning=Mock(return_value={"topic_missing": False, "changed": False}),
        ):
            changed = herdres.sync_pane_once(
                state,
                "-1001",
                state["telegram"],
                pane,
                counters,
                caps,
                turn_only=True,
            )

        self.assertTrue(changed)
        update_live_card.assert_called_once()
        key = herdres.pane_key(pane)
        self.assertEqual(state["panes"][key]["card_status_hash"], herdres.clean_feed_hash(herdres.live_status_item(pane)))
        self.assertEqual(state["panes"][key]["last_pane_message_id"], "601")

    def test_working_update_is_compact_and_expandable(self) -> None:
        item = {
            "kind": "turn",
            "turn_id": "turn-1",
            "user_text": "Check the release status",
            "worklog_label": herdres.WORKING_LABEL,
            "worklog_text": "Reading status files\nRunning focused checks\nPreparing the next update",
            "assistant_final_text": "",
        }

        with self.source_v2_env():
            html = herdres.render_feed_item_html(item)

        self.assertIn("<summary><small><b>Working…</b> Reading status files</small></summary>", html)
        self.assertIn("<details", html)
        self.assertIn("Preparing the next update", html)
        self.assertNotIn("<summary><b>Response</b>", html)

    def test_completed_final_response_stays_expanded(self) -> None:
        item = {
            "kind": "turn",
            "turn_id": "turn-2",
            "user_text": "Summarize the result",
            "worklog_label": herdres.WORKLOG_LABEL,
            "worklog_text": "Checked the source-mode path",
            "assistant_final_text": "Done.\n\nTests pass and the source-mode connector is ready.",
        }

        with self.source_v2_env():
            html = herdres.render_feed_item_html(item)

        self.assertIn("<details open><summary><b>Response</b></summary>", html)
        self.assertIn("Tests pass", html)
        self.assertNotIn("<summary><small><b>Working…</b>", html)

    def test_attention_and_pending_items_are_highlighted(self) -> None:
        attention_payload = {
            "event_type": "attention_created",
            "attention": {
                "kind": "worker_needs_attention",
                "severity": "warning",
                "status": "blocked",
                "reason": "needs input",
            },
        }
        decision = {
            "kind": "decision",
            "summary": "Pick the release tag.",
            "options": [{"label": "RC", "description": "Tag the release candidate."}],
        }
        interaction = {
            "kind": "interaction_readonly",
            "summary": "Need manual confirmation.",
            "questions": [{"question_id": "q1", "title": "Confirm target", "options": []}],
        }

        rich_attention = formatter.attention_notice_html(
            attention_payload,
            sanitize=_sanitize,
            layout="source_v2",
        )
        with self.source_v2_env():
            wrapped_attention = herdres.tendwire_attention_notice_html(attention_payload)
            decision_html = herdres.render_feed_item_html(decision)
            interaction_html = herdres.render_feed_item_html(interaction)

        self.assertIn("<h3>⚠️ Tendwire attention</h3>", rich_attention)
        self.assertIn("<h3>⚠️ Tendwire attention</h3>", wrapped_attention)
        self.assertIn("<h3>⚠️ Decision needed</h3>", decision_html)
        self.assertIn("<h3>⚠️ Input needed</h3>", interaction_html)

    def test_duplicate_same_turn_id_reuses_existing_anchor(self) -> None:
        entry = {
            "last_stream_message_id": "700",
            "last_stream_turn_id": "turn-3",
            "last_pane_message_id": "700",
            "last_turn_id": "turn-3",
            "last_clean_message_id": "700",
            "last_clean_kind": "turn",
        }
        item = {"kind": "turn", "turn_id": "turn-3", "assistant_final_text": "Final text"}

        with self.source_v2_env():
            self.assertEqual(herdres.turn_visible_anchor_message_id(entry, "turn-3"), "700")
            self.assertTrue(herdres.source_turn_already_clean_delivered(entry, item))

    def test_topic_cleanup_report_is_dry_run_before_any_delete(self) -> None:
        state = {
            "panes": {
                "pseudo": {
                    "source": "tendwire",
                    "pane_id": "tendwire:worker-1",
                    "topic_id": "88",
                },
            },
            "spaces": {
                "space:old": {
                    "space_key": "space:old",
                    "topic_id": "99",
                    "pane_keys": [],
                },
            },
            "telegram": {},
        }
        delete_topic = Mock(return_value=True)

        with patch.object(herdres, "load_dotenv"), patch.object(herdres, "load_state", return_value=state), patch.object(
            herdres, "delete_topic", delete_topic
        ), patch.object(herdres, "save_state") as save_state:
            report = herdres.topic_cleanup_report_once()

        self.assertTrue(report["dry_run"])
        self.assertTrue(report["would_change"])
        encoded = json.dumps(report, sort_keys=True)
        self.assertNotIn('"88"', encoded)
        self.assertNotIn('"99"', encoded)
        delete_topic.assert_not_called()
        save_state.assert_not_called()


if __name__ == "__main__":
    unittest.main()
