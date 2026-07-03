from __future__ import annotations

import unittest
from unittest.mock import Mock

from herdres_connector import formatter, source_state, telegram_delivery


def _sanitize(text: str, limit: int = 300) -> str:
    return str(text)[:limit]


def _outbox_item() -> dict:
    return {
        "ref": "opaque-ref-1",
        "key": "attention-job-1",
        "attempt": 1,
        "payload": {
            "schema_version": 1,
            "event_type": "attention_created",
            "attention": {
                "kind": "worker_needs_attention",
                "severity": "warning",
                "status": "blocked",
                "reason": "needs input",
                "signal_count": 1,
                "last_changed_at": "2026-07-01T00:00:00+00:00",
            },
            "transition_at": "2026-07-01T00:00:00+00:00",
        },
    }


class HerdresConnectorExtractionTests(unittest.TestCase):
    def test_attention_formatter_keeps_existing_notice_shape(self) -> None:
        payload = _outbox_item()["payload"]

        plain = formatter.attention_notice_text(payload, sanitize=_sanitize)
        rich = formatter.attention_notice_html(payload, sanitize=_sanitize)

        self.assertIn("Tendwire attention", plain)
        self.assertIn("Severity: warning", plain)
        self.assertIn("<h3>Tendwire attention</h3>", rich)
        self.assertIn("<b>Severity</b>: warning", rich)

    def test_source_state_suppresses_already_delivered_source_turn(self) -> None:
        entry = {
            "last_turn_id": "turn-1",
            "last_clean_message_id": "501",
            "last_clean_kind": "turn",
        }
        item = {"kind": "turn", "turn_id": "turn-1", "assistant_final_text": "done"}
        reasons: list[str] = []
        runtime = source_state.SourceTurnRuntime(
            delivery_seen=Mock(return_value=False),
            record_suppressed=lambda _entry, _item, reason: reasons.append(reason),
            record_identity=Mock(return_value=True),
            note_delivery=Mock(return_value=True),
        )

        suppressed = source_state.suppress_globally_delivered_turn(
            {"panes": {}},
            {"worker_id": "worker-1"},
            entry,
            item,
            runtime=runtime,
        )

        self.assertTrue(suppressed)
        self.assertEqual(reasons, ["source_turn_already_delivered"])

    def test_connector_drain_delivers_and_acks_without_storing_opaque_ref(self) -> None:
        item = _outbox_item()
        state = {"telegram": {}, "panes": {}}
        telegram = {"general_thread_id": "1"}
        counters = {"sends": 0}
        connector_calls: list[str] = []

        def connector_call(action: str, params: dict | None = None) -> dict:
            connector_calls.append(action)
            if action == "poll":
                return {"ok": True, "items": [item]}
            if action == "ack":
                return {"ok": True}
            return {"ok": False}

        send_rich_message = Mock(return_value={"ok": True, "message_id": "55"})
        runtime = telegram_delivery.TelegramDeliveryRuntime(
            sanitize=_sanitize,
            now=lambda: "2026-07-01T00:00:00+00:00",
            send_rich_message=send_rich_message,
            pane_root_reply_target=Mock(return_value=None),
            managed_bot_token_for_entry=Mock(return_value=None),
            connector_call=connector_call,
        )

        result = telegram_delivery.drain_connector_outbox(
            state,
            "-100",
            telegram,
            counters,
            runtime=runtime,
            max_sends=8,
            default_general_thread_id="1",
            enabled=True,
            delivery_configured=True,
        )

        self.assertEqual(connector_calls, ["poll", "ack"])
        self.assertEqual(result["delivered"], 1)
        self.assertEqual(result["acked"], 1)
        self.assertEqual(counters["sends"], 1)
        self.assertEqual(send_rich_message.call_args.kwargs["thread_id"], "1")
        self.assertNotIn("opaque-ref-1", str(state))


if __name__ == "__main__":
    unittest.main()

