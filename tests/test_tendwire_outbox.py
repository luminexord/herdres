from __future__ import annotations

import json
import os
import subprocess
import unittest
from unittest.mock import Mock, patch

import herdres
import herdres_tendwire


def _completed(payload: dict, returncode: int = 0, stderr: str = "") -> subprocess.CompletedProcess[str]:
    return subprocess.CompletedProcess(["tendwire"], returncode, stdout=json.dumps(payload), stderr=stderr)


def _item(**extra) -> dict:
    item = {
        "ref": "twref1.safeopaque",
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
    item.update(extra)
    return item


def _state() -> dict:
    return {
        "version": 1,
        "enabled": True,
        "telegram": {
            "chat_id": "-1001",
            "general_thread_id": "1",
            "owner_user_ids": ["42"],
        },
        "spaces": {},
        "panes": {},
    }


class TendwireOutboxTests(unittest.TestCase):
    def test_tendwire_outbox_helpers_normalize_payload_and_identity(self) -> None:
        item = _item()
        payload = herdres_tendwire.outbox_item_payload(item)

        self.assertEqual(payload["event_type"], "attention_created")
        self.assertEqual(herdres_tendwire.outbox_event_type(payload), "attention_created")
        self.assertEqual(herdres_tendwire.outbox_event_type({}), "attention")
        self.assertEqual(herdres_tendwire.outbox_item_payload({"payload": "not-public-json"}), {})
        self.assertEqual(herdres.tendwire_outbox_item_identity(item), herdres_tendwire.outbox_item_identity(item))
        self.assertEqual(herdres_tendwire.outbox_item_identity(item), herdres_tendwire.outbox_item_identity(dict(item)))

    def test_tendwire_outbox_audit_state_helpers_live_in_tendwire_module(self) -> None:
        state = _state()
        herdres_tendwire.note_outbox_audit(
            state,
            {"identity": "abc123", "status": "delivered"},
            checked_at="2026-07-02T00:00:00+00:00",
        )
        herdres_tendwire.note_outbox_audit(
            state,
            {"identity": "abc123", "status": "delivered"},
            checked_at="2026-07-02T00:01:00+00:00",
        )
        herdres_tendwire.note_outbox_audit(
            state,
            {"identity": "retrying", "status": "failed"},
            checked_at="2026-07-02T00:02:00+00:00",
        )

        self.assertEqual(state["tendwire_outbox"]["last_checked_at"], "2026-07-02T00:02:00+00:00")
        self.assertEqual(state["tendwire_outbox"]["delivered_identities"], ["abc123"])
        self.assertEqual(herdres_tendwire.outbox_delivered_identities(state), {"abc123"})
        self.assertEqual(herdres.tendwire_outbox_delivered_identities(state), {"abc123"})

    def test_drain_posts_attention_and_acks_public_response(self) -> None:
        calls: list[list[str]] = []
        ack_responses: list[dict] = []

        def run_cmd(args: list[str], **_kwargs):
            calls.append(args)
            if "poll" in args:
                return _completed({"ok": True, "items": [_item()]})
            if "ack" in args:
                ack_responses.append(json.loads(args[args.index("--response-json") + 1]))
                return _completed({"ok": True, "status": "acknowledged"})
            return _completed({"ok": False, "status": "unexpected"})

        state = _state()
        counters = herdres.make_sync_counters()
        with patch.dict(os.environ, {"TELEGRAM_BOT_TOKEN": "123:token"}, clear=True), \
                patch.object(herdres, "run_cmd", side_effect=run_cmd), \
                patch.object(herdres, "send_rich_message", return_value={"ok": True, "format": "rich", "message_id": "55"}) as send_rich:
            result = herdres.drain_tendwire_connector_outbox(
                state,
                "-1001",
                state["telegram"],
                counters,
                max_sends=8,
                enabled=True,
                limit=1,
            )

        self.assertEqual(result["delivered"], 1)
        self.assertEqual(result["acked"], 1)
        self.assertEqual(counters["sends"], 1)
        self.assertIn("poll", calls[0])
        self.assertIn("--lease-seconds", calls[0])
        self.assertEqual(send_rich.call_args.args[0], "-1001")
        self.assertEqual(send_rich.call_args.kwargs["thread_id"], "1")
        self.assertIn("Tendwire attention", send_rich.call_args.args[1])
        self.assertEqual(ack_responses, [{"event_type": "attention_created", "sent": True}])
        encoded_state = json.dumps(state, sort_keys=True).lower()
        self.assertNotIn("twref1.safeopaque", encoded_state)
        self.assertNotIn("message_id", json.dumps(ack_responses, sort_keys=True).lower())

    def test_drain_does_not_lease_when_telegram_is_unconfigured(self) -> None:
        state = _state()
        counters = herdres.make_sync_counters()
        with patch.dict(os.environ, {}, clear=True), patch.object(herdres, "run_cmd") as run_cmd:
            result = herdres.drain_tendwire_connector_outbox(
                state,
                "",
                state["telegram"],
                counters,
                max_sends=8,
                enabled=True,
            )

        self.assertEqual(result["status"], "telegram_unconfigured")
        run_cmd.assert_not_called()
        self.assertEqual(counters["sends"], 0)

    def test_drain_deduplicates_previously_delivered_item_before_ack(self) -> None:
        item = _item()
        identity = herdres.tendwire_outbox_item_identity(item)
        state = _state()
        state["tendwire_outbox"] = {"delivered_identities": [identity]}
        ack_responses: list[dict] = []

        def run_cmd(args: list[str], **_kwargs):
            if "poll" in args:
                return _completed({"ok": True, "items": [item]})
            if "ack" in args:
                ack_responses.append(json.loads(args[args.index("--response-json") + 1]))
                return _completed({"ok": True, "status": "acknowledged"})
            return _completed({"ok": False, "status": "unexpected"})

        counters = herdres.make_sync_counters()
        with patch.dict(os.environ, {"TELEGRAM_BOT_TOKEN": "123:token"}, clear=True), \
                patch.object(herdres, "run_cmd", side_effect=run_cmd), \
                patch.object(herdres, "send_rich_message") as send_rich:
            result = herdres.drain_tendwire_connector_outbox(
                state,
                "-1001",
                state["telegram"],
                counters,
                max_sends=8,
                enabled=True,
            )

        send_rich.assert_not_called()
        self.assertEqual(result["acked"], 1)
        self.assertEqual(counters["sends"], 0)
        self.assertEqual(ack_responses[0]["deduplicated"], True)

    def test_sync_once_drains_enabled_outbox_even_without_live_panes(self) -> None:
        state = _state()

        def connector_call(action: str, params: dict | None = None):
            if action == "poll":
                return {"ok": True, "items": [_item()]}
            if action == "ack":
                return {"ok": True, "status": "acknowledged"}
            return {"ok": False, "status": "unexpected"}

        with patch.dict(os.environ, {"HERDRES_TENDWIRE_CONNECTOR_OUTBOX": "1", "TELEGRAM_BOT_TOKEN": "123:token"}, clear=True), \
                patch.object(herdres, "load_dotenv"), \
                patch.object(herdres, "load_state", return_value=state), \
                patch.object(herdres, "save_state") as save_state, \
                patch.object(herdres, "observed_agent_panes", return_value=[]), \
                patch.object(herdres, "sync_closed_pane_records", return_value={"changed": False, "sent": 0}), \
                patch.object(herdres, "workspace_label_map", return_value={}), \
                patch.object(herdres, "reconcile_known_gone_spaces", return_value=0), \
                patch.object(herdres, "prune_orphan_spaces", return_value=0), \
                patch.object(herdres, "reconcile_pinned_status_views", return_value={"changed": False, "updated": 0}), \
                patch.object(herdres, "tendwire_connector_call", side_effect=connector_call), \
                patch.object(herdres, "send_rich_message", return_value={"ok": True, "message_id": "55"}):
            result = herdres.sync_once()

        self.assertTrue(result["ok"])
        self.assertEqual(result["panes"], 0)
        self.assertEqual(result["sent"], 1)
        self.assertEqual(result["tendwire_outbox"]["delivered"], 1)
        save_state.assert_called()

    def test_sync_once_source_mode_drains_outbox_by_default(self) -> None:
        state = _state()

        def connector_call(action: str, params: dict | None = None):
            if action == "poll":
                return {"ok": True, "items": [_item()]}
            if action == "ack":
                return {"ok": True, "status": "acknowledged"}
            return {"ok": False, "status": "unexpected"}

        with patch.dict(os.environ, {"HERDRES_TENDWIRE_MODE": "source", "TELEGRAM_BOT_TOKEN": "123:token"}, clear=True), \
                patch.object(herdres, "load_dotenv"), \
                patch.object(herdres, "load_state", return_value=state), \
                patch.object(herdres, "save_state") as save_state, \
                patch.object(herdres, "observed_agent_panes", return_value=[]), \
                patch.object(herdres, "sync_closed_pane_records", return_value={"changed": False, "sent": 0}), \
                patch.object(herdres, "reconcile_known_gone_spaces", return_value=0), \
                patch.object(herdres, "prune_orphan_spaces", return_value=0), \
                patch.object(herdres, "reconcile_pinned_status_views", return_value={"changed": False, "updated": 0}), \
                patch.object(herdres, "tendwire_connector_call", side_effect=connector_call), \
                patch.object(herdres, "send_rich_message", return_value={"ok": True, "message_id": "55"}):
            result = herdres.sync_once()

        self.assertTrue(result["ok"])
        self.assertEqual(result["tendwire_outbox"]["enabled"], True)
        self.assertEqual(result["tendwire_outbox"]["delivered"], 1)
        self.assertEqual(result["tendwire_outbox"]["acked"], 1)
        self.assertEqual(result["sent"], 1)
        save_state.assert_called()

    def test_sync_once_source_mode_respects_explicit_outbox_disable(self) -> None:
        state = _state()
        with patch.dict(
            os.environ,
            {"HERDRES_TENDWIRE_MODE": "source", "HERDRES_TENDWIRE_CONNECTOR_OUTBOX": "0", "TELEGRAM_BOT_TOKEN": "123:token"},
            clear=True,
        ), \
                patch.object(herdres, "load_dotenv"), \
                patch.object(herdres, "load_state", return_value=state), \
                patch.object(herdres, "save_state"), \
                patch.object(herdres, "observed_agent_panes", return_value=[]), \
                patch.object(herdres, "sync_closed_pane_records", return_value={"changed": False, "sent": 0}), \
                patch.object(herdres, "reconcile_known_gone_spaces", return_value=0), \
                patch.object(herdres, "prune_orphan_spaces", return_value=0), \
                patch.object(herdres, "reconcile_pinned_status_views", return_value={"changed": False, "updated": 0}), \
                patch.object(herdres, "tendwire_connector_call") as connector_call:
            result = herdres.sync_once()

        self.assertTrue(result["ok"])
        self.assertEqual(result["tendwire_outbox"]["enabled"], False)
        connector_call.assert_not_called()


if __name__ == "__main__":
    unittest.main()
