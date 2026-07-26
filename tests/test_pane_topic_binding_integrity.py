"""Production-shaped regressions for pane/topic binding integrity."""

from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timezone

import pytest

from herdres_connector import state
from herdres_connector.source_sync import SyncRuntime, sync_once

from test_source_only import FakeTelegram, FakeTendwire, _store


STABLE_KEY = "wsk1_" + "d" * 64


@pytest.fixture(autouse=True)
def _source_worker_mode(monkeypatch):
    monkeypatch.setenv("HERDRES_TENDWIRE_MODE", "source")
    monkeypatch.setenv("HERDRES_SOURCE_TOPIC_MODE", "worker")
    monkeypatch.setenv("HERDRES_PINNED_STATUS", "0")
    monkeypatch.setenv("HERDR_TELEGRAM_TOPICS_STATUS_ICON", "0")
    monkeypatch.setenv("HERDR_TELEGRAM_TOPICS_RICH_MESSAGES", "0")


def _worker(
    worker_id: str = "claude-live",
    *,
    fingerprint: str = "fp-live",
) -> dict:
    return {
        "id": worker_id,
        "name": "Claude",
        "status": "working",
        "space_id": "workspace-1",
        "fingerprint": fingerprint,
        "last_seen_at": datetime.now(timezone.utc).isoformat(),
        "meta": {
            "agent": "claude",
            "label": "discovery-calls",
            "cwd": "/root/herdres",
            "foreground_cwd": "/root/herdres",
            "stable_key": STABLE_KEY,
            "stable_key_version": 1,
        },
    }


def _final(worker_id: str = "claude-live") -> dict:
    return {
        "id": "turn-next-final",
        "worker_id": worker_id,
        "space_id": "workspace-1",
        "complete": True,
        "user_text": "Finish the pane/topic repair.",
        "assistant_final_text": "Pane/topic binding repaired.",
    }


def _entry(
    store: dict,
    *,
    worker_id: str,
    fingerprint: str,
    topic_id: str | None,
) -> tuple[str, dict]:
    key, entry, _created = state.upsert_worker_entry(
        store,
        _worker(worker_id, fingerprint=fingerprint),
        topic_id=topic_id or "",
    )
    return key, entry


def _duplicate_entry(
    store: dict,
    source: dict,
    *,
    key: str,
    fingerprint: str,
    topic_id: str | None,
) -> tuple[str, dict]:
    duplicate = deepcopy(source)
    duplicate["tendwire_fingerprint"] = fingerprint
    if topic_id is None:
        duplicate.pop("topic_id", None)
    else:
        duplicate["topic_id"] = topic_id
    store["panes"][key] = duplicate
    return key, duplicate


class DeletedTopicTelegram(FakeTelegram):
    def __init__(self, dead_topic_id: str):
        super().__init__()
        self.dead_topic_id = dead_topic_id
        self.failed_topic_sends = 0

    def send_message(self, chat_id, html, **kwargs):
        if str(kwargs.get("thread_id") or "") == self.dead_topic_id:
            self.failed_topic_sends += 1
            return {
                "ok": False,
                "error": "Bad Request: message thread not found",
            }
        return super().send_message(chat_id, html, **kwargs)


def test_deleted_live_topic_is_reminted_and_next_final_delivered_once():
    store = _store()
    _key, entry = _entry(
        store,
        worker_id="claude-live",
        fingerprint="fp-live",
        topic_id="15007",
    )
    tendwire = FakeTendwire(
        workers=[_worker()],
        turns={"turns": [_final()]},
        spaces=[],
    )
    telegram = DeletedTopicTelegram("15007")
    runtime = SyncRuntime(tendwire, telegram, with_outbox=False)

    first = sync_once(store, runtime)

    assert first["feed_sent"] == 0
    assert telegram.failed_topic_sends == 1
    assert "topic_id" not in entry
    audit = store["telegram_topic_binding_audit"]
    assert audit[-1]["topic_id"] == "15007"
    assert audit[-1]["reason"] == "live_delivery_topic_gone"
    assert audit[-1]["error_kind"] == "topic_not_found"

    second = sync_once(store, runtime)
    replacement_topic_id = entry["topic_id"]

    assert second["feed_sent"] == 1
    assert replacement_topic_id != "15007"
    assert telegram.topics == ["discovery-calls"]
    assert entry["last_topic_recovery"]["replacement_topic_id"] == (
        replacement_topic_id
    )
    successful_finals = [
        sent
        for sent in telegram.sent
        if sent[2].get("thread_id") == replacement_topic_id
        and "Pane/topic binding repaired." in sent[1]
    ]
    assert len(successful_finals) == 1

    state_after_delivery = deepcopy(store)
    third = sync_once(store, runtime)

    assert third["feed_sent"] == 0
    assert store == state_after_delivery
    assert (
        len(
            [
                sent
                for sent in telegram.sent
                if sent[2].get("thread_id") == replacement_topic_id
                and "Pane/topic binding repaired." in sent[1]
            ]
        )
        == 1
    )
    assert telegram.failed_topic_sends == 1


def test_deleted_topic_defers_durable_final_until_remint_then_acks_once():
    from test_turn_final_delivery import TurnFinalTendwire, _runtime, _turn_row

    row = _turn_row(
        "turn-durable-remint",
        "twrev1.durable_remint",
        "Durable final delivered after remint.",
        user="Recover the deleted topic.",
    )
    tendwire = TurnFinalTendwire(row)
    store = _store()
    worker = tendwire.snapshot()["workers"][0]
    _key, entry, _created = state.upsert_worker_entry(
        store, worker, topic_id="15007"
    )
    telegram = DeletedTopicTelegram("15007")

    first = sync_once(store, _runtime(tendwire, telegram))

    assert first["tendwire_turn_final"]["deferred"] == 1
    assert tendwire.defer_calls == [
        ("twref1.lease1", "transient_delivery")
    ]
    assert tendwire.ack_calls == []
    assert "topic_id" not in entry

    second = sync_once(store, _runtime(tendwire, telegram))
    replacement_topic_id = entry["topic_id"]

    assert second["tendwire_turn_final"]["acked"] == 1
    assert len(tendwire.ack_calls) == 1
    assert replacement_topic_id != "15007"
    assert (
        len(
            [
                sent
                for sent in telegram.sent
                if sent[2].get("thread_id") == replacement_topic_id
                and "Durable final delivered after remint." in sent[1]
            ]
        )
        == 1
    )

    third = sync_once(store, _runtime(tendwire, telegram))

    assert third["tendwire_turn_final"]["polled"] == 0
    assert len(tendwire.ack_calls) == 1


def test_one_topic_duplicate_live_claim_retires_loser_and_restores_route():
    store = _store()
    survivor_key, survivor = _entry(
        store,
        worker_id="claude-live",
        fingerprint="fp-topic",
        topic_id="16756",
    )
    loser_key, loser = _duplicate_entry(
        store,
        survivor,
        key="worker:claude-live:topicless-duplicate",
        fingerprint="fp-topicless",
        topic_id=None,
    )
    for key, entry in ((survivor_key, survivor), (loser_key, loser)):
        entry["tendwire_stable_key"] = STABLE_KEY
        entry["tendwire_stable_key_version"] = 1
        state.quarantine_worker_entry(
            store, key, reason="preflight_stable_key_conflict"
        )
    worker = _worker("claude-live", fingerprint="fp-topic")
    telegram = FakeTelegram()

    result = sync_once(
        store,
        SyncRuntime(
            FakeTendwire(
                workers=[worker],
                turns={"turns": [_final("claude-live")]},
                spaces=[],
            ),
            telegram,
            with_outbox=False,
        ),
    )

    assert result["feed_sent"] == 1
    assert survivor["topic_id"] == "16756"
    assert not state.entry_is_quarantined(survivor)
    assert state.worker_entry_is_uniquely_routable(
        store, survivor_key, survivor
    )
    assert state.entry_is_retired(loser)
    assert loser["routing_retired_reason"] == "duplicate_live_claim"
    assert state.entry_stable_identity(loser) is None
    assert state.find_worker_entry_by_stable_key(store, STABLE_KEY) == (
        survivor_key,
        survivor,
    )


def test_recent_distinct_topic_claims_remain_fail_closed():
    store = _store()
    first_key, first = _entry(
        store,
        worker_id="claude-live",
        fingerprint="fp-a",
        topic_id="16756",
    )
    second_key, second = _duplicate_entry(
        store,
        first,
        key="worker:claude-live:second-topic",
        fingerprint="fp-b",
        topic_id="16910",
    )
    recent = datetime.now(timezone.utc).isoformat()
    for key, entry in ((first_key, first), (second_key, second)):
        entry["tendwire_stable_key"] = STABLE_KEY
        entry["tendwire_stable_key_version"] = 1
        entry["tendwire_last_seen_at"] = recent
        state.quarantine_worker_entry(
            store, key, reason="preflight_stable_key_conflict"
        )
    telegram = FakeTelegram()

    result = sync_once(
        store,
        SyncRuntime(
            FakeTendwire(
                workers=[_worker("claude-live", fingerprint="fp-a")],
                turns={"turns": [_final("claude-live")]},
                spaces=[],
            ),
            telegram,
            with_outbox=False,
        ),
    )

    assert result["feed_sent"] == 0
    assert not state.entry_is_retired(first)
    assert not state.entry_is_retired(second)
    assert state.entry_is_quarantined(first)
    assert state.entry_is_quarantined(second)
    assert state.find_worker_entry_by_stable_key(store, STABLE_KEY) == (
        None,
        None,
    )
    assert telegram.sent == []
    assert telegram.topics == []


def test_healthy_noop_sync_pass_does_not_write_state():
    store = _store()
    _entry(
        store,
        worker_id="claude-live",
        fingerprint="fp-live",
        topic_id="16756",
    )
    runtime = SyncRuntime(
        FakeTendwire(
            workers=[_worker()],
            turns={"turns": []},
            spaces=[],
        ),
        FakeTelegram(),
        with_outbox=False,
    )
    sync_once(store, runtime)
    sync_once(store, runtime)
    before = deepcopy(store)

    result = sync_once(store, runtime)

    assert result["changed"] is False
    assert store == before
