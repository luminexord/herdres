"""Stable-key delivery across Tendwire worker-generation churn."""

from __future__ import annotations

from copy import deepcopy

import pytest

from herdres_connector import state
from herdres_connector.source_sync import SyncRuntime, sync_once

from test_source_only import FakeTelegram, FakeTendwire, _store
from test_stable_worker_key import KEY_A, _final_turn, _worker


@pytest.fixture(autouse=True)
def _worker_mode(monkeypatch):
    monkeypatch.setenv("HERDRES_TENDWIRE_MODE", "source")
    monkeypatch.setenv("HERDRES_SOURCE_TOPIC_MODE", "worker")
    monkeypatch.setenv("HERDRES_PINNED_STATUS", "0")


def _persist_bound_entry(store: dict, worker_id: str = "gen-A"):
    return state.upsert_worker_entry(
        store,
        _worker(worker_id, KEY_A),
        topic_id="26",
    )[:2]


def _sync(store: dict, workers: list[dict], turns: list[dict]):
    telegram = FakeTelegram()
    result = sync_once(
        store,
        SyncRuntime(
            FakeTendwire(
                workers=workers,
                turns={"turns": turns},
            ),
            telegram,
            with_outbox=False,
        ),
    )
    return result, telegram


def test_same_stable_key_turn_on_new_generation_rebinds_and_delivers():
    store = _store()
    entry_key, _entry = _persist_bound_entry(store)
    latest = _final_turn(
        "gen-B", turn_id="turn-B", text="final from generation B"
    )

    result, telegram = _sync(
        store,
        [_worker("gen-A", KEY_A), _worker("gen-B", KEY_A)],
        [latest],
    )

    entry = state.source_worker_entries(store)[entry_key]
    assert result["worker_rebinds"] == 1
    assert result["feed_sent"] == 1
    assert entry["tendwire_worker_id"] == "gen-B"
    assert entry["last_turn_id"] == "turn-B"
    assert [
        text
        for _chat, text, kwargs, _message_id in telegram.sent
        if kwargs.get("thread_id") == "26"
        and "final from generation B" in text
    ]
    assert store["tendwire_worker_rebind_audit"][-1] == {
        "stable_key": KEY_A,
        "from_worker_id": "gen-A",
        "to_worker_id": "gen-B",
        "reason": "freshest_turn_activity",
        "observed_at": store["tendwire_worker_rebind_audit"][-1][
            "observed_at"
        ],
    }


def test_three_generation_churn_converges_to_generation_with_latest_turn():
    store = _store()
    entry_key, _entry = _persist_bound_entry(store)

    first, _telegram = _sync(
        store,
        [_worker("gen-A", KEY_A), _worker("gen-B", KEY_A)],
        [_final_turn("gen-B", turn_id="turn-B", text="B final")],
    )
    second, telegram = _sync(
        store,
        [
            _worker("gen-A", KEY_A),
            _worker("gen-B", KEY_A),
            _worker("gen-C", KEY_A),
        ],
        [
            _final_turn("gen-C", turn_id="turn-C", text="C final"),
            _final_turn("gen-B", turn_id="turn-B", text="B final"),
            _final_turn("gen-A", turn_id="turn-A", text="A final"),
        ],
    )

    entry = state.source_worker_entries(store)[entry_key]
    assert first["worker_rebinds"] == second["worker_rebinds"] == 1
    assert entry["tendwire_worker_id"] == "gen-C"
    assert entry["last_turn_id"] == "turn-C"
    assert [
        (item["from_worker_id"], item["to_worker_id"])
        for item in store["tendwire_worker_rebind_audit"]
    ] == [("gen-A", "gen-B"), ("gen-B", "gen-C")]
    finals = [
        text
        for _chat, text, kwargs, _message_id in telegram.sent
        if kwargs.get("thread_id") == "26"
    ]
    assert sum("C final" in text for text in finals) == 1
    assert not any("A final" in text for text in finals)


def test_rebind_catchup_delivers_only_newest_completed_turn_from_retired_generation():
    store = _store()
    _entry_key, _entry = _persist_bound_entry(store)
    newest = _final_turn(
        "gen-A", turn_id="turn-new", text="newest completed answer"
    )
    newest.update({"stable_key": KEY_A, "stable_key_version": 1})
    older = _final_turn(
        "gen-A", turn_id="turn-old", text="older completed answer"
    )
    older.update({"stable_key": KEY_A, "stable_key_version": 1})

    result, telegram = _sync(
        store,
        [_worker("gen-B", KEY_A)],
        [newest, older],
    )

    assert result["worker_rebinds"] == 1
    assert result["feed_sent"] == 1
    delivered = "\n".join(text for _chat, text, _kwargs, _mid in telegram.sent)
    assert "newest completed answer" in delivered
    assert "older completed answer" not in delivered


def test_conflicting_live_generation_activity_keeps_binding_and_fails_closed():
    store = _store()
    entry_key, entry = _persist_bound_entry(store)
    before = deepcopy(entry)
    turns = [
        {
            "id": "turn-A",
            "worker_id": "gen-A",
            "assistant_stream_text": "A still working",
            "complete": False,
        },
        {
            "id": "turn-B",
            "worker_id": "gen-B",
            "assistant_stream_text": "B also working",
            "complete": False,
        },
    ]

    result, telegram = _sync(
        store,
        [_worker("gen-A", KEY_A), _worker("gen-B", KEY_A)],
        turns,
    )

    current = state.source_worker_entries(store)[entry_key]
    assert result["worker_rebinds"] == 0
    assert result["feed_sent"] == 0
    assert current["tendwire_worker_id"] == before["tendwire_worker_id"]
    assert state.entry_is_quarantined(current)
    assert (
        current["stable_key_quarantine_reason"]
        == "ambiguous_stable_key_generations"
    )
    assert current["tendwire_worker_generation_ambiguity"][
        "worker_ids"
    ] == ["gen-A", "gen-B"]
    assert "tendwire_worker_rebind_audit" not in store
    assert telegram.sent == []


def test_stable_worker_id_is_noop_without_rebind_audit():
    store = _store()
    entry_key, _entry = _persist_bound_entry(store)

    result, telegram = _sync(
        store,
        [_worker("gen-A", KEY_A)],
        [_final_turn("gen-A", turn_id="turn-A", text="ordinary final")],
    )

    entry = state.source_worker_entries(store)[entry_key]
    assert result["worker_rebinds"] == 0
    assert result["feed_sent"] == 1
    assert entry["tendwire_worker_id"] == "gen-A"
    assert "tendwire_worker_rebind_audit" not in store
    assert any("ordinary final" in text for _chat, text, _kwargs, _mid in telegram.sent)
