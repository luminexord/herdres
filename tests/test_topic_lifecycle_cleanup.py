from __future__ import annotations

import copy
from contextlib import contextmanager

import pytest

from herdres_connector import source_sync, state
from herdres_connector.source_sync import SyncRuntime
from herdres_connector.telegram_delivery import (
    RateLimited,
    TelegramError,
    classify_telegram_error,
)


NOW = 2_000_000.0
DAY = 24 * 60 * 60
STABLE_KEY = "wsk1_" + ("a" * 64)
PANE_UUID = "00000000-0000-4000-8000-000000000001"


class CleanupTelegram:
    dry_run = False

    def __init__(self, responses=None):
        self.responses = list(responses or [])
        self.closed = []
        self.reopened = []

    def _response(self):
        if not self.responses:
            return {"ok": True}
        response = self.responses.pop(0)
        if isinstance(response, Exception):
            raise response
        return response

    def close_topic(self, _chat_id, topic_id):
        self.closed.append(str(topic_id))
        return self._response()

    def reopen_topic(self, _chat_id, topic_id):
        self.reopened.append(str(topic_id))
        return self._response()


def _runtime(telegram):
    return SyncRuntime(
        tendwire=None,
        telegram=telegram,
        with_outbox=False,
    )


def _store(*entries):
    return {
        "version": 2,
        "enabled": True,
        "telegram": {"chat_id": "-100", "general_thread_id": "1"},
        "panes": {
            entry_key: entry for entry_key, entry in entries
        },
        "spaces": {},
    }


def _entry(topic_id, *, status="closed", dormant_at=None, retired_at=None):
    entry = {
        "source": "tendwire",
        "entry_type": "worker",
        "pane_uuid": PANE_UUID,
        "pane_uuid_version": 1,
        "tendwire_stable_key": STABLE_KEY,
        "tendwire_stable_key_version": 1,
        "tendwire_worker_id": "worker-1",
        "tendwire_fingerprint": "fingerprint-1",
        "tendwire_space_id": "space-1",
        "status": status,
        "tendwire_raw_status": status,
        "topic_id": str(topic_id),
        "topic_name": f"Topic {topic_id}",
    }
    if dormant_at is not None:
        entry["topic_dormant_at"] = dormant_at
    if retired_at is not None:
        entry["routing_retired"] = True
        entry["routing_retired_reason"] = "test_retirement"
        entry["routing_retired_at"] = retired_at
    return entry


@pytest.fixture(autouse=True)
def _cleanup_config(monkeypatch):
    monkeypatch.setenv("HERDRES_CLOSE_DORMANT_AFTER_HOURS", "24")
    monkeypatch.setenv("HERDRES_CLEANUP_BUDGET_SECONDS", "5")
    monkeypatch.setenv("HERDRES_CLEANUP_MAX_OPS", "12")


def _cleanup(store, telegram, *, now=NOW):
    return source_sync._sync_topic_lifecycle_cleanup(
        store,
        _runtime(telegram),
        chat_id="-100",
        now=now,
    )


def test_dormant_topic_closes_at_ttl_exactly_once():
    entry = _entry("10", dormant_at=NOW - DAY)
    store = _store(("pane:one", entry))
    telegram = CleanupTelegram()

    first = _cleanup(store, telegram)
    second = _cleanup(store, telegram, now=NOW + 60)

    assert first["closed"] == 1
    assert second["closed"] == 0
    assert telegram.closed == ["10"]
    assert entry["topic_closed_at"] == NOW
    assert store["telegram_topic_cleanup_audit"][0]["action"] == "close"


def test_revived_uuid_topic_reopens_before_future_delivery():
    entry = _entry("11", status="idle")
    entry["topic_closed_at"] = NOW - 10
    entry["topic_auto_closed_at"] = NOW - 10
    store = _store(("pane:one", entry))
    telegram = CleanupTelegram()

    result = _cleanup(store, telegram)

    assert result["reopened"] == 1
    assert telegram.reopened == ["11"]
    assert "topic_closed_at" not in entry
    assert entry["topic_reopened_at"] == NOW


def test_zero_disables_new_closes_but_reopens_prior_auto_close(monkeypatch):
    dormant = _entry("14", dormant_at=NOW - (10 * DAY))
    revived = _entry("15", status="idle")
    revived["topic_closed_at"] = NOW - DAY
    revived["topic_auto_closed_at"] = NOW - DAY
    revived["pane_uuid"] = "00000000-0000-4000-8000-000000000002"
    store = _store(("pane:dormant", dormant), ("pane:revived", revived))
    telegram = CleanupTelegram()
    monkeypatch.setenv("HERDRES_CLOSE_DORMANT_AFTER_HOURS", "0")

    result = _cleanup(store, telegram)

    assert telegram.closed == []
    assert telegram.reopened == ["15"]
    assert result["closed"] == 0
    assert result["reopened"] == 1


def test_manually_closed_topic_without_auto_close_stamp_is_untouched():
    entry = _entry("16", status="idle")
    store = _store(("pane:one", entry))
    telegram = CleanupTelegram()

    result = _cleanup(store, telegram)

    assert result["operations"] == 0
    assert telegram.closed == []
    assert telegram.reopened == []


def test_already_gone_close_is_terminal_success():
    entry = _entry("12", retired_at=NOW - DAY)
    store = _store(("retired:one", entry))
    telegram = CleanupTelegram(
        [{"ok": False, "error": "Bad Request: TOPIC_ID_INVALID"}]
    )

    first = _cleanup(store, telegram)
    second = _cleanup(store, telegram, now=NOW + 60)

    assert first["closed"] == 1
    assert second["operations"] == 0
    assert telegram.closed == ["12"]
    assert entry["retired_topic_missing"] is True
    assert entry["topic_closed_at"] == NOW


def test_three_failures_permanently_abandon_target():
    entry = _entry("13", dormant_at=NOW - DAY)
    store = _store(("pane:one", entry))
    telegram = CleanupTelegram(
        [{"ok": False, "error": "Bad Request: nope"}] * 4
    )

    results = [
        _cleanup(store, telegram, now=NOW + offset)
        for offset in (0, 1, 2, 3)
    ]

    assert telegram.closed == ["13", "13", "13"]
    assert results[2]["abandoned"] == 1
    assert results[3]["abandoned"] == 1
    assert store["telegram_topic_cleanup_abandoned"] == ["close:13"]


def test_many_candidates_respect_time_budget_and_resume_next_pass(monkeypatch):
    entries = [
        (f"pane:{index}", _entry(str(20 + index), dormant_at=NOW - DAY))
        for index in range(4)
    ]
    # UUID uniqueness is irrelevant for closed entries, but topic ownership is
    # distinct and all four are independently eligible.
    store = _store(*entries)
    telegram = CleanupTelegram()
    ticks = iter((0.0, 0.0, 2.0))
    monkeypatch.setenv("HERDRES_CLEANUP_BUDGET_SECONDS", "1")
    monkeypatch.setattr(source_sync.time, "monotonic", lambda: next(ticks))

    first = _cleanup(store, telegram)
    monkeypatch.setattr(source_sync.time, "monotonic", lambda: 0.0)
    second = _cleanup(store, telegram, now=NOW + 1)

    assert first["operations"] == 1
    assert first["deferred"] == 3
    assert second["operations"] == 3
    assert telegram.closed == ["20", "21", "22", "23"]


def test_general_dashboard_shared_live_and_space_topics_are_protected():
    general = _entry("1", retired_at=NOW - DAY)
    dashboard = _entry("30", retired_at=NOW - DAY)
    dashboard["dashboard_topic"] = True
    retired_shared = _entry("31", retired_at=NOW - DAY)
    live_shared = _entry("31", status="idle")
    live_shared["pane_uuid"] = "00000000-0000-4000-8000-000000000002"
    space_topic = _entry("32", retired_at=NOW - DAY)
    ordinary = _entry("33", retired_at=NOW - DAY)
    store = _store(
        ("retired:general", general),
        ("retired:dashboard", dashboard),
        ("retired:shared", retired_shared),
        ("pane:live", live_shared),
        ("retired:space", space_topic),
        ("retired:ordinary", ordinary),
    )
    store["spaces"]["space:one"] = {
        "source": "tendwire",
        "entry_type": "space",
        "topic_id": "32",
    }
    telegram = CleanupTelegram()

    result = _cleanup(store, telegram)

    assert result["closed"] == 1
    assert telegram.closed == ["33"]


def test_retired_archive_waits_full_ttl_then_closes():
    entry = _entry("40", retired_at=NOW - DAY + 1)
    store = _store(("retired:one", entry))
    telegram = CleanupTelegram()

    before = _cleanup(store, telegram)
    at_ttl = _cleanup(store, telegram, now=NOW + 1)

    assert before["closed"] == 0
    assert at_ttl["closed"] == 1
    assert telegram.closed == ["40"]
    assert entry["retired_topic_closed"] is True


def test_pre_lifecycle_retired_close_marker_is_adopted_without_api_call():
    entry = _entry("41", retired_at=NOW - (10 * DAY))
    entry["retired_topic_closed"] = True
    store = _store(("retired:one", entry))
    telegram = CleanupTelegram()

    result = _cleanup(store, telegram)

    assert result["operations"] == 0
    assert telegram.closed == []
    assert entry["topic_closed_at"] == NOW


def test_rate_limit_persists_backoff_without_spending_attempt_cap():
    entry = _entry("50", dormant_at=NOW - DAY)
    store = _store(("pane:one", entry))
    telegram = CleanupTelegram(
        [RateLimited(7, "Too Many Requests"), {"ok": True}]
    )

    limited = _cleanup(store, telegram)
    backed_off = _cleanup(store, telegram, now=NOW + 6)
    recovered = _cleanup(store, telegram, now=NOW + 7)

    assert limited["deferred"] == 1
    assert backed_off["operations"] == 0
    assert recovered["closed"] == 1
    assert telegram.closed == ["50", "50"]
    assert store["telegram_topic_cleanup_attempts"] == {}


def test_cleanup_uses_released_lock_phase(monkeypatch):
    entry = _entry("60", dormant_at=NOW - DAY)
    store = _store(("pane:one", entry))
    telegram = CleanupTelegram()
    released = []

    @contextmanager
    def fake_release():
        released.append(True)
        yield

    monkeypatch.setattr(state, "lock_held", lambda: True)
    monkeypatch.setattr(state, "save_state", lambda current: None)
    monkeypatch.setattr(state, "load_state", lambda: copy.deepcopy(store))
    monkeypatch.setattr(state, "released_lock", fake_release)

    result = _cleanup(store, telegram)

    assert released == [True]
    assert result["closed"] == 1


def test_topic_closed_has_distinct_error_classification():
    assert (
        classify_telegram_error(
            TelegramError("Bad Request: TOPIC_CLOSED")
        )
        == "topic_closed"
    )
    assert classify_telegram_error(
        TelegramError("Bad Request: TOPIC_CLOSED")
    ) not in {"topic_not_found", "bad_request"}
