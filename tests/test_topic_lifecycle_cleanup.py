from __future__ import annotations

import copy
from contextlib import contextmanager

import pytest

from herdres_connector import config, source_sync, state
from herdres_connector.source_sync import SyncRuntime
from herdres_connector.telegram_delivery import (
    RateLimited,
    TelegramClient,
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
        self.deleted = []
        self.reopened = []
        self.created = []

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

    def delete_topic(self, _chat_id, topic_id):
        self.deleted.append(str(topic_id))
        return self._response()

    def reopen_topic(self, _chat_id, topic_id):
        self.reopened.append(str(topic_id))
        return self._response()

    def create_topic(self, _chat_id, name, icon_color=None):
        self.created.append((str(name), icon_color))
        return {"ok": True, "topic_id": str(100 + len(self.created))}


def _runtime(telegram, *, dry_run=False):
    return SyncRuntime(
        tendwire=None,
        telegram=telegram,
        dry_run=dry_run,
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
    monkeypatch.setenv("HERDRES_TOPIC_CLEANUP_ACTION", "close")
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
    assert "topic_id" not in entry
    assert "topic_closed_at" not in entry


def test_three_failures_permanently_abandon_target():
    entry = _entry("13", dormant_at=NOW - DAY)
    store = _store(("pane:one", entry))
    telegram = CleanupTelegram(
        [{"ok": False, "error": "Bad Request: nope"}] * 4
    )

    results = [
        _cleanup(store, telegram, now=NOW + offset)
        for offset in (0, 60, 120, 180)
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


def test_delete_mode_deletes_dormant_and_retired_topics_once_at_ttl(
    monkeypatch,
):
    monkeypatch.setenv("HERDRES_TOPIC_CLEANUP_ACTION", "delete")
    dormant = _entry("42", dormant_at=NOW - DAY)
    retired = _entry("43", retired_at=NOW - DAY)
    retired["pane_uuid"] = "00000000-0000-4000-8000-000000000002"
    store = _store(("pane:dormant", dormant), ("retired:one", retired))
    telegram = CleanupTelegram()

    first = _cleanup(store, telegram)
    second = _cleanup(store, telegram, now=NOW + 60)

    assert first["deleted"] == 2
    assert second["deleted"] == 0
    assert telegram.deleted == ["42", "43"]
    assert "topic_id" not in dormant
    assert "topic_id" not in retired
    assert retired["retired_topic_deleted"] is True
    assert [
        item["action"] for item in store["telegram_topic_cleanup_audit"]
    ] == ["delete", "delete"]


def test_delete_mode_never_touches_protected_topics(monkeypatch):
    monkeypatch.setenv("HERDRES_TOPIC_CLEANUP_ACTION", "delete")
    general = _entry("1", retired_at=NOW - DAY)
    pinned = _entry("44", retired_at=NOW - DAY)
    pinned["pinned_topic"] = True
    retired_shared = _entry("45", retired_at=NOW - DAY)
    live_shared = _entry("45", status="idle")
    live_shared["pane_uuid"] = "00000000-0000-4000-8000-000000000002"
    space_topic = _entry("46", retired_at=NOW - DAY)
    ordinary = _entry("47", retired_at=NOW - DAY)
    store = _store(
        ("retired:general", general),
        ("retired:pinned", pinned),
        ("retired:shared", retired_shared),
        ("pane:live", live_shared),
        ("retired:space", space_topic),
        ("retired:ordinary", ordinary),
    )
    store["spaces"]["space:one"] = {
        "source": "tendwire",
        "entry_type": "space",
        "topic_id": "46",
    }
    telegram = CleanupTelegram()

    result = _cleanup(store, telegram)

    assert result["deleted"] == 1
    assert telegram.deleted == ["47"]
    assert all(
        entry.get("topic_id")
        for entry in (general, pinned, retired_shared, live_shared, space_topic)
    )


def test_revive_after_delete_recreates_topic_with_same_pane_identity(monkeypatch):
    monkeypatch.setenv("HERDRES_TOPIC_CLEANUP_ACTION", "delete")
    entry = _entry("48", dormant_at=NOW - DAY)
    store = _store(("pane:one", entry))
    telegram = CleanupTelegram()

    deleted = _cleanup(store, telegram)
    entry["status"] = "idle"
    entry["tendwire_raw_status"] = "idle"
    source_sync._refresh_topic_cleanup_lifecycle(
        store, now=NOW + 1, dry_run=False
    )
    needed, created = source_sync._ensure_topic(
        store,
        {"name": "revived pane"},
        entry,
        _runtime(telegram),
        chat_id="-100",
    )

    assert deleted["deleted"] == 1
    assert (needed, created) == (True, True)
    assert telegram.deleted == ["48"]
    assert entry["topic_id"] == "101"
    assert entry["pane_uuid"] == PANE_UUID
    assert "topic_dormant_at" not in entry
    assert "topic_closed_at" not in entry
    assert telegram.reopened == []


def test_mode_flip_close_to_delete_removes_already_closed_topic(monkeypatch):
    entry = _entry("49", dormant_at=NOW - DAY)
    store = _store(("pane:one", entry))
    telegram = CleanupTelegram()

    closed = _cleanup(store, telegram)
    monkeypatch.setenv("HERDRES_TOPIC_CLEANUP_ACTION", "delete")
    deleted = _cleanup(store, telegram, now=NOW + 60)
    repeated = _cleanup(store, telegram, now=NOW + 120)

    assert closed["closed"] == 1
    assert deleted["deleted"] == 1
    assert repeated["operations"] == 0
    assert telegram.closed == ["49"]
    assert telegram.deleted == ["49"]
    assert "topic_id" not in entry
    assert [
        item["action"] for item in store["telegram_topic_cleanup_audit"]
    ] == ["close", "delete"]


def test_already_gone_delete_is_terminal_success(monkeypatch):
    monkeypatch.setenv("HERDRES_TOPIC_CLEANUP_ACTION", "delete")
    entry = _entry("53", retired_at=NOW - DAY)
    store = _store(("retired:one", entry))
    telegram = CleanupTelegram(
        [{"ok": False, "error": "Bad Request: TOPIC_ID_INVALID"}]
    )

    first = _cleanup(store, telegram)
    second = _cleanup(store, telegram, now=NOW + 60)

    assert first["deleted"] == 1
    assert second["operations"] == 0
    assert telegram.deleted == ["53"]
    assert "topic_id" not in entry
    assert entry["retired_topic_missing"] is True


def test_pre_lifecycle_retired_close_marker_is_adopted_without_api_call():
    entry = _entry("41", retired_at=NOW - (10 * DAY))
    entry["retired_topic_closed"] = True
    store = _store(("retired:one", entry))
    telegram = CleanupTelegram()

    result = _cleanup(store, telegram)

    assert result["operations"] == 0
    assert telegram.closed == []
    assert entry["topic_closed_at"] == NOW


def test_rate_limit_persists_backoff_without_spending_attempt_cap(monkeypatch):
    monkeypatch.setattr(source_sync.time, "time", lambda: NOW)
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


def test_delete_mode_rate_limit_backoff_and_attempt_cap(monkeypatch):
    monkeypatch.setattr(source_sync.time, "time", lambda: NOW)
    monkeypatch.setenv("HERDRES_TOPIC_CLEANUP_ACTION", "delete")
    limited_entry = _entry("51", dormant_at=NOW - DAY)
    limited_store = _store(("pane:limited", limited_entry))
    limited_telegram = CleanupTelegram(
        [RateLimited(7, "Too Many Requests"), {"ok": True}]
    )

    limited = _cleanup(limited_store, limited_telegram)
    backed_off = _cleanup(limited_store, limited_telegram, now=NOW + 6)
    recovered = _cleanup(limited_store, limited_telegram, now=NOW + 7)

    assert limited["deferred"] == 1
    assert backed_off["operations"] == 0
    assert recovered["deleted"] == 1
    assert limited_telegram.deleted == ["51", "51"]
    assert limited_store["telegram_topic_cleanup_attempts"] == {}

    failing_entry = _entry("52", dormant_at=NOW - DAY)
    failing_store = _store(("pane:failing", failing_entry))
    failing_telegram = CleanupTelegram(
        [{"ok": False, "error": "Bad Request: nope"}] * 4
    )

    results = [
        _cleanup(failing_store, failing_telegram, now=NOW + offset)
        for offset in (0, 60, 120, 180)
    ]

    assert failing_telegram.deleted == ["52", "52", "52"]
    assert results[2]["abandoned"] == 1
    assert results[3]["abandoned"] == 1
    assert failing_store["telegram_topic_cleanup_abandoned"] == ["delete:52"]


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


class AlwaysRateLimitedTopics(TelegramClient):
    def api(self, _method, _payload):
        raise RateLimited(9, "Too Many Requests")


@pytest.mark.parametrize("action", ("close", "delete", "reopen"))
def test_legacy_topic_methods_swallow_rate_limits_but_cleanup_variants_raise(
    action,
):
    telegram = AlwaysRateLimitedTopics(token="test")

    legacy = getattr(telegram, f"{action}_topic")("-100", "70")

    assert legacy == {
        "ok": False,
        "error": "Too Many Requests",
        "rate_limited": True,
        "retry_after": 9,
    }
    with pytest.raises(RateLimited):
        getattr(telegram, f"{action}_topic_for_cleanup")("-100", "70")


def test_empty_cleanup_config_values_match_unset_defaults():
    assert config.close_dormant_after_hours(
        {"HERDRES_CLOSE_DORMANT_AFTER_HOURS": ""}
    ) == config.close_dormant_after_hours({})
    assert config.topic_cleanup_action(
        {"HERDRES_TOPIC_CLEANUP_ACTION": ""}
    ) == config.topic_cleanup_action({})
    assert config.cleanup_budget_seconds(
        {"HERDRES_CLEANUP_BUDGET_SECONDS": ""}
    ) == config.cleanup_budget_seconds({})
    assert config.cleanup_max_ops(
        {"HERDRES_CLEANUP_MAX_OPS": ""}
    ) == config.cleanup_max_ops({})


def test_invalid_dormant_ttl_fails_closed_with_diagnostic(capsys):
    assert config.close_dormant_after_hours(
        {"HERDRES_CLOSE_DORMANT_AFTER_HOURS": "never"}
    ) == 0
    assert "lifecycle cleanup disabled" in capsys.readouterr().err


@pytest.mark.parametrize(
    ("action", "preview_key"),
    (("close", "would_close"), ("delete", "would_delete")),
)
def test_dry_run_previews_underlying_dormancy_without_live_stamps(
    monkeypatch, action, preview_key
):
    monkeypatch.setenv("HERDRES_TOPIC_CLEANUP_ACTION", action)
    entry = _entry("71", status="closed")
    store = _store(("pane:one", entry))

    result = source_sync._sync_topic_lifecycle_cleanup(
        store,
        _runtime(CleanupTelegram(), dry_run=True),
        chat_id="-100",
        now=NOW,
    )

    assert result[preview_key] == 1
    assert "topic_dormant_at" not in entry


@pytest.mark.parametrize(
    "pending_field",
    ("retired_topic_notice_pending", "retired_topic_rename_pending"),
)
def test_retired_ttl_waits_for_notice_and_rename(pending_field):
    entry = _entry("72", retired_at=NOW - DAY)
    entry[pending_field] = True
    store = _store(("retired:one", entry))
    telegram = CleanupTelegram()

    result = _cleanup(store, telegram)

    assert result["closed"] == 0
    assert telegram.closed == []


class TopicClosedArchiveTelegram(CleanupTelegram):
    def __init__(self):
        super().__init__()
        self.notices = []
        self.renames = []

    def send_message(self, _chat_id, _text, **kwargs):
        self.notices.append(str(kwargs.get("thread_id") or ""))
        return {"ok": False, "error": "Bad Request: TOPIC_CLOSED"}

    def rename_topic(self, _chat_id, topic_id, _name):
        self.renames.append(str(topic_id))
        return {"ok": False, "error": "Bad Request: TOPIC_CLOSED"}


@pytest.mark.parametrize("path", ("notice", "rename"))
def test_retired_topic_closed_is_terminal_for_notice_and_rename(path):
    entry = _entry("73", retired_at=NOW)
    if path == "notice":
        entry["retired_topic_notice_pending"] = True
        entry["retired_topic_rename_pending"] = True
    else:
        entry["retired_topic_rename_pending"] = True
    store = _store(("retired:one", entry))
    telegram = TopicClosedArchiveTelegram()

    first = source_sync._sync_retired_worker_topics(
        store, _runtime(telegram), chat_id="-100"
    )
    second = source_sync._sync_retired_worker_topics(
        store, _runtime(telegram), chat_id="-100"
    )

    assert first == 1
    assert second == 0
    assert "retired_topic_notice_pending" not in entry
    assert "retired_topic_rename_pending" not in entry
    assert entry["retired_topic_closed"] is True
    assert entry["topic_closed_at"] == entry["topic_auto_closed_at"]
    assert len(telegram.notices) + len(telegram.renames) == 1


def test_reopen_failures_never_permanently_abandon_and_are_spaced():
    entry = _entry("74", status="idle")
    entry["topic_closed_at"] = NOW - DAY
    entry["topic_auto_closed_at"] = NOW - DAY
    store = _store(("pane:one", entry))
    telegram = CleanupTelegram(
        [{"ok": False, "error": "Bad Request: temporary reopen failure"}] * 4
    )

    results = [
        _cleanup(store, telegram, now=NOW + offset)
        for offset in (0, 1, 60, 120, 180)
    ]

    assert telegram.reopened == ["74", "74", "74", "74"]
    assert results[1]["deferred"] == 1
    assert store["telegram_topic_cleanup_attempts"] == {}
    assert store["telegram_topic_cleanup_abandoned"] == []


def test_transient_failures_are_spaced_without_spending_attempt_cap():
    entry = _entry("75", dormant_at=NOW - DAY)
    store = _store(("pane:one", entry))
    telegram = CleanupTelegram(
        [{"ok": False, "error": "temporary transport outage"}] * 2
    )

    first = _cleanup(store, telegram)
    too_soon = _cleanup(store, telegram, now=NOW + 59)
    retried = _cleanup(store, telegram, now=NOW + 60)

    assert first["operations"] == 1
    assert too_soon["operations"] == 0
    assert too_soon["deferred"] == 1
    assert retried["operations"] == 1
    assert telegram.closed == ["75", "75"]
    assert store["telegram_topic_cleanup_attempts"] == {}


def test_abandon_tracking_is_removed_when_topic_disappears():
    entry = _entry("76", dormant_at=NOW - DAY)
    store = _store(("pane:one", entry))
    telegram = CleanupTelegram(
        [{"ok": False, "error": "Bad Request: permanent"}] * 3
    )
    for offset in (0, 60, 120):
        _cleanup(store, telegram, now=NOW + offset)
    assert store["telegram_topic_cleanup_abandoned"] == ["close:76"]

    entry.pop("topic_id")
    result = _cleanup(store, telegram, now=NOW + 180)

    assert result["changed"] is True
    assert store["telegram_topic_cleanup_abandoned"] == []
    assert store["telegram_topic_cleanup_attempts"] == {}


def test_phase3_real_reload_discards_close_for_concurrently_revived_pane(
    tmp_path, monkeypatch
):
    state_path = tmp_path / "state.json"
    monkeypatch.setenv("HERDR_TELEGRAM_TOPICS_STATE", str(state_path))
    entry = _entry("77", dormant_at=NOW - DAY)
    store = _store(("pane:one", entry))
    state.save_state(store, state_path)

    class RevivingTelegram(CleanupTelegram):
        def close_topic(self, chat_id, topic_id):
            result = super().close_topic(chat_id, topic_id)
            concurrent = state.load_state(state_path)
            revived = concurrent["panes"]["pane:one"]
            revived["status"] = "idle"
            revived["tendwire_raw_status"] = "idle"
            state.save_state(concurrent, state_path)
            return result

    with state.state_lock(path=state_path):
        current = state.load_state(state_path)
        result = _cleanup(current, RevivingTelegram())

    revived = current["panes"]["pane:one"]
    assert result["closed"] == 0
    assert result["deferred"] == 1
    assert revived["status"] == "idle"
    assert "topic_closed_at" not in revived


def test_rate_limit_backoff_uses_receipt_time_even_after_conflict(
    tmp_path, monkeypatch
):
    state_path = tmp_path / "state.json"
    monkeypatch.setenv("HERDR_TELEGRAM_TOPICS_STATE", str(state_path))
    clock = [NOW]
    monkeypatch.setattr(source_sync.time, "time", lambda: clock[0])
    entry = _entry("78", dormant_at=NOW - DAY)
    store = _store(("pane:one", entry))
    state.save_state(store, state_path)

    class RevivingRateLimitedTelegram(CleanupTelegram):
        def close_topic(self, _chat_id, topic_id):
            self.closed.append(str(topic_id))
            concurrent = state.load_state(state_path)
            revived = concurrent["panes"]["pane:one"]
            revived["status"] = "idle"
            revived["tendwire_raw_status"] = "idle"
            state.save_state(concurrent, state_path)
            clock[0] = NOW + 10
            raise RateLimited(7, "Too Many Requests")

    with state.state_lock(path=state_path):
        current = state.load_state(state_path)
        result = _cleanup(current, RevivingRateLimitedTelegram())

    assert result["operations"] == 1
    assert result["deferred"] == 1
    assert current["panes"]["pane:one"]["status"] == "idle"
    assert current["telegram_topic_cleanup_backoff_until"] == NOW + 17
