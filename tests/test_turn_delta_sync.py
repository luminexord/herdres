from __future__ import annotations

from copy import deepcopy
import time

import pytest

from herdres_connector import config, doctor, source_sync, state
from herdres_connector.source_sync import (
    _TURN_CONTENT_OUTCOME_KEY,
    _TurnContentError,
    _validate_delta_page,
    SyncRuntime,
    sync_once,
)
from test_source_only import FakeTelegram, FakeTendwire, _store
from test_turn_final_delivery import TurnFinalTendwire, _stable_key, _turn_row


HOST = "host-public"


def _page(
    changes,
    *,
    mode="bootstrap",
    more=False,
    cursor=None,
    checkpoint="twdelta1.final",
):
    return {
        "schema_version": 1,
        "projection_schema_version": 2,
        "host_id": HOST,
        "mode": mode,
        "changes": deepcopy(changes),
        "has_more": more,
        "next_cursor": cursor if more else None,
        "checkpoint": None if more else checkpoint,
        "aggregate": {
            "journal_rows_scanned": len(changes),
            "projection_rows_read": len(changes),
            "changes_returned": len(changes),
            "duration_ms": 1,
        },
    }


def _upsert(row):
    return {
        "op": "upsert",
        "turn_id": row["id"],
        "changed_at": "2030-01-01T00:00:00Z",
        "turn": deepcopy(row),
    }


def _active_delta(row=None):
    projection = {} if row is None else {row["id"]: deepcopy(row)}
    now = time.time()
    return {
        "schema_version": 1,
        "projection_schema_version": 2,
        "status": "active",
        "watermark": "twdelta1.current",
        "pending_cursor": None,
        "projection": projection,
        "bootstrap_state": None,
        "failure_count": 0,
        "watermark_updated_at": now,
        "last_full_reconcile_at": now,
    }


class DeltaTendwire(FakeTendwire):
    def __init__(self, pages, **kwargs):
        super().__init__(**kwargs)
        self.pages = list(pages)
        self.delta_calls = []
        self.turn_calls = 0
        self.content_calls = 0

    def turn_delta(self, **kwargs):
        self.delta_calls.append(dict(kwargs))
        return deepcopy(self.pages.pop(0))

    def turns(self):
        self.turn_calls += 1
        return super().turns()

    def turn_content_get(self, *_args, **_kwargs):
        self.content_calls += 1
        raise AssertionError("delta list rows must not fetch canonical content")


class DeltaTurnFinalTendwire(TurnFinalTendwire):
    def __init__(self, row, pages):
        super().__init__(row, emit_ready=True, turn_schema_version=2)
        self.delta_pages = list(pages)
        self.delta_calls = []
        self.turn_calls = 0

    def turn_delta(self, **kwargs):
        self.delta_calls.append(dict(kwargs))
        return deepcopy(self.delta_pages.pop(0))

    def turns(self):
        self.turn_calls += 1
        return super().turns()


@pytest.fixture(autouse=True)
def _delta_env(monkeypatch):
    monkeypatch.setenv("HERDRES_TENDWIRE_MODE", "source")
    monkeypatch.setenv("HERDRES_TENDWIRE_FULL_RECONCILE_SECONDS", "3600")
    monkeypatch.setenv("HERDRES_PINNED_STATUS", "0")
    monkeypatch.setenv("HERDR_TELEGRAM_TOPICS_STATUS_ICON", "0")
    monkeypatch.setenv("HERDR_TELEGRAM_TOPICS_WORKING_UPDATE_MIN_SECONDS", "0")


def test_delta_default_page_size_completes_supported_bootstrap_within_cursor_ttl(
    monkeypatch,
):
    monkeypatch.delenv("HERDRES_TENDWIRE_DELTA_LIMIT", raising=False)
    assert config.tendwire_delta_limit() == 500


def test_unchanged_active_sync_uses_only_one_delta_page_and_no_provider_or_content():
    row = _turn_row("turn-live", "twrev1.live", None, user="prompt")
    tendwire = DeltaTendwire(
        [_page([], mode="changes", checkpoint="twdelta1.current")],
        workers=[],
        spaces=[],
    )
    store = _store()
    store["telegram_deleted_topics"] = []
    store["telegram_message_bindings"] = {}
    store["tendwire_delta_sync"] = _active_delta(row)
    before = deepcopy(store)
    checkpoints = []
    telegram = FakeTelegram()

    result = sync_once(
        store,
        SyncRuntime(
            tendwire,
            telegram,
            with_outbox=False,
            checkpoint=lambda: checkpoints.append(deepcopy(store)),
        ),
    )

    assert result["ok"] is True
    assert result["changed"] is False
    assert store == before
    assert checkpoints == []
    assert tendwire.turn_calls == 0
    assert tendwire.content_calls == 0
    assert len(tendwire.delta_calls) == 1
    assert tendwire.delta_calls[0]["watermark"] == "twdelta1.current"
    assert telegram.api_calls == []
    assert telegram.sent == []
    assert telegram.edited == []
    assert telegram.deleted_topics == []


def test_delta_watermark_persists_across_cleanup_phase_reload(
    tmp_path, monkeypatch
):
    state_path = tmp_path / "state.json"
    monkeypatch.setenv("HERDR_TELEGRAM_TOPICS_STATE", str(state_path))
    monkeypatch.setenv("HERDRES_CLOSE_DORMANT_AFTER_HOURS", "24")
    store = _store()
    store["tendwire_delta_sync"] = _active_delta()
    store["panes"]["retired:cleanup"] = {
        "source": "tendwire",
        "entry_type": "worker",
        "pane_uuid": "00000000-0000-4000-8000-000000000099",
        "pane_uuid_version": 1,
        "tendwire_stable_key": _stable_key("retired-cleanup"),
        "tendwire_stable_key_version": 1,
        "tendwire_worker_id": "retired-cleanup",
        "tendwire_fingerprint": "fp-1",
        "status": "closed",
        "tendwire_raw_status": "closed",
        "topic_id": "990",
        "routing_retired": True,
        "routing_retired_at": 0.0,
    }
    state.save_state(store, state_path)
    telegram = FakeTelegram()
    tendwire = DeltaTendwire(
        [
            _page(
                [],
                mode="changes",
                checkpoint="twdelta1.after-cleanup",
            )
        ],
        workers=[],
        spaces=[],
    )

    with state.state_lock(path=state_path):
        current = state.load_state(state_path)
        result = sync_once(
            current,
            SyncRuntime(tendwire, telegram, with_outbox=False),
        )
        state.save_state(current, state_path)
    restarted = state.load_state(state_path)

    assert result["topic_cleanup"]["closed"] == 1
    assert telegram.closed_topics == ["990"]
    assert (
        restarted["tendwire_delta_sync"]["watermark"]
        == "twdelta1.after-cleanup"
    )


def test_offlock_delta_observation_revalidates_cursor_before_apply(
    tmp_path, monkeypatch
):
    state_path = tmp_path / "state.json"
    monkeypatch.setenv("HERDR_TELEGRAM_TOPICS_STATE", str(state_path))
    store = _store()
    store["tendwire_delta_sync"] = _active_delta()
    state.save_state(store, state_path)

    class ConcurrentDelta(DeltaTendwire):
        def turn_delta(self, **kwargs):
            current = state.load_state(state_path)
            current["tendwire_delta_sync"]["watermark"] = "twdelta1.concurrent"
            current["tendwire_delta_sync"]["projection"] = {
                "concurrent": {"id": "concurrent"}
            }
            state.save_state(current, state_path)
            return _page(
                [],
                mode="changes",
                checkpoint="twdelta1.stale-observation",
            )

    with state.state_lock(path=state_path):
        current = state.load_state(state_path)
        result = sync_once(
            current,
            SyncRuntime(
                ConcurrentDelta([], workers=[], spaces=[]),
                FakeTelegram(),
                with_outbox=False,
            ),
        )

    assert result["status"] == "tendwire_delta_cursor_changed"
    assert current["tendwire_delta_sync"]["watermark"] == "twdelta1.concurrent"
    assert set(current["tendwire_delta_sync"]["projection"]) == {"concurrent"}


def test_late_release_window_revalidates_delta_basis_before_checkpoint(
    tmp_path, monkeypatch
):
    state_path = tmp_path / "state.json"
    monkeypatch.setenv("HERDR_TELEGRAM_TOPICS_STATE", str(state_path))
    store = _store()
    store["tendwire_delta_sync"] = _active_delta()
    state.save_state(store, state_path)

    def concurrent_full_pass(
        current, _runtime, *, chat_id, snapshot_worker_ids
    ):
        assert chat_id == "-100"
        assert snapshot_worker_ids == set()
        with state.released_lock():
            concurrent = state.load_state(state_path)
            concurrent["tendwire_delta_sync"]["watermark"] = (
                "twdelta1.concurrent-full"
            )
            concurrent["tendwire_delta_sync"]["projection"] = {
                "concurrent": {"id": "concurrent"}
            }
            state.save_state(concurrent, state_path)
        state.reload_state_in_place(current, state_path)
        return {
            "deleted": 0,
            "failed": 0,
            "pruned": 0,
            "changed": False,
        }

    monkeypatch.setattr(source_sync, "_cleanup_topics", concurrent_full_pass)
    tendwire = DeltaTendwire(
        [
            _page(
                [],
                mode="changes",
                checkpoint="twdelta1.stale-late-checkpoint",
            )
        ],
        workers=[],
        spaces=[],
    )

    with state.state_lock(path=state_path):
        current = state.load_state(state_path)
        result = sync_once(
            current,
            SyncRuntime(tendwire, FakeTelegram(), with_outbox=False),
        )

    assert result["status"] == "tendwire_delta_cursor_changed"
    assert (
        current["tendwire_delta_sync"]["watermark"]
        == "twdelta1.concurrent-full"
    )
    assert (
        state.load_state(state_path)["tendwire_delta_sync"]["watermark"]
        == "twdelta1.concurrent-full"
    )


def test_active_working_upsert_uses_existing_card_delivery_path():
    prior = _turn_row("turn-working", "twrev1.working0", None, user="prompt")
    changed = _turn_row("turn-working", "twrev1.working1", None, user="prompt")
    changed["assistant_stream_text"] = "bounded progress update"
    tendwire = DeltaTendwire(
        [_page([_upsert(changed)], mode="changes", checkpoint="twdelta1.working")]
    )
    store = _store()
    store["tendwire_delta_sync"] = _active_delta(prior)
    telegram = FakeTelegram()

    result = sync_once(
        store,
        SyncRuntime(tendwire, telegram, with_outbox=False),
    )

    assert result["feed_sent"] == 1
    assert len(telegram.sent) == 1
    assert "bounded progress update" in telegram.sent[0][1]
    assert tendwire.turn_calls == 0
    assert tendwire.content_calls == 0


def test_delta_real_placeholder_real_keeps_one_working_card_across_restart():
    real = _turn_row("turn-real", "twrev1.real0", None, user="prompt")
    real["assistant_stream_text"] = "first progress"
    placeholder = _turn_row(
        "turn-placeholder", "twrev1.placeholder", None
    )
    changed_real = deepcopy(real)
    changed_real["assistant_stream_text"] = "second progress"
    telegram = FakeTelegram()
    store = _store()
    store["tendwire_delta_sync"] = _active_delta()

    first = DeltaTendwire(
        [_page([_upsert(real)], mode="changes", checkpoint="twdelta1.real0")]
    )
    result = sync_once(
        store, SyncRuntime(first, telegram, with_outbox=False)
    )
    assert result["feed_sent"] == 1
    assert len(telegram.sent) == 1
    assert telegram.edited == []

    restarted = deepcopy(store)
    second = DeltaTendwire(
        [
            _page(
                [_upsert(placeholder)],
                mode="changes",
                checkpoint="twdelta1.placeholder",
            ),
            _page(
                [_upsert(changed_real)],
                mode="changes",
                checkpoint="twdelta1.real1",
            ),
        ]
    )
    placeholder_result = sync_once(
        restarted, SyncRuntime(second, telegram, with_outbox=False)
    )
    assert placeholder_result["feed_sent"] == 0
    assert len(telegram.sent) == 1
    assert telegram.edited == []

    real_result = sync_once(
        restarted, SyncRuntime(second, telegram, with_outbox=False)
    )
    assert real_result["feed_sent"] == 1
    assert len(telegram.sent) == 1
    assert len(telegram.edited) == 1
    assert "second progress" in telegram.edited[0][2]

    entry = next(
        item
        for item in restarted["panes"].values()
        if item.get("tendwire_worker_id") == "worker-1"
    )
    assert entry["last_stream_turn_id"] == "turn-real"
    assert entry["last_stream_message_id"] == telegram.sent[0][3]
    working_bindings = [
        binding
        for binding in state.message_bindings(restarted).values()
        if binding.get("kind") == "working"
    ]
    assert len(working_bindings) == 1
    assert working_bindings[0]["turn_id"] == "turn-real"


def test_delta_real_turn_reuses_retained_placeholder_card():
    placeholder = _turn_row(
        "turn-placeholder", "twrev1.placeholder", None
    )
    real = _turn_row("turn-real", "twrev1.real", None, user="prompt")
    real["assistant_stream_text"] = "source progress"
    tendwire = DeltaTendwire(
        [
            _page(
                [_upsert(placeholder)],
                mode="changes",
                checkpoint="twdelta1.placeholder",
            ),
            _page([_upsert(real)], mode="changes", checkpoint="twdelta1.real"),
        ]
    )
    store = _store()
    store["tendwire_delta_sync"] = _active_delta()
    telegram = FakeTelegram()

    sync_once(store, SyncRuntime(tendwire, telegram, with_outbox=False))
    restarted = deepcopy(store)
    sync_once(restarted, SyncRuntime(tendwire, telegram, with_outbox=False))

    assert len(telegram.sent) == 1
    assert len(telegram.edited) == 1
    assert "source progress" in telegram.edited[0][2]
    entry = next(
        item
        for item in restarted["panes"].values()
        if item.get("tendwire_worker_id") == "worker-1"
    )
    assert entry["last_stream_turn_id"] == "turn-real"
    assert entry["last_stream_message_id"] == telegram.sent[0][3]


def test_page_replays_idempotently_after_crash_before_state_save():
    row = _turn_row("turn-replay", "twrev1.replay", None, user="prompt")
    response = _page([_upsert(row)], checkpoint="twdelta1.replayed")
    persisted = deepcopy(_store())
    first_store = deepcopy(persisted)
    first = DeltaTendwire([response], workers=[], spaces=[])

    with pytest.raises(RuntimeError, match="crash before save"):
        sync_once(
            first_store,
            SyncRuntime(
                first,
                FakeTelegram(),
                with_outbox=False,
                checkpoint=lambda: (_ for _ in ()).throw(
                    RuntimeError("crash before save")
                ),
            ),
        )

    assert first_store["tendwire_delta_sync"]["projection"] == {
        row["id"]: row
    }
    restarted = deepcopy(persisted)
    checkpoints = []
    second = DeltaTendwire([response], workers=[], spaces=[])
    sync_once(
        restarted,
        SyncRuntime(
            second,
            FakeTelegram(),
            with_outbox=False,
            checkpoint=lambda: checkpoints.append(deepcopy(restarted)),
        ),
    )

    assert list(restarted["tendwire_delta_sync"]["projection"]) == [row["id"]]
    assert restarted["tendwire_delta_sync"]["watermark"] == "twdelta1.replayed"
    assert checkpoints[-1]["tendwire_delta_sync"]["watermark"] == "twdelta1.replayed"


def test_batch_watermark_advances_only_on_final_applied_page():
    row = _turn_row("turn-page", "twrev1.page", None, user="prompt")
    tendwire = DeltaTendwire(
        [
            _page(
                [_upsert(row)],
                more=True,
                cursor="twdeltac1.next",
                checkpoint=None,
            ),
            _page([], mode="bootstrap", checkpoint="twdelta1.complete"),
        ],
        workers=[],
        spaces=[],
    )
    store = _store()
    checkpoints = []
    runtime = SyncRuntime(
        tendwire,
        FakeTelegram(),
        with_outbox=False,
        checkpoint=lambda: checkpoints.append(deepcopy(store)),
    )

    sync_once(store, runtime)
    first = checkpoints[-1]["tendwire_delta_sync"]
    assert first["watermark"] is None
    assert first["pending_cursor"] == "twdeltac1.next"

    sync_once(store, runtime)
    second = checkpoints[-1]["tendwire_delta_sync"]
    assert tendwire.delta_calls[1]["cursor"] == "twdeltac1.next"
    assert second["pending_cursor"] is None
    assert second["watermark"] == "twdelta1.complete"
    assert second["status"] == "active"


def test_multi_page_bootstrap_marks_every_page_before_redelivery_guard_completes(
    monkeypatch,
):
    first_row = _turn_row(
        "turn-bootstrap-first",
        "twrev1.bootstrap_first",
        None,
        user="first historical prompt",
    )
    first_row["assistant_stream_text"] = "first historical progress"
    second_row = _turn_row(
        "turn-bootstrap-second",
        "twrev1.bootstrap_second",
        None,
        user="second historical prompt",
    )
    second_row.update(
        {
            "worker_id": "worker-2",
            "worker_fingerprint": "fp-2",
            "stable_key": _stable_key("worker-2", "fp-2"),
            "assistant_stream_text": "second historical progress",
        }
    )
    workers = [
        {
            "id": "worker-1",
            "name": "Alpha",
            "status": "working",
            "space_id": "space-1",
            "fingerprint": "fp-1",
        },
        {
            "id": "worker-2",
            "name": "Beta",
            "status": "working",
            "space_id": "space-1",
            "fingerprint": "fp-2",
        },
    ]
    tendwire = DeltaTendwire(
        [
            _page(
                [_upsert(first_row)],
                more=True,
                cursor="twdeltac1.bootstrap_next",
                checkpoint=None,
            ),
            _page([_upsert(second_row)], checkpoint="twdelta1.bootstrap_done"),
        ],
        turns={"schema_version": 1, "turns": [second_row, first_row]},
        workers=workers,
    )
    store = _store()
    store.pop("tendwired_bootstrap_complete")
    telegram = FakeTelegram()
    runtime = SyncRuntime(tendwire, telegram, with_outbox=False)

    sync_once(store, runtime)
    assert "tendwired_bootstrap_complete" not in store
    assert store["tendwired_bootstrap_seen"] == 1

    sync_once(store, runtime)
    assert store["tendwired_bootstrap_complete"] is True
    assert store["tendwired_bootstrap_seen"] == 2

    monkeypatch.setenv("HERDRES_TENDWIRE_FORCE_FULL_RECONCILE", "1")
    sync_once(store, runtime)

    assert tendwire.turn_calls == 1
    assert telegram.sent == []


def test_transport_ambiguity_never_bootstraps_or_traverses():
    tendwire = DeltaTendwire(
        [{"ok": False, "status": "transport_ambiguous"}]
    )
    store = _store()
    store["tendwire_delta_sync"] = _active_delta()

    result = sync_once(
        store,
        SyncRuntime(tendwire, FakeTelegram(), with_outbox=False),
    )

    assert result["ok"] is True
    assert len(tendwire.delta_calls) == 1
    assert tendwire.turn_calls == 0
    assert store["tendwire_delta_sync"]["watermark"] == "twdelta1.current"
    assert store["tendwire_delta_sync"]["status"] == "active"
    assert result["tendwire_delta_sync"]["health_flag"] == "turn_delta_transport_ambiguous"


def test_delta_page_isolates_revisionless_legacy_row_without_dropping_valid_rows():
    valid = _turn_row(
        "turn-valid", "twrev1.valid", None, user="current prompt"
    )
    legacy = _turn_row(
        "turn-legacy", "twrev1.legacy", None, user="legacy prompt"
    )
    legacy["content"]["content_revision"] = None

    upserts, removals, _aggregate = _validate_delta_page(
        _page(
            [_upsert(valid), _upsert(legacy)],
            mode="changes",
            checkpoint="twdelta1.legacy_tolerated",
        )
    )

    assert removals == []
    assert [row["id"] for row in upserts] == [
        "turn-valid",
        "turn-legacy",
    ]
    assert _TURN_CONTENT_OUTCOME_KEY not in upserts[0]
    assert upserts[1][_TURN_CONTENT_OUTCOME_KEY] == {
        "turn_id": "turn-legacy",
        "status": "invalid_content_schema",
    }


@pytest.mark.parametrize(
    "change",
    [
        pytest.param(
            {
                "op": "upsert",
                "turn_id": "turn-missing",
                "changed_at": "2030-01-01T00:00:00Z",
            },
            id="missing-turn-projection",
        ),
        pytest.param(
            {
                "op": "replace",
                "turn_id": "turn-bad-op",
                "changed_at": "2030-01-01T00:00:00Z",
            },
            id="bad-operation",
        ),
    ],
)
def test_delta_page_still_rejects_page_level_protocol_errors(change):
    with pytest.raises(_TurnContentError) as exc_info:
        _validate_delta_page(
            _page(
                [change],
                mode="changes",
                checkpoint="twdelta1.invalid_page",
            )
        )

    assert exc_info.value.status == "delta_protocol_ambiguous"


def test_delta_page_isolates_invalid_row_before_checking_its_identity():
    legacy = _turn_row(
        "turn-projection", "twrev1.identity", None
    )
    legacy["content"]["content_revision"] = None
    change = _upsert(legacy)
    change["turn_id"] = "turn-envelope"

    upserts, removals, _aggregate = _validate_delta_page(
        _page(
            [change],
            mode="changes",
            checkpoint="twdelta1.identity_mismatch",
        )
    )

    assert removals == []
    assert upserts == [
        {
            **legacy,
            _TURN_CONTENT_OUTCOME_KEY: {
                "turn_id": "turn-projection",
                "status": "invalid_content_schema",
            },
        }
    ]


def test_invalid_watermark_starts_one_bootstrap_and_ambiguity_resumes_its_cursor():
    row = _turn_row("turn-bootstrap", "twrev1.bootstrap", None, user="prompt")
    tendwire = DeltaTendwire(
        [
            {"ok": False, "status": "invalid_watermark"},
            _page(
                [_upsert(row)],
                mode="bootstrap",
                more=True,
                cursor="twdeltac1.resume",
                checkpoint=None,
            ),
            {"ok": False, "status": "transport_ambiguous"},
        ],
        workers=[],
        spaces=[],
    )
    store = _store()
    store["tendwire_delta_sync"] = _active_delta()
    runtime = SyncRuntime(tendwire, FakeTelegram(), with_outbox=False)

    sync_once(store, runtime)
    assert len(tendwire.delta_calls) == 1
    assert tendwire.turn_calls == 0
    assert store["tendwire_delta_sync"]["status"] == "bootstrapping"
    assert store["tendwire_delta_sync"]["watermark"] is None

    sync_once(store, runtime)
    assert len(tendwire.delta_calls) == 2
    assert tendwire.delta_calls[1]["watermark"] is None

    sync_once(store, runtime)
    assert len(tendwire.delta_calls) == 3
    assert tendwire.delta_calls[2]["cursor"] == "twdeltac1.resume"
    assert store["tendwire_delta_sync"]["pending_cursor"] == "twdeltac1.resume"
    assert store["tendwire_delta_sync"]["status"] == "bootstrapping"
    assert tendwire.turn_calls == 0


def test_explicit_unsupported_is_the_only_immediate_full_poll_fallback():
    tendwire = DeltaTendwire(
        [{"ok": False, "status": "unsupported_method"}],
        turns={"schema_version": 2, "turns": []},
    )
    store = _store()

    result = sync_once(
        store,
        SyncRuntime(tendwire, FakeTelegram(), with_outbox=False),
    )

    assert result["ok"] is True
    assert len(tendwire.delta_calls) == 1
    assert tendwire.turn_calls == 1
    assert store["tendwire_delta_sync"]["status"] == "fallback"
    assert store["tendwire_delta_sync"]["fallback_kind"] == "terminal"
    assert result["tendwire_delta_sync"]["health_flag"] == "turn_delta_unsupported"

    repeated = sync_once(
        store,
        SyncRuntime(tendwire, FakeTelegram(), with_outbox=False),
    )
    assert repeated["ok"] is True
    assert len(tendwire.delta_calls) == 1
    assert tendwire.turn_calls == 2


def test_transient_delta_fallback_recovers_with_persisted_exponential_backoff(
    monkeypatch,
):
    clock = [1000.0]
    monkeypatch.setattr(source_sync.time, "time", lambda: clock[0])
    tendwire = DeltaTendwire(
        [
            {"ok": False, "status": "nonzero_exit"},
            {"ok": False, "status": "nonzero_exit"},
            {"ok": False, "status": "nonzero_exit"},
            {"ok": False, "status": "nonzero_exit"},
            {"ok": False, "status": "nonzero_exit"},
            _page([], mode="changes", checkpoint="twdelta1.current"),
        ],
        turns={"schema_version": 2, "turns": []},
    )
    store = _store()
    store["tendwire_delta_sync"] = _active_delta()
    runtime = SyncRuntime(tendwire, FakeTelegram(), with_outbox=False)

    for _attempt in range(3):
        result = sync_once(store, runtime)
        assert result["ok"] is True
        assert store["tendwire_delta_sync"]["status"] == "active"
        assert tendwire.turn_calls == 0

    fourth = sync_once(store, runtime)
    delta = store["tendwire_delta_sync"]
    assert fourth["ok"] is True
    assert delta["status"] == "fallback"
    assert delta["fallback_kind"] == "transient"
    assert delta["fallback_attempt"] == 1
    assert delta["fallback_retry_at"] == 1060.0
    assert tendwire.turn_calls == 1

    clock[0] = 1059.0
    sync_once(store, runtime)
    assert len(tendwire.delta_calls) == 4
    assert tendwire.turn_calls == 2

    clock[0] = 1060.0
    sync_once(store, runtime)
    delta = store["tendwire_delta_sync"]
    assert len(tendwire.delta_calls) == 5
    assert delta["status"] == "fallback"
    assert delta["fallback_attempt"] == 2
    assert delta["fallback_retry_at"] == 1180.0
    assert tendwire.turn_calls == 3

    clock[0] = 1179.0
    sync_once(store, runtime)
    assert len(tendwire.delta_calls) == 5
    assert tendwire.turn_calls == 4

    clock[0] = 1180.0
    recovered = sync_once(store, runtime)
    delta = store["tendwire_delta_sync"]
    assert recovered["ok"] is True
    assert len(tendwire.delta_calls) == 6
    assert tendwire.turn_calls == 4
    assert delta["status"] == "active"
    assert delta["failure_count"] == 0
    assert not any(key.startswith("fallback_") for key in delta)
    assert "health_flag" not in delta


def test_legacy_transient_fallback_is_probed_immediately_after_upgrade(
    monkeypatch,
):
    clock = [2000.0]
    monkeypatch.setattr(source_sync.time, "time", lambda: clock[0])
    tendwire = DeltaTendwire(
        [_page([], mode="changes", checkpoint="twdelta1.current")]
    )
    store = _store()
    delta = _active_delta()
    delta.update(
        {
            "status": "fallback",
            "failure_count": 2,
            "health_flag": "turn_delta_repeated_nonzero_exit",
        }
    )
    store["tendwire_delta_sync"] = delta

    result = sync_once(
        store, SyncRuntime(tendwire, FakeTelegram(), with_outbox=False)
    )

    assert result["ok"] is True
    assert len(tendwire.delta_calls) == 1
    assert tendwire.turn_calls == 0
    assert store["tendwire_delta_sync"]["status"] == "active"
    assert not any(
        key.startswith("fallback_")
        for key in store["tendwire_delta_sync"]
    )


def test_transport_ambiguity_uses_the_same_four_failure_recovery_lane(
    monkeypatch,
):
    clock = [3000.0]
    monkeypatch.setattr(source_sync.time, "time", lambda: clock[0])
    tendwire = DeltaTendwire(
        [
            {"ok": False, "status": "transport_ambiguous"}
            for _attempt in range(4)
        ],
        turns={"schema_version": 2, "turns": []},
    )
    store = _store()
    store["tendwire_delta_sync"] = _active_delta()
    runtime = SyncRuntime(tendwire, FakeTelegram(), with_outbox=False)

    for _attempt in range(4):
        sync_once(store, runtime)

    delta = store["tendwire_delta_sync"]
    assert len(tendwire.delta_calls) == 4
    assert tendwire.turn_calls == 1
    assert delta["status"] == "fallback"
    assert delta["fallback_kind"] == "transient"
    assert delta["fallback_retry_at"] == 3060.0


def test_bootstrap_too_large_degrades_to_full_poll_with_health_flag():
    tendwire = DeltaTendwire(
        [{"ok": False, "status": "bootstrap_too_large"}],
        turns={"schema_version": 2, "turns": []},
    )
    store = _store()

    result = sync_once(
        store,
        SyncRuntime(tendwire, FakeTelegram(), with_outbox=False),
    )

    assert result["ok"] is True
    assert tendwire.turn_calls == 1
    assert store["tendwire_delta_sync"]["status"] == "fallback"
    assert result["tendwire_delta_sync"]["health_flag"] == "turn_delta_bootstrap_too_large"


def test_explicit_full_reconcile_uses_bounded_full_lane_without_delta(monkeypatch):
    monkeypatch.setenv("HERDRES_TENDWIRE_FORCE_FULL_RECONCILE", "1")
    tendwire = DeltaTendwire(
        [],
        turns={"schema_version": 2, "turns": []},
    )
    store = _store()
    store["tendwire_delta_sync"] = _active_delta()

    result = sync_once(
        store,
        SyncRuntime(tendwire, FakeTelegram(), with_outbox=False),
    )

    assert result["ok"] is True
    assert tendwire.delta_calls == []
    assert tendwire.turn_calls == 1
    assert result["tendwire_delta_sync"]["last_batch"]["mode"] == "full_reconcile"


def test_zero_full_reconcile_interval_retains_hourly_projection_bound():
    assert (
        config.tendwire_full_reconcile_seconds(
            {"HERDRES_TENDWIRE_FULL_RECONCILE_SECONDS": "0"}
        )
        == 3600
    )


def test_remove_clears_local_card_state_without_deleting_telegram_history():
    row = _turn_row("turn-remove", "twrev1.remove", None, user="prompt")
    tendwire = DeltaTendwire(
        [
            _page(
                [
                    {
                        "op": "remove",
                        "turn_id": row["id"],
                        "removed_at": "2030-01-01T00:00:00Z",
                    }
                ],
                mode="changes",
                checkpoint="twdelta1.removed",
            )
        ]
    )
    store = _store()
    store["tendwire_delta_sync"] = _active_delta(row)
    store["panes"]["worker-1"] = {
        "tendwire_worker_id": "worker-1",
        "last_stream_turn_id": row["id"],
        "last_stream_hash": "old",
        "last_stream_message_id": "901",
        "last_turn_id": row["id"],
        "last_clean_hash": "old-final",
        "last_clean_message_id": "902",
    }
    telegram = FakeTelegram()

    sync_once(
        store,
        SyncRuntime(tendwire, telegram, with_outbox=False),
    )

    entry = store["panes"]["worker-1"]
    assert "last_stream_turn_id" not in entry
    assert "last_stream_message_id" not in entry
    assert "last_turn_id" not in entry
    assert "last_clean_message_id" not in entry
    assert row["id"] not in store["tendwire_delta_sync"]["projection"]
    assert telegram.deleted_topics == []
    assert telegram.api_calls == []


def test_remove_follows_superseding_turn_instead_of_orphaning_card():
    row = _turn_row("turn-old", "twrev1.old", None, user="prompt")
    tendwire = DeltaTendwire(
        [
            _page(
                [
                    {
                        "op": "remove",
                        "turn_id": row["id"],
                        "removed_at": "2030-01-01T00:00:00Z",
                        "superseded_by_turn_id": "turn-successor",
                    }
                ],
                mode="changes",
                checkpoint="twdelta1.superseded",
            )
        ]
    )
    store = _store()
    store["tendwire_delta_sync"] = _active_delta(row)
    entry = {
        "tendwire_worker_id": "worker-1",
        "last_stream_turn_id": row["id"],
        "last_stream_hash": "old",
        "last_stream_message_id": "901",
        "last_turn_id": row["id"],
        "last_clean_hash": "old-final",
        "last_clean_message_id": "902",
    }
    store["panes"]["worker-1"] = entry
    state.bind_message_to_worker(
        store,
        "901",
        entry,
        topic_id="77",
        kind="working",
        turn_id=row["id"],
    )
    state.bind_message_to_worker(
        store,
        "902",
        entry,
        topic_id="77",
        kind="final",
        turn_id=row["id"],
    )

    sync_once(
        store,
        SyncRuntime(tendwire, FakeTelegram(), with_outbox=False),
    )

    assert entry["last_stream_turn_id"] == "turn-successor"
    assert entry["last_turn_id"] == "turn-successor"
    assert state.message_bindings(store)["901"]["turn_id"] == "turn-successor"
    assert state.message_bindings(store)["902"]["turn_id"] == "turn-successor"


def test_paginated_prompt_descriptor_allows_working_without_content_fetch():
    row = _turn_row(
        "turn-descriptor",
        "twrev1.descriptor",
        None,
        user="paged prompt",
        inline=False,
    )
    row["content"]["fields"]["user_text"].update(
        {
            "page_count": 1,
            "first_cursor": "twcur1.descriptor_user",
        }
    )
    tendwire = DeltaTendwire(
        [_page([_upsert(row)], mode="changes")]
    )
    telegram = FakeTelegram()
    store = _store()
    store["tendwire_delta_sync"] = _active_delta()

    result = sync_once(
        store,
        SyncRuntime(tendwire, telegram, with_outbox=False),
    )

    assert result["feed_sent"] == 1
    assert result["content_pages"] == 0
    assert tendwire.content_calls == 0
    assert len(telegram.sent) == 1
    assert "Work is in progress." in telegram.sent[0][1]
    assert telegram.edited == []


def test_changes_page_crash_replay_uses_durable_card_identity_without_resend():
    row = _turn_row(
        "turn-crash-replay",
        "twrev1.crash_replay",
        None,
        user="prompt",
    )
    row["assistant_stream_text"] = "durable progress"
    page = _page(
        [_upsert(row)],
        mode="changes",
        checkpoint="twdelta1.after_delivery",
    )
    telegram = FakeTelegram()
    base = _store()
    sync_once(
        base,
        SyncRuntime(
            DeltaTendwire(
                [_page([], mode="changes", checkpoint="twdelta1.warm")]
            ),
            telegram,
            with_outbox=False,
        ),
    )
    base["tendwire_delta_sync"] = _active_delta()
    persisted = deepcopy(base)
    first_store = deepcopy(base)

    def crash_after_delivery_checkpoint():
        persisted.clear()
        persisted.update(deepcopy(first_store))
        assert (
            persisted["tendwire_delta_sync"]["watermark"]
            == "twdelta1.current"
        )
        raise RuntimeError("crash before cursor save")

    with pytest.raises(RuntimeError, match="crash before cursor save"):
        sync_once(
            first_store,
            SyncRuntime(
                DeltaTendwire([page]),
                telegram,
                with_outbox=False,
                checkpoint=crash_after_delivery_checkpoint,
            ),
        )

    assert len(telegram.sent) == 1
    restarted = deepcopy(persisted)
    sync_once(
        restarted,
        SyncRuntime(
            DeltaTendwire([page]),
            telegram,
            with_outbox=False,
        ),
    )

    assert len(telegram.sent) == 1
    assert (
        restarted["tendwire_delta_sync"]["watermark"]
        == "twdelta1.after_delivery"
    )


def test_complete_delta_upsert_delivers_once_only_through_turn_final_outbox():
    row = _turn_row(
        "turn-final-delta",
        "twrev1.deltafinal",
        "final through Goal 10",
        user="prompt",
    )
    tendwire = DeltaTurnFinalTendwire(
        row,
        [
            _page([_upsert(row)], checkpoint="twdelta1.final-ready"),
            _page([], mode="changes", checkpoint="twdelta1.final-ready"),
        ],
    )
    telegram = FakeTelegram()
    store = _store()
    runtime = SyncRuntime(tendwire, telegram, with_outbox=True, max_sends=20)

    first = sync_once(store, runtime)
    second = sync_once(store, runtime)

    assert tendwire.turn_calls == 0
    assert first["feed_sent"] == 0
    assert first["tendwire_turn_final"]["delivered"] == 1
    assert second["tendwire_turn_final"]["delivered"] == 0
    assert len(telegram.sent) == 1
    assert "final through Goal 10" in telegram.sent[0][1]


def test_doctor_delta_health_exposes_only_state_age_and_aggregate(monkeypatch):
    row = _turn_row("private-turn-id", "twrev1.private", None, user="secret prompt")
    delta = _active_delta(row)
    delta["last_batch"] = {
        "mode": "changes",
        "changes_returned": 1,
        "upserts": 1,
        "removals": 0,
        "duration_ms": 2,
    }
    monkeypatch.setattr(
        doctor.state,
        "load_state",
        lambda: {"tendwire_delta_sync": delta},
    )

    result = doctor.tendwire_delta_feed()

    assert result["ok"] is True
    assert result["state"] == "active"
    assert result["last_batch"] == {
        "mode": "changes",
        "changes_returned": 1,
        "upserts": 1,
        "removals": 0,
        "duration_ms": 2,
    }
    assert "private-turn-id" not in repr(result)
    assert "secret prompt" not in repr(result)
