from __future__ import annotations

from copy import deepcopy
import time

import pytest

from herdres_connector import doctor
from herdres_connector.source_sync import SyncRuntime, sync_once
from test_source_only import FakeTelegram, FakeTendwire, _store
from test_turn_final_delivery import TurnFinalTendwire, _turn_row


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


def test_unchanged_active_sync_uses_only_one_delta_page_and_no_provider_or_content():
    row = _turn_row("turn-live", "twrev1.live", None, user="prompt")
    tendwire = DeltaTendwire(
        [_page([], mode="changes", checkpoint="twdelta1.same")]
    )
    store = _store()
    store["tendwire_delta_sync"] = _active_delta(row)
    telegram = FakeTelegram()

    result = sync_once(
        store,
        SyncRuntime(tendwire, telegram, with_outbox=False),
    )

    assert result["ok"] is True
    assert tendwire.turn_calls == 0
    assert tendwire.content_calls == 0
    assert len(tendwire.delta_calls) == 1
    assert tendwire.delta_calls[0]["watermark"] == "twdelta1.current"
    assert telegram.api_calls == []
    assert telegram.sent == []
    assert telegram.edited == []
    assert telegram.deleted_topics == []


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
    assert store["tendwire_delta_sync"]["pending_cursor"] == "twdeltac1.resume"

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
    assert result["tendwire_delta_sync"]["health_flag"] == "turn_delta_unsupported"


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


def test_descriptor_only_upsert_does_not_fabricate_or_fetch_content():
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
    tendwire = DeltaTendwire([_page([_upsert(row)])])
    telegram = FakeTelegram()

    result = sync_once(
        _store(),
        SyncRuntime(tendwire, telegram, with_outbox=False),
    )

    assert result["feed_sent"] == 0
    assert result["content_pages"] == 0
    assert tendwire.content_calls == 0
    assert telegram.sent == []
    assert telegram.edited == []


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
