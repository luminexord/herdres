from __future__ import annotations

import json
import multiprocessing
import os
import signal
import sqlite3
import threading
import time
from pathlib import Path
from types import SimpleNamespace

import pytest

import herdres
import herdres_gateway
from herdres_connector import config, doctor, ingress_requests, speech, state
from herdres_connector.ingress_identity import derive_telegram_request_id
from herdres_connector.ingress_lanes import IngressLaneSpool, lane_key

from test_source_only import (
    REQUEST_ID_KEY,
    _accepted_command_response,
    _failed_command_response,
    _source_worker,
    _store,
)


@pytest.fixture(autouse=True)
def _no_real_lane_telegram(monkeypatch):
    monkeypatch.setattr(
        herdres_gateway,
        "TelegramClient",
        lambda token: SimpleNamespace(
            send_message=lambda *_args, **_kwargs: {
                "ok": True,
                "message_id": "ack",
            }
        ),
    )


def _update(update_id: int, topic_id: int, text: str) -> dict[str, object]:
    return {
        "update_id": update_id,
        "message": {
            "chat": {"id": -100, "is_forum": True},
            "message_thread_id": topic_id,
            "message_id": update_id + 1000,
            "from": {"id": 1, "is_bot": False},
            "text": text,
        },
    }


def _request_id(update: dict[str, object], receiver: str = "manager") -> str:
    message = update["message"]
    assert isinstance(message, dict)
    chat = message["chat"]
    assert isinstance(chat, dict)
    return derive_telegram_request_id(
        REQUEST_ID_KEY,
        receiver_id=receiver,
        update_id=update["update_id"],
        chat_id=chat["id"],
        message_id=message["message_id"],
    )


def _enqueue(
    spool: IngressLaneSpool,
    update: dict[str, object],
    topic: str,
    *,
    first_seen_at: float | None = None,
    deadline_at: float | None = None,
) -> None:
    seen = time.time() if first_seen_at is None else first_seen_at
    spool.enqueue(
        request_id=_request_id(update),
        receiver_kind="manager",
        update_id=int(update["update_id"]),
        lane_key_value=lane_key("manager", topic),
        kind="message",
        update=update,
        route={"chat_id": "-100", "topic_id": topic},
        first_seen_at=seen,
        deadline_at=seen + 60 if deadline_at is None else deadline_at,
        depth_limit=32,
    )


def _prior_enqueue_shape(
    spool_path: Path,
    update: dict[str, object],
    topic: str,
    *,
    first_seen_at: float,
) -> tuple[int, int]:
    """Write with the pre-state_since SQL used by the prior release."""

    update_id = int(update["update_id"])
    update_json = json.dumps(
        update,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )
    route_json = json.dumps(
        {"chat_id": "-100", "topic_id": topic},
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )
    with sqlite3.connect(spool_path, isolation_level=None) as connection:
        connection.execute("BEGIN IMMEDIATE")
        try:
            inserted = connection.execute(
                """
                INSERT OR IGNORE INTO lane_items(
                    request_id, receiver_kind, update_id, lane_key, kind,
                    update_json, route_json, state, attempts, first_seen_at,
                    next_attempt_at, deadline_at, lease_owner, lease_until,
                    notify_state, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0, ?, ?, ?, NULL, NULL, ?, ?)
                """,
                (
                    _request_id(update),
                    "manager",
                    update_id,
                    lane_key("manager", topic),
                    "message",
                    update_json,
                    route_json,
                    "pending",
                    first_seen_at,
                    first_seen_at,
                    first_seen_at + 60,
                    "pending",
                    first_seen_at,
                ),
            )
            connection.execute(
                """
                INSERT INTO receiver_cursors(receiver_kind, next_update_id, updated_at)
                VALUES (?, ?, ?)
                ON CONFLICT(receiver_kind) DO UPDATE SET
                    next_update_id = MAX(
                        receiver_cursors.next_update_id,
                        excluded.next_update_id
                    ),
                    updated_at = excluded.updated_at
                """,
                ("manager", update_id + 1, first_seen_at),
            )
            cursor = connection.execute(
                """
                SELECT next_update_id
                FROM receiver_cursors
                WHERE receiver_kind = 'manager'
                """
            ).fetchone()
            assert cursor is not None
            connection.commit()
        except BaseException:
            connection.rollback()
            raise
    return int(inserted.lastrowid or 0), int(cursor[0])


def _wait_for(predicate, timeout: float = 3.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.01)
    assert predicate()


def _configured_state(path: Path) -> None:
    store = _store()
    for worker_id, topic in (("worker-a", "77"), ("worker-b", "88")):
        state.upsert_worker_entry(
            store,
            _source_worker(
                {
                    "id": worker_id,
                    "name": worker_id,
                    "status": "idle",
                    "space_id": "space-1",
                    "fingerprint": f"fp-{worker_id}",
                }
            ),
            topic_id=topic,
        )
    state.save_state(store, path=path)


def test_pending_lane_is_spool_eligible_even_while_its_head_is_processing(
    tmp_path,
) -> None:
    """Specs and FIFO claimability must never hide a pending spool lane."""

    spool = IngressLaneSpool(tmp_path / "spool.db")
    _enqueue(spool, _update(1, 77, "head"), "77")
    _enqueue(spool, _update(2, 77, "pending follower"), "77")

    claimed = spool.claim("worker", lease_seconds=30)
    assert claimed is not None

    snapshot = spool.dispatch_snapshot()
    assert snapshot.pending_count == 1
    assert snapshot.processing_count == 1
    assert snapshot.eligible_lane_count == 1
    assert snapshot.claimable_lane_count == 0
    assert snapshot.first_claimable_lane == ""


def test_fresh_spool_remains_writable_by_prior_and_current_enqueue_shapes(
    tmp_path,
) -> None:
    spool_path = tmp_path / "spool.db"
    spool = IngressLaneSpool(spool_path)
    _enqueue(spool, _update(1, 77, "current before rollback"), "77")

    prior_seq, prior_cursor = _prior_enqueue_shape(
        spool_path,
        _update(2, 77, "prior release during rollback"),
        "77",
        first_seen_at=1_001.0,
    )
    assert prior_seq == 2
    assert prior_cursor == 3
    assert spool.cursor("manager") == 3
    rollback_rows = spool.rows()
    assert [row["update_id"] for row in rollback_rows] == [1, 2]
    assert rollback_rows[0]["state_since"] == rollback_rows[0]["first_seen_at"]
    assert rollback_rows[1]["state_since"] is None

    with sqlite3.connect(spool_path) as connection:
        state_since = next(
            row
            for row in connection.execute("PRAGMA table_info(lane_items)")
            if row[1] == "state_since"
        )
    assert state_since[3] == 0

    _enqueue(
        spool,
        _update(3, 77, "current after rollback"),
        "77",
        first_seen_at=1_002.0,
    )
    assert [row["update_id"] for row in spool.rows()] == [1, 2, 3]
    assert spool.cursor("manager") == 4


def test_enqueue_constraint_failure_is_loud_and_does_not_advance_cursor(
    tmp_path,
) -> None:
    spool_path = tmp_path / "spool.db"
    spool = IngressLaneSpool(spool_path)
    _enqueue(spool, _update(20, 77, "persisted"), "77")
    with sqlite3.connect(spool_path) as connection:
        connection.execute(
            "CREATE UNIQUE INDEX test_one_item_per_lane ON lane_items(lane_key)"
        )

    with pytest.raises(sqlite3.IntegrityError):
        _enqueue(spool, _update(21, 77, "must fail loudly"), "77")

    assert [row["update_id"] for row in spool.rows()] == [20]
    assert spool.cursor("manager") == 21


def test_existing_spool_conservatively_backfills_open_state_transition_clock(
    tmp_path,
) -> None:
    spool_path = tmp_path / "spool.db"
    spool = IngressLaneSpool(spool_path)
    _enqueue(
        spool,
        _update(10, 77, "legacy processing head"),
        "77",
        first_seen_at=1_001.0,
    )
    _enqueue(
        spool,
        _update(11, 77, "legacy pending follower"),
        "77",
        first_seen_at=1_001.0,
    )
    claimed = spool.claim("legacy-worker", now=1_002.0, lease_seconds=2_000.0)
    assert claimed is not None
    assert spool.renew_lease(
        claimed.seq,
        "legacy-worker",
        now=1_999.0,
        lease_seconds=2_000.0,
    )
    with sqlite3.connect(spool_path) as connection:
        connection.execute("ALTER TABLE lane_items DROP COLUMN state_since")

    migrated = IngressLaneSpool(spool_path)
    rows = migrated.rows()

    assert len(rows) == 2
    assert rows[0]["first_seen_at"] == rows[0]["state_since"] == 1_001.0
    assert rows[0]["updated_at"] == 1_999.0
    snapshot = migrated.dispatch_snapshot(now=2_000.0, stall_after_seconds=5.0)
    assert snapshot.stalled_lane_count == 1
    assert snapshot.oldest_stalled_seconds == 999.0

    prior_seq, prior_cursor = _prior_enqueue_shape(
        spool_path,
        _update(12, 88, "prior release after migration"),
        "88",
        first_seen_at=2_000.0,
    )
    assert prior_seq == 3
    assert prior_cursor == 13
    assert migrated.cursor("manager") == 13
    assert [row["update_id"] for row in migrated.rows()] == [10, 11, 12]
    assert migrated.rows()[2]["state_since"] is None


def test_bounded_hold_terminalizes_before_follower_and_preserves_fifo(
    tmp_path,
) -> None:
    """Ambiguous delivery holds ordering for 15 seconds, never for 24 hours."""

    spool = IngressLaneSpool(tmp_path / "spool.db")
    started = 1_000.0
    deadline = started + 86_400.0
    _enqueue(
        spool,
        _update(3, 77, "ambiguous head"),
        "77",
        first_seen_at=started,
        deadline_at=deadline,
    )
    _enqueue(
        spool,
        _update(4, 77, "ordered follower"),
        "77",
        first_seen_at=started,
        deadline_at=deadline,
    )

    head = spool.claim("worker", now=started, lease_seconds=30)
    assert head is not None
    assert head.update_id == 3
    assert spool.mark_blocked(
        head.seq,
        "worker",
        now=started,
        hold_seconds=15,
    )

    # Strict FIFO remains intact until the predecessor becomes terminal.
    assert spool.claim("early-worker", now=started + 14.999) is None
    assert [row["state"] for row in spool.rows()] == ["blocked", "pending"]

    follower = spool.claim("next-worker", now=started + 15.0)
    assert follower is not None
    assert follower.update_id == 4
    assert [row["state"] for row in spool.rows()] == ["done", "processing"]
    assert started + 15.0 < deadline


def test_dispatcher_hold_bound_overrides_long_idle_backoff(
    tmp_path, monkeypatch
) -> None:
    """A 300-second idle cadence cannot extend the bounded hold."""

    spool = IngressLaneSpool(tmp_path / "spool.db")
    _enqueue(spool, _update(5, 77, "ambiguous head"), "77")
    _enqueue(spool, _update(6, 77, "ordered follower"), "77")
    calls: list[tuple[int, float]] = []

    def handle(update, *_args, **_kwargs):
        calls.append((int(update["update_id"]), time.monotonic()))
        return (
            herdres_gateway.CHECKPOINT_HOLD
            if update["update_id"] == 5
            else herdres_gateway.CHECKPOINT_ADVANCE
        )

    monkeypatch.setattr(herdres_gateway, "handle_update", handle)
    dispatcher = herdres_gateway._InboundLaneDispatcher(
        spool,
        REQUEST_ID_KEY,
        workers=1,
        backoff_seconds=300,
        lease_seconds=1,
        hold_seconds=0.1,
    )
    dispatcher.update_specs([("manager", "token", 0)])
    dispatcher.start()
    try:
        _wait_for(
            lambda: [row["state"] for row in spool.rows()] == ["done", "done"],
            timeout=1.0,
        )
    finally:
        dispatcher.stop()

    assert [update_id for update_id, _at in calls] == [5, 6]
    assert 0.1 <= calls[1][1] - calls[0][1] < 1.0


def test_hold_terminalizes_at_bound_while_dispatch_capacity_is_busy(
    tmp_path, monkeypatch
) -> None:
    """Terminal hold expiry is independent of unrelated service capacity."""

    spool = IngressLaneSpool(tmp_path / "spool.db")
    _enqueue(spool, _update(7, 77, "ambiguous head"), "77")
    _enqueue(spool, _update(8, 88, "slow other lane"), "88")
    _enqueue(spool, _update(9, 77, "ordered follower"), "77")
    slow_started = threading.Event()
    release_slow = threading.Event()

    def handle(update, *_args, **_kwargs):
        if update["update_id"] == 7:
            return herdres_gateway.CHECKPOINT_HOLD
        if update["update_id"] == 8:
            slow_started.set()
            assert release_slow.wait(2.0)
        return herdres_gateway.CHECKPOINT_ADVANCE

    monkeypatch.setattr(herdres_gateway, "handle_update", handle)
    dispatcher = herdres_gateway._InboundLaneDispatcher(
        spool,
        REQUEST_ID_KEY,
        workers=1,
        backoff_seconds=300,
        lease_seconds=1,
        hold_seconds=0.1,
    )
    dispatcher.update_specs([("manager", "token", 0)])
    dispatcher.start()
    try:
        assert slow_started.wait(1.0)
        _wait_for(lambda: spool.rows()[0]["state"] == "done", timeout=0.75)
        rows = spool.rows()
        assert rows[1]["state"] == "processing"
        assert rows[2]["state"] == "pending"
    finally:
        release_slow.set()
        dispatcher.stop()


def test_stalled_lane_is_a_structured_doctor_signal(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("HERDRES_INBOUND_LANES", "1")
    spool_path = tmp_path / "spool.db"
    spool = IngressLaneSpool(spool_path)
    started = 2_000.0
    _enqueue(
        spool,
        _update(10, 77, "ambiguous head"),
        "77",
        first_seen_at=started,
        deadline_at=started + 86_400,
    )
    _enqueue(
        spool,
        _update(11, 77, "silenced follower"),
        "77",
        first_seen_at=started,
        deadline_at=started + 86_400,
    )
    head = spool.claim("worker", now=started, lease_seconds=30)
    assert head is not None
    assert spool.mark_blocked(
        head.seq,
        "worker",
        now=started,
        hold_seconds=15,
    )

    before_threshold = doctor.inbound_lanes(
        spool_path,
        now=started + 4.999,
        stall_after_seconds=5,
    )
    assert before_threshold["ok"] is True
    assert before_threshold["signal"] == ""

    signal = doctor.inbound_lanes(
        spool_path,
        now=started + 5.0,
        stall_after_seconds=5,
    )
    assert signal["ok"] is False
    assert signal["status"] == "stalled"
    assert signal["signal"] == "inbound_lane_stalled"
    assert signal["pending"] == 1
    assert signal["claimable"] == 0
    assert signal["blocked"] == 1
    assert signal["stalled_lanes"] == 1
    assert signal["first_stalled_lane"] == lane_key("manager", "77")
    assert signal["oldest_stalled_seconds"] == 5.0


def test_stall_age_tracks_continuous_head_obstruction_and_clears_on_drain(
    tmp_path, monkeypatch
) -> None:
    """Old followers do not age a lane before its current head obstructs it."""

    monkeypatch.setenv("HERDRES_INBOUND_LANES", "1")
    spool_path = tmp_path / "spool.db"
    spool = IngressLaneSpool(spool_path)
    follower_age_started = 1_000.0
    obstruction_started = 2_000.0
    _enqueue(
        spool,
        _update(12, 77, "old head"),
        "77",
        first_seen_at=follower_age_started,
        deadline_at=follower_age_started + 86_400,
    )
    _enqueue(
        spool,
        _update(13, 77, "old follower"),
        "77",
        first_seen_at=follower_age_started,
        deadline_at=follower_age_started + 86_400,
    )

    head = spool.claim("worker", now=obstruction_started, lease_seconds=30)
    assert head is not None
    healthy = doctor.inbound_lanes(
        spool_path,
        now=obstruction_started + 0.001,
        stall_after_seconds=5,
    )
    assert healthy["ok"] is True
    assert healthy["signal"] == ""
    assert healthy["pending"] == 1
    assert healthy["claimable"] == 0
    assert healthy["stalled_lanes"] == 0

    # Lease heartbeats preserve the original processing transition time.
    assert spool.renew_lease(
        head.seq,
        "worker",
        lease_seconds=30,
        now=obstruction_started + 4,
    )
    stalled = doctor.inbound_lanes(
        spool_path,
        now=obstruction_started + 5,
        stall_after_seconds=5,
    )
    assert stalled["ok"] is False
    assert stalled["signal"] == "inbound_lane_stalled"
    assert stalled["oldest_stalled_seconds"] == 5.0

    assert spool.mark_done(head.seq, "worker", now=obstruction_started + 5.1)
    follower = spool.claim(
        "worker",
        now=obstruction_started + 5.1,
        lease_seconds=30,
    )
    assert follower is not None
    assert spool.mark_done(
        follower.seq,
        "worker",
        now=obstruction_started + 5.2,
    )
    drained = doctor.inbound_lanes(
        spool_path,
        now=obstruction_started + 6,
        stall_after_seconds=5,
    )
    assert drained["ok"] is True
    assert drained["signal"] == ""
    assert drained["pending"] == 0
    assert drained["stalled_lanes"] == 0
    assert drained["oldest_stalled_seconds"] == 0.0


def test_busy_lane_does_not_delay_another_agent_under_two_seconds(
    tmp_path, monkeypatch
) -> None:
    spool = IngressLaneSpool(tmp_path / "spool.db")
    _enqueue(spool, _update(1, 77, "A blocks"), "77")
    _enqueue(spool, _update(2, 88, "B flows"), "88")
    blocked = threading.Event()
    release = threading.Event()
    delivered_b = threading.Event()

    def handle(update, *_args, **_kwargs):
        topic = update["message"]["message_thread_id"]
        if topic == 77:
            blocked.set()
            release.wait(2.0)
        else:
            delivered_b.set()
        return herdres_gateway.CHECKPOINT_ADVANCE

    monkeypatch.setattr(herdres_gateway, "handle_update", handle)
    dispatcher = herdres_gateway._InboundLaneDispatcher(
        spool, REQUEST_ID_KEY, workers=2, backoff_seconds=0.01, lease_seconds=5
    )
    dispatcher.update_specs([("manager", "token", 0)])
    started_at = time.monotonic()
    dispatcher.start()
    try:
        assert blocked.wait(1.0)
        assert delivered_b.wait(1.5)
        assert time.monotonic() - started_at < 2.0
    finally:
        release.set()
        dispatcher.stop()


def test_lease_heartbeat_covers_slow_voice_pretranscription(
    tmp_path, monkeypatch
) -> None:
    state_path = tmp_path / "state.json"
    _configured_state(state_path)
    monkeypatch.setenv("HERDR_TELEGRAM_TOPICS_STATE", str(state_path))
    monkeypatch.setenv("HERDRES_SOURCE_TOPIC_MODE", "worker")
    spool = IngressLaneSpool(tmp_path / "spool.db")
    update = _update(3, 77, "")
    message = update["message"]
    assert isinstance(message, dict)
    message.pop("text")
    message["voice"] = {
        "file_id": "slow-voice",
        "file_unique_id": "slow-voice-unique",
        "mime_type": "audio/ogg",
        "file_size": 1024,
        "duration": 3,
    }
    _enqueue(spool, update, "77")
    pretranscription_started = threading.Event()
    pretranscriptions: list[str] = []
    submits: list[str] = []

    def slow_pretranscribe(payload, *, bot_token):
        pretranscriptions.append(bot_token)
        pretranscription_started.set()
        time.sleep(0.45)
        return {
            **payload,
            "_speech_pretranscribed": True,
            "_speech_transcript": "slow voice instruction",
        }

    class Client:
        def command_json(self, request_json):
            submits.append(request_json)
            return _accepted_command_response(json.loads(request_json))

    monkeypatch.setattr(speech, "pretranscribe_voice_payload", slow_pretranscribe)
    monkeypatch.setattr(herdres, "TendwireClient", Client)
    monkeypatch.setattr(
        herdres_gateway,
        "run_herdres_command",
        lambda payload: herdres.command_reply(payload),
    )
    dispatcher = herdres_gateway._InboundLaneDispatcher(
        spool,
        REQUEST_ID_KEY,
        workers=2,
        backoff_seconds=0.01,
        lease_seconds=0.12,
    )
    dispatcher.update_specs([("manager", "token", 0)])
    dispatcher.start()
    try:
        assert pretranscription_started.wait(1.0)
        _wait_for(lambda: spool.rows()[0]["state"] == "blocked")
    finally:
        dispatcher.stop()

    assert pretranscriptions == ["token"]
    assert len(submits) == 1


def test_same_lane_fifo_including_ack_while_other_lane_interleaves(
    tmp_path, monkeypatch
) -> None:
    spool = IngressLaneSpool(tmp_path / "spool.db")
    _enqueue(spool, _update(10, 77, "A1"), "77")
    _enqueue(spool, _update(11, 88, "B1"), "88")
    _enqueue(spool, _update(12, 77, "A2"), "77")
    a1_started = threading.Event()
    release_a1 = threading.Event()
    b_done = threading.Event()
    events: list[str] = []
    events_lock = threading.Lock()

    def record(value: str) -> None:
        with events_lock:
            events.append(value)

    def handle(update, *_args, **_kwargs):
        text = update["message"]["text"]
        record(f"start-{text}")
        if text == "A1":
            a1_started.set()
            release_a1.wait(2.0)
        if text == "B1":
            b_done.set()
        record(f"ack-{text}")
        return herdres_gateway.CHECKPOINT_ADVANCE

    monkeypatch.setattr(herdres_gateway, "handle_update", handle)
    dispatcher = herdres_gateway._InboundLaneDispatcher(
        spool, REQUEST_ID_KEY, workers=3, backoff_seconds=0.01, lease_seconds=5
    )
    dispatcher.update_specs([("manager", "token", 0)])
    dispatcher.start()
    try:
        assert a1_started.wait(1.0)
        assert b_done.wait(1.0)
        time.sleep(0.05)
        assert "start-A2" not in events
        release_a1.set()
        _wait_for(lambda: all(row["state"] == "done" for row in spool.rows()))
    finally:
        release_a1.set()
        dispatcher.stop()

    assert events.index("start-B1") < events.index("ack-A1")
    assert events.index("ack-A1") < events.index("start-A2")
    assert events.index("start-A2") < events.index("ack-A2")


def test_owner_commands_retain_receiver_wide_fifo(tmp_path, monkeypatch) -> None:
    spool = IngressLaneSpool(tmp_path / "spool.db")
    first = _update(20, 77, "/status")
    second = _update(21, 88, "/help")
    _enqueue(spool, first, "__control__")
    _enqueue(spool, second, "__control__")
    first_started = threading.Event()
    release = threading.Event()
    calls: list[str] = []

    def handle(update, *_args, **_kwargs):
        text = update["message"]["text"]
        calls.append(text)
        if text == "/status":
            first_started.set()
            release.wait(2.0)
        return herdres_gateway.CHECKPOINT_ADVANCE

    monkeypatch.setattr(herdres_gateway, "handle_update", handle)
    dispatcher = herdres_gateway._InboundLaneDispatcher(
        spool, REQUEST_ID_KEY, workers=2, lease_seconds=5
    )
    dispatcher.update_specs([("manager", "token", 0)])
    dispatcher.start()
    try:
        assert first_started.wait(1.0)
        time.sleep(0.05)
        assert calls == ["/status"]
        release.set()
        _wait_for(lambda: len(calls) == 2)
    finally:
        release.set()
        dispatcher.stop()
    assert calls == ["/status", "/help"]


def test_owner_command_lane_is_separate_from_general_chatter(monkeypatch) -> None:
    store = _store()
    monkeypatch.setattr(config, "general_thread_id", lambda _store: "77")

    command = herdres_gateway._preview_lane_update(
        _update(22, 77, "/status"),
        store,
        receiver_kind="manager",
        request_id_key=REQUEST_ID_KEY,
        bot_key="manager",
    )
    chatter = herdres_gateway._preview_lane_update(
        _update(23, 77, "how is everyone?"),
        store,
        receiver_kind="manager",
        request_id_key=REQUEST_ID_KEY,
        bot_key="manager",
    )

    assert command["lane_key"] == lane_key("manager", "__owner_commands__")
    assert chatter["lane_key"] == lane_key("manager", "__general__")


def test_unresolved_topic_keeps_same_provisional_lane_after_resolution() -> None:
    store = _store()
    unresolved = herdres_gateway._preview_lane_update(
        _update(24, 99, "first"),
        store,
        receiver_kind="manager",
        request_id_key=REQUEST_ID_KEY,
        bot_key="manager",
    )
    state.upsert_worker_entry(
        store,
        _source_worker(
            {
                "id": "worker-new",
                "name": "worker-new",
                "status": "idle",
                "space_id": "space-1",
                "fingerprint": "fp-new",
            }
        ),
        topic_id="99",
    )
    resolved = herdres_gateway._preview_lane_update(
        _update(25, 99, "second"),
        store,
        receiver_kind="manager",
        request_id_key=REQUEST_ID_KEY,
        bot_key="manager",
    )

    assert unresolved["lane_key"] == lane_key("manager", "99")
    assert resolved["lane_key"] == unresolved["lane_key"]


def test_poison_head_quarantines_visibly_without_delaying_other_lane(
    tmp_path, monkeypatch
) -> None:
    state_path = tmp_path / "state.json"
    _configured_state(state_path)
    monkeypatch.setenv("HERDR_TELEGRAM_TOPICS_STATE", str(state_path))
    monkeypatch.setenv("HERDRES_SOURCE_TOPIC_MODE", "worker")
    monkeypatch.setenv("HERDRES_INBOUND_SUCCESS_ACK", "1")
    monkeypatch.setenv("HERDRES_INBOUND_LANES", "1")
    monkeypatch.setattr(config, "command_retry_horizon_seconds", lambda _env=None: 0.5)
    monkeypatch.setattr(config, "command_request_retention_seconds", lambda _env=None: 1.0)
    spool = IngressLaneSpool(tmp_path / "spool.db")
    first_seen = time.time()
    _enqueue(
        spool,
        _update(30, 77, "poison A"),
        "77",
        first_seen_at=first_seen,
        deadline_at=first_seen + 0.5,
    )
    _enqueue(spool, _update(31, 88, "healthy B"), "88")
    backend_events: list[tuple[str, float]] = []
    notices: list[tuple[str, str, float]] = []

    class Client:
        def command_json(self, request_json):
            request = json.loads(request_json)
            worker_id = request["target"]["worker_id"]
            backend_events.append((worker_id, time.monotonic()))
            if worker_id == "worker-a":
                return _failed_command_response(
                    request, status="in_progress", disposition="in_progress"
                )
            return _accepted_command_response(request)

    class Telegram:
        def __init__(self, token):
            self.token = token

        def send_message(self, chat_id, reply, **kwargs):
            notices.append((str(kwargs.get("thread_id")), reply, time.monotonic()))
            return {"ok": True, "message_id": "1"}

    monkeypatch.setattr(herdres, "TendwireClient", Client)
    monkeypatch.setattr(
        herdres_gateway,
        "run_herdres_command",
        lambda payload: herdres.command_reply(payload),
    )
    monkeypatch.setattr(herdres_gateway, "TelegramClient", Telegram)
    dispatcher = herdres_gateway._InboundLaneDispatcher(
        spool, REQUEST_ID_KEY, workers=2, backoff_seconds=0.05, lease_seconds=2
    )

    def durable_spool_outcomes() -> bool:
        states = {row["update_id"]: row["state"] for row in spool.rows()}
        return states.get(30) in {"blocked", "done"} and states.get(31) == "blocked"

    dispatcher.update_specs([("manager", "token", 0)])
    dispatcher.start()
    try:
        _wait_for(durable_spool_outcomes)
    finally:
        dispatcher.stop()

    backend_workers = [worker for worker, _at in backend_events]
    assert set(backend_workers) == {"worker-a", "worker-b"}
    assert backend_workers.count("worker-b") == 1
    assert notices == []
    records = state.load_state()[ingress_requests.RECORDS_KEY]
    poison = records[_request_id(_update(30, 77, "poison A"))]
    healthy = records[_request_id(_update(31, 88, "healthy B"))]
    assert poison["state"] == "quarantined"
    assert poison["terminal_outcome"] == "not_delivered"
    assert poison["operator_attention_required"] is True
    assert poison["outcome"]["checkpoint"] == "hold"
    assert healthy["state"] == "terminal"
    assert healthy["request_phase"] == "accepted_unverified"
    assert healthy["transport_disposition"] == "written_to_pty"
    assert healthy["terminal_outcome"] == "delivery_unknown"
    assert healthy["outcome"]["checkpoint"] == "hold"

    rows = {row["update_id"]: row for row in spool.rows()}
    assert rows[30]["state"] in {"blocked", "done"}
    assert rows[30]["next_attempt_at"] == rows[30]["deadline_at"]
    assert rows[30]["lease_owner"] is None
    assert rows[30]["lease_until"] is None
    assert rows[31]["state"] == "blocked"
    assert rows[31]["next_attempt_at"] > rows[31]["first_seen_at"]
    assert rows[31]["lease_owner"] is None
    assert rows[31]["lease_until"] is None


def test_lane_overflow_notifies_once_and_advances_cursor(tmp_path, monkeypatch) -> None:
    spool = IngressLaneSpool(tmp_path / "spool.db")
    spool.initialize_cursor("manager", 41)
    _enqueue(spool, _update(40, 77, "already queued"), "77")
    notices: list[tuple[str, str]] = []
    mirrors: list[int] = []
    durable_commit_wakes = 0
    monkeypatch.setenv("HERDRES_INBOUND_LANE_DEPTH", "1")
    monkeypatch.setenv("HERDRES_SOURCE_TOPIC_MODE", "worker")
    store = _store()
    state.upsert_worker_entry(
        store,
        _source_worker(
            {
                "id": "worker-a",
                "name": "worker-a",
                "status": "idle",
                "space_id": "space-1",
                "fingerprint": "fp-a",
            }
        ),
        topic_id="77",
    )
    monkeypatch.setattr(herdres_gateway.state, "load_state", lambda: store)
    monkeypatch.setattr(
        herdres_gateway,
        "get_updates",
        lambda *_args, **_kwargs: [_update(41, 77, "overflow")],
    )
    monkeypatch.setattr(
        herdres_gateway,
        "_notify_lane_overflow",
        lambda _token, route, lane: notices.append((route["topic_id"], lane)),
    )
    monkeypatch.setattr(
        herdres_gateway, "_save_offset", lambda offset, _key: mirrors.append(offset)
    )

    def wake_after_commit() -> None:
        nonlocal durable_commit_wakes
        durable_commit_wakes += 1

    herdres_gateway._poll_once_lanes(
        "manager",
        "token",
        timeout_seconds=0,
        request_id_key=REQUEST_ID_KEY,
        spool=spool,
        on_enqueue=wake_after_commit,
    )

    assert spool.cursor("manager") == 42
    assert len(spool.rows()) == 1
    assert notices == [("77", lane_key("manager", "77"))]
    assert mirrors == [42]
    assert durable_commit_wakes == 1


def test_lane_overflow_notice_uses_real_sixty_second_throttle(
    tmp_path, monkeypatch
) -> None:
    sent: list[tuple[str, str, str]] = []
    now = [1000.0]
    lane = lane_key("manager", f"overflow-{tmp_path.name}")
    route = {"chat_id": "-100", "topic_id": "77", "message_id": "1041"}

    class Telegram:
        def __init__(self, token):
            self.token = token

        def send_message(self, chat_id, message, **kwargs):
            sent.append((chat_id, message, str(kwargs.get("thread_id"))))
            return {"ok": True, "message_id": "1"}

    monkeypatch.setattr(herdres_gateway.time, "time", lambda: now[0])
    monkeypatch.setattr(herdres_gateway, "TelegramClient", Telegram)

    herdres_gateway._notify_lane_overflow("token", route, lane)
    _wait_for(lambda: len(sent) == 1)
    now[0] += 59.0
    herdres_gateway._notify_lane_overflow("token", route, lane)
    time.sleep(0.05)
    assert len(sent) == 1
    now[0] += 1.0
    herdres_gateway._notify_lane_overflow("token", route, lane)
    _wait_for(lambda: len(sent) == 2)

    assert [topic for _chat, _message, topic in sent] == ["77", "77"]


def test_cursor_commit_survives_failure_before_legacy_mirror(tmp_path, monkeypatch) -> None:
    spool_path = tmp_path / "spool.db"
    spool = IngressLaneSpool(spool_path)
    spool.initialize_cursor("manager", 50)
    offsets_seen: list[int] = []
    update = _update(50, 77, "durable")
    monkeypatch.setattr(herdres_gateway.state, "load_state", _store)

    def updates(_token, offset, *, timeout_seconds):
        offsets_seen.append(offset)
        return [update] if offset == 50 else []

    monkeypatch.setattr(herdres_gateway, "get_updates", updates)
    monkeypatch.setattr(
        herdres_gateway,
        "_save_offset",
        lambda *_args: (_ for _ in ()).throw(OSError("simulated crash window")),
    )

    herdres_gateway._poll_once_lanes(
        "manager", "token", timeout_seconds=0, request_id_key=REQUEST_ID_KEY, spool=spool
    )
    reopened_spool = IngressLaneSpool(spool_path)
    herdres_gateway._poll_once_lanes(
        "manager",
        "token",
        timeout_seconds=0,
        request_id_key=REQUEST_ID_KEY,
        spool=reopened_spool,
    )

    assert offsets_seen == [50, 51]
    assert reopened_spool.cursor("manager") == 51
    assert len(reopened_spool.rows()) == 1


def test_first_lane_start_migrates_legacy_receiver_cursor(tmp_path, monkeypatch) -> None:
    base = tmp_path / "gateway.offset"
    base.write_text("91", encoding="utf-8")
    spool = IngressLaneSpool(tmp_path / "spool.db")
    offsets: list[int] = []
    monkeypatch.setattr(herdres_gateway.config, "offset_path", lambda: base)
    monkeypatch.setattr(
        herdres_gateway,
        "get_updates",
        lambda _token, offset, *, timeout_seconds: offsets.append(offset) or [],
    )

    herdres_gateway._poll_once_lanes(
        "manager", "token", timeout_seconds=0, request_id_key=REQUEST_ID_KEY, spool=spool
    )

    assert offsets == [91]
    assert spool.cursor("manager") == 91


def test_unverified_ingress_cache_never_marks_refetched_update_done(
    tmp_path, monkeypatch
) -> None:
    spool = IngressLaneSpool(tmp_path / "spool.db")
    spool.initialize_cursor("manager", 95)
    update = _update(95, 77, "already accepted")
    request_id = _request_id(update)
    store = _store()
    record, _created = ingress_requests.ensure_request_shell(
        store, request_id, now=10.0, retry_horizon=60, retention=120
    )
    request_json = ingress_requests.canonical_request_json(
        {
            "schema_version": 1,
            "action": "send_instruction",
            "request_id": request_id,
            "dry_run": False,
            "target": {"worker_id": "worker-a"},
            "instruction": {"text": "already accepted"},
        }
    )
    ingress_requests.attach_request_json(record, request_json, now=11.0)
    ingress_requests.mark_terminal(
        record,
        "terminal_accepted",
        now=12.0,
        reply="Sent to Tendwire worker.",
    )
    monkeypatch.setattr(herdres_gateway.state, "load_state", lambda: store)
    monkeypatch.setattr(
        herdres_gateway,
        "get_updates",
        lambda *_args, **_kwargs: [update],
    )
    monkeypatch.setattr(herdres_gateway, "_save_offset", lambda *_args: None)

    herdres_gateway._poll_once_lanes(
        "manager", "token", timeout_seconds=0, request_id_key=REQUEST_ID_KEY, spool=spool
    )

    rows = spool.rows()
    assert len(rows) == 1
    assert rows[0]["state"] == "pending"
    assert rows[0]["notify_state"] == "pending"


def test_feature_flag_off_uses_legacy_synchronous_path(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("HERDRES_INBOUND_LANES", "0")
    handled: list[int] = []
    saved: list[int] = []
    monkeypatch.setattr(herdres_gateway, "_read_offset", lambda _key: 60)
    monkeypatch.setattr(
        herdres_gateway,
        "get_updates",
        lambda *_args, **_kwargs: [_update(60, 77, "legacy")],
    )
    monkeypatch.setattr(
        herdres_gateway,
        "handle_update",
        lambda update, *_args, **_kwargs: (
            handled.append(update["update_id"]) or herdres_gateway.CHECKPOINT_ADVANCE
        ),
    )
    monkeypatch.setattr(
        herdres_gateway, "_save_offset", lambda offset, _key: saved.append(offset)
    )
    monkeypatch.setattr(
        herdres_gateway,
        "IngressLaneSpool",
        lambda *_args, **_kwargs: pytest.fail("flag-off path opened the spool"),
    )

    herdres_gateway._poll_once(
        "manager", "token", timeout_seconds=0, request_id_key=REQUEST_ID_KEY
    )

    assert handled == [60]
    assert saved == [61]


def _kill_stage_child(
    spool_path: str,
    update: dict[str, object],
    stage: str,
    ready,
) -> None:
    spool = IngressLaneSpool(spool_path)
    if stage == "before_dispatch":
        _enqueue(spool, update, "77")
        ready.send("enqueued")
    else:
        item = spool.claim("killed-dispatcher", lease_seconds=120)
        assert item is not None
        if stage == "after_claim":
            ready.send("claimed")
            time.sleep(60)
            return
        if stage == "after_ack_claim":
            assert spool.claim_notification(item.seq)
            ready.send("ack-claimed")
            time.sleep(60)
            return
        checkpoint = herdres_gateway.handle_update(
            item.update,
            "token",
            receiver_id=item.receiver_kind,
            request_id_key=REQUEST_ID_KEY,
            bot_key="manager",
            ingress_first_seen_at=item.first_seen_at,
        )
        assert checkpoint == herdres_gateway.CHECKPOINT_HOLD
        if stage == "after_dispatch":
            ready.send("terminal-cached")
        elif stage == "after_done":
            assert spool.mark_blocked(item.seq, "killed-dispatcher")
            ready.send("done")
    time.sleep(60)


@pytest.mark.parametrize(
    "stage",
    [
        "before_dispatch",
        "after_claim",
        "after_ack_claim",
        "during_command_json",
        "after_dispatch",
        "after_done",
    ],
)
def test_kill_9_restart_submits_each_request_exactly_once(
    stage, tmp_path, monkeypatch
) -> None:
    if "fork" not in multiprocessing.get_all_start_methods():
        pytest.skip("kill -9 regression requires fork")
    state_path = tmp_path / "state.json"
    _configured_state(state_path)
    monkeypatch.setenv("HERDR_TELEGRAM_TOPICS_STATE", str(state_path))
    monkeypatch.setenv("HERDRES_SOURCE_TOPIC_MODE", "worker")
    spool_path = tmp_path / "spool.db"
    spool = IngressLaneSpool(spool_path)
    update_id = {
        "before_dispatch": 70,
        "after_claim": 71,
        "after_ack_claim": 72,
        "during_command_json": 73,
        "after_dispatch": 74,
        "after_done": 75,
    }[stage]
    update = _update(update_id, 77, stage)
    if stage != "before_dispatch":
        _enqueue(spool, update, "77")
    context = multiprocessing.get_context("fork")
    rpc_attempts = context.Value("i", 0)
    logical_submits = context.Value("i", 0)
    byte_conflicts = context.Value("i", 0)
    receipt_path = tmp_path / "fake-tendwire-receipt.json"
    parent, child = context.Pipe(duplex=False)

    class Client:
        def command_json(self, request_json):
            with rpc_attempts.get_lock():
                rpc_attempts.value += 1
            created = False
            try:
                fd = os.open(receipt_path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
            except FileExistsError:
                if receipt_path.read_text(encoding="utf-8") != request_json:
                    with byte_conflicts.get_lock():
                        byte_conflicts.value += 1
            else:
                created = True
                with os.fdopen(fd, "w", encoding="utf-8") as receipt:
                    receipt.write(request_json)
                    receipt.flush()
                    os.fsync(receipt.fileno())
                with logical_submits.get_lock():
                    logical_submits.value += 1
            if stage == "during_command_json" and created:
                child.send("mid-rpc")
                time.sleep(60)
            return _accepted_command_response(json.loads(request_json))

    class Telegram:
        def __init__(self, token):
            self.token = token

        def send_message(self, *_args, **_kwargs):
            return {"ok": True, "message_id": "1"}

    monkeypatch.setattr(herdres, "TendwireClient", Client)
    monkeypatch.setattr(
        herdres_gateway,
        "run_herdres_command",
        lambda payload: herdres.command_reply(payload),
    )
    monkeypatch.setattr(herdres_gateway, "TelegramClient", Telegram)
    process = context.Process(
        target=_kill_stage_child,
        args=(str(spool_path), update, stage, child),
    )
    process.start()
    assert parent.poll(5.0)
    assert parent.recv() in {
        "enqueued",
        "claimed",
        "ack-claimed",
        "mid-rpc",
        "terminal-cached",
        "done",
    }
    os.kill(process.pid, signal.SIGKILL)
    process.join(5.0)
    assert process.exitcode == -signal.SIGKILL
    dispatcher = herdres_gateway._InboundLaneDispatcher(
        IngressLaneSpool(spool_path),
        REQUEST_ID_KEY,
        workers=1,
        backoff_seconds=0.01,
        lease_seconds=1,
    )
    dispatcher.update_specs([("manager", "token", 0)])
    dispatcher.start()
    try:
        _wait_for(
            lambda: all(row["state"] == "blocked" for row in dispatcher.spool.rows())
        )
    finally:
        dispatcher.stop()

    assert logical_submits.value == 1
    assert byte_conflicts.value == 0
    assert rpc_attempts.value == (2 if stage == "during_command_json" else 1)


def test_tendwire_submit_releases_state_lock_and_preserves_concurrent_write(
    tmp_path, monkeypatch
) -> None:
    state_path = tmp_path / "state.json"
    _configured_state(state_path)
    monkeypatch.setenv("HERDR_TELEGRAM_TOPICS_STATE", str(state_path))
    monkeypatch.setenv("HERDRES_SOURCE_TOPIC_MODE", "worker")
    entered = threading.Event()
    release = threading.Event()

    class Client:
        def command_json(self, request_json):
            entered.set()
            assert release.wait(2.0)
            return _accepted_command_response(json.loads(request_json))

    monkeypatch.setattr(herdres, "TendwireClient", Client)
    result: dict[str, object] = {}

    def submit() -> None:
        result.update(
            herdres.command_reply(
                {
                    "request_id": _request_id(_update(80, 77, "off lock")),
                    "topic_id": "77",
                    "message_id": "1080",
                    "text": "off lock",
                }
            )
        )

    thread = threading.Thread(target=submit)
    thread.start()
    assert entered.wait(1.0)
    acquired_at = time.monotonic()
    with state.state_lock():
        concurrent = state.load_state()
        concurrent["concurrent_write"] = True
        state.save_state(concurrent)
    assert time.monotonic() - acquired_at < 0.5
    release.set()
    thread.join(3.0)

    assert result["checkpoint"] == herdres_gateway.CHECKPOINT_HOLD
    assert result["transport_disposition"] == "written_to_pty"
    assert result["request_phase"] == "accepted_unverified"
    assert result["terminal_outcome"] == "delivery_unknown"
    final = state.load_state()
    assert final["concurrent_write"] is True
    assert final[ingress_requests.RECORDS_KEY][
        _request_id(_update(80, 77, "off lock"))
    ]["state"] == "terminal"


def test_lane_configuration_defaults_and_bounds() -> None:
    assert config.inbound_lanes_enabled({}) is True
    assert config.inbound_lanes_enabled({"HERDRES_INBOUND_LANES": "0"}) is False
    assert config.inbound_lanes_enabled({"HERDRES_INBOUND_LANES": "1"}) is True
    assert config.inbound_dispatch_workers({}) == 8
    assert config.inbound_dispatch_workers({"HERDRES_INBOUND_DISPATCH_WORKERS": "0"}) == 1
    assert config.inbound_lane_depth({}) == 32
    assert config.inbound_lane_depth({"HERDRES_INBOUND_LANE_DEPTH": "5000"}) == 4096
    assert config.inbound_lane_backoff_seconds({}) == 2.0
    assert config.inbound_hold_seconds({}) == 15.0
    assert config.inbound_hold_seconds({"HERDRES_INBOUND_HOLD_SECONDS": "0"}) == 1.0
    assert config.inbound_hold_seconds({"HERDRES_INBOUND_HOLD_SECONDS": "120"}) == 60.0
    assert config.inbound_lane_stall_seconds({}) == 5.0
    assert (
        config.inbound_lane_stall_seconds(
            {"HERDRES_INBOUND_LANE_STALL_SECONDS": "120"}
        )
        == 60.0
    )
    assert config.gateway_timing_logs_enabled({}) is True
    assert (
        config.gateway_timing_logs_enabled({"HERDRES_GATEWAY_TIMING_LOGS": "0"})
        is False
    )


def test_slow_terminal_ack_does_not_delay_submission_or_lane_completion(
    tmp_path, monkeypatch
) -> None:
    spool = IngressLaneSpool(tmp_path / "spool.db")
    update = _update(90, 77, "ship it")
    _enqueue(spool, update, "77")
    events: list[str] = []
    ack_started = threading.Event()
    release_ack = threading.Event()

    class Telegram:
        def __init__(self, token):
            self.token = token

        def send_message(self, _chat_id, text, **_kwargs):
            events.append(f"telegram-start:{text}")
            ack_started.set()
            assert release_ack.wait(2.0)
            events.append(f"telegram-done:{text}")
            return {"ok": True, "message_id": "ack-1"}

    def handle(*_args, deferred_reply=None, **_kwargs):
        events.append("submit")
        assert deferred_reply is not None
        deferred_reply("Sent to Tendwire worker.")
        return herdres_gateway.CHECKPOINT_ADVANCE

    monkeypatch.setenv("HERDRES_INBOUND_SUCCESS_ACK", "1")
    monkeypatch.setattr(herdres_gateway, "TelegramClient", Telegram)
    monkeypatch.setattr(herdres_gateway, "handle_update", handle)
    dispatcher = herdres_gateway._InboundLaneDispatcher(
        spool, REQUEST_ID_KEY, workers=1, backoff_seconds=0.01, lease_seconds=1
    )
    dispatcher.update_specs([("manager", "token", 0)])
    dispatcher.start()
    try:
        assert ack_started.wait(1.0)
        _wait_for(lambda: spool.rows()[0]["state"] == "done")
        assert events[:2] == [
            "submit",
            "telegram-start:Sent to Tendwire worker.",
        ]
        assert "telegram-done:Sent to Tendwire worker." not in events
        release_ack.set()
        _wait_for(lambda: spool.rows()[0]["notify_state"] == "sent")
    finally:
        release_ack.set()
        dispatcher.stop()

    assert events == [
        "submit",
        "telegram-start:Sent to Tendwire worker.",
        "telegram-done:Sent to Tendwire worker.",
    ]
    assert spool.rows()[0]["notify_state"] == "sent"


def test_direct_command_keeps_success_reply_without_instant_ack(
    tmp_path, monkeypatch
) -> None:
    state_path = tmp_path / "state.json"
    _configured_state(state_path)
    monkeypatch.setenv("HERDR_TELEGRAM_TOPICS_STATE", str(state_path))
    monkeypatch.setenv("HERDRES_SOURCE_TOPIC_MODE", "worker")
    monkeypatch.setenv("HERDRES_INBOUND_SUCCESS_ACK", "0")
    monkeypatch.setenv("HERDRES_INBOUND_LANES", "1")

    class Client:
        def command_json(self, request_json):
            return _accepted_command_response(json.loads(request_json))

    monkeypatch.setattr(herdres, "TendwireClient", Client)
    result = herdres.command_reply(
        {
            "request_id": _request_id(_update(89, 77, "direct")),
            "topic_id": "77",
            "message_id": "1089",
            "text": "direct",
        }
    )

    assert result["checkpoint"] == herdres_gateway.CHECKPOINT_HOLD
    assert result["transport_disposition"] == "written_to_pty"
    assert result["request_phase"] == "accepted_unverified"
    assert result["terminal_outcome"] == "delivery_unknown"
    assert result["reply"] == ""


def test_gateway_default_suppresses_terminal_success_and_cached_replay(
    tmp_path, monkeypatch
) -> None:
    state_path = tmp_path / "state.json"
    _configured_state(state_path)
    monkeypatch.setenv("HERDR_TELEGRAM_TOPICS_STATE", str(state_path))
    monkeypatch.setenv("HERDRES_SOURCE_TOPIC_MODE", "worker")
    monkeypatch.delenv("HERDRES_INBOUND_SUCCESS_ACK", raising=False)
    command_calls: list[str] = []
    working_cards: list[str] = []
    replies: list[str] = []

    class Client:
        def command_json(self, request_json):
            command_calls.append(request_json)
            response = _accepted_command_response(json.loads(request_json))
            response["result"]["submission_id"] = "submission-1"
            return response

    monkeypatch.setattr(herdres, "TendwireClient", Client)
    monkeypatch.setattr(
        herdres,
        "deliver_submission_working_card",
        lambda _store, request_id, _runtime, **_kwargs: (
            working_cards.append(request_id)
            or {"ok": True, "status": "delivered", "sent": 1}
        ),
    )
    monkeypatch.setattr(
        herdres_gateway,
        "run_herdres_command",
        lambda payload: herdres.command_reply(payload),
    )
    update = _update(90, 77, "ship it")
    message = update["message"]
    assert isinstance(message, dict)

    for _attempt in range(2):
        assert (
            herdres_gateway.handle_message(
                message,
                "token",
                update_id=90,
                receiver_id="manager",
                request_id_key=REQUEST_ID_KEY,
                deferred_reply=replies.append,
            )
            == herdres_gateway.CHECKPOINT_HOLD
        )

    request_id = _request_id(update)
    record = state.load_state(state_path)[ingress_requests.RECORDS_KEY][request_id]
    assert len(command_calls) == 1
    assert working_cards == [request_id]
    assert replies == []
    assert record["outcome"]["reply"] == ""


def test_ack_claim_ambiguity_is_never_retried(tmp_path, monkeypatch) -> None:
    spool = IngressLaneSpool(tmp_path / "spool.db")
    update = _update(91, 77, "ambiguous ack")
    _enqueue(spool, update, "77")
    item = spool.claim("worker", lease_seconds=10)
    assert item is not None
    attempts: list[str] = []

    class Telegram:
        def __init__(self, token):
            self.token = token

        def send_message(self, *_args, **_kwargs):
            attempts.append("send")
            raise RuntimeError("connection lost after write")

    monkeypatch.setenv("HERDRES_INBOUND_SUCCESS_ACK", "1")
    monkeypatch.setattr(herdres_gateway, "TelegramClient", Telegram)
    dispatcher = herdres_gateway._InboundLaneDispatcher(
        spool, REQUEST_ID_KEY, workers=1
    )
    dispatcher._defer_reply(item, "token", "Sent.")
    dispatcher._defer_reply(item, "token", "Sent.")
    notification_queue = dispatcher._notification_queues[0]
    worker = threading.Thread(
        target=dispatcher._notification_worker,
        args=(notification_queue,),
    )
    worker.start()
    _wait_for(lambda: attempts == ["send"])
    dispatcher._stop.set()
    notification_queue.put_nowait(None)
    worker.join(1.0)

    assert attempts == ["send"]
    assert spool.rows()[0]["notify_state"] == "claimed"


def test_reclaim_processing_preserves_retry_and_notification_evidence(
    tmp_path,
) -> None:
    spool = IngressLaneSpool(tmp_path / "spool.db")
    update = _update(92, 77, "reclaim")
    _enqueue(spool, update, "77")
    clock = time.time()
    item = spool.claim("dead-worker", now=clock, lease_seconds=1)
    assert item is not None
    assert spool.claim_notification(item.seq, now=clock + 0.1)
    assert spool.reclaim_processing(now=clock + 1.0) == 1

    rows = spool.rows()
    assert rows[0]["state"] == "pending"
    assert rows[0]["notify_state"] == "claimed"
    reclaimed = spool.claim("new-worker", now=clock + 1.0, lease_seconds=1)
    assert reclaimed is not None
    assert reclaimed.request_id == item.request_id


@pytest.mark.parametrize(
    ("outcome", "success_ack", "expected_reply"),
    [
        ("busy_success", "1", herdres.BUSY_SEND_REPLY),
        ("busy_success", "0", ""),
        ("queued", "0", "Queued for Tendwire worker."),
        ("rejected", "0", herdres.SAFE_SEND_FAILURE_REPLY),
        ("uncertain", "0", herdres.SAFE_SEND_FAILURE_REPLY),
    ],
)
def test_production_lane_ack_order_and_terminal_reply_policy(
    outcome, success_ack, expected_reply, tmp_path, monkeypatch
) -> None:
    state_path = tmp_path / "state.json"
    _configured_state(state_path)
    monkeypatch.setenv("HERDR_TELEGRAM_TOPICS_STATE", str(state_path))
    monkeypatch.setenv("HERDRES_SOURCE_TOPIC_MODE", "worker")
    monkeypatch.setenv("HERDRES_INBOUND_SUCCESS_ACK", success_ack)
    monkeypatch.setenv("HERDRES_INBOUND_LANES", "1")
    spool = IngressLaneSpool(tmp_path / "spool.db")
    update_ids = {
        "busy_success": 93 if success_ack == "1" else 94,
        "queued": 95,
        "rejected": 96,
        "uncertain": 97,
    }
    update = _update(update_ids[outcome], 77, "go")
    _enqueue(spool, update, "77")
    events: list[str] = []

    class Client:
        def command_json(self, request_json):
            events.append("submit")
            request = json.loads(request_json)
            if outcome in {"rejected", "uncertain"}:
                return _failed_command_response(
                    request,
                    status=(
                        "target_not_found"
                        if outcome == "rejected"
                        else "request_state_uncertain"
                    ),
                    disposition=(
                        "terminal_rejected"
                        if outcome == "rejected"
                        else "terminal_uncertain"
                    ),
                )
            accepted = _accepted_command_response(request)
            if outcome == "busy_success":
                accepted["result"]["target_state_at_send"] = "working"
            else:
                accepted["result"]["delivery_state"] = "queued"
            return accepted

    class Telegram:
        def __init__(self, token):
            self.token = token

        def send_message(self, _chat_id, text, **_kwargs):
            events.append(f"telegram:{text}")
            return {"ok": True, "message_id": str(len(events))}

    monkeypatch.setattr(herdres, "TendwireClient", Client)
    monkeypatch.setattr(herdres_gateway, "TelegramClient", Telegram)
    monkeypatch.setattr(
        herdres_gateway,
        "run_herdres_command",
        lambda payload: herdres.command_reply(payload),
    )
    dispatcher = herdres_gateway._InboundLaneDispatcher(
        spool, REQUEST_ID_KEY, workers=1, backoff_seconds=0.01, lease_seconds=1
    )
    dispatcher.update_specs([("manager", "token", 0)])
    dispatcher.start()
    try:
        _wait_for(lambda: spool.rows()[0]["state"] == "blocked")
    finally:
        dispatcher.stop()

    assert events[0] == "submit"
    record = state.load_state(state_path)[ingress_requests.RECORDS_KEY][
        _request_id(update)
    ]
    assert events == ["submit"]
    assert record["outcome"]["reply"] == (
        herdres.SAFE_SEND_FAILURE_REPLY if outcome == "uncertain" else ""
    )
    assert record["outcome"]["checkpoint"] == "hold"


def test_poll_worker_wakes_real_dispatcher_after_every_durable_commit(
    tmp_path, monkeypatch
) -> None:
    """Production startup and polling must not defer claims to reconcile cohorts."""

    state_path = tmp_path / "state.json"
    _configured_state(state_path)
    monkeypatch.setenv("HERDR_TELEGRAM_TOPICS_STATE", str(state_path))
    monkeypatch.setenv("HERDRES_SOURCE_TOPIC_MODE", "worker")
    monkeypatch.setenv("HERDRES_INBOUND_LANES", "1")
    monkeypatch.setenv("HERDRES_INBOUND_DISPATCH_WORKERS", "8")
    monkeypatch.setenv("HERDRES_INBOUND_LANE_BACKOFF_SECONDS", "2")
    monkeypatch.setattr(herdres_gateway, "LONG_POLL_SECONDS", 0)
    spool = IngressLaneSpool(tmp_path / "spool.db")
    spool.initialize_cursor("manager", 200)
    pending_updates = [
        _update(update_id, 77, f"cohort-{update_id}")
        for update_id in range(200, 210)
    ]
    poll_batches: list[int] = []
    poll_drained = threading.Event()
    release_poll = threading.Event()
    release_dispatch = threading.Event()
    timing_lock = threading.Lock()
    dispatcher_diagnostics: list[tuple[str, str]] = []
    enqueue_diagnostics: list[str] = []
    timings: dict[str, dict[int, float]] = {
        "durable_enqueue_commit": {},
        "dispatcher_claim": {},
    }

    def fake_get_updates(_token, offset, *, timeout_seconds):
        assert timeout_seconds == 0
        assert ready_for_enqueue.wait(1.0)
        if not pending_updates:
            poll_drained.set()
            release_poll.wait(3.0)
            return []
        update = pending_updates.pop(0)
        assert update["update_id"] == offset
        # Model separate Telegram responses instead of handing the dispatcher a
        # synthetic in-process burst.
        time.sleep(0.03)
        poll_batches.append(1)
        return [update]

    def capture_timing(hop, *, update_id=None, detail="", **_kwargs):
        if hop.startswith("dispatcher_"):
            with timing_lock:
                dispatcher_diagnostics.append((hop, str(detail)))
        if hop == "durable_enqueue_commit":
            with timing_lock:
                enqueue_diagnostics.append(str(detail))
            if update_id == 209:
                release_dispatch.set()
        if hop not in timings or update_id is None:
            return
        with timing_lock:
            timings[hop][int(update_id)] = time.monotonic()

    monkeypatch.setattr(herdres_gateway, "get_updates", fake_get_updates)
    monkeypatch.setattr(herdres_gateway, "_save_offset", lambda *_args: None)
    monkeypatch.setattr(herdres_gateway, "_timing_log", capture_timing)
    monkeypatch.setattr(
        herdres_gateway,
        "handle_update",
        lambda *_args, **_kwargs: herdres_gateway.CHECKPOINT_ADVANCE,
    )

    # Do not let the fake Telegram source produce updates until the production
    # startup path has all configured dispatch workers waiting.
    ready_for_enqueue = threading.Event()
    empty_claims = 0
    empty_claims_lock = threading.Lock()
    real_claim = spool.claim

    def observed_claim(*args, **kwargs):
        nonlocal empty_claims
        # Preserve the real claim implementation but hold the cohort at its
        # durable boundary so the dispatcher must report all ten visible rows.
        if ready_for_enqueue.is_set() and not release_dispatch.is_set():
            return None
        item = real_claim(*args, **kwargs)
        if item is None:
            with empty_claims_lock:
                empty_claims += 1
                if empty_claims >= 8:
                    ready_for_enqueue.set()
        return item

    monkeypatch.setattr(spool, "claim", observed_claim)
    dispatcher = herdres_gateway._InboundLaneDispatcher(spool, REQUEST_ID_KEY)
    wake_calls = 0
    real_wake = dispatcher.wake

    def counted_wake() -> None:
        nonlocal wake_calls
        wake_calls += 1
        real_wake()

    monkeypatch.setattr(dispatcher, "wake", counted_wake)
    workers: dict[str, dict[str, object]] = {}
    specs = herdres_gateway._poll_specs(state.load_state(), "token")
    try:
        # This helper is the exact update_specs -> dispatcher.start ->
        # poll-worker reconcile sequence used by run().
        herdres_gateway._reconcile_gateway_workers(
            workers,
            specs,
            REQUEST_ID_KEY,
            spool,
            dispatcher,
        )
        assert ready_for_enqueue.wait(1.0)
        assert poll_drained.wait(3.0)
        _wait_for(
            lambda: len(timings["dispatcher_claim"]) == 10,
            timeout=3.0,
        )
        _wait_for(lambda: all(row["state"] == "done" for row in spool.rows()))
    finally:
        for worker in workers.values():
            stop = worker.get("stop")
            if isinstance(stop, threading.Event):
                stop.set()
        release_poll.set()
        for worker in workers.values():
            thread = worker.get("thread")
            if isinstance(thread, threading.Thread):
                thread.join(1.0)
        dispatcher.stop()

    update_ids = set(range(200, 210))
    assert poll_batches == [1] * 10
    assert wake_calls == 10
    assert dispatcher.worker_count == 8
    assert (
        "dispatcher_update_specs",
        f"spool={spool.storage_id},specs=1,receivers=manager",
    ) in dispatcher_diagnostics
    # Production specs are receiver/token records; their receiver key does not
    # equal the topic-derived lane key. The dispatcher must still see the row.
    assert lane_key("manager", "77") not in {
        herdres_gateway._receiver_id_for_key(key)
        for key, _token, _timeout in specs
    }
    assert len(enqueue_diagnostics) == 10
    assert all(
        detail.startswith(
            f"spool={spool.storage_id},status=enqueued,receiver=manager,"
            'lane=["manager","77"]'
        )
        for detail in enqueue_diagnostics
    )
    assert any(
        hop == "dispatcher_workers_started"
        and "configured=8,alive=8" in detail
        for hop, detail in dispatcher_diagnostics
    )
    assert any(
        hop == "dispatcher_workers_running" and "running=8,configured=8" in detail
        for hop, detail in dispatcher_diagnostics
    )
    assert any(
        hop == "dispatcher_iteration"
        and detail.startswith(
            f"spool={spool.storage_id},eligible=1,pending=10,claimable=1,"
        )
        for hop, detail in dispatcher_diagnostics
    )
    assert any(
        hop == "dispatcher_wake_set"
        and "source=poll_enqueue" in detail
        for hop, detail in dispatcher_diagnostics
    )
    assert any(
        hop == "dispatcher_wake_received"
        and "signaled=1" in detail
        and "source=poll_enqueue" in detail
        for hop, detail in dispatcher_diagnostics
    )
    assert any(
        hop == "dispatcher_claim_attempt"
        and "result=claimed" in detail
        and 'lane=["manager","77"]' in detail
        for hop, detail in dispatcher_diagnostics
    )
    assert set(timings["durable_enqueue_commit"]) == update_ids
    assert set(timings["dispatcher_claim"]) == update_ids
    claim_latencies = [
        timings["dispatcher_claim"][update_id]
        - timings["durable_enqueue_commit"][update_id]
        for update_id in sorted(update_ids)
    ]
    assert min(claim_latencies) >= 0.0
    assert max(claim_latencies) < 2.0


def test_unchanged_specs_do_not_broadcast_dispatcher_wakes(
    tmp_path, monkeypatch
) -> None:
    """The one-second reconcile loop must not wake every claimant when idle."""

    diagnostics: list[tuple[str, str]] = []
    monkeypatch.setattr(
        herdres_gateway,
        "_timing_log",
        lambda hop, *, detail="", **_kwargs: diagnostics.append(
            (hop, str(detail))
        ),
    )
    dispatcher = herdres_gateway._InboundLaneDispatcher(
        IngressLaneSpool(tmp_path / "spool.db"),
        REQUEST_ID_KEY,
        workers=8,
    )
    specs = [("manager", "token", 0)]

    dispatcher.update_specs(specs)
    dispatcher.update_specs(specs)

    spec_wakes = [
        detail
        for hop, detail in diagnostics
        if hop == "dispatcher_wake_set" and "source=update_specs" in detail
    ]
    assert len(spec_wakes) == 1
    assert "permits=8" in spec_wakes[0]


def test_real_dispatcher_claim_cadence_for_sixty_idle_seconds_and_enqueue(
    tmp_path, monkeypatch
) -> None:
    """The production thread/spool/wake composition keeps a wall-clock SLA."""

    spool = IngressLaneSpool(tmp_path / "spool.db")
    attempts: list[float] = []
    diagnostics: list[tuple[str, str]] = []
    claimed = threading.Event()
    timing_lock = threading.Lock()

    def capture_timing(hop, *, update_id=None, detail="", **_kwargs):
        observed_at = time.monotonic()
        with timing_lock:
            diagnostics.append((hop, str(detail)))
            if hop == "dispatcher_claim_attempt":
                attempts.append(observed_at)
                if update_id == 600:
                    claimed.set()

    monkeypatch.setattr(herdres_gateway, "_timing_log", capture_timing)
    monkeypatch.setattr(
        herdres_gateway,
        "handle_update",
        lambda *_args, **_kwargs: herdres_gateway.CHECKPOINT_ADVANCE,
    )
    dispatcher = herdres_gateway._InboundLaneDispatcher(
        spool,
        REQUEST_ID_KEY,
        workers=1,
        backoff_seconds=2,
        lease_seconds=5,
    )
    dispatcher.update_specs([("manager", "token", 0)])
    dispatcher.start()
    idle_started = time.monotonic()
    try:
        idle_deadline = idle_started + 60.0
        while True:
            remaining = idle_deadline - time.monotonic()
            if remaining <= 0:
                break
            time.sleep(min(0.1, remaining))

        enqueue_started = time.monotonic()
        _enqueue(spool, _update(600, 77, "after-sixty-idle-seconds"), "77")
        dispatcher.wake()
        assert claimed.wait(2.0)
        claimed_at = time.monotonic()
    finally:
        dispatcher.stop()

    with timing_lock:
        idle_attempts = [
            observed_at
            for observed_at in attempts
            if idle_started <= observed_at < enqueue_started
        ]
        captured_diagnostics = list(diagnostics)
    gaps = [
        current - previous
        for previous, current in zip(idle_attempts, idle_attempts[1:])
    ]
    assert len(idle_attempts) >= 29
    assert gaps
    assert max(gaps) <= 2.5
    assert 0 <= claimed_at - enqueue_started < 2
    assert any(
        hop == "dispatcher_iteration_start"
        and "idle_ms=" in detail
        and "iteration=" in detail
        for hop, detail in captured_diagnostics
    )
    assert any(
        hop == "dispatcher_iteration_end"
        and "snapshot_ms=" in detail
        and "claim_ms=" in detail
        and "dispatch_ms=" in detail
        for hop, detail in captured_diagnostics
    )


def test_slow_service_does_not_block_claim_loop_iterations(
    tmp_path, monkeypatch
) -> None:
    """Claim cadence stays observable while bounded service work is occupied."""

    spool = IngressLaneSpool(tmp_path / "spool.db")
    _enqueue(spool, _update(610, 77, "slow-service"), "77")
    service_started = threading.Event()
    release_service = threading.Event()
    capacity_attempts: list[float] = []
    timing_lock = threading.Lock()

    def capture_timing(hop, *, detail="", **_kwargs):
        if hop != "dispatcher_claim_attempt" or "result=capacity" not in detail:
            return
        with timing_lock:
            capacity_attempts.append(time.monotonic())

    def slow_handle_update(*_args, **_kwargs):
        service_started.set()
        assert release_service.wait(2.0)
        return herdres_gateway.CHECKPOINT_ADVANCE

    monkeypatch.setattr(herdres_gateway, "_timing_log", capture_timing)
    monkeypatch.setattr(herdres_gateway, "handle_update", slow_handle_update)
    dispatcher = herdres_gateway._InboundLaneDispatcher(
        spool,
        REQUEST_ID_KEY,
        workers=1,
        backoff_seconds=0.1,
        lease_seconds=5,
    )
    dispatcher.update_specs([("manager", "token", 0)])
    dispatcher.start()
    try:
        assert service_started.wait(1.0)
        _wait_for(lambda: len(capacity_attempts) >= 2, timeout=0.5)
    finally:
        release_service.set()
        dispatcher.stop()

    assert capacity_attempts[1] - capacity_attempts[0] <= 0.25


def test_single_lane_acceptance_cadence_claim_and_service_stay_under_two_seconds(
    tmp_path, monkeypatch
) -> None:
    """Ten spaced items use the production spool -> gateway -> command path."""

    state_path = tmp_path / "state.json"
    _configured_state(state_path)
    monkeypatch.setenv("HERDR_TELEGRAM_TOPICS_STATE", str(state_path))
    monkeypatch.setenv("HERDRES_SOURCE_TOPIC_MODE", "worker")
    monkeypatch.setenv("HERDRES_INBOUND_SUCCESS_ACK", "0")
    spool = IngressLaneSpool(tmp_path / "spool.db")
    timings: dict[str, dict[int, float]] = {
        "enqueue": {},
        "dispatcher_claim": {},
        "lane_done": {},
    }
    timing_lock = threading.Lock()

    def capture_timing(hop, *, update_id=None, **_kwargs):
        if update_id is None or hop not in timings:
            return
        with timing_lock:
            timings[hop][int(update_id)] = time.monotonic()

    class Client:
        def command_json(self, request_json):
            time.sleep(0.01)
            return _accepted_command_response(json.loads(request_json))

    def forbidden_parent_preflight(*_args, **_kwargs):
        raise AssertionError("durable lane work must not take the parent state lock")

    real_save_state = state.save_state
    command_saves = 0

    def counted_save(store, path=None):
        nonlocal command_saves
        command_saves += 1
        return real_save_state(store, path=path)

    def verified_command(payload):
        # This is a throughput test for the durable lane path. Model the
        # second-half verifier after the current command has persisted its
        # transport receipt so an unverified receipt does not intentionally
        # block the remainder of this synthetic same-lane cohort.
        transport_result = herdres.command_reply(payload)
        assert transport_result["terminal_outcome"] == "delivery_unknown"
        return ingress_requests.child_result(
            payload["request_id"],
            checkpoint="advance",
            transport_disposition="submitted",
            request_phase="terminal",
            terminal_outcome="delivered",
        )

    monkeypatch.setattr(herdres_gateway, "_timing_log", capture_timing)
    monkeypatch.setattr(
        herdres_gateway,
        "_preflight_ingress_request",
        forbidden_parent_preflight,
    )
    monkeypatch.setattr(herdres, "TendwireClient", Client)
    monkeypatch.setattr(state, "save_state", counted_save)
    monkeypatch.setattr(
        herdres_gateway,
        "run_herdres_command",
        verified_command,
    )
    dispatcher = herdres_gateway._InboundLaneDispatcher(
        spool,
        REQUEST_ID_KEY,
        workers=8,
        backoff_seconds=2,
        lease_seconds=5,
    )
    dispatcher.update_specs([("manager", "token", 0)])
    dispatcher.start()
    update_ids = range(300, 310)
    max_open_depth = 0
    try:
        for update_id in update_ids:
            update = _update(update_id, 77, f"cadence-{update_id}")
            _enqueue(spool, update, "77")
            timings["enqueue"][update_id] = time.monotonic()
            dispatcher.wake()
            max_open_depth = max(
                max_open_depth,
                sum(
                    row["state"] in {"pending", "processing"}
                    for row in spool.rows()
                ),
            )
            # Scaled cadence keeps this regression fast while retaining the
            # acceptance invariant that spacing exceeds service. The live
            # verification uses the full six-second cadence.
            time.sleep(0.25)
        _wait_for(
            lambda: len(timings["lane_done"]) == 10,
            timeout=3.0,
        )
    finally:
        dispatcher.stop()

    claims = [
        timings["dispatcher_claim"][update_id] - timings["enqueue"][update_id]
        for update_id in update_ids
    ]
    services = [
        timings["lane_done"][update_id]
        - timings["dispatcher_claim"][update_id]
        for update_id in update_ids
    ]
    assert max_open_depth <= 1
    assert min(claims) >= 0
    assert max(claims) < 2
    assert min(services) >= 0
    assert max(services) < 2
    # One canonical durability barrier and one terminal receipt barrier per
    # item; the former parent-shell and route-owner writes are coalesced.
    assert command_saves == 20


def test_production_reconcile_rejects_a_different_poll_spool(tmp_path) -> None:
    dispatcher_spool = IngressLaneSpool(tmp_path / "dispatcher.db")
    poll_spool = IngressLaneSpool(tmp_path / "poll.db")
    dispatcher = herdres_gateway._InboundLaneDispatcher(
        dispatcher_spool,
        REQUEST_ID_KEY,
        workers=1,
    )

    with pytest.raises(RuntimeError, match="share one spool instance"):
        herdres_gateway._reconcile_gateway_workers(
            {},
            [("manager", "token", 0)],
            REQUEST_ID_KEY,
            poll_spool,
            dispatcher,
        )
