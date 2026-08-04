from __future__ import annotations

import json
import sqlite3
import threading
import time

import herdres
from herdres_connector import ingress_requests, state
from herdres_connector.outbound_dispatcher import OutboundDispatcher
from herdres_connector.source_sync import SyncRuntime, drain_outbound_once
from test_source_only import (
    FakeTelegram,
    REQUEST_ID,
    _accepted_command_response,
    _source_worker,
    _store,
)
from test_turn_final_delivery import (
    DeletingTelegram,
    TurnFinalTendwire,
    _turn_row,
)


def _wait_for(predicate, timeout: float = 3.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.01)
    raise AssertionError("condition was not met before the latency bound")


def test_submission_acceptance_delivers_working_card_within_three_seconds(
    tmp_path, monkeypatch
) -> None:
    state_path = tmp_path / "state.json"
    monkeypatch.setenv("HERDRES_TENDWIRE_MODE", "source")
    monkeypatch.setenv("HERDRES_SOURCE_TOPIC_MODE", "worker")
    monkeypatch.setenv("HERDR_TELEGRAM_TOPICS_STATE", str(state_path))
    monkeypatch.setenv("HERDR_TELEGRAM_TOPICS_MANAGED_BOTS", "0")
    monkeypatch.setenv("HERDRES_PINNED_STATUS", "0")
    monkeypatch.setenv("HERDRES_TENDWIRE_COMMAND_RESPONSE_SCHEMA_VERSION", "3")
    store = _store()
    worker = _source_worker(
        {
            "id": "worker-1",
            "name": "Alpha",
            "status": "idle",
            "space_id": "space-1",
            "fingerprint": "fp-1",
            "meta": {"agent": "codex"},
        }
    )
    state.upsert_worker_entry(store, worker, topic_id="77")
    state.save_state(store, state_path)
    telegram = FakeTelegram()
    delivered_at: list[float] = []

    class Client:
        def command_json(self, request_json):
            request = json.loads(request_json)
            assert request["response_schema_version"] == 3
            accepted = _accepted_command_response(request)
            accepted["schema_version"] = 3
            accepted["result"].update(
                {
                    "submission_id": "twsub1.latency",
                    "turn_id": "turn-latency",
                }
            )
            return accepted

    class Telegram:
        def __init__(self, token="", dry_run=False):
            self._delegate = telegram
            self.token = token
            self.dry_run = dry_run

        def __getattr__(self, name):
            return getattr(self._delegate, name)

        def send_message(self, *args, **kwargs):
            result = self._delegate.send_message(*args, **kwargs)
            delivered_at.append(time.monotonic())
            return result

        def api(self, *args, **kwargs):
            before = len(self._delegate.sent)
            result = self._delegate.api(*args, **kwargs)
            if len(self._delegate.sent) > before:
                delivered_at.append(time.monotonic())
            return result

    monkeypatch.setattr(herdres, "TendwireClient", Client)
    monkeypatch.setattr(herdres, "TelegramClient", Telegram)
    accepted_at = time.monotonic()
    result = herdres.command_reply(
        {
            "request_id": REQUEST_ID,
            "topic_id": "77",
            "user_id": "1",
            "text": "trace outbound acceptance",
        }
    )

    # Submission acceptance is a transport fact. It proves the terminal took
    # the bytes and triggers the working card, but it is deliberately not the
    # verified-delivery outcome that advances the inbound checkpoint.
    assert result["transport_disposition"] == "written_to_pty"
    assert result["request_phase"] == "accepted_unverified"
    assert result["terminal_outcome"] == "delivery_unknown"
    assert result["checkpoint"] == "hold"
    assert "disposition" not in result
    assert len(delivered_at) == 1
    assert delivered_at[0] - accepted_at < 3.0
    persisted = state.load_state(state_path)
    record = persisted[ingress_requests.RECORDS_KEY][REQUEST_ID]
    assert record["submission_id"] == "twsub1.latency"
    entry = next(iter(state.source_worker_entries(persisted).values()))
    assert entry["last_stream_submission_id"] == "twsub1.latency"
    assert entry["last_stream_message_id"]


def test_final_outbox_commit_wakes_delivery_within_three_seconds(
    tmp_path, monkeypatch
) -> None:
    state_path = tmp_path / "state.json"
    database_path = tmp_path / "tendwire.db"
    monkeypatch.setenv("HERDRES_TENDWIRE_MODE", "source")
    monkeypatch.setenv("HERDRES_SOURCE_TOPIC_MODE", "worker")
    monkeypatch.setenv("HERDR_TELEGRAM_TOPICS_STATE", str(state_path))
    monkeypatch.setenv("HERDR_TELEGRAM_TOPICS_MANAGED_BOTS", "0")
    monkeypatch.setenv("HERDRES_PINNED_STATUS", "0")
    with sqlite3.connect(database_path) as connection:
        connection.execute(
            "CREATE TABLE connector_outbox "
            "(id INTEGER PRIMARY KEY, status TEXT NOT NULL, created_at REAL NOT NULL)"
        )

    row = _turn_row(
        "turn-outbound-latency",
        "twrev1.outbound_latency",
        "final delivered by the wakeable outbound lane",
        user="measure the final",
    )
    tendwire = TurnFinalTendwire(
        row,
        emit_ready=False,
        turn_schema_version=2,
    )
    telegram = DeletingTelegram()
    store = _store()
    state.upsert_worker_entry(
        store,
        tendwire.snapshot()["workers"][0],
        topic_id="77",
    )
    state.save_state(store, state_path)
    delivered_at: list[float] = []
    delivered = threading.Event()

    def drain() -> None:
        with state.state_lock(state_path, phase="test.outbound"):
            current = state.load_state(state_path)

            def checkpoint() -> None:
                state.save_state(current, state_path)

            result = drain_outbound_once(
                current,
                SyncRuntime(
                    tendwire,
                    telegram,
                    with_outbox=True,
                    max_sends=8,
                    checkpoint=checkpoint,
                ),
                chat_id="-100",
            )
            if result["changed"]:
                state.save_state(current, state_path)
        if telegram.sent and not delivered.is_set():
            delivered_at.append(time.monotonic())
            delivered.set()

    dispatcher = OutboundDispatcher(
        drain,
        database_path=database_path,
        fallback_seconds=1.0,
    )
    dispatcher.start()
    try:
        _wait_for(lambda: tendwire.poll_calls >= 1)
        tendwire.emit_ready = True
        enqueued_at = time.monotonic()
        with sqlite3.connect(database_path) as connection:
            connection.execute(
                "INSERT INTO connector_outbox(status, created_at) VALUES (?, ?)",
                ("queued", time.time()),
            )
        assert delivered.wait(timeout=3.0)
    finally:
        dispatcher.stop()

    assert delivered_at[0] - enqueued_at < 3.0
    assert len(telegram.sent) == 1
    assert "final delivered by the wakeable outbound lane" in telegram.sent[0][1]
