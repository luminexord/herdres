from __future__ import annotations

import json
import threading
import time
from types import SimpleNamespace

import herdres
import pytest
from herdres_connector import config, ingress_requests, state
from test_source_only import (
    FakeTelegram,
    REQUEST_ID,
    _accepted_command_response,
    _source_worker,
    _store,
)


def test_connector_poll_cadence_is_independent_of_long_reconciliation(
    monkeypatch,
) -> None:
    """A blocked full pass cannot postpone connector polling to its completion."""

    full_entered = threading.Event()
    poll_seen = threading.Event()
    release_full = threading.Event()
    times: dict[str, float] = {}

    class StopLoop(BaseException):
        pass

    def long_reconciliation(**kwargs):
        assert kwargs == {"with_outbox": False}
        times["full"] = time.monotonic()
        full_entered.set()
        assert release_full.wait(2)
        raise StopLoop

    def connector_poll():
        assert full_entered.wait(2)
        times["poll"] = time.monotonic()
        poll_seen.set()
        release_full.set()
        return {"ok": True, "changed": False}

    monkeypatch.setenv("HERDRES_TENDWIRE_CONNECTOR_POLL_SECONDS", "0.2")
    monkeypatch.setattr(herdres.config, "load_env_file", lambda: None)
    monkeypatch.setattr(herdres.config, "require_source_mode", lambda: None)
    monkeypatch.setattr(herdres, "_sync_pass", long_reconciliation)
    monkeypatch.setattr(herdres, "_outbound_pass", connector_poll)

    with pytest.raises(StopLoop):
        herdres.cmd_sync(SimpleNamespace(loop=60.0))

    assert poll_seen.is_set()
    assert times["poll"] - times["full"] < 0.7
    assert times["poll"] - times["full"] < 3.0


def test_connector_poll_cadence_default_and_bounds() -> None:
    assert config.tendwire_connector_poll_seconds(env={}) == 1.0
    assert config.tendwire_connector_poll_seconds(
        env={"HERDRES_TENDWIRE_CONNECTOR_POLL_SECONDS": "0"}
    ) == 0.1
    assert config.tendwire_connector_poll_seconds(
        env={"HERDRES_TENDWIRE_CONNECTOR_POLL_SECONDS": "90"}
    ) == 60.0
    assert config.tendwire_connector_poll_seconds(
        env={"HERDRES_TENDWIRE_CONNECTOR_POLL_SECONDS": "invalid"}
    ) == 1.0


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
