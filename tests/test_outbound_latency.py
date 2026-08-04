from __future__ import annotations

import json
import threading
import time

import herdres
from herdres_connector import config, ingress_requests, state
from test_source_only import (
    FakeTelegram,
    FakeTendwire,
    REQUEST_ID,
    _accepted_command_response,
    _source_worker,
    _store,
)


def test_real_passes_share_flock_without_provider_io_or_lost_state(
    tmp_path, monkeypatch
) -> None:
    """Real reconciliation and outbound delivery make bounded shared progress."""

    state_path = tmp_path / "state.json"
    monkeypatch.setenv("HERDRES_TENDWIRE_MODE", "source")
    monkeypatch.setenv("HERDR_TELEGRAM_TOPICS_STATE", str(state_path))
    monkeypatch.setenv("HERDRES_SOURCE_TOPIC_MODE", "worker")
    monkeypatch.setenv("HERDR_TELEGRAM_TOPICS_MAX_CREATES", "0")
    monkeypatch.setenv("HERDRES_PINNED_STATUS", "0")
    monkeypatch.setenv("HERDR_TELEGRAM_TOPICS_STATUS_ICON", "0")
    monkeypatch.setenv("HERDR_TELEGRAM_TOPICS_MANAGED_BOTS", "0")
    workers = [
        _source_worker(
            {
                "id": f"worker-{index}",
                "name": f"Worker {index}",
                "status": "idle",
                "space_id": "space-1",
                "fingerprint": f"fp-{index}",
                "meta": {"agent": "codex"},
            }
        )
        for index in (1, 2, 3)
    ]
    observation_started = threading.Event()
    release_observation = threading.Event()
    send_started = threading.Event()
    release_send = threading.Event()
    acked = threading.Event()

    class Tendwire(FakeTendwire):
        def __init__(self):
            super().__init__(workers=workers)
            self.poll_count = 0
            self.acks: list[tuple[str, dict]] = []

        def snapshot(self):
            assert not state.lock_actually_held()
            observation_started.set()
            assert release_observation.wait(5)
            return super().snapshot()

        def turn_final_poll(self, **_kwargs):
            assert not state.lock_actually_held()
            return {"ok": True, "items": []}

        def connector_poll(self, **_kwargs):
            assert not state.lock_actually_held()
            self.poll_count += 1
            if self.poll_count > 1:
                return {"ok": True, "items": []}
            return {
                "ok": True,
                "items": [
                    {
                        "ref": "twref1.latency",
                        "key": "attention:latency",
                        "attempt": 1,
                        "payload": {
                            "event_type": "attention_created",
                            "attention": {
                                "severity": "warning",
                                "reason": "Needs input",
                            },
                        },
                    }
                ],
            }

        def connector_ack(self, ref, response, **_kwargs):
            assert not state.lock_actually_held()
            self.acks.append((ref, response))
            acked.set()
            return {"ok": True}

        def connector_fail(self, *_args, **_kwargs):
            raise AssertionError("the valid attention item must not fail")

    class SlowTelegram(FakeTelegram):
        def send_message(self, *args, **kwargs):
            assert not state.lock_actually_held()
            send_started.set()
            assert release_send.wait(5)
            return super().send_message(*args, **kwargs)

    tendwire = Tendwire()
    telegram = SlowTelegram()
    monkeypatch.setattr(herdres, "TendwireClient", lambda: tendwire)
    monkeypatch.setattr(
        herdres, "TelegramClient", lambda token="", dry_run=False: telegram
    )
    state.save_state(_store(), state_path)

    failures: list[BaseException] = []
    full_checkpoints: list[int] = []
    original_save = state.save_state

    def observed_save(store, path=None):
        original_save(store, path)
        if threading.current_thread().name == "full-reconciliation":
            full_checkpoints.append(len(state.source_worker_entries(store)))

    monkeypatch.setattr(state, "save_state", observed_save)

    def run(call) -> None:
        try:
            call()
        except BaseException as exc:  # pragma: no cover - surfaced below
            failures.append(exc)

    full = threading.Thread(
        target=run,
        args=(lambda: herdres._sync_pass(with_outbox=False),),
        name="full-reconciliation",
    )
    full.start()
    assert observation_started.wait(5)

    outbound = threading.Thread(target=run, args=(herdres._outbound_pass,))
    outbound.start()
    assert send_started.wait(5)

    # The outbound pass is blocked inside a real provider write. The flock is
    # available, so an unrelated persisted mutation can complete safely.
    with state.state_lock(path=state_path, phase="test.concurrent_writer"):
        concurrent = state.load_state(state_path)
        concurrent["concurrent_marker"] = "preserved"
        state.save_state(concurrent, state_path)

    release_send.set()
    assert acked.wait(5)
    release_observation.set()
    outbound.join(5)
    full.join(5)
    assert not outbound.is_alive()
    assert not full.is_alive()
    assert failures == []
    current = state.load_state(state_path)
    assert current["concurrent_marker"] == "preserved"
    assert len(state.source_worker_entries(current)) == 3
    assert len(full_checkpoints) >= len(workers) + 1
    assert full_checkpoints[-1] == len(workers)
    assert len(telegram.sent) == 1
    assert tendwire.acks == [
        ("twref1.latency", {"telegram": "delivered"})
    ]


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
