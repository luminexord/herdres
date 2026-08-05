"""Executable Herdres/Tendwire socket pairing against the real daemon stack."""

from __future__ import annotations

import json
import os
import socket
import sqlite3
import sys
import threading
import time
from pathlib import Path

import pytest

from herdres_connector import ingress
from herdres_connector.ingress import IngressPorts
from herdres_connector.ingress_identity import derive_telegram_request_id
from herdres_connector.ingress_queue import IngressQueue
from herdres_connector.state import (
    IngressRouteResult,
    RouteStatus,
    StableOwner,
    StateToken,
)
from herdres_connector.tendwire_client import TendwireClient


def _paired_source() -> Path:
    configured = os.getenv("HERDRES_PAIRED_TENDWIRE_SOURCE_DIR")
    if not configured:
        pytest.skip(
            "set HERDRES_PAIRED_TENDWIRE_SOURCE_DIR to an explicit Tendwire src directory",
            allow_module_level=True,
        )
    source = Path(configured).expanduser().resolve()
    if not (source / "tendwire" / "daemon_api.py").is_file():
        pytest.fail(
            "paired Tendwire source is required; set "
            "HERDRES_PAIRED_TENDWIRE_SOURCE_DIR"
        )
    return source


sys.path.insert(0, str(_paired_source()))

from tendwire.connectors import ConnectorOutboxAPI  # noqa: E402
from tendwire.core.commands import (  # noqa: E402
    CommandEnvelope,
    CommandRequest,
    DISPOSITION_TERMINAL_ACCEPTED,
    STATUS_ACCEPTED,
)
from tendwire.core.models import Snapshot  # noqa: E402
from tendwire.daemon_api import TendwireDaemonAPI, UnixSocketJSONServer  # noqa: E402
from tendwire.store.sqlite import init_store  # noqa: E402


def _enqueue(db_path: Path, key: str) -> None:
    with sqlite3.connect(str(db_path)) as conn:
        conn.execute(
            """
            INSERT INTO connector_outbox (
                host_id, connector, delivery_key, status, payload_json,
                private_state_json, created_at, updated_at, next_attempt_at
            ) VALUES (?, 'attention', ?, 'queued', ?, '{}', ?, ?, NULL)
            """,
            (
                "host-pair",
                key,
                json.dumps(
                    {
                        "schema_version": 1,
                        "event_type": "attention_created",
                        "safe": key,
                    }
                ),
                "2026-08-05T12:00:00+00:00",
                "2026-08-05T12:00:00+00:00",
            ),
        )


def _raw_ack_without_reading(path: Path, ref: str) -> None:
    request = {
        "id": "raw-ack-loss-1",
        "method": "connector.ack",
        "params": {
            "name": "attention",
            "ref": ref,
            "response": {"provider_ref": "stub-loss"},
        },
    }
    raw = json.dumps(request, separators=(",", ":")).encode("utf-8") + b"\n"
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as conn:
        conn.settimeout(2)
        conn.connect(str(path))
        conn.sendall(raw)
        conn.shutdown(socket.SHUT_WR)
        # Deliberately close without reading the ACK response.


def _raw_command_without_reading(path: Path, command: dict[str, object]) -> None:
    request = {
        "id": "raw-command-ack-loss-1",
        "method": "command.submit",
        "params": command,
    }
    raw = json.dumps(request, separators=(",", ":")).encode("utf-8") + b"\n"
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as conn:
        conn.settimeout(2)
        conn.connect(str(path))
        conn.sendall(raw)
        conn.shutdown(socket.SHUT_WR)
        # Simulate a gateway crash after submit: the daemon mutates, but the
        # queue never observes or checkpoints the response.


def test_real_socket_poll_binding_ack_and_ack_response_loss(tmp_path: Path) -> None:
    parent = tmp_path / "daemon"
    parent.mkdir(mode=0o700)
    path = parent / "tendwire.sock"
    db_path = tmp_path / "tendwire.db"
    init_store(db_path)
    connector = ConnectorOutboxAPI(db_path, "host-pair")

    def connector_call(method: str, params: dict[str, object]):
        return getattr(connector, method.removeprefix("connector."))(params)

    api = TendwireDaemonAPI(
        get_snapshot=lambda: Snapshot(
            host_id="host-pair",
            updated_at="2026-08-05T12:00:00+00:00",
        ),
        get_health=lambda: {"ok": True},
        submit_command=lambda _params: {},
        connector_call=connector_call,
    )
    stop = threading.Event()
    server = UnixSocketJSONServer(
        path,
        api.dispatch,
        stop_event=stop,
        prepare_parent=False,
        request_workers=2,
        max_in_flight_requests=4,
    )
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    deadline = time.monotonic() + 3
    while not path.exists() and time.monotonic() < deadline:
        time.sleep(0.01)
    assert path.exists()

    client = TendwireClient(socket_path=path, timeout=3)
    provider_bindings: dict[str, dict[str, str]] = {}
    try:
        _enqueue(db_path, "attention:paired-first")
        first = client.connector_poll(limit=1)["items"][0]
        provider_bindings[first["key"]] = {
            "provider_ref": "stub-message-1",
            "delivery_key": first["key"],
        }
        ack = client.connector_ack(first["ref"], provider_bindings[first["key"]])
        assert ack["status"] == "acknowledged"
        assert client.connector_poll(limit=1)["items"] == []

        _enqueue(db_path, "attention:paired-loss")
        lost = client.connector_poll(limit=1)["items"][0]
        _raw_ack_without_reading(path, lost["ref"])
        deadline = time.monotonic() + 3
        status = ""
        while time.monotonic() < deadline:
            with sqlite3.connect(str(db_path)) as conn:
                row = conn.execute(
                    "SELECT status FROM connector_outbox WHERE delivery_key = ?",
                    (lost["key"],),
                ).fetchone()
            status = str(row[0]) if row else ""
            if status == "delivered":
                break
            time.sleep(0.01)
        assert status == "delivered"
        assert client.connector_poll(limit=1)["items"] == []

    finally:
        stop.set()
        server.close()
        thread.join(timeout=3)
    assert not thread.is_alive()
    assert provider_bindings == {
        "attention:paired-first": {
            "provider_ref": "stub-message-1",
            "delivery_key": "attention:paired-first",
        }
    }


def test_real_ingress_queue_replays_lost_command_ack_after_restart_once(
    tmp_path: Path,
) -> None:
    parent = tmp_path / "command-daemon"
    parent.mkdir(mode=0o700)
    socket_path = parent / "tendwire.sock"
    queue_path = tmp_path / "inbound_spool.db"
    logical_mutations: list[str] = []
    submitted_receipts: list[dict[str, object]] = []
    receipts_by_request: dict[str, CommandEnvelope] = {}
    submit_lock = threading.Lock()
    mutation_recorded = threading.Event()

    def submit_command(params: dict[str, object]) -> CommandEnvelope:
        request = CommandRequest.from_dict(params)
        assert request.request_id is not None
        with submit_lock:
            envelope = receipts_by_request.get(request.request_id)
            if envelope is None:
                logical_mutations.append(request.request_id)
                envelope = CommandEnvelope.from_result(
                    request,
                    ok=True,
                    status=STATUS_ACCEPTED,
                    disposition=DISPOSITION_TERMINAL_ACCEPTED,
                    result={
                        "target": {"worker_id": "worker-1"},
                        "delivery_state": "submitted",
                        "transport_state": "submitted",
                        "target_state_at_send": "idle",
                        "observed_turn_state": "pending_observation",
                        "submission_id": "twsub1." + "a" * 64,
                        "submission_verdict": "submitted",
                        "turn_id": None,
                    },
                    schema_version=3,
                )
                receipts_by_request[request.request_id] = envelope
                mutation_recorded.set()
            submitted_receipts.append(envelope.to_dict())
            return envelope

    api = TendwireDaemonAPI(
        get_snapshot=lambda: Snapshot(
            host_id="host-paired-command",
            updated_at="2026-08-05T12:00:00+00:00",
        ),
        get_health=lambda: {"ok": True},
        submit_command=submit_command,
    )
    stop = threading.Event()
    server = UnixSocketJSONServer(
        socket_path,
        api.dispatch,
        stop_event=stop,
        prepare_parent=False,
        request_workers=2,
        max_in_flight_requests=4,
    )
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    deadline = time.monotonic() + 3
    while not socket_path.exists() and time.monotonic() < deadline:
        time.sleep(0.01)
    assert socket_path.exists()

    request_id = derive_telegram_request_id(
        b"k" * 32,
        receiver_id="manager",
        update_id=701,
        chat_id=-100,
        message_id=9701,
    )
    acceptance = {
        "receiver_id": "manager",
        "update_id": 701,
        "request_id": request_id,
        "ordering_key": '["topic","-100","77"]',
        "kind": "message",
        "input": {
            "chat_id": "-100",
            "topic_id": "77",
            "message_id": "9701",
            "text": "paired command",
        },
        "first_seen_at": 10.0,
        "deadline_at": 100.0,
        "retain_until": 200.0,
        "depth_limit": 32,
    }
    route = IngressRouteResult(
        RouteStatus.RESOLVED,
        StateToken("st1.paired-command"),
        "-100",
        "77",
        "worker-1",
        None,
        StableOwner("wsk1_" + "b" * 64, 1, None),
        "space-1",
        "manager",
        None,
        False,
        "topic_route",
    )
    client = TendwireClient(socket_path=socket_path, timeout=3)
    try:
        with IngressQueue.open_writer(queue_path) as queue:
            accepted = queue.accept_update(acceptance)
            assert accepted.status == "enqueued"
            claimed = queue.claim("paired-first", 10.0, 1.0)
            assert claimed is not None
            command = ingress.build_send_instruction(claimed, route)
            checkpointed = ingress._checkpoint_command(
                queue, claimed, command, route, 10.0
            )
            assert checkpointed is not None
            _raw_command_without_reading(socket_path, command.value)
            assert mutation_recorded.wait(timeout=3)
            assert logical_mutations == [request_id]
            # Deliberately leave the checkpointed row processing. Closing the
            # queue models losing the daemon ACK before durable settlement.

        with IngressQueue.open_writer(queue_path) as queue:
            replay = queue.claim("paired-restart", 12.0, 30.0)
            assert replay is not None
            assert replay.command == command.value
            ports = IngressPorts(
                state_path=tmp_path / "unused-state.json",
                request_id_key=b"k" * 32,
                queue=queue,
                receivers=(),
                telegram_clients={},
                tendwire=client,
                now=lambda: 12.0,
            )
            dispatched = ingress.dispatch_one(queue, replay, ports)
            assert dispatched.status == "settled"
            row = queue._connection.execute(
                "SELECT state, attempts, disposition, receipt_json "
                "FROM requests WHERE seq = ?",
                (replay.seq,),
            ).fetchone()
            assert tuple(row[:3]) == ("terminal", 2, "terminal_accepted")
            durable_receipt = json.loads(row[3])

        with IngressQueue.open_writer(queue_path) as queue:
            duplicate = queue.accept_update(acceptance)
            assert duplicate.status == "duplicate"
            row = queue._connection.execute(
                "SELECT state, attempts, receipt_json FROM requests"
            ).fetchone()
            assert tuple(row[:2]) == ("terminal", 2)
            assert json.loads(row[2]) == durable_receipt
            assert queue._connection.execute(
                "SELECT COUNT(*) FROM requests"
            ).fetchone()[0] == 1

        assert logical_mutations == [request_id]
        assert len(submitted_receipts) == 2
        assert submitted_receipts[0] == submitted_receipts[1] == durable_receipt
    finally:
        stop.set()
        server.close()
        thread.join(timeout=3)
    assert not thread.is_alive()
