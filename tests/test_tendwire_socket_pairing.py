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

from herdres_connector.tendwire_client import TendwireClient


def _paired_source() -> Path:
    configured = os.getenv("HERDRES_PAIRED_TENDWIRE_SOURCE_DIR")
    source = (
        Path(configured).expanduser()
        if configured
        else Path(__file__).resolve().parents[3] / "src"
    )
    if not (source / "tendwire" / "daemon_api.py").is_file():
        pytest.fail(
            "paired Tendwire source is required; set "
            "HERDRES_PAIRED_TENDWIRE_SOURCE_DIR"
        )
    return source


sys.path.insert(0, str(_paired_source()))

from tendwire.connectors import ConnectorOutboxAPI  # noqa: E402
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

        invalid = connector.prepare(
            {
                "schema_version": 1,
                "action": "recover",
                "name": "turn-final",
                "failed_plan_token": "twplan1.missing",
                "request_id": "recover:invalid",
            }
        )
        valid = client.connector_prepare_recover(
            failed_plan_token="twplan1.missing",
            request_id="recover.request-42_ok",
        )
        assert invalid["status"] == "invalid_params"
        assert valid["status"] != "invalid_params"
        assert valid["status"] != "invalid_connector_response"
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
