from __future__ import annotations

import json
import os
import socket
import threading
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Callable, Iterator

import pytest

from herdres_connector import config, tendwire_client
from herdres_connector.tendwire_client import TendwireClient


REQUEST_ID = "hri1_SIodGeqCeIvApzpEvIaEM-L07UzUMgUFyeltRQxPpqU"


def _command_request(*, v3: bool = False) -> dict[str, Any]:
    request = {
        "schema_version": 1,
        "action": "send_instruction",
        "request_id": REQUEST_ID,
        "dry_run": False,
        "target": {"worker_id": "worker-public", "worker_fingerprint": "fingerprint-public"},
        "instruction": {"text": "perform the public instruction"},
    }
    if v3:
        request["response_schema_version"] = 3
    return request


def _accepted(request: dict[str, Any], *, schema: int = 2) -> dict[str, Any]:
    result: dict[str, Any] = {
        "target": {"worker_id": "worker-public"},
        "delivery_state": "submitted",
        "transport_state": "submitted",
        "target_state_at_send": "working",
        "observed_turn_state": "pending_observation",
    }
    if schema == 3:
        result.update({"submission_id": "twsub1.public", "submission_verdict": "submitted", "turn_id": None})
    return {
        "schema_version": schema,
        "action": "send_instruction",
        "request_id": request["request_id"],
        "ok": True,
        "dry_run": False,
        "status": "accepted",
        "disposition": "terminal_accepted",
        "result": result,
        "error": None,
        "warnings": [],
    }


Responder = Callable[[dict[str, Any]], dict[str, Any] | bytes | None]


@contextmanager
def _daemon(tmp_path: Path, responders: list[Responder]) -> Iterator[tuple[TendwireClient, list[dict[str, Any]]]]:
    parent = tmp_path / "daemon"
    parent.mkdir(mode=0o700, exist_ok=True)
    path = parent / "tendwire.sock"
    path.unlink(missing_ok=True)
    listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    listener.bind(str(path))
    os.chmod(path, 0o600)
    listener.listen()
    calls: list[dict[str, Any]] = []
    errors: list[BaseException] = []

    def run() -> None:
        try:
            for responder in responders:
                conn, _ = listener.accept()
                with conn:
                    raw = b""
                    while b"\n" not in raw:
                        chunk = conn.recv(4096)
                        if not chunk:
                            break
                        raw += chunk
                    request = json.loads(raw.split(b"\n", 1)[0].decode("utf-8"))
                    calls.append(request)
                    response = responder(request)
                    if response is None:
                        continue
                    if isinstance(response, bytes):
                        conn.sendall(response)
                        continue
                    envelope = {
                        "schema_version": 1,
                        "ok": True,
                        "status": "ok",
                        "result": response,
                        "error": None,
                        "id": request["id"],
                    }
                    conn.sendall(json.dumps(envelope, ensure_ascii=False, separators=(",", ":")).encode("utf-8") + b"\n")
        except BaseException as exc:  # pragma: no cover - surfaced below
            errors.append(exc)
        finally:
            listener.close()

    thread = threading.Thread(target=run, daemon=True)
    thread.start()
    try:
        yield TendwireClient(socket_path=path, timeout=2), calls
    finally:
        thread.join(timeout=3)
        listener.close()
        path.unlink(missing_ok=True)
        assert not thread.is_alive()
        assert errors == []


def _return(value: dict[str, Any]) -> Responder:
    return lambda _request: value


def test_protocol_prune_is_bounded_and_preserves_exact_protocol_text_and_tokens() -> None:
    nested: dict[str, Any] = {}
    for _ in range(200):
        nested = {"child": nested}
    long_text = "界" * 20_000
    clean = tendwire_client._protocol_prune({
        "nested": nested,
        "assistant_final_text": long_text,
        "plan_token": "twplan1.Exact_TOKEN",
        "api_token": "private",
        "pane_id": "private",
    })
    assert clean["assistant_final_text"] == long_text
    assert clean["plan_token"] == "twplan1.Exact_TOKEN"
    assert "api_token" not in clean
    assert "pane_id" not in clean


def test_command_uses_one_daemon_rpc_and_accepts_exact_v3_response(tmp_path: Path) -> None:
    request = _command_request(v3=True)
    with _daemon(tmp_path, [_return(_accepted(request, schema=3))]) as (client, calls):
        result = client.command(request)
    assert result["ok"] is True
    assert result["result"]["submission_id"] == "twsub1.public"
    assert [(call["method"], call["params"]) for call in calls] == [("command.submit", request)]


@pytest.mark.parametrize(
    "mutate",
    [
        lambda body: body.update(request_id="wrong"),
        lambda body: body.update(schema_version=4),
        lambda body: body.update(disposition="no_receipt"),
        lambda body: body["result"].update(backend_target="private"),
        lambda body: body["result"].update(submission_verdict="unknown"),
    ],
)
def test_command_response_validation_fails_closed(tmp_path: Path, mutate) -> None:
    request = _command_request(v3=True)
    body = _accepted(request, schema=3)
    mutate(body)
    with _daemon(tmp_path, [_return(body)]) as (client, _calls):
        result = client.command(request)
    assert result["status"] == "request_state_uncertain"
    assert tendwire_client.command_process_ambiguous(result)


def test_command_pre_send_failure_is_definite_but_post_send_eof_is_uncertain(tmp_path: Path) -> None:
    missing = TendwireClient(socket_path=tmp_path / "missing" / "tendwire.sock", timeout=1)
    definite = missing.command(_command_request())
    assert definite["status"] == "daemon_unavailable"
    assert tendwire_client.command_process_not_started(definite)
    assert not tendwire_client.command_process_ambiguous(definite)

    with _daemon(tmp_path, [lambda _request: None]) as (client, _calls):
        uncertain = client.command(_command_request())
    assert uncertain["status"] == "request_state_uncertain"
    assert tendwire_client.command_process_ambiguous(uncertain)


def test_outer_envelope_requires_correlated_id_and_single_frame(tmp_path: Path) -> None:
    def wrong_id(request: dict[str, Any]) -> bytes:
        return json.dumps({
            "schema_version": 1, "ok": True, "status": "ok", "result": {},
            "error": None, "id": request["id"] + "x",
        }).encode() + b"\n"

    with _daemon(tmp_path, [wrong_id]) as (client, _calls):
        assert client.snapshot()["status"] == "daemon_protocol_error"

    def two_frames(request: dict[str, Any]) -> bytes:
        envelope = {"schema_version": 1, "ok": True, "status": "ok", "result": {}, "error": None, "id": request["id"]}
        raw = json.dumps(envelope).encode() + b"\n"
        return raw + raw

    with _daemon(tmp_path, [two_frames]) as (client, _calls):
        assert client.snapshot()["status"] == "daemon_protocol_error"


def test_socket_parent_and_endpoint_must_be_private_owned_entries(tmp_path: Path) -> None:
    parent = tmp_path / "open"
    parent.mkdir(mode=0o777)
    os.chmod(parent, 0o777)
    path = parent / "tendwire.sock"
    listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    listener.bind(str(path))
    os.chmod(path, 0o600)
    try:
        result = TendwireClient(socket_path=path, timeout=1).snapshot()
    finally:
        listener.close()
    assert result["status"] == "daemon_unavailable"
    assert "parent" in result["error"]


def test_turn_projection_paginates_and_preserves_exact_restored_text(tmp_path: Path) -> None:
    exact = "final\n" + "界" * 20_000
    first = {
        "schema_version": 2,
        "turns": [{"id": "turn-1", "content": {"schema_version": 1}, "assistant_final_text": exact}],
        "has_more": True,
        "next_cursor": "cursor-2",
    }
    second = {"schema_version": 2, "turns": [], "has_more": False, "next_cursor": None}
    with _daemon(tmp_path, [_return(first), _return(second)]) as (client, calls):
        result = client.turns()
    assert result["turns"][0]["assistant_final_text"] == exact
    assert calls[0]["method"] == calls[1]["method"] == "turn.list"
    assert "cursor" not in calls[0]["params"]
    assert calls[1]["params"]["cursor"] == "cursor-2"


def test_turn_delta_and_content_map_to_daemon_methods_without_retry(tmp_path: Path) -> None:
    exact = "x" * 40_000
    delta = {"schema_version": 1, "projection_schema_version": 2, "changes": [], "has_more": False, "next_cursor": None, "checkpoint": "twdelta1.next"}
    content = {"schema_version": 1, "turn_id": "turn-1", "content_revision": "twrev1.public", "field": "assistant_final_text", "availability": "complete", "text": exact, "next_cursor": None}
    with _daemon(tmp_path, [_return(delta), _return(content)]) as (client, calls):
        assert client.turn_delta(watermark="twdelta1.old", limit=23)["checkpoint"] == "twdelta1.next"
        assert client.turn_content_get("turn-1", "twrev1.public", "assistant_final_text")["text"] == exact
    assert calls[0]["method"] == "turn.delta"
    assert calls[0]["params"] == {"limit": 23, "watermark": "twdelta1.old"}
    assert calls[1]["method"] == "turn.content.get"


def test_connector_helpers_use_connector_rpc_and_preserve_plan_token_bytes(tmp_path: Path) -> None:
    token = "twplan1.Exact_TOKEN-bytes"
    responses = [
        {"schema_version": 1, "ok": True, "status": "ok", "items": [{"ref": "twref1.public", "payload": {"plan_token": token}}]},
        {"schema_version": 1, "ok": True, "status": "acknowledged", "plan_token": token},
        {"schema_version": 1, "ok": True, "status": "deferred"},
        {"schema_version": 1, "ok": True, "status": "begun", "plan_token": token},
    ]
    with _daemon(tmp_path, [_return(v) for v in responses]) as (client, calls):
        polled = client.turn_final_poll(limit=2, lease_seconds=45)
        acked = client.turn_final_ack("twref1.public", {"plan_token": token, "message_id": "private"})
        deferred = client.turn_final_defer("twref1.public", "later", delay_seconds=5)
        begun = client.connector_prepare_begin(turn_id="turn-1", content_revision="twrev1.public", presentation_version="v1", part_count=1)
    assert polled["items"][0]["payload"]["plan_token"] == token
    assert acked["plan_token"] == token
    assert begun["plan_token"] == token
    assert deferred["status"] == "deferred"
    assert [call["method"] for call in calls] == ["connector.poll", "connector.ack", "connector.defer", "connector.prepare"]
    assert calls[1]["params"]["response"] == {"plan_token": token}


def test_ack_response_loss_is_not_retried_and_repoll_observes_authoritative_state(tmp_path: Path) -> None:
    state = {"acked": False}

    def poll(_request: dict[str, Any]) -> dict[str, Any]:
        return {
            "schema_version": 1,
            "ok": True,
            "status": "ok",
            "items": [] if state["acked"] else [{"ref": "twref1.loss", "payload": {"safe": True}}],
        }

    def lost_ack(_request: dict[str, Any]) -> None:
        state["acked"] = True
        return None

    with _daemon(tmp_path, [poll, lost_ack, poll]) as (client, calls):
        assert client.connector_poll()["items"][0]["ref"] == "twref1.loss"
        lost = client.connector_ack("twref1.loss", {"accepted": True})
        assert lost["status"] == "daemon_protocol_error"
        assert client.connector_poll()["items"] == []
    assert [call["method"] for call in calls] == ["connector.poll", "connector.ack", "connector.poll"]


def test_prepare_rejects_invalid_ranges_and_recovery_coordinates_without_rpc(tmp_path: Path) -> None:
    client = TendwireClient(socket_path=tmp_path / "never.sock")
    assert client.connector_prepare_part(plan_token="twplan1.public", ordinal=0, spans=[])["status"] == "invalid_prepare_part"
    assert client.connector_prepare_part(plan_token="twplan1.public", ordinal=0, spans=[{"field": "assistant_final_text", "start_char": 2, "end_char": 1}])["status"] == "invalid_prepare_part"
    assert client.connector_prepare_recover(failed_plan_token="bad", request_id="ok")["status"] == "invalid_recovery_request"


def test_configured_socket_and_timeout_are_bounded(monkeypatch) -> None:
    monkeypatch.setenv("HERDRES_TENDWIRE_SOCKET_PATH", "/tmp/tendwire-public.sock")
    monkeypatch.setenv("HERDRES_TENDWIRE_TIMEOUT_SECONDS", "0")
    assert config.tendwire_socket_path() == Path("/tmp/tendwire-public.sock")
    assert TendwireClient()._timeout_seconds() == 1.0
