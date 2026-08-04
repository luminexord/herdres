from __future__ import annotations

import json
import os
import socket
import struct
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


def _command_failure(
    request: dict[str, Any], status: str, disposition: str
) -> dict[str, Any]:
    return {
        "schema_version": 2,
        "action": request["action"],
        "request_id": request["request_id"],
        "ok": False,
        "dry_run": False,
        "status": status,
        "disposition": disposition,
        "result": None,
        "error": {"code": status, "message": "typed command failure"},
        "warnings": [],
    }


def _decision_request() -> dict[str, Any]:
    return {
        "schema_version": 1,
        "action": "answer_decision",
        "request_id": REQUEST_ID,
        "dry_run": False,
        "target": {"worker_id": "worker-public"},
        "params": {"decision_ref": "decision-public", "selection": {"text": "yes"}},
    }


def _decision_accepted(request: dict[str, Any]) -> dict[str, Any]:
    return {
        "schema_version": 2,
        "action": request["action"],
        "request_id": request["request_id"],
        "ok": True,
        "dry_run": False,
        "status": "accepted",
        "disposition": "terminal_accepted",
        "result": {
            "target": request["target"],
            "decision": {"decision_ref": request["params"]["decision_ref"]},
            "delivery_state": "submitted",
            "transport_state": "submitted",
            "observed_pending_state": "pending_observation",
        },
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


def _connector_poll(items: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "schema_version": 1,
        "ok": True,
        "status": "ok",
        "host_id": "host-public",
        "name": "turn-final",
        "items": items,
    }


def _connector_item(ref: str = "twref1.public") -> dict[str, Any]:
    return {
        "ref": ref,
        "key": "turn-final:revision:twfinal1.public",
        "attempt": 1,
        "leased_until": "2026-08-05T12:01:00+00:00",
        "available_at": "2026-08-05T12:00:00+00:00",
        "payload": {"safe": True},
    }


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


@pytest.mark.parametrize(
    ("status", "disposition"),
    [
        ("pending", "in_progress"),
        ("request_state_uncertain", "terminal_uncertain"),
        ("rejected", "terminal_rejected"),
        ("backend_unavailable", "no_receipt"),
    ],
)
def test_command_failure_tuple_matrix_is_preserved(
    tmp_path: Path, status: str, disposition: str
) -> None:
    request = _command_request()
    response = _command_failure(request, status, disposition)
    with _daemon(tmp_path, [_return(response)]) as (client, calls):
        result = client.command(request)
    assert (result["status"], result["disposition"]) == (status, disposition)
    assert len(calls) == 1


@pytest.mark.parametrize(
    ("status", "disposition"),
    [
        ("accepted", "terminal_accepted"),
        ("answer_in_progress", "in_progress"),
        ("answer_in_progress", "no_receipt"),
        ("decision_not_pending", "terminal_rejected"),
    ],
)
def test_decision_response_tuple_matrix(
    tmp_path: Path, status: str, disposition: str
) -> None:
    request = _decision_request()
    response = (
        _decision_accepted(request)
        if status == "accepted"
        else _command_failure(request, status, disposition)
    )
    with _daemon(tmp_path, [_return(response)]) as (client, calls):
        result = client.command(request)
    assert result["status"] == status
    assert result["disposition"] == disposition
    assert calls[0]["method"] == "command.submit"


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


def test_outer_rpc_error_is_typed_and_distinct_from_inner_connector_results(
    tmp_path: Path,
) -> None:
    def outer_error(request: dict[str, Any]) -> bytes:
        envelope = {
            "schema_version": 1,
            "ok": False,
            "status": "error",
            "result": None,
            "error": {"code": "server_busy", "message": "try later"},
            "id": request["id"],
        }
        return json.dumps(envelope).encode() + b"\n"

    inner_failure = {
        "schema_version": 1,
        "ok": False,
        "status": "invalid_params",
        "host_id": "host-public",
        "name": "turn-final",
        "error": {"code": "invalid_params", "message": "bad limit"},
    }
    malformed_inner = {"ok": False, "status": "invalid_params"}
    with _daemon(
        tmp_path,
        [outer_error, outer_error, _return(inner_failure), _return(malformed_inner)],
    ) as (client, _calls):
        marked = client._request("connector.poll", {"name": "turn-final"})
        outer = client.turn_final_poll()
        inner = client.turn_final_poll()
        malformed = client.turn_final_poll()
    assert isinstance(marked, tendwire_client._RPCResult)
    assert marked.origin == "outer"
    assert marked["status"] == outer["status"] == "server_busy"
    assert inner["status"] == "invalid_params"
    assert malformed["status"] == "invalid_connector_response"


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


def test_socket_connect_is_anchored_when_configured_ancestor_is_replaced(
    tmp_path: Path, monkeypatch
) -> None:
    original_connect = socket.socket.connect
    moved = tmp_path / "daemon-pinned"
    seen: list[str] = []

    def replace_ancestor_then_connect(conn: socket.socket, address: str) -> None:
        seen.append(str(address))
        parent = tmp_path / "daemon"
        parent.rename(moved)
        parent.mkdir(mode=0o700)
        original_connect(conn, address)

    monkeypatch.setattr(socket.socket, "connect", replace_ancestor_then_connect)
    with _daemon(tmp_path, [_return({"ok": True})]) as (client, _calls):
        assert client.snapshot() == {"ok": True}
    assert len(seen) == 1
    assert seen[0].startswith("/proc/self/fd/")


@pytest.mark.parametrize(
    "credential_fault",
    ["unavailable", "malformed", "negative", "wrong_owner"],
)
def test_socket_peer_credentials_fail_closed(
    tmp_path: Path, monkeypatch, credential_fault: str
) -> None:
    parent = tmp_path / "peer"
    parent.mkdir(mode=0o700)
    path = parent / "tendwire.sock"
    listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    listener.bind(str(path))
    os.chmod(path, 0o600)
    listener.listen()
    accepted = threading.Event()

    def accept_once() -> None:
        conn, _ = listener.accept()
        accepted.set()
        conn.close()

    thread = threading.Thread(target=accept_once, daemon=True)
    thread.start()
    if credential_fault == "malformed":
        monkeypatch.setattr(socket.socket, "getsockopt", lambda *_args: b"bad")
    elif credential_fault == "negative":
        monkeypatch.setattr(
            socket.socket,
            "getsockopt",
            lambda *_args: struct.pack("3i", -1, os.geteuid(), os.getegid()),
        )
    elif credential_fault == "wrong_owner":
        monkeypatch.setattr(
            socket.socket,
            "getsockopt",
            lambda *_args: struct.pack("3i", os.getpid(), os.geteuid() + 1, os.getegid()),
        )
    else:
        monkeypatch.delattr(socket, "SO_PEERCRED")
    try:
        result = TendwireClient(socket_path=path, timeout=1).snapshot()
    finally:
        thread.join(timeout=2)
        listener.close()
    assert accepted.is_set()
    assert result["status"] == "daemon_unavailable"
    assert "peer" in result["error"]


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


def test_read_helpers_and_turn_schema_negotiation_map_exactly(tmp_path: Path) -> None:
    responses = [
        {"ok": True, "snapshot": []},
        {"ok": True, "pending": []},
        {"ok": True, "healthy": True},
        {"schema_version": 1, "turns": [], "has_more": False, "next_cursor": None},
    ]
    with _daemon(tmp_path, [_return(value) for value in responses]) as (client, calls):
        assert client.snapshot()["ok"] is True
        assert client.pending()["ok"] is True
        assert client.doctor()["healthy"] is True
        unsupported = client.turns()
    assert [call["method"] for call in calls] == [
        "snapshot.get",
        "pending.list",
        "health.get",
        "turn.list",
    ]
    assert unsupported["status"] == "upgrade_required"


def test_post_send_timeout_is_ambiguous_and_never_retried(tmp_path: Path) -> None:
    def delayed(_request: dict[str, Any]) -> None:
        import time

        time.sleep(1.1)
        return None

    with _daemon(tmp_path, [delayed]) as (client, calls):
        result = client.command(_command_request())
    assert result["status"] == "request_state_uncertain"
    assert tendwire_client.command_process_ambiguous(result)
    assert len(calls) == 1


def test_connector_helpers_use_connector_rpc_and_preserve_plan_token_bytes(tmp_path: Path) -> None:
    token = "twplan1.Exact_TOKEN-bytes"
    item = _connector_item()
    item["payload"] = {"plan_token": token}
    responses = [
        _connector_poll([item]),
        {"schema_version": 1, "ok": True, "status": "acknowledged", "host_id": "host-public", "name": "turn-final", "ref": "twref1.public", "key": item["key"], "attempt": 1},
        {"schema_version": 1, "ok": True, "status": "deferred", "host_id": "host-public", "name": "turn-final", "ref": "twref1.public", "key": item["key"], "attempt": 1, "available_at": "2026-08-05T12:02:00+00:00"},
        {"schema_version": 1, "ok": True, "status": "retry_scheduled", "host_id": "host-public", "name": "turn-final", "ref": "twref1.public", "key": item["key"], "attempt": 1, "available_at": "2026-08-05T12:03:00+00:00"},
        {"schema_version": 1, "ok": True, "status": "ok", "host_id": "host-public", "name": "turn-final", "plan_token": token, "state": "preparing", "generation": 1, "part_count": 1, "accepted_parts": 0},
        {"schema_version": 1, "ok": True, "status": "ok", "host_id": "host-public", "name": "turn-final", "plan_token": token, "ordinal": 0, "accepted_parts": 1},
        {"schema_version": 1, "ok": True, "status": "ok", "host_id": "host-public", "name": "turn-final", "plan_token": token, "state": "active", "generation": 1, "job_count": 1},
    ]
    with _daemon(tmp_path, [_return(v) for v in responses]) as (client, calls):
        polled = client.turn_final_poll(limit=2, lease_seconds=45)
        acked = client.turn_final_ack("twref1.public", {"plan_token": token, "message_id": "private"})
        deferred = client.turn_final_defer("twref1.public", "later", delay_seconds=5)
        failed = client.turn_final_fail("twref1.public", "retry")
        begun = client.connector_prepare_begin(turn_id="turn-1", content_revision="twrev1.public", presentation_version="v1", part_count=1, source_ref="twsource1.public")
        part = client.connector_prepare_part(plan_token=token, ordinal=0, spans=[{"field": "assistant_final_text", "start_char": 0, "end_char": 4}])
        committed = client.connector_prepare_commit(plan_token=token, source_ref="twsource1.public")
    assert polled["items"][0]["payload"]["plan_token"] == token
    assert acked["status"] == "acknowledged"
    assert begun["plan_token"] == token
    assert part["accepted_parts"] == 1
    assert committed["job_count"] == 1
    assert deferred["status"] == "deferred"
    assert failed["status"] == "retry_scheduled"
    assert [call["method"] for call in calls] == ["connector.poll", "connector.ack", "connector.defer", "connector.fail", "connector.prepare", "connector.prepare", "connector.prepare"]
    assert calls[1]["params"]["response"] == {"plan_token": token}
    assert calls[4]["params"]["source_ref"] == "twsource1.public"
    assert calls[6]["params"]["source_ref"] == "twsource1.public"


def test_ack_response_loss_is_not_retried_and_repoll_observes_authoritative_state(tmp_path: Path) -> None:
    state = {"acked": False}

    def poll(_request: dict[str, Any]) -> dict[str, Any]:
        response = _connector_poll(
            [] if state["acked"] else [_connector_item("twref1.loss")]
        )
        response["name"] = "attention"
        return response

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
    assert client.connector_prepare_recover(failed_plan_token="twplan1.public", request_id="bad:id")["status"] == "invalid_recovery_request"
    for reserved in ("telegram.request", "HERDRESrequest", "chat-id", "delivery_1"):
        assert client.connector_prepare_recover(
            failed_plan_token="twplan1.public", request_id=reserved
        )["status"] == "invalid_recovery_request"


def test_prepare_accepts_public_safe_recovery_request_id(tmp_path: Path) -> None:
    response = {
        "schema_version": 1,
        "ok": True,
        "status": "recovered",
        "failed_plan_token": "twplan1.failed",
        "plan_token": "twplan1.recovered",
        "generation": 2,
        "content_revision": "twrev1.public",
        "state": "active",
        "acknowledged_prefix_count": 0,
        "executable_job_count": 1,
        "retained_failed_job_count": 1,
        "prior_attempt_count": 1,
        "idempotent_replay": False,
    }
    with _daemon(tmp_path, [_return(response)]) as (client, calls):
        result = client.connector_prepare_recover(
            failed_plan_token="twplan1.failed",
            request_id="recover.request-42_ok",
        )
    assert result["status"] == "recovered"
    assert calls[0]["params"]["request_id"] == "recover.request-42_ok"


@pytest.mark.parametrize(
    "response",
    [
        {"ok": True, "status": "ok", "host_id": "host-public", "name": "turn-final", "items": []},
        {"schema_version": True, "ok": True, "status": "ok", "host_id": "host-public", "name": "turn-final", "items": []},
        {"schema_version": 1, "status": "ok", "host_id": "host-public", "name": "turn-final", "items": []},
        {"schema_version": 1, "ok": "yes", "status": "ok", "host_id": "host-public", "name": "turn-final", "items": []},
        {"schema_version": 1, "ok": True, "host_id": "host-public", "name": "turn-final", "items": []},
        {"schema_version": 1, "ok": True, "status": "ok", "host_id": "host-public", "name": "turn-final"},
        {"schema_version": 1, "ok": True, "status": "ok", "host_id": "host-public", "name": "turn-final", "items": [{"ref": "twref1.bad", "payload": {}}]},
        {"schema_version": 1, "ok": False, "status": "invalid_params", "host_id": "host-public", "name": "turn-final", "error": {"code": "wrong", "message": "bad"}},
    ],
)
def test_connector_schema_validation_rejects_malformed_envelopes(
    tmp_path: Path, response: dict[str, Any]
) -> None:
    with _daemon(tmp_path, [_return(response)]) as (client, _calls):
        result = client.turn_final_poll()
    assert result["status"] == "invalid_connector_response"


def test_configured_socket_and_timeout_are_bounded(monkeypatch) -> None:
    monkeypatch.setenv("HERDRES_TENDWIRE_SOCKET_PATH", "/tmp/tendwire-public.sock")
    monkeypatch.setenv("HERDRES_TENDWIRE_TIMEOUT_SECONDS", "0")
    assert config.tendwire_socket_path() == Path("/tmp/tendwire-public.sock")
    assert TendwireClient()._timeout_seconds() == 1.0
