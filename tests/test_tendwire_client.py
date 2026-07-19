from __future__ import annotations

import json
import os
from pathlib import Path
from types import SimpleNamespace

import pytest

from herdres_connector import tendwire_client
from herdres_connector.tendwire_client import TendwireClient


REQUEST_ID = "hri1_SIodGeqCeIvApzpEvIaEM-L07UzUMgUFyeltRQxPpqU"


def _command_request() -> dict[str, object]:
    return {
        "schema_version": 1,
        "action": "send_instruction",
        "request_id": REQUEST_ID,
        "dry_run": False,
        "target": {
            "worker_id": "worker-public",
            "worker_fingerprint": "fingerprint-public",
        },
        "instruction": {"text": "perform the public instruction"},
    }


def _accepted_result() -> dict[str, object]:
    return {
        "target": {"worker_id": "worker-public"},
        "delivery_state": "submitted",
        "transport_state": "submitted",
        "target_state_at_send": "working",
        "observed_turn_state": "pending_observation",
    }


def _command_response(
    *,
    status: str = "accepted",
    disposition: str | None = None,
    ok: bool = True,
    result: dict[str, object] | None = None,
) -> dict[str, object]:
    if disposition is None:
        if status == "accepted":
            disposition = "terminal_accepted"
        elif status == "pending":
            disposition = "in_progress"
        elif status == "request_state_uncertain":
            disposition = "terminal_uncertain"
        else:
            disposition = "terminal_rejected"
    return {
        "schema_version": 2,
        "action": "send_instruction",
        "request_id": REQUEST_ID,
        "ok": ok,
        "dry_run": False,
        "status": status,
        "disposition": disposition,
        "result": _accepted_result() if status == "accepted" and result is None else result,
        "error": None
        if ok
        else {"code": status, "message": f"{status} response"},
        "warnings": [],
    }




@pytest.fixture
def client_runner(monkeypatch):
    calls = []
    responses = []

    monkeypatch.setenv("HERDRES_TENDWIRE_BIN", "tw")

    def fake_run(argv, **kwargs):
        calls.append((argv, kwargs))
        response = responses.pop(0)
        body = response["body"]
        returncode = response.get("returncode")
        if returncode is None:
            returncode = (
                1
                if body.get("action") == "send_instruction"
                and body.get("ok") is False
                else 0
            )
        return SimpleNamespace(
            returncode=returncode,
            stdout=json.dumps(body, ensure_ascii=False).encode("utf-8"),
            stderr=response.get("stderr", "").encode("utf-8"),
        )

    monkeypatch.setattr(tendwire_client.subprocess, "run", fake_run)
    return TendwireClient(), calls, responses


def _assert_private_process_ambiguity(result):
    assert tendwire_client.command_process_ambiguous(result) is True
    assert tendwire_client.command_process_not_started(result) is False
    assert all(not key.startswith("_process") for key in result)
    assert "_process" not in json.dumps(result, sort_keys=True)
    serialized = json.loads(json.dumps(result, sort_keys=True))
    assert serialized == dict(result)
    assert tendwire_client.command_process_ambiguous(serialized) is False


def test_turn_delta_uses_one_bounded_cli_call_and_configured_timeout(
    client_runner, monkeypatch
):
    client, calls, responses = client_runner
    monkeypatch.setenv("HERDRES_TENDWIRE_TIMEOUT_SECONDS", "17.5")
    responses.append(
        {
            "body": {
                "schema_version": 1,
                "projection_schema_version": 2,
                "host_id": "host-public",
                "mode": "changes",
                "changes": [],
                "has_more": False,
                "next_cursor": None,
                "checkpoint": "twdelta1.next",
                "aggregate": {"changes_returned": 0},
            }
        }
    )

    result = client.turn_delta(watermark="twdelta1.current", limit=23)

    assert result["checkpoint"] == "twdelta1.next"
    assert len(calls) == 1
    assert calls[0][0] == [
        "tw",
        "turn",
        "delta",
        "--json",
        "--limit",
        "23",
        "--watermark",
        "twdelta1.current",
    ]
    assert calls[0][1]["timeout"] == 17.5


def test_turn_delta_normalizes_only_explicit_unknown_method_to_unsupported(
    client_runner,
):
    client, calls, responses = client_runner
    responses.append(
        {
            "returncode": 1,
            "body": {
                "schema_version": 1,
                "ok": False,
                "error": {"code": "unknown_method", "message": "unknown method"},
            },
        }
    )

    result = client.turn_delta()

    assert result["status"] == "unsupported_method"
    assert len(calls) == 1


def test_turn_delta_timeout_is_transport_ambiguous_and_never_retried(monkeypatch):
    calls = []
    monkeypatch.setenv("HERDRES_TENDWIRE_BIN", "tw")

    def timeout(*args, **kwargs):
        calls.append((args, kwargs))
        raise tendwire_client.subprocess.TimeoutExpired(args[0], kwargs["timeout"])

    monkeypatch.setattr(tendwire_client.subprocess, "run", timeout)

    result = TendwireClient(timeout=4).turn_delta()

    assert result["status"] == "transport_ambiguous"
    assert result["transport_status"] == "timeout"
    assert len(calls) == 1


def test_command_serializes_only_exact_allowlisted_public_request(client_runner):
    client, calls, responses = client_runner
    responses.append({"body": _command_response()})

    result = client.command(_command_request())

    assert result["status"] == "accepted"
    assert len(calls) == 1
    sent = json.loads(calls[0][1]["input"].decode("utf-8"))
    assert sent == _command_request()
    assert calls[0][1]["input"] == json.dumps(
        _command_request(),
        separators=(",", ":"),
        ensure_ascii=False,
    ).encode("utf-8")
    encoded = json.dumps(sent, sort_keys=True)
    assert all(
        forbidden not in encoded
        for forbidden in (
            "chat_id",
            "topic_id",
            "message_id",
            "reply_to_message_id",
            "update_id",
            "bot_token",
            "backend_target",
            "private-route-sentinel",
        )
    )


def test_command_json_repeats_verbatim_utf8_input_bytes(client_runner):
    client, calls, responses = client_runner
    responses.extend([{"body": _command_response()}, {"body": _command_response()}])
    request_json = json.dumps(
        _command_request(),
        ensure_ascii=False,
        indent=2,
    ).replace(
        '"perform the public instruction"',
        '"perform the public instruction \u2603"',
    )
    expected = request_json.encode("utf-8")

    first = client.command_json(request_json)
    second = client.command_json(request_json)

    assert first["disposition"] == second["disposition"] == "terminal_accepted"
    assert len(calls) == 2
    assert calls[0][1]["input"] == calls[1][1]["input"] == expected
    assert json.loads(expected)["instruction"]["text"].endswith(" \u2603")


@pytest.mark.parametrize("request_json", ["not-json", "[]", "{}", "\ud800"])
def test_command_json_rejects_invalid_input_without_spawning(
    client_runner,
    request_json,
):
    client, calls, _responses = client_runner

    result = client.command_json(request_json)

    assert result["status"] == "invalid_request"
    assert calls == []


@pytest.mark.parametrize(
    "mutate",
    [
        lambda request: request.update({"chat_id": "-100-private"}),
        lambda request: request["target"].update(
            {"backend_target": "private-route-sentinel"}
        ),
        lambda request: request["instruction"].update({"message_id": "901"}),
        lambda request: request.update({"params": {"origin": "telegram"}}),
    ],
)
def test_command_rejects_nonallowlisted_fields_without_serializing(
    client_runner, mutate
):
    client, calls, _responses = client_runner
    request = _command_request()
    mutate(request)

    result = client.command(request)

    assert result["ok"] is False
    assert result["status"] == "invalid_request"
    assert calls == []


def test_command_timeout_after_process_start_is_uncertain(monkeypatch):
    calls = []
    monkeypatch.setenv("HERDRES_TENDWIRE_BIN", "tw")

    def timeout(*args, **kwargs):
        calls.append((args, kwargs))
        raise tendwire_client.subprocess.TimeoutExpired(args[0], kwargs["timeout"])

    monkeypatch.setattr(tendwire_client.subprocess, "run", timeout)

    result = TendwireClient().command(_command_request())

    assert result["ok"] is False
    assert result["status"] == "request_state_uncertain"
    assert result["request_id"] == REQUEST_ID
    assert len(calls) == 1
    assert "before delivery" not in result["error"].lower()
    _assert_private_process_ambiguity(result)


@pytest.mark.parametrize(
    ("returncode", "body"),
    [
        pytest.param(
            0,
            _command_response(
                status="stale_target",
                ok=False,
                result={"candidates": []},
            ),
            id="exit-zero-with-rejection",
        ),
        pytest.param(1, _command_response(), id="exit-one-with-success"),
        pytest.param(
            2,
            _command_response(
                status="backend_unavailable",
                ok=False,
                result=None,
            ),
            id="exit-outside-contract-with-rejection",
        ),
        pytest.param(9, _command_response(), id="exit-outside-contract-with-success"),
        pytest.param(False, _command_response(), id="boolean-false-is-not-exit-zero"),
        pytest.param(
            True,
            _command_response(
                status="backend_unavailable",
                disposition="no_receipt",
                ok=False,
                result=None,
            ),
            id="boolean-true-is-not-exit-one",
        ),
    ],
)
def test_command_rejects_inconsistent_exit_body_matrix(
    client_runner,
    returncode,
    body,
):
    client, _calls, responses = client_runner
    responses.append({"returncode": returncode, "body": body})

    result = client.command(_command_request())

    assert result["ok"] is False
    assert result["status"] == "request_state_uncertain"
    assert result["request_id"] == REQUEST_ID
    assert result["action"] == "send_instruction"
    assert "result" not in result
    _assert_private_process_ambiguity(result)


def test_command_spawn_failure_remains_definite_and_single_attempt(monkeypatch):
    calls = []
    monkeypatch.setenv("HERDRES_TENDWIRE_BIN", "missing-tendwire")

    def missing(*args, **kwargs):
        calls.append((args, kwargs))
        raise FileNotFoundError("missing executable")

    monkeypatch.setattr(tendwire_client.subprocess, "run", missing)

    result = TendwireClient().command(_command_request())

    assert result["ok"] is False
    assert result["status"] == "subprocess_failed"
    assert len(calls) == 1
    assert tendwire_client.command_process_ambiguous(result) is False
    assert tendwire_client.command_process_not_started(result) is True
    assert all(not key.startswith("_process") for key in result)
    assert "_process" not in json.dumps(result, sort_keys=True)


@pytest.mark.parametrize(
    "stdout",
    [
        b"not-json",
        b"[]",
        b"{}",
        json.dumps(
            {
                "ok": True,
                "status": "accepted",
                "action": "send_instruction",
                "request_id": "hri1_AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
            }
        ).encode("utf-8"),
        json.dumps(
            {
                **_command_response(),
                "schema_version": 1,
            }
        ).encode("utf-8"),
        json.dumps(
            {
                **_command_response(),
                "request_id": "hri1_AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
            }
        ).encode("utf-8"),
    ],
)
def test_command_malformed_or_uncorrelated_child_result_is_uncertain(
    monkeypatch, stdout
):
    calls = []
    monkeypatch.setenv("HERDRES_TENDWIRE_BIN", "tw")

    def fake_run(*args, **kwargs):
        calls.append((args, kwargs))
        return SimpleNamespace(returncode=1, stdout=stdout, stderr=b"")

    monkeypatch.setattr(tendwire_client.subprocess, "run", fake_run)

    result = TendwireClient().command(_command_request())

    assert result["ok"] is False
    assert result["status"] == "request_state_uncertain"
    assert result["request_id"] == REQUEST_ID
    assert len(calls) == 1
    _assert_private_process_ambiguity(result)


def test_command_rejects_unknown_legacy_status_as_uncertain(client_runner):
    client, _calls, responses = client_runner
    responses.append(
        {
            "body": _command_response(
                status="duplicate_instruction",
                ok=True,
                result={"delivery_state": "duplicate_suppressed"},
            )
        }
    )

    result = client.command(_command_request())

    assert result["ok"] is False
    assert result["status"] == "request_state_uncertain"
    assert result["request_id"] == REQUEST_ID
    _assert_private_process_ambiguity(result)


@pytest.mark.parametrize(
    "mutate",
    [
        lambda body: body.update({"chat_id": "-100-private"}),
        lambda body: body.update({"_process_returncode": 0}),
        lambda body: body.update({"_process_ambiguity": "forged"}),
        lambda body: body.pop("disposition"),
        lambda body: body.update({"disposition": "unknown"}),
        lambda body: body.update({"schema_version": 1}),
        lambda body: body.update({"request_id": "hri1_AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"}),
        lambda body: body.update({"dry_run": True}),
        lambda body: body["error"]["details"].update({"private_fingerprint": "private"})
        if isinstance(body.get("error"), dict)
        else None,
    ],
)
def test_command_validates_exact_raw_schema_before_public_pruning(
    client_runner,
    mutate,
):
    client, _calls, responses = client_runner
    body = _command_response(
        status="stale_target",
        disposition="no_receipt",
        ok=False,
        result={"candidates": []},
    )
    body["error"]["details"] = {}
    mutate(body)
    responses.append({"returncode": 1, "body": body})

    result = client.command(_command_request())

    assert result["ok"] is False
    assert result["status"] == "request_state_uncertain"
    assert result["request_id"] == REQUEST_ID
    assert "private" not in json.dumps(result, sort_keys=True)
    _assert_private_process_ambiguity(result)


def test_command_preserves_correlated_stale_target_on_exit_one(client_runner):
    client, _calls, responses = client_runner
    responses.append(
        {
            "returncode": 1,
            "body": _command_response(
                status="stale_target",
                disposition="no_receipt",
                ok=False,
                result={"candidates": []},
            ),
        }
    )

    result = client.command(_command_request())

    assert result["ok"] is False
    assert result["status"] == "stale_target"
    assert result["disposition"] == "no_receipt"
    assert result["request_id"] == REQUEST_ID
    assert result["result"] == {"candidates": []}
    assert tendwire_client.command_process_ambiguous(result) is False
    assert tendwire_client.command_process_not_started(result) is False
    assert "_process" not in json.dumps(result, sort_keys=True)


@pytest.mark.parametrize(
    ("status", "disposition"),
    [
        *[
            (status, "terminal_rejected")
            for status in (
                "rejected",
                "stale_target",
                "backend_unavailable",
                "backend_unsupported",
                "ambiguous_backend_target",
                "backend_failed",
                "duplicate_request",
            )
        ],
        *[
            (status, "no_receipt")
            for status in (
                "rejected",
                "not_found",
                "ambiguous_target",
                "stale_target",
                "backend_unsupported",
                "ambiguous_backend_target",
                "backend_failed",
                "invalid_request",
                "backend_unavailable",
            )
        ],
        ("request_state_uncertain", "terminal_uncertain"),
        ("pending", "in_progress"),
    ],
)
def test_command_accepts_every_recognized_false_tuple(
    client_runner,
    status,
    disposition,
):
    client, _calls, responses = client_runner
    responses.append(
        {
            "returncode": 1,
            "body": _command_response(
                status=status,
                disposition=disposition,
                ok=False,
                result={"candidates": []},
            ),
        }
    )

    result = client.command(_command_request())

    assert result["ok"] is False
    assert (result["status"], result["disposition"]) == (status, disposition)
    assert tendwire_client.command_process_ambiguous(result) is False


@pytest.mark.parametrize(
    "body",
    [
        _command_response(status="accepted", ok=False, result=_accepted_result()),
        _command_response(status="rejected", ok=True, result={"candidates": []}),
        _command_response(
            status="request_state_uncertain",
            ok=True,
            result={"candidates": []},
        ),
        _command_response(
            status="subprocess_failed",
            ok=False,
            result=None,
        ),
        {
            **_command_response(
                status="stale_target",
                ok=False,
                result={"candidates": []},
            ),
            "error": None,
        },
        {
            **_command_response(
                status="stale_target",
                ok=False,
                result={"candidates": []},
            ),
            "error": {
                "code": "rejected",
                "message": "mismatched error code",
            },
        },
        _command_response(
            status="accepted",
            disposition="no_receipt",
            ok=True,
            result=_accepted_result(),
        ),
        _command_response(
            status="rejected",
            disposition="terminal_accepted",
            ok=False,
            result={"candidates": []},
        ),
        _command_response(
            status="pending",
            disposition="no_receipt",
            ok=False,
            result=None,
        ),
        *[
            _command_response(
                status=status,
                disposition="terminal_rejected",
                ok=False,
                result=None,
            )
            for status in ("not_found", "ambiguous_target", "invalid_request")
        ],
        _command_response(
            status="request_state_uncertain",
            disposition="terminal_rejected",
            ok=False,
            result=None,
        ),
        _command_response(
            status="duplicate_request",
            disposition="no_receipt",
            ok=False,
            result=None,
        ),
        _command_response(
            status="backend_unavailable",
            disposition="in_progress",
            ok=False,
            result=None,
        ),
        {
            **_command_response(
                status="stale_target",
                disposition="no_receipt",
                ok=False,
                result={"candidates": []},
            ),
            "error": {"code": "stale_target", "message": ""},
        },
        _command_response(status="accepted", ok=True, result={}),
        _command_response(
            status="accepted",
            ok=True,
            result={
                **_accepted_result(),
                "delivery_state": "queued",
            },
        ),
        _command_response(
            status="accepted",
            ok=True,
            result={
                **_accepted_result(),
                "target": {"worker_id": "wrong-worker"},
            },
        ),
    ],
)
def test_command_rejects_inconsistent_status_or_accepted_result_as_uncertain(
    client_runner,
    body,
):
    client, _calls, responses = client_runner
    responses.append({"body": body})

    result = client.command(_command_request())

    assert result["ok"] is False
    assert result["status"] == "request_state_uncertain"
    assert result["request_id"] == REQUEST_ID
    _assert_private_process_ambiguity(result)


def test_command_invalid_utf8_after_process_start_is_uncertain(monkeypatch):
    monkeypatch.setenv("HERDRES_TENDWIRE_BIN", "tw")
    monkeypatch.setattr(
        tendwire_client.subprocess,
        "run",
        lambda *_args, **_kwargs: SimpleNamespace(
            returncode=1,
            stdout=b"\xff",
            stderr=b"",
        ),
    )

    result = TendwireClient().command(_command_request())

    assert result["ok"] is False
    assert result["status"] == "request_state_uncertain"
    assert result["request_id"] == REQUEST_ID
    _assert_private_process_ambiguity(result)


def test_tendwire_child_env_strips_private_ingress_and_keeps_tendwire_overrides(
    monkeypatch,
):
    monkeypatch.setenv(
        "HERDRES_TENDWIRE_BIN",
        "env TELEGRAM_BOT_TOKEN=explicit-secret TENDWIRE_HOST_ID=host-public tw",
    )
    private = {
        "HERDRES_OUTBOUND_BOT_TOKEN": "outbound-secret",
        "BOT_TOKEN": "generic-secret",
        "HERDRES_TELEGRAM_CHAT_ID": "-100-private",
        "HERDR_TELEGRAM_TOPICS_STATE": "/private/herdres-state.json",
        "HERDRES_MANAGED_BOT_CODEX_TOKEN": "managed-secret",
        "HERDRES_REQUEST_ID_KEY_PATH": "/private/request-id.key",
        "HERDRES_GATEWAY_COMMAND_TIMEOUT": "123",
    }
    for key, value in private.items():
        monkeypatch.setenv(key, value)
    monkeypatch.setenv("TENDWIRE_DB_PATH", "/public/tendwire.db")

    child_env = TendwireClient()._env()

    assert child_env["TENDWIRE_HOST_ID"] == "host-public"
    assert child_env["TENDWIRE_DB_PATH"] == "/public/tendwire.db"
    assert "TELEGRAM_BOT_TOKEN" not in child_env
    assert all(key not in child_env for key in private)
    assert "HERDRES_TENDWIRE_BIN" not in child_env
    assert "TENDWIRE_BIN" not in child_env
    assert all(
        "secret" not in value and "/private/" not in value
        for value in child_env.values()
    )


def test_explicit_tendwire_binary_does_not_inject_source_checkout(
    tmp_path: Path,
    monkeypatch,
) -> None:
    source = tmp_path / "dirty-source"
    source.mkdir()
    monkeypatch.setenv("HERDRES_TENDWIRE_BIN", "/installed/bin/tendwire")
    monkeypatch.setenv("TENDWIRE_SOURCE_DIR", str(source))
    monkeypatch.setenv("PYTHONPATH", "/existing/runtime")

    child_env = TendwireClient()._env()

    assert child_env["PYTHONPATH"] == "/existing/runtime"
    assert str(source) not in child_env["PYTHONPATH"].split(os.pathsep)


def test_implicit_development_client_retains_source_checkout_fallback(
    tmp_path: Path,
    monkeypatch,
) -> None:
    source = tmp_path / "source"
    source.mkdir()
    monkeypatch.delenv("HERDRES_TENDWIRE_BIN", raising=False)
    monkeypatch.delenv("TENDWIRE_BIN", raising=False)
    monkeypatch.setenv("TENDWIRE_SOURCE_DIR", str(source))
    monkeypatch.setenv("PYTHONPATH", "/existing/runtime")

    child_env = TendwireClient()._env()

    assert child_env["PYTHONPATH"].split(os.pathsep) == [
        str(source),
        "/existing/runtime",
    ]


def test_path_qualified_env_wrapper_cannot_reintroduce_private_secret(monkeypatch):
    calls = []
    secret = "path-wrapper-private-secret"
    monkeypatch.setenv(
        "HERDRES_TENDWIRE_BIN",
        f"/usr/bin/env TELEGRAM_BOT_TOKEN={secret} "
        "TENDWIRE_HOST_ID=host-public tw",
    )

    def fake_run(argv, **kwargs):
        calls.append((argv, kwargs))
        return SimpleNamespace(
            returncode=0,
            stdout=json.dumps(_command_response()).encode("utf-8"),
            stderr=b"",
        )

    monkeypatch.setattr(tendwire_client.subprocess, "run", fake_run)

    result = TendwireClient().command(_command_request())

    assert result["status"] == "accepted"
    assert len(calls) == 1
    argv, kwargs = calls[0]
    assert argv == ["tw", "command", "--json"]
    assert kwargs["env"]["TENDWIRE_HOST_ID"] == "host-public"
    assert "TELEGRAM_BOT_TOKEN" not in kwargs["env"]
    assert secret not in json.dumps(argv)
    assert all(secret not in value for value in kwargs["env"].values())




def test_turn_final_lease_seconds_default_invalid_and_bounds():
    lease_seconds = tendwire_client.config.tendwire_turn_final_lease_seconds

    assert lease_seconds(env={}) == 60
    assert lease_seconds(
        env={"HERDRES_TENDWIRE_TURN_FINAL_LEASE_SECONDS": ""}
    ) == 60
    assert lease_seconds(
        env={"HERDRES_TENDWIRE_TURN_FINAL_LEASE_SECONDS": "invalid"}
    ) == 60
    assert lease_seconds(
        env={"HERDRES_TENDWIRE_TURN_FINAL_LEASE_SECONDS": "120"}
    ) == 120
    assert lease_seconds(
        env={"HERDRES_TENDWIRE_TURN_FINAL_LEASE_SECONDS": "59"}
    ) == 60
    assert lease_seconds(
        env={"HERDRES_TENDWIRE_TURN_FINAL_LEASE_SECONDS": "3601"}
    ) == 3600


def test_turn_final_poll_uses_connector_process_timeout(client_runner):
    client, calls, responses = client_runner
    responses.append(
        {
            "body": {
                "schema_version": 1,
                "ok": True,
                "status": "ok",
                "items": [],
            }
        }
    )

    result = client.turn_final_poll(limit=1, lease_seconds=60)

    assert result["ok"] is True
    assert len(calls) == 1
    assert (
        calls[0][1]["timeout"]
        == tendwire_client.CONNECTOR_PROCESS_TIMEOUT_SECONDS
        == 20
    )


def test_turns_requests_and_requires_v2_content_schema(client_runner):
    client, calls, responses = client_runner
    responses.append(
        {
            "body": {
                "schema_version": 2,
                "turns": [
                    {
                        "id": "turn-1",
                        "content": {
                            "schema_version": 1,
                            "content_revision": "twrev1.public",
                            "fields": {},
                        },
                    }
                ],
            }
        }
    )

    result = client.turns()

    assert result["schema_version"] == 2
    assert calls[0][0] == [
        "tw",
        "turns",
        "--schema-version",
        "2",
        "--limit",
        "250",
        "--json",
    ]
    assert calls[0][1]["input"] is None

    responses.extend(
        [
            {"body": {"schema_version": 1, "turns": []}},
            {"body": {"schema_version": 2, "turns": [{"id": "turn-1"}]}},
        ]
    )
    old = client.turns()
    missing_content = client.turns()

    assert old["ok"] is False
    assert old["status"] == "upgrade_required"
    assert old["required_turn_schema_version"] == 2
    assert missing_content["ok"] is False
    assert missing_content["status"] == "unsupported_content_schema"
    assert missing_content["supported_content_schema_version"] == 1


def test_turns_follows_bounded_list_cursors(client_runner):
    client, calls, responses = client_runner

    def row(turn_id):
        return {
            "id": turn_id,
            "content": {
                "schema_version": 1,
                "content_revision": f"twrev1.{turn_id}",
                "fields": {},
            },
        }

    responses.extend(
        [
            {
                "body": {
                    "schema_version": 2,
                    "turns": [row("first")],
                    "has_more": True,
                    "next_cursor": "twlist1.public",
                }
            },
            {
                "body": {
                    "schema_version": 2,
                    "turns": [row("second")],
                    "has_more": False,
                    "next_cursor": None,
                }
            },
        ]
    )

    result = client.turns()

    assert [item["id"] for item in result["turns"]] == ["first", "second"]
    assert result["has_more"] is False
    assert result["next_cursor"] is None
    assert calls[1][0] == [
        "tw",
        "turns",
        "--schema-version",
        "2",
        "--limit",
        "250",
        "--json",
        "--cursor",
        "twlist1.public",
    ]


def test_turns_preserves_typed_upgrade_error_from_cli(client_runner):
    client, _calls, responses = client_runner
    responses.append(
        {
            "returncode": 1,
            "body": {
                "ok": False,
                "status": "upgrade_required",
                "required_turn_schema_version": 2,
                "error": "turn schema v2 required",
            },
        }
    )

    result = client.turns()

    assert result == {
        "ok": False,
        "status": "upgrade_required",
        "required_turn_schema_version": 2,
        "error": "turn schema v2 required",
    }


def test_turn_content_get_uses_exact_bounded_cli_and_preserves_page_text(client_runner):
    client, calls, responses = client_runner
    page = "\r\n  e\u0301 雪\t\n" + ("x" * (48 * 1024 - 20)) + "  \r\n"
    responses.append(
        {
            "body": {
                "schema_version": 1,
                "turn_id": "turn-1",
                "content_revision": "twrev1.public",
                "field": "assistant_final_text",
                "availability": "complete",
                "segment_id": "twseg1.public",
                "index": 1,
                "count": 2,
                "text": page,
                "segment_char_length": len(page),
                "segment_byte_length": len(page.encode("utf-8")),
                "total_char_length": 50000,
                "total_byte_length": 50003,
                "next_cursor": None,
            }
        }
    )

    result = client.turn_content_get(
        "turn-1",
        "twrev1.public",
        "assistant_final_text",
        cursor="twcur1.public",
    )

    assert result["text"] == page
    assert result["text"].encode("utf-8") == page.encode("utf-8")
    assert calls[0][0] == [
        "tw",
        "turn",
        "content",
        "get",
        "--json",
        "--turn-id",
        "turn-1",
        "--revision",
        "twrev1.public",
        "--field",
        "assistant_final_text",
        "--cursor",
        "twcur1.public",
    ]
    assert calls[0][1]["input"] is None


def test_turn_content_get_omits_null_cursor_and_rejects_schema(client_runner):
    client, calls, responses = client_runner
    responses.append({"body": {"schema_version": 2, "text": "not v1"}})

    result = client.turn_content_get("turn-1", "twrev1.public", "user_text")

    assert "--cursor" not in calls[0][0]
    assert result["ok"] is False
    assert result["status"] == "unsupported_content_schema"
    assert result["supported_content_schema_version"] == 1


def test_prepare_begin_part_commit_send_only_neutral_bounded_json(client_runner):
    client, calls, responses = client_runner
    responses.extend(
        [
            {"body": {"schema_version": 1, "ok": True, "plan_token": "twplan1.public", "state": "preparing", "part_count": 2, "accepted_parts": 0}},
            {"body": {"schema_version": 1, "ok": True, "plan_token": "twplan1.public", "ordinal": 0, "accepted_parts": 1}},
            {"body": {"schema_version": 1, "ok": True, "plan_token": "twplan1.public", "state": "active", "job_count": 2}},
        ]
    )

    begin = client.connector_prepare_begin(
        turn_id="turn-1",
        content_revision="twrev1.public",
        presentation_version="herdres-rich-v3",
        part_count=2,
    )
    part = client.connector_prepare_part(
        plan_token=begin["plan_token"],
        ordinal=0,
        spans=[
            {"field": "user_text", "start_char": 0, "end_char": 4},
            {"field": "assistant_final_text", "start_char": 0, "end_char": 3900},
        ],
    )
    commit = client.connector_prepare_commit(plan_token=part["plan_token"])

    expected_argv = ["tw", "connector", "prepare", "--name", "turn-final", "--json"]
    assert [call[0] for call in calls] == [expected_argv, expected_argv, expected_argv]
    payloads = [json.loads(call[1]["input"].decode("utf-8")) for call in calls]
    assert payloads == [
        {
            "schema_version": 1,
            "action": "begin",
            "name": "turn-final",
            "turn_id": "turn-1",
            "content_revision": "twrev1.public",
            "presentation_version": "herdres-rich-v3",
            "part_count": 2,
        },
        {
            "schema_version": 1,
            "action": "part",
            "name": "turn-final",
            "plan_token": "twplan1.public",
            "ordinal": 0,
            "spans": [
                {"field": "user_text", "start_char": 0, "end_char": 4},
                {"field": "assistant_final_text", "start_char": 0, "end_char": 3900},
            ],
        },
        {
            "schema_version": 1,
            "action": "commit",
            "name": "turn-final",
            "plan_token": "twplan1.public",
        },
    ]
    encoded = b" ".join(call[1]["input"] for call in calls)
    assert b'"text"' not in encoded
    assert b'"user_text"' in encoded  # coordinate enum only
    assert b'"assistant_final_text"' in encoded  # coordinate enum only
    assert begin["plan_token"] == part["plan_token"] == commit["plan_token"]



def test_prepare_source_ref_is_exact_and_optional_and_ack_prunes_provider_ids(client_runner):
    client, calls, responses = client_runner
    responses.extend(
        [
            {
                "body": {
                    "schema_version": 1,
                    "ok": True,
                    "plan_token": "twplan1.source",
                    "state": "preparing",
                }
            },
            {
                "body": {
                    "schema_version": 1,
                    "ok": True,
                    "plan_token": "twplan1.source",
                    "state": "active",
                    "job_count": 1,
                }
            },
            {
                "body": {
                    "schema_version": 1,
                    "ok": True,
                    "status": "acknowledged",
                }
            },
        ]
    )
    source_ref = "twref1.exact_live_source"

    client.connector_prepare_begin(
        turn_id="turn-source",
        content_revision="twrev1.source",
        presentation_version="turn-present-v27",
        part_count=1,
        source_ref=source_ref,
    )
    client.connector_prepare_commit(
        plan_token="twplan1.source",
        source_ref=source_ref,
    )
    client.turn_final_ack(
        "twref1.part",
        {
            "outcome": "applied",
            "message_id": "901",
            "nested": {
                "chat_id": "-100",
                "topic_id": "77",
                "job_key": "turn-final:twplan1.source:000000",
            },
        },
    )

    prepare_payloads = [
        json.loads(calls[index][1]["input"].decode("utf-8"))
        for index in (0, 1)
    ]
    assert prepare_payloads[0]["source_ref"] == source_ref
    assert prepare_payloads[1]["source_ref"] == source_ref
    assert json.loads(calls[2][0][-1]) == {
        "outcome": "applied",
        "nested": {
            "job_key": "turn-final:twplan1.source:000000",
        },
    }
    encoded = json.dumps(prepare_payloads).lower()
    assert all(
        forbidden not in encoded
        for forbidden in ("telegram", "chat_id", "topic_id", "message_id")
    )

def test_prepare_part_rejects_content_or_non_range_fields_without_calling_cli(client_runner):
    client, calls, _responses = client_runner

    result = client.connector_prepare_part(
        plan_token="twplan1.public",
        ordinal=0,
        spans=[
            {
                "field": "assistant_final_text",
                "start_char": 0,
                "end_char": 4,
                "text": "must never cross prepare",
            }
        ],
    )

    assert result["status"] == "invalid_prepare_part"
    assert calls == []


def test_turn_final_methods_use_dedicated_queue_and_preserve_public_plan_tokens(client_runner):
    client, calls, responses = client_runner
    responses.extend(
        [
            {
                "body": {
                    "schema_version": 1,
                    "ok": True,
                    "items": [
                        {
                            "ref": "twref1.lease",
                            "key": "turn-final:twplan1.public:000000",
                            "payload": {
                                "schema_version": 1,
                                "plan_token": "twplan1.public",
                                "replaces_plan_token": "twplan1.old",
                                "spans": [{"field": "assistant_final_text", "start_char": 0, "end_char": 4}],
                            },
                        }
                    ],
                }
            },
            {"body": {"schema_version": 1, "ok": True, "status": "delivered"}},
            {"body": {"schema_version": 1, "ok": True, "status": "retry"}},
            {"body": {"schema_version": 1, "ok": True, "status": "deferred"}},
        ]
    )

    poll = client.turn_final_poll(limit=2, lease_seconds=45)
    client.turn_final_ack("twref1.lease", {"outcome": "applied"})
    client.turn_final_fail("twref1.next", "temporary")
    client.turn_final_defer("twref1.next", "rate limited", delay_seconds=30)

    assert poll["items"][0]["payload"]["plan_token"] == "twplan1.public"
    assert poll["items"][0]["payload"]["replaces_plan_token"] == "twplan1.old"
    assert [call[0] for call in calls] == [
        ["tw", "connector", "poll", "--name", "turn-final", "--limit", "2", "--lease-seconds", "45"],
        ["tw", "connector", "ack", "--name", "turn-final", "--ref", "twref1.lease", "--response-json", '{"outcome":"applied"}'],
        ["tw", "connector", "fail", "--name", "turn-final", "--ref", "twref1.next", "--reason", "temporary"],
        ["tw", "connector", "defer", "--name", "turn-final", "--ref", "twref1.next", "--reason", "rate limited", "--delay-seconds", "30"],
    ]


def test_prepare_and_turn_final_refuse_unsupported_connector_schema(client_runner):
    client, _calls, responses = client_runner
    responses.extend(
        [
            {"body": {"schema_version": 2, "ok": True, "plan_token": "twplan2.unknown"}},
            {"body": {"ok": True, "items": []}},
        ]
    )

    prepare = client.connector_prepare_begin(
        turn_id="turn-1",
        content_revision="twrev1.public",
        presentation_version="herdres-rich-v3",
        part_count=1,
    )
    poll = client.turn_final_poll()

    assert prepare["ok"] is False
    assert prepare["status"] == "unsupported_content_schema"
    assert prepare["supported_content_schema_version"] == 1
    assert poll["ok"] is False
    assert poll["status"] == "unsupported_content_schema"
    assert poll["supported_content_schema_version"] == 1


def test_attention_connector_calls_are_unchanged(client_runner, monkeypatch):
    client, calls, responses = client_runner
    monkeypatch.setattr(tendwire_client.config, "tendwire_db_path", lambda: Path("/private/tendwire.db"))
    responses.extend(
        [
            {"body": {"ok": True, "items": []}},
            {"body": {"ok": True}},
            {"body": {"ok": True}},
        ]
    )

    client.connector_poll()
    client.connector_ack("twref1.attention", {"duplicate": True})
    client.connector_fail("twref1.attention", "failed")

    assert [call[0] for call in calls] == [
        ["tw", "connector", "poll", "--db-path", "/private/tendwire.db", "--name", "attention", "--limit", "3", "--lease-seconds", "60"],
        ["tw", "connector", "ack", "--db-path", "/private/tendwire.db", "--name", "attention", "--ref", "twref1.attention", "--response-json", '{"duplicate":true}'],
        ["tw", "connector", "fail", "--db-path", "/private/tendwire.db", "--name", "attention", "--ref", "twref1.attention", "--error", "failed"],
    ]


def test_recover_failed_turn_final_uses_exact_bounded_prepare_action(client_runner):
    client, calls, responses = client_runner
    responses.append(
        {
            "body": {
                "schema_version": 1,
                "ok": True,
                "status": "recovered",
                "failed_plan_token": "twplan1.failed",
                "plan_token": "twplan1.replacement",
                "generation": 2,
                "content_revision": "twrev1.public",
                "state": "active",
                "acknowledged_prefix_count": 1,
                "executable_job_count": 2,
                "retained_failed_job_count": 1,
                "prior_attempt_count": 4,
                "idempotent_replay": False,
            }
        }
    )

    result = client.connector_prepare_recover(
        failed_plan_token="twplan1.failed",
        request_id="operator-2026.07.11:1",
    )

    assert result["plan_token"] == "twplan1.replacement"
    assert calls[0][0] == [
        "tw",
        "connector",
        "prepare",
        "--name",
        "turn-final",
        "--json",
    ]
    assert json.loads(calls[0][1]["input"].decode("utf-8")) == {
        "schema_version": 1,
        "action": "recover",
        "name": "turn-final",
        "failed_plan_token": "twplan1.failed",
        "request_id": "operator-2026.07.11:1",
    }


@pytest.mark.parametrize(
    ("failed_plan_token", "request_id"),
    [
        ("not-a-plan", "request-1"),
        ("twplan1.failed", ""),
        ("twplan1.failed", "contains space"),
        ("twplan1.failed", "x" * 129),
    ],
)
def test_recover_failed_turn_final_rejects_invalid_public_coordinates_without_rpc(
    client_runner,
    failed_plan_token,
    request_id,
):
    client, calls, _responses = client_runner

    result = client.connector_prepare_recover(
        failed_plan_token=failed_plan_token,
        request_id=request_id,
    )

    assert result["ok"] is False
    assert result["status"] == "invalid_recovery_request"
    assert calls == []
