from __future__ import annotations

import json
import os
import shlex
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

from herdres_connector import tendwire_client
from herdres_connector.tendwire_client import TendwireClient


REQUEST_ID = "hri1_SIodGeqCeIvApzpEvIaEM-L07UzUMgUFyeltRQxPpqU"


def _paired_tendwire_source() -> Path:
    default = Path(__file__).resolve().parents[2] / "tendwire-goal12" / "src"
    source = Path(
        os.environ.get("HERDRES_PAIRED_TENDWIRE_SOURCE_DIR", str(default))
    )
    if not (source / "tendwire" / "cli.py").is_file():
        pytest.skip(f"paired Tendwire source is unavailable at {source}")
    return source


def _command_request(*, dry_run: bool) -> dict[str, object]:
    return {
        "schema_version": 1,
        "action": "send_instruction",
        "request_id": REQUEST_ID,
        "dry_run": dry_run,
        "target": {
            "worker_id": "worker-public",
            "worker_fingerprint": "fingerprint-public",
        },
        "instruction": {"text": "verify the paired CLI contract"},
    }


def _paired_env(tmp_path: Path, source: Path) -> dict[str, str]:
    env = os.environ.copy()
    current_pythonpath = env.get("PYTHONPATH", "")
    env.update(
        {
            "HOME": str(tmp_path),
            "PYTHONPATH": (
                str(source)
                if not current_pythonpath
                else f"{source}{os.pathsep}{current_pythonpath}"
            ),
            "TENDWIRE_DATA_DIR": str(tmp_path / "data"),
            "TENDWIRE_DB_PATH": str(tmp_path / "tendwire.db"),
            "TENDWIRE_HOST_ID": "herdres-paired-cli",
            "TENDWIRE_HERDR_BACKEND": "socket",
            "TENDWIRE_SOCKET_PATH": str(tmp_path / "missing-tendwire.sock"),
        }
    )
    return env


def _run_paired_cli(
    request: dict[str, object],
    *,
    env: dict[str, str],
) -> tuple[subprocess.CompletedProcess[bytes], dict[str, object]]:
    completed = subprocess.run(
        [sys.executable, "-m", "tendwire.cli", "command", "--json"],
        input=json.dumps(request, separators=(",", ":")).encode("utf-8"),
        capture_output=True,
        check=False,
        env=env,
        timeout=20,
    )
    body = json.loads(completed.stdout.decode("utf-8"))
    assert isinstance(body, dict)
    return completed, body


def test_real_tendwire_cli_exit_code_matches_command_ok(tmp_path):
    source = _paired_tendwire_source()
    env = _paired_env(tmp_path, source)

    accepted, accepted_body = _run_paired_cli(
        _command_request(dry_run=True),
        env=env,
    )
    rejected, rejected_body = _run_paired_cli(
        _command_request(dry_run=False),
        env=env,
    )

    assert [
        (
            accepted.returncode,
            accepted_body["ok"],
            accepted_body["status"],
            accepted_body["disposition"],
        ),
        (
            rejected.returncode,
            rejected_body["ok"],
            rejected_body["status"],
            rejected_body["disposition"],
        ),
    ] == [
        (0, True, "dry_run", "no_receipt"),
        (1, False, "backend_unavailable", "no_receipt"),
    ]
    assert accepted_body["request_id"] == rejected_body["request_id"] == REQUEST_ID
    assert accepted_body["action"] == rejected_body["action"] == "send_instruction"
    assert accepted_body["schema_version"] == rejected_body["schema_version"] == 2
    expected_fields = {
        "schema_version",
        "action",
        "request_id",
        "ok",
        "dry_run",
        "status",
        "disposition",
        "result",
        "error",
        "warnings",
    }
    assert set(accepted_body) == set(rejected_body) == expected_fields


def test_herdres_client_preserves_real_exit_one_rejection_without_retry(
    tmp_path,
    monkeypatch,
):
    source = _paired_tendwire_source()
    env = _paired_env(tmp_path, source)
    for key in (
        "HOME",
        "PYTHONPATH",
        "TENDWIRE_DATA_DIR",
        "TENDWIRE_DB_PATH",
        "TENDWIRE_HOST_ID",
        "TENDWIRE_HERDR_BACKEND",
        "TENDWIRE_SOCKET_PATH",
    ):
        monkeypatch.setenv(key, env[key])
    monkeypatch.setenv("TENDWIRE_SOURCE_DIR", str(source))
    monkeypatch.setenv(
        "HERDRES_TENDWIRE_BIN",
        f"{shlex.quote(sys.executable)} -m tendwire.cli",
    )

    real_run = tendwire_client.subprocess.run
    calls = []

    def recording_run(*args, **kwargs):
        calls.append((args, kwargs))
        return real_run(*args, **kwargs)

    monkeypatch.setattr(tendwire_client.subprocess, "run", recording_run)

    result = TendwireClient(timeout=20).command(_command_request(dry_run=False))

    assert len(calls) == 1
    assert result["ok"] is False
    assert result["status"] == "backend_unavailable"
    assert result["schema_version"] == 2
    assert result["disposition"] == "no_receipt"
    assert result["request_id"] == REQUEST_ID
    assert result["action"] == "send_instruction"
    assert getattr(result, "_process_ambiguity", None) is None
    assert tendwire_client.command_process_ambiguous(result) is False
    assert tendwire_client.command_process_not_started(result) is False
    assert all(not key.startswith("_process") for key in result)
    assert "_process" not in json.dumps(result, sort_keys=True)


def test_herdres_live_tuple_matrix_matches_paired_tendwire_producer(
    monkeypatch,
):
    source = _paired_tendwire_source()
    env = os.environ.copy()
    current_pythonpath = env.get("PYTHONPATH", "")
    env["PYTHONPATH"] = (
        str(source)
        if not current_pythonpath
        else f"{source}{os.pathsep}{current_pythonpath}"
    )
    producer = subprocess.run(
        [
            sys.executable,
            "-c",
            """
import json
from tendwire.core.commands import CommandEnvelope, VALID_DISPOSITIONS, VALID_STATUSES

request_id = "hri1_SIodGeqCeIvApzpEvIaEM-L07UzUMgUFyeltRQxPpqU"
accepted_result = {
    "target": {"worker_id": "worker-public"},
    "delivery_state": "submitted",
    "transport_state": "submitted",
    "target_state_at_send": "working",
    "observed_turn_state": "pending_observation",
}
decision_result = {
    "target": {"worker_id": "worker-public"},
    "decision": {"decision_ref": "decision-public"},
    "delivery_state": "submitted",
    "transport_state": "submitted",
    "observed_pending_state": "pending_observation",
}
accepted = []
for action in ("send_instruction", "answer_decision"):
    ok_result = accepted_result if action == "send_instruction" else decision_result
    for ok in (False, True):
        for status in sorted(VALID_STATUSES):
            for disposition in sorted(VALID_DISPOSITIONS):
                try:
                    envelope = CommandEnvelope(
                        schema_version=2,
                        action=action,
                        request_id=request_id,
                        ok=ok,
                        dry_run=False,
                        status=status,
                        disposition=disposition,
                        result=ok_result if ok else None,
                        error=None
                        if ok
                        else {"code": status, "message": "paired failure"},
                        warnings=[],
                    )
                except (TypeError, ValueError):
                    continue
                accepted.append({
                    "tuple": [action, ok, status, disposition],
                    "body": envelope.to_dict(),
                })
print(json.dumps({
    "statuses": sorted(VALID_STATUSES),
    "dispositions": sorted(VALID_DISPOSITIONS),
    "accepted": accepted,
}))
""",
        ],
        capture_output=True,
        check=False,
        env=env,
        timeout=20,
    )
    assert producer.returncode == 0, producer.stderr.decode("utf-8", "replace")
    matrix = json.loads(producer.stdout.decode("utf-8"))
    producer_accepted = {
        tuple(item["tuple"]): item["body"]
        for item in matrix["accepted"]
    }
    decision_statuses = {
        "decision_not_pending",
        "unknown_worker",
        "invalid_selection",
        "unsupported_decision",
    }

    current_body = {}

    def fake_run(*_args, **_kwargs):
        return SimpleNamespace(
            returncode=0 if current_body["ok"] else 1,
            stdout=json.dumps(current_body, separators=(",", ":")).encode("utf-8"),
            stderr=b"",
        )

    monkeypatch.setenv("HERDRES_TENDWIRE_BIN", "tw")
    monkeypatch.setattr(tendwire_client.subprocess, "run", fake_run)
    request = _command_request(dry_run=False)
    accepted_result = {
        "target": {"worker_id": "worker-public"},
        "delivery_state": "submitted",
        "transport_state": "submitted",
        "target_state_at_send": "working",
        "observed_turn_state": "pending_observation",
    }
    client = TendwireClient()

    decision_request = {
        "schema_version": 1,
        "action": "answer_decision",
        "request_id": REQUEST_ID,
        "dry_run": False,
        "target": {"worker_id": "worker-public"},
        "params": {
            "decision_ref": "decision-public",
            "selection": {"option_refs": ["1"]},
        },
    }
    for action, driven in (("send_instruction", request), ("answer_decision", decision_request)):
        for ok in (False, True):
            for status in matrix["statuses"]:
                for disposition in matrix["dispositions"]:
                    key = (action, ok, status, disposition)
                    producer_body = producer_accepted.get(key)
                    current_body = producer_body or {
                        "schema_version": 2,
                        "action": action,
                        "request_id": REQUEST_ID,
                        "ok": ok,
                        "dry_run": False,
                        "status": status,
                        "disposition": disposition,
                        "result": accepted_result if ok else None,
                        "error": None
                        if ok
                        else {"code": status, "message": "paired failure"},
                        "warnings": [],
                    }
                    result = client.command(driven)
                    if action == "send_instruction":
                        # The send path fails closed on the decision-only statuses the
                        # paired Tendwire can now construct but never semantically emits
                        # for a send: they surface as request_state_uncertain.
                        allowed = producer_body is not None and status not in decision_statuses
                    else:
                        # The decision path accepts exactly ok+accepted or a typed
                        # decision failure; every other producer-constructible envelope
                        # fails closed.
                        allowed = producer_body is not None and (
                            (ok and status == "accepted")
                            or (not ok and status in decision_statuses)
                        )
                    if allowed:
                        assert result == producer_body
                        assert tendwire_client.command_process_ambiguous(result) is False
                    else:
                        assert result["status"] == "request_state_uncertain"
                        assert result.get("disposition") is None
                        assert result.get("result") is None
                        assert tendwire_client.command_process_ambiguous(result) is True
                    assert "_process" not in json.dumps(result, sort_keys=True)
