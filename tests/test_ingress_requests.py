from __future__ import annotations

import copy
import json
import subprocess

import pytest

import herdres
import herdres_gateway
from herdres_connector import ingress_requests, state, tendwire_client

from test_source_only import (
    REQUEST_ID,
    REQUEST_ID_2,
    REQUEST_ID_KEY,
    _accepted_command_response,
    _failed_command_response,
    _source_worker,
    _store,
)


def _request(request_id: str = REQUEST_ID) -> dict[str, object]:
    return {
        "schema_version": 1,
        "action": "send_instruction",
        "request_id": request_id,
        "dry_run": False,
        "target": {
            "worker_id": "worker-1",
            "worker_fingerprint": "fp-original",
        },
        "instruction": {"text": "original instruction"},
    }


def _setup_command_state(tmp_path, monkeypatch, *, request_id: str = REQUEST_ID) -> None:
    monkeypatch.setenv("HERDR_TELEGRAM_TOPICS_STATE", str(tmp_path / "state.json"))
    monkeypatch.setenv("HERDRES_SOURCE_TOPIC_MODE", "worker")
    store = _store()
    state.upsert_worker_entry(
        store,
        _source_worker(
            {
                "id": "worker-1",
                "name": "Alpha",
                "status": "idle",
                "space_id": "space-1",
                "fingerprint": "fp-original",
            }
        ),
        topic_id="77",
    )
    state.save_state(store)


def _payload(request_id: str = REQUEST_ID) -> dict[str, str]:
    return {
        "request_id": request_id,
        "topic_id": "77",
        "message_id": "9001",
        "text": "/send original instruction",
    }


def _child(
    request_id: str,
    *,
    checkpoint: str,
    disposition: str | None,
    reply: str = "",
) -> dict[str, object]:
    return {
        "schema_version": 1,
        "handled": True,
        "request_id": request_id,
        "checkpoint": checkpoint,
        "disposition": disposition,
        "reply": reply,
    }


def _record(
    request_id: str,
    *,
    now: float = 100.0,
    with_request: bool = False,
    terminal: bool = False,
) -> dict[str, object]:
    scratch: dict[str, object] = {}
    record, _ = ingress_requests.ensure_request_shell(
        scratch,
        request_id,
        now=now,
        retry_horizon=60,
        retention=120,
    )
    if with_request or terminal:
        ingress_requests.attach_request_json(
            record,
            ingress_requests.canonical_request_json(_request(request_id)),
            now=now + 1,
        )
    if terminal:
        ingress_requests.mark_terminal(
            record,
            "terminal_accepted",
            now=now + 2,
            reply="Sent to Tendwire worker.",
        )
    return record


def test_record_bounds_do_not_slide_and_deadline_equality_quarantines() -> None:
    store: dict[str, object] = {}
    record, child, changed = ingress_requests.preflight_request(
        store,
        REQUEST_ID,
        now=100.0,
        retry_horizon=60,
        retention=120,
    )
    assert changed is True
    assert child is None
    assert (record["created_at"], record["deadline_at"], record["retain_until"]) == (
        100.0,
        160.0,
        220.0,
    )

    request_json = ingress_requests.canonical_request_json(_request())
    ingress_requests.attach_request_json(record, request_json, now=101.0)
    ingress_requests.mark_retryable(record, "no_receipt", now=130.0)
    before = (
        record["created_at"],
        record["deadline_at"],
        record["retain_until"],
    )

    same, child, changed = ingress_requests.preflight_request(
        store,
        REQUEST_ID,
        now=159.999,
        retry_horizon=600,
        retention=1200,
    )
    assert same is record
    assert child is None
    assert changed is False
    assert before == (
        record["created_at"],
        record["deadline_at"],
        record["retain_until"],
    )

    _, child, changed = ingress_requests.preflight_request(
        store,
        REQUEST_ID,
        now=160.0,
        retry_horizon=600,
        retention=1200,
    )
    assert changed is True
    assert child == _child(
        REQUEST_ID, checkpoint="advance", disposition=None, reply=ingress_requests.QUARANTINE_REPLY
    )
    assert record["state"] == "quarantined"
    assert before == (
        record["created_at"],
        record["deadline_at"],
        record["retain_until"],
    )


def test_pruning_is_strictly_after_immutable_retain_until() -> None:
    store: dict[str, object] = {}
    record, _ = ingress_requests.ensure_request_shell(
        store,
        REQUEST_ID,
        now=100.0,
        retry_horizon=60,
        retention=120,
    )
    ingress_requests.quarantine_request(record, "test quarantine", now=105.0)

    assert ingress_requests.prune_requests(store, now=220.0) is False
    assert REQUEST_ID in store[ingress_requests.RECORDS_KEY]
    assert ingress_requests.prune_requests(store, now=220.000001) is True
    assert REQUEST_ID not in store[ingress_requests.RECORDS_KEY]


def test_legacy_record_migrates_once_without_status_finality() -> None:
    legacy_request = _request()
    store = {
        ingress_requests.RECORDS_KEY: {
            REQUEST_ID: {
                "request": legacy_request,
                "created_at": 100.0,
                "updated_at": 125.0,
                "last_status": "accepted",
                "terminal_at": 125.0,
            }
        }
    }

    record, changed = ingress_requests.ensure_request_shell(
        store,
        REQUEST_ID,
        now=130.0,
        retry_horizon=60,
        retention=120,
    )
    assert changed is True
    assert record["state"] == "retryable"
    assert record["last_disposition"] is None
    assert record["outcome"] is None
    assert record["request_json"] == ingress_requests.canonical_request_json(
        legacy_request
    )
    assert (record["created_at"], record["deadline_at"], record["retain_until"]) == (
        100.0,
        160.0,
        220.0,
    )

    same, changed = ingress_requests.ensure_request_shell(
        store,
        REQUEST_ID,
        now=140.0,
        retry_horizon=600,
        retention=1200,
    )
    assert same is record
    assert changed is False


def test_corrupt_current_record_is_a_non_destructive_global_barrier() -> None:
    private = "123456:abcdefghijklmnopqrstuvwxyz_PRIVATE"
    corrupt_record = {
        "schema_version": 2,
        "created_at": 100.0,
        "request_json": private,
        "stderr": private,
    }
    store = {
        ingress_requests.RECORDS_KEY: {
            REQUEST_ID: corrupt_record,
        }
    }
    before = copy.deepcopy(store)

    with pytest.raises(
        RuntimeError, match="^ingress request record store is corrupt$"
    ):
        ingress_requests.preflight_request(
            store,
            REQUEST_ID,
            now=110.0,
            retry_horizon=60,
            retention=120,
        )

    assert store == before
    assert store[ingress_requests.RECORDS_KEY][REQUEST_ID] is corrupt_record


@pytest.mark.parametrize(
    "corrupt_record",
    [
        {
            "schema_version": 2,
            "request_id": REQUEST_ID,
            "created_at": -10_000.0,
            "updated_at": -9_999.0,
            "deadline_at": -9_940.0,
            "retain_until": -9_880.0,
            "state": "resolving",
            "request_json": None,
            "last_disposition": None,
            "stale_target_refreshed": False,
            "terminal_at": None,
            "quarantined_at": None,
            "quarantine_reason": None,
            "outcome": None,
            "unexpected_evidence": "invalidates otherwise plausible old bounds",
        },
        {
            "request": _request(),
            "created_at": -10_000.0,
            "updated_at": float("nan"),
        },
        {
            "schema_version": 2,
            "created_at": float("nan"),
            "deadline_at": float("nan"),
            "retain_until": float("nan"),
        },
        {
            "schema_version": 2,
            "created_at": True,
            "deadline_at": False,
            "retain_until": True,
        },
    ],
)
def test_malformed_record_blocks_prune_and_preflight_without_mutation(
    corrupt_record,
) -> None:
    expired = _record(REQUEST_ID_2)
    store = {
        ingress_requests.RECORDS_KEY: {
            REQUEST_ID_2: expired,
            REQUEST_ID: corrupt_record,
        }
    }
    before = copy.deepcopy(store)

    with pytest.raises(
        RuntimeError, match="^ingress request record store is corrupt$"
    ):
        ingress_requests.prune_requests(store, now=221.0)
    assert store == before

    with pytest.raises(
        RuntimeError, match="^ingress request record store is corrupt$"
    ):
        ingress_requests.preflight_request(
            store,
            REQUEST_ID,
            now=221.0,
            retry_horizon=60,
            retention=120,
        )
    assert store == before


def test_backend_unavailable_authority_comes_only_from_disposition(
    tmp_path, monkeypatch
) -> None:
    outcomes = [
        (REQUEST_ID, "no_receipt", "retry"),
        (REQUEST_ID_2, "terminal_rejected", "advance"),
    ]
    for index, (request_id, disposition, checkpoint) in enumerate(outcomes):
        case_path = tmp_path / str(index)
        case_path.mkdir()
        _setup_command_state(case_path, monkeypatch, request_id=request_id)
        calls: list[str] = []

        class Client:
            def command_json(self, request_json):
                calls.append(request_json)
                return _failed_command_response(
                    json.loads(request_json),
                    status="backend_unavailable",
                    disposition=disposition,
                )

        monkeypatch.setattr(herdres, "TendwireClient", Client)
        result = herdres.command_reply(_payload(request_id))
        assert result["checkpoint"] == checkpoint
        assert result["disposition"] == disposition
        assert result["reply"] == (
            "" if checkpoint == "retry" else herdres.SAFE_SEND_FAILURE_REPLY
        )
        assert len(calls) == 1


def test_terminal_uncertain_quarantines_and_restart_uses_cache(
    tmp_path, monkeypatch
) -> None:
    _setup_command_state(tmp_path, monkeypatch)
    calls: list[str] = []

    class Client:
        def command_json(self, request_json):
            calls.append(request_json)
            return _failed_command_response(
                json.loads(request_json),
                status="request_state_uncertain",
                disposition="terminal_uncertain",
            )

    monkeypatch.setattr(herdres, "TendwireClient", Client)
    first = herdres.command_reply(_payload())
    assert first == _child(
        REQUEST_ID,
        checkpoint="advance",
        disposition="terminal_uncertain",
        reply=ingress_requests.QUARANTINE_REPLY,
    )

    class ForbiddenClient:
        def __init__(self):
            raise AssertionError("terminal cache must bypass client creation")

    monkeypatch.setattr(herdres, "TendwireClient", ForbiddenClient)
    replay = herdres.command_reply(
        {
            "request_id": REQUEST_ID,
            "topic_id": "route-removed",
            "text": "different private replay",
        }
    )
    assert replay == first
    assert len(calls) == 1


def test_terminal_accepted_cache_survives_restart_and_route_loss(
    tmp_path, monkeypatch
) -> None:
    _setup_command_state(tmp_path, monkeypatch)
    calls: list[str] = []

    class Client:
        def command_json(self, request_json):
            calls.append(request_json)
            return _accepted_command_response(json.loads(request_json))

    monkeypatch.setattr(herdres, "TendwireClient", Client)
    first = herdres.command_reply(_payload())
    assert first == _child(
        REQUEST_ID,
        checkpoint="advance",
        disposition="terminal_accepted",
        reply="Sent to Tendwire worker.",
    )

    changed = state.load_state()
    changed["panes"] = {}
    state.save_state(changed)

    class ForbiddenClient:
        def __init__(self):
            raise AssertionError("terminal replay must not construct a client")

    monkeypatch.setattr(herdres, "TendwireClient", ForbiddenClient)
    assert herdres.command_reply(
        {"request_id": REQUEST_ID, "topic_id": "missing", "text": "changed"}
    ) == first
    assert len(calls) == 1


def test_exact_bytes_recover_accepted_response_loss_with_one_backend_send(
    tmp_path, monkeypatch
) -> None:
    _setup_command_state(tmp_path, monkeypatch)
    child_starts: list[bytes] = []
    backend_receipts: set[str] = set()
    backend_sends = 0

    def run(argv, *, input, **_kwargs):
        nonlocal backend_sends
        request_bytes = bytes(input)
        child_starts.append(request_bytes)
        saved = state.load_state()
        assert (
            saved[ingress_requests.RECORDS_KEY][REQUEST_ID][
                "request_json"
            ].encode()
            == request_bytes
        )
        request = json.loads(request_bytes)
        request_id = request["request_id"]
        if request_id not in backend_receipts:
            backend_receipts.add(request_id)
            backend_sends += 1
            return subprocess.CompletedProcess(argv, 0, b"not-json", b"private stderr")
        response = _accepted_command_response(request)
        return subprocess.CompletedProcess(
            argv,
            0,
            json.dumps(response, separators=(",", ":")).encode(),
            b"",
        )

    monkeypatch.setattr(tendwire_client.subprocess, "run", run)
    first = herdres.command_reply(_payload())
    assert first == _child(REQUEST_ID, checkpoint="retry", disposition=None)

    changed = state.load_state()
    changed["panes"] = {}
    state.save_state(changed)
    second = herdres.command_reply(
        {"request_id": REQUEST_ID, "topic_id": "gone", "text": "changed"}
    )
    assert second["checkpoint"] == "advance"
    assert second["disposition"] == "terminal_accepted"
    assert child_starts[0] == child_starts[1]
    assert backend_sends == 1
    assert "private stderr" not in json.dumps(first, sort_keys=True)


def test_stale_refresh_uses_real_client_validation_and_persists_second_bytes(
    tmp_path, monkeypatch
) -> None:
    _setup_command_state(tmp_path, monkeypatch)
    child_starts: list[bytes] = []
    backend_mutations = 0

    def run(argv, *, input, **_kwargs):
        nonlocal backend_mutations
        request_bytes = bytes(input)
        child_starts.append(request_bytes)
        request = json.loads(request_bytes)
        if len(child_starts) == 1:
            response = _failed_command_response(
                request, status="stale_target", disposition="no_receipt"
            )
            return subprocess.CompletedProcess(
                argv,
                1,
                json.dumps(response, separators=(",", ":")).encode(),
                b"",
            )
        backend_mutations += 1
        response = _accepted_command_response(request)
        return subprocess.CompletedProcess(
            argv,
            0,
            json.dumps(response, separators=(",", ":")).encode(),
            b"",
        )

    monkeypatch.setattr(tendwire_client.subprocess, "run", run)
    result = herdres.command_reply(_payload())
    assert result["disposition"] == "terminal_accepted"
    assert len(child_starts) == 2
    first = json.loads(child_starts[0])
    second = json.loads(child_starts[1])
    assert first["target"] == {
        "worker_id": "worker-1",
        "worker_fingerprint": "fp-original",
    }
    assert second["target"] == {"worker_id": "worker-1"}
    assert {key: value for key, value in first.items() if key != "target"} == {
        key: value for key, value in second.items() if key != "target"
    }
    saved = state.load_state()
    record = saved[ingress_requests.RECORDS_KEY][REQUEST_ID]
    assert record["stale_target_refreshed"] is True
    assert record["request_json"].encode() == child_starts[1]
    assert backend_mutations == 1


def test_deadline_equality_skips_client_creation(tmp_path, monkeypatch) -> None:
    _setup_command_state(tmp_path, monkeypatch)
    clock = {"now": 100.0}
    monkeypatch.setattr(herdres.time, "time", lambda: clock["now"])
    monkeypatch.setattr(
        herdres.config, "command_retry_horizon_seconds", lambda env=None: 60
    )
    monkeypatch.setattr(
        herdres.config, "command_request_retention_seconds", lambda env=None: 120
    )

    class RetryClient:
        def command_json(self, request_json):
            return _failed_command_response(
                json.loads(request_json),
                status="backend_unavailable",
                disposition="no_receipt",
            )

    monkeypatch.setattr(herdres, "TendwireClient", RetryClient)
    first = herdres.command_reply(_payload())
    assert first["checkpoint"] == "retry"

    clock["now"] = 160.0

    class ForbiddenClient:
        def __init__(self):
            raise AssertionError("deadline preflight must skip client creation")

    monkeypatch.setattr(herdres, "TendwireClient", ForbiddenClient)
    expired = herdres.command_reply(_payload())
    assert expired == _child(
        REQUEST_ID,
        checkpoint="advance",
        disposition=None,
        reply=ingress_requests.QUARANTINE_REPLY,
    )


def test_retryable_response_at_deadline_is_quarantined_not_retried(
    tmp_path, monkeypatch
) -> None:
    _setup_command_state(tmp_path, monkeypatch)
    clock = {"now": 100.0}
    monkeypatch.setattr(herdres.time, "time", lambda: clock["now"])
    monkeypatch.setattr(
        herdres.config, "command_retry_horizon_seconds", lambda env=None: 60
    )
    monkeypatch.setattr(
        herdres.config, "command_request_retention_seconds", lambda env=None: 120
    )

    class Client:
        def command_json(self, request_json):
            clock["now"] = 160.0
            return _failed_command_response(
                json.loads(request_json),
                status="backend_unavailable",
                disposition="no_receipt",
            )

    monkeypatch.setattr(herdres, "TendwireClient", Client)
    result = herdres.command_reply(_payload())
    assert result == _child(
        REQUEST_ID,
        checkpoint="advance",
        disposition=None,
        reply=ingress_requests.QUARANTINE_REPLY,
    )


def test_invalid_legacy_timestamps_are_preserved_behind_global_barrier() -> None:
    store = {
        ingress_requests.RECORDS_KEY: {
            REQUEST_ID: {
                "request": _request(),
                "created_at": 100.0,
                "updated_at": "not-a-timestamp",
            }
        }
    }
    before = copy.deepcopy(store)

    with pytest.raises(
        RuntimeError, match="^ingress request record store is corrupt$"
    ):
        ingress_requests.preflight_request(
            store,
            REQUEST_ID,
            now=110.0,
            retry_horizon=60,
            retention=120,
        )

    assert store == before


def test_malformed_v2_with_legacy_request_blocks_without_client_or_rewrite(
    tmp_path, monkeypatch
) -> None:
    _setup_command_state(tmp_path, monkeypatch)
    store = state.load_state()
    store[ingress_requests.RECORDS_KEY] = {
        REQUEST_ID: {
            "schema_version": 2,
            "request": _request(),
            "created_at": 100.0,
            "updated_at": 101.0,
            "state": "terminal",
            "terminal_at": 101.0,
        }
    }
    state.save_state(store)
    state_path = tmp_path / "state.json"
    original = state_path.read_bytes()
    monkeypatch.setattr(herdres.time, "time", lambda: 110.0)

    class ForbiddenClient:
        def __init__(self):
            raise AssertionError("malformed v2 evidence must never be replayed")

    monkeypatch.setattr(herdres, "TendwireClient", ForbiddenClient)
    with pytest.raises(
        RuntimeError, match="^ingress request record store is corrupt$"
    ):
        herdres.command_reply(_payload())
    assert state_path.read_bytes() == original


def test_direct_redelivery_blocks_terminal_evidence_under_different_key(
    tmp_path, monkeypatch
) -> None:
    _setup_command_state(tmp_path, monkeypatch)
    store = state.load_state()
    miskeyed_terminal = _record(REQUEST_ID, terminal=True)
    store[ingress_requests.RECORDS_KEY] = {
        REQUEST_ID_2: miskeyed_terminal,
    }
    state.save_state(store)
    state_path = tmp_path / "state.json"
    original = state_path.read_bytes()
    monkeypatch.setattr(herdres.time, "time", lambda: 110.0)

    class ForbiddenClient:
        def __init__(self):
            raise AssertionError("miskeyed terminal evidence must block Tendwire")

    monkeypatch.setattr(herdres, "TendwireClient", ForbiddenClient)
    with pytest.raises(
        RuntimeError, match="^ingress request record store is corrupt$"
    ):
        herdres.command_reply(_payload())

    assert state_path.read_bytes() == original
    assert (
        state.load_state()[ingress_requests.RECORDS_KEY][REQUEST_ID_2]
        == miskeyed_terminal
    )


def test_gateway_crash_window_redelivery_blocks_current_evidence_under_invalid_key(
    tmp_path, monkeypatch
) -> None:
    monkeypatch.setenv("HERDR_TELEGRAM_TOPICS_STATE", str(tmp_path / "state.json"))
    store = _store()
    crash_window_record = _record(REQUEST_ID, with_request=True)
    store[ingress_requests.RECORDS_KEY] = {
        "not-a-canonical-request-id": crash_window_record,
    }
    state.save_state(store)
    state_path = tmp_path / "state.json"
    original = state_path.read_bytes()
    monkeypatch.setattr(herdres_gateway.time, "time", lambda: 110.0)

    def forbidden_child(_payload):
        raise AssertionError("corrupt ingress evidence must block child creation")

    monkeypatch.setattr(herdres_gateway, "run_herdres_command", forbidden_child)
    with pytest.raises(
        RuntimeError, match="^ingress request record store is corrupt$"
    ):
        herdres_gateway.handle_update(
            {
                "update_id": 100,
                "message": {
                    "chat": {"id": -100, "is_forum": True},
                    "message_thread_id": 77,
                    "message_id": 9001,
                    "from": {"id": 1, "is_bot": False},
                    "text": "redelivered after response-loss crash",
                },
            },
            "receiver-token",
            receiver_id="manager",
            request_id_key=REQUEST_ID_KEY,
        )

    assert state_path.read_bytes() == original
    assert (
        state.load_state()[ingress_requests.RECORDS_KEY][
            "not-a-canonical-request-id"
        ]
        == crash_window_record
    )


def test_direct_resolving_shell_with_unrelated_malformed_record_blocks_globally(
    tmp_path, monkeypatch
) -> None:
    _setup_command_state(tmp_path, monkeypatch)
    store = state.load_state()
    current_shell = _record(REQUEST_ID)
    malformed_unrelated = {
        "schema_version": 2,
        "request_id": REQUEST_ID_2,
        "state": "terminal",
        "private_receipt": "ambiguous",
    }
    store[ingress_requests.RECORDS_KEY] = {
        REQUEST_ID: current_shell,
        REQUEST_ID_2: malformed_unrelated,
    }
    state.save_state(store)
    state_path = tmp_path / "state.json"
    original = state_path.read_bytes()
    monkeypatch.setattr(herdres.time, "time", lambda: 110.0)

    class ForbiddenClient:
        def __init__(self):
            raise AssertionError("unrelated corruption must block Tendwire")

    monkeypatch.setattr(herdres, "TendwireClient", ForbiddenClient)
    with pytest.raises(
        RuntimeError, match="^ingress request record store is corrupt$"
    ):
        herdres.command_reply(_payload())

    assert state_path.read_bytes() == original
    records = state.load_state()[ingress_requests.RECORDS_KEY]
    assert records[REQUEST_ID] == current_shell
    assert records[REQUEST_ID_2] == malformed_unrelated


def test_corrupt_record_container_is_a_global_non_destructive_barrier(
    tmp_path, monkeypatch
) -> None:
    _setup_command_state(tmp_path, monkeypatch)
    unknown_evidence = [
        {"request_id": REQUEST_ID, "private_receipt": "unknown"},
        {"request_id": REQUEST_ID_2, "private_receipt": "unknown"},
    ]
    store = state.load_state()
    store[ingress_requests.RECORDS_KEY] = unknown_evidence
    state.save_state(store)

    class ForbiddenClient:
        def __init__(self):
            raise AssertionError("corrupt global evidence must block every client")

    monkeypatch.setattr(herdres, "TendwireClient", ForbiddenClient)
    for request_id in (REQUEST_ID, REQUEST_ID_2):
        with pytest.raises(
            RuntimeError, match="ingress request record store is corrupt"
        ):
            herdres.command_reply(_payload(request_id))
        assert (
            state.load_state()[ingress_requests.RECORDS_KEY] == unknown_evidence
        )


def test_present_null_record_container_blocks_every_id_without_state_rewrite(
    tmp_path, monkeypatch
) -> None:
    _setup_command_state(tmp_path, monkeypatch)
    store = state.load_state()
    store[ingress_requests.RECORDS_KEY] = None
    state.save_state(store)
    state_path = tmp_path / "state.json"
    original = state_path.read_bytes()

    class ForbiddenClient:
        def __init__(self):
            raise AssertionError("present null evidence must block every client")

    monkeypatch.setattr(herdres, "TendwireClient", ForbiddenClient)
    for request_id in (REQUEST_ID, REQUEST_ID_2):
        with pytest.raises(
            RuntimeError, match="^ingress request record store is corrupt$"
        ):
            herdres.command_reply(_payload(request_id))
        assert state_path.read_bytes() == original


def test_corrupt_state_file_fails_closed_without_client_or_rewrite(
    tmp_path, monkeypatch
) -> None:
    state_path = tmp_path / "state.json"
    original = b'{\"unterminated\":'
    state_path.write_bytes(original)
    monkeypatch.setenv("HERDR_TELEGRAM_TOPICS_STATE", str(state_path))

    class ForbiddenClient:
        def __init__(self):
            raise AssertionError("corrupt durable state must prevent client creation")

    monkeypatch.setattr(herdres, "TendwireClient", ForbiddenClient)
    with pytest.raises(RuntimeError, match="state file is corrupt"):
        herdres.command_reply(_payload())
    assert state_path.read_bytes() == original
