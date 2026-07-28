from __future__ import annotations

import ast
import copy
import json
import subprocess
from pathlib import Path

import pytest

import herdres
import herdres_gateway
from herdres_connector import ingress_requests, source_sync, state, tendwire_client

from test_source_only import (
    FakeTelegram,
    FakeTendwire,
    REQUEST_ID,
    REQUEST_ID_2,
    REQUEST_ID_KEY,
    _accepted_command_response,
    _failed_command_response,
    _source_worker,
    _store,
)
from test_turn_final_delivery import _turn_row


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
    transport = ingress_requests.normalize_transport_disposition(disposition)
    if disposition == "terminal_accepted":
        checkpoint = "hold"
        phase = "accepted_unverified"
        terminal_outcome = "delivery_unknown"
        reply = ""
    elif disposition == "terminal_rejected":
        checkpoint = "hold"
        phase = "terminal"
        terminal_outcome = "not_delivered"
        reply = ""
    elif disposition == "terminal_uncertain":
        checkpoint = "hold"
        phase = "terminal"
        terminal_outcome = "delivery_unknown"
    elif checkpoint == "advance":
        checkpoint = "hold"
        phase = "terminal"
        terminal_outcome = "not_delivered"
    else:
        phase = "retryable"
        terminal_outcome = None
    return {
        "schema_version": 2,
        "handled": True,
        "request_id": request_id,
        "checkpoint": checkpoint,
        "transport_disposition": transport,
        "request_phase": phase,
        "terminal_outcome": terminal_outcome,
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


def _old_v3_record(
    request_id: str,
    *,
    now: float = 100.0,
    terminal_disposition: str | None = None,
    linked: bool = False,
) -> dict[str, object]:
    current = _record(
        request_id,
        now=now,
        with_request=True,
    )
    for field in (
        "transport_disposition",
        "request_phase",
        "terminal_outcome",
        "checkpoint_already_advanced",
        "operator_attention_required",
        "blocked_reason",
        "next_action",
        "dedup_witness",
    ):
        current.pop(field)
    current["schema_version"] = 3
    current["last_disposition"] = terminal_disposition
    if terminal_disposition is not None:
        current["state"] = (
            "quarantined"
            if terminal_disposition == "terminal_uncertain"
            else "terminal"
        )
        current["updated_at"] = now + 2
        current["terminal_at"] = (
            None if current["state"] == "quarantined" else now + 2
        )
        current["quarantined_at"] = (
            now + 2 if current["state"] == "quarantined" else None
        )
        current["quarantine_reason"] = (
            "legacy uncertainty"
            if current["state"] == "quarantined"
            else None
        )
        current["outcome"] = {
            "schema_version": 1,
            "handled": True,
            "request_id": request_id,
            "checkpoint": "advance",
            "disposition": terminal_disposition,
            "reply": (
                "Sent to Tendwire worker."
                if terminal_disposition == "terminal_accepted"
                else "Could not send safely."
            ),
        }
        if linked:
            current.update(
                {
                    "submission_id": "submission-verified",
                    "submission_state": "linked",
                    "turn_id": "turn-verified",
                    "target_owner": {
                        "stable_key": f"wsk1_{'a' * 64}",
                        "stable_key_version": 1,
                    },
                    "submitted_at": now + 1,
                    "linked_at": now + 2,
                }
            )
    return current


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
    assert record["transport_disposition"] is None
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


def test_v2_record_migrates_additively_to_current_schema() -> None:
    original = _old_v3_record(REQUEST_ID)
    v2 = {
        key: copy.deepcopy(value)
        for key, value in original.items()
        if key
        not in {
            "submission_id",
            "submission_state",
            "turn_id",
            "target_owner",
            "submitted_at",
            "linked_at",
        }
    }
    v2["schema_version"] = 2
    store = {ingress_requests.RECORDS_KEY: {REQUEST_ID: v2}}

    migrated, changed = ingress_requests.ensure_request_shell(
        store,
        REQUEST_ID,
        now=130.0,
        retry_horizon=60,
        retention=120,
    )

    assert changed is True
    assert migrated["schema_version"] == 4
    assert migrated["request_json"] == v2["request_json"]
    assert {
        key: migrated[key]
        for key in (
            "submission_id",
            "submission_state",
            "turn_id",
            "target_owner",
            "submitted_at",
            "linked_at",
        )
    } == {
        "submission_id": None,
        "submission_state": None,
        "turn_id": None,
        "target_owner": None,
        "submitted_at": None,
        "linked_at": None,
    }


def test_not_delivered_outcome_leaves_checkpoint_unadvanced() -> None:
    record = _record(REQUEST_ID, with_request=True)

    outcome = ingress_requests.record_terminal_outcome(
        record,
        transport_disposition="terminal_rejected",
        request_phase="terminal",
        terminal_outcome="not_delivered",
        now=103.0,
        blocked_reason="provider rejected the request",
        next_action="review before retry",
    )

    assert outcome["checkpoint"] == "hold"
    assert outcome["terminal_outcome"] == "not_delivered"
    assert record["checkpoint_already_advanced"] is False
    assert record["operator_attention_required"] is True


def test_delivery_unknown_leaves_checkpoint_unadvanced_and_never_retries() -> None:
    record = _record(REQUEST_ID, terminal=True)

    assert record["terminal_outcome"] == "delivery_unknown"
    assert record["outcome"]["checkpoint"] == "hold"
    assert record["checkpoint_already_advanced"] is False
    assert ingress_requests.automatic_replay_authorized(record) is False

    # Even evidence that would authorize a replay of a proven non-delivery
    # cannot silently resolve an already-unknown outcome.
    ingress_requests.record_dedup_witness(
        record,
        prompt_fingerprint="herdr-prompt-fingerprint",
        composer_fingerprint="different-composer-fingerprint",
        composer_readable=True,
        provider_verdict="agent_prompt_not_received",
        owner_generation="pane-generation-1",
        now=104.0,
    )
    assert record["dedup_witness"]["automatic_replay_authorized"] is False
    assert ingress_requests.automatic_replay_authorized(record) is False


def test_delivery_unknown_is_loud_in_operator_readable_state() -> None:
    store: dict[str, object] = {}
    record, _ = ingress_requests.ensure_request_shell(
        store,
        REQUEST_ID,
        now=100.0,
        retry_horizon=60,
        retention=120,
    )
    ingress_requests.attach_request_json(
        record,
        ingress_requests.canonical_request_json(_request()),
        now=101.0,
    )
    ingress_requests.mark_terminal(
        record,
        "terminal_accepted",
        now=102.0,
        reply="must not be emitted",
    )

    rows = ingress_requests.operator_status_rows(store, now=110.0)

    assert rows == [
        {
            "request_id": REQUEST_ID,
            "age_seconds": 10.0,
            "state": "terminal",
            "transport_disposition": "written_to_pty",
            "request_phase": "accepted_unverified",
            "terminal_outcome": "delivery_unknown",
            "checkpoint_already_advanced": False,
            "operator_attention_required": True,
            "blocked_reason": "transport accepted but submission was not verified",
            "next_action": "inspect the composer before any replay",
            "submission_id": None,
            "turn_id": None,
            "target_owner": None,
            "dedup_witness": None,
        }
    ]
    assert "original instruction" not in json.dumps(rows)


def test_ingress_status_cli_surfaces_unknown_without_prompt_text(
    tmp_path, monkeypatch, capsys
) -> None:
    state_path = tmp_path / "state.json"
    monkeypatch.setattr(herdres.config, "load_env_file", lambda: None)
    monkeypatch.setenv("HERDR_TELEGRAM_TOPICS_STATE", str(state_path))
    store = _store()
    record, _ = ingress_requests.ensure_request_shell(
        store,
        REQUEST_ID,
        now=100.0,
        retry_horizon=60,
        retention=120,
    )
    ingress_requests.attach_request_json(
        record,
        ingress_requests.canonical_request_json(_request()),
        now=101.0,
    )
    ingress_requests.mark_terminal(
        record,
        "terminal_accepted",
        now=102.0,
        reply="must not be emitted",
    )
    state.save_state(store, path=state_path)
    monkeypatch.setattr(herdres.time, "time", lambda: 110.0)

    assert herdres.cmd_ingress_status(None) == 0
    output = capsys.readouterr().out
    payload = json.loads(output)

    assert payload["attention_required"] == 1
    assert payload["requests"][0]["terminal_outcome"] == "delivery_unknown"
    assert payload["requests"][0]["next_action"] == (
        "inspect the composer before any replay"
    )
    assert "original instruction" not in output


def test_terminal_accepted_can_never_be_a_terminal_outcome() -> None:
    record = _record(REQUEST_ID, with_request=True)

    with pytest.raises(
        ValueError, match="only verified delivery may advance checkpoint"
    ):
        ingress_requests.child_result(
            REQUEST_ID,
            checkpoint="advance",
            transport_disposition="terminal_accepted",
            request_phase="terminal",
            terminal_outcome="delivered",
        )

    with pytest.raises(ValueError, match="invalid terminal outcome"):
        ingress_requests.record_terminal_outcome(
            record,
            transport_disposition="terminal_accepted",
            request_phase="terminal",
            terminal_outcome="terminal_accepted",
            now=103.0,
        )

    outcome = ingress_requests.mark_terminal(
        record,
        "terminal_accepted",
        now=103.0,
        reply="must not advance",
    )
    assert outcome["transport_disposition"] == "written_to_pty"
    assert outcome["terminal_outcome"] == "delivery_unknown"
    assert outcome["checkpoint"] == "hold"
    assert "disposition" not in outcome


_INGRESS_RESULT_FACTORIES = {
    "child_result",
    "command_reply",
    "mark_retryable",
    "mark_terminal",
    "quarantine_request",
    "record_terminal_outcome",
    "run_herdres_command",
    "_submit_ingress_command_record",
}
_REMOVED_INGRESS_RESULT_KEYS = {"disposition", "last_disposition"}


def _local_nodes(scope: ast.AST) -> list[ast.AST]:
    """Walk one lexical scope without borrowing aliases from nested scopes."""

    roots = list(getattr(scope, "body", []))
    found: list[ast.AST] = []
    pending = list(reversed(roots))
    while pending:
        node = pending.pop()
        found.append(node)
        if isinstance(
            node,
            (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef, ast.Lambda),
        ):
            continue
        pending.extend(reversed(list(ast.iter_child_nodes(node))))
    return found


def _call_name(node: ast.AST) -> str:
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Attribute):
        return node.attr
    return ""


def _scope_facts(
    scope: ast.AST,
    *,
    factories: set[str],
    parameter_aliases: set[str],
) -> tuple[list[ast.AST], set[str], dict[str, str]]:
    nodes = _local_nodes(scope)
    aliases = set(parameter_aliases)
    constant_values: dict[str, set[str]] = {}
    constant_aliases: list[tuple[str, str]] = []
    for node in nodes:
        if not isinstance(node, (ast.Assign, ast.AnnAssign)):
            continue
        targets = node.targets if isinstance(node, ast.Assign) else [node.target]
        for target in targets:
            if not isinstance(target, ast.Name):
                continue
            if isinstance(node.value, ast.Constant) and isinstance(
                node.value.value, str
            ):
                constant_values.setdefault(target.id, set()).add(node.value.value)
            elif isinstance(node.value, ast.Name):
                constant_aliases.append((target.id, node.value.id))
    changed = True
    while changed:
        changed = False
        for target, source in constant_aliases:
            inherited = constant_values.get(source, set())
            values = constant_values.setdefault(target, set())
            before = len(values)
            values.update(inherited)
            changed = changed or len(values) != before
    constants = {
        name: next(iter(values))
        for name, values in constant_values.items()
        if len(values) == 1
    }
    changed = True
    while changed:
        changed = False
        for node in nodes:
            if not isinstance(node, (ast.Assign, ast.AnnAssign)):
                continue
            value = node.value
            targets = node.targets if isinstance(node, ast.Assign) else [node.target]
            for target in targets:
                if not isinstance(target, ast.Name):
                    continue
                is_result = (
                    isinstance(value, ast.Call)
                    and _call_name(value.func) in factories
                ) or (isinstance(value, ast.Name) and value.id in aliases)
                if is_result and target.id not in aliases:
                    aliases.add(target.id)
                    changed = True
    return nodes, aliases, constants


def _removed_ingress_result_reads(
    source: str,
    *,
    filename: str = "<snippet>",
) -> list[tuple[str, int, str]]:
    tree = ast.parse(source, filename=filename)
    definitions = {
        node.name: node
        for node in ast.walk(tree)
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
    }
    factories = set(_INGRESS_RESULT_FACTORIES)
    parameter_aliases: dict[str, set[str]] = {
        name: set() for name in definitions
    }

    # Resolve factory wrappers and argument flow together. A helper that returns
    # an ingress result remains a factory, and a parameter receiving one remains
    # an ingress-result alias inside that helper.
    changed = True
    while changed:
        changed = False
        scopes: list[ast.AST] = [tree, *definitions.values()]
        for scope in scopes:
            scope_name = getattr(scope, "name", "")
            nodes, aliases, _constants = _scope_facts(
                scope,
                factories=factories,
                parameter_aliases=parameter_aliases.get(scope_name, set()),
            )
            if scope_name and scope_name not in factories:
                if any(
                    isinstance(node, ast.Return)
                    and node.value is not None
                    and (
                        (
                            isinstance(node.value, ast.Call)
                            and _call_name(node.value.func) in factories
                        )
                        or (
                            isinstance(node.value, ast.Name)
                            and node.value.id in aliases
                        )
                    )
                    for node in nodes
                ):
                    factories.add(scope_name)
                    changed = True
            for node in nodes:
                if not isinstance(node, ast.Call):
                    continue
                definition = definitions.get(_call_name(node.func))
                if definition is None:
                    continue
                positional = list(definition.args.posonlyargs) + list(
                    definition.args.args
                )
                for argument, parameter in zip(node.args, positional):
                    is_result = (
                        isinstance(argument, ast.Call)
                        and _call_name(argument.func) in factories
                    ) or (
                        isinstance(argument, ast.Name)
                        and argument.id in aliases
                    )
                    if (
                        is_result
                        and parameter.arg
                        not in parameter_aliases[definition.name]
                    ):
                        parameter_aliases[definition.name].add(parameter.arg)
                        changed = True
                keyword_parameters = {
                    parameter.arg: parameter
                    for parameter in (
                        list(definition.args.posonlyargs)
                        + list(definition.args.args)
                        + list(definition.args.kwonlyargs)
                    )
                }
                for keyword in node.keywords:
                    parameter = keyword_parameters.get(keyword.arg or "")
                    is_result = (
                        isinstance(keyword.value, ast.Call)
                        and _call_name(keyword.value.func) in factories
                    ) or (
                        isinstance(keyword.value, ast.Name)
                        and keyword.value.id in aliases
                    )
                    if (
                        parameter is not None
                        and is_result
                        and parameter.arg
                        not in parameter_aliases[definition.name]
                    ):
                        parameter_aliases[definition.name].add(parameter.arg)
                        changed = True

    violations: list[tuple[str, int, str]] = []
    for scope in [tree, *definitions.values()]:
        scope_name = getattr(scope, "name", "")
        nodes, aliases, constants = _scope_facts(
            scope,
            factories=factories,
            parameter_aliases=parameter_aliases.get(scope_name, set()),
        )

        def is_result(node: ast.AST) -> bool:
            return (
                isinstance(node, ast.Name) and node.id in aliases
            ) or (
                isinstance(node, ast.Call)
                and _call_name(node.func) in factories
            )

        def key_value(node: ast.AST) -> str | None:
            if isinstance(node, ast.Constant) and isinstance(node.value, str):
                return node.value
            if isinstance(node, ast.Name):
                return constants.get(node.id)
            return None

        for node in nodes:
            key = None
            owner = None
            if isinstance(node, ast.Subscript):
                key = key_value(node.slice)
                owner = node.value
            elif (
                isinstance(node, ast.Call)
                and isinstance(node.func, ast.Attribute)
                and node.func.attr == "get"
                and node.args
            ):
                key = key_value(node.args[0])
                owner = node.func.value
            elif (
                isinstance(node, ast.Call)
                and _call_name(node.func) == "getattr"
                and len(node.args) >= 2
            ):
                key = key_value(node.args[1])
                owner = node.args[0]
            elif (
                isinstance(node, ast.Attribute)
                and node.attr in _REMOVED_INGRESS_RESULT_KEYS
            ):
                key = node.attr
                owner = node.value
            if (
                key in _REMOVED_INGRESS_RESULT_KEYS
                and owner is not None
                and is_result(owner)
            ):
                violations.append((filename, node.lineno, str(key)))
            if isinstance(node, ast.Call):
                for keyword in node.keywords:
                    if keyword.arg is None and is_result(keyword.value):
                        violations.append(
                            (filename, keyword.value.lineno, "**ingress_result")
                        )
    return sorted(set(violations))


def test_ingress_result_consumers_do_not_read_removed_disposition_keys() -> None:
    """Reject old flat-key reads across every ordinary repository module."""

    repository = Path(__file__).resolve().parents[1]
    violations: list[tuple[str, int, str]] = []
    for path in sorted(repository.rglob("*.py")):
        if any(part.startswith(".") for part in path.relative_to(repository).parts):
            continue
        relative = str(path.relative_to(repository))
        violations.extend(
            _removed_ingress_result_reads(
                path.read_text(encoding="utf-8"),
                filename=relative,
            )
        )
    assert violations == []


def _structured_ingress_result() -> ingress_requests.IngressResult:
    return ingress_requests.child_result(
        REQUEST_ID,
        checkpoint="hold",
        transport_disposition="written_to_pty",
        request_phase="accepted_unverified",
        terminal_outcome="delivery_unknown",
    )


def test_ingress_result_guard_tracks_computed_removed_keys() -> None:
    result = _structured_ingress_result()
    key = "disposition"
    with pytest.raises(KeyError, match="removed ingress result field"):
        eval("result.get(key)", {"result": result, "key": key})


def test_cached_ingress_result_restores_structured_removed_key_guard() -> None:
    record = _record(REQUEST_ID, terminal=True)
    store = {
        ingress_requests.RECORDS_KEY: {
            REQUEST_ID: json.loads(json.dumps(record))
        }
    }

    result = ingress_requests.cached_terminal_outcome(
        store,
        REQUEST_ID,
        now=103.0,
    )

    assert isinstance(result, ingress_requests.IngressResult)
    with pytest.raises(KeyError, match="removed ingress result field"):
        eval("result.get('disposition')", {"result": result})


def test_ingress_result_guard_rejects_kwargs_expansion() -> None:
    violations = _removed_ingress_result_reads(
        """
def helper(**values):
    return values

def consumer():
    return helper(**command_reply({}))
"""
    )
    assert violations


def test_ingress_result_guard_tracks_call_argument_flow() -> None:
    namespace: dict[str, object] = {}
    exec(
        "def helper(result):\n    return result.get('disposition')",
        namespace,
    )
    with pytest.raises(KeyError, match="removed ingress result field"):
        namespace["helper"](_structured_ingress_result())


def test_ingress_result_guard_tracks_helper_returns() -> None:
    def wrapper():
        return _structured_ingress_result()

    with pytest.raises(KeyError, match="removed ingress result field"):
        eval("wrapper().get('disposition')", {"wrapper": wrapper})


def test_ingress_result_guard_rejects_getattr_reads() -> None:
    with pytest.raises(KeyError, match="removed ingress result field"):
        eval(
            "getattr(result, 'disposition', None)",
            {"result": _structured_ingress_result()},
        )


def test_ingress_result_guard_scans_module_scope() -> None:
    namespace = {"result": _structured_ingress_result()}
    with pytest.raises(KeyError, match="removed ingress result field"):
        exec("removed = result.get('disposition')", namespace)


def test_unlinked_historical_terminal_accepted_migrates_to_unknown() -> None:
    legacy = _old_v3_record(
        REQUEST_ID,
        terminal_disposition="terminal_accepted",
    )
    store = {ingress_requests.RECORDS_KEY: {REQUEST_ID: legacy}}

    migrated, changed = ingress_requests.ensure_request_shell(
        store,
        REQUEST_ID,
        now=130.0,
        retry_horizon=60,
        retention=120,
    )

    assert changed is True
    assert migrated["schema_version"] == 4
    assert migrated["transport_disposition"] == "written_to_pty"
    assert migrated["request_phase"] == "accepted_unverified"
    assert migrated["terminal_outcome"] == "delivery_unknown"
    assert migrated["terminal_outcome"] != "delivered"
    assert migrated["checkpoint_already_advanced"] is True
    assert migrated["outcome"]["checkpoint"] == "hold"
    assert migrated["operator_attention_required"] is True


def test_linked_historical_terminal_accepted_migrates_to_delivered() -> None:
    legacy = _old_v3_record(
        REQUEST_ID,
        terminal_disposition="terminal_accepted",
        linked=True,
    )
    store = {ingress_requests.RECORDS_KEY: {REQUEST_ID: legacy}}

    migrated, changed = ingress_requests.ensure_request_shell(
        store,
        REQUEST_ID,
        now=130.0,
        retry_horizon=60,
        retention=120,
    )

    assert changed is True
    assert migrated["schema_version"] == 4
    assert migrated["transport_disposition"] == "submitted"
    assert migrated["request_phase"] == "terminal"
    assert migrated["terminal_outcome"] == "delivered"
    assert migrated["checkpoint_already_advanced"] is True
    assert migrated["outcome"]["checkpoint"] == "advance"
    assert migrated["operator_attention_required"] is False
    assert migrated["blocked_reason"] is None
    assert migrated["next_action"] is None

    same, changed = ingress_requests.ensure_request_shell(
        store,
        REQUEST_ID,
        now=131.0,
        retry_horizon=60,
        retention=120,
    )
    assert same is migrated
    assert changed is False


@pytest.mark.parametrize(
    ("legacy_disposition", "transport", "terminal_outcome"),
    [
        ("terminal_rejected", "terminal_rejected", "not_delivered"),
        ("terminal_uncertain", "terminal_uncertain", "delivery_unknown"),
    ],
)
def test_historical_non_success_receipts_migrate_without_rewriting_truth(
    legacy_disposition, transport, terminal_outcome
) -> None:
    store = {
        ingress_requests.RECORDS_KEY: {
            REQUEST_ID: _old_v3_record(
                REQUEST_ID,
                terminal_disposition=legacy_disposition,
            )
        }
    }

    migrated, changed = ingress_requests.ensure_request_shell(
        store,
        REQUEST_ID,
        now=130.0,
        retry_horizon=60,
        retention=120,
    )

    assert changed is True
    assert migrated["transport_disposition"] == transport
    assert migrated["terminal_outcome"] == terminal_outcome
    assert migrated["checkpoint_already_advanced"] is True
    assert migrated["outcome"]["checkpoint"] == "hold"
    assert migrated["operator_attention_required"] is True


def test_dedup_witness_contract_authorizes_only_positive_nonreceipt() -> None:
    record = _record(REQUEST_ID, with_request=True)
    ingress_requests.record_terminal_outcome(
        record,
        transport_disposition="agent_prompt_not_received",
        request_phase="terminal",
        terminal_outcome="not_delivered",
        now=103.0,
    )

    ingress_requests.record_dedup_witness(
        record,
        prompt_fingerprint="prompt-fingerprint",
        composer_fingerprint="empty-composer-fingerprint",
        composer_readable=True,
        provider_verdict="agent_prompt_not_received",
        owner_generation="owner-generation-1",
        now=104.0,
    )

    assert ingress_requests.automatic_replay_authorized(record) is True
    request = ingress_requests.dedup_witness_request(record)
    assert request["required_observation"] == "herdr_composer_prompt_fingerprint"
    assert request["prior_witness"]["comparison"] == "different"
    assert "original instruction" not in json.dumps(request)
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
        (REQUEST_ID_2, "terminal_rejected", "hold"),
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
        assert result["transport_disposition"] == (
            ingress_requests.normalize_transport_disposition(disposition)
        )
        assert result["reply"] == ""
        assert len(calls) == 1


def test_in_progress_receipt_gets_one_inline_idempotent_poll(
    tmp_path, monkeypatch
) -> None:
    _setup_command_state(tmp_path, monkeypatch)
    calls: list[str] = []

    class Client:
        def command_json(self, request_json):
            calls.append(request_json)
            request = json.loads(request_json)
            if len(calls) == 1:
                return _failed_command_response(
                    request,
                    status="pending",
                    disposition="in_progress",
                )
            return _accepted_command_response(request)

    monkeypatch.setattr(herdres, "TendwireClient", Client)

    result = herdres.command_reply(_payload())

    assert result["checkpoint"] == "hold"
    assert result["transport_disposition"] == "written_to_pty"
    assert result["terminal_outcome"] == "delivery_unknown"
    assert len(calls) == 2
    assert calls[0] == calls[1]


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


def test_v2_command_does_not_attach_submission_owner(tmp_path, monkeypatch) -> None:
    _setup_command_state(tmp_path, monkeypatch)

    def forbidden_owner(*_args, **_kwargs):
        raise AssertionError("v2 command must not persist a submission owner")

    class Client:
        def command_json(self, request_json):
            return _accepted_command_response(json.loads(request_json))

    monkeypatch.setattr(ingress_requests, "attach_target_owner", forbidden_owner)
    monkeypatch.setattr(herdres, "TendwireClient", Client)

    result = herdres.command_reply(_payload())

    assert result["transport_disposition"] == "written_to_pty"
    record = state.load_state()[ingress_requests.RECORDS_KEY][REQUEST_ID]
    assert record["target_owner"] is None


def test_v3_submission_receipt_renders_legacy_identical_working_and_links_delta(
    tmp_path, monkeypatch
) -> None:
    _setup_command_state(tmp_path, monkeypatch)
    monkeypatch.setenv(
        "HERDRES_TENDWIRE_COMMAND_RESPONSE_SCHEMA_VERSION", "3"
    )
    monkeypatch.setenv("HERDRES_TENDWIRE_MODE", "source")
    monkeypatch.setenv("HERDRES_PINNED_STATUS", "0")
    submission_id = "twsub1." + ("b" * 64)
    calls: list[dict[str, object]] = []

    class V3Client:
        def command_json(self, request_json):
            request = json.loads(request_json)
            calls.append(request)
            response = _accepted_command_response(request)
            response["schema_version"] = 3
            response["result"].update(
                {"submission_id": submission_id, "turn_id": None}
            )
            return response

    monkeypatch.setattr(herdres, "TendwireClient", V3Client)
    accepted = herdres.command_reply(_payload())
    assert accepted["transport_disposition"] == "written_to_pty"
    assert calls[0]["response_schema_version"] == 3

    submission_store = state.load_state()
    record = submission_store[ingress_requests.RECORDS_KEY][REQUEST_ID]
    assert record["submission_id"] == submission_id
    assert record["submission_state"] == "pending_observation"
    assert record["turn_id"] is None
    assert record["target_owner"]["stable_key"].startswith("wsk1_")
    source_workers = [
        {
            "id": "worker-1",
            "name": "Alpha",
            "status": "working",
            "space_id": "space-1",
            "fingerprint": "fp-original",
        }
    ]

    submission_telegram = FakeTelegram()
    herdres.sync_once(
        submission_store,
        herdres.SyncRuntime(
            FakeTendwire(
                turns={"schema_version": 1, "turns": []},
                workers=source_workers,
            ),
            submission_telegram,
            with_outbox=False,
        ),
    )
    assert len(submission_telegram.sent) == 1

    legacy_store = _store()
    legacy_telegram = FakeTelegram()
    legacy_turn = {
        "id": "turn-predicted",
        "worker_id": "worker-1",
        "space_id": "space-1",
        "complete": False,
        "user_text": "original instruction",
    }
    herdres.sync_once(
        legacy_store,
        herdres.SyncRuntime(
            FakeTendwire(
                turns={"schema_version": 1, "turns": [legacy_turn]}
            ),
            legacy_telegram,
            with_outbox=False,
        ),
    )
    assert submission_telegram.sent[0][1] == legacy_telegram.sent[0][1]

    linked_turn = _turn_row(
        "turn-observed", "twrev1.observed", None, user="original instruction"
    )
    linked_turn["submission_id"] = submission_id
    linked_turn["submission_state"] = "linked"

    class LinkedDelta(FakeTendwire):
        def turn_delta(self, **_kwargs):
            return {
                "schema_version": 1,
                "projection_schema_version": 2,
                "host_id": "host-public",
                "mode": "bootstrap",
                "changes": [
                    {
                        "op": "upsert",
                        "turn_id": linked_turn["id"],
                        "changed_at": "2030-01-01T00:00:00Z",
                        "turn": copy.deepcopy(linked_turn),
                    }
                ],
                "has_more": False,
                "next_cursor": None,
                "checkpoint": "twdelta1.linked",
                "aggregate": {"changes_returned": 1},
            }

    herdres.sync_once(
        submission_store,
        herdres.SyncRuntime(
            LinkedDelta(
                turns={"schema_version": 2, "turns": []},
                workers=source_workers,
            ),
            submission_telegram,
            with_outbox=False,
        ),
    )
    assert len(submission_telegram.sent) == 1
    record = submission_store[ingress_requests.RECORDS_KEY][REQUEST_ID]
    assert record["submission_state"] == "linked"
    assert record["turn_id"] == "turn-observed"
    assert record["linked_at"] is not None
    entry = next(iter(state.source_worker_entries(submission_store).values()))
    assert entry["last_stream_submission_id"] == submission_id
    assert entry["last_stream_turn_id"] == "turn-observed"
    binding = state.find_message_binding(
        submission_store, entry["last_stream_message_id"]
    )
    assert binding["submission_id"] == submission_id
    assert binding["turn_id"] == "turn-observed"
    linked_updated_at = record["updated_at"]
    replayed, replay_changed = ingress_requests.link_submission(
        submission_store,
        submission_id,
        "turn-observed",
        now=linked_updated_at + 1,
    )
    assert replay_changed is False
    assert replayed["updated_at"] == linked_updated_at


def test_unrelated_working_delivery_blocks_stale_submission_rebind() -> None:
    store = _store()
    _entry_key, entry, _created = state.upsert_worker_entry(
        store,
        _source_worker(
            {
                "id": "worker-1",
                "name": "Alpha",
                "status": "working",
                "space_id": "space-1",
                "fingerprint": "fp-original",
            }
        ),
        topic_id="77",
    )
    stale_submission_id = "twsub1." + ("e" * 64)
    entry.update(
        {
            "last_stream_submission_id": stale_submission_id,
            "last_stream_turn_id": stale_submission_id,
            "last_stream_hash": "old-hash",
            "last_stream_message_id": "501",
        }
    )
    state.bind_message_to_worker(
        store,
        "501",
        entry,
        topic_id="77",
        kind="working",
        turn_id=stale_submission_id,
        submission_id=stale_submission_id,
    )

    source_sync._set_stream_delivery(
        entry,
        turn_id="turn-unrelated",
        content_hash="unrelated-hash",
        message_id="777",
    )
    state.bind_message_to_worker(
        store,
        "777",
        entry,
        topic_id="77",
        kind="working",
        turn_id="turn-unrelated",
    )
    stale_record = {
        "submission_id": stale_submission_id,
        "turn_id": "turn-stale-linked",
    }

    assert "last_stream_submission_id" not in entry
    assert source_sync._associate_submission_working(
        store, stale_record, entry
    ) is False
    assert entry["last_stream_turn_id"] == "turn-unrelated"
    assert state.find_message_binding(store, "777")["turn_id"] == "turn-unrelated"


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
    assert second["checkpoint"] == "hold"
    assert second["transport_disposition"] == "written_to_pty"
    assert child_starts[0] == child_starts[1]
    assert backend_sends == 1
    assert "private stderr" not in json.dumps(first, sort_keys=True)


def test_v3_ack_loss_replays_submission_once_without_duplicate_working(
    tmp_path, monkeypatch
) -> None:
    _setup_command_state(tmp_path, monkeypatch)
    monkeypatch.setenv(
        "HERDRES_TENDWIRE_COMMAND_RESPONSE_SCHEMA_VERSION", "3"
    )
    monkeypatch.setenv("HERDRES_TENDWIRE_MODE", "source")
    monkeypatch.setenv("HERDRES_PINNED_STATUS", "0")
    submission_id = "twsub1." + ("c" * 64)
    backend_receipts: set[str] = set()
    backend_sends = 0
    child_starts = 0

    def run(argv, *, input, **_kwargs):
        nonlocal backend_sends, child_starts
        child_starts += 1
        request = json.loads(bytes(input))
        request_id = request["request_id"]
        if request_id not in backend_receipts:
            backend_receipts.add(request_id)
            backend_sends += 1
            return subprocess.CompletedProcess(argv, 0, b"lost-ack", b"")
        response = _accepted_command_response(request)
        response["schema_version"] = 3
        response["result"].update(
            {"submission_id": submission_id, "turn_id": None}
        )
        return subprocess.CompletedProcess(
            argv,
            0,
            json.dumps(response, separators=(",", ":")).encode(),
            b"",
        )

    monkeypatch.setattr(tendwire_client.subprocess, "run", run)
    first = herdres.command_reply(_payload())
    second = herdres.command_reply(_payload())
    assert first["checkpoint"] == "retry"
    assert second["transport_disposition"] == "written_to_pty"
    assert backend_sends == 1
    assert child_starts == 2

    store = state.load_state()
    telegram = FakeTelegram()
    workers = [
        {
            "id": "worker-1",
            "name": "Alpha",
            "status": "working",
            "space_id": "space-1",
            "fingerprint": "fp-original",
        }
    ]
    runtime = lambda: herdres.SyncRuntime(
        FakeTendwire(
            turns={"schema_version": 1, "turns": []}, workers=workers
        ),
        telegram,
        with_outbox=False,
    )
    herdres.sync_once(store, runtime())
    herdres.sync_once(store, runtime())
    assert len(telegram.sent) == 1
    entry = next(iter(state.source_worker_entries(store).values()))
    assert entry["last_stream_submission_id"] == submission_id


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
    assert result["transport_disposition"] == "written_to_pty"
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
