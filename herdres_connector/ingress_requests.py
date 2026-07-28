"""Durable, bounded lifecycle records for mutating ingress requests.

The helpers in this module are deliberately transport-agnostic.  Callers must
hold the connector state lock and durably save the returned mutations before
starting a child process or advancing an ingress checkpoint.
"""

from __future__ import annotations

import copy
import json
import math
from typing import Any

from .ingress_identity import validate_request_id
from .safe import sanitize_text

RECORDS_KEY = "tendwire_ingress_command_requests"
RECORD_SCHEMA_VERSION = 4
PREVIOUS_RECORD_SCHEMA_VERSION = 3
LEGACY_RECORD_SCHEMA_VERSION = 2
CHILD_SCHEMA_VERSION = 2
RECORD_STATES = frozenset({"resolving", "retryable", "terminal", "quarantined"})
TRANSPORT_DISPOSITIONS = frozenset(
    {
        "no_receipt",
        "in_progress",
        "terminal_accepted",
        "terminal_rejected",
        "terminal_uncertain",
        "written_to_pty",
        "submitted",
        "agent_prompt_not_received",
        "agent_prompt_unsubmitted",
        "agent_prompt_stalled",
        "agent_input_pending",
    }
)
PERSISTED_TRANSPORT_DISPOSITIONS = TRANSPORT_DISPOSITIONS - {"terminal_accepted"}
RETRYABLE_DISPOSITIONS = frozenset({"no_receipt", "in_progress"})
_LEGACY_TERMINAL_DISPOSITIONS = frozenset(
    {"terminal_accepted", "terminal_rejected"}
)
REQUEST_PHASES = frozenset(
    {
        "resolving",
        "ready",
        "retryable",
        "retry_authorized",
        "accepted_unverified",
        "queued",
        "terminal",
    }
)
TERMINAL_OUTCOMES = frozenset(
    {"delivered", "not_delivered", "delivery_unknown"}
)
HOLD_OUTCOMES = frozenset({"not_delivered", "delivery_unknown"})
TERMINAL_OUTCOME_TRANSPORTS = {
    "delivered": frozenset({"submitted"}),
    "not_delivered": frozenset(
        {
            None,
            "no_receipt",
            "terminal_rejected",
            "agent_prompt_not_received",
            "agent_prompt_unsubmitted",
            "agent_input_pending",
        }
    ),
    "delivery_unknown": frozenset(
        {
            None,
            "written_to_pty",
            "terminal_uncertain",
            "agent_prompt_stalled",
        }
    ),
}
TERMINAL_OUTCOME_PHASES = {
    "delivered": frozenset({"terminal"}),
    "not_delivered": frozenset({"terminal"}),
    "delivery_unknown": frozenset(
        {"accepted_unverified", "queued", "terminal"}
    ),
}
# Public response validation still consumes Tendwire's legacy spelling. Records
# never persist it: ``terminal_accepted`` is normalized to Herdr's
# ``written_to_pty`` transport verdict at the reduction boundary.
DISPOSITIONS = TRANSPORT_DISPOSITIONS
QUARANTINE_REPLY = "Could not send safely. Refresh status and choose the target again."
_CORRUPT_RECORDS_ERROR = "ingress request record store is corrupt"

_RECORD_FIELDS = frozenset(
    {
        "schema_version",
        "request_id",
        "created_at",
        "updated_at",
        "deadline_at",
        "retain_until",
        "state",
        "request_json",
        "transport_disposition",
        "request_phase",
        "terminal_outcome",
        "checkpoint_already_advanced",
        "operator_attention_required",
        "blocked_reason",
        "next_action",
        "dedup_witness",
        "stale_target_refreshed",
        "terminal_at",
        "quarantined_at",
        "quarantine_reason",
        "outcome",
        "submission_id",
        "submission_state",
        "turn_id",
        "target_owner",
        "submitted_at",
        "linked_at",
    }
)
_V3_RECORD_FIELDS = (
    _RECORD_FIELDS
    - {
        "transport_disposition",
        "request_phase",
        "terminal_outcome",
        "checkpoint_already_advanced",
        "operator_attention_required",
        "blocked_reason",
        "next_action",
        "dedup_witness",
    }
) | {"last_disposition"}
_V2_RECORD_FIELDS = _V3_RECORD_FIELDS - {
    "submission_id",
    "submission_state",
    "turn_id",
    "target_owner",
    "submitted_at",
    "linked_at",
}
_CHILD_FIELDS = frozenset(
    {
        "schema_version",
        "handled",
        "request_id",
        "checkpoint",
        "transport_disposition",
        "request_phase",
        "terminal_outcome",
        "reply",
    }
)
_LEGACY_RECORD_FIELDS = frozenset(
    {"request", "created_at", "updated_at", "last_status", "terminal_at"}
)
_LEGACY_REQUIRED_FIELDS = frozenset({"request", "created_at", "updated_at"})
_COMMAND_REQUEST_FIELDS = frozenset(
    {"schema_version", "action", "request_id", "dry_run", "target", "instruction"}
)
_COMMAND_REQUEST_V3_FIELDS = _COMMAND_REQUEST_FIELDS | {"response_schema_version"}
_COMMAND_TARGET_SHAPES = frozenset(
    {
        frozenset({"worker_id"}),
        frozenset({"worker_id", "worker_fingerprint"}),
        frozenset({"space_id"}),
        frozenset({"name"}),
        frozenset({"name", "space_id"}),
    }
)
_SUBMISSION_STATES = frozenset(
    {"pending_observation", "observed", "complete", "linked"}
)
_TARGET_OWNER_FIELDS = frozenset({"stable_key", "stable_key_version"})
_DEDUP_WITNESS_FIELDS = frozenset(
    {
        "schema_version",
        "prompt_fingerprint",
        "composer_fingerprint",
        "comparison",
        "provider_verdict",
        "owner_generation",
        "observed_at",
        "automatic_replay_authorized",
    }
)
_DEDUP_COMPARISONS = frozenset({"match", "different", "unreadable"})
_DEDUP_PROVIDER_VERDICTS = frozenset(
    {
        "agent_prompt_not_received",
        "agent_prompt_unsubmitted",
        "agent_prompt_stalled",
        "written_to_pty",
    }
)






def _timestamp(value: Any) -> float | None:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    result = float(value)
    if not math.isfinite(result) or result < 0:
        return None
    return result


def _duration(value: Any) -> float:
    result = _timestamp(value)
    if result is None or result <= 0:
        raise ValueError("ingress lifecycle duration must be finite and positive")
    return result


def _fixed_reply(value: Any) -> str:
    return sanitize_text(value, 160)


def normalize_transport_disposition(value: Any) -> str | None:
    """Return the persisted Herdr-aligned spelling for one transport fact."""

    if value is None:
        return None
    if value == "terminal_accepted":
        return "written_to_pty"
    return str(value) if value in PERSISTED_TRANSPORT_DISPOSITIONS else None


def child_result(
    request_id: str,
    *,
    checkpoint: str,
    transport_disposition: str | None,
    request_phase: str,
    terminal_outcome: str | None,
    reply: str = "",
    handled: bool = True,
) -> dict[str, Any]:
    """Build the exact child envelope consumed by the Telegram gateway."""

    request_id = validate_request_id(request_id)
    if type(handled) is not bool:
        raise ValueError("handled must be a boolean")
    if checkpoint not in {"retry", "hold", "advance"}:
        raise ValueError("invalid ingress checkpoint decision")
    transport = normalize_transport_disposition(transport_disposition)
    if transport_disposition is not None and transport is None:
        raise ValueError("invalid transport disposition")
    if request_phase not in REQUEST_PHASES:
        raise ValueError("invalid ingress request phase")
    if terminal_outcome is not None and terminal_outcome not in TERMINAL_OUTCOMES:
        raise ValueError("invalid terminal outcome")
    if checkpoint == "retry":
        if (
            not handled
            or transport not in {None, "no_receipt", "in_progress"}
            or request_phase not in {"ready", "retryable", "retry_authorized"}
            or terminal_outcome is not None
        ):
            raise ValueError("invalid retry child outcome")
        reply = ""
    elif checkpoint == "advance":
        if (
            terminal_outcome != "delivered"
            or request_phase not in TERMINAL_OUTCOME_PHASES["delivered"]
            or transport not in TERMINAL_OUTCOME_TRANSPORTS["delivered"]
        ):
            raise ValueError("only verified delivery may advance checkpoint")
    else:
        if terminal_outcome not in HOLD_OUTCOMES:
            raise ValueError("held checkpoint requires a non-success outcome")
        if transport not in TERMINAL_OUTCOME_TRANSPORTS[terminal_outcome]:
            raise ValueError("transport disposition cannot produce terminal outcome")
        if request_phase not in TERMINAL_OUTCOME_PHASES[terminal_outcome]:
            raise ValueError("invalid held request phase")
        if not handled and (transport is not None or reply):
            raise ValueError("unhandled outcome cannot carry command details")
    return {
        "schema_version": CHILD_SCHEMA_VERSION,
        "handled": handled,
        "request_id": request_id,
        "checkpoint": checkpoint,
        "transport_disposition": transport,
        "request_phase": request_phase,
        "terminal_outcome": terminal_outcome,
        "reply": _fixed_reply(reply),
    }


def _valid_child(value: Any, request_id: str) -> bool:
    if not isinstance(value, dict) or frozenset(value) != _CHILD_FIELDS:
        return False
    if (
        value.get("schema_version") != CHILD_SCHEMA_VERSION
        or type(value.get("handled")) is not bool
        or value.get("request_id") != request_id
        or value.get("checkpoint") not in {"hold", "advance"}
        or value.get("transport_disposition")
        not in PERSISTED_TRANSPORT_DISPOSITIONS | {None}
        or value.get("request_phase") not in REQUEST_PHASES
        or value.get("terminal_outcome") not in TERMINAL_OUTCOMES
        or not isinstance(value.get("reply"), str)
        or value["reply"] != _fixed_reply(value["reply"])
    ):
        return False
    if value["checkpoint"] == "advance":
        if (
            value["terminal_outcome"] != "delivered"
            or value["request_phase"]
            not in TERMINAL_OUTCOME_PHASES["delivered"]
            or value["transport_disposition"]
            not in TERMINAL_OUTCOME_TRANSPORTS["delivered"]
        ):
            return False
    elif (
        value["terminal_outcome"] not in HOLD_OUTCOMES
        or value["transport_disposition"]
        not in TERMINAL_OUTCOME_TRANSPORTS[value["terminal_outcome"]]
        or value["request_phase"]
        not in TERMINAL_OUTCOME_PHASES[value["terminal_outcome"]]
    ):
        return False
    if value["handled"] is False and (
        value["transport_disposition"] is not None or value["reply"]
    ):
        return False
    return True


def _valid_command_request(request: Any) -> bool:
    if (
        not isinstance(request, dict)
        or frozenset(request)
        not in {_COMMAND_REQUEST_FIELDS, _COMMAND_REQUEST_V3_FIELDS}
        or request.get("schema_version") != 1
        or request.get("action") != "send_instruction"
        or request.get("dry_run") is not False
        or (
            "response_schema_version" in request
            and request.get("response_schema_version") != 3
        )
    ):
        return False
    try:
        validate_request_id(request.get("request_id"))
    except ValueError:
        return False
    target = request.get("target")
    instruction = request.get("instruction")
    return (
        isinstance(target, dict)
        and frozenset(target) in _COMMAND_TARGET_SHAPES
        and all(
            isinstance(value, str) and bool(value.strip())
            for value in target.values()
        )
        and isinstance(instruction, dict)
        and frozenset(instruction) == {"text"}
        and isinstance(instruction.get("text"), str)
        and bool(instruction["text"])
    )


def canonical_request_json(request: dict[str, Any]) -> str:
    """Return deterministic public request bytes for durable replay."""

    if not _valid_command_request(request):
        raise ValueError("command request is not an exact public command object")
    try:
        return json.dumps(
            request,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )
    except (TypeError, ValueError) as exc:
        raise ValueError("command request is not canonical JSON") from exc


def _request_id_from_json(request_json: Any) -> str | None:
    if not isinstance(request_json, str):
        return None
    try:
        request = json.loads(request_json)
    except (json.JSONDecodeError, TypeError, ValueError):
        return None
    if not isinstance(request, dict):
        return None
    try:
        request_id = validate_request_id(request.get("request_id"))
        if canonical_request_json(request) != request_json:
            return None
        return request_id
    except ValueError:
        return None


def _valid_target_owner(value: Any) -> bool:
    if value is None:
        return True
    return (
        isinstance(value, dict)
        and frozenset(value) == _TARGET_OWNER_FIELDS
        and isinstance(value.get("stable_key"), str)
        and value["stable_key"].startswith("wsk1_")
        and len(value["stable_key"]) == 69
        and all(char in "0123456789abcdef" for char in value["stable_key"][5:])
        and type(value.get("stable_key_version")) is int
        and value["stable_key_version"] == 1
    )


def _valid_submission_fields(record: dict[str, Any]) -> bool:
    submission_id = record.get("submission_id")
    submission_state = record.get("submission_state")
    turn_id = record.get("turn_id")
    submitted_at = record.get("submitted_at")
    linked_at = record.get("linked_at")
    if submission_id is None:
        return (
            submission_state is None
            and turn_id is None
            and submitted_at is None
            and linked_at is None
            and _valid_target_owner(record.get("target_owner"))
        )
    submitted_timestamp = _timestamp(submitted_at)
    linked_timestamp = _timestamp(linked_at)
    created_timestamp = _timestamp(record.get("created_at"))
    updated_timestamp = _timestamp(record.get("updated_at"))
    if (
        not isinstance(submission_id, str)
        or not submission_id.strip()
        or len(submission_id) > 200
        or submission_state not in _SUBMISSION_STATES
        or (turn_id is not None and (not isinstance(turn_id, str) or not turn_id.strip()))
        or not _valid_target_owner(record.get("target_owner"))
        or record.get("target_owner") is None
        or submitted_timestamp is None
        or created_timestamp is None
        or updated_timestamp is None
        or not created_timestamp <= submitted_timestamp <= updated_timestamp
    ):
        return False
    if submission_state == "linked":
        if turn_id is None:
            return False
    if turn_id is None:
        return linked_at is None
    return (
        linked_timestamp is not None
        and submitted_timestamp <= linked_timestamp <= updated_timestamp
    )


def _valid_legacy_child(value: Any, request_id: str) -> bool:
    fields = frozenset(
        {
            "schema_version",
            "handled",
            "request_id",
            "checkpoint",
            "disposition",
            "reply",
        }
    )
    return (
        isinstance(value, dict)
        and frozenset(value) == fields
        and value.get("schema_version") == 1
        and type(value.get("handled")) is bool
        and value.get("request_id") == request_id
        and value.get("checkpoint") == "advance"
        and value.get("disposition")
        in {"terminal_accepted", "terminal_rejected", "terminal_uncertain", None}
        and isinstance(value.get("reply"), str)
        and value["reply"] == _fixed_reply(value["reply"])
        and not (
            value["handled"] is False
            and (value["disposition"] is not None or value["reply"])
        )
    )


def _valid_dedup_witness(value: Any) -> bool:
    if value is None:
        return True
    if not isinstance(value, dict) or frozenset(value) != _DEDUP_WITNESS_FIELDS:
        return False
    prompt_fingerprint = value.get("prompt_fingerprint")
    composer_fingerprint = value.get("composer_fingerprint")
    comparison = value.get("comparison")
    provider_verdict = value.get("provider_verdict")
    owner_generation = value.get("owner_generation")
    if (
        value.get("schema_version") != 1
        or not isinstance(prompt_fingerprint, str)
        or not prompt_fingerprint
        or len(prompt_fingerprint) > 200
        or (
            composer_fingerprint is not None
            and (
                not isinstance(composer_fingerprint, str)
                or not composer_fingerprint
                or len(composer_fingerprint) > 200
            )
        )
        or comparison not in _DEDUP_COMPARISONS
        or provider_verdict not in _DEDUP_PROVIDER_VERDICTS
        or (
            owner_generation is not None
            and (
                not isinstance(owner_generation, str)
                or not owner_generation
                or len(owner_generation) > 200
            )
        )
        or _timestamp(value.get("observed_at")) is None
        or type(value.get("automatic_replay_authorized")) is not bool
    ):
        return False
    if comparison == "unreadable":
        return (
            composer_fingerprint is None
            and value["automatic_replay_authorized"] is False
        )
    if composer_fingerprint is None:
        return False
    if comparison == "match" and composer_fingerprint != prompt_fingerprint:
        return False
    if comparison == "different" and composer_fingerprint == prompt_fingerprint:
        return False
    expected_authorization = (
        provider_verdict == "agent_prompt_not_received"
        and comparison == "different"
    )
    return not value["automatic_replay_authorized"] or expected_authorization


def _valid_legacy_record_version(
    record: Any,
    request_id: str,
    *,
    schema_version: int,
    fields: frozenset[str],
) -> bool:
    if not isinstance(record, dict) or frozenset(record) != fields:
        return False
    created_at = _timestamp(record.get("created_at"))
    updated_at = _timestamp(record.get("updated_at"))
    deadline_at = _timestamp(record.get("deadline_at"))
    retain_until = _timestamp(record.get("retain_until"))
    state = record.get("state")
    request_json = record.get("request_json")
    last_disposition = record.get("last_disposition")
    terminal_at = record.get("terminal_at")
    quarantined_at = record.get("quarantined_at")
    outcome = record.get("outcome")
    if (
        record.get("schema_version") != schema_version
        or record.get("request_id") != request_id
        or created_at is None
        or updated_at is None
        or deadline_at is None
        or retain_until is None
        or not created_at <= updated_at
        or not created_at < deadline_at < retain_until
        or state not in RECORD_STATES
        or last_disposition not in DISPOSITIONS | {None}
        or type(record.get("stale_target_refreshed")) is not bool
    ):
        return False
    if request_json is not None and _request_id_from_json(request_json) != request_id:
        return False
    if schema_version == PREVIOUS_RECORD_SCHEMA_VERSION and not _valid_submission_fields(record):
        return False
    if record["stale_target_refreshed"] and not isinstance(request_json, str):
        return False
    if state == "resolving":
        return (
            request_json is None
            and updated_at == created_at
            and last_disposition is None
            and record["stale_target_refreshed"] is False
            and terminal_at is None
            and quarantined_at is None
            and record.get("quarantine_reason") is None
            and outcome is None
        )
    if state == "retryable":
        return (
            isinstance(request_json, str)
            and last_disposition in RETRYABLE_DISPOSITIONS | {None}
            and terminal_at is None
            and quarantined_at is None
            and record.get("quarantine_reason") is None
            and outcome is None
        )
    if state == "terminal":
        terminal_timestamp = _timestamp(terminal_at)
        return (
            isinstance(request_json, str)
            and last_disposition in _LEGACY_TERMINAL_DISPOSITIONS
            and terminal_timestamp is not None
            and terminal_timestamp <= updated_at
            and (
                schema_version == PREVIOUS_RECORD_SCHEMA_VERSION
                or terminal_timestamp == updated_at
            )
            and quarantined_at is None
            and record.get("quarantine_reason") is None
            and _valid_legacy_child(outcome, request_id)
            and outcome.get("disposition") == last_disposition
        )
    quarantine_timestamp = _timestamp(quarantined_at)
    return (
        last_disposition in {"terminal_uncertain", None}
        and (
            last_disposition is None
            or isinstance(request_json, str)
        )
        and terminal_at is None
        and quarantine_timestamp == updated_at
        and isinstance(record.get("quarantine_reason"), str)
        and bool(record["quarantine_reason"])
        and _valid_legacy_child(outcome, request_id)
        and outcome.get("disposition") == last_disposition
    )


def _valid_record(record: Any, request_id: str) -> bool:
    if not isinstance(record, dict) or frozenset(record) != _RECORD_FIELDS:
        return False
    created_at = _timestamp(record.get("created_at"))
    updated_at = _timestamp(record.get("updated_at"))
    deadline_at = _timestamp(record.get("deadline_at"))
    retain_until = _timestamp(record.get("retain_until"))
    request_json = record.get("request_json")
    state = record.get("state")
    transport = record.get("transport_disposition")
    phase = record.get("request_phase")
    terminal_outcome = record.get("terminal_outcome")
    terminal_at = record.get("terminal_at")
    quarantined_at = record.get("quarantined_at")
    outcome = record.get("outcome")
    blocked_reason = record.get("blocked_reason")
    next_action = record.get("next_action")
    if (
        record.get("schema_version") != RECORD_SCHEMA_VERSION
        or record.get("request_id") != request_id
        or created_at is None
        or updated_at is None
        or deadline_at is None
        or retain_until is None
        or not created_at <= updated_at
        or not created_at < deadline_at < retain_until
        or state not in RECORD_STATES
        or transport not in PERSISTED_TRANSPORT_DISPOSITIONS | {None}
        or phase not in REQUEST_PHASES
        or terminal_outcome not in TERMINAL_OUTCOMES | {None}
        or type(record.get("checkpoint_already_advanced")) is not bool
        or type(record.get("operator_attention_required")) is not bool
        or (
            blocked_reason is not None
            and (
                not isinstance(blocked_reason, str)
                or not blocked_reason
                or blocked_reason != _fixed_reply(blocked_reason)
            )
        )
        or (
            next_action is not None
            and (
                not isinstance(next_action, str)
                or not next_action
                or next_action != _fixed_reply(next_action)
            )
        )
        or not _valid_dedup_witness(record.get("dedup_witness"))
        or not _valid_submission_fields(record)
        or type(record.get("stale_target_refreshed")) is not bool
    ):
        return False
    if request_json is not None and _request_id_from_json(request_json) != request_id:
        return False
    if record["stale_target_refreshed"] and not isinstance(request_json, str):
        return False
    if terminal_outcome == "delivery_unknown":
        witness = record.get("dedup_witness")
        if isinstance(witness, dict) and witness.get("automatic_replay_authorized") is True:
            return False
    if terminal_outcome in HOLD_OUTCOMES:
        if (
            record.get("operator_attention_required") is not True
            or blocked_reason is None
            or next_action is None
        ):
            return False
    elif record.get("operator_attention_required") is True:
        return False

    if state == "resolving":
        return (
            request_json is None
            and updated_at == created_at
            and transport is None
            and phase == "resolving"
            and terminal_outcome is None
            and record["checkpoint_already_advanced"] is False
            and record["operator_attention_required"] is False
            and blocked_reason is None
            and next_action is None
            and record.get("dedup_witness") is None
            and record["stale_target_refreshed"] is False
            and terminal_at is None
            and quarantined_at is None
            and record.get("quarantine_reason") is None
            and outcome is None
        )
    if state == "retryable":
        return (
            isinstance(request_json, str)
            and transport in RETRYABLE_DISPOSITIONS | {None}
            and phase in {"ready", "retryable", "retry_authorized"}
            and terminal_outcome is None
            and record["checkpoint_already_advanced"] is False
            and record["operator_attention_required"] is False
            and blocked_reason is None
            and next_action is None
            and terminal_at is None
            and quarantined_at is None
            and record.get("quarantine_reason") is None
            and outcome is None
        )

    terminal_timestamp = _timestamp(terminal_at)
    if (
        terminal_timestamp is None
        or terminal_timestamp > updated_at
        or not _valid_child(outcome, request_id)
        or outcome.get("transport_disposition") != transport
        or outcome.get("request_phase") != phase
        or outcome.get("terminal_outcome") != terminal_outcome
    ):
        return False
    if state == "terminal":
        return (
            isinstance(request_json, str)
            and quarantined_at is None
            and record.get("quarantine_reason") is None
        )
    quarantine_timestamp = _timestamp(quarantined_at)
    return (
        (isinstance(request_json, str) or (request_json is None and transport is None))
        and
        quarantine_timestamp == updated_at
        and isinstance(record.get("quarantine_reason"), str)
        and bool(record["quarantine_reason"])
    )


def _valid_v3_record(record: Any, request_id: str) -> bool:
    return _valid_legacy_record_version(
        record,
        request_id,
        schema_version=PREVIOUS_RECORD_SCHEMA_VERSION,
        fields=_V3_RECORD_FIELDS,
    )


def _valid_v2_record(record: Any, request_id: str) -> bool:
    return _valid_legacy_record_version(
        record,
        request_id,
        schema_version=LEGACY_RECORD_SCHEMA_VERSION,
        fields=_V2_RECORD_FIELDS,
    )


def _new_record(
    request_id: str,
    *,
    now: float,
    retry_horizon: float,
    retention: float,
) -> dict[str, Any]:
    created_at = _timestamp(now)
    horizon = _duration(retry_horizon)
    retain_for = _duration(retention)
    deadline_at = created_at + horizon if created_at is not None else None
    retain_until = created_at + retain_for if created_at is not None else None
    if (
        created_at is None
        or retain_for <= horizon
        or _timestamp(deadline_at) is None
        or _timestamp(retain_until) is None
    ):
        raise ValueError("invalid bounded ingress lifecycle")
    return {
        "schema_version": RECORD_SCHEMA_VERSION,
        "request_id": validate_request_id(request_id),
        "created_at": created_at,
        "updated_at": created_at,
        "deadline_at": deadline_at,
        "retain_until": retain_until,
        "state": "resolving",
        "request_json": None,
        "transport_disposition": None,
        "request_phase": "resolving",
        "terminal_outcome": None,
        "checkpoint_already_advanced": False,
        "operator_attention_required": False,
        "blocked_reason": None,
        "next_action": None,
        "dedup_witness": None,
        "stale_target_refreshed": False,
        "terminal_at": None,
        "quarantined_at": None,
        "quarantine_reason": None,
        "outcome": None,
        "submission_id": None,
        "submission_state": None,
        "turn_id": None,
        "target_owner": None,
        "submitted_at": None,
        "linked_at": None,
    }


def _migrate_v2_to_v3(value: Any, request_id: str) -> dict[str, Any] | None:
    if not _valid_v2_record(value, request_id):
        return None
    migrated = copy.deepcopy(value)
    migrated.update(
        {
            "schema_version": PREVIOUS_RECORD_SCHEMA_VERSION,
            "submission_id": None,
            "submission_state": None,
            "turn_id": None,
            "target_owner": None,
            "submitted_at": None,
            "linked_at": None,
        }
    )
    return migrated if _valid_v3_record(migrated, request_id) else None


def _historical_outcome(
    old_disposition: Any,
) -> tuple[str | None, str, str, str]:
    """Map v3 history without upgrading transport acceptance into delivery."""

    if old_disposition == "terminal_accepted":
        return (
            "written_to_pty",
            "accepted_unverified",
            "delivery_unknown",
            "historical_terminal_accepted_unverified",
        )
    if old_disposition == "terminal_rejected":
        return (
            "terminal_rejected",
            "terminal",
            "not_delivered",
            "historical_provider_rejection",
        )
    return (
        normalize_transport_disposition(old_disposition),
        "terminal",
        "delivery_unknown",
        "historical_delivery_truth_unrecoverable",
    )


def _migrate_v3_record(value: Any, request_id: str) -> dict[str, Any] | None:
    if not _valid_v3_record(value, request_id):
        return None
    migrated = copy.deepcopy(value)
    old_disposition = migrated.pop("last_disposition")
    migrated["schema_version"] = RECORD_SCHEMA_VERSION
    migrated.update(
        {
            "transport_disposition": normalize_transport_disposition(
                old_disposition
            ),
            "request_phase": "resolving",
            "terminal_outcome": None,
            "checkpoint_already_advanced": False,
            "operator_attention_required": False,
            "blocked_reason": None,
            "next_action": None,
            "dedup_witness": None,
        }
    )
    if migrated["state"] == "resolving":
        pass
    elif migrated["state"] == "retryable":
        migrated["request_phase"] = "retryable"
    else:
        transport, phase, terminal_outcome, reason = _historical_outcome(
            old_disposition
        )
        migrated.update(
            {
                "state": "terminal",
                "transport_disposition": transport,
                "request_phase": phase,
                "terminal_outcome": terminal_outcome,
                "checkpoint_already_advanced": True,
                "operator_attention_required": True,
                "blocked_reason": reason,
                "next_action": "inspect historical ingress evidence",
                "terminal_at": (
                    migrated.get("terminal_at")
                    or migrated.get("quarantined_at")
                    or migrated["updated_at"]
                ),
                "quarantined_at": None,
                "quarantine_reason": None,
                "outcome": child_result(
                    request_id,
                    checkpoint="hold",
                    transport_disposition=transport,
                    request_phase=phase,
                    terminal_outcome=terminal_outcome,
                ),
            }
        )
    return migrated if _valid_record(migrated, request_id) else None


def _migrate_v2_record(value: Any, request_id: str) -> dict[str, Any] | None:
    v3 = _migrate_v2_to_v3(value, request_id)
    return _migrate_v3_record(v3, request_id) if v3 is not None else None


def _legacy_request_json(
    value: Any,
    request_id: str,
    *,
    now: float,
) -> str | None:
    if (
        not isinstance(value, dict)
        or not _LEGACY_REQUIRED_FIELDS <= frozenset(value)
        or not frozenset(value) <= _LEGACY_RECORD_FIELDS
    ):
        return None
    request = value.get("request")
    created_at = _timestamp(value.get("created_at"))
    updated_at = _timestamp(value.get("updated_at"))
    terminal_at = (
        _timestamp(value.get("terminal_at"))
        if "terminal_at" in value
        else None
    )
    if (
        not isinstance(request, dict)
        or created_at is None
        or updated_at is None
        or not created_at <= updated_at
        or created_at > now
        or (
            "last_status" in value
            and (
                not isinstance(value.get("last_status"), str)
                or not value["last_status"]
            )
        )
        or (
            "terminal_at" in value
            and (terminal_at is None or terminal_at < created_at)
        )
    ):
        return None
    try:
        request_json = canonical_request_json(request)
    except ValueError:
        return None
    if _request_id_from_json(request_json) != request_id:
        return None
    return request_json


def _legacy_record(
    value: Any,
    request_id: str,
    *,
    now: float,
    retry_horizon: float,
    retention: float,
) -> dict[str, Any] | None:
    request_json = _legacy_request_json(value, request_id, now=now)
    if request_json is None:
        return None
    created_at = _timestamp(value.get("created_at"))
    updated_at = _timestamp(value.get("updated_at"))
    if created_at is None or updated_at is None:
        return None
    record = _new_record(
        request_id,
        now=created_at,
        retry_horizon=retry_horizon,
        retention=retention,
    )
    record["updated_at"] = updated_at
    record["state"] = "retryable"
    record["request_json"] = request_json
    record["request_phase"] = "retryable"
    # Legacy status text is deliberately not authoritative finality evidence.
    return record


def _validated_records_mapping(
    store: dict[str, Any],
    *,
    now: float,
) -> dict[str, Any] | None:
    """Validate the complete retained evidence set without mutating it."""

    if RECORDS_KEY not in store:
        return None
    records = store[RECORDS_KEY]
    if not isinstance(records, dict):
        raise RuntimeError(_CORRUPT_RECORDS_ERROR)
    for request_id, record in records.items():
        try:
            canonical_id = validate_request_id(request_id)
        except ValueError:
            raise RuntimeError(_CORRUPT_RECORDS_ERROR) from None
        if (
            canonical_id != request_id
            or (
                not _valid_record(record, canonical_id)
                and not _valid_v3_record(record, canonical_id)
                and not _valid_v2_record(record, canonical_id)
                and _legacy_request_json(record, canonical_id, now=now) is None
            )
        ):
            raise RuntimeError(_CORRUPT_RECORDS_ERROR)
    return records


def cached_terminal_outcome(
    store: dict[str, Any], request_id: str, *, now: float
) -> dict[str, Any] | None:
    """Read a validated terminal/quarantine outcome without mutating state."""

    request_id = validate_request_id(request_id)
    timestamp = _timestamp(now)
    if timestamp is None:
        raise ValueError("invalid ingress timestamp")
    records = _validated_records_mapping(store, now=timestamp)
    if records is None or request_id not in records:
        return None
    record = records[request_id]
    if not _valid_record(record, request_id):
        # Pre-v4 records require conservative migration before their old
        # checkpoint semantics can be interpreted.
        return None
    if record["state"] not in {"terminal", "quarantined"}:
        return None
    return copy.deepcopy(record["outcome"])


def quarantine_request(
    record: dict[str, Any],
    reason: str,
    *,
    now: float,
    disposition: str | None = None,
    reply: str = QUARANTINE_REPLY,
    handled: bool = True,
) -> dict[str, Any]:
    """Make a local failure representable without claiming delivery."""

    if disposition not in {None, "terminal_uncertain"}:
        raise ValueError("invalid quarantine disposition")
    request_id = validate_request_id(record.get("request_id"))
    timestamp = _timestamp(now)
    if timestamp is None:
        raise ValueError("invalid quarantine timestamp")
    transport = normalize_transport_disposition(disposition)
    terminal_outcome = (
        "delivery_unknown"
        if transport in {"terminal_uncertain", "written_to_pty"}
        else "not_delivered"
    )
    blocked_reason = _fixed_reply(reason) or (
        "delivery truth unavailable"
        if terminal_outcome == "delivery_unknown"
        else "request was not delivered"
    )
    next_action = (
        "inspect ingress evidence and resolve manually"
        if terminal_outcome == "delivery_unknown"
        else "review the failure before requesting a retry"
    )
    outcome = child_result(
        request_id,
        checkpoint="hold",
        transport_disposition=transport,
        request_phase="terminal",
        terminal_outcome=terminal_outcome,
        reply=reply,
        handled=handled,
    )
    record["state"] = "quarantined"
    record["updated_at"] = timestamp
    record["transport_disposition"] = transport
    record["request_phase"] = "terminal"
    record["terminal_outcome"] = terminal_outcome
    record["checkpoint_already_advanced"] = False
    record["operator_attention_required"] = True
    record["blocked_reason"] = blocked_reason
    record["next_action"] = next_action
    record["terminal_at"] = timestamp
    record["quarantined_at"] = timestamp
    record["quarantine_reason"] = blocked_reason
    record["outcome"] = outcome
    return copy.deepcopy(outcome)


def ensure_request_shell(
    store: dict[str, Any],
    request_id: str,
    *,
    now: float,
    retry_horizon: float,
    retention: float,
) -> tuple[dict[str, Any], bool]:
    """Return a valid first-seen record after validating all retained evidence."""

    request_id = validate_request_id(request_id)
    timestamp = _timestamp(now)
    if timestamp is None:
        raise ValueError("invalid ingress timestamp")
    raw_records = _validated_records_mapping(store, now=timestamp)
    if raw_records is None:
        record = _new_record(
            request_id,
            now=timestamp,
            retry_horizon=retry_horizon,
            retention=retention,
        )
        store[RECORDS_KEY] = {request_id: record}
        return record, True
    if request_id not in raw_records:
        record = _new_record(
            request_id,
            now=timestamp,
            retry_horizon=retry_horizon,
            retention=retention,
        )
        raw_records[request_id] = record
        return record, True
    current = raw_records[request_id]
    if _valid_record(current, request_id):
        return current, False
    migrated_v3 = _migrate_v3_record(current, request_id)
    if migrated_v3 is not None:
        raw_records[request_id] = migrated_v3
        return migrated_v3, True
    migrated_v2 = _migrate_v2_record(current, request_id)
    if migrated_v2 is not None:
        raw_records[request_id] = migrated_v2
        return migrated_v2, True
    migrated = _legacy_record(
        current,
        request_id,
        now=timestamp,
        retry_horizon=retry_horizon,
        retention=retention,
    )
    if migrated is None:
        raise RuntimeError(_CORRUPT_RECORDS_ERROR)
    raw_records[request_id] = migrated
    return migrated, True


def preflight_request(
    store: dict[str, Any],
    request_id: str,
    *,
    now: float,
    retry_horizon: float,
    retention: float,
) -> tuple[dict[str, Any], dict[str, Any] | None, bool]:
    """Resolve cache/deadline before route reconstruction or child creation."""

    record, changed = ensure_request_shell(
        store,
        request_id,
        now=now,
        retry_horizon=retry_horizon,
        retention=retention,
    )
    if record["state"] in {"terminal", "quarantined"}:
        return record, copy.deepcopy(record["outcome"]), changed
    if float(now) >= record["deadline_at"]:
        outcome = quarantine_request(record, "request deadline reached", now=now)
        return record, outcome, True
    return record, None, changed


def attach_request_json(
    record: dict[str, Any], request_json: str, *, now: float
) -> bool:
    """Attach exact bytes once; a different replay can never replace them."""

    request_id = validate_request_id(record.get("request_id"))
    if _request_id_from_json(request_json) != request_id:
        raise ValueError("request JSON does not correlate to ingress request")
    current = record.get("request_json")
    if current is not None:
        if current != request_json:
            raise ValueError("ingress request JSON is immutable")
        return False
    if record.get("state") != "resolving":
        raise ValueError("request JSON cannot be attached in current state")
    timestamp = _timestamp(now)
    if timestamp is None:
        raise ValueError("invalid ingress timestamp")
    record["request_json"] = request_json
    record["state"] = "retryable"
    record["request_phase"] = "ready"
    record["updated_at"] = timestamp
    return True


def mark_retryable(
    record: dict[str, Any], disposition: str | None, *, now: float
) -> dict[str, Any]:
    if disposition not in RETRYABLE_DISPOSITIONS | {None}:
        raise ValueError("invalid retry disposition")
    if not isinstance(record.get("request_json"), str):
        raise ValueError("retry requires durable request JSON")
    timestamp = _timestamp(now)
    if timestamp is None:
        raise ValueError("invalid ingress timestamp")
    record["state"] = "retryable"
    record["updated_at"] = timestamp
    record["transport_disposition"] = normalize_transport_disposition(disposition)
    record["request_phase"] = "retryable"
    record["terminal_outcome"] = None
    record["checkpoint_already_advanced"] = False
    record["operator_attention_required"] = False
    record["blocked_reason"] = None
    record["next_action"] = None
    record["terminal_at"] = None
    record["quarantined_at"] = None
    record["quarantine_reason"] = None
    record["outcome"] = None
    return child_result(
        record["request_id"],
        checkpoint="retry",
        transport_disposition=disposition,
        request_phase="retryable",
        terminal_outcome=None,
    )


def record_terminal_outcome(
    record: dict[str, Any],
    *,
    transport_disposition: str | None,
    request_phase: str,
    terminal_outcome: str,
    now: float,
    reply: str = "",
    blocked_reason: str = "",
    next_action: str = "",
    handled: bool = True,
) -> dict[str, Any]:
    """Persist one user-visible truth; only verified delivery advances."""

    if terminal_outcome not in TERMINAL_OUTCOMES:
        raise ValueError("invalid terminal outcome")
    transport = normalize_transport_disposition(transport_disposition)
    if transport not in TERMINAL_OUTCOME_TRANSPORTS[terminal_outcome]:
        raise ValueError("transport disposition cannot produce terminal outcome")
    if not isinstance(record.get("request_json"), str):
        raise ValueError("terminal result requires durable request JSON")
    timestamp = _timestamp(now)
    if timestamp is None:
        raise ValueError("invalid ingress timestamp")
    if terminal_outcome == "delivered":
        if request_phase not in TERMINAL_OUTCOME_PHASES["delivered"]:
            raise ValueError("verified delivery must use terminal request phase")
        checkpoint = "advance"
        attention = False
        blocked = ""
        action = ""
    else:
        if request_phase not in TERMINAL_OUTCOME_PHASES[terminal_outcome]:
            raise ValueError("non-success outcome has invalid request phase")
        checkpoint = "hold"
        attention = True
        blocked = _fixed_reply(blocked_reason) or (
            "delivery truth unavailable"
            if terminal_outcome == "delivery_unknown"
            else "request was not delivered"
        )
        action = _fixed_reply(next_action) or (
            "inspect ingress evidence and resolve manually"
            if terminal_outcome == "delivery_unknown"
            else "review the failure before requesting a retry"
        )
    outcome = child_result(
        record["request_id"],
        checkpoint=checkpoint,
        transport_disposition=transport,
        request_phase=request_phase,
        terminal_outcome=terminal_outcome,
        reply=reply if checkpoint == "advance" else "",
        handled=handled,
    )
    record["state"] = "terminal"
    record["updated_at"] = timestamp
    record["transport_disposition"] = transport
    record["request_phase"] = request_phase
    record["terminal_outcome"] = terminal_outcome
    record["checkpoint_already_advanced"] = False
    record["operator_attention_required"] = attention
    record["blocked_reason"] = blocked or None
    record["next_action"] = action or None
    record["terminal_at"] = timestamp
    record["quarantined_at"] = None
    record["quarantine_reason"] = None
    record["outcome"] = outcome
    return copy.deepcopy(outcome)


def mark_terminal(
    record: dict[str, Any],
    disposition: str,
    *,
    now: float,
    reply: str,
) -> dict[str, Any]:
    """Reduce the legacy Tendwire response without treating transport as truth."""

    if disposition == "terminal_accepted":
        return record_terminal_outcome(
            record,
            transport_disposition="written_to_pty",
            request_phase="accepted_unverified",
            terminal_outcome="delivery_unknown",
            now=now,
            blocked_reason="transport accepted but submission was not verified",
            next_action="inspect the composer before any replay",
        )
    if disposition == "terminal_rejected":
        return record_terminal_outcome(
            record,
            transport_disposition=disposition,
            request_phase="terminal",
            terminal_outcome="not_delivered",
            now=now,
            blocked_reason="provider rejected the instruction",
            next_action="review the rejection before requesting a retry",
        )
    raise ValueError("invalid terminal transport disposition")


def attach_target_owner(
    record: dict[str, Any],
    stable_key: str,
    stable_key_version: int,
    *,
    now: float,
) -> bool:
    """Persist the stable public owner used to route a submission card."""

    owner = {
        "stable_key": stable_key,
        "stable_key_version": stable_key_version,
    }
    if not _valid_target_owner(owner):
        raise ValueError("invalid submission target owner")
    current = record.get("target_owner")
    if current is not None:
        if current != owner:
            raise ValueError("submission target owner is immutable")
        return False
    timestamp = _timestamp(now)
    if timestamp is None:
        raise ValueError("invalid ingress timestamp")
    record["target_owner"] = owner
    record["updated_at"] = timestamp
    return True


def attach_submission_receipt(
    record: dict[str, Any],
    submission_id: str,
    submission_state: str,
    turn_id: str | None,
    *,
    now: float,
) -> bool:
    """Attach or replay one validated v3 accepted-command receipt."""

    timestamp = _timestamp(now)
    if (
        timestamp is None
        or not isinstance(submission_id, str)
        or not submission_id.strip()
        or len(submission_id) > 200
        or submission_state not in _SUBMISSION_STATES
        or (turn_id is not None and (not isinstance(turn_id, str) or not turn_id.strip()))
        or not _valid_target_owner(record.get("target_owner"))
        or record.get("target_owner") is None
    ):
        raise ValueError("invalid submission receipt")
    current_id = record.get("submission_id")
    if current_id is not None and current_id != submission_id:
        raise ValueError("submission identity is immutable")
    current_turn = record.get("turn_id")
    if current_turn is not None and turn_id is not None and current_turn != turn_id:
        raise ValueError("submission turn identity is immutable")
    before = (
        record.get("submission_id"),
        record.get("submission_state"),
        record.get("turn_id"),
        record.get("submitted_at"),
        record.get("linked_at"),
    )
    record["submission_id"] = submission_id
    record["submission_state"] = submission_state
    record["turn_id"] = turn_id or current_turn
    if record.get("submitted_at") is None:
        record["submitted_at"] = timestamp
    if submission_state == "linked" and record["turn_id"] is None:
        raise ValueError("linked submission requires a turn identity")
    if record["turn_id"] is not None and record.get("linked_at") is None:
        record["linked_at"] = timestamp
    after = (
        record.get("submission_id"),
        record.get("submission_state"),
        record.get("turn_id"),
        record.get("submitted_at"),
        record.get("linked_at"),
    )
    changed = before != after
    if changed:
        record["updated_at"] = timestamp
    return changed


def link_submission(
    store: dict[str, Any],
    submission_id: str,
    turn_id: str,
    *,
    now: float,
    submission_state: str = "linked",
) -> tuple[dict[str, Any] | None, bool]:
    """Associate a retained receipt with the observed authoritative turn."""

    timestamp = _timestamp(now)
    if timestamp is None:
        raise ValueError("invalid ingress timestamp")
    records = _validated_records_mapping(store, now=timestamp)
    if records is None:
        return None, False
    matches = [
        record
        for request_id, record in records.items()
        if _valid_record(record, request_id)
        and record.get("submission_id") == submission_id
    ]
    if len(matches) > 1:
        raise RuntimeError(_CORRUPT_RECORDS_ERROR)
    if not matches:
        return None, False
    record = matches[0]
    before = copy.deepcopy(record)
    attach_submission_receipt(
        record,
        submission_id,
        submission_state,
        turn_id,
        now=timestamp,
    )
    return record, record != before


def retained_submission_records(
    store: dict[str, Any], *, now: float
) -> list[dict[str, Any]]:
    """Return validated receipt records; older records are inert fallback."""

    timestamp = _timestamp(now)
    if timestamp is None:
        raise ValueError("invalid ingress timestamp")
    records = _validated_records_mapping(store, now=timestamp)
    if records is None:
        return []
    return [
        record
        for request_id, record in records.items()
        if _valid_record(record, request_id)
        and isinstance(record.get("submission_id"), str)
        and (
            record.get("transport_disposition") == "written_to_pty"
            or record.get("terminal_outcome") == "delivered"
        )
    ]


def record_dedup_witness(
    record: dict[str, Any],
    *,
    prompt_fingerprint: str,
    composer_fingerprint: str | None,
    composer_readable: bool,
    provider_verdict: str,
    owner_generation: str | None,
    now: float,
) -> bool:
    """Persist Herdr-owned evidence used before a future replay.

    Herdres treats both fingerprints as opaque. It does not reproduce Herdr's
    classifier or hash algorithm. A replay is automatically eligible only when
    the provider positively proved non-receipt *and* a readable composer does
    not contain the prompt fingerprint. Delivery-unknown always overrides that
    eligibility and requires an operator decision.
    """

    timestamp = _timestamp(now)
    if (
        timestamp is None
        or not isinstance(prompt_fingerprint, str)
        or not prompt_fingerprint
        or len(prompt_fingerprint) > 200
        or provider_verdict not in _DEDUP_PROVIDER_VERDICTS
        or (
            owner_generation is not None
            and (
                not isinstance(owner_generation, str)
                or not owner_generation
                or len(owner_generation) > 200
            )
        )
    ):
        raise ValueError("invalid dedup witness")
    if composer_readable:
        if (
            not isinstance(composer_fingerprint, str)
            or not composer_fingerprint
            or len(composer_fingerprint) > 200
        ):
            raise ValueError("readable composer requires a fingerprint")
        comparison = (
            "match"
            if composer_fingerprint == prompt_fingerprint
            else "different"
        )
    else:
        if composer_fingerprint is not None:
            raise ValueError("unreadable composer cannot carry a fingerprint")
        comparison = "unreadable"
    automatic = (
        provider_verdict == "agent_prompt_not_received"
        and comparison == "different"
        and record.get("terminal_outcome") != "delivery_unknown"
    )
    witness = {
        "schema_version": 1,
        "prompt_fingerprint": prompt_fingerprint,
        "composer_fingerprint": composer_fingerprint,
        "comparison": comparison,
        "provider_verdict": provider_verdict,
        "owner_generation": owner_generation,
        "observed_at": timestamp,
        "automatic_replay_authorized": automatic,
    }
    if not _valid_dedup_witness(witness):
        raise ValueError("invalid dedup witness")
    before = record.get("dedup_witness")
    if before == witness:
        return False
    record["dedup_witness"] = witness
    record["updated_at"] = timestamp
    if comparison == "unreadable":
        record["operator_attention_required"] = True
        record["blocked_reason"] = "composer could not be read"
        record["next_action"] = "inspect the pane before any replay"
    return True


def dedup_witness_request(record: dict[str, Any]) -> dict[str, Any]:
    """Return the opaque evidence a future replay caller must refresh."""

    return {
        "schema_version": 1,
        "request_id": validate_request_id(record.get("request_id")),
        "target_owner": copy.deepcopy(record.get("target_owner")),
        "terminal_outcome": record.get("terminal_outcome"),
        "transport_disposition": record.get("transport_disposition"),
        "prior_witness": copy.deepcopy(record.get("dedup_witness")),
        "required_observation": "herdr_composer_prompt_fingerprint",
    }


def automatic_replay_authorized(record: dict[str, Any]) -> bool:
    """Return whether current evidence positively permits an automatic replay."""

    if record.get("terminal_outcome") == "delivery_unknown":
        return False
    witness = record.get("dedup_witness")
    return (
        record.get("terminal_outcome") == "not_delivered"
        and isinstance(witness, dict)
        and witness.get("provider_verdict") == "agent_prompt_not_received"
        and witness.get("comparison") == "different"
        and witness.get("automatic_replay_authorized") is True
    )


def operator_status_rows(
    store: dict[str, Any], *, now: float
) -> list[dict[str, Any]]:
    """Return public-safe ingress state without prompt or composer content."""

    timestamp = _timestamp(now)
    if timestamp is None:
        raise ValueError("invalid ingress timestamp")
    records = _validated_records_mapping(store, now=timestamp)
    if records is None:
        return []
    rows: list[dict[str, Any]] = []
    for request_id, raw in records.items():
        record = raw if _valid_record(raw, request_id) else None
        if record is None:
            migrated = _migrate_v3_record(raw, request_id)
            if migrated is None:
                migrated = _migrate_v2_record(raw, request_id)
            record = migrated
        if record is None:
            continue
        witness = record.get("dedup_witness")
        rows.append(
            {
                "request_id": request_id,
                "age_seconds": max(0.0, timestamp - float(record["created_at"])),
                "state": record["state"],
                "transport_disposition": record["transport_disposition"],
                "request_phase": record["request_phase"],
                "terminal_outcome": record["terminal_outcome"],
                "checkpoint_already_advanced": record[
                    "checkpoint_already_advanced"
                ],
                "operator_attention_required": record[
                    "operator_attention_required"
                ],
                "blocked_reason": record["blocked_reason"],
                "next_action": record["next_action"],
                "submission_id": record["submission_id"],
                "turn_id": record["turn_id"],
                "target_owner": copy.deepcopy(record["target_owner"]),
                "dedup_witness": (
                    {
                        "comparison": witness["comparison"],
                        "provider_verdict": witness["provider_verdict"],
                        "owner_generation": witness["owner_generation"],
                        "observed_at": witness["observed_at"],
                        "automatic_replay_authorized": witness[
                            "automatic_replay_authorized"
                        ],
                    }
                    if isinstance(witness, dict)
                    else None
                ),
            }
        )
    return sorted(
        rows,
        key=lambda row: (
            not bool(row["operator_attention_required"]),
            -float(row["age_seconds"]),
            str(row["request_id"]),
        ),
    )


def stale_target_refresh_json(record: dict[str, Any], *, now: float) -> str | None:
    """Perform the sole allowed byte rewrite: remove worker_fingerprint once."""

    if record.get("stale_target_refreshed") is True:
        return None
    request_json = record.get("request_json")
    if not isinstance(request_json, str):
        return None
    try:
        request = json.loads(request_json)
    except (json.JSONDecodeError, TypeError, ValueError):
        return None
    target = request.get("target") if isinstance(request, dict) else None
    if not isinstance(target, dict) or "worker_fingerprint" not in target:
        return None
    refreshed = copy.deepcopy(request)
    refreshed["target"].pop("worker_fingerprint")
    refreshed_json = canonical_request_json(refreshed)
    timestamp = _timestamp(now)
    if timestamp is None:
        raise ValueError("invalid ingress timestamp")
    record["request_json"] = refreshed_json
    record["stale_target_refreshed"] = True
    record["updated_at"] = timestamp
    record["transport_disposition"] = "no_receipt"
    record["request_phase"] = "retryable"
    return refreshed_json


def prune_requests(store: dict[str, Any], *, now: float) -> bool:
    """Prune valid v3 records only after validating all retained evidence."""

    timestamp = _timestamp(now)
    if timestamp is None:
        raise ValueError("invalid ingress timestamp")
    records = _validated_records_mapping(store, now=timestamp)
    if records is None:
        return False
    changed = False
    for request_id, record in list(records.items()):
        migrated = _migrate_v3_record(record, request_id)
        if migrated is not None:
            records[request_id] = migrated
            changed = True
            continue
        migrated = _migrate_v2_record(record, request_id)
        if migrated is not None:
            records[request_id] = migrated
            changed = True
    expired = [
        request_id
        for request_id, record in records.items()
        if _valid_record(record, request_id)
        and timestamp > record["retain_until"]
    ]
    for request_id in expired:
        records.pop(request_id)
    return changed or bool(expired)
