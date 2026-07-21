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
RECORD_SCHEMA_VERSION = 3
PREVIOUS_RECORD_SCHEMA_VERSION = 2
CHILD_SCHEMA_VERSION = 1
RECORD_STATES = frozenset({"resolving", "retryable", "terminal", "quarantined"})
DISPOSITIONS = frozenset(
    {
        "no_receipt",
        "in_progress",
        "terminal_accepted",
        "terminal_rejected",
        "terminal_uncertain",
    }
)
RETRYABLE_DISPOSITIONS = frozenset({"no_receipt", "in_progress"})
TERMINAL_DISPOSITIONS = frozenset({"terminal_accepted", "terminal_rejected"})
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
        "last_disposition",
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
_V2_RECORD_FIELDS = _RECORD_FIELDS - {
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
        "disposition",
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


def child_result(
    request_id: str,
    *,
    checkpoint: str,
    disposition: str | None,
    reply: str = "",
    handled: bool = True,
) -> dict[str, Any]:
    """Build the exact child envelope consumed by the Telegram gateway."""

    request_id = validate_request_id(request_id)
    if type(handled) is not bool:
        raise ValueError("handled must be a boolean")
    if checkpoint not in {"retry", "advance"}:
        raise ValueError("invalid ingress checkpoint decision")
    if disposition is not None and disposition not in DISPOSITIONS:
        raise ValueError("invalid command disposition")
    if checkpoint == "retry":
        if not handled or disposition not in {None, "no_receipt", "in_progress"}:
            raise ValueError("invalid retry child outcome")
        reply = ""
    else:
        if disposition in RETRYABLE_DISPOSITIONS:
            raise ValueError("retryable disposition cannot advance checkpoint")
        if not handled and (disposition is not None or reply):
            raise ValueError("unhandled outcome cannot carry command details")
    return {
        "schema_version": CHILD_SCHEMA_VERSION,
        "handled": handled,
        "request_id": request_id,
        "checkpoint": checkpoint,
        "disposition": disposition,
        "reply": _fixed_reply(reply),
    }


def _valid_child(value: Any, request_id: str) -> bool:
    if not isinstance(value, dict) or frozenset(value) != _CHILD_FIELDS:
        return False
    if (
        value.get("schema_version") != CHILD_SCHEMA_VERSION
        or type(value.get("handled")) is not bool
        or value.get("request_id") != request_id
        or value.get("checkpoint") != "advance"
        or value.get("disposition")
        not in TERMINAL_DISPOSITIONS | {"terminal_uncertain", None}
        or not isinstance(value.get("reply"), str)
        or value["reply"] != _fixed_reply(value["reply"])
    ):
        return False
    if value["handled"] is False and (
        value["disposition"] is not None or value["reply"]
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


def _valid_record_version(
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
    if schema_version == RECORD_SCHEMA_VERSION and not _valid_submission_fields(record):
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
            and last_disposition in TERMINAL_DISPOSITIONS
            and terminal_timestamp is not None
            and terminal_timestamp <= updated_at
            and (
                schema_version == RECORD_SCHEMA_VERSION
                or terminal_timestamp == updated_at
            )
            and quarantined_at is None
            and record.get("quarantine_reason") is None
            and _valid_child(outcome, request_id)
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
        and _valid_child(outcome, request_id)
        and outcome.get("disposition") == last_disposition
    )


def _valid_record(record: Any, request_id: str) -> bool:
    return _valid_record_version(
        record,
        request_id,
        schema_version=RECORD_SCHEMA_VERSION,
        fields=_RECORD_FIELDS,
    )


def _valid_v2_record(record: Any, request_id: str) -> bool:
    return _valid_record_version(
        record,
        request_id,
        schema_version=PREVIOUS_RECORD_SCHEMA_VERSION,
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
        "last_disposition": None,
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


def _migrate_v2_record(value: Any, request_id: str) -> dict[str, Any] | None:
    if not _valid_v2_record(value, request_id):
        return None
    migrated = copy.deepcopy(value)
    migrated.update(
        {
            "schema_version": RECORD_SCHEMA_VERSION,
            "submission_id": None,
            "submission_state": None,
            "turn_id": None,
            "target_owner": None,
            "submitted_at": None,
            "linked_at": None,
        }
    )
    return migrated if _valid_record(migrated, request_id) else None


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
    if not (
        _valid_record(record, request_id)
        or _valid_v2_record(record, request_id)
    ):
        # Pre-v2 legacy records are retry evidence, never terminal authority.
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
    """Make uncertainty terminal locally and cache its fixed child outcome."""

    if disposition not in {None, "terminal_uncertain"}:
        raise ValueError("invalid quarantine disposition")
    request_id = validate_request_id(record.get("request_id"))
    timestamp = _timestamp(now)
    if timestamp is None:
        raise ValueError("invalid quarantine timestamp")
    outcome = child_result(
        request_id,
        checkpoint="advance",
        disposition=disposition,
        reply=reply,
        handled=handled,
    )
    record["state"] = "quarantined"
    record["updated_at"] = timestamp
    record["last_disposition"] = disposition
    record["terminal_at"] = None
    record["quarantined_at"] = timestamp
    record["quarantine_reason"] = _fixed_reply(reason) or "uncertain"
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
    record["last_disposition"] = disposition
    record["terminal_at"] = None
    record["quarantined_at"] = None
    record["quarantine_reason"] = None
    record["outcome"] = None
    return child_result(
        record["request_id"], checkpoint="retry", disposition=disposition
    )


def mark_terminal(
    record: dict[str, Any],
    disposition: str,
    *,
    now: float,
    reply: str,
) -> dict[str, Any]:
    if disposition not in TERMINAL_DISPOSITIONS:
        raise ValueError("invalid terminal disposition")
    if not isinstance(record.get("request_json"), str):
        raise ValueError("terminal result requires durable request JSON")
    timestamp = _timestamp(now)
    if timestamp is None:
        raise ValueError("invalid ingress timestamp")
    outcome = child_result(
        record["request_id"],
        checkpoint="advance",
        disposition=disposition,
        reply=reply,
    )
    record["state"] = "terminal"
    record["updated_at"] = timestamp
    record["last_disposition"] = disposition
    record["terminal_at"] = timestamp
    record["quarantined_at"] = None
    record["quarantine_reason"] = None
    record["outcome"] = outcome
    return copy.deepcopy(outcome)


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
    """Return validated v3 receipt records; older records are inert fallback."""

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
        and record.get("last_disposition") == "terminal_accepted"
    ]


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
    record["last_disposition"] = "no_receipt"
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
