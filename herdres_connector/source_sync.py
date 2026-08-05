"""Tendwire source-mode sync to Telegram."""

from __future__ import annotations

import hashlib
import json
import sys
import time
import weakref
from copy import deepcopy
from dataclasses import dataclass
from typing import Any, Callable, Mapping

from . import accounts, config, decisions, speech, state
from .managed_bots import MANAGER_BOT_KIND, managed_bot_kind_for_entry
from .rendering import normalized_status, render_pending, render_status_overview, status_emoji
from .rich_delivery import (
    PresentationContentError,
    PresentationOversizeError,
    RICH_BAD_REQUEST_LIMIT,
    RICH_RENDER_VERSION,
    RICH_STATE_UPDATE_KEY,
    TURN_DELIVERY_MAX_PARTS,
    edit_feed_item,
    edit_turn_delivery_part,
    prepare_turn_delivery_parts,
    send_feed_item,
    send_turn_delivery_part,
    turn_item_from_source,
)
from .safe import (
    PRIVATE_AGENT_CONTAINER_FIELD_NAMES,
    PRIVATE_AGENT_FIELD_NAMES,
    PRIVATE_STRUCTURE_MAX_DEPTH,
    PRIVATE_STRUCTURE_MAX_ITEMS,
    compact_ws,
    html_escape,
    mapping_has_private_agent_discriminator,
    normalized_public_key,
    short_hash,
)
from .telegram_delivery import (
    DELIVERY_FORMAT_STATE_UPDATE_KEY,
    TOPIC_ICON_COLORS,
    RateLimited,
    TelegramClient,
    TelegramError,
    classify_telegram_error,
    delete_turn_delivery_message,
    drain_outbox,
    topic_icon_catalog,
    topic_icon_id,
)
from .tendwire_client import TendwireClient, TendwireError

RENDER_VERSION = "telegram-rich-v28-primary-dual-bound"
PRESENTATION_VERSION = "turn-present-v31"
TURN_SCHEMA_VERSION = 2
TURN_CONTENT_SCHEMA_VERSION = 1
_SUBMISSION_ID_KEY = "_herdres_submission_id"
_SUBMISSION_STATE_KEY = "_herdres_submission_state"
_TURN_STABLE_KEY_KEY = "_herdres_stable_key"
_TURN_STABLE_KEY_VERSION_KEY = "_herdres_stable_key_version"
_SUBMISSION_STATES = frozenset(
    {"pending_observation", "observed", "complete", "linked"}
)
_TOPIC_CLEANUP_ATTEMPT_CAP = 3
_TOPIC_CLEANUP_AUDIT_LIMIT = 200
_TOPIC_CLEANUP_MIN_RETRY_SECONDS = 60.0
_TOPIC_CLEANUP_PERMANENT_ERROR_KINDS = frozenset(
    {"bad_request", "bot_access", "capability"}
)
_DELIVERY_FORMAT_FALLBACK_LIMIT = 64
_BINDING_STATE_BOUND = "bound"
_BINDING_STATE_PENDING_CREATE = "pending_create"
_BINDING_STATE_NO_IDENTITY = "no_stable_identity"
_BINDING_STATE_ABSENT = "absent_from_snapshot"

# These fields belong to ACP's private structured-event side. Herdres consumes
# only Tendwire's neutral turn projection; tool/plan/permission/control details
# require a separately versioned public contract before they can be presented.
# Checking structural keys and exact ACP discriminator values keeps ordinary
# assistant messages free to discuss tools or plans without being mistaken for
# protocol payloads.
_PRIVATE_AGENT_ROOT_FIELD_NAMES = frozenset(
    {
        "availablecommands",
        "configoption",
        "configoptions",
        "control",
        "controlevent",
        "currentmode",
        "extension",
        "extensions",
        "permission",
        "permissionrequest",
        "plan",
        "reasoning",
        "thought",
        "thoughts",
        "toolcall",
        "toolcalls",
        "toolcallupdate",
        "toolcallupdates",
    }
)


@dataclass
class _DeliveryWriteBudget:
    """One pass-wide allowance charged by physical provider attempts."""

    limit: int
    spent: int = 0

    @property
    def remaining(self) -> int:
        return max(0, self.limit - self.spent)

    def charge(self, result: Any, *, default: int = 1) -> int:
        writes = _telegram_physical_writes(result, default=default)
        self.spent += writes
        return writes


class _TurnContentError(RuntimeError):
    def __init__(
        self,
        status: str,
        message: str,
        *,
        conflict: bool = False,
        part_count: int | None = None,
    ) -> None:
        super().__init__(message)
        self.status = status
        self.conflict = conflict
        self.part_count = part_count


def _contains_private_agent_turn_fields(value: Any) -> bool:
    """Return whether a neutral turn illegally embeds private ACP structure.

    Public ``meta`` is intentionally extensible, so generic words such as
    ``plan`` or ``control`` are rejected only as illegal root turn fields.
    Nested detection is limited to unmistakably private keys and exact ACP
    envelope shapes. Bounds and cycle detection make malformed input fail
    closed without risking recursion failure.
    """

    stack: list[tuple[Any, int, bool]] = [(value, 0, True)]
    seen: set[int] = set()
    items = 0
    while stack:
        current, depth, is_root = stack.pop()
        items += 1
        if items > PRIVATE_STRUCTURE_MAX_ITEMS or depth > PRIVATE_STRUCTURE_MAX_DEPTH:
            return True
        if isinstance(current, Mapping):
            identity = id(current)
            if identity in seen:
                return True
            seen.add(identity)
            plain = dict(current)
            if mapping_has_private_agent_discriminator(plain):
                return True
            for raw_key, item in current.items():
                key = str(raw_key)
                normalized = normalized_public_key(key)
                if (
                    key.lower() == "_meta"
                    or normalized in PRIVATE_AGENT_CONTAINER_FIELD_NAMES
                    or normalized in PRIVATE_AGENT_FIELD_NAMES
                    or is_root and normalized in _PRIVATE_AGENT_ROOT_FIELD_NAMES
                ):
                    return True
                stack.append((item, depth + 1, False))
        elif isinstance(current, (list, tuple)):
            identity = id(current)
            if identity in seen:
                return True
            seen.add(identity)
            stack.extend((item, depth + 1, False) for item in current)
    return False


@dataclass
class SyncRuntime:
    tendwire: TendwireClient
    telegram: TelegramClient
    dry_run: bool = False
    with_outbox: bool = True
    max_sends: int = 8
    checkpoint: Callable[[], None] | None = None
    after_provider_accept: Callable[[], None] | None = None
    delivery_write_budget: _DeliveryWriteBudget | None = None


@dataclass(frozen=True)
class _OfflockEntryOperation:
    """Immutable provenance for one provider request made without the state lock."""

    entry_key: str
    entry_type: str
    pane_uuid: str
    stable_key: str
    stable_key_version: int
    space_id: str
    route_topic_id: str
    owner_generation: tuple[str, ...]
    retired: bool
    routable: bool
    observed_fields: tuple[tuple[str, Any], ...] = ()
    message_id: str = ""
    plan_token: str = ""
    revision: str = ""


@dataclass(frozen=True)
class _OfflockEntryResolution:
    disposition: str
    entry: dict[str, Any] | None = None


@dataclass(frozen=True)
class _OfflockEntryExecution:
    """Provider result paired with the only permitted post-call resolution."""

    result: Any
    resolution: _OfflockEntryResolution


_OFFLOCK_APPLY = "apply"
_OFFLOCK_RECONCILE = "reconcile"
_OFFLOCK_ABANDON = "abandon"

# Provider calls fail closed. Only these methods are proven read-only; every
# other callable must be reached through a declared capability. Lease-acquiring
# polls are intentionally absent because they mutate provider lease state.
_READ_ONLY_PROVIDER_METHODS = {
    "telegram": frozenset({"configured", "with_token"}),
    "tendwire": frozenset(
        {
            "doctor",
            "pending",
            "snapshot",
            "turn_content_get",
            "turn_delta",
            "turns",
        }
    ),
}


@dataclass(frozen=True, eq=False)
class _ProviderMutation:
    """One declared provider capability; it never exposes the raw provider."""

    capability: str
    reason: str
    args: tuple[Any, ...] = ()
    kwargs: tuple[tuple[str, Any], ...] = ()
    api_token: str = ""


_DIRECT_PROVIDER_CAPABILITIES = {
    f"telegram.{name}": name
    for name in {
        "answer_callback_query",
        "close_topic",
        "close_topic_for_cleanup",
        "create_topic",
        "delete_message",
        "delete_topic",
        "delete_topic_for_cleanup",
        "edit_message",
        "edit_message_reply_markup",
        "edit_topic_icon",
        "pin_message",
        "rename_topic",
        "reopen_topic",
        "reopen_topic_for_cleanup",
        "send_message",
        "send_photo",
        "send_voice",
    }
} | {
    f"tendwire.{name}": name
    for name in {
        "call",
        "command",
        "command_json",
        "connector_ack",
        "connector_fail",
        "connector_poll",
        "connector_prepare_begin",
        "connector_prepare_commit",
        "connector_prepare_part",
        "turn_final_ack",
        "turn_final_defer",
        "turn_final_fail",
        "turn_final_poll",
    }
}

_ADAPTER_PROVIDER_CAPABILITIES = frozenset(
    {
        "telegram.delete_turn_delivery_message",
        "telegram.edit_feed_item",
        "telegram.edit_turn_delivery_part",
        "telegram.send_feed_item",
        "telegram.send_turn_delivery_part",
        "telegram.send_voice_batch",
    }
)

_TELEGRAM_RATE_LIMIT_MAX_RETRIES = 1
_TELEGRAM_RATE_LIMIT_MAX_WAIT_SECONDS = 15
_TELEGRAM_RATE_LIMIT_EVENT_LIMIT = 64
_PROVIDER_BACKPRESSURE_KEY = "_herdres_backpressure_events"
_TELEGRAM_NO_RETRY_CAPABILITIES = frozenset(
    {
        "telegram.send_feed_item",
        "telegram.send_message",
        "telegram.send_photo",
        "telegram.send_turn_delivery_part",
        "telegram.send_voice",
        "telegram.send_voice_batch",
    }
)
_TELEGRAM_DEDICATED_BACKPRESSURE_CAPABILITIES = frozenset(
    {
        "telegram.close_topic_for_cleanup",
        "telegram.delete_topic_for_cleanup",
        "telegram.reopen_topic_for_cleanup",
    }
)
_TELEGRAM_API_METHOD_BY_CAPABILITY = {
    "telegram.answer_callback_query": "answerCallbackQuery",
    "telegram.close_topic": "closeForumTopic",
    "telegram.close_topic_for_cleanup": "closeForumTopic",
    "telegram.create_topic": "createForumTopic",
    "telegram.delete_message": "deleteMessage",
    "telegram.delete_topic": "deleteForumTopic",
    "telegram.delete_topic_for_cleanup": "deleteForumTopic",
    "telegram.delete_turn_delivery_message": "deleteMessage",
    "telegram.edit_feed_item": "editMessageText",
    "telegram.edit_message": "editMessageText",
    "telegram.edit_message_reply_markup": "editMessageReplyMarkup",
    "telegram.edit_topic_icon": "editForumTopic",
    "telegram.edit_turn_delivery_part": "editMessageText",
    "telegram.pin_message": "pinChatMessage",
    "telegram.rename_topic": "editForumTopic",
    "telegram.reopen_topic": "reopenForumTopic",
    "telegram.reopen_topic_for_cleanup": "reopenForumTopic",
    "telegram.send_feed_item": "sendMessage",
    "telegram.send_message": "sendMessage",
    "telegram.send_photo": "sendPhoto",
    "telegram.send_turn_delivery_part": "sendMessage",
    "telegram.send_voice": "sendVoice",
    "telegram.send_voice_batch": "sendVoice",
}


def _telegram_rate_limit_from_result(
    result: Any,
) -> tuple[int, str, str] | None:
    if not isinstance(result, dict) or result.get("rate_limited") is not True:
        return None
    try:
        retry_after = max(1, int(result.get("retry_after") or 1))
    except (TypeError, ValueError):
        retry_after = 1
    return (
        retry_after,
        compact_ws(result.get("error"), 300)
        or "Telegram rate limited",
        str(result.get("method") or ""),
    )


def _nested_rate_limit(
    error: BaseException,
    *,
    expected_method: str,
) -> RateLimited | None:
    """Find relevant Telegram backpressure hidden by a transport wrapper.

    Explicit causes are authoritative.  Implicit context is only usable when
    the wrapper did not suppress it with ``raise ... from None``.  A rate
    limit from another Bot API method is unrelated to this operation and must
    not be confidently misattributed.
    """

    current: BaseException | None = error
    seen: set[int] = set()
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        if isinstance(current, RateLimited):
            method = str(current.method or "")
            return (
                current
                if not method or method == expected_method
                else None
            )
        if current.__cause__ is not None:
            current = current.__cause__
        elif not current.__suppress_context__:
            current = current.__context__
        else:
            current = None
    return None


def _telegram_rate_limit_failure(
    *,
    status: str,
    error: str,
    method: str,
    retry_after: int,
    retries: int,
    events: list[dict[str, Any]],
    accepted_message_ids: tuple[str, ...] = (),
) -> dict[str, Any]:
    result = {
        "ok": False,
        "status": status,
        "error": compact_ws(error, 300),
        "rate_limited": True,
        "retry_after": retry_after,
        "method": method,
        "retry_attempts": retries,
        "retry_exhausted": status == "telegram_rate_limit_exhausted",
        _PROVIDER_BACKPRESSURE_KEY: events,
    }
    if accepted_message_ids:
        result["accepted_message_ids"] = list(
            accepted_message_ids
        )
    return result


def _record_telegram_backpressure(
    store: dict[str, Any],
    result: Any,
) -> Any:
    """Absorb executor telemetry into bounded operator-readable state."""

    if not isinstance(result, dict):
        return result
    events = result.pop(_PROVIDER_BACKPRESSURE_KEY, None)
    if not isinstance(events, list) or not events:
        return result
    telegram = _telegram_state(store)
    backpressure = telegram.get("rate_limit_backpressure")
    if not isinstance(backpressure, dict):
        backpressure = {"sequence": 0, "events": []}
        telegram["rate_limit_backpressure"] = backpressure
    existing = backpressure.get("events")
    if not isinstance(existing, list):
        existing = []
    for raw in events:
        if not isinstance(raw, dict):
            continue
        sequence = int(backpressure.get("sequence") or 0) + 1
        backpressure["sequence"] = sequence
        existing.append(
            {
                "sequence": sequence,
                "observed_at": float(raw.get("observed_at") or time.time()),
                "method": compact_ws(raw.get("method"), 80),
                "capability": compact_ws(raw.get("capability"), 120),
                "retry_after": int(raw.get("retry_after") or 1),
                "observed_wait_seconds": float(
                    raw.get("observed_wait_seconds") or 0
                ),
                "outcome": compact_ws(raw.get("outcome"), 80),
            }
        )
    backpressure["events"] = existing[-_TELEGRAM_RATE_LIMIT_EVENT_LIMIT:]
    backpressure["last"] = deepcopy(backpressure["events"][-1])
    return result


def _telegram_backpressure_sequence(store: dict[str, Any]) -> int:
    telegram = store.get("telegram")
    if not isinstance(telegram, dict):
        return 0
    backpressure = telegram.get("rate_limit_backpressure")
    if not isinstance(backpressure, dict):
        return 0
    try:
        return max(0, int(backpressure.get("sequence") or 0))
    except (TypeError, ValueError):
        return 0


def _telegram_backpressure_since(
    store: dict[str, Any],
    sequence: int,
) -> dict[str, Any]:
    telegram = store.get("telegram")
    backpressure = (
        telegram.get("rate_limit_backpressure")
        if isinstance(telegram, dict)
        else None
    )
    events = (
        backpressure.get("events")
        if isinstance(backpressure, dict)
        and isinstance(backpressure.get("events"), list)
        else []
    )
    current = _telegram_backpressure_sequence(store)
    return {
        "count": max(0, current - sequence),
        "events": [
            deepcopy(event)
            for event in events
            if isinstance(event, dict)
            and int(event.get("sequence") or 0) > sequence
        ],
        "max_retries": _TELEGRAM_RATE_LIMIT_MAX_RETRIES,
        "max_wait_seconds": _TELEGRAM_RATE_LIMIT_MAX_WAIT_SECONDS,
    }


def _provider_mutation(
    capability: str,
    *,
    reason: str,
    args: tuple[Any, ...] = (),
    kwargs: Mapping[str, Any] | None = None,
    api_token: str = "",
) -> _ProviderMutation:
    capability = str(capability).strip()
    reason = str(reason).strip()
    if (
        capability not in _DIRECT_PROVIDER_CAPABILITIES
        and capability not in _ADAPTER_PROVIDER_CAPABILITIES
    ):
        raise ValueError(f"unknown provider mutation capability: {capability!r}")
    if not reason or capability not in reason:
        raise ValueError(
            "provider mutation reason must name its declared capability"
        )
    return _ProviderMutation(
        capability=capability,
        reason=reason,
        args=tuple(args),
        kwargs=tuple((str(key), value) for key, value in (kwargs or {}).items()),
        api_token=str(api_token or ""),
    )


def _entry_owner_generation(entry: Mapping[str, Any]) -> tuple[str, ...]:
    """Fields whose change means the request no longer owns the same route."""

    identity = state.entry_stable_identity(dict(entry))
    return (
        state.entry_pane_uuid(dict(entry)),
        str((identity or ("", 0))[0]),
        str((identity or ("", 0))[1]),
        _entry_worker_id(dict(entry)),
        compact_ws(entry.get("tendwire_fingerprint"), 160),
        _entry_space_id(dict(entry)),
        compact_ws(entry.get("active_worker_id"), 160),
        compact_ws(entry.get("active_worker_fingerprint"), 160),
        compact_ws(entry.get("active_worker_stable_key"), 160),
        str(entry.get("active_worker_stable_key_version") or ""),
    )


def _entry_operation_key(
    store: dict[str, Any], entry: dict[str, Any]
) -> tuple[str, str]:
    for entry_key, candidate in state.source_worker_entries(store).items():
        if candidate is entry:
            return entry_key, "worker"
    for entry_key, candidate in state.source_space_entries(store).items():
        if candidate is entry:
            return entry_key, "space"
    # Delivery entries in space mode are worker-shaped views whose route came
    # from the owning space. Durable worker identity is still the operation
    # identity, so resolve it rather than falling back to object position.
    pane_uuid = state.entry_pane_uuid(entry)
    if pane_uuid:
        entry_key, candidate = state.find_worker_entry_by_pane_uuid(
            store, pane_uuid
        )
        if entry_key is not None and candidate is not None:
            return entry_key, "worker"
    identity = state.entry_stable_identity(entry)
    if identity is not None:
        entry_key, candidate = state.find_worker_entry_by_stable_key(
            store, identity[0]
        )
        if entry_key is not None and candidate is not None:
            return entry_key, "worker"
    space_id = _entry_space_id(entry)
    if space_id:
        entry_key, candidate = state.find_space_entry_by_id(store, space_id)
        if entry_key is not None and candidate is not None:
            return entry_key, "space"
    return "", str(entry.get("entry_type") or "")


def _capture_entry_operation(
    store: dict[str, Any],
    entry: dict[str, Any],
    *,
    topic_id: str | None = None,
    message_id: str = "",
    plan_token: str = "",
    revision: str = "",
    observe: tuple[str, ...] = (),
) -> _OfflockEntryOperation:
    """Capture request provenance before an off-lock provider call."""

    entry_key, entry_type = _entry_operation_key(store, entry)
    identity = state.entry_stable_identity(entry)
    return _OfflockEntryOperation(
        entry_key=entry_key,
        entry_type=entry_type,
        pane_uuid=state.entry_pane_uuid(entry),
        stable_key=str((identity or ("", 0))[0]),
        stable_key_version=int((identity or ("", 0))[1]),
        space_id=_entry_space_id(entry),
        route_topic_id=str(
            (
                entry.get("topic_id")
                if topic_id is None
                else topic_id
            )
            or ""
        ),
        owner_generation=_entry_owner_generation(entry),
        retired=state.entry_is_retired(entry),
        routable=state.entry_is_routable(entry),
        observed_fields=tuple((field, deepcopy(entry.get(field))) for field in observe),
        message_id=str(message_id or ""),
        plan_token=str(plan_token or ""),
        revision=str(revision or ""),
    )


def _capture_global_operation(
    store: dict[str, Any],
    *,
    topic_id: str,
    message_id: str = "",
) -> _OfflockEntryOperation:
    telegram = _telegram_state(store)
    return _OfflockEntryOperation(
        entry_key="telegram:pinned_status",
        entry_type="global",
        pane_uuid="",
        stable_key="",
        stable_key_version=0,
        space_id="",
        route_topic_id=str(topic_id or ""),
        owner_generation=("",) * 10,
        retired=False,
        routable=False,
        observed_fields=(
            (
                "pinned_status_message_id",
                deepcopy(telegram.get("pinned_status_message_id")),
            ),
            (
                "pinned_status_hash",
                deepcopy(telegram.get("pinned_status_hash")),
            ),
        ),
        message_id=str(message_id or ""),
    )


def _resolve_operation_entry(
    store: dict[str, Any], operation: _OfflockEntryOperation
) -> dict[str, Any] | None:
    entry: dict[str, Any] | None = None
    if operation.entry_type == "worker":
        if operation.pane_uuid:
            _key, entry = state.find_worker_entry_by_pane_uuid(
                store, operation.pane_uuid
            )
            if entry is None:
                matches = [
                    candidate
                    for candidate in state.source_worker_entries(store).values()
                    if state.entry_pane_uuid(candidate) == operation.pane_uuid
                ]
                entry = matches[0] if len(matches) == 1 else None
                if (
                    entry is None
                    and not operation.routable
                    and operation.entry_key
                ):
                    candidate = state.source_worker_entries(store).get(
                        operation.entry_key
                    )
                    if (
                        candidate is not None
                        and state.entry_pane_uuid(candidate)
                        == operation.pane_uuid
                    ):
                        entry = candidate
        elif operation.stable_key:
            _key, entry = state.find_worker_entry_by_stable_key(
                store, operation.stable_key
            )
            if entry is None:
                matches = [
                    candidate
                    for candidate in state.source_worker_entries(store).values()
                    if state.entry_continuity_identity(candidate)
                    == (operation.stable_key, operation.stable_key_version)
                ]
                entry = matches[0] if len(matches) == 1 else None
                if (
                    entry is None
                    and not operation.routable
                    and operation.entry_key
                ):
                    candidate = state.source_worker_entries(store).get(
                        operation.entry_key
                    )
                    if (
                        candidate is not None
                        and state.entry_continuity_identity(candidate)
                        == (
                            operation.stable_key,
                            operation.stable_key_version,
                        )
                    ):
                        entry = candidate
        elif operation.entry_key:
            entry = state.source_worker_entries(store).get(operation.entry_key)
    elif operation.entry_type == "space":
        if operation.space_id:
            _key, entry = state.find_space_entry_by_id(
                store, operation.space_id
            )
        elif operation.entry_key:
            entry = state.source_space_entries(store).get(operation.entry_key)
    elif operation.entry_type == "global":
        entry = _telegram_state(store)
    return entry


def _compare_and_apply_entry_operation(
    store: dict[str, Any], operation: _OfflockEntryOperation
) -> _OfflockEntryResolution:
    """Resolve the request owner and make the only post-call apply decision.

    Exact provider facts are deliberately applied outside this guard:
    tombstoning the exact requested topic and retiring the exact operated
    message remain valid even if the pane moved while the lock was released.
    """

    entry = _resolve_operation_entry(store, operation)
    if operation.entry_type == "global":
        if entry is None:
            return _OfflockEntryResolution(_OFFLOCK_ABANDON)
        if (
            any(
                entry.get(field) != expected
                for field, expected in operation.observed_fields
            )
            or (
                operation.route_topic_id
                and str(config.general_thread_id(store))
                != operation.route_topic_id
            )
        ):
            return _OfflockEntryResolution(_OFFLOCK_RECONCILE)
        return _OfflockEntryResolution(_OFFLOCK_APPLY, entry)
    if entry is None or state.entry_is_retired(entry) != operation.retired:
        return _OfflockEntryResolution(_OFFLOCK_ABANDON)
    if (
        state.entry_is_routable(entry) != operation.routable
        or _entry_owner_generation(entry) != operation.owner_generation
        or str(entry.get("topic_id") or "") != operation.route_topic_id
        or any(
            entry.get(field) != expected
            for field, expected in operation.observed_fields
        )
        or (
            operation.plan_token
            and entry.get("pending_plan_token") != operation.plan_token
        )
        or (
            operation.revision
            and entry.get("pending_content_revision") != operation.revision
        )
    ):
        return _OfflockEntryResolution(_OFFLOCK_RECONCILE)
    return _OfflockEntryResolution(_OFFLOCK_APPLY, entry)


def _operation_binding_entry(
    operation: _OfflockEntryOperation,
) -> dict[str, Any]:
    """Minimal immutable owner snapshot for an accepted stale message."""

    entry: dict[str, Any] = {
        "entry_type": operation.entry_type,
        "topic_id": operation.route_topic_id,
        "tendwire_worker_id": operation.owner_generation[3],
        "tendwire_fingerprint": operation.owner_generation[4],
        "tendwire_space_id": operation.owner_generation[5],
        # Include the local binding aliases as well.  Most callers pass this
        # snapshot through state.bind_message_to_worker(), but post-ACK
        # reconciliation applies it directly to an existing binding.  Keeping
        # both views aligned prevents a reply from following a stale pane
        # owner in a shared space topic.
        "worker_id": operation.owner_generation[3],
        "worker_fingerprint": operation.owner_generation[4],
        "space_id": operation.owner_generation[5],
    }
    if operation.pane_uuid:
        entry["pane_uuid"] = operation.pane_uuid
        entry["pane_uuid_version"] = state.PANE_UUID_VERSION
    if operation.stable_key:
        entry["tendwire_stable_key"] = operation.stable_key
        entry["tendwire_stable_key_version"] = operation.stable_key_version
        entry["stable_key"] = operation.stable_key
        entry["stable_key_version"] = operation.stable_key_version
    return entry


def _operation_provenance(
    operation: _OfflockEntryOperation,
) -> dict[str, Any]:
    """JSON-safe durable identity for locally drainable provider facts."""

    return {
        "entry_key": operation.entry_key,
        "entry_type": operation.entry_type,
        "pane_uuid": operation.pane_uuid,
        "stable_key": operation.stable_key,
        "stable_key_version": operation.stable_key_version,
        "space_id": operation.space_id,
        "route_topic_id": operation.route_topic_id,
        "owner_generation": list(operation.owner_generation),
        "retired": operation.retired,
        "routable": operation.routable,
        "observed_fields": [
            [field, deepcopy(value)]
            for field, value in operation.observed_fields
        ],
        "message_id": operation.message_id,
        "plan_token": operation.plan_token,
        "revision": operation.revision,
    }


def _operation_from_provenance(
    value: Mapping[str, Any],
) -> _OfflockEntryOperation:
    owner_generation = value.get("owner_generation")
    if not isinstance(owner_generation, list):
        owner_generation = []
    observed_fields = value.get("observed_fields")
    return _OfflockEntryOperation(
        entry_key=str(value.get("entry_key") or ""),
        entry_type=str(value.get("entry_type") or ""),
        pane_uuid=str(value.get("pane_uuid") or ""),
        stable_key=str(value.get("stable_key") or ""),
        stable_key_version=int(value.get("stable_key_version") or 0),
        space_id=str(value.get("space_id") or ""),
        route_topic_id=str(value.get("route_topic_id") or ""),
        owner_generation=tuple(str(item) for item in owner_generation),
        retired=bool(value.get("retired")),
        routable=bool(value.get("routable")),
        observed_fields=tuple(
            (str(item[0]), deepcopy(item[1]))
            for item in (
                observed_fields
                if isinstance(observed_fields, list)
                else []
            )
            if isinstance(item, list) and len(item) == 2
        ),
        message_id=str(value.get("message_id") or ""),
        plan_token=str(value.get("plan_token") or ""),
        revision=str(value.get("revision") or ""),
    )


class _OfflockClient:
    """Release the state flock around one provider call and reload afterwards."""

    __slots__ = ("__provider", "__provider_kind", "__store")

    def __init__(
        self,
        client: Any,
        store: dict[str, Any],
        provider_kind: str = "telegram",
    ) -> None:
        object.__setattr__(self, "_OfflockClient__provider", client)
        object.__setattr__(self, "_OfflockClient__store", store)
        object.__setattr__(
            self, "_OfflockClient__provider_kind", str(provider_kind)
        )

    def __getattribute__(self, name: str) -> Any:
        if name in {
            "_client",
            "_store",
            "_provider",
            "_raw",
            "_invoke",
            "_invoke_read",
            "_OfflockClient__provider",
            "_OfflockClient__store",
        }:
            raise AttributeError(
                "raw provider state is private to the capability executor"
            )
        return object.__getattribute__(self, name)

    def _execute_entry(
        self,
        *_args: Any,
        **_kwargs: Any,
    ) -> Any:
        raise RuntimeError(
            "low-level off-lock executor is private to audited wrappers"
        )

    def _execute_exact(
        self,
        *_args: Any,
        **_kwargs: Any,
    ) -> Any:
        raise RuntimeError(
            "low-level off-lock executor is private to audited wrappers"
        )

    def __getattr__(self, name: str) -> Any:
        if name in {
            "_client",
            "_store",
            "_provider",
            "_raw",
            "_invoke",
            "_invoke_read",
            "_OfflockClient__provider",
            "_OfflockClient__store",
        }:
            raise AttributeError(
                "raw provider state is private to the capability executor"
            )
        callable_attribute, value, provider_kind = (
            _OFFLOCK_EXECUTOR.describe(self, name)
        )
        if not callable_attribute:
            return value
        if name == "with_token":
            return lambda *args, **kwargs: _OFFLOCK_EXECUTOR.with_token(
                self, *args, **kwargs
            )
        if name == "api":
            def checked_api(method: str, *args: Any, **kwargs: Any) -> Any:
                if not str(method).startswith("get"):
                    raise RuntimeError(
                        "off-lock mutating provider api call requires "
                        "_execute_entry_operation or "
                        "_execute_exact_provider_operation"
                    )
                return _OFFLOCK_EXECUTOR.read(
                    self, "api", method, *args, **kwargs
                )

            return checked_api
        if name not in _READ_ONLY_PROVIDER_METHODS.get(
            provider_kind, frozenset()
        ):
            def rejected(*_args: Any, **_kwargs: Any) -> Any:
                raise RuntimeError(
                    f"off-lock mutating provider method {name!r} requires "
                    "_execute_entry_operation or "
                    "_execute_exact_provider_operation"
                )

            return rejected

        def call(*args: Any, **kwargs: Any) -> Any:
            return _OFFLOCK_EXECUTOR.read(self, name, *args, **kwargs)

        return call


class _ExactOfflockClient:
    """Reason-scoped capability proxy for exact-id/lease workflows."""

    def __init__(self, client: _OfflockClient, reason: str) -> None:
        if not str(reason).strip():
            raise ValueError("exact off-lock client requires a written reason")
        object.__setattr__(self, "_ExactOfflockClient__guarded", client)
        object.__setattr__(self, "_ExactOfflockClient__reason", reason)

    def __getattr__(self, name: str) -> Any:
        guarded = object.__getattribute__(
            self, "_ExactOfflockClient__guarded"
        )
        reason = object.__getattribute__(
            self, "_ExactOfflockClient__reason"
        )
        if name in {"_client", "_provider", "_store"}:
            raise AttributeError(
                "raw provider state is private to the capability executor"
            )
        if name == "with_token":
            return lambda *args, **kwargs: _ExactOfflockClient(
                guarded.with_token(*args, **kwargs),
                reason,
            )
        provider_kind = _OFFLOCK_EXECUTOR.provider_kind(guarded)
        if name in _READ_ONLY_PROVIDER_METHODS.get(
            provider_kind, frozenset()
        ):
            return getattr(guarded, name)
        capability = f"{provider_kind}.{name}"
        return lambda *args, **kwargs: _OFFLOCK_EXECUTOR.mutation(
            guarded,
            _provider_mutation(
                capability,
                reason=f"{capability}: {reason}",
                args=args,
                kwargs=kwargs,
            ),
        )


def _build_offlock_executor() -> Any:
    """Create the only raw-provider authority.

    The raw provider and the single-consumption ledger live behind this
    closure.  There is deliberately no module-visible token that ordinary
    source code can borrow to turn an arbitrary callback into an executor.
    """

    consumed: weakref.WeakSet[_ProviderMutation] = weakref.WeakSet()

    def internals(client: _OfflockClient) -> tuple[Any, dict[str, Any], str]:
        return (
            object.__getattribute__(client, "_OfflockClient__provider"),
            object.__getattribute__(client, "_OfflockClient__store"),
            object.__getattribute__(
                client, "_OfflockClient__provider_kind"
            ),
        )

    def invoke_provider_mutation(
        provider: Any, mutation: _ProviderMutation
    ) -> Any:
        if mutation.api_token:
            provider = provider.with_token(mutation.api_token)
        args = mutation.args
        kwargs = dict(mutation.kwargs)
        method_name = _DIRECT_PROVIDER_CAPABILITIES.get(
            mutation.capability
        )
        if method_name is not None:
            method = getattr(provider, method_name, None)
            if method is None and method_name.endswith("_for_cleanup"):
                method = getattr(
                    provider, method_name.removesuffix("_for_cleanup")
                )
            return method(*args, **kwargs)
        if mutation.capability == "telegram.edit_feed_item":
            return edit_feed_item(provider, *args, **kwargs)
        if mutation.capability == "telegram.send_feed_item":
            return send_feed_item(provider, *args, **kwargs)
        if mutation.capability == "telegram.edit_turn_delivery_part":
            return edit_turn_delivery_part(provider, *args, **kwargs)
        if mutation.capability == "telegram.send_turn_delivery_part":
            return send_turn_delivery_part(provider, *args, **kwargs)
        if mutation.capability == "telegram.delete_turn_delivery_message":
            return delete_turn_delivery_message(provider, *args, **kwargs)
        if mutation.capability == "telegram.send_voice_batch":
            (
                chunks,
                turn_id,
                chat_id,
                thread_id,
                reply_to,
            ) = args
            ids: list[str] = []
            max_writes = max(
                0,
                int(
                    kwargs.get(
                        "max_physical_writes", len(chunks)
                    )
                ),
            )
            for index, chunk in enumerate(chunks):
                if index >= max_writes:
                    return {
                        "ok": False,
                        "status": "telegram_voice_batch_budget_exhausted",
                        "error": "Telegram physical-write budget exhausted",
                        "accepted_message_ids": ids,
                        "physical_writes": len(ids),
                    }
                try:
                    dest = (
                        speech.outbound_speech_dir(prune=(index == 0))
                        / (
                            "reply-"
                            + short_hash(
                                {"t": turn_id, "i": index, "h": chunk},
                                16,
                            )
                            + ".ogg"
                        )
                    )
                    if not speech.speech_request(
                        "tts", {"text": chunk, "dest": str(dest)}
                    ).get("ok"):
                        return {
                            "ok": False,
                            "status": (
                                "telegram_voice_batch_local_failure"
                            ),
                            "error": "voice synthesis failed",
                            "accepted_message_ids": ids,
                            "physical_writes": len(ids),
                        }
                    sent = provider.send_voice(
                        chat_id,
                        dest,
                        thread_id=thread_id,
                        reply_to_message_id=(
                            reply_to if index == 0 else None
                        ),
                        notify=False,
                    )
                    message_id = str(
                        sent.get("message_id") or ""
                    ).strip()
                    if (
                        sent.get("ok")
                        and message_id
                        and message_id != "0"
                    ):
                        ids.append(message_id)
                    elif sent.get("ok") is False:
                        return {
                            "ok": False,
                            "status": "telegram_voice_batch_rejected",
                            "error": compact_ws(
                                sent.get("error")
                                or "Telegram rejected voice note",
                                300,
                            ),
                            "accepted_message_ids": ids,
                            "physical_writes": len(ids) + 1,
                        }
                    else:
                        return {
                            "ok": False,
                            "status": (
                                "telegram_voice_batch_delivery_unknown"
                            ),
                            "error": (
                                "Telegram accepted voice note without "
                                "an attributable message id"
                            ),
                            "accepted_message_ids": ids,
                            "physical_writes": len(ids) + 1,
                        }
                except RateLimited as exc:
                    return {
                        "ok": False,
                        "status": "telegram_voice_batch_rate_limited",
                        "error": compact_ws(str(exc), 300),
                        "rate_limited": True,
                        "retry_after": exc.retry_after,
                        "method": str(exc.method or "sendVoice"),
                        "accepted_message_ids": ids,
                        "physical_writes": len(ids) + 1,
                    }
                except TelegramError as exc:
                    nested = _nested_rate_limit(
                        exc, expected_method="sendVoice"
                    )
                    if nested is not None:
                        return {
                            "ok": False,
                            "status": (
                                "telegram_voice_batch_rate_limited"
                            ),
                            "error": compact_ws(str(nested), 300),
                            "rate_limited": True,
                            "retry_after": nested.retry_after,
                            "method": str(
                                nested.method or "sendVoice"
                            ),
                            "accepted_message_ids": ids,
                            "physical_writes": len(ids) + 1,
                        }
                    return {
                        "ok": False,
                        "status": (
                            "telegram_voice_batch_delivery_unknown"
                        ),
                        "error": compact_ws(str(exc), 300),
                        "accepted_message_ids": ids,
                        "physical_writes": len(ids) + 1,
                    }
                except OSError as exc:
                    # Local filesystem failures occur before this chunk's
                    # provider send.  Stop the additive batch and retain any
                    # already-accepted prefix without replaying it.
                    return {
                        "ok": False,
                        "status": "telegram_voice_batch_local_failure",
                        "error": compact_ws(str(exc), 300),
                        "accepted_message_ids": ids,
                        "physical_writes": len(ids),
                    }
            return {
                "ok": True,
                "accepted_message_ids": ids,
                "physical_writes": len(ids),
            }
        raise AssertionError(
            "unhandled provider mutation capability: "
            f"{mutation.capability}"
        )

    def invoke_provider_mutation_with_backpressure(
        provider: Any, mutation: _ProviderMutation
    ) -> Any:
        """Consume one capability with bounded Telegram backpressure.

        A capability is single-use from the caller's perspective, but grants
        the executor one bounded replay for non-message Telegram operations.
        Message-producing calls are never replayed because their 429 response
        is not a receiver-side idempotency witness.
        """

        if mutation in consumed:
            raise RuntimeError(
                "provider mutation capability was already consumed"
            )
        consumed.add(mutation)
        if not mutation.capability.startswith("telegram."):
            return invoke_provider_mutation(provider, mutation)
        if (
            mutation.capability
            in _TELEGRAM_DEDICATED_BACKPRESSURE_CAPABILITIES
        ):
            # Topic lifecycle cleanup already persists retry_after per exact
            # target and skips it until due. Preserve that specialized
            # cooldown instead of layering an immediate retry over it.
            return invoke_provider_mutation(provider, mutation)

        retries = 0
        events: list[dict[str, Any]] = []
        default_method = _TELEGRAM_API_METHOD_BY_CAPABILITY.get(
            mutation.capability,
            mutation.capability.removeprefix("telegram."),
        )
        while True:
            rate_limited_result: Any = None
            try:
                result = invoke_provider_mutation(provider, mutation)
                rate_limit = _telegram_rate_limit_from_result(result)
                if rate_limit is None:
                    if events and isinstance(result, dict):
                        events[-1]["outcome"] = "recovered"
                        result = dict(result)
                        result[_PROVIDER_BACKPRESSURE_KEY] = events
                    return result
                retry_after, error, result_method = rate_limit
                method = result_method or default_method
                rate_limited_result = result
            except RateLimited as exc:
                retry_after = exc.retry_after
                error = compact_ws(str(exc), 300)
                method = str(exc.method or default_method)

            event = {
                "observed_at": time.time(),
                "method": method,
                "capability": mutation.capability,
                "retry_after": retry_after,
                "observed_wait_seconds": 0.0,
                "outcome": "",
            }
            events.append(event)
            accepted_message_ids = (
                tuple(
                    str(message_id)
                    for message_id in rate_limited_result.get(
                        "accepted_message_ids", ()
                    )
                    if str(message_id)
                )
                if isinstance(rate_limited_result, dict)
                else ()
            )
            if mutation.capability in _TELEGRAM_NO_RETRY_CAPABILITIES:
                event["outcome"] = "not_retried_message_send"
                return _telegram_rate_limit_failure(
                    status="telegram_rate_limited",
                    error=error,
                    method=method,
                    retry_after=retry_after,
                    retries=retries,
                    events=events,
                    accepted_message_ids=accepted_message_ids,
                )
            if retries >= _TELEGRAM_RATE_LIMIT_MAX_RETRIES:
                event["outcome"] = "retry_exhausted"
                return _telegram_rate_limit_failure(
                    status="telegram_rate_limit_exhausted",
                    error=error,
                    method=method,
                    retry_after=retry_after,
                    retries=retries,
                    events=events,
                )
            if retry_after > _TELEGRAM_RATE_LIMIT_MAX_WAIT_SECONDS:
                event["outcome"] = "wait_exceeds_ceiling"
                return _telegram_rate_limit_failure(
                    status="telegram_rate_limit_wait_exceeds_ceiling",
                    error=error,
                    method=method,
                    retry_after=retry_after,
                    retries=retries,
                    events=events,
                )
            started = time.monotonic()
            time.sleep(retry_after)
            event["observed_wait_seconds"] = max(
                0.0, time.monotonic() - started
            )
            event["outcome"] = "retrying"
            retries += 1

    def invoke_offlock(
        client: _OfflockClient, call: Callable[[Any], Any]
    ) -> Any:
        provider, store, _kind = internals(client)
        if not state.lock_actually_held():
            return call(provider)
        state.save_state(store)
        try:
            with state.released_lock():
                return call(provider)
        finally:
            # Replace nested state instead of reconciling it in place.  Any
            # entry reference retained across this provider window is now
            # detectably stale and cannot mutate the reloaded owner.
            fresh = state.load_state()
            store.clear()
            store.update(fresh)

    class Executor:
        @staticmethod
        def describe(
            client: _OfflockClient, name: str
        ) -> tuple[bool, Any, str]:
            provider, _store, provider_kind = internals(client)
            attribute = getattr(provider, name)
            return callable(attribute), (
                None if callable(attribute) else attribute
            ), provider_kind

        @staticmethod
        def store(client: _OfflockClient) -> dict[str, Any]:
            _provider, store, _kind = internals(client)
            return store

        @staticmethod
        def provider_kind(client: _OfflockClient) -> str:
            _provider, _store, provider_kind = internals(client)
            return provider_kind

        @staticmethod
        def with_token(
            client: _OfflockClient, *args: Any, **kwargs: Any
        ) -> Any:
            provider, store, provider_kind = internals(client)
            return _OfflockClient(
                provider.with_token(*args, **kwargs),
                store,
                provider_kind,
            )

        @staticmethod
        def read(
            client: _OfflockClient,
            method_name: str,
            *args: Any,
            **kwargs: Any,
        ) -> Any:
            _provider, _store, provider_kind = internals(client)
            allowed = method_name in _READ_ONLY_PROVIDER_METHODS.get(
                provider_kind, frozenset()
            ) and method_name != "with_token"
            if method_name == "api":
                allowed = bool(args and str(args[0]).startswith("get"))
            if not allowed:
                raise RuntimeError(
                    f"off-lock provider method {method_name!r} is not "
                    "classified read-only"
                )
            return invoke_offlock(
                client,
                lambda provider: getattr(provider, method_name)(
                    *args, **kwargs
                ),
            )

        @staticmethod
        def mutation(
            client: _OfflockClient, mutation: _ProviderMutation
        ) -> Any:
            result = invoke_offlock(
                client,
                lambda provider: invoke_provider_mutation_with_backpressure(
                    provider, mutation
                ),
            )
            _provider, store, _kind = internals(client)
            return _record_telegram_backpressure(store, result)

        @staticmethod
        def direct(
            provider: Any, mutation: _ProviderMutation
        ) -> Any:
            return invoke_provider_mutation_with_backpressure(
                provider, mutation
            )

    return Executor()


_OFFLOCK_EXECUTOR = _build_offlock_executor()


def _apply_provider_capability_state(
    store: dict[str, Any], result: Any
) -> None:
    """Apply provider facts returned by pure off-lock adapters after reload."""

    if not isinstance(result, dict):
        return
    format_updates = result.get(DELIVERY_FORMAT_STATE_UPDATE_KEY)
    if isinstance(format_updates, dict):
        format_updates = [format_updates]
    if isinstance(format_updates, list):
        telegram = _telegram_state(store)
        fallback_state = telegram.setdefault(
            "delivery_format_fallbacks", {}
        )
        if not isinstance(fallback_state, dict):
            fallback_state = {}
            telegram["delivery_format_fallbacks"] = fallback_state
        events = fallback_state.setdefault("events", [])
        if not isinstance(events, list):
            events = []
        try:
            sequence = int(fallback_state.get("sequence") or 0)
        except (TypeError, ValueError):
            sequence = 0
        for raw_update in format_updates:
            if not isinstance(raw_update, dict):
                continue
            sequence += 1
            raw_rejections = raw_update.get("rejections")
            rejections = []
            if isinstance(raw_rejections, list):
                rejections = [
                    {
                        "format": compact_ws(rejection.get("format"), 40),
                        "error": compact_ws(rejection.get("error"), 300),
                    }
                    for rejection in raw_rejections
                    if isinstance(rejection, dict)
                ]
            event = {
                "sequence": sequence,
                "observed_at": time.time(),
                "method": compact_ws(raw_update.get("method"), 80),
                "requested_format": compact_ws(
                    raw_update.get("requested_format"), 40
                ),
                "delivered_format": compact_ws(
                    raw_update.get("delivered_format"), 40
                ),
                "rejections": rejections,
            }
            events.append(event)
            fallback_state["last"] = event
        fallback_state["sequence"] = sequence
        fallback_state["events"] = events[
            -_DELIVERY_FORMAT_FALLBACK_LIMIT:
        ]

    update = result.get(RICH_STATE_UPDATE_KEY)
    if not isinstance(update, dict):
        return
    transition = str(update.get("transition") or "")
    reason = compact_ws(update.get("reason"), 300)
    telegram = _telegram_state(store)
    rich = telegram.setdefault("rich_messages", {})
    if not isinstance(rich, dict):
        rich = {}
        telegram["rich_messages"] = rich
    if transition == "supported":
        rich["supported"] = "yes"
        rich.pop("disabled_reason", None)
        rich.pop("bad_request_streak", None)
        return
    if transition == "disabled":
        rich["supported"] = "no"
        rich["disabled_reason"] = reason
        rich["disabled_render_version"] = RICH_RENDER_VERSION
        return
    if transition != "bad_request":
        return
    try:
        streak = int(rich.get("bad_request_streak") or 0)
    except (TypeError, ValueError):
        streak = 0
    streak += 1
    rich["bad_request_streak"] = streak
    if streak >= RICH_BAD_REQUEST_LIMIT:
        rich["supported"] = "no"
        rich["disabled_reason"] = f"repeated bad_request: {reason}"
        rich["disabled_render_version"] = RICH_RENDER_VERSION


def _exact_provider_client(client: Any, *, reason: str) -> Any:
    """Deliberate capability for a documented non-entry provider workflow."""

    if not str(reason).strip():
        raise ValueError("exact provider client requires a written reason")
    if isinstance(client, _OfflockClient):
        return _ExactOfflockClient(client, reason)
    return client


def _execute_entry_operation(
    store: dict[str, Any],
    client: Any,
    operation: _OfflockEntryOperation,
    mutation: _ProviderMutation,
    *,
    acceptance_checkpoint: (
        Callable[[Any, _OfflockEntryOperation], None] | None
    ) = None,
) -> _OfflockEntryExecution:
    """Execute an entry mutation and resolve its immutable request owner.

    This is the sole entry-targeted mutating provider surface. Callers receive
    the provider result and disposition together, so post-call code never needs
    to recover routing provenance from a reloaded entry.
    """

    if not isinstance(operation, _OfflockEntryOperation):
        raise TypeError("entry mutation requires _OfflockEntryOperation")
    if not isinstance(mutation, _ProviderMutation):
        raise TypeError("entry mutation requires one provider capability")
    if isinstance(client, _OfflockClient):
        result = _OFFLOCK_EXECUTOR.mutation(client, mutation)
    elif state.lock_actually_held():
        state.save_state(store)
        try:
            with state.released_lock():
                result = _OFFLOCK_EXECUTOR.direct(client, mutation)
        finally:
            fresh = state.load_state()
            store.clear()
            store.update(fresh)
    else:
        result = _OFFLOCK_EXECUTOR.direct(client, mutation)
    result = _record_telegram_backpressure(store, result)
    _apply_provider_capability_state(store, result)
    if acceptance_checkpoint is not None:
        acceptance_checkpoint(result, operation)
    return _OfflockEntryExecution(
        result=result,
        resolution=_compare_and_apply_entry_operation(store, operation),
    )


def _delivery_write_budget(runtime: SyncRuntime) -> _DeliveryWriteBudget:
    budget = runtime.delivery_write_budget
    if budget is None:
        budget = _DeliveryWriteBudget(max(0, int(runtime.max_sends)))
        runtime.delivery_write_budget = budget
    return budget


def _execute_accounted_delivery_write(
    store: dict[str, Any],
    runtime: SyncRuntime,
    operation: _OfflockEntryOperation,
    mutation: _ProviderMutation,
    *,
    acceptance_checkpoint: (
        Callable[[Any, _OfflockEntryOperation], None] | None
    ) = None,
) -> _OfflockEntryExecution:
    """Execute and charge one owner-visible Telegram delivery capability.

    Adapter results report the whole rich/HTML/plain ladder as
    ``physical_writes``.  The wrapper charges that fact whether the logical
    delivery succeeded or failed. Every delivery caller must pass a positive
    allowance no larger than the wrapper's ``remaining`` value as
    ``max_physical_writes``. A caller may impose a stricter local cap (the fold
    sweep does); this runtime check still makes a missing or excessive
    allowance loud even if a static guard is accidentally weakened.
    """

    budget = _delivery_write_budget(runtime)
    kwargs = dict(mutation.kwargs)
    allowance = kwargs.get("max_physical_writes")
    if (
        isinstance(allowance, bool)
        or not isinstance(allowance, int)
        or allowance <= 0
        or allowance > budget.remaining
    ):
        raise RuntimeError(
            "delivery mutation must carry a positive physical-write allowance "
            "within the remaining pass budget"
        )
    if budget.remaining <= 0:
        raise RuntimeError(
            "delivery mutation attempted after physical-write budget exhaustion"
        )
    try:
        execution = _execute_entry_operation(
            store,
            runtime.telegram,
            operation,
            mutation,
            acceptance_checkpoint=acceptance_checkpoint,
        )
    except Exception:
        # The adapter contract converts expected provider outcomes into a
        # structured result. An escaping exception is ambiguous about whether
        # the provider saw the request, so conservatively charge one attempt
        # before surfacing it loudly.
        budget.spent += 1
        raise
    budget.charge(execution.result, default=1)
    return execution


def _execute_exact_provider_operation(
    client: Any,
    *,
    mutation: _ProviderMutation,
    store: dict[str, Any] | None = None,
) -> Any:
    """Execute an exact-provider-id mutation independent of pane ownership."""

    if not isinstance(mutation, _ProviderMutation):
        raise TypeError("exact mutation requires one provider capability")
    if isinstance(client, _OfflockClient):
        result = _OFFLOCK_EXECUTOR.mutation(client, mutation)
        if store is not None:
            _apply_provider_capability_state(store, result)
        return result
    if store is not None and state.lock_actually_held():
        state.save_state(store)
        try:
            with state.released_lock():
                result = _OFFLOCK_EXECUTOR.direct(client, mutation)
        finally:
            fresh = state.load_state()
            store.clear()
            store.update(fresh)
        result = _record_telegram_backpressure(store, result)
        _apply_provider_capability_state(store, result)
        return result
    result = _OFFLOCK_EXECUTOR.direct(client, mutation)
    if store is not None:
        result = _record_telegram_backpressure(store, result)
        _apply_provider_capability_state(store, result)
    return result


def _offlock_runtime(
    store: dict[str, Any], runtime: SyncRuntime
) -> SyncRuntime:
    """Release the connector-state flock around provider calls when held."""

    if not state.lock_held() or runtime.dry_run:
        return runtime
    return SyncRuntime(
        _OfflockClient(runtime.tendwire, store, "tendwire"),
        _OfflockClient(runtime.telegram, store, "telegram"),
        dry_run=runtime.dry_run,
        with_outbox=runtime.with_outbox,
        max_sends=runtime.max_sends,
        checkpoint=runtime.checkpoint,
        after_provider_accept=runtime.after_provider_accept,
        delivery_write_budget=runtime.delivery_write_budget,
    )


def _workers(snapshot: dict[str, Any]) -> list[dict[str, Any]]:
    return [item for item in snapshot.get("workers", []) if isinstance(item, dict)]


def _spaces(snapshot: dict[str, Any]) -> list[dict[str, Any]]:
    return [item for item in snapshot.get("spaces", []) if isinstance(item, dict)]


def _turns(payload: dict[str, Any]) -> list[dict[str, Any]]:
    return [item for item in payload.get("turns", []) if isinstance(item, dict)]


def _pending(payload: dict[str, Any]) -> list[dict[str, Any]]:
    items = payload.get("pending_interactions", payload.get("pending", []))
    return [item for item in items if isinstance(item, dict)]


def _normalize_voice_mode(value: Any) -> str:
    clean = str(value or "").strip().lower().replace("-", "_")
    if clean in {"per_agent", "peragent", "agent", "agents", "voice"}:
        return "per_agent"
    return "shared"


def _default_voice_mode() -> str:
    return "per_agent" if config.managed_bots_enabled() else "shared"


def _entry_voice_mode(entry: dict[str, Any] | None) -> str:
    if isinstance(entry, dict) and str(entry.get("voice_mode") or "").strip():
        return _normalize_voice_mode(entry.get("voice_mode"))
    return _default_voice_mode()


def _space_voice_mode(store: dict[str, Any], space_id: str | None) -> str:
    _space_key, space_entry = state.find_space_entry_by_id(store, str(space_id or ""))
    return _entry_voice_mode(space_entry)


def _stamp_managed_voice(entry: dict[str, Any], voice_mode: str) -> None:
    mode = _normalize_voice_mode(voice_mode)
    entry["voice_mode"] = mode
    entry["managed_voice_active"] = mode == "per_agent"


def _meta_raw_status(worker: dict[str, Any]) -> str:
    meta = worker.get("meta") if isinstance(worker.get("meta"), dict) else {}
    return compact_ws(meta.get("raw_status"), 80)


def _source_status(value: Any) -> str:
    raw = str(value or "").strip().lower().replace("-", "_")
    if raw == "active":
        return "idle"
    return normalized_status(value)


def _dominant_status(*values: Any) -> str:
    statuses = [normalized_status(value) for value in values if str(value or "").strip()]
    for wanted in ("failed", "attention", "working"):
        if wanted in statuses:
            return wanted
    return statuses[0] if statuses else ""


def _turn_activity_status(item: dict[str, Any]) -> str:
    if bool(item.get("complete")) or bool(item.get("assistant_final_text")):
        return "idle"
    if item.get("complete") is False or item.get("has_open_turn") is True or bool(item.get("assistant_stream_text")):
        return "working"
    return ""


def _turn_stable_identity(
    item: dict[str, Any],
) -> tuple[str, int] | None:
    stable_key = item.get("stable_key")
    stable_key_version = item.get("stable_key_version")
    if stable_key is None and stable_key_version is None:
        meta = item.get("meta")
        if isinstance(meta, dict):
            stable_key = meta.get("stable_key")
            stable_key_version = meta.get("stable_key_version")
    if state.valid_stable_worker_key_pair(
        stable_key, stable_key_version
    ):
        return str(stable_key), int(stable_key_version)
    stable_key = item.get(_TURN_STABLE_KEY_KEY)
    stable_key_version = item.get(_TURN_STABLE_KEY_VERSION_KEY)
    if state.valid_stable_worker_key_pair(
        stable_key, stable_key_version
    ):
        return str(stable_key), int(stable_key_version)
    return None


def _annotate_turn_generation_identities(
    turns_payload: dict[str, Any],
    workers: list[dict[str, Any]],
) -> None:
    """Attach an internal stable identity to legacy rows from the snapshot."""
    identities_by_worker: dict[str, set[tuple[str, int]]] = {}
    spaces_by_worker: dict[str, set[str]] = {}
    for worker in workers:
        worker_id = compact_ws(worker.get("id"), 160)
        identity = state.worker_stable_identity(worker)
        if worker_id and identity is not None:
            identities_by_worker.setdefault(worker_id, set()).add(identity)
            space_id = compact_ws(worker.get("space_id"), 160)
            if space_id:
                spaces_by_worker.setdefault(worker_id, set()).add(space_id)
    for item in _turns(turns_payload):
        if _turn_stable_identity(item) is not None:
            continue
        worker_id = compact_ws(item.get("worker_id"), 160)
        identities = identities_by_worker.get(worker_id, set())
        if len(identities) != 1:
            continue
        turn_space_id = compact_ws(item.get("space_id"), 160)
        worker_spaces = spaces_by_worker.get(worker_id, set())
        if (
            turn_space_id
            and worker_spaces
            and turn_space_id not in worker_spaces
        ):
            continue
        identity = next(iter(identities))
        item[_TURN_STABLE_KEY_KEY] = identity[0]
        item[_TURN_STABLE_KEY_VERSION_KEY] = identity[1]


def _turn_recency(item: Mapping[str, Any]) -> str:
    """Return Tendwire's explicit, canonical per-turn recency coordinate."""
    value = item.get("updated_at")
    return str(value) if isinstance(value, str) and value else ""


def _select_stable_generation(
    workers: list[dict[str, Any]],
    turns: list[dict[str, Any]],
    *,
    incumbent_worker_id: str,
    incumbent_last_turn_id: str,
) -> tuple[dict[str, Any] | None, str, str]:
    """Prefer the incumbent unless fresh turn evidence proves a handoff."""
    workers_by_id = {
        compact_ws(worker.get("id"), 160): worker for worker in workers
    }
    workers_by_id.pop("", None)
    latest_rows: dict[str, dict[str, Any]] = {}
    for item in turns:
        worker_id = compact_ws(item.get("worker_id"), 160)
        if worker_id not in workers_by_id or _turn_has_content_outcome(item):
            continue
        current = latest_rows.get(worker_id)
        if current is None or (
            _turn_recency(item),
            _turn_id(item) == incumbent_last_turn_id,
        ) > (
            _turn_recency(current),
            _turn_id(current) == incumbent_last_turn_id,
        ):
            latest_rows[worker_id] = item

    open_turn_ids = {
        worker_id
        for worker_id, item in latest_rows.items()
        if _turn_activity_status(item) == "working"
    }
    if len(open_turn_ids) > 1:
        return None, "conflicting_turn_activity", ""
    if len(open_turn_ids) == 1:
        wanted = next(iter(open_turn_ids))
        return (
            workers_by_id[wanted],
            "freshest_turn_activity",
            _turn_id(latest_rows[wanted]),
        )

    if latest_rows:
        highest = max(_turn_recency(item) for item in latest_rows.values())
        freshest_ids = {
            worker_id
            for worker_id, item in latest_rows.items()
            if _turn_recency(item) == highest
        }
        if incumbent_worker_id in freshest_ids:
            wanted = incumbent_worker_id
        elif len(freshest_ids) == 1:
            wanted = next(iter(freshest_ids))
        else:
            return None, "conflicting_turn_activity", ""
        return (
            workers_by_id[wanted],
            "freshest_turn_activity",
            _turn_id(latest_rows[wanted]),
        )

    incumbent = workers_by_id.get(incumbent_worker_id)
    if incumbent is not None:
        return incumbent, "incumbent_quiet", ""
    return None, "no_turn_evidence", ""


def _resolve_stable_worker_generations(
    store: dict[str, Any],
    snapshot: dict[str, Any],
    turns_payload: dict[str, Any],
    *,
    observed_at: float,
) -> tuple[dict[str, Any], list[dict[str, str]], bool]:
    """Reduce restart generations to one stable-key observation per pane."""
    workers = _workers(snapshot)
    _annotate_turn_generation_identities(turns_payload, workers)
    live_by_stable_key: dict[str, list[dict[str, Any]]] = {}
    for worker in workers:
        identity = state.worker_stable_identity(worker)
        if identity is None or not _worker_is_open(worker):
            continue
        live_by_stable_key.setdefault(identity[0], []).append(worker)

    excluded_worker_refs: set[int] = set()
    resolutions: list[dict[str, str]] = []
    changed = False
    entries = state.source_worker_entries(store)
    for stable_key, generations in live_by_stable_key.items():
        claims = [
            (entry_key, entry)
            for entry_key, entry in entries.items()
            if not state.entry_is_retired(entry)
            and state.entry_stable_identity(entry)
            == (stable_key, state.STABLE_WORKER_KEY_VERSION)
        ]
        topic_claims = [
            pair for pair in claims if str(pair[1].get("topic_id") or "")
        ]
        owners = topic_claims if len(topic_claims) == 1 else claims
        if len(owners) != 1:
            continue
        entry_key, entry = owners[0]
        matching_turns = [
            item
            for item in _turns(turns_payload)
            if _turn_stable_identity(item)
            == (stable_key, state.STABLE_WORKER_KEY_VERSION)
        ]
        previous_id = _entry_worker_id(entry)
        if len(generations) == 1:
            selected = generations[0]
            reason = "stable_key_cache_refresh"
            evidence_turn_id = ""
        else:
            selected, reason, evidence_turn_id = _select_stable_generation(
                generations,
                matching_turns,
                incumbent_worker_id=previous_id,
                incumbent_last_turn_id=str(entry.get("last_turn_id") or ""),
            )
        worker_ids = [
            compact_ws(worker.get("id"), 160) for worker in generations
        ]
        if selected is None:
            if reason == "conflicting_turn_activity":
                changed = (
                    state.mark_worker_generation_ambiguous(
                        store,
                        entry_key,
                        worker_ids=worker_ids,
                        observed_at=observed_at,
                    )
                    or changed
                )
            else:
                # A replacement snapshot row without a turn is not evidence
                # of ownership. Hide it from source upsert and preserve the
                # incumbent route unchanged.
                excluded_worker_refs.update(id(worker) for worker in generations)
            continue
        if reason == "freshest_turn_activity":
            changed = (
                state.clear_worker_generation_ambiguity(store, entry_key)
                or changed
            )
        selected_id = compact_ws(selected.get("id"), 160)
        excluded_worker_refs.update(
            id(worker) for worker in generations if worker is not selected
        )
        resolutions.append(
            {
                "stable_key": stable_key,
                "entry_key": entry_key,
                "from_worker_id": previous_id,
                "to_worker_id": selected_id,
                "reason": reason,
                "evidence_turn_id": evidence_turn_id,
            }
        )

    if not excluded_worker_refs:
        return snapshot, resolutions, changed
    resolved_snapshot = dict(snapshot)
    resolved_snapshot["workers"] = [
        worker for worker in workers if id(worker) not in excluded_worker_refs
    ]
    return resolved_snapshot, resolutions, changed


def _turn_activity_statuses(payload: dict[str, Any], live_worker_ids: set[str] | None = None) -> tuple[dict[str, str], dict[str, str]]:
    by_worker: dict[str, str] = {}
    by_space: dict[str, str] = {}
    for item in _turns(payload):
        status = _turn_activity_status(item)
        if not status:
            continue
        worker_id = compact_ws(item.get("worker_id"), 160)
        space_id = compact_ws(item.get("space_id"), 160)
        if live_worker_ids is not None and worker_id and worker_id not in live_worker_ids:
            # Stale turn rows from retired worker ids must not pin a live
            # space/worker status (e.g. an abandoned open turn reading as
            # "working" forever).
            continue
        if worker_id and worker_id not in by_worker:
            by_worker[worker_id] = status
        if space_id and space_id not in by_space:
            by_space[space_id] = status
    return by_worker, by_space


def _effective_worker_status(worker: dict[str, Any], turn_status_by_worker: dict[str, str]) -> str:
    raw_status = normalized_status(worker.get("status"))
    if raw_status in {"closed", "failed", "attention"}:
        return raw_status
    public_raw_status = normalized_status(_meta_raw_status(worker))
    if public_raw_status in {"failed", "attention", "working"}:
        return public_raw_status
    worker_id = compact_ws(worker.get("id"), 160)
    if worker_id and turn_status_by_worker.get(worker_id):
        return turn_status_by_worker[worker_id]
    return _source_status(worker.get("status"))


def _worker_is_open(worker: dict[str, Any]) -> bool:
    return normalized_status(worker.get("status")) not in {"closed", "failed"}


def _worker_status_is_finished(value: Any) -> bool:
    status = str(value or "").strip().lower().replace("-", "_")
    return status in {"closed", "complete", "completed", "done", "failed", "failure"}


def _entry_status_is_finished(entry: dict[str, Any]) -> bool:
    return _worker_status_is_finished(entry.get("tendwire_raw_status") or entry.get("status"))


def _entry_is_reapable(entry: dict[str, Any]) -> bool:
    """Reap-eligibility for the worker-topic reaper: ONLY a genuinely closed/failed entry.

    This is the strict inverse of _worker_is_open on the persisted entry fields — deliberately NOT
    _entry_status_is_finished, which also counts 'done'/'complete' as finished (the done-council
    cleanup relies on that, so it is left untouched). Here 'done'/'idle'/'working' are all LIVE:
    normalized_status('done') == 'idle', an idle agent whose terminal is still open. herdr reports
    agent_status='done' for a pane that merely finished its last turn, so such a pane dropping out of
    a snapshot for a reconcile-lag blip must NEVER be reaped (it would take the whole scrollback).
    Only 'closed'/'failed' — a truly gone pane — is reapable."""
    return normalized_status(entry.get("tendwire_raw_status") or entry.get("status")) in {"closed", "failed"}


def _entry_is_council_topic(entry: dict[str, Any]) -> bool:
    """Ephemeral gitmoot delegation/council entries (gm-local-as workers, "gitmoot · local-as"
    delegation spaces, "Council · …" topics). The markers are deliberately PRECISE: a bare "gitmoot"
    substring would also match regular panes whose topic is named after the /root/gitmoot project
    dir (labels/cwd naming), and done-council cleanup would then delete a normal pane's topic every
    time it finished a task (live incident: "Gitmoot2"/"gitmoot 2" churned create/delete)."""
    material = " ".join(
        str(entry.get(key) or "").lower()
        for key in ("topic_name", "worker_name", "agent", "space_topic_name")
    )
    return any(marker in material for marker in ("council", "gm-local", "gm_", "gitmoot \u00b7"))


def _should_delete_done_council_topic(entry: dict[str, Any]) -> bool:
    return config.delete_done_council_topics() and _entry_is_council_topic(entry) and _entry_status_is_finished(entry)


def _topic_missing(error: Any) -> bool:
    text = str(error or "").lower()
    return "topic_id_invalid" in text or "message thread not found" in text


def _topic_not_modified(error: Any) -> bool:
    text = str(error or "").lower()
    return "topic_not_modified" in text or "not modified" in text


def _message_missing(error: Any) -> bool:
    text = str(error or "").lower()
    return "message to edit not found" in text or "message not found" in text


def _space_is_open(space: dict[str, Any]) -> bool:
    return normalized_status(space.get("status")) not in {"closed", "failed"}


def _select_space_worker(workers: list[dict[str, Any]], turn_status_by_worker: dict[str, str] | None = None) -> dict[str, Any]:
    turn_status_by_worker = turn_status_by_worker or {}
    for wanted in ("working", "attention", "idle"):
        matches = [worker for worker in workers if _effective_worker_status(worker, turn_status_by_worker) == wanted]
        if matches:
            return max(matches, key=lambda worker: str(worker.get("last_seen_at") or ""))
    return max(workers, key=lambda worker: str(worker.get("last_seen_at") or "")) if workers else {}


def _complete_topic_recovery(entry: dict[str, Any], topic_id: str) -> None:
    recovery = entry.pop("topic_recovery_pending", None)
    if not isinstance(recovery, dict):
        return
    recovery = dict(recovery)
    recovery["replacement_topic_id"] = topic_id
    recovery["recovered_at"] = time.time()
    entry["last_topic_recovery"] = recovery


def _delivery_entry(
    store: dict[str, Any],
    space_entry: dict[str, Any],
    worker_entry: dict[str, Any] | None = None,
) -> dict[str, Any]:
    worker_entry = worker_entry or {}
    entry = worker_entry
    worker_name = compact_ws(worker_entry.get("worker_name") or worker_entry.get("agent"), 80)
    space_name = compact_ws(space_entry.get("topic_name"), 80)
    if worker_name and space_name:
        entry["topic_name"] = f"{space_name} · {worker_name}"
    elif space_name:
        entry["topic_name"] = space_name
    state.discard_tombstoned_topic_binding(store, space_entry)
    space_topic_id = str(space_entry.get("topic_id") or "")
    if space_topic_id:
        entry["topic_id"] = space_topic_id
        _complete_topic_recovery(entry, space_topic_id)
    else:
        entry.pop("topic_id", None)
    entry["tendwire_space_id"] = space_entry.get("tendwire_space_id") or worker_entry.get("tendwire_space_id")
    entry["space_topic_name"] = space_name
    entry["tendwire_worker_id"] = worker_entry.get("tendwire_worker_id") or space_entry.get("active_worker_id")
    entry["tendwire_fingerprint"] = worker_entry.get("tendwire_fingerprint") or space_entry.get("active_worker_fingerprint")
    entry["agent"] = worker_entry.get("agent") or entry.get("agent")
    entry["managed_bot_kind"] = worker_entry.get("managed_bot_kind") or managed_bot_kind_for_entry(worker_entry)
    voice_mode = _entry_voice_mode(space_entry)
    entry["voice_mode"] = voice_mode
    entry["managed_voice_active"] = voice_mode == "per_agent"
    return entry


def _entry_worker_id(entry: dict[str, Any]) -> str:
    return compact_ws(entry.get("tendwire_worker_id") or entry.get("worker_id") or entry.get("active_worker_id"), 160)


def _entry_space_id(entry: dict[str, Any]) -> str:
    return compact_ws(entry.get("tendwire_space_id") or entry.get("space_id"), 160)


def _source_space_topic_ids(store: dict[str, Any]) -> dict[str, str]:
    topic_ids: dict[str, str] = {}
    for entry in state.source_space_entries(store).values():
        space_id = _entry_space_id(entry)
        topic_id = compact_ws(entry.get("topic_id"), 80)
        if space_id and topic_id:
            topic_ids[space_id] = topic_id
    return topic_ids


def _worker_entry_for_turn(store: dict[str, Any], worker_id: str, space_id: str) -> tuple[str | None, dict[str, Any] | None]:
    candidates = [
        (key, entry)
        for key, entry in state.source_worker_entries(store).items()
        if _entry_worker_id(entry) == worker_id
        and state.worker_entry_is_uniquely_routable(store, key, entry)
    ]
    if space_id:
        candidates = [(key, entry) for key, entry in candidates if _entry_space_id(entry) == space_id]
    return candidates[0] if len(candidates) == 1 else (None, None)


def _telegram_state(store: dict[str, Any]) -> dict[str, Any]:
    telegram = store.get("telegram")
    if not isinstance(telegram, dict):
        telegram = {}
        store["telegram"] = telegram
    return telegram


def _accepted_created_topics(
    store: dict[str, Any], *, create: bool
) -> dict[str, dict[str, Any]]:
    telegram = _telegram_state(store)
    records = telegram.get("accepted_created_topics")
    if not isinstance(records, dict):
        if not create:
            return {}
        records = {}
        telegram["accepted_created_topics"] = records
    return records


def _ambiguous_created_topic_for_entry(
    store: dict[str, Any], entry: dict[str, Any]
) -> dict[str, Any] | None:
    entry_key, entry_type = _entry_operation_key(store, entry)
    identity = state.entry_stable_identity(entry)
    space_id = _entry_space_id(entry)
    for record in _accepted_created_topics(
        store, create=False
    ).values():
        if (
            not isinstance(record, dict)
            or record.get("kind") != "ambiguous_created_topic"
        ):
            continue
        owner = record.get("owner")
        if not isinstance(owner, dict) or owner.get("entry_type") != entry_type:
            continue
        if entry_type == "worker" and identity is not None:
            if (
                owner.get("stable_key"),
                owner.get("stable_key_version"),
            ) == identity:
                return record
        elif entry_type == "space" and space_id:
            if str(owner.get("space_id") or "") == space_id:
                return record
        elif entry_key and owner.get("entry_key") == entry_key:
            return record
    return None


def _stamp_ambiguous_topic_create(
    entry: dict[str, Any], record: Mapping[str, Any]
) -> None:
    entry["binding_state"] = "quarantined:ambiguous_topic_create"
    entry["ambiguous_topic_create_name"] = compact_ws(
        record.get("topic_name"), 120
    )
    entry["ambiguous_topic_create_at_unix"] = float(
        record.get("observed_at_unix") or time.time()
    )
    entry["last_topic_error"] = compact_ws(record.get("error"), 240)
    entry.pop("binding_topic_id", None)


def _checkpoint_accepted_created_topic(
    store: dict[str, Any],
    operation: _OfflockEntryOperation,
    result: Any,
    *,
    topic_name: str,
) -> str:
    """Record a non-idempotent accepted topic before content delivery."""

    if not isinstance(result, Mapping):
        return ""
    topic_id = str(result.get("topic_id") or "")
    if result.get("ok") is True and topic_id:
        record = {
            "kind": "created_topic",
            "topic_id": topic_id,
            "topic_name": compact_ws(topic_name, 120),
            "owner": _operation_provenance(operation),
        }
    elif result.get("ambiguous_acceptance"):
        record = {
            "kind": "ambiguous_created_topic",
            "topic_name": compact_ws(topic_name, 120),
            "error": compact_ws(result.get("error"), 240),
            "observed_at_unix": time.time(),
            "owner": _operation_provenance(operation),
        }
        current = _resolve_operation_entry(store, operation)
        if current is not None and not current.get("topic_id"):
            _stamp_ambiguous_topic_create(current, record)
    else:
        return ""
    receipt_id = short_hash(record, 32)
    _accepted_created_topics(store, create=True).setdefault(
        receipt_id, record
    )
    return receipt_id


def _complete_accepted_created_topic(
    store: dict[str, Any], receipt_id: str
) -> None:
    if receipt_id:
        _accepted_created_topics(store, create=False).pop(
            receipt_id, None
        )


def _checkpoint_topic_lifecycle(
    store: dict[str, Any], runtime: SyncRuntime
) -> None:
    if runtime.checkpoint is not None:
        runtime.checkpoint()
    elif state.lock_actually_held():
        state.save_state(store)


def _recover_accepted_created_topics(
    store: dict[str, Any], runtime: SyncRuntime
) -> int:
    """Adopt or quarantine topic creates from the lifecycle mirror."""

    recovered = 0
    records = list(
        _accepted_created_topics(store, create=False).items()
    )
    for receipt_id, record in records:
        if not isinstance(record, dict):
            _complete_accepted_created_topic(store, receipt_id)
            recovered += 1
            continue
        topic_id = str(record.get("topic_id") or "")
        owner = record.get("owner")
        operation = (
            _operation_from_provenance(owner)
            if isinstance(owner, dict)
            else None
        )
        resolution = (
            _compare_and_apply_entry_operation(store, operation)
            if operation is not None
            else _OfflockEntryResolution(_OFFLOCK_ABANDON)
        )
        entry = resolution.entry
        if record.get("kind") == "ambiguous_created_topic":
            current = (
                _resolve_operation_entry(store, operation)
                if operation is not None
                else None
            )
            if current is None or current.get("topic_id"):
                continue
            _stamp_ambiguous_topic_create(current, record)
            # This receipt is the independent create guard. Mutable routing
            # state may later overwrite binding_state, so retain the durable
            # owner/name record until an explicit audited adopt/reset clears
            # the ambiguity.
            continue
        elif (
            topic_id
            and resolution.disposition == _OFFLOCK_APPLY
            and entry is not None
            and not entry.get("topic_id")
        ):
            entry["topic_id"] = topic_id
            _complete_topic_recovery(entry, topic_id)
        elif topic_id:
            state.record_orphaned_created_topic(
                store,
                {
                    "topic_id": topic_id,
                    "topic_name": compact_ws(
                        record.get("topic_name"), 120
                    ),
                    "owner": deepcopy(owner)
                    if isinstance(owner, dict)
                    else {},
                    "reason": (
                        "accepted_create_recovery_owner_changed"
                    ),
                },
            )
        _complete_accepted_created_topic(store, receipt_id)
        recovered += 1
    if recovered:
        if runtime.checkpoint is not None:
            runtime.checkpoint()
        elif state.lock_actually_held():
            state.save_state(store)
    return recovered


def _set_pending_turn_plan(
    entry: dict[str, Any],
    *,
    turn_id: str,
    revision: str,
    plan_token: str,
    part_count: int,
    job_count: int,
    now: float | None = None,
) -> None:
    """Stamp one route-local plan and its non-resetting starvation clock."""

    prior_token = str(entry.get("pending_plan_token") or "")
    entry["pending_turn_id"] = turn_id
    entry["pending_content_revision"] = revision
    entry["pending_plan_token"] = plan_token
    entry["pending_turn_part_count"] = int(part_count)
    entry["pending_turn_job_count"] = int(job_count)
    if (
        prior_token != plan_token
        or not isinstance(
            entry.get("pending_turn_started_at"), (int, float)
        )
        or isinstance(entry.get("pending_turn_started_at"), bool)
    ):
        entry["pending_turn_started_at"] = (
            time.time() if now is None else float(now)
        )


def _delivery_bot(
    store: dict[str, Any], entry: dict[str, Any]
) -> tuple[str | None, str]:
    kind = managed_bot_kind_for_entry(entry)
    if not kind or entry.get("managed_voice_active") is False:
        return None, MANAGER_BOT_KIND
    token = _legacy_presenter_bot_token(store, kind)
    return (token, kind) if token else (None, MANAGER_BOT_KIND)


def _legacy_presenter_bot_token(store: dict[str, Any], bot_kind: str) -> str:
    """Read a managed delivery token only for the temporary old presenter."""

    if not config.managed_bots_enabled():
        return ""
    telegram = _telegram_state(store)
    bots = telegram.get("managed_bots")
    record = bots.get(bot_kind) if isinstance(bots, dict) else None
    if isinstance(record, dict) and record.get("enabled") is False:
        return ""
    configured = config.managed_bot_token(bot_kind)
    return configured or (str(record.get("token") or "").strip() if isinstance(record, dict) else "")


def _record_delivery_error(
    entry: dict[str, Any], result: dict[str, Any], bot_kind: str
) -> None:
    error = compact_ws(
        result.get("error")
        or result.get("kind")
        or "Telegram delivery failed",
        240,
    )
    entry["last_delivery_error"] = error
    if bot_kind != MANAGER_BOT_KIND:
        entry["last_managed_bot_kind"] = bot_kind
        entry["last_managed_bot_error"] = error


def _repair_provider_gone_topic(
    store: dict[str, Any],
    entry: dict[str, Any] | None,
    result: dict[str, Any],
    *,
    topic_id: str,
) -> bool:
    """Repair a gone topic only when the failed operation named its id."""
    kind = str(result.get("kind") or "")
    classified = classify_telegram_error(result.get("error"))
    if (
        result.get("topic_missing") is not True
        and kind != "topic_not_found"
        and classified != "topic_not_found"
    ):
        return False
    proven_topic_id = str(topic_id or "")
    if not proven_topic_id:
        return False
    if (
        entry is not None
        and str(entry.get("topic_id") or "") == proven_topic_id
        and state.clear_gone_live_topic(
            store,
            entry,
            error_kind="topic_not_found",
            error=result.get("error"),
        )
    ):
        return True
    state.tombstone_dead_topic(store, proven_topic_id)
    return True


def _record_delivery_success(entry: dict[str, Any], bot_kind: str) -> None:
    entry.pop("last_delivery_error", None)
    if bot_kind != MANAGER_BOT_KIND:
        entry["last_managed_bot_kind"] = bot_kind
        entry.pop("last_managed_bot_error", None)


def _entry_for_turn(store: dict[str, Any], item: dict[str, Any]) -> tuple[str | None, dict[str, Any] | None]:
    worker_id = compact_ws(item.get("worker_id"), 160)
    space_id = compact_ws(item.get("space_id"), 160)
    identity = _turn_stable_identity(item)
    if identity is not None:
        key, worker_entry = state.find_worker_entry_by_stable_key(
            store, identity[0]
        )
        if (
            key is None
            or worker_entry is None
            or state.entry_stable_identity(worker_entry) != identity
            or not state.worker_entry_is_uniquely_routable(
                store, key, worker_entry
            )
        ):
            return None, None
        # A restart generation may carry the pre-handoff space id. Stable-key
        # routing follows the current entry's space/topic cache.
        space_id = _entry_space_id(worker_entry)
    else:
        key, worker_entry = _worker_entry_for_turn(
            store, worker_id, space_id
        )
    if key is None:
        return None, None
    if worker_entry is None:
        return None, None
    if config.source_topic_mode() == "worker":
        return key, worker_entry
    _space_key, space_entry = state.find_space_entry_by_id(
        store,
        compact_ws(space_id or worker_entry.get("tendwire_space_id") or worker_entry.get("space_id"), 160),
    )
    if space_entry is None:
        return None, None
    return key, _delivery_entry(store, space_entry, worker_entry)


def _turn_id(item: dict[str, Any]) -> str:
    return compact_ws(item.get("id") or item.get("turn_id"), 200)


_TURN_CONTENT_OUTCOME_KEY = "_herdres_content_outcome"
_TURN_CONTENT_OUTCOME_LIMIT = 100
_TURN_CONTENT_MATERIALIZED_KEY = "_herdres_content_materialized"


def _strict_nonnegative_int(
    value: Any,
    field: str,
    *,
    status: str = "invalid_content_schema",
) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise _TurnContentError(status, f"invalid {field}")
    return value


def _validate_turn_field_descriptor(
    item: dict[str, Any],
    *,
    field: str,
    descriptor: dict[str, Any],
) -> None:
    availability = descriptor.get("availability")
    inline = descriptor.get("inline")
    char_length = _strict_nonnegative_int(
        descriptor.get("char_length"), f"{field}.char_length"
    )
    byte_length = _strict_nonnegative_int(
        descriptor.get("byte_length"), f"{field}.byte_length"
    )
    page_count = _strict_nonnegative_int(
        descriptor.get("page_count"), f"{field}.page_count"
    )
    first_cursor = descriptor.get("first_cursor")
    if type(inline) is not bool:
        raise _TurnContentError(
            "invalid_content_schema", f"invalid {field}.inline"
        )
    if availability == "absent":
        if inline or char_length or byte_length or page_count or first_cursor is not None:
            raise _TurnContentError(
                "invalid_content_schema", f"inconsistent absent {field}"
            )
        if field in item:
            raise _TurnContentError(
                "invalid_content_schema", f"unexpected absent {field}"
            )
        return
    if availability == "known_incomplete":
        if inline or page_count or first_cursor is not None or field in item:
            raise _TurnContentError(
                "invalid_content_schema",
                f"known-incomplete {field} must be non-inline and non-pageable",
            )
        return
    if availability != "complete":
        raise _TurnContentError(
            "invalid_content_schema", f"invalid {field}.availability"
        )
    if inline:
        value = item.get(field)
        if not isinstance(value, str):
            raise _TurnContentError(
                "invalid_content_schema", f"missing inline {field}"
            )
        if (
            len(value) != char_length
            or len(value.encode("utf-8")) != byte_length
            or page_count != 1
            or first_cursor is not None
        ):
            raise _TurnContentError(
                "invalid_content_schema", f"inline {field} metadata mismatch"
            )
        return
    if (
        field in item
        or page_count <= 0
        or not isinstance(first_cursor, str)
        or not first_cursor.startswith("twcur1.")
    ):
        raise _TurnContentError(
            "invalid_content_schema", f"non-inline {field} is not pageable"
        )


def _turn_local_outcome(
    item: dict[str, Any], status: str
) -> dict[str, str]:
    outcome = {
        "turn_id": compact_ws(
            item.get("id") or item.get("turn_id") or "unidentified", 200
        ),
        "status": status,
    }
    revision = _content_revision(item)
    if revision:
        outcome["content_revision"] = revision
    return outcome


def _quarantined_turn_row(
    item: dict[str, Any], status: str
) -> dict[str, Any]:
    """Retain public routing identity, never the rejected private structure."""

    quarantined = {
        key: item[key]
        for key in ("id", "turn_id", "worker_id", "space_id")
        if isinstance(item.get(key), str) and item[key]
    }
    quarantined[_TURN_CONTENT_OUTCOME_KEY] = _turn_local_outcome(item, status)
    return quarantined


def _validate_turn_row(raw: dict[str, Any]) -> dict[str, Any]:
    if _contains_private_agent_turn_fields(raw):
        raise _TurnContentError(
            "private_agent_content",
            "neutral Tendwire turn contains private structured agent data",
        )
    item = dict(raw)
    content = item.get("content")
    content_schema = (
        content.get("schema_version") if isinstance(content, dict) else None
    )
    if (
        type(content_schema) is not int
        or content_schema != TURN_CONTENT_SCHEMA_VERSION
    ):
        raise _TurnContentError(
            "unsupported_content_schema", "turn content schema v1 is required"
        )
    revision = content.get("content_revision")
    fields = content.get("fields")
    known_incomplete = content.get("known_incomplete")
    if (
        not isinstance(revision, str)
        or not revision.startswith("twrev1.")
        or not isinstance(fields, dict)
    ):
        raise _TurnContentError(
            "invalid_content_schema", "invalid content revision or fields"
        )
    if type(known_incomplete) is not bool:
        raise _TurnContentError(
            "invalid_content_schema", "known_incomplete must be boolean"
        )
    incomplete_field = False
    for field in ("user_text", "assistant_final_text"):
        descriptor = fields.get(field)
        if not isinstance(descriptor, dict):
            raise _TurnContentError(
                "invalid_content_schema", f"missing {field} descriptor"
            )
        _validate_turn_field_descriptor(item, field=field, descriptor=descriptor)
        incomplete_field = (
            incomplete_field
            or descriptor.get("availability") == "known_incomplete"
        )
    if known_incomplete != incomplete_field:
        raise _TurnContentError(
            "invalid_content_schema", "known-incomplete summary mismatch"
        )
    if known_incomplete:
        item[_TURN_CONTENT_OUTCOME_KEY] = _turn_local_outcome(
            item, "content_known_incomplete"
        )
    return item


def _validate_turns_payload(payload: dict[str, Any]) -> dict[str, Any]:
    schema = payload.get("schema_version")
    if type(schema) is not int or schema != TURN_SCHEMA_VERSION:
        raise _TurnContentError(
            "upgrade_required", "Tendwire turn schema v2 is required"
        )
    rows = payload.get("turns")
    if not isinstance(rows, list):
        raise _TurnContentError(
            "invalid_content_schema", "turns must be a list"
        )
    validated_rows: list[dict[str, Any]] = []
    for raw in rows:
        if not isinstance(raw, dict):
            # A row-level protocol defect is isolated just like a malformed
            # descriptor; it cannot safely participate in delivery.
            validated_rows.append(
                {
                    _TURN_CONTENT_OUTCOME_KEY: {
                        "turn_id": "unidentified",
                        "status": "invalid_content_schema",
                    }
                }
            )
            continue
        try:
            validated_rows.append(_validate_turn_row(raw))
        except _TurnContentError as exc:
            item = (
                _quarantined_turn_row(raw, exc.status)
                if exc.status == "private_agent_content"
                else dict(raw)
            )
            if _TURN_CONTENT_OUTCOME_KEY not in item:
                item[_TURN_CONTENT_OUTCOME_KEY] = _turn_local_outcome(
                    item, exc.status
                )
            validated_rows.append(item)
    validated = dict(payload)
    validated["turns"] = validated_rows
    return validated


def _materialize_turn_field(
    runtime: SyncRuntime,
    item: dict[str, Any],
    *,
    content_revision: str,
    field: str,
    descriptor: dict[str, Any],
) -> tuple[str, int]:
    availability = descriptor["availability"]
    if availability == "absent":
        return "", 0
    if availability == "known_incomplete":
        raise _TurnContentError(
            "content_known_incomplete", f"{field} is known incomplete"
        )
    if descriptor["inline"]:
        return str(item[field]), 0

    char_length = int(descriptor["char_length"])
    byte_length = int(descriptor["byte_length"])
    page_count = int(descriptor["page_count"])
    turn_id = _turn_id(item)
    cursor: str | None = str(descriptor["first_cursor"])
    seen_cursors: set[str] = set()
    seen_segments: set[str] = set()
    chunks: list[str] = []
    for expected_index in range(page_count):
        if cursor is None or cursor in seen_cursors:
            raise _TurnContentError(
                "invalid_content_page", f"{field} cursor cycle or early end"
            )
        seen_cursors.add(cursor)
        page = runtime.tendwire.turn_content_get(
            turn_id, content_revision, field, cursor
        )
        if page.get("ok") is False:
            status = str(page.get("status") or "content_fetch_failed")
            raise _TurnContentError(
                status,
                str(page.get("error") or f"failed to fetch {field}"),
                conflict=status
                in {
                    "content_revision_not_found",
                    "revision_conflict",
                    "stale_revision",
                },
            )
        if (
            type(page.get("schema_version")) is not int
            or page.get("schema_version") != TURN_CONTENT_SCHEMA_VERSION
            or page.get("turn_id") != turn_id
            or page.get("content_revision") != content_revision
            or page.get("field") != field
            or page.get("availability") != "complete"
        ):
            raise _TurnContentError(
                "invalid_content_page",
                f"{field} page identity mismatch",
            )
        index = _strict_nonnegative_int(
            page.get("index"), "page.index", status="invalid_content_page"
        )
        count = _strict_nonnegative_int(
            page.get("count"), "page.count", status="invalid_content_page"
        )
        if index != expected_index or count != page_count:
            raise _TurnContentError(
                "invalid_content_page", f"{field} page order/count mismatch"
            )
        text = page.get("text")
        segment_id = page.get("segment_id")
        if (
            not isinstance(text, str)
            or not isinstance(segment_id, str)
            or not segment_id.startswith("twseg1.")
            or segment_id in seen_segments
        ):
            raise _TurnContentError(
                "invalid_content_page", f"{field} invalid or duplicate segment"
            )
        seen_segments.add(segment_id)
        if (
            _strict_nonnegative_int(
                page.get("segment_char_length"),
                "segment_char_length",
                status="invalid_content_page",
            )
            != len(text)
            or _strict_nonnegative_int(
                page.get("segment_byte_length"),
                "segment_byte_length",
                status="invalid_content_page",
            )
            != len(text.encode("utf-8"))
            or _strict_nonnegative_int(
                page.get("total_char_length"),
                "total_char_length",
                status="invalid_content_page",
            )
            != char_length
            or _strict_nonnegative_int(
                page.get("total_byte_length"),
                "total_byte_length",
                status="invalid_content_page",
            )
            != byte_length
        ):
            raise _TurnContentError(
                "invalid_content_page", f"{field} page length mismatch"
            )
        next_cursor = page.get("next_cursor")
        if expected_index + 1 < page_count:
            if (
                not isinstance(next_cursor, str)
                or not next_cursor.startswith("twcur1.")
                or next_cursor in seen_cursors
            ):
                raise _TurnContentError(
                    "invalid_content_page", f"{field} invalid next cursor"
                )
            cursor = next_cursor
        else:
            if next_cursor is not None:
                raise _TurnContentError(
                    "invalid_content_page",
                    f"{field} final cursor must be null",
                )
            cursor = None
        chunks.append(text)
    value = "".join(chunks)
    if (
        len(value) != char_length
        or len(value.encode("utf-8")) != byte_length
    ):
        raise _TurnContentError(
            "invalid_content_page", f"{field} reconstructed length mismatch"
        )
    return value, page_count


def _materialize_turn_item(
    item: dict[str, Any], runtime: SyncRuntime
) -> int:
    if item.get(_TURN_CONTENT_MATERIALIZED_KEY) is True:
        return 0
    if item.get(_TURN_CONTENT_OUTCOME_KEY):
        raise _TurnContentError(
            str(item[_TURN_CONTENT_OUTCOME_KEY].get("status")),
            "turn content is not eligible for materialization",
        )
    content = item.get("content")
    if not isinstance(content, dict):
        raise _TurnContentError(
            "unsupported_content_schema", "turn content schema v1 is required"
        )
    fields = content["fields"]
    revision = str(content["content_revision"])
    materialized = dict(item)
    page_calls = 0
    for field in ("user_text", "assistant_final_text"):
        descriptor = fields[field]
        value, fetched = _materialize_turn_field(
            runtime,
            item,
            content_revision=revision,
            field=field,
            descriptor=descriptor,
        )
        page_calls += fetched
        if descriptor["availability"] == "absent":
            materialized.pop(field, None)
        else:
            materialized[field] = value
    materialized[_TURN_CONTENT_MATERIALIZED_KEY] = True
    item.clear()
    item.update(materialized)
    return page_calls


def _turn_content_outcomes(
    payload: dict[str, Any],
) -> dict[str, Any]:
    outcomes = [
        dict(item[_TURN_CONTENT_OUTCOME_KEY])
        for item in _turns(payload)
        if isinstance(item.get(_TURN_CONTENT_OUTCOME_KEY), dict)
    ]
    return {
        "count": len(outcomes),
        "truncated": len(outcomes) > _TURN_CONTENT_OUTCOME_LIMIT,
        "items": outcomes[:_TURN_CONTENT_OUTCOME_LIMIT],
    }


def _turn_has_content_outcome(item: dict[str, Any]) -> bool:
    return isinstance(item.get(_TURN_CONTENT_OUTCOME_KEY), dict)


def _turn_has_complete_final(item: dict[str, Any]) -> bool:
    content = item.get("content")
    if not isinstance(content, dict):
        return bool(item.get("complete")) or isinstance(
            item.get("assistant_final_text"), str
        )
    fields = content.get("fields")
    descriptor = (
        fields.get("assistant_final_text")
        if isinstance(fields, dict)
        else None
    )
    return (
        not _turn_has_content_outcome(item)
        and isinstance(descriptor, dict)
        and descriptor.get("availability") == "complete"
    )


def _turn_has_real_content(item: dict[str, Any]) -> bool:
    return bool(
        not _turn_has_content_outcome(item)
        and (
            item.get("assistant_stream_text")
            or item.get("user_text")
            or _turn_has_complete_final(item)
        )
    )


def _turn_content_hash(item: dict[str, Any], kind: str) -> str:
    return short_hash(
        {
            "kind": kind,
            "turn_id": _turn_id(item),
            "user": item.get("user_text"),
            "final": item.get("assistant_final_text"),
            "stream": item.get("assistant_stream_text"),
        },
        20,
    )


def _turn_user_hash(item: dict[str, Any]) -> str:
    text = compact_ws(item.get("user_text"), 2000)
    return short_hash({"user": text}, 16) if text else ""


# --- Delivery-state single writers ------------------------------------------
# These keys describe the last delivered final/stream message for an entry.
# Every write goes through the helpers below so the group stays consistent;
# never assign the keys directly.

_FINAL_DELIVERY_KEYS = (
    "last_turn_id",
    "last_clean_hash",
    "last_clean_user_hash",
    "last_clean_message_id",
    "last_clean_message_ids",
    "last_clean_bot_kind",
    "last_render_version",
)
_STREAM_DELIVERY_KEYS = (
    "last_stream_turn_id",
    "last_stream_submission_id",
    "last_stream_hash",
    "last_stream_message_id",
    "last_stream_bot_kind",
    "last_stream_updated_at",
)


def _pop_keys(entry: dict[str, Any], keys: tuple[str, ...]) -> bool:
    changed = False
    for key in keys:
        if key in entry:
            entry.pop(key, None)
            changed = True
    return changed


def _clear_final_delivery_keys(entry: dict[str, Any]) -> bool:
    return _pop_keys(entry, _FINAL_DELIVERY_KEYS)


def _clear_stream_delivery_keys(entry: dict[str, Any]) -> bool:
    return _pop_keys(entry, _STREAM_DELIVERY_KEYS)


def _entry_put(entry: dict[str, Any], key: str, value: Any) -> bool:
    if entry.get(key) == value:
        return False
    entry[key] = value
    return True


def _entry_float(entry: dict[str, Any], key: str) -> float:
    try:
        return float(entry.get(key) or 0)
    except (TypeError, ValueError):
        return 0.0


def _same_turn_working_update_too_soon(entry: dict[str, Any], turn_id: str, *, now: float) -> bool:
    if not turn_id or entry.get("last_stream_turn_id") != turn_id:
        return False
    if not entry.get("last_stream_message_id"):
        return False
    min_seconds = config.working_update_min_seconds()
    if min_seconds <= 0:
        return False
    last_at = _entry_float(entry, "last_stream_updated_at")
    return bool(last_at and now - last_at < min_seconds)


def _set_final_delivery(
    entry: dict[str, Any],
    *,
    turn_id: str,
    content_hash: str,
    user_hash: str | None = None,
    message_ids: list[str] | None = None,
    bot_kind: str | None = None,
    render_version: int | None = None,
    placeholder: bool = False,
) -> bool:
    """Single writer for the final-delivery key group.

    ``user_hash``/``message_ids``/``bot_kind``/``render_version`` are left
    untouched when None. ``placeholder`` records the "0" sentinel used by
    dry-run and bootstrap paths without clobbering a real message id.
    """
    changed = _entry_put(entry, "last_turn_id", turn_id)
    changed = _entry_put(entry, "last_clean_hash", content_hash) or changed
    if user_hash is not None:
        if user_hash:
            changed = _entry_put(entry, "last_clean_user_hash", user_hash) or changed
        elif "last_clean_user_hash" in entry:
            entry.pop("last_clean_user_hash", None)
            changed = True
    if render_version is not None:
        changed = _entry_put(entry, "last_render_version", render_version) or changed
    if bot_kind:
        changed = _entry_put(entry, "last_clean_bot_kind", bot_kind) or changed
    if placeholder:
        if not entry.get("last_clean_message_id"):
            entry["last_clean_message_id"] = "0"
            changed = True
        changed = _entry_put(entry, "last_clean_message_ids", ["0"]) or changed
    elif message_ids is not None:
        kept = [message_id for message_id in message_ids if message_id]
        changed = _entry_put(entry, "last_clean_message_ids", kept) or changed
        changed = _entry_put(entry, "last_clean_message_id", kept[0] if kept else "") or changed
    return changed


def _set_stream_delivery(
    entry: dict[str, Any],
    *,
    turn_id: str,
    content_hash: str,
    message_id: str | None = None,
    bot_kind: str | None = None,
    submission_id: str | None = None,
    placeholder: bool = False,
) -> bool:
    """Single writer for the stream-delivery key group."""
    changed = False
    if submission_id:
        changed = (
            _entry_put(
                entry, "last_stream_submission_id", submission_id
            )
            or changed
        )
    elif "last_stream_submission_id" in entry:
        entry.pop("last_stream_submission_id", None)
        changed = True
    changed = _entry_put(entry, "last_stream_turn_id", turn_id) or changed
    changed = _entry_put(entry, "last_stream_hash", content_hash) or changed
    if placeholder:
        if not entry.get("last_stream_message_id"):
            entry["last_stream_message_id"] = "0"
            changed = True
    elif message_id is not None:
        changed = _entry_put(entry, "last_stream_message_id", message_id) or changed
    if bot_kind:
        changed = _entry_put(entry, "last_stream_bot_kind", bot_kind) or changed
    return changed


def _record_stream_update_time(entry: dict[str, Any], now: float | None = None) -> None:
    entry["last_stream_updated_at"] = f"{(time.time() if now is None else now):.3f}"


def _stream_submission_id(
    item: dict[str, Any], entry: dict[str, Any]
) -> str:
    """Return the stable owner for a pass-level working projection.

    Tendwire may rotate a projection turn id while one submitted agent turn is
    still running.  The explicit link wins (and therefore starts a new card
    when a new Telegram submission arrives); otherwise the current card keeps
    its durable submission owner across later unlinked pass rows.
    """

    explicit = item.get(_SUBMISSION_ID_KEY)
    if isinstance(explicit, str) and explicit:
        return explicit
    return str(entry.get("last_stream_submission_id") or "")


def _turn_feed_item(
    item: dict[str, Any], entry: dict[str, Any]
) -> dict[str, Any]:
    """Build one feed item without carrying a stale prompt into a new turn."""

    presentation = item
    user_hash = _turn_user_hash(item)
    if (
        user_hash
        and not _stream_submission_id(item, entry)
        and entry.get("last_turn_id") != _turn_id(item)
        and entry.get("last_clean_user_hash") == user_hash
    ):
        # Some worker/automation rows repeat the last submitted Telegram text
        # even though the new turn has no Telegram submission.  Keep the turn
        # itself visible, but do not mislabel that stale text as a fresh "You"
        # quote.
        presentation = dict(item)
        presentation.pop("user_text", None)
    return turn_item_from_source(presentation, entry)


def _canonical_final_feed_item(
    item: dict[str, Any], entry: dict[str, Any]
) -> dict[str, Any]:
    """Build a lossless feed item for a Tendwire canonical final plan."""

    return turn_item_from_source(item, entry)


def _working_delivery_item(item: dict[str, Any]) -> dict[str, Any]:
    if item.get("assistant_stream_text") or item.get("assistant_final_text"):
        return item
    updated = dict(item)
    updated["assistant_stream_text"] = "Work is in progress."
    return updated


def _turn_is_working_placeholder(item: dict[str, Any], entry: dict[str, Any]) -> bool:
    if item.get("assistant_stream_text") or item.get("assistant_final_text"):
        return False
    content = item.get("content")
    fields = content.get("fields") if isinstance(content, dict) else None
    if isinstance(fields, dict) and any(
        isinstance(descriptor, dict)
        and descriptor.get("availability") == "complete"
        and descriptor.get("inline") is False
        for field, descriptor in fields.items()
        if field in {"assistant_stream_text", "assistant_final_text"}
    ):
        # A bounded delta can intentionally carry assistant-output descriptors
        # only. Treating that as empty would fabricate a Working card and
        # bypass the canonical Goal 05 content path. A paged user prompt does
        # not block the independent Working placeholder.
        return False
    if bool(item.get("complete")):
        return False
    if not _turn_id(item):
        return False
    return normalized_status(entry.get("status")) == "working"


def _final_delivery_bindings(
    store: dict[str, Any],
    turn_id: str,
    *,
    topic_id: str | None = None,
) -> list[tuple[str, dict[str, Any]]]:
    return [
        (message_id, binding)
        for message_id, binding in state.message_bindings(store).items()
        if isinstance(binding, dict)
        and str(binding.get("kind") or "") == "final"
        and str(binding.get("turn_id") or "") == turn_id
        and (
            topic_id is None
            or str(binding.get("topic_id") or "") == topic_id
        )
    ]


def _clear_open_turn_final_delivery_state(store: dict[str, Any], entry: dict[str, Any], turn_id: str) -> bool:
    """Remove stale final-delivery markers for a turn Tendwire still reports open.

    Older source syncs could accidentally render stream-only progress as a final
    Response. If those markers remain, the real completed response for the same
    turn_id is suppressed later by the duplicate guard.
    """
    if not turn_id:
        return False
    changed = False
    delivered = state.delivered_turns(store)
    for identity, record in list(delivered.items()):
        same_turn_record = isinstance(record, dict) and str(record.get("turn_id") or "") == turn_id
        if str(identity).startswith(f"final:{turn_id}:") or same_turn_record:
            delivered.pop(identity, None)
            changed = True
    bindings = state.message_bindings(store)
    for message_id, binding in list(bindings.items()):
        if (
            isinstance(binding, dict)
            and str(binding.get("kind") or "") == "final"
            and str(binding.get("turn_id") or "") == turn_id
        ):
            bindings.pop(message_id, None)
            changed = True
    if entry.get("last_turn_id") == turn_id:
        changed = _clear_final_delivery_keys(entry) or changed
    return changed


def _clear_stream_delivery_state(entry: dict[str, Any], turn_id: str) -> None:
    if entry.get("last_stream_turn_id") != turn_id:
        return
    _clear_stream_delivery_keys(entry)


_FOLD_ATTEMPT_CAP = state.RESPONSE_FOLD_ATTEMPT_CAP
_FOLD_PASS_CAP = 3


def _record_fold_failure(
    binding: dict[str, Any],
    reason: Any,
    fold_state: dict[str, int] | None,
    *,
    attempted: bool = True,
) -> None:
    """Persist a structured fold failure without an extra state save."""

    previous_error = binding.get("fold_error")
    previous_attempts = int(binding.get("fold_attempts") or 0)
    binding["fold_error"] = compact_ws(reason or "response fold failed", 240)
    if attempted:
        binding["fold_attempts"] = previous_attempts + 1
    if fold_state is not None:
        fold_state["failed"] = fold_state.get("failed", 0) + 1
        if (
            binding.get("fold_error") != previous_error
            or int(binding.get("fold_attempts") or 0) != previous_attempts
        ):
            fold_state["changed"] = fold_state.get("changed", 0) + 1


def _fold_superseded_final(
    store: dict[str, Any],
    item: dict[str, Any],
    entry: dict[str, Any],
    runtime: SyncRuntime,
    *,
    chat_id: str,
    fold_state: dict[str, int] | None = None,
) -> bool:
    """Collapse the Response of a SUPERSEDED final (opt-in via
    HERDR_TELEGRAM_TOPICS_RESPONSE_COLLAPSE_PREVIOUS): re-render the previously delivered message with
    collapse_response=True so only the newest answer stays expanded. Runs in the historical-final
    branch of _sync_turns, which sees every non-latest completed final WITH its content each sync (the
    store retains a short per-worker turn history) — a self-healing sweep, no extra text persisted.
    Idempotent per physical binding via binding["folded"] and bounded by
    _FOLD_ATTEMPT_CAP. Multipart finals are folded part-by-part so no stale
    sibling remains expanded. Never touches the latest delivery."""
    if runtime.dry_run or not config.response_collapse_previous_default():
        return False
    if fold_state is not None and fold_state.get("issued", 0) >= _FOLD_PASS_CAP:
        return False  # per-pass physical-write budget spent; remaining folds wait for later ticks
    if not str(item.get("assistant_final_text") or "").strip():
        return False
    bindings = _final_delivery_bindings(store, _turn_id(item))
    if not bindings:
        return False
    changed_before = fold_state.get("changed", 0) if fold_state else 0
    telegram = _telegram_state(store)
    api_token, bot_kind = _delivery_bot(store, entry)
    folded_item = dict(_turn_feed_item(item, entry))
    folded_item["collapse_response"] = True
    try:
        plans = _prepare_final_delivery_parts(folded_item)
    except _TurnContentError as exc:
        for _message_id, binding in bindings:
            _record_fold_failure(
                binding, exc.status, fold_state, attempted=False
            )
        return bool(
            fold_state
            and fold_state.get("changed", 0) != changed_before
        )
    ordered = sorted(
        bindings,
        key=lambda pair: (
            pair[1].get("part_ordinal")
            if isinstance(pair[1].get("part_ordinal"), int)
            else len(bindings),
            pair[0],
        ),
    )
    if len(ordered) != len(plans):
        for _message_id, binding in ordered:
            _record_fold_failure(
                binding,
                "fold delivery-part count does not match bindings",
                fold_state,
                attempted=False,
            )
        return bool(
            fold_state
            and fold_state.get("changed", 0) != changed_before
        )
    attempted = False
    for fallback_ordinal, (message_id, binding) in enumerate(ordered):
        if (
            binding.get("folded")
            or binding.get("fold_unavailable")
            or int(binding.get("fold_attempts") or 0)
            >= _FOLD_ATTEMPT_CAP
        ):
            continue
        if not message_id:
            _record_fold_failure(
                binding,
                "fold binding has no message id",
                fold_state,
                attempted=False,
            )
            continue
        if (
            fold_state is not None
            and fold_state.get("issued", 0) >= _FOLD_PASS_CAP
        ):
            break
        if _delivery_write_budget(runtime).remaining == 0:
            break
        if str(message_id) == str(
            entry.get("last_clean_message_id") or ""
        ):
            _record_fold_failure(
                binding,
                "historical fold binding aliases the latest message",
                fold_state,
                attempted=False,
            )
            continue
        # The binding itself owns the bot identity. The latest entry's bot can
        # describe another delivery and must not authorize this historical edit.
        stored_bot_kind = str(binding.get("bot_kind") or "")
        if not stored_bot_kind or stored_bot_kind != bot_kind:
            _record_fold_failure(
                binding,
                "fold binding bot identity is unavailable or mismatched",
                fold_state,
                attempted=False,
            )
            continue
        ordinal = binding.get("part_ordinal")
        if not isinstance(ordinal, int):
            ordinal = fallback_ordinal
        if not 0 <= ordinal < len(plans):
            _record_fold_failure(
                binding,
                "fold binding part ordinal is outside the delivery plan",
                fold_state,
                attempted=False,
            )
            continue
        operation = _capture_entry_operation(
            store,
            entry,
            topic_id=str(binding.get("topic_id") or ""),
            message_id=message_id,
        )
        try:
            if fold_state is not None:
                fold_state["attempted"] = (
                    fold_state.get("attempted", 0) + 1
                )
            global_remaining = _delivery_write_budget(runtime).remaining
            fold_remaining = (
                max(0, _FOLD_PASS_CAP - fold_state.get("issued", 0))
                if fold_state is not None
                else global_remaining
            )
            execution = _execute_accounted_delivery_write(
                store,
                runtime,
                operation,
                _provider_mutation(
                    "telegram.edit_turn_delivery_part",
                    reason=(
                        "telegram.edit_turn_delivery_part: fold "
                        "superseded final part"
                    ),
                    args=(
                        chat_id,
                        message_id,
                        folded_item,
                        plans[ordinal],
                    ),
                    kwargs={
                        "telegram": telegram,
                        "api_token": api_token,
                        "max_physical_writes": min(
                            global_remaining, fold_remaining
                        ),
                    },
                ),
            )
        except Exception as exc:  # a provider blip must not abort the pass
            print(f"herdres fold edit failed: {exc}", file=sys.stderr)
            _compare_and_apply_entry_operation(store, operation)
            current = state.find_message_binding(store, message_id)
            if current is not None:
                _record_fold_failure(current, exc, fold_state)
            elif fold_state is not None:
                fold_state["failed"] = fold_state.get("failed", 0) + 1
            if fold_state is not None:
                fold_state["issued"] = (
                    fold_state.get("issued", 0) + 1
                )
            attempted = True
            continue
        sent = execution.result
        writes = _telegram_physical_writes(sent)
        if fold_state is not None:
            fold_state["issued"] = (
                fold_state.get("issued", 0) + writes
            )
        attempted = True
        binding = state.find_message_binding(store, message_id) or binding
        error = str(sent.get("error") or "").lower()
        if sent.get("ok") and sent.get("collapse_applied") is True:
            binding["folded"] = True
            binding.pop("fold_error", None)
            if fold_state is not None:
                fold_state["folded"] = fold_state.get("folded", 0) + 1
            continue
        if sent.get("ok"):
            _record_fold_failure(
                binding,
                "rich Response details presentation was not applied "
                f"(format={sent.get('format') or 'unknown'})",
                fold_state,
            )
            continue
        if (
            _message_missing(sent.get("error"))
            or _topic_missing(sent.get("error"))
            or "not found" in error
        ):
            binding["fold_unavailable"] = True
            _record_fold_failure(
                binding,
                sent.get("error") or sent.get("kind") or "fold unavailable",
                fold_state,
            )
            continue
        _record_fold_failure(
            binding,
            sent.get("error") or sent.get("kind") or "response fold failed",
            fold_state,
        )
    return attempted or bool(
        fold_state and fold_state.get("changed", 0) != changed_before
    )

def _stamp_worker_binding_refusal(entry: dict[str, Any]) -> str:
    """Persist the exact gate that keeps a snapshot-observed pane unbound."""

    if state.entry_stable_identity(entry) is None:
        binding_state = _BINDING_STATE_NO_IDENTITY
    elif state.entry_is_quarantined(entry):
        reason = compact_ws(
            entry.get("stable_key_quarantine_reason"), 120
        ) or "quarantined"
        binding_state = f"quarantined:{reason}"
    elif state.entry_is_retired(entry):
        binding_state = "quarantined:retired_route"
    else:
        binding_state = "quarantined:ambiguous_route"
    entry["binding_state"] = binding_state
    entry.pop("binding_topic_id", None)
    return binding_state


def _ensure_topic(
    store: dict[str, Any],
    source: dict[str, Any],
    entry: dict[str, Any],
    runtime: SyncRuntime,
    *,
    chat_id: str,
    can_create: bool = True,
) -> tuple[bool, bool]:
    if (
        str(entry.get("entry_type") or "") == "worker"
        and not state.entry_is_routable(entry)
    ):
        _stamp_worker_binding_refusal(entry)
        return False, False
    state.discard_tombstoned_topic_binding(store, entry)
    if entry.get("topic_id"):
        entry["binding_state"] = _BINDING_STATE_BOUND
        entry["binding_topic_id"] = str(entry["topic_id"])
        return False, False
    ambiguous_record = _ambiguous_created_topic_for_entry(store, entry)
    if ambiguous_record is not None:
        _stamp_ambiguous_topic_create(entry, ambiguous_record)
    if entry.get("binding_state") == "quarantined:ambiguous_topic_create":
        # createForumTopic has no idempotency key. A transport failure after
        # submission may still have created the topic, so blindly retrying can
        # mint an untracked duplicate on every sync pass. Keep this owner
        # quarantined until an operator inventories/adopts or resets topics.
        entry.pop("binding_topic_id", None)
        return False, False
    if str(entry.get("entry_type") or "") == "worker":
        identity = state.entry_stable_identity(entry)
        if identity is not None and any(
            other is not entry
            and other.get("topic_id")
            and not state.topic_id_is_tombstoned(
                store, other.get("topic_id")
            )
            and state.entry_is_routable(other)
            and state.entry_stable_identity(other) == identity
            for other in state.source_worker_entries(store).values()
        ):
            # Telegram has no idempotency key for createForumTopic. Stable-key
            # ownership is therefore the final guard against a positional-id
            # duplicate minting a second topic.
            entry["binding_state"] = "quarantined:stable_identity_collision"
            return False, False
    if runtime.dry_run:
        entry["binding_state"] = _BINDING_STATE_PENDING_CREATE
        entry.pop("binding_topic_id", None)
        return True, False
    if (
        not can_create
        or len(state.orphaned_created_topics(store))
        >= state.ORPHANED_CREATED_TOPIC_LIMIT
    ):
        entry["binding_state"] = _BINDING_STATE_PENDING_CREATE
        entry.pop("binding_topic_id", None)
        return True, False   # real create deferred by the per-pass create cap; retry next tick
    topic_name = entry.get("topic_name") or state.topic_name_for_space(source)
    operation = _capture_entry_operation(store, entry)
    accepted_receipt_id = ""

    def checkpoint_created_topic(
        result: Any, captured: _OfflockEntryOperation
    ) -> None:
        nonlocal accepted_receipt_id
        accepted_receipt_id = _checkpoint_accepted_created_topic(
            store,
            captured,
            result,
            topic_name=str(topic_name),
        )
        if accepted_receipt_id:
            _checkpoint_topic_lifecycle(store, runtime)

    execution = _execute_entry_operation(
        store,
        runtime.telegram,
        operation,
        _provider_mutation(
            "telegram.create_topic",
            reason="telegram.create_topic: mint missing pane topic",
            args=(chat_id, topic_name),
            kwargs={"icon_color": topic_color_for_name(topic_name)},
        ),
        acceptance_checkpoint=checkpoint_created_topic,
    )
    created, resolution = execution.result, execution.resolution
    if resolution.disposition != _OFFLOCK_APPLY:
        created_topic_id = str(created.get("topic_id") or "")
        if created.get("ok") and created_topic_id:
            state.record_orphaned_created_topic(
                store,
                {
                    "topic_id": created_topic_id,
                    "topic_name": compact_ws(topic_name, 120),
                    "owner": _operation_provenance(operation),
                    "reason": "owner_changed_during_create",
                },
            )
            _complete_accepted_created_topic(
                store, accepted_receipt_id
            )
            _checkpoint_topic_lifecycle(store, runtime)
        return False, bool(created.get("ok") and created_topic_id)
    entry = resolution.entry
    assert entry is not None
    if created.get("ok") and created.get("topic_id"):
        entry["topic_id"] = str(created["topic_id"])
        entry["binding_state"] = _BINDING_STATE_BOUND
        entry["binding_topic_id"] = str(created["topic_id"])
        _complete_topic_recovery(entry, str(created["topic_id"]))
        _complete_accepted_created_topic(
            store, accepted_receipt_id
        )
        # Topic creation has no provider idempotency key. Persist the compact
        # lifecycle receipt at acceptance, then atomically absorb the final
        # binding into the ordinary state file before returning.
        _checkpoint_topic_lifecycle(store, runtime)
        return True, True
    entry["last_topic_error"] = compact_ws(created.get("error"), 240)
    if created.get("ambiguous_acceptance"):
        _stamp_ambiguous_topic_create(
            entry,
            {
                "topic_name": topic_name,
                "error": created.get("error"),
                "observed_at_unix": time.time(),
            },
        )
        entry.pop("binding_topic_id", None)
        return False, False
    entry["binding_state"] = (
        "create_error:"
        + (entry["last_topic_error"] or "unknown_create_error")
    )
    entry.pop("binding_topic_id", None)
    return False, False


_ALERT_STATUSES = frozenset({"attention", "failed"})
_RESERVED_STATUS_EMOJIS = frozenset({"\u2753", "\u203c\ufe0f", "\u2705", "\u26a1\ufe0f", "\u2615\ufe0f"})


def _identity_topic_icon(
    store: dict[str, Any],
    topic_identity: str,
    runtime: SyncRuntime,
) -> tuple[str, str]:
    """Deterministic per-topic identity icon from the allowed forum icon set."""
    catalog = topic_icon_catalog(
        store,
        runtime.telegram,
        checkpoint=runtime.checkpoint,
    )
    choices = sorted(emoji for emoji in catalog if emoji not in _RESERVED_STATUS_EMOJIS)
    if not choices:
        return "", ""
    emoji = choices[
        int(
            short_hash(
                {"topic_icon": compact_ws(topic_identity, 80)}, 8
            ),
            16,
        )
        % len(choices)
    ]
    return emoji, catalog.get(emoji, "")


def topic_color_for_name(name: str) -> int:
    return TOPIC_ICON_COLORS[int(short_hash({"topic_color": compact_ws(name, 80)}, 8), 16) % len(TOPIC_ICON_COLORS)]


def _sync_topic_icon(store: dict[str, Any], entry: dict[str, Any], runtime: SyncRuntime, *, chat_id: str) -> bool:
    """Alert-only status icons: flip to attention/failed markers, restore the
    topic's stable identity icon on recovery, and never churn icons (which post
    unread-generating service messages) for routine working/idle transitions."""
    if not config.topic_status_icons_enabled():
        return False
    thread_id = str(entry.get("topic_id") or "")
    if not thread_id:
        return False
    status = normalized_status(entry.get("status") or entry.get("tendwire_status_line"))
    current = str(entry.get("last_topic_icon") or "")
    operation = _capture_entry_operation(
        store,
        entry,
        topic_id=thread_id,
        observe=(
            "status",
            "tendwire_status_line",
            "last_topic_icon",
            "last_topic_icon_id",
        ),
    )
    topic_identity = str(
        entry.get("topic_name") or entry.get("topic_id") or ""
    )
    if status in _ALERT_STATUSES:
        emoji = status_emoji(status)
        emoji_id = topic_icon_id(
            store,
            emoji,
            runtime.telegram,
            checkpoint=runtime.checkpoint,
        )
    else:
        if current and current not in _RESERVED_STATUS_EMOJIS and entry.get("last_topic_icon_id"):
            return False
        emoji, emoji_id = _identity_topic_icon(
            store, topic_identity, runtime
        )
        if not emoji:
            return False
    # Catalogue reads are guarded provider calls and can replace every nested
    # state object. Resolve the captured durable owner before any icon field is
    # inspected or changed; the pre-read ``entry`` is dead from here onward.
    resolution = _compare_and_apply_entry_operation(store, operation)
    if resolution.disposition != _OFFLOCK_APPLY:
        return False
    entry = resolution.entry
    assert entry is not None
    if not emoji_id:
        entry["last_topic_icon_missing"] = emoji
        return False
    if entry.get("last_topic_icon") == emoji and entry.get("last_topic_icon_id") == emoji_id:
        return False
    if runtime.dry_run:
        entry["last_topic_icon"] = emoji
        entry["last_topic_icon_id"] = emoji_id
        entry.pop("last_topic_icon_missing", None)
        return True
    operation = _capture_entry_operation(store, entry, topic_id=thread_id)
    execution = _execute_entry_operation(
        store,
        runtime.telegram,
        operation,
        _provider_mutation(
            "telegram.edit_topic_icon",
            reason="telegram.edit_topic_icon: update pane status icon",
            args=(chat_id, thread_id, emoji_id),
        ),
    )
    result, resolution = execution.result, execution.resolution
    if _topic_missing(result.get("error")):
        _repair_provider_gone_topic(
            store,
            resolution.entry
            if resolution.disposition == _OFFLOCK_APPLY
            else None,
            result,
            topic_id=thread_id,
        )
        return False
    if resolution.disposition != _OFFLOCK_APPLY:
        return False
    entry = resolution.entry
    assert entry is not None
    if result.get("ok") or _topic_not_modified(result.get("error")):
        entry["last_topic_icon"] = emoji
        entry["last_topic_icon_id"] = emoji_id
        entry.pop("last_topic_icon_missing", None)
        entry.pop("last_topic_icon_error", None)
        return True
    entry["last_topic_icon_error"] = compact_ws(result.get("error"), 240)
    return False


def _record_topic_pinned_status(entry: dict[str, Any], *, message_id: str, content_hash: str, pinned: bool = False) -> None:
    entry["pinned_status_message_id"] = str(message_id)
    entry["pinned_status_hash"] = content_hash
    if pinned:
        entry["pinned_status_pinned"] = True
    entry.pop("pinned_status_last_error", None)


def _entry_open_for_pin(entry: dict[str, Any]) -> bool:
    raw_status = str(entry.get("status") or entry.get("tendwire_raw_status") or entry.get("tendwire_status_line") or "").strip().lower().replace("-", "_")
    if raw_status in {"closed", "exited"}:
        return False
    status = normalized_status(raw_status)
    if status in {"closed", "failed"}:
        return False
    return not (entry.get("closed") or entry.get("exited") or entry.get("process_exited"))


def _worker_visible_on_status_board(entry: dict[str, Any]) -> bool:
    """Select display membership without consulting delivery routability.

    Binding state, unique-route checks, topic ids, and a space's worker_ids are
    delivery concerns.  A live pane must remain owner-visible when any of those
    gates refuses it.  Explicitly historical entries stay hidden; entries from
    older state files without the additive visibility field retain the
    compatibility behavior established in issue #198.
    """

    return (
        _entry_open_for_pin(entry)
        and entry.get("live_in_snapshot") is not False
    )


def _status_entries_for_topic_pin(store: dict[str, Any], entry: dict[str, Any]) -> list[dict[str, Any]]:
    if str(entry.get("entry_type") or "") != "space":
        return [entry] if _worker_visible_on_status_board(entry) else []
    space_id = str(entry.get("tendwire_space_id") or entry.get("space_id") or "")
    workers = [
        worker_entry
        for worker_entry in state.source_worker_entries(store).values()
        if _worker_visible_on_status_board(worker_entry)
        and str(worker_entry.get("tendwire_space_id") or worker_entry.get("space_id") or "") == space_id
    ]
    return workers or ([entry] if _entry_open_for_pin(entry) else [])


def _account_lines_html(
    entries: list[dict[str, Any]],
    *,
    usage_snapshot: dict[str, Any] | None = None,
) -> str:
    """The who-am-I/usage footer for a pinned board: one line per account kind present in
    `entries` ('' when disabled or nothing resolvable). Escaped, ready to append."""
    if not config.pinned_account_enabled():
        return ""
    kinds: list[str] = []
    for entry in entries:
        for field in ("agent", "worker_name", "tendwire_worker_id", "worker_id", "active_worker_id"):
            kind = accounts.agent_kind(entry.get(field))
            if kind:
                if kind not in kinds:
                    kinds.append(kind)
                break
    if not kinds:
        return ""
    snapshot = (
        accounts.usage_snapshot()
        if usage_snapshot is None
        else usage_snapshot
    )
    lines = [line for kind in sorted(kinds) for line in (accounts.account_line(kind, snapshot=snapshot),) if line]
    return "\n".join(html_escape(line, 200) for line in lines)


def _account_usage_snapshot_offlock(
    store: dict[str, Any], runtime: SyncRuntime
) -> dict[str, Any]:
    """Refresh optional account usage without holding the global state flock."""

    if not config.pinned_account_enabled():
        return {}
    if not state.lock_actually_held() or runtime.dry_run:
        return accounts.usage_snapshot()
    # The OAuth usage endpoint has a 15-second timeout.  Persist the complete
    # pre-refresh state, release the flock for cache/network collection, then
    # adopt any concurrent ingress-lane commit before pinned rendering resumes.
    state.save_state(store)
    try:
        with state.released_lock():
            snapshot = accounts.usage_snapshot()
    finally:
        state.reload_state_in_place(store)
    return snapshot


def _sync_topic_pinned(
    store: dict[str, Any],
    entry: dict[str, Any],
    runtime: SyncRuntime,
    *,
    chat_id: str,
    account_usage: dict[str, Any] | None = None,
) -> bool:
    thread_id = str(entry.get("topic_id") or "")
    if not thread_id:
        return False
    pin_entries = _status_entries_for_topic_pin(store, entry)
    html = render_status_overview(pin_entries)
    account_html = _account_lines_html(
        pin_entries or [entry],
        usage_snapshot=account_usage,
    )
    if account_html:
        html = f"{html}\n{account_html}"
    content_hash = short_hash(html, 20)
    message_id = str(entry.get("pinned_status_message_id") or "")
    if message_id and entry.get("pinned_status_hash") == content_hash and entry.get("pinned_status_pinned"):
        return False
    if runtime.dry_run:
        _record_topic_pinned_status(entry, message_id=message_id or "0", content_hash=content_hash, pinned=True)
        return True
    sent: dict[str, Any]
    if message_id:
        edit_operation = _capture_entry_operation(
            store,
            entry,
            topic_id=thread_id,
            message_id=message_id,
            observe=("pinned_status_message_id",),
        )
        execution = _execute_entry_operation(
            store,
            runtime.telegram,
            edit_operation,
            _provider_mutation(
                "telegram.edit_message",
                reason="telegram.edit_message: refresh per-topic pin",
                args=(chat_id, message_id, html),
            ),
        )
        sent, edit_resolution = execution.result, execution.resolution
        if edit_resolution.disposition != _OFFLOCK_APPLY:
            return False
        entry = edit_resolution.entry
        assert entry is not None
        if sent.get("ok"):
            pass
        elif _message_missing(sent.get("error")) or _topic_missing(
            sent.get("error")
        ):
            entry.pop("pinned_status_message_id", None)
            message_id = ""
        else:
            entry["pinned_status_last_error"] = compact_ws(sent.get("error"), 240)
            return False
    if not message_id:
        send_operation = _capture_entry_operation(
            store, entry, topic_id=thread_id
        )

        execution = _execute_entry_operation(
            store,
            runtime.telegram,
            send_operation,
            _provider_mutation(
                "telegram.send_message",
                reason="telegram.send_message: create per-topic pin",
                args=(chat_id, html),
                kwargs={"thread_id": thread_id, "notify": False},
            ),
        )
        sent, send_resolution = execution.result, execution.resolution
        if not sent.get("ok") and _topic_missing(sent.get("error")):
            _repair_provider_gone_topic(
                store,
                send_resolution.entry
                if send_resolution.disposition == _OFFLOCK_APPLY
                else None,
                sent,
                topic_id=thread_id,
            )
            return False
        if send_resolution.disposition != _OFFLOCK_APPLY:
            return False
        entry = send_resolution.entry
        assert entry is not None
        if not sent.get("ok"):
            entry["pinned_status_last_error"] = compact_ws(
                sent.get("error"), 240
            )
            return False
        message_id = str(sent.get("message_id") or "")
        if not message_id:
            entry["pinned_status_last_error"] = "Telegram returned no message id for topic pinned status"
            return False
    pin_operation = _capture_entry_operation(
        store,
        entry,
        topic_id=thread_id,
        message_id=message_id,
        observe=("pinned_status_message_id",),
    )
    execution = _execute_entry_operation(
        store,
        runtime.telegram,
        pin_operation,
        _provider_mutation(
            "telegram.pin_message",
            reason="telegram.pin_message: pin per-topic status card",
            args=(chat_id, message_id),
        ),
    )
    pin_result, pin_resolution = execution.result, execution.resolution
    if pin_resolution.disposition != _OFFLOCK_APPLY:
        return False
    entry = pin_resolution.entry
    assert entry is not None
    pinned = bool(pin_result.get("ok"))
    _record_topic_pinned_status(entry, message_id=message_id, content_hash=content_hash, pinned=pinned)
    if not pinned:
        entry["pinned_status_pin_error"] = compact_ws(pin_result.get("error"), 240)
    else:
        entry.pop("pinned_status_pin_error", None)
    return True


_RENAME_ATTEMPT_CAP = 3

# Consecutive sync passes a finished worker must be ABSENT from the tendwire snapshot before its
# stranded topic is reaped (see config.reap_closed_worker_topics). A small streak absorbs a one-tick
# partial snapshot without letting a genuinely-gone worker linger.
_REAP_ABSENCE_STREAK = 2


def _ordered_workers(workers: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return sorted(workers, key=state.canonical_worker_observation_key)


def _worker_topic_assignment_keys(workers: list[dict[str, Any]]) -> dict[int, str]:
    counts: dict[str, int] = {}
    for worker in workers:
        worker_id = compact_ws(worker.get("id"), 160)
        counts[worker_id] = counts.get(worker_id, 0) + 1
    result: dict[int, str] = {}
    for worker in workers:
        worker_id = compact_ws(worker.get("id"), 160)
        if counts.get(worker_id) == 1:
            result[id(worker)] = worker_id
            continue
        result[id(worker)] = "\x1f".join(
            state.canonical_worker_observation_key(worker)
        )
    return result


def _assign_worker_topic_names(
    store: dict[str, Any],
    workers: list[dict[str, Any]],
    *,
    blocked_stable_keys: set[str] | None = None,
    blocked_worker_ids: set[str] | None = None,
    worker_entry_reservations: Mapping[int, str | None] | None = None,
) -> tuple[dict[str, str], dict[str, str], dict[str, str]]:
    """Map each not-yet-topiced worker id -> a unique topic name (cwd basename, numbered on collision).
    Names already bound to a created topic are reserved and never renumbered, so numbers stay stable as
    panes come and go. Ordered by the shared canonical observation key for deterministic numbering."""
    # Telegram topic names are case-insensitive, so reserve them the same way.
    def _is_variant_of(current: str, base: str) -> bool:
        # "gitmoot" and "gitmoot 3" are both variants of base "gitmoot" — keep them (stable numbering).
        cur, b = current.casefold(), base.casefold()
        return cur == b or (cur.startswith(b + " ") and current[len(base) + 1 :].strip().isdigit())

    entries = state.source_worker_entries(store)
    # EVERY existing topic name starts reserved (absent/closed workers' topics included) so a new
    # pane never proposes a name already owned by a different topic.
    reserved: set[str] = set()
    for entry in entries.values():
        if entry.get("topic_id") and entry.get("topic_name"):
            reserved.add(compact_ws(entry.get("topic_name"), 120).casefold())
    # Names currently backing a real topic — a de-number target must be absent from this set (i.e. no
    # other topic already holds the bare base name).
    all_named = {
        compact_ws(e.get("topic_name"), 120).casefold()
        for e in entries.values()
        if e.get("topic_id") and e.get("topic_name")
    }
    keeps: dict[str, bool] = {}
    assignment_keys = _worker_topic_assignment_keys(workers)
    ordered = _ordered_workers(workers)
    for worker in ordered:
        wid = compact_ws(worker.get("id"), 160)
        assignment_key = assignment_keys[id(worker)]
        key = (
            worker_entry_reservations.get(id(worker))
            if worker_entry_reservations is not None
            else (
                None
                if wid in (blocked_worker_ids or set())
                else state.resolve_worker_entry_key(
                    store, worker, blocked_stable_keys=blocked_stable_keys
                )
                if wid
                else None
            )
        )
        existing = entries.get(key) if key is not None else None
        if not existing or not existing.get("topic_id") or not existing.get("topic_name"):
            continue
        current = compact_ws(existing.get("topic_name"), 120)
        keep = _is_variant_of(current, state.topic_name_for_worker(worker))
        keeps[assignment_key] = keep
        # NOTE: the old name stays RESERVED even when a rename is proposed. It frees naturally on
        # the pass AFTER the rename lands, preventing two live panes from claiming the same name.
    assigned: dict[str, str] = {}
    renames: dict[str, str] = {}
    # wid -> base for names the connector itself minted a " N" suffix onto this pass (name != base after
    # the while-reserved loop). _sync_sources stamps this as connector_numbered_base on the entry when it
    # applies the name — numbering-time provenance, so a later de-number acts only on connector-minted
    # numbers and can never collapse a user's own "Sonnet 4"-style label.
    numbered_bases: dict[str, str] = {}
    for worker in ordered:
        wid = compact_ws(worker.get("id"), 160)
        assignment_key = assignment_keys[id(worker)]
        if not wid:
            continue
        key = (
            worker_entry_reservations.get(id(worker))
            if worker_entry_reservations is not None
            else (
                None
                if wid in (blocked_worker_ids or set())
                else state.resolve_worker_entry_key(
                    store, worker, blocked_stable_keys=blocked_stable_keys
                )
            )
        )
        existing = entries.get(key) if key is not None else None
        has_topic = bool(existing and existing.get("topic_id"))
        # De-number a connector-minted suffix once its base name is free again. The marker
        # (connector_numbered_base) is stamped at NUMBERING time (when the connector mints the " N" —
        # see the while-reserved loop below, applied in _sync_sources), so it records true provenance
        # and this can never rename a user's genuinely-numbered label.
        marker = compact_ws((existing or {}).get("connector_numbered_base"), 120)
        if has_topic and marker and _worker_is_open(worker):
            current = compact_ws(existing.get("topic_name"), 120)
            numbered_variant = (
                current.casefold() != marker.casefold()
                and current.casefold().startswith(marker.casefold() + " ")
                and current[len(marker) + 1 :].strip().isdigit()
            )
            if numbered_variant and marker.casefold() not in all_named and marker.casefold() not in reserved:
                renames[assignment_key] = marker
                reserved.add(marker.casefold())
                continue
        if has_topic and keeps.get(assignment_key, True):
            continue  # topic name still matches its desired base; locked
        if has_topic and not _worker_is_open(worker):
            continue  # never rename a closed pane's topic (and never burn budget on it)
        if has_topic and int((existing or {}).get("rename_attempts") or 0) >= _RENAME_ATTEMPT_CAP:
            continue  # permanently-failing rename: stop proposing (no per-pass budget burn)
        base = state.topic_name_for_worker(worker)
        name, n = base, 2
        while name.casefold() in reserved:
            name = f"{base} {n}"
            n += 1
        reserved.add(name.casefold())
        if name.casefold() != base.casefold():
            numbered_bases[assignment_key] = base   # connector-minted number -> record its base as provenance
        if has_topic:
            renames[assignment_key] = name   # desired name changed (e.g. the pane label appeared) -> rename in place
        else:
            assigned[assignment_key] = name
    return assigned, renames, numbered_bases


def _stamp_numbered_base(entry: dict[str, Any], wid: str, numbered_bases: dict[str, str]) -> None:
    """Apply numbering-time provenance to an entry as its name is set: stamp connector_numbered_base
    when the connector minted a " N" suffix onto this pane's name, else clear any prior marker (a
    de-number or a bare rename removes the connector-minted number)."""
    base = numbered_bases.get(wid)
    if base:
        entry["connector_numbered_base"] = base
    else:
        entry.pop("connector_numbered_base", None)


def _sync_retired_worker_topics(
    store: dict[str, Any],
    runtime: SyncRuntime,
    *,
    chat_id: str,
) -> int:
    """Archive retired restart-era topics without ever deleting history."""
    def stamp_closed(entry: dict[str, Any]) -> None:
        closed_at = time.time()
        entry["retired_topic_closed"] = True
        entry.setdefault("topic_closed_at", closed_at)
        entry.setdefault("topic_auto_closed_at", closed_at)
        for field in (
            "retired_topic_notice_pending",
            "retired_topic_notice_error",
            "retired_topic_rename_pending",
            "retired_topic_rename_error",
        ):
            entry.pop(field, None)

    changed = 0
    # Guarded provider calls reload the store, so never carry a nested entry
    # reference from one retired worker to the next.
    for entry_key in list(state.source_worker_entries(store)):
        entry = state.source_worker_entries(store).get(entry_key)
        if entry is None:
            continue
        if not state.entry_is_retired(entry):
            continue
        topic_id = str(entry.get("topic_id") or "")
        if not topic_id:
            continue
        if entry.get("retired_topic_notice_pending"):
            if runtime.dry_run:
                continue
            survivor_topic_id = str(entry.get("consolidated_into_topic_id") or "")
            target = (
                f" (topic {html_escape(survivor_topic_id)})"
                if survivor_topic_id
                else ""
            )
            notice_operation = _capture_entry_operation(
                store,
                entry,
                topic_id=topic_id,
                observe=("retired_topic_notice_pending",),
            )
            execution = _execute_entry_operation(
                store,
                runtime.telegram,
                notice_operation,
                _provider_mutation(
                    "telegram.send_message",
                    reason=(
                        "telegram.send_message: post retired-pane notice"
                    ),
                    args=(
                        chat_id,
                        "This duplicate pane topic was retired after its stable pane "
                        f"identity was consolidated into the original topic{target}. "
                        "Please continue there.",
                    ),
                    kwargs={"thread_id": topic_id, "notify": False},
                ),
            )
            sent, notice_resolution = execution.result, execution.resolution
            if _topic_missing(sent.get("error")):
                _repair_provider_gone_topic(
                    store,
                    notice_resolution.entry
                    if notice_resolution.disposition == _OFFLOCK_APPLY
                    else None,
                    sent,
                    topic_id=topic_id,
                )
                changed += 1
                continue
            if notice_resolution.disposition != _OFFLOCK_APPLY:
                continue
            entry = notice_resolution.entry
            assert entry is not None
            if sent.get("ok"):
                entry.pop("retired_topic_notice_pending", None)
                entry.pop("retired_topic_notice_error", None)
                entry["retired_topic_notice_message_id"] = str(
                    sent.get("message_id") or ""
                )
                changed += 1
                if runtime.checkpoint is not None:
                    runtime.checkpoint()
            elif classify_telegram_error(sent.get("error")) == "topic_closed":
                stamp_closed(entry)
                changed += 1
                continue
            else:
                entry["retired_topic_notice_error"] = compact_ws(
                    sent.get("error"), 240
                )
                continue
        if entry.get("retired_topic_rename_pending"):
            if runtime.dry_run:
                continue
            rename_operation = _capture_entry_operation(
                store,
                entry,
                topic_id=topic_id,
                observe=("retired_topic_rename_pending",),
            )
            retired_topic_name = str(
                entry.get("topic_name") or "📁 Retired pane"
            )
            execution = _execute_entry_operation(
                store,
                runtime.telegram,
                rename_operation,
                _provider_mutation(
                    "telegram.rename_topic",
                    reason=(
                        "telegram.rename_topic: label retired pane archive"
                    ),
                    args=(chat_id, topic_id, retired_topic_name),
                ),
            )
            renamed, rename_resolution = execution.result, execution.resolution
            if _topic_missing(renamed.get("error")):
                _repair_provider_gone_topic(
                    store,
                    rename_resolution.entry
                    if rename_resolution.disposition == _OFFLOCK_APPLY
                    else None,
                    renamed,
                    topic_id=topic_id,
                )
                changed += 1
                continue
            if rename_resolution.disposition != _OFFLOCK_APPLY:
                continue
            entry = rename_resolution.entry
            assert entry is not None
            if renamed.get("ok") or _topic_not_modified(renamed.get("error")):
                entry.pop("retired_topic_rename_pending", None)
                entry.pop("retired_topic_rename_error", None)
                entry["retired_topic_renamed"] = True
                changed += 1
            elif classify_telegram_error(renamed.get("error")) == "topic_closed":
                stamp_closed(entry)
                changed += 1
            else:
                entry["retired_topic_rename_error"] = compact_ws(
                    renamed.get("error"), 240
                )
                continue
    return changed


def _sync_sources(
    store: dict[str, Any],
    snapshot: dict[str, Any],
    turns_payload: dict[str, Any],
    runtime: SyncRuntime,
    *,
    chat_id: str,
    yield_barrier: Callable[[], None] | None = None,
) -> dict[str, int]:
    counts = {"created": 0, "updated": 0, "panes": 0, "spaces": 0, "icon_updated": 0}
    counts["updated"] += _recover_accepted_created_topics(
        store, runtime
    )
    counts["updated"] += _cleanup_orphaned_created_topics(
        store, runtime, chat_id=chat_id
    )
    topic_mode = config.source_topic_mode()
    # Bound real topic-create calls per pass so a first source sync (a topic per open worker/space)
    # amortizes creation over ticks instead of one create burst under the state lock.
    create_cap = config.source_topic_create_cap()
    creates_issued = 0
    # One topic per pane, named by the pane label (else cwd basename); disambiguate same-name panes
    # ("gitmoot", "gitmoot 2"). Existing topics keep their name while it still matches the desired
    # base; when the desired name CHANGES (a pane label appeared/changed), the topic is renamed in
    # place (bounded per pass) so history is preserved.
    worker_topic_names: dict[str, str] = {}
    worker_topic_renames: dict[str, str] = {}
    worker_numbered_bases: dict[str, str] = {}
    workers = _workers(snapshot)
    durable = state.reconcile_durable_pane_identities(store, workers)
    counts["updated"] += durable.changed
    legacy_workers = [
        worker for worker in workers if id(worker) not in durable.reservations
    ]
    continuity_plan = state.plan_worker_rekey_continuity(store, workers)
    continuity_handoffs = state.apply_worker_rekey_continuity_plan(
        store, workers, continuity_plan
    )
    counts["updated"] += len(continuity_plan.stale_entry_keys)
    # Physical continuity owns first choice of an existing topic. Stable-key
    # consolidation runs only after those handoffs are reserved, and may not
    # revive/reassign a matched historical row before finalize moves its topic.
    counts["updated"] += state.consolidate_worker_entries_by_stable_key(
        store,
        workers,
        excluded_entry_keys=frozenset(
            set(continuity_handoffs.values())
            | set(durable.reservations.values())
        ),
    )
    blocked_stable_keys = state.blocked_worker_stable_keys(store, workers)
    blocked_worker_ids = state.conflicting_snapshot_worker_ids(workers)
    counts["updated"] += state.quarantine_worker_stable_key_owners(
        store,
        blocked_stable_keys,
        reason="preflight_stable_key_conflict",
    )
    worker_assignment_keys = _worker_topic_assignment_keys(workers)
    legacy_reservations = state.precompute_worker_entry_reservations(
        store,
        legacy_workers,
        blocked_stable_keys=blocked_stable_keys,
        blocked_worker_ids=blocked_worker_ids,
    )
    # Private stable-key migrations become eligible for UUID adoption only
    # after their existing fail-closed planner has authenticated and applied
    # them.  Re-run durable attachment in this same transaction so migration
    # is idempotent after the first persisted sync, not one tick later.
    post_migration_durable = state.reconcile_durable_pane_identities(
        store, legacy_workers
    )
    counts["updated"] += post_migration_durable.changed
    worker_entry_reservations = {
        **dict(legacy_reservations),
        **dict(durable.reservations),
        **dict(post_migration_durable.reservations),
    }
    reserved_entry_keys = frozenset(
        key for key in worker_entry_reservations.values() if key is not None
    )
    if topic_mode == "worker":
        worker_topic_names, worker_topic_renames, worker_numbered_bases = _assign_worker_topic_names(
            store,
            workers,
            blocked_stable_keys=blocked_stable_keys,
            blocked_worker_ids=blocked_worker_ids,
            worker_entry_reservations=worker_entry_reservations,
        )
    renames_issued = 0
    # Latest model per worker from the turn rows (recency-ordered: first non-empty wins). Stamped
    # cache-and-keep so an idle pane keeps showing its last-known model on the pinned board.
    model_by_worker: dict[str, str] = {}
    for row in _turns(turns_payload):
        row_wid = compact_ws(row.get("worker_id"), 160)
        row_model = compact_ws(row.get("model"), 80)
        if row_wid and row_model and row_wid not in model_by_worker:
            model_by_worker[row_wid] = row_model
    live_worker_ids = {
        compact_ws(worker.get("id"), 160)
        for worker in _workers(snapshot)
        if _worker_is_open(worker)
    }
    live_worker_ids.discard("")
    turn_status_by_worker, turn_status_by_space = _turn_activity_statuses(turns_payload, live_worker_ids)
    spaces = {compact_ws(item.get("id"), 160): item for item in _spaces(snapshot) if compact_ws(item.get("id"), 160)}
    workers_by_space: dict[str, list[dict[str, Any]]] = {}
    observed_worker_entry_keys: set[str] = set()
    for worker in _ordered_workers(workers):
        if yield_barrier is not None:
            yield_barrier()
        space_id = compact_ws(worker.get("space_id"), 160)
        existing_key = worker_entry_reservations.get(id(worker))
        before = dict(state.source_worker_entries(store).get(existing_key) or {}) if existing_key is not None else {}
        _key, entry, created = state.upsert_worker_entry(
            store,
            worker,
            blocked_stable_keys=blocked_stable_keys,
            blocked_worker_ids=blocked_worker_ids,
            preplanned_key=existing_key,
            use_preplanned_key=True,
            reserved_entry_keys=reserved_entry_keys,
        )
        observed_worker_entry_keys.add(_key)
        stale_entry_key = continuity_handoffs.get(id(worker))
        if stale_entry_key is not None:
            counts["updated"] += int(
                state.finalize_worker_rekey_topic_handoff(
                    store, stale_entry_key, entry
                )
            )
        entry["status"] = _effective_worker_status(worker, turn_status_by_worker)
        entry["live_in_snapshot"] = _worker_is_open(worker)
        _stamp_managed_voice(entry, _space_voice_mode(store, space_id))
        if not state.worker_entry_is_uniquely_routable(store, _key, entry):
            _stamp_worker_binding_refusal(entry)
            counts["created"] += int(created)
            counts["updated"] += int(not created and before != entry)
            continue
        if entry.get("topic_id"):
            entry["binding_state"] = _BINDING_STATE_BOUND
            entry["binding_topic_id"] = str(entry["topic_id"])
        elif entry.get("binding_state") != "quarantined:ambiguous_topic_create":
            entry["binding_state"] = _BINDING_STATE_PENDING_CREATE
            entry.pop("binding_topic_id", None)
        # Apply the cwd-based, disambiguated name before the topic is created (once it has a topic_id
        # the name is locked, so a later renumber can't rename an existing topic).
        wid = compact_ws(worker.get("id"), 160)
        assignment_key = worker_assignment_keys[id(worker)]
        if not entry.get("topic_id") and assignment_key in worker_topic_names:
            entry["topic_name"] = worker_topic_names[assignment_key]
            _stamp_numbered_base(entry, assignment_key, worker_numbered_bases)
        elif (
            entry.get("topic_id")
            and assignment_key in worker_topic_renames
            and not runtime.dry_run
            and renames_issued < create_cap
        ):
            rename_topic_id = str(entry["topic_id"])
            rename_operation = _capture_entry_operation(
                store, entry, topic_id=rename_topic_id
            )
            requested_topic_name = worker_topic_renames[assignment_key]
            execution = _execute_entry_operation(
                store,
                runtime.telegram,
                rename_operation,
                _provider_mutation(
                    "telegram.rename_topic",
                    reason=(
                        "telegram.rename_topic: apply live pane label"
                    ),
                    args=(
                        chat_id,
                        rename_topic_id,
                        requested_topic_name,
                    ),
                ),
            )
            renames_issued += 1
            renamed, rename_resolution = execution.result, execution.resolution
            if _topic_missing(renamed.get("error")):
                _repair_provider_gone_topic(
                    store,
                    rename_resolution.entry
                    if rename_resolution.disposition
                    == _OFFLOCK_APPLY
                    else None,
                    renamed,
                    topic_id=rename_topic_id,
                )
            if rename_resolution.disposition != _OFFLOCK_APPLY:
                continue
            entry = rename_resolution.entry
            assert entry is not None
            if renamed.get("ok"):
                entry["topic_name"] = worker_topic_renames[assignment_key]
                entry.pop("rename_attempts", None)
                _stamp_numbered_base(entry, assignment_key, worker_numbered_bases)
            elif _topic_missing(renamed.get("error")):
                entry["topic_name"] = worker_topic_renames[assignment_key]
                entry.pop("rename_attempts", None)
                _stamp_numbered_base(entry, assignment_key, worker_numbered_bases)
            else:
                entry["rename_attempts"] = int(entry.get("rename_attempts") or 0) + 1
        model = model_by_worker.get(wid)
        if model:
            entry["model"] = model
        counts["created"] += int(created)
        counts["updated"] += int(not created and before != entry)
        if not _worker_is_open(worker):
            entry["live_in_snapshot"] = False
            entry["binding_state"] = _BINDING_STATE_ABSENT
            entry.pop("binding_topic_id", None)
            continue
        if space_id:
            workers_by_space.setdefault(space_id, []).append(worker)
        if topic_mode == "worker" and not _should_delete_done_council_topic(entry):
            topic_needed, topic_created = _ensure_topic(
                store, worker, entry, runtime, chat_id=chat_id, can_create=creates_issued < create_cap
            )
            creates_issued += int(topic_created)
            counts["created"] += int(topic_created or topic_needed)
            entry = state.source_worker_entries(store).get(_key)
            if entry is None:
                continue
            counts["icon_updated"] += int(_sync_topic_icon(store, entry, runtime, chat_id=chat_id))
        counts["panes"] += 1

    if workers and yield_barrier is not None:
        yield_barrier()

    # Visibility is a current-snapshot fact. Historical state survives for
    # lifecycle/reply ownership, but it must never inflate the live-unbound
    # board or doctor alarm.
    for key, historical in state.source_worker_entries(store).items():
        if key in observed_worker_entry_keys:
            continue
        before_visibility = (
            historical.get("live_in_snapshot"),
            historical.get("binding_state"),
            historical.get("binding_topic_id"),
        )
        historical["live_in_snapshot"] = False
        historical["binding_state"] = _BINDING_STATE_ABSENT
        historical.pop("binding_topic_id", None)
        after_visibility = (
            historical.get("live_in_snapshot"),
            historical.get("binding_state"),
            historical.get("binding_topic_id"),
        )
        counts["updated"] += int(
            before_visibility != after_visibility
        )

    for space_id, workers in workers_by_space.items():
        if space_id not in spaces:
            spaces[space_id] = {"id": space_id, "name": space_id, "status": "unknown"}

    seen_space_keys: set[str] = set()
    if topic_mode == "worker":
        counts["updated"] += _sync_retired_worker_topics(
            store, runtime, chat_id=chat_id
        )
        return counts

    for space_id, space in spaces.items():
        if yield_barrier is not None:
            yield_barrier()
        if not _space_is_open(space):
            continue
        selectable = [worker for worker in workers_by_space.get(space_id, []) if _worker_is_open(worker)]
        if not selectable:
            continue
        existing_key = state.find_entry_key_by_space(store, space_id)
        before = dict(state.source_space_entries(store).get(existing_key) or {}) if existing_key is not None else {}
        _key, entry, created = state.upsert_space_entry(store, space)
        if not entry.get("voice_mode"):
            entry["voice_mode"] = _default_voice_mode()
        _stamp_managed_voice(entry, _entry_voice_mode(entry))
        selected = _select_space_worker(selectable, turn_status_by_worker)
        seen_space_keys.add(_key)
        entry.pop("stale_space_topic", None)
        selected_status = _effective_worker_status(selected, turn_status_by_worker) if selected else ""
        space_turn_status = turn_status_by_space.get(space_id) or ""
        entry["status"] = _dominant_status(space_turn_status, selected_status, _source_status(space.get("status")))
        entry["worker_count"] = len(selectable)
        entry["worker_ids"] = [compact_ws(worker.get("id"), 160) for worker in selectable if compact_ws(worker.get("id"), 160)]
        state.clear_space_active_worker(entry)
        if selected:
            _selected_key, selected_entry = state.find_worker_entry_by_id(
                store, compact_ws(selected.get("id"), 160)
            )
            if selected_entry is not None and state.cache_space_active_worker(
                entry, selected_entry
            ):
                entry["active_worker_name"] = compact_ws(selected.get("name"), 80)
                selected_model = model_by_worker.get(compact_ws(selected.get("id"), 160))
                if selected_model:
                    entry["active_worker_model"] = selected_model
                entry["active_worker_status"] = _dominant_status(space_turn_status, selected_status)
        topic_needed, topic_created = _ensure_topic(
            store, space, entry, runtime, chat_id=chat_id, can_create=creates_issued < create_cap
        )
        creates_issued += int(topic_created)
        entry = state.source_space_entries(store).get(_key)
        if entry is None:
            continue
        route_topic_id = str(entry.get("topic_id") or "")
        for worker in selectable:
            worker_id = compact_ws(worker.get("id"), 160)
            _worker_key, worker_entry = state.find_worker_entry_by_id(
                store, worker_id
            )
            if worker_entry is None:
                continue
            if route_topic_id:
                worker_entry["binding_state"] = _BINDING_STATE_BOUND
                worker_entry["binding_topic_id"] = route_topic_id
            else:
                worker_entry["binding_state"] = (
                    _BINDING_STATE_PENDING_CREATE
                )
                worker_entry.pop("binding_topic_id", None)
        counts["created"] += int(created or topic_created or topic_needed)
        counts["updated"] += int(not created and before != entry)
        counts["icon_updated"] += int(_sync_topic_icon(store, entry, runtime, chat_id=chat_id))
        counts["spaces"] += 1
    if spaces and yield_barrier is not None:
        yield_barrier()
    for key in list(state.source_space_entries(store)):
        if key not in seen_space_keys:
            stale_entry = state.source_space_entries(store)[key]
            state.clear_space_active_worker(stale_entry)
            stale_entry["stale_space_topic"] = True
    return counts


def _record_topic_delete_success(
    store: dict[str, Any],
    *,
    topic_id: str,
    name: Any,
    reason: str,
) -> tuple[frozenset[str], bool]:
    """Record a deletion and return (cleared worker keys, state changed)."""
    previous_tombstones = store.get("telegram_dead_topic_ids")
    previous_tombstones = (
        list(previous_tombstones)
        if isinstance(previous_tombstones, list)
        else None
    )
    space_alias_present = any(
        str(entry.get("topic_id") or "") == topic_id
        for entry in state.source_space_entries(store).values()
    )
    cleared_worker_keys = state.tombstone_dead_topic(store, topic_id)
    changed = (
        previous_tombstones != store.get("telegram_dead_topic_ids")
        or space_alias_present
        or bool(cleared_worker_keys)
    )
    audit = store.get("telegram_deleted_topics")
    if not isinstance(audit, list):
        audit = []
    fact = {
        "topic_id": topic_id,
        "name": compact_ws(name, 120),
        "reason": reason,
    }
    if fact not in audit:
        audit.append(fact)
        store["telegram_deleted_topics"] = audit[
            -_TOPIC_CLEANUP_AUDIT_LIMIT:
        ]
        changed = True
    return cleared_worker_keys, changed


def _cleanup_orphaned_created_topics(
    store: dict[str, Any],
    runtime: SyncRuntime,
    *,
    chat_id: str,
) -> int:
    """Retire accepted create results that lost their owner while off-lock."""

    if runtime.dry_run:
        return 0
    changed = 0
    for record in state.orphaned_created_topics(store)[
        : max(1, config.source_orphan_delete_cap())
    ]:
        topic_id = str(record.get("topic_id") or "")
        if not topic_id:
            continue
        try:
            deleted = _execute_exact_provider_operation(
                runtime.telegram,
                store=store,
                mutation=_provider_mutation(
                    "telegram.delete_topic",
                    reason=(
                        "telegram.delete_topic: retire orphaned accepted create"
                    ),
                    args=(chat_id, topic_id),
                ),
            )
        except RateLimited:
            break
        except Exception:
            continue
        if not deleted.get("ok") and not _topic_missing(
            deleted.get("error")
        ):
            continue
        _record_topic_delete_success(
            store,
            topic_id=topic_id,
            name=record.get("topic_name"),
            reason="orphaned_create_after_owner_change",
        )
        changed += int(state.retire_orphaned_created_topic(store, topic_id))
        if runtime.checkpoint is not None:
            runtime.checkpoint()
    return changed


def _cleanup_topics(
    store: dict[str, Any],
    runtime: SyncRuntime,
    *,
    chat_id: str,
    snapshot_worker_ids: set[str] | None = None,
) -> dict[str, Any]:
    result = {"deleted": 0, "failed": 0, "pruned": 0, "changed": False}

    def pop_worker(key: str) -> None:
        panes = store.get("panes")
        if isinstance(panes, dict):
            panes.pop(key, None)

    def pop_space(key: str) -> None:
        spaces = store.get("spaces")
        if isinstance(spaces, dict):
            spaces.pop(key, None)

    visible_space_topics = {
        str(entry.get("topic_id"))
        for entry in state.source_space_entries(store).values()
        if entry.get("topic_id")
    }
    deleted_topic_ids: set[str] = set()
    # Bound real topic-delete calls per pass so a first source sync (which can reclassify many legacy
    # per-worker topics at once) amortizes the deletes over ticks instead of one burst under the lock.
    delete_cap = config.source_orphan_delete_cap()
    deletes_issued = 0

    # Worker-mode reaper (opt-in, DESTRUCTIVE): delete topics of workers that have durably CLOSED/FAILED
    # and left the tendwire snapshot. Positional worker-id churn across herdr restarts (claude-2 ->
    # claude-2-2 for a fresh terminal) otherwise strands the old pane's topic forever, and its squatted
    # name forces the live pane's topic to a " 2" suffix. Guards: opt-in flag, strict closed/failed
    # liveness (NOT 'done'/'idle'), absence across _REAP_ABSENCE_STREAK passes, a non-degraded snapshot,
    # and the shared per-pass delete cap.
    reap_enabled = (
        config.reap_closed_worker_topics()
        and config.source_topic_mode() == "worker"
        and snapshot_worker_ids is not None
    )
    if reap_enabled and not snapshot_worker_ids and state.source_worker_entries(store):
        reap_enabled = False  # a transient empty snapshot must never mass-reap live topics
    if reap_enabled:
        # Degraded/partial-snapshot guard: if NONE of the workers we still consider LIVE appear in this
        # snapshot, treat the whole pass as untrustworthy (a tendwire reconcile-lag / binding-expiry blip
        # that transiently dropped live panes) and skip reaping — otherwise the absent closed entries
        # keep marching toward a delete on a bad pass. A purely-closed store has no live anchor to check,
        # so it still reaps normally.
        live_known_ids = {
            compact_ws(entry.get("tendwire_worker_id") or entry.get("worker_id"), 160)
            for entry in state.source_worker_entries(store).values()
            if not _entry_is_reapable(entry)
        }
        live_known_ids.discard("")
        if live_known_ids and not (live_known_ids & snapshot_worker_ids):
            reap_enabled = False
    if reap_enabled:
        for key in list(state.source_worker_entries(store)):
            entry = state.source_worker_entries(store).get(key)
            if entry is None:
                continue
            if state.entry_is_retired(entry):
                continue
            wid = compact_ws(entry.get("tendwire_worker_id") or entry.get("worker_id"), 160)
            if wid and wid in snapshot_worker_ids:
                if entry.pop("reap_miss_count", None) is not None:
                    result["changed"] = True  # worker reappeared: reset its absence streak
                continue
            if not _entry_is_reapable(entry):
                # Only a genuinely closed/failed pane is reapable. 'done'/'idle'/'working' is a LIVE
                # idle/busy agent (normalized_status('done') == 'idle') whose terminal is still open — a
                # snapshot-absence blip must never delete its topic (and whole scrollback).
                continue
            topic_id = str(entry.get("topic_id") or "")
            if runtime.dry_run:
                # Preview every closed/failed+absent topic (no streak, no state mutation).
                if topic_id and topic_id not in deleted_topic_ids:
                    result["deleted"] += 1
                    deleted_topic_ids.add(topic_id)
                    result["changed"] = True
                continue
            misses = min(int(entry.get("reap_miss_count") or 0) + 1, _REAP_ABSENCE_STREAK)
            if misses < _REAP_ABSENCE_STREAK:
                entry["reap_miss_count"] = misses
                result["changed"] = True
                continue
            if not topic_id:
                pop_worker(key)  # finished, gone, no topic: dead cruft
                result["pruned"] += 1
                result["changed"] = True
                continue
            if deletes_issued >= delete_cap:
                continue  # per-pass delete budget spent; retry next tick (entry still eligible)
            deletes_issued += 1
            delete_topic_name = entry.get("topic_name")
            delete_operation = _capture_entry_operation(
                store, entry, topic_id=topic_id
            )
            execution = _execute_entry_operation(
                store,
                runtime.telegram,
                delete_operation,
                _provider_mutation(
                    "telegram.delete_topic",
                    reason=(
                        "telegram.delete_topic: reap absent closed worker"
                    ),
                    args=(chat_id, topic_id),
                ),
            )
            deleted, delete_resolution = execution.result, execution.resolution
            if not deleted.get("ok") and not _topic_missing(deleted.get("error")):
                result["failed"] += 1
                if delete_resolution.disposition == _OFFLOCK_APPLY:
                    assert delete_resolution.entry is not None
                    delete_resolution.entry["last_topic_delete_error"] = (
                        compact_ws(deleted.get("error"), 240)
                    )
                continue
            _record_topic_delete_success(
                store,
                topic_id=topic_id,
                name=delete_topic_name,
                reason="reaped_closed_worker_topic",
            )
            deleted_topic_ids.add(topic_id)
            if deleted.get("ok"):
                result["deleted"] += 1
            # No de-number marker is stamped here: provenance is recorded at NUMBERING time (see
            # _assign_worker_topic_names / _stamp_numbered_base). Reaping merely frees the base name; the
            # live sibling that the connector minted "<base> N" already carries connector_numbered_base and
            # de-numbers on the next assign pass. Stamping by name-pattern at reap time could collapse a
            # user's own "<base> N" label, so it is deliberately gone.
            result["changed"] = True
            if delete_resolution.disposition == _OFFLOCK_APPLY:
                assert delete_resolution.entry is not None
                current_key, _kind = _entry_operation_key(
                    store, delete_resolution.entry
                )
                pop_worker(current_key)

    def finalize_deleted_space_worker_aliases(
        worker_keys: frozenset[str], reason: str
    ) -> None:
        for worker_key in worker_keys:
            worker_entry = state.source_worker_entries(store).get(worker_key)
            if worker_entry is None:
                continue
            if _should_delete_done_council_topic(worker_entry):
                pop_worker(worker_key)
            else:
                worker_entry["deleted_topic_reason"] = reason

    for key in list(state.source_worker_entries(store)):
        entry = state.source_worker_entries(store).get(key)
        if entry is None:
            continue
        if state.entry_is_retired(entry):
            continue
        topic_id = str(entry.get("topic_id") or "")
        if not topic_id:
            continue
        stale_worker_topic = config.source_topic_mode() == "space" and topic_id not in visible_space_topics
        done_council_topic = _should_delete_done_council_topic(entry) and (
            config.source_topic_mode() == "worker" or topic_id not in visible_space_topics
        )
        if not stale_worker_topic and not done_council_topic:
            continue
        reason = "done_council_topic" if done_council_topic else "stale_worker_topic"
        if runtime.dry_run:
            if topic_id not in deleted_topic_ids:
                result["deleted"] += 1
                deleted_topic_ids.add(topic_id)
            result["changed"] = True
            continue
        if deletes_issued >= delete_cap:
            continue  # per-pass delete budget spent; retry this topic next tick (record untouched)
        deletes_issued += 1
        delete_topic_name = entry.get("topic_name")
        delete_operation = _capture_entry_operation(
            store, entry, topic_id=topic_id
        )
        execution = _execute_entry_operation(
            store,
            runtime.telegram,
            delete_operation,
            _provider_mutation(
                "telegram.delete_topic",
                reason="telegram.delete_topic: delete stale worker topic",
                args=(chat_id, topic_id),
            ),
        )
        deleted, delete_resolution = execution.result, execution.resolution
        if not deleted.get("ok"):
            if _topic_missing(deleted.get("error")):
                _record_topic_delete_success(
                    store,
                    topic_id=topic_id,
                    name=delete_topic_name,
                    reason=reason,
                )
                deleted_topic_ids.add(topic_id)
                result["changed"] = True
                if delete_resolution.disposition == _OFFLOCK_APPLY:
                    assert delete_resolution.entry is not None
                    current_entry = delete_resolution.entry
                    current_key, _kind = _entry_operation_key(
                        store, current_entry
                    )
                    if done_council_topic:
                        pop_worker(current_key)
                    else:
                        current_entry["deleted_topic_reason"] = reason
                continue
            result["failed"] += 1
            if delete_resolution.disposition == _OFFLOCK_APPLY:
                assert delete_resolution.entry is not None
                delete_resolution.entry["last_topic_delete_error"] = (
                    compact_ws(deleted.get("error"), 240)
                )
            continue
        result["deleted"] += 1
        deleted_topic_ids.add(topic_id)
        result["changed"] = True
        _record_topic_delete_success(
            store,
            topic_id=topic_id,
            name=delete_topic_name,
            reason=reason,
        )
        if delete_resolution.disposition == _OFFLOCK_APPLY:
            assert delete_resolution.entry is not None
            current_entry = delete_resolution.entry
            current_key, _kind = _entry_operation_key(store, current_entry)
            if done_council_topic:
                pop_worker(current_key)
            else:
                current_entry["deleted_topic_reason"] = reason
    for key in list(state.source_space_entries(store)):
        entry = state.source_space_entries(store).get(key)
        if entry is None:
            continue
        if not entry.get("stale_space_topic"):
            continue
        topic_id = str(entry.get("topic_id") or "")
        should_delete = config.delete_done_council_topics() and _entry_is_council_topic(entry) and bool(topic_id)
        if should_delete and not runtime.dry_run and topic_id not in deleted_topic_ids and deletes_issued >= delete_cap:
            continue  # budget spent; retry this space's delete+prune next tick (record untouched)
        if should_delete and topic_id not in deleted_topic_ids:
            if runtime.dry_run:
                result["deleted"] += 1
                deleted_topic_ids.add(topic_id)
                result["changed"] = True
                continue
            deletes_issued += 1
            delete_topic_name = entry.get("topic_name")
            delete_operation = _capture_entry_operation(
                store, entry, topic_id=topic_id
            )
            execution = _execute_entry_operation(
                store,
                runtime.telegram,
                delete_operation,
                _provider_mutation(
                    "telegram.delete_topic",
                    reason=(
                        "telegram.delete_topic: delete done council space"
                    ),
                    args=(chat_id, topic_id),
                ),
            )
            deleted, delete_resolution = execution.result, execution.resolution
            if not deleted.get("ok"):
                if _topic_missing(deleted.get("error")):
                    cleared_worker_keys, _changed = (
                        _record_topic_delete_success(
                            store,
                            topic_id=topic_id,
                            name=delete_topic_name,
                            reason="done_council_space_topic",
                        )
                    )
                    deleted_topic_ids.add(topic_id)
                    finalize_deleted_space_worker_aliases(
                        cleared_worker_keys, "done_council_space_topic"
                    )
                    if delete_resolution.disposition == _OFFLOCK_APPLY:
                        assert delete_resolution.entry is not None
                        current_key, _kind = _entry_operation_key(
                            store, delete_resolution.entry
                        )
                        pop_space(current_key)
                        result["pruned"] += 1
                    result["changed"] = True
                    continue
                result["failed"] += 1
                if delete_resolution.disposition == _OFFLOCK_APPLY:
                    assert delete_resolution.entry is not None
                    delete_resolution.entry["last_topic_delete_error"] = (
                        compact_ws(deleted.get("error"), 240)
                    )
                continue
            result["deleted"] += 1
            deleted_topic_ids.add(topic_id)
            cleared_worker_keys, _changed = _record_topic_delete_success(
                store,
                topic_id=topic_id,
                name=delete_topic_name,
                reason="done_council_space_topic",
            )
        if not runtime.dry_run and should_delete:
            finalize_deleted_space_worker_aliases(
                cleared_worker_keys, "done_council_space_topic"
            )
            if delete_resolution.disposition == _OFFLOCK_APPLY:
                current_entry = delete_resolution.entry
                assert current_entry is not None
                current_key, _kind = _entry_operation_key(
                    store, current_entry
                )
                pop_space(current_key)
                result["pruned"] += 1
        elif not runtime.dry_run:
            # Snapshot absence is not proof that a long-lived space/topic was
            # deleted. Retain its identity and topic binding so a transient
            # source blip cannot orphan the live Telegram topic and remint a
            # duplicate when the same space returns.
            before_retained = (
                entry.get("binding_state"),
                entry.get("binding_topic_id"),
            )
            entry["binding_state"] = _BINDING_STATE_ABSENT
            entry.pop("binding_topic_id", None)
            result["changed"] = result["changed"] or before_retained != (
                entry.get("binding_state"),
                entry.get("binding_topic_id"),
            )
            continue
        result["changed"] = True
    return result


def _topic_cleanup_protected_ids(
    store: dict[str, Any], *, include_live: bool = True
) -> set[str]:
    """Topics that lifecycle cleanup must never close, delete, or reopen."""
    protected = {str(config.general_thread_id(store))}
    for entry in state.source_space_entries(store).values():
        topic_id = str(entry.get("topic_id") or "")
        if topic_id:
            # Space topics can route several panes; pane dormancy is never
            # sufficient evidence to close one.
            protected.add(topic_id)
    for entry in state.source_worker_entries(store).values():
        topic_id = str(entry.get("topic_id") or "")
        if not topic_id:
            continue
        if include_live and state.entry_is_routable(entry):
            protected.add(topic_id)
        if any(
            entry.get(field) is True
            for field in (
                "cleanup_protected",
                "dashboard_topic",
                "pinned_topic",
                "personal_space",
            )
        ):
            protected.add(topic_id)
    telegram = (
        store.get("telegram") if isinstance(store.get("telegram"), dict) else {}
    )
    for key, value in telegram.items():
        normalized_key = str(key).lower()
        if not any(
            marker in normalized_key
            for marker in ("general", "dashboard", "setup", "offer", "personal")
        ):
            continue
        if "topic" in normalized_key or "thread" in normalized_key:
            clean = str(value or "").strip()
            if clean:
                protected.add(clean)
    protected.discard("")
    return protected


def _topic_cleanup_empty_result() -> dict[str, Any]:
    return {
        "closed": 0,
        "deleted": 0,
        "reopened": 0,
        "abandoned": 0,
        "deferred": 0,
        "candidates": 0,
        "operations": 0,
        "would_close": 0,
        "would_delete": 0,
        "would_reopen": 0,
        "changed": False,
    }


def _topic_cleanup_attempts(store: dict[str, Any]) -> dict[str, int]:
    raw = store.get("telegram_topic_cleanup_attempts")
    if not isinstance(raw, dict):
        raw = {}
    return raw


def _topic_cleanup_abandoned(store: dict[str, Any]) -> set[str]:
    raw = store.get("telegram_topic_cleanup_abandoned")
    if not isinstance(raw, list):
        raw = []
    return {str(item) for item in raw if str(item)}


def _topic_cleanup_retry_after(store: dict[str, Any]) -> dict[str, float]:
    raw = store.get("telegram_topic_cleanup_retry_after")
    if not isinstance(raw, dict):
        raw = {}
    return raw


def _topic_cleanup_target_key(action: str, topic_id: str) -> str:
    return f"{action}:{topic_id}"


def _prune_topic_cleanup_tracking(store: dict[str, Any]) -> bool:
    """Drop retry/abandon state once its provider topic can no longer exist."""
    topic_ids = {
        str(entry.get("topic_id"))
        for entry in state.source_worker_entries(store).values()
        if entry.get("topic_id")
    }

    def stale(target_key: Any, *, drop_reopen: bool) -> bool:
        action, separator, topic_id = str(target_key).partition(":")
        return (
            not separator
            or not topic_id
            or (drop_reopen and action == "reopen")
            or topic_id not in topic_ids
        )

    changed = False
    attempts = _topic_cleanup_attempts(store)
    for target_key in list(attempts):
        if stale(target_key, drop_reopen=True):
            attempts.pop(target_key, None)
            changed = True
    retry_after = _topic_cleanup_retry_after(store)
    for target_key in list(retry_after):
        if stale(target_key, drop_reopen=False):
            retry_after.pop(target_key, None)
            changed = True
    abandoned = _topic_cleanup_abandoned(store)
    kept_abandoned = {
        target_key
        for target_key in abandoned
        if not stale(target_key, drop_reopen=True)
    }
    if kept_abandoned != abandoned:
        store["telegram_topic_cleanup_abandoned"] = sorted(kept_abandoned)
        changed = True
    return changed


def _refresh_topic_cleanup_lifecycle(
    store: dict[str, Any], *, now: float, dry_run: bool
) -> bool:
    """Persist the first observed closed/retired instant and clear it on revive."""
    if dry_run:
        return False
    changed = False
    for entry in state.source_worker_entries(store).values():
        if state.entry_is_retired(entry):
            if "routing_retired_at" not in entry:
                entry["routing_retired_at"] = now
                changed = True
            if (
                entry.get("retired_topic_closed") is True
                and entry.get("topic_id")
                and "topic_closed_at" not in entry
            ):
                # Pre-lifecycle RCs closed archives immediately. Adopt their
                # terminal marker instead of issuing one redundant close after
                # the newly introduced TTL.
                entry["topic_closed_at"] = now
                entry["topic_auto_closed_at"] = now
                changed = True
            continue
        if state.entry_is_routable(entry):
            if entry.pop("topic_dormant_at", None) is not None:
                changed = True
            continue
        if (
            normalized_status(
                entry.get("status") or entry.get("tendwire_raw_status")
            )
            in {"closed", "failed"}
            and entry.get("topic_id")
            and "topic_dormant_at" not in entry
        ):
            entry["topic_dormant_at"] = now
            changed = True
    return changed


def _topic_cleanup_targets(
    store: dict[str, Any], *, now: float, preview: bool = False
) -> tuple[list[dict[str, Any]], int, int]:
    statically_protected = _topic_cleanup_protected_ids(
        store, include_live=False
    )
    mutation_protected = _topic_cleanup_protected_ids(store)
    abandoned = {
        target_key
        for target_key in _topic_cleanup_abandoned(store)
        if not target_key.startswith("reopen:")
    }
    ttl_seconds = config.close_dormant_after_hours() * 3600.0
    cleanup_action = config.topic_cleanup_action()
    targets: list[dict[str, Any]] = []
    abandoned_eligible = 0
    delayed_eligible = 0
    retry_after = _topic_cleanup_retry_after(store)
    seen: set[tuple[str, str]] = set()

    def add_target(
        action: str,
        entry_key: str,
        entry: dict[str, Any],
        *,
        since: float,
        reason: str,
        protected: set[str],
    ) -> None:
        nonlocal abandoned_eligible, delayed_eligible
        topic_id = str(entry.get("topic_id") or "")
        dedup = (action, topic_id)
        if not topic_id or topic_id in protected or dedup in seen:
            return
        seen.add(dedup)
        target_key = _topic_cleanup_target_key(action, topic_id)
        if target_key in abandoned:
            abandoned_eligible += 1
            return
        try:
            next_attempt_at = float(retry_after.get(target_key) or 0)
        except (TypeError, ValueError):
            next_attempt_at = 0
        if next_attempt_at > now:
            delayed_eligible += 1
            return
        targets.append(
            {
                "action": action,
                "entry_key": entry_key,
                "topic_id": topic_id,
                "topic_name": compact_ws(entry.get("topic_name"), 120),
                "since": since,
                "reason": reason,
                "target_key": target_key,
                "_operation": _capture_entry_operation(
                    store, entry, topic_id=topic_id
                ),
            }
        )

    # Reopens are first so a revived pane is writable before this pass starts
    # delivering its turn feed.
    for entry_key, entry in state.source_worker_entries(store).items():
        if (
            not state.entry_is_retired(entry)
            and state.entry_is_routable(entry)
            and entry.get("topic_closed_at") is not None
        ):
            try:
                closed_at = float(entry.get("topic_closed_at"))
            except (TypeError, ValueError):
                continue
            add_target(
                "reopen",
                entry_key,
                entry,
                since=closed_at,
                reason="pane_revived",
                protected=statically_protected,
            )

    if ttl_seconds <= 0:
        return targets, abandoned_eligible, delayed_eligible
    for entry_key, entry in state.source_worker_entries(store).items():
        if cleanup_action == "close" and entry.get("topic_closed_at") is not None:
            continue
        if state.entry_is_retired(entry):
            if entry.get("retired_topic_notice_pending") or entry.get(
                "retired_topic_rename_pending"
            ):
                continue
            raw_since = entry.get("routing_retired_at")
            reason = "retired_topic_ttl"
        else:
            if state.entry_is_routable(entry):
                continue
            if normalized_status(
                entry.get("status") or entry.get("tendwire_raw_status")
            ) not in {"closed", "failed"}:
                continue
            raw_since = entry.get("topic_dormant_at")
            reason = "dormant_pane_ttl"
        try:
            since = float(raw_since)
        except (TypeError, ValueError):
            if not preview:
                continue
            # A dry run must reveal destructive candidates even before a real
            # pass has written its first-observed lifecycle watermark.
            since = now - ttl_seconds
        if now - since < ttl_seconds:
            continue
        add_target(
            cleanup_action,
            entry_key,
            entry,
            since=since,
            reason=reason,
            protected=mutation_protected,
        )
    return targets, abandoned_eligible, delayed_eligible


def _topic_cleanup_target_still_valid(
    store: dict[str, Any], target: dict[str, Any], *, now: float
) -> tuple[bool, dict[str, Any] | None]:
    operation = target.get("_operation")
    if not isinstance(operation, _OfflockEntryOperation):
        return False, None
    resolution = _compare_and_apply_entry_operation(store, operation)
    if resolution.disposition != _OFFLOCK_APPLY:
        return False, None
    entry = resolution.entry
    assert entry is not None
    topic_id = str(target["topic_id"])
    if (
        str(entry.get("topic_id") or "") != topic_id
        or topic_id
        in _topic_cleanup_protected_ids(
            store, include_live=target["action"] != "reopen"
        )
    ):
        return False, None
    if target["action"] == "reopen":
        valid = (
            not state.entry_is_retired(entry)
            and state.entry_is_routable(entry)
            and entry.get("topic_closed_at") is not None
        )
        return valid, entry if valid else None
    if target["action"] == "close" and entry.get("topic_closed_at") is not None:
        return False, None
    if state.entry_is_retired(entry) and (
        entry.get("retired_topic_notice_pending")
        or entry.get("retired_topic_rename_pending")
    ):
        return False, None
    ttl_seconds = config.close_dormant_after_hours() * 3600.0
    if ttl_seconds <= 0:
        return False, None
    raw_since = (
        entry.get("routing_retired_at")
        if state.entry_is_retired(entry)
        else entry.get("topic_dormant_at")
    )
    try:
        since = float(raw_since)
    except (TypeError, ValueError):
        return False, None
    if since != float(target["since"]) or now - since < ttl_seconds:
        return False, None
    valid = state.entry_is_retired(entry) or (
        not state.entry_is_routable(entry)
        and normalized_status(
            entry.get("status") or entry.get("tendwire_raw_status")
        )
        in {"closed", "failed"}
    )
    return valid, entry if valid else None


def _execute_topic_cleanup_targets(
    store: dict[str, Any],
    targets: list[dict[str, Any]],
    runtime: SyncRuntime,
    *,
    chat_id: str,
) -> tuple[list[dict[str, Any]], int]:
    budget = config.cleanup_budget_seconds()
    max_ops = config.cleanup_max_ops()
    if budget <= 0 or max_ops <= 0:
        return [], len(targets)
    deadline = time.monotonic() + budget
    outcomes: list[dict[str, Any]] = []
    for index, target in enumerate(targets):
        if len(outcomes) >= max_ops or time.monotonic() >= deadline:
            return outcomes, len(targets) - index
        try:
            action = str(target["action"])
            operation = target.get("_operation")
            if not isinstance(operation, _OfflockEntryOperation):
                raise TypeError("cleanup target lacks off-lock operation")

            capability = f"telegram.{action}_topic_for_cleanup"
            execution = _execute_entry_operation(
                store,
                runtime.telegram,
                operation,
                _provider_mutation(
                    capability,
                    reason=(
                        f"{capability}: lifecycle cleanup "
                        f"{target['topic_id']}"
                    ),
                    args=(chat_id, str(target["topic_id"])),
                ),
            )
            response = execution.result
        except RateLimited as exc:
            method = str(
                exc.method
                or _TELEGRAM_API_METHOD_BY_CAPABILITY.get(
                    capability, capability
                )
            )
            _record_telegram_backpressure(
                store,
                _telegram_rate_limit_failure(
                    status="telegram_rate_limited",
                    error=str(exc),
                    method=method,
                    retry_after=exc.retry_after,
                    retries=0,
                    events=[
                        {
                            "observed_at": time.time(),
                            "method": method,
                            "capability": capability,
                            "retry_after": exc.retry_after,
                            "observed_wait_seconds": 0.0,
                            "outcome": "dedicated_cooldown",
                        }
                    ],
                ),
            )
            outcomes.append(
                {
                    "target": target,
                    "status": "rate_limited",
                    "retry_after": exc.retry_after,
                    "received_at": time.time(),
                    "error": compact_ws(exc, 240),
                }
            )
            return outcomes, len(targets) - index - 1
        if response.get("rate_limited"):
            method = str(
                response.get("method")
                or _TELEGRAM_API_METHOD_BY_CAPABILITY.get(
                    capability, capability
                )
            )
            retry_after = int(response.get("retry_after") or 1)
            _record_telegram_backpressure(
                store,
                _telegram_rate_limit_failure(
                    status="telegram_rate_limited",
                    error=str(response.get("error") or ""),
                    method=method,
                    retry_after=retry_after,
                    retries=0,
                    events=[
                        {
                            "observed_at": time.time(),
                            "method": method,
                            "capability": capability,
                            "retry_after": retry_after,
                            "observed_wait_seconds": 0.0,
                            "outcome": "dedicated_cooldown",
                        }
                    ],
                ),
            )
            outcomes.append(
                {
                    "target": target,
                    "status": "rate_limited",
                    "retry_after": retry_after,
                    "received_at": time.time(),
                    "error": compact_ws(response.get("error"), 240),
                }
            )
            return outcomes, len(targets) - index - 1
        error = response.get("error")
        kind = "" if response.get("ok") else classify_telegram_error(error)
        if target["action"] == "close":
            success_kinds = {
                "topic_closed",
                "already_closed",
                "topic_not_found",
                "not_found",
            }
        elif target["action"] == "delete":
            success_kinds = {"topic_not_found", "not_found"}
        else:
            success_kinds = {
                "already_open",
                "not_modified",
                "topic_not_found",
                "not_found",
            }
        outcomes.append(
            {
                "target": target,
                "status": "success"
                if response.get("ok") or kind in success_kinds
                else "failed",
                "kind": kind,
                "topic_missing": kind in {
                    "topic_not_found",
                    "not_found",
                },
                "error": compact_ws(error, 240),
            }
        )
    return outcomes, 0


_DELETED_TOPIC_ENTRY_FIELDS = (
    "last_topic_icon",
    "last_topic_icon_id",
    "last_topic_icon_missing",
    "last_topic_icon_error",
    "pinned_status_message_id",
    "pinned_status_hash",
    "pinned_status_pinned",
    "pinned_status_last_error",
    "rename_attempts",
    "voice_reply_message_ids",
)


def _record_lifecycle_topic_deleted(
    entry: dict[str, Any],
    *,
    topic_id: str,
    reason: str,
    now: float,
) -> None:
    """Drop the provider identity while retaining the pane continuity row."""
    entry.pop("topic_id", None)
    entry["deleted_topic_id"] = topic_id
    entry["deleted_topic_reason"] = reason
    entry["topic_deleted_at"] = now
    entry.pop("topic_closed_at", None)
    entry.pop("topic_auto_closed_at", None)
    entry.pop("topic_reopened_at", None)
    entry.pop("topic_missing_at", None)
    entry.pop("retired_topic_close_pending", None)
    entry.pop("retired_topic_close_error", None)
    for field in _DELETED_TOPIC_ENTRY_FIELDS:
        entry.pop(field, None)
    if state.entry_is_retired(entry):
        entry.pop("retired_topic_closed", None)
        entry["retired_topic_deleted"] = True
        entry["retired_topic_missing"] = True


def _apply_topic_cleanup_outcomes(
    store: dict[str, Any],
    outcomes: list[dict[str, Any]],
    result: dict[str, Any],
    *,
    now: float,
) -> None:
    attempts = _topic_cleanup_attempts(store)
    retry_after = _topic_cleanup_retry_after(store)
    abandoned = _topic_cleanup_abandoned(store)
    audit = store.get("telegram_topic_cleanup_audit")
    if not isinstance(audit, list):
        audit = []
    for outcome in outcomes:
        target = outcome["target"]
        target_key = str(target["target_key"])
        if outcome["status"] == "rate_limited":
            try:
                current_backoff = float(
                    store.get("telegram_topic_cleanup_backoff_until") or 0
                )
            except (TypeError, ValueError):
                current_backoff = 0
            received_at = float(outcome.get("received_at") or time.time())
            store["telegram_topic_cleanup_backoff_until"] = max(
                current_backoff,
                received_at + float(outcome.get("retry_after") or 1),
            )
            result["operations"] += 1
            result["deferred"] += 1
            result["changed"] = True
            continue
        result["operations"] += 1
        target_still_valid, resolved_entry = _topic_cleanup_target_still_valid(
            store, target, now=now
        )
        if (
            outcome["status"] == "success"
            and target["action"] == "delete"
        ):
            # A provider-confirmed deletion remains true even when concurrent
            # state has rebound the selected pane. Invalidate every alias
            # before deciding whether the target-specific lifecycle mutation
            # is still safe to apply.
            _cleared_worker_keys, provider_fact_changed = (
                _record_topic_delete_success(
                    store,
                    topic_id=str(target["topic_id"]),
                    name=target.get("topic_name"),
                    reason=str(target["reason"]),
                )
            )
            result["changed"] = (
                provider_fact_changed or result["changed"]
            )
        elif (
            outcome["status"] == "success"
            and outcome.get("topic_missing") is True
        ):
            _repair_provider_gone_topic(
                store,
                state.source_worker_entries(store).get(
                    str(target["entry_key"])
                ),
                outcome,
                topic_id=str(target["topic_id"]),
            )
            result["changed"] = True
        if not target_still_valid:
            result["deferred"] += 1
            continue
        entry = resolved_entry
        assert entry is not None
        if outcome["status"] == "success":
            attempts.pop(target_key, None)
            retry_after.pop(target_key, None)
            kind = str(outcome.get("kind") or "")
            if target["action"] == "delete":
                _record_lifecycle_topic_deleted(
                    entry,
                    topic_id=str(target["topic_id"]),
                    reason=str(target["reason"]),
                    now=now,
                )
                result["deleted"] += 1
            elif target["action"] == "close":
                entry["topic_closed_at"] = now
                entry["topic_auto_closed_at"] = now
                entry.pop("retired_topic_close_pending", None)
                entry.pop("retired_topic_close_error", None)
                if state.entry_is_retired(entry):
                    entry["retired_topic_closed"] = True
                if kind in {"topic_not_found", "not_found"}:
                    entry.pop("topic_id", None)
                    entry.pop("topic_closed_at", None)
                    entry.pop("topic_auto_closed_at", None)
                    entry["topic_missing_at"] = now
                    if state.entry_is_retired(entry):
                        entry["retired_topic_missing"] = True
                result["closed"] += 1
            else:
                entry.pop("topic_closed_at", None)
                entry.pop("topic_auto_closed_at", None)
                entry["topic_reopened_at"] = now
                if kind in {"topic_not_found", "not_found"}:
                    entry.pop("topic_id", None)
                    entry["topic_missing_at"] = now
                result["reopened"] += 1
            audit.append(
                {
                    "action": str(target["action"]),
                    "at": now,
                    "entry_key": str(target["entry_key"]),
                    "pane_uuid": state.entry_pane_uuid(entry),
                    "reason": str(target["reason"]),
                    "result": kind or "ok",
                    "topic_id": str(target["topic_id"]),
                }
            )
            result["changed"] = True
            continue
        entry["topic_cleanup_last_error"] = str(outcome.get("error") or "")
        entry["topic_cleanup_last_error_at"] = now
        retry_after[target_key] = now + _TOPIC_CLEANUP_MIN_RETRY_SECONDS
        kind = str(outcome.get("kind") or "")
        if (
            target["action"] != "reopen"
            and kind in _TOPIC_CLEANUP_PERMANENT_ERROR_KINDS
        ):
            try:
                previous_attempts = int(attempts.get(target_key) or 0)
            except (TypeError, ValueError):
                previous_attempts = 0
            count = previous_attempts + 1
            attempts[target_key] = count
            if count >= _TOPIC_CLEANUP_ATTEMPT_CAP:
                abandoned.add(target_key)
                retry_after.pop(target_key, None)
                result["abandoned"] += 1
        result["changed"] = True
    store["telegram_topic_cleanup_attempts"] = attempts
    store["telegram_topic_cleanup_retry_after"] = retry_after
    store["telegram_topic_cleanup_abandoned"] = sorted(abandoned)
    store["telegram_topic_cleanup_audit"] = audit[-_TOPIC_CLEANUP_AUDIT_LIMIT:]


def _sync_topic_lifecycle_cleanup(
    store: dict[str, Any],
    runtime: SyncRuntime,
    *,
    chat_id: str,
    now: float | None = None,
) -> dict[str, Any]:
    """Run bounded close/reopen operations outside the state-lock hot path."""
    clock = time.time() if now is None else float(now)
    result = _topic_cleanup_empty_result()
    result["changed"] = _refresh_topic_cleanup_lifecycle(
        store, now=clock, dry_run=runtime.dry_run
    )
    if not runtime.dry_run:
        result["changed"] = _prune_topic_cleanup_tracking(store) or result["changed"]
    targets, abandoned_eligible, delayed_eligible = _topic_cleanup_targets(
        store, now=clock, preview=runtime.dry_run
    )
    result["candidates"] = (
        len(targets) + abandoned_eligible + delayed_eligible
    )
    result["abandoned"] = abandoned_eligible
    result["deferred"] = delayed_eligible
    if runtime.dry_run:
        result["would_close"] = sum(
            target["action"] == "close" for target in targets
        )
        result["would_delete"] = sum(
            target["action"] == "delete" for target in targets
        )
        result["would_reopen"] = sum(
            target["action"] == "reopen" for target in targets
        )
        return result
    try:
        backoff_until = float(
            store.get("telegram_topic_cleanup_backoff_until") or 0
        )
    except (TypeError, ValueError):
        backoff_until = 0
    if backoff_until > clock:
        result["deferred"] += len(targets)
        return result
    if store.pop("telegram_topic_cleanup_backoff_until", None) is not None:
        result["changed"] = True
    if not targets:
        return result

    outcomes, deferred = _execute_topic_cleanup_targets(
        store, targets, runtime, chat_id=chat_id
    )
    result["deferred"] += deferred
    _apply_topic_cleanup_outcomes(store, outcomes, result, now=clock)
    return result


def _clear_entry_message_reference(entry: dict[str, Any], message_id: str, kind: str) -> bool:
    changed = False
    if kind == "working" and str(entry.get("last_stream_message_id") or "") == message_id:
        changed = _clear_stream_delivery_keys(entry)
    if kind == "final":
        message_ids = entry.get("last_clean_message_ids")
        if isinstance(message_ids, list) and message_id in {str(item) for item in message_ids}:
            kept = [str(item) for item in message_ids if str(item) != message_id]
            if kept:
                entry["last_clean_message_ids"] = kept
                entry["last_clean_message_id"] = kept[0]
            else:
                _clear_final_delivery_keys(entry)
            changed = True
        elif str(entry.get("last_clean_message_id") or "") == message_id:
            changed = _clear_final_delivery_keys(entry) or changed
    return changed


def _repair_space_mode_routing_state(store: dict[str, Any]) -> int:
    if config.source_topic_mode() != "space":
        return 0
    repaired = 0
    topic_by_space = _source_space_topic_ids(store)
    for entry in state.source_worker_entries(store).values():
        space_id = _entry_space_id(entry)
        expected_topic = topic_by_space.get(space_id)
        actual_topic = compact_ws(entry.get("topic_id"), 80)
        if expected_topic and actual_topic and actual_topic != expected_topic:
            entry.pop("topic_id", None)
            repaired += 1
    bindings = state.message_bindings(store)
    for message_id, binding in list(bindings.items()):
        if not isinstance(binding, dict):
            continue
        space_id = compact_ws(binding.get("space_id"), 160)
        expected_topic = topic_by_space.get(space_id)
        actual_topic = compact_ws(binding.get("topic_id"), 80)
        if not expected_topic or not actual_topic or actual_topic == expected_topic:
            continue
        worker_id = compact_ws(binding.get("worker_id"), 160)
        kind = str(binding.get("kind") or "")
        if kind == "final" and binding.get("plan_token"):
            # Keep the private delivery coordinate long enough for a replacement plan to
            # converge/delete it, but quarantine it from reply routing immediately.
            binding["routing_quarantined"] = True
            repaired += 1
            continue
        for entry in state.source_worker_entries(store).values():
            if _entry_worker_id(entry) == worker_id and _entry_space_id(entry) == space_id:
                repaired += int(_clear_entry_message_reference(entry, str(message_id), kind))
        bindings.pop(str(message_id), None)
        repaired += 1
    return repaired


def _backfill_message_bindings(store: dict[str, Any]) -> int:
    before = set(state.message_bindings(store))
    for entry in state.source_worker_entries(store).values():
        if not state.entry_is_routable(entry):
            continue
        topic_id = str(entry.get("topic_id") or "")
        if not topic_id:
            _space_key, space_entry = state.find_space_entry_by_id(store, str(entry.get("tendwire_space_id") or entry.get("space_id") or ""))
            topic_id = str((space_entry or {}).get("topic_id") or "")
        if not topic_id:
            continue
        stream_id = str(entry.get("last_stream_message_id") or "")
        if stream_id and state.find_message_binding(store, stream_id) is None:
            state.bind_message_to_worker(
                store,
                stream_id,
                entry,
                topic_id=topic_id,
                kind="working",
                turn_id=str(entry.get("last_stream_turn_id") or ""),
                bot_kind=str(entry.get("last_stream_bot_kind") or ""),
                submission_id=str(entry.get("last_stream_submission_id") or ""),
            )
        final_ids = entry.get("last_clean_message_ids")
        if not isinstance(final_ids, list) or not final_ids:
            final_ids = [entry.get("last_clean_message_id")]
        for message_id in final_ids:
            if state.find_message_binding(store, message_id) is not None:
                continue
            state.bind_message_to_worker(
                store,
                message_id,
                entry,
                topic_id=topic_id,
                kind="final",
                turn_id=str(entry.get("last_turn_id") or ""),
                bot_kind=str(entry.get("last_clean_bot_kind") or ""),
            )
    return len(set(state.message_bindings(store)) - before)


def _deliver_working(
    store: dict[str, Any],
    item: dict[str, Any],
    entry: dict[str, Any],
    runtime: SyncRuntime,
    *,
    chat_id: str,
    reuse_previous_working: bool = False,
) -> bool:
    thread_id = str(entry.get("topic_id") or "")
    if not thread_id:
        return False
    delivery_item = _working_delivery_item(item)
    turn_id = _turn_id(item)
    submission_id = _stream_submission_id(item, entry)
    content_hash = _turn_content_hash(delivery_item, "working")
    feed_item = _turn_feed_item(delivery_item, entry)
    if entry.get("last_stream_turn_id") == turn_id and entry.get("last_stream_hash") == content_hash:
        return False
    now = time.time()
    if _same_turn_working_update_too_soon(entry, turn_id, now=now):
        return False
    if runtime.dry_run:
        _set_stream_delivery(
            entry,
            turn_id=turn_id,
            content_hash=content_hash,
            submission_id=submission_id,
            placeholder=True,
        )
        _record_stream_update_time(entry, now)
        return True
    if _delivery_write_budget(runtime).remaining == 0:
        return False
    telegram = _telegram_state(store)
    api_token, bot_kind = _delivery_bot(store, entry)
    stored_bot_kind = str(entry.get("last_stream_bot_kind") or MANAGER_BOT_KIND)
    edit_attempted = bool(
        entry.get("last_stream_message_id")
        and stored_bot_kind == bot_kind
        and (
            entry.get("last_stream_turn_id") == turn_id
            or reuse_previous_working
            or (
                submission_id
                and entry.get("last_stream_submission_id")
                == submission_id
            )
        )
    )
    operation = _capture_entry_operation(
        store,
        entry,
        topic_id=thread_id,
        message_id=str(entry.get("last_stream_message_id") or "")
        if edit_attempted
        else "",
        observe=(
            ("last_stream_message_id",)
            if edit_attempted
            else (
                "last_stream_message_id",
                "last_stream_turn_id",
                "last_stream_submission_id",
            )
        ),
    )
    if edit_attempted:
        execution = _execute_accounted_delivery_write(
            store,
            runtime,
            operation,
            _provider_mutation(
                "telegram.edit_feed_item",
                reason="telegram.edit_feed_item: update working card",
                args=(chat_id, operation.message_id, feed_item),
                kwargs={
                    "telegram": telegram,
                    "live": True,
                    "api_token": api_token,
                    "max_physical_writes": _delivery_write_budget(
                        runtime
                    ).remaining,
                },
            ),
        )
    else:
        execution = _execute_accounted_delivery_write(
            store,
            runtime,
            operation,
            _provider_mutation(
                "telegram.send_feed_item",
                reason="telegram.send_feed_item: create working card",
                args=(chat_id, feed_item),
                kwargs={
                    "telegram": telegram,
                    "thread_id": thread_id,
                    "notify": False,
                    "live": True,
                    "api_token": api_token,
                    "max_physical_writes": _delivery_write_budget(
                        runtime
                    ).remaining,
                },
            ),
        )
    sent, resolution = execution.result, execution.resolution
    if (
        edit_attempted
        and not sent.get("ok")
        and (
            str(sent.get("kind") or "")
            in {"not_found", "topic_not_found"}
            or classify_telegram_error(sent.get("error"))
            in {"not_found", "topic_not_found"}
        )
    ):
        # The exact historical message is gone regardless of where its pane
        # moved. Only an unchanged owner may turn that fact into a resend.
        _retire_local_message(store, None, operation.message_id)
        if resolution.disposition != _OFFLOCK_APPLY:
            return False
        entry = resolution.entry
        assert entry is not None
        _clear_stream_delivery_keys(entry)
        if (
            _delivery_write_budget(runtime).remaining == 0
        ):
            return False
        operation = _capture_entry_operation(
            store,
            entry,
            topic_id=thread_id,
            observe=(
                "last_stream_message_id",
                "last_stream_turn_id",
                "last_stream_submission_id",
            ),
        )
        execution = _execute_accounted_delivery_write(
            store,
            runtime,
            operation,
            _provider_mutation(
                "telegram.send_feed_item",
                reason=(
                    "telegram.send_feed_item: replace missing working card"
                ),
                args=(chat_id, feed_item),
                kwargs={
                    "telegram": telegram,
                    "thread_id": thread_id,
                    "notify": False,
                    "live": True,
                    "api_token": api_token,
                    "max_physical_writes": _delivery_write_budget(
                        runtime
                    ).remaining,
                },
            ),
        )
        sent, resolution = execution.result, execution.resolution
    if not sent.get("ok") and _topic_missing(sent.get("error")):
        _repair_provider_gone_topic(
            store,
            resolution.entry
            if resolution.disposition == _OFFLOCK_APPLY
            else None,
            sent,
            topic_id=operation.route_topic_id,
        )
        return False
    if sent.get("ok"):
        message_id = str(
            sent.get("message_id")
            or operation.message_id
            or ""
        )
        if resolution.disposition != _OFFLOCK_APPLY:
            if message_id:
                state.bind_message_to_worker(
                    store,
                    message_id,
                    _operation_binding_entry(operation),
                    topic_id=operation.route_topic_id,
                    kind="working_stale",
                    turn_id=turn_id,
                    bot_kind=bot_kind,
                    submission_id=submission_id,
                )
            # Preserve the exact provider id as presentation-retirement state;
            # never turn an accepted stale write into an untracked resend.
            return False
        entry = resolution.entry
        assert entry is not None
        _set_stream_delivery(
            entry,
            turn_id=turn_id,
            content_hash=content_hash,
            message_id=message_id,
            bot_kind=bot_kind,
            submission_id=submission_id,
        )
        _record_stream_update_time(entry, now)
        _record_delivery_success(entry, bot_kind)
        state.bind_message_to_worker(
            store,
            entry.get("last_stream_message_id"),
            entry,
            topic_id=thread_id,
            kind="working",
            turn_id=turn_id,
            bot_kind=bot_kind,
            submission_id=submission_id,
        )
        return True
    if resolution.disposition != _OFFLOCK_APPLY:
        return False
    entry = resolution.entry
    assert entry is not None
    _record_delivery_error(entry, sent, bot_kind)
    return False


def _refind_entry(store: dict[str, Any], entry_key: str | None) -> dict[str, Any] | None:
    if not entry_key:
        return None
    for bucket in ("panes", "spaces"):
        candidate = (store.get(bucket) or {}).get(entry_key)
        if isinstance(candidate, dict):
            return candidate
    return None


def _speak_reply(
    store: dict[str, Any],
    item: dict[str, Any],
    entry: dict[str, Any],
    entry_key: str | None,
    runtime: SyncRuntime,
    *,
    chat_id: str,
    thread_id: str,
    reply_to: str | None,
) -> dict[str, Any]:
    """Strictly additive (issue #4): after a final text turn is delivered, optionally speak it back as
    one or more Telegram voice notes (long replies are chunked). Fires on the one-shot speak_next_reply
    (owner replied to a voice note), the trigger phrase, or force-all. Never breaks the delivered text
    turn. Returns the entry to keep using (re-derived when we reload off-lock).

    Phase 2: SYNTHESIS + SEND run OFF the state lock. We commit the delivered turn first, drop the lock
    for the ~1-3s synth (no `store` mutation in that window), then reload — so a competitor's write
    during synth survives — and record the sent voice-note ids on the freshly-reloaded entry."""
    if runtime.dry_run:
        return entry  # preview pass: don't consume the flag or synth; the real send speaks
    want = bool(entry.pop("speak_next_reply", None))
    if not (want or speech.speech_reply_triggered(item.get("user_text")) or speech.speech_replies_enabled()):
        return entry
    chunks = speech.speech_reply_chunks(item.get("assistant_final_text") or item.get("assistant_stream_text") or "")
    if not chunks:
        return entry
    if _delivery_write_budget(runtime).remaining == 0:
        return entry
    api_token, _bot_kind = _delivery_bot(store, entry)
    operation = _capture_entry_operation(
        store,
        entry,
        topic_id=thread_id,
        message_id=str(reply_to or ""),
    )

    execution = _execute_accounted_delivery_write(
        store,
        runtime,
        operation,
        _provider_mutation(
            "telegram.send_voice_batch",
            reason="telegram.send_voice_batch: synthesize pane voice reply",
            args=(
                tuple(chunks),
                _turn_id(item),
                chat_id,
                thread_id,
                reply_to,
            ),
            kwargs={
                "max_physical_writes": _delivery_write_budget(
                    runtime
                ).remaining,
            },
            api_token=api_token,
        ),
    )
    voice_ids = (
        execution.result.get("accepted_message_ids", ())
        if isinstance(execution.result, dict)
        else execution.result
    )
    voice_ids = [str(voice_id) for voice_id in voice_ids]
    resolution = execution.resolution
    if resolution.disposition == _OFFLOCK_APPLY:
        target = resolution.entry
        assert target is not None
        for voice_id in voice_ids:
            state.record_voice_reply_message_id(target, voice_id)
        return target

    # Accepted voice notes are exact provider facts. Keep them bound to the
    # captured owner/route, but never write entry-level voice state onto a
    # reloaded owner after RECONCILE or ABANDON.
    binding_entry = _operation_binding_entry(operation)
    for voice_id in voice_ids:
        state.bind_message_to_worker(
            store,
            voice_id,
            binding_entry,
            topic_id=operation.route_topic_id,
            kind="voice_stale",
            turn_id=_turn_id(item),
            bot_kind=_bot_kind,
        )
        binding = state.find_message_binding(store, voice_id)
        if binding is not None:
            binding["reply_to_message_id"] = operation.message_id
            binding["provider_fact"] = "accepted_voice"
    return entry


def _content_revision(item: dict[str, Any]) -> str:
    content = item.get("content")
    if not isinstance(content, dict):
        return ""
    revision = content.get("content_revision")
    return revision if isinstance(revision, str) and revision.startswith("twrev1.") else ""


def _stage_final_plan(
    store: dict[str, Any],
    item: dict[str, Any],
    entry: dict[str, Any],
    runtime: SyncRuntime,
    *,
    source_ref: str | None = None,
) -> tuple[bool, int, dict[str, Any]]:
    revision = _content_revision(item)
    if not revision:
        return False, 0, entry

    def _prepare_begin_kwargs(
        part_count: int,
        presentation_version: str = PRESENTATION_VERSION,
    ) -> dict[str, Any]:
        kwargs: dict[str, Any] = {
            "turn_id": _turn_id(item),
            "content_revision": revision,
            "presentation_version": presentation_version,
            "part_count": part_count,
        }
        if source_ref is not None:
            kwargs["source_ref"] = source_ref
        return kwargs

    def _prepare_commit_kwargs(plan_token: str) -> dict[str, Any]:
        kwargs: dict[str, Any] = {"plan_token": plan_token}
        if source_ref is not None:
            kwargs["source_ref"] = source_ref
        return kwargs

    if (
        source_ref is None
        and entry.get("last_turn_id") == _turn_id(item)
        and entry.get("last_clean_content_revision") == revision
    ):
        return False, 0, entry
    if (
        entry.get("pending_turn_id") == _turn_id(item)
        and entry.get("pending_content_revision") == revision
        and isinstance(entry.get("pending_plan_token"), str)
        and str(entry.get("pending_plan_token")).startswith("twplan1.")
    ):
        pending_token = str(entry["pending_plan_token"])
        pending_count = entry.get("pending_turn_part_count")
        if (
            isinstance(pending_count, bool)
            or not isinstance(pending_count, int)
            or pending_count <= 0
        ):
            raise _TurnContentError(
                "invalid_pending_plan", "pending plan has invalid part count"
            )
        pending_presentation_version = entry.get(
            "pending_presentation_version"
        )
        if pending_presentation_version != PRESENTATION_VERSION:
            raise _TurnContentError(
                "invalid_pending_plan",
                "pending plan has an unsupported presentation version",
            )
        operation = _capture_entry_operation(
            store,
            entry,
            plan_token=pending_token,
            revision=revision,
        )
        execution = _execute_entry_operation(
            store,
            runtime.tendwire,
            operation,
            _provider_mutation(
                "tendwire.connector_prepare_begin",
                reason=(
                    "tendwire.connector_prepare_begin: reconcile pending plan"
                ),
                kwargs=_prepare_begin_kwargs(
                    pending_count,
                    pending_presentation_version,
                ),
            ),
        )
        observed, resolution = execution.result, execution.resolution
        if resolution.disposition != _OFFLOCK_APPLY:
            raise _TurnContentError(
                "stale_or_unroutable_turn_plan",
                "pending plan owner changed during reconciliation",
                conflict=True,
            )
        entry = resolution.entry
        assert entry is not None
        if observed.get("ok") is False or observed.get("plan_token") != pending_token:
            raise _TurnContentError(
                str(observed.get("status") or "prepare_failed"),
                str(observed.get("error") or "pending plan reconciliation failed"),
            )
        if source_ref is not None:
            operation = _capture_entry_operation(
                store,
                entry,
                plan_token=pending_token,
                revision=revision,
            )
            execution = _execute_entry_operation(
                store,
                runtime.tendwire,
                operation,
                _provider_mutation(
                    "tendwire.connector_prepare_commit",
                    reason=(
                        "tendwire.connector_prepare_commit: hand off pending plan"
                    ),
                    kwargs=_prepare_commit_kwargs(pending_token),
                ),
            )
            observed, resolution = execution.result, execution.resolution
            if resolution.disposition != _OFFLOCK_APPLY:
                raise _TurnContentError(
                    "stale_or_unroutable_turn_plan",
                    "pending plan owner changed during handoff",
                    conflict=True,
                )
            entry = resolution.entry
            assert entry is not None
            if observed.get("ok") is False or observed.get("plan_token") != pending_token:
                raise _TurnContentError(
                    str(observed.get("status") or "prepare_failed"),
                    str(observed.get("error") or "pending plan handoff failed"),
                    conflict=str(observed.get("status") or "")
                    in {"revision_conflict", "stale_revision", "stale_ref"},
                )
        if observed.get("state") == "completed" and _complete_turn_plan_from_bindings(
            store,
            item,
            entry,
            plan_token=pending_token,
            revision=revision,
            part_count=pending_count,
        ):
            _checkpoint_turn_job(runtime)
        return False, 0, entry

    page_calls = _materialize_turn_item(item, runtime)
    # Final plans are a lossless projection of Tendwire's canonical revision.
    # Repeated-prompt suppression is a working-card display policy; applying it
    # here drops a required user_text span and makes Tendwire reject the plan as
    # incomplete before any Telegram job can be created.
    feed_item = _canonical_final_feed_item(item, entry)
    parts = _prepare_final_delivery_parts(
        feed_item,
        rich_transport=config.rich_messages_enabled(),
    )
    _validate_final_plan_exact_coverage(item, parts)
    if not parts:
        raise _TurnContentError(
            "invalid_presentation_plan",
            "completed turn has no presentation parts",
        )
    if runtime.dry_run:
        _clear_abandoned_plan_handle(entry)
        _set_pending_turn_plan(
            entry,
            turn_id=_turn_id(item),
            revision=revision,
            plan_token="dry-run",
            part_count=len(parts),
            job_count=len(parts),
        )
        entry["pending_turn_user_hash"] = _turn_user_hash(item)
        entry["pending_plan_generation"] = 1
        entry["pending_presentation_version"] = PRESENTATION_VERSION
        entry.pop("pending_stream_submission_id", None)
        if entry.get("last_stream_submission_id"):
            entry["pending_stream_submission_id"] = str(
                entry["last_stream_submission_id"]
            )
        return True, page_calls, entry

    begin_operation = _capture_entry_operation(
        store,
        entry,
        observe=("pending_plan_token", "pending_content_revision"),
    )
    execution = _execute_entry_operation(
        store,
        runtime.tendwire,
        begin_operation,
        _provider_mutation(
            "tendwire.connector_prepare_begin",
            reason=(
                "tendwire.connector_prepare_begin: stage final delivery plan"
            ),
            kwargs=_prepare_begin_kwargs(len(parts)),
        ),
    )
    begin, begin_resolution = execution.result, execution.resolution
    if begin_resolution.disposition != _OFFLOCK_APPLY:
        raise _TurnContentError(
            "stale_or_unroutable_turn_plan",
            "plan owner changed during preparation",
            conflict=True,
        )
    entry = begin_resolution.entry
    assert entry is not None
    if begin.get("ok") is False:
        raise _TurnContentError(
            str(begin.get("status") or "prepare_failed"),
            str(begin.get("error") or "connector prepare begin failed"),
            conflict=str(begin.get("status") or "")
            in {"revision_conflict", "stale_revision", "stale_ref"},
        )
    plan_token = begin.get("plan_token")
    if not isinstance(plan_token, str) or not plan_token.startswith("twplan1."):
        raise _TurnContentError(
            "invalid_prepare_response",
            "prepare begin omitted a public plan token",
        )
    state_name = str(begin.get("state") or "")
    if state_name not in {
        "preparing",
        "active",
        "waiting_predecessor",
        "completed",
    }:
        raise _TurnContentError(
            "invalid_prepare_response", "prepare begin returned invalid state"
        )
    # Stamp the plan before creating part jobs. If part creation stops, the
    # ordinary pass barrier persists a named, ageing plan instead of leaving a
    # route-local pin with no clock.  Re-observing the same token never resets
    # the clock.
    begin_job_count = begin.get("job_count")
    _set_pending_turn_plan(
        entry,
        turn_id=_turn_id(item),
        revision=revision,
        plan_token=plan_token,
        part_count=len(parts),
        job_count=(
            int(begin_job_count)
            if isinstance(begin_job_count, int)
            and not isinstance(begin_job_count, bool)
            and begin_job_count >= 0
            else 0
        ),
    )
    # The plan checkpoint is the durable resume contract for every part job
    # and commit call below.  Persist the protocol version with that parent,
    # not only after a successful commit: if the request or transport stops
    # after prepare-part, the retry must reopen the same plan version/token.
    # Falling back to the legacy version here creates a second empty plan and
    # permanently rejects the source final.
    entry["pending_presentation_version"] = PRESENTATION_VERSION
    final_identity = item.get(_TURN_FINAL_IDENTITY_KEY)
    if isinstance(final_identity, str) and final_identity:
        entry["pending_final_identity"] = final_identity
    if state_name == "preparing":
        for ordinal, part in enumerate(parts):
            part_operation = _capture_entry_operation(
                store,
                entry,
                observe=(
                    "pending_plan_token",
                    "pending_content_revision",
                ),
            )
            execution = _execute_entry_operation(
                store,
                runtime.tendwire,
                part_operation,
                _provider_mutation(
                    "tendwire.connector_prepare_part",
                    reason=(
                        "tendwire.connector_prepare_part: stage final plan part"
                    ),
                    kwargs={
                        "plan_token": plan_token,
                        "ordinal": ordinal,
                        "spans": part["spans"],
                    },
                ),
            )
            response, part_resolution = execution.result, execution.resolution
            if part_resolution.disposition != _OFFLOCK_APPLY:
                raise _TurnContentError(
                    "stale_or_unroutable_turn_plan",
                    "plan owner changed during part preparation",
                    conflict=True,
                )
            entry = part_resolution.entry
            assert entry is not None
            if (
                response.get("ok") is False
                or response.get("plan_token") != plan_token
                or response.get("ordinal") != ordinal
            ):
                raise _TurnContentError(
                    str(response.get("status") or "prepare_failed"),
                    str(response.get("error") or "connector prepare part failed"),
                )
        commit_operation = _capture_entry_operation(
            store,
            entry,
            observe=("pending_plan_token", "pending_content_revision"),
        )
        execution = _execute_entry_operation(
            store,
            runtime.tendwire,
            commit_operation,
            _provider_mutation(
                "tendwire.connector_prepare_commit",
                reason=(
                    "tendwire.connector_prepare_commit: activate staged plan"
                ),
                kwargs=_prepare_commit_kwargs(plan_token),
            ),
        )
        commit, commit_resolution = execution.result, execution.resolution
        if commit_resolution.disposition != _OFFLOCK_APPLY:
            raise _TurnContentError(
                "stale_or_unroutable_turn_plan",
                "plan owner changed during commit",
                conflict=True,
            )
        entry = commit_resolution.entry
        assert entry is not None
    elif source_ref is not None:
        commit_operation = _capture_entry_operation(
            store,
            entry,
            observe=("pending_plan_token", "pending_content_revision"),
        )
        execution = _execute_entry_operation(
            store,
            runtime.tendwire,
            commit_operation,
            _provider_mutation(
                "tendwire.connector_prepare_commit",
                reason=(
                    "tendwire.connector_prepare_commit: activate handed-off plan"
                ),
                kwargs=_prepare_commit_kwargs(plan_token),
            ),
        )
        commit, commit_resolution = execution.result, execution.resolution
        if commit_resolution.disposition != _OFFLOCK_APPLY:
            raise _TurnContentError(
                "stale_or_unroutable_turn_plan",
                "plan owner changed during commit",
                conflict=True,
            )
        entry = commit_resolution.entry
        assert entry is not None
    else:
        commit = begin
    if commit.get("ok") is False or commit.get("plan_token") != plan_token:
        raise _TurnContentError(
            str(commit.get("status") or "prepare_failed"),
            str(commit.get("error") or "connector prepare commit failed"),
            conflict=str(commit.get("status") or "")
            in {"revision_conflict", "stale_revision", "stale_ref"},
        )
    committed_state = str(commit.get("state") or state_name)
    if committed_state not in {"active", "waiting_predecessor", "completed"}:
        raise _TurnContentError(
            "invalid_prepare_response",
            "prepare commit did not activate the plan",
        )
    job_count = commit.get("job_count")
    if committed_state != "completed" and (
        isinstance(job_count, bool)
        or not isinstance(job_count, int)
        or job_count < len(parts)
    ):
        raise _TurnContentError(
            "invalid_prepare_response",
            "prepare commit returned invalid job count",
        )
    generation = commit.get("generation", begin.get("generation", 1))
    if (
        isinstance(generation, bool)
        or not isinstance(generation, int)
        or generation < 1
    ):
        raise _TurnContentError(
            "invalid_prepare_response",
            "prepare response returned an invalid plan generation",
        )
    _clear_abandoned_plan_handle(entry)
    _set_pending_turn_plan(
        entry,
        turn_id=_turn_id(item),
        revision=revision,
        plan_token=plan_token,
        part_count=len(parts),
        job_count=int(job_count or 0),
    )
    entry["pending_turn_user_hash"] = _turn_user_hash(item)
    entry["pending_plan_generation"] = generation
    entry["pending_presentation_version"] = PRESENTATION_VERSION
    entry.pop("pending_stream_submission_id", None)
    if entry.get("last_stream_submission_id"):
        entry["pending_stream_submission_id"] = str(
            entry["last_stream_submission_id"]
        )
    final_identity = item.get(_TURN_FINAL_IDENTITY_KEY)
    if isinstance(final_identity, str) and final_identity:
        entry["pending_final_identity"] = final_identity
    return True, page_calls, entry


def _prepare_final_delivery_parts(
    feed_item: dict[str, Any],
    *,
    rich_transport: bool = False,
) -> list[dict[str, Any]]:
    """Turn expected content limits into a per-item outcome.

    Programming defects deliberately propagate to the loop-level systemic
    failure reporter. Quietly converting AttributeError, TypeError, ValueError,
    or another unexpected exception would make a lost final look healthy.
    """

    try:
        parts = prepare_turn_delivery_parts(
            feed_item,
            rich_transport=rich_transport,
        )
        # Keep canonical planning reconstructable at every size.  The cap is a
        # delivery policy: reject the whole logical send into an observable
        # terminal hold before any physical card is attempted.
        if len(parts) > TURN_DELIVERY_MAX_PARTS:
            raise PresentationOversizeError(len(parts))
        return parts
    except PresentationOversizeError as exc:
        raise _TurnContentError(
            "oversize_presentation",
            str(exc),
            part_count=exc.part_count,
        ) from exc
    except PresentationContentError as exc:
        raise _TurnContentError(
            "invalid_presentation_plan",
            str(exc),
        ) from exc


def _validate_final_plan_exact_coverage(
    item: dict[str, Any], parts: list[dict[str, Any]]
) -> None:
    """Reject lossy local plans before creating a durable Tendwire plan."""

    content = item.get("content")
    fields = content.get("fields") if isinstance(content, dict) else None
    if not isinstance(fields, dict):
        raise _TurnContentError(
            "invalid_presentation_plan",
            "completed turn has no canonical field descriptors",
        )
    spans_by_field: dict[str, list[tuple[int, int]]] = {}
    for part in parts:
        for span in part.get("spans") or []:
            field = str(span.get("field") or "")
            start = span.get("start_char")
            end = span.get("end_char")
            if (
                field not in {"user_text", "assistant_final_text"}
                or isinstance(start, bool)
                or not isinstance(start, int)
                or isinstance(end, bool)
                or not isinstance(end, int)
            ):
                raise _TurnContentError(
                    "invalid_presentation_plan",
                    "final presentation contains an invalid content span",
                )
            spans_by_field.setdefault(field, []).append((start, end))
    for field in ("user_text", "assistant_final_text"):
        descriptor = fields.get(field)
        if not isinstance(descriptor, dict):
            raise _TurnContentError(
                "invalid_presentation_plan",
                f"completed turn is missing {field} descriptor",
            )
        expected = (
            int(descriptor.get("char_length") or 0)
            if descriptor.get("availability") == "complete"
            else 0
        )
        spans = spans_by_field.get(field, [])
        cursor = 0
        for start, end in spans:
            if start != cursor or end <= start or end > expected:
                raise _TurnContentError(
                    "invalid_presentation_plan",
                    f"final presentation does not exactly cover {field}",
                )
            cursor = end
        if cursor != expected:
            raise _TurnContentError(
                "invalid_presentation_plan",
                f"final presentation does not exactly cover {field}",
            )


def _notify_unbound_final(
    store: dict[str, Any],
    item: dict[str, Any],
    entry: dict[str, Any],
    runtime: SyncRuntime,
    *,
    chat_id: str,
) -> int:
    """Send one bounded General notice instead of dropping a live final."""

    binding_state = compact_ws(
        entry.get("binding_state"), 160
    ) or _BINDING_STATE_PENDING_CREATE
    # Identity consolidation and quarantine are routing work in progress, not
    # failed topic provisioning.  Their owner-visible surface is the pinned
    # board plus doctor; sending here would violate the consolidation lane's
    # no-delivery contract and could emit once per duplicate claimant.  A
    # General notice is reserved for a pane whose identity is already usable
    # but whose topic still cannot be created.
    notice_eligible = (
        binding_state == _BINDING_STATE_PENDING_CREATE
        or binding_state.startswith("create_error:")
    )
    if (
        runtime.dry_run
        or entry.get("live_in_snapshot") is not True
        or str(entry.get("topic_id") or "")
        or entry.get("binding_state") == _BINDING_STATE_BOUND
        or not notice_eligible
        or _delivery_write_budget(runtime).remaining <= 0
    ):
        return 0
    now = time.time()
    prior = entry.get("unbound_final_notice_at")
    if (
        isinstance(prior, (int, float))
        and not isinstance(prior, bool)
        and now - float(prior)
        < config.unbound_final_notice_cooldown_seconds()
    ):
        return 0
    worker_id = _entry_worker_id(entry) or "unknown pane"
    turn_id = _turn_id(item)
    html = (
        "<b>Live pane has no Telegram topic</b>\n"
        f"{html_escape(worker_id, 160)} has a completed turn waiting "
        f"({html_escape(binding_state, 160)})."
    )
    operation = _capture_entry_operation(
        store,
        entry,
        topic_id="",
        observe=("unbound_final_notice_at",),
    )
    execution = _execute_accounted_delivery_write(
        store,
        runtime,
        operation,
        _provider_mutation(
            "telegram.send_message",
            reason=(
                "telegram.send_message: expose unbound live pane final"
            ),
            args=(chat_id, html),
            kwargs={
                "thread_id": str(config.general_thread_id(store)),
                "notify": False,
                "max_physical_writes": _delivery_write_budget(
                    runtime
                ).remaining,
            },
        ),
    )
    sent, resolution = execution.result, execution.resolution
    writes = _telegram_physical_writes(sent)
    if resolution.disposition != _OFFLOCK_APPLY:
        return writes
    current = resolution.entry
    assert current is not None
    if sent.get("ok"):
        current["unbound_final_notice_at"] = now
        current["unbound_final_notice_turn_id"] = turn_id
        current["unbound_final_notice_message_id"] = str(
            sent.get("message_id") or ""
        )
        current.pop("unbound_final_notice_error", None)
    else:
        current["unbound_final_notice_error"] = compact_ws(
            sent.get("error") or sent.get("kind"), 240
        )
    return writes


def _unbound_live_entry_for_item(
    store: dict[str, Any], item: dict[str, Any]
) -> dict[str, Any] | None:
    """Resolve a notice owner without weakening delivery routability."""

    identity = _turn_stable_identity(item)
    if identity is not None:
        identity_matches = [
            entry
            for entry in state.source_worker_entries(store).values()
            if entry.get("live_in_snapshot") is True
            and state.entry_stable_identity(entry) == identity
        ]
        if len(identity_matches) == 1:
            return identity_matches[0]
    worker_id = compact_ws(item.get("worker_id"), 160)
    matches = [
        entry
        for entry in state.source_worker_entries(store).values()
        if entry.get("live_in_snapshot") is True
        and _entry_worker_id(entry) == worker_id
    ]
    return matches[0] if worker_id and len(matches) == 1 else None


def _deliver_pending(
    store: dict[str, Any],
    item: dict[str, Any],
    runtime: SyncRuntime,
    *,
    chat_id: str,
) -> bool:
    key, entry = _entry_for_turn(store, item)
    if key is None or entry is None:
        return False
    thread_id = str(entry.get("topic_id") or "")
    if not thread_id:
        return False
    pending_id = compact_ws(item.get("id") or item.get("pending_id") or item.get("turn_id"), 200)
    content_hash = short_hash({"pending": pending_id, "text": item.get("prompt_text") or item.get("text")}, 20)
    identity = f"pending:{pending_id}:{content_hash}"
    if identity in state.delivered_turns(store):
        return False
    html = render_pending(item, entry)
    if runtime.dry_run:
        return state.mark_delivered(store, identity, {"worker_id": entry.get("tendwire_worker_id"), "pending_id": pending_id})
    if _delivery_write_budget(runtime).remaining == 0:
        return False
    api_token, bot_kind = _delivery_bot(store, entry)
    operation = _capture_entry_operation(
        store, entry, topic_id=thread_id
    )
    execution = _execute_accounted_delivery_write(
        store,
        runtime,
        operation,
        _provider_mutation(
            "telegram.send_message",
            reason="telegram.send_message: deliver pending interaction",
            args=(chat_id, html),
            kwargs={
                "thread_id": thread_id,
                "notify": True,
                "max_physical_writes": _delivery_write_budget(
                    runtime
                ).remaining,
            },
            api_token=api_token,
        ),
    )
    sent, resolution = execution.result, execution.resolution
    if sent.get("ok"):
        if resolution.disposition != _OFFLOCK_APPLY:
            message_id = str(sent.get("message_id") or "")
            if message_id:
                state.bind_message_to_worker(
                    store,
                    message_id,
                    _operation_binding_entry(operation),
                    topic_id=thread_id,
                    kind="pending_stale",
                    turn_id=pending_id,
                    bot_kind=bot_kind,
                )
            return False
        entry = resolution.entry
        assert entry is not None
        entry["last_prompt_bot_kind"] = bot_kind
        _record_delivery_success(entry, bot_kind)
        state.bind_message_to_worker(store, sent.get("message_id"), entry, topic_id=thread_id, kind="pending", turn_id=pending_id, bot_kind=bot_kind)
        return state.mark_delivered(store, identity, {"worker_id": entry.get("tendwire_worker_id"), "pending_id": pending_id})
    if _topic_missing(sent.get("error")):
        _repair_provider_gone_topic(
            store,
            resolution.entry
            if resolution.disposition == _OFFLOCK_APPLY
            else None,
            sent,
            topic_id=thread_id,
        )
    elif resolution.disposition == _OFFLOCK_APPLY:
        assert resolution.entry is not None
        _record_delivery_error(resolution.entry, sent, bot_kind)
    return False


def _sync_turns(
    store: dict[str, Any],
    turns_payload: dict[str, Any],
    pending_payload: dict[str, Any],
    runtime: SyncRuntime,
    *,
    relist_on_conflict: bool = True,
    chat_id: str,
    live_worker_ids: set[str] | None = None,
    yield_barrier: Any | None = None,
    checkpoint_after_delivery: bool = False,
    retained_projection: dict[str, Any] | None = None,
) -> dict[str, int]:
    def retained_turn_for_entry(
        turn_id: str, entry_key: str | None
    ) -> dict[str, Any] | None:
        if not isinstance(retained_projection, dict) or not turn_id or not entry_key:
            return None
        candidate = retained_projection.get(turn_id)
        if not isinstance(candidate, dict):
            return None
        candidate_entry_key, _candidate_entry = _entry_for_turn(store, candidate)
        return candidate if candidate_entry_key == entry_key else None

    counts = {
        "feed_sent": 0,
        "sent": 0,
        "updated": 0,
        "content_pages": 0,
        "physical_writes": 0,
        "work_pending": 0,
        "failed_writes": 0,
        "response_fold_attempted": 0,
        "response_folded": 0,
        "response_fold_failed": 0,
    }
    budget = _delivery_write_budget(runtime)
    budget_start = budget.spent
    turns = _turns(turns_payload)
    if live_worker_ids is not None:
        # Retired-worker turns must not be delivered (same rule already applied
        # to status aggregation), except that a row from a previous positional
        # generation remains eligible when its stable key still owns a live
        # route. That exception is the restart catch-up path.
        turns = [
            item
            for item in turns
            if compact_ws(item.get("worker_id"), 160) in live_worker_ids
            or (
                (identity := _turn_stable_identity(item)) is not None
                and state.find_entry_key_by_stable_key(
                    store, identity[0]
                )
                is not None
            )
        ]
    latest_content_turn_by_worker: dict[str, str] = {}
    placeholder_turn_ids_by_worker: dict[str, set[str]] = {}
    # Pass 1: real content only (user prompt, stream, or a completed final).
    # Tendwire store output is already ordered by per-worker observed recency.
    # Payload updated_at can be absent on current worker-derived turns, so do
    # not let an older command row with updated_at suppress the live turn.
    for item in turns:
        if _turn_has_content_outcome(item):
            continue
        _key, entry = _entry_for_turn(store, item)
        if entry is None:
            continue
        worker_key = str(entry.get("tendwire_worker_id") or item.get("worker_id") or "")
        if not worker_key:
            continue
        if not _turn_has_real_content(item):
            continue
        latest_content_turn_by_worker.setdefault(worker_key, _turn_id(item))
    # Pass 2: synthetic "Work is in progress." placeholders only fill workers
    # with no real turn at all — a placeholder must never outrank a real turn.
    for item in turns:
        if _turn_has_content_outcome(item):
            continue
        _key, entry = _entry_for_turn(store, item)
        if entry is None:
            continue
        worker_key = str(entry.get("tendwire_worker_id") or item.get("worker_id") or "")
        if not worker_key:
            continue
        if _turn_is_working_placeholder(item, entry):
            placeholder_turn_ids_by_worker.setdefault(worker_key, set()).add(
                _turn_id(item)
            )
        if worker_key in latest_content_turn_by_worker:
            continue
        if _turn_is_working_placeholder(item, entry):
            latest_content_turn_by_worker.setdefault(worker_key, _turn_id(item))
    # Fairness is pass-to-pass round-robin. A failing logical turn may consume
    # the remaining allowance in this pass, but the durable cursor starts the
    # next pass immediately after it so the same turn cannot starve every
    # independent owner indefinitely.
    cursor = compact_ws(store.get("telegram_delivery_turn_cursor"), 200)
    delivery_turns = list(turns)
    if cursor:
        cursor_indexes = [
            index
            for index, candidate in enumerate(delivery_turns)
            if _turn_id(candidate) == cursor
        ]
        if cursor_indexes:
            split_at = cursor_indexes[-1] + 1
            delivery_turns = (
                delivery_turns[split_at:] + delivery_turns[:split_at]
            )
    seen_working_workers: set[str] = set()
    fold_state: dict[str, int] = {
        "issued": 0,
        "attempted": 0,
        "folded": 0,
        "failed": 0,
        "changed": 0,
    }
    fairness_cursor_candidate = ""
    turn_count = len(delivery_turns)
    for idx, item in enumerate(delivery_turns):
        if _turn_has_content_outcome(item):
            continue
        entry_key, entry = _entry_for_turn(store, item)
        if entry is None:
            if _turn_has_complete_final(item):
                notice_entry = _unbound_live_entry_for_item(
                    store, item
                )
                if notice_entry is not None:
                    writes_before = budget.spent
                    if budget.remaining:
                        _notify_unbound_final(
                            store,
                            item,
                            notice_entry,
                            runtime,
                            chat_id=chat_id,
                        )
                    writes_used = budget.spent - writes_before
                    if writes_used:
                        counts["physical_writes"] = (
                            budget.spent - budget_start
                        )
                        counts["failed_writes"] += 1
                    counts["work_pending"] += 1
            continue
        before = dict(entry)
        repaired_open_final = False
        worker_key = str(entry.get("tendwire_worker_id") or item.get("worker_id") or "")
        latest_turn_id = latest_content_turn_by_worker.get(worker_key)
        complete = _turn_has_complete_final(item)
        if complete:
            if latest_turn_id and _turn_id(item) != latest_turn_id:
                writes_before = budget.spent
                counts["updated"] += int(_fold_superseded_final(store, item, entry, runtime, chat_id=chat_id, fold_state=fold_state))
                if budget.spent > writes_before:
                    counts["physical_writes"] = budget.spent - budget_start
                continue
            if _content_revision(item):
                continue
            continue
        elif item.get("assistant_stream_text") or _turn_is_working_placeholder(item, entry):
            if latest_turn_id and _turn_id(item) != latest_turn_id:
                continue
            if worker_key in seen_working_workers:
                continue
            previous_stream_turn_id = str(entry.get("last_stream_turn_id") or "")
            previous_item = retained_turn_for_entry(
                previous_stream_turn_id, entry_key
            )
            current_is_placeholder = _turn_is_working_placeholder(item, entry)
            previous_has_real_content = bool(
                isinstance(previous_item, dict)
                and _turn_has_real_content(previous_item)
            )
            previous_is_placeholder = bool(
                isinstance(previous_item, dict)
                and _turn_is_working_placeholder(previous_item, entry)
            )
            if (
                current_is_placeholder
                and previous_stream_turn_id != _turn_id(item)
                and previous_has_real_content
            ):
                # A delta page contains only changed rows. Do not let an
                # isolated synthetic status row displace the retained real
                # turn and force a new Telegram message on the next update.
                continue
            seen_working_workers.add(worker_key)
            repaired_open_final = _clear_open_turn_final_delivery_state(
                store, entry, _turn_id(item)
            )
            reuse_previous_working = bool(
                item.get("assistant_stream_text")
                and previous_stream_turn_id
                and previous_stream_turn_id != _turn_id(item)
                and (
                    previous_stream_turn_id
                    in placeholder_turn_ids_by_worker.get(worker_key, set())
                    or previous_is_placeholder
                )
            )
            writes_before = budget.spent
            delivered = (
                _deliver_working(
                    store,
                    item,
                    entry,
                    runtime,
                    chat_id=chat_id,
                    reuse_previous_working=reuse_previous_working,
                )
                if budget.remaining
                else False
            )
            writes_used = budget.spent - writes_before
            if writes_used:
                counts["physical_writes"] = budget.spent - budget_start
            if not delivered and (writes_used or not budget.remaining):
                counts["work_pending"] += 1
                counts["failed_writes"] += int(bool(writes_used))
                if writes_used:
                    fairness_cursor_candidate = _turn_id(item)
        else:
            delivered = False
        counts["feed_sent"] += int(delivered)
        counts["sent"] += int(delivered)
        counts["updated"] += int((not delivered and before != entry) or (repaired_open_final and not delivered))
        if (
            delivered
            and checkpoint_after_delivery
            and runtime.checkpoint is not None
        ):
            # Persist the Telegram identity/ledger before the delta cursor or
            # watermark. A replay can then reapply the page without resending.
            runtime.checkpoint()
        # Only turns that changed the store did a Telegram send (the slow part). After such a turn,
        # yield the state lock so a queued inbound command can interleave instead of stalling behind
        # the rest of the loop. The barrier commits `store` under the lock, releases briefly, then
        # reloads in place — so a competitor's write survives and `entry` is re-derived fresh next
        # iteration (no detached reference). Skip after the last turn (nothing left to unblock for).
        if yield_barrier is not None and (delivered or before != entry) and idx + 1 < turn_count:
            yield_barrier()
    pending_items = _pending(pending_payload)
    pending_count = len(pending_items)
    for p_idx, item in enumerate(pending_items):
        writes_before = budget.spent
        delivered = (
            _deliver_pending(store, item, runtime, chat_id=chat_id)
            if budget.remaining
            else False
        )
        writes_used = budget.spent - writes_before
        counts["physical_writes"] = budget.spent - budget_start
        if not delivered and (writes_used or not budget.remaining):
            counts["work_pending"] += 1
            counts["failed_writes"] += int(bool(writes_used))
        counts["feed_sent"] += int(delivered)
        counts["sent"] += int(delivered)
        if (
            delivered
            and checkpoint_after_delivery
            and runtime.checkpoint is not None
        ):
            runtime.checkpoint()
        # Same yield between delivered pending prompts (each is a send under the lock).
        if yield_barrier is not None and delivered and p_idx + 1 < pending_count:
            yield_barrier()
    if fairness_cursor_candidate and counts["work_pending"] > 1:
        store["telegram_delivery_turn_cursor"] = fairness_cursor_candidate
    counts["response_fold_attempted"] = fold_state["attempted"]
    counts["response_folded"] = fold_state["folded"]
    counts["response_fold_failed"] = fold_state["failed"]
    return counts


def _after_provider_accept(runtime: SyncRuntime) -> None:
    if runtime.after_provider_accept is not None:
        runtime.after_provider_accept()

def _checkpoint_turn_job(runtime: SyncRuntime) -> None:
    if runtime.checkpoint is not None:
        runtime.checkpoint()


_PUBLIC_OPAQUE_CHARS = frozenset(
    "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_"
)
_PUBLIC_LABEL_CHARS = _PUBLIC_OPAQUE_CHARS | frozenset(".")


def _strict_public_opaque(value: Any, prefix: str, field: str) -> str:
    if (
        not isinstance(value, str)
        or not value.startswith(prefix)
        or not value[len(prefix) :]
        or any(
            char not in _PUBLIC_OPAQUE_CHARS
            for char in value[len(prefix) :]
        )
        or len(value) > 200
    ):
        raise _TurnContentError(
            "invalid_turn_final_job", f"invalid public {field}"
        )
    return value


def _strict_public_label(
    value: Any, field: str, *, prefix: str | None = None
) -> str:
    if (
        not isinstance(value, str)
        or not value
        or len(value) > 200
        or (prefix is not None and not value.startswith(prefix))
        or any(char not in _PUBLIC_LABEL_CHARS for char in value)
    ):
        raise _TurnContentError(
            "invalid_turn_final_job", f"invalid public {field}"
        )
    return value


def _validate_ready_descriptor(
    value: Any,
    field: str,
    *,
    final_required: bool = False,
) -> dict[str, Any]:
    expected = {
        "availability",
        "inline",
        "char_length",
        "byte_length",
        "page_count",
        "first_cursor",
    }
    if not isinstance(value, dict) or set(value) != expected:
        raise _TurnContentError(
            "invalid_turn_final_job",
            f"invalid {field} descriptor shape",
        )
    availability = value.get("availability")
    inline = value.get("inline")
    char_length = value.get("char_length")
    byte_length = value.get("byte_length")
    page_count = value.get("page_count")
    cursor = value.get("first_cursor")
    if (
        type(inline) is not bool
        or inline
        or any(
            isinstance(number, bool)
            or not isinstance(number, int)
            or number < 0
            for number in (char_length, byte_length, page_count)
        )
        or byte_length < char_length
    ):
        raise _TurnContentError(
            "invalid_turn_final_job",
            f"invalid {field} descriptor values",
        )
    if availability == "absent":
        if (
            final_required
            or char_length
            or byte_length
            or page_count
            or cursor is not None
        ):
            raise _TurnContentError(
                "invalid_turn_final_job",
                f"inconsistent absent {field}",
            )
    elif availability == "complete":
        if char_length == 0:
            if byte_length or page_count or cursor is not None:
                raise _TurnContentError(
                    "invalid_turn_final_job",
                    f"inconsistent empty {field}",
                )
        elif (
            page_count <= 0
            or not isinstance(cursor, str)
            or not cursor.startswith("twcur1.")
            or not cursor[7:]
            or any(
                char not in _PUBLIC_OPAQUE_CHARS for char in cursor[7:]
            )
        ):
            raise _TurnContentError(
                "invalid_turn_final_job",
                f"unpageable complete {field}",
            )
    else:
        raise _TurnContentError(
            "invalid_turn_final_job",
            f"invalid {field} availability",
        )
    return value


def _validate_final_ready_payload(
    payload: Any,
    *,
    delivery_key: str | None = None,
) -> dict[str, Any]:
    base_fields = {
        "schema_version",
        "operation",
        "final_identity",
        "turn_id",
        "worker_id",
        "space_id",
        "content_revision",
        "content",
    }
    schema = (
        payload.get("schema_version")
        if isinstance(payload, dict)
        else None
    )
    expected = (
        base_fields | {"stable_key", "stable_key_version"}
        if type(schema) is int and schema == 2
        else base_fields
    )
    if (
        type(schema) is int
        and schema == 2
        and isinstance(payload, dict)
        and "working_predecessor_turn_id" in payload
    ):
        expected.add("working_predecessor_turn_id")
    if (
        not isinstance(payload, dict)
        or set(payload) != expected
        or type(schema) is not int
        or schema not in {1, 2}
        or payload.get("operation") != "materialize"
    ):
        raise _TurnContentError(
            "invalid_turn_final_job",
            "final-ready payload shape is invalid",
        )
    if schema == 2 and not state.valid_stable_worker_key_pair(
        payload.get("stable_key"),
        payload.get("stable_key_version"),
    ):
        raise _TurnContentError(
            "invalid_turn_final_job",
            "final-ready stable worker identity is invalid",
        )
    predecessor = payload.get("working_predecessor_turn_id")
    if predecessor is not None:
        _strict_public_label(
            predecessor,
            "working predecessor turn id",
            prefix="turn-",
        )
        if predecessor == payload.get("turn_id"):
            raise _TurnContentError(
                "invalid_turn_final_job",
                "working predecessor must differ from final turn",
            )
    final_identity = _strict_public_opaque(
        payload.get("final_identity"),
        "twfinal1.",
        "final identity",
    )
    if (
        delivery_key is not None
        and delivery_key != f"turn-final:revision:{final_identity}"
    ):
        raise _TurnContentError(
            "invalid_turn_final_job",
            "final-ready delivery key is inconsistent",
        )
    _strict_public_label(
        payload.get("turn_id"), "turn id", prefix="turn-"
    )
    _strict_public_label(payload.get("worker_id"), "worker id")
    if payload.get("space_id") is not None:
        _strict_public_label(payload.get("space_id"), "space id")
    _strict_public_opaque(
        payload.get("content_revision"),
        "twrev1.",
        "content revision",
    )
    content = payload.get("content")
    if (
        not isinstance(content, dict)
        or set(content)
        != {
            "schema_version",
            "content_revision",
            "known_incomplete",
            "fields",
        }
        or type(content.get("schema_version")) is not int
        or content.get("schema_version") != TURN_CONTENT_SCHEMA_VERSION
        or content.get("content_revision")
        != payload["content_revision"]
        or content.get("known_incomplete") is not False
        or not isinstance(content.get("fields"), dict)
        or set(content["fields"])
        != {"user_text", "assistant_final_text"}
    ):
        raise _TurnContentError(
            "invalid_turn_final_job",
            "final-ready content descriptor is invalid",
        )
    _validate_ready_descriptor(
        content["fields"]["user_text"], "user_text"
    )
    _validate_ready_descriptor(
        content["fields"]["assistant_final_text"],
        "assistant_final_text",
        final_required=True,
    )
    return payload


_TURN_FINAL_SOURCE_OWNERS_KEY = "tendwire_turn_final_source_owners"
_TURN_FINAL_IDENTITY_KEY = "_herdres_final_identity"
_TURN_FINAL_WORKING_PREDECESSOR_KEY = (
    "_herdres_working_predecessor_turn_id"
)


def _public_turn_stable_identity(
    item: dict[str, Any],
) -> tuple[str, int] | None:
    stable_key = item.get("stable_key")
    stable_key_version = item.get("stable_key_version")
    if stable_key is None and stable_key_version is None:
        meta = item.get("meta")
        if isinstance(meta, dict):
            stable_key = meta.get("stable_key")
            stable_key_version = meta.get("stable_key_version")
    if not state.valid_stable_worker_key_pair(
        stable_key, stable_key_version
    ):
        return None
    return str(stable_key), int(stable_key_version)


def _final_ready_row(payload: dict[str, Any]) -> dict[str, Any]:
    row = {
        "id": payload["turn_id"],
        "worker_id": payload["worker_id"],
        "space_id": payload["space_id"],
        "complete": True,
        _TURN_FINAL_IDENTITY_KEY: payload["final_identity"],
        "content": {
            **payload["content"],
            "content_revision": payload["content_revision"],
        },
    }
    identity = _public_turn_stable_identity(payload)
    if identity is not None:
        row["stable_key"] = identity[0]
        row["stable_key_version"] = identity[1]
    validated = _validate_turn_row(row)
    predecessor = payload.get("working_predecessor_turn_id")
    if isinstance(predecessor, str):
        validated[_TURN_FINAL_WORKING_PREDECESSOR_KEY] = predecessor
    return validated


def _turn_final_source_owners(
    store: dict[str, Any], *, create: bool = False
) -> dict[str, Any]:
    owners = store.get(_TURN_FINAL_SOURCE_OWNERS_KEY)
    if isinstance(owners, dict):
        return owners
    if not create:
        return {}
    owners = {}
    store[_TURN_FINAL_SOURCE_OWNERS_KEY] = owners
    return owners


def _canonical_final_source_owner(
    record: Mapping[str, Any],
) -> dict[str, Any] | None:
    if not isinstance(record, Mapping):
        return None
    turn_id = record.get("turn_id")
    content_revision = record.get("content_revision")
    stable_key = record.get("stable_key")
    stable_key_version = record.get("stable_key_version")
    if (
        not isinstance(turn_id, str)
        or not turn_id
        or not isinstance(content_revision, str)
        or not content_revision
        or not state.valid_stable_worker_key_pair(
            stable_key, stable_key_version
        )
    ):
        return None
    return {
        "turn_id": turn_id,
        "content_revision": content_revision,
        "stable_key": stable_key,
        "stable_key_version": stable_key_version,
    }


def _resolve_final_source_entry(
    store: dict[str, Any],
    item: dict[str, Any],
) -> tuple[str | None, dict[str, Any] | None]:
    identity = _public_turn_stable_identity(item)
    if identity is None:
        return None, None
    entry_key, worker_entry = state.find_worker_entry_by_stable_key(
        store, identity[0]
    )
    if (
        entry_key is None
        or worker_entry is None
        or state.entry_stable_identity(worker_entry) != identity
        or not state.worker_entry_is_uniquely_routable(
            store, entry_key, worker_entry
        )
    ):
        return None, None
    if config.source_topic_mode() == "worker":
        return entry_key, worker_entry
    _space_key, space_entry = state.find_space_entry_by_id(
        store, _entry_space_id(worker_entry)
    )
    if space_entry is None:
        return None, None
    return entry_key, _delivery_entry(store, space_entry, worker_entry)


def _bind_or_verify_final_source_owner(
    store: dict[str, Any],
    payload: dict[str, Any],
    entry_key: str | None,
    entry: dict[str, Any],
    *,
    allow_bind: bool,
) -> tuple[bool, bool]:
    identity = _public_turn_stable_identity(payload)
    if (
        identity is None
        or not entry_key
        or state.entry_stable_identity(entry) != identity
        or not state.worker_entry_is_uniquely_routable(
            store, entry_key, entry
        )
    ):
        return False, False
    record = {
        "turn_id": payload["turn_id"],
        "content_revision": payload["content_revision"],
        "stable_key": identity[0],
        "stable_key_version": identity[1],
    }
    final_identity = str(payload["final_identity"])
    owners = _turn_final_source_owners(store)
    if final_identity in owners:
        existing = _canonical_final_source_owner(
            owners[final_identity]
        )
        if existing is None or existing != record:
            return False, False
        if owners[final_identity] != record:
            owners[final_identity] = record
            return True, True
        return True, False
    if not allow_bind:
        return False, False
    owners = _turn_final_source_owners(store, create=True)
    owners[final_identity] = record
    return True, True


def _clear_final_source_owner(
    store: dict[str, Any], final_identity: Any
) -> None:
    if not isinstance(final_identity, str) or not final_identity:
        return
    owners = store.get(_TURN_FINAL_SOURCE_OWNERS_KEY)
    if not isinstance(owners, dict):
        return
    owners.pop(final_identity, None)
    if not owners:
        store.pop(_TURN_FINAL_SOURCE_OWNERS_KEY, None)


def _turn_item_by_revision(
    turns_payload: dict[str, Any], revision: str
) -> dict[str, Any] | None:
    matches = [item for item in _turns(turns_payload) if _content_revision(item) == revision]
    return matches[0] if len(matches) == 1 else None


def _slot_binding(
    store: dict[str, Any],
    *,
    turn_id: str,
    ordinal: int,
    plan_token: str,
) -> tuple[str, dict[str, Any]] | tuple[None, None]:
    matches = [
        (message_id, binding)
        for message_id, binding in _final_delivery_bindings(store, turn_id)
        if binding.get("plan_token") == plan_token
        and binding.get("part_ordinal") == ordinal
    ]
    return matches[0] if len(matches) == 1 else (None, None)


def _exact_job_binding(
    store: dict[str, Any],
    *,
    job_key: str,
    turn_id: str,
    revision: str,
    plan_token: str,
    ordinal: int,
    part_count: int,
) -> tuple[str, dict[str, Any]] | tuple[None, None]:
    matches = [
        (message_id, binding)
        for message_id, binding in _final_delivery_bindings(store, turn_id)
        if binding.get("tendwire_job_key") == job_key
        and binding.get("content_revision") == revision
        and binding.get("plan_token") == plan_token
        and binding.get("part_ordinal") == ordinal
        and binding.get("part_count") == part_count
    ]
    if len(matches) > 1:
        raise _TurnContentError(
            "invalid_turn_final_job",
            "multiple Telegram messages claim the same final-delivery job",
        )
    return matches[0] if matches else (None, None)


def _owning_bot_token(store: dict[str, Any], bot_kind: str) -> str | None:
    if not bot_kind or bot_kind == MANAGER_BOT_KIND:
        return None
    token = _legacy_presenter_bot_token(store, bot_kind)
    if not token:
        raise _TurnContentError(
            "missing_message_owner_token",
            f"cannot mutate a message owned by unavailable bot kind {bot_kind}",
        )
    return token


def _retire_local_message(
    store: dict[str, Any],
    entry: dict[str, Any] | None,
    message_id: str,
) -> None:
    state.message_bindings(store).pop(str(message_id), None)
    candidates = (
        [entry]
        if entry is not None
        else list(state.source_worker_entries(store).values())
        + list(state.source_space_entries(store).values())
    )
    for candidate in candidates:
        _clear_entry_message_reference(candidate, str(message_id), "final")
        if candidate.get("last_stream_message_id") == str(message_id):
            _clear_stream_delivery_keys(candidate)


def _current_upsert_candidate(
    store: dict[str, Any],
    item: dict[str, Any],
    entry: dict[str, Any],
    *,
    ordinal: int,
    replaces_plan_token: str,
) -> tuple[str, str, str, str]:
    turn_id = _turn_id(item)
    if ordinal == 0:
        working_id = str(entry.get("last_stream_message_id") or "")
        predecessor = str(
            item.get(_TURN_FINAL_WORKING_PREDECESSOR_KEY)
            or entry.get("pending_working_predecessor_turn_id")
            or ""
        )
        stream_turn = str(entry.get("last_stream_turn_id") or "")
        if working_id and stream_turn in {turn_id, predecessor}:
            binding = state.find_message_binding(store, working_id)
            return (
                working_id,
                str(
                    (binding or {}).get("bot_kind")
                    or entry.get("last_stream_bot_kind")
                    or MANAGER_BOT_KIND
                ),
                str((binding or {}).get("topic_id") or entry.get("topic_id") or ""),
                "working",
            )
    if replaces_plan_token:
        message_id, binding = _slot_binding(
            store,
            turn_id=turn_id,
            ordinal=ordinal,
            plan_token=replaces_plan_token,
        )
        if message_id and binding:
            return (
                message_id,
                str(binding.get("bot_kind") or MANAGER_BOT_KIND),
                str(binding.get("topic_id") or ""),
                "final",
            )
    return "", "", "", ""


def _clear_pending_turn_plan(
    store: dict[str, Any], entry: dict[str, Any]
) -> None:
    _clear_final_source_owner(store, entry.get("pending_final_identity"))
    for field in (
        "pending_turn_id",
        "pending_content_revision",
        "pending_plan_token",
        "pending_turn_part_count",
        "pending_turn_job_count",
        "pending_turn_started_at",
        "pending_turn_user_hash",
        "pending_stream_submission_id",
        "pending_plan_generation",
        "pending_presentation_version",
        "pending_final_identity",
        "pending_working_predecessor_turn_id",
    ):
        entry.pop(field, None)


def _complete_turn_plan_from_bindings(
    store: dict[str, Any],
    item: dict[str, Any],
    entry: dict[str, Any],
    *,
    plan_token: str,
    revision: str,
    part_count: int,
) -> bool:
    selected: list[tuple[str, dict[str, Any]]] = []
    for ordinal in range(part_count):
        matches = [
            (message_id, binding)
            for message_id, binding in _final_delivery_bindings(
                store, _turn_id(item)
            )
            if binding.get("plan_token") == plan_token
            and binding.get("content_revision") == revision
            and binding.get("part_ordinal") == ordinal
            and binding.get("part_count") == part_count
        ]
        if len(matches) != 1:
            return False
        selected.append(matches[0])
    message_ids = [message_id for message_id, _binding in selected]
    canonical_message_id = message_ids[0]
    for _message_id, binding in selected:
        binding["message_ids"] = list(message_ids)
        binding["canonical_message_id"] = canonical_message_id
    bot_kind = str(selected[0][1].get("bot_kind") or MANAGER_BOT_KIND)
    state.mark_delivered(
        store,
        f"final:{_turn_id(item)}:{revision}",
        {
            "worker_id": entry.get("tendwire_worker_id"),
            "turn_id": _turn_id(item),
            "content_revision": revision,
            "message_ids": list(message_ids),
            "canonical_message_id": canonical_message_id,
        },
    )
    stream_submission_id = str(
        entry.get("pending_stream_submission_id") or ""
    )
    _set_final_delivery(
        entry,
        turn_id=_turn_id(item),
        content_hash=revision,
        user_hash=str(entry.get("pending_turn_user_hash") or "")
        or _turn_user_hash(item),
        message_ids=message_ids,
        bot_kind=bot_kind,
        render_version=RENDER_VERSION,
    )
    entry["last_clean_content_revision"] = revision
    entry["last_clean_plan_token"] = plan_token
    _clear_pending_turn_plan(store, entry)
    if stream_submission_id:
        _clear_stream_delivery_keys(entry)
    else:
        _clear_stream_delivery_state(entry, _turn_id(item))
    _record_delivery_success(entry, bot_kind)
    return True


def _clear_abandoned_plan_handle(entry: dict[str, Any]) -> None:
    for field in (
        "abandoned_plan_token",
        "abandoned_content_revision",
        "abandoned_turn_id",
        "abandoned_turn_part_count",
        "abandoned_turn_job_count",
        "abandoned_plan_generation",
        "abandoned_replaces_failed_plan_token",
    ):
        entry.pop(field, None)



def _validate_turn_final_item(item: dict[str, Any]) -> dict[str, Any]:
    key = item.get("key")
    ref = item.get("ref")
    if not isinstance(key, str):
        raise _TurnContentError(
            "invalid_turn_final_job",
            "turn-final delivery key is invalid",
        )
    _strict_public_opaque(ref, "twref1.", "lease ref")
    payload = item.get("payload")
    if key.startswith("turn-final:revision:"):
        return _validate_final_ready_payload(
            payload, delivery_key=key
        )
    if not key.startswith("turn-final:twplan1.") or not isinstance(
        payload, dict
    ):
        raise _TurnContentError(
            "invalid_turn_final_job",
            "turn-final lease identity is invalid",
        )
    required = {
        "schema_version",
        "plan_token",
        "content_revision",
        "presentation_version",
        "operation",
        "sequence_index",
        "part_ordinal",
        "part_count",
        "spans",
    }
    if (
        not required.issubset(payload)
        or type(payload.get("schema_version")) is not int
        or payload.get("schema_version") != 1
    ):
        raise _TurnContentError(
            "invalid_turn_final_job",
            "turn-final payload is incomplete",
        )
    plan_token = _strict_public_opaque(
        payload.get("plan_token"), "twplan1.", "plan token"
    )
    revision = _strict_public_opaque(
        payload.get("content_revision"),
        "twrev1.",
        "content revision",
    )
    sequence = payload.get("sequence_index")
    ordinal = payload.get("part_ordinal")
    part_count = payload.get("part_count")
    if (
        payload.get("presentation_version") != PRESENTATION_VERSION
        or payload.get("operation") not in {"upsert", "retire"}
        or any(
            isinstance(value, bool)
            or not isinstance(value, int)
            or value < 0
            for value in (sequence, ordinal)
        )
        or isinstance(part_count, bool)
        or not isinstance(part_count, int)
        or part_count <= 0
        or key != f"turn-final:{plan_token}:{sequence:06d}"
    ):
        raise _TurnContentError(
            "invalid_turn_final_job",
            "turn-final payload identity is inconsistent",
        )
    if "turn" in payload:
        source = _validate_final_ready_payload(payload.get("turn"))
        if source["content_revision"] != revision:
            raise _TurnContentError(
                "invalid_turn_final_job",
                "turn-final plan and source revisions differ",
            )
    replaces = payload.get("replaces_plan_token")
    if replaces is not None:
        _strict_public_opaque(
            replaces, "twplan1.", "replaced plan token"
        )
    predecessor_job_key = payload.get("predecessor_job_key")
    if predecessor_job_key is not None and (
        not isinstance(predecessor_job_key, str)
        or not predecessor_job_key.startswith("turn-final:twplan1.")
        or predecessor_job_key == key
    ):
        raise _TurnContentError(
            "invalid_turn_final_job",
            "turn-final predecessor receipt identity is invalid",
        )
    spans = payload.get("spans")
    operation = payload.get("operation")
    if (
        not isinstance(spans, list)
        or (
            operation == "upsert"
            and (ordinal >= part_count or not spans)
        )
        or (operation == "retire" and (ordinal < part_count or spans))
    ):
        raise _TurnContentError(
            "invalid_turn_final_job",
            "turn-final operation coordinates are invalid",
        )
    for span in spans:
        if (
            not isinstance(span, dict)
            or set(span) != {"field", "start_char", "end_char"}
            or span.get("field")
            not in {"user_text", "assistant_final_text"}
            or isinstance(span.get("start_char"), bool)
            or not isinstance(span.get("start_char"), int)
            or isinstance(span.get("end_char"), bool)
            or not isinstance(span.get("end_char"), int)
            or span["start_char"] < 0
            or span["end_char"] <= span["start_char"]
        ):
            raise _TurnContentError(
                "invalid_turn_final_job",
                "turn-final span is invalid",
            )
    return payload




_TURN_FINAL_FAILURE_REASON_CODES = frozenset(
    {
        "content_fetch_failed",
        "content_known_incomplete",
        "content_revision_not_found",
        "delivery_rejected",
        "delivery_uncertain",
        "invalid_content_page",
        "invalid_content_schema",
        "invalid_pending_plan",
        "invalid_prepare_response",
        "invalid_presentation_plan",
        "oversize_presentation",
        "invalid_recovery_predecessor_receipt",
        "invalid_turn_final_job",
        "missing_message_owner_token",
        "plan_incomplete",
        "prepare_failed",
        "presentation_plan_mismatch",
        "receipt_reservation_failed",
        "revision_conflict",
        "stale_or_unavailable_content_revision",
        "stale_or_unroutable_turn_plan",
        "stale_ref",
        "stale_revision",
        "timeout",
        "unsupported_content_schema",
        "unroutable_final_ready",
        "upgrade_required",
    }
)
_TURN_FINAL_DEFER_REASON_CODES = frozenset(
    {
        "operation_budget_exhausted",
        "predecessor_pending",
        "rate_limited",
        "transient_delivery",
    }
)
_TURN_FINAL_DEFER_REASON_ALIASES = {
    "earlier presentation plan is still pending": "predecessor_pending",
    "edit target unavailable; retry as send": "transient_delivery",
    "physical operation budget exhausted": "operation_budget_exhausted",
}


def _turn_final_reason_code(
    reason: str,
    *,
    uncertain: bool = False,
    deferred: bool = False,
) -> str:
    if uncertain:
        return "delivery_uncertain"
    candidate = str(reason).split(":", 1)[0].strip()
    if deferred:
        if candidate in _TURN_FINAL_DEFER_REASON_CODES:
            return candidate
        return _TURN_FINAL_DEFER_REASON_ALIASES.get(
            str(reason), "transient_delivery"
        )
    if candidate in _TURN_FINAL_FAILURE_REASON_CODES:
        return candidate
    return "delivery_rejected"


def _fail_turn_final(
    runtime: SyncRuntime,
    ref: str,
    reason: str,
    result: dict[str, Any],
    *,
    uncertain: bool = False,
) -> None:
    reason_code = _turn_final_reason_code(
        reason, uncertain=uncertain
    )
    response = _execute_exact_provider_operation(
        runtime.tendwire,
        mutation=_provider_mutation(
            "tendwire.turn_final_fail",
            reason="tendwire.turn_final_fail: reject exact leased final",
            args=(ref, reason_code),
        ),
    )
    result["failed"] += 1
    result["changed"] = True
    result["status"] = reason_code
    response_status = str(response.get("status") or "")
    if response_status in {"attempts_exhausted", "dead_letter"}:
        result["status"] = response_status
        result["_terminal_failure"] = True
    if uncertain:
        result["uncertain"] += 1
        result["status"] = "delivery_uncertain"
    if response.get("ok") is False:
        result["status"] = str(
            response.get("status") or "turn_final_fail_failed"
        )


def _telegram_result_is_transient(result: dict[str, Any]) -> bool:
    if result.get("rate_limited") is True:
        return True
    if str(result.get("kind") or "") == "transient":
        return True
    error = str(result.get("error") or "").lower()
    return any(marker in error for marker in ("rate limit", "too many requests", "retry after"))


def _telegram_physical_writes(
    result: dict[str, Any],
    *,
    default: int = 1,
) -> int:
    raw = result.get("physical_writes")
    if (
        isinstance(raw, int)
        and not isinstance(raw, bool)
        and raw >= 0
    ):
        return raw
    return default


def _telegram_result_retry_after(
    result: dict[str, Any],
    *,
    default: int = 1,
) -> int:
    try:
        return max(1, int(result.get("retry_after") or default))
    except (TypeError, ValueError):
        return max(1, int(default))


def _defer_turn_final(
    runtime: SyncRuntime,
    ref: str,
    reason: str,
    result: dict[str, Any],
    *,
    delay_seconds: int,
) -> None:
    response = _execute_exact_provider_operation(
        runtime.tendwire,
        mutation=_provider_mutation(
            "tendwire.turn_final_defer",
            reason="tendwire.turn_final_defer: defer exact leased final",
            args=(ref, _turn_final_reason_code(reason, deferred=True)),
            kwargs={"delay_seconds": max(1, int(delay_seconds))},
        ),
    )
    result["deferred"] += 1
    result["changed"] = True
    if response.get("ok") is False:
        result["status"] = str(
            response.get("status") or "turn_final_defer_failed"
        )


def _turn_final_item_source(
    payload: dict[str, Any],
    turns_payload: dict[str, Any],
    turn_projection: Mapping[str, Any] | None,
) -> dict[str, Any]:
    source = payload.get("turn")
    if isinstance(source, dict):
        return _final_ready_row(source)
    revision = str(payload["content_revision"])
    item = _turn_item_by_revision(turns_payload, revision)
    if item is None and isinstance(turn_projection, Mapping):
        matches = [
            candidate
            for candidate in turn_projection.values()
            if isinstance(candidate, dict)
            and _content_revision(candidate) == revision
        ]
        item = matches[0] if len(matches) == 1 else None
    if item is None:
        raise _TurnContentError(
            "stale_or_unavailable_content_revision",
            "turn-final source revision is unavailable",
        )
    return item


def _ack_turn_final(
    runtime: SyncRuntime,
    ref: str,
    job_key: str,
    result: dict[str, Any],
) -> bool:
    ack = _execute_exact_provider_operation(
        runtime.tendwire,
        mutation=_provider_mutation(
            "tendwire.turn_final_ack",
            reason="tendwire.turn_final_ack: acknowledge applied final",
            args=(ref, {"outcome": "applied", "job_key": job_key}),
        ),
    )
    if ack.get("ok") is False:
        result["status"] = str(
            ack.get("status") or "turn_final_ack_failed"
        )
        result["changed"] = True
        return False
    result["delivered"] += 1
    result["acked"] += 1
    result["changed"] = True
    return True


def _delete_final_message(
    store: dict[str, Any],
    runtime: SyncRuntime,
    *,
    chat_id: str,
    message_id: str,
    bot_kind: str,
) -> dict[str, Any]:
    token = _owning_bot_token(store, bot_kind)
    return _execute_exact_provider_operation(
        runtime.telegram,
        store=store,
        mutation=_provider_mutation(
            "telegram.delete_turn_delivery_message",
            reason="telegram.delete_turn_delivery_message: retire final slot",
            args=(chat_id, message_id),
            kwargs={"api_token": token},
        ),
    )


def _retire_bound_predecessor(
    store: dict[str, Any],
    runtime: SyncRuntime,
    binding: dict[str, Any],
    result: dict[str, Any],
    *,
    chat_id: str,
    ref: str,
    max_operations: int,
) -> bool:
    message_id = str(binding.get("superseded_message_id") or "")
    if not message_id:
        return True
    if result["operations"] >= max_operations:
        _defer_turn_final(
            runtime,
            ref,
            "delivery_budget_exhausted",
            result,
            delay_seconds=1,
        )
        return False
    bot_kind = str(
        binding.get("superseded_bot_kind") or MANAGER_BOT_KIND
    )
    try:
        deleted = _delete_final_message(
            store,
            runtime,
            chat_id=chat_id,
            message_id=message_id,
            bot_kind=bot_kind,
        )
        result["operations"] += 1
    except RateLimited as exc:
        _defer_turn_final(
            runtime,
            ref,
            "rate_limited",
            result,
            delay_seconds=exc.retry_after,
        )
        return False
    except Exception:
        _defer_turn_final(
            runtime,
            ref,
            "transient_delivery",
            result,
            delay_seconds=1,
        )
        return False
    if not deleted.get("ok"):
        if _telegram_result_is_transient(deleted):
            _defer_turn_final(
                runtime,
                ref,
                "transient_delivery",
                result,
                delay_seconds=_telegram_result_retry_after(deleted),
            )
        else:
            _fail_turn_final(
                runtime,
                ref,
                str(deleted.get("error") or "delivery_rejected"),
                result,
            )
        return False
    _retire_local_message(store, None, message_id)
    binding.pop("superseded_message_id", None)
    binding.pop("superseded_bot_kind", None)
    _checkpoint_turn_job(runtime)
    return True


def _retire_quarantined_job_binding(
    store: dict[str, Any],
    runtime: SyncRuntime,
    message_id: str,
    binding: dict[str, Any],
    result: dict[str, Any],
    *,
    chat_id: str,
    ref: str,
    max_operations: int,
) -> bool:
    if result["operations"] >= max_operations:
        _defer_turn_final(
            runtime,
            ref,
            "operation_budget_exhausted",
            result,
            delay_seconds=1,
        )
        return False
    try:
        deleted = _delete_final_message(
            store,
            runtime,
            chat_id=chat_id,
            message_id=message_id,
            bot_kind=str(
                binding.get("bot_kind") or MANAGER_BOT_KIND
            ),
        )
        result["operations"] += 1
    except RateLimited as exc:
        _defer_turn_final(
            runtime,
            ref,
            "rate_limited",
            result,
            delay_seconds=exc.retry_after,
        )
        return False
    except TelegramError:
        _defer_turn_final(
            runtime,
            ref,
            "transient_delivery",
            result,
            delay_seconds=1,
        )
        return False
    if not deleted.get("ok") and not _message_missing(
        deleted.get("error")
    ):
        if _telegram_result_is_transient(deleted):
            _defer_turn_final(
                runtime,
                ref,
                "transient_delivery",
                result,
                delay_seconds=_telegram_result_retry_after(deleted),
            )
        else:
            _fail_turn_final(
                runtime,
                ref,
                str(deleted.get("error") or "delivery_rejected"),
                result,
            )
        return False
    _retire_local_message(store, None, message_id)
    _checkpoint_turn_job(runtime)
    _defer_turn_final(
        runtime,
        ref,
        "transient_delivery",
        result,
        delay_seconds=1,
    )
    return False


def _apply_retire_job(
    store: dict[str, Any],
    runtime: SyncRuntime,
    item: dict[str, Any],
    payload: dict[str, Any],
    result: dict[str, Any],
    *,
    chat_id: str,
    ref: str,
) -> bool:
    ordinal = int(payload["part_ordinal"])
    message_id, binding = _slot_binding(
        store,
        turn_id=_turn_id(item),
        ordinal=ordinal,
        plan_token=str(payload.get("replaces_plan_token") or ""),
    )
    if not message_id:
        return True
    try:
        deleted = _delete_final_message(
            store,
            runtime,
            chat_id=chat_id,
            message_id=message_id,
            bot_kind=str(
                (binding or {}).get("bot_kind") or MANAGER_BOT_KIND
            ),
        )
        result["operations"] += 1
    except RateLimited as exc:
        _defer_turn_final(
            runtime,
            ref,
            "rate_limited",
            result,
            delay_seconds=exc.retry_after,
        )
        return False
    except Exception:
        _defer_turn_final(
            runtime,
            ref,
            "transient_delivery",
            result,
            delay_seconds=1,
        )
        return False
    if not deleted.get("ok"):
        if _telegram_result_is_transient(deleted):
            _defer_turn_final(
                runtime,
                ref,
                "transient_delivery",
                result,
                delay_seconds=_telegram_result_retry_after(deleted),
            )
        else:
            _fail_turn_final(
                runtime,
                ref,
                str(deleted.get("error") or "delivery_rejected"),
                result,
            )
        return False
    _retire_local_message(store, None, message_id)
    _checkpoint_turn_job(runtime)
    _after_provider_accept(runtime)
    return True


def _send_or_edit_final_part(
    store: dict[str, Any],
    runtime: SyncRuntime,
    source_item: dict[str, Any],
    feed_item: dict[str, Any],
    entry: dict[str, Any],
    payload: dict[str, Any],
    plan: dict[str, Any],
    result: dict[str, Any],
    *,
    chat_id: str,
    max_operations: int,
) -> tuple[
    dict[str, Any],
    _OfflockEntryOperation,
    _OfflockEntryResolution,
    str,
    str,
    str,
]:
    ordinal = int(payload["part_ordinal"])
    candidate_id, candidate_bot, candidate_topic, _kind = (
        _current_upsert_candidate(
            store,
            source_item,
            entry,
            ordinal=ordinal,
            replaces_plan_token=str(
                payload.get("replaces_plan_token") or ""
            ),
        )
    )
    desired_token, desired_bot = _delivery_bot(store, entry)
    route_topic = str(entry.get("topic_id") or "")
    compatible = bool(
        candidate_id
        and candidate_bot == desired_bot
        and candidate_topic == route_topic
    )
    operation = _capture_entry_operation(
        store,
        entry,
        topic_id=route_topic,
        message_id=candidate_id if compatible else "",
        plan_token=str(payload["plan_token"]),
        revision=str(payload["content_revision"]),
    )
    kwargs: dict[str, Any] = {
        "telegram": _telegram_state(store),
        "api_token": desired_token,
        "max_physical_writes": max(
            1, max_operations - result["operations"]
        ),
    }
    if not compatible:
        kwargs.update({"thread_id": route_topic, "notify": False})
    if compatible:
        execution = _execute_entry_operation(
            store,
            runtime.telegram,
            operation,
            _provider_mutation(
                "telegram.edit_turn_delivery_part",
                reason=(
                    "telegram.edit_turn_delivery_part: apply final-delivery job"
                ),
                args=(chat_id, candidate_id, feed_item, plan),
                kwargs=kwargs,
            ),
        )
    else:
        execution = _execute_entry_operation(
            store,
            runtime.telegram,
            operation,
            _provider_mutation(
                "telegram.send_turn_delivery_part",
                reason=(
                    "telegram.send_turn_delivery_part: apply final-delivery job"
                ),
                args=(chat_id, feed_item, plan),
                kwargs=kwargs,
            ),
        )
    applied = execution.result
    result["operations"] += _telegram_physical_writes(applied)
    return (
        applied,
        operation,
        execution.resolution,
        candidate_id,
        candidate_bot,
        desired_bot,
    )


def _canonical_turn_final_part(
    item: dict[str, Any],
    entry: dict[str, Any],
    payload: dict[str, Any],
    runtime: SyncRuntime,
    result: dict[str, Any],
) -> tuple[dict[str, Any], dict[str, Any]]:
    result["content_pages"] += _materialize_turn_item(item, runtime)
    feed_item = _canonical_final_feed_item(item, entry)
    plans = _prepare_final_delivery_parts(
        feed_item,
        rich_transport=config.rich_messages_enabled(),
    )
    ordinal = int(payload["part_ordinal"])
    if (
        ordinal >= len(plans)
        or int(payload["part_count"]) != len(plans)
        or payload.get("spans") != plans[ordinal].get("spans")
    ):
        raise _TurnContentError(
            "presentation_plan_mismatch",
            "turn-final job does not match canonical presentation",
        )
    return feed_item, plans[ordinal]


def _handle_failed_upsert_result(
    store: dict[str, Any],
    runtime: SyncRuntime,
    applied: dict[str, Any],
    operation: _OfflockEntryOperation,
    result: dict[str, Any],
    *,
    candidate_id: str,
    ref: str,
) -> None:
    kind = str(applied.get("kind") or "")
    if candidate_id and (
        kind == "not_found" or _message_missing(applied.get("error"))
    ):
        _retire_local_message(store, None, candidate_id)
        _checkpoint_turn_job(runtime)
        _defer_turn_final(
            runtime,
            ref,
            "transient_delivery",
            result,
            delay_seconds=1,
        )
    elif kind == "topic_not_found" or _topic_missing(
        applied.get("error")
    ):
        state.tombstone_dead_topic(store, operation.route_topic_id)
        _checkpoint_turn_job(runtime)
        _defer_turn_final(
            runtime,
            ref,
            "transient_delivery",
            result,
            delay_seconds=1,
        )
    elif _telegram_result_is_transient(applied):
        _defer_turn_final(
            runtime,
            ref,
            "rate_limited"
            if applied.get("rate_limited")
            else "transient_delivery",
            result,
            delay_seconds=_telegram_result_retry_after(applied),
        )
    else:
        _fail_turn_final(
            runtime,
            ref,
            str(applied.get("error") or "delivery_rejected"),
            result,
        )


def _persist_exact_job_binding(
    store: dict[str, Any],
    runtime: SyncRuntime,
    item: dict[str, Any],
    operation: _OfflockEntryOperation,
    resolution: _OfflockEntryResolution,
    applied: dict[str, Any],
    *,
    message_id: str,
    candidate_id: str,
    candidate_bot: str,
    desired_bot: str,
    revision: str,
    plan_token: str,
    ordinal: int,
    part_count: int,
    job_key: str,
) -> dict[str, Any] | None:
    state.bind_message_to_worker(
        store,
        message_id,
        _operation_binding_entry(operation),
        topic_id=operation.route_topic_id,
        kind="final",
        turn_id=_turn_id(item),
        bot_kind=desired_bot,
        content_revision=revision,
        plan_token=plan_token,
        part_ordinal=ordinal,
        part_count=part_count,
        tendwire_job_key=job_key,
        delivery_format=str(applied.get("format") or ""),
    )
    binding = state.find_message_binding(store, message_id)
    if binding is not None and resolution.disposition != _OFFLOCK_APPLY:
        binding["routing_quarantined"] = True
        binding["routing_quarantine_reason"] = (
            "owner_changed_during_delivery"
        )
    if binding is not None and candidate_id and candidate_id != message_id:
        binding["superseded_message_id"] = candidate_id
        binding["superseded_bot_kind"] = (
            candidate_bot or MANAGER_BOT_KIND
        )
    _checkpoint_turn_job(runtime)
    _after_provider_accept(runtime)
    return binding


def _apply_upsert_job(
    store: dict[str, Any],
    runtime: SyncRuntime,
    item: dict[str, Any],
    entry: dict[str, Any],
    payload: dict[str, Any],
    result: dict[str, Any],
    *,
    chat_id: str,
    job_key: str,
    ref: str,
    max_operations: int,
) -> bool:
    plan_token = str(payload["plan_token"])
    revision = str(payload["content_revision"])
    ordinal = int(payload["part_ordinal"])
    part_count = int(payload["part_count"])
    message_id, binding = _exact_job_binding(
        store,
        job_key=job_key,
        turn_id=_turn_id(item),
        revision=revision,
        plan_token=plan_token,
        ordinal=ordinal,
        part_count=part_count,
    )
    if message_id and binding:
        if binding.get("routing_quarantined") is True:
            return _retire_quarantined_job_binding(
                store,
                runtime,
                message_id,
                binding,
                result,
                chat_id=chat_id,
                ref=ref,
                max_operations=max_operations,
            )
        return _retire_bound_predecessor(
            store,
            runtime,
            binding,
            result,
            chat_id=chat_id,
            ref=ref,
            max_operations=max_operations,
        )
    feed_item, plan = _canonical_turn_final_part(
        item, entry, payload, runtime, result
    )
    (
        applied,
        operation,
        resolution,
        candidate_id,
        candidate_bot,
        desired_bot,
    ) = (
        _send_or_edit_final_part(
            store,
            runtime,
            item,
            feed_item,
            entry,
            payload,
            plan,
            result,
            chat_id=chat_id,
            max_operations=max_operations,
        )
    )
    if not applied.get("ok"):
        _handle_failed_upsert_result(
            store,
            runtime,
            applied,
            operation,
            result,
            candidate_id=candidate_id,
            ref=ref,
        )
        return False
    message_id = str(applied.get("message_id") or candidate_id or "")
    if not message_id or message_id == "0":
        _fail_turn_final(
            runtime,
            ref,
            "delivery_uncertain",
            result,
            uncertain=True,
        )
        return False
    binding = _persist_exact_job_binding(
        store,
        runtime,
        item,
        operation,
        resolution,
        applied,
        message_id=message_id,
        candidate_id=candidate_id,
        candidate_bot=candidate_bot,
        desired_bot=desired_bot,
        revision=revision,
        plan_token=plan_token,
        ordinal=ordinal,
        part_count=part_count,
        job_key=job_key,
    )
    if binding is not None and binding.get("routing_quarantined") is True:
        return _retire_quarantined_job_binding(
            store,
            runtime,
            message_id,
            binding,
            result,
            chat_id=chat_id,
            ref=ref,
            max_operations=max_operations,
        )
    return bool(
        binding is None
        or _retire_bound_predecessor(
            store,
            runtime,
            binding,
            result,
            chat_id=chat_id,
            ref=ref,
            max_operations=max_operations,
        )
    )


def _stage_final_root(
    store: dict[str, Any],
    runtime: SyncRuntime,
    payload: dict[str, Any],
    result: dict[str, Any],
    *,
    ref: str,
) -> dict[str, Any] | None:
    item = _final_ready_row(payload)
    entry_key, entry = _resolve_final_source_entry(store, payload)
    if entry is None or not str(entry.get("topic_id") or ""):
        return None
    owner_matches, owner_bound = _bind_or_verify_final_source_owner(
        store, payload, entry_key, entry, allow_bind=True
    )
    if not owner_matches:
        return None
    if owner_bound:
        _checkpoint_turn_job(runtime)
    page_calls = _materialize_turn_item(item, runtime)
    result["content_pages"] += page_calls
    staged, staged_pages, entry = _stage_final_plan(
        store,
        item,
        entry,
        runtime,
        source_ref=ref,
    )
    predecessor = str(
        payload.get("working_predecessor_turn_id") or ""
    )
    if predecessor:
        entry["pending_working_predecessor_turn_id"] = predecessor
    result["content_pages"] += staged_pages
    result["staged"] += int(staged)
    result["changed"] = True
    _checkpoint_turn_job(runtime)
    return item


def _poll_turn_final(
    runtime: SyncRuntime, *, lease_seconds: int
) -> dict[str, Any]:
    return _execute_exact_provider_operation(
        runtime.tendwire,
        mutation=_provider_mutation(
            "tendwire.turn_final_poll",
            reason="tendwire.turn_final_poll: acquire final lease",
            kwargs={"limit": 1, "lease_seconds": lease_seconds},
        ),
    )


def _prepare_turn_final_job(
    store: dict[str, Any],
    turns_payload: dict[str, Any],
    runtime: SyncRuntime,
    payload: dict[str, Any],
    result: dict[str, Any],
    materialized_sources: dict[str, dict[str, Any]],
    *,
    ref: str,
    turn_projection: Mapping[str, Any] | None,
) -> tuple[dict[str, Any], dict[str, Any], str, str, int] | None:
    revision = str(payload["content_revision"])
    item = materialized_sources.get(revision) or _turn_final_item_source(
        payload, turns_payload, turn_projection
    )
    entry_key, entry = _resolve_final_source_entry(
        store,
        payload.get("turn")
        if isinstance(payload.get("turn"), dict)
        else item,
    )
    if entry is None or not str(entry.get("topic_id") or ""):
        _defer_turn_final(
            runtime,
            ref,
            "transient_delivery",
            result,
            delay_seconds=1,
        )
        return None
    source = payload.get("turn")
    if isinstance(source, dict):
        owner_matches, owner_bound = _bind_or_verify_final_source_owner(
            store, source, entry_key, entry, allow_bind=True
        )
        if not owner_matches:
            _defer_turn_final(
                runtime,
                ref,
                "transient_delivery",
                result,
                delay_seconds=1,
            )
            return None
        if owner_bound:
            _checkpoint_turn_job(runtime)
    plan_token = str(payload["plan_token"])
    part_count = int(payload["part_count"])
    _set_pending_turn_plan(
        entry,
        turn_id=_turn_id(item),
        revision=revision,
        plan_token=plan_token,
        part_count=part_count,
        job_count=max(part_count, int(payload["sequence_index"]) + 1),
    )
    entry["pending_presentation_version"] = PRESENTATION_VERSION
    return item, entry, plan_token, revision, part_count


def _apply_turn_final_lease(
    store: dict[str, Any],
    turns_payload: dict[str, Any],
    runtime: SyncRuntime,
    lease: dict[str, Any],
    result: dict[str, Any],
    materialized_sources: dict[str, dict[str, Any]],
    *,
    chat_id: str,
    max_operations: int,
    turn_projection: Mapping[str, Any] | None,
) -> bool:
    ref = str(lease.get("ref") or "")
    payload = _validate_turn_final_item(lease)
    if payload["operation"] == "materialize":
        staged_item = _stage_final_root(
            store, runtime, payload, result, ref=ref
        )
        if staged_item is None:
            _defer_turn_final(
                runtime,
                ref,
                "transient_delivery",
                result,
                delay_seconds=1,
            )
            return False
        materialized_sources[str(payload["content_revision"])] = staged_item
        return True
    prepared = _prepare_turn_final_job(
        store,
        turns_payload,
        runtime,
        payload,
        result,
        materialized_sources,
        ref=ref,
        turn_projection=turn_projection,
    )
    if prepared is None:
        return False
    item, entry, plan_token, revision, part_count = prepared
    if payload["operation"] == "retire":
        applied = _apply_retire_job(
            store,
            runtime,
            item,
            payload,
            result,
            chat_id=chat_id,
            ref=ref,
        )
    else:
        applied = _apply_upsert_job(
            store,
            runtime,
            item,
            entry,
            payload,
            result,
            chat_id=chat_id,
            job_key=str(lease["key"]),
            ref=ref,
            max_operations=max_operations,
        )
    if not applied:
        return False
    if payload["operation"] == "upsert":
        _entry_key, authoritative_entry = _resolve_final_source_entry(
            store, item
        )
        if authoritative_entry is None:
            _defer_turn_final(
                runtime,
                ref,
                "transient_delivery",
                result,
                delay_seconds=1,
            )
            return False
        if _complete_turn_plan_from_bindings(
            store,
            item,
            authoritative_entry,
            plan_token=plan_token,
            revision=revision,
            part_count=part_count,
        ):
            _checkpoint_turn_job(runtime)
    return _ack_turn_final(
        runtime, ref, str(lease["key"]), result
    )


def _drain_turn_final(
    store: dict[str, Any],
    turns_payload: dict[str, Any],
    runtime: SyncRuntime,
    *,
    chat_id: str,
    max_operations: int,
    turn_projection: Mapping[str, Any] | None = None,
    yield_barrier: Callable[[], None] | None = None,
) -> dict[str, Any]:
    result = {
        "enabled": runtime.with_outbox,
        "polled": 0,
        "operations": 0,
        "delivered": 0,
        "acked": 0,
        "failed": 0,
        "deferred": 0,
        "uncertain": 0,
        "staged": 0,
        "content_pages": 0,
        "changed": False,
    }
    if not runtime.with_outbox or max_operations <= 0 or runtime.dry_run:
        return result
    lease_seconds = config.tendwire_turn_final_lease_seconds()
    materialized_sources: dict[str, dict[str, Any]] = {}
    for _iteration in range(max_operations + 100):
        if result["operations"] >= max_operations:
            break
        if yield_barrier is not None:
            yield_barrier()
        poll = _poll_turn_final(runtime, lease_seconds=lease_seconds)
        if poll.get("ok") is False:
            result["status"] = str(
                poll.get("status") or "turn_final_poll_failed"
            )
            result["changed"] = True
            break
        jobs = [
            item
            for item in poll.get("items", [])
            if isinstance(item, dict)
        ]
        if not jobs:
            break
        result["polled"] += 1
        lease = jobs[0]
        ref = str(lease.get("ref") or "")
        try:
            if not _apply_turn_final_lease(
                store,
                turns_payload,
                runtime,
                lease,
                result,
                materialized_sources,
                chat_id=chat_id,
                max_operations=max_operations,
                turn_projection=turn_projection,
            ):
                break
        except RateLimited as exc:
            _defer_turn_final(
                runtime,
                ref,
                "rate_limited",
                result,
                delay_seconds=exc.retry_after,
            )
            break
        except TelegramError:
            _fail_turn_final(
                runtime,
                ref,
                "delivery_uncertain",
                result,
                uncertain=True,
            )
            break
        except _TurnContentError as exc:
            if exc.conflict:
                _defer_turn_final(
                    runtime,
                    ref,
                    "transient_delivery",
                    result,
                    delay_seconds=1,
                )
            else:
                _fail_turn_final(
                    runtime, ref, f"{exc.status}: {exc}", result
                )
            break
        except TendwireError:
            _defer_turn_final(
                runtime,
                ref,
                "transient_delivery",
                result,
                delay_seconds=1,
            )
            break
        except Exception:
            raise
    return result



def _sync_pinned(
    store: dict[str, Any],
    runtime: SyncRuntime,
    *,
    chat_id: str,
    yield_barrier: Callable[[], None] | None = None,
    account_usage: dict[str, Any] | None = None,
) -> bool:
    entries = [
        entry
        for entry in state.source_worker_entries(store).values()
        if _worker_visible_on_status_board(entry)
    ]
    if not entries and config.source_topic_mode() != "worker":
        entries = [
            entry
            for entry in state.source_space_entries(store).values()
            if _entry_open_for_pin(entry)
            and not entry.get("stale_space_topic")
        ]
    if not entries:
        return False
    html = render_status_overview(entries)
    account_html = _account_lines_html(
        entries,
        usage_snapshot=account_usage,
    )
    if account_html:
        html = f"{html}\n{account_html}"
    if yield_barrier is not None:
        yield_barrier()
    telegram = store.setdefault("telegram", {})
    message_id = str(telegram.get("pinned_status_message_id") or "")
    content_hash = short_hash(html, 20)
    if telegram.get("pinned_status_hash") == content_hash:
        return False
    if runtime.dry_run:
        telegram["pinned_status_hash"] = content_hash
        telegram.setdefault("pinned_status_message_id", "0")
        return True
    general_thread_id = str(config.general_thread_id(store))
    def send_to_general_thread() -> tuple[
        dict[str, Any], _OfflockEntryResolution
    ]:
        operation = _capture_global_operation(
            store, topic_id=general_thread_id
        )

        execution = _execute_entry_operation(
            store,
            runtime.telegram,
            operation,
            _provider_mutation(
                "telegram.send_message",
                reason=(
                    "telegram.send_message: create global status in general thread"
                ),
                args=(chat_id, html),
                kwargs={
                    "thread_id": general_thread_id,
                    "notify": False,
                },
            ),
        )
        result, resolution = execution.result, execution.resolution
        if not result.get("ok") and _topic_missing(
            result.get("error")
        ):
            _repair_provider_gone_topic(
                store,
                None,
                result,
                topic_id=general_thread_id,
            )
            fallback_operation = _capture_global_operation(
                store, topic_id=""
            )
            execution = _execute_entry_operation(
                store,
                runtime.telegram,
                fallback_operation,
                _provider_mutation(
                    "telegram.send_message",
                    reason=(
                        "telegram.send_message: create global status in root fallback"
                    ),
                    args=(chat_id, html),
                    kwargs={"notify": False},
                ),
            )
            result, resolution = execution.result, execution.resolution
        return result, resolution

    sent_new = False
    if message_id:
        operation = _capture_global_operation(
            store,
            topic_id=general_thread_id,
            message_id=message_id,
        )
        execution = _execute_entry_operation(
            store,
            runtime.telegram,
            operation,
            _provider_mutation(
                "telegram.edit_message",
                reason="telegram.edit_message: refresh global status",
                args=(chat_id, message_id, html),
            ),
        )
        sent, resolution = execution.result, execution.resolution
        if not sent.get("ok") and (
            _message_missing(sent.get("error"))
            or _topic_missing(sent.get("error"))
        ):
            sent, resolution = send_to_general_thread()
            sent_new = True
    else:
        sent, resolution = send_to_general_thread()
        sent_new = True
    if resolution.disposition != _OFFLOCK_APPLY:
        return False
    telegram = resolution.entry
    assert telegram is not None
    if sent_new and sent.get("ok") and sent.get("message_id"):
        pin_operation = _capture_global_operation(
            store,
            topic_id=general_thread_id
            if general_thread_id not in state.dead_topic_ids(store)
            else "",
            message_id=str(sent["message_id"]),
        )
        execution = _execute_entry_operation(
            store,
            runtime.telegram,
            pin_operation,
            _provider_mutation(
                "telegram.pin_message",
                reason="telegram.pin_message: pin global status",
                args=(chat_id, str(sent["message_id"])),
            ),
        )
        pin_resolution = execution.resolution
        if pin_resolution.disposition != _OFFLOCK_APPLY:
            return False
        telegram = pin_resolution.entry
        assert telegram is not None
    if sent.get("ok"):
        telegram["pinned_status_hash"] = content_hash
        if sent.get("message_id"):
            telegram["pinned_status_message_id"] = str(sent["message_id"])
        telegram.pop("pinned_status_last_error", None)
        return True
    telegram["pinned_status_last_error"] = compact_ws(sent.get("error"), 240)
    return False


def _sync_topic_pinned_statuses(
    store: dict[str, Any],
    runtime: SyncRuntime,
    *,
    chat_id: str,
    yield_barrier: Callable[[], None] | None = None,
    account_usage: dict[str, Any] | None = None,
) -> int:
    def owns_topic_pin(entry: dict[str, Any]) -> bool:
        is_space = str(entry.get("entry_type") or "") == "space"
        if config.source_topic_mode() == "space":
            return (
                is_space
                and bool(entry.get("topic_id"))
                and not entry.get("stale_space_topic")
            )
        return (
            not is_space
            and bool(entry.get("topic_id"))
            and (
                entry.get("live_in_snapshot") is True
                or (
                    "live_in_snapshot" not in entry
                    and _entry_open_for_pin(entry)
                )
            )
        )

    updated = 0
    for entry_key in list(state.source_entries(store)):
        entry = state.source_entries(store).get(entry_key)
        if entry is None or not owns_topic_pin(entry):
            continue
        if yield_barrier is not None:
            yield_barrier()
        entry = state.source_entries(store).get(entry_key)
        if entry is None or not owns_topic_pin(entry):
            continue
        updated += int(
            _sync_topic_pinned(
                store,
                entry,
                runtime,
                chat_id=chat_id,
                account_usage=account_usage,
            )
        )
    return updated


def _decision_provider_executor(
    store: dict[str, Any],
    runtime: SyncRuntime,
) -> decisions.ProviderExecutor:
    """Bind decision operations to the same immutable owner guard as panes."""

    def _execute_decision_provider_operation(
        request: decisions.ProviderOperation,
    ) -> decisions.ProviderExecution:
        if not decisions.provider_acceptance_capacity_available(
            store, request
        ):
            return decisions.ProviderExecution(
                {"ok": False, "status": "accepted_artifact_backpressure"},
                _OFFLOCK_ABANDON,
            )
        operation: _OfflockEntryOperation | None = None
        if request.provenance:
            operation = _operation_from_provenance(request.provenance)
        else:
            entry = state.source_worker_entries(store).get(
                request.entry_key
            )
            if (
                entry is not None
                and request.worker_id
                and _entry_worker_id(entry) != request.worker_id
            ):
                entry = None
            if entry is None and request.worker_id:
                _key, entry = state.find_worker_entry_by_id(
                    store, request.worker_id
                )
            if (
                entry is None
                or str(entry.get("topic_id") or "") != request.topic_id
            ):
                return decisions.ProviderExecution(
                    {"ok": False, "status": "owner_changed"},
                    _OFFLOCK_ABANDON,
                )
            operation = _capture_entry_operation(
                store,
                entry,
                topic_id=request.topic_id,
                message_id=request.message_id,
            )
        client = (
            runtime.tendwire
            if request.capability.startswith("tendwire.")
            else runtime.telegram
        )
        mutation = _provider_mutation(
            request.capability,
            reason=request.reason,
            args=request.args,
            kwargs=dict(request.kwargs),
        )
        provenance = _operation_provenance(operation)
        receipt_id = ""

        def checkpoint_acceptance(
            result: Any, _operation: _OfflockEntryOperation
        ) -> None:
            nonlocal receipt_id
            receipt_id = decisions.checkpoint_provider_acceptance(
                store,
                request,
                result if isinstance(result, Mapping) else {},
                provenance,
            )
            if receipt_id and runtime.checkpoint is not None:
                runtime.checkpoint()

        if request.scope == "exact":
            provider_result = _execute_exact_provider_operation(
                client, mutation=mutation, store=store
            )
            checkpoint_acceptance(provider_result, operation)
            resolution = _compare_and_apply_entry_operation(
                store, operation
            )
        else:
            execution = _execute_entry_operation(
                store,
                client,
                operation,
                mutation,
                acceptance_checkpoint=checkpoint_acceptance,
            )
            provider_result = execution.result
            resolution = execution.resolution
        return decisions.ProviderExecution(
            dict(provider_result),
            resolution.disposition,
            provenance=provenance,
            receipt_id=receipt_id,
        )

    return _execute_decision_provider_operation


def _deliver_decisions(
    store: dict[str, Any],
    pending_payload: dict[str, Any],
    runtime: SyncRuntime,
    *,
    chat_id: str,
    yield_barrier: Callable[[], None] | None = None,
) -> dict[str, Any]:
    """Reconcile decision keyboards without ever endangering the sync pass.

    Callback mutations and the normal source shim both hold ``state_lock``.
    The late barrier commits our preceding source work, briefly lets a queued
    callback save its decision bucket, and reloads the whole store before this
    step.  Therefore the shim's final save cannot clobber a callback that used
    the lock; only an out-of-contract writer that bypasses the shared lock can
    race the whole-file state save.
    """

    if not config.remote_decisions_enabled():
        return {"enabled": False, "changed": False, "posted": 0, "retracted": 0}
    if not decisions.needs_sync(store, pending_payload):
        return {"enabled": True, "changed": False, "posted": 0, "retracted": 0}
    try:
        if yield_barrier is not None:
            yield_barrier()
        return decisions.sync_decisions(
            store,
            pending_payload,
            runtime.telegram,
            chat_id=chat_id,
            dry_run=runtime.dry_run,
            provider_executor=_decision_provider_executor(
                store, runtime
            ),
        )
    except Exception as exc:  # noqa: BLE001 - decisions are additive to the core sync loop
        return {
            "enabled": True,
            "changed": False,
            "posted": 0,
            "retracted": 0,
            "status": "failed",
            "error": compact_ws(exc, 240),
        }


def _tendwire_non_success(runtime: SyncRuntime, status: str) -> dict[str, Any]:
    return {
        "ok": False,
        "status": status,
        "changed": False,
        "created": 0,
        "updated": 0,
        "panes": 0,
        "spaces": 0,
        "icon_updated": 0,
        "pinned_status_updated": 0,
        "feed_sent": 0,
        "sent": 0,
        "routing_repaired": 0,
        "message_bindings": 0,
        "turn_updates": 0,
        "topic_cleanup": {
            "deleted": 0,
            "failed": 0,
            "pruned": 0,
            "changed": False,
        },
        "content_pages": 0,
        "tendwire_turn_final": {
            "enabled": runtime.with_outbox,
            "polled": 0,
            "operations": 0,
            "delivered": 0,
            "acked": 0,
            "failed": 0,
            "deferred": 0,
            "uncertain": 0,
            "changed": False,
        },
        "tendwire_outbox": {
            "enabled": runtime.with_outbox,
            "polled": 0,
            "delivered": 0,
            "acked": 0,
            "failed": 0,
            "deferred": 0,
            "changed": False,
        },
    }


def _unsupported_turn_schema_version(
    runtime: SyncRuntime, received: Any
) -> dict[str, Any]:
    result = _tendwire_non_success(runtime, "unsupported_turn_schema_version")
    result["required_turn_schema_version"] = TURN_SCHEMA_VERSION
    if isinstance(received, str):
        safe_received: Any = compact_ws(received, 80)
    elif received is None or isinstance(received, (bool, int, float)):
        safe_received = received
    else:
        safe_received = None
    result["received_turn_schema_version"] = safe_received
    return result


def _herdr_backend_explicitly_unhealthy(snapshot: dict[str, Any]) -> bool:
    backend_health = snapshot.get("backend_health")
    if not isinstance(backend_health, list):
        return False
    for item in backend_health:
        if not isinstance(item, dict):
            continue
        if str(item.get("name") or "").strip().lower() != "herdr":
            continue
        status = item.get("status")
        if isinstance(status, str) and status.strip() and status.strip().lower().replace("-", "_") != "healthy":
            return True
    return False


_DELTA_STATE_KEY = "tendwire_delta_sync"
_DELTA_SCHEMA_VERSION = 1
_DELTA_PROJECTION_SCHEMA_VERSION = 2


def _new_delta_state(*, reason: str, now: float) -> dict[str, Any]:
    return {
        "schema_version": _DELTA_SCHEMA_VERSION,
        "projection_schema_version": _DELTA_PROJECTION_SCHEMA_VERSION,
        "status": "bootstrapping",
        "watermark": None,
        "pending_cursor": None,
        "projection": {},
        "bootstrap_state": {
            "reason": reason,
            "attempt": 1,
            "pages_applied": 0,
            "started_at": now,
        },
        "failure_count": 0,
        "last_full_reconcile_at": now,
    }


def _delta_state(store: dict[str, Any], *, now: float) -> dict[str, Any]:
    current = store.get(_DELTA_STATE_KEY)
    if not isinstance(current, dict):
        current = _new_delta_state(reason="first_activation", now=now)
        store[_DELTA_STATE_KEY] = current
        return current
    if (
        current.get("schema_version") != _DELTA_SCHEMA_VERSION
        or current.get("projection_schema_version")
        != _DELTA_PROJECTION_SCHEMA_VERSION
        or not isinstance(current.get("projection"), dict)
        or current.get("status") not in {"active", "bootstrapping"}
    ):
        raise _TurnContentError(
            "invalid_delta_state",
            "persisted turn-delta state is not supported",
        )
    return current


def _delta_health(delta: dict[str, Any] | None, *, now: float | None = None) -> dict[str, Any]:
    if not isinstance(delta, dict):
        return {"state": "bootstrapping", "watermark_age_seconds": None, "last_batch": {}}
    clock = time.time() if now is None else now
    updated_at = delta.get("watermark_updated_at")
    age: int | None = None
    if isinstance(updated_at, (int, float)) and not isinstance(updated_at, bool):
        age = max(0, int(clock - float(updated_at)))
    raw_batch = delta.get("last_batch")
    batch: dict[str, Any] = {}
    if isinstance(raw_batch, dict):
        for key in (
            "mode",
            "changes_returned",
            "upserts",
            "removals",
            "journal_rows_scanned",
            "projection_rows_read",
            "duration_ms",
        ):
            value = raw_batch.get(key)
            if isinstance(value, str) and key == "mode":
                batch[key] = compact_ws(value, 24)
            elif isinstance(value, int) and not isinstance(value, bool) and value >= 0:
                batch[key] = value
    state_name = str(delta.get("status") or "bootstrapping")
    result: dict[str, Any] = {
        "state": state_name
        if state_name in {"active", "bootstrapping"}
        else "bootstrapping",
        "watermark_age_seconds": age,
        "last_batch": batch,
    }
    health_flag = delta.get("health_flag")
    if isinstance(health_flag, str) and health_flag:
        result["health_flag"] = compact_ws(health_flag, 80)
    return result


def _delta_full_reconcile_due(delta: dict[str, Any], *, now: float) -> bool:
    if delta.get("status") != "active" or delta.get("pending_cursor"):
        return False
    if config.tendwire_force_full_reconcile():
        return True
    interval = config.tendwire_full_reconcile_seconds()
    if interval <= 0:
        return False
    try:
        last_at = float(delta.get("last_full_reconcile_at") or 0)
    except (TypeError, ValueError):
        last_at = 0
    return now - last_at >= interval


def _delta_error_code(payload: dict[str, Any]) -> str:
    status = str(payload.get("status") or "").strip().lower()
    error = payload.get("error")
    if not status and isinstance(error, dict):
        status = str(error.get("code") or "").strip().lower()
    return status


def _validate_delta_page(
    payload: dict[str, Any],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], dict[str, int]]:
    if (
        payload.get("schema_version") != _DELTA_SCHEMA_VERSION
        or payload.get("projection_schema_version")
        != _DELTA_PROJECTION_SCHEMA_VERSION
        or payload.get("mode") not in {"bootstrap", "changes"}
        or type(payload.get("has_more")) is not bool
        or not isinstance(payload.get("changes"), list)
        or not isinstance(payload.get("host_id"), str)
        or not payload.get("host_id")
    ):
        raise _TurnContentError(
            "delta_protocol_ambiguous",
            "Tendwire turn.delta returned a malformed envelope",
        )
    has_more = payload["has_more"]
    next_cursor = payload.get("next_cursor")
    checkpoint = payload.get("checkpoint")
    if has_more:
        if (
            not isinstance(next_cursor, str)
            or not next_cursor.startswith("twdeltac1.")
            or checkpoint is not None
        ):
            raise _TurnContentError(
                "delta_protocol_ambiguous",
                "Tendwire turn.delta returned invalid continuation state",
            )
    elif (
        next_cursor is not None
        or not isinstance(checkpoint, str)
        or not checkpoint.startswith("twdelta1.")
    ):
        raise _TurnContentError(
            "delta_protocol_ambiguous",
            "Tendwire turn.delta returned invalid checkpoint state",
        )
    upserts: list[dict[str, Any]] = []
    removals: list[dict[str, Any]] = []
    seen: set[str] = set()
    for raw_change in payload["changes"]:
        if not isinstance(raw_change, dict):
            raise _TurnContentError(
                "delta_protocol_ambiguous",
                "Tendwire turn.delta returned a malformed change",
            )
        op = raw_change.get("op")
        change_turn_id = raw_change.get("turn_id")
        if (
            not isinstance(change_turn_id, str)
            or not change_turn_id
            or change_turn_id in seen
        ):
            raise _TurnContentError(
                "delta_protocol_ambiguous",
                "Tendwire turn.delta returned an invalid turn identity",
            )
        seen.add(change_turn_id)
        if op == "upsert":
            raw_turn = raw_change.get("turn")
            if not isinstance(raw_turn, dict):
                raise _TurnContentError(
                    "delta_protocol_ambiguous",
                    "Tendwire turn.delta upsert omitted its projection",
                )
            try:
                turn = _validate_turn_row(raw_turn)
            except _TurnContentError as exc:
                turn = (
                    _quarantined_turn_row(raw_turn, exc.status)
                    if exc.status == "private_agent_content"
                    else dict(raw_turn)
                )
                if _TURN_CONTENT_OUTCOME_KEY not in turn:
                    turn[_TURN_CONTENT_OUTCOME_KEY] = _turn_local_outcome(
                        turn, exc.status
                    )
                if (
                    exc.status == "private_agent_content"
                    and _turn_id(turn) != change_turn_id
                ):
                    # Privacy quarantine changes presentation, not journal
                    # identity. It must not advance the checkpoint under a
                    # different turn key. Content-schema isolation remains
                    # row-local below this boundary.
                    raise _TurnContentError(
                        "delta_protocol_ambiguous",
                        "Tendwire turn.delta projection identity mismatched",
                    )
            else:
                if _turn_id(turn) != change_turn_id:
                    raise _TurnContentError(
                        "delta_protocol_ambiguous",
                        "Tendwire turn.delta projection identity mismatched",
                    )
            link_values: list[tuple[str, str]] = []
            for source in (raw_change, raw_turn):
                if any(
                    key in source
                    for key in (
                        "submission_id",
                        "linked_submission_id",
                        "submission_state",
                        "observed_turn_state",
                    )
                ):
                    submission_id = source.get(
                        "submission_id", source.get("linked_submission_id")
                    )
                    submission_state = source.get(
                        "submission_state",
                        source.get("observed_turn_state", "linked"),
                    )
                    if (
                        not isinstance(submission_id, str)
                        or not submission_id.strip()
                        or len(submission_id) > 200
                        or submission_state not in _SUBMISSION_STATES
                    ):
                        raise _TurnContentError(
                            "delta_protocol_ambiguous",
                            "Tendwire turn.delta carried an invalid submission link",
                        )
                    link_values.append((submission_id, submission_state))
                linked = source.get("linked_submission")
                if linked is not None:
                    if not isinstance(linked, dict):
                        raise _TurnContentError(
                            "delta_protocol_ambiguous",
                            "Tendwire turn.delta carried a malformed linked submission",
                        )
                    submission_id = linked.get(
                        "submission_id", linked.get("id")
                    )
                    submission_state = linked.get(
                        "submission_state", linked.get("state", "linked")
                    )
                    linked_turn_id = linked.get("turn_id", change_turn_id)
                    if (
                        not isinstance(submission_id, str)
                        or not submission_id.strip()
                        or len(submission_id) > 200
                        or submission_state not in _SUBMISSION_STATES
                        or linked_turn_id != change_turn_id
                    ):
                        raise _TurnContentError(
                            "delta_protocol_ambiguous",
                            "Tendwire turn.delta carried an invalid linked submission",
                        )
                    link_values.append((submission_id, submission_state))
            if link_values:
                if any(value != link_values[0] for value in link_values[1:]):
                    raise _TurnContentError(
                        "delta_protocol_ambiguous",
                        "Tendwire turn.delta submission links disagree",
                    )
                turn[_SUBMISSION_ID_KEY] = link_values[0][0]
                turn[_SUBMISSION_STATE_KEY] = link_values[0][1]
            upserts.append(turn)
        elif op == "remove":
            if not isinstance(raw_change.get("removed_at"), str):
                raise _TurnContentError(
                    "delta_protocol_ambiguous",
                    "Tendwire turn.delta removal omitted its timestamp",
                )
            successor = raw_change.get("superseded_by_turn_id")
            if successor is not None and (
                not isinstance(successor, str)
                or not successor
                or successor == change_turn_id
            ):
                raise _TurnContentError(
                    "delta_protocol_ambiguous",
                    "Tendwire turn.delta removal has an invalid successor",
                )
            removals.append(raw_change)
        else:
            raise _TurnContentError(
                "delta_protocol_ambiguous",
                "Tendwire turn.delta returned an unsupported operation",
            )
    aggregate: dict[str, int] = {}
    raw_aggregate = payload.get("aggregate")
    if isinstance(raw_aggregate, dict):
        for key in (
            "journal_rows_scanned",
            "projection_rows_read",
            "changes_returned",
            "duration_ms",
        ):
            value = raw_aggregate.get(key)
            if isinstance(value, int) and not isinstance(value, bool) and value >= 0:
                aggregate[key] = value
    aggregate.setdefault("changes_returned", len(upserts) + len(removals))
    return upserts, removals, aggregate


def _clear_removed_turn_state(store: dict[str, Any], turn_id: str) -> bool:
    changed = False
    for bucket_name in ("panes", "spaces"):
        bucket = store.get(bucket_name)
        if not isinstance(bucket, dict):
            continue
        for entry in bucket.values():
            if not isinstance(entry, dict):
                continue
            if entry.get("last_stream_turn_id") == turn_id:
                changed = _clear_stream_delivery_keys(entry) or changed
            if entry.get("last_turn_id") == turn_id:
                changed = _clear_final_delivery_keys(entry) or changed
    bindings = state.message_bindings(store)
    for message_id, binding in list(bindings.items()):
        if isinstance(binding, dict) and str(binding.get("turn_id") or "") == turn_id:
            bindings.pop(message_id, None)
            changed = True
    return changed


def _follow_removed_turn_successor(
    store: dict[str, Any],
    turn_id: str,
    successor_turn_id: str,
) -> bool:
    """Retarget local card identity when Tendwire supplies a successor."""
    changed = False
    for bucket_name in ("panes", "spaces"):
        bucket = store.get(bucket_name)
        if not isinstance(bucket, dict):
            continue
        for entry in bucket.values():
            if not isinstance(entry, dict):
                continue
            if entry.get("last_stream_turn_id") == turn_id:
                entry["last_stream_turn_id"] = successor_turn_id
                changed = True
            if entry.get("last_turn_id") == turn_id:
                entry["last_turn_id"] = successor_turn_id
                changed = True
    for binding in state.message_bindings(store).values():
        if (
            isinstance(binding, dict)
            and str(binding.get("turn_id") or "") == turn_id
        ):
            binding["turn_id"] = successor_turn_id
            changed = True
    return changed


def _clear_projection_stale_cards(store: dict[str, Any], projection: dict[str, Any]) -> int:
    cleared = 0
    known = set(projection)
    candidates: set[str] = set()
    for bucket_name in ("panes", "spaces"):
        bucket = store.get(bucket_name)
        if not isinstance(bucket, dict):
            continue
        for entry in bucket.values():
            if not isinstance(entry, dict):
                continue
            for key in ("last_stream_turn_id", "last_turn_id"):
                value = entry.get(key)
                if isinstance(value, str) and value and value not in known:
                    candidates.add(value)
    for turn_id in candidates:
        cleared += int(_clear_removed_turn_state(store, turn_id))
    return cleared


def _record_delta_failure(
    delta: dict[str, Any], *, status: str, now: float
) -> None:
    delta["last_error_at"] = now
    delta["failure_count"] = int(delta.get("failure_count") or 0) + 1
    safe_status = compact_ws(status, 48)
    delta["health_flag"] = f"turn_delta_{safe_status}"


def _clear_delta_failure(delta: dict[str, Any]) -> None:
    delta["failure_count"] = 0
    for key in ("last_error_at", "health_flag"):
        delta.pop(key, None)


def _observe_turn_delta(
    store: dict[str, Any],
    runtime: SyncRuntime,
    *,
    now: float,
) -> dict[str, Any]:
    def transition(result: dict[str, Any]) -> dict[str, Any]:
        if runtime.checkpoint is not None:
            runtime.checkpoint()
        return result

    delta = _delta_state(store, now=now)
    if _delta_full_reconcile_due(delta, now=now):
        return {"kind": "full", "delta": delta, "reason": "reconcile"}
    cursor = delta.get("pending_cursor")
    watermark = delta.get("watermark")
    if not isinstance(cursor, str) or not cursor:
        cursor = None
    if not isinstance(watermark, str) or not watermark:
        watermark = None
    page = runtime.tendwire.turn_delta(
        cursor=cursor,
        watermark=None if cursor is not None else watermark,
        limit=config.tendwire_delta_limit(),
    )
    if page.get("ok") is False:
        status = _delta_error_code(page) or "delta_failed"
        _record_delta_failure(delta, status=status, now=now)
        return transition(
            {
                "kind": "error",
                "delta": delta,
                "reason": status,
                "status": f"turn_delta_{compact_ws(status, 48)}",
            }
        )
    try:
        upserts, removals, aggregate = _validate_delta_page(page)
    except _TurnContentError:
        _record_delta_failure(
            delta, status="delta_protocol_ambiguous", now=now
        )
        return transition(
            {
                "kind": "error",
                "delta": delta,
                "reason": "delta_protocol_ambiguous",
                "status": "turn_delta_delta_protocol_ambiguous",
            }
        )
    expected_mode = "bootstrap" if watermark is None and delta.get("status") == "bootstrapping" else "changes"
    if page.get("mode") != expected_mode:
        _record_delta_failure(
            delta, status="delta_protocol_ambiguous", now=now
        )
        return transition(
            {
                "kind": "error",
                "delta": delta,
                "reason": "delta_protocol_ambiguous",
                "status": "turn_delta_delta_protocol_ambiguous",
            }
        )
    if (
        expected_mode == "changes"
        and cursor is None
        and not page["has_more"]
        and not upserts
        and not removals
        and page.get("checkpoint") == watermark
        and int(delta.get("failure_count") or 0) == 0
        and "last_error_at" not in delta
        and "health_flag" not in delta
    ):
        return {"kind": "noop", "delta": delta, "reason": "unchanged"}
    projection = delta["projection"]
    for row in upserts:
        projection[_turn_id(row)] = row
    for removal in removals:
        turn_id = str(removal["turn_id"])
        projection.pop(turn_id, None)
    _clear_delta_failure(delta)
    delta["last_batch"] = {
        "mode": str(page["mode"]),
        **aggregate,
        "upserts": len(upserts),
        "removals": len(removals),
    }
    return {
        "kind": "delta",
        "delta": delta,
        "page": page,
        "upserts": upserts,
        "removals": removals,
    }


def _finish_delta_page(
    store: dict[str, Any],
    observation: dict[str, Any],
    runtime: SyncRuntime,
    *,
    now: float,
) -> int:
    delta = observation["delta"]
    page = observation["page"]
    changed = 0
    for removal in observation["removals"]:
        turn_id = str(removal["turn_id"])
        successor = removal.get("superseded_by_turn_id")
        if isinstance(successor, str) and successor:
            changed += int(
                _follow_removed_turn_successor(store, turn_id, successor)
            )
        else:
            changed += int(_clear_removed_turn_state(store, turn_id))
    bootstrap = delta.get("bootstrap_state")
    if isinstance(bootstrap, dict):
        bootstrap["pages_applied"] = int(bootstrap.get("pages_applied") or 0) + 1
    if page["has_more"]:
        delta["pending_cursor"] = page["next_cursor"]
    else:
        delta["watermark"] = page["checkpoint"]
        delta["pending_cursor"] = None
        delta["status"] = "active"
        delta["bootstrap_state"] = None
        delta["watermark_updated_at"] = now
        delta["last_full_reconcile_at"] = (
            now if page["mode"] == "bootstrap" else delta.get("last_full_reconcile_at", now)
        )
        delta.pop("health_flag", None)
        if page["mode"] == "bootstrap":
            changed += _clear_projection_stale_cards(store, delta["projection"])
    if runtime.checkpoint is not None:
        runtime.checkpoint()
    return changed


def _apply_full_reconciliation(
    store: dict[str, Any],
    delta: dict[str, Any],
    turns_payload: dict[str, Any],
    runtime: SyncRuntime,
    *,
    now: float,
) -> int:
    projection = {_turn_id(row): row for row in _turns(turns_payload) if _turn_id(row)}
    delta["projection"] = projection
    delta["last_full_reconcile_at"] = now
    delta["last_batch"] = {
        "mode": "full_reconcile",
        "changes_returned": len(projection),
        "upserts": len(projection),
        "removals": 0,
    }
    changed = _clear_projection_stale_cards(store, projection)
    if runtime.checkpoint is not None:
        runtime.checkpoint()
    return changed


def _observe_sync_inputs(
    store: dict[str, Any],
    runtime: SyncRuntime,
    *,
    now: float,
) -> dict[str, Any]:
    """Observe Tendwire off-lock, then atomically adopt the observed delta lane.

    ``_observe_turn_delta`` intentionally mutates its supplied delta projection
    and checkpoint fields.  Under the production flock it therefore runs
    against a private state copy.  After the RPC window closes, the live state
    is reloaded and its complete pre-observation delta basis must still match
    before the private projection can be adopted.
    """

    if not state.lock_held():
        snapshot = runtime.tendwire.snapshot()
        if _herdr_backend_explicitly_unhealthy(snapshot):
            return {"unhealthy": True, "snapshot": snapshot}
        observation = _observe_turn_delta(store, runtime, now=now)
        if observation["kind"] == "full":
            turns_payload = runtime.tendwire.turns()
        elif observation["kind"] == "delta":
            turns_payload = {
                "schema_version": TURN_SCHEMA_VERSION,
                "turns": observation["upserts"],
            }
        else:
            turns_payload = {
                "schema_version": TURN_SCHEMA_VERSION,
                "turns": [],
            }
        observation["apply_basis"] = deepcopy(observation["delta"])
        return {
            "snapshot": snapshot,
            "delta_observation": observation,
            "turns": turns_payload,
            "pending": runtime.tendwire.pending(),
        }

    # Materialize default delta state before the phase-1 save so the equality
    # check below compares a durable, explicit cursor/projection basis.
    _delta_state(store, now=now)
    state.save_state(store)
    delta_basis = deepcopy(store[_DELTA_STATE_KEY])
    observed_store = deepcopy(store)
    observed_runtime = SyncRuntime(
        runtime.tendwire,
        runtime.telegram,
        dry_run=runtime.dry_run,
        with_outbox=runtime.with_outbox,
        max_sends=runtime.max_sends,
        # Private observation must never checkpoint its speculative copy.
        checkpoint=None,
        after_provider_accept=runtime.after_provider_accept,
    )
    with state.released_lock():
        snapshot = runtime.tendwire.snapshot()
        if _herdr_backend_explicitly_unhealthy(snapshot):
            observed = {"unhealthy": True, "snapshot": snapshot}
        else:
            observation = _observe_turn_delta(
                observed_store, observed_runtime, now=now
            )
            if observation["kind"] == "full":
                turns_payload = runtime.tendwire.turns()
            elif observation["kind"] == "delta":
                turns_payload = {
                    "schema_version": TURN_SCHEMA_VERSION,
                    "turns": observation["upserts"],
                }
            else:
                turns_payload = {
                    "schema_version": TURN_SCHEMA_VERSION,
                    "turns": [],
                }
            observed = {
                "snapshot": snapshot,
                "delta_observation": observation,
                "turns": turns_payload,
                "pending": runtime.tendwire.pending(),
            }

    state.reload_state_in_place(store)
    if observed.get("unhealthy"):
        return observed
    if store.get(_DELTA_STATE_KEY) != delta_basis:
        # Another observer advanced the cursor while the RPC was in flight.
        # Discard this page; applying it to the new basis would fork the
        # retained projection.
        return {"cursor_conflict": True}
    observation = observed["delta_observation"]
    store[_DELTA_STATE_KEY] = deepcopy(observation["delta"])
    observation["delta"] = store[_DELTA_STATE_KEY]
    # Later delivery/cleanup phases open more release windows. Keep the exact
    # adopted basis so the page can be revalidated before its checkpoint.
    observation["apply_basis"] = deepcopy(observation["delta"])
    return observed


def drain_outbound_once(
    store: dict[str, Any],
    runtime: SyncRuntime,
    *,
    chat_id: str,
    max_operations: int | None = None,
) -> dict[str, Any]:
    """Drain outbound-only work without waiting for a reconciliation pass.

    Modern turn-final jobs carry their immutable source descriptor. The retained
    delta projection remains the compatibility source for older queued jobs.
    This path deliberately performs no snapshot, pending, topic, or pane scan.
    """

    backpressure_sequence = _telegram_backpressure_sequence(store)
    effective_runtime = _offlock_runtime(store, runtime)
    operation_limit = max(
        1,
        int(
            effective_runtime.max_sends
            if max_operations is None
            else max_operations
        ),
    )
    delta = store.get(_DELTA_STATE_KEY)
    projection = (
        delta.get("projection")
        if isinstance(delta, dict)
        and isinstance(delta.get("projection"), dict)
        else None
    )
    turn_final_result = _drain_turn_final(
        store,
        {"schema_version": TURN_SCHEMA_VERSION, "turns": []},
        effective_runtime,
        chat_id=chat_id,
        max_operations=operation_limit,
        turn_projection=projection,
    )
    remaining = max(
        0,
        operation_limit - int(turn_final_result.get("operations") or 0),
    )
    attention_result = drain_outbox(
        store,
        _exact_provider_client(
            effective_runtime.telegram,
            reason=(
                "outbound attention Telegram exact identifiers"
            ),
        ),
        _exact_provider_client(
            effective_runtime.tendwire,
            reason="outbound attention Tendwire exact leased references",
        ),
        chat_id=chat_id,
        max_sends=remaining,
        dry_run=effective_runtime.dry_run,
        ack_barrier_persists_state=True,
    )
    telegram_backpressure = _telegram_backpressure_since(
        store, backpressure_sequence
    )
    return {
        "ok": True,
        "changed": bool(
            turn_final_result.get("changed")
            or attention_result.get("changed")
            or telegram_backpressure["count"]
        ),
        "tendwire_turn_final": turn_final_result,
        "tendwire_outbox": attention_result,
        "telegram_backpressure": telegram_backpressure,
    }


def sync_once(store: dict[str, Any], runtime: SyncRuntime) -> dict[str, Any]:
    config.require_source_mode()
    # SyncRuntime instances are routinely reused by tests and service loops.
    # The allowance is per pass, never per runtime object.
    runtime.delivery_write_budget = _DeliveryWriteBudget(
        max(0, int(runtime.max_sends))
    )
    backpressure_sequence = _telegram_backpressure_sequence(store)
    observed_at = time.time()
    try:
        with state.lock_phase("sync.observe"):
            observed = _observe_sync_inputs(store, runtime, now=observed_at)
    except _TurnContentError as exc:
        return _tendwire_non_success(runtime, exc.status)
    if observed.get("unhealthy"):
        return _tendwire_non_success(runtime, "tendwire_herdr_unhealthy")
    if observed.get("cursor_conflict"):
        return _tendwire_non_success(runtime, "tendwire_delta_cursor_changed")
    delta_observation = observed.get("delta_observation")
    delta: dict[str, Any] | None = None
    snapshot = observed["snapshot"]
    observed_snapshot_workers: list[dict[str, Any]] = []
    if delta_observation is not None:
        delta = delta_observation["delta"]
        if delta_observation.get("kind") == "error":
            return _tendwire_non_success(
                runtime,
                str(delta_observation.get("status") or "turn_delta_failed"),
            )
    turns_payload = observed["turns"]
    pending_payload = observed["pending"]
    for name, payload in (("snapshot", snapshot), ("turns", turns_payload), ("pending", pending_payload)):
        if payload.get("ok") is False:
            return _tendwire_non_success(runtime, f"tendwire_{name}_failed")
    turn_schema = turns_payload.get("schema_version")
    if (
        type(turn_schema) is not int
        or turn_schema != TURN_SCHEMA_VERSION
    ):
        return _unsupported_turn_schema_version(runtime, turn_schema)
    chat_id = config.telegram_chat_id(store)
    with state.lock_phase("sync.validate"):
        try:
            turns_payload = _validate_turns_payload(turns_payload)
        except _TurnContentError as exc:
            # The list envelope/schema is connector-wide. Descriptor defects
            # are converted to bounded row-local outcomes by validation.
            return _tendwire_non_success(runtime, exc.status)
    runtime = _offlock_runtime(store, runtime)

    def _yield_between_turns() -> None:
        if not state.lock_held():
            return
        state.save_state(store)
        with state.released_lock():
            pass
        state.reload_state_in_place(store)

    yield_barrier = (
        _yield_between_turns
        if config.offlock_interpane_yield_enabled() and not runtime.dry_run
        else None
    )
    if yield_barrier is not None:
        # Ingress deliberately needs two consecutive state-lock acquisitions:
        # the gateway first fsyncs its immutable request shell, then the AF_UNIX
        # submitter acquires the lock to attach canonical bytes and submit them.
        # The off-lock observation window can admit the first acquisition, but
        # without this handoff sync immediately reacquires the flock and enters
        # the comparatively heavy reconciliation phase before the request starts.
        # Yield once more after observation so the waiting submitter wins a
        # lock window instead of holding every later item in its strict FIFO
        # lane behind a full sync pass.
        with state.lock_phase("sync.post_observe_handoff"):
            yield_barrier()
    with state.lock_phase("sync.generations"):
        observed_snapshot_workers = _workers(snapshot)
        (
            snapshot,
            generation_resolutions,
            generation_resolution_changed,
        ) = _resolve_stable_worker_generations(
            store,
            snapshot,
            turns_payload,
            observed_at=observed_at,
        )
    changed = bool(generation_resolution_changed)
    with state.lock_phase("sync.sources"):
        source_counts = _sync_sources(
            store,
            snapshot,
            turns_payload,
            runtime,
            chat_id=chat_id,
            yield_barrier=yield_barrier,
        )
    worker_rebinds = 0
    for resolution in generation_resolutions:
        from_worker_id = resolution["from_worker_id"]
        to_worker_id = resolution["to_worker_id"]
        if not from_worker_id or from_worker_id == to_worker_id:
            continue
        _entry_key, rebound_entry = state.find_worker_entry_by_stable_key(
            store, resolution["stable_key"]
        )
        if (
            rebound_entry is None
            or _entry_worker_id(rebound_entry) != to_worker_id
        ):
            continue
        worker_rebinds += int(
            state.record_worker_generation_rebind(
                store,
                rebound_entry,
                stable_key=resolution["stable_key"],
                from_worker_id=from_worker_id,
                to_worker_id=to_worker_id,
                reason=resolution["reason"],
                observed_at=observed_at,
            )
        )
    with state.lock_phase("sync.lifecycle_cleanup"):
        lifecycle_cleanup = _sync_topic_lifecycle_cleanup(
            store,
            runtime,
            chat_id=chat_id,
            now=observed_at,
        )
    if delta_observation is not None:
        # Lifecycle cleanup may save, release the lock, and reload the store in
        # place. Rebind the observation to the reloaded delta lane so the final
        # page watermark/checkpoint is not written into an orphaned sub-dict.
        delta = _delta_state(store, now=observed_at)
        delta_observation["delta"] = delta
    with state.lock_phase("sync.routing"):
        routing_repaired = _repair_space_mode_routing_state(store)
        message_bindings = _backfill_message_bindings(store)
    live_worker_ids = {
        compact_ws(worker.get("id"), 160)
        for worker in observed_snapshot_workers
        if _worker_is_open(worker)
    }
    live_worker_ids.discard("")
    try:
        feed_turns_payload = (
            {"schema_version": TURN_SCHEMA_VERSION, "turns": []}
            if delta_observation is not None
            and delta_observation.get("kind") == "delta"
            and delta_observation.get("page", {}).get("mode") == "bootstrap"
            else turns_payload
        )
        with state.lock_phase("sync.turns"):
            turn_counts = _sync_turns(
                store,
                feed_turns_payload,
                pending_payload,
                runtime,
                chat_id=chat_id,
                live_worker_ids=live_worker_ids,
                yield_barrier=yield_barrier,
                checkpoint_after_delivery=(
                    delta_observation is not None
                    and delta_observation.get("kind") == "delta"
                    and delta_observation.get("page", {}).get("mode")
                    == "changes"
                ),
                retained_projection=(
                    delta.get("projection")
                    if delta_observation is not None
                    and delta_observation.get("kind") == "delta"
                    and isinstance(delta, dict)
                    else None
                ),
            )
    except _TurnContentError as exc:
        if not exc.conflict:
            return _tendwire_non_success(runtime, exc.status)
        relisted = runtime.tendwire.turns()
        if relisted.get("ok") is False:
            return _tendwire_non_success(
                runtime, "tendwire_turns_relist_failed"
            )
        try:
            turns_payload = _validate_turns_payload(relisted)
            with state.lock_phase("sync.turns_relist"):
                turn_counts = _sync_turns(
                    store,
                    turns_payload,
                    pending_payload,
                    runtime,
                    chat_id=chat_id,
                    live_worker_ids=live_worker_ids,
                    relist_on_conflict=False,
                    yield_barrier=yield_barrier,
                    checkpoint_after_delivery=(
                        delta_observation is not None
                        and delta_observation.get("kind") == "delta"
                        and delta_observation.get("page", {}).get("mode")
                        == "changes"
                    ),
                )
        except _TurnContentError as retry_exc:
            return _tendwire_non_success(runtime, retry_exc.status)
    with state.lock_phase("sync.decisions"):
        decision_result = _deliver_decisions(
            store,
            pending_payload,
            runtime,
            chat_id=chat_id,
            yield_barrier=yield_barrier,
        )
        routing_repaired += _repair_space_mode_routing_state(store)
    snapshot_worker_ids = {
        compact_ws(worker.get("id"), 160)
        for worker in observed_snapshot_workers
    }
    snapshot_worker_ids.discard("")
    with state.lock_phase("sync.topic_cleanup"):
        deletion_cleanup = _cleanup_topics(
            store,
            runtime,
            chat_id=chat_id,
            snapshot_worker_ids=snapshot_worker_ids,
        )
    topic_cleanup = {
        **deletion_cleanup,
        **{
            key: value
            for key, value in lifecycle_cleanup.items()
            if key not in {"changed", "deleted"}
        },
        "deleted": int(deletion_cleanup.get("deleted") or 0)
        + int(lifecycle_cleanup.get("deleted") or 0),
        "changed": bool(
            deletion_cleanup.get("changed")
            or lifecycle_cleanup.get("changed")
        ),
    }
    changed = changed or bool(
        source_counts["created"]
        or source_counts["updated"]
        or source_counts["icon_updated"]
        or worker_rebinds
        or routing_repaired
        or turn_counts["sent"]
        or turn_counts["updated"]
        or topic_cleanup.get("changed")
        or message_bindings
        or decision_result.get("changed")
    )
    if config.pinned_status_enabled():
        with state.lock_phase("sync.pinned.account_usage"):
            account_usage = _account_usage_snapshot_offlock(store, runtime)
        with state.lock_phase("sync.pinned.overview"):
            pinned_changed = _sync_pinned(
                store,
                runtime,
                chat_id=chat_id,
                yield_barrier=yield_barrier,
                account_usage=account_usage,
            )
        with state.lock_phase("sync.pinned.topics"):
            topic_pinned_updated = _sync_topic_pinned_statuses(
                store,
                runtime,
                chat_id=chat_id,
                yield_barrier=yield_barrier,
                account_usage=account_usage,
            )
    else:
        pinned_changed = False
        topic_pinned_updated = 0
    changed = changed or pinned_changed or bool(topic_pinned_updated)
    delta_card_updates = 0
    with state.lock_phase("sync.delta_apply"):
        if delta_observation is not None:
            if delta_observation["kind"] == "delta":
                if (
                    store.get(_DELTA_STATE_KEY)
                    != delta_observation.get("apply_basis")
                ):
                    # A later off-lock phase let another full pass advance the
                    # watermark.  Do not apply this pass's stale checkpoint.
                    return _tendwire_non_success(
                        runtime, "tendwire_delta_cursor_changed"
                    )
                delta_card_updates = _finish_delta_page(
                    store,
                    delta_observation,
                    runtime,
                    now=observed_at,
                )
            elif delta_observation["kind"] == "full":
                delta_card_updates = _apply_full_reconciliation(
                    store,
                    delta_observation["delta"],
                    turns_payload,
                    runtime,
                    now=observed_at,
                )
            changed = changed or delta_observation["kind"] != "noop"
    turn_final_result = {
        "enabled": runtime.with_outbox,
        "polled": 0,
        "operations": 0,
        "delivered": 0,
        "acked": 0,
        "failed": 0,
        "deferred": 0,
        "uncertain": 0,
        "staged": 0,
        "content_pages": 0,
        "changed": False,
    }
    outbox_result = {"enabled": runtime.with_outbox, "polled": 0, "delivered": 0, "acked": 0, "failed": 0, "deferred": 0, "changed": False}
    if runtime.with_outbox:
        with state.lock_phase("sync.outbox"):
            delivery_budget = _delivery_write_budget(runtime)
            remaining = delivery_budget.remaining
            turn_final_result = _drain_turn_final(
                store,
                turns_payload,
                runtime,
                chat_id=chat_id,
                max_operations=remaining,
                yield_barrier=yield_barrier,
                turn_projection=(
                    delta.get("projection")
                    if isinstance(delta, dict)
                    and isinstance(delta.get("projection"), dict)
                    else None
                ),
            )
            delivery_budget.spent += int(
                turn_final_result.get("operations") or 0
            )
            remaining = max(
                0, delivery_budget.remaining
            )
            outbox_result = drain_outbox(
                store,
                _exact_provider_client(
                    runtime.telegram,
                    reason=(
                        "sync attention Telegram exact identifiers"
                    ),
                ),
                _exact_provider_client(
                    runtime.tendwire,
                    reason=(
                        "sync attention Tendwire exact leased references"
                    ),
                ),
                chat_id=chat_id,
                max_sends=remaining,
                dry_run=runtime.dry_run,
                yield_barrier=yield_barrier,
                ack_barrier_persists_state=True,
            )
            delivery_budget.spent += int(
                outbox_result.get("physical_writes") or 0
            )
        changed = changed or bool(turn_final_result.get("changed")) or bool(outbox_result.get("changed"))
    telegram_backpressure = _telegram_backpressure_since(
        store, backpressure_sequence
    )
    changed = changed or bool(telegram_backpressure["count"])
    response_fold_health = state.response_fold_health(store)
    completed_deliveries = (
        int(turn_counts["sent"])
        + int(turn_final_result.get("delivered") or 0)
        + int(outbox_result.get("delivered") or 0)
    )
    pending_delivery_work = (
        int(turn_counts.get("work_pending") or 0)
        + int(turn_final_result.get("failed") or 0)
        + int(turn_final_result.get("deferred") or 0)
        + int(outbox_result.get("failed") or 0)
        + int(outbox_result.get("deferred") or 0)
    )
    delivery_stalled = bool(
        pending_delivery_work and completed_deliveries == 0
    )
    outbound_delivery_health = {
        "ok": not delivery_stalled,
        "status": (
            "outbound_delivery_stalled"
            if delivery_stalled
            else "healthy"
        ),
        "pending_count": pending_delivery_work,
        "completed_count": completed_deliveries,
        "physical_writes": _delivery_write_budget(runtime).spent,
    }
    overall_ok = bool(not delivery_stalled and response_fold_health["ok"])
    return {
        "ok": overall_ok,
        **(
            {
                "status": (
                    outbound_delivery_health["status"]
                    if delivery_stalled
                    else response_fold_health["status"]
                )
            }
            if not overall_ok
            else {}
        ),
        "changed": changed,
        "created": source_counts["created"],
        "updated": source_counts["updated"],
        "panes": source_counts["panes"],
        "spaces": source_counts["spaces"],
        "icon_updated": source_counts["icon_updated"],
        "worker_rebinds": worker_rebinds,
        "pinned_status_updated": int(pinned_changed) + topic_pinned_updated,
        "feed_sent": turn_counts["feed_sent"],
        "sent": turn_counts["sent"],
        "routing_repaired": routing_repaired,
        "turn_updates": turn_counts["updated"],
        "response_folds": {
            "attempted": int(
                turn_counts.get("response_fold_attempted") or 0
            ),
            "folded": int(turn_counts.get("response_folded") or 0),
            "failed": int(
                turn_counts.get("response_fold_failed") or 0
            ),
        },
        **(
            {"tendwire_delta_sync": _delta_health(delta, now=observed_at)}
            if delta is not None
            else {}
        ),
        "message_bindings": message_bindings,
        "topic_cleanup": topic_cleanup,
        "content_pages": int(turn_counts["content_pages"])
        + int(turn_final_result.get("content_pages") or 0),
        "turn_content_outcomes": _turn_content_outcomes(turns_payload),
        "remote_decisions": decision_result,
        "tendwire_turn_final": turn_final_result,
        "tendwire_outbox": outbox_result,
        "telegram_backpressure": telegram_backpressure,
        "outbound_delivery": outbound_delivery_health,
        "outbound_response_folds": response_fold_health,
    }
