"""Remote Telegram controls for Tendwire's structured Claude decisions.

The connector only acts on Tendwire's neutral, single-question decision
contract.  Anything ambiguous stays visible through the ordinary pending
notice but deliberately gets no buttons: a wrong remote answer is worse than
requiring the owner to finish the prompt at the desk.
"""

from __future__ import annotations

# H8-SLOC-BEGIN decisions-imports
import base64
import fcntl
import json
import time
from contextlib import contextmanager
# H8-SLOC-END decisions-imports
import hashlib
from dataclasses import dataclass, field, replace
from typing import Any, Callable, Mapping

from . import config, state
from .ingress_identity import validate_request_id
from .safe import compact_ws, html_escape, sanitize_text, short_hash
from .telegram_delivery import TelegramClient, classify_telegram_error
from .tendwire_client import TendwireClient


CALLBACK_PREFIX = "hdec:"
CUSTOM_TOKEN = "custom"
SUBMIT_TOKEN = "__submit__"
SUPPORTED_KINDS = frozenset({"single", "multi", "plan"})
RESERVED_OPTION_REFS = frozenset({CUSTOM_TOKEN, SUBMIT_TOKEN})
CALLBACK_DATA_LIMIT = 64
ANSWER_IN_PROGRESS_REPLY = (
    "That prompt is being answered right now — try again in a moment."
)
ACCEPTED_ARTIFACT_LIMIT = 64
# H8-SLOC-BEGIN decisions-constants
INGRESS_MARKUP_LIMIT_BYTES = 32 * 1024
PROVIDER_GUARD_TIMEOUT_SECONDS = 30.0
_GUARDED_TELEGRAM_CAPABILITIES = frozenset(
    {
        "telegram.edit_message",
        "telegram.edit_message_reply_markup",
        "telegram.delete_message",
    }
)
# H8-SLOC-END decisions-constants


@dataclass(frozen=True)
class ProviderOperation:
    """Immutable decision-provider request with captured pane provenance."""

    capability: str
    reason: str
    entry_key: str
    worker_id: str
    topic_id: str
    decision_id: str
    # H8-SLOC-BEGIN decisions-operation-field
    decision_state_fingerprint: str
    # H8-SLOC-END decisions-operation-field
    message_id: str = ""
    scope: str = "entry"
    artifact_kind: str = ""
    provenance: Mapping[str, Any] | None = None
    args: tuple[Any, ...] = ()
    kwargs: tuple[tuple[str, Any], ...] = ()


@dataclass(frozen=True)
class ProviderExecution:
    result: dict[str, Any]
    disposition: str
    provenance: dict[str, Any] = field(default_factory=dict)
    receipt_id: str = ""


ProviderExecutor = Callable[[ProviderOperation], ProviderExecution]


# H8-SLOC-BEGIN decisions-presenter-guard
@contextmanager
def _legacy_provider_mutation_guard(
    owner: state.PhysicalOwner, deadline: float,
):
    held_fd = state._held_lock_fd()
    if held_fd is None or not state.lock_actually_held():
        with state.provider_mutation_guard(config.state_path(), owner, deadline) as guard:
            yield guard
        return
    fcntl.flock(held_fd, fcntl.LOCK_UN)
    state._record_lock_released(held_fd)
    state._LOCK_STATE.release_depth = 1
    restored = False
    try:
        with state.provider_mutation_guard(config.state_path(), owner, deadline) as guard:
            fcntl.flock(held_fd, fcntl.LOCK_EX)
            state._record_lock_acquired(held_fd)
            state._LOCK_STATE.release_depth = 0
            try:
                yield guard
            finally:
                fcntl.flock(held_fd, fcntl.LOCK_UN)
                state._record_lock_released(held_fd)
                state._LOCK_STATE.release_depth = 1
        fcntl.flock(held_fd, fcntl.LOCK_EX)
        state._record_lock_acquired(held_fd)
        state._LOCK_STATE.release_depth = 0
        restored = True
    finally:
        if not restored:
            fcntl.flock(held_fd, fcntl.LOCK_EX)
            state._record_lock_acquired(held_fd)
            state._LOCK_STATE.release_depth = 0


def _decision_state_fingerprint(record: Mapping[str, Any]) -> str:
    values = {
        "decision_id": str(record.get("decision_id") or record.get("decision_ref") or ""),
        "revision_digest": str(record.get("revision_digest") or record.get("content_hash") or ""),
        "message_binding_id": str(record.get("message_binding_id") or record.get("message_id") or ""),
        "message_id": str(record.get("message_id") or ""),
        "selected": [str(value) for value in record.get("selected", []) if isinstance(value, str)],
        "await_freeform": record.get("await_freeform") is True,
        "desired_markup_fingerprint": str(record.get("desired_markup_fingerprint") or ""),
        "applied_markup_fingerprint": str(record.get("applied_markup_fingerprint") or ""),
    }
    return short_hash(values, 32)


def _guarded_decision_target_is_current(
    store: dict[str, Any], operation: ProviderOperation
) -> bool:
    current = active_decision(store, operation.topic_id)
    if current is not None:
        identity = str(current.get("decision_id") or current.get("decision_ref") or "")
        if identity == operation.decision_id and str(current.get("message_id") or "") == operation.message_id:
            return _decision_state_fingerprint(current) == operation.decision_state_fingerprint
    for raw in _accepted_artifacts(store, create=False).values():
        if isinstance(raw, dict) and (
            str(raw.get("decision_id") or ""), str(raw.get("topic_id") or ""),
            str(raw.get("message_id") or ""),
        ) == (operation.decision_id, operation.topic_id, operation.message_id):
            return True
    return False
# H8-SLOC-END decisions-presenter-guard


def _operation(
    capability: str,
    *,
    reason: str,
    record: Mapping[str, Any],
    topic_id: str,
    message_id: str = "",
    scope: str = "entry",
    artifact_kind: str = "",
    args: tuple[Any, ...] = (),
    kwargs: Mapping[str, Any] | None = None,
) -> ProviderOperation:
    if not reason or capability not in reason:
        raise ValueError(
            "decision provider reason must name its capability"
        )
    return ProviderOperation(
        capability=capability,
        reason=reason,
        entry_key=str(record.get("entry_key") or ""),
        worker_id=str(record.get("worker_id") or ""),
        topic_id=str(topic_id or ""),
        decision_id=str(record.get("decision_id") or ""),
        # H8-SLOC-BEGIN decisions-operation-capture
        decision_state_fingerprint=_decision_state_fingerprint(record),
        # H8-SLOC-END decisions-operation-capture
        message_id=str(message_id or ""),
        scope=str(scope or "entry"),
        artifact_kind=str(artifact_kind or ""),
        provenance=(
            dict(record.get("provider_provenance"))
            if isinstance(record.get("provider_provenance"), Mapping)
            else None
        ),
        args=tuple(args),
        kwargs=tuple((str(key), value) for key, value in (kwargs or {}).items()),
    )


def _execute_direct(
    operation: ProviderOperation,
    *,
    telegram: TelegramClient | None,
    tendwire: TendwireClient | None,
) -> ProviderExecution:
    """Synchronous non-offlock adapter used by gateway and unit-test callers."""

    kwargs = dict(operation.kwargs)
    if operation.capability == "telegram.send_message":
        assert telegram is not None
        result = telegram.send_message(*operation.args, **kwargs)
    elif operation.capability == "telegram.edit_message":
        assert telegram is not None
        result = telegram.edit_message(*operation.args, **kwargs)
    elif operation.capability == "telegram.edit_message_reply_markup":
        assert telegram is not None
        result = telegram.edit_message_reply_markup(
            *operation.args, **kwargs
        )
    elif operation.capability == "telegram.delete_message":
        assert telegram is not None
        result = telegram.delete_message(*operation.args, **kwargs)
    elif operation.capability == "tendwire.command":
        assert tendwire is not None
        result = tendwire.command(*operation.args, **kwargs)
    else:
        raise ValueError(
            f"unsupported decision capability: {operation.capability}"
        )
    return ProviderExecution(
        dict(result),
        "apply",
        provenance=dict(operation.provenance or {}),
    )


def _execute(
    operation: ProviderOperation,
    *,
    telegram: TelegramClient | None = None,
    tendwire: TendwireClient | None = None,
    provider_executor: ProviderExecutor | None = None,
    # H8-SLOC-BEGIN decisions-execute-signature
    state_store: dict[str, Any] | None = None,
    # H8-SLOC-END decisions-execute-signature
) -> ProviderExecution:
    # H8-SLOC-BEGIN decisions-execute-guard
    def invoke() -> ProviderExecution:
        if provider_executor is not None:
            return provider_executor(operation)
        return _execute_direct(
            operation, telegram=telegram, tendwire=tendwire
        )

    if operation.capability not in _GUARDED_TELEGRAM_CAPABILITIES:
        return invoke()
    if not operation.args:
        raise ValueError("guarded Telegram mutation lacks chat identity")
    owner = state.PhysicalOwner(
        bot_identity="manager",
        chat_id=str(operation.args[0]),
        topic_id=operation.topic_id,
    )
    deadline = time.monotonic() + PROVIDER_GUARD_TIMEOUT_SECONDS
    if state_store is not None and state.lock_actually_held():
        # The retained presenter owns a mutable schema-2 projection.  Its
        # accepted-artifact and active-control changes must be durable before
        # releasing the state flock to wait for the physical-owner guard.
        state.save_state(state_store)
    with _legacy_provider_mutation_guard(owner, deadline):
        if state_store is not None and state.lock_actually_held():
            state.reload_state_in_place(state_store)
            if not _guarded_decision_target_is_current(state_store, operation):
                return ProviderExecution(
                    {"ok": False, "status": "owner_changed"}, "abandon"
                )
        if provider_executor is None and state.lock_actually_held():
            with state.released_lock():
                execution = invoke()
            if state_store is not None:
                state.reload_state_in_place(state_store)
            return execution
        return invoke()
    # H8-SLOC-END decisions-execute-guard


def _ref56(decision_id: str) -> str:
    """Return a deterministic 56-bit handle, keeping callback data private and short."""

    return hashlib.sha256(decision_id.encode("utf-8")).hexdigest()[:14]


def _callback_data(decision_id: str, option_ref: str) -> str | None:
    value = f"{CALLBACK_PREFIX}{_ref56(decision_id)}:{option_ref}"
    return value if len(value.encode("utf-8")) <= CALLBACK_DATA_LIMIT else None


def _decision_blob(item: dict[str, Any]) -> dict[str, Any] | None:
    meta = item.get("meta") if isinstance(item.get("meta"), dict) else {}
    decision = meta.get("decision") if isinstance(meta, dict) else None
    return decision if isinstance(decision, dict) else None


def _pending_items(payload: dict[str, Any]) -> list[dict[str, Any]]:
    rows = payload.get("pending_interactions", payload.get("pending", []))
    return [row for row in rows if isinstance(row, dict)] if isinstance(rows, list) else []


def _normalize_options(value: Any, decision_id: str) -> list[dict[str, str]] | None:
    if not isinstance(value, list) or not value:
        return None
    options: list[dict[str, str]] = []
    seen: set[str] = set()
    for raw in value:
        if not isinstance(raw, dict):
            return None
        option_ref = raw.get("ref")
        label = compact_ws(raw.get("label"), 80)
        if (
            not isinstance(option_ref, str)
            or not option_ref
            or sanitize_text(option_ref, 160) != option_ref
            or option_ref in seen
            or option_ref in RESERVED_OPTION_REFS
            or not label
            or _callback_data(decision_id, option_ref) is None
        ):
            return None
        seen.add(option_ref)
        options.append({"ref": option_ref, "label": label})
    return options


def resolve_decisions(
    store: dict[str, Any], pending_payload: dict[str, Any]
) -> list[dict[str, Any]]:
    """Join safe decision blobs to one uniquely routable worker topic.

    A shared topic can hold only one active keyboard record.  If Tendwire ever
    reports two decisions for the same topic, both are skipped rather than
    letting arrival order choose which worker Telegram controls.
    """

    candidates: list[dict[str, Any]] = []
    topic_counts: dict[str, int] = {}
    for item in _pending_items(pending_payload):
        decision = _decision_blob(item)
        if decision is None:
            continue
        kind = str(decision.get("kind") or "").strip().lower()
        question_count = decision.get("question_count", 1)
        if (
            kind not in SUPPORTED_KINDS
            or isinstance(question_count, bool)
            or not isinstance(question_count, int)
            or question_count > 1
            or question_count < 1
        ):
            continue
        worker_id = compact_ws(item.get("worker_id"), 160)
        entry_key = state.find_entry_key_by_worker(store, worker_id)
        if entry_key is None:
            continue
        entry = state.source_worker_entries(store).get(entry_key)
        topic_id = compact_ws((entry or {}).get("topic_id"), 80)
        if not topic_id:
            continue
        decision_id = decision.get("decision_ref")
        prompt = sanitize_text(decision.get("prompt"), 12000).strip()
        if (
            not isinstance(decision_id, str)
            or not decision_id
            or sanitize_text(decision_id, 4096) != decision_id
            or not prompt
        ):
            continue
        options = _normalize_options(decision.get("options"), decision_id)
        if options is None:
            continue
        content_hash = short_hash(
            {
                "kind": kind,
                "prompt": prompt,
                "options": options,
                "multi_select": decision.get("multi_select") is True,
                "question_count": question_count,
            },
            24,
        )
        candidates.append(
            {
                "decision_id": decision_id,
                "worker_id": worker_id,
                "entry_key": entry_key,
                "topic_id": topic_id,
                "kind": kind,
                "prompt": prompt,
                "options": options,
                "content_hash": content_hash,
            }
        )
        topic_counts[topic_id] = topic_counts.get(topic_id, 0) + 1
    return [row for row in candidates if topic_counts[row["topic_id"]] == 1]


def _active_records(
    store: dict[str, Any], *, create: bool
) -> dict[str, dict[str, Any]]:
    decisions = store.get("decisions")
    if not isinstance(decisions, dict):
        if not create:
            return {}
        decisions = {}
        store["decisions"] = decisions
    active = decisions.get("active")
    if not isinstance(active, dict):
        if not create:
            return {}
        active = {}
        decisions["active"] = active
    return active


def _accepted_artifacts(
    store: dict[str, Any], *, create: bool
) -> dict[str, dict[str, Any]]:
    bucket = store.get("decisions")
    if not isinstance(bucket, dict):
        if not create:
            return {}
        bucket = {}
        store["decisions"] = bucket
    artifacts = bucket.get("accepted_artifacts")
    if not isinstance(artifacts, dict):
        if not create:
            return {}
        artifacts = {}
        bucket["accepted_artifacts"] = artifacts
    return artifacts


def provider_acceptance_capacity_available(
    store: dict[str, Any], operation: ProviderOperation
) -> bool:
    if not operation.artifact_kind:
        return True
    return len(_accepted_artifacts(store, create=False)) < (
        ACCEPTED_ARTIFACT_LIMIT
    )


def checkpoint_provider_acceptance(
    store: dict[str, Any],
    operation: ProviderOperation,
    result: Mapping[str, Any],
    provenance: Mapping[str, Any],
) -> str:
    """Durably name a provider-accepted artifact before disposition handling."""

    kind = operation.artifact_kind
    accepted_message_id = str(
        result.get("reply_markup_message_id")
        or result.get("message_id")
        or ""
    )
    accepted = (
        kind in {"decision_card", "decision_notice"}
        and result.get("ok") is True
        and bool(accepted_message_id)
    ) or (
        kind == "decision_submission"
        and result.get("ok") is True
        and str(result.get("status") or "") == "accepted"
    )
    if not accepted:
        return ""
    receipt_id = short_hash(
        {
            "kind": kind,
            "decision_id": operation.decision_id,
            "worker_id": operation.worker_id,
            "topic_id": operation.topic_id,
            "message_id": accepted_message_id,
            "request": (
                operation.args[0]
                if kind == "decision_submission" and operation.args
                else None
            ),
        },
        32,
    )
    artifacts = _accepted_artifacts(store, create=True)
    # Capacity controls whether new provider work may start. Once the provider
    # has accepted an artefact, that fact must be recordable even if a
    # concurrent writer filled the nominal ledger while the lock was released.
    artifacts.setdefault(
        receipt_id,
        {
            "kind": kind,
            "decision_id": operation.decision_id,
            "worker_id": operation.worker_id,
            "entry_key": operation.entry_key,
            "topic_id": operation.topic_id,
            "message_id": accepted_message_id or operation.message_id,
            "provenance": dict(provenance),
        },
    )
    return receipt_id


def complete_provider_acceptance(
    store: dict[str, Any], receipt_id: str
) -> None:
    if receipt_id:
        _accepted_artifacts(store, create=False).pop(receipt_id, None)


def active_decision(store: dict[str, Any], topic_id: str | int) -> dict[str, Any] | None:
    record = _active_records(store, create=False).get(str(topic_id))
    return record if isinstance(record, dict) else None


def needs_sync(store: dict[str, Any], pending_payload: dict[str, Any]) -> bool:
    """Return whether a pass can post or retract a decision keyboard."""

    return (
        bool(_active_records(store, create=False))
        or bool(_accepted_artifacts(store, create=False))
        or any(
            _decision_blob(item) is not None
            for item in _pending_items(pending_payload)
        )
    )


def render_decision(record: dict[str, Any]) -> str:
    labels = {
        "single": "Choose one answer",
        "multi": "Choose one or more answers",
        "plan": "Review the plan",
    }
    label = labels.get(str(record.get("kind") or ""), "Decision required")
    return (
        f"<b>{html_escape(label, 80)}</b>\n"
        f"{html_escape(record.get('prompt'), 12000)}"
    )


def inline_keyboard(record: dict[str, Any]) -> dict[str, list[list[dict[str, str]]]]:
    """Build Telegram's InlineKeyboardMarkup JSON object for one active record."""

    selected = {
        str(value)
        for value in record.get("selected", [])
        if isinstance(value, str)
    }
    kind = str(record.get("kind") or "")
    decision_id = str(record.get("decision_id") or "")
    rows: list[list[dict[str, str]]] = []
    for option in record.get("options", []):
        if not isinstance(option, dict):
            continue
        option_ref = str(option.get("ref") or "")
        callback_data = _callback_data(decision_id, option_ref)
        if callback_data is None:
            continue
        marker = ""
        if kind == "multi":
            marker = "✅ " if option_ref in selected else "▫️ "
        rows.append(
            [
                {
                    "text": marker + compact_ws(option.get("label"), 80),
                    "callback_data": callback_data,
                }
            ]
        )
    if kind == "single":
        rows.append(
            [
                {
                    "text": "✍️ Write a different answer",
                    "callback_data": str(_callback_data(decision_id, CUSTOM_TOKEN)),
                }
            ]
        )
    elif kind == "multi":
        rows.append(
            [
                {
                    "text": "✅ Submit",
                    "callback_data": str(_callback_data(decision_id, SUBMIT_TOKEN)),
                }
            ]
        )
    return {"inline_keyboard": rows}


# H8-SLOC-BEGIN decisions-renderer
def render_ingress_markup(
    snapshot: state.DecisionIngressResult,
    desired_selected_refs: tuple[str, ...],
) -> tuple[dict[str, Any], str]:
    """Render the exact bounded markup used by a typed local decision action."""

    if not isinstance(snapshot, state.DecisionIngressResult):
        raise TypeError("decision ingress markup requires a typed snapshot")
    if not isinstance(desired_selected_refs, tuple):
        raise TypeError("desired selected refs must be a tuple")
    if snapshot.status is not state.DecisionStatus.ACTIVE or snapshot.mode not in SUPPORTED_KINDS:
        raise ValueError("decision ingress markup requires an active supported decision")
    if any(not isinstance(option, state.DecisionOption) for option in snapshot.options):
        raise ValueError("invalid decision ingress option")
    record = {
        "decision_id": snapshot.decision_ref,
        "kind": snapshot.mode,
        "options": [
            {"ref": option.option_ref, "label": option.label}
            for option in snapshot.options
        ],
        "selected": list(desired_selected_refs),
    }
    projected = _project_ingress_record(record)
    if projected is None or projected["refs"] != snapshot.option_refs:
        raise ValueError("invalid decision ingress options or selections")
    markup = inline_keyboard(record)
    encoded = json.dumps(
        markup,
        sort_keys=True,
        ensure_ascii=False,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")
    if len(encoded) > INGRESS_MARKUP_LIMIT_BYTES:
        raise ValueError("decision ingress markup is too large")
    return markup, short_hash(markup, 32)
# H8-SLOC-END decisions-renderer


# H8-SLOC-BEGIN decisions-state-adapter
def _project_ingress_record(record: Mapping[str, Any]) -> dict[str, Any] | None:
    decision_ref = str(record.get("decision_id") or record.get("decision_ref") or "")
    raw_options = record.get("options")
    raw_selected = record.get("selected")
    if not decision_ref or not isinstance(raw_options, list) or len(raw_options) > 64:
        return None
    options: list[tuple[str, str]] = []
    for raw in raw_options:
        if not isinstance(raw, dict):
            return None
        ref, label = raw.get("ref"), raw.get("label")
        if (not isinstance(ref, str) or not ref or len(ref.encode("utf-8")) > 512
                or ref in {item[0] for item in options} or not isinstance(label, str)
                or compact_ws(label, 80) != label or _callback_data(decision_ref, ref) is None):
            return None
        options.append((ref, label))
    refs = tuple(ref for ref, _label in options)
    if not isinstance(raw_selected, list) or len(raw_selected) > 64 or any(not isinstance(ref, str) for ref in raw_selected):
        return None
    selected = tuple(raw_selected)
    if len(set(selected)) != len(selected) or selected != tuple(ref for ref in refs if ref in set(selected)):
        return None
    message_id = str(record.get("message_id") or "")
    return {
        "decision_ref": decision_ref,
        "revision": str(record.get("revision_digest") or record.get("content_hash") or ""),
        "mode": str(record.get("kind") or record.get("mode") or ""),
        "worker_id": str(record.get("worker_id") or ""),
        "message_id": message_id,
        "binding_id": str(record.get("message_binding_id") or message_id),
        "options": tuple(options), "refs": refs, "selected": selected,
        "await_freeform": record.get("await_freeform") is True,
        "render_fingerprint": str(record.get("render_fingerprint") or record.get("content_hash") or ""),
        "desired_markup_fingerprint": str(record.get("desired_markup_fingerprint") or ""),
        "bot_identity": str(record.get("provider_bot_identity") or record.get("bot_identity") or record.get("bot_kind") or "manager"),
    }


def _ingress_mutation_digest(mutation: state.DecisionMutation) -> str:
    body = {"request_id": mutation.request_id, "kind": mutation.kind.value,
            "decision_ref": mutation.decision_ref, "revision_digest": mutation.revision_digest,
            "option_ref": mutation.option_ref, "desired_selected_refs": list(mutation.desired_selected_refs),
            "desired_markup_fingerprint": mutation.desired_markup_fingerprint}
    encoded = json.dumps(body, sort_keys=True, ensure_ascii=False, separators=(",", ":"), allow_nan=False).encode("utf-8")
    digest = hashlib.sha256(b"herdres-decision-mutation-v1\0" + encoded).digest()
    return base64.urlsafe_b64encode(digest).rstrip(b"=").decode("ascii")


def _reduce_ingress_mutation(
    record: dict[str, Any], mutation: state.DecisionMutation,
) -> state.DecisionMutationStatus:
    projected = _project_ingress_record(record)
    if projected is None or projected["revision"] != mutation.revision_digest:
        return state.DecisionMutationStatus.CONFLICT
    digest = _ingress_mutation_digest(mutation)
    applied = record.setdefault("applied_ingress_identities", [])
    if not isinstance(applied, list):
        raise RuntimeError("decision ingress identity store is corrupt")
    for raw in applied:
        if not isinstance(raw, dict):
            raise RuntimeError("decision ingress identity store is corrupt")
        if raw.get("request_id") == mutation.request_id and raw.get("kind") == mutation.kind.value:
            return (state.DecisionMutationStatus.ALREADY_APPLIED
                    if raw.get("mutation_digest") == digest else state.DecisionMutationStatus.CONFLICT)
    if len(applied) >= 256:
        return state.DecisionMutationStatus.CONFLICT
    refs, current, desired = projected["refs"], projected["selected"], mutation.desired_selected_refs
    if len(set(desired)) != len(desired) or desired != tuple(ref for ref in refs if ref in set(desired)):
        return state.DecisionMutationStatus.CONFLICT
    mode, option, markup = projected["mode"], mutation.option_ref, mutation.desired_markup_fingerprint
    if mutation.kind is state.DecisionMutationKind.ARM_FREEFORM:
        valid = mode == "single" and option is None and desired == current and not markup
        if valid:
            record["await_freeform"] = True
    elif mutation.kind is state.DecisionMutationKind.TOGGLE_OPTION:
        toggled = set(current)
        toggled.discard(option) if option in toggled else toggled.add(option)
        valid = mode == "multi" and option in refs and bool(markup) and desired == tuple(ref for ref in refs if ref in toggled)
        if valid:
            record["selected"], record["desired_markup_fingerprint"] = list(desired), markup
    else:
        valid = option in refs and bool(markup) and desired == current and markup == str(record.get("desired_markup_fingerprint") or "")
        if valid:
            record["applied_markup_fingerprint"] = markup
    if not valid:
        return state.DecisionMutationStatus.CONFLICT
    applied.append({"request_id": mutation.request_id, "kind": mutation.kind.value, "mutation_digest": digest})
    return state.DecisionMutationStatus.APPLIED
# H8-SLOC-END decisions-state-adapter


def _retract(
    # H8-SLOC-BEGIN decisions-retract-store
    store: dict[str, Any],
    # H8-SLOC-END decisions-retract-store
    telegram: TelegramClient,
    chat_id: str,
    topic_id: str,
    record: dict[str, Any],
    note: str,
    *,
    provider_executor: ProviderExecutor | None = None,
) -> bool:
    message_id = str(record.get("message_id") or "")
    if not message_id:
        return False
    markup = _execute(
        _operation(
            "telegram.edit_message_reply_markup",
            reason=(
                "telegram.edit_message_reply_markup: retract decision keyboard"
            ),
            record=record,
            topic_id=topic_id,
            message_id=message_id,
            scope="exact",
            args=(chat_id, message_id, {"inline_keyboard": []}),
        ),
        telegram=telegram,
        provider_executor=provider_executor,
        # H8-SLOC-BEGIN decisions-retract-markup-guard
        state_store=store,
        # H8-SLOC-END decisions-retract-markup-guard
    )
    if markup.result.get("ok") is not True:
        return False
    edited = _execute(
        _operation(
            "telegram.edit_message",
            reason="telegram.edit_message: annotate retracted decision",
            record=record,
            topic_id=topic_id,
            message_id=message_id,
            scope="exact",
            args=(
                chat_id,
                message_id,
                f"{render_decision(record)}\n\n{html_escape(note, 240)}",
            ),
        ),
        telegram=telegram,
        provider_executor=provider_executor,
        # H8-SLOC-BEGIN decisions-retract-edit-guard
        state_store=store,
        # H8-SLOC-END decisions-retract-edit-guard
    )
    return bool(edited.result.get("ok") is True)


def _drain_accepted_artifacts(
    store: dict[str, Any],
    telegram: TelegramClient,
    *,
    chat_id: str,
    provider_executor: ProviderExecutor | None,
) -> tuple[int, int]:
    """Retire accepted stale cards and finish accepted submissions first."""

    completed = 0
    pending = 0
    for receipt_id, raw in list(
        _accepted_artifacts(store, create=False).items()
    ):
        if not isinstance(raw, dict):
            complete_provider_acceptance(store, receipt_id)
            completed += 1
            continue
        kind = str(raw.get("kind") or "")
        topic_id = str(raw.get("topic_id") or "")
        record = {
            "decision_id": str(raw.get("decision_id") or ""),
            "worker_id": str(raw.get("worker_id") or ""),
            "entry_key": str(raw.get("entry_key") or ""),
            "message_id": str(raw.get("message_id") or ""),
            "provider_provenance": dict(raw.get("provenance") or {}),
        }
        if kind in {"decision_card", "decision_notice"}:
            execution = _execute(
                _operation(
                    "telegram.delete_message",
                    reason=(
                        "telegram.delete_message: retire accepted stale "
                        "decision card"
                    ),
                    record=record,
                    topic_id=topic_id,
                    message_id=record["message_id"],
                    scope="exact",
                    args=(chat_id, record["message_id"]),
                ),
                telegram=telegram,
                provider_executor=provider_executor,
                # H8-SLOC-BEGIN decisions-artifact-delete-guard
                state_store=store,
                # H8-SLOC-END decisions-artifact-delete-guard
            )
            if (
                execution.result.get("ok") is not True
                and classify_telegram_error(
                    execution.result.get("error")
                )
                != "not_found"
            ):
                pending += 1
                continue
            complete_provider_acceptance(store, receipt_id)
            completed += 1
            continue
        if kind == "decision_submission":
            active = active_decision(store, topic_id)
            if active is not None and not _retract(
                # H8-SLOC-BEGIN decisions-artifact-retract-store
                store,
                # H8-SLOC-END decisions-artifact-retract-store
                telegram,
                chat_id,
                topic_id,
                active,
                "✅ Answered.",
                provider_executor=provider_executor,
            ):
                pending += 1
                continue
            active = _active_records(store, create=True)
            current = active.get(topic_id)
            if (
                not isinstance(current, dict)
                or str(current.get("decision_id") or "")
                == record["decision_id"]
            ):
                active.pop(topic_id, None)
            complete_provider_acceptance(store, receipt_id)
            completed += 1
            continue
        complete_provider_acceptance(store, receipt_id)
        completed += 1
    return completed, pending


def sync_decisions(
    store: dict[str, Any],
    pending_payload: dict[str, Any],
    telegram: TelegramClient,
    *,
    chat_id: str,
    dry_run: bool = False,
    provider_executor: ProviderExecutor | None = None,
) -> dict[str, Any]:
    """Reconcile active inline keyboards with one already-fetched pending list."""

    if not config.remote_decisions_enabled():
        return {"enabled": False, "changed": False, "posted": 0, "retracted": 0}
    resolved = resolve_decisions(store, pending_payload)
    if dry_run:
        return {
            "enabled": True,
            "changed": False,
            "posted": 0,
            "retracted": 0,
            "resolved": len(resolved),
            "dry_run": True,
        }
    if not resolved and not _active_records(store, create=False):
        if not _accepted_artifacts(store, create=False):
            return {
                "enabled": True,
                "changed": False,
                "posted": 0,
                "retracted": 0,
                "resolved": 0,
            }
    artifact_completed, artifact_pending = _drain_accepted_artifacts(
        store,
        telegram,
        chat_id=chat_id,
        provider_executor=provider_executor,
    )
    if artifact_pending:
        return {
            "enabled": True,
            "changed": bool(artifact_completed),
            "posted": 0,
            "retracted": 0,
            "resolved": len(resolved),
            "artifact_reconciled": artifact_completed,
            "artifact_pending": artifact_pending,
        }
    active = _active_records(store, create=True)
    desired = {row["topic_id"]: row for row in resolved}
    raw_pending_ids = {
        blob.get("decision_ref")
        for item in _pending_items(pending_payload)
        if (blob := _decision_blob(item)) is not None
        and isinstance(blob.get("decision_ref"), str)
    }
    raw_pending_ids.discard("")
    posted = 0
    retracted = 0
    changed = bool(artifact_completed)

    for topic_id, raw_record in list(active.items()):
        if not isinstance(raw_record, dict):
            active.pop(topic_id, None)
            changed = True
            continue
        wanted = desired.get(topic_id)
        if (
            wanted is not None
            and raw_record.get("decision_id") == wanted["decision_id"]
            and raw_record.get("content_hash") == wanted["content_hash"]
        ):
            desired.pop(topic_id, None)
            continue
        note = (
            "⚠️ This prompt must be answered at the desk."
            if str(raw_record.get("decision_id") or "") in raw_pending_ids
            else "✅ Answered."
        )
        if not _retract(
            # H8-SLOC-BEGIN decisions-sync-retract-store
            store,
            # H8-SLOC-END decisions-sync-retract-store
            telegram,
            chat_id,
            topic_id,
            raw_record,
            note,
            provider_executor=provider_executor,
        ):
            continue
        active = _active_records(store, create=True)
        active.pop(topic_id, None)
        retracted += 1
        changed = True

    for topic_id, candidate in desired.items():
        record = {
            "decision_id": candidate["decision_id"],
            # H8-SLOC-BEGIN decisions-record-fields
            "revision_digest": candidate["content_hash"],
            # H8-SLOC-END decisions-record-fields
            "worker_id": candidate["worker_id"],
            "entry_key": candidate["entry_key"],
            "kind": candidate["kind"],
            "prompt": candidate["prompt"],
            "options": candidate["options"],
            "message_id": "",
            "selected": [],
            "await_freeform": False,
            "content_hash": candidate["content_hash"],
            # H8-SLOC-BEGIN decisions-record-mutation-fields
            "render_fingerprint": candidate["content_hash"],
            "desired_markup_fingerprint": "",
            "applied_markup_fingerprint": "",
            "applied_ingress_identities": [],
            # H8-SLOC-END decisions-record-mutation-fields
        }
        # H8-SLOC-BEGIN decisions-record-markup-fingerprint
        record["desired_markup_fingerprint"] = short_hash(inline_keyboard(record), 32)
        # H8-SLOC-END decisions-record-markup-fingerprint
        execution = _execute(
            _operation(
                "telegram.send_message",
                reason="telegram.send_message: post decision keyboard",
                record=record,
                topic_id=topic_id,
                artifact_kind="decision_card",
                args=(chat_id, render_decision(record)),
                kwargs={
                    "thread_id": topic_id,
                    "notify": True,
                    "reply_markup": inline_keyboard(record),
                },
            ),
            telegram=telegram,
            provider_executor=provider_executor,
        )
        sent = execution.result
        if execution.disposition != "apply":
            changed = changed or bool(execution.receipt_id)
            continue
        if not sent.get("ok"):
            continue
        record["message_id"] = str(
            sent.get("reply_markup_message_id") or sent.get("message_id") or ""
        )
        # H8-SLOC-BEGIN decisions-record-binding-fields
        record["message_binding_id"] = record["message_id"]
        record["applied_markup_fingerprint"] = record[
            "desired_markup_fingerprint"
        ]
        # H8-SLOC-END decisions-record-binding-fields
        if execution.provenance:
            record["provider_provenance"] = dict(execution.provenance)
        active = _active_records(store, create=True)
        active[topic_id] = record
        complete_provider_acceptance(store, execution.receipt_id)
        posted += 1
        changed = True
    return {
        "enabled": True,
        "changed": changed,
        "posted": posted,
        "retracted": retracted,
        "resolved": len(resolved),
        "artifact_reconciled": artifact_completed,
    }


def _parse_callback(value: Any) -> tuple[str, str] | None:
    data = str(value or "")
    if not data.startswith(CALLBACK_PREFIX):
        return None
    parts = data.split(":", 2)
    if len(parts) != 3 or not parts[1] or not parts[2]:
        return None
    return parts[1], parts[2]


def _failure_text(result: dict[str, Any]) -> str:
    status = compact_ws(result.get("status") or "answer_failed", 80)
    return f"⚠️ Could not answer that prompt ({status}). Try again or answer at the desk."


def _send_failure(
    store: dict[str, Any],
    telegram: TelegramClient,
    chat_id: str,
    topic_id: str,
    record: dict[str, Any],
    text: str,
    *,
    provider_executor: ProviderExecutor | None = None,
) -> None:
    execution = _execute(
        _operation(
            "telegram.send_message",
            reason="telegram.send_message: report decision callback failure",
            record=record,
            topic_id=topic_id,
            artifact_kind="decision_notice",
            args=(chat_id, html_escape(text, 300)),
            kwargs={
                "thread_id": topic_id,
                "reply_to_message_id": (
                    str(record.get("message_id") or "") or None
                ),
                "notify": True,
            },
        ),
        telegram=telegram,
        provider_executor=provider_executor,
    )
    if (
        execution.disposition == "apply"
        and execution.result.get("ok") is True
    ):
        message_id = str(
            execution.result.get("message_id")
            or execution.result.get("reply_markup_message_id")
            or ""
        )
        if message_id:
            bucket = store.setdefault("decisions", {})
            notices = bucket.setdefault("failure_notices", [])
            if isinstance(notices, list):
                notices.append(
                    {
                        "decision_id": str(
                            record.get("decision_id") or ""
                        ),
                        "topic_id": topic_id,
                        "message_id": message_id,
                    }
                )
                bucket["failure_notices"] = notices[-64:]
        complete_provider_acceptance(store, execution.receipt_id)


def _answer_request(
    record: dict[str, Any], request_id: str, selection: dict[str, Any]
) -> dict[str, Any]:
    return {
        "schema_version": 1,
        "action": "answer_decision",
        "request_id": validate_request_id(request_id),
        # Explicit, like send_instruction: Tendwire's mutating actions default to
        # dry-run, so a live answer must say so rather than rely on a flipped default.
        "dry_run": False,
        "target": {"worker_id": str(record.get("worker_id") or "")},
        "params": {
            "decision_ref": str(record.get("decision_id") or ""),
            "selection": selection,
        },
    }


def _submit(
    store: dict[str, Any],
    topic_id: str,
    record: dict[str, Any],
    selection: dict[str, Any],
    *,
    request_id: str,
    telegram: TelegramClient,
    tendwire: TendwireClient,
    chat_id: str,
    callback: bool,
    provider_executor: ProviderExecutor | None = None,
) -> dict[str, Any]:
    try:
        execution = _execute(
            _operation(
                "tendwire.command",
                reason="tendwire.command: submit remote decision answer",
                record=record,
                topic_id=topic_id,
                artifact_kind="decision_submission",
                args=(
                    _answer_request(record, request_id, selection),
                ),
            ),
            tendwire=tendwire,
            provider_executor=provider_executor,
        )
        result = execution.result
        if (
            execution.disposition != "apply"
            and not (
                result.get("ok") is True
                and result.get("status") == "accepted"
            )
        ):
            result = {
                "ok": False,
                "status": "owner_changed",
            }
    except Exception as exc:  # noqa: BLE001 - the keyboard must survive every submit failure
        result = {
            "ok": False,
            "status": "connector_error",
            "error": sanitize_text(str(exc), 160),
        }
    active = _active_records(store, create=True)
    if result.get("ok") is True and result.get("status") == "accepted":
        if not _retract(
            # H8-SLOC-BEGIN decisions-submit-accepted-store
            store,
            # H8-SLOC-END decisions-submit-accepted-store
            telegram,
            chat_id,
            topic_id,
            record,
            "✅ Answered.",
            provider_executor=provider_executor,
        ):
            return {
                "handled": True,
                "changed": False,
                "toast": "Answer accepted; card reconciliation pending.",
                "reply": "",
                "status": "accepted_reconcile",
            }
        active = _active_records(store, create=True)
        active.pop(topic_id, None)
        complete_provider_acceptance(store, execution.receipt_id)
        return {
            "handled": True,
            "changed": True,
            "toast": "Answered.",
            "reply": "",
            "status": "accepted",
        }
    status = str(result.get("status") or "answer_failed")
    if status == "answer_in_progress":
        if callback:
            _send_failure(
                store,
                telegram,
                chat_id,
                topic_id,
                record,
                ANSWER_IN_PROGRESS_REPLY,
                provider_executor=provider_executor,
            )
        return {
            "handled": True,
            "changed": False,
            "toast": ANSWER_IN_PROGRESS_REPLY,
            "reply": ANSWER_IN_PROGRESS_REPLY,
            "status": status,
        }
    if status == "decision_not_pending":
        note = "⚠️ That prompt is no longer pending (answered at the desk?)"
        if not _retract(
            # H8-SLOC-BEGIN decisions-submit-expired-store
            store,
            # H8-SLOC-END decisions-submit-expired-store
            telegram,
            chat_id,
            topic_id,
            record,
            note,
            provider_executor=provider_executor,
        ):
            return {
                "handled": True,
                "changed": False,
                "toast": "Prompt ended; card reconciliation pending.",
                "reply": "" if callback else note,
                "status": "decision_not_pending_reconcile",
            }
        active = _active_records(store, create=True)
        active.pop(topic_id, None)
        return {
            "handled": True,
            "changed": True,
            "toast": "Prompt is no longer pending.",
            "reply": "" if callback else note,
            "status": status,
        }
    error = _failure_text(result)
    if callback:
        _send_failure(
            store,
            telegram,
            chat_id,
            topic_id,
            record,
            error,
            provider_executor=provider_executor,
        )
    return {
        "handled": True,
        "changed": False,
        "toast": "Could not answer; try again.",
        "reply": error,
        "status": status,
    }


def handle_callback(
    store: dict[str, Any],
    *,
    callback_data: str,
    topic_id: str,
    chat_id: str,
    request_id: str,
    telegram: TelegramClient,
    tendwire: TendwireClient,
    provider_executor: ProviderExecutor | None = None,
) -> dict[str, Any]:
    """Select, toggle, submit, or arm write-in for one Telegram callback."""

    if not config.remote_decisions_enabled():
        return {
            "handled": False,
            "changed": False,
            "toast": "Remote decisions are disabled.",
            "reply": "",
            "status": "disabled",
        }
    parsed = _parse_callback(callback_data)
    if parsed is None:
        return {
            "handled": False,
            "changed": False,
            "toast": "Unknown action.",
            "reply": "",
            "status": "unknown_callback",
        }
    callback_ref, option_ref = parsed
    record = active_decision(store, topic_id)
    if record is None or callback_ref != _ref56(str(record.get("decision_id") or "")):
        return {
            "handled": True,
            "changed": False,
            "toast": "That button has expired.",
            "reply": "",
            "status": "expired",
        }
    option_refs = {
        str(option.get("ref") or "")
        for option in record.get("options", [])
        if isinstance(option, dict)
    }
    kind = str(record.get("kind") or "")
    if kind == "single" and option_ref == CUSTOM_TOKEN:
        record["await_freeform"] = True
        return {
            "handled": True,
            "changed": True,
            "toast": "Write your answer in this topic.",
            "reply": "",
            "status": "await_freeform",
        }
    if kind == "multi" and option_ref in option_refs:
        selected = [
            str(value)
            for value in record.get("selected", [])
            if isinstance(value, str) and value in option_refs
        ]
        if option_ref in selected:
            selected.remove(option_ref)
            toast = "Choice cleared."
        else:
            selected.append(option_ref)
            toast = "Choice selected."
        preview = dict(record)
        preview["selected"] = selected
        # H8-SLOC-BEGIN decisions-callback-markup-fingerprint
        desired_markup_fingerprint = short_hash(inline_keyboard(preview), 32)
        # H8-SLOC-END decisions-callback-markup-fingerprint
        toggle_operation = _operation(
            "telegram.edit_message_reply_markup",
            reason=(
                "telegram.edit_message_reply_markup: toggle decision choice"
            ),
            record=record,
            topic_id=topic_id,
            message_id=str(record.get("message_id") or ""),
            scope="exact",
            args=(
                chat_id,
                str(record.get("message_id") or ""),
                inline_keyboard(preview),
            ),
        )
        execution = _execute(
            toggle_operation,
            telegram=telegram,
            provider_executor=provider_executor,
            # H8-SLOC-BEGIN decisions-callback-provider-guard
            state_store=store,
            # H8-SLOC-END decisions-callback-provider-guard
        )
        edited = execution.result
        if execution.disposition != "apply":
            if edited.get("ok") and toggle_operation.message_id:
                stale_operation = replace(
                    toggle_operation,
                    artifact_kind="decision_card",
                )
                checkpoint_provider_acceptance(
                    store,
                    stale_operation,
                    {
                        "ok": True,
                        "message_id": toggle_operation.message_id,
                    },
                    execution.provenance,
                )
                active = _active_records(store, create=True)
                for active_topic, active_record in list(
                    active.items()
                ):
                    if (
                        isinstance(active_record, dict)
                        and str(active_record.get("message_id") or "")
                        == toggle_operation.message_id
                        and str(
                            active_record.get("decision_id") or ""
                        )
                        == str(record.get("decision_id") or "")
                    ):
                        active.pop(active_topic, None)
            return {
                "handled": True,
                "changed": bool(edited.get("ok")),
                "toast": "Could not update choices.",
                "reply": "",
                "status": "telegram_edit_failed",
            }
        if not edited.get("ok"):
            return {
                "handled": True,
                "changed": False,
                "toast": "Could not update choices.",
                "reply": "",
                "status": "telegram_edit_failed",
            }
        current = active_decision(store, topic_id)
        if (
            current is None
            or str(current.get("decision_id") or "")
            != str(record.get("decision_id") or "")
        ):
            return {
                "handled": True,
                "changed": False,
                "toast": "That button has expired.",
                "reply": "",
                "status": "expired",
            }
        current["selected"] = selected
        # H8-SLOC-BEGIN decisions-callback-checkpoint
        current["desired_markup_fingerprint"] = desired_markup_fingerprint
        current["applied_markup_fingerprint"] = desired_markup_fingerprint
        # H8-SLOC-END decisions-callback-checkpoint
        return {
            "handled": True,
            "changed": True,
            "toast": toast,
            "reply": "",
            "status": "toggled",
        }
    if kind == "multi" and option_ref == SUBMIT_TOKEN:
        selection = {
            "option_refs": [
                str(value)
                for value in record.get("selected", [])
                if isinstance(value, str) and value in option_refs
            ]
        }
        return _submit(
            store,
            topic_id,
            record,
            selection,
            request_id=request_id,
            telegram=telegram,
            tendwire=tendwire,
            chat_id=chat_id,
            callback=True,
            provider_executor=provider_executor,
        )
    if kind in {"single", "plan"} and option_ref in option_refs:
        return _submit(
            store,
            topic_id,
            record,
            {"option_refs": [option_ref]},
            request_id=request_id,
            telegram=telegram,
            tendwire=tendwire,
            chat_id=chat_id,
            callback=True,
            provider_executor=provider_executor,
        )
    return {
        "handled": True,
        "changed": False,
        "toast": "That choice is no longer available.",
        "reply": "",
        "status": "invalid_selection",
    }


def handle_freeform(
    store: dict[str, Any],
    *,
    topic_id: str,
    text: str,
    request_id: str,
    telegram: TelegramClient,
    tendwire: TendwireClient,
    chat_id: str,
) -> dict[str, Any]:
    """Submit plain text only after the owner explicitly armed the write-in path."""

    if not config.remote_decisions_enabled():
        return {"handled": False, "changed": False, "reply": "", "status": "disabled"}
    record = active_decision(store, topic_id)
    if record is None or record.get("await_freeform") is not True:
        return {"handled": False, "changed": False, "reply": "", "status": "not_armed"}
    answer = sanitize_text(text, 12000).strip()
    if not answer:
        return {
            "handled": True,
            "changed": False,
            "reply": "Write a non-empty answer, or use the buttons.",
            "status": "invalid_selection",
        }
    return _submit(
        store,
        str(topic_id),
        record,
        {"text": answer},
        request_id=request_id,
        telegram=telegram,
        tendwire=tendwire,
        chat_id=chat_id,
        callback=False,
    )
