"""Durable Telegram ingress orchestration.

This module deliberately owns no persistence format.  It turns bounded
Telegram updates into canonical queue inputs, resolves them through the typed
state facade, checkpoints one immutable operation, and reduces provider
receipts into queue transitions.
"""

from __future__ import annotations

import json, re, threading, time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import Any, Callable, Mapping, Protocol, Sequence

from . import decisions
from .ingress_identity import derive_telegram_request_id
from .ingress_queue import IngressQueue, NoticeClaim, QueueItem
from .managed_bots import (
    MANAGER_BOT_KIND,
    managed_bot_kind_for_username,
    normalize_bot_kind,
    receiver_for_id,
)
from .safe import html_escape, sanitize_text
from .state import (
    DecisionIngressQuery,
    DecisionIngressResult,
    DecisionMutation,
    DecisionMutationKind,
    DecisionMutationStatus,
    DecisionStatus,
    IngressPolicy,
    IngressReceiver,
    IngressReplyQuery,
    IngressRouteQuery,
    IngressRouteResult,
    PhysicalOwner,
    RouteStatus,
    StateToken,
    apply_decision_ingress,
    provider_mutation_guard,
    read_decision_ingress,
    read_ingress_policy,
    resolve_ingress_reply,
    resolve_ingress_route,
)
from .telegram_delivery import RateLimited, TelegramError
from .tendwire_client import command_process_ambiguous, command_process_not_started

MENTION_RE = re.compile(r"@([A-Za-z0-9_]{3,64})")
CALLBACK_RE = re.compile(r"hdec:([0-9a-f]{14}):([^:\s]{1,160})\Z", re.ASCII)
CUSTOM_TOKEN = "custom"
SUBMIT_TOKEN = "__submit__"
INPUT_TEXT_LIMIT = 12_000
SAFE_FAILURE_REPLY = "Could not complete that request safely. Refresh status and try again."
SAFE_UNCERTAIN_REPLY = "The request result is uncertain. Check the worker before trying again."
BUSY_WORKER_REPLY = "Submitted to busy Tendwire worker."
OVERFLOW_REPLY = "Inbound requests are backed up. Wait for the agent to catch up and try again."


class TendwirePort(Protocol):
    def command_json(self, request_json: str) -> dict[str, Any]:
        ...


class TelegramPort(Protocol):
    def api(self, method: str, payload: dict[str, Any]) -> dict[str, Any]:
        ...
    def send_message(
        self,
        chat_id: str,
        html_text: str,
        *,
        thread_id: str | None = None,
        reply_to_message_id: str | None = None,
        notify: bool = False,
        max_physical_writes: int | None = None,
        ambiguous_errors_are_unknown: bool = False,
    ) -> dict[str, Any]: ...
    def edit_message_reply_markup(
        self, chat_id: str, message_id: str, reply_markup: dict[str, Any]
    ) -> dict[str, Any]: ...
    def answer_callback_query(
        self, callback_query_id: str, text: str = "", *, show_alert: bool = False
    ) -> dict[str, Any]: ...


@dataclass(frozen=True, slots=True)
class Preview:
    disposition: str
    reason: str
    update_id: int
    receiver_id: str
    receiver_bot_kind: str
    kind: str = ""
    chat_id: str = ""
    topic_id: str = ""
    message_id: str = ""
    sender_id: str = ""
    text: str = ""
    reply_message_id: str = ""
    reply_author_bot_kind: str = ""
    explicit_alias: str = ""
    explicit_bot_kind: str = ""
    callback_query_id: str = ""
    callback_data: str = ""
    callback_ref: str = ""
    callback_token: str = ""
    owner_command: bool = False
    general_topic: bool = False


@dataclass(frozen=True, slots=True)
class CanonicalCommand:
    value: dict[str, Any]
    json: str


@dataclass(frozen=True, slots=True)
class ReceiptReduction:
    disposition: str
    reason: str
    receipt: dict[str, Any]
    terminal_reply: str = ""
    notify: bool = False


@dataclass(frozen=True, slots=True)
class DispatchResult:
    status: str
    seq: int
    disposition: str = ""
    notice_pending: bool = False


@dataclass(frozen=True, slots=True)
class NoticeResult:
    status: str
    seq: int
    message_id: str = ""


@dataclass(frozen=True, slots=True)
class PollResult:
    receiver_id: str
    received: int = 0
    enqueued: int = 0
    advanced: int = 0
    overflow: int = 0
    duplicates: int = 0
    errors: int = 0


@dataclass(slots=True)
class IngressPorts:
    state_path: Path
    request_id_key: bytes
    queue: IngressQueue
    receivers: tuple[IngressReceiver, ...]
    telegram_clients: Mapping[str, TelegramPort]
    tendwire: TendwirePort
    stop_event: threading.Event = field(default_factory=threading.Event)
    now: Callable[[], float] = time.time
    monotonic: Callable[[], float] = time.monotonic
    poll_timeout_seconds: int = 50
    lease_seconds: float = 90.0
    provider_timeout_seconds: float = 30.0
    retry_horizon_seconds: float = 86_400.0
    retention_seconds: float = 172_800.0
    depth_limit: int = 32
    dispatch_workers: int = 8
    idle_seconds: float = 0.05
    log: Callable[[str], None] = lambda _message: None

    def telegram_for(self, receiver_id: str) -> TelegramPort:
        client = self.telegram_clients.get(receiver_id)
        if client is None:
            raise RuntimeError("missing Telegram receiver client")
        return client


def _integer(value: Any, *, minimum: int = 0) -> int | None:
    return value if type(value) is int and value >= minimum else None


def _coordinate(value: Any) -> str:
    return str(value) if type(value) is int else ""


def _opaque_coordinate(value: Any) -> str:
    return value if isinstance(value, str) and 0 < len(value.encode("utf-8")) <= 256 else ""


def _topic_id(message: Mapping[str, Any], policy: IngressPolicy) -> str:
    explicit = _coordinate(message.get("message_thread_id"))
    if explicit:
        return explicit
    chat = message.get("chat") if isinstance(message.get("chat"), Mapping) else {}
    return policy.general_topic_id if chat.get("is_forum") is True else ""


def _sender_allowed(user: Mapping[str, Any], policy: IngressPolicy) -> bool:
    sender = _coordinate(user.get("id"))
    if not sender or user.get("is_bot") is True:
        return False
    return not policy.owner_user_ids or sender in policy.owner_user_ids


def _text_and_alias(message: Mapping[str, Any], policy: IngressPolicy) -> tuple[str, str, str]:
    raw = message.get("text") if isinstance(message.get("text"), str) else ""
    if not raw:
        raw = message.get("caption") if isinstance(message.get("caption"), str) else ""
    text = sanitize_text(raw, INPUT_TEXT_LIMIT).strip()
    send_text = text[5:].strip() if text.startswith("/send") else text
    first = send_text.split(maxsplit=1)[0] if send_text else ""
    alias = first.strip("@:,. ") if first.startswith("@") else ""
    managed = managed_bot_kind_for_username(policy, alias)
    if alias:
        return send_text[len(first) :].strip(), "" if managed else alias, managed
    for match in MENTION_RE.finditer(send_text):
        managed = managed_bot_kind_for_username(policy, match.group(1))
        if managed:
            return send_text, "", managed
    return send_text, "", ""


def _reply_details(message: Mapping[str, Any], policy: IngressPolicy) -> tuple[str, str]:
    reply = message.get("reply_to_message")
    if not isinstance(reply, Mapping):
        return "", ""
    author = reply.get("from") if isinstance(reply.get("from"), Mapping) else {}
    kind = managed_bot_kind_for_username(policy, str(author.get("username") or ""))
    return _coordinate(reply.get("message_id")), kind


def _receiver_disposition(receiver: IngressReceiver, target_kind: str) -> tuple[str, str]:
    current = normalize_bot_kind(receiver.bot_kind)
    target = normalize_bot_kind(target_kind, allow_manager=False)
    if current == MANAGER_BOT_KIND and target:
        return "advance", "deferred_to_managed_receiver"
    if current != MANAGER_BOT_KIND and target != current:
        return "advance", "deferred_to_other_receiver"
    return "enqueue", "accepted"


def _preview_message(
    update: Mapping[str, Any],
    message: Mapping[str, Any],
    policy: IngressPolicy,
    receiver: IngressReceiver,
    reply_route: IngressRouteResult | None,
) -> Preview:
    update_id = int(update["update_id"])
    chat = message.get("chat") if isinstance(message.get("chat"), Mapping) else {}
    user = message.get("from") if isinstance(message.get("from"), Mapping) else {}
    chat_id, message_id = _coordinate(chat.get("id")), _coordinate(message.get("message_id"))
    topic_id = _topic_id(message, policy)
    base = dict(
        update_id=update_id,
        receiver_id=receiver.receiver_id,
        receiver_bot_kind=normalize_bot_kind(receiver.bot_kind),
        kind="message",
        chat_id=chat_id,
        topic_id=topic_id,
        message_id=message_id,
        sender_id=_coordinate(user.get("id")),
        general_topic=topic_id == policy.general_topic_id,
    )
    if not chat_id or not message_id:
        return Preview("advance", "invalid_message_coordinates", **base)
    if policy.chat_id and chat_id != policy.chat_id:
        return Preview("advance", "wrong_chat", **base)
    if not _sender_allowed(user, policy):
        return Preview("advance", "sender_not_allowed", **base)
    text, alias, mention_kind = _text_and_alias(message, policy)
    if not text or (
        str(message.get("text") or "").startswith("/")
        and not str(message.get("text") or "").startswith("/send")
    ):
        return Preview("advance", "unsupported_message", **base)
    reply_id, author_kind = _reply_details(message, policy)
    binding_kind = ""
    route_reason = ""
    if reply_route is not None:
        route_reason = reply_route.reason
        if reply_route.status is RouteStatus.RESOLVED:
            binding_kind = normalize_bot_kind(reply_route.bot_kind, allow_manager=False)
        elif reply_route.binding_was_present or reply_route.status in {
            RouteStatus.BINDING_AMBIGUOUS,
            RouteStatus.AUTHOR_AMBIGUOUS,
        }:
            return Preview(
                "quarantine",
                route_reason or "ambiguous_reply_target",
                text=text,
                reply_message_id=reply_id,
                reply_author_bot_kind=author_kind,
                explicit_alias=alias,
                explicit_bot_kind=mention_kind,
                **base,
            )
    target_kind = author_kind or binding_kind or mention_kind
    disposition, reason = _receiver_disposition(receiver, target_kind)
    return Preview(
        disposition,
        reason,
        text=text,
        reply_message_id=reply_id,
        reply_author_bot_kind=author_kind,
        explicit_alias=alias,
        explicit_bot_kind=(target_kind or mention_kind),
        owner_command=str(message.get("text") or "").lstrip().startswith("/"),
        **base,
    )


def _preview_callback(
    update: Mapping[str, Any],
    callback: Mapping[str, Any],
    policy: IngressPolicy,
    receiver: IngressReceiver,
) -> Preview:
    update_id = int(update["update_id"])
    message = callback.get("message") if isinstance(callback.get("message"), Mapping) else {}
    chat = message.get("chat") if isinstance(message.get("chat"), Mapping) else {}
    user = callback.get("from") if isinstance(callback.get("from"), Mapping) else {}
    chat_id, message_id = _coordinate(chat.get("id")), _coordinate(message.get("message_id"))
    base = dict(
        update_id=update_id,
        receiver_id=receiver.receiver_id,
        receiver_bot_kind=normalize_bot_kind(receiver.bot_kind),
        kind="decision",
        chat_id=chat_id,
        topic_id=_topic_id(message, policy),
        message_id=message_id,
        sender_id=_coordinate(user.get("id")),
        general_topic=_topic_id(message, policy) == policy.general_topic_id,
    )
    if not chat_id or not message_id:
        return Preview("advance", "invalid_callback_coordinates", **base)
    if policy.chat_id and chat_id != policy.chat_id:
        return Preview("advance", "wrong_chat", **base)
    if not _sender_allowed(user, policy):
        return Preview("advance", "sender_not_allowed", **base)
    callback_id = _opaque_coordinate(callback.get("id"))
    data = str(callback.get("data") or "")
    parsed = CALLBACK_RE.fullmatch(data)
    if not callback_id or parsed is None:
        return Preview("advance", "unsupported_callback", **base)
    return Preview(
        "enqueue",
        "accepted",
        callback_query_id=callback_id,
        callback_data=data,
        callback_ref=parsed.group(1),
        callback_token=parsed.group(2),
        **base,
    )


def preview_update(
    update: Mapping[str, Any],
    policy: IngressPolicy,
    receiver: IngressReceiver,
    reply_route: IngressRouteResult | None = None,
) -> Preview:
    """Parse one update without retaining raw provider data or credentials."""

    update_id = _integer(update.get("update_id")) if isinstance(update, Mapping) else None
    if update_id is None:
        return Preview(
            "invalid",
            "invalid_update_id",
            -1,
            receiver.receiver_id,
            normalize_bot_kind(receiver.bot_kind),
        )
    callback = update.get("callback_query")
    if isinstance(callback, Mapping):
        return _preview_callback(update, callback, policy, receiver)
    message = update.get("message")
    if isinstance(message, Mapping):
        return _preview_message(update, message, policy, receiver, reply_route)
    return Preview(
        "advance",
        "unsupported_update",
        update_id,
        receiver.receiver_id,
        normalize_bot_kind(receiver.bot_kind),
    )


def _input_mapping(preview: Preview) -> dict[str, Any]:
    common: dict[str, Any] = {
        "schema_version": 1,
        "kind": preview.kind,
        "receiver_id": preview.receiver_id,
        "receiver_bot_kind": preview.receiver_bot_kind,
        "update_id": preview.update_id,
        "chat_id": preview.chat_id,
        "topic_id": preview.topic_id,
        "message_id": preview.message_id,
        "sender_id": preview.sender_id,
    }
    if preview.kind == "message":
        common.update(
            {
                "text": preview.text,
                "reply_message_id": preview.reply_message_id,
                "reply_author_bot_kind": preview.reply_author_bot_kind,
                "explicit_alias": preview.explicit_alias,
                "explicit_bot_kind": preview.explicit_bot_kind,
                "owner_command": preview.owner_command,
                "general_topic": preview.general_topic,
            }
        )
    elif preview.kind == "decision":
        common.update(
            {
                "callback_query_id": preview.callback_query_id,
                "callback_data": preview.callback_data,
                "callback_ref": preview.callback_ref,
                "callback_token": preview.callback_token,
            }
        )
    if preview.disposition == "quarantine":
        common["preview_quarantine"] = preview.reason
    return common


def canonical_input(preview: Preview) -> str:
    """Return canonical UTF-8 JSON for the queue input payload."""

    if preview.disposition not in {"enqueue", "quarantine"}:
        raise ValueError("preview is not queueable")
    return json.dumps(
        _input_mapping(preview),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )


def ordering_key(preview: Preview) -> str:
    if preview.owner_command:
        parts = ["owner-command", preview.receiver_id]
    elif preview.general_topic or preview.topic_id == "":
        parts = ["general", preview.chat_id]
    else:
        parts = ["topic", preview.chat_id, preview.topic_id]
    return json.dumps(parts, ensure_ascii=True, separators=(",", ":"))


def _canonical_command(value: dict[str, Any]) -> CanonicalCommand:
    encoded = json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False
    )
    return CanonicalCommand(value=value, json=encoded)


def build_send_instruction(item: QueueItem, route: IngressRouteResult) -> CanonicalCommand:
    text = str(item.input.get("text") or "")
    if not text:
        raise ValueError("empty instruction")
    if route.worker_id:
        target = {"worker_id": route.worker_id}
        if route.worker_fingerprint:
            target["worker_fingerprint"] = route.worker_fingerprint
    elif route.space_id:
        target = {"space_id": route.space_id}
    else:
        raise ValueError("route has no Tendwire target")
    return _canonical_command(
        {
            "schema_version": 1,
            "action": "send_instruction",
            "request_id": item.request_id,
            "dry_run": False,
            "target": target,
            "instruction": {"text": text},
            "response_schema_version": 3,
        }
    )


def build_answer_decision(
    item: QueueItem,
    decision: DecisionIngressResult,
) -> CanonicalCommand:
    selection = item.input.get("selection")
    if not isinstance(selection, Mapping):
        text = str(item.input.get("text") or "").strip()
        selection = {"text": text} if text else {}
    selection = dict(selection)
    refs = selection.get("option_refs")
    valid = (
        set(selection) == {"text"}
        and isinstance(selection.get("text"), str)
        and bool(selection["text"].strip())
    )
    valid = valid or (
        set(selection) == {"option_refs"}
        and isinstance(refs, list)
        and bool(refs)
        and all(isinstance(ref, str) and ref for ref in refs)
        and len(refs) == len(set(refs))
    )
    if not valid or not decision.worker_id or not decision.decision_ref:
        raise ValueError("invalid decision answer")
    return _canonical_command(
        {
            "schema_version": 1,
            "action": "answer_decision",
            "request_id": item.request_id,
            "dry_run": False,
            "target": {"worker_id": decision.worker_id},
            "params": {"decision_ref": decision.decision_ref, "selection": selection},
        }
    )


def reduce_daemon_receipt(
    command: CanonicalCommand | Mapping[str, Any],
    response: Any,
) -> ReceiptReduction:
    value = command.value if isinstance(command, CanonicalCommand) else dict(command)
    action = value.get("action")
    if command_process_not_started(response):
        return ReceiptReduction("retry", "definitely_not_started", {})
    if command_process_ambiguous(response):
        return ReceiptReduction("quarantine", "started_transport_ambiguity", {})
    if not isinstance(response, dict):
        return ReceiptReduction("quarantine", "malformed_daemon_response", {})
    if response.get("request_id") != value.get("request_id") or response.get("action") != action:
        return ReceiptReduction("quarantine", "malformed_daemon_correlation", dict(response))
    disposition = response.get("disposition")
    if disposition == "terminal_accepted" and response.get("ok") is True:
        if action == "send_instruction" and response.get("schema_version") != 3:
            return ReceiptReduction("quarantine", "unsupported_success_schema", dict(response))
        result = response.get("result") if isinstance(response.get("result"), dict) else {}
        busy = action == "send_instruction" and result.get("target_state_at_send") == "working"
        return ReceiptReduction(
            "terminal", "terminal_accepted", dict(response), BUSY_WORKER_REPLY if busy else "", busy
        )
    if disposition == "terminal_rejected" and response.get("ok") is False:
        return ReceiptReduction(
            "terminal", "terminal_rejected", dict(response), SAFE_FAILURE_REPLY, True
        )
    if disposition in {"no_receipt", "in_progress"} and response.get("ok") is False:
        return ReceiptReduction("retry", str(disposition), dict(response))
    if disposition == "terminal_uncertain":
        return ReceiptReduction(
            "quarantine", "terminal_uncertain", dict(response), SAFE_UNCERTAIN_REPLY, True
        )
    return ReceiptReduction(
        "quarantine", "malformed_daemon_response", dict(response), SAFE_UNCERTAIN_REPLY, True
    )


def _backoff(item: QueueItem, now: float) -> float:
    return min(item.deadline_at, now + min(60.0, float(2 ** min(max(item.attempts - 1, 0), 6))))


def _quarantine(
    queue: IngressQueue,
    item: QueueItem,
    ports: IngressPorts,
    reason: str,
    *,
    reply: str = SAFE_FAILURE_REPLY,
    digest: str | None = None,
) -> DispatchResult:
    result = queue.quarantine(
        item.seq,
        item.lease_owner,
        {
            "reason": reason,
            "disposition": reason,
            "now": ports.now(),
            "terminal_reply": reply,
            "notify": bool(reply),
            **({"operation_digest": digest} if digest else {}),
        },
    )
    return DispatchResult(
        result.status, item.seq, reason, result.status == "quarantined" and bool(reply)
    )


def _apply_reduction(
    queue: IngressQueue,
    item: QueueItem,
    reduction: ReceiptReduction,
    ports: IngressPorts,
) -> DispatchResult:
    digest = item.operation_digest
    if not digest:
        return _quarantine(queue, item, ports, "missing_operation_digest")
    now = ports.now()
    if reduction.disposition == "terminal":
        settled = queue.settle_receipt(
            item.seq,
            item.lease_owner,
            {
                "operation_digest": digest,
                "receipt_kind": "daemon",
                "receipt": reduction.receipt,
                "disposition": reduction.reason,
                "now": now,
                "terminal_reply": reduction.terminal_reply or None,
                "notify": reduction.notify,
            },
        )
        return DispatchResult(settled.status, item.seq, reduction.reason, reduction.notify)
    if reduction.disposition == "retry" and now < item.deadline_at:
        retried = queue.schedule_retry(
            item.seq,
            item.lease_owner,
            {
                "operation_digest": digest,
                "disposition": reduction.reason,
                "now": now,
                "next_attempt_at": _backoff(item, now),
            },
        )
        return DispatchResult(
            retried.status, item.seq, reduction.reason, retried.status == "quarantined"
        )
    reason = "deadline_expired" if now >= item.deadline_at else reduction.reason
    return _quarantine(
        queue,
        item,
        ports,
        reason,
        reply=(reduction.terminal_reply or SAFE_UNCERTAIN_REPLY),
        digest=digest,
    )


def _send_stored_command(
    queue: IngressQueue,
    item: QueueItem,
    ports: IngressPorts,
) -> DispatchResult:
    if item.command is None or item.operation_digest is None:
        return _quarantine(queue, item, ports, "missing_command_checkpoint")
    encoded = json.dumps(
        item.command, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False
    )
    try:
        response = ports.tendwire.command_json(encoded)
    except Exception:  # transport did not yield a trustworthy receipt
        reduction = ReceiptReduction(
            "quarantine", "started_transport_ambiguity", {}, SAFE_UNCERTAIN_REPLY, True
        )
    else:
        reduction = reduce_daemon_receipt(item.command, response)
    return _apply_reduction(queue, item, reduction, ports)


def _read_policy_and_decision(
    ports: IngressPorts,
    input_data: Mapping[str, Any],
    callback_ref: str | None,
) -> DecisionIngressResult:
    policy = read_ingress_policy(ports.state_path)
    query = DecisionIngressQuery(
        chat_id=str(input_data.get("chat_id") or ""),
        topic_id=str(input_data.get("topic_id") or ""),
        callback_ref=callback_ref,
        state_token=policy.state_token,
    )
    result = read_decision_ingress(ports.state_path, query)
    if result.status is DecisionStatus.STALE:
        policy = read_ingress_policy(ports.state_path)
        result = read_decision_ingress(
            ports.state_path, replace(query, state_token=policy.state_token)
        )
    return result


def _route_for_item(item: QueueItem, ports: IngressPorts) -> IngressRouteResult:
    policy = read_ingress_policy(ports.state_path)
    data = item.input
    if data.get("reply_message_id"):
        query: IngressReplyQuery | IngressRouteQuery = IngressReplyQuery(
            chat_id=str(data.get("chat_id") or ""),
            topic_id=str(data.get("topic_id") or ""),
            reply_message_id=str(data.get("reply_message_id") or ""),
            observed_author_bot_kind=str(data.get("reply_author_bot_kind") or ""),
            explicit_alias=str(data.get("explicit_alias") or ""),
            explicit_bot_kind=str(data.get("explicit_bot_kind") or ""),
            state_token=policy.state_token,
        )
        result = resolve_ingress_reply(ports.state_path, query)
    else:
        query = IngressRouteQuery(
            chat_id=str(data.get("chat_id") or ""),
            topic_id=str(data.get("topic_id") or ""),
            receiver_bot_kind=str(data.get("receiver_bot_kind") or ""),
            explicit_alias=str(data.get("explicit_alias") or ""),
            explicit_bot_kind=str(data.get("explicit_bot_kind") or ""),
            state_token=policy.state_token,
        )
        result = resolve_ingress_route(ports.state_path, query)
    if result.status is RouteStatus.STALE:
        policy = read_ingress_policy(ports.state_path)
        resolver = (
            resolve_ingress_reply if isinstance(query, IngressReplyQuery) else resolve_ingress_route
        )
        result = resolver(ports.state_path, replace(query, state_token=policy.state_token))
    return result


def _checkpoint_command(
    queue: IngressQueue,
    item: QueueItem,
    command: CanonicalCommand,
    route: IngressRouteResult,
    now: float,
) -> QueueItem | None:
    if route.owner is None:
        return None
    stored = queue.store_command(
        item.seq,
        item.lease_owner,
        {
            "command": command.value,
            "target_stable_key": route.owner.stable_key,
            "target_stable_key_version": route.owner.stable_key_version,
            "target_route_generation": route.owner.route_generation,
            "target_worker_id": route.worker_id or None,
            "target_space_id": route.space_id or None,
            "target_bot_kind": route.bot_kind or None,
            "now": now,
        },
    )
    if stored.status not in {"stored", "existing", "refreshed"} or not stored.digest:
        return None
    return replace(item, command=command.value, operation_digest=stored.digest)


def _local_action(
    item: QueueItem,
    decision: DecisionIngressResult,
    action_kind: str,
    option_ref: str = "",
) -> dict[str, Any]:
    common: dict[str, Any] = {
        "schema_version": 1,
        "action": action_kind,
        "request_id": item.request_id,
        "callback_ref": str(item.input.get("callback_ref") or ""),
        "decision_ref": decision.decision_ref,
        "revision_digest": decision.revision_digest,
    }
    if action_kind == "ARM_FREEFORM":
        return common
    selected = list(decision.selected_refs)
    selected = (
        [value for value in selected if value != option_ref]
        if option_ref in selected
        else [*selected, option_ref]
    )
    desired = tuple(value for value in decision.option_refs if value in set(selected))
    markup, fingerprint = decisions.render_ingress_markup(decision, desired)
    common.update(
        {
            "option_ref": option_ref,
            "desired_selected_refs": list(desired),
            "desired_markup_fingerprint": fingerprint,
            "physical_owner": {
                "bot_identity": decision.physical_owner.bot_identity,
                "chat_id": decision.physical_owner.chat_id,
                "topic_id": decision.physical_owner.topic_id,
            },
            "message_binding_id": decision.message_binding_id,
            "message_id": decision.message_id,
            "reply_markup": markup,
        }
    )
    return common


def dispatch_decision(
    queue: IngressQueue,
    item: QueueItem,
    ports: IngressPorts,
) -> DispatchResult:
    data = item.input
    decision = _read_policy_and_decision(ports, data, str(data.get("callback_ref") or ""))
    if decision.status is not DecisionStatus.ACTIVE:
        return _quarantine(queue, item, ports, f"decision_{decision.status.value}")
    if decision.physical_owner.bot_identity != str(data.get("receiver_bot_kind") or ""):
        return _quarantine(queue, item, ports, "wrong_decision_receiver")
    token = str(data.get("callback_token") or "")
    if decision.mode == "single" and token == CUSTOM_TOKEN:
        action = _local_action(item, decision, "ARM_FREEFORM")
    elif decision.mode == "multi" and token in decision.option_refs:
        action = _local_action(item, decision, "TOGGLE_OPTION", token)
    elif (decision.mode == "multi" and token == SUBMIT_TOKEN) or (
        decision.mode in {"single", "plan"} and token in decision.option_refs
    ):
        refs = list(decision.selected_refs) if token == SUBMIT_TOKEN else [token]
        if not refs:
            return _quarantine(queue, item, ports, "invalid_decision_selection")
        item = replace(item, input={**data, "selection": {"option_refs": refs}})
        command = build_answer_decision(item, decision)
        route = IngressRouteResult(
            RouteStatus.RESOLVED,
            decision.state_token,
            decision.chat_id,
            decision.topic_id,
            decision.worker_id,
            None,
            decision.owner,
            "",
            decision.physical_owner.bot_identity,
            decision.message_binding_id,
            True,
            "decision",
        )
        checkpointed = _checkpoint_command(queue, item, command, route, ports.now())
        return (
            _send_stored_command(queue, checkpointed, ports)
            if checkpointed
            else _quarantine(queue, item, ports, "checkpoint_conflict")
        )
    else:
        return _quarantine(queue, item, ports, "invalid_decision_selection")
    stored = queue.store_local_action(
        item.seq,
        item.lease_owner,
        {
            "local_action": action,
            "expected_state_token": decision.state_token.value,
            "now": ports.now(),
        },
    )
    if stored.status not in {"stored", "existing"} or not stored.digest:
        return DispatchResult(stored.status, item.seq, "checkpoint_conflict")
    local = replace(
        item,
        local_action=action,
        operation_digest=stored.digest,
        local_phase="checkpointed",
        local_expected_state_token=decision.state_token.value,
    )
    return apply_local_decision(queue, local, action, ports)


def _same_control(
    decision: DecisionIngressResult,
    action: Mapping[str, Any],
    *,
    provider_phase: bool,
) -> bool:
    owner = (
        action.get("physical_owner") if isinstance(action.get("physical_owner"), Mapping) else {}
    )
    try:
        captured_owner = PhysicalOwner(
            str(owner.get("bot_identity") or ""),
            str(owner.get("chat_id") or ""),
            str(owner.get("topic_id") or ""),
        )
    except ValueError:
        return False
    exact = all(
        (
            decision.status is DecisionStatus.ACTIVE,
            decision.decision_ref == action.get("decision_ref"),
            decision.revision_digest == action.get("revision_digest"),
            decision.message_binding_id == action.get("message_binding_id"),
            decision.message_id == action.get("message_id"),
            decision.physical_owner == captured_owner,
        )
    )
    if not exact:
        return False
    desired = tuple(action.get("desired_selected_refs") or ())
    if provider_phase and decision.selected_refs != desired:
        return False
    try:
        markup, fingerprint = decisions.render_ingress_markup(decision, desired)
    except (TypeError, ValueError):
        return False
    return markup == action.get("reply_markup") and fingerprint == action.get(
        "desired_markup_fingerprint"
    )


def _mutation(
    action: Mapping[str, Any], kind: DecisionMutationKind, token: str
) -> DecisionMutation:
    return DecisionMutation(
        request_id=str(action["request_id"]),
        kind=kind,
        decision_ref=str(action["decision_ref"]),
        revision_digest=str(action["revision_digest"]),
        option_ref=(
            str(action.get("option_ref")) if action.get("option_ref") is not None else None
        ),
        desired_selected_refs=tuple(action.get("desired_selected_refs") or ()),
        desired_markup_fingerprint=str(action.get("desired_markup_fingerprint") or ""),
        expected_state_token=StateToken(token),
    )


def _advance(
    queue: IngressQueue,
    item: QueueItem,
    old: str,
    new: str,
    expected: str,
    *,
    token: str | None = None,
    outcome: str | None = None,
    ports: IngressPorts,
) -> bool:
    transition: dict[str, Any] = {
        "operation_digest": item.operation_digest,
        "from_phase": old,
        "to_phase": new,
        "expected_token": expected,
        "now": ports.now(),
    }
    if token is not None:
        transition["state_token"] = token
    if outcome is not None:
        transition.update({"provider_outcome": outcome, "provider_at": ports.now()})
    return queue.advance_local_phase(item.seq, item.lease_owner, transition).status == "advanced"


def _settle_local(queue: IngressQueue, item: QueueItem, ports: IngressPorts) -> DispatchResult:
    action = item.local_action or {}
    disposition = (
        "local_applied" if action.get("action") == "ARM_FREEFORM" else "local_markup_applied"
    )
    receipt = {
        "schema_version": 1,
        "action": action.get("action"),
        "request_id": item.request_id,
        "phase": item.local_phase,
        "provider_outcome": item.local_provider_outcome,
    }
    result = queue.settle_receipt(
        item.seq,
        item.lease_owner,
        {
            "operation_digest": item.operation_digest,
            "receipt_kind": "local",
            "receipt": receipt,
            "disposition": disposition,
            "now": ports.now(),
            "notify": False,
        },
    )
    return DispatchResult(result.status, item.seq, disposition)


def _apply_state_phase(
    queue: IngressQueue,
    item: QueueItem,
    action: Mapping[str, Any],
    ports: IngressPorts,
) -> QueueItem | DispatchResult:
    expected = item.local_expected_state_token or ""
    kind = (
        DecisionMutationKind.ARM_FREEFORM
        if action.get("action") == "ARM_FREEFORM"
        else DecisionMutationKind.TOGGLE_OPTION
    )
    result = apply_decision_ingress(ports.state_path, _mutation(action, kind, expected))
    if result.status is DecisionMutationStatus.STALE:
        fresh = _read_policy_and_decision(ports, item.input, str(action.get("callback_ref") or ""))
        valid = (
            fresh.status is DecisionStatus.ACTIVE
            and fresh.decision_ref == action.get("decision_ref")
            and fresh.revision_digest == action.get("revision_digest")
        )
        if action.get("action") == "TOGGLE_OPTION":
            valid = valid and _same_control(fresh, action, provider_phase=False)
        if not valid or not _advance(
            queue,
            item,
            "checkpointed",
            "checkpointed",
            expected,
            token=fresh.state_token.value,
            ports=ports,
        ):
            return _quarantine(
                queue, item, ports, "local_decision_drift", digest=item.operation_digest
            )
        item = replace(item, local_expected_state_token=fresh.state_token.value)
        result = apply_decision_ingress(
            ports.state_path, _mutation(action, kind, fresh.state_token.value)
        )
    if result.status not in {
        DecisionMutationStatus.APPLIED,
        DecisionMutationStatus.ALREADY_APPLIED,
    }:
        return _quarantine(
            queue, item, ports, f"local_state_{result.status.value}", digest=item.operation_digest
        )
    if not _advance(
        queue,
        item,
        "checkpointed",
        "state_applied",
        item.local_expected_state_token or "",
        token=result.state_token.value,
        ports=ports,
    ):
        return DispatchResult("lost", item.seq, "local_phase_lost")
    return replace(
        item, local_phase="state_applied", local_applied_state_token=result.state_token.value
    )


def _provider_ready(
    queue: IngressQueue,
    item: QueueItem,
    action: Mapping[str, Any],
    ports: IngressPorts,
) -> QueueItem | DispatchResult:
    decision = _read_policy_and_decision(ports, item.input, str(action.get("callback_ref") or ""))
    if not _same_control(decision, action, provider_phase=True):
        return _quarantine(
            queue, item, ports, "provider_control_drift", digest=item.operation_digest
        )
    expected = item.local_applied_state_token or ""
    if not _advance(
        queue,
        item,
        "state_applied",
        "provider_ready",
        expected,
        token=decision.state_token.value,
        ports=ports,
    ):
        return DispatchResult("lost", item.seq, "local_phase_lost")
    return replace(
        item, local_phase="provider_ready", local_provider_state_token=decision.state_token.value
    )


def _provider_edit(
    queue: IngressQueue,
    item: QueueItem,
    action: Mapping[str, Any],
    ports: IngressPorts,
    telegram: TelegramPort,
) -> QueueItem | DispatchResult:
    if not queue.renew(item.seq, item.lease_owner, ports.now(), ports.lease_seconds):
        return DispatchResult("lost", item.seq, "lease_lost")
    decision = _read_policy_and_decision(ports, item.input, str(action.get("callback_ref") or ""))
    if not _same_control(decision, action, provider_phase=True):
        return _quarantine(
            queue, item, ports, "provider_control_drift", digest=item.operation_digest
        )
    expected = item.local_provider_state_token or ""
    if decision.state_token.value != expected:
        if not _advance(
            queue,
            item,
            "provider_ready",
            "provider_ready",
            expected,
            token=decision.state_token.value,
            ports=ports,
        ):
            return DispatchResult("lost", item.seq, "local_phase_lost")
        expected = decision.state_token.value
        item = replace(item, local_provider_state_token=expected)
    try:
        result = telegram.edit_message_reply_markup(
            str(action["physical_owner"]["chat_id"]),
            str(action["message_id"]),
            dict(action["reply_markup"]),
        )
    except RateLimited:
        result = {"ok": False, "kind": "definitely_not_started"}
    except TelegramError as exc:
        result = {"ok": False, "kind": ("ambiguous" if exc.ambiguous_acceptance else "rejected")}
    if result.get("ok") is not True:
        kind = result.get("kind")
        if result.get("ambiguous_acceptance") is True or kind in {
            "ambiguous",
            "definitely_not_started",
            "transport_ambiguous",
        }:
            retried = queue.schedule_retry(
                item.seq,
                item.lease_owner,
                {
                    "operation_digest": item.operation_digest,
                    "disposition": "no_receipt",
                    "now": ports.now(),
                    "next_attempt_at": _backoff(item, ports.now()),
                },
            )
            return DispatchResult(retried.status, item.seq, "provider_retry")
        return _quarantine(queue, item, ports, "provider_edit_failed", digest=item.operation_digest)
    outcome = "not_modified" if result.get("kind") == "unchanged" else "accepted"
    if not _advance(
        queue, item, "provider_ready", "provider_applied", expected, outcome=outcome, ports=ports
    ):
        return DispatchResult("lost", item.seq, "local_phase_lost")
    return replace(
        item,
        local_phase="provider_applied",
        local_provider_outcome=outcome,
        local_provider_at=ports.now(),
    )


def _record_markup(
    queue: IngressQueue,
    item: QueueItem,
    action: Mapping[str, Any],
    ports: IngressPorts,
) -> QueueItem | DispatchResult:
    expected = item.local_provider_state_token or ""
    result = apply_decision_ingress(
        ports.state_path, _mutation(action, DecisionMutationKind.RECORD_LOCAL_MARKUP, expected)
    )
    if result.status is DecisionMutationStatus.STALE:
        fresh = _read_policy_and_decision(ports, item.input, str(action.get("callback_ref") or ""))
        if not _same_control(fresh, action, provider_phase=True):
            return _quarantine(
                queue, item, ports, "provider_applied_control_drift", digest=item.operation_digest
            )
        if not _advance(
            queue,
            item,
            "provider_applied",
            "provider_applied",
            expected,
            token=fresh.state_token.value,
            ports=ports,
        ):
            return DispatchResult("lost", item.seq, "local_phase_lost")
        expected = fresh.state_token.value
        item = replace(item, local_provider_state_token=expected)
        result = apply_decision_ingress(
            ports.state_path, _mutation(action, DecisionMutationKind.RECORD_LOCAL_MARKUP, expected)
        )
    if result.status not in {
        DecisionMutationStatus.APPLIED,
        DecisionMutationStatus.ALREADY_APPLIED,
    }:
        return _quarantine(
            queue, item, ports, "provider_applied_control_drift", digest=item.operation_digest
        )
    if not _advance(
        queue,
        item,
        "provider_applied",
        "markup_recorded",
        expected,
        token=result.state_token.value,
        ports=ports,
    ):
        return DispatchResult("lost", item.seq, "local_phase_lost")
    return replace(
        item, local_phase="markup_recorded", local_markup_state_token=result.state_token.value
    )


def apply_local_decision(
    queue: IngressQueue,
    item: QueueItem,
    action: Mapping[str, Any],
    ports: IngressPorts,
) -> DispatchResult:
    """Resume one checkpointed local decision from its durable phase."""

    if not item.operation_digest:
        return _quarantine(queue, item, ports, "missing_local_digest")
    if item.local_phase == "checkpointed":
        advanced = _apply_state_phase(queue, item, action, ports)
        if isinstance(advanced, DispatchResult):
            return advanced
        item = advanced
    if action.get("action") == "ARM_FREEFORM":
        return _settle_local(queue, item, ports)
    owner_raw = action.get("physical_owner")
    if not isinstance(owner_raw, Mapping):
        return _quarantine(
            queue, item, ports, "invalid_physical_owner", digest=item.operation_digest
        )
    owner = PhysicalOwner(
        str(owner_raw.get("bot_identity") or ""),
        str(owner_raw.get("chat_id") or ""),
        str(owner_raw.get("topic_id") or ""),
    )
    try:
        with provider_mutation_guard(
            ports.state_path, owner, ports.monotonic() + ports.provider_timeout_seconds
        ):
            if not queue.renew(
                item.seq,
                item.lease_owner,
                ports.now(),
                ports.provider_timeout_seconds + ports.lease_seconds,
            ):
                return DispatchResult("lost", item.seq, "lease_lost")
            if item.local_phase == "state_applied":
                advanced = _provider_ready(queue, item, action, ports)
                if isinstance(advanced, DispatchResult):
                    return advanced
                item = advanced
            telegram = ports.telegram_for(item.receiver_id)
            if item.local_phase == "provider_ready":
                advanced = _provider_edit(queue, item, action, ports, telegram)
                if isinstance(advanced, DispatchResult):
                    return advanced
                item = advanced
            if item.local_phase == "provider_applied":
                advanced = _record_markup(queue, item, action, ports)
                if isinstance(advanced, DispatchResult):
                    return advanced
                item = advanced
    except TimeoutError:
        retried = queue.schedule_retry(
            item.seq,
            item.lease_owner,
            {
                "operation_digest": item.operation_digest,
                "disposition": "definitely_not_started",
                "now": ports.now(),
                "next_attempt_at": _backoff(item, ports.now()),
            },
        )
        return DispatchResult(retried.status, item.seq, "provider_guard_timeout")
    return _settle_local(queue, item, ports)


def dispatch_one(
    queue: IngressQueue,
    item: QueueItem,
    ports: IngressPorts,
) -> DispatchResult:
    if item.command is not None:
        return _send_stored_command(queue, item, ports)
    if item.local_action is not None:
        return apply_local_decision(queue, item, item.local_action, ports)
    if item.input.get("preview_quarantine"):
        return _quarantine(queue, item, ports, str(item.input["preview_quarantine"]))
    if item.kind == "decision":
        return dispatch_decision(queue, item, ports)
    decision = _read_policy_and_decision(ports, item.input, None)
    if decision.status is DecisionStatus.ACTIVE and decision.await_freeform:
        command = build_answer_decision(item, decision)
        route = IngressRouteResult(
            RouteStatus.RESOLVED,
            decision.state_token,
            decision.chat_id,
            decision.topic_id,
            decision.worker_id,
            None,
            decision.owner,
            "",
            decision.physical_owner.bot_identity,
            decision.message_binding_id,
            True,
            "freeform",
        )
    else:
        route = _route_for_item(item, ports)
        if route.status is not RouteStatus.RESOLVED:
            reason = (
                "ambiguous_reply_target"
                if route.status is RouteStatus.BINDING_AMBIGUOUS
                else (
                    "ambiguous_reply_author_target"
                    if route.status is RouteStatus.AUTHOR_AMBIGUOUS
                    else route.reason or f"route_{route.status.value}"
                )
            )
            return _quarantine(queue, item, ports, reason)
        command = build_send_instruction(item, route)
    checkpointed = _checkpoint_command(queue, item, command, route, ports.now())
    if checkpointed is None:
        return _quarantine(queue, item, ports, "checkpoint_conflict")
    return _send_stored_command(queue, checkpointed, ports)


def send_terminal_notice(
    queue: IngressQueue,
    claim: NoticeClaim,
    telegram: TelegramPort,
    *,
    now: Callable[[], float] = time.time,
) -> NoticeResult:
    """Attempt one at-most-once terminal notice after its durable claim."""

    data = claim.input
    try:
        result = telegram.send_message(
            str(data.get("chat_id") or ""),
            html_escape(claim.terminal_reply, 160),
            thread_id=str(data.get("topic_id") or "") or None,
            reply_to_message_id=str(data.get("message_id") or "") or None,
            notify=True,
            max_physical_writes=1,
            ambiguous_errors_are_unknown=True,
        )
    except Exception:
        return NoticeResult("claimed_ambiguous", claim.seq)
    message_id = str(result.get("message_id") or result.get("reply_markup_message_id") or "")
    if result.get("ok") is not True or not message_id:
        return NoticeResult("claimed_ambiguous", claim.seq)
    marked = queue.mark_notice_sent(claim.seq, claim.claim_id, message_id, now())
    return NoticeResult("sent" if marked else "checkpoint_lost", claim.seq, message_id)


def _minimal_reply_route(
    update: Mapping[str, Any],
    policy: IngressPolicy,
    receiver: IngressReceiver,
    ports: IngressPorts,
) -> IngressRouteResult | None:
    message = update.get("message")
    if not isinstance(message, Mapping):
        return None
    reply_id, author_kind = _reply_details(message, policy)
    if not reply_id:
        return None
    chat = message.get("chat") if isinstance(message.get("chat"), Mapping) else {}
    text, alias, mention_kind = _text_and_alias(message, policy)
    del text
    query = IngressReplyQuery(
        chat_id=_coordinate(chat.get("id")),
        topic_id=_topic_id(message, policy),
        reply_message_id=reply_id,
        observed_author_bot_kind=author_kind,
        explicit_alias=alias,
        explicit_bot_kind=mention_kind,
        state_token=policy.state_token,
    )
    result = resolve_ingress_reply(ports.state_path, query)
    if result.status is RouteStatus.STALE:
        fresh = read_ingress_policy(ports.state_path)
        result = resolve_ingress_reply(
            ports.state_path, replace(query, state_token=fresh.state_token)
        )
    return result


def _accept_preview(
    preview: Preview,
    queue: IngressQueue,
    ports: IngressPorts,
) -> tuple[str, int | None]:
    now = ports.now()
    if preview.disposition not in {"enqueue", "quarantine"}:
        result = queue.accept_update(
            {
                "receiver_id": preview.receiver_id,
                "update_id": preview.update_id,
                "first_seen_at": now,
                "enqueue": False,
            }
        )
        return result.status, result.seq
    request_id = derive_telegram_request_id(
        ports.request_id_key,
        receiver_id=preview.receiver_id,
        update_id=preview.update_id,
        chat_id=int(preview.chat_id),
        message_id=int(preview.message_id),
    )
    result = queue.accept_update(
        {
            "receiver_id": preview.receiver_id,
            "update_id": preview.update_id,
            "first_seen_at": now,
            "request_id": request_id,
            "ordering_key": ordering_key(preview),
            "kind": preview.kind,
            "input": json.loads(canonical_input(preview)),
            "deadline_at": now + ports.retry_horizon_seconds,
            "retain_until": now + ports.retention_seconds,
            "depth_limit": ports.depth_limit,
            "overflow_reply": OVERFLOW_REPLY,
        }
    )
    return result.status, result.seq


def _ack_callback(preview: Preview, telegram: TelegramPort, status: str) -> None:
    if not preview.callback_query_id:
        return
    toast = {
        "enqueued": "Queued.",
        "duplicate": "Already queued.",
        "overflow": "Queue is busy; try again shortly.",
    }.get(status, "")
    try:
        telegram.answer_callback_query(preview.callback_query_id, toast)
    except Exception:
        pass


def poll_receiver_once(
    receiver: IngressReceiver,
    queue: IngressQueue,
    ports: IngressPorts,
) -> PollResult:
    telegram = ports.telegram_for(receiver.receiver_id)
    cursor = queue.cursor(receiver.receiver_id)
    payload: dict[str, Any] = {
        "timeout": 0 if cursor is None else ports.poll_timeout_seconds,
        "allowed_updates": json.dumps(["message", "callback_query"], separators=(",", ":")),
    }
    if cursor is not None:
        payload["offset"] = cursor
    try:
        response = telegram.api("getUpdates", payload)
        updates = response.get("result") if isinstance(response, Mapping) else None
        rows = (
            [row for row in updates if isinstance(row, Mapping)]
            if isinstance(updates, list)
            else []
        )
    except Exception:
        return PollResult(receiver.receiver_id, errors=1)
    if cursor is None:
        valid_ids = [value for row in rows if (value := _integer(row.get("update_id"))) is not None]
        queue.initialize_cursor(receiver.receiver_id, max(valid_ids, default=-1) + 1)
        return PollResult(receiver.receiver_id, received=len(rows), advanced=len(rows))
    counts = {"enqueued": 0, "advanced": 0, "overflow": 0, "duplicate": 0, "errors": 0}
    rows.sort(
        key=lambda row: (
            _integer(row.get("update_id")) if _integer(row.get("update_id")) is not None else 2**63
        )
    )
    for update in rows:
        try:
            policy = read_ingress_policy(ports.state_path)
            reply_route = _minimal_reply_route(update, policy, receiver, ports)
            preview = preview_update(update, policy, receiver, reply_route)
            if preview.disposition == "invalid":
                counts["errors"] += 1
                continue
            status, seq = _accept_preview(preview, queue, ports)
            counts[status if status in counts else "advanced"] += 1
            _ack_callback(preview, telegram, status)
            if status == "overflow" and seq is not None:
                claim = queue.claim_notice(seq, ports.now())
                if claim is not None:
                    send_terminal_notice(queue, claim, telegram, now=ports.now)
        except (RuntimeError, ValueError, TypeError):
            counts["errors"] += 1
            break
    return PollResult(
        receiver.receiver_id,
        len(rows),
        counts["enqueued"],
        counts["advanced"],
        counts["overflow"],
        counts["duplicate"],
        counts["errors"],
    )


def _dispatch_loop(ports: IngressPorts, worker_number: int) -> None:
    lease_owner = f"gateway-{worker_number}-{threading.get_native_id()}"
    while not ports.stop_event.is_set():
        item = ports.queue.claim(lease_owner, ports.now(), ports.lease_seconds)
        if item is None:
            claim = ports.queue.claim_next_notice(ports.now())
            if claim is not None:
                receiver = receiver_for_id(ports.receivers, claim.receiver_id)
                if receiver is not None:
                    send_terminal_notice(
                        ports.queue, claim, ports.telegram_for(receiver.receiver_id), now=ports.now
                    )
                continue
            ports.stop_event.wait(ports.idle_seconds)
            continue
        try:
            result = dispatch_one(ports.queue, item, ports)
            claim = ports.queue.claim_notice(result.seq, ports.now())
            if claim is not None:
                receiver = receiver_for_id(ports.receivers, claim.receiver_id)
                if receiver is not None:
                    send_terminal_notice(
                        ports.queue, claim, ports.telegram_for(receiver.receiver_id), now=ports.now
                    )
        except Exception as exc:
            ports.log(f"ingress dispatch failed seq={item.seq}: {type(exc).__name__}")
            if item.operation_digest:
                ports.queue.schedule_retry(
                    item.seq,
                    item.lease_owner,
                    {
                        "operation_digest": item.operation_digest,
                        "disposition": "no_receipt",
                        "now": ports.now(),
                        "next_attempt_at": _backoff(item, ports.now()),
                    },
                )
            else:
                _quarantine(ports.queue, item, ports, "orchestration_failure")


def _poll_loop(ports: IngressPorts, receiver: IngressReceiver) -> None:
    while not ports.stop_event.is_set():
        result = poll_receiver_once(receiver, ports.queue, ports)
        if result.errors:
            ports.stop_event.wait(min(5.0, max(ports.idle_seconds, 1.0)))


def run_gateway(ports: IngressPorts) -> int:
    """Run pollers and dispatchers until the supplied stop event is set."""

    if not ports.receivers:
        raise RuntimeError("no Telegram ingress receivers configured")
    workers = max(1, min(64, int(ports.dispatch_workers)))
    with ThreadPoolExecutor(
        max_workers=len(ports.receivers) + workers, thread_name_prefix="herdres-ingress"
    ) as pool:
        futures = [pool.submit(_poll_loop, ports, receiver) for receiver in ports.receivers]
        futures.extend(pool.submit(_dispatch_loop, ports, number) for number in range(workers))
        try:
            while not ports.stop_event.wait(0.25):
                for future in futures:
                    if future.done() and future.exception() is not None:
                        raise future.exception()  # type: ignore[misc]
        finally:
            ports.stop_event.set()
    return 0


__all__ = [
    "CanonicalCommand",
    "DispatchResult",
    "IngressPorts",
    "NoticeResult",
    "PollResult",
    "Preview",
    "ReceiptReduction",
    "apply_local_decision",
    "build_answer_decision",
    "build_send_instruction",
    "canonical_input",
    "dispatch_decision",
    "dispatch_one",
    "ordering_key",
    "poll_receiver_once",
    "preview_update",
    "reduce_daemon_receipt",
    "run_gateway",
    "send_terminal_notice",
]
