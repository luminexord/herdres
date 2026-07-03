"""Telegram delivery helpers for Tendwire connector outbox items."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any
import os

import herdres_tendwire

from .formatter import attention_notice_html, attention_notice_text


@dataclass(frozen=True)
class TelegramDeliveryRuntime:
    sanitize: Callable[[str, int], str]
    now: Callable[[], str]
    send_rich_message: Callable[..., dict[str, Any]]
    pane_root_reply_target: Callable[[dict[str, Any]], str | None]
    managed_bot_token_for_entry: Callable[..., str | None]
    connector_call: Callable[[str, dict[str, Any] | None], dict[str, Any]]
    rate_limited_exceptions: tuple[type[BaseException], ...] = ()


def outbound_delivery_configured(chat_id: str, *, env: Any | None = None) -> bool:
    source = os.environ if env is None else env
    get = source.get if hasattr(source, "get") else os.environ.get
    return bool(
        str(chat_id or "").strip()
        and (
            str(get("HERDRES_OUTBOUND_BOT_TOKEN", "") or "").strip()
            or str(get("TELEGRAM_BOT_TOKEN", "") or "").strip()
        )
    )


def deliver_outbox_item(
    state: dict[str, Any],
    chat_id: str,
    telegram: dict[str, Any],
    item: dict[str, Any],
    *,
    runtime: TelegramDeliveryRuntime,
    default_general_thread_id: str,
) -> dict[str, Any]:
    payload = herdres_tendwire.outbox_item_payload(item)
    html_text = attention_notice_html(payload, sanitize=runtime.sanitize)
    fallback_text = attention_notice_text(payload, sanitize=runtime.sanitize)
    route_entry = herdres_tendwire.outbox_worker_route_entry(state, payload, sanitize=runtime.sanitize)
    if route_entry is not None:
        thread_id = route_entry.get("topic_id") or telegram.get("general_thread_id", default_general_thread_id)
        reply_to_message_id = runtime.pane_root_reply_target(route_entry)
        api_token = runtime.managed_bot_token_for_entry(telegram, route_entry, route_entry)
    else:
        thread_id = telegram.get("general_thread_id", default_general_thread_id)
        reply_to_message_id = None
        api_token = None
    result = runtime.send_rich_message(
        chat_id,
        html_text,
        telegram=telegram,
        fallback_text=fallback_text,
        thread_id=thread_id,
        notify=True,
        reply_to_message_id=reply_to_message_id,
        api_token=api_token,
    )
    if result.get("ok"):
        herdres_tendwire.note_outbox_audit(
            state,
            {
                "event_type": herdres_tendwire.outbox_event_type(payload, sanitize=runtime.sanitize),
                "attempt": int(item.get("attempt") or 0),
                "status": "delivered",
                "delivered_at": runtime.now(),
            },
            checked_at=runtime.now(),
        )
    return result


def drain_connector_outbox(
    state: dict[str, Any],
    chat_id: str,
    telegram: dict[str, Any],
    counters: dict[str, int],
    *,
    runtime: TelegramDeliveryRuntime,
    max_sends: int,
    default_general_thread_id: str,
    enabled: bool | None = None,
    limit: int | None = None,
    delivery_configured: bool | None = None,
) -> dict[str, Any]:
    if enabled is None:
        enabled = herdres_tendwire.connector_outbox_enabled()
    if delivery_configured is None:
        delivery_configured = outbound_delivery_configured(chat_id)
    prepared = herdres_tendwire.outbox_prepare_drain(
        enabled=bool(enabled),
        max_sends=max_sends,
        sent_count=int(counters.get("sends", 0)),
        delivery_configured=bool(delivery_configured),
        limit=limit,
    )
    result = prepared["result"] if isinstance(prepared.get("result"), dict) else herdres_tendwire.outbox_drain_result(False)
    if not prepared.get("should_poll"):
        return result
    poll = runtime.connector_call(
        "poll",
        prepared.get("poll_params") if isinstance(prepared.get("poll_params"), dict) else {},
    )
    items, audit_event = herdres_tendwire.outbox_apply_poll_response(
        result,
        poll,
        sanitize=runtime.sanitize,
    )
    if audit_event is not None:
        herdres_tendwire.note_outbox_audit(
            state,
            {"status": audit_event.get("status"), "checked_at": runtime.now()},
            checked_at=runtime.now(),
        )
        return result

    def execute_outbox_connector_plan(plan: dict[str, Any]) -> None:
        params = plan.get("params") if isinstance(plan.get("params"), dict) else {}
        response = runtime.connector_call(str(plan.get("action") or ""), params)
        herdres_tendwire.outbox_record_connector_response(result, plan, response)

    for item in items:
        action = herdres_tendwire.outbox_item_action(item, herdres_tendwire.outbox_delivered_identities(state))
        ref = str(action.get("ref") or "")
        if not ref:
            continue
        identity = str(action.get("identity") or "")
        if action.get("action") == "ack_duplicate":
            plan = herdres_tendwire.outbox_connector_plan(item, action, "duplicate", sanitize=runtime.sanitize)
            execute_outbox_connector_plan(plan)
            continue
        try:
            sent = deliver_outbox_item(
                state,
                chat_id,
                telegram,
                item,
                runtime=runtime,
                default_general_thread_id=default_general_thread_id,
            )
        except runtime.rate_limited_exceptions as exc:
            plan = herdres_tendwire.outbox_connector_plan(
                item,
                action,
                "rate_limited",
                retry_after=max(1, int(getattr(exc, "retry_after", 1) or 1)),
                sanitize=runtime.sanitize,
            )
            execute_outbox_connector_plan(plan)
            continue
        except Exception as exc:  # noqa: BLE001 - connector failures are converted to Tendwire fail plans
            sent = {"ok": False, "error": runtime.sanitize(str(exc), 300)}
        if sent.get("ok"):
            counters["sends"] = counters.get("sends", 0) + 1
            herdres_tendwire.outbox_record_delivery_success(result)
            herdres_tendwire.note_outbox_audit(
                state,
                {"identity": identity, "status": "delivered", "recorded_at": runtime.now()},
                checked_at=runtime.now(),
            )
            plan = herdres_tendwire.outbox_connector_plan(item, action, "delivered", sanitize=runtime.sanitize)
            execute_outbox_connector_plan(plan)
            continue
        plan = herdres_tendwire.outbox_connector_plan(item, action, "failed", sanitize=runtime.sanitize)
        execute_outbox_connector_plan(plan)
    return result

