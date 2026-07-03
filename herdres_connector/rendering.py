"""Telegram HTML rendering for source-mode Herdres."""

from __future__ import annotations

import re
from typing import Any

from .safe import compact_ws, html_escape, sanitize_text


ACTIVE_STATUSES = {"active", "busy", "in_progress", "pending", "running", "waiting", "working"}


def normalized_status(value: Any) -> str:
    text = str(value or "").strip().lower().replace("-", "_")
    if text in ACTIVE_STATUSES:
        return "working"
    if text in {"complete", "completed", "done", "idle", "ready"}:
        return "idle"
    if text in {"blocked", "needs_attention", "waiting_for_input"}:
        return "attention"
    if text in {"error", "failed", "failure"}:
        return "failed"
    return text or "unknown"


def status_emoji(status: str) -> str:
    return {
        "attention": "❓",
        "failed": "‼️",
        "idle": "✅",
        "working": "🔵",
    }.get(normalized_status(status), "☕️")


def worker_label(entry: dict[str, Any] | None, worker: dict[str, Any] | None = None) -> str:
    entry = entry or {}
    worker = worker or {}
    return compact_ws(
        entry.get("topic_name")
        or worker.get("name")
        or entry.get("tendwire_worker_id")
        or worker.get("id")
        or "Worker",
        80,
    )


def html_to_plain(value: str) -> str:
    text = re.sub(r"<br\s*/?>", "\n", value)
    text = re.sub(r"<[^>]+>", "", text)
    return sanitize_text(text, 4000)


def render_working_update(item: dict[str, Any], entry: dict[str, Any]) -> str:
    label = html_escape(worker_label(entry), 80)
    body = compact_ws(item.get("assistant_stream_text") or item.get("user_text") or "Work is in progress.", 700)
    body_html = html_escape(body, 900)
    return f"<b>Working</b> · {label}\n<blockquote>{body_html}</blockquote>"


def render_final_turn(item: dict[str, Any], entry: dict[str, Any]) -> str:
    label = html_escape(worker_label(entry), 80)
    parts = [f"<b>{label}</b>"]
    user_text = sanitize_text(item.get("user_text"), 3500).strip()
    final_text = sanitize_text(item.get("assistant_final_text") or item.get("assistant_stream_text"), 12000).strip()
    if user_text:
        parts.append(f"<b>You</b>\n{html_escape(user_text, 3500)}")
    if final_text:
        parts.append(f"<b>Response</b>\n{html_escape(final_text, 12000)}")
    return "\n\n".join(parts)


def render_pending(item: dict[str, Any], entry: dict[str, Any]) -> str:
    label = html_escape(worker_label(entry), 80)
    prompt = html_escape(item.get("prompt_text") or item.get("text") or "Input needed.", 3000)
    return f"<b>Input Needed</b> · {label}\n\n{prompt}"


def render_status_overview(entries: list[dict[str, Any]]) -> str:
    rows = ["<b>Herdres</b> · Tendwire source mode"]
    for entry in sorted(entries, key=lambda item: str(item.get("topic_name") or item.get("tendwire_worker_id"))):
        status = normalized_status(entry.get("tendwire_status_line") or entry.get("status"))
        rows.append(f"{status_emoji(status)} {html_escape(worker_label(entry), 80)}")
    return "\n".join(rows)


def render_attention_notice(payload: dict[str, Any]) -> str:
    attention = payload.get("attention") if isinstance(payload.get("attention"), dict) else {}
    severity = html_escape(attention.get("severity") or "attention", 40)
    reason = html_escape(attention.get("reason") or attention.get("status") or "Tendwire attention", 1000)
    return f"<b>Tendwire attention</b>\n<b>Severity</b>: {severity}\n{reason}"
