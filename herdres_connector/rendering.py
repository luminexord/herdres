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


def _inline_markdown_html(text: str) -> str:
    escaped = html_escape(text, 4000)
    code_spans: list[str] = []

    def hold_code(match: re.Match[str]) -> str:
        code_spans.append(f"<code>{match.group(1)}</code>")
        return f"\u0000{len(code_spans) - 1}\u0000"

    escaped = re.sub(r"`([^`\n]+)`", hold_code, escaped)
    escaped = re.sub(r"\*\*([^*\n][^*\n]*(?:\*[^*\n]+)*)\*\*", r"<b>\1</b>", escaped)
    escaped = re.sub(r"(?<!\*)\*([^*\n]+)\*(?!\*)", r"<i>\1</i>", escaped)
    escaped = re.sub(r"__([^_\n]+)__", r"<b>\1</b>", escaped)
    for index, code in enumerate(code_spans):
        escaped = escaped.replace(f"\u0000{index}\u0000", code)
    return escaped


def markdownish_to_html(value: Any, *, limit: int = 12000) -> str:
    """Render common agent Markdown into Telegram HTML.

    This intentionally covers the small Markdown subset agents emit most often:
    headings, bold/italic, inline code, fenced code blocks, and bullets. Unknown
    Markdown remains readable plain text instead of leaking raw HTML.
    """
    text = sanitize_text(value, limit).strip()
    if not text:
        return ""
    rendered: list[str] = []
    in_code = False
    code_lines: list[str] = []
    for raw_line in text.splitlines():
        line = raw_line.rstrip()
        if line.strip().startswith("```"):
            if in_code:
                rendered.append(f"<pre>{html_escape(chr(10).join(code_lines), limit)}</pre>")
                code_lines = []
                in_code = False
            else:
                in_code = True
                code_lines = []
            continue
        if in_code:
            code_lines.append(line)
            continue
        stripped = line.strip()
        if not stripped:
            rendered.append("")
            continue
        heading = re.match(r"^(#{1,6})\s+(.+)$", stripped)
        if heading:
            inner = _inline_markdown_html(heading.group(2).strip())
            rendered.append(inner if inner.startswith("<b>") and inner.endswith("</b>") else f"<b>{inner}</b>")
            continue
        bullet = re.match(r"^[-*]\s+(.+)$", stripped)
        if bullet:
            rendered.append(f"• {_inline_markdown_html(bullet.group(1).strip())}")
            continue
        numbered = re.match(r"^(\d+[.)])\s+(.+)$", stripped)
        if numbered:
            rendered.append(f"{html_escape(numbered.group(1), 16)} {_inline_markdown_html(numbered.group(2).strip())}")
            continue
        rendered.append(_inline_markdown_html(line))
    if in_code:
        rendered.append(f"<pre>{html_escape(chr(10).join(code_lines), limit)}</pre>")
    return "\n".join(rendered).strip()


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
        parts.append(f"<b>Response</b>\n{markdownish_to_html(final_text, limit=12000)}")
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
