"""Telegram HTML rendering for source-mode Herdres."""

from __future__ import annotations

import re
from typing import Any

from .safe import compact_ws, html_escape, sanitize_text


ACTIVE_STATUSES = {"active", "busy", "in_progress", "pending", "running", "waiting", "working"}
EXPANDABLE_SECTION_CHARS = 700
FINAL_CHUNK_SOURCE_CHARS = 2400
TELEGRAM_SAFE_HTML_CHARS = 3600


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
        "working": "⚡️",
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


def html_to_plain(value: str, *, limit: int = 12000) -> str:
    text = re.sub(r"<br\s*/?>", "\n", value)
    text = re.sub(r"<[^>]+>", "", text)
    return sanitize_text(text, limit)


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


def section_html(label: str, body_html: str, *, expandable: bool = False) -> str:
    body = str(body_html or "").strip()
    if not body:
        return ""
    title = html_escape(label, 80)
    attr = " expandable" if expandable else ""
    return f"<b>{title}</b>\n<blockquote{attr}>{body}</blockquote>"


def split_text_chunks(value: Any, *, limit: int = FINAL_CHUNK_SOURCE_CHARS) -> list[str]:
    text = sanitize_text(value, 12000).strip()
    if not text:
        return []
    chunks: list[str] = []
    current: list[str] = []
    current_len = 0

    def flush() -> None:
        nonlocal current, current_len
        body = "\n".join(current).strip()
        if body:
            chunks.append(body)
        current = []
        current_len = 0

    for line in text.splitlines():
        if len(line) > limit:
            flush()
            start = 0
            while start < len(line):
                chunks.append(line[start : start + limit].strip())
                start += limit
            continue
        extra = len(line) + (1 if current else 0)
        if current and current_len + extra > limit:
            flush()
        current.append(line)
        current_len += extra
    flush()
    return chunks


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
        parts.append(
            section_html(
                "You",
                html_escape(user_text, 3500),
                expandable=len(user_text) > EXPANDABLE_SECTION_CHARS,
            )
        )
    if final_text:
        parts.append(
            section_html(
                "Response",
                markdownish_to_html(final_text, limit=12000),
                expandable=False,
            )
        )
    return "\n\n".join(parts)


def render_final_turn_chunks(item: dict[str, Any], entry: dict[str, Any], *, max_chars: int = TELEGRAM_SAFE_HTML_CHARS) -> list[str]:
    full = render_final_turn(item, entry)
    if len(full) <= max_chars:
        return [full]

    label = html_escape(worker_label(entry), 80)
    header = f"<b>{label}</b>"
    user_text = sanitize_text(item.get("user_text"), 3500).strip()
    final_text = sanitize_text(item.get("assistant_final_text") or item.get("assistant_stream_text"), 12000).strip()
    messages: list[str] = []

    if user_text:
        user_section = section_html(
            "You",
            html_escape(user_text, 3200),
            expandable=len(user_text) > EXPANDABLE_SECTION_CHARS,
        )
        user_message = "\n\n".join([header, user_section])
        if len(user_message) <= max_chars:
            messages.append(user_message)

    raw_chunks = split_text_chunks(final_text)
    total = len(raw_chunks)
    for index, chunk in enumerate(raw_chunks, start=1):
        response_label = "Response" if total <= 1 else f"Response {index}/{total}"
        response_section = section_html(
            response_label,
            markdownish_to_html(chunk, limit=FINAL_CHUNK_SOURCE_CHARS + 800),
            expandable=False,
        )
        message = "\n\n".join([header, response_section])
        if len(message) <= max_chars:
            messages.append(message)
            continue
        for smaller in split_text_chunks(chunk, limit=max(800, FINAL_CHUNK_SOURCE_CHARS // 2)):
            messages.append(
                "\n\n".join(
                    [
                        header,
                        section_html(response_label, markdownish_to_html(smaller, limit=FINAL_CHUNK_SOURCE_CHARS), expandable=False),
                    ]
                )
            )
    return messages or [full[:max_chars]]


def render_pending(item: dict[str, Any], entry: dict[str, Any]) -> str:
    label = html_escape(worker_label(entry), 80)
    prompt = html_escape(item.get("prompt_text") or item.get("text") or "Input needed.", 3000)
    return f"<b>Input Needed</b> · {label}\n\n{prompt}"


def _pane_count_label(value: Any) -> str:
    try:
        count = int(value)
    except (TypeError, ValueError):
        return ""
    if count <= 0:
        return ""
    return "1 pane" if count == 1 else f"{count} panes"


def _active_worker_label(entry: dict[str, Any]) -> str:
    return compact_ws(
        entry.get("active_worker_name")
        or entry.get("active_worker_id")
        or entry.get("worker_name")
        or entry.get("tendwire_worker_id"),
        80,
    )


def render_status_entry(entry: dict[str, Any]) -> str:
    status = normalized_status(
        entry.get("active_worker_status")
        or entry.get("tendwire_status_line")
        or entry.get("status")
    )
    topic = html_escape(worker_label(entry), 80)
    details: list[str] = []
    active_worker = _active_worker_label(entry)
    if active_worker:
        details.append(f"active: {html_escape(active_worker, 80)}")
        details.append(status)
    else:
        details.append("no active pane")
    pane_count = _pane_count_label(entry.get("worker_count"))
    if pane_count:
        details.append(pane_count)
    return f"{status_emoji(status)} <b>{topic}</b> · {' · '.join(details)}"


def render_status_overview(entries: list[dict[str, Any]]) -> str:
    rows = ["<b>Herdres</b> · Tendwire source mode"]
    for entry in sorted(entries, key=lambda item: str(item.get("topic_name") or item.get("tendwire_worker_id"))):
        rows.append(render_status_entry(entry))
    return "\n".join(rows)


def render_attention_notice(payload: dict[str, Any]) -> str:
    attention = payload.get("attention") if isinstance(payload.get("attention"), dict) else {}
    severity = html_escape(attention.get("severity") or "attention", 40)
    reason = html_escape(attention.get("reason") or attention.get("status") or "Tendwire attention", 1000)
    return f"<b>Tendwire attention</b>\n<b>Severity</b>: {severity}\n{reason}"
