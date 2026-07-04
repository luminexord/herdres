"""Telegram HTML rendering for source-mode Herdres."""

from __future__ import annotations

import re
from typing import Any

from .safe import compact_ws, html_escape, sanitize_text


ACTIVE_STATUSES = {"active", "busy", "in_progress", "pending", "running", "waiting", "working"}
EXPANDABLE_SECTION_CHARS = 700
FINAL_CHUNK_SOURCE_CHARS = 2400
TELEGRAM_SAFE_HTML_CHARS = 3600
PINNED_STATUS_DOTS = {
    "attention": "🔴",
    "failed": "🔴",
    "idle": "🟢",
    "unknown": "⬜",
    "working": "🟡",
}
PINNED_STATUS_SEVERITY = {
    "failed": 8,
    "attention": 7,
    "working": 5,
    "idle": 3,
    "unknown": 1,
}


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


def pretty_model_label(raw: Any) -> str:
    clean = re.sub(r"\[[^\]]*\]", "", str(raw or "")).strip()
    if not clean:
        return ""
    low = clean.lower()
    match = re.match(r"^claude-(opus|sonnet|haiku|fable)-(\d+)(?:[-.](\d+))?", low)
    if match:
        version = match.group(2) + ("." + match.group(3) if match.group(3) else "")
        return f"Claude {match.group(1).capitalize()} {version}"
    match = re.match(r"^gpt-([\d.]+o?)(?:-(codex|mini|turbo|pro|nano))?", low)
    if match:
        suffix = (" " + match.group(2).capitalize()) if match.group(2) else ""
        return f"GPT-{match.group(1)}{suffix}"
    match = re.match(r"^(glm|kimi|gemini|deepseek|grok|qwen)[-.]?(.*)$", low)
    if match:
        family = {
            "deepseek": "DeepSeek",
            "gemini": "Gemini",
            "glm": "GLM",
            "grok": "Grok",
            "kimi": "Kimi",
            "qwen": "Qwen",
        }[match.group(1)]
        rest = " ".join(part.capitalize() if part.isalpha() else part for part in re.split(r"[-_]+", match.group(2)) if part)
        return f"{family} {rest}".strip()
    return clean.replace("_", " ").strip()


def _title_label(value: Any) -> str:
    text = compact_ws(value, 80)
    if not text:
        return ""
    upper = {"glm": "GLM", "gpt": "GPT", "omp": "OMP"}
    lowered = text.lower()
    if lowered in upper:
        return upper[lowered]
    return " ".join(word.upper() if word.lower() in upper else word.capitalize() for word in text.split())


def _active_worker_label(entry: dict[str, Any]) -> str:
    return compact_ws(
        entry.get("active_worker_name")
        or entry.get("active_worker_id")
        or entry.get("worker_name")
        or entry.get("tendwire_worker_id"),
        80,
    )


def _pinned_status_dot(status: str) -> str:
    return PINNED_STATUS_DOTS.get(normalized_status(status), "⬜")


def _pinned_status_severity(status: str) -> int:
    return PINNED_STATUS_SEVERITY.get(normalized_status(status), PINNED_STATUS_SEVERITY["unknown"])


def _pinned_model_suffix(entry: dict[str, Any], label: str) -> str:
    pretty = pretty_model_label(entry.get("model") or entry.get("active_worker_model"))
    if not pretty:
        return ""
    words = pretty.split()
    first_label_word = label.strip().split()[0].lower() if label.strip() else ""
    if words and first_label_word and words[0].lower() == first_label_word:
        pretty = " ".join(words[1:]).strip() or pretty
    return f" · {pretty}"


def _duplicate_worker_suffix(entry: dict[str, Any], label: str) -> str:
    worker_id = compact_ws(entry.get("tendwire_worker_id") or entry.get("worker_id") or entry.get("active_worker_id"), 40)
    if not worker_id:
        return ""
    label_key = re.sub(r"[^a-z0-9]+", "", label.lower())
    worker_key = re.sub(r"[^a-z0-9]+", "", worker_id.lower())
    if worker_key == label_key:
        return ""
    suffix = worker_id
    lowered = worker_id.lower()
    for prefix in (label.lower() + "-", label.lower() + "_", label.lower()):
        if lowered.startswith(prefix):
            suffix = worker_id[len(prefix) :].strip("-_ ")
            break
    return f" {suffix}" if suffix else ""


def pinned_status_entry_label(entry: dict[str, Any]) -> str:
    return (
        _title_label(entry.get("agent"))
        or _title_label(entry.get("worker_name"))
        or _title_label(_active_worker_label(entry))
        or _title_label(worker_label(entry))
        or "Pane"
    )


def _render_status_entry_display(entry: dict[str, Any], display: str) -> str:
    status = normalized_status(
        entry.get("active_worker_status")
        or entry.get("tendwire_status_line")
        or entry.get("status")
    )
    return f"{html_escape(display, 120)} {_pinned_status_dot(status)}"


def render_status_entry(entry: dict[str, Any]) -> str:
    label = pinned_status_entry_label(entry)
    display = f"{label}{_pinned_model_suffix(entry, label)}"
    return _render_status_entry_display(entry, display)


def render_status_overview(entries: list[dict[str, Any]]) -> str:
    records: list[dict[str, Any]] = []
    for entry in entries:
        status = normalized_status(entry.get("active_worker_status") or entry.get("tendwire_status_line") or entry.get("status"))
        label = pinned_status_entry_label(entry)
        model_suffix = _pinned_model_suffix(entry, label)
        display = f"{label}{model_suffix}"
        records.append({"entry": entry, "status": status, "label": label, "model_suffix": model_suffix, "display": display})
    if not records:
        return "No active panes."
    display_counts: dict[str, int] = {}
    for record in records:
        key = str(record["display"]).casefold()
        display_counts[key] = display_counts.get(key, 0) + 1
    rows: list[tuple[int, str, str]] = []
    for record in records:
        entry = record["entry"]
        label = str(record["label"])
        display = str(record["display"])
        if display_counts.get(display.casefold(), 0) > 1:
            display = f"{label}{_duplicate_worker_suffix(entry, label)}{record['model_suffix']}"
        rows.append(
            (
                _pinned_status_severity(str(record["status"])),
                display.casefold(),
                _render_status_entry_display(entry, display),
            )
        )
    rows.sort(key=lambda row: (-row[0], row[1]))
    return "\n".join(row[2] for row in rows)


def render_attention_notice(payload: dict[str, Any]) -> str:
    attention = payload.get("attention") if isinstance(payload.get("attention"), dict) else {}
    severity = html_escape(attention.get("severity") or "attention", 40)
    reason = html_escape(attention.get("reason") or attention.get("status") or "Tendwire attention", 1000)
    return f"<b>Tendwire attention</b>\n<b>Severity</b>: {severity}\n{reason}"
