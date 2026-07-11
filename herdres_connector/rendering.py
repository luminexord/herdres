"""Telegram HTML rendering for source-mode Herdres."""

from __future__ import annotations

import re
from typing import Any, Callable

from .safe import canonical_text, compact_ws, html_escape, sanitize_text


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
    # Table-aware: adjacent cells become ` | ` and row/block ends become newlines, so a <table> that
    # never reaches the rich path (rich disabled / oversize fallback) degrades to readable
    # `|`-separated rows rather than mashed-together cell text.
    text = re.sub(r"</t[dh]>\s*<t[dh]\b[^>]*>", " | ", value, flags=re.IGNORECASE)
    text = re.sub(r"</tr>\s*", "\n", text, flags=re.IGNORECASE)
    text = re.sub(r"<br\s*/?>", "\n", text)
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


# A GitHub-style pipe-table delimiter row, e.g. ``| :--- | ---: |`` or ``---|---``.
_TABLE_SEPARATOR_RE = re.compile(r"^\s*\|?\s*:?-{1,}:?\s*(?:\|\s*:?-{1,}:?\s*)*\|?\s*$")
_TABLE_MAX_ROWS = 40


def _looks_like_table_row(line: str) -> bool:
    return "|" in line and bool(line.strip())


def _looks_like_table_separator(line: str) -> bool:
    # Require a pipe: a bare `---`/`------` under a pipe-containing prose line (e.g. a horizontal rule
    # after "run `foo | grep bar`") is NOT a table delimiter — matching GitHub, which needs the pipe
    # structure. This is the guard against mangling ordinary prose + rules into a fake table.
    s = line.strip()
    return "-" in s and "|" in s and bool(_TABLE_SEPARATOR_RE.match(s))


def _table_cells(line: str) -> list[str]:
    """Split a pipe-table row into cells, respecting inline-code spans and escaped ``\\|`` so a pipe
    inside ``\\`a | b\\``` or written ``\\|`` does not create a spurious column (ported from the
    pre-tendwire renderer)."""
    text = str(line or "").strip()
    if "|" not in text:
        return []
    if text.startswith("|"):
        text = text[1:]
    if text.endswith("|") and not text.endswith("\\|"):
        text = text[:-1]
    cells: list[str] = []
    buf: list[str] = []
    in_code = False
    i = 0
    while i < len(text):
        ch = text[i]
        if ch == "\\" and i + 1 < len(text) and text[i + 1] == "|":
            buf.append("|")
            i += 2
            continue
        if ch == "`":
            in_code = not in_code
            buf.append(ch)
        elif ch == "|" and not in_code:
            cells.append("".join(buf).strip())
            buf = []
        else:
            buf.append(ch)
        i += 1
    cells.append("".join(buf).strip())
    return cells


def _render_table_html(rows: list[list[str]], *, cell_html: "Callable[[str], str]") -> str:
    """Render parsed rows (first = header) as a native ``<table bordered striped>`` — the Telegram
    rich-message path turns this into a real ``PageBlockTable`` (native table), far better than a
    monospace box. Cell content is rendered rich (bold/code/links) via ``cell_html``."""
    trimmed = rows[:_TABLE_MAX_ROWS]
    width = max(len(row) for row in trimmed)
    grid = [row + [""] * (width - len(row)) for row in trimmed]
    header, body = grid[0], grid[1:]
    html_rows = ["<tr>" + "".join(f"<th>{cell_html(cell)}</th>" for cell in header) + "</tr>"]
    html_rows.extend(
        "<tr>" + "".join(f"<td>{cell_html(cell)}</td>" for cell in row) + "</tr>" for row in body
    )
    return "<table bordered striped>\n" + "\n".join(html_rows) + "\n</table>"


def try_render_table(
    lines: list[str],
    i: int,
    *,
    limit: int = 12000,
    cell_html: "Callable[[str], str] | None" = None,
) -> tuple[str, int] | None:
    """If a GitHub-style pipe table starts at ``lines[i]`` (a row immediately followed by a
    ``---|---`` delimiter), render the whole block to a native ``<table>`` and return
    ``(html, next_index)``; otherwise ``None``. Shared by both markdown engines; each passes its own
    ``cell_html`` inline renderer so cell content matches the surrounding formatting. ``limit`` is
    accepted for signature stability (the native table isn't length-padded)."""
    if not (
        _looks_like_table_row(lines[i])
        and i + 1 < len(lines)
        and _looks_like_table_separator(lines[i + 1])
    ):
        return None
    render_cell = cell_html or (lambda c: _inline_markdown_html(c))
    header = _table_cells(lines[i])
    data_rows: list[list[str]] = []
    j = i + 2
    while j < len(lines) and _looks_like_table_row(lines[j]):
        if _looks_like_table_separator(lines[j]):
            j += 1  # absorb a repeated interior `---|---` (LLMs use it to group sections)
            continue
        data_rows.append(_table_cells(lines[j]))
        j += 1
    if not any(cell.strip() for row in (header, *data_rows) for cell in row):
        return None  # all-empty header/body → leave as plain text, not an empty table
    return _render_table_html([header, *data_rows], cell_html=render_cell), j


def markdownish_to_html(value: Any, *, limit: int = 12000) -> str:
    """Render common agent Markdown into Telegram HTML.

    This intentionally covers the small Markdown subset agents emit most often:
    headings, bold/italic, inline code, fenced code blocks, bullets, and pipe
    tables. Unknown Markdown remains readable plain text instead of leaking raw
    HTML.
    """
    text = sanitize_text(value, limit).strip()
    if not text:
        return ""
    rendered: list[str] = []
    in_code = False
    code_lines: list[str] = []
    lines = text.splitlines()
    i = 0
    while i < len(lines):
        line = lines[i].rstrip()
        if line.strip().startswith("```"):
            if in_code:
                rendered.append(f"<pre>{html_escape(chr(10).join(code_lines), limit)}</pre>")
                code_lines = []
                in_code = False
            else:
                in_code = True
                code_lines = []
            i += 1
            continue
        if in_code:
            code_lines.append(line)
            i += 1
            continue
        # Pipe table (row + `---|---` delimiter): render the block as one aligned <pre> here rather
        # than leaking per-line `| a | b |` markup through the paragraph path.
        table = try_render_table(lines, i, limit=limit)
        if table is not None:
            rendered.append(table[0])
            i = table[1]
            continue
        stripped = line.strip()
        if not stripped:
            rendered.append("")
            i += 1
            continue
        heading = re.match(r"^(#{1,6})\s+(.+)$", stripped)
        if heading:
            inner = _inline_markdown_html(heading.group(2).strip())
            rendered.append(inner if inner.startswith("<b>") and inner.endswith("</b>") else f"<b>{inner}</b>")
            i += 1
            continue
        bullet = re.match(r"^[-*]\s+(.+)$", stripped)
        if bullet:
            rendered.append(f"• {_inline_markdown_html(bullet.group(1).strip())}")
            i += 1
            continue
        numbered = re.match(r"^(\d+[.)])\s+(.+)$", stripped)
        if numbered:
            rendered.append(f"{html_escape(numbered.group(1), 16)} {_inline_markdown_html(numbered.group(2).strip())}")
            i += 1
            continue
        rendered.append(_inline_markdown_html(line))
        i += 1
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


def _preferred_span_end(text: str, start: int, hard_end: int, limit: int) -> int:
    """Choose a stable Markdown-friendly boundary without changing coverage."""
    if hard_end >= len(text):
        return len(text)
    # Never split the two code points of CRLF when another in-boundary choice
    # exists.  (A limit of one code point necessarily splits it.)
    if hard_end > start + 1 and text[hard_end - 1 : hard_end + 1] == "\r\n":
        hard_end -= 1
    lower = start + max(1, limit // 2)
    if lower >= hard_end:
        return hard_end
    # Prefer whole Markdown blocks, then line/list/fence boundaries, then a
    # word boundary.  The separator belongs to the left span, so joining the
    # returned half-open spans reproduces the source exactly.
    for separator in ("\r\n\r\n", "\n\n", "\r\r"):
        found = text.rfind(separator, lower, hard_end)
        if found >= lower:
            return found + len(separator)
    line_ends = [
        found + len(separator)
        for separator in ("\r\n", "\n", "\r")
        if (found := text.rfind(separator, lower, hard_end)) >= lower
    ]
    if line_ends:
        return max(line_ends)
    word_ends = [
        found + 1
        for separator in (" ", "\t")
        if (found := text.rfind(separator, lower, hard_end)) >= lower
    ]
    return max(word_ends) if word_ends else hard_end


def split_text_spans(value: Any, *, limit: int = FINAL_CHUNK_SOURCE_CHARS) -> list[tuple[int, int]]:
    """Return deterministic, exact half-open code-point spans for ``value``."""
    text = canonical_text(value)
    size = int(limit)
    if size <= 0:
        raise ValueError("split limit must be positive")
    if not text:
        return []
    spans: list[tuple[int, int]] = []
    start = 0
    while start < len(text):
        hard_end = min(len(text), start + size)
        end = _preferred_span_end(text, start, hard_end, size)
        if end <= start:
            end = hard_end
        spans.append((start, end))
        start = end
    return spans


def split_text_chunks(value: Any, *, limit: int = FINAL_CHUNK_SOURCE_CHARS) -> list[str]:
    """Split losslessly; unlike the legacy splitter this never caps or strips."""
    text = canonical_text(value)
    return [text[start:end] for start, end in split_text_spans(text, limit=limit)]


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
    # tendwire's pending payload carries the content as `question` (+ optional structured choices);
    # prompt_text/text are legacy shapes. Without `question` the user just saw "Input needed."
    prompt = html_escape(
        item.get("question") or item.get("prompt_text") or item.get("text") or "Input needed.", 3000
    )
    lines = [f"<b>Input Needed</b> · {label}", "", prompt]
    choices = item.get("choices") if isinstance(item.get("choices"), list) else []
    numbered = []
    for i, choice in enumerate(choices, start=1):
        text = choice.get("label") or choice.get("text") if isinstance(choice, dict) else choice
        text = html_escape(str(text or ""), 200)
        if text:
            numbered.append(f"{i}. {text}")
    if numbered:
        lines.append("")
        lines.extend(numbered)
        lines.append("")
        lines.append("<i>Reply with a number or type your answer.</i>")
    return "\n".join(lines)


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
