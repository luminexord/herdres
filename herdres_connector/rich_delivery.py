"""Rich Telegram rendering/delivery extracted for source-mode Herdres.

The names and rendering model are intentionally aligned with the old full
Herdres rich path: turns render through ``render_turn_item_html`` and are sent
through ``send_feed_item``/``send_rich_message``. This module is Telegram-only;
it has no Herdr pane access.
"""

from __future__ import annotations

import html
import json
import os
import re
from typing import Any

from . import config
from .rendering import (
    html_to_plain,
    split_table_aware_spans,
    split_text_chunks,
    split_text_spans,
    table_continuation_header,
    telegram_html,
    try_render_table,
    worker_label,
)
from .safe import canonical_text, sanitize_text
from .telegram_delivery import (
    DELIVERY_FORMAT_STATE_UPDATE_KEY,
    MESSAGE_TEXT_LIMIT,
    SPLIT_TEXT_LIMIT,
    RateLimited,
    TelegramClient,
    TelegramError,
    classify_telegram_error,
)


MAX_REPLY_CHARS = int(os.getenv("HERDR_TELEGRAM_TOPICS_FINAL_REPLY_MAX_CHARS", "64000"))
TELEGRAM_RICH_TEXT_LIMIT = 32768
TELEGRAM_RICH_BLOCK_LIMIT = 500
MAX_RICH_HTML_CHARS = min(
    TELEGRAM_RICH_TEXT_LIMIT,
    int(os.getenv("HERDR_TELEGRAM_TOPICS_RICH_MAX_CHARS", str(TELEGRAM_RICH_TEXT_LIMIT))),
)
# A turn is sent as ONE rich message when its full rendered HTML fits this; it is
# only split into "Response i/N" parts when it cannot fit a single message.
# Default = MAX_RICH_HTML_CHARS so a response splits iff Telegram would reject it
# as too large -- not before. (The split itself stays lossless.)
RICH_SINGLE_MESSAGE_CHARS = int(os.getenv("HERDR_TELEGRAM_TOPICS_RICH_SINGLE_MESSAGE_CHARS", str(MAX_RICH_HTML_CHARS)))
# Source-text chunk size used when a response DOES need splitting. Bigger chunks
# => fewer parts. Multipart cards keep an operational margin below Telegram's
# nominal ceiling because provider acceptance does not guarantee every client
# will display a boundary-sized rich message.
RICH_SPLIT_CHUNK_CHARS = int(os.getenv("HERDR_TELEGRAM_TOPICS_RICH_SPLIT_CHUNK_CHARS", "24000"))
RICH_MULTIPART_MAX_BYTES = 28 * 1024
USER_PROMPT_MAX_CHARS = int(os.getenv("HERDR_TELEGRAM_TOPICS_USER_PROMPT_MAX_CHARS", "1200"))
WORKLOG_MAX_CHARS = int(os.getenv("HERDR_TELEGRAM_TOPICS_WORKLOG_MAX_CHARS", "1200"))
RICH_FALLBACK_MAX_CHARS = MESSAGE_TEXT_LIMIT
TURN_DELIVERY_PLAN_SCHEMA_VERSION = 1
TURN_DELIVERY_PLAIN_SOURCE_CHARS = min(SPLIT_TEXT_LIMIT, MESSAGE_TEXT_LIMIT)
# Eight owner-visible cards is the hard presentation ceiling for one logical
# turn.  It preserves the existing exact 20K-answer path while preventing the
# unbounded 11+ card plans that caused issue #228. Beyond this, emitting an
# incomplete prefix would be silent data loss; callers surface an explicit
# oversize outcome instead.
TURN_DELIVERY_MAX_PARTS = 8
PROMPT_PREVIEW_CHARS = 80
USER_PROMPT_LABEL = "You"
RESPONSE_LABEL = "Response"
WORKING_LABEL = "Working"
YOU_ICON = "💬"
WORKING_ICON = "⚙️"
RESPONSE_ICON = "✅"
RICH_RENDER_VERSION = 28
RICH_BAD_REQUEST_LIMIT = 3
RICH_STATE_UPDATE_KEY = "_herdres_rich_state_update"

FENCE_START_RE = re.compile(r"^\s*(`{3,}|~{3,})\s*([A-Za-z0-9_+-]{0,32})\s*$")
HRULE_RE = re.compile(r"^\s*([-*_])(?:[ \t]*\1){2,}[ \t]*$")
INLINE_CODE_RE = re.compile(r"`([^`\n]{1,300})`")
_INLINE_LINK_SCHEMES = ("http://", "https://", "mailto:", "tg://")


class PresentationContentError(RuntimeError):
    """Expected inability to fit specific valid content in one presentation."""


class PresentationOversizeError(PresentationContentError):
    """Exact content would require more owner-visible cards than permitted."""

    def __init__(self, part_count: int):
        self.part_count = int(part_count)
        super().__init__(
            "turn requires "
            f"{self.part_count} cards; hard maximum is "
            f"{TURN_DELIVERY_MAX_PARTS}"
        )


def _html_text(value: Any, max_chars: int = MAX_REPLY_CHARS) -> str:
    return html.escape(sanitize_text(str(value or ""), max_chars), quote=False)


def _replace_inline_links(
    text: str,
    link_spans: list[str],
) -> str:
    """Hold complete Markdown links in one forward, linear-time scan.

    Parentheses inside a destination must balance; an escaped parenthesis is
    literal URL content and does not affect that balance. Destinations with
    whitespace or angle brackets stay literal because converting only part of
    an ambiguous destination would create a confidently wrong link. Rejected
    candidates consume the characters already inspected, so no suffix is
    searched again.
    """

    rendered: list[str] = []
    index = 0

    while index < len(text):
        if text[index] != "[":
            rendered.append(text[index])
            index += 1
            continue
        label_start = index
        if label_start > 0 and text[label_start - 1] == "!":
            rendered.append("[")
            index += 1
            continue

        cursor = label_start + 1
        label_end = -1
        rejected_at = -1
        nested_candidate = False
        label_length = 0
        while cursor < len(text):
            char = text[cursor]
            if char == "[":
                rejected_at = cursor
                nested_candidate = True
                break
            if char == "\n":
                rejected_at = cursor
                break
            if char == "]":
                if cursor + 1 < len(text) and text[cursor + 1] == "(":
                    label_end = cursor
                else:
                    rejected_at = cursor
                break
            label_length += 1
            if label_length > 300:
                rejected_at = cursor
                break
            cursor += 1
        if label_end < 0:
            if rejected_at < 0:
                rendered.append(text[label_start:])
                break
            if nested_candidate:
                # The inner opener is a new candidate, not part of the
                # rejected outer label. Process it next without searching any
                # already-inspected suffix again; this preserves the existing
                # unmatched-bracket behavior in linear time.
                rendered.append(text[label_start:rejected_at])
                index = rejected_at
            else:
                rendered.append(text[label_start : rejected_at + 1])
                index = rejected_at + 1
            continue
        if label_length == 0:
            rendered.append(text[label_start : label_end + 2])
            index = label_end + 2
            continue

        cursor = label_end + 2
        depth = 0
        destination: list[str] = []
        destination_length = 0
        delimiter = -1
        invalid = False
        while cursor < len(text):
            char = text[cursor]
            if char == "\\" and cursor + 1 < len(text):
                escaped = text[cursor + 1]
                if escaped in {"(", ")", "\\"}:
                    destination_length += 1
                    if destination_length <= 2000:
                        destination.append(escaped)
                    else:
                        invalid = True
                    cursor += 2
                    continue
            if char.isspace():
                invalid = True
            if text.startswith("&lt;", cursor) or text.startswith(
                "&gt;", cursor
            ):
                invalid = True
            if char == "(":
                depth += 1
            elif char == ")":
                if depth == 0:
                    delimiter = cursor
                    break
                depth -= 1
            destination_length += 1
            if destination_length <= 2000:
                destination.append(char)
            else:
                invalid = True
            cursor += 1

        if delimiter < 0:
            rendered.append(text[label_start:])
            break
        href = html.unescape("".join(destination))
        if (
            invalid
            or depth
            or not href.lower().startswith(_INLINE_LINK_SCHEMES)
            or destination_length > 2000
        ):
            rendered.append(text[label_start : delimiter + 1])
            index = delimiter + 1
            continue

        label = text[label_start + 1 : label_end]
        safe_href = html.escape(href, quote=True)
        link_spans.append(f'<a href="{safe_href}">{label}</a>')
        rendered.append(f"\u0001{len(link_spans) - 1}\u0001")
        index = delimiter + 1
    return "".join(rendered)


def _rich_inline(value: Any, max_chars: int = MAX_REPLY_CHARS) -> str:
    text = _html_text(value, max_chars)
    code_spans: list[str] = []
    link_spans: list[str] = []

    def hold_code(match: re.Match[str]) -> str:
        code_spans.append(f"<code>{html.escape(match.group(1), quote=False)}</code>")
        return f"\u0000{len(code_spans) - 1}\u0000"

    text = INLINE_CODE_RE.sub(hold_code, text)

    text = _replace_inline_links(text, link_spans)
    text = re.sub(r"\*\*\*([^\n]+?)\*\*\*", r"<b><i>\1</i></b>", text)
    text = re.sub(r"\*\*([^\n]+?)\*\*", r"<b>\1</b>", text)
    text = re.sub(r"(?<![\w*])\*(?!\s)([^*\n]+?)(?<!\s)\*(?![\w*])", r"<i>\1</i>", text)
    text = re.sub(r"__([^\n]+?)__", r"<b>\1</b>", text)
    text = re.sub(r"~~([^\s~][^\n]*?)~~", r"<s>\1</s>", text)
    for index, code in enumerate(code_spans):
        text = text.replace(f"\u0000{index}\u0000", code)
    for index, link in enumerate(link_spans):
        text = text.replace(f"\u0001{index}\u0001", link)
    return text


def _rich_paragraph(value: Any) -> str:
    clean = _rich_inline(value, MAX_REPLY_CHARS).strip()
    return f"<p>{clean}</p>" if clean else ""


def _prompt_preview(value: Any) -> str:
    for line in str(value or "").splitlines():
        clean = line.strip()
        if clean:
            return sanitize_text(clean, PROMPT_PREVIEW_CHARS)
    return ""


def _prompt_should_collapse(value: Any, collapse_chars: int = 0) -> bool:
    try:
        threshold = int(collapse_chars or 0)
    except (TypeError, ValueError):
        threshold = 0
    text = str(value or "")
    return bool(threshold and len(text) > threshold)


def _rich_details_quote_html(
    summary: str,
    body_html: str,
    *,
    icon: str = "",
    summary_max_chars: int = 80,
    open_by_default: bool = True,
    quote: bool = False,
    preview: str = "",
    de_emphasize: bool = False,
) -> str:
    # Telegram rich messages do not support <small> (it is silently dropped) and
    # every block element carries a fixed native margin. So secondary sections
    # (prompt, worklog) are de-emphasized with <footer> instead of a <blockquote>
    # (which would stack a second margin and add a heavy left bar). A colored
    # emoji icon marks the section in the summary.
    body = str(body_html or "").strip()
    if not body:
        return ""
    label = _html_text(summary, summary_max_chars)
    open_attr = " open" if open_by_default else ""
    preview_text = str(preview or "").strip()
    preview_html = f" {_html_text(preview_text, PROMPT_PREVIEW_CHARS + 6)}" if preview_text else ""
    icon_html = f"{icon} " if icon else ""
    summary_html = f"{icon_html}<b>{label}</b>{preview_html}"
    if de_emphasize:
        body_content = f"<footer>{body}</footer>"
    elif quote:
        body_content = f"<blockquote>{body}</blockquote>"
    else:
        body_content = body
    return f"<details{open_attr}><summary>{summary_html}</summary>{body_content}</details>"


def render_user_prompt_quote_html(user_text: str, collapse_chars: int = 0) -> str:
    body = "<br>".join(
        _rich_inline(line, MAX_REPLY_CHARS)
        for line in sanitize_text(user_text, MAX_REPLY_CHARS).splitlines()
    )
    body = body.strip()
    if not body:
        return ""
    collapse = _prompt_should_collapse(user_text, collapse_chars)
    return _rich_details_quote_html(
        USER_PROMPT_LABEL,
        body,
        icon=YOU_ICON,
        summary_max_chars=20,
        open_by_default=not collapse,
        preview=_prompt_preview(user_text) if collapse else "",
        de_emphasize=True,
    )


_RICH_SPACIOUS_BLOCK_TAG_RE = r"pre|h[1-6]|ul|ol|blockquote|details|table"
_SPACIOUS_END = re.compile(rf"</(?:{_RICH_SPACIOUS_BLOCK_TAG_RE})>$")
_SPACIOUS_START = re.compile(rf"^<(?:{_RICH_SPACIOUS_BLOCK_TAG_RE})\b")


def _join_blocks(parts: list[str]) -> str:
    kept = [part for part in parts if str(part or "").strip()]
    if not kept:
        return ""
    result = kept[0]
    for part in kept[1:]:
        prev = result.rstrip()
        nxt = part.lstrip()
        if _SPACIOUS_END.search(prev) or _SPACIOUS_START.match(nxt):
            sep = ""
        elif prev.endswith(">"):
            sep = "<br>"
        else:
            sep = "<br><br>"
        result += sep + part
    return result


def _bullet_text(line: str) -> str | None:
    match = re.match(r"^\s*(?:[-*+]|\u2022)\s+(.+)$", line or "")
    return match.group(1).strip() if match else None


def _numbered_text(line: str) -> tuple[int, str] | None:
    match = re.match(r"^\s*(\d{1,2})[.)]\s+(.+)$", line or "")
    if not match:
        return None
    return int(match.group(1)), match.group(2).strip()


def _heading_title(line: str) -> str:
    clean = re.sub(r"^\s{0,3}#{1,6}\s+", "", str(line or "").strip())
    clean = clean.rstrip(":").rstrip(".").strip()
    clean = re.sub(r"`([^`\n]{1,300})`", r"\1", clean)
    clean = re.sub(r"\*\*([^\n]+?)\*\*", r"\1", clean)
    return re.sub(r"\s+", " ", clean)


def _is_heading(line: str, *, first_block: bool = False, previous_blank: bool = False) -> bool:
    clean = str(line or "").strip()
    if not clean or len(clean) > 120 or HRULE_RE.match(clean):
        return False
    if re.match(r"^#{1,6}\s+\S", clean):
        return True
    if clean.startswith(">") or _bullet_text(clean) or _numbered_text(clean) or FENCE_START_RE.match(clean):
        return False
    title = _heading_title(clean)
    words = title.split()
    if clean.endswith(":") and 1 <= len(words) <= 6:
        return True
    return bool((first_block or previous_blank) and 1 <= len(words) <= 5 and clean[:1].isupper() and not clean.endswith(("?", "!", ".")))


def _render_final_reply_blocks(lines: list[str], *, seen_heading: bool = False) -> str:
    parts: list[str] = []
    idx = 0
    previous_blank = True
    while idx < len(lines):
        line = str(lines[idx] or "").rstrip()
        stripped = line.strip()
        if not stripped:
            previous_blank = True
            idx += 1
            continue
        if HRULE_RE.match(stripped):
            previous_blank = True
            idx += 1
            continue
        fence = FENCE_START_RE.match(line)
        if fence:
            marker = fence.group(1)[0] * 3
            language = fence.group(2).strip()
            code_lines: list[str] = []
            idx += 1
            while idx < len(lines) and not str(lines[idx]).strip().startswith(marker):
                code_lines.append(str(lines[idx]).rstrip())
                idx += 1
            if idx < len(lines):
                idx += 1
            if language.lower() == "mermaid":
                parts.append("<blockquote>mermaid diagram - see full text outside Telegram</blockquote>")
            else:
                class_attr = f' class="language-{html.escape(language, quote=True)}"' if language else ""
                parts.append(
                    f"<pre><code{class_attr}>{_html_text(chr(10).join(code_lines), MAX_REPLY_CHARS)}</code></pre>"
                )
            previous_blank = False
            continue
        # Pipe table (row + `---|---` delimiter): render as a native <table> (the rich path turns it
        # into a PageBlockTable). Cells use _rich_inline so bold/code/links inside cells render. Must
        # precede the paragraph fallthrough, which would otherwise emit raw `| a | b |` / `|---|`.
        table = try_render_table(
            lines, idx, cell_html=lambda c: _rich_inline(c, MAX_REPLY_CHARS)
        )
        if table is not None:
            parts.append(table[0])
            idx = table[1]
            previous_blank = False
            continue
        if _is_heading(line, first_block=not seen_heading, previous_blank=previous_blank):
            title = _heading_title(line)
            # First section heading is prominent (<h3>); later ones drop to <h4>
            # so a multi-section response stays compact -- every <h3> adds a big
            # native margin in Telegram's rich renderer.
            tag = "h3" if not seen_heading else "h4"
            parts.append(f"<{tag}>{_html_text(title, MAX_REPLY_CHARS)}</{tag}>")
            seen_heading = True
            previous_blank = False
            idx += 1
            continue
        bullet = _bullet_text(line)
        if bullet:
            items: list[str] = []
            while idx < len(lines):
                parsed = _bullet_text(str(lines[idx] or ""))
                if parsed is None:
                    break
                items.append(parsed)
                idx += 1
            parts.append(
                "<ul>\n"
                + "\n".join(
                    f"<li>{_rich_inline(item, MAX_REPLY_CHARS)}</li>"
                    for item in items
                )
                + "\n</ul>"
            )
            previous_blank = False
            continue
        numbered = _numbered_text(line)
        if numbered:
            items: list[str] = []
            while idx < len(lines):
                parsed_numbered = _numbered_text(str(lines[idx] or ""))
                if parsed_numbered is None:
                    break
                _number, text = parsed_numbered
                items.append(text)
                idx += 1
            parts.append(
                "<ol>\n"
                + "\n".join(
                    f"<li>{_rich_inline(item, MAX_REPLY_CHARS)}</li>"
                    for item in items
                )
                + "\n</ol>"
            )
            previous_blank = False
            continue
        if stripped.startswith(">"):
            quote_lines: list[str] = []
            while idx < len(lines) and str(lines[idx] or "").strip().startswith(">"):
                quote_lines.append(re.sub(r"^\s*>\s?", "", str(lines[idx]).rstrip()))
                idx += 1
            parts.append(
                "<blockquote>"
                + "<br>".join(
                    _rich_inline(quote, MAX_REPLY_CHARS) for quote in quote_lines
                )
                + "</blockquote>"
            )
            previous_blank = False
            continue
        paragraph = [stripped]
        idx += 1
        while idx < len(lines):
            candidate = str(lines[idx] or "").rstrip()
            if not candidate.strip():
                break
            if (
                FENCE_START_RE.match(candidate)
                or _bullet_text(candidate)
                or _numbered_text(candidate)
                or candidate.strip().startswith(">")
                or _is_heading(candidate, previous_blank=False)
            ):
                break
            paragraph.append(candidate.strip())
            idx += 1
        parts.append(_rich_paragraph(" ".join(paragraph)))
        previous_blank = False
    return _join_blocks(parts)


def render_final_reply_html(value: str, *, seen_heading: bool = False) -> str:
    clean = sanitize_text(str(value or ""), MAX_REPLY_CHARS).strip()
    if not clean:
        return ""
    return _render_final_reply_blocks(clean.splitlines(), seen_heading=seen_heading)


def render_assistant_response_html(
    assistant_final: str,
    *,
    label: str = RESPONSE_LABEL,
    open_by_default: bool = True,
) -> str:
    # The owner wants the Response itself to be expandable: the newest final is
    # open, and the historical-final sweep re-renders the same details block
    # closed once a newer final supersedes it. Keep the answer in this one block
    # so changing presentation never duplicates or rewrites its content.
    clean = str(assistant_final or "").strip()
    if not clean:
        return ""
    body_html = render_final_reply_html(clean, seen_heading=True) or _rich_paragraph(clean)
    return _rich_details_quote_html(
        label or RESPONSE_LABEL,
        body_html,
        icon=RESPONSE_ICON,
        open_by_default=open_by_default,
    )


def render_source_v2_working_update_html(worklog_text: str, *, label: str = WORKING_LABEL) -> str:
    clean = str(worklog_text or "").strip()
    if not clean:
        return ""
    # Match the "You" section styling exactly: flat inline lines joined by a
    # single <br>. Rendering the worklog as rich <p>/<ul> blocks would give each
    # line a native block margin, and _join_blocks adds a <br> between <p>s,
    # stacking into the big gaps between paragraphs. Flat-inline keeps the
    # worklog as small and gap-free as the prompt.
    body = "<br>".join(_rich_inline(line, 900) for line in sanitize_text(clean, WORKLOG_MAX_CHARS).splitlines())
    body = body.strip()
    if not body:
        return ""
    return _rich_details_quote_html(
        label or WORKING_LABEL,
        body,
        icon=WORKING_ICON,
        open_by_default=False,
        preview=_prompt_preview(clean),
        de_emphasize=True,
    )


def render_turn_item_html(item: dict[str, Any]) -> str:
    # Layout (source mode): the Response is an open details block until a newer
    # final supersedes it, while the user prompt (and any in-progress worklog)
    # are de-emphasized collapsible sections below it. No redundant top worker
    # title -- the Telegram topic already names the worker.
    user_text = str(item.get("user_text") or "").strip()
    worklog_text = str(item.get("worklog_text") or item.get("assistant_stream_text") or "").strip()
    worklog_label = str(item.get("worklog_label") or WORKING_LABEL).strip() or WORKING_LABEL
    assistant_final = str(item.get("assistant_final_text") or "").strip()
    parts: list[str] = []
    response_html = render_assistant_response_html(
        assistant_final,
        label=str(item.get("response_label") or RESPONSE_LABEL),
        open_by_default=not bool(item.get("collapse_response")),
    )
    if response_html:
        parts.append(response_html)
    if user_text:
        parts.append(render_user_prompt_quote_html(user_text, int(item.get("prompt_collapse_chars") or 0)))
    if worklog_text:
        parts.append(render_source_v2_working_update_html(worklog_text, label=worklog_label))
    return _join_blocks(parts).strip()


def render_feed_item_html(item: dict[str, Any], *, live: bool = False) -> str:
    kind = str(item.get("kind") or "update").lower()
    if kind == "turn":
        return render_turn_item_html(item)
    title = str(item.get("title") or item.get("kind") or "Update").strip()
    summary = str(item.get("summary") or item.get("text") or "").strip()
    if live:
        title = f"Latest {title}"
    parts = [f"<h3>{_html_text(title, 100)}</h3>"]
    if summary:
        parts.append(render_final_reply_html(summary) or _rich_paragraph(summary))
    return _join_blocks(parts)


def _turn_response_text(item: dict[str, Any]) -> str:
    return canonical_text(item.get("assistant_final_text"), field="assistant_final_text")


def _canonical_turn_fields(item: dict[str, Any]) -> dict[str, str]:
    return {
        "user_text": canonical_text(item.get("user_text"), field="user_text"),
        "assistant_final_text": _turn_response_text(item),
    }


def _full_turn_source_is_render_safe(fields: dict[str, str]) -> bool:
    """Avoid the rich renderer's intentional per-block preview slices."""
    user_text = fields["user_text"]
    final_text = fields["assistant_final_text"]
    if len(user_text) > USER_PROMPT_MAX_CHARS:
        return False
    if any(len(line) > 900 for line in user_text.splitlines()):
        return False
    if len(final_text) > 1600 or any(len(line) > 900 for line in final_text.splitlines()):
        return False
    # Heading titles and native table cells have tighter presentation limits.
    if any(
        (re.match(r"^\s{0,3}#{1,6}\s+\S", line) and len(_heading_title(line)) > 100)
        or ("|" in line and len(line) > 160)
        for line in final_text.splitlines()
    ):
        return False
    return True


def _planned_parts_for_limits(
    fields: dict[str, str],
    *,
    user_limit: int,
    final_limit: int,
) -> list[dict[str, Any]]:
    field_spans = {
        "user_text": split_text_spans(fields["user_text"], limit=user_limit),
        "assistant_final_text": split_table_aware_spans(
            fields["assistant_final_text"], limit=final_limit
        ),
    }
    part_count = max((len(spans) for spans in field_spans.values()), default=0)
    parts: list[dict[str, Any]] = []
    for ordinal in range(part_count):
        spans: list[dict[str, Any]] = []
        # Stable schema order matches Tendwire's canonical turn field order.
        for field in ("user_text", "assistant_final_text"):
            if ordinal < len(field_spans[field]):
                start, end = field_spans[field][ordinal]
                spans.append({"field": field, "start_char": start, "end_char": end})
        parts.append(
            {
                "schema_version": TURN_DELIVERY_PLAN_SCHEMA_VERSION,
                "ordinal": ordinal,
                "part_count": part_count,
                "spans": spans,
                # Local-only metadata: Tendwire persists the canonical spans,
                # while delivery recomputes this deterministic limit before
                # materializing them.
                "_source_limits": {
                    "user_text": user_limit,
                    "assistant_final_text": final_limit,
                },
            }
        )
    return parts


def _materialize_turn_delivery_part(item: dict[str, Any], part: dict[str, Any]) -> dict[str, Any]:
    if type(part.get("schema_version")) is not int or part["schema_version"] != TURN_DELIVERY_PLAN_SCHEMA_VERSION:
        raise ValueError("unsupported turn delivery plan schema")
    ordinal = part.get("ordinal")
    part_count = part.get("part_count")
    if type(ordinal) is not int or type(part_count) is not int or ordinal < 0 or part_count <= ordinal:
        raise ValueError("invalid turn delivery part ordinal")

    fields = _canonical_turn_fields(item)
    fragments = {"user_text": [], "assistant_final_text": []}
    spans = part.get("spans")
    if not isinstance(spans, list) or not spans:
        raise ValueError("turn delivery part spans must be a non-empty list")
    source_limits = part.get("_source_limits")
    if source_limits is None:
        source_limits = {}
    if not isinstance(source_limits, dict):
        raise ValueError("turn delivery source limits must be an object")
    seen_fields: set[str] = set()
    for span in spans:
        if not isinstance(span, dict):
            raise ValueError("turn delivery span must be an object")
        field = str(span.get("field") or "")
        if field not in fragments:
            raise ValueError(f"unsupported turn delivery field: {field}")
        if field in seen_fields:
            raise ValueError(f"duplicate turn delivery field span: {field}")
        seen_fields.add(field)
        start = span.get("start_char")
        end = span.get("end_char")
        if type(start) is not int or type(end) is not int:
            raise ValueError("turn delivery span coordinates must be integers")
        source = fields[field]
        if start < 0 or end <= start or end > len(source):
            raise ValueError("turn delivery span is outside canonical content")
        fragment = source[start:end]
        if field == "assistant_final_text":
            planning_limit = source_limits.get(
                field, TURN_DELIVERY_PLAIN_SOURCE_CHARS
            )
            if (
                isinstance(planning_limit, bool)
                or not isinstance(planning_limit, int)
                or planning_limit <= 0
            ):
                raise ValueError(
                    "turn delivery source limit must be a positive integer"
                )
            fragment = (
                table_continuation_header(
                    source,
                    start,
                    planning_limit=planning_limit,
                )
                + fragment
            )
        fragments[field].append(fragment)
    materialized = dict(item)
    materialized["user_text"] = "".join(fragments["user_text"])
    materialized["assistant_final_text"] = "".join(fragments["assistant_final_text"])
    if part_count > 1 and materialized["assistant_final_text"]:
        materialized["response_label"] = f"{RESPONSE_LABEL} {ordinal + 1}/{part_count}"
    if ordinal > 0:
        materialized["worklog_text"] = ""
        materialized["assistant_stream_text"] = ""
    return materialized


def render_turn_delivery_part_html(item: dict[str, Any], part: dict[str, Any]) -> str:
    """Render exactly one planned part; this surface never plans or sends siblings."""
    return render_turn_item_html(_materialize_turn_delivery_part(item, part))


def render_turn_delivery_part_plain_text(item: dict[str, Any], part: dict[str, Any]) -> str:
    return html_to_plain(render_turn_delivery_part_html(item, part), limit=MAX_REPLY_CHARS)


_RICH_BLOCK_START_RE = re.compile(
    r"<(?:p|h[1-6]|pre|ul|ol|li|tr|blockquote|details|footer|table)\b"
)


def _turn_delivery_part_is_bounded(
    item: dict[str, Any],
    part: dict[str, Any],
    *,
    rich_transport: bool,
    multipart: bool = False,
) -> bool:
    rendered = render_turn_delivery_part_html(item, part)
    plain = html_to_plain(rendered, limit=MAX_REPLY_CHARS)
    if len(plain) > RICH_FALLBACK_MAX_CHARS:
        return False
    if not rich_transport:
        return True
    return (
        len(rendered) <= min(RICH_SINGLE_MESSAGE_CHARS, MAX_RICH_HTML_CHARS)
        and (
            not multipart
            or len(rendered.encode("utf-8")) <= RICH_MULTIPART_MAX_BYTES
        )
        and len(plain) <= TELEGRAM_RICH_TEXT_LIMIT
        and len(_RICH_BLOCK_START_RE.findall(rendered)) <= TELEGRAM_RICH_BLOCK_LIMIT
    )


def prepare_turn_delivery_parts(
    item: dict[str, Any],
    *,
    live: bool = False,
    rich_transport: bool = False,
) -> list[dict[str, Any]]:
    """Plan deterministic exact spans for bounded one-operation Telegram parts.

    Production plans to the stricter plain bound even though rich is attempted
    first. That makes a provider rejection degradable without changing the
    durable part coordinates or inventing a second lifecycle. This canonical
    planner is deliberately lossless and uncapped; the logical-delivery caller
    applies the owner-visible physical-card ceiling and records its outcome.
    """
    if live or str(item.get("kind") or "").lower() != "turn":
        return []
    fields = _canonical_turn_fields(item)
    if not fields["user_text"] and not fields["assistant_final_text"]:
        return []

    # Preserve the established one-message rendering for ordinary short turns.
    full_spans = [
        {"field": field, "start_char": 0, "end_char": len(text)}
        for field, text in fields.items()
        if text
    ]
    full_part = {
        "schema_version": TURN_DELIVERY_PLAN_SCHEMA_VERSION,
        "ordinal": 0,
        "part_count": 1,
        "spans": full_spans,
    }
    if _full_turn_source_is_render_safe(fields) and _turn_delivery_part_is_bounded(
        item, full_part, rich_transport=rich_transport
    ):
        return [full_part]

    # Every deterministic part must fit the plain fallback as well as the rich
    # primary transport.
    source_limit = TURN_DELIVERY_PLAIN_SOURCE_CHARS
    user_limit = max(1, source_limit)
    final_limit = max(1, source_limit)
    while True:
        parts = _planned_parts_for_limits(
            fields,
            user_limit=user_limit,
            final_limit=final_limit,
        )
        if all(
            _turn_delivery_part_is_bounded(
                item,
                part,
                rich_transport=rich_transport,
                multipart=True,
            )
            for part in parts
        ):
            return parts
        if user_limit == 1 and final_limit == 1:
            raise PresentationContentError(
                "one code point cannot fit Telegram presentation limits"
            )
        user_limit = max(1, user_limit // 2)
        final_limit = max(1, final_limit // 2)


def _turn_item_delivery_parts(item: dict[str, Any], *, live: bool = False) -> list[dict[str, Any]]:
    plans = prepare_turn_delivery_parts(item, live=live)
    if not plans:
        return [item]
    return [_materialize_turn_delivery_part(item, part) for part in plans]


def render_feed_item_delivery_html_parts(item: dict[str, Any], *, live: bool = False) -> list[str]:
    plans = prepare_turn_delivery_parts(item, live=live)
    if not plans:
        return [render_feed_item_html(item, live=live)]
    return [render_turn_delivery_part_html(item, part) for part in plans]


def feed_item_requires_send_split(item: dict[str, Any], *, live: bool = False) -> bool:
    return len(render_feed_item_delivery_html_parts(item, live=live)) > 1


def item_plain_text(item: dict[str, Any]) -> str:
    if str(item.get("kind") or "").lower() == "turn":
        parts: list[str] = []
        user_text = str(item.get("user_text") or "").strip()
        final_text = str(item.get("assistant_final_text") or "").strip()
        if user_text:
            parts.extend([USER_PROMPT_LABEL, user_text, ""])
        if final_text:
            parts.append(final_text)
        return sanitize_text("\n".join(parts).strip(), MAX_REPLY_CHARS)
    title = str(item.get("title") or item.get("kind") or "Update").strip()
    summary = str(item.get("summary") or item.get("text") or "").strip()
    return sanitize_text("\n".join(part for part in (title, summary) if part).strip(), MAX_REPLY_CHARS)


def rich_telegram_state(telegram: dict[str, Any] | None) -> dict[str, Any]:
    if not isinstance(telegram, dict):
        return {}
    current = telegram.get("rich_messages")
    rich = dict(current) if isinstance(current, dict) else {}
    rich.setdefault("supported", "unknown")
    return rich


def _rich_disabled_reason_is_capability(reason: str) -> bool:
    text = str(reason or "").lower()
    return any(marker in text for marker in ("method not found", "no such method", "does not exist", "http 404"))


def rich_enabled(telegram: dict[str, Any] | None) -> bool:
    if not config.rich_messages_enabled():
        return False
    rich = rich_telegram_state(telegram)
    if str(rich.get("supported") or "unknown") != "no":
        return True
    if _rich_disabled_reason_is_capability(str(rich.get("disabled_reason") or "")):
        return False
    disabled_version_text = str(rich.get("disabled_render_version") or "").strip()
    disabled_version = int(disabled_version_text) if disabled_version_text.isdigit() else 0
    if disabled_version == RICH_RENDER_VERSION:
        return False
    return True


def rich_message_send_enabled(telegram: dict[str, Any] | None) -> bool:
    return isinstance(telegram, dict) and rich_enabled(telegram)


def _with_rich_state_update(
    result: dict[str, Any],
    transition: str,
    *,
    reason: str = "",
) -> dict[str, Any]:
    """Attach a pure provider fact for guarded post-reload application."""

    result[RICH_STATE_UPDATE_KEY] = {
        "transition": transition,
        "reason": sanitize_text(reason, 300),
    }
    return result


def _client_for_token(client: TelegramClient, api_token: str | None) -> TelegramClient:
    token = str(api_token or "").strip()
    return client.with_token(token) if token else client


def _telegram_message_id(response: dict[str, Any]) -> str:
    result = (
        response.get("result")
        if isinstance(response.get("result"), dict)
        else {}
    )
    message_id = str(result.get("message_id") or "").strip()
    return "" if message_id == "0" else message_id


def _physical_writes(result: dict[str, Any], *, default: int = 1) -> int:
    raw = result.get("physical_writes")
    if isinstance(raw, int) and not isinstance(raw, bool) and raw >= 0:
        return raw
    return default


def _partial_terminal_outcome(result: dict[str, Any]) -> str:
    """Classify the missing suffix without changing replay authority."""

    kind = str(result.get("kind") or "").strip()
    if kind in {"transient", "delivery_unknown"}:
        return "delivery_unknown"
    if kind in {
        "bad_request",
        "capability",
        "empty_plain_text",
        "operation_budget_exhausted",
        "permanent",
        "presentation_transport_changed",
    }:
        return "not_delivered"
    return (
        "delivery_unknown"
        if classify_telegram_error(result.get("error")) == "transient"
        else "not_delivered"
    )


def _with_delivery_identity(result: dict[str, Any]) -> dict[str, Any]:
    ids = split_legacy_message_ids(result)
    if ids:
        result.setdefault("message_ids", ids)
        result.setdefault("canonical_message_id", ids[0])
    return result


def _rich_failure(
    kind: str,
    error: Exception,
    *,
    physical_writes: int = 1,
) -> dict[str, Any]:
    return {
        "ok": False,
        "format": "rich",
        "kind": kind,
        "error": sanitize_text(str(error), 300),
        "physical_writes": physical_writes,
    }


def _readable_telegram_html(html_text: str, fallback_text: str) -> str:
    """Preserve Telegram formatting while flattening unsupported structure.

    ``telegram_html`` is an allowlist: supported sendMessage markup survives,
    while tables, headings, lists and unknown tags become readable text before
    Telegram sees them.  A caller fallback remains plain content and therefore
    must be escaped before it enters the HTML-first transport ladder.
    """

    rendered = telegram_html(html_text, limit=MAX_REPLY_CHARS).strip()
    fallback = html.escape(
        sanitize_text(str(fallback_text or ""), MAX_REPLY_CHARS).strip(),
        quote=False,
    )
    return rendered or fallback


def _fallback_send(
    client: TelegramClient,
    chat_id: str,
    fallback: str,
    *,
    thread_id: str | int | None,
    notify: bool,
    reply_to_message_id: str | int | None,
    require_single_operation: bool = False,
    max_physical_writes: int | None = None,
) -> dict[str, Any]:
    if require_single_operation:
        plain = html_to_plain(fallback, limit=MAX_REPLY_CHARS)
        if len(plain) > RICH_FALLBACK_MAX_CHARS:
            return {
                "ok": False,
                "format": "plain",
                "kind": "presentation_transport_changed",
                "error": "presentation transport changed",
            }
    return _with_delivery_identity(client.send_message(
        chat_id,
        fallback,
        thread_id=thread_id,
        notify=notify,
        reply_to_message_id=reply_to_message_id,
        max_physical_writes=max_physical_writes,
    ))


def _fallback_edit(
    client: TelegramClient,
    chat_id: str,
    message_id: str | int,
    fallback: str,
    *,
    max_physical_writes: int | None,
) -> dict[str, Any]:
    # Real TelegramClient instances own the variant ladder and consume the
    # exact allowance. Lightweight provider fakes/adapters implement one
    # physical edit directly, so preserve their established three-argument
    # protocol rather than requiring a test-only keyword.
    if (
        getattr(type(client), "edit_message", None)
        is TelegramClient.edit_message
    ):
        return client.edit_message(
            chat_id,
            message_id,
            fallback,
            max_physical_writes=max_physical_writes,
        )
    return client.edit_message(chat_id, message_id, fallback)


def send_rich_message(
    client: TelegramClient,
    chat_id: str,
    html_text: str,
    *,
    telegram: dict[str, Any] | None,
    fallback_text: str = "",
    thread_id: str | int | None = None,
    notify: bool = False,
    reply_to_message_id: str | int | None = None,
    api_token: str | None = None,
    require_single_operation: bool = False,
    max_physical_writes: int | None = None,
) -> dict[str, Any]:
    target = _client_for_token(client, api_token)
    write_allowance = (
        max(0, int(max_physical_writes))
        if max_physical_writes is not None
        else None
    )
    if write_allowance == 0:
        return _rich_failure(
            "operation_budget_exhausted",
            RuntimeError("Telegram physical-write budget exhausted"),
            physical_writes=0,
        )
    fallback = _readable_telegram_html(html_text, fallback_text)
    if not fallback:
        return {
            "ok": False,
            "format": "plain",
            "kind": "empty_plain_text",
            "error": "readable Telegram text is empty",
        }

    if not rich_message_send_enabled(telegram):
        return _fallback_send(
            target,
            chat_id,
            fallback,
            thread_id=thread_id,
            notify=notify,
            reply_to_message_id=reply_to_message_id,
            require_single_operation=require_single_operation,
            max_physical_writes=write_allowance,
        )
    if len(html_text) > MAX_RICH_HTML_CHARS:
        result = _fallback_send(
            target,
            chat_id,
            fallback,
            thread_id=thread_id,
            notify=notify,
            reply_to_message_id=reply_to_message_id,
            require_single_operation=require_single_operation,
            max_physical_writes=write_allowance,
        )
        result["fallback_reason"] = "rich_too_large"
        return result

    payload: dict[str, Any] = {
        "chat_id": chat_id,
        "disable_notification": "false" if notify else "true",
        "rich_message": json.dumps(
            {
                "html": sanitize_text(html_text, MAX_RICH_HTML_CHARS),
                "skip_entity_detection": True,
            },
            separators=(",", ":"),
            ensure_ascii=False,
        ),
    }
    if thread_id:
        payload["message_thread_id"] = str(thread_id)
    if reply_to_message_id:
        payload["reply_parameters"] = json.dumps(
            {"message_id": int(reply_to_message_id)},
            separators=(",", ":"),
        )
    try:
        response = target.api("sendRichMessage", payload)
    except RateLimited:
        raise
    except TelegramError as exc:
        kind = classify_telegram_error(exc)
        if kind == "transient" or (api_token and kind == "bot_access"):
            return _rich_failure(kind, exc)
        state_transition = (
            "disabled" if kind == "capability" else "bad_request"
            if kind == "bad_request" else ""
        )
        remaining_writes = (
            None
            if write_allowance is None
            else max(0, write_allowance - 1)
        )
        if remaining_writes == 0:
            result = _rich_failure(
                "operation_budget_exhausted",
                RuntimeError(
                    "rich delivery was rejected but the plain fallback "
                    "would exceed the physical-write budget"
                ),
            )
            if state_transition:
                _with_rich_state_update(
                    result, state_transition, reason=str(exc)
                )
            return result
        fallback_result = _fallback_send(
            target,
            chat_id,
            fallback,
            thread_id=thread_id,
            notify=notify,
            reply_to_message_id=reply_to_message_id,
            require_single_operation=require_single_operation,
            max_physical_writes=remaining_writes,
        )
        fallback_result["physical_writes"] = (
            1 + _physical_writes(fallback_result)
        )
        fallback_result["fallback_reason"] = kind
        if state_transition:
            _with_rich_state_update(
                fallback_result, state_transition, reason=str(exc)
            )
        return fallback_result
    message_id = _telegram_message_id(response)
    result = _with_delivery_identity(
        {
            "ok": bool(message_id),
            "format": "rich",
            "message_id": message_id,
            "physical_writes": 1,
        }
    )
    if not message_id:
        result.update(
            {
                "kind": "delivery_unknown",
                "error": "sendRichMessage returned no message id",
            }
        )
        return result
    return _with_rich_state_update(result, "supported")


def edit_rich_message(
    client: TelegramClient,
    chat_id: str,
    message_id: str | int,
    html_text: str,
    *,
    telegram: dict[str, Any] | None,
    fallback_text: str = "",
    api_token: str | None = None,
    require_single_operation: bool = False,
    preserve_plain_html: bool = False,
    max_physical_writes: int | None = None,
) -> dict[str, Any]:
    target = _client_for_token(client, api_token)
    write_allowance = (
        max(0, int(max_physical_writes))
        if max_physical_writes is not None
        else None
    )
    if write_allowance == 0:
        return _rich_failure(
            "operation_budget_exhausted",
            RuntimeError("Telegram physical-write budget exhausted"),
            physical_writes=0,
        )
    # A rich-only presentation (currently the closed Response details block)
    # degrades through the normal readable Telegram allowlist. The fallback is
    # intentionally expanded: sendMessage cannot preserve rich-card details.
    fallback = (
        telegram_html(str(html_text or ""), limit=MAX_REPLY_CHARS).strip()
        if preserve_plain_html
        else _readable_telegram_html(html_text, fallback_text)
    )
    if not fallback:
        return {
            "ok": False,
            "format": "plain",
            "kind": "empty_plain_text",
            "error": "readable Telegram text is empty",
        }
    if require_single_operation and len(html_to_plain(fallback, limit=MAX_REPLY_CHARS)) > RICH_FALLBACK_MAX_CHARS:
        return {
            "ok": False,
            "format": "plain",
            "kind": "presentation_transport_changed",
            "error": "presentation transport changed",
        }
    if not rich_message_send_enabled(telegram) or len(html_text) > MAX_RICH_HTML_CHARS:
        result = _fallback_edit(
            target,
            chat_id,
            message_id,
            fallback,
            max_physical_writes=write_allowance,
        )
        if len(html_text) > MAX_RICH_HTML_CHARS:
            result["fallback_reason"] = "rich_too_large"
    else:
        payload = {
            "chat_id": chat_id,
            "message_id": str(message_id),
            "rich_message": json.dumps(
                {
                    "html": sanitize_text(
                        html_text, MAX_RICH_HTML_CHARS
                    ),
                    "skip_entity_detection": True,
                },
                separators=(",", ":"),
                ensure_ascii=False,
            ),
        }
        try:
            response = target.api("editMessageText", payload)
        except RateLimited:
            raise
        except TelegramError as exc:
            kind = classify_telegram_error(exc)
            if kind == "transient":
                return _rich_failure(kind, exc)
            if kind == "not_modified":
                result = _with_rich_state_update(
                    {
                        "ok": True,
                        "format": "rich",
                        "kind": kind,
                        "message_id": str(message_id),
                        "physical_writes": 1,
                    },
                    "supported",
                )
            elif kind in {"not_found", "topic_not_found"}:
                return {
                    "ok": False,
                    "format": "rich",
                    "kind": kind,
                    "not_found": kind == "not_found",
                    "topic_missing": kind == "topic_not_found",
                    "error": str(exc),
                    "physical_writes": 1,
                }
            else:
                transition = (
                    "disabled" if kind == "capability" else "bad_request"
                    if kind == "bad_request" else ""
                )
                remaining_writes = (
                    None
                    if write_allowance is None
                    else max(0, write_allowance - 1)
                )
                if remaining_writes == 0:
                    result = _rich_failure(
                        "operation_budget_exhausted",
                        RuntimeError(
                            "rich edit was rejected but the plain fallback "
                            "would exceed the physical-write budget"
                        ),
                    )
                    if transition:
                        _with_rich_state_update(
                            result, transition, reason=str(exc)
                        )
                    return result
                result = _fallback_edit(
                    target,
                    chat_id,
                    message_id,
                    fallback,
                    max_physical_writes=remaining_writes,
                )
                result["physical_writes"] = (
                    1 + _physical_writes(result)
                )
                result["fallback_reason"] = kind
                if transition:
                    _with_rich_state_update(
                        result, transition, reason=str(exc)
                    )
        else:
            result = _with_rich_state_update(
                {
                    "ok": True,
                    "format": "rich",
                    "kind": "edited",
                    "message_id": _telegram_message_id(response)
                    or str(message_id),
                    "physical_writes": 1,
                },
                "supported",
            )
    if preserve_plain_html:
        # Only sendRichMessage can apply the Response details presentation.
        # Any HTML/plain fallback is readable and expanded, so it must never
        # authorize binding["folded"].
        result["collapse_applied"] = bool(
            result.get("ok")
            and str(result.get("format") or "") == "rich"
        )
    return result


def send_turn_delivery_part(
    client: TelegramClient,
    chat_id: str,
    item: dict[str, Any],
    part: dict[str, Any],
    *,
    telegram: dict[str, Any] | None,
    thread_id: str | int | None,
    notify: bool = False,
    reply_to_message_id: str | int | None = None,
    api_token: str | None = None,
    max_physical_writes: int | None = None,
) -> dict[str, Any]:
    """Execute one planned upsert as at most one Telegram message."""
    if not _turn_delivery_part_is_bounded(
        item,
        part,
        rich_transport=False,
    ):
        raise ValueError("turn delivery part exceeds Telegram presentation limits")
    html_text = render_turn_delivery_part_html(item, part)
    return send_rich_message(
        client,
        chat_id,
        html_text,
        telegram=telegram,
        fallback_text=html_to_plain(html_text, limit=RICH_FALLBACK_MAX_CHARS),
        thread_id=thread_id,
        notify=notify,
        reply_to_message_id=reply_to_message_id,
        api_token=api_token,
        require_single_operation=True,
        max_physical_writes=max_physical_writes,
    )


def edit_turn_delivery_part(
    client: TelegramClient,
    chat_id: str,
    message_id: str | int,
    item: dict[str, Any],
    part: dict[str, Any],
    *,
    telegram: dict[str, Any] | None,
    api_token: str | None = None,
    max_physical_writes: int | None = None,
) -> dict[str, Any]:
    """Execute one planned edit without consulting or rendering sibling parts."""
    if not _turn_delivery_part_is_bounded(
        item,
        part,
        rich_transport=False,
    ):
        raise ValueError("turn delivery part exceeds Telegram presentation limits")
    html_text = render_turn_delivery_part_html(item, part)
    return edit_rich_message(
        client,
        chat_id,
        message_id,
        html_text,
        telegram=telegram,
        fallback_text=html_to_plain(html_text, limit=RICH_FALLBACK_MAX_CHARS),
        api_token=api_token,
        require_single_operation=True,
        max_physical_writes=max_physical_writes,
        preserve_plain_html=bool(item.get("collapse_response")),
    )


def send_feed_item(
    client: TelegramClient,
    chat_id: str,
    item: dict[str, Any],
    *,
    telegram: dict[str, Any] | None,
    thread_id: str | int | None,
    notify: bool = False,
    reply_to_message_id: str | int | None = None,
    live: bool = False,
    api_token: str | None = None,
    start_part_index: int = 0,
    max_physical_writes: int | None = None,
) -> dict[str, Any]:
    remaining_writes = (
        max(0, int(max_physical_writes))
        if max_physical_writes is not None
        else None
    )
    html_parts = render_feed_item_delivery_html_parts(item, live=live)
    if (
        isinstance(start_part_index, bool)
        or not isinstance(start_part_index, int)
        or start_part_index < 0
        or start_part_index >= len(html_parts)
    ):
        raise ValueError("invalid feed-item start part")
    html_parts = html_parts[start_part_index:]
    if len(html_parts) <= 1:
        result = send_rich_message(
            client,
            chat_id,
            html_parts[0] if html_parts else render_feed_item_html(item, live=live),
            telegram=telegram,
            fallback_text=item_plain_text(item),
            thread_id=thread_id,
            notify=notify,
            reply_to_message_id=reply_to_message_id,
            api_token=api_token,
            max_physical_writes=remaining_writes,
        )
        if start_part_index and not result.get("ok"):
            result["failed_part_index"] = start_part_index
            result["terminal_outcome"] = _partial_terminal_outcome(
                result
            )
        return result
    message_ids: list[str] = []
    formats: list[str] = []
    physical_writes = 0
    format_state_updates: list[dict[str, Any]] = []
    rich_state_update: dict[str, Any] | None = None
    last_result: dict[str, Any] = {}
    for index, html_part in enumerate(
        html_parts, start=start_part_index
    ):
        if remaining_writes == 0:
            return {
                "ok": False,
                "partial": bool(message_ids or start_part_index),
                "incomplete_logical_delivery": True,
                "format": "rich-partial",
                "formats": formats,
                "message_id": message_ids[0] if message_ids else "",
                "message_ids": message_ids,
                "partial_message_ids": message_ids,
                "canonical_message_id": (
                    message_ids[0] if message_ids else ""
                ),
                "failed_part_index": index,
                "physical_writes": physical_writes,
                "kind": "operation_budget_exhausted",
                "error": "Telegram physical-write budget exhausted",
                "terminal_outcome": "not_delivered",
                "operator_attention_required": True,
                "automatic_replay_authorized": False,
            }
        result = send_rich_message(
            client,
            chat_id,
            html_part,
            telegram=telegram,
            fallback_text=html_to_plain(html_part, limit=MAX_REPLY_CHARS),
            thread_id=thread_id,
            notify=notify,
            reply_to_message_id=(
                reply_to_message_id
                if index == start_part_index
                and start_part_index == 0
                else None
            ),
            api_token=api_token,
            max_physical_writes=remaining_writes,
        )
        last_result = result
        result_writes = _physical_writes(result)
        physical_writes += result_writes
        if remaining_writes is not None:
            remaining_writes = max(0, remaining_writes - result_writes)
        if not result.get("ok"):
            if message_ids:
                # The accepted prefix is real provider state, but a multipart
                # logical final is not complete until every part is accepted.
                # Do not replay either an ambiguous suffix (it may exist) or a
                # definite suffix (this legacy adapter has no resumable part
                # coordinates); surface both for explicit reconciliation.
                result.update(
                    {
                        "ok": False,
                        "partial": True,
                        "incomplete_logical_delivery": True,
                        "format": "rich-partial",
                        "formats": formats,
                        "message_id": message_ids[0],
                        "message_ids": message_ids,
                        "partial_message_ids": message_ids,
                        "canonical_message_id": message_ids[0],
                        "failed_part_index": index,
                        "physical_writes": physical_writes,
                        "terminal_outcome": (
                            _partial_terminal_outcome(result)
                        ),
                        "operator_attention_required": True,
                        "automatic_replay_authorized": False,
                    }
                )
            else:
                result["partial_message_ids"] = []
                result["physical_writes"] = physical_writes
            if format_state_updates:
                result[DELIVERY_FORMAT_STATE_UPDATE_KEY] = (
                    format_state_updates
                )
            return result
        message_ids.extend(split_legacy_message_ids(result))
        formats.append(str(result.get("format") or ""))
        update = result.get(DELIVERY_FORMAT_STATE_UPDATE_KEY)
        if isinstance(update, dict):
            format_state_updates.append(update)
        update = result.get(RICH_STATE_UPDATE_KEY)
        if isinstance(update, dict):
            rich_state_update = dict(update)
    combined = {
        "ok": True,
        "format": "rich-split",
        "formats": formats,
        "message_id": message_ids[0] if message_ids else str(last_result.get("message_id") or ""),
        "message_ids": message_ids,
        "canonical_message_id": message_ids[0] if message_ids else "",
        "physical_writes": physical_writes,
    }
    if format_state_updates:
        combined[DELIVERY_FORMAT_STATE_UPDATE_KEY] = format_state_updates
    if rich_state_update is not None:
        combined[RICH_STATE_UPDATE_KEY] = rich_state_update
    return combined


def edit_feed_item(
    client: TelegramClient,
    chat_id: str,
    message_id: str | int,
    item: dict[str, Any],
    *,
    telegram: dict[str, Any] | None,
    live: bool = False,
    api_token: str | None = None,
    max_physical_writes: int | None = None,
) -> dict[str, Any]:
    return edit_rich_message(
        client,
        chat_id,
        message_id,
        render_feed_item_html(item, live=live),
        telegram=telegram,
        fallback_text=item_plain_text(item),
        api_token=api_token,
        preserve_plain_html=bool(item.get("collapse_response")),
        max_physical_writes=max_physical_writes,
    )


def split_legacy_message_ids(result: dict[str, Any]) -> list[str]:
    raw_ids = result.get("message_ids")
    if isinstance(raw_ids, list):
        ids = [str(item) for item in raw_ids if str(item or "").strip()]
        if ids:
            return ids
    message_id = str(result.get("message_id") or "").strip()
    return [message_id] if message_id else []


def plain_chunks_for_result(text: str) -> list[str]:
    return split_text_chunks(text, limit=3400)


def turn_item_from_source(item: dict[str, Any], entry: dict[str, Any]) -> dict[str, Any]:
    stream_text = str(item.get("assistant_stream_text") or "")
    final_text = str(item.get("assistant_final_text") or "")
    if not final_text and item.get("complete") is True:
        final_text = stream_text
        stream_text = ""
    return {
        "kind": "turn",
        "title": worker_label(entry),
        "user_text": str(item.get("user_text") or ""),
        "worklog_text": stream_text if not final_text else "",
        "worklog_label": WORKING_LABEL,
        "assistant_final_text": final_text,
        "prompt_collapse_chars": 700,
    }
