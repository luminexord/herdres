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
from .rendering import html_to_plain, split_text_chunks, try_render_table, worker_label
from .safe import sanitize_text
from .telegram_delivery import RateLimited, TelegramClient, TelegramError


MAX_REPLY_CHARS = int(os.getenv("HERDR_TELEGRAM_TOPICS_FINAL_REPLY_MAX_CHARS", "64000"))
MAX_RICH_HTML_CHARS = int(os.getenv("HERDR_TELEGRAM_TOPICS_RICH_MAX_CHARS", "14000"))
# A turn is sent as ONE rich message when its full rendered HTML fits this; it is
# only split into "Response i/N" parts when it cannot fit a single message.
# Default = MAX_RICH_HTML_CHARS so a response splits iff Telegram would reject it
# as too large -- not before. (The split itself stays lossless.)
RICH_SINGLE_MESSAGE_CHARS = int(os.getenv("HERDR_TELEGRAM_TOPICS_RICH_SINGLE_MESSAGE_CHARS", str(MAX_RICH_HTML_CHARS)))
# Source-text chunk size used when a response DOES need splitting. Bigger chunks
# => fewer parts; kept well under the per-message cap so each rendered part
# (plus the You/Working sections on part 1) stays rich rather than falling back.
RICH_SPLIT_CHUNK_CHARS = int(os.getenv("HERDR_TELEGRAM_TOPICS_RICH_SPLIT_CHUNK_CHARS", "4000"))
USER_PROMPT_MAX_CHARS = int(os.getenv("HERDR_TELEGRAM_TOPICS_USER_PROMPT_MAX_CHARS", "1200"))
WORKLOG_MAX_CHARS = int(os.getenv("HERDR_TELEGRAM_TOPICS_WORKLOG_MAX_CHARS", "1200"))
PROMPT_PREVIEW_CHARS = 80
USER_PROMPT_LABEL = "You"
RESPONSE_LABEL = "Response"
WORKING_LABEL = "Working"
YOU_ICON = "💬"
WORKING_ICON = "⚙️"
RESPONSE_ICON = "✅"
RICH_RENDER_VERSION = 26
RICH_BAD_REQUEST_LIMIT = 3

FENCE_START_RE = re.compile(r"^\s*(`{3,}|~{3,})\s*([A-Za-z0-9_+-]{0,32})\s*$")
HRULE_RE = re.compile(r"^\s*([-*_])(?:[ \t]*\1){2,}[ \t]*$")
INLINE_CODE_RE = re.compile(r"`([^`\n]{1,300})`")


def _html_text(value: Any, max_chars: int = MAX_REPLY_CHARS) -> str:
    return html.escape(sanitize_text(str(value or ""), max_chars), quote=False)


def _rich_inline(value: Any, max_chars: int = 900) -> str:
    text = _html_text(value, max_chars)
    code_spans: list[str] = []

    def hold_code(match: re.Match[str]) -> str:
        code_spans.append(f"<code>{html.escape(match.group(1), quote=False)}</code>")
        return f"\u0000{len(code_spans) - 1}\u0000"

    text = INLINE_CODE_RE.sub(hold_code, text)
    text = re.sub(r"\*\*\*([^\n]+?)\*\*\*", r"<b><i>\1</i></b>", text)
    text = re.sub(r"\*\*([^\n]+?)\*\*", r"<b>\1</b>", text)
    text = re.sub(r"(?<![\w*])\*(?!\s)([^*\n]+?)(?<!\s)\*(?![\w*])", r"<i>\1</i>", text)
    text = re.sub(r"__([^\n]+?)__", r"<b>\1</b>", text)
    text = re.sub(r"~~([^\s~][^\n]*?)~~", r"<s>\1</s>", text)
    for index, code in enumerate(code_spans):
        text = text.replace(f"\u0000{index}\u0000", code)
    return text


def _rich_paragraph(value: Any) -> str:
    clean = _rich_inline(value, 1600).strip()
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
    body = "<br>".join(_rich_inline(line, 900) for line in sanitize_text(user_text, USER_PROMPT_MAX_CHARS).splitlines())
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
                parts.append(f"<pre><code{class_attr}>{_html_text(chr(10).join(code_lines), 3000)}</code></pre>")
            previous_blank = False
            continue
        # Pipe table (row + `---|---` delimiter): render as a native <table> (the rich path turns it
        # into a PageBlockTable). Cells use _rich_inline so bold/code/links inside cells render. Must
        # precede the paragraph fallthrough, which would otherwise emit raw `| a | b |` / `|---|`.
        table = try_render_table(lines, idx, cell_html=lambda c: _rich_inline(c, 160))
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
            parts.append(f"<{tag}>{_html_text(title, 100)}</{tag}>")
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
            parts.append("<ul>\n" + "\n".join(f"<li>{_rich_inline(item, 900)}</li>" for item in items) + "\n</ul>")
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
            parts.append("<ol>\n" + "\n".join(f"<li>{_rich_inline(item, 900)}</li>" for item in items) + "\n</ol>")
            previous_blank = False
            continue
        if stripped.startswith(">"):
            quote_lines: list[str] = []
            while idx < len(lines) and str(lines[idx] or "").strip().startswith(">"):
                quote_lines.append(re.sub(r"^\s*>\s?", "", str(lines[idx]).rstrip()))
                idx += 1
            parts.append("<blockquote>" + "<br>".join(_rich_inline(quote, 900) for quote in quote_lines) + "</blockquote>")
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


def render_assistant_response_html(assistant_final: str, *, label: str = RESPONSE_LABEL) -> str:
    # The Response is the answer the reader came for, so it renders as the open
    # top-level body (not buried in a <details> card): a bold marker, one blank
    # line, then the rich blocks.
    #
    # Spacing note (empirical, from Telegram rich rendering): <p>/<h4> block
    # margins are negligible (a <p> title glues to the body) and a single <br>
    # glues too -- only a blank line (<br><br>) makes a visible gap. To keep that
    # gap to ONE empty line rather than a huge one, the body's headings are
    # demoted to <h4> (negligible top margin) so a full <h3> heading doesn't stack
    # its own margin on top of the blank line below the title.
    clean = str(assistant_final or "").strip()
    if not clean:
        return ""
    body_html = render_final_reply_html(clean, seen_heading=True) or _rich_paragraph(clean)
    marker = f"<b>{RESPONSE_ICON} {_html_text(label or RESPONSE_LABEL, 80)}</b>"
    return f"{marker}<br><br>{body_html}"


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
    # Layout (source mode): the Response is the open, prominent top-level body;
    # the user prompt (and any in-progress worklog) are de-emphasized collapsible
    # sections below it. No redundant top worker title -- the Telegram topic
    # already names the worker, and an extra <h3> only added a margin-bearing
    # block above the first section.
    user_text = str(item.get("user_text") or "").strip()
    worklog_text = str(item.get("worklog_text") or item.get("assistant_stream_text") or "").strip()
    worklog_label = str(item.get("worklog_label") or WORKING_LABEL).strip() or WORKING_LABEL
    assistant_final = str(item.get("assistant_final_text") or "").strip()
    parts: list[str] = []
    if assistant_final and item.get("collapse_response"):
        # Superseded final: the Response folds into a closed <details> with a one-line preview, so
        # older answers read as a compact history while staying expandable in place.
        body_html = render_final_reply_html(assistant_final, seen_heading=True) or _rich_paragraph(assistant_final)
        response_html = _rich_details_quote_html(
            str(item.get("response_label") or RESPONSE_LABEL),
            body_html,
            icon=RESPONSE_ICON,
            open_by_default=False,
            preview=_prompt_preview(assistant_final),
        )
    else:
        response_html = render_assistant_response_html(assistant_final, label=str(item.get("response_label") or RESPONSE_LABEL))
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
    return sanitize_text(str(item.get("assistant_final_text") or ""), MAX_REPLY_CHARS).strip()


def _turn_item_delivery_parts(item: dict[str, Any], *, live: bool = False) -> list[dict[str, Any]]:
    if live or str(item.get("kind") or "").lower() != "turn":
        return [item]
    final_text = _turn_response_text(item)
    if not final_text:
        return [item]
    if len(render_turn_item_html(item)) <= min(RICH_SINGLE_MESSAGE_CHARS, MAX_RICH_HTML_CHARS):
        return [item]
    chunks = split_text_chunks(final_text, limit=RICH_SPLIT_CHUNK_CHARS)
    if len(chunks) <= 1:
        return [item]
    total = len(chunks)
    parts: list[dict[str, Any]] = []
    for index, chunk in enumerate(chunks, start=1):
        part = dict(item)
        part["assistant_final_text"] = chunk
        part["response_label"] = f"{RESPONSE_LABEL} {index}/{total}"
        if index > 1:
            part["user_text"] = ""
            part["worklog_text"] = ""
        parts.append(part)
    return parts


def render_feed_item_delivery_html_parts(item: dict[str, Any], *, live: bool = False) -> list[str]:
    return [render_feed_item_html(part, live=live) for part in _turn_item_delivery_parts(item, live=live)]


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
    rich = telegram.setdefault("rich_messages", {})
    if not isinstance(rich, dict):
        rich = {}
        telegram["rich_messages"] = rich
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
    rich["supported"] = "unknown"
    rich.pop("disabled_reason", None)
    rich.pop("bad_request_streak", None)
    rich.pop("disabled_render_version", None)
    return True


def rich_message_send_enabled(telegram: dict[str, Any] | None) -> bool:
    return isinstance(telegram, dict) and rich_enabled(telegram)


def mark_rich_supported(telegram: dict[str, Any] | None) -> None:
    rich = rich_telegram_state(telegram)
    if rich:
        rich["supported"] = "yes"
        rich.pop("disabled_reason", None)
        rich.pop("bad_request_streak", None)


def mark_rich_disabled(telegram: dict[str, Any] | None, reason: str) -> None:
    rich = rich_telegram_state(telegram)
    if rich:
        rich["supported"] = "no"
        rich["disabled_reason"] = sanitize_text(reason, 300)
        rich["disabled_render_version"] = RICH_RENDER_VERSION


def note_rich_bad_request(telegram: dict[str, Any] | None, reason: str) -> None:
    rich = rich_telegram_state(telegram)
    if not rich:
        return
    try:
        streak = int(rich.get("bad_request_streak") or 0)
    except (TypeError, ValueError):
        streak = 0
    streak += 1
    rich["bad_request_streak"] = streak
    if streak >= RICH_BAD_REQUEST_LIMIT:
        mark_rich_disabled(telegram, f"repeated bad_request: {reason}")


def _client_for_token(client: TelegramClient, api_token: str | None) -> TelegramClient:
    token = str(api_token or "").strip()
    return client.with_token(token) if token else client


def _telegram_message_id(response: dict[str, Any]) -> str:
    result = response.get("result") if isinstance(response.get("result"), dict) else {}
    return str(result.get("message_id") or "0")


def _classify_telegram_error(error: Exception) -> str:
    text = str(error or "").lower()
    if any(marker in text for marker in ("method not found", "no such method", "not found: method", "404")):
        return "capability"
    if "message is not modified" in text:
        return "not_modified"
    if "message to edit not found" in text or "message not found" in text:
        return "not_found"
    if "topic_id_invalid" in text or "message thread not found" in text:
        return "topic_not_found"
    if "chat not found" in text or "bot was kicked" in text or "not enough rights" in text:
        return "bot_access"
    if "bad request" in text:
        return "bad_request"
    return "transient"


def _retry_rich_delivery(kind: str, error: Exception) -> dict[str, Any]:
    return {"ok": False, "format": "rich", "kind": kind, "error": sanitize_text(str(error), 300)}


def _fallback_send(
    client: TelegramClient,
    chat_id: str,
    fallback: str,
    *,
    thread_id: str | int | None,
    notify: bool,
    reply_to_message_id: str | int | None,
) -> dict[str, Any]:
    return client.send_message(
        chat_id,
        fallback,
        thread_id=thread_id,
        notify=notify,
        reply_to_message_id=reply_to_message_id,
    )


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
) -> dict[str, Any]:
    target = _client_for_token(client, api_token)
    rendered_fallback = sanitize_text(html_to_plain(html_text, limit=MAX_REPLY_CHARS), MAX_REPLY_CHARS)
    fallback = rendered_fallback or fallback_text or sanitize_text(str(html_text or ""), MAX_REPLY_CHARS)
    if not rich_message_send_enabled(telegram):
        return _fallback_send(target, chat_id, fallback, thread_id=thread_id, notify=notify, reply_to_message_id=reply_to_message_id)
    if len(html_text) > MAX_RICH_HTML_CHARS:
        fallback_result = _fallback_send(
            target,
            chat_id,
            fallback,
            thread_id=thread_id,
            notify=notify,
            reply_to_message_id=reply_to_message_id,
        )
        fallback_result["fallback_reason"] = "rich_too_large"
        return fallback_result

    payload: dict[str, Any] = {
        "chat_id": chat_id,
        "disable_notification": "false" if notify else "true",
        "rich_message": json.dumps(
            {"html": sanitize_text(html_text, MAX_RICH_HTML_CHARS), "skip_entity_detection": True},
            separators=(",", ":"),
            ensure_ascii=False,
        ),
    }
    if thread_id:
        payload["message_thread_id"] = str(thread_id)
    if reply_to_message_id:
        payload["reply_parameters"] = json.dumps({"message_id": int(reply_to_message_id)}, separators=(",", ":"))
    try:
        response = target.api("sendRichMessage", payload)
    except RateLimited:
        raise
    except TelegramError as exc:
        kind = _classify_telegram_error(exc)
        if kind == "transient":
            return _retry_rich_delivery(kind, exc)
        if kind == "capability":
            mark_rich_disabled(telegram, str(exc))
        elif kind == "bad_request":
            note_rich_bad_request(telegram, str(exc))
        elif api_token and kind == "bot_access":
            return {"ok": False, "format": "rich", "kind": kind, "error": str(exc)}
        fallback_result = _fallback_send(target, chat_id, fallback, thread_id=thread_id, notify=notify, reply_to_message_id=reply_to_message_id)
        fallback_result["fallback_reason"] = kind
        return fallback_result
    mark_rich_supported(telegram)
    return {"ok": True, "format": "rich", "message_id": _telegram_message_id(response)}


def edit_rich_message(
    client: TelegramClient,
    chat_id: str,
    message_id: str | int,
    html_text: str,
    *,
    telegram: dict[str, Any] | None,
    fallback_text: str = "",
    api_token: str | None = None,
) -> dict[str, Any]:
    target = _client_for_token(client, api_token)
    rendered_fallback = sanitize_text(html_to_plain(html_text, limit=MAX_REPLY_CHARS), MAX_REPLY_CHARS)
    fallback = rendered_fallback or fallback_text or sanitize_text(str(html_text or ""), MAX_REPLY_CHARS)
    if not rich_message_send_enabled(telegram):
        return target.edit_message(chat_id, message_id, fallback)
    if len(html_text) > MAX_RICH_HTML_CHARS:
        legacy = target.edit_message(chat_id, message_id, fallback)
        legacy["fallback_reason"] = "rich_too_large"
        return legacy
    payload = {
        "chat_id": chat_id,
        "message_id": str(message_id),
        "rich_message": json.dumps(
            {"html": sanitize_text(html_text, MAX_RICH_HTML_CHARS), "skip_entity_detection": True},
            separators=(",", ":"),
            ensure_ascii=False,
        ),
    }
    try:
        response = target.api("editMessageText", payload)
    except RateLimited:
        raise
    except TelegramError as exc:
        kind = _classify_telegram_error(exc)
        if kind == "transient":
            return _retry_rich_delivery(kind, exc)
        if kind == "not_modified":
            return {"ok": True, "format": "rich", "kind": kind, "message_id": str(message_id)}
        if kind in {"not_found", "topic_not_found"}:
            return {"ok": False, "format": "rich", "kind": kind, "not_found": kind == "not_found", "topic_missing": kind == "topic_not_found", "error": str(exc)}
        if kind == "capability":
            mark_rich_disabled(telegram, str(exc))
        elif kind == "bad_request":
            note_rich_bad_request(telegram, str(exc))
        legacy = target.edit_message(chat_id, message_id, fallback)
        legacy["fallback_reason"] = kind
        return legacy
    mark_rich_supported(telegram)
    return {"ok": True, "format": "rich", "kind": "edited", "message_id": _telegram_message_id(response) or str(message_id)}


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
) -> dict[str, Any]:
    html_parts = render_feed_item_delivery_html_parts(item, live=live)
    if len(html_parts) <= 1:
        return send_rich_message(
            client,
            chat_id,
            html_parts[0] if html_parts else render_feed_item_html(item, live=live),
            telegram=telegram,
            fallback_text=item_plain_text(item),
            thread_id=thread_id,
            notify=notify,
            reply_to_message_id=reply_to_message_id,
            api_token=api_token,
        )
    message_ids: list[str] = []
    formats: list[str] = []
    last_result: dict[str, Any] = {}
    for index, html_part in enumerate(html_parts):
        result = send_rich_message(
            client,
            chat_id,
            html_part,
            telegram=telegram,
            fallback_text=html_to_plain(html_part, limit=MAX_REPLY_CHARS),
            thread_id=thread_id,
            notify=notify,
            reply_to_message_id=reply_to_message_id if index == 0 else None,
            api_token=api_token,
        )
        last_result = result
        if not result.get("ok"):
            result["partial_message_ids"] = message_ids
            return result
        message_ids.extend(split_legacy_message_ids(result))
        formats.append(str(result.get("format") or ""))
    return {
        "ok": True,
        "format": "rich-split",
        "formats": formats,
        "message_id": message_ids[0] if message_ids else str(last_result.get("message_id") or ""),
        "message_ids": message_ids,
    }


def edit_feed_item(
    client: TelegramClient,
    chat_id: str,
    message_id: str | int,
    item: dict[str, Any],
    *,
    telegram: dict[str, Any] | None,
    live: bool = False,
    api_token: str | None = None,
) -> dict[str, Any]:
    return edit_rich_message(
        client,
        chat_id,
        message_id,
        render_feed_item_html(item, live=live),
        telegram=telegram,
        fallback_text=item_plain_text(item),
        api_token=api_token,
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
