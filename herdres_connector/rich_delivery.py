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
from .rendering import html_to_plain, split_text_chunks, worker_label
from .safe import sanitize_text
from .telegram_delivery import RateLimited, TelegramClient, TelegramError


MAX_REPLY_CHARS = int(os.getenv("HERDR_TELEGRAM_TOPICS_FINAL_REPLY_MAX_CHARS", "16000"))
MAX_RICH_HTML_CHARS = int(os.getenv("HERDR_TELEGRAM_TOPICS_RICH_MAX_CHARS", "14000"))
RICH_SAFE_CHARS = 12000
USER_PROMPT_MAX_CHARS = int(os.getenv("HERDR_TELEGRAM_TOPICS_USER_PROMPT_MAX_CHARS", "1200"))
PROMPT_PREVIEW_CHARS = 80
USER_PROMPT_LABEL = "You"
RESPONSE_LABEL = "Response"
WORKING_LABEL = "Working"
RICH_RENDER_VERSION = 22
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
    return clean


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
    summary_max_chars: int = 80,
    open_by_default: bool = True,
    quote: bool = True,
    preview: str = "",
    body_small: bool = True,
) -> str:
    body = str(body_html or "").strip()
    if not body:
        return ""
    label = _html_text(summary, summary_max_chars)
    open_attr = " open" if open_by_default else ""
    preview_text = str(preview or "").strip()
    preview_html = f" {_html_text(preview_text, PROMPT_PREVIEW_CHARS + 6)}" if preview_text else ""
    summary_html = f"<small><b>{label}</b>{preview_html}</small>"
    body_content = f"<small>{body}</small>" if body_small else body
    inner = f"<blockquote>{body_content}</blockquote>" if quote else body_content
    return f"<details{open_attr}><summary>{summary_html}</summary>{inner}</details>"


def render_user_prompt_quote_html(user_text: str, collapse_chars: int = 0) -> str:
    body = "<br>".join(_rich_inline(line, 900) for line in sanitize_text(user_text, USER_PROMPT_MAX_CHARS).splitlines())
    body = body.strip()
    if not body:
        return ""
    collapse = _prompt_should_collapse(user_text, collapse_chars)
    return _rich_details_quote_html(
        USER_PROMPT_LABEL,
        body,
        summary_max_chars=20,
        open_by_default=not collapse,
        preview=_prompt_preview(user_text) if collapse else "",
    )


def _join_blocks(parts: list[str]) -> str:
    kept = [part for part in parts if str(part or "").strip()]
    return "<br>".join(kept)


def _join_sections(parts: list[str]) -> str:
    kept = [part for part in parts if str(part or "").strip()]
    return "\n".join(kept)


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
        if _is_heading(line, first_block=not seen_heading, previous_blank=previous_blank):
            title = _heading_title(line)
            parts.append(f"<b>{_html_text(title, 100)}</b>")
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
            parts.append("<br>".join(f"• {_rich_inline(item, 900)}" for item in items))
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
            parts.append("<br>".join(f"{number}. {_rich_inline(item, 900)}" for number, item in enumerate(items, start=1)))
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


def render_final_reply_html(value: str) -> str:
    clean = sanitize_text(str(value or ""), MAX_REPLY_CHARS).strip()
    if not clean:
        return ""
    return _render_final_reply_blocks(clean.splitlines())


def render_assistant_response_quote_html(
    assistant_final: str,
    *,
    open_by_default: bool = True,
    preview: str = "",
) -> str:
    clean = str(assistant_final or "").strip()
    if not clean:
        return ""
    body_html = render_final_reply_html(clean) or _rich_paragraph(clean)
    return _rich_details_quote_html(
        RESPONSE_LABEL,
        body_html,
        quote=False,
        open_by_default=open_by_default,
        preview=preview,
    )


def render_source_v2_working_update_html(worklog_text: str, *, label: str = WORKING_LABEL) -> str:
    clean = str(worklog_text or "").strip()
    if not clean:
        return ""
    label_html = _html_text(label or WORKING_LABEL, 100)
    preview = _prompt_preview(clean)
    preview_html = f" {_html_text(preview, PROMPT_PREVIEW_CHARS + 6)}" if preview else ""
    body_html = render_final_reply_html(clean) or _rich_paragraph(clean)
    return f"<details><summary><small><b>{label_html}</b>{preview_html}</small></summary><blockquote><small>{body_html}</small></blockquote></details>"


def render_turn_item_html(item: dict[str, Any]) -> str:
    title = str(item.get("title") or "").strip()
    user_text = str(item.get("user_text") or "").strip()
    worklog_text = str(item.get("worklog_text") or item.get("assistant_stream_text") or "").strip()
    worklog_label = str(item.get("worklog_label") or WORKING_LABEL).strip() or WORKING_LABEL
    assistant_final = str(item.get("assistant_final_text") or "").strip()
    response_open = not bool(item.get("collapse_response"))
    response_preview = "" if response_open else _prompt_preview(assistant_final)
    parts: list[str] = []
    if title:
        parts.append(f"<small><b>{_html_text(title, 100)}</b></small>")
    if user_text:
        parts.append(render_user_prompt_quote_html(user_text, int(item.get("prompt_collapse_chars") or 0)))
    if worklog_text and not assistant_final:
        parts.append(render_source_v2_working_update_html(worklog_text, label=worklog_label))
    elif worklog_text:
        body_html = render_final_reply_html(worklog_text) or _rich_paragraph(worklog_text)
        parts.append(_rich_details_quote_html(worklog_label, body_html, open_by_default=False, preview=_prompt_preview(worklog_text)))
    response_html = render_assistant_response_quote_html(assistant_final, open_by_default=response_open, preview=response_preview)
    if response_html:
        parts.append(response_html)
    return _join_sections(parts).strip()


def render_feed_item_html(item: dict[str, Any], *, live: bool = False) -> str:
    kind = str(item.get("kind") or "update").lower()
    if kind == "turn":
        return render_turn_item_html(item)
    title = str(item.get("title") or item.get("kind") or "Update").strip()
    summary = str(item.get("summary") or item.get("text") or "").strip()
    if live:
        title = f"Latest {title}"
    parts = [f"<small><b>{_html_text(title, 100)}</b></small>"]
    if summary:
        parts.append(render_final_reply_html(summary) or _rich_paragraph(summary))
    return _join_sections(parts)


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
    rendered_fallback = sanitize_text(html_to_plain(html_text), MAX_REPLY_CHARS)
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
    rendered_fallback = sanitize_text(html_to_plain(html_text), MAX_REPLY_CHARS)
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
    return send_rich_message(
        client,
        chat_id,
        render_feed_item_html(item, live=live),
        telegram=telegram,
        fallback_text=item_plain_text(item),
        thread_id=thread_id,
        notify=notify,
        reply_to_message_id=reply_to_message_id,
        api_token=api_token,
    )


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
    return {
        "kind": "turn",
        "title": worker_label(entry),
        "user_text": str(item.get("user_text") or ""),
        "worklog_text": str(item.get("assistant_stream_text") or "") if not item.get("assistant_final_text") else "",
        "worklog_label": WORKING_LABEL,
        "assistant_final_text": str(item.get("assistant_final_text") or item.get("assistant_stream_text") or ""),
        "prompt_collapse_chars": 700,
    }
