"""Telegram API and connector-outbox delivery for source-only Herdres."""

from __future__ import annotations

import json
import urllib.error
import urllib.parse
import urllib.request
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from . import config, state
from .rendering import html_to_plain, render_attention_notice, split_text_chunks
from .safe import sanitize_text, short_hash
from .tendwire_client import TendwireClient


class TelegramError(RuntimeError):
    pass


class RateLimited(TelegramError):
    def __init__(self, retry_after: int, message: str = "Telegram rate limited") -> None:
        super().__init__(message)
        self.retry_after = max(1, int(retry_after or 1))


MESSAGE_TEXT_LIMIT = 3900
SPLIT_TEXT_LIMIT = 3400
MESSAGE_SOURCE_LIMIT = 64000


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _cache_fresh(value: str, ttl_seconds: int) -> bool:
    try:
        then = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except Exception:
        return False
    return (datetime.now(timezone.utc) - then).total_seconds() <= ttl_seconds


def _topic_missing(error: Any) -> bool:
    text = str(error or "").lower()
    return "topic_id_invalid" in text or "message thread not found" in text


def _multipart_body(boundary: str, fields: dict[str, str], file_field: str, filename: str,
                    content_type: str, content: bytes) -> bytes:
    parts: list[bytes] = []
    for key, value in fields.items():
        parts.append(f"--{boundary}\r\n".encode())
        parts.append(f'Content-Disposition: form-data; name="{key}"\r\n\r\n'.encode())
        parts.append(f"{value}\r\n".encode())
    parts.append(f"--{boundary}\r\n".encode())
    parts.append(f'Content-Disposition: form-data; name="{file_field}"; filename="{filename}"\r\n'.encode())
    parts.append(f"Content-Type: {content_type}\r\n\r\n".encode())
    parts.append(content)
    parts.append(f"\r\n--{boundary}--\r\n".encode())
    return b"".join(parts)


@dataclass(frozen=True)
class TelegramClient:
    token: str
    timeout: float = 20.0
    dry_run: bool = False

    def configured(self) -> bool:
        return bool(str(self.token or "").strip())

    def with_token(self, token: str) -> "TelegramClient":
        return TelegramClient(token=token, timeout=self.timeout, dry_run=self.dry_run)

    def api(self, method: str, payload: dict[str, Any]) -> dict[str, Any]:
        if self.dry_run:
            return {"ok": True, "result": {"message_id": 0, "message_thread_id": payload.get("message_thread_id", 0)}}
        if not self.configured():
            raise TelegramError("Telegram bot token is not configured")
        data = urllib.parse.urlencode({key: str(value) for key, value in payload.items() if value is not None}).encode()
        url = f"https://api.telegram.org/bot{self.token}/{method}"
        try:
            with urllib.request.urlopen(url, data=data, timeout=self.timeout) as response:
                body = response.read().decode("utf-8", "replace")
        except urllib.error.HTTPError as exc:
            detail = exc.read().decode("utf-8", "replace")
            try:
                data = json.loads(detail)
            except json.JSONDecodeError:
                data = {}
            params = data.get("parameters") if isinstance(data.get("parameters"), dict) else {}
            if exc.code == 429:
                raise RateLimited(int(params.get("retry_after") or 1), sanitize_text(data.get("description") or detail, 300)) from exc
            raise TelegramError(sanitize_text(data.get("description") or detail or str(exc), 300)) from exc
        except Exception as exc:  # noqa: BLE001
            raise TelegramError(sanitize_text(str(exc), 300)) from exc
        try:
            data = json.loads(body)
        except json.JSONDecodeError as exc:
            raise TelegramError("Telegram returned non-json response") from exc
        if not data.get("ok"):
            raise TelegramError(sanitize_text(data.get("description") or "Telegram API error", 300))
        return data

    def _html_variants(self, html_text: str) -> list[tuple[str, str]]:
        html = sanitize_text(html_text, 3900)
        variants = [("html", html)]
        if "<blockquote expandable>" in html:
            variants.append(("html-no-expandable", html.replace("<blockquote expandable>", "<blockquote>")))
        plain = html_to_plain(html)
        if plain and plain != html:
            variants.append(("plain", sanitize_text(plain, 3900)))
        return variants

    def send_message(
        self,
        chat_id: str,
        html_text: str,
        *,
        thread_id: str | int | None = None,
        reply_to_message_id: str | int | None = None,
        notify: bool = False,
    ) -> dict[str, Any]:
        base_payload: dict[str, Any] = {
            "chat_id": chat_id,
            "disable_web_page_preview": "true",
            "disable_notification": "false" if notify else "true",
        }
        if thread_id:
            base_payload["message_thread_id"] = str(thread_id)
        if reply_to_message_id:
            base_payload["reply_parameters"] = json.dumps({"message_id": int(reply_to_message_id)}, separators=(",", ":"))
        source = sanitize_text(html_text, MESSAGE_SOURCE_LIMIT)
        plain = html_to_plain(source, limit=MESSAGE_SOURCE_LIMIT)
        if len(source) > MESSAGE_TEXT_LIMIT and plain:
            chunks = split_text_chunks(plain, limit=SPLIT_TEXT_LIMIT)
            message_ids: list[str] = []
            last_error = ""
            total = len(chunks)
            for index, chunk in enumerate(chunks, start=1):
                text = chunk if total == 1 else f"Part {index}/{total}\n{chunk}"
                payload = dict(base_payload)
                payload["text"] = sanitize_text(text, MESSAGE_TEXT_LIMIT)
                if index > 1:
                    payload.pop("reply_parameters", None)
                try:
                    result = self.api("sendMessage", payload).get("result") or {}
                    message_ids.append(str(result.get("message_id") or "0"))
                except TelegramError as exc:
                    last_error = str(exc)
                    break
            if len(message_ids) == total:
                return {"ok": True, "message_id": message_ids[0] if message_ids else "0", "message_ids": message_ids, "format": "plain-split"}
            return {"ok": False, "error": sanitize_text(last_error, 300)}
        last_error = ""
        for fmt, text in self._html_variants(html_text):
            payload = dict(base_payload)
            payload["text"] = text
            if fmt != "plain":
                payload["parse_mode"] = "HTML"
            try:
                result = self.api("sendMessage", payload).get("result") or {}
                return {"ok": True, "message_id": str(result.get("message_id") or "0"), "format": fmt}
            except TelegramError as exc:
                last_error = str(exc)
        return {"ok": False, "error": sanitize_text(last_error, 300)}

    def send_voice(
        self,
        chat_id: str,
        file_path: str | Path,
        *,
        thread_id: str | int | None = None,
        reply_to_message_id: str | int | None = None,
        notify: bool = False,
    ) -> dict[str, Any]:
        """Upload an OGG/Opus file as a Telegram voice note (multipart). Additive to the text turn —
        callers treat a False/error result as "text-only", never an error."""
        if self.dry_run:
            return {"ok": True, "message_id": "0"}
        if not self.configured():
            raise TelegramError("Telegram bot token is not configured")
        path = Path(file_path)
        try:
            audio = path.read_bytes()
        except OSError as exc:
            return {"ok": False, "error": sanitize_text(str(exc), 200)}
        if not audio:
            return {"ok": False, "error": "empty voice file"}
        fields: dict[str, str] = {"chat_id": str(chat_id), "disable_notification": "false" if notify else "true"}
        if thread_id:
            fields["message_thread_id"] = str(thread_id)
        if reply_to_message_id:
            fields["reply_parameters"] = json.dumps({"message_id": int(reply_to_message_id)}, separators=(",", ":"))
        boundary = "----herdres" + uuid.uuid4().hex
        body = _multipart_body(boundary, fields, "voice", path.name or "reply.ogg", "audio/ogg", audio)
        request = urllib.request.Request(
            f"https://api.telegram.org/bot{self.token}/sendVoice",
            data=body,
            headers={"Content-Type": f"multipart/form-data; boundary={boundary}"},
        )
        try:
            with urllib.request.urlopen(request, timeout=max(self.timeout, 60.0)) as response:
                data = json.loads(response.read().decode("utf-8", "replace"))
        except urllib.error.HTTPError as exc:
            detail = exc.read().decode("utf-8", "replace")
            try:
                parsed = json.loads(detail)
            except json.JSONDecodeError:
                parsed = {}
            params = parsed.get("parameters") if isinstance(parsed.get("parameters"), dict) else {}
            if exc.code == 429:
                raise RateLimited(int(params.get("retry_after") or 1), sanitize_text(parsed.get("description") or detail, 300)) from exc
            raise TelegramError(sanitize_text(parsed.get("description") or detail or str(exc), 300)) from exc
        except Exception as exc:  # noqa: BLE001
            raise TelegramError(sanitize_text(str(exc), 300)) from exc
        if not isinstance(data, dict) or not data.get("ok"):
            raise TelegramError(sanitize_text((data or {}).get("description") or "Telegram sendVoice error", 300))
        result = data.get("result") or {}
        return {"ok": True, "message_id": str(result.get("message_id") or "0")}

    def edit_message(self, chat_id: str, message_id: str | int, html_text: str) -> dict[str, Any]:
        base_payload = {
            "chat_id": chat_id,
            "message_id": str(message_id),
            "disable_web_page_preview": "true",
        }
        last_error = ""
        for fmt, text in self._html_variants(html_text):
            payload = dict(base_payload)
            payload["text"] = text
            if fmt != "plain":
                payload["parse_mode"] = "HTML"
            try:
                self.api("editMessageText", payload)
                return {"ok": True, "message_id": str(message_id), "kind": "edited", "format": fmt}
            except TelegramError as exc:
                last_error = str(exc)
                if "message is not modified" in last_error.lower():
                    return {"ok": True, "message_id": str(message_id), "kind": "unchanged", "format": fmt}
        return {"ok": False, "error": sanitize_text(last_error, 300)}

    def delete_message(self, chat_id: str, message_id: str | int) -> dict[str, Any]:
        try:
            self.api("deleteMessage", {"chat_id": chat_id, "message_id": str(message_id)})
            return {"ok": True}
        except TelegramError as exc:
            return {"ok": False, "error": sanitize_text(str(exc), 300)}

    def create_topic(self, chat_id: str, name: str, icon_color: int | None = None) -> dict[str, Any]:
        payload = {"chat_id": chat_id, "name": sanitize_text(name, 128)}
        if icon_color:
            payload["icon_color"] = str(int(icon_color))
        try:
            result = self.api("createForumTopic", payload).get("result") or {}
            return {"ok": True, "topic_id": str(result.get("message_thread_id") or "")}
        except TelegramError as exc:
            return {"ok": False, "error": sanitize_text(str(exc), 300)}

    def edit_topic_icon(self, chat_id: str, thread_id: str, emoji_id: str) -> dict[str, Any]:
        if not emoji_id:
            return {"ok": False, "skipped": True}
        try:
            self.api("editForumTopic", {"chat_id": chat_id, "message_thread_id": str(thread_id), "icon_custom_emoji_id": emoji_id})
            return {"ok": True}
        except TelegramError as exc:
            return {"ok": False, "error": sanitize_text(str(exc), 300)}

    def delete_topic(self, chat_id: str, thread_id: str) -> dict[str, Any]:
        try:
            self.api("deleteForumTopic", {"chat_id": chat_id, "message_thread_id": str(thread_id)})
            return {"ok": True}
        except TelegramError as exc:
            return {"ok": False, "error": sanitize_text(str(exc), 300)}

    def pin_message(self, chat_id: str, message_id: str | int) -> dict[str, Any]:
        try:
            self.api("pinChatMessage", {"chat_id": chat_id, "message_id": str(message_id), "disable_notification": "true"})
            return {"ok": True}
        except TelegramError as exc:
            return {"ok": False, "error": sanitize_text(str(exc), 300)}


# Telegram's fixed allowed createForumTopic colors.
TOPIC_ICON_COLORS = (0x6FB9F0, 0xFFD67E, 0xCB86DB, 0x8EEE98, 0xFF93B2, 0xFB6F5F)


def topic_icon_catalog(store: dict[str, Any], telegram_client: TelegramClient | None = None) -> dict[str, str]:
    """Return the emoji -> custom_emoji_id map of the forum topic icon set."""
    telegram = store.get("telegram") if isinstance(store.get("telegram"), dict) else {}
    icons = telegram.get("forum_topic_icons") if isinstance(telegram, dict) else {}
    by_emoji = icons.get("by_emoji") if isinstance(icons, dict) and isinstance(icons.get("by_emoji"), dict) else {}
    if not by_emoji:
        # Populate the cache through the existing fetch path.
        topic_icon_id(store, "\u2705", telegram_client)
        telegram = store.get("telegram") if isinstance(store.get("telegram"), dict) else {}
        icons = telegram.get("forum_topic_icons") if isinstance(telegram, dict) else {}
        by_emoji = icons.get("by_emoji") if isinstance(icons, dict) and isinstance(icons.get("by_emoji"), dict) else {}
    return {str(k): str(v) for k, v in by_emoji.items() if v}


def topic_icon_id(store: dict[str, Any], emoji: str, telegram_client: TelegramClient | None = None) -> str:
    telegram = store.get("telegram") if isinstance(store.get("telegram"), dict) else {}
    icons = telegram.setdefault("forum_topic_icons", {}) if isinstance(telegram, dict) else {}
    if not isinstance(icons, dict):
        icons = {}
        telegram["forum_topic_icons"] = icons
    by_emoji = icons.get("by_emoji") if isinstance(icons.get("by_emoji"), dict) else {}
    cached = str(by_emoji.get(emoji) or "")
    if cached:
        return cached
    if telegram_client is None or getattr(telegram_client, "dry_run", False):
        return ""
    if by_emoji and _cache_fresh(str(icons.get("fetched_at") or ""), config.topic_icon_cache_ttl_seconds()):
        return ""
    try:
        response = telegram_client.api("getForumTopicIconStickers", {})
    except Exception as exc:  # noqa: BLE001
        icons["last_error"] = sanitize_text(str(exc), 300)
        icons["last_error_at"] = _utc_now()
        return ""
    fresh: dict[str, str] = {}
    for sticker in response.get("result") or []:
        if not isinstance(sticker, dict):
            continue
        sticker_emoji = str(sticker.get("emoji") or "").strip()
        custom_emoji_id = str(sticker.get("custom_emoji_id") or "").strip()
        if sticker_emoji and custom_emoji_id and sticker_emoji not in fresh:
            fresh[sticker_emoji] = custom_emoji_id
    icons["by_emoji"] = fresh
    icons["fetched_at"] = _utc_now()
    icons.pop("last_error", None)
    icons.pop("last_error_at", None)
    return str(fresh.get(emoji) or "")


def drain_outbox(
    store: dict[str, Any],
    telegram: TelegramClient,
    tendwire: TendwireClient,
    *,
    chat_id: str,
    max_sends: int,
    dry_run: bool = False,
) -> dict[str, Any]:
    result = {"enabled": True, "polled": 0, "delivered": 0, "acked": 0, "failed": 0, "deferred": 0, "changed": False}
    if max_sends <= 0:
        return result
    poll = tendwire.connector_poll(limit=max_sends)
    if not poll.get("ok"):
        result.update({"changed": True, "status": poll.get("status") or "poll_failed"})
        return result
    items = [item for item in poll.get("items", []) if isinstance(item, dict)]
    result["polled"] = len(items)
    audit = store.setdefault("tendwire_outbox", {})
    delivered = audit.setdefault("delivered_identities", [])
    delivered_set = {str(item) for item in delivered}
    for item in items[:max_sends]:
        ref = str(item.get("ref") or "")
        payload = item.get("payload") if isinstance(item.get("payload"), dict) else {}
        identity = short_hash({"key": item.get("key"), "payload": payload}, 24)
        if identity in delivered_set:
            if ref and not dry_run:
                tendwire.connector_ack(ref, {"duplicate": True})
            result["acked"] += 1
            result["changed"] = True
            continue
        html = render_attention_notice(payload)
        thread_id = config.general_thread_id(store)
        sent = {"ok": True, "message_id": "0"} if dry_run else telegram.send_message(chat_id, html, thread_id=thread_id, notify=True)
        if not dry_run and not sent.get("ok") and thread_id and _topic_missing(sent.get("error")):
            sent = telegram.send_message(chat_id, html, notify=True)
            if sent.get("ok"):
                sent["fallback_reason"] = "general_thread_missing"
        if sent.get("ok"):
            result["delivered"] += 1
            result["acked"] += 1
            result["changed"] = True
            delivered.append(identity)
            delivered_set.add(identity)
            if ref and not dry_run:
                tendwire.connector_ack(ref, {"telegram": "delivered"})
        else:
            result["failed"] += 1
            result["changed"] = True
            recent = audit.setdefault("recent", [])
            if isinstance(recent, list):
                recent.append(
                    {
                        "status": "failed",
                        "event_type": str(payload.get("event_type") or ""),
                        "error": sanitize_text(sent.get("error") or "Telegram delivery failed", 300),
                        "attempt": item.get("attempt"),
                    }
                )
                audit["recent"] = recent[-50:]
            if ref and not dry_run:
                tendwire.connector_fail(ref, str(sent.get("error") or "Telegram delivery failed"))
    audit["delivered_identities"] = delivered[-200:]
    return result
