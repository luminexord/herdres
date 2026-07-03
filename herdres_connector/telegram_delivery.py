"""Telegram API and connector-outbox delivery for source-only Herdres."""

from __future__ import annotations

import json
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from typing import Any

from . import config, state
from .rendering import html_to_plain, render_attention_notice
from .safe import sanitize_text, short_hash
from .tendwire_client import TendwireClient


class TelegramError(RuntimeError):
    pass


class RateLimited(TelegramError):
    def __init__(self, retry_after: int, message: str = "Telegram rate limited") -> None:
        super().__init__(message)
        self.retry_after = max(1, int(retry_after or 1))


@dataclass(frozen=True)
class TelegramClient:
    token: str
    timeout: float = 20.0
    dry_run: bool = False

    def configured(self) -> bool:
        return bool(str(self.token or "").strip())

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

    def send_message(
        self,
        chat_id: str,
        html_text: str,
        *,
        thread_id: str | int | None = None,
        reply_to_message_id: str | int | None = None,
        notify: bool = False,
    ) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "chat_id": chat_id,
            "text": sanitize_text(html_text, 3900),
            "parse_mode": "HTML",
            "disable_web_page_preview": "true",
            "disable_notification": "false" if notify else "true",
        }
        if thread_id:
            payload["message_thread_id"] = str(thread_id)
        if reply_to_message_id:
            payload["reply_parameters"] = json.dumps({"message_id": int(reply_to_message_id)}, separators=(",", ":"))
        try:
            result = self.api("sendMessage", payload).get("result") or {}
            return {"ok": True, "message_id": str(result.get("message_id") or "0")}
        except TelegramError as exc:
            fallback = html_to_plain(html_text)
            if fallback and fallback != html_text:
                plain_payload = dict(payload)
                plain_payload.pop("parse_mode", None)
                plain_payload["text"] = sanitize_text(fallback, 3900)
                try:
                    result = self.api("sendMessage", plain_payload).get("result") or {}
                    return {"ok": True, "message_id": str(result.get("message_id") or "0"), "format": "plain"}
                except TelegramError:
                    pass
            return {"ok": False, "error": sanitize_text(str(exc), 300)}

    def edit_message(self, chat_id: str, message_id: str | int, html_text: str) -> dict[str, Any]:
        payload = {
            "chat_id": chat_id,
            "message_id": str(message_id),
            "text": sanitize_text(html_text, 3900),
            "parse_mode": "HTML",
            "disable_web_page_preview": "true",
        }
        try:
            self.api("editMessageText", payload)
            return {"ok": True, "message_id": str(message_id), "kind": "edited"}
        except TelegramError as exc:
            text = str(exc)
            if "message is not modified" in text.lower():
                return {"ok": True, "message_id": str(message_id), "kind": "unchanged"}
            return {"ok": False, "error": sanitize_text(text, 300)}

    def create_topic(self, chat_id: str, name: str) -> dict[str, Any]:
        payload = {"chat_id": chat_id, "name": sanitize_text(name, 128)}
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

    def pin_message(self, chat_id: str, message_id: str | int) -> dict[str, Any]:
        try:
            self.api("pinChatMessage", {"chat_id": chat_id, "message_id": str(message_id), "disable_notification": "true"})
            return {"ok": True}
        except TelegramError as exc:
            return {"ok": False, "error": sanitize_text(str(exc), 300)}


def topic_icon_id(store: dict[str, Any], emoji: str) -> str:
    telegram = store.get("telegram") if isinstance(store.get("telegram"), dict) else {}
    icons = telegram.get("forum_topic_icons") if isinstance(telegram.get("forum_topic_icons"), dict) else {}
    by_emoji = icons.get("by_emoji") if isinstance(icons.get("by_emoji"), dict) else {}
    return str(by_emoji.get(emoji) or "")


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
        sent = {"ok": True, "message_id": "0"} if dry_run else telegram.send_message(chat_id, html, thread_id=config.general_thread_id(store), notify=True)
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
            if ref and not dry_run:
                tendwire.connector_fail(ref, str(sent.get("error") or "Telegram delivery failed"))
    audit["delivered_identities"] = delivered[-200:]
    return result
