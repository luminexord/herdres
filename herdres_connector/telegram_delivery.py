"""Telegram API and connector-outbox delivery for source-only Herdres."""

from __future__ import annotations

import json
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
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
        plain = html_to_plain(sanitize_text(html_text, 12000))
        if len(sanitize_text(html_text, 12000)) > MESSAGE_TEXT_LIMIT and plain:
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
