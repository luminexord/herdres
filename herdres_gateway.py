#!/usr/bin/env python3
"""Tiny Telegram getUpdates gateway for source-only Herdres."""

from __future__ import annotations

import json
import os
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime
from pathlib import Path
from typing import Any

from herdres_connector import config, state
from herdres_connector.safe import sanitize_text, short_hash
from herdres_connector.telegram_delivery import TelegramClient

LONG_POLL_SECONDS = int(os.getenv("HERDRES_GATEWAY_LONG_POLL_SECONDS", "50"))
ERROR_BACKOFF = float(os.getenv("HERDRES_GATEWAY_NETWORK_ERROR_BACKOFF", "1.0"))
COMMAND_TIMEOUT = int(os.getenv("HERDRES_GATEWAY_COMMAND_TIMEOUT", "90"))


def log(message: str) -> None:
    stamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{stamp}] [herdres-gateway] {message}", flush=True)


def _load_offset() -> int:
    path = config.offset_path()
    try:
        return int(path.read_text(encoding="utf-8").strip() or "0")
    except Exception:
        return 0


def _save_offset(offset: int) -> None:
    path = config.offset_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(str(int(offset)), encoding="utf-8")


def _processed() -> set[str]:
    path = config.processed_path()
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return set()
    if not isinstance(data, list):
        return set()
    return {str(item) for item in data[-2000:]}


def _mark_processed(key: str) -> None:
    path = config.processed_path()
    seen = list(_processed())
    seen.append(key)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(list(dict.fromkeys(seen))[-2000:]), encoding="utf-8")


def _api(token: str, method: str, payload: dict[str, Any]) -> dict[str, Any]:
    data = urllib.parse.urlencode({key: str(value) for key, value in payload.items() if value is not None}).encode()
    url = f"https://api.telegram.org/bot{token}/{method}"
    with urllib.request.urlopen(url, data=data, timeout=LONG_POLL_SECONDS + 15) as response:
        body = response.read().decode("utf-8", "replace")
    result = json.loads(body)
    if not result.get("ok"):
        raise RuntimeError(sanitize_text(result.get("description") or "Telegram API error", 300))
    return result


def get_updates(token: str, offset: int) -> list[dict[str, Any]]:
    result = _api(
        token,
        "getUpdates",
        {
            "offset": offset,
            "timeout": LONG_POLL_SECONDS,
            "allowed_updates": json.dumps(["message", "callback_query"], separators=(",", ":")),
        },
    )
    updates = result.get("result")
    return [item for item in updates if isinstance(item, dict)] if isinstance(updates, list) else []


def _message_thread_id(message: dict[str, Any], store: dict[str, Any]) -> str:
    if message.get("message_thread_id") is not None:
        return str(message.get("message_thread_id"))
    chat = message.get("chat") if isinstance(message.get("chat"), dict) else {}
    if chat.get("is_forum"):
        return config.general_thread_id(store)
    return ""


def _owner_allowed(store: dict[str, Any], user_id: str, from_bot: bool) -> bool:
    if from_bot:
        return False
    telegram = store.get("telegram") if isinstance(store.get("telegram"), dict) else {}
    owners = {str(item) for item in telegram.get("owner_user_ids", [])}
    if not owners:
        return True
    return str(user_id) in owners


def _payload_for_message(message: dict[str, Any], store: dict[str, Any]) -> dict[str, Any] | None:
    chat = message.get("chat") if isinstance(message.get("chat"), dict) else {}
    configured_chat = config.telegram_chat_id(store)
    if configured_chat and str(chat.get("id") or "") != configured_chat:
        return None
    thread_id = _message_thread_id(message, store)
    _key, entry = state.find_entry_by_thread(store, thread_id)
    if entry is None:
        return None
    user = message.get("from") if isinstance(message.get("from"), dict) else {}
    if not _owner_allowed(store, str(user.get("id") or ""), bool(user.get("is_bot"))):
        return None
    reply_to = message.get("reply_to_message") if isinstance(message.get("reply_to_message"), dict) else {}
    return {
        "chat_id": str(chat.get("id") or ""),
        "topic_id": thread_id,
        "message_id": str(message.get("message_id") or ""),
        "reply_to_message_id": str(reply_to.get("message_id") or ""),
        "user_id": str(user.get("id") or ""),
        "from_bot": bool(user.get("is_bot")) if user else True,
        "text": str(message.get("text") or ""),
        "caption": str(message.get("caption") or ""),
    }


def _script_path() -> str:
    return os.getenv("HERDR_TELEGRAM_TOPICS_SCRIPT", str(Path.home() / ".local/bin/herdres"))


def run_herdres_command(payload: dict[str, Any]) -> dict[str, Any]:
    try:
        proc = subprocess.run(
            [_script_path(), "command"],
            input=json.dumps(payload, separators=(",", ":")).encode("utf-8"),
            capture_output=True,
            timeout=COMMAND_TIMEOUT,
            check=False,
        )
    except subprocess.TimeoutExpired:
        return {"handled": True, "reply": "Herdres command timed out before delivery."}
    except Exception as exc:  # noqa: BLE001
        return {"handled": True, "reply": f"Herdres command failed: {sanitize_text(str(exc), 160)}"}
    try:
        data = json.loads(proc.stdout.decode("utf-8", "replace") or "{}")
    except json.JSONDecodeError:
        return {"handled": True, "reply": "Herdres returned an unreadable command result."}
    return data if isinstance(data, dict) else {"handled": True}


def handle_message(message: dict[str, Any], token: str) -> None:
    store = state.load_state()
    payload = _payload_for_message(message, store)
    if payload is None:
        return
    key = short_hash({"message": payload.get("message_id"), "topic": payload.get("topic_id"), "text": payload.get("text") or payload.get("caption")}, 24)
    if key in _processed():
        return
    result = run_herdres_command(payload)
    _mark_processed(key)
    reply = sanitize_text(result.get("reply"), 3500).strip()
    if reply:
        TelegramClient(token=token).send_message(
            str(payload["chat_id"]),
            reply,
            thread_id=str(payload["topic_id"]),
            reply_to_message_id=str(payload["message_id"]),
            notify=True,
        )


def handle_update(update: dict[str, Any], token: str) -> None:
    message = update.get("message") if isinstance(update.get("message"), dict) else None
    if message is not None:
        handle_message(message, token)


def run() -> int:
    config.load_env_file()
    config.require_source_mode()
    token = config.telegram_token()
    if not token:
        log("Telegram bot token is not configured")
        return 1
    offset = _load_offset()
    log("started")
    while True:
        try:
            for update in get_updates(token, offset):
                update_id = int(update.get("update_id") or 0)
                handle_update(update, token)
                if update_id >= offset:
                    offset = update_id + 1
                    _save_offset(offset)
        except KeyboardInterrupt:
            return 0
        except (urllib.error.URLError, TimeoutError, RuntimeError) as exc:
            log(f"poll error: {sanitize_text(str(exc), 200)}")
            time.sleep(ERROR_BACKOFF)


if __name__ == "__main__":
    raise SystemExit(run())
