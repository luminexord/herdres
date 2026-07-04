#!/usr/bin/env python3
"""Tiny Telegram getUpdates gateway for source-only Herdres."""

from __future__ import annotations

import json
import os
import re
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime
from pathlib import Path
from typing import Any

from herdres_connector import config, speech, state
from herdres_connector.managed_bots import MANAGER_BOT_KIND, managed_bot_kind_for_key, managed_bot_kind_for_username, managed_bot_tokens
from herdres_connector.safe import sanitize_text, short_hash
from herdres_connector.telegram_delivery import TelegramClient

LONG_POLL_SECONDS = int(os.getenv("HERDRES_GATEWAY_LONG_POLL_SECONDS", "50"))
CHILD_POLL_SECONDS = int(os.getenv("HERDRES_GATEWAY_CHILD_POLL_SECONDS", "0"))
ERROR_BACKOFF = float(os.getenv("HERDRES_GATEWAY_NETWORK_ERROR_BACKOFF", "1.0"))
COMMAND_TIMEOUT = int(os.getenv("HERDRES_GATEWAY_COMMAND_TIMEOUT", "90"))
WORKER_RECONCILE_SECONDS = float(os.getenv("HERDRES_GATEWAY_WORKER_RECONCILE_SECONDS", "1.0"))
MENTION_RE = re.compile(r"@([A-Za-z0-9_]{3,64})")
PROCESSED_LOCK = threading.Lock()


def log(message: str) -> None:
    stamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{stamp}] [herdres-gateway] {message}", flush=True)


def _offset_path_for(key: str = MANAGER_BOT_KIND) -> Path:
    if key == MANAGER_BOT_KIND:
        return config.offset_path()
    safe = "".join(char if char.isalnum() or char in {"-", "_"} else "_" for char in str(key or "managed"))
    base = config.offset_path()
    return base.with_name(f"{base.name}.{safe}")


def _read_offset(key: str = MANAGER_BOT_KIND) -> int | None:
    path = _offset_path_for(key)
    try:
        return int(path.read_text(encoding="utf-8").strip() or "0")
    except Exception:
        return None


def _load_offset(key: str = MANAGER_BOT_KIND) -> int:
    return _read_offset(key) or 0


def _save_offset(offset: int, key: str = MANAGER_BOT_KIND) -> None:
    path = _offset_path_for(key)
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


def _reserve_processed(key: str) -> bool:
    with PROCESSED_LOCK:
        if key in _processed():
            return False
        _mark_processed(key)
        return True


def _api(token: str, method: str, payload: dict[str, Any]) -> dict[str, Any]:
    data = urllib.parse.urlencode({key: str(value) for key, value in payload.items() if value is not None}).encode()
    url = f"https://api.telegram.org/bot{token}/{method}"
    with urllib.request.urlopen(url, data=data, timeout=LONG_POLL_SECONDS + 15) as response:
        body = response.read().decode("utf-8", "replace")
    result = json.loads(body)
    if not result.get("ok"):
        raise RuntimeError(sanitize_text(result.get("description") or "Telegram API error", 300))
    return result


def get_updates(token: str, offset: int | None, *, timeout_seconds: int | None = None) -> list[dict[str, Any]]:
    payload: dict[str, Any] = {
        "timeout": LONG_POLL_SECONDS if timeout_seconds is None else max(0, int(timeout_seconds)),
        "allowed_updates": json.dumps(["message", "callback_query"], separators=(",", ":")),
    }
    if offset is not None:
        payload["offset"] = offset
    result = _api(
        token,
        "getUpdates",
        payload,
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


def _managed_bot_token_kinds(store: dict[str, Any]) -> set[str]:
    telegram = store.get("telegram") if isinstance(store.get("telegram"), dict) else {}
    return {kind for _key, kind, _token in managed_bot_tokens(telegram)}


def _mentioned_managed_bot_kind(store: dict[str, Any], text: str) -> str:
    telegram = store.get("telegram") if isinstance(store.get("telegram"), dict) else {}
    for match in MENTION_RE.finditer(str(text or "")):
        kind = managed_bot_kind_for_username(telegram, match.group(1))
        if kind:
            return kind
    return ""


def _reply_bot_kind_from_username(store: dict[str, Any], reply_to: dict[str, Any]) -> str:
    user = reply_to.get("from") if isinstance(reply_to, dict) else {}
    if not isinstance(user, dict):
        return ""
    telegram = store.get("telegram") if isinstance(store.get("telegram"), dict) else {}
    return managed_bot_kind_for_username(telegram, str(user.get("username") or ""))


def _reply_bot_kind_from_binding(store: dict[str, Any], reply_to: dict[str, Any], thread_id: str) -> str:
    binding = state.find_message_binding(store, reply_to.get("message_id"), topic_id=thread_id)
    if not binding:
        return ""
    kind = str(binding.get("bot_kind") or "").strip().lower()
    return kind if kind and kind != MANAGER_BOT_KIND else ""


def _explicit_target_bot_kind_for_message(store: dict[str, Any], message: dict[str, Any], text: str, thread_id: str) -> str:
    reply_to = message.get("reply_to_message") if isinstance(message.get("reply_to_message"), dict) else {}
    return (
        _reply_bot_kind_from_binding(store, reply_to, thread_id)
        or _reply_bot_kind_from_username(store, reply_to)
        or _mentioned_managed_bot_kind(store, text)
    )


def _drop(message: dict[str, Any], reason: str, detail: str = "") -> None:
    message_id = str(message.get("message_id") or "")
    suffix = f" ({detail})" if detail else ""
    log(f"drop message {message_id}: {reason}{suffix}")


def _payload_for_message(message: dict[str, Any], store: dict[str, Any], *, bot_key: str | None = None) -> dict[str, Any] | None:
    chat = message.get("chat") if isinstance(message.get("chat"), dict) else {}
    configured_chat = config.telegram_chat_id(store)
    if configured_chat and str(chat.get("id") or "") != configured_chat:
        return None
    thread_id = _message_thread_id(message, store)
    _key, entry = state.find_entry_by_thread(store, thread_id)
    if entry is None:
        _drop(message, "no_topic_entry", f"thread {thread_id}")
        return None
    user = message.get("from") if isinstance(message.get("from"), dict) else {}
    if not _owner_allowed(store, str(user.get("id") or ""), bool(user.get("is_bot"))):
        _drop(message, "sender_not_allowed")
        return None
    reply_to = message.get("reply_to_message") if isinstance(message.get("reply_to_message"), dict) else {}
    text = str(message.get("text") or "")
    caption = str(message.get("caption") or "")
    attachment = speech.voice_attachment_from_message(message)
    if not text and not caption and not attachment:
        return None
    target_bot_kind = _explicit_target_bot_kind_for_message(store, message, text or caption, thread_id)
    current_bot_kind = managed_bot_kind_for_key(bot_key) or MANAGER_BOT_KIND
    if current_bot_kind == MANAGER_BOT_KIND and target_bot_kind in _managed_bot_token_kinds(store):
        # Deferred to the targeted managed bot poller; only that poller may handle it.
        return None
    if current_bot_kind != MANAGER_BOT_KIND and target_bot_kind != current_bot_kind:
        return None
    payload = {
        "chat_id": str(chat.get("id") or ""),
        "topic_id": thread_id,
        "message_id": str(message.get("message_id") or ""),
        "reply_to_message_id": str(reply_to.get("message_id") or ""),
        "user_id": str(user.get("id") or ""),
        "from_bot": bool(user.get("is_bot")) if user else True,
        "text": text,
        "caption": caption,
    }
    if attachment:
        payload["attachment"] = attachment
    if target_bot_kind:
        payload["target_bot_kind"] = target_bot_kind
    return payload


def _delete_topic_icon_service_message(message: dict[str, Any], store: dict[str, Any], token: str) -> bool:
    if not config.delete_topic_icon_service_messages():
        return False
    edited = message.get("forum_topic_edited")
    if not isinstance(edited, dict) or "icon_custom_emoji_id" not in edited:
        return False
    chat = message.get("chat") if isinstance(message.get("chat"), dict) else {}
    chat_id = str(chat.get("id") or "")
    configured_chat = config.telegram_chat_id(store)
    if configured_chat and chat_id != configured_chat:
        return False
    message_id = str(message.get("message_id") or "")
    if not chat_id or not message_id:
        return False
    result = TelegramClient(token=token).delete_message(chat_id, message_id)
    if not result.get("ok"):
        log(f"topic icon service cleanup failed: {sanitize_text(result.get('error'), 160)}")
    return True


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


def handle_message(message: dict[str, Any], token: str, *, bot_key: str | None = None) -> None:
    store = state.load_state()
    if _delete_topic_icon_service_message(message, store, token):
        return
    payload = _payload_for_message(message, store, bot_key=bot_key)
    if payload is None:
        return
    key = short_hash({"message": payload.get("message_id"), "topic": payload.get("topic_id"), "text": payload.get("text") or payload.get("caption")}, 24)
    if not _reserve_processed(key):
        return
    payload = speech.pretranscribe_voice_payload(payload, bot_token=token)
    result = run_herdres_command(payload)
    reply = sanitize_text(result.get("reply"), 3500).strip()
    if reply:
        TelegramClient(token=token).send_message(
            str(payload["chat_id"]),
            reply,
            thread_id=str(payload["topic_id"]),
            reply_to_message_id=str(payload["message_id"]),
            notify=True,
        )


def handle_update(update: dict[str, Any], token: str, *, bot_key: str | None = None) -> None:
    message = update.get("message") if isinstance(update.get("message"), dict) else None
    if message is not None:
        handle_message(message, token, bot_key=bot_key)


def _drain_backlog(key: str, token: str) -> int | None:
    try:
        updates = get_updates(token, None, timeout_seconds=0)
    except Exception:
        return None
    if not updates:
        return None
    offset = int(updates[-1].get("update_id") or 0) + 1
    _save_offset(offset, key)
    log(f"drained {len(updates)} backlog update(s) for {key}")
    return offset


def _poll_once(key: str, token: str, *, timeout_seconds: int) -> None:
    offset = _read_offset(key)
    if offset is None:
        offset = _drain_backlog(key, token)
        if offset is not None:
            return
    for update in get_updates(token, offset, timeout_seconds=timeout_seconds):
        update_id = int(update.get("update_id") or 0)
        try:
            handle_update(update, token, bot_key=key)
        except Exception as exc:  # noqa: BLE001 - skip poison updates instead of re-fetching them forever
            log(f"update {update_id} failed for {key}: {type(exc).__name__}: {sanitize_text(str(exc), 200)}")
        if offset is None or update_id >= offset:
            offset = update_id + 1
            _save_offset(offset, key)


def _poll_worker(key: str, token: str, timeout_seconds: int, stop_event: threading.Event) -> None:
    while not stop_event.is_set():
        try:
            _poll_once(key, token, timeout_seconds=timeout_seconds)
        except Exception as exc:  # noqa: BLE001 - a dead poll thread silently drops inbound messages
            log(f"poll error for {key}: {type(exc).__name__}: {sanitize_text(str(exc), 200)}")
            time.sleep(ERROR_BACKOFF)


def _poll_specs(store: dict[str, Any], manager_token: str) -> list[tuple[str, str, int]]:
    telegram = store.get("telegram") if isinstance(store.get("telegram"), dict) else {}
    child_timeout = CHILD_POLL_SECONDS if CHILD_POLL_SECONDS > 0 else LONG_POLL_SECONDS
    specs = [(MANAGER_BOT_KIND, manager_token, LONG_POLL_SECONDS)]
    specs.extend((key, token, child_timeout) for key, _kind, token in managed_bot_tokens(telegram))
    return specs


def _reconcile_workers(workers: dict[str, dict[str, Any]], specs: list[tuple[str, str, int]]) -> None:
    desired = {key: (token, timeout) for key, token, timeout in specs}
    for key, worker in list(workers.items()):
        if key in desired and worker.get("token") == desired[key][0] and worker.get("timeout") == desired[key][1]:
            continue
        stop = worker.get("stop")
        if isinstance(stop, threading.Event):
            stop.set()
        workers.pop(key, None)
        log(f"poll worker stopped: {key}")
    for key, (token, timeout_seconds) in desired.items():
        if key in workers:
            continue
        stop = threading.Event()
        thread = threading.Thread(
            target=_poll_worker,
            args=(key, token, timeout_seconds, stop),
            name=f"herdres-gateway-{key}",
            daemon=True,
        )
        workers[key] = {"token": token, "timeout": timeout_seconds, "stop": stop, "thread": thread}
        thread.start()
        log(f"poll worker started: {key}")


def run() -> int:
    config.load_env_file()
    config.require_source_mode()
    token = config.telegram_token()
    if not token:
        log("Telegram bot token is not configured")
        return 1
    workers: dict[str, dict[str, Any]] = {}
    log("started")
    while True:
        try:
            store = state.load_state()
            _reconcile_workers(workers, _poll_specs(store, token))
            time.sleep(WORKER_RECONCILE_SECONDS)
        except KeyboardInterrupt:
            return 0
        except Exception as exc:  # noqa: BLE001
            log(f"gateway reconcile error: {sanitize_text(str(exc), 200)}")
            time.sleep(ERROR_BACKOFF)


if __name__ == "__main__":
    raise SystemExit(run())
