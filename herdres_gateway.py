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

from herdres_connector import config, decisions, ingress_requests, speech, state
from herdres_connector.ingress_identity import derive_telegram_request_id, load_request_id_key, validate_request_id
from herdres_connector.managed_bots import MANAGER_BOT_KIND, managed_bot_kind_for_key, managed_bot_kind_for_username, managed_bot_tokens
from herdres_connector.safe import sanitize_text
from herdres_connector.telegram_delivery import TelegramClient
from herdres_connector.tendwire_client import TendwireClient

LONG_POLL_SECONDS = int(os.getenv("HERDRES_GATEWAY_LONG_POLL_SECONDS", "50"))
CHILD_POLL_SECONDS = int(os.getenv("HERDRES_GATEWAY_CHILD_POLL_SECONDS", "0"))
ERROR_BACKOFF = float(os.getenv("HERDRES_GATEWAY_NETWORK_ERROR_BACKOFF", "1.0"))
COMMAND_TIMEOUT = int(os.getenv("HERDRES_GATEWAY_COMMAND_TIMEOUT", "90"))
WORKER_RECONCILE_SECONDS = float(os.getenv("HERDRES_GATEWAY_WORKER_RECONCILE_SECONDS", "1.0"))
MENTION_RE = re.compile(r"@([A-Za-z0-9_]{3,64})")
CHECKPOINT_ADVANCE = "advance"
CHECKPOINT_RETRY = "retry"
_CHILD_SCHEMA_VERSION = 1
_CHILD_RESPONSE_FIELDS = frozenset(
    {
        "schema_version",
        "handled",
        "request_id",
        "checkpoint",
        "disposition",
        "reply",
    }
)
_COMMAND_DISPOSITIONS = frozenset(
    {
        "no_receipt",
        "in_progress",
        "terminal_accepted",
        "terminal_rejected",
        "terminal_uncertain",
    }
)
_RETRY_DISPOSITIONS = frozenset({"no_receipt", "in_progress"})
_CHILD_REPLY_LIMIT = 160


def log(message: str) -> None:
    stamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{stamp}] [herdres-gateway] {message}", flush=True)


def _legacy_offset_path_for(key: str = MANAGER_BOT_KIND) -> Path:
    if key == MANAGER_BOT_KIND:
        return config.offset_path()
    safe = "".join(
        char if char.isalnum() or char in {"-", "_"} else "_"
        for char in str(key or "managed")
    )
    base = config.offset_path()
    return base.with_name(f"{base.name}.{safe}")


def _offset_path_for(key: str = MANAGER_BOT_KIND) -> Path:
    receiver_kind = managed_bot_kind_for_key(key) or str(key or MANAGER_BOT_KIND)
    return _legacy_offset_path_for(receiver_kind)


def _read_offset_checkpoint(path: Path) -> int:
    try:
        raw = path.read_text(encoding="utf-8").strip()
    except OSError as exc:
        raise RuntimeError("offset checkpoint could not be read") from exc
    if not raw or re.fullmatch(r"[0-9]+", raw) is None:
        raise RuntimeError("offset checkpoint is corrupt")
    return int(raw)


def _legacy_offset_paths_for_managed_kind(key: str) -> list[Path]:
    receiver_kind = managed_bot_kind_for_key(key)
    if not receiver_kind:
        return []
    base = config.offset_path()
    prefix = f"{base.name}.managed-{receiver_kind}-"
    try:
        candidates = sorted(
            (candidate for candidate in base.parent.iterdir() if candidate.name.startswith(prefix)),
            key=lambda candidate: candidate.name,
        )
    except FileNotFoundError:
        return []
    valid_name = re.compile(
        rf"^{re.escape(prefix)}[0-9a-f]{{12}}$"
    )
    if any(valid_name.fullmatch(candidate.name) is None for candidate in candidates):
        raise RuntimeError(
            f"legacy offset evidence is ambiguous for managed kind {receiver_kind}"
        )
    return candidates


def _migrate_legacy_managed_offsets(key: str) -> int | None:
    legacy_paths = _legacy_offset_paths_for_managed_kind(key)
    if not legacy_paths:
        return None
    checkpoints = [_read_offset_checkpoint(legacy) for legacy in legacy_paths]
    checkpoint = min(checkpoints)
    _save_offset(checkpoint, key)
    for legacy in legacy_paths:
        legacy.unlink()
    return checkpoint




def _read_offset(key: str = MANAGER_BOT_KIND) -> int | None:
    path = _offset_path_for(key)
    if path.exists() or path.is_symlink():
        return _read_offset_checkpoint(path)
    return _migrate_legacy_managed_offsets(key)


def _load_offset(key: str = MANAGER_BOT_KIND) -> int:
    return _read_offset(key) or 0


def _save_offset(offset: int, key: str = MANAGER_BOT_KIND) -> None:
    path = _offset_path_for(key)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(str(int(offset)), encoding="utf-8")




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


def _private_retry_child_result(request_id: str) -> dict[str, Any]:
    return {
        "schema_version": _CHILD_SCHEMA_VERSION,
        "handled": True,
        "request_id": request_id,
        "checkpoint": CHECKPOINT_RETRY,
        "disposition": None,
        "reply": "",
    }


def _validated_child_response(
    value: Any,
    *,
    request_id: str,
) -> dict[str, Any] | None:
    if (
        not isinstance(value, dict)
        or set(value) != _CHILD_RESPONSE_FIELDS
        or type(value.get("schema_version")) is not int
        or value["schema_version"] != _CHILD_SCHEMA_VERSION
        or type(value.get("handled")) is not bool
        or value.get("request_id") != request_id
        or value.get("checkpoint") not in {CHECKPOINT_RETRY, CHECKPOINT_ADVANCE}
        or (
            value.get("disposition") is not None
            and value.get("disposition") not in _COMMAND_DISPOSITIONS
        )
        or not isinstance(value.get("reply"), str)
        or sanitize_text(value["reply"], _CHILD_REPLY_LIMIT) != value["reply"]
    ):
        return None
    checkpoint = value["checkpoint"]
    disposition = value["disposition"]
    if checkpoint == CHECKPOINT_RETRY:
        if disposition not in _RETRY_DISPOSITIONS and disposition is not None:
            return None
        if value["reply"]:
            return None
        return value
    if disposition in _RETRY_DISPOSITIONS:
        return None
    if value["handled"] is False and (
        disposition is not None or value["reply"]
    ):
        return None
    return value


def run_herdres_command(payload: dict[str, Any]) -> dict[str, Any]:
    try:
        request_id = validate_request_id(payload.get("request_id"))
    except ValueError:
        return _private_retry_child_result("")
    try:
        proc = subprocess.run(
            [_script_path(), "command"],
            input=json.dumps(payload, separators=(",", ":")).encode("utf-8"),
            capture_output=True,
            timeout=COMMAND_TIMEOUT,
            check=False,
        )
    except Exception:  # noqa: BLE001 - any child-start/result loss is private ambiguity
        # Once process creation is attempted, the parent cannot prove whether
        # the child durably called Tendwire. Keep this evidence private and
        # retry only through the same durable request ID.
        return _private_retry_child_result(request_id)
    if proc.returncode != 0:
        return _private_retry_child_result(request_id)
    try:
        data = json.loads(proc.stdout.decode("utf-8") or "{}")
    except (UnicodeDecodeError, json.JSONDecodeError):
        return _private_retry_child_result(request_id)
    validated = _validated_child_response(data, request_id=request_id)
    return validated if validated is not None else _private_retry_child_result(request_id)


def _checkpoint_for_command_result(
    result: dict[str, Any],
    *,
    request_id: str,
) -> str:
    validated = _validated_child_response(result, request_id=request_id)
    return (
        str(validated["checkpoint"])
        if validated is not None
        else CHECKPOINT_RETRY
    )


def _preflight_ingress_request(
    request_id: str,
    *,
    now: float | None = None,
) -> tuple[dict[str, Any], dict[str, Any] | None]:
    timestamp = time.time() if now is None else float(now)
    retry_horizon = config.command_retry_horizon_seconds()
    retention = config.command_request_retention_seconds()
    with state.state_lock():
        store = state.load_state()
        changed = ingress_requests.prune_requests(store, now=timestamp)
        record, outcome, preflight_changed = ingress_requests.preflight_request(
            store,
            request_id,
            now=timestamp,
            retry_horizon=retry_horizon,
            retention=retention,
        )
        if changed or preflight_changed:
            state.save_state(store)
    return record, outcome


def handle_message(
    message: dict[str, Any],
    token: str,
    *,
    update_id: int,
    receiver_id: str,
    request_id_key: bytes,
    bot_key: str | None = None,
) -> str:
    chat = message.get("chat") if isinstance(message.get("chat"), dict) else {}
    try:
        request_id = derive_telegram_request_id(
            request_id_key,
            receiver_id=receiver_id,
            update_id=update_id,
            chat_id=chat.get("id"),
            message_id=message.get("message_id"),
        )
    except ValueError:
        _drop(message, "invalid_ingress_identity")
        return CHECKPOINT_ADVANCE

    # The immutable first-seen/deadline/retention shell is fsynced before
    # routing, voice transcription, or child-process creation. A terminal or
    # expired redelivery is answered from its cached local outcome and never
    # creates a child that could call Tendwire.
    record, cached_outcome = _preflight_ingress_request(request_id)
    if cached_outcome is not None:
        result = cached_outcome
    else:
        store = state.load_state()
        if _delete_topic_icon_service_message(message, store, token):
            command_payload = {"request_id": request_id}
        elif isinstance(record.get("request_json"), str):
            command_payload = {"request_id": request_id}
        else:
            payload = _payload_for_message(message, store, bot_key=bot_key)
            if payload is None:
                command_payload = {"request_id": request_id}
            else:
                payload["request_id"] = request_id
                command_payload = speech.pretranscribe_voice_payload(
                    payload,
                    bot_token=token,
                )
        result = run_herdres_command(command_payload)
    checkpoint = _checkpoint_for_command_result(
        result,
        request_id=request_id,
    )
    if checkpoint == CHECKPOINT_RETRY:
        return checkpoint
    reply = result["reply"].strip()
    if not reply:
        return CHECKPOINT_ADVANCE
    try:
        TelegramClient(token=token).send_message(
            str(chat.get("id") or ""),
            reply,
            thread_id=_message_thread_id(message, state.load_state()),
            reply_to_message_id=str(message.get("message_id") or ""),
            notify=True,
        )
    except Exception:  # noqa: BLE001 - notification is best-effort after terminalization
        return CHECKPOINT_ADVANCE
    return CHECKPOINT_ADVANCE


def handle_callback_query(
    query: dict[str, Any],
    token: str,
    *,
    update_id: int,
    receiver_id: str,
    request_id_key: bytes,
) -> str:
    """Handle one inline decision callback and always dismiss its spinner."""

    callback_id = str(query.get("id") or "")
    telegram = TelegramClient(token=token)
    toast = "Unknown action."
    checkpoint = CHECKPOINT_ADVANCE
    try:
        message = query.get("message") if isinstance(query.get("message"), dict) else {}
        chat = message.get("chat") if isinstance(message.get("chat"), dict) else {}
        user = query.get("from") if isinstance(query.get("from"), dict) else {}
        with state.state_lock():
            store = state.load_state()
            configured_chat = config.telegram_chat_id(store)
            if configured_chat and str(chat.get("id") or "") != configured_chat:
                toast = "This button is not available here."
            elif not _owner_allowed(
                store,
                str(user.get("id") or ""),
                bool(user.get("is_bot")),
            ):
                toast = "You are not allowed to answer this prompt."
            elif not config.remote_decisions_enabled():
                toast = "Remote decisions are disabled."
            else:
                data = str(query.get("data") or "")
                if data.startswith(decisions.CALLBACK_PREFIX):
                    request_id = derive_telegram_request_id(
                        request_id_key,
                        receiver_id=receiver_id,
                        update_id=update_id,
                        chat_id=chat.get("id"),
                        message_id=message.get("message_id"),
                    )
                    result = decisions.handle_callback(
                        store,
                        callback_data=data,
                        topic_id=_message_thread_id(message, store),
                        chat_id=str(chat.get("id") or ""),
                        request_id=request_id,
                        telegram=telegram,
                        tendwire=TendwireClient(),
                    )
                    toast = sanitize_text(result.get("toast") or "Done.", 180)
                    if result.get("changed"):
                        state.save_state(store)
                else:
                    toast = "Unknown action."
    except Exception as exc:  # noqa: BLE001 - preserve the update across uncertain local failure
        toast = "Could not process that choice."
        # Retain the update on unexpected state/transport loss. A submit retry
        # reuses the same derived request ID, so Tendwire can deduplicate it.
        checkpoint = CHECKPOINT_RETRY
        log(f"callback failed: {type(exc).__name__}: {sanitize_text(str(exc), 160)}")
    finally:
        if callback_id:
            try:
                telegram.answer_callback_query(callback_id, toast)
            except Exception as exc:  # noqa: BLE001 - answering the toast is best effort
                log(f"answerCallbackQuery failed: {sanitize_text(str(exc), 160)}")
    return checkpoint


def handle_update(
    update: dict[str, Any],
    token: str,
    *,
    receiver_id: str,
    request_id_key: bytes,
    bot_key: str | None = None,
) -> str:
    update_id = update.get("update_id")
    if type(update_id) is not int or update_id < 0:
        log("drop update: invalid_update_id")
        return CHECKPOINT_ADVANCE
    callback_query = (
        update.get("callback_query")
        if isinstance(update.get("callback_query"), dict)
        else None
    )
    if callback_query is not None:
        return handle_callback_query(
            callback_query,
            token,
            update_id=update_id,
            receiver_id=receiver_id,
            request_id_key=request_id_key,
        )
    message = update.get("message") if isinstance(update.get("message"), dict) else None
    if message is None:
        return CHECKPOINT_ADVANCE
    return handle_message(
        message,
        token,
        update_id=update_id,
        receiver_id=receiver_id,
        request_id_key=request_id_key,
        bot_key=bot_key,
    )


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


def _receiver_id_for_key(key: str) -> str:
    return managed_bot_kind_for_key(key) or key


def _poll_once(
    key: str,
    token: str,
    *,
    timeout_seconds: int,
    request_id_key: bytes,
) -> None:
    offset = _read_offset(key)
    if offset is None:
        offset = _drain_backlog(key, token)
        if offset is not None:
            return
    for update in get_updates(token, offset, timeout_seconds=timeout_seconds):
        raw_update_id = update.get("update_id")
        if type(raw_update_id) is not int or raw_update_id < 0:
            log(f"invalid update id for {key}; offset retained")
            break
        update_id = raw_update_id
        try:
            checkpoint = handle_update(
                update,
                token,
                receiver_id=_receiver_id_for_key(key),
                request_id_key=request_id_key,
                bot_key=key,
            )
        except Exception as exc:  # noqa: BLE001 - redelivery keeps the same opaque request identity
            log(f"update {update_id} failed for {key}: {type(exc).__name__}: {sanitize_text(str(exc), 200)}")
            break
        if checkpoint != CHECKPOINT_ADVANCE:
            log(f"update {update_id} retained for {key}: {checkpoint}")
            break
        if offset is None or update_id >= offset:
            offset = update_id + 1
            _save_offset(offset, key)


def _poll_worker(
    key: str,
    token: str,
    timeout_seconds: int,
    request_id_key: bytes,
    stop_event: threading.Event,
) -> None:
    while not stop_event.is_set():
        try:
            _poll_once(
                key,
                token,
                timeout_seconds=timeout_seconds,
                request_id_key=request_id_key,
            )
        except Exception as exc:  # noqa: BLE001 - a dead poll thread silently drops inbound messages
            log(f"poll error for {key}: {type(exc).__name__}: {sanitize_text(str(exc), 200)}")
            time.sleep(ERROR_BACKOFF)


def _poll_specs(store: dict[str, Any], manager_token: str) -> list[tuple[str, str, int]]:
    telegram = store.get("telegram") if isinstance(store.get("telegram"), dict) else {}
    child_timeout = CHILD_POLL_SECONDS if CHILD_POLL_SECONDS > 0 else LONG_POLL_SECONDS
    specs = [(MANAGER_BOT_KIND, manager_token, LONG_POLL_SECONDS)]
    specs.extend((key, token, child_timeout) for key, _kind, token in managed_bot_tokens(telegram))
    return specs


def _reconcile_workers(
    workers: dict[str, dict[str, Any]],
    specs: list[tuple[str, str, int]],
    request_id_key: bytes,
) -> None:
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
            args=(key, token, timeout_seconds, request_id_key, stop),
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
    try:
        request_id_key = load_request_id_key()
    except RuntimeError:
        log("Herdres request identity key is missing or unsafe")
        return 1
    workers: dict[str, dict[str, Any]] = {}
    log("started")
    while True:
        try:
            store = state.load_state()
            _reconcile_workers(
                workers,
                _poll_specs(store, token),
                request_id_key,
            )
            time.sleep(WORKER_RECONCILE_SECONDS)
        except KeyboardInterrupt:
            return 0
        except Exception as exc:  # noqa: BLE001
            log(f"gateway reconcile error: {sanitize_text(str(exc), 200)}")
            time.sleep(ERROR_BACKOFF)


if __name__ == "__main__":
    raise SystemExit(run())
