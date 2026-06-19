#!/usr/bin/env python3
"""Standalone inbound gateway for Herdres (macOS / no-Hermes deployments).

The upstream design routes inbound Telegram pane-topic control through the Hermes
Telegram gateway, hooked in via ``herdr_topic_bridge.py`` and a systemd unit. That
assumes Hermes already long-polls the same bot. When Herdres owns its own bot and
there is no other ``getUpdates`` consumer (the common macOS case), this script is a
drop-in replacement for that role.

It long-polls ``getUpdates`` and, for messages/callbacks that fall inside a mapped
Herdr pane topic, pipes the exact same JSON payload contract to ``herdres command``
/ ``herdres callback`` on stdin, then delivers the reply. It is stdlib-only and does
not import Herdres or Hermes.

Safe to run alongside the outbound ``herdres sync`` timer and the ``herdres event``
plugin: those only *send*, they never consume ``getUpdates``.
"""

from __future__ import annotations

import json
import hashlib
import os
import re
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

HOME = Path.home()
STATE_PATH = Path(
    os.getenv("HERDR_TELEGRAM_TOPICS_STATE", str(HOME / ".local/share/herdres/state.json"))
).expanduser()
SCRIPT_PATH = Path(
    os.getenv("HERDR_TELEGRAM_TOPICS_SCRIPT", str(HOME / ".local/bin/herdres"))
).expanduser()
OFFSET_PATH = Path(
    os.getenv("HERDR_TELEGRAM_TOPICS_GATEWAY_OFFSET", str(HOME / ".local/share/herdres/gateway_offset"))
).expanduser()
GENERAL_THREAD_ID = os.getenv("HERDR_TELEGRAM_TOPICS_GENERAL_THREAD_ID", "1")
AMBIGUOUS_PANE_THREAD_REPLY = "Reply inside a pane thread so I know which Herdr pane to control."
MANAGED_BOT_SUGGESTED_USERNAMES = {
    "codex": "herdr_codex_bot",
    "claude": "herdr_claude_bot",
    "kimi": "herdr_kimi_bot",
    "omp": "herdr_omp_bot",
}
MANAGED_BOT_KEY_RE = re.compile(r"^managed-([a-z0-9_]+)-")
MANAGED_BOT_MENTION_RE = re.compile(r"@([A-Za-z0-9_]{3,64})")

LONG_POLL_SECONDS = int(os.getenv("HERDRES_GATEWAY_LONG_POLL_SECONDS", "50"))
MANAGER_POLL_WITH_CHILDREN_SECONDS = int(
    os.getenv("HERDRES_GATEWAY_MANAGER_POLL_SECONDS", "1")
)
CHILD_POLL_SECONDS = int(os.getenv("HERDRES_GATEWAY_CHILD_POLL_SECONDS", "0"))
SOCKET_TIMEOUT = LONG_POLL_SECONDS + 15
COMMAND_TIMEOUT = 30
ERROR_BACKOFF = 3
ALLOWED_UPDATES = json.dumps(["message", "callback_query", "managed_bot"])

TOKEN = ""
DEBUG = os.getenv("HERDRES_GATEWAY_DEBUG", "").strip().lower() not in ("", "0", "false", "no")
CLEARED_WEBHOOK_KEYS: set[str] = set()
TRACE_PATH = Path(
    os.getenv("HERDRES_GATEWAY_TRACE", str(HOME / ".local/share/herdres/gateway.trace.log"))
).expanduser()


def _emit(line: str) -> None:
    # stdout (may be block-buffered under launchd) + an always-flushed trace file.
    print(line, flush=True)
    try:
        with open(TRACE_PATH, "a", encoding="utf-8") as fh:
            fh.write(line + "\n")
    except Exception:
        pass


def log(msg: str) -> None:
    _emit(f"[herdres-gateway] {msg}")


def dlog(msg: str) -> None:
    if DEBUG:
        _emit(f"[herdres-gateway] DEBUG {msg}")


def _token() -> str:
    tok = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
    if tok:
        return tok
    # Fallback: parse the herdres env file directly.
    env_file = Path(
        os.getenv("HERDRES_ENV_FILE", str(HOME / ".config/herdres/herdres.env"))
    ).expanduser()
    try:
        for line in env_file.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if line.startswith("TELEGRAM_BOT_TOKEN="):
                return line.split("=", 1)[1].strip()
    except Exception:
        pass
    return ""


def api(method: str, params: dict | None = None, timeout: float = 30, *, token: str | None = None) -> dict:
    api_token = token or TOKEN
    url = f"https://api.telegram.org/bot{api_token}/{method}"
    data = urllib.parse.urlencode(params or {}).encode("utf-8") if params else None
    req = urllib.request.Request(url, data=data)
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode("utf-8"))


# ---------------------------------------------------------------------------
# State + mapping helpers (dict mirror of herdr_topic_bridge.py)
# ---------------------------------------------------------------------------

def load_state() -> dict | None:
    try:
        data = json.loads(STATE_PATH.read_text(encoding="utf-8"))
    except Exception:
        return None
    if not isinstance(data, dict) or data.get("version") != 1 or not data.get("enabled", True):
        return None
    return data


def managed_bot_tokens(state: dict | None = None) -> list[tuple[str, str]]:
    current = state if state is not None else load_state()
    if not current:
        return []
    telegram = current.get("telegram") if isinstance(current.get("telegram"), dict) else {}
    bots = telegram.get("managed_bots") if isinstance(telegram.get("managed_bots"), dict) else {}
    records: list[tuple[str, str]] = []
    for kind, record in bots.items():
        if not isinstance(record, dict) or record.get("enabled") is False:
            continue
        token = str(record.get("token") or "").strip()
        if not token:
            continue
        digest = hashlib.sha1(token.encode("utf-8")).hexdigest()[:12]
        records.append((f"managed-{kind}-{digest}", token))
    return records


def managed_bot_kind_for_key(key: str | None) -> str:
    match = MANAGED_BOT_KEY_RE.match(str(key or ""))
    if not match:
        return ""
    kind = match.group(1)
    if kind in MANAGED_BOT_SUGGESTED_USERNAMES:
        return kind
    return ""


def managed_bot_kind_for_username(state: dict, username: str) -> str:
    clean_username = str(username or "").strip().lstrip("@").lower()
    if not clean_username:
        return ""
    telegram = state.get("telegram") if isinstance(state.get("telegram"), dict) else {}
    bots = telegram.get("managed_bots") if isinstance(telegram.get("managed_bots"), dict) else {}
    for kind, suggested in MANAGED_BOT_SUGGESTED_USERNAMES.items():
        candidates = {suggested}
        record = bots.get(kind) if isinstance(bots, dict) else None
        if isinstance(record, dict):
            candidates.add(str(record.get("username") or "").strip().lstrip("@").lower())
        if clean_username in candidates:
            return kind
    return ""


def mentioned_managed_bot_kind(state: dict, text: str) -> str:
    for match in MANAGED_BOT_MENTION_RE.finditer(str(text or "")):
        kind = managed_bot_kind_for_username(state, match.group(1))
        if kind:
            return kind
    return ""


def target_bot_kind_for_message(state: dict, text: str, bot_key: str | None) -> str:
    key_kind = managed_bot_kind_for_key(bot_key)
    if key_kind:
        return key_kind
    return mentioned_managed_bot_kind(state, text)


def thread_id_of(message: dict) -> str | None:
    tid = message.get("message_thread_id")
    if tid is not None:
        return str(tid)
    if (message.get("chat") or {}).get("is_forum"):
        return GENERAL_THREAD_ID
    return None


def topic_space_entry(state: dict, chat_id: str, thread_id: str | None) -> tuple[str, dict] | None:
    telegram = state.get("telegram") or {}
    if str(chat_id) != str(telegram.get("chat_id") or ""):
        return None
    if not thread_id or thread_id == str(telegram.get("general_thread_id", GENERAL_THREAD_ID)):
        return None
    for key, space in (state.get("spaces") or {}).items():
        if isinstance(space, dict) and str(space.get("topic_id") or "") == str(thread_id):
            return str(key), space
    return None


def live_entries_for_space(state: dict, space: dict) -> list[tuple[str, dict]]:
    panes = state.get("panes") if isinstance(state.get("panes"), dict) else {}
    entries = []
    for pane_key in space.get("pane_keys") or []:
        key = str(pane_key)
        entry = panes.get(key)
        if isinstance(entry, dict) and str(entry.get("last_known_status") or "").lower() != "closed":
            entries.append((key, entry))
    return entries


def route_message_entry(state: dict, chat_id: str, thread_id: str | None, message_id: str | int | None) -> tuple[str, dict] | None:
    message_key = str(message_id or "").strip()
    if not message_key:
        return None
    mapped_space = topic_space_entry(state, chat_id, thread_id)
    if not mapped_space:
        return None
    _space_key, space = mapped_space
    panes = state.get("panes") if isinstance(state.get("panes"), dict) else {}
    routes = space.get("message_routes") if isinstance(space.get("message_routes"), dict) else {}
    routed_key = str(routes.get(message_key) or "")
    routed_entry = panes.get(routed_key)
    if routed_key and isinstance(routed_entry, dict):
        return routed_key, routed_entry
    for pane_key, entry in live_entries_for_space(state, space):
        if str(entry.get("pane_root_message_id") or "") == message_key:
            return pane_key, entry
    return None


def resolve_mapped_entry(
    state: dict,
    chat_id: str,
    thread_id: str | None,
    *,
    message_id: str | int | None = None,
    reply_to_message_id: str | int | None = None,
    prefer_message_id: bool = False,
) -> tuple[str, dict] | None:
    if prefer_message_id:
        routed = route_message_entry(state, chat_id, thread_id, message_id)
        if routed:
            return routed
    routed = route_message_entry(state, chat_id, thread_id, reply_to_message_id)
    if routed:
        return routed
    mapped_space = topic_space_entry(state, chat_id, thread_id)
    if mapped_space:
        _space_key, space = mapped_space
        live_entries = live_entries_for_space(state, space)
        if len(live_entries) == 1:
            return live_entries[0]
        return None
    for entry in (state.get("panes") or {}).values():
        if isinstance(entry, dict) and str(entry.get("topic_id") or "") == str(thread_id):
            return str(entry.get("pane_key") or ""), entry
    return None


def mapped_entry(state: dict, chat_id: str, thread_id: str | None) -> dict | None:
    resolved = resolve_mapped_entry(state, chat_id, thread_id)
    if not resolved:
        return None
    _pane_key, entry = resolved
    return entry


def attachment_of(message: dict) -> dict | None:
    doc = message.get("document")
    if isinstance(doc, dict) and doc.get("file_id"):
        return {
            "kind": "document",
            "file_id": str(doc.get("file_id")),
            "file_name": str(doc.get("file_name") or ""),
            "mime_type": str(doc.get("mime_type") or ""),
            "file_size": int(doc.get("file_size") or 0),
        }
    photo = message.get("photo")
    if isinstance(photo, list) and photo:
        largest = photo[-1]
        if isinstance(largest, dict) and largest.get("file_id"):
            return {
                "kind": "photo",
                "file_id": str(largest.get("file_id")),
                "file_name": "",
                "mime_type": "image/jpeg",
                "file_size": int(largest.get("file_size") or 0),
            }
    return None


def is_forwarded(message: dict) -> bool:
    return any(
        message.get(attr) is not None
        for attr in ("forward_origin", "forward_from", "forward_sender_name", "forward_date")
    )


def run_script(payload: dict, mode: str) -> dict:
    env = os.environ.copy()
    env["HERDR_TELEGRAM_TOPICS_STATE"] = str(STATE_PATH)
    try:
        proc = subprocess.run(
            [str(SCRIPT_PATH), mode],
            input=json.dumps(payload).encode("utf-8"),
            capture_output=True,
            env=env,
            timeout=COMMAND_TIMEOUT,
        )
    except subprocess.TimeoutExpired:
        log(f"{mode} timed out for topic {payload.get('topic_id')}")
        return {"handled": True}
    except Exception as exc:
        log(f"{mode} failed to start: {exc}")
        return {"handled": True}
    if proc.returncode != 0:
        detail = (proc.stderr or proc.stdout).decode("utf-8", "replace").strip()
        log(f"{mode} exited {proc.returncode}: {detail[:300]}")
        return {"handled": True}
    try:
        return json.loads(proc.stdout.decode("utf-8"))
    except Exception:
        return {"handled": True}


# ---------------------------------------------------------------------------
# Update handlers
# ---------------------------------------------------------------------------

def handle_message(message: dict, *, bot_token: str | None = None, bot_key: str | None = None) -> None:
    text = message.get("text")
    attachment = attachment_of(message)
    if not (text or attachment):
        return
    state = load_state()
    if not state:
        return
    chat = message.get("chat") or {}
    chat_id = str(chat.get("id") or "")
    thread_id = thread_id_of(message)
    user = message.get("from") or {}
    user_id = str(user.get("id") or "")
    from_bot = bool(user.get("is_bot"))
    dlog(
        f"message chat={chat_id} thread={thread_id} from={user_id} bot={from_bot} "
        f"text={str(text or '')[:40]!r}"
    )
    target_bot_kind = target_bot_kind_for_message(state, str(text or ""), bot_key)
    reply_to = message.get("reply_to_message") or {}
    resolved = resolve_mapped_entry(
        state,
        chat_id,
        thread_id,
        message_id=message.get("message_id") or "",
        reply_to_message_id=reply_to.get("message_id") or "",
    )
    if not resolved:
        if topic_space_entry(state, chat_id, thread_id):
            owners = {str(x) for x in (state.get("telegram") or {}).get("owner_user_ids", [])}
            if from_bot or (owners and user_id not in owners):
                return
            if target_bot_kind:
                pane_key = ""
            else:
                params = {
                    "chat_id": chat_id,
                    "text": AMBIGUOUS_PANE_THREAD_REPLY,
                    "reply_to_message_id": str(message.get("message_id") or ""),
                }
                if thread_id:
                    params["message_thread_id"] = thread_id
                try:
                    api("sendMessage", params, token=bot_token)
                except Exception as exc:
                    log(f"ambiguous shared-topic reply failed: {exc}")
                return
        else:
            tg = state.get("telegram") or {}
            dlog(
                f"NOT mapped (state chat={tg.get('chat_id')} general={tg.get('general_thread_id')} "
                f"topics={[str(e.get('topic_id')) for e in (state.get('panes') or {}).values()][:12]})"
            )
            return  # not a mapped pane topic (General chat / other chats pass through)
    else:
        pane_key, _entry = resolved

    owners = {str(x) for x in (state.get("telegram") or {}).get("owner_user_ids", [])}
    if from_bot or (owners and user_id not in owners):
        dlog(f"filtered: from_bot={from_bot} owners={owners} user={user_id}")
        return  # cheap pre-filter; command_reply re-applies the authoritative gate
    dlog(f"dispatching to herdres command (topic {thread_id})")

    payload = {
        "chat_id": chat_id,
        "topic_id": thread_id,
        "pane_key": pane_key,
        "message_id": str(message.get("message_id") or ""),
        "reply_to_message_id": str(reply_to.get("message_id") or ""),
        "user_id": user_id,
        "from_bot": from_bot,
        "forwarded": is_forwarded(message),
        "edited": bool(message.get("edit_date")),
        "text": str(text or ""),
        "caption": str(message.get("caption") or ""),
        "attachment": attachment,
    }
    if target_bot_kind:
        payload["target_bot_kind"] = target_bot_kind
    result = run_script(payload, "command")
    dlog(f"command result handled={result.get('handled')} reply_len={len(str(result.get('reply') or ''))}")
    if not result.get("handled", True):
        return
    reply = str(result.get("reply") or "").strip()
    if not reply:
        return
    params = {
        "chat_id": chat_id,
        "text": reply,
        "reply_to_message_id": payload["message_id"],
    }
    if thread_id:
        params["message_thread_id"] = thread_id
    try:
        api("sendMessage", params, token=bot_token)
    except Exception as exc:
        log(f"sendMessage reply failed: {exc}")


def handle_callback(query: dict, *, bot_token: str | None = None) -> None:
    data = str(query.get("data") or "")
    if not data.startswith("herdr:"):
        return
    state = load_state()
    if not state:
        return
    message = query.get("message") or {}
    chat = message.get("chat") or {}
    if not chat:
        return
    chat_id = str(chat.get("id") or "")
    thread_id = thread_id_of(message)
    resolved = resolve_mapped_entry(
        state,
        chat_id,
        thread_id,
        message_id=message.get("message_id") or "",
        prefer_message_id=True,
    )
    if not resolved:
        return
    pane_key, _entry = resolved

    user = query.get("from") or {}
    payload = {
        "chat_id": chat_id,
        "topic_id": thread_id,
        "pane_key": pane_key,
        "message_id": str(message.get("message_id") or ""),
        "user_id": str(user.get("id") or ""),
        "data": data,
    }
    result = run_script(payload, "callback")
    answer_params = {"callback_query_id": str(query.get("id") or "")}
    answer = str(result.get("answer") or "").strip()
    if answer:
        answer_params["text"] = answer
    if result.get("show_alert"):
        answer_params["show_alert"] = "true"
    try:
        api("answerCallbackQuery", answer_params, token=bot_token)
    except Exception as exc:
        log(f"answerCallbackQuery failed: {exc}")


def handle_managed_bot_update(payload: dict) -> None:
    result = run_script(payload, "managed-bot")
    if not result.get("handled", True):
        return
    if result.get("ok"):
        log(f"managed bot registered: {result.get('kind')}")
    elif result.get("error"):
        log(f"managed bot update failed: {str(result.get('error'))[:200]}")


# ---------------------------------------------------------------------------
# Offset persistence + main loop
# ---------------------------------------------------------------------------

def offset_path_for(key: str) -> Path:
    if key == "manager":
        return OFFSET_PATH
    safe = "".join(ch if ch.isalnum() or ch in ("-", "_") else "_" for ch in key)
    return OFFSET_PATH.with_name(f"{OFFSET_PATH.name}.{safe}")


def read_offset(key: str = "manager") -> int | None:
    try:
        return int(offset_path_for(key).read_text(encoding="utf-8").strip())
    except Exception:
        return None


def write_offset(offset: int, key: str = "manager") -> None:
    try:
        path = offset_path_for(key)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(str(offset), encoding="utf-8")
    except Exception:
        pass


def drain_backlog(key: str, bot_token: str) -> int | None:
    """Confirm any pending updates without processing them, so a fresh gateway
    never replays historical messages as live pane commands."""
    try:
        resp = api("getUpdates", {"timeout": 0}, timeout=20, token=bot_token)
    except Exception:
        return None
    updates = resp.get("result") or []
    if not updates:
        return None
    last = updates[-1]["update_id"] + 1
    write_offset(last, key)
    log(f"drained {len(updates)} backlog update(s) for {key}; starting at offset {last}")
    return last


def clear_webhook(key: str, bot_token: str) -> None:
    if key in CLEARED_WEBHOOK_KEYS:
        return
    try:
        api("deleteWebhook", {"drop_pending_updates": "false"}, timeout=15, token=bot_token)
    except Exception:
        pass
    CLEARED_WEBHOOK_KEYS.add(key)


def handle_update(update: dict, *, bot_token: str | None = None, bot_key: str | None = None) -> None:
    if "managed_bot" in update:
        handle_managed_bot_update({"managed_bot": update["managed_bot"]})
        return
    if "message" in update:
        message = update["message"]
        if isinstance(message, dict) and isinstance(message.get("managed_bot_created"), dict):
            handle_managed_bot_update({"message": message})
            return
        handle_message(message, bot_token=bot_token, bot_key=bot_key)
        return
    if "callback_query" in update:
        handle_callback(update["callback_query"], bot_token=bot_token)


def poll_once(key: str, bot_token: str, *, timeout_seconds: int) -> None:
    clear_webhook(key, bot_token)
    offset = read_offset(key)
    if offset is None:
        offset = drain_backlog(key, bot_token)
        if offset is not None:
            return
    params = {"timeout": max(0, timeout_seconds), "allowed_updates": ALLOWED_UPDATES}
    if offset is not None:
        params["offset"] = offset
    try:
        resp = api("getUpdates", params, timeout=max(timeout_seconds + 15, 20), token=bot_token)
    except urllib.error.HTTPError as exc:
        log(f"getUpdates HTTP {exc.code} for {key}; backing off")
        time.sleep(ERROR_BACKOFF)
        return
    except Exception as exc:
        log(f"getUpdates error for {key}: {exc}; backing off")
        time.sleep(ERROR_BACKOFF)
        return
    if not resp.get("ok"):
        log(f"getUpdates not ok for {key}: {str(resp)[:200]}")
        time.sleep(ERROR_BACKOFF)
        return
    updates = resp.get("result") or []
    if updates:
        dlog(f"{key} received {len(updates)} update(s)")
    for update in updates:
        offset = update["update_id"] + 1
        try:
            handle_update(update, bot_token=bot_token, bot_key=key)
        except Exception as exc:
            log(f"handler error for {key}: {exc}")
        write_offset(offset, key)


def poll_timeout_plan(child_bots: list[tuple[str, str]]) -> list[tuple[str, str, int]]:
    manager_timeout = LONG_POLL_SECONDS if not child_bots else MANAGER_POLL_WITH_CHILDREN_SECONDS
    plan = [("manager", TOKEN, manager_timeout)]
    plan.extend((key, bot_token, CHILD_POLL_SECONDS) for key, bot_token in child_bots)
    return plan


def main() -> int:
    global TOKEN
    log(f"booting (pid {os.getpid()}, debug={DEBUG})")
    TOKEN = _token()
    if not TOKEN:
        log("no TELEGRAM_BOT_TOKEN found; refusing to start")
        return 1

    log("started; polling getUpdates for manager and managed pane bots")
    while True:
        state = load_state()
        child_bots = managed_bot_tokens(state)
        for key, bot_token, timeout_seconds in poll_timeout_plan(child_bots):
            poll_once(key, bot_token, timeout_seconds=timeout_seconds)


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        raise SystemExit(0)
