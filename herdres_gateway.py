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

import concurrent.futures
import hashlib
import importlib.machinery
import importlib.util
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

from herdres_routing import attachment_payload_dict, is_forwarded_dict, message_thread_id_dict

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
PROCESSED_PATH = Path(
    os.getenv(
        "HERDR_TELEGRAM_TOPICS_GATEWAY_PROCESSED",
        str(HOME / ".local/share/herdres/gateway_processed_messages.json"),
    )
).expanduser()
GENERAL_THREAD_ID = os.getenv("HERDR_TELEGRAM_TOPICS_GENERAL_THREAD_ID", "1")
AMBIGUOUS_PANE_THREAD_REPLY = "Reply inside a pane thread so I know which Herdr pane to control."
MANAGER_BOT_KIND = "manager"
MANAGED_BOT_SUGGESTED_USERNAMES = {
    "codex": "herdr_codex_bot",
    "claude": "herdr_claude_bot",
    "kimi": "herdr_kimi_bot",
    "omp": "herdr_omp_bot",
    "devin": "herdr_devin_bot",
}
MANAGED_BOT_ALIASES = {
    "codex": ("codex", "gpt", "openai"),
    "claude": ("claude", "anthropic"),
    "kimi": ("kimi", "moonshot"),
    "omp": ("omp",),
    "devin": ("devin", "cognition"),
}
MANAGED_BOT_KEY_RE = re.compile(r"^managed-([a-z0-9_]+)-")
MANAGED_BOT_MENTION_RE = re.compile(r"@([A-Za-z0-9_]{3,64})")

LONG_POLL_SECONDS = int(os.getenv("HERDRES_GATEWAY_LONG_POLL_SECONDS", "50"))
CHILD_POLL_SECONDS = int(os.getenv("HERDRES_GATEWAY_CHILD_POLL_SECONDS", "0"))
SOCKET_TIMEOUT = LONG_POLL_SECONDS + 15
COMMAND_TIMEOUT = int(os.getenv("HERDRES_GATEWAY_COMMAND_TIMEOUT", "60"))
ERROR_BACKOFF = 3
NETWORK_ERROR_BACKOFF = float(os.getenv("HERDRES_GATEWAY_NETWORK_ERROR_BACKOFF", "0.5"))
WORKER_RECONCILE_SECONDS = 1
ALLOWED_UPDATES = json.dumps(["message", "callback_query", "managed_bot"])
PROCESSED_MESSAGE_LIMIT = int(os.getenv("HERDRES_GATEWAY_PROCESSED_LIMIT", "2000"))
DISPATCH_WORKERS = max(1, int(os.getenv("HERDRES_GATEWAY_DISPATCH_WORKERS", "8")))
DISPATCH_QUEUE_LIMIT = max(
    DISPATCH_WORKERS,
    int(os.getenv("HERDRES_GATEWAY_DISPATCH_QUEUE_LIMIT", "128")),
)

TOKEN = ""
DEBUG = os.getenv("HERDRES_GATEWAY_DEBUG", "").strip().lower() not in ("", "0", "false", "no")
CLEARED_WEBHOOK_KEYS: set[str] = set()
CLEARED_WEBHOOK_LOCK = threading.Lock()
QUARANTINED_KEYS: set[str] = set()
QUARANTINED_KEYS_LOCK = threading.Lock()
PROCESSED_LOCK = threading.Lock()
PROCESSED_MESSAGE_KEYS: set[str] | None = None
PROCESSED_MESSAGE_ORDER: list[str] = []
DISPATCH_EXECUTOR: concurrent.futures.ThreadPoolExecutor | None = None
DISPATCH_EXECUTOR_LOCK = threading.Lock()
DISPATCH_QUEUE_SEMAPHORE = threading.BoundedSemaphore(DISPATCH_QUEUE_LIMIT)
ROUTE_LOCKS: dict[str, threading.Lock] = {}
ROUTE_LOCKS_LOCK = threading.Lock()
HERDRES_MODULE = None
HERDRES_MODULE_KEY: tuple[str, int, int] | None = None
HERDRES_MODULE_LOCK = threading.Lock()
TRACE_PATH = Path(
    os.getenv("HERDRES_GATEWAY_TRACE", str(HOME / ".local/share/herdres/gateway.trace.log"))
).expanduser()


class WorkerStop(Exception):
    def __init__(self, code: int):
        super().__init__(str(code))
        self.code = code


def _emit(line: str) -> None:
    # stdout (may be block-buffered under launchd) + an always-flushed trace file.
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    stamped = f"[{ts}] {line}"
    print(stamped, flush=True)
    try:
        with open(TRACE_PATH, "a", encoding="utf-8") as fh:
            fh.write(stamped + "\n")
    except Exception:
        pass


def log(msg: str) -> None:
    _emit(f"[herdres-gateway] {msg}")


def dlog(msg: str) -> None:
    if DEBUG:
        _emit(f"[herdres-gateway] DEBUG {msg}")


def get_dispatch_executor() -> concurrent.futures.ThreadPoolExecutor:
    global DISPATCH_EXECUTOR
    with DISPATCH_EXECUTOR_LOCK:
        if DISPATCH_EXECUTOR is None:
            DISPATCH_EXECUTOR = concurrent.futures.ThreadPoolExecutor(
                max_workers=DISPATCH_WORKERS,
                thread_name_prefix="herdres-gateway-dispatch",
            )
        return DISPATCH_EXECUTOR


def dispatch_update(update: dict, *, bot_token: str | None = None, bot_key: str | None = None) -> None:
    if not DISPATCH_QUEUE_SEMAPHORE.acquire(blocking=False):
        log("dispatch queue full; handling update inline")
        handle_update_guarded(update, bot_token=bot_token, bot_key=bot_key)
        return
    try:
        future = get_dispatch_executor().submit(
            handle_update_guarded,
            update,
            bot_token=bot_token,
            bot_key=bot_key,
        )
    except Exception:
        DISPATCH_QUEUE_SEMAPHORE.release()
        raise
    future.add_done_callback(finish_dispatched_update)


def finish_dispatched_update(future: concurrent.futures.Future) -> None:
    try:
        future.result()
    except Exception as exc:
        log(f"dispatch worker failed: {exc}")
    finally:
        try:
            DISPATCH_QUEUE_SEMAPHORE.release()
        except ValueError:
            pass


def handle_update_guarded(
    update: dict,
    *,
    bot_token: str | None = None,
    bot_key: str | None = None,
) -> None:
    try:
        handle_update(update, bot_token=bot_token, bot_key=bot_key)
    except Exception as exc:
        log(f"handler error for {bot_key or 'manager'}: {exc}")


def route_lock_for(key: str) -> threading.Lock:
    with ROUTE_LOCKS_LOCK:
        lock = ROUTE_LOCKS.get(key)
        if lock is None:
            lock = threading.Lock()
            ROUTE_LOCKS[key] = lock
        return lock


def processed_message_key(chat_id: str, thread_id: str | None, message: dict) -> str:
    message_id = str(message.get("message_id") or "").strip()
    if not chat_id or not message_id:
        return ""
    return "|".join([str(chat_id), str(thread_id or ""), message_id])


def load_processed_messages_locked() -> None:
    global PROCESSED_MESSAGE_KEYS, PROCESSED_MESSAGE_ORDER
    if PROCESSED_MESSAGE_KEYS is not None:
        return
    keys: list[str] = []
    try:
        raw = json.loads(PROCESSED_PATH.read_text(encoding="utf-8"))
        if isinstance(raw, dict) and isinstance(raw.get("messages"), list):
            keys = [str(item) for item in raw["messages"] if str(item)]
        elif isinstance(raw, list):
            keys = [str(item) for item in raw if str(item)]
    except Exception:
        keys = []
    if len(keys) > PROCESSED_MESSAGE_LIMIT:
        keys = keys[-PROCESSED_MESSAGE_LIMIT:]
    PROCESSED_MESSAGE_ORDER = keys
    PROCESSED_MESSAGE_KEYS = set(keys)


def save_processed_messages_locked() -> None:
    try:
        PROCESSED_PATH.parent.mkdir(parents=True, exist_ok=True)
        tmp = PROCESSED_PATH.with_suffix(PROCESSED_PATH.suffix + ".tmp")
        tmp.write_text(json.dumps({"messages": PROCESSED_MESSAGE_ORDER[-PROCESSED_MESSAGE_LIMIT:]}), encoding="utf-8")
        tmp.replace(PROCESSED_PATH)
    except Exception as exc:
        log(f"processed message cache write failed: {exc}")


def reserve_message_processing(key: str) -> bool:
    if not key:
        return True
    with PROCESSED_LOCK:
        load_processed_messages_locked()
        assert PROCESSED_MESSAGE_KEYS is not None
        if key in PROCESSED_MESSAGE_KEYS:
            return False
        PROCESSED_MESSAGE_KEYS.add(key)
        PROCESSED_MESSAGE_ORDER.append(key)
        if len(PROCESSED_MESSAGE_ORDER) > PROCESSED_MESSAGE_LIMIT:
            stale = PROCESSED_MESSAGE_ORDER[:-PROCESSED_MESSAGE_LIMIT]
            del PROCESSED_MESSAGE_ORDER[:-PROCESSED_MESSAGE_LIMIT]
            for old_key in stale:
                PROCESSED_MESSAGE_KEYS.discard(old_key)
        save_processed_messages_locked()
        return True


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


def managed_bot_kind_for_agent(value: str | None) -> str:
    text = re.sub(r"[^a-z0-9]+", " ", str(value or "").lower())
    words = set(text.split())
    for kind, aliases in MANAGED_BOT_ALIASES.items():
        alias_set = {str(alias).lower() for alias in aliases}
        if kind in words or alias_set.intersection(words):
            return kind
        if any(alias and alias in text for alias in alias_set):
            return kind
    return ""


def managed_bot_kind_for_entry(entry: dict) -> str:
    explicit = str(entry.get("managed_bot_kind") or "").strip().lower()
    if explicit in MANAGED_BOT_SUGGESTED_USERNAMES:
        return explicit
    return managed_bot_kind_for_agent(str(entry.get("agent") or ""))


def managed_bot_has_token(state: dict, kind: str) -> bool:
    telegram = state.get("telegram") if isinstance(state.get("telegram"), dict) else {}
    bots = telegram.get("managed_bots") if isinstance(telegram.get("managed_bots"), dict) else {}
    record = bots.get(str(kind or "")) if isinstance(bots, dict) else None
    if not isinstance(record, dict) or record.get("enabled") is False:
        return False
    return bool(str(record.get("token") or "").strip())


def mentioned_managed_bot_kind(state: dict, text: str) -> str:
    for match in MANAGED_BOT_MENTION_RE.finditer(str(text or "")):
        kind = managed_bot_kind_for_username(state, match.group(1))
        if kind:
            return kind
    return ""


def replied_managed_bot_kind(state: dict, message: dict | None) -> str:
    reply = (message or {}).get("reply_to_message") or {}
    user = reply.get("from") if isinstance(reply, dict) else {}
    if not isinstance(user, dict):
        return ""
    return managed_bot_kind_for_username(state, str(user.get("username") or ""))


def targeted_managed_bot_kind(state: dict, text: str, message: dict | None = None) -> str:
    return replied_managed_bot_kind(state, message) or mentioned_managed_bot_kind(state, text)


def is_space_level_command(text: str) -> bool:
    stripped = str(text or "").strip()
    if not stripped.startswith("/"):
        return False
    command = stripped.split(None, 1)[0][1:].split("@", 1)[0].strip().lower().replace("_", "-")
    return command == "new"


def current_bot_owner_kind(bot_key: str | None) -> str:
    return managed_bot_kind_for_key(bot_key) or MANAGER_BOT_KIND


def owner_for_entry(state: dict, entry: dict) -> str:
    kind = managed_bot_kind_for_entry(entry)
    if kind and managed_bot_has_token(state, kind):
        return kind
    return MANAGER_BOT_KIND


def message_owner_kinds(
    state: dict,
    text: str,
    message: dict,
    chat_id: str,
    thread_id: str | None,
) -> set[str]:
    owners = {MANAGER_BOT_KIND}
    if is_space_level_command(text):
        return owners
    targeted = targeted_managed_bot_kind(state, text, message)
    if targeted and managed_bot_has_token(state, targeted):
        owners.add(targeted)
        return owners

    reply_to = message.get("reply_to_message") or {}
    resolved = resolve_mapped_entry(
        state,
        chat_id,
        thread_id,
        message_id=message.get("message_id") or "",
        reply_to_message_id=reply_to.get("message_id") or "",
    )
    if resolved:
        _pane_key, entry = resolved
        owners.add(owner_for_entry(state, entry))
    return owners


def target_bot_kind_for_message(state: dict, text: str, bot_key: str | None, message: dict | None = None) -> str:
    key_kind = managed_bot_kind_for_key(bot_key)
    if key_kind:
        return key_kind
    return targeted_managed_bot_kind(state, text, message)


def thread_id_of(message: dict) -> str | None:
    # Thin wrapper over the shared routing helper so this gateway keeps its
    # env-configured GENERAL_THREAD_ID while reusing the deduplicated logic.
    return message_thread_id_dict(message, GENERAL_THREAD_ID)


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
    return attachment_payload_dict(message)


def is_forwarded(message: dict) -> bool:
    return is_forwarded_dict(message)


def owner_allowed(state: dict, user_id: str, from_bot: bool) -> bool:
    """True when the sender is an owner (and not a bot).

    Centralizes the owner pre-filter applied before dispatching to herdres
    command/callback. command_reply re-applies the authoritative gate.
    """
    if from_bot:
        return False
    owners = {str(x) for x in (state.get("telegram") or {}).get("owner_user_ids", [])}
    return not owners or user_id in owners


def send_reply(
    bot_token: str | None,
    chat_id: str,
    thread_id: str | None,
    text: str,
    *,
    reply_to_message_id: str = "",
) -> None:
    """Send a plain sendMessage reply in a mapped topic, logging failures."""
    params: dict = {"chat_id": chat_id, "text": text}
    if reply_to_message_id:
        params["reply_to_message_id"] = reply_to_message_id
    if thread_id:
        params["message_thread_id"] = thread_id
    try:
        api("sendMessage", params, token=bot_token)
    except Exception as exc:
        log(f"sendMessage reply failed: {exc}")


def answer_callback_query(bot_token: str | None, callback_query_id: str, result: dict) -> None:
    """Answer a callback query with optional text/alert from the herdres result."""
    params: dict = {"callback_query_id": callback_query_id}
    answer = str(result.get("answer") or "").strip()
    if answer:
        params["text"] = answer
    if result.get("show_alert"):
        params["show_alert"] = "true"
    try:
        api("answerCallbackQuery", params, token=bot_token)
    except Exception as exc:
        log(f"answerCallbackQuery failed: {exc}")


def gateway_runner_mode() -> str:
    return os.getenv("HERDRES_GATEWAY_RUNNER", "embedded").strip().lower()


def load_herdres_module():
    global HERDRES_MODULE, HERDRES_MODULE_KEY
    stat = SCRIPT_PATH.stat()
    module_key = (str(SCRIPT_PATH), int(stat.st_mtime_ns), int(stat.st_size))
    with HERDRES_MODULE_LOCK:
        if HERDRES_MODULE is not None and HERDRES_MODULE_KEY == module_key:
            return HERDRES_MODULE
        loader = importlib.machinery.SourceFileLoader("_herdres_gateway_embedded", str(SCRIPT_PATH))
        spec = importlib.util.spec_from_loader(loader.name, loader)
        if spec is None:
            raise RuntimeError(f"could not create import spec for {SCRIPT_PATH}")
        module = importlib.util.module_from_spec(spec)
        loader.exec_module(module)
        HERDRES_MODULE = module
        HERDRES_MODULE_KEY = module_key
        return module


def embedded_handler_name(mode: str) -> str:
    return {
        "command": "command_reply",
        "callback": "callback_reply",
        "managed-bot": "managed_bot_update",
    }.get(mode, "")


def run_embedded_herdres(payload: dict, mode: str) -> dict:
    module = load_herdres_module()
    handler_name = embedded_handler_name(mode)
    if not handler_name:
        return {"handled": True}
    handler = getattr(module, handler_name)
    try:
        return module.with_lock(lambda: handler(payload), blocking=True)
    except Exception as exc:
        rate_limited = getattr(module, "RateLimited", None)
        if rate_limited is not None and isinstance(exc, rate_limited):
            return {
                "ok": False,
                "rate_limited": True,
                "retry_after": getattr(exc, "retry_after", 1),
                "error": str(exc),
            }
        log(f"{mode} embedded runner failed: {exc}")
        return {"handled": True}


def run_subprocess_herdres(payload: dict, mode: str, env: dict[str, str]) -> dict:
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


def run_script(payload: dict, mode: str) -> dict:
    env = os.environ.copy()
    env["HERDR_TELEGRAM_TOPICS_STATE"] = str(STATE_PATH)
    os.environ["HERDR_TELEGRAM_TOPICS_STATE"] = str(STATE_PATH)
    if gateway_runner_mode() != "subprocess":
        try:
            return run_embedded_herdres(payload, mode)
        except Exception as exc:
            log(f"{mode} embedded runner unavailable: {exc}; using subprocess")
    return run_subprocess_herdres(payload, mode, env)


# ---------------------------------------------------------------------------
# Update handlers
# ---------------------------------------------------------------------------

def handle_message(message: dict, *, bot_token: str | None = None, bot_key: str | None = None) -> None:
    received_at = time.monotonic()
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
    message_age = ""
    if message.get("date"):
        try:
            message_age = f" age={max(0.0, time.time() - float(message['date'])):.3f}s"
        except Exception:
            message_age = ""
    dlog(
        f"message chat={chat_id} thread={thread_id} from={user_id} bot={from_bot} "
        f"text={str(text or '')[:40]!r}{message_age}"
    )
    owner_kinds = message_owner_kinds(state, str(text or ""), message, chat_id, thread_id)
    current_kind = current_bot_owner_kind(bot_key)
    if current_kind not in owner_kinds:
        dlog(f"ignored message owned by {','.join(sorted(owner_kinds))} on {current_kind}")
        return
    target_bot_kind = target_bot_kind_for_message(state, str(text or ""), bot_key, message)
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
            if not owner_allowed(state, user_id, from_bot):
                return
            pane_key = ""
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
    if not owner_allowed(state, user_id, from_bot):
        dlog(f"filtered: from_bot={from_bot} owners={owners} user={user_id}")
        return  # cheap pre-filter; command_reply re-applies the authoritative gate
    route_key = processed_message_key(chat_id, thread_id, message)
    if not reserve_message_processing(route_key):
        dlog(f"ignored already processed message {route_key}")
        return
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
    lock_key = f"pane:{pane_key}" if pane_key else f"space:{chat_id}:{thread_id or ''}"
    with route_lock_for(lock_key):
        result = run_script(payload, "command")
    elapsed = time.monotonic() - received_at
    dlog(
        "command result "
        f"handled={result.get('handled')} reply_len={len(str(result.get('reply') or ''))} "
        f"elapsed={elapsed:.3f}s"
    )
    if not result.get("handled", True):
        return
    reply = str(result.get("reply") or "").strip()
    if not reply:
        return
    send_reply(bot_token, chat_id, thread_id, reply, reply_to_message_id=payload["message_id"])


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
        if (
            data.startswith("herdr:ob:")
            or data.startswith("herdr:ag:")
            or data.startswith("herdr:mb:")
        ) and topic_space_entry(state, chat_id, thread_id):
            pane_key = ""
        else:
            return
    else:
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
    lock_key = f"pane:{pane_key}" if pane_key else f"topic:{chat_id}:{thread_id or ''}"
    with route_lock_for(lock_key):
        result = run_script(payload, "callback")
    answer_callback_query(bot_token, str(query.get("id") or ""), result)


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
    with CLEARED_WEBHOOK_LOCK:
        if key in CLEARED_WEBHOOK_KEYS:
            return
    try:
        api("deleteWebhook", {"drop_pending_updates": "false"}, timeout=15, token=bot_token)
    except Exception:
        pass
    with CLEARED_WEBHOOK_LOCK:
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
        if exc.code in (401, 404) and key != "manager":
            raise WorkerStop(exc.code) from exc
        log(f"getUpdates HTTP {exc.code} for {key}; backing off")
        time.sleep(ERROR_BACKOFF)
        return
    except urllib.error.URLError as exc:
        log(f"getUpdates network error for {key}: {exc}; backing off")
        time.sleep(NETWORK_ERROR_BACKOFF)
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
            dispatch_update(update, bot_token=bot_token, bot_key=key)
        except Exception as exc:
            log(f"dispatch submit error for {key}: {exc}")
        write_offset(offset, key)


def poll_worker_specs(child_bots: list[tuple[str, str]]) -> list[tuple[str, str, int]]:
    child_timeout = CHILD_POLL_SECONDS if CHILD_POLL_SECONDS > 0 else LONG_POLL_SECONDS
    plan = [("manager", TOKEN, LONG_POLL_SECONDS)]
    plan.extend((key, bot_token, child_timeout) for key, bot_token in child_bots)
    return plan


def poll_worker(key: str, bot_token: str, timeout_seconds: int, stop_event: threading.Event) -> None:
    """Poll one bot token until stopped; stop is cooperative after any in-flight long-poll returns."""
    while not stop_event.is_set():
        try:
            poll_once(key, bot_token, timeout_seconds=timeout_seconds)
        except WorkerStop as exc:
            with QUARANTINED_KEYS_LOCK:
                QUARANTINED_KEYS.add(key)
            log(f"child token {key} revoked (HTTP {exc.code}); stopping worker")
            return


def reconcile_poll_workers(
    workers: dict[str, dict[str, object]],
    specs: list[tuple[str, str, int]],
) -> None:
    desired = {key: (bot_token, timeout_seconds) for key, bot_token, timeout_seconds in specs}
    for key, worker in list(workers.items()):
        if key in desired and worker.get("token") == desired[key][0]:
            continue
        with QUARANTINED_KEYS_LOCK:
            QUARANTINED_KEYS.discard(key)
        stop_event = worker.get("stop")
        if hasattr(stop_event, "set"):
            stop_event.set()
        workers.pop(key, None)
        log(f"poll worker stopped: {key}")

    for key, (bot_token, timeout_seconds) in desired.items():
        if key in workers:
            continue
        with QUARANTINED_KEYS_LOCK:
            if key in QUARANTINED_KEYS:
                continue
        stop_event = threading.Event()
        thread = threading.Thread(
            target=poll_worker,
            args=(key, bot_token, timeout_seconds, stop_event),
            name=f"herdres-gateway-{key}",
            daemon=True,
        )
        workers[key] = {"token": bot_token, "stop": stop_event, "thread": thread}
        thread.start()
        log(f"poll worker started: {key}")


def main() -> int:
    global TOKEN
    log(f"booting (pid {os.getpid()}, debug={DEBUG})")
    TOKEN = _token()
    if not TOKEN:
        log("no TELEGRAM_BOT_TOKEN found; refusing to start")
        return 1
    try:
        info = api("getWebhookInfo", token=TOKEN)
        result = info.get("result") if isinstance(info, dict) else {}
        webhook_url = str((result or {}).get("url") or "").strip()
        if webhook_url:
            log("WARNING: manager token has a webhook configured; run this gateway OR another Telegram consumer, never both")
    except Exception as exc:
        log(f"getWebhookInfo startup check failed: {exc}")

    log("started; polling getUpdates for manager and managed pane bots")
    workers: dict[str, dict[str, object]] = {}
    while True:
        try:
            state = load_state()
            if state is None:
                # Transient unreadable/partial state (e.g. read mid-write). Do NOT
                # reconcile against an empty token set — that would stop every child
                # poller and force a 409 window when they restart next tick while the
                # old long-poll is still draining. Keep the existing workers as-is.
                time.sleep(WORKER_RECONCILE_SECONDS)
                continue
            child_bots = managed_bot_tokens(state)
            reconcile_poll_workers(workers, poll_worker_specs(child_bots))
        except Exception as exc:
            log(f"worker reconcile failed; keeping existing workers: {exc}")
        time.sleep(WORKER_RECONCILE_SECONDS)


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        raise SystemExit(0)
