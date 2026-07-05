"""State helpers for the source-only Telegram connector."""

from __future__ import annotations

import json
import os
import fcntl
import threading
from contextlib import contextmanager
from pathlib import Path
from typing import Any

from . import config
from .safe import compact_ws, short_hash


def load_state(path: Path | None = None) -> dict[str, Any]:
    state_file = path or config.state_path()
    if not state_file.exists():
        return {"version": 2, "enabled": True, "telegram": {}, "panes": {}, "spaces": {}}
    try:
        data = json.loads(state_file.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {"version": 2, "enabled": True, "telegram": {}, "panes": {}, "spaces": {}}
    if not isinstance(data, dict):
        return {"version": 2, "enabled": True, "telegram": {}, "panes": {}, "spaces": {}}
    data.setdefault("version", 2)
    data.setdefault("enabled", True)
    data.setdefault("telegram", {})
    data.setdefault("panes", {})
    data.setdefault("spaces", {})
    return data


def save_state(data: dict[str, Any], path: Path | None = None) -> None:
    state_file = path or config.state_path()
    state_file.parent.mkdir(parents=True, exist_ok=True)
    tmp = state_file.with_suffix(state_file.suffix + ".tmp")
    tmp.write_text(json.dumps(data, indent=2, sort_keys=True, ensure_ascii=False) + "\n", encoding="utf-8")
    os.replace(tmp, state_file)


# Off-lock delivery (issue #122): sync_once holds state_lock() across the whole source-mode delivery
# loop, so queued inbound commands (which also take state_lock()) stall behind its Telegram sends.
# released_lock() drops the held lock for a bounded window and re-acquires it, so the delivery loop can
# yield between items. The held fd + release depth are THREAD-LOCAL: fcntl.flock is per open-file
# description, so an in-process caller with two concurrent state_lock() holders (e.g. an embedded
# runner) would let one thread's released_lock() unlock another thread's fd; per-thread state keeps
# each holder dropping only its OWN fd. (Prod routes inbound commands through subprocesses, one holder
# per process, where this is equivalent to a module global.)
_LOCK_STATE = threading.local()


def _held_lock_fd() -> int | None:
    return getattr(_LOCK_STATE, "held_fd", None)


def lock_held() -> bool:
    """True when this thread is inside a state_lock() (so released_lock() would actually drop it).
    Lets the delivery loop's yield stay inert when sync_once runs outside the lock (tests/dry-run),
    where there is nothing to yield and no on-disk state to reload."""
    return _held_lock_fd() is not None


class _ReleasedLock:
    """Drop the state lock (if held) for the `with` body, then re-acquire it on exit. A no-op when no
    lock is held (called directly in tests) or when already nested inside another released window (the
    depth guard prevents a double-unlock)."""

    def __init__(self) -> None:
        self._fd: int | None = None

    def __enter__(self) -> "_ReleasedLock":
        depth = getattr(_LOCK_STATE, "release_depth", 0)
        held = _held_lock_fd()
        if held is not None and depth == 0:
            self._fd = held
            fcntl.flock(self._fd, fcntl.LOCK_UN)
            _LOCK_STATE.release_depth = depth + 1
        return self

    def __exit__(self, *exc: Any) -> bool:
        if self._fd is not None:
            _LOCK_STATE.release_depth = getattr(_LOCK_STATE, "release_depth", 1) - 1
            # Re-acquire BLOCKING so the caller resumes holding the lock exactly as before. A failure
            # (the lock file vanished) propagates and aborts the pass rather than continuing unlocked.
            fcntl.flock(self._fd, fcntl.LOCK_EX)
        return False


def released_lock() -> "_ReleasedLock":
    # Single-window contract: a released_lock() nested inside another released window is intentionally
    # inert (its body runs but the lock is NOT dropped again) — the depth guard prevents a double
    # unlock. Today only the sync_once turn/pending loop opens one window at a time.
    return _ReleasedLock()


@contextmanager
def state_lock(path: Path | None = None):
    state_file = path or config.state_path()
    lock_file = state_file.with_suffix(state_file.suffix + ".lock")
    lock_file.parent.mkdir(parents=True, exist_ok=True)
    with lock_file.open("a+", encoding="utf-8") as handle:
        fcntl.flock(handle, fcntl.LOCK_EX)
        # Expose the held fd so released_lock() can drop it around slow off-lock sends. Save/restore
        # (rather than force None) so a nested state_lock can't strand an outer holder.
        prev_held = _held_lock_fd()
        _LOCK_STATE.held_fd = handle.fileno()
        try:
            yield
        finally:
            _LOCK_STATE.held_fd = prev_held
            fcntl.flock(handle, fcntl.LOCK_UN)


def source_worker_entries(data: dict[str, Any]) -> dict[str, dict[str, Any]]:
    panes = data.get("panes") if isinstance(data.get("panes"), dict) else {}
    return {
        str(key): value
        for key, value in panes.items()
        if isinstance(value, dict)
        and str(value.get("source") or "") == "tendwire"
        and str(value.get("entry_type") or "") == "worker"
    }


def source_space_entries(data: dict[str, Any]) -> dict[str, dict[str, Any]]:
    spaces = data.get("spaces") if isinstance(data.get("spaces"), dict) else {}
    return {
        str(key): value
        for key, value in spaces.items()
        if isinstance(value, dict)
        and str(value.get("source") or "") == "tendwire"
        and str(value.get("entry_type") or "") == "space"
    }


def source_entries(data: dict[str, Any]) -> dict[str, dict[str, Any]]:
    return source_worker_entries(data) if config.source_topic_mode() == "worker" else source_space_entries(data)


VOICE_REPLY_ID_HISTORY = 30


def record_voice_reply_message_id(entry: dict[str, Any], message_id: str | int) -> None:
    """Remember the message-ids of the voice notes we send for an entry (bounded ring) so a Telegram
    reply TO one of them can be recognized and auto-enable 'speak the next reply'."""
    mid = str(message_id or "").strip()
    if not mid or not isinstance(entry, dict):
        return
    ids = [str(x) for x in entry.get("voice_reply_message_ids")] if isinstance(entry.get("voice_reply_message_ids"), list) else []
    if mid in ids:
        ids.remove(mid)
    ids.append(mid)
    entry["voice_reply_message_ids"] = ids[-VOICE_REPLY_ID_HISTORY:]


def message_is_voice_reply(entry: dict[str, Any], reply_to_message_id: str | int | None) -> bool:
    """True when reply_to_message_id points at one of this entry's own voice notes."""
    rt = str(reply_to_message_id or "").strip()
    if not rt or not isinstance(entry, dict):
        return False
    ids = entry.get("voice_reply_message_ids")
    return isinstance(ids, list) and rt in {str(x) for x in ids}


def find_entry_key_by_worker(data: dict[str, Any], worker_id: str) -> str | None:
    for key, entry in source_worker_entries(data).items():
        if str(entry.get("tendwire_worker_id") or entry.get("worker_id") or "") == worker_id:
            return key
    return None


def find_worker_entry_by_id(data: dict[str, Any], worker_id: str) -> tuple[str, dict[str, Any]] | tuple[None, None]:
    key = find_entry_key_by_worker(data, worker_id)
    if key is None:
        return None, None
    return key, source_worker_entries(data).get(key)


def _alias_token(value: Any) -> str:
    text = str(value or "").strip().lower().lstrip("@")
    return "".join(char for char in text if char.isalnum())


def find_worker_entry_by_alias(
    data: dict[str, Any],
    alias: str,
    *,
    space_id: str | None = None,
) -> tuple[str, dict[str, Any]] | tuple[None, None]:
    wanted = _alias_token(alias)
    if not wanted:
        return None, None
    candidates: list[tuple[str, dict[str, Any]]] = []
    for key, entry in source_worker_entries(data).items():
        if space_id and str(entry.get("tendwire_space_id") or entry.get("space_id") or "") != str(space_id):
            continue
        values = (
            entry.get("worker_name"),
            entry.get("agent"),
            entry.get("topic_name"),
            entry.get("tendwire_worker_id"),
            entry.get("worker_id"),
        )
        aliases = {_alias_token(value) for value in values if value}
        if wanted in aliases:
            candidates.append((key, entry))
    if not candidates:
        return None, None
    order = {"working": 0, "attention": 1, "idle": 2}
    candidates.sort(key=lambda item: str(item[1].get("tendwire_last_seen_at") or ""), reverse=True)
    candidates.sort(key=lambda item: order.get(str(item[1].get("status") or "").lower(), 3))
    return candidates[0]


def find_entry_key_by_space(data: dict[str, Any], space_id: str) -> str | None:
    for key, entry in source_space_entries(data).items():
        if str(entry.get("tendwire_space_id") or entry.get("space_id") or "") == space_id:
            return key
    return None


def find_space_entry_by_id(data: dict[str, Any], space_id: str | None) -> tuple[str, dict[str, Any]] | tuple[None, None]:
    if not space_id:
        return None, None
    key = find_entry_key_by_space(data, str(space_id))
    if key is None:
        return None, None
    return key, source_space_entries(data).get(key)


def find_entry_by_thread(data: dict[str, Any], thread_id: str | None) -> tuple[str, dict[str, Any]] | tuple[None, None]:
    if not thread_id:
        return None, None
    for key, entry in source_entries(data).items():
        if str(entry.get("topic_id") or "") == str(thread_id):
            return key, entry
    return None, None


def worker_agent(worker: dict[str, Any]) -> str:
    meta = worker.get("meta") if isinstance(worker.get("meta"), dict) else {}
    for key in ("agent", "kind", "bot_kind"):
        value = compact_ws(meta.get(key) or worker.get(key), 40).lower()
        if value:
            return value
    name = compact_ws(worker.get("name") or worker.get("id"), 40).lower()
    for candidate in ("codex", "claude", "kimi", "glm", "omp", "devin"):
        if candidate in name:
            return candidate
    return "agent"


# cwd basenames too generic to name a topic after — fall back to the agent name instead.
_GENERIC_CWD_NAMES = {"", "root", "~", "home", "tmp", "temp"}


def topic_name_for_worker(worker: dict[str, Any]) -> str:
    """Name a worker's topic after the user's PANE LABEL when herdr has one ("doro", "Gitmoot2", ...)
    — tendwire exposes it as meta.label — else the working-directory basename (the project,
    /root/herdres -> "herdres"), else space/cwd name, agent name, worker id."""
    meta = worker.get("meta") if isinstance(worker.get("meta"), dict) else {}
    label = compact_ws(meta.get("label"), 120)
    if label:
        return label
    cwd = str(meta.get("foreground_cwd") or meta.get("cwd") or "").strip().rstrip("/")
    base = os.path.basename(cwd).strip() if cwd else ""
    if base and base.casefold() not in _GENERIC_CWD_NAMES:
        return compact_ws(base, 120)
    value = compact_ws(meta.get("space_name") or meta.get("cwd_name") or worker.get("name") or worker.get("id"), 120)
    return value or "Worker"


def topic_name_for_space(space: dict[str, Any]) -> str:
    meta = space.get("meta") if isinstance(space.get("meta"), dict) else {}
    value = compact_ws(space.get("name") or meta.get("name") or space.get("id"), 120)
    return value or "Space"


def find_legacy_topic_id_by_name(data: dict[str, Any], name: str) -> str:
    wanted = compact_ws(name, 120).casefold()
    if not wanted:
        return ""
    for entry in source_space_entries(data).values():
        if compact_ws(entry.get("topic_name"), 120).casefold() == wanted and entry.get("topic_id"):
            return str(entry["topic_id"])
    for entry in source_worker_entries(data).values():
        if compact_ws(entry.get("topic_name"), 120).casefold() == wanted and entry.get("topic_id"):
            return str(entry["topic_id"])
    return ""


def upsert_space_entry(data: dict[str, Any], space: dict[str, Any], *, topic_id: str = "") -> tuple[str, dict[str, Any], bool]:
    space_id = compact_ws(space.get("id"), 160)
    fingerprint = compact_ws(space.get("fingerprint"), 160)
    key = find_entry_key_by_space(data, space_id)
    created = False
    if key is None:
        key = f"space:{space_id}:{short_hash(fingerprint or space_id, 10)}"
        created = True
    spaces = data.setdefault("spaces", {})
    entry = spaces.get(key) if isinstance(spaces.get(key), dict) else {}
    topic_name = topic_name_for_space(space)
    entry.update(
        {
            "source": "tendwire",
            "entry_type": "space",
            "tendwire_space_id": space_id,
            "space_id": space_id,
            "tendwire_space_fingerprint": fingerprint,
            "tendwire_status_line": compact_ws(space.get("status_line") or space.get("status"), 240),
            "tendwire_last_seen_at": str(space.get("updated_at") or ""),
            "topic_name": entry.get("topic_name") or topic_name,
        }
    )
    if topic_id:
        entry["topic_id"] = str(topic_id)
    spaces[key] = entry
    return key, entry, created


def upsert_worker_entry(data: dict[str, Any], worker: dict[str, Any], *, topic_id: str = "") -> tuple[str, dict[str, Any], bool]:
    worker_id = compact_ws(worker.get("id"), 160)
    fingerprint = compact_ws(worker.get("fingerprint"), 160)
    space_id = compact_ws(worker.get("space_id"), 160)
    agent = worker_agent(worker)
    meta = worker.get("meta") if isinstance(worker.get("meta"), dict) else {}
    model = compact_ws(worker.get("model") or meta.get("model"), 80)
    key = find_entry_key_by_worker(data, worker_id)
    created = False
    if key is None:
        key = f"worker:{worker_id}:{short_hash(fingerprint or worker_id, 10)}"
        created = True
    panes = data.setdefault("panes", {})
    entry = panes.get(key) if isinstance(panes.get(key), dict) else {}
    entry.update(
        {
            "source": "tendwire",
            "entry_type": "worker",
            "tendwire_worker_id": worker_id,
            "worker_id": worker_id,
            "tendwire_space_id": space_id,
            "space_id": space_id,
            "tendwire_fingerprint": fingerprint,
            "agent": agent,
            "managed_bot_kind": agent if agent in config.MANAGED_BOT_KINDS else "",
            "worker_name": compact_ws(worker.get("name") or worker_id, 80),
            "tendwire_raw_status": compact_ws(worker.get("status"), 80),
            "tendwire_status_line": compact_ws(worker.get("summary") or worker.get("status"), 240),
            "tendwire_last_seen_at": str(worker.get("last_seen_at") or ""),
            "topic_name": entry.get("topic_name") or topic_name_for_worker(worker),
        }
    )
    if topic_id:
        entry["topic_id"] = str(topic_id)
    if model:
        entry["model"] = model
    panes[key] = entry
    return key, entry, created


def delivered_turns(data: dict[str, Any]) -> dict[str, Any]:
    ledger = data.get("tendwire_source_delivered_turns")
    if not isinstance(ledger, dict):
        ledger = {}
        data["tendwire_source_delivered_turns"] = ledger
    return ledger


def mark_delivered(data: dict[str, Any], identity: str, record: dict[str, Any]) -> bool:
    ledger = delivered_turns(data)
    if identity in ledger:
        return False
    ledger[identity] = record
    if len(ledger) > 1000:
        for key in list(ledger)[: len(ledger) - 1000]:
            ledger.pop(key, None)
    return True


def message_bindings(data: dict[str, Any]) -> dict[str, Any]:
    bindings = data.get("telegram_message_bindings")
    if not isinstance(bindings, dict):
        bindings = {}
        data["telegram_message_bindings"] = bindings
    return bindings


def bind_message_to_worker(
    data: dict[str, Any],
    message_id: str | int | None,
    entry: dict[str, Any],
    *,
    topic_id: str | int | None = None,
    kind: str = "",
    turn_id: str = "",
    bot_kind: str = "",
) -> None:
    message = str(message_id or "").strip()
    if not message or message == "0":
        return
    bindings = message_bindings(data)
    bindings[message] = {
        "topic_id": str(topic_id or entry.get("topic_id") or ""),
        "worker_id": str(entry.get("tendwire_worker_id") or entry.get("active_worker_id") or ""),
        "worker_fingerprint": str(entry.get("tendwire_fingerprint") or entry.get("active_worker_fingerprint") or ""),
        "space_id": str(entry.get("tendwire_space_id") or entry.get("space_id") or ""),
        "kind": str(kind or ""),
        "turn_id": str(turn_id or ""),
        "bot_kind": str(bot_kind or ""),
    }
    if len(bindings) > 2000:
        for key in list(bindings)[: len(bindings) - 2000]:
            bindings.pop(key, None)


def find_message_binding(data: dict[str, Any], message_id: str | int | None, *, topic_id: str | int | None = None) -> dict[str, Any] | None:
    message = str(message_id or "").strip()
    if not message:
        return None
    binding = message_bindings(data).get(message)
    if not isinstance(binding, dict):
        return None
    if topic_id and str(binding.get("topic_id") or "") and str(binding.get("topic_id")) != str(topic_id):
        return None
    return binding
