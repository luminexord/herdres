"""State helpers for the source-only Telegram connector."""

from __future__ import annotations

import fcntl
import json
import os
import re
import threading
import time
import uuid
from copy import deepcopy
from contextlib import contextmanager
from pathlib import Path
from types import MappingProxyType
from typing import Any, Mapping, NamedTuple

from . import config
from .rendering import normalized_status
from .safe import compact_ws, short_hash

DELIVERED_TURN_LEDGER_LIMIT = 10000
TENDWIRE_TURN_JOB_LIMIT = 20001
TENDWIRE_TURN_JOB_SUBSTATES = frozenset(
    {
        "reserved",
        "retryable",
        "telegram_applied",
        "old_slot_retired",
        "suppressed",
        "acknowledged",
        "failed",
    }
)
_TENDWIRE_TURN_JOB_TERMINAL_SUBSTATES = frozenset({"acknowledged", "failed"})
_TENDWIRE_OPAQUE_TOKEN_RE = re.compile(r"^tw(?:plan|rev)1\.[A-Za-z0-9_-]{1,256}$")
_TENDWIRE_TURN_JOB_KEY_RE = re.compile(
    r"^turn-final:(twplan1\.[A-Za-z0-9_-]{1,256}):([0-9]+)$"
)

STABLE_WORKER_KEY_VERSION = 1
STABLE_WORKER_KEY_RE = re.compile(r"^wsk1_[0-9a-f]{64}$")
WORKER_REBIND_AUDIT_LIMIT = 200
PANE_UUID_VERSION = 1
PANE_UUID_RE = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$"
)


def load_state(path: Path | None = None) -> dict[str, Any]:
    state_file = path or config.state_path()
    if not state_file.exists():
        return {"version": 2, "enabled": True, "telegram": {}, "panes": {}, "spaces": {}}
    try:
        data = json.loads(state_file.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        raise RuntimeError("Herdres state file is corrupt") from exc
    if not isinstance(data, dict):
        raise RuntimeError("Herdres state file is corrupt")
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
    # This file is an internal durability boundary, not a hand-edited config.
    # Compact separators cut serialization and fsync bytes on the live state
    # ledger; inbound commands cross this boundary before and after Tendwire,
    # so pretty-print whitespace was directly extending every lane service.
    payload = (
        json.dumps(
            data,
            sort_keys=True,
            ensure_ascii=False,
            separators=(",", ":"),
        )
        + "\n"
    )
    try:
        with tmp.open("w", encoding="utf-8") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp, state_file)
        directory_fd = os.open(
            state_file.parent,
            os.O_RDONLY
            | getattr(os, "O_DIRECTORY", 0)
            | getattr(os, "O_CLOEXEC", 0),
        )
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
    finally:
        try:
            tmp.unlink(missing_ok=True)
        except OSError:
            pass


def _replace_in_place(current: Any, fresh: Any) -> Any:
    """Replace a JSON-shaped value while preserving live container references.

    Slow sync phases release the process flock and reload state afterwards.  A
    number of delivery helpers retain references to nested worker/Telegram
    dictionaries across one provider call, so replacing only the root mapping
    would leave those references detached.  Reconcile matching containers
    recursively and return a replacement only when their shapes differ.
    """

    if isinstance(current, dict) and isinstance(fresh, dict):
        for key in list(current):
            if key not in fresh:
                current.pop(key, None)
        for key, value in fresh.items():
            if key in current:
                current[key] = _replace_in_place(current[key], value)
            else:
                current[key] = deepcopy(value)
        return current
    if isinstance(current, list) and isinstance(fresh, list):
        # JSON arrays have no stable identity key.  Reconcile strictly by index
        # so callers retaining an element reference keep the same logical slot;
        # changing this requires an explicit identity schema and regression.
        common = min(len(current), len(fresh))
        for index in range(common):
            current[index] = _replace_in_place(current[index], fresh[index])
        if len(current) > len(fresh):
            del current[len(fresh) :]
        elif len(fresh) > len(current):
            current.extend(deepcopy(fresh[len(current) :]))
        return current
    return deepcopy(fresh)


def reload_state_in_place(data: dict[str, Any], path: Path | None = None) -> None:
    """Reload authoritative state without detaching nested sync references."""

    _replace_in_place(data, load_state(path))


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
    """True when this thread is inside a ``state_lock`` scope.

    This remains true inside a ``released_lock`` window because the outer scope
    still owns the descriptor and will re-acquire it on exit.
    """
    return _held_lock_fd() is not None


def lock_actually_held() -> bool:
    """True only while this thread's state descriptor currently holds the flock."""

    return lock_held() and getattr(_LOCK_STATE, "release_depth", 0) == 0


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


def valid_stable_worker_key_pair(stable_key: Any, version: Any) -> bool:
    """Return whether a public Tendwire stable-worker identity is exactly v1.

    Deliberately do not coerce or trim either field: bool is an int subclass,
    strings such as ``"1"`` are not protocol version 1, and whitespace around
    a key makes it malformed rather than equivalent.
    """
    return (
        isinstance(stable_key, str)
        and STABLE_WORKER_KEY_RE.fullmatch(stable_key) is not None
        and type(version) is int
        and version == STABLE_WORKER_KEY_VERSION
    )


def worker_stable_identity_class(worker: dict[str, Any]) -> str:
    """Classify the identity carried by this exact source observation."""
    meta = worker.get("meta") if isinstance(worker.get("meta"), dict) else {}
    has_key = "stable_key" in meta
    has_version = "stable_key_version" in meta
    if not has_key and not has_version:
        return "absent"
    if has_key != has_version:
        return "partial"
    stable_key = meta.get("stable_key")
    version = meta.get("stable_key_version")
    if valid_stable_worker_key_pair(stable_key, version):
        return "current_v1"
    if (
        isinstance(stable_key, str)
        and STABLE_WORKER_KEY_RE.fullmatch(stable_key) is not None
        and type(version) is int
        and version != STABLE_WORKER_KEY_VERSION
    ):
        return "unknown_version"
    return "malformed"


def worker_stable_identity(worker: dict[str, Any]) -> tuple[str, int] | None:
    meta = worker.get("meta") if isinstance(worker.get("meta"), dict) else {}
    stable_key = meta.get("stable_key")
    version = meta.get("stable_key_version")
    if not valid_stable_worker_key_pair(stable_key, version):
        return None
    return stable_key, STABLE_WORKER_KEY_VERSION


def worker_stable_key(worker: dict[str, Any]) -> str:
    """Return only a fully validated Tendwire v1 worker key."""
    identity = worker_stable_identity(worker)
    return identity[0] if identity is not None else ""


def canonical_worker_observation_key(worker: dict[str, Any]) -> tuple[str, ...]:
    """Return the shared total ordering key for one immutable snapshot row.

    Reconciliation, reservations, and topic naming must observe rows in the
    same order.  The explicit identity class/raw pair prevents absent and
    malformed identities from tying, the liveness component gives live rows
    deterministic precedence over terminal copies, and the canonical full row
    covers every current and future field that an upsert may persist.
    """
    meta = worker.get("meta") if isinstance(worker.get("meta"), dict) else {}

    def raw_field(name: str) -> str:
        if name not in meta:
            return "missing"
        return "present:" + json.dumps(
            meta.get(name),
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
        )

    return (
        compact_ws(worker.get("id"), 160),
        "1"
        if normalized_status(worker.get("status")) in {"closed", "failed"}
        else "0",
        worker_stable_identity_class(worker),
        raw_field("stable_key"),
        raw_field("stable_key_version"),
        json.dumps(
            worker,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
        ),
    )


def entry_stable_identity(entry: dict[str, Any]) -> tuple[str, int] | None:
    stable_key = entry.get("tendwire_stable_key")
    version = entry.get("tendwire_stable_key_version")
    if not valid_stable_worker_key_pair(stable_key, version):
        return None
    return stable_key, STABLE_WORKER_KEY_VERSION


def entry_continuity_identity(entry: dict[str, Any]) -> tuple[str, int] | None:
    """Return the current or deliberately released v1 identity for handoff.

    Closed-history healing releases the active identity pair so it cannot
    remain a routable owner.  The paired ``retired_*`` fields preserve exactly
    which authenticated public identity owned its Telegram topic and are safe
    to use only while planning a one-to-one physical topic handoff.
    """
    identity = entry_stable_identity(entry)
    if identity is not None:
        return identity
    stable_key = entry.get("retired_tendwire_stable_key")
    version = entry.get("retired_tendwire_stable_key_version")
    if not valid_stable_worker_key_pair(stable_key, version):
        return None
    return stable_key, STABLE_WORKER_KEY_VERSION


def message_binding_stable_identity(binding: dict[str, Any]) -> tuple[str, int] | None:
    stable_key = binding.get("stable_key")
    version = binding.get("stable_key_version")
    if not valid_stable_worker_key_pair(stable_key, version):
        return None
    return stable_key, STABLE_WORKER_KEY_VERSION


def valid_pane_uuid(value: Any, version: Any = PANE_UUID_VERSION) -> bool:
    """Return whether a value is a canonical Herdres-owned v4 pane UUID."""
    return (
        isinstance(value, str)
        and PANE_UUID_RE.fullmatch(value) is not None
        and type(version) is int
        and version == PANE_UUID_VERSION
    )


def entry_pane_uuid(entry: dict[str, Any]) -> str:
    value = entry.get("pane_uuid")
    version = entry.get("pane_uuid_version")
    return value if valid_pane_uuid(value, version) else ""


def message_binding_pane_uuid(binding: dict[str, Any]) -> str:
    value = binding.get("pane_uuid")
    version = binding.get("pane_uuid_version")
    return value if valid_pane_uuid(value, version) else ""


def _new_pane_uuid() -> str:
    return str(uuid.uuid4())


def entry_is_quarantined(entry: dict[str, Any]) -> bool:
    # Private quarantine markers are write-once and presence-based. Treat any
    # persisted value as quarantined rather than coercing malformed state.
    return "stable_key_quarantined" in entry


def entry_is_retired(entry: dict[str, Any]) -> bool:
    """Return whether reconciliation has retired this historical route.

    Like quarantine, retirement is presence-based.  A malformed persisted
    value must never accidentally make an old route live again.
    """
    return "routing_retired" in entry


def _entry_identity_is_allowed(entry: dict[str, Any]) -> bool:
    """Only an exact persisted v1 identity is independently routable."""
    return entry_stable_identity(entry) is not None


def _entry_is_live(entry: dict[str, Any]) -> bool:
    return normalized_status(entry.get("status") or entry.get("tendwire_raw_status")) not in {"closed", "failed"}


def entry_is_routable(entry: dict[str, Any]) -> bool:
    return (
        not entry_is_quarantined(entry)
        and not entry_is_retired(entry)
        and _entry_identity_is_allowed(entry)
        and _entry_is_live(entry)
    )


def _worker_entry_keys_by_worker(data: dict[str, Any], worker_id: str) -> list[str]:
    if not worker_id:
        return []
    return [
        key
        for key, entry in source_worker_entries(data).items()
        if entry_is_routable(entry)
        and str(entry.get("tendwire_worker_id") or entry.get("worker_id") or "") == worker_id
    ]


def _worker_entry_keys_by_worker_any_status(data: dict[str, Any], worker_id: str) -> list[str]:
    if not worker_id:
        return []
    return [
        key
        for key, entry in source_worker_entries(data).items()
        if not entry_is_quarantined(entry)
        and not entry_is_retired(entry)
        and _entry_identity_is_allowed(entry)
        and str(entry.get("tendwire_worker_id") or entry.get("worker_id") or "") == worker_id
    ]


def _all_worker_entry_keys_by_worker(data: dict[str, Any], worker_id: str) -> list[str]:
    if not worker_id:
        return []
    return [
        key
        for key, entry in source_worker_entries(data).items()
        if str(entry.get("tendwire_worker_id") or entry.get("worker_id") or "") == worker_id
    ]




def find_entry_key_by_worker(data: dict[str, Any], worker_id: str) -> str | None:
    """Resolve only one live worker whose validated stable identity is also unique."""
    matches = _worker_entry_keys_by_worker(data, worker_id)
    if len(matches) != 1:
        return None
    entry = source_worker_entries(data).get(matches[0]) or {}
    return matches[0] if worker_entry_is_uniquely_routable(data, matches[0], entry) else None


def _all_worker_entry_keys_by_stable_key(data: dict[str, Any], stable_key: str) -> list[str]:
    if STABLE_WORKER_KEY_RE.fullmatch(stable_key) is None:
        return []
    return [
        key
        for key, entry in source_worker_entries(data).items()
        if entry_stable_identity(entry) == (stable_key, STABLE_WORKER_KEY_VERSION)
    ]


def _worker_entry_keys_by_stable_key(data: dict[str, Any], stable_key: str) -> list[str]:
    if STABLE_WORKER_KEY_RE.fullmatch(stable_key) is None:
        return []
    return [
        key
        for key, entry in source_worker_entries(data).items()
        if entry_is_routable(entry)
        and entry_stable_identity(entry) == (stable_key, STABLE_WORKER_KEY_VERSION)
    ]


def worker_entry_is_uniquely_routable(
    data: dict[str, Any], key: str, entry: dict[str, Any]
) -> bool:
    if not entry_is_routable(entry):
        return False
    identity = entry_stable_identity(entry)
    if identity is None:
        return False
    pane_uuid = entry_pane_uuid(entry)
    if pane_uuid:
        # Herdr's stable key and worker id are mutable routing hints.  Once a
        # pane has a Herdres UUID, that UUID is the durable topic owner.
        return [
            entry_key
            for entry_key, candidate in source_worker_entries(data).items()
            if entry_is_routable(candidate)
            and entry_pane_uuid(candidate) == pane_uuid
        ] == [key]
    # Worker ids are positional observations: Herdr renumbers them whenever
    # panes are opened or closed.  The validated stable key is the route
    # identity, so a harmless worker-id collision/renumber must not make an
    # otherwise unique pane unroutable.
    return _worker_entry_keys_by_stable_key(data, identity[0]) == [key]


def find_worker_entry_by_pane_uuid(
    data: dict[str, Any], pane_uuid: str
) -> tuple[str, dict[str, Any]] | tuple[None, None]:
    if not valid_pane_uuid(pane_uuid):
        return None, None
    matches = [
        (key, entry)
        for key, entry in source_worker_entries(data).items()
        if entry_pane_uuid(entry) == pane_uuid
        and worker_entry_is_uniquely_routable(data, key, entry)
    ]
    return matches[0] if len(matches) == 1 else (None, None)


def find_entry_key_by_stable_key(data: dict[str, Any], stable_key: str) -> str | None:
    """Resolve a validated v1 key only when exactly one live entry owns it."""
    matches = _worker_entry_keys_by_stable_key(data, stable_key)
    return matches[0] if len(matches) == 1 else None


def find_worker_entry_by_stable_key(
    data: dict[str, Any], stable_key: str
) -> tuple[str, dict[str, Any]] | tuple[None, None]:
    key = find_entry_key_by_stable_key(data, stable_key)
    if key is None:
        return None, None
    return key, source_worker_entries(data).get(key)


def blocked_worker_stable_keys(data: dict[str, Any], workers: list[dict[str, Any]]) -> set[str]:
    """Preflight every snapshot observation and persisted entry before any naming/upsert.

    A key is blocked when either snapshot or persisted state cannot prove a
    one-to-one worker-id/stable-identity owner.  This is deliberately computed
    against the immutable pre-pass graph; mutations during reconciliation may
    not make a later row appear safe.
    """
    claims: dict[str, list[str]] = {}
    keys_by_worker: dict[str, list[str]] = {}
    for worker in workers:
        stable_key = worker_stable_key(worker)
        worker_id = compact_ws(worker.get("id"), 160)
        if stable_key and worker_id:
            claims.setdefault(stable_key, []).append(worker_id)
            keys_by_worker.setdefault(worker_id, []).append(stable_key)
    blocked = {
        stable_key
        for stable_key, worker_ids in claims.items()
        if len(worker_ids) > 1
    }
    for stable_keys in keys_by_worker.values():
        if len(stable_keys) > 1:
            blocked.update(stable_keys)
    persisted_by_stable: dict[str, list[str]] = {}
    for key, entry in source_worker_entries(data).items():
        # Retired rows are historical routes, not current ownership claims.
        # Let a live observation create a fresh owner even when the historical
        # row was also quarantined before it was retired.
        if entry_is_retired(entry):
            continue
        identity = entry_stable_identity(entry)
        if identity is None:
            continue
        if entry_is_quarantined(entry):
            blocked.add(identity[0])
            continue
        if not entry_is_routable(entry):
            continue
        persisted_by_stable.setdefault(identity[0], []).append(key)
    blocked.update(
        stable_key
        for stable_key, entry_keys in persisted_by_stable.items()
        if len(entry_keys) > 1
    )
    return blocked


def conflicting_snapshot_worker_ids(workers: list[dict[str, Any]]) -> set[str]:
    claims: dict[str, int] = {}
    for worker in workers:
        worker_id = compact_ws(worker.get("id"), 160)
        if worker_id:
            claims[worker_id] = claims.get(worker_id, 0) + 1
    return {worker_id for worker_id, count in claims.items() if count > 1}


class WorkerRekeyContinuityPlan(NamedTuple):
    """One immutable restart-rekey decision over the pre-sync state graph."""

    matches: tuple[tuple[int, str], ...]
    stale_entry_keys: tuple[str, ...]


_PHYSICAL_SIGNAL_FIELDS = ("label", "cwd", "agent", "terminal_title", "space")
_TOPIC_BINDING_FIELDS = (
    "topic_id",
    "topic_name",
    "connector_numbered_base",
    "last_topic_icon",
    "last_topic_icon_id",
    "last_topic_icon_missing",
    "last_topic_icon_error",
    "pinned_status_message_id",
    "pinned_status_hash",
    "pinned_status_pinned",
    "pinned_status_last_error",
    "rename_attempts",
    "voice_reply_message_ids",
)


def _physical_text(
    value: Any, limit: int = 240, *, case_insensitive: bool = True
) -> str:
    text = compact_ws(value, limit)
    return text.casefold() if case_insensitive else text


def worker_physical_identity_signals(worker: dict[str, Any]) -> dict[str, str]:
    """Stable-enough pane traits explicitly excluding positional worker id."""
    meta = worker.get("meta") if isinstance(worker.get("meta"), dict) else {}
    return {
        "label": _physical_text(meta.get("label"), 120),
        "cwd": _physical_text(
            meta.get("foreground_cwd")
            or meta.get("cwd")
            or worker.get("foreground_cwd")
            or worker.get("cwd"),
            320,
            case_insensitive=False,
        ),
        "agent": _physical_text(worker_agent(worker), 40),
        "terminal_title": _physical_text(
            meta.get("terminal_title")
            or meta.get("pane_title")
            or meta.get("title"),
            160,
            case_insensitive=False,
        ),
        "space": _physical_text(
            worker.get("space_id")
            or meta.get("space_id")
            or meta.get("space_name")
            or meta.get("space"),
            160,
            case_insensitive=False,
        ),
    }


def entry_physical_identity_signals(entry: dict[str, Any]) -> dict[str, str]:
    label = entry.get("tendwire_pane_label")
    if not label:
        # Older Herdres rows predate explicit physical-signal persistence.  In
        # worker-topic mode their topic name was sourced from the pane label
        # first, so it is the only safe legacy bridge for the live incident.
        label = entry.get("connector_numbered_base") or entry.get("topic_name")
        if isinstance(label, str) and label.startswith("📁 "):
            label = label[2:]
    return {
        "label": _physical_text(label, 120),
        "cwd": _physical_text(
            entry.get("tendwire_foreground_cwd")
            or entry.get("tendwire_cwd"),
            320,
            case_insensitive=False,
        ),
        "agent": _physical_text(entry.get("agent"), 40),
        "terminal_title": _physical_text(
            entry.get("tendwire_terminal_title"),
            160,
            case_insensitive=False,
        ),
        "space": _physical_text(
            entry.get("tendwire_space_id") or entry.get("space_id"),
            160,
            case_insensitive=False,
        ),
    }


def _physical_identity_matches(
    entry: dict[str, Any], worker: dict[str, Any]
) -> bool:
    """Require label+agent or a strict majority of explicit pane signals."""
    previous = entry_physical_identity_signals(entry)
    current = worker_physical_identity_signals(worker)
    comparable = [
        field
        for field in _PHYSICAL_SIGNAL_FIELDS
        if previous[field] and current[field]
    ]
    agreements = [field for field in comparable if previous[field] == current[field]]
    conflicts = [field for field in comparable if previous[field] != current[field]]
    # A differing working directory is a hard veto, never just one outvoted
    # signal: same-label panes in one workspace routinely differ ONLY by cwd,
    # and migrating a topic across that boundary routes messages to the wrong
    # pane (the failure this matcher exists to prevent).
    if "cwd" in conflicts:
        return False
    label_and_agent = "label" in agreements and "agent" in agreements
    explicit_majority = len(agreements) >= 3 and len(agreements) > len(conflicts)
    return (label_and_agent and len(agreements) > len(conflicts)) or explicit_majority


def _labeled_physical_identity_matches(
    entry: dict[str, Any], worker: dict[str, Any]
) -> bool:
    """Require the operator-visible label and agent for released-key recovery."""
    previous = entry_physical_identity_signals(entry)
    current = worker_physical_identity_signals(worker)
    return (
        bool(previous["label"])
        and previous["label"] == current["label"]
        and bool(previous["agent"])
        and previous["agent"] == current["agent"]
        and _physical_identity_matches(entry, worker)
    )


class DurablePaneReconciliation(NamedTuple):
    """One sync pass's durable worker-to-pane reservations."""

    reservations: Mapping[int, str]
    changed: int


def _durable_candidate_entry(entry: dict[str, Any]) -> bool:
    pane_uuid = entry_pane_uuid(entry)
    # A UUID retired after this migration represents a genuinely gone pane and
    # must never be reclaimed by a later pane that happens to look similar.
    # UUID-less retired rows are legacy compensation history and remain
    # eligible during the one-time adoption/consolidation migration.
    if pane_uuid:
        return not entry_is_retired(entry)
    compensation_history = (
        compact_ws(entry.get("routing_retired_reason"), 120)
        == "herdr_restart_rekey_unmatched"
    )
    if compensation_history:
        return True
    return not entry_is_retired(entry) and _entry_is_live(entry)


def _durable_survivor_key(
    entries: dict[str, dict[str, Any]], candidate_keys: set[str]
) -> str:
    durable = [key for key in candidate_keys if entry_pane_uuid(entries[key])]
    pool = durable or list(candidate_keys)
    topic_holders = [key for key in pool if entries[key].get("topic_id")]
    return min(
        topic_holders or pool,
        key=lambda key: _topic_age_key(key, entries[key]),
    )


def _retarget_durable_bindings(
    data: dict[str, Any],
    entry: dict[str, Any],
    *,
    related_topic_ids: set[str],
    surviving_topic_id: str,
) -> None:
    bindings = data.get("telegram_message_bindings")
    if not isinstance(bindings, dict):
        return
    identity = entry_stable_identity(entry)
    pane_uuid = entry_pane_uuid(entry)
    if identity is None or not pane_uuid:
        return
    for binding in bindings.values():
        if not isinstance(binding, dict):
            continue
        topic_id = str(binding.get("topic_id") or "")
        if topic_id not in related_topic_ids:
            continue
        if topic_id != surviving_topic_id:
            binding["routing_quarantined"] = True
            continue
        binding["worker_id"] = str(entry.get("tendwire_worker_id") or "")
        binding["worker_fingerprint"] = str(
            entry.get("tendwire_fingerprint") or ""
        )
        binding["space_id"] = str(
            entry.get("tendwire_space_id") or entry.get("space_id") or ""
        )
        binding["stable_key"] = identity[0]
        binding["stable_key_version"] = identity[1]
        binding["pane_uuid"] = pane_uuid
        binding["pane_uuid_version"] = PANE_UUID_VERSION
        binding.pop("routing_quarantined", None)


def reconcile_durable_pane_identities(
    data: dict[str, Any], workers: list[dict[str, Any]]
) -> DurablePaneReconciliation:
    """Attach current workers to Herdres-owned pane UUIDs in place.

    Herdr stable keys are consulted first as a cheap observation match, then
    the existing label/agent/working-directory matcher reattaches a pane after
    drift.  Neither hint becomes the topic identity: the selected state row and
    its ``pane_uuid`` survive while those observations are refreshed.
    """
    ordered_workers = sorted(workers, key=canonical_worker_observation_key)
    snapshot_key_counts: dict[str, int] = {}
    snapshot_worker_id_counts: dict[str, int] = {}
    for worker in ordered_workers:
        stable_key = worker_stable_key(worker)
        worker_id = compact_ws(worker.get("id"), 160)
        if stable_key:
            snapshot_key_counts[stable_key] = (
                snapshot_key_counts.get(stable_key, 0) + 1
            )
        if worker_id:
            snapshot_worker_id_counts[worker_id] = (
                snapshot_worker_id_counts.get(worker_id, 0) + 1
            )
    all_entries = source_worker_entries(data)
    persisted_blocked_keys = blocked_worker_stable_keys(data, ordered_workers)

    def compensation_match(worker: dict[str, Any]) -> bool:
        return any(
            compact_ws(entry.get("routing_retired_reason"), 120)
            == "herdr_restart_rekey_unmatched"
            and _labeled_physical_identity_matches(entry, worker)
            for entry in all_entries.values()
        )

    eligible_workers = [
        worker
        for worker in ordered_workers
        if normalized_status(worker.get("status")) not in {"closed", "failed"}
        and worker_stable_identity(worker) is not None
        and snapshot_key_counts.get(worker_stable_key(worker)) == 1
        and snapshot_worker_id_counts.get(compact_ws(worker.get("id"), 160)) == 1
        and (
            worker_stable_key(worker) not in persisted_blocked_keys
            or compensation_match(worker)
        )
    ]
    if not eligible_workers:
        return DurablePaneReconciliation(MappingProxyType({}), 0)

    entries = all_entries
    candidates = {
        key: entry
        for key, entry in entries.items()
        if _durable_candidate_entry(entry)
    }
    physical_workers_by_entry: dict[str, set[int]] = {}
    for key, entry in candidates.items():
        physical_workers_by_entry[key] = {
            id(worker)
            for worker in eligible_workers
            if _labeled_physical_identity_matches(entry, worker)
        }

    # A current exact-key row is an observation of the live worker, not pane
    # history competing for adoption.  Every other row that physically fits
    # exactly one live worker is a historical candidate.  More than one such
    # history is indistinguishable evidence: those rows may represent separate
    # panes with identical labels, cwd, agent, title, and space.  Never choose
    # one by topic age (or absorb all of them) because that lets one pane steal
    # another pane's UUID and Telegram topic.
    ambiguous_history_by_worker: dict[int, set[str]] = {}
    for worker in eligible_workers:
        worker_ref = id(worker)
        identity = worker_stable_identity(worker)
        historical = {
            key
            for key, worker_refs in physical_workers_by_entry.items()
            if worker_refs == {worker_ref}
            and entry_stable_identity(entries[key]) != identity
        }
        if len(historical) > 1:
            ambiguous_history_by_worker[worker_ref] = historical

    reservations: dict[int, str] = {}
    groups: dict[int, set[str]] = {}
    claimed: set[str] = {
        key
        for history in ambiguous_history_by_worker.values()
        for key in history
    }
    changed = 0

    # Fail closed before continuity/consolidation can reconsider these rows.
    # Archive every ambiguous historical topic, quarantine replies to those
    # topics, and leave the live observation unreserved so it receives a fresh
    # pane UUID (and therefore a fresh topic) below.
    bindings = data.get("telegram_message_bindings")
    for history in ambiguous_history_by_worker.values():
        ambiguous_topic_ids = {
            str(entries[key].get("topic_id") or "") for key in history
        }
        ambiguous_topic_ids.discard("")
        for key in sorted(history):
            entry = entries[key]
            before = json.dumps(
                entry, sort_keys=True, separators=(",", ":"), ensure_ascii=True
            )
            _retire_rekey_entry(
                entry,
                reason="durable_pane_identity_ambiguous",
                archive_topic=True,
            )
            # Compensation rows may already carry a different retirement
            # reason via setdefault.  Override it so the legacy continuity
            # planner cannot later reclaim an ambiguity we deliberately closed.
            entry["routing_retired_reason"] = "durable_pane_identity_ambiguous"
            if entry.get("topic_id"):
                entry["retired_topic_notice_pending"] = True
            after = json.dumps(
                entry, sort_keys=True, separators=(",", ":"), ensure_ascii=True
            )
            changed += int(before != after)
        if isinstance(bindings, dict):
            for binding in bindings.values():
                if not isinstance(binding, dict):
                    continue
                if str(binding.get("topic_id") or "") in ambiguous_topic_ids:
                    if binding.get("routing_quarantined") is not True:
                        binding["routing_quarantined"] = True
                        changed += 1

    # Cheap path: one live observation of a key adopts its exact persisted
    # owner.  If compensation left several copies, prefer those that also fit
    # the physical pane before choosing the oldest topic during consolidation.
    live_key_counts: dict[str, int] = {}
    for worker in eligible_workers:
        stable_key = worker_stable_key(worker)
        live_key_counts[stable_key] = live_key_counts.get(stable_key, 0) + 1
    for worker in eligible_workers:
        worker_ref = id(worker)
        stable_key = worker_stable_key(worker)
        if live_key_counts.get(stable_key) != 1:
            continue
        exact = {
            key
            for key, entry in candidates.items()
            if key not in claimed
            and not entry_is_quarantined(entry)
            and entry_stable_identity(entry)
            == (stable_key, STABLE_WORKER_KEY_VERSION)
        }
        physical_exact = {
            key for key in exact if worker_ref in physical_workers_by_entry[key]
        }
        exact = physical_exact or exact
        if not exact:
            continue
        seed = _durable_survivor_key(entries, exact)
        groups[worker_ref] = {seed}
        claimed.add(seed)

    # Drift path: accept only candidates that fit this worker and no other live
    # worker.  Multiple rows for the same one pane are intentional migration
    # stragglers and are consolidated into the original topic below.
    for worker in eligible_workers:
        worker_ref = id(worker)
        if worker_ref in groups:
            continue
        physical = {
            key
            for key, worker_refs in physical_workers_by_entry.items()
            if key not in claimed and worker_refs == {worker_ref}
        }
        if physical:
            groups[worker_ref] = physical
            claimed.update(physical)

    # Once a worker has a seed, absorb every other unambiguous physical copy.
    # This is what heals #170/#172/#173 rows without allowing a shared-label
    # ambiguity to steal either pane's UUID.
    for worker in eligible_workers:
        worker_ref = id(worker)
        if worker_ref not in groups:
            worker_id = compact_ws(worker.get("id"), 160)
            has_private_migration_claim = any(
                compact_ws(
                    entry.get("tendwire_worker_id") or entry.get("worker_id"),
                    160,
                )
                == worker_id
                and not entry_pane_uuid(entry)
                and entry_stable_identity(entry) is None
                and not entry_is_retired(entry)
                for entry in entries.values()
            )
            if has_private_migration_claim:
                continue
            groups[worker_ref] = set()
            continue
        extras = {
            key
            for key, worker_refs in physical_workers_by_entry.items()
            if key not in claimed and worker_refs == {worker_ref}
        }
        groups[worker_ref].update(extras)
        claimed.update(extras)

    # A compensation-era pane can also have closed/topicless quarantine rows
    # that are intentionally excluded from ordinary matching.  Once an
    # unambiguous #170/#172/#173 history row has established the physical pane,
    # absorb those remaining labeled copies into the same migration group.
    for worker in eligible_workers:
        worker_ref = id(worker)
        group = groups.get(worker_ref)
        if not group or not any(
            compact_ws(entries[key].get("routing_retired_reason"), 120)
            == "herdr_restart_rekey_unmatched"
            for key in group
        ):
            continue
        stragglers = set()
        for key, entry in entries.items():
            if key in claimed or entry_pane_uuid(entry):
                continue
            matching_workers = {
                id(candidate)
                for candidate in eligible_workers
                if _labeled_physical_identity_matches(entry, candidate)
            }
            if matching_workers == {worker_ref}:
                stragglers.add(key)
        group.update(stragglers)
        claimed.update(stragglers)

    panes = data.setdefault("panes", {})

    def allocate_pane_uuid() -> str:
        while True:
            candidate = _new_pane_uuid()
            if f"pane:{candidate}" in panes:
                continue
            if any(
                entry_pane_uuid(entry) == candidate
                for entry in panes.values()
                if isinstance(entry, dict)
            ):
                continue
            return candidate

    for worker in eligible_workers:
        worker_ref = id(worker)
        if worker_ref not in groups:
            continue
        group = groups.get(worker_ref)
        if not group:
            pane_uuid = allocate_pane_uuid()
            survivor_key = f"pane:{pane_uuid}"
            panes[survivor_key] = {
                "source": "tendwire",
                "entry_type": "worker",
                "pane_uuid": pane_uuid,
                "pane_uuid_version": PANE_UUID_VERSION,
                "_pane_identity_pending_create": True,
            }
            entries[survivor_key] = panes[survivor_key]
            group = {survivor_key}
            changed += 1
        else:
            survivor_key = _durable_survivor_key(entries, group)

        survivor = entries[survivor_key]
        pane_uuid = entry_pane_uuid(survivor)
        if not pane_uuid:
            inherited = sorted(
                {
                    entry_pane_uuid(entries[key])
                    for key in group
                    if entry_pane_uuid(entries[key])
                }
            )
            pane_uuid = inherited[0] if inherited else allocate_pane_uuid()

        before_survivor = json.dumps(
            survivor, sort_keys=True, separators=(",", ":"), ensure_ascii=True
        )
        related_topic_ids = {
            str(entries[key].get("topic_id") or "") for key in group
        }
        related_topic_ids.discard("")
        surviving_topic_id = str(survivor.get("topic_id") or "")

        protected = {
            "topic_id",
            "topic_name",
            "pane_uuid",
            "pane_uuid_version",
            "tendwire_stable_key",
            "tendwire_stable_key_version",
            "routing_retired",
            "routing_retired_reason",
            "stable_key_quarantined",
            "stable_key_quarantine_reason",
        }
        for duplicate_key in sorted(group):
            if duplicate_key == survivor_key:
                continue
            duplicate = entries[duplicate_key]
            for field, value in duplicate.items():
                if field not in protected and field not in survivor:
                    survivor[field] = value

        _clear_consolidated_survivor_markers(
            survivor,
            heal_quarantine=not isinstance(
                survivor.get("tendwire_worker_generation_ambiguity"),
                dict,
            ),
        )
        survivor["pane_uuid"] = pane_uuid
        survivor["pane_uuid_version"] = PANE_UUID_VERSION
        identity = worker_stable_identity(worker)
        assert identity is not None
        survivor["tendwire_stable_key"] = identity[0]
        survivor["tendwire_stable_key_version"] = identity[1]
        _stamp_consolidated_worker_observation(survivor, worker)

        for duplicate_key in sorted(group):
            if duplicate_key == survivor_key:
                continue
            duplicate = entries[duplicate_key]
            duplicate_topic_id = str(duplicate.get("topic_id") or "")
            duplicate.pop("pane_uuid", None)
            duplicate.pop("pane_uuid_version", None)
            if duplicate_topic_id and duplicate_topic_id != surviving_topic_id:
                _retire_rekey_entry(
                    duplicate,
                    reason="durable_pane_duplicate_consolidated",
                    archive_topic=True,
                )
                duplicate["retired_topic_notice_pending"] = True
                duplicate["consolidated_into_entry_key"] = survivor_key
                duplicate["consolidated_into_topic_id"] = surviving_topic_id
                duplicate["tendwire_stable_identity_class"] = (
                    "retired_durable_duplicate"
                )
                duplicate.pop("tendwire_stable_key", None)
                duplicate.pop("tendwire_stable_key_version", None)
            else:
                panes.pop(duplicate_key, None)
            changed += 1

        _retarget_durable_bindings(
            data,
            survivor,
            related_topic_ids=related_topic_ids,
            surviving_topic_id=surviving_topic_id,
        )
        after_survivor = json.dumps(
            survivor, sort_keys=True, separators=(",", ":"), ensure_ascii=True
        )
        changed += int(before_survivor != after_survivor)
        reservations[worker_ref] = survivor_key

    return DurablePaneReconciliation(
        MappingProxyType(dict(reservations)), changed
    )


def _retired_rekey_topic_is_recoverable(entry: dict[str, Any]) -> bool:
    """Allow continuity to finish a previously planned restart handoff.

    A restart pass can retire and release an unmatched old owner before the
    corresponding live observation is available to continuity planning.  That
    row remains a safe candidate only while its topic still exists and the
    retirement came from restart re-key reconciliation.  Arbitrary closed
    history must remain fail-closed.
    """
    if not entry_is_retired(entry):
        return True
    return (
        compact_ws(entry.get("routing_retired_reason"), 120)
        == "herdr_restart_rekey_unmatched"
        and "retired_topic_closed" not in entry
        and "retired_topic_missing" not in entry
    )


def plan_worker_rekey_continuity(
    data: dict[str, Any], workers: list[dict[str, Any]]
) -> WorkerRekeyContinuityPlan:
    """Plan one-to-one topic continuity after a Herdr restart re-keys panes.

    Once live stable-keyed rows have been persisted without topics and multiple
    displaced topic claims corroborate a global re-key, every topic-owning row
    whose stable identity claim disappeared from the live snapshot must retire,
    even when no live pane can safely inherit its topic. Positional worker ids
    still identify displaced topicless duplicate rows which must stop claiming
    a live route. They are deliberately never part of the physical match score
    used to migrate a topic.
    """
    live_workers = [
        worker
        for worker in workers
        if normalized_status(worker.get("status")) not in {"closed", "failed"}
        and worker_stable_identity(worker) is not None
    ]
    live_key_counts: dict[str, int] = {}
    for worker in live_workers:
        identity = worker_stable_identity(worker)
        if identity is not None:
            live_key_counts[identity[0]] = live_key_counts.get(identity[0], 0) + 1
    eligible_workers = [
        worker
        for worker in live_workers
        if live_key_counts.get(worker_stable_key(worker)) == 1
    ]
    live_keys = set(live_key_counts)
    live_worker_ids = {
        compact_ws(worker.get("id"), 160) for worker in eligible_workers
    }
    live_worker_ids.discard("")
    entries = source_worker_entries(data)
    # A closed old owner can already have had its active identity released by
    # the stable-key healer while its Telegram topic remains intact.  Treat a
    # unique physical match as a handoff candidate even when the historical
    # identity is still present in the live snapshot (closed+live Tendwire
    # observations commonly share it).  Without this pre-pass, consolidation
    # skips the duplicated observation and the live row reaches topic minting.
    physical_topic_candidate_keys = {
        entry_key
        for entry_key, entry in entries.items()
        if entry.get("topic_id")
        and _retired_rekey_topic_is_recoverable(entry)
        and entry_stable_identity(entry) is None
        and entry_continuity_identity(entry) is not None
        and any(
            _labeled_physical_identity_matches(entry, worker)
            for worker in eligible_workers
        )
    }
    topicless_live_claim_persisted = any(
        not entry_is_retired(entry)
        and _entry_is_live(entry)
        and not entry.get("topic_id")
        and (identity := entry_stable_identity(entry)) is not None
        and identity[0] in live_keys
        for entry in entries.values()
    )
    displaced_topic_entry_keys = {
        key
        for key, entry in entries.items()
        if not entry_is_retired(entry)
        and entry.get("topic_id")
        and (
            (identity := entry_stable_identity(entry)) is None
            or identity[0] not in live_keys
        )
    }
    corroborating_topic_entry_keys = {
        key
        for key in displaced_topic_entry_keys
        if entry_stable_identity(entries[key]) is not None
    }
    # One quarantined owner can be an ordinary collision and must remain
    # fail-closed. Two or more displaced topic claims corroborate a global pane
    # re-key, allowing already-unroutable blockers to retire as one batch.
    broad_rekey_recovery = (
        topicless_live_claim_persisted
        and len(corroborating_topic_entry_keys) >= 2
    )
    displaced_entries = {
        key: entry
        for key, entry in entries.items()
        if not entry_is_retired(entry)
        and (
            (
                (identity := entry_stable_identity(entry)) is not None
                and identity[0] not in live_keys
            )
            or (
                broad_rekey_recovery
                and key in displaced_topic_entry_keys
            )
            or (
                broad_rekey_recovery
                and entry_is_quarantined(entry)
                and (
                    identity is None
                    and compact_ws(
                        entry.get("tendwire_worker_id")
                        or entry.get("worker_id"),
                        160,
                    )
                    in live_worker_ids
                )
            )
        )
    }
    stale_keys = tuple(
        sorted(
            {
                key
                for key, entry in displaced_entries.items()
                if (broad_rekey_recovery and entry.get("topic_id"))
                or (broad_rekey_recovery and entry_is_quarantined(entry))
                or str(
                    entry.get("tendwire_worker_id")
                    or entry.get("worker_id")
                    or ""
                )
                in live_worker_ids
                or any(
                    _physical_identity_matches(entry, worker)
                    for worker in eligible_workers
                )
            }
            | physical_topic_candidate_keys
        )
    )
    candidates_by_worker: dict[int, list[str]] = {}
    workers_by_candidate: dict[str, list[int]] = {}
    migration_candidate_keys = {
        entry_key
        for entry_key in stale_keys
        if entries[entry_key].get("topic_id")
        and entry_continuity_identity(entries[entry_key]) is not None
    }
    for worker in eligible_workers:
        worker_ref = id(worker)
        for entry_key in migration_candidate_keys:
            if _physical_identity_matches(entries[entry_key], worker):
                candidates_by_worker.setdefault(worker_ref, []).append(entry_key)
                workers_by_candidate.setdefault(entry_key, []).append(worker_ref)
    matches = tuple(
        sorted(
            (
                (id(worker), candidates_by_worker[id(worker)][0])
                for worker in eligible_workers
                if len(candidates_by_worker.get(id(worker), ())) == 1
                and len(
                    workers_by_candidate.get(
                        candidates_by_worker[id(worker)][0], ()
                    )
                )
                == 1
            ),
            key=lambda item: item[1],
        )
    )
    return WorkerRekeyContinuityPlan(matches, stale_keys)


def _retired_topic_name(entry: dict[str, Any]) -> str:
    original = compact_ws(
        entry.get("retired_original_topic_name") or entry.get("topic_name"),
        110,
    )
    if original.startswith("📁 "):
        return original
    return f"📁 {original or 'Retired pane'}"


def _retire_rekey_entry(
    entry: dict[str, Any], *, reason: str, archive_topic: bool
) -> None:
    identity = entry_stable_identity(entry)
    entry["routing_retired"] = True
    entry.setdefault("routing_retired_reason", reason)
    entry.setdefault("routing_retired_at", time.time())
    if identity is not None:
        entry.setdefault("retired_tendwire_stable_key", identity[0])
        entry.setdefault("retired_tendwire_stable_key_version", identity[1])
    entry["status"] = "closed"
    if archive_topic and entry.get("topic_id"):
        entry.setdefault(
            "retired_original_topic_name",
            compact_ws(entry.get("topic_name"), 120) or "Retired pane",
        )
        desired_name = _retired_topic_name(entry)
        if entry.get("topic_name") != desired_name:
            entry["topic_name"] = desired_name
            entry["retired_topic_rename_pending"] = True


def _topic_age_key(entry_key: str, entry: dict[str, Any]) -> tuple[int, int | str, str]:
    """Sort Telegram topics oldest-first (forum topic ids are monotonic)."""
    topic_id = str(entry.get("topic_id") or "")
    if topic_id.isdecimal():
        return 0, int(topic_id), entry_key
    return 1, topic_id, entry_key


_CONSOLIDATION_HEALABLE_QUARANTINE_REASONS = frozenset(
    {
        "closed_stable_key_reuse",
        "persisted_stable_key_collision",
        "preflight_stable_key_conflict",
        "recycled_worker_id",
        "snapshot_stable_key_conflict",
        "snapshot_worker_key_conflict",
        "source_identity_absent",
        "stable_key_mismatch",
    }
)


def _consolidation_may_heal_quarantine(entry: dict[str, Any]) -> bool:
    if not entry_is_quarantined(entry):
        return True
    return (
        compact_ws(entry.get("stable_key_quarantine_reason"), 120)
        in _CONSOLIDATION_HEALABLE_QUARANTINE_REASONS
    )


def _clear_consolidated_survivor_markers(
    entry: dict[str, Any], *, heal_quarantine: bool
) -> None:
    """Make a formerly retired/quarantined row the live stable-key owner."""
    original_name = compact_ws(entry.get("retired_original_topic_name"), 120)
    if original_name:
        entry["topic_name"] = original_name
    for field in (
        "routing_retired",
        "routing_retired_reason",
        "routing_retired_at",
        "retired_tendwire_stable_key",
        "retired_tendwire_stable_key_version",
        "retired_original_topic_name",
        "retired_topic_id",
        "retired_topic_name",
        "retired_topic_rename_pending",
        "retired_topic_renamed",
        "retired_topic_rename_error",
        "retired_topic_close_pending",
        "retired_topic_closed",
        "retired_topic_close_error",
        "retired_topic_missing",
        "retired_topic_notice_pending",
        "retired_topic_notice_error",
        "retired_topic_notice_message_id",
        "topic_migrated_to_stable_key",
        "topic_migrated_to_stable_key_version",
    ):
        entry.pop(field, None)
    if heal_quarantine:
        entry.pop("stable_key_quarantined", None)
        entry.pop("stable_key_quarantine_reason", None)


def _stamp_consolidated_worker_observation(
    entry: dict[str, Any], worker: dict[str, Any]
) -> None:
    """Refresh positional attributes before legacy re-key planning runs."""
    worker_id = compact_ws(worker.get("id"), 160)
    space_id = compact_ws(worker.get("space_id"), 160)
    fingerprint = compact_ws(worker.get("fingerprint"), 160)
    meta = worker.get("meta") if isinstance(worker.get("meta"), dict) else {}
    entry["tendwire_worker_id"] = worker_id
    entry["worker_id"] = worker_id
    entry["tendwire_space_id"] = space_id
    entry["space_id"] = space_id
    entry["tendwire_fingerprint"] = fingerprint
    entry["tendwire_stable_identity_class"] = "current_v1"
    entry["agent"] = worker_agent(worker)
    entry["worker_name"] = compact_ws(worker.get("name") or worker_id, 80)
    entry["tendwire_raw_status"] = compact_ws(worker.get("status"), 80)
    entry["status"] = normalized_status(worker.get("status"))
    entry["tendwire_status_line"] = compact_ws(
        worker.get("summary") or worker.get("status"), 240
    )
    entry["tendwire_last_seen_at"] = str(worker.get("last_seen_at") or "")
    if meta.get("label"):
        entry["tendwire_pane_label"] = compact_ws(meta.get("label"), 120)
    if meta.get("foreground_cwd") or meta.get("cwd"):
        entry["tendwire_foreground_cwd"] = compact_ws(
            meta.get("foreground_cwd") or meta.get("cwd"), 320
        )
    if meta.get("terminal_title") or meta.get("pane_title") or meta.get("title"):
        entry["tendwire_terminal_title"] = compact_ws(
            meta.get("terminal_title")
            or meta.get("pane_title")
            or meta.get("title"),
            160,
        )


def consolidate_worker_entries_by_stable_key(
    data: dict[str, Any],
    workers: list[dict[str, Any]],
    *,
    excluded_entry_keys: frozenset[str] = frozenset(),
) -> int:
    """Collapse positional-id duplicates into one stable-key owner.

    Consolidation is intentionally limited to a stable key with exactly one
    live snapshot observation.  Conflicting source claims remain fail-closed.
    The oldest topic survives; newer duplicate topics remain as retired history
    long enough for source sync to post a visible notice, rename, and close
    them.  Topicless duplicates are removed immediately.
    """
    observed_workers: dict[str, list[dict[str, Any]]] = {}
    for worker in workers:
        identity = worker_stable_identity(worker)
        if identity is not None:
            observed_workers.setdefault(identity[0], []).append(worker)

    entries = source_worker_entries(data)
    by_stable_key: dict[str, list[str]] = {}
    for entry_key, entry in entries.items():
        if entry_key in excluded_entry_keys:
            continue
        identity = entry_stable_identity(entry)
        if identity is not None:
            by_stable_key.setdefault(identity[0], []).append(entry_key)

    panes = data.setdefault("panes", {})
    bindings = data.get("telegram_message_bindings")
    changed = 0
    for stable_key in sorted(by_stable_key):
        entry_keys = by_stable_key[stable_key]
        current_workers = observed_workers.get(stable_key, [])
        if (
            len(entry_keys) <= 1
            or len(current_workers) != 1
            or normalized_status(current_workers[0].get("status"))
            in {"closed", "failed"}
        ):
            continue
        worker = current_workers[0]
        topic_holders = [key for key in entry_keys if entries[key].get("topic_id")]
        survivor_key = min(
            topic_holders or entry_keys,
            key=lambda key: _topic_age_key(key, entries[key]),
        )
        survivor = entries[survivor_key]
        heal_quarantine = _consolidation_may_heal_quarantine(survivor)
        survivor_topic_id = str(survivor.get("topic_id") or "")
        duplicate_topic_ids = {
            str(entries[key].get("topic_id") or "")
            for key in entry_keys
            if key != survivor_key and entries[key].get("topic_id")
        }

        # Preserve useful non-identity cache fields and voice reply provenance.
        voice_ids = (
            [
                str(value)
                for value in survivor.get("voice_reply_message_ids", [])
                if str(value)
            ]
            if isinstance(survivor.get("voice_reply_message_ids"), list)
            else []
        )
        protected = {
            "topic_id",
            "topic_name",
            "tendwire_stable_key",
            "tendwire_stable_key_version",
            "routing_retired",
            "routing_retired_reason",
            "stable_key_quarantined",
            "stable_key_quarantine_reason",
        }
        for entry_key in entry_keys:
            if entry_key == survivor_key:
                continue
            duplicate = entries[entry_key]
            for field, value in duplicate.items():
                if field not in protected and field not in survivor:
                    survivor[field] = value
            if isinstance(duplicate.get("voice_reply_message_ids"), list):
                for value in duplicate["voice_reply_message_ids"]:
                    text = str(value)
                    if text and text not in voice_ids:
                        voice_ids.append(text)
        if voice_ids:
            survivor["voice_reply_message_ids"] = voice_ids[-VOICE_REPLY_ID_HISTORY:]

        _clear_consolidated_survivor_markers(
            survivor, heal_quarantine=heal_quarantine
        )
        survivor["tendwire_stable_key"] = stable_key
        survivor["tendwire_stable_key_version"] = STABLE_WORKER_KEY_VERSION
        _stamp_consolidated_worker_observation(survivor, worker)

        current_worker_id = compact_ws(worker.get("id"), 160)
        current_fingerprint = compact_ws(worker.get("fingerprint"), 160)
        current_space_id = compact_ws(worker.get("space_id"), 160)
        group_topic_ids = duplicate_topic_ids | (
            {survivor_topic_id} if survivor_topic_id else set()
        )
        if isinstance(bindings, dict):
            for binding in bindings.values():
                if not isinstance(binding, dict):
                    continue
                binding_identity = message_binding_stable_identity(binding)
                related_topic = str(binding.get("topic_id") or "") in group_topic_ids
                has_stable_fields = (
                    "stable_key" in binding or "stable_key_version" in binding
                )
                legacy_related_topic = related_topic and not has_stable_fields
                if (
                    binding_identity != (stable_key, STABLE_WORKER_KEY_VERSION)
                    and not legacy_related_topic
                ):
                    continue
                binding["worker_id"] = current_worker_id
                binding["worker_fingerprint"] = current_fingerprint
                binding["space_id"] = current_space_id
                binding["stable_key"] = stable_key
                binding["stable_key_version"] = STABLE_WORKER_KEY_VERSION
                if heal_quarantine:
                    binding.pop("routing_quarantined", None)
                else:
                    binding["routing_quarantined"] = True

        for entry_key in entry_keys:
            if entry_key == survivor_key:
                continue
            duplicate = entries[entry_key]
            topic_id = str(duplicate.get("topic_id") or "")
            if topic_id and topic_id != survivor_topic_id:
                _retire_rekey_entry(
                    duplicate,
                    reason="stable_key_duplicate_consolidated",
                    archive_topic=True,
                )
                duplicate["retired_topic_notice_pending"] = True
                duplicate["consolidated_into_entry_key"] = survivor_key
                duplicate["consolidated_into_topic_id"] = survivor_topic_id
                duplicate["retired_tendwire_stable_key"] = stable_key
                duplicate["retired_tendwire_stable_key_version"] = (
                    STABLE_WORKER_KEY_VERSION
                )
                duplicate["tendwire_stable_identity_class"] = "retired_duplicate_v1"
                duplicate.pop("tendwire_stable_key", None)
                duplicate.pop("tendwire_stable_key_version", None)
            else:
                panes.pop(entry_key, None)
            changed += 1
    return changed


def apply_worker_rekey_continuity_plan(
    data: dict[str, Any],
    workers: list[dict[str, Any]],
    plan: WorkerRekeyContinuityPlan,
) -> Mapping[int, str]:
    """Retire displaced rows and return safe worker-to-topic handoffs."""
    if plan_worker_rekey_continuity(data, workers) != plan:
        return MappingProxyType({})
    entries = source_worker_entries(data)
    matched_stale = {entry_key for _worker_ref, entry_key in plan.matches}
    for entry_key in plan.stale_entry_keys:
        entry = entries.get(entry_key)
        if entry is None:
            return MappingProxyType({})
        _retire_rekey_entry(
            entry,
            reason=(
                "herdr_restart_rekey_continuity"
                if entry_key in matched_stale
                else "herdr_restart_rekey_unmatched"
            ),
            archive_topic=entry_key not in matched_stale,
        )
    return MappingProxyType(dict(plan.matches))


def _retarget_rekey_topic_bindings(
    data: dict[str, Any],
    stale: dict[str, Any],
    current: dict[str, Any],
    *,
    topic_id: str,
) -> None:
    stale_identity = entry_continuity_identity(stale)
    current_identity = entry_stable_identity(current)
    bindings = data.get("telegram_message_bindings")
    if stale_identity is None or current_identity is None or not isinstance(bindings, dict):
        return
    stale_fingerprint = str(stale.get("tendwire_fingerprint") or "")
    stale_space = str(stale.get("tendwire_space_id") or stale.get("space_id") or "")
    for binding in bindings.values():
        if not isinstance(binding, dict) or str(binding.get("topic_id") or "") != topic_id:
            continue
        binding_identity = message_binding_stable_identity(binding)
        legacy_exact_owner = (
            binding_identity is None
            and "stable_key" not in binding
            and "stable_key_version" not in binding
            and str(binding.get("worker_fingerprint") or "") == stale_fingerprint
            and str(binding.get("space_id") or "") == stale_space
        )
        if binding_identity != stale_identity and not legacy_exact_owner:
            continue
        binding["worker_id"] = str(current.get("tendwire_worker_id") or "")
        binding["worker_fingerprint"] = str(current.get("tendwire_fingerprint") or "")
        binding["space_id"] = str(
            current.get("tendwire_space_id") or current.get("space_id") or ""
        )
        binding["stable_key"] = current_identity[0]
        binding["stable_key_version"] = current_identity[1]


def finalize_worker_rekey_topic_handoff(
    data: dict[str, Any],
    stale_entry_key: str,
    current_entry: dict[str, Any],
) -> bool:
    """Move one safely matched historical Telegram topic to its live row."""
    stale = source_worker_entries(data).get(stale_entry_key)
    if stale is None or not entry_is_retired(stale):
        return False
    topic_id = str(stale.get("topic_id") or "")
    if not topic_id:
        return False
    if current_entry.get("topic_id"):
        # A concurrently-created live topic wins; preserve and close the old
        # history rather than deleting either side.
        _retire_rekey_entry(
            stale, reason="herdr_restart_rekey_topic_already_replaced", archive_topic=True
        )
        return False
    for field in _TOPIC_BINDING_FIELDS:
        if field in stale:
            current_entry[field] = stale[field]
    # Retirement prefixes an archived topic name before Telegram close/rename
    # side effects run.  A handoff recovered on the next planning pass must
    # retain the pane's original visible name, not the temporary archive name.
    original_topic_name = compact_ws(stale.get("retired_original_topic_name"), 120)
    if original_topic_name:
        current_entry["topic_name"] = original_topic_name
    _retarget_rekey_topic_bindings(
        data, stale, current_entry, topic_id=topic_id
    )
    stale["retired_topic_id"] = topic_id
    stale["retired_topic_name"] = str(stale.get("topic_name") or "")
    current_identity = entry_stable_identity(current_entry)
    if current_identity is not None:
        stale["topic_migrated_to_stable_key"] = current_identity[0]
        stale["topic_migrated_to_stable_key_version"] = current_identity[1]
    for field in _TOPIC_BINDING_FIELDS:
        stale.pop(field, None)
    stale.pop("retired_topic_rename_pending", None)
    stale.pop("retired_topic_close_pending", None)
    return True




class PersistedStableKeyMigration(NamedTuple):
    """One immutable decision from the missing-version migration preflight."""

    stable_key: str
    action: str
    worker_ref: int | None
    candidate_key: str | None
    claimant_entry_keys: tuple[str, ...]
    stale_entry_keys: tuple[str, ...]
    compatible_binding_ids: tuple[str, ...]
    quarantine_binding_ids: tuple[str, ...]
    reason: str


class PersistedStableKeyMigrationPlan(NamedTuple):
    """Complete deterministic plan for all private missing-version claimants."""

    migrations: tuple[PersistedStableKeyMigration, ...]
    blocked_stable_keys: frozenset[str]


def _persisted_pane_entries(
    data: dict[str, Any],
) -> dict[str, dict[str, Any]]:
    panes = data.get("panes")
    if not isinstance(panes, dict):
        return {}
    return {
        str(key): entry
        for key, entry in panes.items()
        if isinstance(entry, dict)
    }


def _persisted_missing_version_stable_key(entry: dict[str, Any]) -> str:
    stable_key = entry.get("tendwire_stable_key")
    if (
        "tendwire_stable_key_version" not in entry
        and isinstance(stable_key, str)
        and STABLE_WORKER_KEY_RE.fullmatch(stable_key) is not None
    ):
        return stable_key
    return ""


def _migration_candidate_matches_worker(
    entry: dict[str, Any], worker: dict[str, Any]
) -> bool:
    worker_id = compact_ws(worker.get("id"), 160)
    space_id = compact_ws(worker.get("space_id"), 160)
    fingerprint = compact_ws(worker.get("fingerprint"), 160)
    agent = worker_agent(worker)
    return (
        bool(worker_id and space_id and fingerprint and agent)
        and normalized_status(worker.get("status")) not in {"closed", "failed"}
        and entry.get("source") == "tendwire"
        and entry.get("entry_type") == "worker"
        and entry.get("tendwire_worker_id") == worker_id
        and (
            "worker_id" not in entry
            or entry.get("worker_id") == worker_id
        )
        and entry.get("tendwire_space_id") == space_id
        and (
            "space_id" not in entry
            or entry.get("space_id") == space_id
        )
        and entry.get("agent") == agent
        and bool(str(entry.get("tendwire_fingerprint") or ""))
        and _entry_is_live(entry)
        and not entry_is_quarantined(entry)
    )


def _migration_entry_owner(
    entry: dict[str, Any],
) -> tuple[str, str, str] | None:
    owner = (
        str(entry.get("tendwire_worker_id") or ""),
        str(entry.get("tendwire_fingerprint") or ""),
        str(entry.get("tendwire_space_id") or ""),
    )
    return owner if all(owner) else None


def _migration_worker_owner(
    worker: dict[str, Any],
) -> tuple[str, str, str] | None:
    owner = (
        compact_ws(worker.get("id"), 160),
        compact_ws(worker.get("fingerprint"), 160),
        compact_ws(worker.get("space_id"), 160),
    )
    return owner if all(owner) else None


def _migration_binding_owner(
    binding: dict[str, Any],
) -> tuple[str, str, str] | None:
    owner = (
        str(binding.get("worker_id") or ""),
        str(binding.get("worker_fingerprint") or ""),
        str(binding.get("space_id") or ""),
    )
    return owner if all(owner) else None


def _migration_stale_owner_claim(
    entry: dict[str, Any],
) -> tuple[str, str, str, str, int] | None:
    identity = entry_stable_identity(entry)
    owner = _migration_entry_owner(entry)
    if identity is None or owner is None:
        return None
    return owner[0], owner[1], owner[2], identity[0], identity[1]


def _migration_binding_decisions(
    data: dict[str, Any],
    *,
    stable_key: str,
    entry: dict[str, Any],
    stale_owner_claims: frozenset[
        tuple[str, str, str, str, int]
    ] = frozenset(),
) -> tuple[tuple[str, ...], tuple[str, ...], bool]:
    """Classify replies without ever treating a worker-id match as ownership."""
    bindings = data.get("telegram_message_bindings")
    if not isinstance(bindings, dict):
        return (), (), False
    worker_id = str(entry.get("tendwire_worker_id") or "")
    fingerprint = str(entry.get("tendwire_fingerprint") or "")
    space_id = str(entry.get("tendwire_space_id") or "")
    topic_id = str(entry.get("topic_id") or "")
    compatible: list[str] = []
    quarantine: list[str] = []
    conflicting_owner = False
    for message_id in sorted(bindings, key=str):
        binding = bindings.get(message_id)
        if not isinstance(binding, dict):
            continue
        bound_worker = str(binding.get("worker_id") or "")
        bound_fingerprint = str(binding.get("worker_fingerprint") or "")
        bound_space = str(binding.get("space_id") or "")
        bound_topic = str(binding.get("topic_id") or "")
        raw_key_match = binding.get("stable_key") == stable_key
        topic_match = bool(topic_id and bound_topic == topic_id)
        exact_old_owner = (
            bound_worker == worker_id
            and bound_fingerprint == fingerprint
            and bound_space == space_id
        )
        core_owner = bound_worker == worker_id and bound_space == space_id
        related = raw_key_match or exact_old_owner
        if not related:
            continue
        binding_identity = message_binding_stable_identity(binding)
        if (
            binding_identity is not None
            and (
                bound_worker,
                bound_fingerprint,
                bound_space,
                binding_identity[0],
                binding_identity[1],
            )
            in stale_owner_claims
        ):
            quarantine.append(str(message_id))
            continue
        has_stable_fields = (
            "stable_key" in binding or "stable_key_version" in binding
        )
        if (
            has_stable_fields
            and message_binding_stable_identity(binding)
            != (stable_key, STABLE_WORKER_KEY_VERSION)
        ):
            conflicting_owner = True
            quarantine.append(str(message_id))
            continue
        if (
            (raw_key_match or topic_match)
            and (
                not core_owner
                or (
                    bool(bound_fingerprint)
                    and bound_fingerprint != fingerprint
                )
            )
        ):
            conflicting_owner = True
            quarantine.append(str(message_id))
            continue
        if "routing_quarantined" in binding:
            conflicting_owner = True
            quarantine.append(str(message_id))
            continue
        identity_compatible = (
            not has_stable_fields
            or message_binding_stable_identity(binding)
            == (stable_key, STABLE_WORKER_KEY_VERSION)
        )
        if exact_old_owner and topic_match and identity_compatible:
            compatible.append(str(message_id))
        else:
            quarantine.append(str(message_id))
    return tuple(compatible), tuple(dict.fromkeys(quarantine)), conflicting_owner


def plan_persisted_stable_key_migrations(
    data: dict[str, Any], workers: list[dict[str, Any]]
) -> PersistedStableKeyMigrationPlan:
    """Plan exact-key private adoption before any state mutation or reservation.

    Source observations remain subject to the public exact-v1 classifier.  The
    only private exception is a persisted exact ``wsk1_`` key whose version
    field is absent; an absent key, legacy hash, explicit null, or malformed
    version is never a candidate.
    """
    entries = _persisted_pane_entries(data)
    candidates_by_key: dict[str, list[str]] = {}
    for entry_key, entry in entries.items():
        stable_key = _persisted_missing_version_stable_key(entry)
        if stable_key:
            candidates_by_key.setdefault(stable_key, []).append(entry_key)
    workers_by_key: dict[str, list[dict[str, Any]]] = {}
    for worker in workers:
        identity = worker_stable_identity(worker)
        if identity is not None:
            workers_by_key.setdefault(identity[0], []).append(worker)

    migrations: list[PersistedStableKeyMigration] = []
    blocked: set[str] = set()
    for stable_key in sorted(candidates_by_key):
        candidate_keys = tuple(sorted(candidates_by_key[stable_key]))
        claimants = sorted(
            workers_by_key.get(stable_key, []),
            key=canonical_worker_observation_key,
        )
        raw_same_key = tuple(
            sorted(
                entry_key
                for entry_key, entry in entries.items()
                if entry.get("tendwire_stable_key") == stable_key
            )
        )
        exact_v1_owners = tuple(
            entry_key
            for entry_key in raw_same_key
            if entry_stable_identity(entries.get(entry_key) or {})
            == (stable_key, STABLE_WORKER_KEY_VERSION)
        )
        if not claimants:
            migrations.append(
                PersistedStableKeyMigration(
                    stable_key,
                    "wait",
                    None,
                    None,
                    candidate_keys,
                    (),
                    (),
                    (),
                    "no_current_claimant",
                )
            )
            continue

        reason = ""
        worker = claimants[0] if len(claimants) == 1 else None
        candidate_key = candidate_keys[0] if len(candidate_keys) == 1 else None
        candidate = entries.get(candidate_key) if candidate_key is not None else None
        compatible_bindings: tuple[str, ...] = ()
        quarantine_bindings: tuple[str, ...] = ()
        terminal_topic_history: tuple[str, ...] = ()
        if len(claimants) != 1:
            reason = "multiple_current_claimants"
        elif len(candidate_keys) != 1:
            reason = "multiple_persisted_candidates"
        elif exact_v1_owners:
            reason = "existing_exact_v1_owner"
        elif not isinstance(candidate, dict) or not _migration_candidate_matches_worker(
            candidate, worker
        ):
            reason = "incompatible_persisted_candidate"
        else:
            topic_id = str(candidate.get("topic_id") or "")
            terminal_topic_history = tuple(
                sorted(
                    entry_key
                    for entry_key, entry in entries.items()
                    if entry_key != candidate_key
                    and not _entry_is_live(entry)
                    and str(entry.get("topic_id") or "") == topic_id
                )
            )
            topic_owners = tuple(
                sorted(
                    entry_key
                    for entry_key, entry in entries.items()
                    if _entry_is_live(entry)
                    and str(entry.get("topic_id") or "") == topic_id
                )
            )
            if not topic_id or topic_owners != (candidate_key,):
                reason = "ambiguous_topic_owner"
            else:
                stale_owner_claims = frozenset(
                    claim
                    for entry_key in terminal_topic_history
                    if (
                        claim := _migration_stale_owner_claim(
                            entries.get(entry_key) or {}
                        )
                    )
                    is not None
                )
                (
                    compatible_bindings,
                    quarantine_bindings,
                    conflicting_owner,
                ) = _migration_binding_decisions(
                    data,
                    stable_key=stable_key,
                    entry=candidate,
                    stale_owner_claims=stale_owner_claims,
                )
                if conflicting_owner:
                    reason = "conflicting_binding_owner"

        if reason:
            blocked.add(stable_key)
            blocked_owner_tuples = {
                owner
                for entry_key in raw_same_key
                if (
                    owner := _migration_entry_owner(
                        entries.get(entry_key) or {}
                    )
                )
                is not None
            }
            blocked_owner_tuples.update(
                owner
                for claimant in claimants
                if (owner := _migration_worker_owner(claimant)) is not None
            )
            planned_quarantines = set(quarantine_bindings)
            bindings = data.get("telegram_message_bindings")
            if isinstance(bindings, dict):
                planned_quarantines.update(
                    str(message_id)
                    for message_id, binding in bindings.items()
                    if isinstance(binding, dict)
                    and (
                        binding.get("stable_key") == stable_key
                        or _migration_binding_owner(binding)
                        in blocked_owner_tuples
                    )
                )
            quarantine_bindings = tuple(sorted(planned_quarantines))
            migrations.append(
                PersistedStableKeyMigration(
                    stable_key,
                    "block",
                    id(worker) if worker is not None else None,
                    candidate_key,
                    candidate_keys,
                    (),
                    (),
                    quarantine_bindings,
                    reason,
                )
            )
            continue

        stale_entries = tuple(
            sorted(
                {
                    entry_key
                    for entry_key in raw_same_key
                    if entry_key != candidate_key
                }
                | set(terminal_topic_history)
            )
        )
        migrations.append(
            PersistedStableKeyMigration(
                stable_key,
                "adopt",
                id(worker),
                candidate_key,
                candidate_keys,
                stale_entries,
                compatible_bindings,
                quarantine_bindings,
                "",
            )
        )
    return PersistedStableKeyMigrationPlan(
        tuple(migrations), frozenset(blocked)
    )


def _quarantine_private_migration_entry(
    entry: dict[str, Any], *, reason: str
) -> None:
    entry["stable_key_quarantined"] = True
    entry.setdefault("stable_key_quarantine_reason", reason)


def apply_persisted_stable_key_migration_plan(
    data: dict[str, Any],
    workers: list[dict[str, Any]],
    plan: PersistedStableKeyMigrationPlan,
) -> Mapping[int, str]:
    """Revalidate and atomically apply a previously computed private plan."""
    if plan_persisted_stable_key_migrations(data, workers) != plan:
        return MappingProxyType({})
    entries = _persisted_pane_entries(data)
    workers_by_ref = {id(worker): worker for worker in workers}
    adopted: dict[int, str] = {}
    bindings = data.get("telegram_message_bindings")
    for migration in plan.migrations:
        if migration.action == "wait":
            continue
        if migration.action == "block":
            for entry_key in migration.claimant_entry_keys:
                entry = entries.get(entry_key)
                if entry is not None:
                    _quarantine_private_migration_entry(
                        entry, reason=migration.reason
                    )
            if isinstance(bindings, dict):
                for message_id in migration.quarantine_binding_ids:
                    binding = bindings.get(message_id)
                    if isinstance(binding, dict):
                        binding["routing_quarantined"] = True
            continue
        worker = workers_by_ref.get(migration.worker_ref)
        entry = entries.get(migration.candidate_key or "")
        if worker is None or entry is None:
            return MappingProxyType({})
        for entry_key in migration.stale_entry_keys:
            stale = entries.get(entry_key)
            if stale is not None:
                _quarantine_private_migration_entry(
                    stale, reason="stale_same_stable_key_claimant"
                )
        entry["tendwire_stable_key_version"] = STABLE_WORKER_KEY_VERSION
        entry["tendwire_stable_identity_class"] = "current_v1"
        entry["tendwire_fingerprint"] = compact_ws(
            worker.get("fingerprint"), 160
        )
        if isinstance(bindings, dict):
            for message_id in migration.compatible_binding_ids:
                binding = bindings.get(message_id)
                if not isinstance(binding, dict):
                    continue
                binding["worker_id"] = compact_ws(worker.get("id"), 160)
                binding["worker_fingerprint"] = compact_ws(
                    worker.get("fingerprint"), 160
                )
                binding["space_id"] = compact_ws(worker.get("space_id"), 160)
                binding["stable_key"] = migration.stable_key
                binding["stable_key_version"] = STABLE_WORKER_KEY_VERSION
            for message_id in migration.quarantine_binding_ids:
                binding = bindings.get(message_id)
                if isinstance(binding, dict):
                    binding["routing_quarantined"] = True
        adopted[migration.worker_ref] = migration.candidate_key or ""
    return MappingProxyType(adopted)


def resolve_worker_entry_key(
    data: dict[str, Any],
    worker: dict[str, Any],
    *,
    blocked_stable_keys: set[str] | None = None,
) -> str | None:
    """Resolve a current v1 worker; private migration is planned separately."""
    worker_id = compact_ws(worker.get("id"), 160)
    identity = worker_stable_identity(worker)
    if identity is None:
        return None

    is_finished = normalized_status(worker.get("status")) in {"closed", "failed"}
    worker_matches = (
        _worker_entry_keys_by_worker_any_status(data, worker_id)
        if is_finished
        else _worker_entry_keys_by_worker(data, worker_id)
    )
    if is_finished:
        if len(worker_matches) != 1:
            return None
        entry = source_worker_entries(data).get(worker_matches[0]) or {}
        return worker_matches[0] if entry_stable_identity(entry) == identity else None

    stable_key = identity[0]
    stable_matches = _worker_entry_keys_by_stable_key(data, stable_key)
    if stable_key in (blocked_stable_keys or set()):
        if len(worker_matches) != 1:
            return None
        entry = source_worker_entries(data).get(worker_matches[0]) or {}
        return worker_matches[0] if entry_stable_identity(entry) == identity else None
    return stable_matches[0] if len(stable_matches) == 1 else None

def _resolve_worker_upsert_entry_key(
    data: dict[str, Any],
    worker: dict[str, Any],
    *,
    blocked_stable_keys: set[str] | None = None,
    blocked_worker_ids: set[str] | None = None,
) -> str | None:
    """Resolve the row an upsert would own without mutating reconciliation state."""
    worker_id = compact_ws(worker.get("id"), 160)
    identity = worker_stable_identity(worker)
    raw_exact_matches = [
        key
        for key in _all_worker_entry_keys_by_worker(data, worker_id)
        if not entry_is_retired(source_worker_entries(data).get(key) or {})
    ]
    exact_matches = _worker_entry_keys_by_worker(data, worker_id)
    if worker_id in (blocked_worker_ids or set()):
        exact_identity_matches = [
            match_key
            for match_key in raw_exact_matches
            if entry_stable_identity(source_worker_entries(data).get(match_key) or {})
            == identity
        ]
        key = (
            exact_identity_matches[0]
            if len(exact_identity_matches) == 1
            else None
        )
    else:
        key = resolve_worker_entry_key(
            data, worker, blocked_stable_keys=blocked_stable_keys
        )
    if (
        key is None
        and identity is None
        and worker_id not in (blocked_worker_ids or set())
    ):
        candidates = exact_matches if len(exact_matches) == 1 else raw_exact_matches
        if len(candidates) == 1:
            key = candidates[0]
    if key is None and identity is not None:
        repeated_claimants = [
            match_key
            for match_key in raw_exact_matches
            if entry_is_quarantined(source_worker_entries(data).get(match_key) or {})
            and entry_stable_identity(source_worker_entries(data).get(match_key) or {})
            == identity
        ]
        if len(repeated_claimants) == 1:
            key = repeated_claimants[0]
    return key


def precompute_worker_entry_reservations(
    data: dict[str, Any],
    workers: list[dict[str, Any]],
    *,
    blocked_stable_keys: set[str] | None = None,
    blocked_worker_ids: set[str] | None = None,
) -> Mapping[int, str | None]:
    """Freeze a one-to-one snapshot-row -> persisted-row plan for one sync pass.

    Resolution is performed entirely against the pre-pass graph. Competing
    snapshot rows never share a persisted row; unresolved rows are
    deterministically paired with exact-identity storage owners only to keep
    quarantine repeatable.
    """
    migration_plan = plan_persisted_stable_key_migrations(data, workers)
    effective_blocked_stable_keys = set(blocked_stable_keys or ())
    effective_blocked_stable_keys.update(migration_plan.blocked_stable_keys)
    if blocked_stable_keys is not None:
        blocked_stable_keys.update(migration_plan.blocked_stable_keys)
    apply_persisted_stable_key_migration_plan(
        data, workers, migration_plan
    )
    preliminary = {
        id(worker): _resolve_worker_upsert_entry_key(
            data,
            worker,
            blocked_stable_keys=effective_blocked_stable_keys,
            blocked_worker_ids=blocked_worker_ids,
        )
        for worker in workers
    }
    claim_counts: dict[str, int] = {}
    for candidate in preliminary.values():
        if candidate is not None:
            claim_counts[candidate] = claim_counts.get(candidate, 0) + 1
    reservations = {
        worker_ref: (
            candidate
            if candidate is not None and claim_counts.get(candidate) == 1
            else None
        )
        for worker_ref, candidate in preliminary.items()
    }
    claimed = {
        candidate for candidate in reservations.values() if candidate is not None
    }
    ordered_workers = sorted(workers, key=canonical_worker_observation_key)
    entries = source_worker_entries(data)
    for worker in ordered_workers:
        worker_ref = id(worker)
        worker_id = compact_ws(worker.get("id"), 160)
        if reservations[worker_ref] is not None:
            continue
        identity = worker_stable_identity(worker)
        fingerprint = compact_ws(worker.get("fingerprint"), 160)
        allow_nonquarantined = (
            worker_id in (blocked_worker_ids or set())
            or (
                identity is not None
                and identity[0] in effective_blocked_stable_keys
            )
        )
        candidates = [
            key
            for key in _all_worker_entry_keys_by_worker(data, worker_id)
            if not entry_is_retired(entries.get(key) or {})
            if entry_stable_identity(entries.get(key) or {}) == identity
            and (
                allow_nonquarantined
                or entry_is_quarantined(entries.get(key) or {})
            )
            and key not in claimed
        ]
        candidates.sort(
            key=lambda key: (
                compact_ws((entries.get(key) or {}).get("tendwire_fingerprint"), 160)
                != fingerprint,
                key,
            )
        )
        if candidates:
            reservations[worker_ref] = candidates[0]
            claimed.add(candidates[0])
    return MappingProxyType(reservations)

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
        if not worker_entry_is_uniquely_routable(data, key, entry):
            continue
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

def worker_entry_allowed_topic_ids(
    data: dict[str, Any], entry: dict[str, Any]
) -> set[str]:
    """Return current source topics on which this exact worker may be targeted."""
    topic_ids = {str(entry.get("topic_id") or "")}
    space_id = str(entry.get("tendwire_space_id") or entry.get("space_id") or "")
    for space_entry in source_space_entries(data).values():
        if "stale_space_topic" in space_entry:
            continue
        if (
            space_id
            and str(
                space_entry.get("tendwire_space_id")
                or space_entry.get("space_id")
                or ""
            )
            == space_id
        ):
            topic_ids.add(str(space_entry.get("topic_id") or ""))
    topic_ids.discard("")
    return topic_ids


_SPACE_ACTIVE_WORKER_FIELDS = (
    "active_worker_id",
    "active_worker_fingerprint",
    "active_worker_stable_key",
    "active_worker_stable_key_version",
    "active_worker_name",
    "active_worker_model",
    "active_worker_status",
)


def clear_space_active_worker(entry: dict[str, Any]) -> None:
    for field in _SPACE_ACTIVE_WORKER_FIELDS:
        entry.pop(field, None)


def cache_space_active_worker(
    entry: dict[str, Any], worker_entry: dict[str, Any]
) -> bool:
    identity = entry_stable_identity(worker_entry)
    worker_id = str(
        worker_entry.get("tendwire_worker_id")
        or worker_entry.get("worker_id")
        or ""
    )
    fingerprint = str(worker_entry.get("tendwire_fingerprint") or "")
    if identity is None or not worker_id or not fingerprint:
        clear_space_active_worker(entry)
        return False
    entry["active_worker_id"] = worker_id
    entry["active_worker_fingerprint"] = fingerprint
    entry["active_worker_stable_key"] = identity[0]
    entry["active_worker_stable_key_version"] = identity[1]
    entry["active_worker_name"] = compact_ws(
        worker_entry.get("worker_name") or worker_entry.get("agent"), 80
    )
    return True


def active_worker_entry_for_space(
    data: dict[str, Any],
    entry: dict[str, Any],
    *,
    topic_id: str | None = None,
) -> tuple[str, dict[str, Any]] | tuple[None, None]:
    """Resolve a space's cached active worker only with exact current provenance."""
    if "stale_space_topic" in entry:
        return None, None
    worker_id = str(entry.get("active_worker_id") or "")
    fingerprint = str(entry.get("active_worker_fingerprint") or "")
    cached_identity = (
        entry.get("active_worker_stable_key"),
        entry.get("active_worker_stable_key_version"),
    )
    if (
        not worker_id
        or not fingerprint
        or not valid_stable_worker_key_pair(*cached_identity)
    ):
        return None, None
    key, worker_entry = find_worker_entry_by_id(data, worker_id)
    if key is None or worker_entry is None:
        return None, None
    space_id = str(entry.get("tendwire_space_id") or entry.get("space_id") or "")
    worker_space_id = str(
        worker_entry.get("tendwire_space_id") or worker_entry.get("space_id") or ""
    )
    if (
        not space_id
        or worker_space_id != space_id
        or str(worker_entry.get("tendwire_fingerprint") or "") != fingerprint
        or entry_stable_identity(worker_entry) != cached_identity
    ):
        return None, None
    requested_topic = str(topic_id or entry.get("topic_id") or "")
    if (
        not requested_topic
        or requested_topic not in worker_entry_allowed_topic_ids(data, worker_entry)
    ):
        return None, None
    return key, worker_entry


def find_entry_by_thread(data: dict[str, Any], thread_id: str | None) -> tuple[str, dict[str, Any]] | tuple[None, None]:
    if not thread_id:
        return None, None
    candidates: list[tuple[str, dict[str, Any]]] = []
    for key, entry in source_entries(data).items():
        if str(entry.get("topic_id") or "") != str(thread_id):
            continue
        if str(entry.get("entry_type") or "") == "worker":
            if worker_entry_is_uniquely_routable(data, key, entry):
                candidates.append((key, entry))
            continue
        active_key, active_worker = active_worker_entry_for_space(
            data, entry, topic_id=str(thread_id)
        )
        if active_key is not None and active_worker is not None:
            candidates.append((key, entry))
    return candidates[0] if len(candidates) == 1 else (None, None)


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
    eligible_entries = list(source_space_entries(data).values())
    eligible_entries.extend(
        entry
        for key, entry in source_worker_entries(data).items()
        if worker_entry_is_uniquely_routable(data, key, entry)
    )
    matches = {
        str(entry["topic_id"])
        for entry in eligible_entries
        if compact_ws(entry.get("topic_name"), 120).casefold() == wanted and entry.get("topic_id")
    }
    return next(iter(matches)) if len(matches) == 1 else ""


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
    if not any(field in entry for field in _SPACE_ACTIVE_WORKER_FIELDS):
        candidates = [
            worker_entry
            for worker_key, worker_entry in source_worker_entries(data).items()
            if str(
                worker_entry.get("tendwire_space_id")
                or worker_entry.get("space_id")
                or ""
            )
            == space_id
            and worker_entry_is_uniquely_routable(
                data, worker_key, worker_entry
            )
        ]
        if len(candidates) == 1:
            cache_space_active_worker(entry, candidates[0])
    return key, entry, created


def _binding_matches_previous_entry(
    binding: dict[str, Any],
    *,
    worker_id: str,
    fingerprint: str,
    space_id: str,
) -> bool:
    if not worker_id or str(binding.get("worker_id") or "") != worker_id:
        return False
    bound_fingerprint = str(binding.get("worker_fingerprint") or "")
    bound_space = str(binding.get("space_id") or "")
    if fingerprint and bound_fingerprint and bound_fingerprint != fingerprint:
        return False
    if space_id and bound_space and bound_space != space_id:
        return False
    return True


def _binding_topic_ids_for_entry(
    data: dict[str, Any], entry: dict[str, Any], space_id: str
) -> set[str]:
    topic_ids = {str(entry.get("topic_id") or "")}
    for space_entry in source_space_entries(data).values():
        if str(space_entry.get("tendwire_space_id") or space_entry.get("space_id") or "") == space_id:
            topic_ids.add(str(space_entry.get("topic_id") or ""))
    topic_ids.discard("")
    return topic_ids


def quarantine_worker_entry(data: dict[str, Any], key: str, *, reason: str) -> bool:
    entry = source_worker_entries(data).get(key)
    if entry is None:
        return False
    changed = entry.get("stable_key_quarantined") is not True
    entry["stable_key_quarantined"] = True
    if "stable_key_quarantine_reason" not in entry:
        entry["stable_key_quarantine_reason"] = reason
        changed = True
    worker_id = str(entry.get("tendwire_worker_id") or entry.get("worker_id") or "")
    fingerprint = str(entry.get("tendwire_fingerprint") or "")
    space_id = str(entry.get("tendwire_space_id") or entry.get("space_id") or "")
    identity = entry_stable_identity(entry)
    bindings = data.get("telegram_message_bindings")
    if not isinstance(bindings, dict):
        return changed
    for binding in bindings.values():
        if not isinstance(binding, dict) or not _binding_matches_previous_entry(
            binding, worker_id=worker_id, fingerprint=fingerprint, space_id=space_id
        ):
            continue
        if binding.get("routing_quarantined") is not True:
            binding["routing_quarantined"] = True
            changed = True
        if identity is not None:
            if binding.get("stable_key") != identity[0]:
                binding["stable_key"] = identity[0]
                changed = True
            if binding.get("stable_key_version") != identity[1]:
                binding["stable_key_version"] = identity[1]
                changed = True
    return changed


def quarantine_worker_stable_key_owners(
    data: dict[str, Any], stable_keys: set[str], *, reason: str
) -> int:
    """Quarantine every persisted exact-v1 owner of a preflight-blocked key."""
    changed = 0
    for key, entry in list(source_worker_entries(data).items()):
        identity = entry_stable_identity(entry)
        if identity is not None and identity[0] in stable_keys:
            changed += int(quarantine_worker_entry(data, key, reason=reason))
    return changed


def mark_worker_generation_ambiguous(
    data: dict[str, Any],
    key: str,
    *,
    worker_ids: list[str],
    observed_at: float,
) -> bool:
    """Fail closed when one stable pane has competing active generations."""
    entry = source_worker_entries(data).get(key)
    if entry is None:
        return False
    changed = quarantine_worker_entry(
        data, key, reason="ambiguous_stable_key_generations"
    )
    normalized_ids = sorted(
        {str(value) for value in worker_ids if str(value)}
    )
    previous = entry.get("tendwire_worker_generation_ambiguity")
    marker = (
        previous
        if isinstance(previous, dict)
        and previous.get("worker_ids") == normalized_ids
        else {
            "worker_ids": normalized_ids,
            "observed_at": float(observed_at),
        }
    )
    if entry.get("tendwire_worker_generation_ambiguity") != marker:
        entry["tendwire_worker_generation_ambiguity"] = marker
        changed = True
    if entry.get("stable_key_quarantine_reason") in {
        "preflight_stable_key_conflict",
        "snapshot_stable_key_conflict",
    }:
        entry["stable_key_quarantine_reason"] = (
            "ambiguous_stable_key_generations"
        )
        changed = True
    return changed


def clear_worker_generation_ambiguity(
    data: dict[str, Any], key: str
) -> bool:
    """Heal only the quarantine lane created by generation ambiguity."""
    entry = source_worker_entries(data).get(key)
    if entry is None:
        return False
    changed = entry.pop("tendwire_worker_generation_ambiguity", None) is not None
    if entry.get("stable_key_quarantine_reason") != (
        "ambiguous_stable_key_generations"
    ):
        return changed
    entry.pop("stable_key_quarantined", None)
    entry.pop("stable_key_quarantine_reason", None)
    identity = entry_stable_identity(entry)
    pane_uuid = entry_pane_uuid(entry)
    topic_id = str(entry.get("topic_id") or "")
    bindings = data.get("telegram_message_bindings")
    if isinstance(bindings, dict) and identity is not None:
        for binding in bindings.values():
            if (
                not isinstance(binding, dict)
                or message_binding_stable_identity(binding) != identity
                or (
                    topic_id
                    and str(binding.get("topic_id") or "") != topic_id
                )
                or (
                    pane_uuid
                    and message_binding_pane_uuid(binding) not in {"", pane_uuid}
                )
            ):
                continue
            if "routing_quarantined" in binding:
                binding.pop("routing_quarantined", None)
                changed = True
    return changed


def record_worker_generation_rebind(
    data: dict[str, Any],
    entry: dict[str, Any],
    *,
    stable_key: str,
    from_worker_id: str,
    to_worker_id: str,
    reason: str,
    observed_at: float,
    evidence_turn_id: str = "",
) -> bool:
    """Persist bounded #174 evidence for a stable-key cache refresh."""
    if (
        not valid_stable_worker_key_pair(stable_key, STABLE_WORKER_KEY_VERSION)
        or not from_worker_id
        or not to_worker_id
        or from_worker_id == to_worker_id
    ):
        return False
    audit = data.get("tendwire_worker_rebind_audit")
    if not isinstance(audit, list):
        audit = []
    record = {
        "stable_key": stable_key,
        "from_worker_id": str(from_worker_id),
        "to_worker_id": str(to_worker_id),
        "reason": compact_ws(reason, 80),
        "observed_at": float(observed_at),
    }
    audit.append(record)
    data["tendwire_worker_rebind_audit"] = audit[-WORKER_REBIND_AUDIT_LIMIT:]
    if reason == "freshest_turn_activity" and evidence_turn_id:
        entry.pop("tendwire_rebind_catchup_bound", None)
        entry["tendwire_rebind_catchup_pending"] = {
            "from_worker_id": str(from_worker_id),
            "to_worker_id": str(to_worker_id),
            "evidence_turn_id": str(evidence_turn_id),
        }
    else:
        entry.pop("tendwire_rebind_catchup_pending", None)
        entry.pop("tendwire_rebind_catchup_bound", None)
    entry.pop("tendwire_worker_generation_ambiguity", None)
    return True


def _retarget_worker_bindings(
    data: dict[str, Any],
    entry: dict[str, Any],
    *,
    previous_worker_id: str,
    previous_fingerprint: str,
    previous_space_id: str,
    previous_topic_ids: set[str],
) -> None:
    identity = entry_stable_identity(entry)
    if identity is None:
        return
    pane_uuid = entry_pane_uuid(entry)
    bindings = data.get("telegram_message_bindings")
    if not isinstance(bindings, dict):
        return
    for binding in bindings.values():
        if not isinstance(binding, dict) or not _binding_matches_previous_entry(
            binding,
            worker_id=previous_worker_id,
            fingerprint=previous_fingerprint,
            space_id=previous_space_id,
        ):
            continue
        bound_topic_id = str(binding.get("topic_id") or "")
        has_stable_fields = "stable_key" in binding or "stable_key_version" in binding
        binding_identity = message_binding_stable_identity(binding)
        binding_uuid = message_binding_pane_uuid(binding)
        if "routing_quarantined" in binding and (
            not pane_uuid or binding_uuid != pane_uuid
        ):
            continue
        if (
            not bound_topic_id
            or bound_topic_id not in previous_topic_ids
            or (binding_uuid and binding_uuid != pane_uuid)
            or (
                not pane_uuid
                and has_stable_fields
                and binding_identity != identity
            )
        ):
            binding["routing_quarantined"] = True
            continue
        binding["worker_id"] = str(entry.get("tendwire_worker_id") or "")
        binding["worker_fingerprint"] = str(entry.get("tendwire_fingerprint") or "")
        binding["space_id"] = str(entry.get("tendwire_space_id") or entry.get("space_id") or "")
        binding["stable_key"] = identity[0]
        binding["stable_key_version"] = identity[1]
        if pane_uuid:
            binding["pane_uuid"] = pane_uuid
            binding["pane_uuid_version"] = PANE_UUID_VERSION
            binding.pop("routing_quarantined", None)


def upsert_worker_entry(
    data: dict[str, Any],
    worker: dict[str, Any],
    *,
    topic_id: str = "",
    blocked_stable_keys: set[str] | None = None,
    blocked_worker_ids: set[str] | None = None,
    preplanned_key: str | None = None,
    use_preplanned_key: bool = False,
    reserved_entry_keys: frozenset[str] | None = None,
) -> tuple[str, dict[str, Any], bool]:
    worker_id = compact_ws(worker.get("id"), 160)
    fingerprint = compact_ws(worker.get("fingerprint"), 160)
    space_id = compact_ws(worker.get("space_id"), 160)
    agent = worker_agent(worker)
    meta = worker.get("meta") if isinstance(worker.get("meta"), dict) else {}
    model = compact_ws(worker.get("model") or meta.get("model"), 80)
    physical = worker_physical_identity_signals(worker)
    identity_class = worker_stable_identity_class(worker)
    identity = worker_stable_identity(worker)
    raw_exact_matches = [
        key
        for key in _all_worker_entry_keys_by_worker(data, worker_id)
        if not entry_is_retired(source_worker_entries(data).get(key) or {})
    ]
    exact_matches = _worker_entry_keys_by_worker(data, worker_id)
    all_stable_matches = (
        _all_worker_entry_keys_by_stable_key(data, identity[0]) if identity is not None else []
    )
    stable_matches = [
        match_key
        for match_key in all_stable_matches
        if entry_is_routable(source_worker_entries(data).get(match_key) or {})
    ]
    releasable_closed_history = {
        match_key
        for match_key in all_stable_matches
        if not entry_is_quarantined(source_worker_entries(data).get(match_key) or {})
        and not _entry_is_live(source_worker_entries(data).get(match_key) or {})
    }
    key = (
        preplanned_key
        if use_preplanned_key
        else _resolve_worker_upsert_entry_key(
            data,
            worker,
            blocked_stable_keys=blocked_stable_keys,
            blocked_worker_ids=blocked_worker_ids,
        )
    )
    blocked_key = identity is not None and identity[0] in (blocked_stable_keys or set())
    if blocked_key:
        for collided_key in all_stable_matches:
            quarantine_worker_entry(
                data, collided_key, reason="snapshot_stable_key_conflict"
            )
    protected_entry_keys = reserved_entry_keys or frozenset()
    if key is None:
        if len(stable_matches) > 1:
            for collided_key in stable_matches:
                quarantine_worker_entry(data, collided_key, reason="persisted_stable_key_collision")
        for conflicting_key in raw_exact_matches:
            if conflicting_key not in protected_entry_keys:
                quarantine_worker_entry(data, conflicting_key, reason="stable_key_mismatch")
    else:
        for conflicting_key in exact_matches:
            if conflicting_key != key and conflicting_key not in protected_entry_keys:
                quarantine_worker_entry(data, conflicting_key, reason="recycled_worker_id")

    if identity is not None:
        for historical_key in all_stable_matches:
            historical_entry = source_worker_entries(data).get(historical_key) or {}
            if historical_key == key or entry_is_routable(historical_entry):
                continue
            quarantine_worker_entry(
                data, historical_key, reason="closed_stable_key_reuse"
            )
            if historical_key in releasable_closed_history:
                # A closed, previously nonquarantined row is history rather
                # than conflicting provenance.  Its bindings stay quarantined,
                # but release the public identity pair so that this deliberate
                # retirement cannot poison the new live owner on the next pass.
                historical_entry["retired_tendwire_stable_key"] = identity[0]
                historical_entry["retired_tendwire_stable_key_version"] = identity[1]
                historical_entry["tendwire_stable_identity_class"] = "retired_closed_v1"
                historical_entry.pop("tendwire_stable_key", None)
                historical_entry.pop("tendwire_stable_key_version", None)

    must_quarantine = (
        identity_class != "current_v1"
        or blocked_key
        or worker_id in (blocked_worker_ids or set())
    )
    quarantine_reason = (
        "snapshot_worker_key_conflict"
        if worker_id in (blocked_worker_ids or set())
        else "snapshot_stable_key_conflict"
        if blocked_key
        else f"source_identity_{identity_class}"
    )
    panes = data.setdefault("panes", {})
    created = (
        key is None
        or not isinstance(panes.get(key), dict)
        or (panes.get(key) or {}).get("_pane_identity_pending_create") is True
    )
    if key is None:
        base_key = f"worker:{worker_id}:{short_hash(fingerprint or worker_id, 10)}"
        key = base_key
        suffix = 2
        while key in panes:
            key = f"{base_key}:{suffix}"
            suffix += 1
    entry = panes.get(key) if isinstance(panes.get(key), dict) else {}
    previous_worker_id = str(entry.get("tendwire_worker_id") or entry.get("worker_id") or "")
    previous_fingerprint = str(entry.get("tendwire_fingerprint") or "")
    previous_space_id = str(entry.get("tendwire_space_id") or entry.get("space_id") or "")
    previous_topic_ids = _binding_topic_ids_for_entry(data, entry, previous_space_id)
    if must_quarantine and not created:
        # Quarantine historical bindings before observation fields such as the
        # fingerprint are refreshed, or they would no longer match their owner.
        quarantine_worker_entry(data, key, reason=quarantine_reason)
    entry.update(
        {
            "source": "tendwire",
            "entry_type": "worker",
            "tendwire_worker_id": worker_id,
            "worker_id": worker_id,
            "tendwire_space_id": space_id,
            "space_id": space_id,
            "tendwire_fingerprint": fingerprint,
            "tendwire_stable_identity_class": identity_class,
            "agent": agent,
            "managed_bot_kind": agent if agent in config.MANAGED_BOT_KINDS else "",
            "worker_name": compact_ws(worker.get("name") or worker_id, 80),
            "tendwire_raw_status": compact_ws(worker.get("status"), 80),
            "tendwire_status_line": compact_ws(worker.get("summary") or worker.get("status"), 240),
            "tendwire_last_seen_at": str(worker.get("last_seen_at") or ""),
            "topic_name": entry.get("topic_name") or topic_name_for_worker(worker),
        }
    )
    entry.pop("_pane_identity_pending_create", None)
    if physical["label"]:
        entry["tendwire_pane_label"] = compact_ws(meta.get("label"), 120)
    if physical["cwd"]:
        entry["tendwire_foreground_cwd"] = compact_ws(
            meta.get("foreground_cwd") or meta.get("cwd"), 320
        )
    if physical["terminal_title"]:
        entry["tendwire_terminal_title"] = compact_ws(
            meta.get("terminal_title")
            or meta.get("pane_title")
            or meta.get("title"),
            160,
        )
    if topic_id and not must_quarantine:
        entry["topic_id"] = str(topic_id)
    if model:
        entry["model"] = model
    if identity is not None:
        entry["tendwire_stable_key"] = identity[0]
        entry["tendwire_stable_key_version"] = identity[1]
    panes[key] = entry
    if not created and identity is not None and not must_quarantine:
        _retarget_worker_bindings(
            data,
            entry,
            previous_worker_id=previous_worker_id,
            previous_fingerprint=previous_fingerprint,
            previous_space_id=previous_space_id,
            previous_topic_ids=previous_topic_ids,
        )
    if must_quarantine:
        quarantine_worker_entry(data, key, reason=quarantine_reason)
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
    if len(ledger) > DELIVERED_TURN_LEDGER_LIMIT:
        for key in list(ledger)[: len(ledger) - DELIVERED_TURN_LEDGER_LIMIT]:
            ledger.pop(key, None)
    return True


def tendwire_turn_jobs(data: dict[str, Any]) -> dict[str, Any]:
    """Return the private, stable-job-keyed multipart checkpoint ledger."""
    jobs = data.get("tendwire_turn_jobs")
    if not isinstance(jobs, dict):
        jobs = {}
        data["tendwire_turn_jobs"] = jobs
    return jobs


def _tendwire_opaque_token(value: Any, *, kind: str) -> str:
    if not isinstance(value, str) or not _TENDWIRE_OPAQUE_TOKEN_RE.fullmatch(value):
        raise ValueError(f"invalid {kind}")
    prefix = "twplan1." if kind == "plan_token" else "twrev1."
    if not value.startswith(prefix):
        raise ValueError(f"invalid {kind}")
    return value


def _tendwire_job_key_parts(job_key: Any) -> tuple[str, int]:
    if not isinstance(job_key, str):
        raise ValueError("invalid tendwire job key")
    match = _TENDWIRE_TURN_JOB_KEY_RE.fullmatch(job_key)
    if match is None:
        raise ValueError("invalid tendwire job key")
    return match.group(1), int(match.group(2))


def _tendwire_job_index(value: Any, *, field: str, positive: bool = False) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"invalid {field}")
    if value < (1 if positive else 0):
        raise ValueError(f"invalid {field}")
    return value


def _tendwire_job_outcome_value(value: Any, *, field: str, limit: int) -> str:
    if not isinstance(value, str) or not value or len(value) > limit:
        raise ValueError(f"invalid {field}")
    return value


def _next_tendwire_turn_job_checkpoint(data: dict[str, Any]) -> int:
    previous = data.get("tendwire_turn_job_checkpoint_sequence")
    if isinstance(previous, bool) or not isinstance(previous, int) or previous < 0:
        previous = 0
    current = previous + 1
    data["tendwire_turn_job_checkpoint_sequence"] = current
    return current


def find_tendwire_turn_job(
    data: dict[str, Any], job_key: str
) -> dict[str, Any] | None:
    """Find a receipt by stable outbox key; transient lease refs are never accepted."""
    _tendwire_job_key_parts(job_key)
    receipt = tendwire_turn_jobs(data).get(job_key)
    return receipt if isinstance(receipt, dict) else None


def _active_tendwire_turn_job_references(
    data: dict[str, Any],
) -> tuple[set[str], set[str], set[str]]:
    job_keys: set[str] = set()
    plan_tokens: set[str] = set()
    content_revisions: set[str] = set()
    bindings = data.get("telegram_message_bindings")
    if not isinstance(bindings, dict):
        bindings = {}

    active_bindings: list[dict[str, Any]] = []
    for binding in bindings.values():
        if not isinstance(binding, dict):
            continue
        if (
            binding.get("active") is False
            or binding.get("retired") is True
            or binding.get("superseded") is True
            or binding.get("folded") is True
        ):
            continue
        active_bindings.append(binding)
        job_key = binding.get("tendwire_job_key")
        if isinstance(job_key, str):
            job_keys.add(job_key)
        plan_token = binding.get("plan_token")
        if isinstance(plan_token, str) and plan_token:
            plan_tokens.add(plan_token)
        else:
            content_revision = binding.get("content_revision")
            if isinstance(content_revision, str) and content_revision:
                content_revisions.add(content_revision)

    for collection_name in ("panes", "spaces"):
        collection = data.get(collection_name)
        if not isinstance(collection, dict):
            continue
        for entry in collection.values():
            if not isinstance(entry, dict):
                continue
            for field in (
                "tendwire_job_key",
                "active_turn_job_key",
                "pending_turn_job_key",
                "last_clean_job_key",
            ):
                value = entry.get(field)
                if isinstance(value, str):
                    job_keys.add(value)
            for field in ("active_turn_job_keys", "pending_turn_job_keys"):
                values = entry.get(field)
                if isinstance(values, list):
                    job_keys.update(value for value in values if isinstance(value, str))
            entry_plan_tokens: set[str] = set()
            for field in (
                "last_clean_plan_token",
                "active_plan_token",
                "pending_plan_token",
                "tendwire_plan_token",
            ):
                value = entry.get(field)
                if isinstance(value, str) and value:
                    entry_plan_tokens.add(value)
            plan_tokens.update(entry_plan_tokens)
            if not entry_plan_tokens:
                for field in (
                    "last_clean_content_revision",
                    "active_content_revision",
                    "pending_content_revision",
                ):
                    value = entry.get(field)
                    if isinstance(value, str) and value:
                        content_revisions.add(value)
            message_ids: list[Any] = [entry.get("last_clean_message_id")]
            stored_ids = entry.get("last_clean_message_ids")
            if isinstance(stored_ids, list):
                message_ids.extend(stored_ids)
            for message_id in message_ids:
                binding = bindings.get(str(message_id or ""))
                if isinstance(binding, dict) and binding in active_bindings:
                    job_key = binding.get("tendwire_job_key")
                    if isinstance(job_key, str):
                        job_keys.add(job_key)

    return job_keys, plan_tokens, content_revisions


def cleanup_tendwire_turn_jobs(
    data: dict[str, Any], *, max_records: int = TENDWIRE_TURN_JOB_LIMIT
) -> int:
    """Remove only oldest terminal receipts not required by live delivery state."""
    max_records = _tendwire_job_index(
        max_records, field="max_records", positive=False
    )
    jobs = tendwire_turn_jobs(data)
    excess = len(jobs) - max_records
    if excess <= 0:
        return 0
    job_refs, plan_refs, revision_refs = _active_tendwire_turn_job_references(data)
    removable: list[tuple[int, str]] = []
    for job_key, receipt in jobs.items():
        if not isinstance(receipt, dict):
            continue
        if receipt.get("substate") not in _TENDWIRE_TURN_JOB_TERMINAL_SUBSTATES:
            continue
        if (
            job_key in job_refs
            or receipt.get("plan_token") in plan_refs
            or receipt.get("content_revision") in revision_refs
        ):
            continue
        sequence = receipt.get("checkpoint_sequence")
        if isinstance(sequence, bool) or not isinstance(sequence, int):
            sequence = 0
        removable.append((sequence, job_key))
    removable.sort(key=lambda item: (item[0], item[1]))
    for _sequence, job_key in removable[:excess]:
        jobs.pop(job_key, None)
    return min(excess, len(removable))


def reserve_tendwire_turn_job(
    data: dict[str, Any],
    job_key: str,
    *,
    plan_token: str,
    content_revision: str,
    operation: str,
    sequence_index: int,
    part_ordinal: int,
    part_count: int,
    telegram_message_id: str = "",
    prior_message_id: str = "",
    bot_kind: str = "",
) -> dict[str, Any]:
    """Reserve one immutable delivery intent, idempotently, by stable outbox key."""
    key_plan_token, key_sequence = _tendwire_job_key_parts(job_key)
    plan_token = _tendwire_opaque_token(plan_token, kind="plan_token")
    content_revision = _tendwire_opaque_token(
        content_revision, kind="content_revision"
    )
    if key_plan_token != plan_token:
        raise ValueError("tendwire job key plan mismatch")
    sequence_index = _tendwire_job_index(
        sequence_index, field="sequence_index"
    )
    if key_sequence != sequence_index:
        raise ValueError("tendwire job key sequence mismatch")
    if operation not in {"upsert", "retire"}:
        raise ValueError("invalid operation")
    part_ordinal = _tendwire_job_index(part_ordinal, field="part_ordinal")
    part_count = _tendwire_job_index(
        part_count, field="part_count", positive=True
    )
    if operation == "upsert" and part_ordinal >= part_count:
        raise ValueError("upsert ordinal outside part count")

    optional_values = {
        "telegram_message_id": (telegram_message_id, 80),
        "prior_message_id": (prior_message_id, 80),
        "bot_kind": (bot_kind, 40),
    }
    clean_optional: dict[str, str] = {}
    for field, (value, limit) in optional_values.items():
        if value:
            clean_optional[field] = _tendwire_job_outcome_value(
                value, field=field, limit=limit
            )

    immutable = {
        "plan_token": plan_token,
        "content_revision": content_revision,
        "operation": operation,
        "sequence_index": sequence_index,
        "part_ordinal": part_ordinal,
        "part_count": part_count,
    }
    jobs = tendwire_turn_jobs(data)
    existing = jobs.get(job_key)
    if existing is not None:
        if not isinstance(existing, dict):
            raise ValueError("invalid existing tendwire job receipt")
        if any(existing.get(field) != value for field, value in immutable.items()):
            raise ValueError("conflicting tendwire job reservation")
        for field, value in clean_optional.items():
            if existing.get(field) not in (None, "", value):
                raise ValueError("conflicting tendwire job outcome")
        return existing

    cleanup_tendwire_turn_jobs(
        data, max_records=TENDWIRE_TURN_JOB_LIMIT - 1
    )
    if len(jobs) >= TENDWIRE_TURN_JOB_LIMIT:
        raise RuntimeError("tendwire turn job ledger is full")
    receipt: dict[str, Any] = {
        **immutable,
        **clean_optional,
        "substate": "reserved",
        "checkpoint_sequence": _next_tendwire_turn_job_checkpoint(data),
    }
    jobs[job_key] = receipt
    return receipt


def update_tendwire_turn_job(
    data: dict[str, Any],
    job_key: str,
    *,
    substate: str,
    telegram_message_id: str | None = None,
    prior_message_id: str | None = None,
    bot_kind: str | None = None,
) -> dict[str, Any]:
    """Advance a reserved intent through the durable Telegram/ACK substates."""
    receipt = find_tendwire_turn_job(data, job_key)
    if receipt is None:
        raise KeyError(job_key)
    if substate not in TENDWIRE_TURN_JOB_SUBSTATES:
        raise ValueError("invalid tendwire job substate")
    current = receipt.get("substate")
    transitions = {
        "reserved": {
            "reserved",
            "retryable",
            "telegram_applied",
            "suppressed",
            "failed",
        },
        "retryable": {"retryable", "reserved", "failed"},
        "telegram_applied": {
            "telegram_applied",
            "old_slot_retired",
            "acknowledged",
            "failed",
        },
        "old_slot_retired": {"old_slot_retired", "acknowledged", "failed"},
        "suppressed": {"suppressed", "acknowledged", "failed"},
        "acknowledged": {"acknowledged"},
        "failed": {"failed"},
    }
    if current not in transitions or substate not in transitions[current]:
        raise ValueError(f"invalid tendwire job transition {current!r} -> {substate!r}")

    updates = {
        "telegram_message_id": (telegram_message_id, 80),
        "prior_message_id": (prior_message_id, 80),
        "bot_kind": (bot_kind, 40),
    }
    for field, (value, limit) in updates.items():
        if value is None:
            continue
        clean = _tendwire_job_outcome_value(value, field=field, limit=limit)
        if receipt.get(field) not in (None, "", clean):
            raise ValueError("conflicting tendwire job outcome")
        receipt[field] = clean

    if substate == "telegram_applied":
        if receipt.get("operation") == "upsert" and not receipt.get(
            "telegram_message_id"
        ):
            raise ValueError("upsert telegram outcome requires message id")
        if receipt.get("operation") == "retire" and not receipt.get(
            "prior_message_id"
        ):
            raise ValueError("retire telegram outcome requires prior message id")
    if substate == "old_slot_retired":
        if receipt.get("operation") != "upsert" or not receipt.get(
            "prior_message_id"
        ):
            raise ValueError("old slot retirement requires an upsert prior message")

    if substate != current or any(value is not None for value, _limit in updates.values()):
        receipt["substate"] = substate
        receipt["checkpoint_sequence"] = _next_tendwire_turn_job_checkpoint(data)
    return receipt


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
    content_revision: str = "",
    plan_token: str = "",
    part_ordinal: int | None = None,
    part_count: int | None = None,
    tendwire_job_key: str = "",
    submission_id: str = "",
) -> None:
    message = str(message_id or "").strip()
    if not message or message == "0":
        return
    bindings = message_bindings(data)
    binding = {
        "topic_id": str(topic_id or entry.get("topic_id") or ""),
        "worker_id": str(entry.get("tendwire_worker_id") or entry.get("active_worker_id") or ""),
        "worker_fingerprint": str(entry.get("tendwire_fingerprint") or entry.get("active_worker_fingerprint") or ""),
        "space_id": str(entry.get("tendwire_space_id") or entry.get("space_id") or ""),
        "kind": str(kind or ""),
        "turn_id": str(turn_id or ""),
        "bot_kind": str(bot_kind or ""),
    }
    if submission_id:
        binding["submission_id"] = str(submission_id)
    delivery_values_present = bool(
        content_revision
        or plan_token
        or tendwire_job_key
        or part_ordinal is not None
        or part_count is not None
    )
    if delivery_values_present:
        content_revision = _tendwire_opaque_token(
            content_revision, kind="content_revision"
        )
        plan_token = _tendwire_opaque_token(plan_token, kind="plan_token")
        key_plan_token, _key_sequence = _tendwire_job_key_parts(
            tendwire_job_key
        )
        if key_plan_token != plan_token:
            raise ValueError("binding job key plan mismatch")
        part_ordinal = _tendwire_job_index(
            part_ordinal, field="part_ordinal"
        )
        part_count = _tendwire_job_index(
            part_count, field="part_count", positive=True
        )
        if part_ordinal >= part_count:
            raise ValueError("binding ordinal outside part count")
        binding.update(
            {
                "content_revision": content_revision,
                "plan_token": plan_token,
                "part_ordinal": part_ordinal,
                "part_count": part_count,
                "tendwire_job_key": tendwire_job_key,
            }
        )
    identity = entry_stable_identity(entry)
    if identity is not None:
        binding["stable_key"] = identity[0]
        binding["stable_key_version"] = identity[1]
    pane_uuid = entry_pane_uuid(entry)
    if pane_uuid:
        binding["pane_uuid"] = pane_uuid
        binding["pane_uuid_version"] = PANE_UUID_VERSION
    if entry_is_quarantined(entry):
        binding["routing_quarantined"] = True
    bindings[message] = binding
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
