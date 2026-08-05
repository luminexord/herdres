"""Single-writer durable ingress queue with strict SQLite and CAS boundaries."""

from __future__ import annotations

import base64
import contextlib
import fcntl
import hashlib
import json
import math
import os
import secrets
import sqlite3
import stat
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterator, Mapping
from urllib.parse import quote

from .ingress_identity import validate_request_id


SCHEMA_VERSION = 1
INPUT_LIMIT = OPERATION_LIMIT = RECEIPT_LIMIT = 64 * 1024
NOTICE_EVIDENCE_SECONDS = 604_800.0
OVERFLOW_NOTICE_SECONDS = 60.0
_WRITER_LOCK_LEAF = ".inbound-spool.sqlite.writer.lock"
_RETRY_DISPOSITIONS = frozenset({"definitely_not_started", "no_receipt", "in_progress", "lease_expired"})
_TERMINAL_DISPOSITIONS = frozenset({"terminal_accepted", "terminal_rejected", "local_applied", "local_markup_applied"})
_PHASE_PAIRS = frozenset({
        ("checkpointed", "checkpointed"), ("checkpointed", "state_applied"),
        ("state_applied", "state_applied"), ("state_applied", "provider_ready"),
        ("provider_ready", "provider_ready"), ("provider_ready", "provider_applied"),
        ("provider_applied", "provider_applied"), ("provider_applied", "markup_recorded"),
})

_DDL = """
CREATE TABLE receiver_cursors (receiver_id TEXT PRIMARY KEY,next_update_id INTEGER NOT NULL CHECK(next_update_id >= 0),updated_at REAL NOT NULL);
CREATE TABLE requests (seq INTEGER PRIMARY KEY AUTOINCREMENT,request_id TEXT NOT NULL UNIQUE,receiver_id TEXT NOT NULL,update_id INTEGER NOT NULL CHECK(update_id >= 0),ordering_key TEXT NOT NULL,kind TEXT NOT NULL CHECK(kind IN ('message','decision')),input_json TEXT NOT NULL,command_json TEXT,command_digest TEXT,local_action_json TEXT,local_action_digest TEXT,
 local_phase TEXT CHECK(local_phase IN ('checkpointed','state_applied','provider_ready','provider_applied','markup_recorded') OR local_phase IS NULL),local_expected_state_token TEXT,local_applied_state_token TEXT,local_provider_state_token TEXT,local_markup_state_token TEXT,local_provider_outcome TEXT CHECK(local_provider_outcome IN ('accepted','not_modified') OR local_provider_outcome IS NULL),local_provider_at REAL,
 target_stable_key TEXT,target_stable_key_version INTEGER,target_route_generation TEXT,target_worker_id TEXT,target_space_id TEXT,target_bot_kind TEXT,route_refresh_count INTEGER NOT NULL DEFAULT 0 CHECK(route_refresh_count IN (0,1)),state TEXT NOT NULL CHECK(state IN ('pending','processing','retry','terminal','quarantine')),attempts INTEGER NOT NULL DEFAULT 0 CHECK(attempts >= 0),first_seen_at REAL NOT NULL,deadline_at REAL NOT NULL,retain_until REAL NOT NULL,next_attempt_at REAL NOT NULL,lease_owner TEXT,lease_until REAL,disposition TEXT,
 receipt_kind TEXT CHECK(receipt_kind IN ('daemon','local') OR receipt_kind IS NULL),receipt_json TEXT,terminal_reply TEXT,quarantine_reason TEXT,notify_state TEXT NOT NULL CHECK(notify_state IN ('none','pending','claimed','sent')),notice_claim_id TEXT,notice_claimed_at REAL,notice_message_id TEXT,created_at REAL NOT NULL,updated_at REAL NOT NULL,UNIQUE(receiver_id,update_id),
 CHECK(first_seen_at < deadline_at AND deadline_at < retain_until),CHECK((state='processing' AND lease_owner IS NOT NULL AND lease_until IS NOT NULL) OR (state!='processing' AND lease_owner IS NULL AND lease_until IS NULL)),CHECK((command_json IS NULL)=(command_digest IS NULL)),CHECK((local_action_json IS NULL)=(local_action_digest IS NULL)),CHECK(command_json IS NULL OR local_action_json IS NULL),
 CHECK((local_action_json IS NULL AND local_phase IS NULL AND local_expected_state_token IS NULL AND local_applied_state_token IS NULL AND local_provider_state_token IS NULL AND local_markup_state_token IS NULL AND local_provider_outcome IS NULL AND local_provider_at IS NULL) OR (local_action_json IS NOT NULL AND local_phase IS NOT NULL AND local_expected_state_token IS NOT NULL)),CHECK(local_phase IS NULL OR local_phase!='checkpointed' OR (local_applied_state_token IS NULL AND local_provider_state_token IS NULL AND local_markup_state_token IS NULL AND local_provider_outcome IS NULL AND local_provider_at IS NULL)),
 CHECK(local_phase IS NULL OR local_phase NOT IN ('state_applied','provider_ready','provider_applied','markup_recorded') OR local_applied_state_token IS NOT NULL),CHECK(local_phase IS NULL OR local_phase NOT IN ('provider_ready','provider_applied','markup_recorded') OR local_provider_state_token IS NOT NULL),CHECK(local_phase IS NULL OR local_phase NOT IN ('provider_applied','markup_recorded') OR (local_provider_outcome IS NOT NULL AND local_provider_at IS NOT NULL)),CHECK(local_phase IS NULL OR local_phase!='markup_recorded' OR local_markup_state_token IS NOT NULL),
 CHECK(target_stable_key_version IS NULL OR target_stable_key_version=1),CHECK((receipt_kind IS NULL AND receipt_json IS NULL) OR (receipt_kind IS NOT NULL AND receipt_json IS NOT NULL)),CHECK(notify_state IN ('none','pending') OR (notice_claim_id IS NOT NULL AND notice_claimed_at IS NOT NULL)),CHECK(notify_state!='sent' OR notice_message_id IS NOT NULL));
CREATE INDEX requests_open_head ON requests(ordering_key,seq) WHERE state IN ('pending','processing','retry');CREATE INDEX requests_retry ON requests(next_attempt_at,seq) WHERE state IN ('pending','retry');
CREATE INDEX requests_lease ON requests(lease_until) WHERE state='processing';CREATE INDEX requests_retention ON requests(retain_until);
"""


@dataclass(frozen=True, slots=True)
class AcceptResult:
    status: str
    next_update_id: int
    seq: int | None = None


@dataclass(frozen=True, slots=True)
class StoreResult:
    status: str
    seq: int
    digest: str | None = None


@dataclass(frozen=True, slots=True)
class SettleResult:
    status: str
    seq: int


@dataclass(frozen=True, slots=True)
class QueueItem:
    seq: int
    request_id: str
    receiver_id: str
    update_id: int
    ordering_key: str
    kind: str
    input: dict[str, Any]
    state: str
    attempts: int
    deadline_at: float
    lease_owner: str
    lease_until: float
    command: dict[str, Any] | None
    local_action: dict[str, Any] | None
    operation_digest: str | None
    local_phase: str | None
    local_expected_state_token: str | None
    local_applied_state_token: str | None
    local_provider_state_token: str | None
    local_markup_state_token: str | None
    local_provider_outcome: str | None
    local_provider_at: float | None


@dataclass(frozen=True, slots=True)
class NoticeClaim:
    seq: int
    claim_id: str
    receiver_id: str
    input: dict[str, Any]
    terminal_reply: str


@dataclass(frozen=True, slots=True)
class QueueHealth:
    pending: int
    processing: int
    retry: int
    terminal: int
    quarantine: int
    pending_notices: int
    claimed_notices: int
    expired_leases: int
    overdue_open: int


@dataclass(frozen=True, slots=True)
class QueueStatus:
    state: str
    kind: str
    count: int
    oldest_seconds: float


def _invalid(name: str) -> Any:
    raise ValueError(f"invalid {name}")


def _time(value: Any, name: str = "timestamp") -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return _invalid(name)
    result = float(value)
    return result if math.isfinite(result) else _invalid(name)


def _integer(value: Any, name: str, *, minimum: int = 0) -> int:
    if not isinstance(value, bool) and isinstance(value, int) and value >= minimum:
        return value
    return _invalid(name)


def _text(value: Any, name: str, limit: int = 256) -> str:
    if isinstance(value, str) and value and len(value.encode()) <= limit:
        return value
    return _invalid(name)


def _optional_text(value: Any, name: str, limit: int = 256) -> str | None:
    return None if value is None else _text(value, name, limit)


def _canonical(value: Any, limit: int, name: str) -> str:
    if not isinstance(value, Mapping):
        raise ValueError(f"invalid {name}")
    try:
        encoded = json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )
        decoded = json.loads(encoded)
    except (TypeError, ValueError, json.JSONDecodeError) as exc:
        raise ValueError(f"invalid {name}") from exc
    if not isinstance(decoded, dict) or len(encoded.encode()) > limit:
        raise ValueError(f"invalid {name}")
    return encoded


def _decode(value: str | None, limit: int, name: str) -> dict[str, Any] | None:
    if value is None:
        return None
    try:
        decoded = json.loads(value)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"corrupt {name}") from exc
    if _canonical(decoded, limit, name) != value:
        raise RuntimeError(f"corrupt {name}")
    return decoded


def _digest(value: str) -> str:
    digest = hashlib.sha256(value.encode()).digest()
    return base64.urlsafe_b64encode(digest).rstrip(b"=").decode()


def _mapping(
    value: Mapping[str, Any], required: set[str], optional: set[str] = frozenset()
) -> dict[str, Any]:
    result = dict(value)
    if required <= set(result) <= required | optional:
        return result
    return _invalid("queue operation fields")


def _same_file(left: os.stat_result, right: os.stat_result) -> bool:
    return (left.st_dev, left.st_ino) == (right.st_dev, right.st_ino)


def _safe_regular(metadata: os.stat_result) -> bool:
    return (
        stat.S_ISREG(metadata.st_mode) and metadata.st_uid == os.geteuid()
        and stat.S_IMODE(metadata.st_mode) == 0o600
        and metadata.st_nlink == 1
    )


def _pin_parent(path: Path) -> tuple[int, str]:
    if not path.is_absolute() or path.name in {"", ".", ".."} or ".." in path.parts:
        raise OSError("ingress queue path must be an absolute leaf")
    flags = os.O_RDONLY | os.O_DIRECTORY | getattr(os, "O_CLOEXEC", 0)
    parent_fd = os.open("/", flags)
    try:
        for part in path.parts[1:-1]:
            next_fd = os.open(part, flags | getattr(os, "O_NOFOLLOW", 0), dir_fd=parent_fd)
            os.close(parent_fd)
            parent_fd = next_fd
        metadata = os.fstat(parent_fd)
        if metadata.st_uid != os.geteuid() or stat.S_IMODE(metadata.st_mode) & 0o022:
            raise OSError("ingress queue parent is not private and owned")
        return parent_fd, path.name
    except BaseException:
        os.close(parent_fd)
        raise


def _open_owned(parent_fd: int, leaf: str, *, create: bool, readonly: bool = False) -> int:
    flags = (
        (os.O_RDONLY if readonly else os.O_RDWR) | getattr(os, "O_CLOEXEC", 0)
        | getattr(os, "O_NOFOLLOW", 0)
    )
    if create:
        flags |= os.O_CREAT | os.O_EXCL
    fd = os.open(leaf, flags, 0o600, dir_fd=parent_fd)
    metadata = os.fstat(fd)
    if not _safe_regular(metadata):
        os.close(fd)
        raise OSError("ingress queue file is unsafe")
    return fd


def _open_leaf(parent_fd: int, leaf: str, *, readonly: bool) -> tuple[int, bool]:
    try:
        return _open_owned(parent_fd, leaf, create=False, readonly=readonly), False
    except FileNotFoundError:
        if readonly:
            raise
        old_umask = os.umask(0o077)
        try:
            fd = _open_owned(parent_fd, leaf, create=True)
            os.fchmod(fd, 0o600)
            return fd, True
        finally:
            os.umask(old_umask)


def _anchored(parent_fd: int, leaf: str) -> str:
    anchor = f"/proc/self/fd/{parent_fd}"
    if not _same_file(os.fstat(parent_fd), os.stat(anchor)):
        raise OSError("ingress queue anchoring is unavailable")
    return f"{anchor}/{leaf}"


def _validate_named(
    parent_fd: int, leaf: str, expected: os.stat_result | None = None
) -> os.stat_result:
    metadata = os.stat(leaf, dir_fd=parent_fd, follow_symlinks=False)
    if not _safe_regular(metadata) or expected is not None and not _same_file(metadata, expected):
        raise OSError("ingress queue family changed or is unsafe")
    return metadata


def _validate_sidecars(
    parent_fd: int,
    leaf: str,
    known: Mapping[str, os.stat_result] | None = None,
) -> dict[str, os.stat_result]:
    current: dict[str, os.stat_result] = {}
    for suffix in ("-wal", "-shm"):
        try:
            expected = None if known is None else known.get(suffix)
            current[suffix] = _validate_named(parent_fd, leaf + suffix, expected)
        except FileNotFoundError:
            if known is not None and suffix in known:
                raise OSError("ingress queue sidecar disappeared")
            continue
    return current


def _schema_fingerprint(
    connection: sqlite3.Connection,
) -> tuple[tuple[str, str, str], ...]:
    rows = connection.execute(
        "SELECT type,name,sql FROM sqlite_master "
        "WHERE name NOT LIKE 'sqlite_%' ORDER BY type,name"
    )
    return tuple((str(row[0]), str(row[1]), " ".join(str(row[2]).split())) for row in rows)


def _expected_schema() -> tuple[tuple[str, str, str], ...]:
    connection = sqlite3.connect(":memory:")
    try:
        connection.executescript(_DDL)
        return _schema_fingerprint(connection)
    finally:
        connection.close()


_EXPECTED_SCHEMA = _expected_schema()


def _configure(connection: sqlite3.Connection, *, writer: bool) -> None:
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA busy_timeout=30000")
    connection.execute("PRAGMA trusted_schema=OFF")
    connection.execute("PRAGMA foreign_keys=ON")
    if writer:
        connection.execute("PRAGMA synchronous=FULL")
        if connection.execute("PRAGMA journal_mode=WAL").fetchone()[0] != "wal":
            raise RuntimeError("ingress queue did not enter WAL mode")
    else:
        connection.execute("PRAGMA query_only=ON")


class IngressQueue:
    def __init__(
        self, connection: sqlite3.Connection, parent_fd: int, file_fd: int,
        lock_fd: int, leaf: str, sidecars: Mapping[str, os.stat_result],
    ) -> None:
        self._connection = connection
        self._parent_fd = parent_fd
        self._file_fd = file_fd
        self._lock_fd = lock_fd
        self._leaf = leaf
        self._mutex = threading.RLock()
        self._closed = False
        self._sidecars = dict(sidecars)

    @classmethod
    def open_writer(cls, path: str | Path) -> "IngressQueue":
        parent_fd, leaf = _pin_parent(Path(path).expanduser())
        file_fd = lock_fd = -1
        try:
            old_umask = os.umask(0o077)
            try:
                lock_fd, _ = _open_leaf(parent_fd, _WRITER_LOCK_LEAF, readonly=False)
                fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                file_fd, created = _open_leaf(parent_fd, leaf, readonly=False)
                connection = sqlite3.connect(
                    _anchored(parent_fd, leaf), isolation_level=None, check_same_thread=False
                )
                if not created:
                    connection.execute("PRAGMA trusted_schema=OFF")
                    unsupported = (
                        connection.execute("PRAGMA user_version").fetchone()[0] != SCHEMA_VERSION
                        or _schema_fingerprint(connection) != _EXPECTED_SCHEMA
                        or connection.execute("PRAGMA integrity_check").fetchone()[0] != "ok"
                    )
                    if unsupported:
                        raise RuntimeError("unsupported or corrupt ingress queue schema")
                _configure(connection, writer=True)
                _validate_named(parent_fd, leaf, os.fstat(file_fd))
                _validate_named(parent_fd, _WRITER_LOCK_LEAF, os.fstat(lock_fd))
                if created:
                    connection.executescript(
                        f"BEGIN IMMEDIATE;{_DDL}"
                        f"PRAGMA user_version={SCHEMA_VERSION};COMMIT;"
                    )
                if _schema_fingerprint(connection) != _EXPECTED_SCHEMA:
                    raise RuntimeError("ingress queue schema is corrupt")
                if connection.execute("PRAGMA integrity_check").fetchone()[0] != "ok":
                    raise RuntimeError("ingress queue integrity check failed")
                return cls(
                    connection, parent_fd, file_fd, lock_fd, leaf,
                    _validate_sidecars(parent_fd, leaf),
                )
            finally:
                os.umask(old_umask)
        except BaseException:
            if "connection" in locals():
                connection.close()
            for fd in (file_fd, lock_fd, parent_fd):
                if fd >= 0:
                    os.close(fd)
            raise

    @classmethod
    @contextlib.contextmanager
    def observe(cls, path: str | Path) -> Iterator["IngressQueueObserver"]:
        observer = IngressQueueObserver._open(path)
        try:
            yield observer
        finally:
            observer.close()

    def __enter__(self) -> "IngressQueue":
        return self

    def __exit__(self, *_args: Any) -> None:
        self.close()

    def close(self) -> None:
        with self._mutex:
            if self._closed:
                return
            error: BaseException | None = None
            try:
                self._validate()
            except BaseException as exc:
                error = exc
            self._connection.close()
            for fd in (self._file_fd, self._lock_fd, self._parent_fd):
                os.close(fd)
            self._closed = True
            if error is not None:
                raise error

    def _validate(self) -> None:
        if self._closed:
            raise RuntimeError("ingress queue is closed")
        _validate_named(self._parent_fd, _WRITER_LOCK_LEAF, os.fstat(self._lock_fd))
        _validate_named(self._parent_fd, self._leaf, os.fstat(self._file_fd))
        self._sidecars.update(_validate_sidecars(self._parent_fd, self._leaf, self._sidecars))

    @contextlib.contextmanager
    def _write(self) -> Iterator[sqlite3.Connection]:
        with self._mutex:
            self._validate()
            self._connection.execute("BEGIN IMMEDIATE")
            try:
                yield self._connection
                self._connection.commit()
            except BaseException:
                self._connection.rollback()
                raise

    def cursor(self, receiver_id: str) -> int | None:
        with self._mutex:
            self._validate()
            row = self._connection.execute(
                "SELECT next_update_id FROM receiver_cursors WHERE receiver_id=?",
                (_text(receiver_id, "receiver_id", 128),),
            ).fetchone()
            return None if row is None else int(row[0])

    @staticmethod
    def _cursor_tx(
        connection: sqlite3.Connection, receiver_id: str, update_id: int, now: float
    ) -> int:
        connection.execute(
            "INSERT INTO receiver_cursors VALUES(?,?,?) "
            "ON CONFLICT(receiver_id) DO UPDATE SET "
            "next_update_id=MAX(next_update_id,excluded.next_update_id),"
            "updated_at=excluded.updated_at",
            (receiver_id, update_id, now),
        )
        row = connection.execute(
            "SELECT next_update_id FROM receiver_cursors WHERE receiver_id=?",
            (receiver_id,),
        ).fetchone()
        return int(row[0])

    def initialize_cursor(self, receiver_id: str, next_update_id: int) -> int:
        receiver = _text(receiver_id, "receiver_id", 128)
        update = _integer(next_update_id, "next_update_id")
        now = time.time()
        with self._write() as connection:
            return self._cursor_tx(connection, receiver, update, now)

    def accept_update(self, acceptance: Mapping[str, Any]) -> AcceptResult:
        optional = {
            "request_id", "ordering_key", "kind", "input", "deadline_at",
            "retain_until", "depth_limit", "enqueue", "overflow_reply",
        }
        data = _mapping(acceptance, {"receiver_id", "update_id", "first_seen_at"}, optional)
        receiver = _text(data["receiver_id"], "receiver_id", 128)
        update_id = _integer(data["update_id"], "update_id")
        now = _time(data["first_seen_at"])
        enqueue = data.get("enqueue", True)
        if not isinstance(enqueue, bool):
            raise ValueError("invalid enqueue")
        if not enqueue:
            with self._write() as connection:
                return AcceptResult("advanced", self._cursor_tx(connection, receiver, update_id + 1, now))
        request_id = validate_request_id(data.get("request_id"))
        ordering_key = _text(data.get("ordering_key"), "ordering_key", 512)
        kind = data.get("kind")
        if kind not in {"message", "decision"}:
            raise ValueError("invalid request kind")
        input_json = _canonical(data.get("input"), INPUT_LIMIT, "input")
        deadline = _time(data.get("deadline_at"), "deadline_at")
        retain = _time(data.get("retain_until"), "retain_until")
        if not now < deadline < retain:
            raise ValueError("invalid request horizons")
        depth_limit = _integer(data.get("depth_limit", 32), "depth_limit", minimum=1)
        overflow_reply = _optional_text(data.get("overflow_reply"), "overflow_reply", 160)
        overflow_reply = overflow_reply or "The request could not be queued."
        with self._write() as connection:
            existing = connection.execute(
                "SELECT * FROM requests "
                "WHERE request_id=? OR (receiver_id=? AND update_id=?)",
                (request_id, receiver, update_id),
            ).fetchall()
            if existing:
                identity = (
                    ("request_id", request_id),
                    ("receiver_id", receiver),
                    ("update_id", update_id),
                    ("ordering_key", ordering_key),
                    ("kind", kind),
                    ("input_json", input_json),
                )
                if len(existing) != 1 or any(existing[0][key] != value for key, value in identity):
                    raise RuntimeError("ingress queue identity collision")
                cursor = self._cursor_tx(connection, receiver, update_id + 1, now)
                return AcceptResult("duplicate", cursor, int(existing[0]["seq"]))
            depth = int(
                connection.execute(
                    "SELECT COUNT(*) FROM requests WHERE ordering_key=? "
                    "AND state IN ('pending','processing','retry')",
                    (ordering_key,),
                ).fetchone()[0]
            )
            overflow = depth >= depth_limit
            notify = "none"
            if overflow:
                recent = connection.execute(
                    "SELECT 1 FROM requests WHERE ordering_key=? "
                    "AND disposition='queue_overflow' AND created_at>? "
                    "AND notify_state IN ('pending','claimed','sent') LIMIT 1",
                    (ordering_key, now - OVERFLOW_NOTICE_SECONDS),
                ).fetchone()
                notify = "pending" if recent is None else "none"
            inserted = connection.execute(
                """INSERT INTO requests(
                request_id,receiver_id,update_id,ordering_key,kind,input_json,state,
                attempts,first_seen_at,deadline_at,retain_until,next_attempt_at,
                disposition,terminal_reply,quarantine_reason,notify_state,
                created_at,updated_at
                ) VALUES(?,?,?,?,?,?,?,0,?,?,?,?,?,?,?,?,?,?)""",
                (
                    request_id, receiver, update_id, ordering_key, kind, input_json,
                    "quarantine" if overflow else "pending", now, deadline,
                    max(retain, now + OVERFLOW_NOTICE_SECONDS) if overflow else retain,
                    now, "queue_overflow" if overflow else None,
                    overflow_reply if overflow else None,
                    "queue_overflow" if overflow else None, notify, now, now,
                ),
            )
            cursor = self._cursor_tx(connection, receiver, update_id + 1, now)
            return AcceptResult("overflow" if overflow else "enqueued", cursor, int(inserted.lastrowid))

    @staticmethod
    def _expire_tx(connection: sqlite3.Connection, now: float) -> None:
        connection.execute(
            """UPDATE requests
            SET state='retry',next_attempt_at=?,disposition='lease_expired',
                lease_owner=NULL,lease_until=NULL,updated_at=?
            WHERE state='processing' AND lease_until<=? AND deadline_at>?""",
            (now, now, now, now),
        )
        connection.execute(
            """UPDATE requests
            SET state='quarantine',disposition='deadline_expired',
                quarantine_reason='deadline_expired',
                terminal_reply='The request deadline expired before a certain result.',
                notify_state='pending',lease_owner=NULL,lease_until=NULL,updated_at=?
            WHERE state IN ('pending','processing','retry') AND deadline_at<=?""",
            (now, now),
        )

    @classmethod
    def _owned_row_tx(
        cls, connection: sqlite3.Connection, seq: int, lease_owner: str, now: float,
    ) -> sqlite3.Row | None:
        cls._expire_tx(connection, now)
        row = connection.execute("SELECT * FROM requests WHERE seq=?", (seq,)).fetchone()
        if (
            row is None
            or row["state"] != "processing"
            or row["lease_owner"] != lease_owner
            or float(row["lease_until"]) <= now
            or float(row["deadline_at"]) <= now
        ):
            return None
        return row

    @staticmethod
    def _item(row: sqlite3.Row) -> QueueItem:
        command = _decode(row["command_json"], OPERATION_LIMIT, "command")
        local = _decode(row["local_action_json"], OPERATION_LIMIT, "local_action")
        if command is not None and _digest(row["command_json"]) != row["command_digest"]:
            raise RuntimeError("corrupt command digest")
        if local is not None and _digest(row["local_action_json"]) != row["local_action_digest"]:
            raise RuntimeError("corrupt local action digest")
        payload = _decode(row["input_json"], INPUT_LIMIT, "input")
        if payload is None:
            raise RuntimeError("corrupt input")
        return QueueItem(
            seq=int(row["seq"]), request_id=row["request_id"], receiver_id=row["receiver_id"],
            update_id=int(row["update_id"]), ordering_key=row["ordering_key"], kind=row["kind"],
            input=payload, state=row["state"], attempts=int(row["attempts"]),
            deadline_at=float(row["deadline_at"]), lease_owner=row["lease_owner"],
            lease_until=float(row["lease_until"]), command=command, local_action=local,
            operation_digest=row["command_digest"] or row["local_action_digest"],
            local_phase=row["local_phase"],
            local_expected_state_token=row["local_expected_state_token"],
            local_applied_state_token=row["local_applied_state_token"],
            local_provider_state_token=row["local_provider_state_token"],
            local_markup_state_token=row["local_markup_state_token"],
            local_provider_outcome=row["local_provider_outcome"],
            local_provider_at=(
                None
                if row["local_provider_at"] is None
                else float(row["local_provider_at"])
            ),
        )

    def claim(self, lease_owner: str, now: float, lease_seconds: float) -> QueueItem | None:
        owner = _text(lease_owner, "lease_owner", 128)
        current = _time(now)
        duration = _time(lease_seconds, "lease_seconds")
        if duration <= 0:
            raise ValueError("invalid lease_seconds")
        with self._write() as connection:
            self._expire_tx(connection, current)
            row = connection.execute(
                """SELECT r.* FROM requests r
                WHERE r.state IN ('pending','retry')
                  AND r.next_attempt_at<=? AND r.deadline_at>?
                  AND NOT EXISTS(
                    SELECT 1 FROM requests h
                    WHERE h.ordering_key=r.ordering_key AND h.seq<r.seq
                      AND h.state IN ('pending','processing','retry')
                  )
                ORDER BY r.seq LIMIT 1""",
                (current, current),
            ).fetchone()
            if row is None:
                return None
            lease_until = min(float(row["deadline_at"]), current + duration)
            changed = connection.execute(
                """UPDATE requests
                SET state='processing',attempts=attempts+1,
                    lease_owner=?,lease_until=?,updated_at=?
                WHERE seq=? AND state IN ('pending','retry')
                  AND next_attempt_at<=? AND deadline_at>?
                  AND NOT EXISTS(
                    SELECT 1 FROM requests h
                    WHERE h.ordering_key=requests.ordering_key AND h.seq<requests.seq
                      AND h.state IN ('pending','processing','retry')
                  )""",
                (owner, lease_until, current, row["seq"], current, current),
            ).rowcount
            if changed != 1:
                return None
            claimed = connection.execute(
                "SELECT * FROM requests WHERE seq=?", (row["seq"],)
            ).fetchone()
            return self._item(claimed)

    def renew(self, seq: int, lease_owner: str, now: float, lease_seconds: float) -> bool:
        number = _integer(seq, "seq", minimum=1)
        owner = _text(lease_owner, "lease_owner", 128)
        current = _time(now)
        duration = _time(lease_seconds, "lease_seconds")
        if duration <= 0:
            raise ValueError("invalid lease_seconds")
        with self._write() as connection:
            self._expire_tx(connection, current)
            changed = connection.execute(
                """UPDATE requests
                SET lease_until=MAX(lease_until,MIN(deadline_at,?)),updated_at=?
                WHERE seq=? AND state='processing' AND lease_owner=?
                  AND lease_until>? AND deadline_at>?""",
                (current + duration, current, number, owner, current, current),
            ).rowcount
            return changed == 1

    @staticmethod
    def _route_generation(value: Any) -> str | None:
        result = _optional_text(value, "target_route_generation", 52)
        alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_"
        if result is not None and (
            not result.startswith("twroute1.")
            or len(result) != 52
            or any(character not in alphabet for character in result[9:])
        ):
            raise ValueError("invalid target_route_generation")
        return result

    @staticmethod
    def _quarantine_conflict(
        connection: sqlite3.Connection, seq: int, lease_owner: str, now: float,
    ) -> StoreResult:
        connection.execute(
            """UPDATE requests
            SET state='quarantine',disposition='checkpoint_conflict',
                quarantine_reason='checkpoint_conflict',
                terminal_reply='The request could not be completed safely.',
                notify_state='pending',lease_owner=NULL,lease_until=NULL,updated_at=?
            WHERE seq=? AND state='processing' AND lease_owner=?
              AND lease_until>? AND deadline_at>?""",
            (now, seq, lease_owner, now, now),
        )
        return StoreResult("conflict", seq)

    def store_command(self, seq: int, lease_owner: str, checkpoint: Mapping[str, Any]) -> StoreResult:
        number = _integer(seq, "seq", minimum=1)
        owner = _text(lease_owner, "lease_owner", 128)
        required = {"command", "target_stable_key", "target_stable_key_version", "now"}
        optional = {
            "target_route_generation", "target_worker_id", "target_space_id", "target_bot_kind",
        }
        data = _mapping(checkpoint, required, optional)
        encoded = _canonical(data["command"], OPERATION_LIMIT, "command")
        stable_key = _text(data["target_stable_key"], "target_stable_key", 512)
        version = _integer(data["target_stable_key_version"], "target_stable_key_version", minimum=1)
        current = _time(data["now"], "now")
        if version != 1:
            raise ValueError("invalid target_stable_key_version")
        generation = self._route_generation(data.get("target_route_generation"))
        coordinates = (
            stable_key, version, generation,
            _optional_text(data.get("target_worker_id"), "target_worker_id", 256),
            _optional_text(data.get("target_space_id"), "target_space_id", 256),
            _optional_text(data.get("target_bot_kind"), "target_bot_kind", 64),
        )
        digest = _digest(encoded)
        with self._write() as connection:
            row = self._owned_row_tx(connection, number, owner, current)
            if row is None:
                return StoreResult("lost", number)
            command = json.loads(encoded)
            if command.get("request_id") != row["request_id"]:
                raise ValueError("command request_id mismatch")
            stored = (
                row["target_stable_key"], row["target_stable_key_version"],
                row["target_route_generation"], row["target_worker_id"],
                row["target_space_id"], row["target_bot_kind"],
            )
            if row["command_json"] is None and row["local_action_json"] is None:
                changed = connection.execute(
                    """UPDATE requests
                    SET command_json=?,command_digest=?,target_stable_key=?,
                        target_stable_key_version=?,target_route_generation=?,
                        target_worker_id=?,target_space_id=?,target_bot_kind=?,updated_at=?
                    WHERE seq=? AND state='processing' AND lease_owner=?
                      AND lease_until>? AND deadline_at>?
                      AND command_json IS NULL AND local_action_json IS NULL""",
                    (encoded, digest, *coordinates, current, number, owner, current, current),
                ).rowcount
                return StoreResult("stored" if changed == 1 else "lost", number, digest)
            if row["command_json"] == encoded and row["command_digest"] == digest and stored == coordinates:
                return StoreResult("existing", number, digest)
            old_command = _decode(row["command_json"], OPERATION_LIMIT, "command")
            old_target = old_command.get("target") if old_command else None
            new_target = command.get("target")
            removable = (
                isinstance(old_target, dict) and isinstance(new_target, dict)
                and "worker_fingerprint" in old_target
            )
            if removable:
                weakened = dict(old_target)
                weakened.pop("worker_fingerprint")
                clone = dict(old_command)
                clone["target"] = weakened
                removable = (
                    clone == command and row["target_route_generation"] is None
                    and generation is None and row["route_refresh_count"] == 0
                    and stored == coordinates
                )
            if removable:
                changed = connection.execute(
                    """UPDATE requests
                    SET command_json=?,command_digest=?,route_refresh_count=1,updated_at=?
                    WHERE seq=? AND state='processing' AND lease_owner=?
                      AND lease_until>? AND deadline_at>? AND route_refresh_count=0""",
                    (encoded, digest, current, number, owner, current, current),
                ).rowcount
                return StoreResult("refreshed" if changed == 1 else "lost", number, digest)
            return self._quarantine_conflict(connection, number, owner, current)

    def store_local_action(self, seq: int, lease_owner: str, checkpoint: Mapping[str, Any]) -> StoreResult:
        number = _integer(seq, "seq", minimum=1)
        owner = _text(lease_owner, "lease_owner", 128)
        data = _mapping(checkpoint, {"local_action", "expected_state_token", "now"})
        encoded = _canonical(data["local_action"], OPERATION_LIMIT, "local_action")
        token = _text(data["expected_state_token"], "expected_state_token", 128)
        current = _time(data["now"], "now")
        digest = _digest(encoded)
        with self._write() as connection:
            row = self._owned_row_tx(connection, number, owner, current)
            if row is None:
                return StoreResult("lost", number)
            action = json.loads(encoded)
            if action.get("request_id") != row["request_id"] or action.get("action") not in {
                "ARM_FREEFORM", "TOGGLE_OPTION",
            }:
                raise ValueError("invalid local action correlation")
            if row["local_action_json"] is None and row["command_json"] is None:
                changed = connection.execute(
                    """UPDATE requests
                    SET local_action_json=?,local_action_digest=?,
                        local_phase='checkpointed',local_expected_state_token=?,updated_at=?
                    WHERE seq=? AND state='processing' AND lease_owner=?
                      AND lease_until>? AND deadline_at>?
                      AND local_action_json IS NULL AND command_json IS NULL""",
                    (encoded, digest, token, current, number, owner, current, current),
                ).rowcount
                return StoreResult("stored" if changed == 1 else "lost", number, digest)
            if (
                row["local_action_json"] == encoded and row["local_action_digest"] == digest
                and row["local_expected_state_token"] == token
            ):
                return StoreResult("existing", number, digest)
            return self._quarantine_conflict(connection, number, owner, current)

    def advance_local_phase(self, seq: int, lease_owner: str, transition: Mapping[str, Any]) -> StoreResult:
        number = _integer(seq, "seq", minimum=1)
        owner = _text(lease_owner, "lease_owner", 128)
        required = {"operation_digest", "from_phase", "to_phase", "expected_token", "now"}
        optional = {"state_token", "provider_outcome", "provider_at"}
        data = _mapping(transition, required, optional)
        digest = _text(data["operation_digest"], "operation_digest", 64)
        old = data["from_phase"]
        new = data["to_phase"]
        if (old, new) not in _PHASE_PAIRS:
            raise ValueError("invalid local phase transition")
        expected = _text(data["expected_token"], "expected_token", 128)
        next_token = _optional_text(data.get("state_token"), "state_token", 128)
        current = _time(data["now"], "now")
        token_column = {
            "checkpointed": "local_expected_state_token",
            "state_applied": "local_applied_state_token",
            "provider_ready": "local_provider_state_token",
            "provider_applied": "local_provider_state_token",
        }[old]
        with self._write() as connection:
            row = self._owned_row_tx(connection, number, owner, current)
            if row is None or row["local_action_digest"] != digest:
                return StoreResult("lost", number, digest)
            action = _decode(row["local_action_json"], OPERATION_LIMIT, "local_action")
            if row["local_phase"] != old or row[token_column] != expected:
                return StoreResult("lost", number, digest)
            if action is None or (
                action.get("action") == "ARM_FREEFORM" and (old, new) != ("checkpointed", "state_applied")
            ):
                raise ValueError("invalid phase for local action")
            updates: dict[str, Any] = {"local_phase": new, "updated_at": current}
            if old == new:
                if next_token is None:
                    raise ValueError("token refresh requires state_token")
                updates[token_column] = next_token
            elif new == "state_applied":
                updates["local_applied_state_token"] = _text(next_token, "state_token", 128)
            elif new == "provider_ready":
                updates["local_provider_state_token"] = _text(next_token, "state_token", 128)
            elif new == "provider_applied":
                outcome = data.get("provider_outcome")
                if outcome not in {"accepted", "not_modified"}:
                    raise ValueError("invalid provider_outcome")
                updates["local_provider_outcome"] = outcome
                updates["local_provider_at"] = _time(data.get("provider_at"), "provider_at")
            elif new == "markup_recorded":
                updates["local_markup_state_token"] = _text(next_token, "state_token", 128)
            assignments = ",".join(f"{column}=?" for column in updates)
            values = [*updates.values(), number, owner, digest, old, expected, current, current]
            changed = connection.execute(
                f"""UPDATE requests SET {assignments}
                WHERE seq=? AND state='processing' AND lease_owner=?
                  AND local_action_digest=? AND local_phase=? AND {token_column}=?
                  AND lease_until>? AND deadline_at>?""",
                values,
            ).rowcount
            return StoreResult("advanced" if changed == 1 else "lost", number, digest)

    @staticmethod
    def _operation_matches(row: sqlite3.Row, digest: str) -> bool:
        return digest in {row["command_digest"], row["local_action_digest"]}

    def settle_receipt(self, seq: int, lease_owner: str, settlement: Mapping[str, Any]) -> SettleResult:
        number = _integer(seq, "seq", minimum=1)
        owner = _text(lease_owner, "lease_owner", 128)
        required = {"operation_digest", "receipt_kind", "receipt", "disposition", "now"}
        data = _mapping(settlement, required, {"terminal_reply", "notify"})
        digest = _text(data["operation_digest"], "operation_digest", 64)
        kind = data["receipt_kind"]
        disposition = data["disposition"]
        if kind not in {"daemon", "local"} or disposition not in _TERMINAL_DISPOSITIONS:
            raise ValueError("invalid terminal settlement")
        encoded = _canonical(data["receipt"], RECEIPT_LIMIT, "receipt")
        current = _time(data["now"], "now")
        reply = _optional_text(data.get("terminal_reply"), "terminal_reply", 160)
        notify = data.get("notify", False)
        if not isinstance(notify, bool) or notify and reply is None:
            raise ValueError("invalid notice settlement")
        with self._write() as connection:
            self._expire_tx(connection, current)
            row = connection.execute("SELECT * FROM requests WHERE seq=?", (number,)).fetchone()
            if row is None:
                return SettleResult("lost", number)
            if (
                row["state"] == "terminal"
                and self._operation_matches(row, digest)
                and row["receipt_kind"] == kind
                and row["receipt_json"] == encoded
                and row["disposition"] == disposition
            ):
                return SettleResult("existing", number)
            if (
                row["state"] != "processing"
                or row["lease_owner"] != owner
                or float(row["lease_until"]) <= current
                or float(row["deadline_at"]) <= current
                or not self._operation_matches(row, digest)
            ):
                return SettleResult("lost", number)
            if (kind == "daemon" and row["command_digest"] != digest) or (
                kind == "local" and row["local_action_digest"] != digest
            ):
                raise ValueError("receipt kind does not match operation")
            if kind == "local":
                action = _decode(row["local_action_json"], OPERATION_LIMIT, "local_action")
                required_phase = (
                    "state_applied" if action and action.get("action") == "ARM_FREEFORM"
                    else "markup_recorded"
                )
                if row["local_phase"] != required_phase:
                    raise ValueError("local action is not ready to settle")
            changed = connection.execute(
                """UPDATE requests
                SET state='terminal',disposition=?,receipt_kind=?,receipt_json=?,
                    terminal_reply=?,notify_state=?,lease_owner=NULL,lease_until=NULL,
                    updated_at=?
                WHERE seq=? AND state='processing' AND lease_owner=?
                  AND lease_until>? AND deadline_at>?""",
                (
                    disposition, kind, encoded, reply, "pending" if notify else "none",
                    current, number, owner, current, current,
                ),
            ).rowcount
            return SettleResult("settled" if changed == 1 else "lost", number)

    def schedule_retry(self, seq: int, lease_owner: str, retry: Mapping[str, Any]) -> SettleResult:
        number = _integer(seq, "seq", minimum=1)
        owner = _text(lease_owner, "lease_owner", 128)
        data = _mapping(retry, {"operation_digest", "disposition", "now", "next_attempt_at"})
        digest = _text(data["operation_digest"], "operation_digest", 64)
        disposition = data["disposition"]
        if disposition not in _RETRY_DISPOSITIONS:
            raise ValueError("invalid retry disposition")
        current = _time(data["now"], "now")
        next_attempt = _time(data["next_attempt_at"], "next_attempt_at")
        if next_attempt < current:
            raise ValueError("invalid next_attempt_at")
        with self._write() as connection:
            row = self._owned_row_tx(connection, number, owner, current)
            if row is None or not self._operation_matches(row, digest):
                return SettleResult("lost", number)
            if next_attempt >= row["deadline_at"]:
                changed = connection.execute(
                    """UPDATE requests
                    SET state='quarantine',disposition='deadline_expired',
                        quarantine_reason='deadline_expired',notify_state='pending',
                        terminal_reply='The request deadline expired before a certain result.',
                        lease_owner=NULL,lease_until=NULL,updated_at=?
                    WHERE seq=? AND state='processing' AND lease_owner=?
                      AND lease_until>? AND deadline_at>?""",
                    (current, number, owner, current, current),
                ).rowcount
                return SettleResult("quarantined" if changed == 1 else "lost", number)
            changed = connection.execute(
                """UPDATE requests
                SET state='retry',disposition=?,next_attempt_at=?,
                    lease_owner=NULL,lease_until=NULL,updated_at=?
                WHERE seq=? AND state='processing' AND lease_owner=?
                  AND lease_until>? AND deadline_at>?""",
                (disposition, next_attempt, current, number, owner, current, current),
            ).rowcount
            return SettleResult("retry" if changed == 1 else "lost", number)

    def quarantine(self, seq: int, lease_owner: str, quarantine: Mapping[str, Any]) -> SettleResult:
        number = _integer(seq, "seq", minimum=1)
        owner = _text(lease_owner, "lease_owner", 128)
        optional = {"operation_digest", "disposition", "terminal_reply", "notify"}
        data = _mapping(quarantine, {"reason", "now"}, optional)
        reason = _text(data["reason"], "reason", 240)
        current = _time(data["now"], "now")
        disposition = _optional_text(data.get("disposition"), "disposition", 128) or reason
        digest = _optional_text(data.get("operation_digest"), "operation_digest", 64)
        reply = _optional_text(data.get("terminal_reply"), "terminal_reply", 160)
        notify = data.get("notify", True)
        if not isinstance(notify, bool) or notify and reply is None:
            raise ValueError("invalid quarantine notice")
        with self._write() as connection:
            row = self._owned_row_tx(connection, number, owner, current)
            if row is None or digest is not None and not self._operation_matches(row, digest):
                return SettleResult("lost", number)
            changed = connection.execute(
                """UPDATE requests
                SET state='quarantine',disposition=?,quarantine_reason=?,
                    terminal_reply=?,notify_state=?,lease_owner=NULL,lease_until=NULL,
                    updated_at=?
                WHERE seq=? AND state='processing' AND lease_owner=?
                  AND lease_until>? AND deadline_at>?""",
                (
                    disposition, reason, reply, "pending" if notify else "none",
                    current, number, owner, current, current,
                ),
            ).rowcount
            return SettleResult("quarantined" if changed == 1 else "lost", number)

    def claim_notice(self, seq: int, now: float) -> NoticeClaim | None:
        number = _integer(seq, "seq", minimum=1)
        current = _time(now)
        claim_token = base64.urlsafe_b64encode(secrets.token_bytes(32))
        claim_id = "hnc1_" + claim_token.rstrip(b"=").decode()
        with self._write() as connection:
            changed = connection.execute(
                """UPDATE requests
                SET notify_state='claimed',notice_claim_id=?,
                    notice_claimed_at=?,updated_at=?
                WHERE seq=? AND state IN ('terminal','quarantine')
                  AND notify_state='pending' AND terminal_reply IS NOT NULL""",
                (claim_id, current, current, number),
            ).rowcount
            if changed != 1:
                return None
            row = connection.execute(
                "SELECT receiver_id,input_json,terminal_reply FROM requests WHERE seq=?",
                (number,),
            ).fetchone()
            payload = _decode(row["input_json"], INPUT_LIMIT, "input")
            if payload is None:
                raise RuntimeError("corrupt input")
            return NoticeClaim(number, claim_id, row["receiver_id"], payload, row["terminal_reply"])

    def claim_next_notice(self, now: float) -> NoticeClaim | None:
        current = _time(now)
        with self._mutex:
            self._validate()
            row = self._connection.execute(
                "SELECT seq FROM requests "
                "WHERE state IN ('terminal','quarantine') "
                "AND notify_state='pending' AND terminal_reply IS NOT NULL "
                "ORDER BY seq LIMIT 1"
            ).fetchone()
        return None if row is None else self.claim_notice(int(row[0]), current)

    def mark_notice_sent(self, seq: int, notice_claim_id: str, message_id: str, now: float) -> bool:
        number = _integer(seq, "seq", minimum=1)
        claim = _text(notice_claim_id, "notice_claim_id", 64)
        message = _text(message_id, "message_id", 256)
        current = _time(now)
        with self._write() as connection:
            changed = connection.execute(
                """UPDATE requests
                SET notify_state='sent',notice_message_id=?,updated_at=?
                WHERE seq=? AND notify_state='claimed' AND notice_claim_id=?""",
                (message, current, number, claim),
            ).rowcount
            return changed == 1

    def prune(self, now: float) -> int:
        current = _time(now)
        with self._write() as connection:
            removed = connection.execute(
                """DELETE FROM requests
                WHERE state IN ('terminal','quarantine') AND ?>retain_until
                  AND (notify_state!='claimed'
                       OR ?>MAX(retain_until,notice_claimed_at+?))""",
                (current, current, NOTICE_EVIDENCE_SECONDS),
            ).rowcount
            connection.execute(
                "DELETE FROM receiver_cursors "
                "WHERE next_update_id=0 AND updated_at<?",
                (current - NOTICE_EVIDENCE_SECONDS,),
            )
            return removed


class IngressQueueObserver:
    def __init__(
        self, connection: sqlite3.Connection, parent_fd: int, file_fd: int,
        leaf: str, sidecars: Mapping[str, os.stat_result],
    ) -> None:
        self._connection = connection
        self._parent_fd = parent_fd
        self._file_fd = file_fd
        self._leaf = leaf
        self._sidecars = dict(sidecars)
        self._mutex = threading.RLock()
        self._closed = False

    @classmethod
    def _open(cls, path: str | Path) -> "IngressQueueObserver":
        parent_fd, leaf = _pin_parent(Path(path).expanduser())
        file_fd = -1
        try:
            file_fd, _ = _open_leaf(parent_fd, leaf, readonly=True)
            anchored = _anchored(parent_fd, leaf)
            uri = "file:" + quote(anchored, safe="/") + "?mode=ro"
            connection = sqlite3.connect(
                uri, uri=True, isolation_level=None, check_same_thread=False
            )
            _configure(connection, writer=False)
            _validate_named(parent_fd, leaf, os.fstat(file_fd))
            if (
                connection.execute("PRAGMA user_version").fetchone()[0] != SCHEMA_VERSION
                or _schema_fingerprint(connection) != _EXPECTED_SCHEMA
            ):
                raise RuntimeError("unsupported or corrupt ingress queue schema")
            if connection.execute("PRAGMA integrity_check").fetchone()[0] != "ok":
                raise RuntimeError("ingress queue integrity check failed")
            return cls(connection, parent_fd, file_fd, leaf, _validate_sidecars(parent_fd, leaf))
        except BaseException:
            if "connection" in locals():
                connection.close()
            if file_fd >= 0:
                os.close(file_fd)
            os.close(parent_fd)
            raise

    def _validate(self) -> None:
        if self._closed:
            raise RuntimeError("ingress queue observer is closed")
        _validate_named(self._parent_fd, self._leaf, os.fstat(self._file_fd))
        self._sidecars.update(_validate_sidecars(self._parent_fd, self._leaf, self._sidecars))

    def close(self) -> None:
        with self._mutex:
            if self._closed:
                return
            error: BaseException | None = None
            try:
                self._validate()
            except BaseException as exc:
                error = exc
            self._connection.close()
            os.close(self._file_fd)
            os.close(self._parent_fd)
            self._closed = True
            if error is not None:
                raise error

    def health_snapshot(self, now: float) -> QueueHealth:
        with self._mutex:
            current = _time(now)
            self._validate()
            state_rows = self._connection.execute(
                "SELECT state,COUNT(*) count FROM requests GROUP BY state"
            )
            states = {row["state"]: int(row["count"]) for row in state_rows}
            notice_rows = self._connection.execute(
                "SELECT notify_state,COUNT(*) count FROM requests "
                "WHERE notify_state IN ('pending','claimed') GROUP BY notify_state"
            )
            notices = {row["notify_state"]: int(row["count"]) for row in notice_rows}
            expired = int(
                self._connection.execute(
                    "SELECT COUNT(*) FROM requests "
                    "WHERE state='processing' AND lease_until<=?",
                    (current,),
                ).fetchone()[0]
            )
            overdue = int(
                self._connection.execute(
                    "SELECT COUNT(*) FROM requests "
                    "WHERE state IN ('pending','processing','retry') AND deadline_at<=?",
                    (current,),
                ).fetchone()[0]
            )
            state_order = ("pending", "processing", "retry", "terminal", "quarantine")
            return QueueHealth(
                *(states.get(state, 0) for state in state_order),
                notices.get("pending", 0), notices.get("claimed", 0), expired, overdue,
            )

    def status_rows(self, now: float) -> tuple[QueueStatus, ...]:
        with self._mutex:
            current = _time(now)
            self._validate()
            rows = self._connection.execute(
                "SELECT state,kind,COUNT(*) count,MIN(created_at) oldest "
                "FROM requests GROUP BY state,kind ORDER BY state,kind"
            ).fetchall()
            return tuple(
                QueueStatus(
                    row["state"], row["kind"], int(row["count"]),
                    max(0.0, current - float(row["oldest"])),
                )
                for row in rows
            )
