"""Durable SQLite spool for independently dispatched Telegram ingress lanes.

The spool is intentionally separate from ``state.json``.  Ingestion performs one
FULL-synchronous transaction per update so the durable lane item and Telegram
cursor can never disagree.  Dispatch leases only lane heads, which gives each
lane strict FIFO while allowing unrelated lanes to progress concurrently.
"""

from __future__ import annotations

import json
import os
import sqlite3
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from . import config
from .ingress_identity import validate_request_id


_SCHEMA = """
CREATE TABLE IF NOT EXISTS receiver_cursors (
    receiver_kind TEXT PRIMARY KEY,
    next_update_id INTEGER NOT NULL,
    updated_at REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS lane_items (
    seq INTEGER PRIMARY KEY AUTOINCREMENT,
    request_id TEXT NOT NULL UNIQUE,
    receiver_kind TEXT NOT NULL,
    update_id INTEGER NOT NULL,
    lane_key TEXT NOT NULL,
    kind TEXT NOT NULL,
    update_json TEXT NOT NULL,
    route_json TEXT NOT NULL,
    state TEXT NOT NULL CHECK(state IN ('pending','processing','done')),
    attempts INTEGER NOT NULL DEFAULT 0,
    first_seen_at REAL NOT NULL,
    next_attempt_at REAL NOT NULL,
    deadline_at REAL NOT NULL,
    lease_owner TEXT,
    lease_until REAL,
    notify_state TEXT NOT NULL DEFAULT 'pending',
    updated_at REAL NOT NULL,
    UNIQUE(receiver_kind, update_id)
);
CREATE INDEX IF NOT EXISTS lane_items_open_by_lane
    ON lane_items(lane_key, seq) WHERE state != 'done';
"""


@dataclass(frozen=True)
class LaneItem:
    seq: int
    request_id: str
    receiver_kind: str
    update_id: int
    lane_key: str
    kind: str
    update: dict[str, Any]
    route: dict[str, Any]
    attempts: int
    first_seen_at: float
    next_attempt_at: float
    deadline_at: float
    lease_owner: str
    lease_until: float
    notify_state: str


@dataclass(frozen=True)
class EnqueueResult:
    status: str
    next_update_id: int
    seq: int | None = None


@dataclass(frozen=True)
class DispatchSnapshot:
    """Small diagnostic view of work visible to a dispatcher iteration."""

    pending_count: int
    eligible_lane_count: int
    first_eligible_lane: str


def lane_key(receiver_kind: str, topic_id: str) -> str:
    """Return the stable, collision-free on-disk representation of a lane."""

    return json.dumps(
        [str(receiver_kind), str(topic_id)],
        ensure_ascii=False,
        separators=(",", ":"),
    )


class IngressLaneSpool:
    def __init__(self, path: str | Path | None = None) -> None:
        self.path = Path(path or config.inbound_spool_path()).expanduser()
        self._initialize()

    def _initialize(self) -> None:
        self.path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        flags = os.O_RDWR | os.O_CREAT
        if hasattr(os, "O_NOFOLLOW"):
            flags |= os.O_NOFOLLOW
        fd = os.open(self.path, flags, 0o600)
        os.close(fd)
        os.chmod(self.path, 0o600)
        with self._connect() as connection:
            connection.executescript(_SCHEMA)

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(
            self.path,
            timeout=30.0,
            isolation_level=None,
        )
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA busy_timeout=30000")
        connection.execute("PRAGMA journal_mode=WAL")
        connection.execute("PRAGMA synchronous=FULL")
        return connection

    @staticmethod
    def _begin(connection: sqlite3.Connection) -> None:
        connection.execute("BEGIN IMMEDIATE")

    @staticmethod
    def _advance_cursor_tx(
        connection: sqlite3.Connection,
        receiver_kind: str,
        next_update_id: int,
        now: float,
    ) -> int:
        connection.execute(
            """
            INSERT INTO receiver_cursors(receiver_kind, next_update_id, updated_at)
            VALUES (?, ?, ?)
            ON CONFLICT(receiver_kind) DO UPDATE SET
                next_update_id = MAX(receiver_cursors.next_update_id, excluded.next_update_id),
                updated_at = excluded.updated_at
            """,
            (receiver_kind, int(next_update_id), float(now)),
        )
        row = connection.execute(
            "SELECT next_update_id FROM receiver_cursors WHERE receiver_kind = ?",
            (receiver_kind,),
        ).fetchone()
        if row is None:
            raise RuntimeError("receiver cursor commit failed")
        return int(row["next_update_id"])

    def cursor(self, receiver_kind: str) -> int | None:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT next_update_id FROM receiver_cursors WHERE receiver_kind = ?",
                (str(receiver_kind),),
            ).fetchone()
        return None if row is None else int(row["next_update_id"])

    def initialize_cursor(self, receiver_kind: str, next_update_id: int) -> int:
        now = time.time()
        with self._connect() as connection:
            self._begin(connection)
            try:
                cursor = self._advance_cursor_tx(
                    connection, str(receiver_kind), int(next_update_id), now
                )
                connection.commit()
                return cursor
            except BaseException:
                connection.rollback()
                raise

    def advance_cursor(self, receiver_kind: str, next_update_id: int) -> int:
        return self.initialize_cursor(receiver_kind, next_update_id)

    def enqueue(
        self,
        *,
        request_id: str,
        receiver_kind: str,
        update_id: int,
        lane_key_value: str,
        kind: str,
        update: dict[str, Any],
        route: dict[str, Any],
        first_seen_at: float,
        deadline_at: float,
        depth_limit: int,
        already_done: bool = False,
    ) -> EnqueueResult:
        request_id = validate_request_id(request_id)
        receiver_kind = str(receiver_kind)
        update_id = int(update_id)
        now = float(first_seen_at)
        update_json = json.dumps(
            update, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False
        )
        route_json = json.dumps(
            route, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False
        )
        with self._connect() as connection:
            self._begin(connection)
            try:
                existing = connection.execute(
                    """
                    SELECT seq, request_id, receiver_kind, update_id
                    FROM lane_items
                    WHERE request_id = ? OR (receiver_kind = ? AND update_id = ?)
                    """,
                    (request_id, receiver_kind, update_id),
                ).fetchone()
                if existing is not None:
                    if (
                        existing["request_id"] != request_id
                        or existing["receiver_kind"] != receiver_kind
                        or int(existing["update_id"]) != update_id
                    ):
                        raise RuntimeError("inbound spool identity collision")
                    cursor = self._advance_cursor_tx(
                        connection, receiver_kind, update_id + 1, now
                    )
                    connection.commit()
                    return EnqueueResult("duplicate", cursor, int(existing["seq"]))

                if not already_done:
                    depth = connection.execute(
                        "SELECT COUNT(*) AS depth FROM lane_items WHERE lane_key = ? AND state != 'done'",
                        (lane_key_value,),
                    ).fetchone()
                    if depth is not None and int(depth["depth"]) >= int(depth_limit):
                        cursor = self._advance_cursor_tx(
                            connection, receiver_kind, update_id + 1, now
                        )
                        connection.commit()
                        return EnqueueResult("overflow", cursor)

                cursor_row = connection.execute(
                    """
                    INSERT OR IGNORE INTO lane_items(
                        request_id, receiver_kind, update_id, lane_key, kind,
                        update_json, route_json, state, attempts, first_seen_at,
                        next_attempt_at, deadline_at, lease_owner, lease_until,
                        notify_state, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0, ?, ?, ?, NULL, NULL, ?, ?)
                    """,
                    (
                        request_id,
                        receiver_kind,
                        update_id,
                        lane_key_value,
                        str(kind),
                        update_json,
                        route_json,
                        "done" if already_done else "pending",
                        now,
                        now,
                        float(deadline_at),
                        "cached" if already_done else "pending",
                        now,
                    ),
                )
                seq = int(cursor_row.lastrowid)
                cursor = self._advance_cursor_tx(
                    connection, receiver_kind, update_id + 1, now
                )
                connection.commit()
                return EnqueueResult("done" if already_done else "enqueued", cursor, seq)
            except BaseException:
                connection.rollback()
                raise

    def claim(
        self,
        lease_owner: str,
        *,
        now: float | None = None,
        lease_seconds: float = 120.0,
    ) -> LaneItem | None:
        timestamp = time.time() if now is None else float(now)
        with self._connect() as connection:
            self._begin(connection)
            try:
                connection.execute(
                    """
                    UPDATE lane_items
                    SET state = 'pending', lease_owner = NULL, lease_until = NULL,
                        updated_at = ?
                    WHERE state = 'processing' AND lease_until <= ?
                    """,
                    (timestamp, timestamp),
                )
                row = connection.execute(
                    """
                    SELECT candidate.*
                    FROM lane_items AS candidate
                    WHERE candidate.state = 'pending'
                      AND candidate.next_attempt_at <= ?
                      AND NOT EXISTS (
                          SELECT 1 FROM lane_items AS prior
                          WHERE prior.lane_key = candidate.lane_key
                            AND prior.state != 'done'
                            AND prior.seq < candidate.seq
                      )
                    ORDER BY candidate.seq
                    LIMIT 1
                    """,
                    (timestamp,),
                ).fetchone()
                if row is None:
                    connection.commit()
                    return None
                lease_until = timestamp + max(0.1, float(lease_seconds))
                changed = connection.execute(
                    """
                    UPDATE lane_items
                    SET state = 'processing', lease_owner = ?, lease_until = ?, updated_at = ?
                    WHERE seq = ? AND state = 'pending'
                    """,
                    (str(lease_owner), lease_until, timestamp, int(row["seq"])),
                ).rowcount
                if changed != 1:
                    connection.rollback()
                    return None
                connection.commit()
            except BaseException:
                connection.rollback()
                raise
        return LaneItem(
            seq=int(row["seq"]),
            request_id=str(row["request_id"]),
            receiver_kind=str(row["receiver_kind"]),
            update_id=int(row["update_id"]),
            lane_key=str(row["lane_key"]),
            kind=str(row["kind"]),
            update=json.loads(str(row["update_json"])),
            route=json.loads(str(row["route_json"])),
            attempts=int(row["attempts"]),
            first_seen_at=float(row["first_seen_at"]),
            next_attempt_at=float(row["next_attempt_at"]),
            deadline_at=float(row["deadline_at"]),
            lease_owner=str(lease_owner),
            lease_until=lease_until,
            notify_state=str(row["notify_state"]),
        )

    def dispatch_snapshot(self, *, now: float | None = None) -> DispatchSnapshot:
        """Count pending rows and lane heads currently eligible for a lease."""

        timestamp = time.time() if now is None else float(now)
        with self._connect() as connection:
            pending_row = connection.execute(
                "SELECT COUNT(*) AS count FROM lane_items WHERE state = 'pending'"
            ).fetchone()
            eligible_row = connection.execute(
                """
                WITH eligible AS (
                    SELECT candidate.seq, candidate.lane_key
                    FROM lane_items AS candidate
                    WHERE candidate.state = 'pending'
                      AND candidate.next_attempt_at <= ?
                      AND NOT EXISTS (
                          SELECT 1 FROM lane_items AS prior
                          WHERE prior.lane_key = candidate.lane_key
                            AND prior.state != 'done'
                            AND prior.seq < candidate.seq
                      )
                )
                SELECT
                    COUNT(*) AS count,
                    (
                        SELECT lane_key
                        FROM eligible
                        ORDER BY seq
                        LIMIT 1
                    ) AS first_lane
                FROM eligible
                """,
                (timestamp,),
            ).fetchone()
        return DispatchSnapshot(
            pending_count=int(pending_row["count"]) if pending_row is not None else 0,
            eligible_lane_count=(
                int(eligible_row["count"]) if eligible_row is not None else 0
            ),
            first_eligible_lane=(
                str(eligible_row["first_lane"])
                if eligible_row is not None and eligible_row["first_lane"] is not None
                else ""
            ),
        )

    def reclaim_processing(self, *, now: float | None = None) -> int:
        """Make leases from a previous dispatcher process immediately runnable."""

        timestamp = time.time() if now is None else float(now)
        with self._connect() as connection:
            changed = connection.execute(
                """
                UPDATE lane_items
                SET state = 'pending', next_attempt_at = MIN(next_attempt_at, ?),
                    lease_owner = NULL, lease_until = NULL, updated_at = ?
                WHERE state = 'processing'
                """,
                (timestamp, timestamp),
            ).rowcount
        return int(changed)

    def renew_lease(
        self,
        seq: int,
        lease_owner: str,
        *,
        lease_seconds: float,
        now: float | None = None,
    ) -> bool:
        """Extend a live dispatch lease without changing lane ordering."""

        timestamp = time.time() if now is None else float(now)
        lease_until = timestamp + max(0.1, float(lease_seconds))
        with self._connect() as connection:
            changed = connection.execute(
                """
                UPDATE lane_items
                SET lease_until = ?, updated_at = ?
                WHERE seq = ? AND state = 'processing' AND lease_owner = ?
                """,
                (lease_until, timestamp, int(seq), str(lease_owner)),
            ).rowcount
        return changed == 1

    def claim_notification(
        self, seq: int, *, now: float | None = None
    ) -> bool:
        """Claim the one permitted Queued notification attempt.

        ``claimed`` is intentionally irreversible.  A process or transport loss
        after this CAS is ambiguous, so retrying could visibly duplicate the
        acknowledgement.
        """

        timestamp = time.time() if now is None else float(now)
        with self._connect() as connection:
            changed = connection.execute(
                """
                UPDATE lane_items
                SET notify_state = 'claimed', updated_at = ?
                WHERE seq = ? AND notify_state = 'pending'
                """,
                (timestamp, int(seq)),
            ).rowcount
        return changed == 1

    def mark_notification_sent(
        self, seq: int, *, now: float | None = None
    ) -> bool:
        timestamp = time.time() if now is None else float(now)
        with self._connect() as connection:
            changed = connection.execute(
                """
                UPDATE lane_items
                SET notify_state = 'sent', updated_at = ?
                WHERE seq = ? AND notify_state = 'claimed'
                """,
                (timestamp, int(seq)),
            ).rowcount
        return changed == 1

    def mark_done(
        self,
        seq: int,
        lease_owner: str,
        *,
        now: float | None = None,
        notify_state: str | None = None,
    ) -> bool:
        timestamp = time.time() if now is None else float(now)
        with self._connect() as connection:
            changed = connection.execute(
                """
                UPDATE lane_items
                SET state = 'done', lease_owner = NULL, lease_until = NULL,
                    notify_state = COALESCE(?, notify_state), updated_at = ?
                WHERE seq = ? AND state = 'processing' AND lease_owner = ?
                """,
                (
                    None if notify_state is None else str(notify_state),
                    timestamp,
                    int(seq),
                    str(lease_owner),
                ),
            ).rowcount
        return changed == 1

    def retry(
        self,
        seq: int,
        lease_owner: str,
        *,
        backoff_seconds: float,
        now: float | None = None,
    ) -> bool:
        timestamp = time.time() if now is None else float(now)
        with self._connect() as connection:
            self._begin(connection)
            try:
                row = connection.execute(
                    """
                    SELECT attempts, deadline_at FROM lane_items
                    WHERE seq = ? AND state = 'processing' AND lease_owner = ?
                    """,
                    (int(seq), str(lease_owner)),
                ).fetchone()
                if row is None:
                    connection.commit()
                    return False
                attempts = int(row["attempts"]) + 1
                delay = min(300.0, float(backoff_seconds) * (2 ** min(attempts - 1, 16)))
                next_attempt = min(timestamp + delay, float(row["deadline_at"]))
                connection.execute(
                    """
                    UPDATE lane_items
                    SET state = 'pending', attempts = ?, next_attempt_at = ?,
                        lease_owner = NULL, lease_until = NULL, updated_at = ?
                    WHERE seq = ? AND state = 'processing' AND lease_owner = ?
                    """,
                    (attempts, next_attempt, timestamp, int(seq), str(lease_owner)),
                )
                connection.commit()
                return True
            except BaseException:
                connection.rollback()
                raise

    def rows(self) -> list[dict[str, Any]]:
        """Return a diagnostic snapshot used by tests and operator tooling."""

        with self._connect() as connection:
            rows = connection.execute("SELECT * FROM lane_items ORDER BY seq").fetchall()
        return [dict(row) for row in rows]
