"""Wakeable outbound delivery dispatcher for Tendwire connector work."""

from __future__ import annotations

import ctypes
import errno
import os
import select
import struct
import sys
import threading
import time
from pathlib import Path
from typing import Callable


_IN_CLOSE_WRITE = 0x00000008
_IN_MODIFY = 0x00000002
_IN_MOVED_TO = 0x00000080
_IN_CREATE = 0x00000100
_IN_DELETE = 0x00000200
_IN_NONBLOCK = getattr(os, "O_NONBLOCK", 0x800)
_IN_CLOEXEC = getattr(os, "O_CLOEXEC", 0x80000)
_INOTIFY_EVENT = struct.Struct("iIII")


class _DatabaseWakeWatcher:
    """Translate SQLite database/WAL writes into dispatcher wake edges."""

    def __init__(
        self,
        database_path: str | Path,
        wake: Callable[[str], None],
    ) -> None:
        self.database_path = Path(database_path)
        self.wake = wake
        self._stop = threading.Event()
        self._ready = threading.Event()
        self._thread: threading.Thread | None = None
        self._fd = -1

    def start(self) -> None:
        if self._thread is not None:
            return
        self._thread = threading.Thread(
            target=self._run,
            name="herdres-outbound-db-wake",
            daemon=True,
        )
        self._thread.start()
        self._ready.wait(timeout=1.0)

    def stop(self) -> None:
        self._stop.set()
        thread = self._thread
        if thread is not None:
            thread.join(timeout=1.0)
        if self._fd >= 0:
            try:
                os.close(self._fd)
            except OSError:
                pass
            self._fd = -1

    def _open_inotify(self) -> int:
        if not sys.platform.startswith("linux"):
            return -1
        libc = ctypes.CDLL(None, use_errno=True)
        init = getattr(libc, "inotify_init1", None)
        add_watch = getattr(libc, "inotify_add_watch", None)
        if init is None or add_watch is None:
            return -1
        init.argtypes = [ctypes.c_int]
        init.restype = ctypes.c_int
        add_watch.argtypes = [ctypes.c_int, ctypes.c_char_p, ctypes.c_uint32]
        add_watch.restype = ctypes.c_int
        fd = int(init(_IN_NONBLOCK | _IN_CLOEXEC))
        if fd < 0:
            return -1
        directory = os.fsencode(str(self.database_path.parent))
        mask = (
            _IN_MODIFY
            | _IN_CLOSE_WRITE
            | _IN_MOVED_TO
            | _IN_CREATE
            | _IN_DELETE
        )
        if int(add_watch(fd, directory, mask)) < 0:
            os.close(fd)
            return -1
        return fd

    def _run(self) -> None:
        self._fd = self._open_inotify()
        self._ready.set()
        if self._fd < 0:
            return
        watched_names = {
            self.database_path.name,
            self.database_path.name + "-wal",
            self.database_path.name + "-shm",
        }
        while not self._stop.is_set():
            try:
                readable, _writable, _exceptional = select.select(
                    [self._fd], [], [], 0.25
                )
            except (OSError, ValueError):
                return
            if not readable:
                continue
            try:
                payload = os.read(self._fd, 64 * 1024)
            except BlockingIOError:
                continue
            except OSError as exc:
                if exc.errno in {errno.EBADF, errno.EINVAL}:
                    return
                continue
            offset = 0
            matched = False
            while offset + _INOTIFY_EVENT.size <= len(payload):
                _wd, _mask, _cookie, name_length = _INOTIFY_EVENT.unpack_from(
                    payload, offset
                )
                offset += _INOTIFY_EVENT.size
                name_bytes = payload[offset : offset + name_length]
                offset += name_length
                name = os.fsdecode(name_bytes.rstrip(b"\0"))
                if name in watched_names:
                    matched = True
            if matched:
                self.wake("tendwire_db_commit")


class OutboundDispatcher:
    """Run one coalesced outbound drain per wake, with a bounded fallback."""

    def __init__(
        self,
        drain: Callable[[], None],
        *,
        database_path: str | Path,
        fallback_seconds: float = 1.0,
        min_interval_seconds: float = 0.05,
        on_error: Callable[[Exception], None] | None = None,
    ) -> None:
        self.drain = drain
        self.fallback_seconds = max(0.1, float(fallback_seconds))
        self.min_interval_seconds = max(0.0, float(min_interval_seconds))
        self.on_error = on_error
        self._wake = threading.Event()
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._watcher = _DatabaseWakeWatcher(database_path, self.wake)

    def start(self) -> None:
        if self._thread is not None:
            return
        self._watcher.start()
        self._thread = threading.Thread(
            target=self._run,
            name="herdres-outbound-dispatch",
            daemon=True,
        )
        self._thread.start()
        self.wake("startup")

    def stop(self) -> None:
        self._stop.set()
        self._wake.set()
        self._watcher.stop()
        thread = self._thread
        if thread is not None:
            thread.join(timeout=2.0)

    def wake(self, _source: str = "explicit") -> None:
        self._wake.set()

    def _run(self) -> None:
        last_started = 0.0
        while not self._stop.is_set():
            self._wake.wait(timeout=self.fallback_seconds)
            self._wake.clear()
            if self._stop.is_set():
                return
            remaining = (
                last_started
                + self.min_interval_seconds
                - time.monotonic()
            )
            if remaining > 0 and self._stop.wait(remaining):
                return
            last_started = time.monotonic()
            try:
                self.drain()
            except Exception as exc:  # noqa: BLE001 - dispatcher stays live
                if self.on_error is not None:
                    self.on_error(exc)
