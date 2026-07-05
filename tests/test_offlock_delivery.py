"""Off-lock delivery machinery + the source-mode inter-pane yield (issue #63 + #122).

The global fcntl lock is held for the whole body of sync_once, including every pane's Telegram sends.
released_lock() drops it for a bounded window and re-acquires after; sync_once uses it BETWEEN panes so
a queued event/command can interleave instead of stalling behind the whole ~20-pane loop. These tests
pin the mechanism: with_lock exposes/restores the held fd, released_lock is a no-op when no lock is held
(keeping the large suite of direct command_reply tests green), the lock is genuinely free during the
yield (a competitor can grab it) yet re-held after, a re-acquire failure propagates, and the
commit-before-yield / reload-after pattern means a competitor's write is never clobbered.
"""
from __future__ import annotations

import fcntl
import os
import tempfile
import threading
import unittest
from pathlib import Path
from unittest.mock import patch

import herdres


class OffLockDeliveryTests(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        base = Path(self._tmp.name)
        self.lockpath = base / "sync.lock"
        self.statepath = base / "state.json"
        self._patches = [
            patch.object(herdres, "lock_path", return_value=self.lockpath),
            patch.object(herdres, "state_path", return_value=self.statepath),
        ]
        for p in self._patches:
            p.start()
        herdres.save_state({"version": 1, "spaces": {}, "panes": {"B": {"pane_id": "B", "field": "orig"}}})
        herdres._LOCK_STATE.held_fd = None
        herdres._LOCK_STATE.release_depth = 0

    def tearDown(self) -> None:
        for p in self._patches:
            p.stop()
        herdres._LOCK_STATE.held_fd = None
        herdres._LOCK_STATE.release_depth = 0
        self._tmp.cleanup()

    def test_with_lock_exposes_and_restores_fd(self) -> None:
        seen = {}
        herdres.with_lock(lambda: seen.update(fd=herdres._held_lock_fd()) or {"ok": True}, blocking=True)
        self.assertIsInstance(seen["fd"], int)          # held fd visible inside the call
        self.assertIsNone(herdres._held_lock_fd())       # restored (to None) after

    def test_with_lock_blocked_path_leaves_fd_none(self) -> None:
        # A non-blocking acquire that loses leaves the held fd untouched (None).
        def hold_and_probe():
            return herdres.with_lock(lambda: "inner", blocking=False)
        result = herdres.with_lock(hold_and_probe, blocking=True)
        self.assertEqual(result, {"ok": True, "changed": False, "message": "another sync is running"})
        self.assertIsNone(herdres._held_lock_fd())

    def test_released_lock_noop_without_held_lock(self) -> None:
        self.assertIsNone(herdres._held_lock_fd())
        with patch("fcntl.flock") as fl:
            with herdres.released_lock():
                pass
        fl.assert_not_called()  # the no-op property that keeps direct-call command_reply tests green

    def test_held_fd_is_thread_local(self) -> None:
        # The held fd lives in thread-local state: a with_lock holder in one thread does not expose
        # its fd to another thread. A module global would leak it, letting a competing thread's
        # released_lock() unlock the wrong fd (the embedded-gateway hazard).
        seen = {}
        inside = threading.Event()
        release = threading.Event()

        def holder():
            def body():
                seen["holder_fd"] = herdres._held_lock_fd()
                inside.set()
                release.wait(2)
                return {"ok": True}
            herdres.with_lock(body, blocking=True)

        t = threading.Thread(target=holder)
        t.start()
        try:
            self.assertTrue(inside.wait(2))
            # While the holder thread is inside with_lock, THIS thread sees its own (empty) lock state.
            seen["main_fd"] = herdres._held_lock_fd()
        finally:
            release.set()
            t.join(3)
        self.assertIsInstance(seen["holder_fd"], int)   # holder thread saw its own fd
        self.assertIsNone(seen["main_fd"])              # main thread's thread-local is independent

    def test_released_lock_drops_then_reacquires(self) -> None:
        ops = []
        real = fcntl.flock

        def rec(fd, op):
            ops.append(op)
            return real(fd, op)

        def body():
            ops.clear()  # discard with_lock's own acquire op
            with herdres.released_lock():
                inside = list(ops)
            return inside

        with patch("fcntl.flock", side_effect=rec):
            inside = herdres.with_lock(body, blocking=True)
        self.assertEqual(inside, [fcntl.LOCK_UN])               # dropped on enter
        self.assertEqual(ops, [fcntl.LOCK_UN, fcntl.LOCK_EX])   # re-acquired on exit

    def test_competitor_acquires_lock_during_interpane_yield(self) -> None:
        # The load-bearing proof: sync_once yields the lock between panes via `with released_lock()`.
        # A competitor's non-blocking acquire succeeds DURING that window (it would be refused if the
        # loop held the lock across all panes) yet is refused again after the loop re-acquires.
        result = {}

        def body():
            with herdres.released_lock():                       # the between-pane yield
                result["during"] = herdres.with_lock(lambda: "got-it", blocking=False)
            result["after"] = herdres.with_lock(lambda: "got-it", blocking=False)
            return {"ok": True}

        herdres.with_lock(body, blocking=True)
        self.assertEqual(result["during"], "got-it")           # lock free during the yield
        self.assertEqual(
            result["after"], {"ok": True, "changed": False, "message": "another sync is running"}
        )                                                       # re-held after

    def test_on_lock_work_blocks_competitor(self) -> None:
        # Control: work that does NOT release the lock refuses the competitor (proves the test above
        # is exercising the release, not a per-process flock quirk).
        result = {}

        def body():
            result["competitor"] = herdres.with_lock(lambda: "got-it", blocking=False)
            return {"ok": True}

        herdres.with_lock(body, blocking=True)
        self.assertEqual(
            result["competitor"], {"ok": True, "changed": False, "message": "another sync is running"}
        )

    def test_offlock_interpane_yield_flag_default_on_and_overridable(self) -> None:
        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop("HERDRES_OFFLOCK_INTERPANE_YIELD", None)
            self.assertTrue(herdres.offlock_interpane_yield_enabled())      # default on
        with patch.dict(os.environ, {"HERDRES_OFFLOCK_INTERPANE_YIELD": "0"}, clear=False):
            self.assertFalse(herdres.offlock_interpane_yield_enabled())

    def test_source_orphan_delete_cap_default_and_override(self) -> None:
        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop("HERDR_TELEGRAM_TOPICS_SOURCE_DELETE_LIMIT", None)
            self.assertEqual(herdres.source_orphan_delete_cap(), 3)          # smaller than the 12 general cap
        with patch.dict(os.environ, {"HERDR_TELEGRAM_TOPICS_SOURCE_DELETE_LIMIT": "5"}, clear=False):
            self.assertEqual(herdres.source_orphan_delete_cap(), 5)
        with patch.dict(os.environ, {"HERDR_TELEGRAM_TOPICS_SOURCE_DELETE_LIMIT": "bad"}, clear=False):
            self.assertEqual(herdres.source_orphan_delete_cap(), 3)          # malformed -> default

    def test_reacquire_failure_is_not_swallowed_by_inner_except(self) -> None:
        # Fail-safe: a released_lock() re-acquire failure must PROPAGATE (abort the turn) even when the
        # send is wrapped in a try/except for send errors — never silently continue without the lock.
        # (This is why the agent-pick arm scopes its try/except to send_to_pane only.)
        real = fcntl.flock
        armed = [False]

        def flaky(fd, op):
            if op == fcntl.LOCK_EX and armed[0]:
                raise OSError("re-acquire failed")
            return real(fd, op)

        def body():
            with herdres.released_lock():
                armed[0] = True  # only the LOCK_EX re-acquire on exit will now fail
                try:
                    pass  # a real body's send error would be caught here, not the re-acquire
                except Exception:
                    pass
            return "unreached"

        with patch("fcntl.flock", side_effect=flaky):
            with self.assertRaises(OSError):
                herdres.with_lock(body, blocking=True)

    def test_interpane_yield_no_clobber_competitor_write_survives(self) -> None:
        # The commit-before-yield / reload-after pattern: sync_once save_state()s a pane's work UNDER
        # the lock, yields, a competitor writes state.json, and sync_once reloads AFTER the yield. The
        # competitor's write survives instead of being clobbered by a stale in-memory save.
        def body():
            herdres.save_state({"version": 1, "spaces": {}, "panes": {"B": {"pane_id": "B", "field": "orig"}}})
            with herdres.released_lock():                       # the yield window
                st = herdres.load_state()
                st["panes"]["B"]["field"] = "by-competitor"
                herdres.save_state(st)
            return herdres.load_state()["panes"]["B"]["field"]  # sync_once reloads here

        self.assertEqual(herdres.with_lock(body, blocking=True), "by-competitor")


if __name__ == "__main__":
    unittest.main()
