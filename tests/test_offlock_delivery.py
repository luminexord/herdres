"""Durable inbound fix: command_reply delivers to the pane OFF the global lock.

send_to_pane's herdr cascade touches the herdr pane only (never state.json), so we drop the global
fcntl lock for exactly that window via released_lock() and re-acquire after. These tests pin the
mechanism: with_lock exposes/restores the held fd, released_lock is a no-op when no lock is held
(keeping the large suite of direct command_reply tests green), and the lock is genuinely free during
an off-lock send (a competitor can grab it) yet re-held after — with no post-send clobber.
"""
from __future__ import annotations

import fcntl
import os
import tempfile
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
        herdres._HELD_LOCK_FD = None

    def tearDown(self) -> None:
        for p in self._patches:
            p.stop()
        herdres._HELD_LOCK_FD = None
        self._tmp.cleanup()

    def test_with_lock_exposes_and_restores_fd(self) -> None:
        seen = {}
        herdres.with_lock(lambda: seen.update(fd=herdres._HELD_LOCK_FD) or {"ok": True}, blocking=True)
        self.assertIsInstance(seen["fd"], int)          # held fd visible inside the call
        self.assertIsNone(herdres._HELD_LOCK_FD)         # restored (to None) after

    def test_with_lock_blocked_path_leaves_fd_none(self) -> None:
        # A non-blocking acquire that loses leaves _HELD_LOCK_FD untouched (None).
        def hold_and_probe():
            return herdres.with_lock(lambda: "inner", blocking=False)
        result = herdres.with_lock(hold_and_probe, blocking=True)
        self.assertEqual(result, {"ok": True, "changed": False, "message": "another sync is running"})
        self.assertIsNone(herdres._HELD_LOCK_FD)

    def test_released_lock_noop_without_held_lock(self) -> None:
        self.assertIsNone(herdres._HELD_LOCK_FD)
        with patch("fcntl.flock") as fl:
            with herdres.released_lock():
                pass
        fl.assert_not_called()  # the no-op property that keeps direct-call command_reply tests green

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

    def test_competitor_acquires_lock_during_offlock_send(self) -> None:
        # The load-bearing proof: while an off-lock send runs, the global lock is FREE, so a
        # competitor's non-blocking acquire succeeds instead of "another sync is running".
        result = {}

        def during_send(pane_id, text, **kw):
            result["competitor"] = herdres.with_lock(lambda: "got-it", blocking=False)
            return (True, "")

        with patch.object(herdres, "send_to_pane", during_send):
            herdres.with_lock(lambda: herdres.forward_text_to_pane_response("A", "hi"), blocking=True)
        self.assertEqual(result["competitor"], "got-it")

    def test_on_lock_send_blocks_competitor(self) -> None:
        # Control: work that does NOT release the lock refuses the competitor (proves the test above
        # is actually exercising the release, not a per-process flock quirk).
        result = {}

        def on_lock(pane_id, text, **kw):
            result["competitor"] = herdres.with_lock(lambda: "got-it", blocking=False)
            return (True, "")

        with patch.object(herdres, "send_to_pane", on_lock):
            # call send_to_pane directly WITHOUT released_lock by bypassing forward_*'s wrapper:
            herdres.with_lock(lambda: herdres.send_to_pane("A", "hi"), blocking=True)
        self.assertEqual(
            result["competitor"], {"ok": True, "changed": False, "message": "another sync is running"}
        )

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

    def test_no_clobber_competitor_write_survives_offlock_send(self) -> None:
        # A competitor writes state.json DURING the off-lock send; the command does no post-send save,
        # so the competitor's write survives (the clobber a post-send save would cause is gone).
        def during_send(pane_id, text, **kw):
            st = herdres.load_state()
            st["panes"]["B"]["field"] = "by-competitor"
            herdres.save_state(st)
            return (True, "")

        with patch.object(herdres, "send_to_pane", during_send):
            herdres.with_lock(lambda: herdres.forward_text_to_pane_response("A", "hi"), blocking=True)
        self.assertEqual(herdres.load_state()["panes"]["B"]["field"], "by-competitor")


if __name__ == "__main__":
    unittest.main()
