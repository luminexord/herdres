"""Off-lock inter-work delivery for source mode (issue #122).

sync_once holds state.state_lock() across the whole source-mode delivery loop, so queued inbound
commands (which also take state_lock()) stall behind its Telegram sends. released_lock() drops the held
lock for a bounded window between delivered turns and re-acquires it. These tests pin the mechanism
(state_lock exposes/restores the held fd, released_lock is a no-op when no lock is held, the fd is
thread-local, drop-then-reacquire, re-acquire-failure propagates), the two runtime flag readers, the
commit-before-yield / reload-after no-clobber invariant, and the _cleanup_topics per-pass delete cap.
"""
from __future__ import annotations

import fcntl
import json
import threading
from unittest.mock import patch

from herdres_connector import config, state
from herdres_connector.source_sync import SyncRuntime, _cleanup_topics, _sync_sources, sync_once

from test_source_only import FakeTelegram, FakeTendwire, _store


def _reset_lock_state():
    state._LOCK_STATE.held_fd = None
    state._LOCK_STATE.release_depth = 0


def _competitor_can_acquire(lock_path) -> bool:
    """A fresh, independent fd tries a non-blocking acquire on the state lock file — the flock a
    queued inbound command would attempt. Succeeds only when the holder has released the lock."""
    with open(lock_path, "a+", encoding="utf-8") as fh:
        try:
            fcntl.flock(fh, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return False
        fcntl.flock(fh, fcntl.LOCK_UN)
        return True


# --- machinery ---------------------------------------------------------------

def test_state_lock_exposes_and_restores_fd(tmp_path):
    _reset_lock_state()
    statepath = tmp_path / "state.json"
    seen = {}
    with state.state_lock(path=statepath):
        seen["fd"] = state._held_lock_fd()
        seen["held"] = state.lock_held()
    assert isinstance(seen["fd"], int)      # held fd visible inside
    assert seen["held"] is True
    assert state._held_lock_fd() is None    # restored after
    assert state.lock_held() is False


def test_released_lock_noop_without_held_lock():
    _reset_lock_state()
    assert state.lock_held() is False
    with patch("fcntl.flock") as fl:
        with state.released_lock():
            pass
    fl.assert_not_called()   # no-op keeps direct-call sync_once tests green


def test_held_fd_is_thread_local(tmp_path):
    # A holder in one thread must not expose its fd to another; a module global would leak it, letting
    # a competing thread's released_lock() unlock the wrong fd (the embedded-runner hazard).
    _reset_lock_state()
    statepath = tmp_path / "state.json"
    seen = {}
    inside = threading.Event()
    release = threading.Event()

    def holder():
        with state.state_lock(path=statepath):
            seen["holder_fd"] = state._held_lock_fd()
            inside.set()
            release.wait(2)

    t = threading.Thread(target=holder)
    t.start()
    try:
        assert inside.wait(2)
        seen["main_fd"] = state._held_lock_fd()   # this thread's thread-local is independent
    finally:
        release.set()
        t.join(3)
    assert isinstance(seen["holder_fd"], int)
    assert seen["main_fd"] is None


def test_released_lock_drops_then_reacquires(tmp_path):
    _reset_lock_state()
    statepath = tmp_path / "state.json"
    ops = []
    real = fcntl.flock

    def rec(fd, op):
        ops.append(op)
        return real(fd, op)

    with state.state_lock(path=statepath):
        with patch("fcntl.flock", side_effect=rec):
            with state.released_lock():
                inside = list(ops)
    assert inside == [fcntl.LOCK_UN]                 # dropped on enter
    assert ops == [fcntl.LOCK_UN, fcntl.LOCK_EX]     # re-acquired on exit


def test_reacquire_failure_propagates(tmp_path):
    # Fail-safe: a released_lock() re-acquire failure must PROPAGATE, never silently continue unlocked.
    _reset_lock_state()
    statepath = tmp_path / "state.json"
    real = fcntl.flock
    armed = [False]

    def flaky(fd, op):
        if op == fcntl.LOCK_EX and armed[0]:
            raise OSError("re-acquire failed")
        return real(fd, op)

    with state.state_lock(path=statepath):
        with patch("fcntl.flock", side_effect=flaky):
            try:
                with state.released_lock():
                    armed[0] = True   # only the LOCK_EX re-acquire on exit fails now
                assert False, "expected the re-acquire failure to propagate"
            except OSError:
                pass
    _reset_lock_state()


def test_competitor_acquires_lock_during_yield(tmp_path):
    _reset_lock_state()
    statepath = tmp_path / "state.json"
    lockpath = statepath.with_suffix(statepath.suffix + ".lock")
    result = {}
    with state.state_lock(path=statepath):
        result["held"] = _competitor_can_acquire(lockpath)      # blocked while held -> False
        with state.released_lock():
            result["during"] = _competitor_can_acquire(lockpath)  # free during the yield -> True
        result["after"] = _competitor_can_acquire(lockpath)      # re-held -> False
    assert result == {"held": False, "during": True, "after": False}


# --- config flags ------------------------------------------------------------

def test_offlock_interpane_yield_flag():
    assert config.offlock_interpane_yield_enabled(env={}) is True                  # default on
    assert config.offlock_interpane_yield_enabled(env={"HERDRES_OFFLOCK_INTERPANE_YIELD": "0"}) is False


def test_source_orphan_delete_cap():
    assert config.source_orphan_delete_cap(env={}) == 3                            # default
    assert config.source_orphan_delete_cap(env={"HERDR_TELEGRAM_TOPICS_SOURCE_DELETE_LIMIT": "5"}) == 5
    assert config.source_orphan_delete_cap(env={"HERDR_TELEGRAM_TOPICS_SOURCE_DELETE_LIMIT": "bad"}) == 3


# --- yield behaviour in sync_once -------------------------------------------

def _two_final_turns_tendwire():
    return FakeTendwire(
        turns={"turns": [
            {"id": "turn-a", "worker_id": "worker-a", "worker_fingerprint": "fp-a",
             "assistant_final_text": "Final A", "complete": True},
            {"id": "turn-b", "worker_id": "worker-b", "worker_fingerprint": "fp-b",
             "assistant_final_text": "Final B", "complete": True},
        ]},
        workers=[
            {"id": "worker-a", "name": "a", "status": "idle", "space_id": "space-1",
             "fingerprint": "fp-a", "meta": {"agent": "codex"}},
            {"id": "worker-b", "name": "b", "status": "idle", "space_id": "space-1",
             "fingerprint": "fp-b", "meta": {"agent": "claude"}},
        ],
    )


def test_yield_no_clobber_competitor_write_survives(tmp_path, monkeypatch):
    # commit-before-yield + reload-after: during the released window a competitor loads state.json,
    # modifies it, and saves; sync_once reloads after the yield, so the competitor's write survives
    # rather than being clobbered by a stale in-memory save.
    _reset_lock_state()
    monkeypatch.setenv("HERDRES_TENDWIRE_MODE", "source")
    statepath = tmp_path / "state.json"
    monkeypatch.setenv("HERDR_TELEGRAM_TOPICS_STATE", str(statepath))

    class _CompetitorWindow:
        """Stand in for released_lock(): a queued command grabs the freed lock, load-modify-saves."""
        def __enter__(self):
            disk = json.loads(statepath.read_text(encoding="utf-8"))
            disk["competitor_sentinel"] = "written-during-yield"
            statepath.write_text(json.dumps(disk), encoding="utf-8")
            return self
        def __exit__(self, *_a):
            return False

    monkeypatch.setattr(state, "released_lock", lambda: _CompetitorWindow())

    store = _store()
    telegram = FakeTelegram()
    with state.state_lock(path=statepath):
        result = sync_once(store, SyncRuntime(_two_final_turns_tendwire(), telegram, with_outbox=False))

    assert result["sent"] == 2                                       # both turns delivered
    assert store.get("competitor_sentinel") == "written-during-yield"  # reload picked up the write


def test_yield_preserves_both_deliveries(tmp_path, monkeypatch):
    # With the real released_lock, the mid-loop save/reload must not lose either worker's delivery
    # record (the RC-edition of the detached-reference bug).
    _reset_lock_state()
    monkeypatch.setenv("HERDRES_TENDWIRE_MODE", "source")
    statepath = tmp_path / "state.json"
    monkeypatch.setenv("HERDR_TELEGRAM_TOPICS_STATE", str(statepath))
    store = _store()
    telegram = FakeTelegram()
    with state.state_lock(path=statepath):
        result = sync_once(store, SyncRuntime(_two_final_turns_tendwire(), telegram, with_outbox=False))
    assert result["sent"] == 2
    workers = state.source_worker_entries(store)
    delivered = [w for w in workers.values() if w.get("last_clean_message_id") or w.get("final_message_ids")]
    assert len(delivered) == 2   # both survived the mid-loop reload


def test_cleanup_topics_delete_cap(tmp_path, monkeypatch):
    monkeypatch.setenv("HERDRES_TENDWIRE_MODE", "source")
    monkeypatch.setenv("HERDR_TELEGRAM_TOPICS_SOURCE_DELETE_LIMIT", "2")
    store = _store()
    for i in range(5):
        store["panes"][f"w{i}"] = {
            "source": "tendwire",
            "entry_type": "worker",
            "tendwire_worker_id": f"worker-{i}",
            "tendwire_space_id": "space-1",
            "topic_id": str(200 + i),
            "topic_name": f"T{i}",
        }
    telegram = FakeTelegram()
    runtime = SyncRuntime(FakeTendwire(), telegram, with_outbox=False)
    result = _cleanup_topics(store, runtime, chat_id="-100")
    assert len(telegram.deleted_topics) == 2   # capped at 2 this pass
    assert result["deleted"] == 2
    # the remaining 3 still carry their topic_id (untouched, retried next tick)
    remaining = [e for e in state.source_worker_entries(store).values() if e.get("topic_id")]
    assert len(remaining) == 3


def test_sync_sources_create_cap(monkeypatch):
    monkeypatch.setenv("HERDRES_TENDWIRE_MODE", "source")
    monkeypatch.setenv("HERDRES_SOURCE_TOPIC_MODE", "worker")
    monkeypatch.setenv("HERDR_TELEGRAM_TOPICS_MAX_CREATES", "2")
    store = _store()
    workers = [
        {"id": f"worker-{i}", "name": f"w{i}", "status": "working", "space_id": "space-1",
         "fingerprint": f"fp-{i}", "meta": {"agent": "codex"}}
        for i in range(5)
    ]
    snapshot = {"ok": True, "spaces": [{"id": "space-1", "name": "S", "status": "active"}], "workers": workers}
    telegram = FakeTelegram()
    runtime = SyncRuntime(FakeTendwire(), telegram, with_outbox=False)
    _sync_sources(store, snapshot, {"turns": []}, runtime, chat_id="-100")
    assert len(telegram.topics) == 2   # only 2 real creates this pass (5 workers, cap 2)
    # the other 3 workers have no topic yet (deferred to next tick)
    no_topic = [e for e in state.source_worker_entries(store).values() if not e.get("topic_id")]
    assert len(no_topic) == 3
