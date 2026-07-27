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
import math
from types import SimpleNamespace
import threading
import time
from unittest.mock import patch

import herdres
import pytest

from herdres_connector import config, source_sync, state
from herdres_connector.source_sync import (
    SyncRuntime,
    _OfflockClient,
    _cleanup_topics,
    _sync_sources,
    sync_once,
)
from herdres_connector.telegram_delivery import drain_outbox, topic_icon_id

from test_source_only import (
    REQUEST_ID,
    FakeTelegram,
    FakeTendwire,
    _accepted_command_response,
    _source_worker,
    _store,
)


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


def test_lock_actually_held_exposes_released_window(tmp_path):
    _reset_lock_state()
    statepath = tmp_path / "state.json"

    with state.state_lock(path=statepath):
        assert state.lock_held() is True
        assert state.lock_actually_held() is True
        with state.released_lock():
            assert state.lock_held() is True
            assert state.lock_actually_held() is False
        assert state.lock_actually_held() is True


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


def test_slow_provider_call_does_not_hold_state_lock(tmp_path, monkeypatch):
    statepath = tmp_path / "state.json"
    monkeypatch.setenv("HERDR_TELEGRAM_TOPICS_STATE", str(statepath))
    store = _store()
    state.save_state(store, statepath)
    entered = threading.Event()
    release = threading.Event()

    class SlowProvider:
        def configured(self):
            entered.set()
            assert release.wait(3)
            return {"ok": True}

    finished = threading.Event()

    def invoke():
        with state.state_lock(path=statepath):
            client = _OfflockClient(SlowProvider(), store)
            assert client.configured()["ok"] is True
        finished.set()

    thread = threading.Thread(target=invoke)
    thread.start()
    assert entered.wait(1)
    started = time.monotonic()
    with state.state_lock(path=statepath):
        concurrent = state.load_state(statepath)
        concurrent["concurrent_command"] = True
        state.save_state(concurrent, statepath)
    contiguous_hold = time.monotonic() - started
    release.set()
    thread.join(3)

    assert finished.is_set()
    assert contiguous_hold < 2.0
    assert store["concurrent_command"] is True


def test_nested_offlock_client_does_not_rollback_lane_child_commit(
    tmp_path, monkeypatch
):
    """Regression for cleanup/speak phases that already own a release window."""

    _reset_lock_state()
    statepath = tmp_path / "state.json"
    monkeypatch.setenv("HERDR_TELEGRAM_TOPICS_STATE", str(statepath))
    state.save_state(_store(), statepath)

    class Provider:
        def configured(self):
            return {"ok": True}

    with state.state_lock(path=statepath):
        current = state.load_state(statepath)
        client = _OfflockClient(Provider(), current)
        with state.released_lock():
            child = state.load_state(statepath)
            child["child_commit_survived"] = True
            state.save_state(child, statepath)
            assert client.configured()["ok"] is True
        state.reload_state_in_place(current, statepath)

    assert current["child_commit_survived"] is True
    assert state.load_state(statepath)["child_commit_survived"] is True


def test_guarded_outbox_checkpoints_before_ack_and_replay_only_acks(
    tmp_path, monkeypatch
) -> None:
    statepath = tmp_path / "state.json"
    monkeypatch.setenv("HERDR_TELEGRAM_TOPICS_STATE", str(statepath))
    state.save_state(_store(), statepath)
    real_save_state = state.save_state
    save_calls = 0

    def counted_save(store, path=None):
        nonlocal save_calls
        save_calls += 1
        return real_save_state(store, path=path)

    monkeypatch.setattr(state, "save_state", counted_save)

    class ReplayTendwire(FakeTendwire):
        def __init__(self):
            super().__init__()
            self.acks = 0

        def connector_poll(self, **_kwargs):
            return {
                "ok": True,
                "items": [
                    {
                        "ref": "outbox-ref",
                        "key": "attention:1",
                        "payload": {
                            "event_type": "attention_created",
                            "attention": {
                                "severity": "warning",
                                "reason": "Needs input",
                            },
                        },
                    }
                ],
            }

        def connector_ack(self, _ref, _response):
            self.acks += 1
            return {"ok": self.acks > 1}

    telegram = FakeTelegram()
    tendwire = ReplayTendwire()
    with state.state_lock(statepath):
        current = state.load_state(statepath)
        runtime = source_sync._offlock_runtime(
            current,
            SyncRuntime(
                tendwire=tendwire,
                telegram=telegram,
                with_outbox=True,
            ),
        )
        first = drain_outbox(
            current,
            source_sync._exact_provider_client(
                runtime.telegram,
                reason="test outbox Telegram exact identifiers",
            ),
            source_sync._exact_provider_client(
                runtime.tendwire,
                reason="test outbox Tendwire exact lease",
            ),
            chat_id="-100",
            max_sends=1,
            ack_barrier_persists_state=True,
        )
        second = drain_outbox(
            current,
            source_sync._exact_provider_client(
                runtime.telegram,
                reason="test outbox Telegram exact identifiers",
            ),
            source_sync._exact_provider_client(
                runtime.tendwire,
                reason="test outbox Tendwire exact lease",
            ),
            chat_id="-100",
            max_sends=1,
            ack_barrier_persists_state=True,
        )

    assert first["delivered"] == 1
    assert first["acked"] == 0
    assert first["deferred"] == 1
    assert second["delivered"] == 0
    assert second["acked"] == 1
    assert len(telegram.sent) == 1
    assert tendwire.acks == 2
    # poll/send/ack, then poll/duplicate-ack: no extra whole-state save for
    # the identity because each guarded ACK already supplies that barrier.
    assert save_calls == 5
    assert len(
        state.load_state(statepath)["tendwire_outbox"][
            "delivered_identities"
        ]
    ) == 1


def test_guarded_topic_icon_catalog_persists_and_is_reused(
    tmp_path, monkeypatch
) -> None:
    statepath = tmp_path / "state.json"
    monkeypatch.setenv("HERDR_TELEGRAM_TOPICS_STATE", str(statepath))
    state.save_state(_store(), statepath)
    telegram = FakeTelegram()

    with state.state_lock(statepath):
        current = state.load_state(statepath)
        runtime = source_sync._offlock_runtime(
            current,
            SyncRuntime(
                FakeTendwire(),
                telegram,
                with_outbox=False,
                checkpoint=lambda: state.save_state(
                    current, statepath
                ),
            ),
        )
        first = topic_icon_id(
            current,
            "✅",
            runtime.telegram,
            checkpoint=runtime.checkpoint,
        )
        assert first == "icon-idle"
        assert current["telegram"]["forum_topic_icons"]["by_emoji"][
            "✅"
        ] == "icon-idle"
        assert state.load_state(statepath)["telegram"][
            "forum_topic_icons"
        ]["by_emoji"]["✅"] == "icon-idle"
        second = topic_icon_id(
            current,
            "✅",
            runtime.telegram,
            checkpoint=runtime.checkpoint,
        )

    assert second == "icon-idle"
    assert [
        method
        for method, _payload, _token in telegram.api_calls
        if method == "getForumTopicIconStickers"
    ] == ["getForumTopicIconStickers"]


def test_guarded_topic_icon_error_cache_throttles_ten_panes(
    tmp_path, monkeypatch
) -> None:
    statepath = tmp_path / "state.json"
    monkeypatch.setenv(
        "HERDR_TELEGRAM_TOPICS_STATE", str(statepath)
    )
    store = _store()
    for index in range(10):
        worker = _source_worker(
            {
                "id": f"worker-{index}",
                "name": f"Worker {index}",
                "status": "failed",
                "space_id": "space-1",
                "fingerprint": f"fp-{index}",
            }
        )
        state.upsert_worker_entry(
            store, worker, topic_id=str(770 + index)
        )
    state.save_state(store, statepath)

    class FailingIconTelegram(FakeTelegram):
        def api(self, method, payload):
            self.api_calls.append((method, dict(payload), self.token))
            if method == "getForumTopicIconStickers":
                raise RuntimeError("icon catalogue unavailable")
            return super().api(method, payload)

    telegram = FailingIconTelegram()
    real_save = state.save_state
    save_calls = 0

    def counted_save(current, path=None):
        nonlocal save_calls
        save_calls += 1
        return real_save(current, path=path)

    monkeypatch.setattr(state, "save_state", counted_save)
    with state.state_lock(statepath):
        current = state.load_state(statepath)
        runtime = source_sync._offlock_runtime(
            current,
            SyncRuntime(
                FakeTendwire(),
                telegram,
                with_outbox=False,
                checkpoint=lambda: state.save_state(
                    current, statepath
                ),
            ),
        )
        for key in list(state.source_worker_entries(current)):
            entry = state.source_worker_entries(current)[key]
            assert not source_sync._sync_topic_icon(
                current, entry, runtime, chat_id="-100"
            )

    assert [
        method
        for method, _payload, _token in telegram.api_calls
        if method == "getForumTopicIconStickers"
    ] == ["getForumTopicIconStickers"]
    # One guarded-read barrier and one shared error-cache barrier, not two
    # full-state saves for each pane.
    assert save_calls == 2
    persisted = state.load_state(statepath)["telegram"][
        "forum_topic_icons"
    ]
    assert persisted["last_error"] == "icon catalogue unavailable"
    assert persisted["last_error_at"]


def test_sync_topic_icon_reresolves_owner_after_guarded_catalog_read(
    tmp_path, monkeypatch
) -> None:
    statepath = tmp_path / "state.json"
    monkeypatch.setenv(
        "HERDR_TELEGRAM_TOPICS_STATE", str(statepath)
    )
    store = _store()
    worker = _source_worker(
        {
            "id": "worker-icon-missing",
            "name": "Missing icon",
            "status": "failed",
            "space_id": "space-1",
            "fingerprint": "fp-icon-missing",
        }
    )
    key, _entry, _created = state.upsert_worker_entry(
        store, worker, topic_id="77"
    )
    state.save_state(store, statepath)

    class MissingFailedIconTelegram(FakeTelegram):
        def api(self, method, payload):
            self.api_calls.append((method, dict(payload), self.token))
            if method == "getForumTopicIconStickers":
                return {
                    "ok": True,
                    "result": [
                        {
                            "emoji": "✅",
                            "custom_emoji_id": "icon-idle",
                        }
                    ],
                }
            return super().api(method, payload)

    telegram = MissingFailedIconTelegram()
    with state.state_lock(statepath):
        current = state.load_state(statepath)
        stale_entry = state.source_worker_entries(current)[key]
        runtime = source_sync._offlock_runtime(
            current,
            SyncRuntime(
                FakeTendwire(),
                telegram,
                with_outbox=False,
                checkpoint=lambda: state.save_state(
                    current, statepath
                ),
            ),
        )
        assert not source_sync._sync_topic_icon(
            current, stale_entry, runtime, chat_id="-100"
        )
        live_entry = state.source_worker_entries(current)[key]
        assert stale_entry is not live_entry
        assert live_entry["last_topic_icon_missing"] == "‼️"
        state.save_state(current, statepath)

    assert state.load_state(statepath)["panes"][key][
        "last_topic_icon_missing"
    ] == "‼️"


@pytest.mark.parametrize("topic_mode", ["worker", "space"])
def test_guarded_create_then_icon_uses_reloaded_owner(
    tmp_path, monkeypatch, topic_mode
) -> None:
    statepath = tmp_path / f"{topic_mode}.json"
    monkeypatch.setenv(
        "HERDR_TELEGRAM_TOPICS_STATE", str(statepath)
    )
    monkeypatch.setenv("HERDRES_SOURCE_TOPIC_MODE", topic_mode)
    worker = _source_worker(
        {
            "id": "worker-create-icon",
            "name": "Create icon",
            "status": "attention",
            "space_id": "space-1",
            "fingerprint": "fp-create-icon",
        }
    )
    snapshot = {
        "ok": True,
        "workers": [worker],
        "spaces": [
            {
                "id": "space-1",
                "name": "Create icon space",
                "status": "active",
                "fingerprint": "space-fp",
            }
        ],
    }
    state.save_state(_store(), statepath)
    telegram = FakeTelegram()

    with state.state_lock(statepath):
        current = state.load_state(statepath)
        runtime = source_sync._offlock_runtime(
            current,
            SyncRuntime(
                FakeTendwire(),
                telegram,
                with_outbox=False,
                checkpoint=lambda: state.save_state(
                    current, statepath
                ),
            ),
        )
        counts = _sync_sources(
            current,
            snapshot,
            {"turns": []},
            runtime,
            chat_id="-100",
        )
        state.save_state(current, statepath)

    bucket = (
        state.source_worker_entries(current)
        if topic_mode == "worker"
        else state.source_space_entries(current)
    )
    entry = next(iter(bucket.values()))
    assert entry["topic_id"] == "77"
    assert entry["last_topic_icon"] == "❓"
    assert entry["last_topic_icon_id"] == "icon-attention"
    assert counts["icon_updated"] == 1
    persisted_bucket = (
        state.source_worker_entries(state.load_state(statepath))
        if topic_mode == "worker"
        else state.source_space_entries(state.load_state(statepath))
    )
    assert next(iter(persisted_bucket.values()))[
        "last_topic_icon_id"
    ] == "icon-attention"


@pytest.mark.parametrize("topic_mode", ["worker", "space"])
def test_guarded_cleanup_prunes_from_reloaded_bucket(
    tmp_path, monkeypatch, topic_mode
) -> None:
    statepath = tmp_path / f"cleanup-{topic_mode}.json"
    monkeypatch.setenv(
        "HERDR_TELEGRAM_TOPICS_STATE", str(statepath)
    )
    monkeypatch.setenv("HERDRES_SOURCE_TOPIC_MODE", topic_mode)
    store = _store()
    if topic_mode == "worker":
        for index, topic_id in enumerate(("77", "78"), start=1):
            worker_id = f"council-{index}"
            store["panes"][f"worker:{worker_id}"] = {
                "source": "tendwire",
                "entry_type": "worker",
                "tendwire_worker_id": worker_id,
                "worker_id": worker_id,
                "tendwire_fingerprint": f"worker-fp-{index}",
                "topic_id": topic_id,
                "topic_name": f"Council · cleanup {index}",
                "worker_name": "gm-local-as",
                "status": "closed",
                "tendwire_raw_status": "closed",
            }
    else:
        for index, topic_id in enumerate(("77", "78"), start=1):
            _key, entry, _created = state.upsert_space_entry(
                store,
                {
                    "id": f"council-space-{index}",
                    "name": f"Council · cleanup {index}",
                    "status": "closed",
                    "fingerprint": f"space-fp-{index}",
                },
                topic_id=topic_id,
            )
            entry["stale_space_topic"] = True
            entry["space_topic_name"] = (
                f"Council · cleanup {index}"
            )
    state.save_state(store, statepath)

    class RebindSecondEntryOnFirstDelete(FakeTelegram):
        def delete_topic(self, chat_id, thread_id):
            if str(thread_id) == "77":
                concurrent = state.load_state(statepath)
                bucket = (
                    state.source_worker_entries(concurrent)
                    if topic_mode == "worker"
                    else state.source_space_entries(concurrent)
                )
                second = next(
                    entry
                    for entry in bucket.values()
                    if str(entry.get("topic_id") or "") == "78"
                )
                second["tendwire_fingerprint"] = (
                    "concurrently-rebound-fingerprint"
                )
                state.save_state(concurrent, statepath)
            return super().delete_topic(chat_id, thread_id)

    telegram = RebindSecondEntryOnFirstDelete()

    with state.state_lock(statepath):
        current = state.load_state(statepath)
        runtime = source_sync._offlock_runtime(
            current,
            SyncRuntime(
                FakeTendwire(),
                telegram,
                with_outbox=False,
                checkpoint=lambda: state.save_state(
                    current, statepath
                ),
            ),
        )
        result = _cleanup_topics(
            current,
            runtime,
            chat_id="-100",
            snapshot_worker_ids={"live"},
        )
        state.save_state(current, statepath)

    bucket = (
        state.source_worker_entries(current)
        if topic_mode == "worker"
        else state.source_space_entries(current)
    )
    persisted_bucket = (
        state.source_worker_entries(state.load_state(statepath))
        if topic_mode == "worker"
        else state.source_space_entries(state.load_state(statepath))
    )
    assert result["deleted"] == 2
    assert result["pruned"] == (
        2 if topic_mode == "space" else 0
    )
    assert telegram.deleted_topics == ["77", "78"]
    assert bucket == {}
    assert persisted_bucket == {}


def test_executor_read_rejects_mutator_and_with_token_stays_guarded() -> None:
    store = _store()
    guarded = _OfflockClient(FakeTelegram(), store, "telegram")

    with pytest.raises(RuntimeError, match="not classified read-only"):
        source_sync._OFFLOCK_EXECUTOR.read(
            guarded,
            "send_message",
            "-100",
            "unsafe",
        )

    rebound = source_sync._OFFLOCK_EXECUTOR.with_token(
        guarded, "another-token"
    )
    assert isinstance(rebound, _OfflockClient)
    with pytest.raises(RuntimeError, match="requires"):
        rebound.send_message("-100", "still unsafe")


def test_raising_offlock_provider_reloads_before_caller_continues(
    tmp_path, monkeypatch
):
    _reset_lock_state()
    statepath = tmp_path / "state.json"
    monkeypatch.setenv("HERDR_TELEGRAM_TOPICS_STATE", str(statepath))
    state.save_state(_store(), statepath)
    entered = threading.Event()
    concurrent_committed = threading.Event()
    finished = threading.Event()

    class RaisingProvider:
        def configured(self):
            entered.set()
            assert concurrent_committed.wait(2)
            raise RuntimeError("provider rate limited")

    def invoke():
        with state.state_lock(path=statepath):
            current = state.load_state(statepath)
            try:
                _OfflockClient(RaisingProvider(), current).configured()
            except RuntimeError:
                current["caller_continued"] = True
                state.save_state(current, statepath)
        finished.set()

    thread = threading.Thread(target=invoke)
    thread.start()
    assert entered.wait(1)
    with state.state_lock(path=statepath):
        concurrent = state.load_state(statepath)
        concurrent["child_terminal_receipt"] = "committed"
        state.save_state(concurrent, statepath)
    concurrent_committed.set()
    thread.join(3)

    assert finished.is_set()
    persisted = state.load_state(statepath)
    assert persisted["child_terminal_receipt"] == "committed"
    assert persisted["caller_continued"] is True


def test_blocked_sync_observation_does_not_delay_command_submission(
    tmp_path, monkeypatch
):
    statepath = tmp_path / "state.json"
    monkeypatch.setenv("HERDR_TELEGRAM_TOPICS_STATE", str(statepath))
    monkeypatch.setenv("HERDRES_SOURCE_TOPIC_MODE", "worker")
    monkeypatch.setenv("HERDRES_PINNED_STATUS", "0")
    monkeypatch.setenv("HERDRES_INBOUND_LANES", "1")
    worker = _source_worker(
        {
            "id": "worker-1",
            "name": "Alpha",
            "status": "idle",
            "space_id": "space-1",
            "fingerprint": "fp-1",
        }
    )
    store = _store()
    state.upsert_worker_entry(store, worker, topic_id="77")
    state.save_state(store, statepath)
    entered = threading.Event()
    release = threading.Event()
    submitted: list[str] = []

    class SlowSyncTendwire(FakeTendwire):
        def snapshot(self):
            entered.set()
            assert release.wait(6)
            return super().snapshot()

    class CommandTendwire:
        def command_json(self, request_json):
            submitted.append(request_json)
            return _accepted_command_response(json.loads(request_json))

    slow = SlowSyncTendwire(workers=[worker])
    monkeypatch.setattr(
        herdres,
        "_runtime",
        lambda **_kwargs: SyncRuntime(
            slow, FakeTelegram(), with_outbox=False
        ),
    )
    monkeypatch.setattr(herdres, "TendwireClient", CommandTendwire)
    result: dict[str, object] = {}

    thread = threading.Thread(target=lambda: result.update(herdres._sync_pass()))
    thread.start()
    assert entered.wait(1)
    started = time.monotonic()
    reply = herdres.command_reply(
        {
            "request_id": REQUEST_ID,
            "topic_id": "77",
            "message_id": "9001",
            "text": "submit while sync RPC is blocked",
        }
    )
    elapsed = time.monotonic() - started
    release.set()
    thread.join(6)

    assert reply["checkpoint"] == "advance"
    assert reply["disposition"] == "terminal_accepted"
    # This direct caller has no gateway instant-ack evidence, so lane mode alone
    # must not suppress the terminal success reply.
    assert reply["reply"] == "Sent to Tendwire worker."
    assert len(submitted) == 1
    assert elapsed < 5.0
    assert not thread.is_alive()
    assert result["ok"] is True


def test_sync_yields_again_before_heavy_source_reconciliation(
    tmp_path, monkeypatch
):
    """The command child gets a lock window after gateway preflight."""

    _reset_lock_state()
    statepath = tmp_path / "state.json"
    monkeypatch.setenv("HERDRES_TENDWIRE_MODE", "source")
    monkeypatch.setenv("HERDRES_SOURCE_TOPIC_MODE", "worker")
    monkeypatch.setenv("HERDR_TELEGRAM_TOPICS_STATE", str(statepath))
    state.save_state(_store(), statepath)
    release_windows = 0
    real_released_lock = state.released_lock
    real_sync_sources = source_sync._sync_sources

    class ObservedRelease:
        def __enter__(self):
            nonlocal release_windows
            release_windows += 1
            self._window = real_released_lock()
            return self._window.__enter__()

        def __exit__(self, *args):
            return self._window.__exit__(*args)

    def observed_sync_sources(*args, **kwargs):
        # One window belongs to provider observation. The second is the
        # explicit post-observation handoff for the gateway command child.
        assert release_windows >= 2
        return real_sync_sources(*args, **kwargs)

    monkeypatch.setattr(state, "released_lock", ObservedRelease)
    monkeypatch.setattr(source_sync, "_sync_sources", observed_sync_sources)
    with state.state_lock(path=statepath):
        current = state.load_state(statepath)
        result = sync_once(
            current,
            SyncRuntime(FakeTendwire(), FakeTelegram(), with_outbox=False),
        )

    assert result["ok"] is True
    assert release_windows >= 2


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


def test_real_sync_delivery_releases_lock_within_budget(tmp_path, monkeypatch):
    """The production sync_once wrapping is what keeps Telegram delivery off-lock."""

    _reset_lock_state()
    monkeypatch.setenv("HERDRES_TENDWIRE_MODE", "source")
    monkeypatch.setenv("HERDRES_SOURCE_TOPIC_MODE", "worker")
    monkeypatch.setenv("HERDR_TELEGRAM_TOPICS_RICH_MESSAGES", "0")
    statepath = tmp_path / "state.json"
    monkeypatch.setenv("HERDR_TELEGRAM_TOPICS_STATE", str(statepath))
    state.save_state(_store(), statepath)
    delivery_entered = threading.Event()
    release_delivery = threading.Event()
    competitor_acquired = threading.Event()
    sync_finished = threading.Event()

    class SlowDeliveryTelegram(FakeTelegram):
        def send_message(self, chat_id, html, **kwargs):
            delivery_entered.set()
            assert release_delivery.wait(3)
            return super().send_message(chat_id, html, **kwargs)

    def run_sync():
        with state.state_lock(path=statepath):
            current = state.load_state(statepath)
            result = sync_once(
                current,
                SyncRuntime(
                    _two_final_turns_tendwire(),
                    SlowDeliveryTelegram(),
                    with_outbox=False,
                ),
            )
            assert result["sent"] == 2
            state.save_state(current, statepath)
        sync_finished.set()

    def compete():
        with state.state_lock(path=statepath):
            competitor_acquired.set()

    sync_thread = threading.Thread(target=run_sync)
    sync_thread.start()
    assert delivery_entered.wait(1)
    competitor_thread = threading.Thread(target=compete)
    competitor_thread.start()
    try:
        assert competitor_acquired.wait(0.75)
    finally:
        release_delivery.set()
    sync_thread.join(4)
    competitor_thread.join(4)

    assert sync_finished.is_set()


def test_flock_hold_instrumentation_reports_phase_and_timestamps(
    tmp_path, monkeypatch, capsys
):
    statepath = tmp_path / "state.json"
    holds = []
    monkeypatch.setattr(state, "_LOCK_HOLD_WARN_SECONDS", 0.0)

    with state.observe_lock_holds(holds.append):
        with state.state_lock(path=statepath, phase="sync.test_probe"):
            time.sleep(0.002)

    assert len(holds) == 1
    hold = holds[0]
    assert hold["phase"] == "sync.test_probe"
    assert hold["phase_trace"] == ("sync.test_probe",)
    assert hold["released_at"] >= hold["acquired_at"]
    assert hold["hold_seconds"] > 0
    warning = capsys.readouterr().err
    assert "[herdres-state-lock]" in warning
    assert "phase=sync.test_probe" in warning
    assert "acquired_at=" in warning
    assert "released_at=" in warning


def test_full_busy_pass_flock_p99_and_concurrent_canonical_commit_budget(
    tmp_path, monkeypatch
):
    """Production composition: slow account refresh is off-lock for a lane child."""

    _reset_lock_state()
    statepath = tmp_path / "state.json"
    monkeypatch.setenv("HERDRES_TENDWIRE_MODE", "source")
    monkeypatch.setenv("HERDRES_SOURCE_TOPIC_MODE", "worker")
    monkeypatch.setenv("HERDRES_PINNED_STATUS", "1")
    monkeypatch.setenv("HERDRES_PINNED_ACCOUNT", "1")
    monkeypatch.setenv("HERDR_TELEGRAM_TOPICS_STATUS_ICON", "0")
    monkeypatch.setenv("HERDR_TELEGRAM_TOPICS_RICH_MESSAGES", "0")
    monkeypatch.setenv("HERDR_TELEGRAM_TOPICS_STATE", str(statepath))
    workers = [
        _source_worker(
            {
                "id": f"worker-{index}",
                "name": f"Worker {index}",
                "status": "idle",
                "space_id": "space-1",
                "fingerprint": f"fp-{index}",
                "meta": {"agent": "codex"},
            }
        )
        for index in range(12)
    ]
    turns = {
        "turns": [
            {
                "id": f"turn-{index}",
                "worker_id": worker["id"],
                "worker_fingerprint": worker["fingerprint"],
                "assistant_final_text": f"Final {index}",
                "complete": True,
            }
            for index, worker in enumerate(workers)
        ]
    }
    store = _store()
    for index, worker in enumerate(workers):
        state.upsert_worker_entry(
            store, worker, topic_id=str(700 + index)
        )
    state.save_state(store, statepath)

    account_refresh_entered = threading.Event()
    release_account_refresh = threading.Event()

    def slow_account_refresh():
        account_refresh_entered.set()
        assert release_account_refresh.wait(5)
        return {"fetched_at": time.time()}

    monkeypatch.setattr(
        source_sync.accounts, "usage_snapshot", slow_account_refresh
    )
    tendwire = FakeTendwire(
        turns=turns,
        pending={"pending_interactions": []},
        workers=workers,
        spaces=[],
    )
    tendwire.turn_final_poll = lambda **_kwargs: {"ok": True, "items": []}
    telegram = FakeTelegram()

    def runtime_factory(**kwargs):
        return SyncRuntime(
            tendwire,
            telegram,
            with_outbox=bool(kwargs.get("with_outbox", True)),
            checkpoint=kwargs.get("checkpoint"),
        )

    class CommandTendwire:
        def command_json(self, request_json):
            return _accepted_command_response(json.loads(request_json))

    monkeypatch.setattr(herdres, "_runtime", runtime_factory)
    monkeypatch.setattr(herdres, "TendwireClient", CommandTendwire)
    holds = []
    sync_result: dict[str, object] = {}
    sync_error: list[BaseException] = []

    def run_sync():
        try:
            sync_result.update(herdres._sync_pass())
        except BaseException as exc:  # pragma: no cover - surfaced below
            sync_error.append(exc)

    with state.observe_lock_holds(holds.append):
        sync_thread = threading.Thread(target=run_sync)
        sync_thread.start()
        assert account_refresh_entered.wait(2)
        started = time.monotonic()
        reply = herdres.command_reply(
            {
                "request_id": REQUEST_ID,
                "topic_id": "700",
                "message_id": "9001",
                "text": "commit while a full pass refreshes account usage",
            }
        )
        canonical_commit_seconds = time.monotonic() - started
        release_account_refresh.set()
        sync_thread.join(8)

    assert not sync_error
    assert not sync_thread.is_alive()
    assert sync_result["ok"] is True
    assert reply["disposition"] == "terminal_accepted"
    assert canonical_commit_seconds < 3.0
    assert holds
    hold_seconds = sorted(float(hold["hold_seconds"]) for hold in holds)
    p99 = hold_seconds[math.ceil(0.99 * len(hold_seconds)) - 1]
    assert p99 < 2.0
    assert any(
        hold["phase"] == "sync.pinned.account_usage" for hold in holds
    )
    persisted = state.load_state(statepath)
    request = persisted["tendwire_ingress_command_requests"][REQUEST_ID]
    assert isinstance(request.get("request_json"), str)


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
        _source_worker(
            {"id": f"worker-{i}", "name": f"w{i}", "status": "working", "space_id": "space-1",
             "fingerprint": f"fp-{i}", "meta": {"agent": "codex"}},
        )
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


# --- durable multipart checkpoints -------------------------------------------

_PLAN_A = "twplan1.plan_A"
_PLAN_B = "twplan1.plan_B"
_PLAN_C = "twplan1.plan_C"
_PLAN_D = "twplan1.plan_D"
_REV_A = "twrev1.revision_A"
_REV_B = "twrev1.revision_B"
_REV_C = "twrev1.revision_C"
_REV_D = "twrev1.revision_D"


def _turn_job_key(plan_token: str, sequence_index: int) -> str:
    return f"turn-final:{plan_token}:{sequence_index:08d}"


def _reserve_job(
    store,
    *,
    plan_token=_PLAN_A,
    content_revision=_REV_A,
    sequence_index=0,
    part_ordinal=0,
    part_count=2,
    operation="upsert",
    prior_message_id="",
):
    return state.reserve_tendwire_turn_job(
        store,
        _turn_job_key(plan_token, sequence_index),
        plan_token=plan_token,
        content_revision=content_revision,
        operation=operation,
        sequence_index=sequence_index,
        part_ordinal=part_ordinal,
        part_count=part_count,
        prior_message_id=prior_message_id,
    )


def _checkpointed_success(store, *, plan_token, content_revision, sequence_index):
    receipt = _reserve_job(
        store,
        plan_token=plan_token,
        content_revision=content_revision,
        sequence_index=sequence_index,
        part_ordinal=0,
        part_count=1,
    )
    job_key = _turn_job_key(plan_token, sequence_index)
    state.update_tendwire_turn_job(
        store,
        job_key,
        substate="telegram_applied",
        telegram_message_id=str(700 + sequence_index),
        bot_kind="codex",
    )
    state.update_tendwire_turn_job(
        store,
        job_key,
        substate="acknowledged",
    )
    return receipt


def test_stable_job_key_resume_ignores_new_lease_ref_and_preserves_success(tmp_path):
    store = _store()
    job_key = _turn_job_key(_PLAN_A, 0)
    original = _reserve_job(store)
    state.update_tendwire_turn_job(
        store,
        job_key,
        substate="telegram_applied",
        telegram_message_id="901",
        bot_kind="codex",
    )
    statepath = tmp_path / "state.json"
    state.save_state(store, statepath)

    restarted = state.load_state(statepath)
    resumed = _reserve_job(restarted)
    assert resumed is state.find_tendwire_turn_job(restarted, job_key)
    assert resumed["substate"] == "telegram_applied"
    assert resumed["telegram_message_id"] == "901"
    assert len(state.tendwire_turn_jobs(restarted)) == 1
    assert "lease_ref" not in resumed and "ref" not in resumed
    with pytest.raises(ValueError):
        state.find_tendwire_turn_job(restarted, "twref1.new-attempt")

    # A retry carrying any new lease ref consults only the stable key. Since Telegram success
    # survived restart, it may advance directly to acknowledged without another send intent.
    state.update_tendwire_turn_job(
        restarted,
        job_key,
        substate="acknowledged",
    )
    assert original["plan_token"] == resumed["plan_token"]
    assert resumed["substate"] == "acknowledged"


def test_job_substates_are_strict_and_checkpoint_old_slot_retirement():
    store = _store()
    job_key = _turn_job_key(_PLAN_A, 0)
    receipt = _reserve_job(store, prior_message_id="501")
    assert receipt["substate"] == "reserved"
    state.update_tendwire_turn_job(
        store,
        job_key,
        substate="telegram_applied",
        telegram_message_id="901",
        bot_kind="codex",
    )
    telegram_checkpoint = receipt["checkpoint_sequence"]
    state.update_tendwire_turn_job(
        store,
        job_key,
        substate="old_slot_retired",
    )
    assert receipt["substate"] == "old_slot_retired"
    assert receipt["checkpoint_sequence"] > telegram_checkpoint
    state.update_tendwire_turn_job(store, job_key, substate="acknowledged")
    with pytest.raises(ValueError, match="invalid tendwire job transition"):
        state.update_tendwire_turn_job(store, job_key, substate="reserved")

    failed_key = _turn_job_key(_PLAN_B, 1)
    failed = _reserve_job(
        store,
        plan_token=_PLAN_B,
        content_revision=_REV_B,
        sequence_index=1,
        part_ordinal=0,
        part_count=1,
    )
    state.update_tendwire_turn_job(store, failed_key, substate="failed")
    assert failed["substate"] == "failed"
    with pytest.raises(ValueError, match="invalid tendwire job transition"):
        state.update_tendwire_turn_job(
            store,
            failed_key,
            substate="telegram_applied",
            telegram_message_id="999",
        )
    with pytest.raises(ValueError, match="conflicting tendwire job reservation"):
        state.reserve_tendwire_turn_job(
            store,
            failed_key,
            plan_token=_PLAN_B,
            content_revision=_REV_B,
            operation="retire",
            sequence_index=1,
            part_ordinal=1,
            part_count=1,
        )


def test_sync_pass_checkpoint_persists_successful_prefix_before_ack(
    tmp_path, monkeypatch
):
    _reset_lock_state()
    statepath = tmp_path / "state.json"
    monkeypatch.setenv("HERDR_TELEGRAM_TOPICS_STATE", str(statepath))
    store = _store()
    first_key = _turn_job_key(_PLAN_A, 0)
    second_key = _turn_job_key(_PLAN_A, 1)
    _reserve_job(store, sequence_index=0, part_ordinal=0)
    _reserve_job(store, sequence_index=1, part_ordinal=1)
    state.save_state(store, statepath)
    events = []

    def runtime_factory(**kwargs):
        return SimpleNamespace(checkpoint=kwargs["checkpoint"])

    def interrupted_sync(current, runtime):
        state.update_tendwire_turn_job(
            current,
            first_key,
            substate="telegram_applied",
            telegram_message_id="801",
            bot_kind="codex",
        )
        runtime.checkpoint()
        events.append("checkpoint")
        on_disk = state.load_state(statepath)
        assert on_disk["tendwire_turn_jobs"][first_key]["substate"] == "telegram_applied"
        assert on_disk["tendwire_turn_jobs"][second_key]["substate"] == "reserved"
        events.append("ack_attempt")
        raise RuntimeError("simulated Tendwire ACK outage")

    monkeypatch.setattr(herdres, "_runtime", runtime_factory)
    monkeypatch.setattr(herdres, "sync_once", interrupted_sync)
    with pytest.raises(RuntimeError, match="ACK outage"):
        herdres._sync_pass()

    assert events == ["checkpoint", "ack_attempt"]
    restarted = state.load_state(statepath)
    assert restarted["tendwire_turn_jobs"][first_key]["substate"] == "telegram_applied"
    assert restarted["tendwire_turn_jobs"][first_key]["telegram_message_id"] == "801"
    assert restarted["tendwire_turn_jobs"][second_key]["substate"] == "reserved"


def test_runtime_exposes_optional_checkpoint_without_breaking_old_construction(
    monkeypatch,
):
    checkpoint = lambda: None
    monkeypatch.setattr(herdres.config, "telegram_token", lambda: "token")
    monkeypatch.setattr(herdres, "TendwireClient", lambda: object())
    monkeypatch.setattr(
        herdres,
        "TelegramClient",
        lambda *, token, dry_run: SimpleNamespace(token=token, dry_run=dry_run),
    )
    runtime = herdres._runtime(
        dry_run=False,
        with_outbox=True,
        checkpoint=checkpoint,
    )
    assert runtime.checkpoint is checkpoint
    assert runtime.with_outbox is True


def test_continuation_bindings_share_worker_topic_and_delivery_identity(monkeypatch):
    monkeypatch.setenv("HERDRES_SOURCE_TOPIC_MODE", "worker")
    store = _store()
    worker_key, worker, _created = state.upsert_worker_entry(
        store,
        _source_worker(
            {
                "id": "worker-1",
                "name": "Alpha",
                "status": "idle",
                "space_id": "space-1",
                "fingerprint": "fp-1",
            }
        ),
        topic_id="77",
    )
    for ordinal, message_id in enumerate(("901", "902")):
        job_key = _turn_job_key(_PLAN_A, ordinal)
        state.bind_message_to_worker(
            store,
            message_id,
            worker,
            topic_id="77",
            kind="final",
            turn_id="turn-1",
            bot_kind="codex",
            content_revision=_REV_A,
            plan_token=_PLAN_A,
            part_ordinal=ordinal,
            part_count=2,
            tendwire_job_key=job_key,
        )

    bindings = [state.find_message_binding(store, mid, topic_id="77") for mid in ("901", "902")]
    assert all(binding is not None for binding in bindings)
    assert {binding["worker_id"] for binding in bindings} == {"worker-1"}
    assert {binding["topic_id"] for binding in bindings} == {"77"}
    assert {binding["content_revision"] for binding in bindings} == {_REV_A}
    assert {binding["plan_token"] for binding in bindings} == {_PLAN_A}
    assert [binding["part_ordinal"] for binding in bindings] == [0, 1]
    for message_id in ("901", "902"):
        assert herdres._worker_entry_from_reply(
            store,
            {"reply_to_message_id": message_id, "topic_id": "77"},
        ) == (worker_key, worker)


def test_old_binding_callers_keep_exact_legacy_shape():
    store = _store()
    _key, worker, _created = state.upsert_worker_entry(
        store,
        _source_worker(
            {
                "id": "worker-1",
                "name": "Alpha",
                "status": "idle",
                "space_id": "space-1",
                "fingerprint": "fp-1",
            }
        ),
        topic_id="77",
    )
    state.bind_message_to_worker(
        store,
        "500",
        worker,
        topic_id="77",
        kind="final",
        turn_id="turn-legacy",
        bot_kind="codex",
    )
    binding = state.message_bindings(store)["500"]
    assert binding == {
        "topic_id": "77",
        "worker_id": "worker-1",
        "worker_fingerprint": "fp-1",
        "space_id": "space-1",
        "kind": "final",
        "turn_id": "turn-legacy",
        "bot_kind": "codex",
        "stable_key": worker["tendwire_stable_key"],
        "stable_key_version": 1,
    }


def test_job_cleanup_removes_only_unreferenced_terminal_receipts():
    store = _store()
    _key, worker, _created = state.upsert_worker_entry(
        store,
        _source_worker(
            {
                "id": "worker-1",
                "name": "Alpha",
                "status": "idle",
                "space_id": "space-1",
                "fingerprint": "fp-1",
            }
        ),
        topic_id="77",
    )
    old = _checkpointed_success(
        store,
        plan_token=_PLAN_A,
        content_revision=_REV_A,
        sequence_index=0,
    )
    bound = _checkpointed_success(
        store,
        plan_token=_PLAN_B,
        content_revision=_REV_B,
        sequence_index=1,
    )
    entry_referenced = _checkpointed_success(
        store,
        plan_token=_PLAN_C,
        content_revision=_REV_C,
        sequence_index=2,
    )
    pending = _reserve_job(
        store,
        plan_token=_PLAN_D,
        content_revision=_REV_D,
        sequence_index=3,
        part_ordinal=0,
        part_count=1,
    )
    bound_key = _turn_job_key(_PLAN_B, 1)
    state.bind_message_to_worker(
        store,
        bound["telegram_message_id"],
        worker,
        topic_id="77",
        kind="final",
        turn_id="turn-bound",
        bot_kind="codex",
        content_revision=_REV_B,
        plan_token=_PLAN_B,
        part_ordinal=0,
        part_count=1,
        tendwire_job_key=bound_key,
    )
    worker["last_clean_plan_token"] = _PLAN_C
    worker["last_clean_content_revision"] = _REV_C

    removed = state.cleanup_tendwire_turn_jobs(store, max_records=1)
    jobs = state.tendwire_turn_jobs(store)
    assert removed == 1
    assert _turn_job_key(_PLAN_A, 0) not in jobs
    assert jobs[bound_key] is bound
    assert jobs[_turn_job_key(_PLAN_C, 2)] is entry_referenced
    assert jobs[_turn_job_key(_PLAN_D, 3)] is pending
    assert pending["substate"] == "reserved"
    assert not any(
        forbidden in receipt
        for receipt in jobs.values()
        for forbidden in ("text", "html", "payload", "lease_ref", "ref")
    )


def test_receipt_capacity_covers_maximum_old_and_replacement_plans():
    store = _store()
    old_plan = "twplan1.capacity_old"
    old_revision = "twrev1.capacity_old"
    new_plan = "twplan1.capacity_new"
    new_revision = "twrev1.capacity_new"
    maximum_parts = 10_000

    for ordinal in range(maximum_parts):
        _reserve_job(
            store,
            plan_token=old_plan,
            content_revision=old_revision,
            sequence_index=ordinal,
            part_ordinal=ordinal,
            part_count=maximum_parts,
        )
    _reserve_job(
        store,
        plan_token=new_plan,
        content_revision=new_revision,
        sequence_index=0,
        part_ordinal=0,
        part_count=1,
    )
    for sequence in range(1, maximum_parts):
        _reserve_job(
            store,
            plan_token=new_plan,
            content_revision=new_revision,
            sequence_index=sequence,
            part_ordinal=maximum_parts - sequence,
            part_count=1,
            operation="retire",
        )

    assert len(state.tendwire_turn_jobs(store)) == maximum_parts * 2
    assert state.TENDWIRE_TURN_JOB_LIMIT >= maximum_parts * 2 + 1
