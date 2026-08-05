"""Off-lock presenter delivery for source mode (issue #122).

Provider operations run outside the state flock and reload durable presenter state afterward.
These tests pin the mechanism
(state_lock exposes/restores the held fd, released_lock is a no-op when no lock is held, the fd is
thread-local, drop-then-reacquire, re-acquire-failure propagates), the two runtime flag readers, the
commit-before-yield / reload-after no-clobber invariant, and the _cleanup_topics per-pass delete cap.
"""
from __future__ import annotations

import fcntl
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
    FakeTelegram,
    FakeTendwire,
    _source_worker,
    _store,
)
from test_turn_final_delivery import (
    DeletingTelegram,
    TurnFinalTendwire,
    _runtime as _turn_runtime,
    _turn_row,
)


_PENDING_FINAL_FIELDS = {
    "pending_turn_id",
    "pending_content_revision",
    "pending_plan_token",
    "pending_turn_part_count",
    "pending_turn_job_count",
    "pending_turn_started_at",
    "pending_turn_user_hash",
    "pending_stream_submission_id",
    "pending_plan_generation",
    "pending_presentation_version",
    "pending_final_identity",
    "pending_working_predecessor_turn_id",
}


def _reset_lock_state():
    state._LOCK_STATE.held_fd = None
    state._LOCK_STATE.release_depth = 0


def _competitor_can_acquire(lock_path) -> bool:
    """Return whether an independent state writer can acquire the released flock."""
    with open(lock_path, "a+", encoding="utf-8") as fh:
        try:
            fcntl.flock(fh, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return False
        fcntl.flock(fh, fcntl.LOCK_UN)
        return True


def _fsynced_turn_pass(
    statepath,
    tendwire,
    telegram,
    *,
    max_sends: int,
    after_provider_accept=None,
):
    with state.state_lock(path=statepath):
        store = state.load_state(statepath)

        def checkpoint() -> None:
            assert state.lock_actually_held()
            state.save_state(store, statepath)

        result = sync_once(
            store,
            _turn_runtime(
                tendwire,
                telegram,
                max_sends=max_sends,
                checkpoint=checkpoint,
                after_provider_accept=(
                    None
                    if after_provider_accept is None
                    else lambda: after_provider_accept(store)
                ),
            ),
        )
        if result.get("changed"):
            state.save_state(store, statepath)
    return result, state.load_state(statepath)


def _seed_fsynced_turn_state(statepath, row) -> None:
    store = _store()
    state.upsert_worker_entry(
        store,
        _source_worker(
            {
                "id": row["worker_id"],
                "name": "Alpha",
                "status": "idle",
                "space_id": row.get("space_id") or "space-1",
                "fingerprint": row["worker_fingerprint"],
                "meta": {
                    "agent": "codex",
                    "stable_key": row["stable_key"],
                    "stable_key_version": row["stable_key_version"],
                },
            }
        ),
        topic_id="77",
    )
    state.save_state(store, statepath)


def _final_bindings(store, turn_id: str):
    return sorted(
        (
            (message_id, binding)
            for message_id, binding in state.message_bindings(store).items()
            if binding.get("kind") == "final"
            and binding.get("turn_id") == turn_id
        ),
        key=lambda item: item[1]["part_ordinal"],
    )


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


def test_fsynced_committed_ack_loss_restart_keeps_completed_identity(
    tmp_path, monkeypatch
) -> None:
    statepath = tmp_path / "state.json"
    monkeypatch.setenv("HERDR_TELEGRAM_TOPICS_STATE", str(statepath))
    monkeypatch.setenv("HERDRES_TENDWIRE_MODE", "source")
    monkeypatch.setenv("HERDRES_PINNED_STATUS", "0")
    monkeypatch.setenv("HERDR_TELEGRAM_TOPICS_STATUS_ICON", "0")
    row = _turn_row(
        "turn-fsynced-committed-loss",
        "twrev1.fsynced_committed_loss",
        "accepted exactly once",
    )
    _seed_fsynced_turn_state(statepath, row)
    tendwire = TurnFinalTendwire(row)
    tendwire.ack_committed_response_lost_once = True
    telegram = DeletingTelegram()

    first, persisted = _fsynced_turn_pass(
        statepath, tendwire, telegram, max_sends=1
    )

    assert first["tendwire_turn_final"]["operations"] == 1
    assert first["tendwire_turn_final"]["acked"] == 0
    assert first["tendwire_turn_final"]["status"] == "timeout"
    bindings = _final_bindings(persisted, row["id"])
    assert len(bindings) == 1
    message_ids = [message_id for message_id, _binding in bindings]
    assert len(telegram.sent) == 1
    assert telegram.edited == []
    assert telegram.deleted_messages == []
    assert [sent[3] for sent in telegram.sent] == message_ids
    entry = next(iter(state.source_worker_entries(persisted).values()))
    assert entry["last_clean_message_ids"] == message_ids
    assert entry["last_clean_content_revision"] == row["content"][
        "content_revision"
    ]
    assert entry["last_clean_plan_token"] == "twplan1.plan1"
    assert _PENDING_FINAL_FIELDS.isdisjoint(entry)
    identity = (
        f"final:{row['id']}:"
        f"{row['content']['content_revision']}"
    )
    delivered = state.delivered_turns(persisted)[identity]
    assert delivered["message_ids"] == message_ids
    assert delivered["content_revision"] == row["content"][
        "content_revision"
    ]
    writes = len(telegram.sent) + len(telegram.edited)

    restarted, restarted_store = _fsynced_turn_pass(
        statepath, tendwire, telegram, max_sends=1
    )

    assert restarted["tendwire_turn_final"]["polled"] == 0
    assert restarted["tendwire_turn_final"]["operations"] == 0
    assert len(telegram.sent) + len(telegram.edited) == writes == 1
    restarted_entry = next(
        iter(state.source_worker_entries(restarted_store).values())
    )
    assert restarted_entry["last_clean_message_ids"] == message_ids
    assert _PENDING_FINAL_FIELDS.isdisjoint(restarted_entry)


def test_fsynced_upsert_defers_when_reloaded_owner_is_absent(
    tmp_path, monkeypatch
) -> None:
    statepath = tmp_path / "state.json"
    monkeypatch.setenv("HERDR_TELEGRAM_TOPICS_STATE", str(statepath))
    monkeypatch.setenv("HERDRES_TENDWIRE_MODE", "source")
    monkeypatch.setenv("HERDRES_PINNED_STATUS", "0")
    monkeypatch.setenv("HERDR_TELEGRAM_TOPICS_STATUS_ICON", "0")
    row = _turn_row(
        "turn-owner-removed-offlock",
        "twrev1.owner_removed_offlock",
        "accepted after owner removal",
    )
    _seed_fsynced_turn_state(statepath, row)
    tendwire = TurnFinalTendwire(row)

    telegram = DeletingTelegram()

    def remove_owner_after_binding(store) -> None:
        concurrent = state.load_state(statepath)
        concurrent["panes"] = {}
        state.save_state(concurrent, statepath)
        state.reload_state_in_place(store, statepath)

    result, persisted = _fsynced_turn_pass(
        statepath,
        tendwire,
        telegram,
        max_sends=1,
        after_provider_accept=remove_owner_after_binding,
    )

    assert len(telegram.sent) == 1
    assert tendwire.ack_calls == []
    assert len(tendwire.defer_calls) == 1
    assert tendwire.defer_calls[0][1] == "transient_delivery"
    assert result["tendwire_turn_final"]["acked"] == 0
    assert result["tendwire_turn_final"]["deferred"] == 1
    assert state.source_worker_entries(persisted) == {}


@pytest.mark.parametrize(
    "ack_loss_flag",
    ["ack_loss_once", "ack_committed_response_lost_once"],
)
def test_fsynced_multipart_ack_loss_restart_writes_each_ordinal_once(
    tmp_path, monkeypatch, ack_loss_flag
) -> None:
    statepath = tmp_path / f"{ack_loss_flag}.json"
    monkeypatch.setenv("HERDR_TELEGRAM_TOPICS_STATE", str(statepath))
    monkeypatch.setenv("HERDRES_TENDWIRE_MODE", "source")
    monkeypatch.setenv("HERDRES_PINNED_STATUS", "0")
    monkeypatch.setenv("HERDR_TELEGRAM_TOPICS_STATUS_ICON", "0")
    text = ("ordered fsynced multipart answer\n\n" * 900) + "FINAL_TAIL"
    row = _turn_row(
        f"turn-fsynced-{ack_loss_flag}",
        f"twrev1.fsynced_{ack_loss_flag}",
        text,
    )
    _seed_fsynced_turn_state(statepath, row)
    tendwire = TurnFinalTendwire(row)
    setattr(tendwire, ack_loss_flag, True)
    telegram = DeletingTelegram()

    first, prefix_store = _fsynced_turn_pass(
        statepath, tendwire, telegram, max_sends=1
    )

    part_count = int(tendwire._plans["twplan1.plan1"]["part_count"])
    assert part_count > 1
    assert first["tendwire_turn_final"]["operations"] == 1
    assert first["tendwire_turn_final"]["acked"] == 0
    assert len(_final_bindings(prefix_store, row["id"])) == 1
    assert len(telegram.sent) == 1

    resumed, converged = _fsynced_turn_pass(
        statepath, tendwire, telegram, max_sends=part_count + 1
    )

    bindings = _final_bindings(converged, row["id"])
    message_ids = [message_id for message_id, _binding in bindings]
    assert [binding["part_ordinal"] for _message_id, binding in bindings] == list(
        range(part_count)
    )
    assert [sent[3] for sent in telegram.sent] == message_ids
    assert len(telegram.sent) == part_count
    assert len(set(message_ids)) == part_count
    assert telegram.edited == []
    assert telegram.deleted_messages == []
    entry = next(iter(state.source_worker_entries(converged).values()))
    assert entry["last_clean_message_ids"] == message_ids
    assert entry["last_clean_content_revision"] == row["content"][
        "content_revision"
    ]
    assert entry["last_clean_plan_token"] == "twplan1.plan1"
    assert _PENDING_FINAL_FIELDS.isdisjoint(entry)
    identity = (
        f"final:{row['id']}:"
        f"{row['content']['content_revision']}"
    )
    assert state.delivered_turns(converged)[identity][
        "message_ids"
    ] == message_ids
    assert resumed["tendwire_turn_final"]["operations"] == part_count - 1

    restarted, restarted_store = _fsynced_turn_pass(
        statepath, tendwire, telegram, max_sends=part_count + 1
    )

    assert restarted["tendwire_turn_final"]["polled"] == 0
    assert restarted["tendwire_turn_final"]["operations"] == 0
    assert len(telegram.sent) == part_count
    restarted_entry = next(
        iter(state.source_worker_entries(restarted_store).values())
    )
    assert restarted_entry["last_clean_message_ids"] == message_ids
    assert _PENDING_FINAL_FIELDS.isdisjoint(restarted_entry)


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






# --- config flags ------------------------------------------------------------

def test_offlock_interpane_yield_flag():
    assert config.offlock_interpane_yield_enabled(env={}) is True                  # default on
    assert config.offlock_interpane_yield_enabled(env={"HERDRES_OFFLOCK_INTERPANE_YIELD": "0"}) is False


def test_source_orphan_delete_cap():
    assert config.source_orphan_delete_cap(env={}) == 3                            # default
    assert config.source_orphan_delete_cap(env={"HERDR_TELEGRAM_TOPICS_SOURCE_DELETE_LIMIT": "5"}) == 5
    assert config.source_orphan_delete_cap(env={"HERDR_TELEGRAM_TOPICS_SOURCE_DELETE_LIMIT": "bad"}) == 3


# --- yield behaviour in sync_once -------------------------------------------









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
