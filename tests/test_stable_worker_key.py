"""Stable per-pane worker key (HERDRES_STABLE_WORKER_KEY): reconcile a re-lettered worker id back to its
existing entry via tendwire's session-independent meta.stable_key, so a herdr restart (claude-2 ->
claude-2-2 for the SAME terminal) reuses the topic instead of stranding a duplicate. Degrades to
worker-id keying when no stable_key is present (older tendwire), gated by an agent/space sanity check.
"""
from __future__ import annotations

import pytest

from herdres_connector import config, state
from herdres_connector.source_sync import SyncRuntime, sync_once

from test_source_only import FakeTelegram, FakeTendwire, _store


@pytest.fixture(autouse=True)
def _worker_mode(monkeypatch):
    monkeypatch.setenv("HERDRES_TENDWIRE_MODE", "source")
    monkeypatch.setenv("HERDRES_SOURCE_TOPIC_MODE", "worker")


def _worker(wid, stable=None, agent="claude", space="w1", cwd="/x/telegram-bot", status="working"):
    meta = {"cwd": cwd, "foreground_cwd": cwd}
    if stable is not None:
        meta["stable_key"] = stable
    return {"id": wid, "name": agent, "status": status, "space_id": space,
            "fingerprint": f"fp-{wid}", "meta": meta}


# --- config ------------------------------------------------------------------

def test_config_default_and_override():
    assert config.stable_worker_key_enabled(env={}) is True                          # default on
    assert config.stable_worker_key_enabled(env={"HERDRES_STABLE_WORKER_KEY": "1"}) is True
    assert config.stable_worker_key_enabled(env={"HERDRES_STABLE_WORKER_KEY": "0"}) is False
    assert config.stable_worker_key_enabled(env={"HERDRES_STABLE_WORKER_KEY": "off"}) is False


# --- keying at the upsert layer ---------------------------------------------

def test_new_worker_stamps_stable_key():
    store = _store()
    _key, entry, created = state.upsert_worker_entry(store, _worker("claude-2", stable="K1"), topic_id="26")
    assert created is True
    assert entry["tendwire_stable_key"] == "K1"
    assert entry["topic_id"] == "26"


def test_restart_reuses_entry_via_stable_key():
    store = _store()
    key1, _e, _c = state.upsert_worker_entry(store, _worker("claude-2", stable="K1"), topic_id="26")
    # herdr restart: same terminal, NEW positional worker id + new fingerprint, SAME stable_key.
    key2, entry, created = state.upsert_worker_entry(store, _worker("claude-2-2", stable="K1"))
    assert key2 == key1              # re-pointed the SAME entry
    assert created is False
    assert entry["topic_id"] == "26"                 # topic preserved (no new topic)
    assert entry["tendwire_worker_id"] == "claude-2-2"   # id updated in place
    assert len(state.source_worker_entries(store)) == 1  # no duplicate entry


def test_absent_stable_key_falls_back_to_worker_id():
    # No stable_key (older tendwire): a re-letter is a brand-new entry — today's behavior, unchanged.
    store = _store()
    state.upsert_worker_entry(store, _worker("claude-2", stable=None), topic_id="26")
    _key, _entry, created = state.upsert_worker_entry(store, _worker("claude-2-2", stable=None))
    assert created is True
    assert len(state.source_worker_entries(store)) == 2


def test_flag_off_forces_worker_id_keying(monkeypatch):
    monkeypatch.setenv("HERDRES_STABLE_WORKER_KEY", "0")
    store = _store()
    state.upsert_worker_entry(store, _worker("claude-2", stable="K1"), topic_id="26")
    _key, _entry, created = state.upsert_worker_entry(store, _worker("claude-2-2", stable="K1"))
    assert created is True                                # stable_key ignored
    assert len(state.source_worker_entries(store)) == 2


def test_sanity_gate_blocks_agent_mismatch():
    # A recycled pane id whose worker is a DIFFERENT agent must not adopt the old entry.
    store = _store()
    state.upsert_worker_entry(store, _worker("claude-2", stable="K1", agent="claude"), topic_id="26")
    _key, _entry, created = state.upsert_worker_entry(store, _worker("codex-1", stable="K1", agent="codex"))
    assert created is True
    assert len(state.source_worker_entries(store)) == 2


def test_new_stable_key_creates_entry():
    store = _store()
    state.upsert_worker_entry(store, _worker("claude-2", stable="K1"), topic_id="26")
    _key, _entry, created = state.upsert_worker_entry(store, _worker("claude-3", stable="K2"), topic_id="28")
    assert created is True
    assert len(state.source_worker_entries(store)) == 2


# --- collision hardening: never fuse two distinct panes into one topic --------

def test_closed_entry_collision_does_not_adopt():
    # A new pane whose stable_key collides with an OLD, FINISHED entry must NOT adopt it (that would
    # merge a dead agent's history into the new topic). It falls through to worker-id keying -> a
    # fresh entry with its own topic, leaving the dead pane untouched.
    store = _store()
    dead_key, dead_entry, _c = state.upsert_worker_entry(
        store, _worker("claude-2", stable="K1", status="closed"), topic_id="26"
    )
    new_key, new_entry, created = state.upsert_worker_entry(
        store, _worker("claude-9", stable="K1", status="working"), topic_id="30"
    )
    assert created is True                                 # did NOT adopt the closed ghost
    assert new_key != dead_key
    assert new_entry["topic_id"] == "30"                   # its own topic
    assert dead_entry["topic_id"] == "26"                  # dead pane untouched
    assert dead_entry["tendwire_worker_id"] == "claude-2"  # not re-pointed
    assert len(state.source_worker_entries(store)) == 2


def test_stable_key_prefers_live_over_closed(monkeypatch):
    # When the SAME stable_key exists on both a closed ghost and a live entry, a re-letter adopts the
    # LIVE one (the skip-closed filter must not break legitimate reuse).
    store = _store()
    monkeypatch.setenv("HERDRES_STABLE_WORKER_KEY", "0")   # seed two entries sharing K1 (no adoption)
    dead_key, _de, _dc = state.upsert_worker_entry(store, _worker("claude-2", stable="K1", status="closed"), topic_id="26")
    live_key, _le, _lc = state.upsert_worker_entry(store, _worker("claude-3", stable="K1", status="working"), topic_id="28")
    assert dead_key != live_key
    monkeypatch.setenv("HERDRES_STABLE_WORKER_KEY", "1")
    key, entry, created = state.upsert_worker_entry(store, _worker("claude-3-2", stable="K1", status="working"))
    assert created is False
    assert key == live_key                                 # adopted the LIVE entry, not the ghost
    assert entry["topic_id"] == "28"
    assert entry["tendwire_worker_id"] == "claude-3-2"
    assert len(state.source_worker_entries(store)) == 2


def test_two_live_entries_same_key_falls_back(monkeypatch):
    # Two DISTINCT live panes ended up sharing a stable_key. A THIRD worker with that key must NOT be
    # silently fused onto either — the resolver detects the duplicate and degrades to worker-id keying.
    store = _store()
    monkeypatch.setenv("HERDRES_STABLE_WORKER_KEY", "0")   # seed two live entries sharing K1
    key_a, _ea, _ca = state.upsert_worker_entry(store, _worker("claude-2", stable="K1", status="working"), topic_id="26")
    key_b, _eb, _cb = state.upsert_worker_entry(store, _worker("claude-3", stable="K1", status="working"), topic_id="28")
    assert key_a != key_b
    assert len(state.source_worker_entries(store)) == 2

    # A direct query returns None (ambiguous), NOT the first match.
    assert state.find_entry_key_by_stable_key(store, "K1") is None

    monkeypatch.setenv("HERDRES_STABLE_WORKER_KEY", "1")
    key_c, _ec, created = state.upsert_worker_entry(store, _worker("claude-9", stable="K1", status="working"))
    assert created is True                                 # new entry, no fusion
    assert key_c not in {key_a, key_b}
    assert len(state.source_worker_entries(store)) == 3


# --- end-to-end: restart reuses the topic, no " 2" -------------------------

def test_sync_once_restart_reuses_topic_no_duplicate(monkeypatch):
    monkeypatch.setenv("HERDRES_PINNED_STATUS", "0")
    store = _store()
    # pass 1: a fresh terminal -> one topic created.
    sync_once(store, SyncRuntime(FakeTendwire(workers=[_worker("claude-2", stable="K1")]),
                                 FakeTelegram(), with_outbox=False))
    entries = state.source_worker_entries(store)
    assert len(entries) == 1
    (created_entry,) = entries.values()
    topic_id = created_entry["topic_id"]

    # pass 2: herdr restart re-letters the SAME terminal (claude-2 gone, claude-2-2 present, same K1).
    telegram = FakeTelegram()
    sync_once(store, SyncRuntime(FakeTendwire(workers=[_worker("claude-2-2", stable="K1")]),
                                 telegram, with_outbox=False))
    entries = state.source_worker_entries(store)
    assert len(entries) == 1                              # SAME entry reused, no duplicate
    (entry,) = entries.values()
    assert entry["topic_id"] == topic_id                 # SAME topic
    assert entry["tendwire_worker_id"] == "claude-2-2"
    assert telegram.topics == []                         # no new topic created on restart
    assert not any(name.endswith(" 2") for name in telegram.topics)  # and never a " 2"


def test_sync_once_without_stable_key_still_strands(monkeypatch):
    # Control: without stable_key, the restart DOES create a second topic (the bug the flag fixes),
    # proving the reuse above is caused by stable_key resolution, not some other dedup.
    monkeypatch.setenv("HERDRES_PINNED_STATUS", "0")
    store = _store()
    sync_once(store, SyncRuntime(FakeTendwire(workers=[_worker("claude-2", stable=None)]),
                                 FakeTelegram(), with_outbox=False))
    telegram = FakeTelegram()
    sync_once(store, SyncRuntime(FakeTendwire(workers=[_worker("claude-2-2", stable=None)]),
                                 telegram, with_outbox=False))
    assert len(state.source_worker_entries(store)) == 2  # duplicate entry (legacy churn)
    assert len(telegram.topics) == 1                     # a second topic was created
