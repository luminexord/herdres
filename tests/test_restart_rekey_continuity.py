"""Production-shaped Herdr restart re-key continuity regression."""

from __future__ import annotations

import json
from copy import deepcopy
from pathlib import Path

import pytest

from herdres_connector import state
from herdres_connector.source_sync import SyncRuntime, sync_once

from test_source_only import FakeTelegram, FakeTendwire, _store


_REKEY_REPRO_FIXTURES = Path(__file__).parent / "fixtures" / "rekey-repro"


def _key(letter: str) -> str:
    return "wsk1_" + letter * 64


def _worker(
    worker_id: str,
    stable_key: str,
    *,
    label: str,
    agent: str,
    cwd: str,
    title: str,
    space: str,
    fingerprint: str,
) -> dict:
    return {
        "id": worker_id,
        "name": agent,
        "status": "working",
        "space_id": space,
        "fingerprint": fingerprint,
        "meta": {
            "agent": agent,
            "label": label,
            "cwd": cwd,
            "foreground_cwd": cwd,
            "terminal_title": title,
            "stable_key": stable_key,
            "stable_key_version": 1,
        },
    }


@pytest.fixture(autouse=True)
def _worker_mode(monkeypatch):
    monkeypatch.setenv("HERDRES_TENDWIRE_MODE", "source")
    monkeypatch.setenv("HERDRES_SOURCE_TOPIC_MODE", "worker")
    monkeypatch.setenv("HERDRES_PINNED_STATUS", "0")
    monkeypatch.setenv("HERDRES_REAP_CLOSED_WORKER_TOPICS", "1")


def _persist(store: dict, worker: dict, topic_id: str | None = None):
    return state.upsert_worker_entry(
        store, worker, topic_id=topic_id or ""
    )[:2]


def _captured_rekey_repro() -> tuple[dict, list[dict]]:
    store = json.loads(
        (_REKEY_REPRO_FIXTURES / "state-live.json").read_text(encoding="utf-8")
    )
    workers = json.loads(
        (_REKEY_REPRO_FIXTURES / "workers-live.json").read_text(encoding="utf-8")
    )
    return store, workers


def _captured_duplicate_repro() -> tuple[dict, list[dict]]:
    store = json.loads(
        (_REKEY_REPRO_FIXTURES / "state-dupes-live.json").read_text(
            encoding="utf-8"
        )
    )
    workers = json.loads(
        (_REKEY_REPRO_FIXTURES / "workers-live.json").read_text(encoding="utf-8")
    )
    return store, workers


def test_captured_duplicate_state_consolidates_by_stable_key_and_survives_renumber():
    store, workers = _captured_duplicate_repro()
    original_topics = {
        "14985",
        "14987",
        "14989",
        "14997",
        "14999",
        "15001",
        "15003",
        "15005",
        "15007",
        "15009",
        "15011",
    }
    captured_duplicate_topics = {
        "15136",
        "15140",
        "15142",
        "15151",
        "15153",
        "15155",
    }
    assert any(
        len(state._all_worker_entry_keys_by_stable_key(store, stable_key)) >= 3
        for stable_key in {state.worker_stable_key(worker) for worker in workers}
    )

    telegram = FakeTelegram()
    sync_once(
        store,
        SyncRuntime(FakeTendwire(workers=workers), telegram, with_outbox=False),
    )

    for worker in workers:
        stable_key = state.worker_stable_key(worker)
        assert len(state._all_worker_entry_keys_by_stable_key(store, stable_key)) == 1
    assert telegram.topics == []
    assert set(telegram.closed_topics) == captured_duplicate_topics
    assert {thread for _chat, thread, _name in telegram.renamed_topics} == (
        captured_duplicate_topics
    )
    assert {
        str(kwargs.get("thread_id") or "")
        for _chat, text, kwargs, _message_id in telegram.sent
        if "stable pane identity was consolidated" in text
    } == captured_duplicate_topics

    for topic_id in original_topics:
        entry_key, entry = state.find_entry_by_thread(store, topic_id)
        assert entry_key is not None
        assert entry is not None
        assert entry["topic_id"] == topic_id
        assert state.worker_entry_is_uniquely_routable(store, entry_key, entry)
    for topic_id in captured_duplicate_topics:
        assert state.find_entry_by_thread(store, topic_id) == (None, None)

    stable_topics = {
        state.entry_stable_identity(entry): str(entry.get("topic_id") or "")
        for entry in state.source_worker_entries(store).values()
        if state.entry_stable_identity(entry) is not None
    }
    pane_keys = {
        identity: key
        for key, entry in state.source_worker_entries(store).items()
        if (identity := state.entry_stable_identity(entry)) is not None
    }
    renumbered = deepcopy(workers)
    for index, worker in enumerate(renumbered, start=1):
        worker["id"] = f"renumbered-{index}"
        worker["fingerprint"] = f"renumbered-fingerprint-{index}"

    renumber_telegram = FakeTelegram()
    sync_once(
        store,
        SyncRuntime(
            FakeTendwire(workers=renumbered),
            renumber_telegram,
            with_outbox=False,
        ),
    )
    assert renumber_telegram.topics == []
    assert renumber_telegram.sent == []
    assert renumber_telegram.renamed_topics == []
    assert renumber_telegram.closed_topics == []
    assert {
        state.entry_stable_identity(entry): str(entry.get("topic_id") or "")
        for entry in state.source_worker_entries(store).values()
        if state.entry_stable_identity(entry) is not None
    } == stable_topics
    assert {
        identity: key
        for key, entry in state.source_worker_entries(store).items()
        if (identity := state.entry_stable_identity(entry)) is not None
    } == pane_keys
    for topic_id in original_topics:
        assert state.find_entry_by_thread(store, topic_id) != (None, None)

    panes_after_renumber = deepcopy(store["panes"])
    repeated_telegram = FakeTelegram()
    sync_once(
        store,
        SyncRuntime(
            FakeTendwire(workers=renumbered),
            repeated_telegram,
            with_outbox=False,
        ),
    )
    assert store["panes"] == panes_after_renumber
    assert repeated_telegram.topics == []
    assert repeated_telegram.sent == []
    assert repeated_telegram.renamed_topics == []
    assert repeated_telegram.closed_topics == []


def test_captured_live_shape_stable_owners_are_not_displaced_by_worker_ids():
    store, workers = _captured_rekey_repro()
    assert state.consolidate_worker_entries_by_stable_key(store, workers) > 0
    stable_owner_keys = {
        key
        for key, entry in state.source_worker_entries(store).items()
        if state.entry_stable_identity(entry)
        in {state.worker_stable_identity(worker) for worker in workers}
    }
    assert len(stable_owner_keys) == len(workers)
    assert all(
        len(
            state._all_worker_entry_keys_by_stable_key(
                store, state.worker_stable_key(worker)
            )
        )
        == 1
        for worker in workers
    )
    plan = state.plan_worker_rekey_continuity(store, workers)
    assert plan.matches == ()
    assert stable_owner_keys.isdisjoint(plan.stale_entry_keys)


def test_captured_live_shape_reuses_stable_topics_and_only_mints_missing_topic():
    store, workers = _captured_rekey_repro()
    def routable_topic_entries(worker: dict) -> list[dict]:
        identity = state.worker_stable_identity(worker)
        return [
            entry
            for key, entry in state.source_worker_entries(store).items()
            if entry.get("topic_id")
            and state.worker_entry_is_uniquely_routable(store, key, entry)
            and state.entry_stable_identity(entry) == identity
            and entry.get("tendwire_worker_id") == worker["id"]
        ]

    telegram = FakeTelegram()
    runtime = SyncRuntime(FakeTendwire(workers=workers), telegram, with_outbox=False)
    sync_once(store, runtime)

    assert telegram.topics == ["gitmoot-codex"]
    assert all(len(routable_topic_entries(worker)) == 1 for worker in workers)

    stable_topics_after_healing = {
        state.entry_stable_identity(entry): str(entry.get("topic_id") or "")
        for entry in state.source_worker_entries(store).values()
        if state.entry_stable_identity(entry) is not None
    }
    repeated_telegram = FakeTelegram()
    sync_once(
        store,
        SyncRuntime(
            FakeTendwire(workers=workers),
            repeated_telegram,
            with_outbox=False,
        ),
    )
    assert {
        state.entry_stable_identity(entry): str(entry.get("topic_id") or "")
        for entry in state.source_worker_entries(store).values()
        if state.entry_stable_identity(entry) is not None
    } == stable_topics_after_healing
    assert all(len(routable_topic_entries(worker)) == 1 for worker in workers)
    assert repeated_telegram.topics == []
    assert repeated_telegram.renamed_topics == []
    assert repeated_telegram.closed_topics == []


def test_retired_only_worker_history_does_not_block_fresh_creation():
    store = _store()
    worker = _worker(
        "claude-2",
        _key("6"),
        label="pane-r",
        agent="claude",
        cwd="/work/r",
        title="Pane R",
        space="space-r",
        fingerprint="fp-r",
    )
    retired_key, retired = _persist(store, worker, "14000")
    state._retire_rekey_entry(
        retired,
        reason="test_retired_history",
        archive_topic=True,
    )

    telegram = FakeTelegram()
    runtime = SyncRuntime(FakeTendwire(workers=[worker]), telegram, with_outbox=False)
    sync_once(store, runtime)

    fresh = [
        (key, entry)
        for key, entry in state.source_worker_entries(store).items()
        if key != retired_key
        and entry.get("tendwire_worker_id") == worker["id"]
        and entry.get("topic_id")
    ]
    assert len(fresh) == 1
    fresh_key, fresh_entry = fresh[0]
    assert fresh_entry["topic_id"] != "14000"
    assert state.worker_entry_is_uniquely_routable(store, fresh_key, fresh_entry)
    assert state.entry_is_retired(state.source_worker_entries(store)[retired_key])

    sync_once(store, runtime)
    assert len(telegram.topics) == 1


def test_physical_match_migrates_without_any_worker_id_match():
    store = _store()
    old_key, old = _persist(
        store,
        _worker(
            "claude-9",
            _key("1"),
            label="pane-a",
            agent="claude",
            cwd="/work/a",
            title="Pane A",
            space="space-a",
            fingerprint="old-a",
        ),
        "13000",
    )
    current = _worker(
        "claude-2",
        _key("2"),
        label="pane-a",
        agent="claude",
        cwd="/work/a",
        title="Pane A",
        space="space-a",
        fingerprint="new-a",
    )
    current_key, current_entry = _persist(store, current)

    telegram = FakeTelegram()
    sync_once(
        store,
        SyncRuntime(
            FakeTendwire(workers=[current]),
            telegram,
            with_outbox=False,
        ),
    )

    assert old["tendwire_worker_id"] != current["id"]
    assert state.entry_is_retired(store["panes"][old_key])
    assert store["panes"][old_key]["retired_topic_id"] == "13000"
    assert current_entry["topic_id"] == "13000"
    assert state.find_entry_by_thread(store, "13000") == (
        current_key,
        current_entry,
    )
    assert telegram.topics == []
    assert telegram.deleted_topics == []


def test_restart_rekey_worker_id_reshuffle_heals_safely_and_idempotently():
    store = _store()

    # Historical rows own the Telegram topics.  Their worker ids are
    # positional and will be claimed by different physical panes after the
    # restart (the live claude-7 -> historical claude-1 incident shape).
    old_alpha_key, old_alpha = _persist(
        store,
        _worker(
            "claude-1",
            _key("a"),
            label="alpha",
            agent="claude",
            cwd="/work/alpha",
            title="Alpha shell",
            space="space-alpha",
            fingerprint="old-alpha",
        ),
        "13857",
    )
    old_beta_key, old_beta = _persist(
        store,
        _worker(
            "claude-7",
            _key("b"),
            label="beta",
            agent="claude",
            cwd="/work/beta",
            title="Beta shell",
            space="space-beta",
            fingerprint="old-beta",
        ),
        "13898",
    )
    old_gamma_a_key, old_gamma_a = _persist(
        store,
        _worker(
            "codex",
            _key("c"),
            label="gamma",
            agent="codex",
            cwd="/work/gamma",
            title="Gamma shell",
            space="space-gamma",
            fingerprint="old-gamma-a",
        ),
        "13900",
    )
    old_gamma_b_key, old_gamma_b = _persist(
        store,
        _worker(
            "codex",
            _key("d"),
            label="gamma",
            agent="codex",
            cwd="/work/gamma",
            title="Gamma shell",
            space="space-gamma",
            fingerprint="old-gamma-b",
        ),
        "13901",
    )
    old_delta_key, old_delta = _persist(
        store,
        _worker(
            "kimi",
            _key("7"),
            label="delta",
            agent="kimi",
            cwd="/work/delta",
            title="Delta shell",
            space="space-delta",
            fingerprint="old-delta",
        ),
        "13902",
    )

    current_alpha = _worker(
        "claude-7",
        _key("e"),
        label="alpha",
        agent="claude",
        cwd="/work/alpha",
        title="Alpha shell",
        space="space-alpha",
        fingerprint="new-alpha",
    )
    current_beta = _worker(
        "claude-1",
        _key("f"),
        label="beta",
        agent="claude",
        cwd="/work/beta",
        title="Beta shell",
        space="space-beta",
        fingerprint="new-beta",
    )
    current_gamma = _worker(
        "codex",
        _key("9"),
        label="gamma",
        agent="codex",
        cwd="/work/gamma",
        title="Gamma shell",
        space="space-gamma",
        fingerprint="new-gamma",
    )
    current_epsilon = _worker(
        "kimi",
        _key("8"),
        label="epsilon",
        agent="kimi",
        cwd="/work/epsilon",
        title="Epsilon shell",
        space="space-epsilon",
        fingerprint="new-epsilon",
    )

    # This is the exact persisted post-restart shape: new stable-keyed rows
    # exist beside old topic-owning rows but do not yet own topics.
    new_alpha_key, new_alpha = _persist(store, current_alpha)
    new_beta_key, new_beta = _persist(store, current_beta)
    new_gamma_key, new_gamma = _persist(store, current_gamma)
    new_epsilon_key, new_epsilon = _persist(store, current_epsilon)
    assert all(
        "topic_id" not in entry
        for entry in (new_alpha, new_beta, new_gamma, new_epsilon)
    )
    assert old_beta["tendwire_worker_id"] == current_alpha["id"]
    assert old_alpha["tendwire_worker_id"] == current_beta["id"]

    telegram = FakeTelegram()
    sync_once(
        store,
        SyncRuntime(
            FakeTendwire(
                workers=[
                    current_alpha,
                    current_beta,
                    current_gamma,
                    current_epsilon,
                ]
            ),
            telegram,
            with_outbox=False,
        ),
    )

    entries = state.source_worker_entries(store)
    assert entries[new_alpha_key]["topic_id"] == "13857"
    assert entries[new_beta_key]["topic_id"] == "13898"
    assert entries[old_alpha_key]["retired_topic_id"] == "13857"
    assert entries[old_beta_key]["retired_topic_id"] == "13898"
    assert "topic_id" not in entries[old_alpha_key]
    assert "topic_id" not in entries[old_beta_key]

    # The two indistinguishable historical codex rows are not guessed between:
    # both histories are archived, and the live pane receives a fresh topic.
    assert entries[new_gamma_key]["topic_id"] not in {"13900", "13901"}
    assert entries[new_epsilon_key]["topic_id"] != "13902"
    assert entries[old_gamma_a_key]["topic_id"] == "13900"
    assert entries[old_gamma_b_key]["topic_id"] == "13901"
    assert entries[old_delta_key]["topic_id"] == "13902"
    assert entries[old_gamma_a_key]["topic_name"].startswith("📁 ")
    assert entries[old_gamma_b_key]["topic_name"].startswith("📁 ")
    assert entries[old_delta_key]["topic_name"].startswith("📁 ")
    assert telegram.closed_topics == ["13900", "13901", "13902"]
    assert {thread for _chat, thread, _name in telegram.renamed_topics} == {
        "13900",
        "13901",
        "13902",
    }
    assert telegram.deleted_topics == []

    for current_key in (
        new_alpha_key,
        new_beta_key,
        new_gamma_key,
        new_epsilon_key,
    ):
        current = entries[current_key]
        routed_key, routed = state.find_entry_by_thread(
            store, str(current["topic_id"])
        )
        assert routed_key == current_key
        assert routed is current
        assert state.worker_entry_is_uniquely_routable(
            store, current_key, current
        )
    assert all(
        state.entry_is_retired(entries[key])
        for key in (
            old_alpha_key,
            old_beta_key,
            old_gamma_a_key,
            old_gamma_b_key,
            old_delta_key,
        )
    )

    panes_after_first = deepcopy(entries)
    repeated_telegram = FakeTelegram()
    sync_once(
        store,
        SyncRuntime(
            FakeTendwire(
                workers=[
                    current_alpha,
                    current_beta,
                    current_gamma,
                    current_epsilon,
                ]
            ),
            repeated_telegram,
            with_outbox=False,
        ),
    )

    assert state.source_worker_entries(store) == panes_after_first
    assert repeated_telegram.topics == []
    assert repeated_telegram.renamed_topics == []
    assert repeated_telegram.closed_topics == []
    assert repeated_telegram.deleted_topics == []


def test_cwd_conflict_vetoes_migration_even_with_other_agreements() -> None:
    # Verifier-reproduced hazard: same label+agent+terminal_title+space but a
    # DIFFERENT cwd must never migrate (two panes in one workspace that share
    # a reused label differ only by directory).
    from herdres_connector import state

    stale = _worker(
        "claude-1",
        "wsk1_" + "a" * 64,
        label="worker-pane",
        agent="claude",
        cwd="/work/OLD-PATH",
        title="shared-title",
        space="space-1",
        fingerprint="fp-old",
    )
    live = _worker(
        "claude-2",
        "wsk1_" + "b" * 64,
        label="worker-pane",
        agent="claude",
        cwd="/work/NEW-PATH",
        title="shared-title",
        space="space-1",
        fingerprint="fp-new",
    )
    entry = {
        "entry_type": "worker",
        "tendwire_worker_id": "claude-1",
        "tendwire_pane_label": "worker-pane",
        "tendwire_foreground_cwd": "/work/OLD-PATH",
        "tendwire_terminal_title": "shared-title",
        "agent": "claude",
        "space_id": "space-1",
    }
    assert state._physical_identity_matches(entry, live) is False

    # Same shape with MATCHING cwd migrates (control: the veto is cwd-specific).
    live_same_cwd = _worker(
        "claude-2",
        "wsk1_" + "c" * 64,
        label="worker-pane",
        agent="claude",
        cwd="/work/OLD-PATH",
        title="shared-title",
        space="space-1",
        fingerprint="fp-new2",
    )
    assert state._physical_identity_matches(entry, live_same_cwd) is True
