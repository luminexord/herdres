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


def test_captured_live_shape_retires_every_displaced_topic_holder():
    store, workers = _captured_rekey_repro()
    expected_new_retirements = (
        "worker:claude-1-1-1-1:0e8605e506",
        "worker:claude-1-1-2-1:96044de7d5",
        "worker:claude-1-2-1:e504eacfcc",
        "worker:claude-1-2-2-1:82436b9eff",
        "worker:claude-1-2-2-2:fbadaa6841",
        "worker:claude-1-2:53d81a5f5b",
        "worker:claude-2-1-2-2:29c310e21b",
        "worker:claude-2-1:07fcdba57d",
        "worker:claude:73fd1aefd9",
        "worker:codex-1-1:9d8e174441",
        "worker:codex-1:0550716aa5",
        "worker:codex:03205b2610",
        "worker:codex:03205b2610:2",
        "worker:codex:a2cc350e22",
    )
    topic_holders = {
        key: entry
        for key, entry in state.source_worker_entries(store).items()
        if entry.get("topic_id")
    }
    already_retired = {
        key for key, entry in topic_holders.items() if state.entry_is_retired(entry)
    }
    assert len(topic_holders) == 19
    assert len(already_retired) == 4

    plan = state.plan_worker_rekey_continuity(store, workers)

    # No topic is safe to hand off in this captured incident. The one exact
    # current owner remains live; every other unretired topic holder retires.
    assert plan.matches == ()
    assert len(plan.stale_entry_keys) == 39
    assert tuple(
        key
        for key in plan.stale_entry_keys
        if topic_holders.get(key, {}).get("topic_id")
    ) == expected_new_retirements
    assert len(expected_new_retirements) == 14
    assert dict(state.apply_worker_rekey_continuity_plan(store, workers, plan)) == {}

    retired_topic_holders = {
        key
        for key, entry in topic_holders.items()
        if state.entry_is_retired(entry)
    }
    assert len(retired_topic_holders) == 18
    assert retired_topic_holders == already_retired | set(expected_new_retirements)
    for key in expected_new_retirements:
        entry = topic_holders[key]
        assert entry["routing_retired_reason"] == "herdr_restart_rekey_unmatched"
        assert entry["topic_id"]

    # Retirement is presence-based, so the next planner pass has no work.
    assert state.plan_worker_rekey_continuity(store, workers) == ((), ())


def test_captured_live_shape_recreates_every_worker_topic_in_bounded_passes(
    monkeypatch,
):
    store, workers = _captured_rekey_repro()
    plan = state.plan_worker_rekey_continuity(store, workers)
    state.apply_worker_rekey_continuity_plan(store, workers, plan)
    original_retired_topics = {
        str(entry["topic_id"])
        for entry in state.source_worker_entries(store).values()
        if entry.get("topic_id") and state.entry_is_retired(entry)
    }
    assert len(original_retired_topics) == 18

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

    # The fixture has one already-fresh exact owner; ten workers need creation.
    assert sum(len(routable_topic_entries(worker)) for worker in workers) == 1
    monkeypatch.setenv("HERDR_TELEGRAM_TOPICS_MAX_CREATES", "3")
    telegram = FakeTelegram()
    runtime = SyncRuntime(FakeTendwire(workers=workers), telegram, with_outbox=False)
    create_deltas = []
    for _ in range(4):
        before = len(telegram.topics)
        sync_once(store, runtime)
        create_deltas.append(len(telegram.topics) - before)

    assert create_deltas == [3, 3, 3, 1]
    assert len(telegram.topics) == 10
    assert all(len(routable_topic_entries(worker)) == 1 for worker in workers)
    assert {
        str(entry["topic_id"])
        for entry in state.source_worker_entries(store).values()
        if state.entry_is_retired(entry) and entry.get("topic_id")
    } == original_retired_topics
    assert not any(
        str(entry.get("topic_id") or "") in original_retired_topics
        for key, entry in state.source_worker_entries(store).items()
        if state.worker_entry_is_uniquely_routable(store, key, entry)
    )

    panes_after_healing = deepcopy(state.source_worker_entries(store))
    before_calls = (
        len(telegram.topics),
        len(telegram.renamed_topics),
        len(telegram.closed_topics),
    )
    sync_once(store, runtime)
    assert state.source_worker_entries(store) == panes_after_healing
    assert (
        len(telegram.topics),
        len(telegram.renamed_topics),
        len(telegram.closed_topics),
    ) == before_calls


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
