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


def _worker_from_captured_entry(entry: dict) -> dict:
    return {
        "id": entry["tendwire_worker_id"],
        "name": entry["worker_name"],
        "status": entry["tendwire_raw_status"],
        "space_id": entry["tendwire_space_id"],
        "fingerprint": entry["tendwire_fingerprint"],
        "meta": {
            "agent": entry["agent"],
            "label": entry["tendwire_pane_label"],
            "terminal_title": entry["tendwire_terminal_title"],
            "stable_key": entry["tendwire_stable_key"],
            "stable_key_version": entry["tendwire_stable_key_version"],
        },
    }


def test_rotation3_captured_pairs_heal_to_one_uuid_entry_with_original_topics():
    captured = json.loads(
        (_REKEY_REPRO_FIXTURES / "state-rotation3-pairs-live.json").read_text(
            encoding="utf-8"
        )
    )
    store = _store()
    store["panes"] = captured["panes"]
    pair_keys = (
        (
            "worker:claude-1-1:a65adfc545",
            "worker:claude-1:5cf213bebc",
            "15347",
            "15476",
        ),
        (
            "worker:claude-2-2:e48831025e",
            "worker:claude-2-2:fe2b44ae95",
            "15362",
            "15468",
        ),
    )
    workers = [
        _worker_from_captured_entry(store["panes"][live_key])
        for _retired_key, live_key, _topic_id, _duplicate_topic in pair_keys
    ]

    telegram = FakeTelegram()
    sync_once(
        store,
        SyncRuntime(FakeTendwire(workers=workers), telegram, with_outbox=False),
    )

    assert telegram.topics == []
    # Newly retired archives wait the configured TTL before lifecycle cleanup
    # closes them; reconciliation itself never performs topic-close calls.
    assert telegram.closed_topics == []
    assert {thread for _chat, thread, _name in telegram.renamed_topics} == {
        "15476",
        "15468",
    }
    for worker, (original_key, duplicate_key, topic_id, duplicate_topic) in zip(
        workers, pair_keys
    ):
        current_key, current = state.find_worker_entry_by_stable_key(
            store, state.worker_stable_key(worker)
        )
        assert current_key == original_key and current is not None
        assert current["topic_id"] == topic_id
        assert current["topic_name"] == worker["meta"]["label"]
        assert state.entry_pane_uuid(current)
        assert not state.entry_is_retired(current)
        duplicate = store["panes"][duplicate_key]
        assert duplicate["topic_id"] == duplicate_topic
        assert state.entry_is_retired(duplicate)
        assert not state.entry_pane_uuid(duplicate)

    panes_after_first = deepcopy(store["panes"])
    repeated = FakeTelegram()
    sync_once(
        store,
        SyncRuntime(FakeTendwire(workers=workers), repeated, with_outbox=False),
    )
    assert store["panes"] == panes_after_first
    assert repeated.topics == []
    assert repeated.renamed_topics == []
    assert repeated.closed_topics == []


def test_same_busy_pane_keeps_uuid_and_topic_across_five_serial_key_drifts():
    store = _store()
    topic_id = "15347"
    worker = _worker(
        "claude-1-1",
        _key("1"),
        label="herdres",
        agent="claude",
        cwd="",
        title="busy task 0",
        space="w6536a4e5b44342",
        fingerprint="rotation-0",
    )
    original_key, initial_entry = _persist(store, worker, topic_id)
    state.bind_message_to_worker(
        store,
        "500",
        initial_entry,
        topic_id=topic_id,
        kind="final",
        turn_id="turn-before-drift",
    )
    initial_telegram = FakeTelegram()
    sync_once(
        store,
        SyncRuntime(
            FakeTendwire(workers=[worker]), initial_telegram, with_outbox=False
        ),
    )
    current = state.source_worker_entries(store)[original_key]
    pane_uuid = state.entry_pane_uuid(current)
    assert pane_uuid
    assert state.message_bindings(store)["500"]["pane_uuid"] == pane_uuid
    assert initial_telegram.topics == []

    for generation in range(1, 6):
        worker = _worker(
            f"claude-{generation}",
            "wsk1_" + f"{generation + 1:064x}",
            label="herdres",
            agent="claude",
            cwd="",
            title=f"busy task {generation}",
            space="w6536a4e5b44342",
            fingerprint=f"rotation-{generation}",
        )
        telegram = FakeTelegram()
        sync_once(
            store,
            SyncRuntime(
                FakeTendwire(workers=[worker]), telegram, with_outbox=False
            ),
        )

        current_key, current = state.find_worker_entry_by_stable_key(
            store, state.worker_stable_key(worker)
        )
        assert current_key == original_key and current is not None
        assert state.entry_pane_uuid(current) == pane_uuid
        assert current["topic_id"] == topic_id
        binding = state.message_bindings(store)["500"]
        assert binding["pane_uuid"] == pane_uuid
        assert binding["stable_key"] == state.worker_stable_key(worker)
        assert binding["worker_id"] == worker["id"]
        assert telegram.topics == []
        assert telegram.renamed_topics == []
        assert telegram.closed_topics == []
        assert not any(
            state.entry_is_retired(entry)
            for entry in state.source_worker_entries(store).values()
        )
        assert [
            entry
            for entry in state.source_worker_entries(store).values()
            if state.entry_pane_uuid(entry) == pane_uuid
        ] == [current]

    panes_after_drift = deepcopy(store["panes"])
    repeated = FakeTelegram()
    sync_once(
        store,
        SyncRuntime(FakeTendwire(workers=[worker]), repeated, with_outbox=False),
    )
    assert store["panes"] == panes_after_drift
    assert repeated.topics == []
    assert repeated.renamed_topics == []
    assert repeated.closed_topics == []


@pytest.fixture
def session_key_drift_live_shape() -> tuple[dict, list[dict], dict[str, str]]:
    """Sanitized 2026-07-22 shape: released old owners plus new keys."""
    store = _store()
    physical_panes = (
        ("herdres", "14985", "/root/herdres", "claude"),
        ("pipeline", "14987", "/root", "claude"),
        ("joltra-2", "14989", "/root/Clipping-Platform", "claude"),
        ("temp", "14997", "/root", "claude"),
        ("Gitmoot2", "14999", "/root/gitmoot", "claude"),
        ("vetrina", "15003", "/root", "claude"),
        ("joltra", "15005", "/root/Clipping-Platform", "claude"),
        ("Gitmoot4", "15007", "/root/gitmoot", "claude"),
    )
    workers: list[dict] = []
    topics_by_label: dict[str, str] = {}
    for index, (label, topic_id, cwd, agent) in enumerate(physical_panes, start=1):
        old_stable_key = "wsk1_" + f"{index:064x}"
        new_stable_key = "wsk1_" + f"{index + 100:064x}"
        old_worker = _worker(
            f"old-{index}",
            old_stable_key,
            label=label,
            agent=agent,
            cwd=cwd,
            title=f"old session · {label}",
            space="w6536a4e5b44342",
            fingerprint=f"old-fingerprint-{index}",
        )
        _old_entry_key, old_entry = _persist(store, old_worker, topic_id)
        if index == 1:
            state.bind_message_to_worker(
                store,
                "500",
                old_entry,
                topic_id=topic_id,
                kind="final",
                turn_id="turn-before-session-drift",
            )
        # #170 can release the active key from closed history before #168 sees
        # the row.  The topic and exact historical identity remain available.
        old_entry["status"] = "closed"
        old_entry["tendwire_raw_status"] = "closed"
        old_entry["stable_key_quarantined"] = True
        old_entry["stable_key_quarantine_reason"] = "closed_stable_key_reuse"
        old_entry["retired_tendwire_stable_key"] = old_stable_key
        old_entry["retired_tendwire_stable_key_version"] = 1
        old_entry.pop("tendwire_stable_key", None)
        old_entry.pop("tendwire_stable_key_version", None)

        current = _worker(
            f"current-{index}",
            new_stable_key,
            label=label,
            agent=agent,
            cwd=cwd,
            title=f"new session · {label}",
            space="w6536a4e5b44342",
            fingerprint=f"new-fingerprint-{index}",
        )
        # The production Tendwire snapshot omitted cwd even though Herdr still
        # reported it. Missing cwd is neutral; an explicit disagreement vetoes.
        current["meta"].pop("cwd")
        current["meta"].pop("foreground_cwd")
        _persist(store, current)
        workers.append(current)
        topics_by_label[label] = topic_id
    return store, workers, topics_by_label


def test_session_key_drift_migrates_every_live_topic_before_mint_and_is_idempotent(
    session_key_drift_live_shape,
):
    store, workers, topics_by_label = session_key_drift_live_shape
    telegram = FakeTelegram()

    sync_once(
        store,
        SyncRuntime(FakeTendwire(workers=workers), telegram, with_outbox=False),
    )

    assert telegram.topics == []
    assert telegram.renamed_topics == []
    assert telegram.closed_topics == []
    for worker in workers:
        entry_key, entry = state.find_worker_entry_by_stable_key(
            store, state.worker_stable_key(worker)
        )
        assert entry_key is not None and entry is not None
        assert entry["topic_id"] == topics_by_label[worker["meta"]["label"]]
        assert entry["topic_name"] == worker["meta"]["label"]
        assert state.worker_entry_is_uniquely_routable(store, entry_key, entry)
    binding = state.find_message_binding(store, "500", topic_id="14985")
    assert binding is not None
    assert binding["stable_key"] == state.worker_stable_key(workers[0])
    assert binding["worker_id"] == workers[0]["id"]

    panes_after_first = deepcopy(store["panes"])
    repeated_telegram = FakeTelegram()
    sync_once(
        store,
        SyncRuntime(
            FakeTendwire(workers=workers), repeated_telegram, with_outbox=False
        ),
    )
    assert store["panes"] == panes_after_first
    assert repeated_telegram.topics == []
    assert repeated_telegram.renamed_topics == []
    assert repeated_telegram.closed_topics == []


def test_captured_duplicate_state_heals_to_one_uuid_entry_per_pane_and_survives_renumber():
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

    uuid_entries = [
        entry
        for entry in state.source_worker_entries(store).values()
        if state.entry_pane_uuid(entry)
    ]
    assert len(uuid_entries) == len(workers)
    assert len({state.entry_pane_uuid(entry) for entry in uuid_entries}) == len(
        workers
    )
    for worker in workers:
        stable_key = state.worker_stable_key(worker)
        assert len(state._all_worker_entry_keys_by_stable_key(store, stable_key)) == 1
    assert telegram.topics == []
    assert telegram.closed_topics == []
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
    assert "retired_topic_close_pending" not in retired

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
    current_key, _current_entry = _persist(store, current)

    telegram = FakeTelegram()
    sync_once(
        store,
        SyncRuntime(
            FakeTendwire(workers=[current]),
            telegram,
            with_outbox=False,
        ),
    )

    assert old["tendwire_worker_id"] == current["id"]
    assert old["tendwire_stable_key"] == state.worker_stable_key(current)
    assert state.entry_pane_uuid(old)
    assert not state.entry_is_retired(store["panes"][old_key])
    assert current_key not in store["panes"]
    assert old["topic_id"] == "13000"
    assert state.find_entry_by_thread(store, "13000") == (
        old_key,
        old,
    )
    assert telegram.topics == []
    assert telegram.deleted_topics == []


def test_renumber_and_stable_key_drift_update_uuid_entries_in_place():
    store = _store()
    alpha = _worker(
        "claude-1",
        _key("a"),
        label="alpha",
        agent="claude",
        cwd="/work/alpha",
        title="Alpha shell",
        space="space-alpha",
        fingerprint="old-alpha",
    )
    beta = _worker(
        "claude-7",
        _key("b"),
        label="beta",
        agent="claude",
        cwd="/work/beta",
        title="Beta shell",
        space="space-beta",
        fingerprint="old-beta",
    )
    alpha_key, _alpha_entry = _persist(store, alpha, "13857")
    beta_key, _beta_entry = _persist(store, beta, "13898")
    sync_once(
        store,
        SyncRuntime(
            FakeTendwire(workers=[alpha, beta]), FakeTelegram(), with_outbox=False
        ),
    )
    original = {
        alpha_key: (
            state.entry_pane_uuid(store["panes"][alpha_key]),
            "13857",
        ),
        beta_key: (
            state.entry_pane_uuid(store["panes"][beta_key]),
            "13898",
        ),
    }

    # Positional ids swap while both Herdr stable keys drift in the same pass.
    current_alpha = _worker(
        "claude-7",
        _key("e"),
        label="alpha",
        agent="claude",
        cwd="/work/alpha",
        title="Alpha shell · busy",
        space="space-alpha",
        fingerprint="new-alpha",
    )
    current_beta = _worker(
        "claude-1",
        _key("f"),
        label="beta",
        agent="claude",
        cwd="/work/beta",
        title="Beta shell · busy",
        space="space-beta",
        fingerprint="new-beta",
    )
    telegram = FakeTelegram()
    sync_once(
        store,
        SyncRuntime(
            FakeTendwire(workers=[current_alpha, current_beta]),
            telegram,
            with_outbox=False,
        ),
    )

    assert telegram.topics == []
    assert telegram.renamed_topics == []
    assert telegram.closed_topics == []
    for entry_key, (pane_uuid, topic_id) in original.items():
        entry = store["panes"][entry_key]
        assert state.entry_pane_uuid(entry) == pane_uuid
        assert entry["topic_id"] == topic_id
        assert not state.entry_is_retired(entry)
    assert store["panes"][alpha_key]["tendwire_worker_id"] == "claude-7"
    assert store["panes"][beta_key]["tendwire_worker_id"] == "claude-1"

    panes_after_first = deepcopy(store["panes"])
    repeated = FakeTelegram()
    sync_once(
        store,
        SyncRuntime(
            FakeTendwire(workers=[current_beta, current_alpha]),
            repeated,
            with_outbox=False,
        ),
    )
    assert store["panes"] == panes_after_first
    assert repeated.topics == []
    assert repeated.renamed_topics == []
    assert repeated.closed_topics == []


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
    alpha_key, alpha_entry = state.find_worker_entry_by_stable_key(
        store, state.worker_stable_key(current_alpha)
    )
    beta_key, beta_entry = state.find_worker_entry_by_stable_key(
        store, state.worker_stable_key(current_beta)
    )
    assert alpha_key == old_alpha_key and alpha_entry is entries[old_alpha_key]
    assert beta_key == old_beta_key and beta_entry is entries[old_beta_key]
    assert alpha_entry["topic_id"] == "13857"
    assert beta_entry["topic_id"] == "13898"
    assert new_alpha_key not in entries
    assert new_beta_key not in entries

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
    assert telegram.closed_topics == []
    assert {thread for _chat, thread, _name in telegram.renamed_topics} == {
        "13900",
        "13901",
        "13902",
    }
    assert telegram.deleted_topics == []

    for current_key in (alpha_key, beta_key, new_gamma_key, new_epsilon_key):
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


def test_indistinguishable_durable_twins_are_both_archived_without_uuid_theft():
    store = _store()
    twin_a = _worker(
        "codex-1",
        _key("a"),
        label="twin",
        agent="codex",
        cwd="/work/shared",
        title="Shared shell",
        space="space-shared",
        fingerprint="twin-a",
    )
    twin_b = _worker(
        "codex-2",
        _key("b"),
        label="twin",
        agent="codex",
        cwd="/work/shared",
        title="Shared shell",
        space="space-shared",
        fingerprint="twin-b",
    )
    twin_a_key, _twin_a_entry = _persist(store, twin_a, "14100")
    twin_b_key, _twin_b_entry = _persist(store, twin_b, "14101")

    sync_once(
        store,
        SyncRuntime(
            FakeTendwire(workers=[twin_a, twin_b]),
            FakeTelegram(),
            with_outbox=False,
        ),
    )
    entries = state.source_worker_entries(store)
    twin_a_uuid = state.entry_pane_uuid(entries[twin_a_key])
    twin_b_uuid = state.entry_pane_uuid(entries[twin_b_key])
    assert twin_a_uuid and twin_b_uuid and twin_a_uuid != twin_b_uuid
    state.bind_message_to_worker(
        store,
        "601",
        entries[twin_a_key],
        topic_id="14100",
        kind="final",
    )
    state.bind_message_to_worker(
        store,
        "602",
        entries[twin_b_key],
        topic_id="14101",
        kind="final",
    )

    live = _worker(
        "codex-3",
        _key("c"),
        label="twin",
        agent="codex",
        cwd="/work/shared",
        title="Shared shell",
        space="space-shared",
        fingerprint="live-twin",
    )
    telegram = FakeTelegram()
    sync_once(
        store,
        SyncRuntime(FakeTendwire(workers=[live]), telegram, with_outbox=False),
    )

    live_key, live_entry = state.find_worker_entry_by_stable_key(
        store, state.worker_stable_key(live)
    )
    assert live_key is not None and live_entry is not None
    assert live_entry["topic_id"] not in {"14100", "14101"}
    assert state.entry_pane_uuid(live_entry) not in {twin_a_uuid, twin_b_uuid}
    assert len(telegram.topics) == 1
    assert telegram.closed_topics == []
    assert {
        thread for _chat, thread, _name in telegram.renamed_topics
    } == {"14100", "14101"}
    for twin_key, topic_id, pane_uuid in (
        (twin_a_key, "14100", twin_a_uuid),
        (twin_b_key, "14101", twin_b_uuid),
    ):
        twin = state.source_worker_entries(store)[twin_key]
        assert state.entry_is_retired(twin)
        assert twin["routing_retired_reason"] == "durable_pane_identity_ambiguous"
        assert twin["topic_id"] == topic_id
        assert state.entry_pane_uuid(twin) == pane_uuid
        assert state.find_entry_by_thread(store, topic_id) == (None, None)
    assert state.message_bindings(store)["601"]["routing_quarantined"] is True
    assert state.message_bindings(store)["602"]["routing_quarantined"] is True

    panes_after_first = deepcopy(store["panes"])
    repeated = FakeTelegram()
    sync_once(
        store,
        SyncRuntime(FakeTendwire(workers=[live]), repeated, with_outbox=False),
    )
    assert store["panes"] == panes_after_first
    assert repeated.topics == []
    assert repeated.renamed_topics == []
    assert repeated.closed_topics == []


def test_sync_quarantine_heal_requires_the_matching_pane_uuid(monkeypatch):
    store = _store()
    worker = _worker(
        "claude-1",
        _key("4"),
        label="pane-q",
        agent="claude",
        cwd="/work/q",
        title="Pane Q",
        space="space-q",
        fingerprint="before-q",
    )
    worker_key, _entry = _persist(store, worker, "14200")
    space = {
        "id": "space-q",
        "name": "Space Q",
        "status": "active",
        "fingerprint": "space-q-fingerprint",
    }
    state.upsert_space_entry(store, space, topic_id="14250")
    sync_once(
        store,
        SyncRuntime(
            FakeTendwire(workers=[worker], spaces=[space]),
            FakeTelegram(),
            with_outbox=False,
        ),
    )
    current = state.source_worker_entries(store)[worker_key]
    pane_uuid = state.entry_pane_uuid(current)
    assert pane_uuid
    for message_id in ("701", "702"):
        state.bind_message_to_worker(
            store,
            message_id,
            current,
            topic_id="14250",
            kind="final",
        )
        state.message_bindings(store)[message_id]["routing_quarantined"] = True
    mismatched_uuid = "00000000-0000-4000-8000-000000000001"
    state.message_bindings(store)["702"]["pane_uuid"] = mismatched_uuid

    inspected_quarantined_uuids = []
    real_binding_pane_uuid = state.message_binding_pane_uuid

    def recording_binding_pane_uuid(binding):
        if "routing_quarantined" in binding:
            inspected_quarantined_uuids.append(binding.get("pane_uuid"))
        return real_binding_pane_uuid(binding)

    monkeypatch.setattr(
        state, "message_binding_pane_uuid", recording_binding_pane_uuid
    )

    sync_once(
        store,
        SyncRuntime(
            FakeTendwire(workers=[worker], spaces=[space]),
            FakeTelegram(),
            with_outbox=False,
        ),
    )

    healed = state.message_bindings(store)["701"]
    assert "routing_quarantined" not in healed
    assert healed["pane_uuid"] == pane_uuid
    assert healed["worker_id"] == worker["id"]
    assert healed["stable_key"] == state.worker_stable_key(worker)
    rejected = state.message_bindings(store)["702"]
    assert rejected["routing_quarantined"] is True
    assert rejected["pane_uuid"] == mismatched_uuid
    assert pane_uuid in inspected_quarantined_uuids
    assert mismatched_uuid in inspected_quarantined_uuids


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
