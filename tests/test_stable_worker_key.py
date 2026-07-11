"""Stable v1 worker continuity and fail-closed reconciliation contracts."""
from __future__ import annotations

from copy import deepcopy
import json

import pytest

import herdres
from herdres_connector import state
from herdres_connector.source_sync import SyncRuntime, _worker_entry_for_turn, sync_once

from test_source_only import FakeTelegram, FakeTendwire, _store


KEY_A = "wsk1_" + "a" * 64
KEY_B = "wsk1_" + "b" * 64
KEY_C = "wsk1_" + "c" * 64
LEGACY_KEY = "d" * 24
_AUTO_VERSION = object()


@pytest.fixture(autouse=True)
def _worker_mode(monkeypatch):
    monkeypatch.setenv("HERDRES_TENDWIRE_MODE", "source")
    monkeypatch.setenv("HERDRES_SOURCE_TOPIC_MODE", "worker")
    monkeypatch.setenv("HERDRES_PINNED_STATUS", "0")


def _worker(
    wid,
    stable=None,
    *,
    version=_AUTO_VERSION,
    agent="claude",
    space="w1",
    cwd="/x/telegram-bot",
    status="working",
    fingerprint=None,
    meta_extra=None,
):
    meta = {"cwd": cwd, "foreground_cwd": cwd, "agent": agent}
    if stable is not None:
        meta["stable_key"] = stable
        if version is _AUTO_VERSION:
            meta["stable_key_version"] = 1
    if version is not _AUTO_VERSION:
        meta["stable_key_version"] = version
    if meta_extra:
        meta.update(meta_extra)
    return {
        "id": wid,
        "name": agent,
        "status": status,
        "space_id": space,
        "fingerprint": fingerprint or f"fp-{wid}",
        "meta": meta,
    }


def _persisted_legacy_worker(store, wid, *, legacy_value=None, topic_id="26", **worker_kwargs):
    key, entry, _created = state.upsert_worker_entry(
        store,
        _worker(wid, KEY_C, **worker_kwargs),
        topic_id=topic_id,
    )
    entry.pop("tendwire_stable_key", None)
    entry.pop("tendwire_stable_key_version", None)
    entry.pop("tendwire_stable_identity_class", None)
    if legacy_value is not None:
        entry["tendwire_stable_key"] = legacy_value
    return key, entry


def _persisted_missing_version_worker(
    store, wid, stable_key, *, topic_id="26", **worker_kwargs
):
    key, entry, _created = state.upsert_worker_entry(
        store,
        _worker(wid, stable_key, **worker_kwargs),
        topic_id=topic_id,
    )
    entry.pop("tendwire_stable_key_version")
    entry["tendwire_stable_identity_class"] = "private_missing_version"
    return key, entry


def _final_turn(worker_id, *, turn_id="turn-1", text="Full final answer"):
    return {
        "id": turn_id,
        "worker_id": worker_id,
        "worker_fingerprint": f"fp-{worker_id}",
        "user_text": "Question",
        "assistant_final_text": text,
        "complete": True,
    }


_BACKEND_HEALTH_ABSENT = object()


class _BackendHealthTendwire(FakeTendwire):
    def __init__(self, *, backend_health=_BACKEND_HEALTH_ABSENT, **kwargs):
        super().__init__(**kwargs)
        self.backend_health = backend_health
        self.snapshot_calls = 0
        self.turn_calls = 0
        self.pending_calls = 0

    def snapshot(self):
        self.snapshot_calls += 1
        payload = super().snapshot()
        if self.backend_health is not _BACKEND_HEALTH_ABSENT:
            payload["backend_health"] = self.backend_health
        return payload

    def turns(self):
        self.turn_calls += 1
        return super().turns()

    def pending(self):
        self.pending_calls += 1
        return super().pending()


@pytest.mark.parametrize(
    ("stable_key", "version"),
    [
        ("K1", 1),
        ("wsk1_" + "a" * 63, 1),
        ("wsk1_" + "a" * 65, 1),
        ("wsk1_" + "A" * 64, 1),
        ("wsk1_" + "g" * 64, 1),
        ("wsk2_" + "a" * 64, 1),
        (KEY_A + "\nterminal_id=spoof", 1),
        ({"stable_key": KEY_A, "terminal_id": "spoof"}, 1),
        (KEY_A, None),
        (KEY_A, 2),
        (KEY_A, "1"),
        (KEY_A, True),
        (None, 1),
    ],
)
def test_malformed_or_unknown_pairs_are_quarantined_and_never_persisted(stable_key, version):
    store = _store()
    worker = _worker("claude-2", stable_key, version=version)

    assert state.worker_stable_identity(worker) is None
    assert state.worker_stable_identity_class(worker) != "current_v1"
    key, entry, created = state.upsert_worker_entry(store, worker, topic_id="26")

    assert created is True
    assert "tendwire_stable_key" not in entry
    assert "tendwire_stable_key_version" not in entry
    assert "topic_id" not in entry
    assert state.entry_is_quarantined(entry) is True
    assert state.find_worker_entry_by_id(store, "claude-2") == (None, None)
    assert state.find_entry_by_thread(store, "26") == (None, None)
    assert _worker_entry_for_turn(store, "claude-2", "w1") == (None, None)
    assert key in state.source_worker_entries(store)


@pytest.mark.parametrize(
    "worker",
    [
        _worker("claude-2"),
        _worker("claude-2", "source-spoof", version=1),
    ],
    ids=["missing", "malformed"],
)
@pytest.mark.parametrize("topic_mode", ["worker", "space"])
def test_fresh_invalid_or_missing_snapshot_never_routes_creates_topics_or_delivers_and_repeats_idempotently(
    monkeypatch, topic_mode, worker
):
    monkeypatch.setenv("HERDRES_SOURCE_TOPIC_MODE", topic_mode)
    store = _store()
    turns = {"turns": [_final_turn("claude-2")]}
    first_telegram = FakeTelegram()

    first = sync_once(
        store,
        SyncRuntime(
            FakeTendwire(workers=[worker], turns=turns, stable_identities=False),
            first_telegram,
            with_outbox=False,
        ),
    )
    entries_before = deepcopy(state.source_worker_entries(store))
    second_telegram = FakeTelegram()
    second = sync_once(
        store,
        SyncRuntime(
            FakeTendwire(workers=[worker], turns=turns, stable_identities=False),
            second_telegram,
            with_outbox=False,
        ),
    )

    entries = state.source_worker_entries(store)
    assert len(entries) == 1
    assert entries == entries_before
    assert all(state.entry_is_quarantined(entry) for entry in entries.values())
    assert all("topic_id" not in entry for entry in entries.values())
    assert first["feed_sent"] == second["feed_sent"] == 0
    assert first_telegram.topics == second_telegram.topics == []
    assert first_telegram.sent == second_telegram.sent == []
    assert state.find_worker_entry_by_id(store, "claude-2") == (None, None)
    assert _worker_entry_for_turn(store, "claude-2", "w1") == (None, None)


def test_exact_v1_pair_is_centralized_and_stored_values_are_validated():
    assert state.valid_stable_worker_key_pair(KEY_A, 1) is True
    assert state.valid_stable_worker_key_pair(KEY_A, True) is False
    store = _store()
    key, entry, _created = state.upsert_worker_entry(store, _worker("claude-2", KEY_A))
    assert entry["tendwire_stable_key"] == KEY_A
    assert entry["tendwire_stable_key_version"] == 1
    assert state.find_entry_key_by_stable_key(store, KEY_A) == key

    entry["tendwire_stable_key_version"] = "1"
    assert state.entry_stable_identity(entry) is None
    assert state.find_entry_key_by_stable_key(store, KEY_A) is None


def test_protocol_safety_cannot_be_disabled_by_legacy_flag(monkeypatch):
    monkeypatch.setenv("HERDRES_STABLE_WORKER_KEY", "0")
    store = _store()
    key, _entry, _created = state.upsert_worker_entry(store, _worker("claude-2", KEY_A), topic_id="26")

    rebound_key, rebound, created = state.upsert_worker_entry(
        store,
        _worker("claude-2-2", KEY_A, agent="codex", space="renamed-space"),
    )

    assert created is False
    assert rebound_key == key
    assert rebound["topic_id"] == "26"


def test_unique_v1_handle_is_authoritative_across_restart_and_presentation_changes():
    store = _store()
    key, entry, _created = state.upsert_worker_entry(
        store,
        _worker("claude-2", KEY_A, fingerprint="fp-old"),
        topic_id="26",
    )
    entry.update(
        {
            "last_clean_message_id": "500",
            "last_turn_id": "turn-old",
            "last_stream_message_id": "501",
            "last_stream_turn_id": "turn-live",
            "pinned_status_message_id": "600",
            "voice_reply_message_ids": ["700"],
        }
    )
    state.bind_message_to_worker(store, "500", entry, topic_id="26", kind="final", turn_id="turn-old")
    state.mark_delivered(store, "final:turn-old:hash", {"worker_id": "claude-2", "turn_id": "turn-old"})
    ledger_before = deepcopy(state.delivered_turns(store))

    rebound_key, rebound, created = state.upsert_worker_entry(
        store,
        _worker(
            "claude-2-2",
            KEY_A,
            agent="codex",
            space="w2",
            fingerprint="fp-new",
            cwd="/another/project",
        ),
    )

    assert created is False
    assert rebound_key == key
    assert len(state.source_worker_entries(store)) == 1
    assert rebound["topic_id"] == "26"
    assert rebound["last_clean_message_id"] == "500"
    assert rebound["last_stream_message_id"] == "501"
    assert rebound["pinned_status_message_id"] == "600"
    assert rebound["voice_reply_message_ids"] == ["700"]
    assert state.delivered_turns(store) == ledger_before
    binding = state.find_message_binding(store, "500", topic_id="26")
    assert binding == {
        "topic_id": "26",
        "worker_id": "claude-2-2",
        "worker_fingerprint": "fp-new",
        "space_id": "w2",
        "kind": "final",
        "turn_id": "turn-old",
        "bot_kind": "",
        "stable_key": KEY_A,
        "stable_key_version": 1,
    }


def test_fresh_absent_identity_is_quarantined_and_repeated_upsert_is_idempotent():
    store = _store()
    first_key, first, first_created = state.upsert_worker_entry(
        store, _worker("claude-2"), topic_id="26"
    )
    repeated_key, repeated, repeated_created = state.upsert_worker_entry(
        store, _worker("claude-2")
    )

    assert first_created is True
    assert repeated_created is False
    assert repeated_key == first_key
    assert repeated is first
    assert len(state.source_worker_entries(store)) == 1
    assert "topic_id" not in repeated
    assert state.entry_is_quarantined(repeated) is True
    assert state.find_worker_entry_by_id(store, "claude-2") == (None, None)


def test_private_missing_version_planner_and_mutator_preserve_state_and_are_idempotent():
    store = _store()
    key, entry = _persisted_missing_version_worker(
        store,
        "claude-2",
        KEY_A,
        fingerprint="fp-old",
        topic_id="26",
    )
    entry.update(
        {
            "last_clean_message_id": "500",
            "last_clean_message_ids": ["500", "502"],
            "last_turn_id": "turn-old",
            "last_stream_message_id": "501",
            "last_stream_turn_id": "turn-live",
            "pinned_status_message_id": "600",
            "managed_account_id": "claude-primary",
            "bootstrap_placeholder": True,
            "future_private_field": {"keep": ["exactly", 1]},
        }
    )
    state.bind_message_to_worker(
        store, "500", entry, topic_id="26", kind="final", turn_id="turn-old"
    )
    state.bind_message_to_worker(
        store, "501", entry, topic_id="26", kind="working", turn_id="turn-live"
    )
    state.message_bindings(store)["shared-unrelated"] = {
        "topic_id": "26",
        "worker_id": "unrelated-worker",
        "worker_fingerprint": "unrelated-fingerprint",
        "space_id": "w1",
        "kind": "final",
        "turn_id": "unrelated-history",
    }
    state.mark_delivered(
        store,
        "final:turn-old:hash",
        {"worker_id": "claude-2", "turn_id": "turn-old"},
    )
    store["tendwired_bootstrap_complete"] = True
    ledger_before = deepcopy(state.delivered_turns(store))
    incoming = _worker("claude-2", KEY_A, fingerprint="fp-current")

    plan = state.plan_persisted_stable_key_migrations(store, [incoming])
    assert plan.blocked_stable_keys == frozenset()
    assert len(plan.migrations) == 1
    assert plan.migrations[0].action == "adopt"
    assert plan.migrations[0].candidate_key == key
    assert plan.migrations[0].compatible_binding_ids == ("500", "501")
    assert plan.migrations[0].quarantine_binding_ids == ()
    adopted = state.apply_persisted_stable_key_migration_plan(
        store, [incoming], plan
    )

    assert adopted == {id(incoming): key}
    assert entry["topic_id"] == "26"
    assert entry["last_clean_message_ids"] == ["500", "502"]
    assert entry["last_stream_message_id"] == "501"
    assert entry["pinned_status_message_id"] == "600"
    assert entry["managed_account_id"] == "claude-primary"
    assert entry["future_private_field"] == {"keep": ["exactly", 1]}
    assert entry["bootstrap_placeholder"] is True
    assert store["tendwired_bootstrap_complete"] is True
    assert state.delivered_turns(store) == ledger_before
    assert entry["tendwire_stable_key"] == KEY_A
    assert entry["tendwire_stable_key_version"] == 1
    assert entry["tendwire_fingerprint"] == "fp-current"
    for message_id in ("500", "501"):
        binding = state.find_message_binding(store, message_id, topic_id="26")
        assert binding["worker_id"] == "claude-2"
        assert binding["worker_fingerprint"] == "fp-current"
        assert binding["space_id"] == "w1"
        assert binding["stable_key"] == KEY_A
        assert binding["stable_key_version"] == 1
    unrelated = state.message_bindings(store)["shared-unrelated"]
    assert unrelated["worker_id"] == "unrelated-worker"
    assert "stable_key" not in unrelated
    assert "routing_quarantined" not in unrelated

    stable_snapshot = deepcopy(store)
    repeated_plan = state.plan_persisted_stable_key_migrations(
        store, [incoming]
    )
    assert repeated_plan.migrations == ()
    assert state.apply_persisted_stable_key_migration_plan(
        store, [incoming], repeated_plan
    ) == {}
    assert store == stable_snapshot


def test_production_shaped_missing_version_migration_is_order_reload_and_delivery_safe():
    def run(*, reverse: bool):
        store = _store()
        adopt_key, adoptable = _persisted_missing_version_worker(
            store,
            "claude-2",
            KEY_A,
            topic_id="26",
            fingerprint="fp-old-a",
        )
        adoptable.update(
            {
                "pinned_status_message_id": "600",
                "managed_account_id": "claude-primary",
                "last_clean_message_ids": ["500"],
                "last_stream_message_id": "501",
                "last_stream_turn_id": "turn-working",
                "delivery_private": {"attempt": 7, "receipt": "kept"},
            }
        )
        state.bind_message_to_worker(
            store,
            "500",
            adoptable,
            topic_id="26",
            kind="final",
            turn_id="turn-old",
        )
        state.bind_message_to_worker(
            store,
            "501",
            adoptable,
            topic_id="26",
            kind="working",
            turn_id="turn-working",
        )
        state.bind_message_to_worker(
            store,
            "502",
            adoptable,
            topic_id="99",
            kind="final",
            turn_id="wrong-topic",
        )
        stale_a = deepcopy(adoptable)
        stale_a.update(
            {
                "tendwire_worker_id": "stale-a",
                "worker_id": "stale-a",
                "tendwire_fingerprint": "fp-stale-a",
                "topic_id": "90",
                "tendwire_stable_key_version": "1",
            }
        )
        store["panes"]["persisted:stale-a"] = stale_a

        no_claimant_key, no_claimant = _persisted_missing_version_worker(
            store,
            "claude-3",
            KEY_B,
            topic_id="28",
            fingerprint="fp-old-b",
        )
        collision_key, collision_a = _persisted_missing_version_worker(
            store,
            "claude-4",
            KEY_C,
            topic_id="30",
            fingerprint="fp-old-c1",
        )
        collision_b = deepcopy(collision_a)
        collision_b["tendwire_fingerprint"] = "fp-old-c2"
        collision_b["topic_id"] = "31"
        store["panes"]["persisted:collision-c2"] = collision_b
        store["telegram_message_bindings"]["900"] = {
            "topic_id": "30",
            "worker_id": "other-worker",
            "worker_fingerprint": "other-fingerprint",
            "space_id": "w1",
            "stable_key": KEY_C,
            "kind": "final",
            "turn_id": "conflicting-reply",
        }
        store["tendwired_bootstrap_complete"] = True
        state.mark_delivered(
            store,
            "final:turn-old:existing",
            {"worker_id": "claude-2", "turn_id": "turn-old"},
        )
        ledger_before = deepcopy(state.delivered_turns(store))
        workers = [
            _worker("claude-2", KEY_A, fingerprint="fp-current-a"),
            _worker("claude-4", KEY_C, fingerprint="fp-current-c"),
        ]
        if reverse:
            store["panes"] = dict(reversed(list(store["panes"].items())))
            store["telegram_message_bindings"] = dict(
                reversed(list(store["telegram_message_bindings"].items()))
            )
            workers.reverse()

        plan = state.plan_persisted_stable_key_migrations(store, workers)
        decisions = {
            migration.stable_key: (
                migration.action,
                migration.reason,
                migration.candidate_key,
            )
            for migration in plan.migrations
        }
        assert decisions == {
            KEY_A: ("adopt", "", adopt_key),
            KEY_B: ("wait", "no_current_claimant", None),
            KEY_C: ("block", "multiple_persisted_candidates", None),
        }
        assert plan.blocked_stable_keys == frozenset({KEY_C})

        turns = {"turns": []}
        first_telegram = FakeTelegram()
        first = sync_once(
            store,
            SyncRuntime(
                FakeTendwire(workers=workers, turns=turns),
                first_telegram,
                with_outbox=False,
            ),
        )

        entries = state.source_worker_entries(store)
        migrated = entries[adopt_key]
        assert migrated["topic_id"] == "26"
        assert migrated["tendwire_stable_key_version"] == 1
        assert migrated["tendwire_fingerprint"] == "fp-current-a"
        assert migrated["pinned_status_message_id"] == "600"
        assert migrated["managed_account_id"] == "claude-primary"
        assert migrated["delivery_private"] == {
            "attempt": 7,
            "receipt": "kept",
        }
        assert state.entry_is_quarantined(entries["persisted:stale-a"])
        assert "tendwire_stable_key_version" not in entries[no_claimant_key]
        assert not state.entry_is_quarantined(entries[no_claimant_key])
        assert state.entry_is_quarantined(entries[collision_key])
        assert state.entry_is_quarantined(
            entries["persisted:collision-c2"]
        )
        compatible_final = state.message_bindings(store)["500"]
        compatible_working = state.message_bindings(store)["501"]
        for binding in (compatible_final, compatible_working):
            assert binding["worker_id"] == "claude-2"
            assert binding["worker_fingerprint"] == "fp-current-a"
            assert binding["stable_key"] == KEY_A
            assert binding["stable_key_version"] == 1
            assert "routing_quarantined" not in binding
        assert compatible_working["kind"] == "working"
        assert state.message_bindings(store)["502"][
            "routing_quarantined"
        ] is True
        assert state.message_bindings(store)["900"][
            "routing_quarantined"
        ] is True
        assert state.delivered_turns(store) == ledger_before
        assert first["feed_sent"] == 0
        assert first_telegram.sent == []
        assert first_telegram.topics == []

        first_bytes = json.dumps(
            store, sort_keys=True, separators=(",", ":")
        )
        reloaded = json.loads(first_bytes)
        second_telegram = FakeTelegram()
        second = sync_once(
            reloaded,
            SyncRuntime(
                FakeTendwire(workers=workers, turns=turns),
                second_telegram,
                with_outbox=False,
            ),
        )
        second_bytes = json.dumps(
            reloaded, sort_keys=True, separators=(",", ":")
        )
        assert second_bytes == first_bytes
        assert second["feed_sent"] == 0
        assert second_telegram.sent == []
        assert second_telegram.topics == []
        normalized = deepcopy(reloaded)
        normalized.get("telegram", {}).get(
            "forum_topic_icons", {}
        ).pop("fetched_at", None)
        return json.dumps(
            normalized, sort_keys=True, separators=(",", ":")
        )

    assert run(reverse=False) == run(reverse=True)


def test_blocked_shared_topic_migration_preserves_unrelated_binding_history():
    store = _store()
    _candidate_key, candidate = _persisted_missing_version_worker(
        store,
        "claude-2",
        KEY_A,
        topic_id="26",
        fingerprint="fp-old-1",
    )
    duplicate = deepcopy(candidate)
    duplicate["tendwire_fingerprint"] = "fp-old-2"
    store["panes"]["persisted:duplicate"] = duplicate
    bindings = state.message_bindings(store)
    bindings["candidate-owner"] = {
        "topic_id": "26",
        "worker_id": "claude-2",
        "worker_fingerprint": "fp-old-1",
        "space_id": "w1",
        "kind": "final",
    }
    bindings["current-owner"] = {
        "topic_id": "26",
        "worker_id": "claude-2",
        "worker_fingerprint": "fp-current",
        "space_id": "w1",
        "kind": "working",
    }
    bindings["exact-key-owner"] = {
        "topic_id": "99",
        "worker_id": "unrelated-exact-key",
        "worker_fingerprint": "fp-unrelated-exact-key",
        "space_id": "other-space",
        "stable_key": KEY_A,
        "stable_key_version": 1,
        "kind": "final",
    }
    unrelated_ids = []
    for index in range(250):
        message_id = f"shared-{index:03d}"
        unrelated_ids.append(message_id)
        bindings[message_id] = {
            "topic_id": "26",
            "worker_id": f"unrelated-worker-{index}",
            "worker_fingerprint": f"unrelated-fingerprint-{index}",
            "space_id": "w1",
            "kind": "final",
        }
    incoming = _worker(
        "claude-2", KEY_A, fingerprint="fp-current"
    )

    plan = state.plan_persisted_stable_key_migrations(store, [incoming])
    assert plan.migrations[0].reason == "multiple_persisted_candidates"
    assert plan.migrations[0].quarantine_binding_ids == (
        "candidate-owner",
        "current-owner",
        "exact-key-owner",
    )

    copied = deepcopy(store)
    binding_count = len(state.message_bindings(copied))
    state.apply_persisted_stable_key_migration_plan(
        copied, [incoming], plan
    )

    copied_bindings = state.message_bindings(copied)
    assert len(copied_bindings) == binding_count
    for message_id in unrelated_ids:
        assert "routing_quarantined" not in copied_bindings[message_id]
    for message_id in (
        "candidate-owner",
        "current-owner",
        "exact-key-owner",
    ):
        assert copied_bindings[message_id]["routing_quarantined"] is True


def test_private_migration_has_no_worker_id_fallback_and_revalidates_before_apply():
    store = _store()
    candidate_key, candidate = _persisted_missing_version_worker(
        store,
        "claude-2",
        KEY_B,
        topic_id="26",
        fingerprint="fp-old",
    )
    incoming = _worker(
        "claude-2", KEY_A, fingerprint="fp-current"
    )

    plan = state.plan_persisted_stable_key_migrations(store, [incoming])
    assert plan.migrations[0].action == "wait"
    assert state.apply_persisted_stable_key_migration_plan(
        store, [incoming], plan
    ) == {}
    reservations = state.precompute_worker_entry_reservations(
        store, [incoming]
    )
    assert reservations[id(incoming)] is None
    assert "tendwire_stable_key_version" not in candidate

    exact = _worker("claude-2", KEY_B, fingerprint="fp-current")
    exact_plan = state.plan_persisted_stable_key_migrations(
        store, [exact]
    )
    assert exact_plan.migrations[0].candidate_key == candidate_key
    candidate["agent"] = "codex"
    before = deepcopy(store)
    assert state.apply_persisted_stable_key_migration_plan(
        store, [exact], exact_plan
    ) == {}
    assert store == before


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("source", "legacy"),
        ("entry_type", "space"),
        ("tendwire_worker_id", "other-worker"),
        ("worker_id", "other-worker"),
        ("tendwire_space_id", "w2"),
        ("space_id", "w2"),
        ("agent", "codex"),
        ("tendwire_raw_status", "closed"),
        ("stable_key_quarantined", False),
    ],
)
def test_private_candidate_requires_exact_live_source_worker_compatibility(
    field, value
):
    store = _store()
    _key, candidate = _persisted_missing_version_worker(
        store,
        "claude-2",
        KEY_A,
        topic_id="26",
        fingerprint="fp-old",
    )
    candidate[field] = value
    incoming = _worker("claude-2", KEY_A, fingerprint="fp-current")

    plan = state.plan_persisted_stable_key_migrations(store, [incoming])

    assert plan.blocked_stable_keys == frozenset({KEY_A})
    assert plan.migrations[0].action == "block"
    assert plan.migrations[0].reason == "incompatible_persisted_candidate"


def test_private_candidate_requires_one_live_topic_owner_but_ignores_closed_history():
    store = _store()
    candidate_key, candidate = _persisted_missing_version_worker(
        store,
        "claude-2",
        KEY_A,
        topic_id="26",
        fingerprint="fp-old",
    )
    closed_history = deepcopy(candidate)
    closed_history.update(
        {
            "tendwire_stable_key": KEY_B,
            "tendwire_stable_key_version": 1,
            "tendwire_raw_status": "closed",
        }
    )
    store["panes"]["closed:topic-owner"] = closed_history
    state.message_bindings(store)["700"] = {
        "topic_id": "26",
        "worker_id": "claude-2",
        "worker_fingerprint": closed_history["tendwire_fingerprint"],
        "space_id": "w1",
        "stable_key": KEY_B,
        "stable_key_version": 1,
        "kind": "final",
        "turn_id": "closed-history",
    }
    state.message_bindings(store)["701"] = {
        "topic_id": "26",
        "worker_id": "claude-2",
        "worker_fingerprint": "fp-old",
        "space_id": "w1",
        "kind": "working",
        "turn_id": "candidate-working",
    }
    incoming = _worker(
        "claude-2", KEY_A, fingerprint="fp-current"
    )

    safe = state.plan_persisted_stable_key_migrations(store, [incoming])
    assert safe.migrations[0].action == "adopt"
    assert safe.migrations[0].candidate_key == candidate_key
    assert safe.migrations[0].stale_entry_keys == (
        "closed:topic-owner",
    )
    assert safe.migrations[0].quarantine_binding_ids == ("700",)
    assert safe.migrations[0].compatible_binding_ids == ("701",)

    live_owner = deepcopy(closed_history)
    live_owner["tendwire_raw_status"] = "working"
    store["panes"]["live:topic-owner"] = live_owner
    blocked = state.plan_persisted_stable_key_migrations(store, [incoming])
    assert blocked.migrations[0].action == "block"
    assert blocked.migrations[0].reason == "ambiguous_topic_owner"
    del store["panes"]["live:topic-owner"]
    assert state.apply_persisted_stable_key_migration_plan(
        store, [incoming], safe
    ) == {id(incoming): candidate_key}
    assert state.entry_is_quarantined(closed_history)
    assert state.message_bindings(store)["700"]["routing_quarantined"] is True
    compatible = state.message_bindings(store)["701"]
    assert compatible["worker_fingerprint"] == "fp-current"
    assert compatible["stable_key"] == KEY_A
    assert compatible["stable_key_version"] == 1
    assert "routing_quarantined" not in compatible


def test_private_candidate_rejects_existing_v1_owner_and_multiple_current_claimants():
    store = _store()
    _candidate_key, candidate = _persisted_missing_version_worker(
        store, "claude-2", KEY_A, topic_id="26"
    )
    exact_owner = deepcopy(candidate)
    exact_owner.update(
        {
            "tendwire_worker_id": "other-worker",
            "worker_id": "other-worker",
            "tendwire_fingerprint": "fp-other",
            "topic_id": "28",
            "tendwire_stable_key_version": 1,
        }
    )
    store["panes"]["exact:v1-owner"] = exact_owner
    incoming = _worker("claude-2", KEY_A)

    exact_owner_plan = state.plan_persisted_stable_key_migrations(
        store, [incoming]
    )
    assert exact_owner_plan.migrations[0].reason == "existing_exact_v1_owner"

    del store["panes"]["exact:v1-owner"]
    multiple_plan = state.plan_persisted_stable_key_migrations(
        store,
        [
            incoming,
            _worker("other-worker", KEY_A, fingerprint="fp-other"),
        ],
    )
    assert multiple_plan.migrations[0].reason == "multiple_current_claimants"
    assert multiple_plan.blocked_stable_keys == frozenset({KEY_A})


def test_conflicting_reply_owner_blocks_and_is_quarantined_without_adoption():
    store = _store()
    candidate_key, candidate = _persisted_missing_version_worker(
        store,
        "claude-2",
        KEY_A,
        topic_id="26",
        fingerprint="fp-old",
    )
    state.message_bindings(store)["900"] = {
        "topic_id": "26",
        "worker_id": "other-worker",
        "worker_fingerprint": "fp-other",
        "space_id": "w1",
        "stable_key": KEY_A,
        "kind": "final",
    }
    incoming = _worker("claude-2", KEY_A, fingerprint="fp-current")

    plan = state.plan_persisted_stable_key_migrations(store, [incoming])
    assert plan.migrations[0].reason == "conflicting_binding_owner"
    assert state.apply_persisted_stable_key_migration_plan(
        store, [incoming], plan
    ) == {}

    assert state.entry_is_quarantined(
        state.source_worker_entries(store)[candidate_key]
    )
    assert "tendwire_stable_key_version" not in candidate
    assert state.message_bindings(store)["900"]["routing_quarantined"] is True


def test_candidate_topic_binding_with_different_exact_v1_key_blocks_adoption():
    store = _store()
    candidate_key, candidate = _persisted_missing_version_worker(
        store,
        "claude-2",
        KEY_A,
        topic_id="26",
        fingerprint="fp-old",
    )
    state.message_bindings(store)["901"] = {
        "topic_id": "26",
        "worker_id": "claude-2",
        "worker_fingerprint": "fp-old",
        "space_id": "w1",
        "stable_key": KEY_B,
        "stable_key_version": 1,
        "kind": "working",
        "turn_id": "different-key",
    }
    incoming = _worker("claude-2", KEY_A, fingerprint="fp-current")

    plan = state.plan_persisted_stable_key_migrations(store, [incoming])
    assert plan.migrations[0].reason == "conflicting_binding_owner"
    assert state.apply_persisted_stable_key_migration_plan(
        store, [incoming], plan
    ) == {}

    assert state.entry_is_quarantined(
        state.source_worker_entries(store)[candidate_key]
    )
    assert "tendwire_stable_key_version" not in candidate
    assert state.message_bindings(store)["901"]["stable_key"] == KEY_B
    assert state.message_bindings(store)["901"]["stable_key_version"] == 1
    assert state.message_bindings(store)["901"]["routing_quarantined"] is True


@pytest.mark.parametrize(
    "persisted_version",
    [None, "1", 2, True],
)
def test_only_absent_persisted_version_field_is_a_private_candidate(
    persisted_version,
):
    store = _store()
    _key, entry = _persisted_missing_version_worker(
        store, "claude-2", KEY_A, topic_id="26"
    )
    entry["tendwire_stable_key_version"] = persisted_version
    incoming = _worker("claude-2", KEY_A)

    plan = state.plan_persisted_stable_key_migrations(store, [incoming])

    assert plan.migrations == ()
    assert state.worker_stable_identity(incoming) == (KEY_A, 1)


def test_legacy_migration_requires_unambiguous_live_same_worker_and_sanity():
    store = _store()
    old_key, old = _persisted_legacy_worker(
        store, "claude-2", legacy_value=LEGACY_KEY, topic_id="26"
    )

    new_key, current, created = state.upsert_worker_entry(
        store,
        _worker("claude-2", KEY_A, agent="codex"),
    )

    assert created is True
    assert new_key != old_key
    assert state.entry_is_quarantined(old) is True
    assert old["topic_id"] == "26"
    assert current["tendwire_stable_key"] == KEY_A


def test_valid_migration_and_restart_never_create_a_duplicate_topic():
    store = _store()
    _key, first_entry = _persisted_missing_version_worker(
        store, "claude-2", KEY_A, topic_id="26"
    )
    topic_id = first_entry["topic_id"]

    migration_telegram = FakeTelegram()
    sync_once(
        store,
        SyncRuntime(FakeTendwire(workers=[_worker("claude-2", KEY_A)]), migration_telegram, with_outbox=False),
    )
    restart_telegram = FakeTelegram()
    sync_once(
        store,
        SyncRuntime(FakeTendwire(workers=[_worker("claude-2-2", KEY_A)]), restart_telegram, with_outbox=False),
    )

    assert len(state.source_worker_entries(store)) == 1
    entry = next(iter(state.source_worker_entries(store).values()))
    assert entry["topic_id"] == topic_id
    assert entry["tendwire_worker_id"] == "claude-2-2"
    assert migration_telegram.topics == []
    assert restart_telegram.topics == []


def test_restart_repoints_all_constrained_historical_bindings_and_replies():
    store = _store()
    key, entry, _created = state.upsert_worker_entry(
        store,
        _worker("claude-2", KEY_A, fingerprint="fp-old"),
        topic_id="26",
    )
    state.bind_message_to_worker(store, "500", entry, topic_id="26", kind="final", turn_id="turn-old")
    state.bind_message_to_worker(store, "501", entry, topic_id="26", kind="working", turn_id="turn-live")
    store["telegram_message_bindings"]["999"] = {
        "topic_id": "99",
        "worker_id": "claude-2",
        "worker_fingerprint": "different-pane",
        "space_id": "w1",
        "kind": "final",
    }

    rebound_key, rebound, _created = state.upsert_worker_entry(
        store,
        _worker("claude-2-2", KEY_A, fingerprint="fp-new"),
    )

    assert rebound_key == key
    for message_id in ("500", "501"):
        binding = store["telegram_message_bindings"][message_id]
        assert binding["worker_id"] == "claude-2-2"
        assert binding["worker_fingerprint"] == "fp-new"
        assert binding["stable_key"] == KEY_A
        assert binding["stable_key_version"] == 1
        reply_key, reply_entry = herdres._worker_entry_from_reply(
            store,
            {"reply_to_message_id": message_id, "topic_id": "26"},
        )
        assert reply_key == key
        assert reply_entry is rebound
    assert store["telegram_message_bindings"]["999"]["worker_id"] == "claude-2"


def test_restart_replays_zero_old_turns_and_preserves_ledger_and_message():
    store = _store()
    first_telegram = FakeTelegram()
    first_turns = {"turns": [_final_turn("claude-2")]}
    first = sync_once(
        store,
        SyncRuntime(
            FakeTendwire(workers=[_worker("claude-2", KEY_A)], turns=first_turns),
            first_telegram,
            with_outbox=False,
        ),
    )
    ledger_before = deepcopy(state.delivered_turns(store))
    final_message_id = next(sent[3] for sent in first_telegram.sent if "Full final answer" in sent[1])

    second_telegram = FakeTelegram()
    second_turns = {"turns": [_final_turn("claude-2-2")]}
    second = sync_once(
        store,
        SyncRuntime(
            FakeTendwire(workers=[_worker("claude-2-2", KEY_A)], turns=second_turns),
            second_telegram,
            with_outbox=False,
        ),
    )

    assert first["feed_sent"] == 1
    assert second["feed_sent"] == 0
    assert second_telegram.sent == []
    assert second_telegram.topics == []
    assert state.delivered_turns(store) == ledger_before
    binding = state.find_message_binding(store, final_message_id)
    assert binding["worker_id"] == "claude-2-2"
    assert binding["stable_key"] == KEY_A


@pytest.mark.parametrize("topic_mode", ["worker", "space"])
def test_fresh_duplicate_key_claimants_are_all_quarantined_without_routing_or_topics(
    monkeypatch, topic_mode
):
    monkeypatch.setenv("HERDRES_SOURCE_TOPIC_MODE", topic_mode)
    store = _store()
    workers = [_worker("claude-2", KEY_A), _worker("claude-3", KEY_A)]
    turns = {"turns": [_final_turn("claude-2"), _final_turn("claude-3", turn_id="turn-2")]}
    first_telegram = FakeTelegram()

    assert state.blocked_worker_stable_keys(store, workers) == {KEY_A}
    first = sync_once(
        store,
        SyncRuntime(FakeTendwire(workers=workers, turns=turns), first_telegram, with_outbox=False),
    )
    entries_before = deepcopy(state.source_worker_entries(store))
    second_telegram = FakeTelegram()
    second = sync_once(
        store,
        SyncRuntime(FakeTendwire(workers=list(reversed(workers)), turns=turns), second_telegram, with_outbox=False),
    )

    entries = state.source_worker_entries(store)
    assert entries == entries_before
    assert len(entries) == 2
    assert {entry["tendwire_worker_id"] for entry in entries.values()} == {"claude-2", "claude-3"}
    assert all(state.entry_is_quarantined(entry) for entry in entries.values())
    assert all("topic_id" not in entry for entry in entries.values())
    assert state.find_entry_key_by_stable_key(store, KEY_A) is None
    for worker_id in ("claude-2", "claude-3"):
        assert state.find_worker_entry_by_id(store, worker_id) == (None, None)
        assert _worker_entry_for_turn(store, worker_id, "w1") == (None, None)
    assert first["feed_sent"] == second["feed_sent"] == 0
    assert first_telegram.topics == second_telegram.topics == []
    assert first_telegram.sent == second_telegram.sent == []


def test_persisted_collision_quarantines_old_claimants_and_creates_distinct_current_entry():
    store = _store()
    key_a, entry_a, _created = state.upsert_worker_entry(
        store, _worker("claude-2", KEY_B), topic_id="26"
    )
    key_b, entry_b, _created = state.upsert_worker_entry(
        store, _worker("claude-3", KEY_C), topic_id="28"
    )
    for entry in (entry_a, entry_b):
        entry["tendwire_stable_key"] = KEY_A
        entry["tendwire_stable_key_version"] = 1
    telegram = FakeTelegram()

    sync_once(
        store,
        SyncRuntime(FakeTendwire(workers=[_worker("claude-9", KEY_A)]), telegram, with_outbox=False),
    )

    entries = state.source_worker_entries(store)
    assert len(entries) == 3
    assert state.entry_is_quarantined(entries[key_a]) is True
    assert state.entry_is_quarantined(entries[key_b]) is True
    assert state.find_worker_entry_by_id(store, "claude-2") == (None, None)
    assert state.entry_is_quarantined(entries[key_a]) is True
    assert state.entry_is_quarantined(entries[key_b]) is True
    assert all(state.entry_is_quarantined(entry) for entry in entries.values())
    assert state.find_worker_entry_by_id(store, "claude-9") == (None, None)
    assert state.find_entry_key_by_stable_key(store, KEY_A) is None
    assert telegram.topics == []


def test_closed_key_reuse_never_adopts_or_routes_the_closed_entry():
    store = _store()
    old_key, old, _created = state.upsert_worker_entry(
        store,
        _worker("claude-2", KEY_A, status="closed", fingerprint="fp-old"),
        topic_id="26",
    )
    state.bind_message_to_worker(store, "500", old, topic_id="26", kind="final", turn_id="turn-old")

    current_key, current, created = state.upsert_worker_entry(
        store,
        _worker("claude-9", KEY_A, fingerprint="fp-new"),
    )

    assert created is True
    assert current_key != old_key
    assert current.get("topic_id") is None
    assert old["topic_id"] == "26"
    assert state.entry_is_quarantined(old) is True
    assert state.find_entry_by_thread(store, "26") == (None, None)
    assert herdres._worker_entry_from_reply(
        store,
        {"reply_to_message_id": "500", "topic_id": "26"},
    ) == (None, None)


def test_sync_planner_never_reopens_closed_same_id_stable_owner():
    store = _store()
    old_key, old, _created = state.upsert_worker_entry(
        store,
        _worker(
            "worker-1",
            KEY_A,
            status="closed",
            fingerprint="fp-closed",
        ),
        topic_id="26",
    )
    telegram = FakeTelegram()

    sync_once(
        store,
        SyncRuntime(
            FakeTendwire(
                workers=[
                    _worker(
                        "worker-1",
                        KEY_A,
                        status="working",
                        fingerprint="fp-current",
                    )
                ],
                turns={"turns": []},
            ),
            telegram,
            with_outbox=False,
        ),
    )

    current_key, current = state.find_worker_entry_by_id(store, "worker-1")
    assert current_key != old_key
    assert current is not old
    assert old["topic_id"] == "26"
    assert state.entry_is_quarantined(old) is True
    assert current["topic_id"] != "26"
    assert telegram.topics == ["telegram-bot 2"]


@pytest.mark.parametrize(
    "incoming",
    [
        _worker("claude-2", fingerprint="fp-new"),
        _worker("claude-2", "source-spoof", version=1, fingerprint="fp-new"),
    ],
)
def test_rotation_loss_or_invalid_replacement_quarantines_old_binding(incoming):
    store = _store()
    old_key, old, _created = state.upsert_worker_entry(
        store,
        _worker("claude-2", KEY_A, fingerprint="fp-old"),
        topic_id="26",
    )
    state.bind_message_to_worker(store, "500", old, topic_id="26", kind="final", turn_id="turn-old")

    current_key, current, created = state.upsert_worker_entry(store, incoming)
    repeated_key, repeated, repeated_created = state.upsert_worker_entry(store, incoming)

    assert created is False
    assert repeated_created is False
    assert current_key == repeated_key == old_key
    assert current is repeated is old
    assert len(state.source_worker_entries(store)) == 1
    assert old["topic_id"] == "26"
    assert state.entry_is_quarantined(old) is True
    assert state.find_worker_entry_by_id(store, "claude-2") == (None, None)
    assert store["telegram_message_bindings"]["500"]["routing_quarantined"] is True
    assert herdres._worker_entry_from_reply(
        store,
        {"reply_to_message_id": "500", "topic_id": "26"},
    ) == (None, None)


@pytest.mark.parametrize(
    "faulty_worker",
    [
        _worker("claude-2", fingerprint="fp-old"),
        _worker("claude-2", "source-spoof", version=1, fingerprint="fp-old"),
    ],
)
def test_faulty_snapshot_after_valid_state_reuses_and_quarantines_without_duplicate_topic(
    faulty_worker,
):
    store = _store()
    first_telegram = FakeTelegram()
    sync_once(
        store,
        SyncRuntime(
            FakeTendwire(workers=[_worker("claude-2", KEY_A, fingerprint="fp-old")]),
            first_telegram,
            with_outbox=False,
        ),
    )
    first_entries = state.source_worker_entries(store)
    assert len(first_entries) == 1
    original_key = next(iter(first_entries))
    original_topic = first_entries[original_key]["topic_id"]

    faulty_telegram = FakeTelegram()
    sync_once(
        store,
        SyncRuntime(
            FakeTendwire(
                workers=[faulty_worker],
                stable_identities=False,
            ),
            faulty_telegram,
            with_outbox=False,
        ),
    )
    entries_before_repeat = deepcopy(state.source_worker_entries(store))
    repeated_telegram = FakeTelegram()
    sync_once(
        store,
        SyncRuntime(
            FakeTendwire(
                workers=[faulty_worker],
                stable_identities=False,
            ),
            repeated_telegram,
            with_outbox=False,
        ),
    )

    entries = state.source_worker_entries(store)
    assert entries == entries_before_repeat
    assert list(entries) == [original_key]
    assert entries[original_key]["topic_id"] == original_topic
    assert state.entry_is_quarantined(entries[original_key]) is True
    assert state.find_worker_entry_by_id(store, "claude-2") == (None, None)
    assert state.find_entry_by_thread(store, original_topic) == (None, None)
    assert _worker_entry_for_turn(store, "claude-2", "w1") == (None, None)
    assert first_telegram.topics == ["telegram-bot"]
    assert faulty_telegram.topics == repeated_telegram.topics == []


def test_rotation_creates_a_numbered_topic_instead_of_reusing_old_topic():
    store = _store()
    old_key, old, _created = state.upsert_worker_entry(
        store,
        _worker("claude-2", KEY_A, fingerprint="fp-old"),
        topic_id="26",
    )
    telegram = FakeTelegram()

    sync_once(
        store,
        SyncRuntime(
            FakeTendwire(workers=[_worker("claude-2", KEY_B, fingerprint="fp-new")]),
            telegram,
            with_outbox=False,
        ),
    )

    current_key, current = state.find_worker_entry_by_id(store, "claude-2")
    assert current_key != old_key
    assert old["topic_id"] == "26"
    assert current["topic_id"] != "26"
    assert current["topic_name"] == "telegram-bot 2"
    assert telegram.topics == ["telegram-bot 2"]


def test_stable_bearing_reply_binding_resolves_stable_first_and_fails_closed():
    store = _store()
    _key_a, entry_a, _created = state.upsert_worker_entry(
        store, _worker("claude-2", KEY_B), topic_id="26"
    )
    _key_b, entry_b, _created = state.upsert_worker_entry(
        store, _worker("claude-3", KEY_C), topic_id="28"
    )
    for entry in (entry_a, entry_b):
        entry["tendwire_stable_key"] = KEY_A
        entry["tendwire_stable_key_version"] = 1
    state.bind_message_to_worker(store, "500", entry_a, topic_id="26", kind="final", turn_id="turn-a")

    assert state.find_worker_entry_by_id(store, "claude-2") == (None, None)
    assert state.find_entry_key_by_stable_key(store, KEY_A) is None
    assert herdres._worker_entry_from_reply(
        store,
        {"reply_to_message_id": "500", "topic_id": "26"},
    ) == (None, None)

    store["telegram_message_bindings"]["500"]["stable_key_version"] = "1"
    assert herdres._worker_entry_from_reply(
        store,
        {"reply_to_message_id": "500", "topic_id": "26"},
    ) == (None, None)


def test_new_message_bindings_carry_only_validated_v1_identity():
    store = _store()
    _key, stable_entry, _created = state.upsert_worker_entry(store, _worker("claude-2", KEY_A), topic_id="26")
    state.bind_message_to_worker(store, "500", stable_entry, topic_id="26", kind="final")
    assert store["telegram_message_bindings"]["500"]["stable_key"] == KEY_A
    assert store["telegram_message_bindings"]["500"]["stable_key_version"] == 1

    _key, malformed_entry, _created = state.upsert_worker_entry(
        store,
        _worker("claude-3", "source-spoof", version=1),
        topic_id="28",
    )
    state.bind_message_to_worker(store, "501", malformed_entry, topic_id="28", kind="final")
    assert "stable_key" not in store["telegram_message_bindings"]["501"]
    assert "stable_key_version" not in store["telegram_message_bindings"]["501"]


@pytest.mark.parametrize("marker", [1, "true", False])
def test_any_present_entry_quarantine_marker_fails_closed(marker):
    store = _store()
    key, entry, _created = state.upsert_worker_entry(
        store, _worker("claude-2", KEY_A), topic_id="26"
    )
    entry["stable_key_quarantined"] = marker

    assert state.entry_is_quarantined(entry) is True
    assert state.find_worker_entry_by_id(store, "claude-2") == (None, None)
    assert state.find_worker_entry_by_stable_key(store, KEY_A) == (None, None)
    assert state.find_entry_by_thread(store, "26") == (None, None)
    assert _worker_entry_for_turn(store, "claude-2", "w1") == (None, None)
    assert state.find_worker_entry_by_alias(store, "claude-2", space_id="w1") == (None, None)
    assert key in state.source_worker_entries(store)


@pytest.mark.parametrize("marker", [1, "true", False])
def test_any_present_binding_quarantine_marker_fails_closed(marker):
    store = _store()
    _key, entry, _created = state.upsert_worker_entry(
        store, _worker("claude-2", KEY_A), topic_id="26"
    )
    state.bind_message_to_worker(store, "500", entry, topic_id="26", kind="final")
    store["telegram_message_bindings"]["500"]["routing_quarantined"] = marker

    assert herdres._worker_entry_from_reply(
        store,
        {"reply_to_message_id": "500", "topic_id": "26"},
    ) == (None, None)


@pytest.mark.parametrize(
    ("stored_key", "stored_version"),
    [
        ("K1", 1),
        (KEY_A, "1"),
        (KEY_A, 2),
        (None, 1),
    ],
)
def test_malformed_stored_identity_is_unroutable_everywhere(stored_key, stored_version):
    store = _store()
    _key, entry, _created = state.upsert_worker_entry(
        store, _worker("claude-2", KEY_C), topic_id="26"
    )
    if stored_key is None:
        entry.pop("tendwire_stable_key", None)
    else:
        entry["tendwire_stable_key"] = stored_key
    entry["tendwire_stable_key_version"] = stored_version
    state.bind_message_to_worker(store, "500", entry, topic_id="26", kind="final")

    assert state.entry_stable_identity(entry) is None
    assert state.entry_is_routable(entry) is False
    assert state.find_worker_entry_by_id(store, "claude-2") == (None, None)
    assert state.find_entry_by_thread(store, "26") == (None, None)
    assert _worker_entry_for_turn(store, "claude-2", "w1") == (None, None)
    assert state.find_worker_entry_by_alias(store, "claude-2", space_id="w1") == (None, None)
    assert herdres._worker_entry_from_reply(
        store,
        {"reply_to_message_id": "500", "topic_id": "26"},
    ) == (None, None)


def test_absent_and_legacy24_persisted_identities_are_migration_only():
    store = _store()
    absent_key, absent = _persisted_legacy_worker(
        store, "claude-2", topic_id="26"
    )
    legacy_key, legacy = _persisted_legacy_worker(
        store, "claude-3", legacy_value=LEGACY_KEY, topic_id="28"
    )

    for key, entry, worker_id, topic_id in (
        (absent_key, absent, "claude-2", "26"),
        (legacy_key, legacy, "claude-3", "28"),
    ):
        assert state.entry_is_routable(entry) is False
        assert state.find_worker_entry_by_id(store, worker_id) == (None, None)
        assert state.find_entry_by_thread(store, topic_id) == (None, None)
        assert _worker_entry_for_turn(store, worker_id, "w1") == (None, None)
        assert key in state.source_worker_entries(store)


def test_binding_retarget_requires_topic_and_identity_compatibility():
    store = _store()
    key, entry, _created = state.upsert_worker_entry(
        store,
        _worker("claude-2", KEY_A, fingerprint="fp-old"),
        topic_id="26",
    )
    state.bind_message_to_worker(store, "500", entry, topic_id="26", kind="final")
    base = {
        "worker_id": "claude-2",
        "worker_fingerprint": "fp-old",
        "space_id": "w1",
        "kind": "final",
        "turn_id": "turn-old",
        "bot_kind": "",
    }
    store["telegram_message_bindings"]["501"] = {**base, "topic_id": "99"}
    store["telegram_message_bindings"]["502"] = {
        **base,
        "topic_id": "26",
        "stable_key": KEY_B,
        "stable_key_version": 1,
    }
    store["telegram_message_bindings"]["503"] = {
        **base,
        "topic_id": "26",
        "stable_key": KEY_A,
        "stable_key_version": "1",
    }
    store["telegram_message_bindings"]["504"] = {
        **base,
        "topic_id": "26",
        "routing_quarantined": "true",
    }

    rebound_key, _rebound, created = state.upsert_worker_entry(
        store,
        _worker("claude-2-2", KEY_A, fingerprint="fp-new"),
    )

    assert created is False
    assert rebound_key == key
    compatible = store["telegram_message_bindings"]["500"]
    assert compatible["worker_id"] == "claude-2-2"
    assert compatible["worker_fingerprint"] == "fp-new"
    for message_id in ("501", "502", "503"):
        binding = store["telegram_message_bindings"][message_id]
        assert binding["worker_id"] == "claude-2"
        assert binding["worker_fingerprint"] == "fp-old"
        assert binding["routing_quarantined"] is True
    prequarantined = store["telegram_message_bindings"]["504"]
    assert prequarantined["worker_id"] == "claude-2"
    assert prequarantined["worker_fingerprint"] == "fp-old"
    assert prequarantined["routing_quarantined"] == "true"


def test_repeated_closed_snapshot_is_idempotent_but_not_live_adoptable():
    store = _store()
    closed_worker = _worker("claude-2", KEY_A, status="closed")

    sync_once(
        store,
        SyncRuntime(FakeTendwire(workers=[closed_worker]), FakeTelegram(), with_outbox=False),
    )
    first_entries = state.source_worker_entries(store)
    assert len(first_entries) == 1
    first_key = next(iter(first_entries))
    sync_once(
        store,
        SyncRuntime(FakeTendwire(workers=[closed_worker]), FakeTelegram(), with_outbox=False),
    )

    entries = state.source_worker_entries(store)
    assert list(entries) == [first_key]
    live_key, live, created = state.upsert_worker_entry(
        store,
        _worker("claude-2", KEY_A, status="working", fingerprint="fp-live"),
    )
    assert created is True
    assert live_key != first_key
    assert state.entry_is_quarantined(entries[first_key]) is True
    assert state.find_worker_entry_by_id(store, "claude-2") == (live_key, live)


def test_same_snapshot_worker_id_with_distinct_keys_is_order_independent_and_quarantined():
    def run(workers):
        store = _store()
        first_telegram = FakeTelegram()
        assert state.blocked_worker_stable_keys(store, workers) == {KEY_A, KEY_B}
        assert state.conflicting_snapshot_worker_ids(workers) == {"claude-2"}
        sync_once(
            store,
            SyncRuntime(FakeTendwire(workers=workers), first_telegram, with_outbox=False),
        )
        entries_before = deepcopy(state.source_worker_entries(store))
        second_telegram = FakeTelegram()
        sync_once(
            store,
            SyncRuntime(FakeTendwire(workers=workers), second_telegram, with_outbox=False),
        )
        assert state.source_worker_entries(store) == entries_before
        return store, first_telegram, second_telegram

    workers = [
        _worker("claude-2", KEY_A, fingerprint="fp-a"),
        _worker("claude-2", KEY_B, fingerprint="fp-b"),
    ]
    forward_store, forward_first, forward_second = run(workers)
    reverse_store, reverse_first, reverse_second = run(list(reversed(workers)))

    for telegram in (forward_first, forward_second, reverse_first, reverse_second):
        assert telegram.topics == []
        assert telegram.sent == []
    for store in (forward_store, reverse_store):
        entries = state.source_worker_entries(store)
        assert len(entries) == 2
        assert {entry["tendwire_stable_key"] for entry in entries.values()} == {KEY_A, KEY_B}
        assert all(state.entry_is_quarantined(entry) for entry in entries.values())
        assert all("topic_id" not in entry for entry in entries.values())
        assert state.find_worker_entry_by_id(store, "claude-2") == (None, None)
        assert state.find_entry_key_by_stable_key(store, KEY_A) is None
        assert state.find_entry_key_by_stable_key(store, KEY_B) is None
        assert _worker_entry_for_turn(store, "claude-2", "w1") == (None, None)


def test_preflight_blocked_key_quarantines_every_exact_and_stable_owner_on_repeat():
    store = _store()
    exact_key, exact_owner, _created = state.upsert_worker_entry(
        store,
        _worker("worker-1", KEY_A, space="space-1"),
        topic_id="26",
    )
    other_key, other_owner, _created = state.upsert_worker_entry(
        store,
        _worker("worker-2", KEY_B, space="space-1"),
        topic_id="28",
    )
    other_owner["tendwire_stable_key"] = KEY_A
    other_owner["tendwire_stable_key_version"] = 1
    state.bind_message_to_worker(
        store, "500", exact_owner, topic_id="26", kind="final"
    )
    state.bind_message_to_worker(
        store, "501", other_owner, topic_id="28", kind="final"
    )
    snapshot = [_worker("worker-1", KEY_A, space="space-1")]

    assert state.blocked_worker_stable_keys(store, snapshot) == {KEY_A}
    first_telegram = FakeTelegram()
    first = sync_once(
        store,
        SyncRuntime(
            FakeTendwire(workers=snapshot, turns={"turns": []}),
            first_telegram,
            with_outbox=False,
        ),
    )
    entries_after_first = deepcopy(state.source_worker_entries(store))
    bindings_after_first = deepcopy(state.message_bindings(store))
    second_telegram = FakeTelegram()
    second = sync_once(
        store,
        SyncRuntime(
            FakeTendwire(workers=snapshot, turns={"turns": []}),
            second_telegram,
            with_outbox=False,
        ),
    )

    assert set(state.source_worker_entries(store)) == {exact_key, other_key}
    assert state.source_worker_entries(store) == entries_after_first
    assert state.message_bindings(store) == bindings_after_first
    assert all(
        state.entry_is_quarantined(entry)
        for entry in state.source_worker_entries(store).values()
    )
    assert all(
        binding["routing_quarantined"] is True
        for binding in state.message_bindings(store).values()
    )
    assert state.find_worker_entry_by_id(store, "worker-1") == (None, None)
    assert state.find_worker_entry_by_stable_key(store, KEY_A) == (None, None)
    assert first["feed_sent"] == second["feed_sent"] == 0
    assert first_telegram.topics == second_telegram.topics == []
    assert first_telegram.sent == second_telegram.sent == []


def test_cross_dimensional_reconciliation_reserves_stable_owner_before_worker_id_in_both_orders():
    snapshot = [
        _worker(
            "worker-1",
            KEY_B,
            space="space-1",
            fingerprint="fp-worker-1-k2",
        ),
        _worker(
            "worker-2",
            KEY_A,
            space="space-1",
            fingerprint="fp-worker-2-k1",
        ),
    ]

    def run(rows):
        store = _store()
        persisted_key, persisted, _created = state.upsert_worker_entry(
            store,
            _worker(
                "worker-1",
                KEY_A,
                space="space-1",
                fingerprint="fp-persisted-k1",
            ),
            topic_id="26",
        )
        persisted["last_turn_id"] = "turn-old"
        state.bind_message_to_worker(
            store,
            "500",
            persisted,
            topic_id="26",
            kind="final",
            turn_id="turn-old",
        )
        state.mark_delivered(
            store,
            "final:turn-old:hash",
            {"worker_id": "worker-1", "turn_id": "turn-old"},
        )
        passes = []
        for _pass in range(3):
            telegram = FakeTelegram()
            result = sync_once(
                store,
                SyncRuntime(
                    FakeTendwire(workers=rows, turns={"turns": []}),
                    telegram,
                    with_outbox=False,
                ),
            )
            k1_key, k1 = state.find_worker_entry_by_stable_key(store, KEY_A)
            k2_key, k2 = state.find_worker_entry_by_stable_key(store, KEY_B)
            assert k1_key == persisted_key
            assert k1["tendwire_worker_id"] == "worker-2"
            assert k1["topic_id"] == "26"
            assert k2_key != persisted_key
            assert k2["tendwire_worker_id"] == "worker-1"
            assert all(
                not state.entry_is_quarantined(entry)
                for entry in state.source_worker_entries(store).values()
            )
            assert result["feed_sent"] == 0
            assert telegram.sent == []
            passes.append(
                {
                    "entries": deepcopy(state.source_worker_entries(store)),
                    "bindings": deepcopy(state.message_bindings(store)),
                    "ledger": deepcopy(state.delivered_turns(store)),
                    "topics": list(telegram.topics),
                }
            )
        return passes

    forward = run(snapshot)
    reverse = run(list(reversed(snapshot)))

    assert forward == reverse
    assert forward[0]["topics"] == []
    assert forward[1]["topics"] == ["telegram-bot 2"]
    assert forward[2]["topics"] == []
    assert forward[1]["entries"] == forward[2]["entries"]
    assert forward[1]["bindings"] == forward[2]["bindings"]
    assert forward[1]["ledger"] == forward[2]["ledger"] == {
        "final:turn-old:hash": {
            "worker_id": "worker-1",
            "turn_id": "turn-old",
        }
    }
    assert forward[2]["bindings"]["500"]["worker_id"] == "worker-2"


def test_blocked_worker_id_reuses_exact_persisted_identity_owner_on_every_pass():
    store = _store()
    persisted_key, persisted, _created = state.upsert_worker_entry(
        store,
        _worker(
            "worker-1",
            KEY_A,
            space="space-1",
            fingerprint="fp-persisted-k1",
        ),
        topic_id="26",
    )
    snapshot = [
        _worker(
            "worker-1",
            KEY_A,
            space="space-1",
            fingerprint="fp-current-k1",
        ),
        _worker(
            "worker-1",
            KEY_B,
            space="space-1",
            fingerprint="fp-current-k2",
        ),
    ]

    assert state.conflicting_snapshot_worker_ids(snapshot) == {"worker-1"}
    assert state.blocked_worker_stable_keys(store, snapshot) == {KEY_A, KEY_B}
    observed = []
    for rows in (snapshot, list(reversed(snapshot)), snapshot):
        telegram = FakeTelegram()
        result = sync_once(
            store,
            SyncRuntime(
                FakeTendwire(workers=rows, turns={"turns": []}),
                telegram,
                with_outbox=False,
            ),
        )
        entries = state.source_worker_entries(store)
        assert len(entries) == 2
        assert persisted_key in entries
        assert entries[persisted_key] is persisted
        assert persisted["tendwire_stable_key"] == KEY_A
        assert persisted["topic_id"] == "26"
        assert {
            entry["tendwire_stable_key"] for entry in entries.values()
        } == {KEY_A, KEY_B}
        assert all(state.entry_is_quarantined(entry) for entry in entries.values())
        assert result["feed_sent"] == 0
        assert telegram.topics == []
        assert telegram.sent == []
        observed.append(deepcopy(entries))

    assert observed[0] == observed[1] == observed[2]


@pytest.mark.parametrize(
    "identityless",
    [
        _worker(
            "worker-1",
            space="space-1",
            fingerprint="fp-identityless",
        ),
        _worker(
            "worker-1",
            "malformed",
            version=1,
            space="space-1",
            fingerprint="fp-identityless",
        ),
    ],
    ids=["missing", "malformed"],
)
def test_blocked_valid_owner_outranks_identityless_same_id_claimant_on_repeat_and_order(
    identityless,
):
    valid = _worker(
        "worker-1",
        KEY_A,
        space="space-1",
        fingerprint="fp-current-k1",
    )

    def run(rows):
        store = _store()
        persisted_key, persisted, _created = state.upsert_worker_entry(
            store,
            _worker(
                "worker-1",
                KEY_A,
                space="space-1",
                fingerprint="fp-persisted-k1",
            ),
            topic_id="26",
        )
        passes = []
        for _pass in range(3):
            telegram = FakeTelegram()
            result = sync_once(
                store,
                SyncRuntime(
                    FakeTendwire(
                        workers=rows,
                        turns={"turns": []},
                        stable_identities=False,
                    ),
                    telegram,
                    with_outbox=False,
                ),
            )
            entries = state.source_worker_entries(store)
            assert len(entries) == 2
            assert persisted_key in entries
            assert entries[persisted_key] is persisted
            assert persisted["tendwire_stable_key"] == KEY_A
            assert persisted["topic_id"] == "26"
            assert (
                sum(
                    entry.get("tendwire_stable_key") == KEY_A
                    for entry in entries.values()
                )
                == 1
            )
            assert all(
                state.entry_is_quarantined(entry) for entry in entries.values()
            )
            assert result["feed_sent"] == 0
            assert telegram.topics == []
            assert telegram.sent == []
            passes.append(deepcopy(entries))
        assert passes[0] == passes[1] == passes[2]
        return passes

    assert run([identityless, valid]) == run([valid, identityless])


def test_identical_blocked_snapshot_claims_pair_with_quarantine_rows_idempotently():
    snapshot = [
        _worker(
            "worker-1",
            KEY_A,
            space="space-1",
            fingerprint="fp-identical",
        ),
        _worker(
            "worker-1",
            KEY_A,
            space="space-1",
            fingerprint="fp-identical",
        ),
    ]

    def run(rows):
        store = _store()
        passes = []
        for _pass in range(3):
            telegram = FakeTelegram()
            result = sync_once(
                store,
                SyncRuntime(
                    FakeTendwire(workers=rows, turns={"turns": []}),
                    telegram,
                    with_outbox=False,
                ),
            )
            entries = state.source_worker_entries(store)
            assert len(entries) == 2
            assert all(
                entry["tendwire_stable_key"] == KEY_A
                and state.entry_is_quarantined(entry)
                for entry in entries.values()
            )
            assert result["feed_sent"] == 0
            assert telegram.topics == []
            assert telegram.sent == []
            passes.append(deepcopy(entries))
        assert passes[0] == passes[1] == passes[2]
        return passes

    assert run(snapshot) == run(list(reversed(snapshot)))


@pytest.mark.parametrize("topic_mode", ["worker", "space"])
def test_quarantined_exact_v1_owner_blocks_id_churn_without_topic_routing_or_replay(
    monkeypatch, topic_mode
):
    monkeypatch.setenv("HERDRES_SOURCE_TOPIC_MODE", topic_mode)
    store = _store()
    old_key, old_owner, _created = state.upsert_worker_entry(
        store,
        _worker(
            "worker-old",
            KEY_A,
            space="space-1",
            fingerprint="fp-old",
        ),
        topic_id="26",
    )
    state.bind_message_to_worker(
        store,
        "500",
        old_owner,
        topic_id="26",
        kind="final",
        turn_id="turn-old",
    )
    state.quarantine_worker_entry(
        store,
        old_key,
        reason="preexisting_quarantine",
    )
    current = _worker(
        "worker-new",
        KEY_A,
        space="space-1",
        fingerprint="fp-new",
    )
    turns = {"turns": [_final_turn("worker-new", turn_id="turn-new")]}

    assert state.blocked_worker_stable_keys(store, [current]) == {KEY_A}
    passes = []
    for _pass in range(3):
        telegram = FakeTelegram()
        result = sync_once(
            store,
            SyncRuntime(
                FakeTendwire(workers=[current], turns=turns),
                telegram,
                with_outbox=False,
            ),
        )
        entries = state.source_worker_entries(store)
        assert len(entries) == 2
        assert all(state.entry_is_quarantined(entry) for entry in entries.values())
        new_owner = next(
            entry
            for entry in entries.values()
            if entry["tendwire_worker_id"] == "worker-new"
        )
        assert "topic_id" not in new_owner
        assert state.find_worker_entry_by_id(store, "worker-new") == (None, None)
        assert state.find_worker_entry_by_stable_key(store, KEY_A) == (None, None)
        assert _worker_entry_for_turn(store, "worker-new", "space-1") == (None, None)
        assert herdres._worker_entry_from_reply(
            store,
            {"reply_to_message_id": "500", "topic_id": "26"},
        ) == (None, None)
        assert state.message_bindings(store)["500"]["routing_quarantined"] is True
        assert state.delivered_turns(store) == {}
        assert result["feed_sent"] == 0
        assert telegram.topics == []
        assert telegram.sent == []
        passes.append(
            (
                deepcopy(entries),
                deepcopy(state.message_bindings(store)),
                deepcopy(state.delivered_turns(store)),
            )
        )
    assert passes[0] == passes[1] == passes[2]


def test_closed_nonquarantined_exact_v1_history_does_not_block_id_churn_on_repeat():
    store = _store()
    old_key, old_owner, _created = state.upsert_worker_entry(
        store,
        _worker(
            "worker-old",
            KEY_A,
            space="space-1",
            status="closed",
            fingerprint="fp-old",
        ),
        topic_id="26",
    )
    current = _worker(
        "worker-new",
        KEY_A,
        space="space-1",
        fingerprint="fp-new",
    )

    assert state.entry_is_quarantined(old_owner) is False
    assert state.blocked_worker_stable_keys(store, [current]) == set()
    passes = []
    for _pass in range(3):
        telegram = FakeTelegram()
        result = sync_once(
            store,
            SyncRuntime(
                FakeTendwire(workers=[current], turns={"turns": []}),
                telegram,
                with_outbox=False,
            ),
        )
        new_key, new_owner = state.find_worker_entry_by_id(store, "worker-new")
        assert new_key is not None and new_key != old_key
        assert new_owner is not None
        assert state.entry_is_quarantined(old_owner) is True
        assert state.entry_stable_identity(old_owner) is None
        assert old_owner["retired_tendwire_stable_key"] == KEY_A
        assert state.blocked_worker_stable_keys(store, [current]) == set()
        assert (
            state.worker_entry_is_uniquely_routable(store, new_key, new_owner)
            is True
        )
        assert result["feed_sent"] == 0
        assert telegram.sent == []
        passes.append(deepcopy(state.source_worker_entries(store)))

    assert passes[0] == passes[1] == passes[2]


@pytest.mark.parametrize(
    "statuses",
    [
        ("closed", "closed"),
        ("failed", "failed"),
        ("working", "closed"),
        ("working", "failed"),
    ],
    ids=["closed-duplicates", "failed-duplicates", "live-closed", "live-failed"],
)
def test_terminal_snapshot_duplicates_are_order_and_repeat_idempotent_without_delivery(
    statuses,
):
    rows = [
        _worker(
            "worker-1",
            KEY_A,
            space="space-1",
            status=status,
            fingerprint="fp-identical",
        )
        for status in statuses
    ]
    turns = {"turns": [_final_turn("worker-1")]}

    def run(observations):
        store = _store()
        assert state.blocked_worker_stable_keys(store, observations) == {KEY_A}
        assert state.conflicting_snapshot_worker_ids(observations) == {"worker-1"}
        passes = []
        for _pass in range(3):
            telegram = FakeTelegram()
            result = sync_once(
                store,
                SyncRuntime(
                    FakeTendwire(workers=observations, turns=turns),
                    telegram,
                    with_outbox=False,
                ),
            )
            entries = state.source_worker_entries(store)
            assert len(entries) == 2
            assert all(
                entry["tendwire_stable_key"] == KEY_A
                and state.entry_is_quarantined(entry)
                and "topic_id" not in entry
                for entry in entries.values()
            )
            assert state.find_worker_entry_by_id(store, "worker-1") == (None, None)
            assert state.find_worker_entry_by_stable_key(store, KEY_A) == (None, None)
            assert _worker_entry_for_turn(store, "worker-1", "space-1") == (
                None,
                None,
            )
            assert state.delivered_turns(store) == {}
            assert result["feed_sent"] == 0
            assert telegram.topics == []
            assert telegram.sent == []
            passes.append(deepcopy(entries))
        assert passes[0] == passes[1] == passes[2]
        return passes

    assert run(rows) == run(list(reversed(rows)))


def test_missing_and_malformed_same_id_rows_have_total_slot_order_on_repeat():
    missing = _worker(
        "worker-1",
        space="space-1",
        fingerprint="fp-identical",
    )
    malformed = _worker(
        "worker-1",
        "malformed",
        version=1,
        space="space-1",
        fingerprint="fp-identical",
    )

    def run(observations):
        store = _store()
        passes = []
        for _pass in range(3):
            telegram = FakeTelegram()
            result = sync_once(
                store,
                SyncRuntime(
                    FakeTendwire(
                        workers=observations,
                        turns={"turns": [_final_turn("worker-1")]},
                        stable_identities=False,
                    ),
                    telegram,
                    with_outbox=False,
                ),
            )
            entries = state.source_worker_entries(store)
            ordered_keys = sorted(entries)
            assert len(ordered_keys) == 2
            assert [
                entries[key]["tendwire_stable_identity_class"]
                for key in ordered_keys
            ] == ["absent", "malformed"]
            assert all(
                state.entry_is_quarantined(entry) and "topic_id" not in entry
                for entry in entries.values()
            )
            assert state.find_worker_entry_by_id(store, "worker-1") == (None, None)
            assert _worker_entry_for_turn(store, "worker-1", "space-1") == (
                None,
                None,
            )
            assert state.delivered_turns(store) == {}
            assert result["feed_sent"] == 0
            assert telegram.topics == []
            assert telegram.sent == []
            passes.append(deepcopy(entries))
        assert passes[0] == passes[1] == passes[2]
        return passes

    assert run([missing, malformed]) == run([malformed, missing])


@pytest.mark.parametrize("topic_mode", ["worker", "space"])
def test_absent_current_owners_of_preflight_blocked_key_are_quarantined_before_reconciliation(
    monkeypatch, topic_mode
):
    monkeypatch.setenv("HERDRES_SOURCE_TOPIC_MODE", topic_mode)
    store = _store()
    first_key, first_owner, _created = state.upsert_worker_entry(
        store,
        _worker(
            "worker-1",
            KEY_A,
            space="space-1",
            fingerprint="fp-1",
        ),
        topic_id="26",
    )
    second_key, second_owner, _created = state.upsert_worker_entry(
        store,
        _worker(
            "worker-2",
            KEY_B,
            space="space-1",
            fingerprint="fp-2",
        ),
        topic_id="28",
    )
    second_owner["tendwire_stable_key"] = KEY_A
    second_owner["tendwire_stable_key_version"] = 1
    state.bind_message_to_worker(
        store,
        "500",
        first_owner,
        topic_id="26",
        kind="final",
    )
    state.bind_message_to_worker(
        store,
        "501",
        second_owner,
        topic_id="28",
        kind="final",
    )

    assert state.blocked_worker_stable_keys(store, []) == {KEY_A}
    passes = []
    for _pass in range(3):
        telegram = FakeTelegram()
        result = sync_once(
            store,
            SyncRuntime(
                FakeTendwire(
                    workers=[],
                    turns={"turns": [_final_turn("worker-1")]},
                ),
                telegram,
                with_outbox=False,
            ),
        )
        assert set(state.source_worker_entries(store)) == {first_key, second_key}
        assert all(
            state.entry_is_quarantined(entry)
            for entry in state.source_worker_entries(store).values()
        )
        assert all(
            binding["routing_quarantined"] is True
            for binding in state.message_bindings(store).values()
        )
        for message_id, topic_id in (("500", "26"), ("501", "28")):
            assert herdres._worker_entry_from_reply(
                store,
                {"reply_to_message_id": message_id, "topic_id": topic_id},
            ) == (None, None)
        assert state.find_worker_entry_by_stable_key(store, KEY_A) == (None, None)
        assert state.find_entry_by_thread(store, "26") == (None, None)
        assert state.find_entry_by_thread(store, "28") == (None, None)
        assert state.delivered_turns(store) == {}
        assert result["feed_sent"] == 0
        assert telegram.topics == []
        assert telegram.sent == []
        passes.append(
            (
                deepcopy(state.source_worker_entries(store)),
                deepcopy(state.message_bindings(store)),
            )
        )
    assert passes[0] == passes[1] == passes[2]


@pytest.mark.parametrize("unhealthy_status", ["degraded", "unavailable", "unknown"])
def test_explicit_unhealthy_herdr_snapshot_preserves_authenticated_state_until_healthy_recovery(
    unhealthy_status,
):
    store = _store()
    initial_telegram = FakeTelegram()
    initial = sync_once(
        store,
        SyncRuntime(
            _BackendHealthTendwire(
                backend_health=[{"name": "herdr", "status": "healthy"}],
                workers=[_worker("claude-2", KEY_A)],
                turns={"turns": [_final_turn("claude-2")]},
            ),
            initial_telegram,
            with_outbox=False,
        ),
    )
    original_entries = deepcopy(state.source_worker_entries(store))
    original_ledger = deepcopy(state.delivered_turns(store))
    original_topic = next(iter(original_entries.values()))["topic_id"]

    assert initial["feed_sent"] == 1
    assert initial_telegram.topics == ["telegram-bot"]

    degraded_tendwire = _BackendHealthTendwire(
        backend_health=[
            {
                "name": "Herdr",
                "status": unhealthy_status,
                "outcome": "continuity_unavailable",
            }
        ],
        workers=[_worker("claude-2", stable=None, fingerprint="identity-less")],
        stable_identities=False,
        turns={"turns": [_final_turn("claude-2", turn_id="turn-poison", text="Must not route")]},
        pending={"pending_interactions": [{"id": "pending-poison", "worker_id": "claude-2"}]},
    )
    degraded_telegram = FakeTelegram()
    degraded = sync_once(
        store,
        SyncRuntime(degraded_tendwire, degraded_telegram, with_outbox=False),
    )

    assert degraded == {
        "ok": False,
        "status": "tendwire_herdr_unhealthy",
        "changed": False,
        "created": 0,
        "updated": 0,
        "panes": 0,
        "spaces": 0,
        "icon_updated": 0,
        "pinned_status_updated": 0,
        "feed_sent": 0,
        "sent": 0,
        "routing_repaired": 0,
        "message_bindings": 0,
        "turn_updates": 0,
        "topic_cleanup": {"deleted": 0, "failed": 0, "pruned": 0, "changed": False},
        "content_pages": 0,
        "tendwire_turn_final": {
            "enabled": False,
            "polled": 0,
            "operations": 0,
            "delivered": 0,
            "acked": 0,
            "failed": 0,
            "deferred": 0,
            "uncertain": 0,
            "changed": False,
        },
        "tendwire_outbox": {
            "enabled": False,
            "polled": 0,
            "delivered": 0,
            "acked": 0,
            "failed": 0,
            "deferred": 0,
            "changed": False,
        },
    }
    assert (degraded_tendwire.snapshot_calls, degraded_tendwire.turn_calls, degraded_tendwire.pending_calls) == (1, 0, 0)
    assert degraded_tendwire.commands == []
    assert state.source_worker_entries(store) == original_entries
    assert state.delivered_turns(store) == original_ledger
    assert degraded_telegram.topics == []
    assert degraded_telegram.sent == []
    assert degraded_telegram.edited == []
    assert degraded_telegram.deleted_topics == []
    assert degraded_telegram.renamed_topics == []
    assert degraded_telegram.pins == []
    assert degraded_telegram.api_calls == []
    assert degraded_telegram.icon_edits == []
    assert degraded_telegram.voice_notes == []

    recovery_tendwire = _BackendHealthTendwire(
        backend_health=[{"name": "herdr", "status": "healthy", "outcome": "healthy_non_empty"}],
        workers=[_worker("claude-2-2", KEY_A)],
        turns={"turns": [_final_turn("claude-2-2")]},
    )
    recovery_telegram = FakeTelegram()
    recovered = sync_once(
        store,
        SyncRuntime(recovery_tendwire, recovery_telegram, with_outbox=False),
    )

    recovered_entries = state.source_worker_entries(store)
    assert recovered["ok"] is True
    assert recovered["feed_sent"] == 0
    assert (recovery_tendwire.snapshot_calls, recovery_tendwire.turn_calls, recovery_tendwire.pending_calls) == (1, 1, 1)
    assert len(recovered_entries) == 1
    assert next(iter(recovered_entries.values()))["topic_id"] == original_topic
    assert next(iter(recovered_entries.values()))["tendwire_worker_id"] == "claude-2-2"
    assert state.delivered_turns(store) == original_ledger
    assert recovery_tendwire.commands == []
    assert recovery_telegram.topics == []
    assert recovery_telegram.sent == []


@pytest.mark.parametrize(
    "backend_health",
    [
        _BACKEND_HEALTH_ABSENT,
        None,
        {"name": "herdr", "status": "degraded"},
        "malformed",
        [{"name": "telegram", "status": "degraded"}],
    ],
)
def test_absent_malformed_or_non_herdr_backend_health_remains_compatible(backend_health):
    store = _store()
    tendwire = _BackendHealthTendwire(
        backend_health=backend_health,
        workers=[_worker("claude-2", KEY_A)],
    )
    telegram = FakeTelegram()

    result = sync_once(
        store,
        SyncRuntime(tendwire, telegram, with_outbox=False),
    )

    assert result["ok"] is True
    assert result["created"] == 2
    assert (tendwire.snapshot_calls, tendwire.turn_calls, tendwire.pending_calls) == (1, 1, 1)
    assert telegram.topics == ["telegram-bot"]
