"""Stable v1 worker continuity and fail-closed reconciliation contracts."""
from __future__ import annotations

from copy import deepcopy
import json

import pytest

from herdres_connector import state
from herdres_connector.source_sync import SyncRuntime, _worker_entry_for_turn, sync_once

from test_source_only import FakeTelegram, FakeTendwire as _FakeTendwire, _store


KEY_A = "wsk1_" + "a" * 64
KEY_B = "wsk1_" + "b" * 64
KEY_C = "wsk1_" + "c" * 64
LEGACY_KEY = "d" * 24
_AUTO_VERSION = object()


class FakeTendwire(_FakeTendwire):
    def __init__(self, *, turns=None, **kwargs):
        current_turns = dict(turns) if turns is not None else {"turns": []}
        current_turns.setdefault("schema_version", 2)
        super().__init__(turns=current_turns, **kwargs)


def _normalize_generated_pane_uuids(value):
    """Keep order-invariance assertions focused on identity ownership.

    UUIDv4 values are intentionally random.  Replace each with a stable label
    derived from the entry's current public routing hint, including UUIDs
    embedded in new ``pane:<uuid>`` storage keys and message bindings.
    """
    normalized = deepcopy(value)
    replacements = {}

    def collect(item):
        if isinstance(item, dict):
            if state.entry_pane_uuid(item):
                replacements[item["pane_uuid"]] = "pane-for:" + str(
                    item.get("tendwire_stable_key")
                    or item.get("topic_id")
                    or "unknown"
                )
            for child in item.values():
                collect(child)
        elif isinstance(item, list):
            for child in item:
                collect(child)

    collect(normalized)

    def rewrite(item):
        if isinstance(item, dict):
            return {
                next(
                    (
                        key.replace(old, new)
                        for old, new in replacements.items()
                        if old in key
                    ),
                    key,
                ): rewrite(child)
                for key, child in item.items()
            }
        if isinstance(item, list):
            return [rewrite(child) for child in item]
        if isinstance(item, str):
            return replacements.get(item, item)
        return item

    return rewrite(normalized)


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


def _typed_state(store, tmp_path):
    tmp_path.chmod(0o700)
    path = tmp_path / "typed-state.json"
    state.save_state(store, path)
    path.chmod(0o600)
    return path, state.read_ingress_policy(path)


def _typed_topic_route(store, tmp_path, topic_id, *, alias="", bot_kind=""):
    path, policy = _typed_state(store, tmp_path)
    return state.resolve_ingress_route(path, state.IngressRouteQuery(
        chat_id=policy.chat_id, topic_id=str(topic_id), receiver_bot_kind="manager",
        explicit_alias=alias, explicit_bot_kind=bot_kind, state_token=policy.state_token,
    ))


def _typed_reply_route(store, tmp_path, message_id, topic_id, *, author_kind=""):
    path, policy = _typed_state(store, tmp_path)
    return state.resolve_ingress_reply(path, state.IngressReplyQuery(
        chat_id=policy.chat_id, topic_id=str(topic_id), reply_message_id=str(message_id),
        observed_author_bot_kind=author_kind, explicit_alias="", explicit_bot_kind="",
        state_token=policy.state_token,
    ))


def test_acp_frontend_fingerprint_churn_keeps_topic_and_compatible_ingress_target(tmp_path):
    """A visible frontend may rotate while owner evidence stays separate."""
    store = _store()
    original_key, original, created = state.upsert_worker_entry(
        store,
        _worker(
            "codex",
            KEY_A,
            agent="codex",
            fingerprint="fp-headless",
        ),
        topic_id="14303",
    )
    assert created is True
    state.bind_message_to_worker(
        store,
        "14310",
        original,
        topic_id="14303",
        kind="final",
        turn_id="turn-before-frontend",
    )

    frontend_key, frontend, created = state.upsert_worker_entry(
        store,
        _worker(
            "codex",
            KEY_A,
            agent="codex",
            fingerprint="fp-visible-frontend",
        ),
    )

    assert created is False
    assert frontend_key == original_key
    assert frontend is original
    assert frontend["topic_id"] == "14303"
    route = _typed_topic_route(store, tmp_path, "14303")
    assert route.status is state.RouteStatus.RESOLVED
    assert (route.worker_id, route.worker_fingerprint) == (
        "codex", "fp-visible-frontend"
    )
    assert route.owner == state.StableOwner(KEY_A, 1, None)
    binding = state.find_message_binding(store, "14310", topic_id="14303")
    assert binding is not None
    assert binding["worker_fingerprint"] == "fp-visible-frontend"
    assert state.find_entry_by_thread(store, "14303") == (
        original_key,
        frontend,
    )


def test_ingress_route_fails_closed_without_exact_stable_owner(tmp_path):
    store = _store()
    _key, entry, _created = state.upsert_worker_entry(
        store, _worker("codex", KEY_A, fingerprint="fp-legacy"), topic_id="14303"
    )
    entry["tendwire_stable_key_version"] = "1"

    assert state.entry_stable_identity(entry) is None
    route = _typed_topic_route(store, tmp_path, "14303")
    assert route.status is state.RouteStatus.MISSING
    assert route.owner is None
    assert not route.worker_id and not route.worker_fingerprint


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
        _worker("claude-2", status="idle"),
        _worker(
            "claude-2", "source-spoof", version=1, status="idle"
        ),
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


def test_exact_v1_identity_return_heals_transient_absent_identity_quarantine():
    store = _store()
    key, entry, _created = state.upsert_worker_entry(
        store, _worker("claude-2", KEY_A), topic_id="26"
    )
    entry["pane_uuid"] = "00000000-0000-4000-8000-000000000001"
    entry["pane_uuid_version"] = 1
    state.bind_message_to_worker(
        store, "500", entry, topic_id="26", kind="final"
    )

    absent_key, absent, absent_created = state.upsert_worker_entry(
        store, _worker("claude-2")
    )

    assert absent_key == key
    assert absent_created is False
    assert absent is entry
    assert absent["stable_key_quarantine_reason"] == "source_identity_absent"
    assert state.message_bindings(store)["500"]["routing_quarantined"] is True

    restored = _worker("claude-2", KEY_A)
    blocked = state.blocked_worker_stable_keys(store, [restored])
    assert KEY_A not in blocked

    healed_key, healed, healed_created = state.upsert_worker_entry(
        store, restored, blocked_stable_keys=blocked
    )

    assert healed_key == key
    assert healed_created is False
    assert healed is entry
    assert "stable_key_quarantined" not in healed
    assert "stable_key_quarantine_reason" not in healed
    assert "routing_quarantined" not in state.message_bindings(store)["500"]
    assert state.find_worker_entry_by_stable_key(store, KEY_A) == (key, entry)


def test_exact_v1_identity_return_does_not_heal_other_quarantine_reasons():
    store = _store()
    key, entry, _created = state.upsert_worker_entry(
        store, _worker("claude-2", KEY_A), topic_id="26"
    )
    entry["stable_key_quarantined"] = True
    entry["stable_key_quarantine_reason"] = "operator_hold"

    restored = _worker("claude-2", KEY_A)
    blocked = state.blocked_worker_stable_keys(store, [restored])
    assert KEY_A in blocked

    repeated_key, repeated, repeated_created = state.upsert_worker_entry(
        store, restored, blocked_stable_keys=blocked
    )

    assert repeated_key == key
    assert repeated_created is False
    assert repeated is entry
    assert repeated["stable_key_quarantined"] is True
    assert repeated["stable_key_quarantine_reason"] == "operator_hold"
    assert state.find_worker_entry_by_stable_key(store, KEY_A) == (None, None)


def test_unsupported_persisted_identity_is_quarantined_not_adopted():
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


def test_restart_repoints_all_constrained_historical_bindings_and_replies(tmp_path):
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
        route = _typed_reply_route(store, tmp_path, message_id, "26")
        assert route.status is state.RouteStatus.RESOLVED
        assert (route.worker_id, route.worker_fingerprint) == (
            "claude-2-2", "fp-new"
        )
        assert route.owner == state.StableOwner(KEY_A, 1, None)
        assert route.binding_was_present is True
    assert store["telegram_message_bindings"]["999"]["worker_id"] == "claude-2"



@pytest.mark.parametrize("topic_mode", ["worker", "space"])
def test_fresh_duplicate_key_claimants_are_all_quarantined_without_routing_or_topics(
    monkeypatch, topic_mode
):
    monkeypatch.setenv("HERDRES_SOURCE_TOPIC_MODE", topic_mode)
    store = _store()
    workers = [
        _worker("claude-2", KEY_A, status="idle"),
        _worker("claude-3", KEY_A, status="idle"),
    ]
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


def test_persisted_duplicate_stable_key_consolidates_to_oldest_topic():
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
        entry["tendwire_last_seen_at"] = "2000-01-01T00:00:00+00:00"
    telegram = FakeTelegram()

    sync_once(
        store,
        SyncRuntime(FakeTendwire(workers=[_worker("claude-9", KEY_A)]), telegram, with_outbox=False),
    )

    entries = state.source_worker_entries(store)
    assert len(entries) == 2
    assert entries[key_a]["topic_id"] == "26"
    assert entries[key_a]["tendwire_worker_id"] == "claude-9"
    assert not state.entry_is_quarantined(entries[key_a])
    assert state.worker_entry_is_uniquely_routable(store, key_a, entries[key_a])
    assert state.find_entry_by_thread(store, "26") == (key_a, entries[key_a])
    assert state.entry_is_retired(entries[key_b])
    assert state.entry_stable_identity(entries[key_b]) is None
    assert entries[key_b]["retired_tendwire_stable_key"] == KEY_A
    assert state.find_entry_by_thread(store, "28") == (None, None)
    assert telegram.topics == []
    assert telegram.closed_topics == []
    assert any(kwargs.get("thread_id") == "28" for _chat, _text, kwargs, _mid in telegram.sent)




def test_closed_key_reuse_never_adopts_or_routes_the_closed_entry(tmp_path):
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
    route = _typed_reply_route(store, tmp_path, "500", "26")
    assert route.status is state.RouteStatus.BINDING_AMBIGUOUS
    assert route.binding_was_present is True
    assert route.owner is None and not route.worker_id


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
def test_rotation_loss_or_invalid_replacement_quarantines_old_binding(incoming, tmp_path):
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
    route = _typed_reply_route(store, tmp_path, "500", "26")
    assert route.status is state.RouteStatus.BINDING_AMBIGUOUS
    assert route.binding_was_present is True
    assert route.owner is None and not route.worker_id


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


def test_rotation_with_majority_physical_identity_migrates_the_old_topic():
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
    assert old["retired_topic_id"] == "26"
    assert "topic_id" not in old
    assert state.entry_is_retired(old) is True
    assert current["topic_id"] == "26"
    assert current["topic_name"] == "telegram-bot"
    assert telegram.topics == []


def test_stable_bearing_reply_binding_resolves_stable_first_and_fails_closed(tmp_path):
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
    route = _typed_reply_route(store, tmp_path, "500", "26")
    assert route.status is state.RouteStatus.BINDING_AMBIGUOUS
    assert route.binding_was_present is True
    assert route.owner is None and not route.worker_id

    store["telegram_message_bindings"]["500"]["stable_key_version"] = "1"
    malformed = _typed_reply_route(store, tmp_path, "500", "26")
    assert malformed.status is state.RouteStatus.BINDING_AMBIGUOUS
    assert malformed.binding_was_present is True
    assert malformed.owner is None and not malformed.worker_id


@pytest.mark.parametrize("malformed_binding", [None, "corrupt", [], 7])
def test_present_malformed_reply_binding_never_falls_through_to_topic_route(
    malformed_binding, tmp_path
):
    store = _store()
    state.upsert_worker_entry(
        store, _worker("claude-2", KEY_A), topic_id="26"
    )
    store["telegram_message_bindings"] = {"500": malformed_binding}

    route = _typed_reply_route(store, tmp_path, "500", "26")

    assert route.status is state.RouteStatus.BINDING_AMBIGUOUS
    assert route.reason == "binding_topic_mismatch_or_malformed"
    assert route.binding_was_present is True
    assert route.owner is None and not route.worker_id


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
def test_any_present_binding_quarantine_marker_fails_closed(marker, tmp_path):
    store = _store()
    _key, entry, _created = state.upsert_worker_entry(
        store, _worker("claude-2", KEY_A), topic_id="26"
    )
    state.bind_message_to_worker(store, "500", entry, topic_id="26", kind="final")
    store["telegram_message_bindings"]["500"]["routing_quarantined"] = marker

    route = _typed_reply_route(store, tmp_path, "500", "26")
    assert route.status is state.RouteStatus.BINDING_AMBIGUOUS
    assert route.binding_was_present is True
    assert route.owner is None and not route.worker_id


@pytest.mark.parametrize(
    ("stored_key", "stored_version"),
    [
        ("K1", 1),
        (KEY_A, "1"),
        (KEY_A, 2),
        (None, 1),
    ],
)
def test_malformed_stored_identity_is_unroutable_everywhere(
    stored_key, stored_version, tmp_path
):
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
    route = _typed_reply_route(store, tmp_path, "500", "26")
    assert route.status is state.RouteStatus.BINDING_AMBIGUOUS
    assert route.binding_was_present is True
    assert route.owner is None and not route.worker_id


def test_absent_and_noncurrent_persisted_identities_are_unroutable():
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


def test_preflight_duplicate_key_consolidates_bindings_and_is_repeat_idempotent():
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
    exact_owner["tendwire_last_seen_at"] = "2000-01-01T00:00:00+00:00"
    other_owner["tendwire_last_seen_at"] = "2000-01-01T00:00:00+00:00"
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
    entries = state.source_worker_entries(store)
    assert not state.entry_is_quarantined(entries[exact_key])
    assert state.worker_entry_is_uniquely_routable(store, exact_key, entries[exact_key])
    assert state.entry_is_retired(entries[other_key])
    assert state.entry_stable_identity(entries[other_key]) is None
    assert "routing_quarantined" not in state.message_bindings(store)["500"]
    assert state.message_bindings(store)["501"]["routing_quarantined"] is True
    assert all(
        binding["worker_id"] == "worker-1" and binding["stable_key"] == KEY_A
        for binding in state.message_bindings(store).values()
    )
    assert state.find_worker_entry_by_id(store, "worker-1") == (
        exact_key,
        entries[exact_key],
    )
    assert state.find_worker_entry_by_stable_key(store, KEY_A) == (
        exact_key,
        entries[exact_key],
    )
    assert first["feed_sent"] == second["feed_sent"] == 0
    assert first_telegram.topics == second_telegram.topics == []
    assert first_telegram.closed_topics == []
    assert second_telegram.sent == []
    assert second_telegram.closed_topics == []


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
        return _normalize_generated_pane_uuids({"passes": passes})["passes"]

    forward = run(snapshot)
    reverse = run(list(reversed(snapshot)))

    assert forward == reverse
    assert forward[0]["topics"] == ["telegram-bot 2"]
    assert forward[1]["topics"] == []
    assert forward[2]["topics"] == []
    assert forward[0]["entries"] == forward[1]["entries"] == forward[2]["entries"]
    assert forward[0]["bindings"] == forward[1]["bindings"] == forward[2]["bindings"]
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
def test_quarantined_exact_v1_owner_heals_after_stable_key_duplicate_consolidation(
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
        reason="preflight_stable_key_conflict",
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
    for pass_index in range(3):
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
        if pass_index == 0:
            assert len(entries) == 2
            assert all(state.entry_is_quarantined(entry) for entry in entries.values())
            assert state.find_worker_entry_by_stable_key(store, KEY_A) == (None, None)
            assert state.delivered_turns(store) == {}
            assert result["feed_sent"] == 0
            assert telegram.topics == []
            assert telegram.sent == []
        else:
            if topic_mode == "worker":
                retired = [
                    entry
                    for entry in entries.values()
                    if state.entry_is_retired(entry)
                ]
                assert len(entries) == 2
                assert len(retired) == 1
                assert (
                    retired[0]["routing_retired_reason"]
                    == "duplicate_live_claim"
                )
                assert state.entry_stable_identity(retired[0]) is None
            else:
                assert len(entries) == 1
            new_key, new_owner = state.find_worker_entry_by_stable_key(store, KEY_A)
            assert new_key is not None and new_owner is not None
            assert new_owner["tendwire_worker_id"] == "worker-new"
            assert state.worker_entry_is_uniquely_routable(store, new_key, new_owner)
            assert _worker_entry_for_turn(store, "worker-new", "space-1") == (
                new_key,
                new_owner,
            )
            assert result["feed_sent"] == 0
            assert telegram.sent == []
            if topic_mode == "worker":
                assert new_owner["topic_id"] == "26"
                assert telegram.topics == []
            elif pass_index == 1:
                assert telegram.topics == ["Project"]
        passes.append(
            (
                deepcopy(entries),
                deepcopy(state.message_bindings(store)),
                deepcopy(state.delivered_turns(store)),
            )
        )
    assert passes[1] == passes[2]


def test_duplicate_consolidation_preserves_non_identity_quarantine():
    store = _store()
    survivor_key, survivor, _created = state.upsert_worker_entry(
        store, _worker("worker-old", KEY_B), topic_id="26"
    )
    duplicate_key, duplicate, _created = state.upsert_worker_entry(
        store, _worker("worker-new", KEY_C), topic_id="28"
    )
    for entry in (survivor, duplicate):
        entry["tendwire_stable_key"] = KEY_A
        entry["tendwire_stable_key_version"] = 1
        entry["tendwire_last_seen_at"] = "2000-01-01T00:00:00+00:00"
    state.quarantine_worker_entry(store, survivor_key, reason="operator_hold")
    state.bind_message_to_worker(
        store, "500", survivor, topic_id="26", kind="final"
    )

    assert state.consolidate_worker_entries_by_stable_key(
        store, [_worker("worker-current", KEY_A)]
    ) == 1

    entries = state.source_worker_entries(store)
    assert entries[survivor_key]["stable_key_quarantine_reason"] == "operator_hold"
    assert state.entry_is_quarantined(entries[survivor_key])
    assert state.entry_is_retired(entries[duplicate_key])
    assert state.message_bindings(store)["500"]["routing_quarantined"] is True
    assert state.find_worker_entry_by_stable_key(store, KEY_A) == (None, None)


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
    turns = {
        "turns": [
            {
                "id": "turn-1",
                "worker_id": "worker-1",
                "worker_fingerprint": "fp-identical",
                "assistant_stream_text": "Still working",
                "complete": False,
            }
            if "working" in statuses
            else _final_turn("worker-1")
        ]
    }

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
        status="idle",
        fingerprint="fp-identical",
    )
    malformed = _worker(
        "worker-1",
        "malformed",
        version=1,
        space="space-1",
        status="idle",
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
    monkeypatch, topic_mode, tmp_path
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
            route = _typed_reply_route(store, tmp_path, message_id, topic_id)
            assert route.status is state.RouteStatus.BINDING_AMBIGUOUS
            assert route.binding_was_present is True
            assert route.owner is None and not route.worker_id
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

    assert initial["feed_sent"] == 0
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
    assert (
        recovery_tendwire.snapshot_calls,
        len(recovery_tendwire.delta_calls),
        recovery_tendwire.turn_calls,
        recovery_tendwire.pending_calls,
    ) == (1, 1, 0, 1)
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
    assert (
        tendwire.snapshot_calls,
        len(tendwire.delta_calls),
        tendwire.turn_calls,
        tendwire.pending_calls,
    ) == (1, 1, 0, 1)
    assert telegram.topics == ["telegram-bot"]
