"""Worker-mode topic dedup: the reaper for stranded closed-worker topics (HERDRES_REAP_CLOSED_WORKER_TOPICS)
and the de-numbering of the live sibling's " N" suffix.

Background: herdr/tendwire re-letters worker ids positionally across restarts (claude-2 -> claude-2-2 for
a fresh terminal), so the connector mints a new topic for the re-registered pane while the old one strands
(worker-mode cleanup otherwise only deletes done-council topics). The old topic keeps its name reserved,
forcing the live pane to a "telegram-bot 2" suffix. These tests pin the opt-in reaper (STRICT closed/failed
liveness — never 'done'/'idle' — + N-pass absence + non-degraded-snapshot + delete-cap guards) and the
de-numbering, which fires only for a connector_numbered_base marker stamped at NUMBERING time (the moment
the connector itself minted the " N"). That numbering-time provenance is the guard that a user-authored
"Sonnet 4"-style label is never silently collapsed; the reaper never stamps markers by name-pattern.
"""
from __future__ import annotations

import pytest

from herdres_connector import config, state
from herdres_connector.source_sync import (
    SyncRuntime,
    _REAP_ABSENCE_STREAK,
    _assign_worker_topic_names,
    _cleanup_topics,
    sync_once,
)

from test_source_only import FakeTelegram, FakeTendwire, _source_worker, _store


@pytest.fixture(autouse=True)
def _worker_mode(monkeypatch):
    monkeypatch.setenv("HERDRES_TENDWIRE_MODE", "source")
    monkeypatch.setenv("HERDRES_SOURCE_TOPIC_MODE", "worker")


def _worker(worker):
    result = dict(worker)
    meta = dict(result.get("meta") or {})
    identity = _source_worker({"id": result["id"]})["meta"]
    meta["stable_key"] = identity["stable_key"]
    meta["stable_key_version"] = identity["stable_key_version"]
    result["meta"] = meta
    return result


def _wentry(worker_id, topic_id=None, topic_name="x", status="closed", **extra):
    identity = _worker({"id": worker_id})["meta"]
    entry = {
        "source": "tendwire",
        "entry_type": "worker",
        "tendwire_worker_id": worker_id,
        "tendwire_stable_key": identity["stable_key"],
        "tendwire_stable_key_version": identity["stable_key_version"],
        "tendwire_stable_identity_class": "current_v1",
        "worker_id": worker_id,
        "worker_name": "claude",
        "topic_name": topic_name,
        "status": status,
        "tendwire_raw_status": status,
        "tendwire_space_id": "w1",
    }
    if topic_id is not None:
        entry["topic_id"] = str(topic_id)
    entry.update(extra)
    return entry


def _store_with(entries):
    store = _store()
    store["panes"] = {f"worker:{e['tendwire_worker_id']}:h{i}": e for i, e in enumerate(entries)}
    return store


def _runtime(dry_run=False):
    return SyncRuntime(FakeTendwire(), FakeTelegram(), dry_run=dry_run, with_outbox=False)


def _clean(store, rt, present, times=1):
    result = None
    for _ in range(times):
        result = _cleanup_topics(store, rt, chat_id="-100", snapshot_worker_ids=set(present))
    return result


# --- reaper gating -----------------------------------------------------------

def test_reaper_off_by_default():
    # flag unset -> never delete, even a finished+absent worker's topic.
    store = _store_with([_wentry("claude", 124, "claude", "closed")])
    rt = _runtime()
    _clean(store, rt, present={"claude-live"}, times=_REAP_ABSENCE_STREAK + 2)
    assert rt.telegram.deleted_topics == []
    assert len(state.source_worker_entries(store)) == 1


def test_reaper_deletes_finished_absent_after_streak(monkeypatch):
    monkeypatch.setenv("HERDRES_REAP_CLOSED_WORKER_TOPICS", "1")
    store = _store_with([_wentry("claude", 124, "claude", "closed")])
    rt = _runtime()
    # streak-1 absent passes only accrue the miss counter; nothing is deleted yet.
    _clean(store, rt, present={"other"}, times=_REAP_ABSENCE_STREAK - 1)
    assert rt.telegram.deleted_topics == []
    assert len(state.source_worker_entries(store)) == 1
    # the streak-th absent pass reaps the topic and prunes the entry.
    _clean(store, rt, present={"other"})
    assert rt.telegram.deleted_topics == ["124"]
    assert state.source_worker_entries(store) == {}


def test_reaper_keeps_present_finished_worker(monkeypatch):
    # A finished worker STILL in the snapshot has not left — never reaped, streak stays reset.
    monkeypatch.setenv("HERDRES_REAP_CLOSED_WORKER_TOPICS", "1")
    store = _store_with([_wentry("claude-2", 26, "telegram-bot", "closed")])
    rt = _runtime()
    _clean(store, rt, present={"claude-2"}, times=_REAP_ABSENCE_STREAK + 2)
    assert rt.telegram.deleted_topics == []
    assert "reap_miss_count" not in next(iter(state.source_worker_entries(store).values()))


def test_reaper_never_touches_unfinished_absent(monkeypatch):
    # Absent from the snapshot but not durably finished (still working) -> never reaped, even for many passes.
    monkeypatch.setenv("HERDRES_REAP_CLOSED_WORKER_TOPICS", "1")
    store = _store_with([_wentry("ghost", 50, "foo", "working")])
    rt = _runtime()
    _clean(store, rt, present={"other"}, times=_REAP_ABSENCE_STREAK + 3)
    assert rt.telegram.deleted_topics == []
    assert len(state.source_worker_entries(store)) == 1


def test_reaper_never_reaps_done_idle_absent(monkeypatch):
    # CRITICAL: herdr reports agent_status='done' for a pane that merely finished its last turn while the
    # terminal stays OPEN — an ordinary idle agent (normalized_status('done') == 'idle', _worker_is_open
    # True). Such a LIVE pane blipping out of the snapshot must NEVER be reaped, even past the streak;
    # only a genuinely closed/failed pane is reap-eligible. A closed absent pane in the same store IS
    # reaped, proving the gate discriminates by real liveness, not by the finished-council predicate.
    monkeypatch.setenv("HERDRES_REAP_CLOSED_WORKER_TOPICS", "1")
    store = _store_with([
        _wentry("anchor", 299, "anchor", "working"),    # live anchor stays present -> snapshot stays healthy
        _wentry("done-pane", 300, "alpha", "done"),     # LIVE idle agent (finished a turn), absent this pass
        _wentry("closed-pane", 301, "beta", "closed"),  # genuinely gone, absent this pass
    ])
    rt = _runtime()
    _clean(store, rt, present={"anchor"}, times=_REAP_ABSENCE_STREAK + 2)
    assert "300" not in rt.telegram.deleted_topics    # 'done' (idle, live) pane never reaped
    assert "301" in rt.telegram.deleted_topics         # closed pane reaped after the streak
    survivors = {e["tendwire_worker_id"] for e in state.source_worker_entries(store).values()}
    assert "done-pane" in survivors and "closed-pane" not in survivors
    done_entry = next(e for e in state.source_worker_entries(store).values() if e["tendwire_worker_id"] == "done-pane")
    assert "reap_miss_count" not in done_entry         # a live 'done' pane never even accrues the streak


def test_reaper_empty_snapshot_guard(monkeypatch):
    # A transient fully-empty snapshot must never mass-reap while entries exist.
    monkeypatch.setenv("HERDRES_REAP_CLOSED_WORKER_TOPICS", "1")
    store = _store_with([_wentry("claude", 124, "claude", "closed")])
    rt = _runtime()
    _clean(store, rt, present=set(), times=_REAP_ABSENCE_STREAK + 2)
    assert rt.telegram.deleted_topics == []
    assert len(state.source_worker_entries(store)) == 1


def test_reaper_partial_snapshot_guard(monkeypatch):
    # A DEGRADED/partial snapshot (some workers transiently missing) that shows NONE of the known-LIVE
    # workers is untrustworthy: the absent closed entry must not march toward a reap on it. Only once a
    # known-live worker reappears does the pass count as healthy and the streak advance.
    monkeypatch.setenv("HERDRES_REAP_CLOSED_WORKER_TOPICS", "1")
    store = _store_with([
        _wentry("live-1", 200, "alpha", "idle"),      # a known-live anchor
        _wentry("claude", 124, "claude", "closed"),   # stranded, absent
    ])
    rt = _runtime()
    # Degraded passes: the snapshot shows only an unrelated id, none of our known-live workers -> skip.
    _clean(store, rt, present={"stranger"}, times=_REAP_ABSENCE_STREAK + 2)
    assert rt.telegram.deleted_topics == []
    assert all("reap_miss_count" not in e for e in state.source_worker_entries(store).values())
    # Healthy passes: the live anchor is back in the snapshot -> the absent closed entry reaps after the streak.
    _clean(store, rt, present={"live-1"}, times=_REAP_ABSENCE_STREAK)
    assert rt.telegram.deleted_topics == ["124"]


def test_reaper_disabled_in_space_mode(monkeypatch):
    # Space mode has its own stale-topic cleanup; assert only that the WORKER reaper never fired.
    monkeypatch.setenv("HERDRES_REAP_CLOSED_WORKER_TOPICS", "1")
    monkeypatch.setenv("HERDRES_SOURCE_TOPIC_MODE", "space")
    store = _store_with([_wentry("claude", 124, "claude", "closed")])
    rt = _runtime()
    _clean(store, rt, present={"other"}, times=_REAP_ABSENCE_STREAK + 2)
    reasons = {a.get("reason") for a in store.get("telegram_deleted_topics", [])}
    assert "reaped_closed_worker_topic" not in reasons
    assert all("reap_miss_count" not in e for e in state.source_worker_entries(store).values())


def test_reaper_respects_delete_cap(monkeypatch):
    monkeypatch.setenv("HERDRES_REAP_CLOSED_WORKER_TOPICS", "1")
    monkeypatch.setenv("HERDR_TELEGRAM_TOPICS_SOURCE_DELETE_LIMIT", "2")
    entries = [_wentry(f"claude-{i}", 100 + i, f"proj-{i}", "closed") for i in range(5)]
    store = _store_with(entries)
    rt = _runtime()
    # warm the streak, then one reaping pass: only the cap (2) is deleted this tick.
    _clean(store, rt, present={"live"}, times=_REAP_ABSENCE_STREAK)
    assert len(rt.telegram.deleted_topics) == 2
    # remaining eligible topics reap on subsequent ticks (amortized).
    _clean(store, rt, present={"live"}, times=3)
    assert sorted(rt.telegram.deleted_topics) == ["100", "101", "102", "103", "104"]
    assert state.source_worker_entries(store) == {}


def test_reaper_dry_run_previews_without_side_effects(monkeypatch):
    monkeypatch.setenv("HERDRES_REAP_CLOSED_WORKER_TOPICS", "1")
    store = _store_with([_wentry("claude", 124, "claude", "closed")])
    rt = _runtime(dry_run=True)
    result = _cleanup_topics(store, rt, chat_id="-100", snapshot_worker_ids={"other"})
    assert result["deleted"] == 1              # previewed (no streak wait)
    assert rt.telegram.deleted_topics == []    # nothing actually deleted
    assert len(state.source_worker_entries(store)) == 1
    assert "reap_miss_count" not in next(iter(state.source_worker_entries(store).values()))


# --- numbering-time provenance + de-numbering -------------------------------

def test_numbering_time_provenance_marks_minted_suffix(monkeypatch):
    # A NEW pane whose desired base is reserved by a stranded namesake is minted "<base> N"; the connector
    # records connector_numbered_base at THAT numbering time (via _sync_sources), NOT by the reaper — this
    # is the true provenance a later de-number relies on.
    monkeypatch.setenv("HERDRES_PINNED_STATUS", "0")
    store = _store_with([_wentry("claude-2", 26, "telegram-bot", "closed")])   # stranded, holds the base
    worker = _worker({"id": "claude-2-2", "name": "claude", "status": "working", "space_id": "w1",
                      "fingerprint": "fp", "meta": {"label": "telegram-bot"}})
    telegram = FakeTelegram()
    sync_once(store, SyncRuntime(FakeTendwire(workers=[worker]), telegram, with_outbox=False))
    live = next(e for e in state.source_worker_entries(store).values() if e["tendwire_worker_id"] == "claude-2-2")
    assert live.get("topic_name") == "telegram-bot 2"             # numbered around the reserved base
    assert live.get("connector_numbered_base") == "telegram-bot"  # provenance stamped at mint time


def test_reap_frees_base_then_denumbers_minted_sibling(monkeypatch):
    # End-to-end: the stranded closed namesake reaps, freeing the base; the live sibling that the connector
    # minted "<base> 2" (carrying the numbering-time marker) de-numbers back to the bare base — no reaper
    # name-pattern stamping involved.
    monkeypatch.setenv("HERDRES_REAP_CLOSED_WORKER_TOPICS", "1")
    monkeypatch.setenv("HERDRES_PINNED_STATUS", "0")
    store = _store_with([
        _wentry("claude-2", 26, "telegram-bot", "closed"),                    # stranded, absent
        _wentry("claude-2-2", 184, "telegram-bot 2", "working",
                connector_numbered_base="telegram-bot"),                       # minted sibling w/ provenance
    ])
    worker = _worker({"id": "claude-2-2", "name": "claude", "status": "working", "space_id": "w1",
                      "fingerprint": "fp", "meta": {"label": "telegram-bot"}})
    telegram = FakeTelegram()
    for _ in range(_REAP_ABSENCE_STREAK + 2):
        sync_once(store, SyncRuntime(FakeTendwire(workers=[worker]), telegram, with_outbox=False))
    assert "26" in telegram.deleted_topics
    live = next(e for e in state.source_worker_entries(store).values() if e["tendwire_worker_id"] == "claude-2-2")
    assert live.get("topic_name") == "telegram-bot"      # de-numbered back to the freed base
    assert "connector_numbered_base" not in live          # marker cleared after the rename


def test_assign_stamps_numbered_base_for_new_pane():
    # Unit-level provenance: a not-yet-topiced pane forced to "<base> 2" is returned in numbered_bases,
    # while the bare-base pane and a genuinely unique base are NOT (nothing was minted onto them).
    store = _store_with([_wentry("w-a", 90, "foo", "working")])  # w-a already holds "foo"
    workers = [
        _worker({"id": "w-a", "name": "claude", "status": "working", "space_id": "w1", "meta": {"label": "foo"}}),
        _worker({"id": "w-b", "name": "claude", "status": "working", "space_id": "w1", "meta": {"label": "foo"}}),
        _worker({"id": "w-c", "name": "claude", "status": "working", "space_id": "w1", "meta": {"label": "bar"}}),
    ]
    _assigned, _renames, numbered_bases = _assign_worker_topic_names(store, workers)
    assert numbered_bases == {"w-b": "foo"}   # only the minted "foo 2" carries provenance


def test_denumber_requires_the_marker():
    # A live worker whose desired name IS its numbered label, without the numbering-time marker, is left
    # alone — this is the guard that a user-authored "telegram-bot 2" label is never silently collapsed.
    worker = _worker({"id": "claude-2-2", "name": "claude", "status": "working", "space_id": "w1",
                      "meta": {"label": "telegram-bot 2"}})
    store = _store_with([_wentry("claude-2-2", 184, "telegram-bot 2", "working")])
    _assigned, renames, _bases = _assign_worker_topic_names(store, [worker])
    assert "claude-2-2" not in renames

    store = _store_with([_wentry("claude-2-2", 184, "telegram-bot 2", "working",
                                 connector_numbered_base="telegram-bot")])
    _assigned, renames, _bases = _assign_worker_topic_names(store, [worker])
    assert renames.get("claude-2-2") == "telegram-bot"


def test_denumber_holds_off_while_base_still_taken():
    # Marker present but another live topic still holds the bare base -> do NOT de-number (would dup the name).
    store = _store_with([
        _wentry("claude-x", 26, "telegram-bot", "working"),       # live holder of the bare base
        _wentry("claude-2-2", 184, "telegram-bot 2", "working", connector_numbered_base="telegram-bot"),
    ])
    workers = [
        _worker({"id": "claude-x", "name": "claude", "status": "working", "space_id": "w1", "meta": {"label": "telegram-bot"}}),
        _worker({"id": "claude-2-2", "name": "claude", "status": "working", "space_id": "w1", "meta": {"label": "telegram-bot 2"}}),
    ]
    _assigned, renames, _bases = _assign_worker_topic_names(store, workers)
    assert "claude-2-2" not in renames


def test_reaper_never_stamps_denumber_marker_by_name_pattern(monkeypatch):
    # Provenance is numbering-time only. When the reaper deletes a stranded "<base>" topic it must NOT
    # stamp connector_numbered_base on a live sibling merely because its name matches "<base> N" — that
    # name-pattern heuristic (now removed) could collapse a user's own "Sonnet 4" label. Only a mint stamps.
    monkeypatch.setenv("HERDRES_REAP_CLOSED_WORKER_TOPICS", "1")
    store = _store_with([
        _wentry("claude-2", 26, "Sonnet", "closed"),     # stranded namesake "Sonnet" -> reaped
        _wentry("sonnet-b", 184, "Sonnet 4", "working"),  # user-authored label matching "Sonnet N", NO marker
    ])
    rt = _runtime()
    _clean(store, rt, present={"sonnet-b"}, times=_REAP_ABSENCE_STREAK)
    assert rt.telegram.deleted_topics == ["26"]           # stranded "Sonnet" reaped
    live = next(e for e in state.source_worker_entries(store).values() if e["tendwire_worker_id"] == "sonnet-b")
    assert "connector_numbered_base" not in live          # reaper never stamps by name-pattern


def test_normal_numbering_unchanged():
    # Regression: with no reaper marker, two genuinely-distinct live panes sharing a base still get
    # "foo"/"foo 2" (the disambiguation the design must preserve), and a unique base stays bare.
    store = _store_with([_wentry("w-a", 90, "foo", "working")])  # w-a already holds "foo"
    workers = [
        _worker({"id": "w-a", "name": "claude", "status": "working", "space_id": "w1", "meta": {"label": "foo"}}),
        _worker({"id": "w-b", "name": "claude", "status": "working", "space_id": "w1", "meta": {"label": "foo"}}),
        _worker({"id": "w-c", "name": "claude", "status": "working", "space_id": "w1", "meta": {"label": "bar"}}),
    ]
    assigned, renames, _bases = _assign_worker_topic_names(store, workers)
    assert assigned.get("w-b") == "foo 2"   # distinct same-base live pane still numbered
    assert assigned.get("w-c") == "bar"     # unique base stays bare
    assert "w-a" not in renames             # existing holder untouched


# --- end-to-end heal over the real strand shape ------------------------------

def _live_state_fixture():
    """Mirror of a live state.json worker set: 7 stranded CLOSED entries + 4 LIVE ones."""
    return _store_with([
        _wentry("claude", 124, "claude", "closed"),
        _wentry("claude-1", 24, "whisp-flow", "closed"),
        _wentry("claude-2", 26, "telegram-bot", "closed"),
        _wentry("claude-3", 28, "whispr-bro", "closed"),
        _wentry("claude-4", 38, "brewfather", "closed"),
        _wentry("claude-5", 40, "log-in", "closed"),
        _wentry("claude-6", 42, "usage 2", "closed"),
        _wentry("claude-1-1", 177, "usage", "idle"),
        _wentry("claude-1-2", 179, "whisp-flow 2", "idle"),
        _wentry("claude-2-1", 181, "brewable", "idle"),
        _wentry("claude-2-2", 184, "telegram-bot 2", "working"),
    ])


def _live_workers():
    def w(wid, cwd, status="idle"):
        return _worker({"id": wid, "name": "claude", "status": status, "space_id": "w1",
                        "fingerprint": f"fp-{wid}", "meta": {"cwd": cwd, "foreground_cwd": cwd}})
    return [
        w("claude-1-1", "/repo"),
        w("claude-1-2", "/repo/whispr-bro"),
        w("claude-2-1", "/repo/brewfather"),
        w("claude-2-2", "/repo/telegram-remote", status="working"),
    ]


def test_full_heal_over_live_state(monkeypatch):
    monkeypatch.setenv("HERDRES_REAP_CLOSED_WORKER_TOPICS", "1")
    monkeypatch.setenv("HERDRES_PINNED_STATUS", "0")
    monkeypatch.setenv("HERDR_TELEGRAM_TOPICS_SOURCE_DELETE_LIMIT", "20")
    store = _live_state_fixture()
    telegram = FakeTelegram()

    def _pass():
        return sync_once(store, SyncRuntime(FakeTendwire(workers=_live_workers()), telegram, with_outbox=False))

    for _ in range(_REAP_ABSENCE_STREAK + 3):
        _pass()

    # the 7 stranded closed topics are gone; the 4 live entries survive.
    assert sorted(telegram.deleted_topics) == ["124", "24", "26", "28", "38", "40", "42"]
    survivors = {e["tendwire_worker_id"]: e for e in state.source_worker_entries(store).values()}
    assert set(survivors) == {"claude-1-1", "claude-1-2", "claude-2-1", "claude-2-2"}

    # the strand-forced " N" suffixes are gone from the live survivors once their base names freed
    # (here they heal all the way to the current cwd basename, since this fixture names by cwd).
    names = {e.get("topic_name") for e in survivors.values()}
    assert "telegram-bot 2" not in names and "whisp-flow 2" not in names
    assert all("connector_numbered_base" not in e for e in survivors.values())  # markers resolved

    # idempotent: one more pass makes no further deletes (the strays are already gone).
    before_del = list(telegram.deleted_topics)
    _pass()
    assert telegram.deleted_topics == before_del
