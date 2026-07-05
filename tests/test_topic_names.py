"""cwd-based, one-topic-per-pane naming (RC): name a worker topic after its working directory
(/root/herdres -> "herdres") and disambiguate same-dir panes stably (gitmoot, gitmoot 2, ...)."""
from __future__ import annotations

from herdres_connector import source_sync, state


def _w(cwd, wid="w", name="claude"):
    return {"id": wid, "name": name, "meta": {"foreground_cwd": cwd, "cwd": cwd}}


def test_topic_name_from_cwd_basename():
    assert state.topic_name_for_worker(_w("/root/herdres")) == "herdres"
    assert state.topic_name_for_worker(_w("/root/bitcoin-price-pred")) == "bitcoin-price-pred"
    assert state.topic_name_for_worker(_w("/root/gitmoot/")) == "gitmoot"       # trailing slash


def test_topic_name_generic_cwd_falls_back_to_agent():
    assert state.topic_name_for_worker(_w("/root", name="claude")) == "claude"   # generic root
    assert state.topic_name_for_worker({"id": "x", "name": "codex", "meta": {}}) == "codex"  # no cwd
    assert state.topic_name_for_worker(_w("/tmp", name="claude")) == "claude"     # generic tmp


def test_assign_disambiguates_same_dir_panes_ordered_by_id():
    store = {"version": 2, "panes": {}, "spaces": {}}
    workers = [_w("/root/gitmoot", "claude-5"), _w("/root/gitmoot", "claude-1"),
               _w("/root/gitmoot", "claude-4"), _w("/root/herdres", "claude-3")]
    got = source_sync._assign_worker_topic_names(store, workers)
    assert got == {"claude-1": "gitmoot", "claude-4": "gitmoot 2", "claude-5": "gitmoot 3", "claude-3": "herdres"}


def test_assign_reserves_locked_names_and_numbers_around_them():
    # A gitmoot pane already has a created topic named "gitmoot"; a new gitmoot pane must not collide.
    store = {"version": 2, "spaces": {}, "panes": {
        "worker:claude-1": {"source": "tendwire", "entry_type": "worker", "tendwire_worker_id": "claude-1",
                             "topic_id": "500", "topic_name": "gitmoot"},
    }}
    workers = [_w("/root/gitmoot", "claude-1"), _w("/root/gitmoot", "claude-9")]
    got = source_sync._assign_worker_topic_names(store, workers)
    assert "claude-1" not in got                      # already topiced -> name locked, not reassigned
    assert got["claude-9"] == "gitmoot 2"             # new pane numbers around the locked "gitmoot"


def test_assign_dedup_is_case_insensitive():
    # _ensure_topic's reuse match casefolds, so "Foo"/"foo" must be numbered apart here too, else they
    # collapse into one topic. And a new pane must number around a locked topic that differs only in case.
    store = {"version": 2, "spaces": {}, "panes": {}}
    got = source_sync._assign_worker_topic_names(store, [_w("/root/Foo", "a"), _w("/root/foo", "b")])
    assert got["a"] == "Foo" and got["b"] == "foo 2"
    store2 = {"version": 2, "spaces": {}, "panes": {
        "worker:a": {"source": "tendwire", "entry_type": "worker", "tendwire_worker_id": "a",
                     "topic_id": "1", "topic_name": "Herdres"},
    }}
    got2 = source_sync._assign_worker_topic_names(store2, [_w("/root/herdres", "z")])
    assert got2["z"] == "herdres 2"                    # numbers around the case-different locked name
