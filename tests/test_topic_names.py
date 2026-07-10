"""cwd-based, one-topic-per-pane naming (RC): name a worker topic after its working directory
(/root/herdres -> "herdres") and disambiguate same-dir panes stably (gitmoot, gitmoot 2, ...)."""
from __future__ import annotations

from herdres_connector import source_sync, state
from test_source_only import _source_worker


def _worker(worker):
    result = dict(worker)
    meta = dict(result.get("meta") or {})
    identity = _source_worker({"id": result["id"]})["meta"]
    meta["stable_key"] = identity["stable_key"]
    meta["stable_key_version"] = identity["stable_key_version"]
    result["meta"] = meta
    return result


def _wentry(worker_id, **extra):
    identity = _worker({"id": worker_id})["meta"]
    return {
        "source": "tendwire",
        "entry_type": "worker",
        "tendwire_worker_id": worker_id,
        "tendwire_stable_key": identity["stable_key"],
        "tendwire_stable_key_version": identity["stable_key_version"],
        "tendwire_stable_identity_class": "current_v1",
        **extra,
    }


def _w(cwd, wid="w", name="claude"):
    return _worker({"id": wid, "name": name, "meta": {"foreground_cwd": cwd, "cwd": cwd}})


def test_topic_name_from_cwd_basename():
    assert state.topic_name_for_worker(_w("/root/herdres")) == "herdres"
    assert state.topic_name_for_worker(_w("/root/bitcoin-price-pred")) == "bitcoin-price-pred"
    assert state.topic_name_for_worker(_w("/root/gitmoot/")) == "gitmoot"       # trailing slash


def test_topic_name_generic_cwd_falls_back_to_agent():
    assert state.topic_name_for_worker(_w("/root", name="claude")) == "claude"   # generic root
    assert state.topic_name_for_worker(_worker({"id": "x", "name": "codex", "meta": {}})) == "codex"  # no cwd
    assert state.topic_name_for_worker(_w("/tmp", name="claude")) == "claude"     # generic tmp


def test_assign_disambiguates_same_dir_panes_ordered_by_id():
    store = {"version": 2, "panes": {}, "spaces": {}}
    workers = [_w("/root/gitmoot", "claude-5"), _w("/root/gitmoot", "claude-1"),
               _w("/root/gitmoot", "claude-4"), _w("/root/herdres", "claude-3")]
    got, _renames, _bases = source_sync._assign_worker_topic_names(store, workers)
    assert got == {"claude-1": "gitmoot", "claude-4": "gitmoot 2", "claude-5": "gitmoot 3", "claude-3": "herdres"}


def test_assign_reserves_locked_names_and_numbers_around_them():
    # A gitmoot pane already has a created topic named "gitmoot"; a new gitmoot pane must not collide.
    store = {"version": 2, "spaces": {}, "panes": {
        "worker:claude-1": _wentry("claude-1", topic_id="500", topic_name="gitmoot"),
    }}
    workers = [_w("/root/gitmoot", "claude-1"), _w("/root/gitmoot", "claude-9")]
    got, _renames, _bases = source_sync._assign_worker_topic_names(store, workers)
    assert "claude-1" not in got                      # already topiced -> name locked, not reassigned
    assert got["claude-9"] == "gitmoot 2"             # new pane numbers around the locked "gitmoot"


def test_assign_dedup_is_case_insensitive():
    # _ensure_topic's reuse match casefolds, so "Foo"/"foo" must be numbered apart here too, else they
    # collapse into one topic. And a new pane must number around a locked topic that differs only in case.
    store = {"version": 2, "spaces": {}, "panes": {}}
    got, _r, _bases = source_sync._assign_worker_topic_names(store, [_w("/root/Foo", "a"), _w("/root/foo", "b")])
    assert got["a"] == "Foo" and got["b"] == "foo 2"
    store2 = {"version": 2, "spaces": {}, "panes": {
        "worker:a": _wentry("a", topic_id="1", topic_name="Herdres"),
    }}
    got2, _renames2, _bases = source_sync._assign_worker_topic_names(store2, [_w("/root/herdres", "z")])
    assert got2["z"] == "herdres 2"                    # numbers around the case-different locked name


def _w_labeled(label, wid, cwd="/root/temp"):
    return _worker({"id": wid, "name": "claude", "meta": {"label": label, "foreground_cwd": cwd, "cwd": cwd}})


def test_topic_name_prefers_pane_label():
    assert state.topic_name_for_worker(_w_labeled("doro", "claude-7")) == "doro"
    # no label -> cwd basename fallback unchanged
    assert state.topic_name_for_worker(_w("/root/herdres", "claude-3")) == "herdres"


def test_assign_proposes_rename_when_label_appears():
    # topic was created under the cwd fallback name ("claude 2"); the pane label ("doro") now flows
    # through tendwire -> the entry is proposed for an in-place RENAME, not a new topic.
    store = {"version": 2, "spaces": {}, "panes": {
        "worker:claude-7": _wentry("claude-7", topic_id="9018", topic_name="claude 2"),
    }}
    assigned, renames, _bases = source_sync._assign_worker_topic_names(store, [_w_labeled("doro", "claude-7")])
    assert assigned == {}
    assert renames == {"claude-7": "doro"}


def test_assign_keeps_matching_names_and_numbered_variants():
    # names still matching their desired base (incl. "base N" variants) are kept — no rename churn.
    store = {"version": 2, "spaces": {}, "panes": {
        "worker:a": _wentry("a", topic_id="1", topic_name="gitmoot"),
        "worker:b": _wentry("b", topic_id="2", topic_name="gitmoot 2"),
    }}
    assigned, renames, _bases = source_sync._assign_worker_topic_names(
        store, [_w("/root/gitmoot", "a"), _w("/root/gitmoot", "b")])
    assert assigned == {} and renames == {}


def test_rename_candidates_old_name_stays_reserved():
    # While a rename is pending, a NEW pane must not take the old name (it would collide into the
    # old topic via _ensure_topic's reuse-by-name). It gets a numbered variant instead.
    store = {"version": 2, "spaces": {}, "panes": {
        "worker:a": _wentry("a", topic_id="1", topic_name="doro"),
    }}
    workers = [
        _worker({"id": "a", "name": "claude", "status": "active", "meta": {"label": "alpha"}}),  # relabeled
        _worker({"id": "b", "name": "claude", "status": "active", "meta": {"label": "doro"}}),   # new pane wants "doro"
    ]
    assigned, renames, _bases = source_sync._assign_worker_topic_names(store, workers)
    assert renames == {"a": "alpha"}
    assert assigned == {"b": "doro 2"}      # old name still reserved -> numbered, no collision


def test_rename_skips_closed_workers_and_capped_attempts():
    store = {"version": 2, "spaces": {}, "panes": {
        "worker:closed": _wentry("closed", topic_id="1", topic_name="oldname"),
        "worker:failing": _wentry(
            "failing",
            topic_id="2",
            topic_name="othername",
            rename_attempts=source_sync._RENAME_ATTEMPT_CAP,
        ),
    }}
    workers = [
        _worker({"id": "closed", "name": "claude", "status": "closed", "meta": {"label": "newname"}}),
        _worker({"id": "failing", "name": "claude", "status": "active", "meta": {"label": "wanted"}}),
    ]
    _assigned, renames, _bases = source_sync._assign_worker_topic_names(store, workers)
    assert renames == {}                     # closed pane + capped-attempts pane both skipped


def test_council_heuristic_never_matches_project_named_panes():
    # Live incident: a bare "gitmoot" marker matched regular panes whose topics are named after the
    # /root/gitmoot project dir, and done-council cleanup deleted their topics on every task finish.
    from herdres_connector.source_sync import _entry_is_council_topic
    assert _entry_is_council_topic({"topic_name": "gitmoot 2", "worker_name": "claude", "agent": "claude"}) is False
    assert _entry_is_council_topic({"topic_name": "Gitmoot2", "worker_name": "claude", "agent": "claude"}) is False
    assert _entry_is_council_topic({"topic_name": "audit-x", "worker_name": "gm-local-as-1", "agent": "agent"}) is True
    assert _entry_is_council_topic({"topic_name": "gitmoot · local-as", "worker_name": "", "agent": ""}) is True
    assert _entry_is_council_topic({"topic_name": "Council · 1Q", "worker_name": "", "agent": ""}) is True
