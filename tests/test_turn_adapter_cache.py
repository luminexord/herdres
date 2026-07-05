"""herdr_turn_adapter pane-list cache: tendwire captures turns for ~15 panes concurrently each cycle
and every capture used to run its own `herdr pane list` (twice) at seconds per call under load — a
storm that wedged the tendwire daemon (submits then failed with "Could not send safely"). The cache
lets all concurrent captures share one listing."""
from __future__ import annotations

import importlib.util
import json
from pathlib import Path

_SPEC = importlib.util.spec_from_file_location(
    "herdr_turn_adapter", Path(__file__).resolve().parent.parent / "herdr_turn_adapter.py"
)
adapter = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(adapter)


def _fake_listing(marker):
    return {"result": {"panes": [{"pane_id": "p1", "marker": marker}]}}


def test_cache_shares_one_listing(tmp_path, monkeypatch):
    calls = []
    monkeypatch.setattr(adapter, "PANE_LIST_CACHE", str(tmp_path / "pane_list.json"))
    monkeypatch.setenv("HERDR_ADAPTER_PANE_LIST_TTL", "60")

    def fake_run(args):
        calls.append(args)
        return _fake_listing("fresh")

    monkeypatch.setattr(adapter, "run_real_herdr_json", fake_run)
    first = adapter.cached_pane_list_json()
    second = adapter.cached_pane_list_json()   # within TTL -> served from the cache file
    assert first == second == _fake_listing("fresh")
    assert len(calls) == 1                     # only ONE real herdr call for both reads


def test_cache_expires_after_ttl(tmp_path, monkeypatch):
    cache = tmp_path / "pane_list.json"
    monkeypatch.setattr(adapter, "PANE_LIST_CACHE", str(cache))
    monkeypatch.setenv("HERDR_ADAPTER_PANE_LIST_TTL", "60")
    cache.write_text(json.dumps(_fake_listing("stale")))
    import os
    old = 10_000  # epoch seconds long past
    os.utime(cache, (old, old))
    monkeypatch.setattr(adapter, "run_real_herdr_json", lambda args: _fake_listing("fresh"))
    assert adapter.cached_pane_list_json() == _fake_listing("fresh")   # expired -> refetched
    assert json.loads(cache.read_text()) == _fake_listing("fresh")     # cache updated


def test_ttl_zero_disables_cache(tmp_path, monkeypatch):
    calls = []
    monkeypatch.setattr(adapter, "PANE_LIST_CACHE", str(tmp_path / "pane_list.json"))
    monkeypatch.setenv("HERDR_ADAPTER_PANE_LIST_TTL", "0")
    monkeypatch.setattr(adapter, "run_real_herdr_json", lambda args: calls.append(1) or _fake_listing("x"))
    adapter.cached_pane_list_json()
    adapter.cached_pane_list_json()
    assert len(calls) == 2                     # no caching, no cache file
    assert not (tmp_path / "pane_list.json").exists()


def test_corrupt_cache_falls_back_to_cli(tmp_path, monkeypatch):
    cache = tmp_path / "pane_list.json"
    monkeypatch.setattr(adapter, "PANE_LIST_CACHE", str(cache))
    monkeypatch.setenv("HERDR_ADAPTER_PANE_LIST_TTL", "60")
    cache.write_text("not json{{{")
    monkeypatch.setattr(adapter, "run_real_herdr_json", lambda args: _fake_listing("fresh"))
    assert adapter.cached_pane_list_json() == _fake_listing("fresh")   # fail-open


def test_pane_from_list_uses_cache(tmp_path, monkeypatch):
    monkeypatch.setattr(adapter, "PANE_LIST_CACHE", str(tmp_path / "pane_list.json"))
    monkeypatch.setenv("HERDR_ADAPTER_PANE_LIST_TTL", "60")
    monkeypatch.setattr(adapter, "run_real_herdr_json", lambda args: _fake_listing("fresh"))
    pane = adapter.pane_from_list("p1")
    assert pane == {"pane_id": "p1", "marker": "fresh"}
