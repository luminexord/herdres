"""Model on the pinned board ("Claude · Fable 5"): tendwire now passes the adapter's `model` through
turn payloads; _sync_sources stamps it on entries (cache-and-keep) and rendering shows the suffix."""
from __future__ import annotations

from herdres_connector import config, state
from herdres_connector.rendering import pretty_model_label, render_status_overview
from herdres_connector.source_sync import SyncRuntime, sync_once

from test_source_only import FakeTelegram, FakeTendwire, _store


def test_pretty_model_label():
    assert pretty_model_label("claude-fable-5[1m]") == "Claude Fable 5"
    assert pretty_model_label("claude-opus-4-8") == "Claude Opus 4.8"
    assert pretty_model_label("gpt-5-codex") == "GPT-5 Codex"


def test_status_overview_shows_model_suffix():
    html = render_status_overview([
        {"agent": "claude", "label": "Claude", "status": "working", "model": "claude-fable-5[1m]"},
    ])
    assert "Fable 5" in html


def test_sync_stamps_model_from_turns(monkeypatch):
    monkeypatch.setenv("HERDRES_TENDWIRE_MODE", "source")
    monkeypatch.setenv("HERDRES_SOURCE_TOPIC_MODE", "worker")
    store = _store()
    tendwire = FakeTendwire(
        turns={"turns": [
            {"id": "t1", "worker_id": "worker-1", "worker_fingerprint": "fp-1",
             "assistant_final_text": "done", "complete": True, "model": "claude-fable-5[1m]"},
        ]},
    )
    sync_once(store, SyncRuntime(tendwire, FakeTelegram(), with_outbox=False))
    entry = next(iter(state.source_worker_entries(store).values()))
    assert entry.get("model") == "claude-fable-5[1m]"

    # cache-and-keep: a later sync whose turns carry no model must NOT clear it
    sync_once(store, SyncRuntime(FakeTendwire(turns={"turns": []}), FakeTelegram(), with_outbox=False))
    entry = next(iter(state.source_worker_entries(store).values()))
    assert entry.get("model") == "claude-fable-5[1m]"


# --- HERDRES_PINNED_STATUS opt-out ------------------------------------------

def test_pinned_status_flag_default_and_override():
    assert config.pinned_status_enabled(env={}) is True                              # default on
    assert config.pinned_status_enabled(env={"HERDRES_PINNED_STATUS": "0"}) is False
    assert config.pinned_status_enabled(env={"HERDRES_PINNED_STATUS": "off"}) is False


# --- HERDRES_INBOUND_SUCCESS_ACK opt-in -------------------------------------

def test_inbound_success_ack_flag_default_and_override():
    assert config.inbound_success_ack_enabled(env={}) is False
    assert config.inbound_success_ack_enabled(
        env={"HERDRES_INBOUND_SUCCESS_ACK": "1"}
    ) is True
    assert config.inbound_success_ack_enabled(
        env={"HERDRES_INBOUND_SUCCESS_ACK": "true"}
    ) is True
    assert config.inbound_success_ack_enabled(
        env={"HERDRES_INBOUND_SUCCESS_ACK": "0"}
    ) is False


def _space_for_pinned_status():
    return _store()


def test_pinned_status_disabled_skips_posters(monkeypatch):
    monkeypatch.setenv("HERDRES_TENDWIRE_MODE", "source")
    monkeypatch.setenv("HERDRES_PINNED_STATUS", "0")
    telegram = FakeTelegram()
    result = sync_once(_space_for_pinned_status(),
                       SyncRuntime(FakeTendwire(turns={"turns": []}), telegram, with_outbox=False))
    assert result["pinned_status_updated"] == 0   # neither poster ran
    assert telegram.pins == []                     # nothing pinned

    # Control: the identical setup DOES pin when the flag is on, so the assertions
    # above gate the posters rather than passing on an inert setup.
    monkeypatch.setenv("HERDRES_PINNED_STATUS", "1")
    telegram_on = FakeTelegram()
    result_on = sync_once(_space_for_pinned_status(),
                          SyncRuntime(FakeTendwire(turns={"turns": []}), telegram_on, with_outbox=False))
    assert result_on["pinned_status_updated"] >= 1
    assert telegram_on.pins
