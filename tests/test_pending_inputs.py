"""Number-replies to captured input prompts: a bare digit is validated against the worker's LIVE
pending (question + choices from the backend capture) and fails closed on stale/out-of-range/custom;
non-numeric text and choice-less pendings pass through unchanged."""
from __future__ import annotations

from unittest.mock import patch

import herdres
from herdres_connector import state

from test_source_only import _store


def _setup(tmp_path, monkeypatch):
    monkeypatch.setenv("HERDRES_TENDWIRE_MODE", "source")
    monkeypatch.setenv("HERDRES_SOURCE_TOPIC_MODE", "worker")
    monkeypatch.setenv("HERDR_TELEGRAM_TOPICS_STATE", str(tmp_path / "state.json"))
    store = _store()
    store["panes"]["worker:w1"] = {"source": "tendwire", "entry_type": "worker", "tendwire_worker_id": "w1",
                                   "tendwire_space_id": "s1", "topic_id": "77"}
    state.save_state(store)


def _pending(choices):
    return {"pending_interactions": [
        {"worker_id": "w1", "status": "open", "question": "Which db?", "choices": choices},
    ]}


_CHOICES = [
    {"choice_id": "1", "label": "Postgres", "value": "Postgres"},
    {"choice_id": "2", "label": "SQLite", "value": "SQLite"},
    {"choice_id": "custom", "label": "Tell me differently", "value": ""},
]


def _reply(text, pending):
    sent = {}

    def fake_command(self, request):
        sent.update(request)
        return {"ok": True, "status": "accepted", "result": {"delivery_state": "submitted"}}

    with patch.object(herdres.TendwireClient, "command", fake_command), \
            patch.object(herdres.TendwireClient, "pending", lambda self: pending):
        result = herdres.command_reply({"topic_id": "77", "user_id": "1", "text": text})
    return result, sent


def test_valid_number_sends_digit(tmp_path, monkeypatch):
    _setup(tmp_path, monkeypatch)
    result, sent = _reply("2", _pending(_CHOICES))
    assert sent["instruction"]["text"] == "2"          # the picker's native input
    assert result["handled"] is True


def test_out_of_range_fails_closed(tmp_path, monkeypatch):
    _setup(tmp_path, monkeypatch)
    result, sent = _reply("7", _pending(_CHOICES))
    assert "1–3" in result["reply"]
    assert not sent                                     # nothing submitted


def test_custom_choice_asks_for_text(tmp_path, monkeypatch):
    _setup(tmp_path, monkeypatch)
    result, sent = _reply("3", _pending(_CHOICES))
    assert "custom answer" in result["reply"]
    assert not sent


def test_number_passes_through_without_live_pending(tmp_path, monkeypatch):
    _setup(tmp_path, monkeypatch)
    _result, sent = _reply("2", {"pending_interactions": []})
    assert sent["instruction"]["text"] == "2"           # unchanged behavior


def test_number_passes_through_for_choiceless_pending(tmp_path, monkeypatch):
    _setup(tmp_path, monkeypatch)
    _result, sent = _reply("1", _pending([]))
    assert sent["instruction"]["text"] == "1"


def test_non_numeric_text_untouched(tmp_path, monkeypatch):
    _setup(tmp_path, monkeypatch)
    _result, sent = _reply("yes please", _pending(_CHOICES))
    assert sent["instruction"]["text"] == "yes please"
