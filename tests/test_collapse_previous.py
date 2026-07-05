"""Collapse-previous-responses (monolith port, first time working in source mode): superseded finals
get their Response folded into a closed <details> so only the newest answer stays expanded. Opt-in via
HERDR_TELEGRAM_TOPICS_RESPONSE_COLLAPSE_PREVIOUS (the user's env sets =1)."""
from __future__ import annotations

from herdres_connector import config, source_sync, state
from herdres_connector.rich_delivery import render_turn_item_html
from herdres_connector.source_sync import SyncRuntime, _sync_turns

from test_source_only import FakeTelegram, FakeTendwire, _store


# --- flag ---------------------------------------------------------------------

def test_collapse_flag_default_off_env_on():
    assert config.response_collapse_previous_default(env={}) is False
    assert config.response_collapse_previous_default(env={"HERDR_TELEGRAM_TOPICS_RESPONSE_COLLAPSE_PREVIOUS": "1"}) is True


# --- renderer -------------------------------------------------------------------

def _turn_item(**extra):
    item = {"kind": "turn", "user_text": "do the thing", "assistant_final_text": "All done. The result is ready."}
    item.update(extra)
    return item


def test_render_open_by_default():
    html = render_turn_item_html(_turn_item())
    assert "All done." in html
    # the Response is the open top-level body, not wrapped in a details card
    assert "<b>✅ Response</b><br><br>" in html


def test_render_collapsed_when_flagged():
    html = render_turn_item_html(_turn_item(collapse_response=True))
    assert html.startswith("<details><summary>✅")         # Response card is CLOSED (no ` open` attr)
    assert "<details open><summary>✅" not in html          # (the prompt section may be open — that's fine)
    assert "Response" in html
    assert "All done." in html                             # preview and/or body retain the text


# --- the fold sweep in _sync_turns ---------------------------------------------

def _two_turn_payload():
    # rows are per-worker recency ordered: newest FIRST (pass 1 setdefault picks the latest)
    return {"turns": [
        {"id": "turn-new", "worker_id": "w1", "worker_fingerprint": "fp1",
         "assistant_final_text": "New answer", "complete": True},
        {"id": "turn-old", "worker_id": "w1", "worker_fingerprint": "fp1",
         "assistant_final_text": "Old answer text", "complete": True},
    ]}


def _folded_store(monkeypatch):
    monkeypatch.setenv("HERDRES_SOURCE_TOPIC_MODE", "worker")
    store = _store()
    store["panes"]["worker:w1"] = {
        "source": "tendwire", "entry_type": "worker", "tendwire_worker_id": "w1",
        "tendwire_space_id": "s1", "topic_id": "77",
        "last_clean_message_id": "501",       # the NEW final's message (never folded)
        "last_clean_hash": "irrelevant",
    }
    # both turns already delivered + bound (the sweep edits the OLD one)
    state.mark_delivered(store, "final:turn-new:whatever", {"worker_id": "w1", "turn_id": "turn-new"})
    state.mark_delivered(store, "final:turn-old:whatever", {"worker_id": "w1", "turn_id": "turn-old"})
    state.bind_message_to_worker(store, "400", store["panes"]["worker:w1"], topic_id="77", kind="final", turn_id="turn-old")
    state.bind_message_to_worker(store, "501", store["panes"]["worker:w1"], topic_id="77", kind="final", turn_id="turn-new")
    return store


def _run(store, monkeypatch, flag="1"):
    monkeypatch.setenv("HERDR_TELEGRAM_TOPICS_RESPONSE_COLLAPSE_PREVIOUS", flag)
    telegram = FakeTelegram()
    runtime = SyncRuntime(FakeTendwire(), telegram, with_outbox=False)
    _sync_turns(store, _two_turn_payload(), {"pending": []}, runtime, chat_id="-100")
    return telegram


def test_superseded_final_gets_folded(monkeypatch):
    store = _folded_store(monkeypatch)
    telegram = _run(store, monkeypatch)
    # the OLD message (400) was edited into a collapsed rendering
    edited = [(mid, html) for _chat, mid, html in telegram.edited]
    assert any(mid == "400" and "<details><summary>" in html and "Old answer text" in html for mid, html in edited)
    assert not any(mid == "501" for mid, _ in edited)                 # latest never folded
    assert state.message_bindings(store)["400"].get("folded") is True  # idempotency marker


def test_fold_idempotent_second_sweep_skips(monkeypatch):
    store = _folded_store(monkeypatch)
    _run(store, monkeypatch)
    telegram2 = _run(store, monkeypatch)                  # second sweep
    assert not any(mid == "400" for _c, mid, _h in telegram2.edited)  # already folded -> no re-edit


def test_fold_disabled_without_flag(monkeypatch):
    store = _folded_store(monkeypatch)
    telegram = _run(store, monkeypatch, flag="0")
    assert not any(mid == "400" for _c, mid, _h in telegram.edited)
    assert state.message_bindings(store)["400"].get("folded") is None


def test_fold_failure_bounded_by_attempt_cap(monkeypatch):
    store = _folded_store(monkeypatch)
    monkeypatch.setenv("HERDR_TELEGRAM_TOPICS_RESPONSE_COLLAPSE_PREVIOUS", "1")

    class _FailingTelegram(FakeTelegram):
        def api(self, method, payload):
            if method == "editMessageText":
                raise __import__("herdres_connector.telegram_delivery", fromlist=["TelegramError"]).TelegramError("boom")
            return super().api(method, payload)

        def edit_message(self, chat_id, message_id, html):
            return {"ok": False, "error": "boom"}   # block the plain-text fallback too

    telegram = _FailingTelegram()
    runtime = SyncRuntime(FakeTendwire(), telegram, with_outbox=False)
    for _ in range(5):
        _sync_turns(store, _two_turn_payload(), {"pending": []}, runtime, chat_id="-100")
    binding = state.message_bindings(store)["400"]
    assert binding.get("folded") is None
    assert int(binding.get("fold_attempts") or 0) == source_sync._FOLD_ATTEMPT_CAP   # gave up at the cap
