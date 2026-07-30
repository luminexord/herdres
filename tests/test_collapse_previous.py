"""Collapse-previous-responses (monolith port, first time working in source mode): superseded finals
get their Response folded into an expandable blockquote so only the newest answer stays expanded. Opt-in via
HERDR_TELEGRAM_TOPICS_RESPONSE_COLLAPSE_PREVIOUS (the user's env sets =1)."""
from __future__ import annotations

from herdres_connector import config, source_sync, state
from herdres_connector.managed_bots import MANAGER_BOT_KIND
from herdres_connector.rich_delivery import render_turn_item_html
from herdres_connector.source_sync import SyncRuntime, _sync_turns
from herdres_connector.telegram_delivery import TelegramError

from test_source_only import FakeTelegram, FakeTendwire, _source_worker, _store


import pytest


@pytest.fixture(autouse=True)
def _plain_fallback_by_default(monkeypatch):
    """Legacy collapse assertions describe the force-plain fallback."""

    monkeypatch.setenv("HERDRES_FORCE_PLAIN_DELIVERY", "1")


# --- flag ---------------------------------------------------------------------

def test_collapse_flag_default_off_env_on():
    assert config.response_collapse_previous_default(env={}) is False
    assert config.response_collapse_previous_default(env={"HERDR_TELEGRAM_TOPICS_RESPONSE_COLLAPSE_PREVIOUS": "1"}) is True


# --- renderer -------------------------------------------------------------------

_FORMATTED_RESPONSE = (
    "First response line has **Bold result** and `inline()`.\n"
    "Second response line has *italic detail* and "
    "[reference](https://example.test/response).\n"
    "Third response line makes the folded section meaningful."
)
_FORMATTED_PROMPT = (
    "First prompt line has **Bold request** and `inline()`.\n"
    "Second prompt line has *italic detail* and "
    "[reference](https://example.test/prompt).\n"
    "Third prompt line makes the folded section meaningful."
)
_FORMATTED_WORKLOG = (
    "First worklog line has **Bold progress** and `inline()`.\n"
    "Second worklog line has *italic detail* and "
    "[reference](https://example.test/worklog).\n"
    "Third worklog line makes the folded section meaningful."
)


def _turn_item(**extra):
    item = {
        "kind": "turn",
        "user_text": _FORMATTED_PROMPT,
        "assistant_final_text": _FORMATTED_RESPONSE,
        "worklog_text": _FORMATTED_WORKLOG,
    }
    item.update(extra)
    return item


def test_render_open_by_default():
    html = render_turn_item_html(_turn_item())
    assert "Bold result" in html
    # the Response is the open top-level body, not wrapped in a details card
    assert "<b>✅ Response</b><br><br>" in html


def test_render_collapsed_when_flagged():
    html = render_turn_item_html(_turn_item(collapse_response=True))
    assert html.startswith(
        "<b>✅ Response</b>\n<blockquote expandable>"
    )
    assert "<b>Bold result</b>" in html
    assert "<code>inline()</code>" in html
    assert "<i>italic detail</i>" in html
    assert (
        '<a href="https://example.test/response">reference</a>'
        in html
    )
    assert (
        '<a href="https://example.test/prompt">reference</a>'
        in html
    )
    assert (
        '<a href="https://example.test/worklog">reference</a>'
        in html
    )
    assert html.count("First response line") == 1
    assert "<details" not in html


# --- the fold sweep in _sync_turns ---------------------------------------------

def _two_turn_payload():
    # rows are per-worker recency ordered: newest FIRST (pass 1 setdefault picks the latest)
    return {"turns": [
        {"id": "turn-new", "worker_id": "w1", "worker_fingerprint": "fp1",
         "assistant_final_text": "New answer", "complete": True},
        {"id": "turn-old", "worker_id": "w1", "worker_fingerprint": "fp1",
         "assistant_final_text": _FORMATTED_RESPONSE, "complete": True},
    ]}


def _folded_store(monkeypatch):
    monkeypatch.setenv("HERDRES_SOURCE_TOPIC_MODE", "worker")
    store = _store()
    _worker_key, worker, _created = state.upsert_worker_entry(
        store,
        _source_worker({
            "id": "w1",
            "name": "w1",
            "status": "idle",
            "space_id": "s1",
            "fingerprint": "fp1",
        }),
        topic_id="77",
    )
    worker["last_clean_message_id"] = "501"  # the NEW final's message (never folded)
    worker["last_clean_message_ids"] = ["501"]
    worker["last_turn_id"] = "turn-new"
    new_hash = source_sync._turn_content_hash(
        _two_turn_payload()["turns"][0], "final"
    )
    worker["last_clean_hash"] = new_hash
    worker["last_clean_bot_kind"] = MANAGER_BOT_KIND
    # both turns already delivered + bound (the sweep edits the OLD one)
    state.mark_delivered(store, f"final:turn-new:{new_hash}", {"worker_id": "w1", "turn_id": "turn-new"})
    state.mark_delivered(store, "final:turn-old:whatever", {"worker_id": "w1", "turn_id": "turn-old"})
    state.bind_message_to_worker(store, "400", worker, topic_id="77", kind="final", turn_id="turn-old", bot_kind=MANAGER_BOT_KIND)
    state.bind_message_to_worker(store, "501", worker, topic_id="77", kind="final", turn_id="turn-new", bot_kind=MANAGER_BOT_KIND)
    return store


def _run(store, monkeypatch, flag="1", telegram=None):
    monkeypatch.setenv("HERDR_TELEGRAM_TOPICS_RESPONSE_COLLAPSE_PREVIOUS", flag)
    telegram = telegram or FakeTelegram()
    runtime = SyncRuntime(FakeTendwire(), telegram, with_outbox=False)
    _sync_turns(store, _two_turn_payload(), {"pending": []}, runtime, chat_id="-100")
    return telegram


def test_superseded_final_gets_folded(monkeypatch):
    store = _folded_store(monkeypatch)
    telegram = _run(store, monkeypatch)
    # The OLD message is actually collapsed, contains its answer exactly once,
    # and uses only markup supported by sendMessage/editMessageText.
    edited = [(mid, html) for _chat, mid, html in telegram.edited]
    assert (
        "400",
        "<b>✅ Response</b>\n"
        "<blockquote expandable>"
        "First response line has <b>Bold result</b> and "
        "<code>inline()</code>.\n"
        "Second response line has <i>italic detail</i> and "
        '<a href="https://example.test/response">reference</a>.\n'
        "Third response line makes the folded section meaningful."
        "</blockquote>",
    ) in edited
    assert not any(mid == "501" for mid, _ in edited)                 # latest never folded
    assert state.message_bindings(store)["400"].get("folded") is True  # idempotency marker


def test_fold_idempotent_second_sweep_skips(monkeypatch):
    store = _folded_store(monkeypatch)
    _run(store, monkeypatch)
    telegram2 = _run(store, monkeypatch)                  # second sweep
    assert not any(mid == "400" for _c, mid, _h in telegram2.edited)  # already folded -> no re-edit


def test_multipart_rich_final_folds_every_recipient_message(monkeypatch):
    monkeypatch.setenv("HERDRES_FORCE_PLAIN_DELIVERY", "0")
    monkeypatch.setenv("HERDRES_SOURCE_TOPIC_MODE", "worker")
    monkeypatch.setenv(
        "HERDR_TELEGRAM_TOPICS_RESPONSE_COLLAPSE_PREVIOUS", "1"
    )
    old_text = "Old detailed answer.\n\n" * 300
    turns = {
        "turns": [
            {
                "id": "turn-new",
                "worker_id": "w1",
                "worker_fingerprint": "fp1",
                "assistant_final_text": "New answer",
                "complete": True,
            },
            {
                "id": "turn-old",
                "worker_id": "w1",
                "worker_fingerprint": "fp1",
                "assistant_final_text": old_text,
                "complete": True,
            },
        ]
    }
    store = _store()
    _worker_key, worker, _created = state.upsert_worker_entry(
        store,
        _source_worker(
            {
                "id": "w1",
                "name": "w1",
                "status": "idle",
                "space_id": "s1",
                "fingerprint": "fp1",
            }
        ),
        topic_id="77",
    )
    worker["last_clean_message_id"] = "501"
    worker["last_clean_message_ids"] = ["501"]
    worker["last_turn_id"] = "turn-new"
    for ordinal, message_id in enumerate(("400", "401")):
        state.bind_message_to_worker(
            store,
            message_id,
            worker,
            topic_id="77",
            kind="final",
            turn_id="turn-old",
            bot_kind=MANAGER_BOT_KIND,
            content_revision="twrev1.old",
            plan_token="twplan1.old",
            part_ordinal=ordinal,
            part_count=2,
            tendwire_job_key=(
                f"turn-final:twplan1.old:{ordinal:06d}"
            ),
        )
    state.bind_message_to_worker(
        store,
        "501",
        worker,
        topic_id="77",
        kind="final",
        turn_id="turn-new",
        bot_kind=MANAGER_BOT_KIND,
    )
    telegram = FakeTelegram()

    _sync_turns(
        store,
        turns,
        {"pending": []},
        SyncRuntime(FakeTendwire(), telegram, with_outbox=False),
        chat_id="-100",
    )

    edited_ids = {message_id for _chat, message_id, _html in telegram.edited}
    assert {"400", "401"} <= edited_ids
    assert all(
        "<blockquote expandable>" in html
        for _chat, message_id, html in telegram.edited
        if message_id in {"400", "401"}
    )
    assert not any(
        message_id == "501" and "<blockquote expandable>" in html
        for _chat, message_id, html in telegram.edited
    )
    assert all(
        state.message_bindings(store)[message_id]["folded"] is True
        for message_id in ("400", "401")
    )


def test_readable_noncollapsed_fallback_is_never_marked_folded(
    monkeypatch,
):
    store = _folded_store(monkeypatch)

    class PlainFallbackTelegram(FakeTelegram):
        def with_token(self, _token):
            return self

        def edit_message(self, chat_id, message_id, html):
            self.edited.append((chat_id, str(message_id), html))
            return {
                "ok": True,
                "message_id": str(message_id),
                "format": "plain",
            }

    _run(store, monkeypatch, telegram=PlainFallbackTelegram())

    binding = state.message_bindings(store)["400"]
    assert binding.get("folded") is None
    assert binding["fold_attempts"] == 1


def test_fold_topic_not_found_never_tombstones_rebound_live_topic(
    monkeypatch,
):
    store = _folded_store(monkeypatch)
    entry = next(iter(state.source_worker_entries(store).values()))
    binding = state.message_bindings(store)["400"]
    binding["topic_id"] = "15007"
    entry["topic_id"] = "16000"

    class TopicMissingEditTelegram(FakeTelegram):
        def __init__(self):
            super().__init__()
            self.edit_attempts = 0

        def with_token(self, _token):
            return self

        def edit_message(self, _chat_id, message_id, _html):
            self.edit_attempts += 1
            return {
                "ok": False,
                "message_id": str(message_id),
                "kind": "topic_not_found",
                "topic_missing": True,
                "error": "Bad Request: message thread not found",
            }

        def api(self, method, payload):
            if method == "editMessageText":
                raise TelegramError(
                    "Bad Request: message thread not found"
                )
            return super().api(method, payload)

    telegram = TopicMissingEditTelegram()
    _run(
        store,
        monkeypatch,
        telegram=telegram,
    )
    assert telegram.edit_attempts == 1
    _run(store, monkeypatch, telegram=telegram)

    assert binding.get("folded") is None
    assert binding["fold_unavailable"] is True
    assert telegram.edit_attempts == 1
    assert binding["topic_id"] == "15007"
    assert entry["topic_id"] == "16000"
    assert not state.topic_id_is_tombstoned(store, "15007")
    assert not state.topic_id_is_tombstoned(store, "16000")
    assert "topic_recovery_pending" not in entry


def test_fold_message_not_found_never_tombstones_topic(monkeypatch):
    store = _folded_store(monkeypatch)
    entry = next(iter(state.source_worker_entries(store).values()))
    binding = state.message_bindings(store)["400"]

    class MessageMissingEditTelegram(FakeTelegram):
        def __init__(self):
            super().__init__()
            self.edit_attempts = 0

        def with_token(self, _token):
            return self

        def edit_message(self, _chat_id, message_id, _html):
            self.edit_attempts += 1
            return {
                "ok": False,
                "message_id": str(message_id),
                "kind": "not_found",
                "not_found": True,
                "error": "Bad Request: message to edit not found",
            }

        def api(self, method, payload):
            if method == "editMessageText":
                raise TelegramError(
                    "Bad Request: message to edit not found"
                )
            return super().api(method, payload)

    telegram = MessageMissingEditTelegram()
    _run(
        store,
        monkeypatch,
        telegram=telegram,
    )
    assert telegram.edit_attempts == 1
    _run(store, monkeypatch, telegram=telegram)

    assert binding.get("folded") is None
    assert binding["fold_unavailable"] is True
    assert telegram.edit_attempts == 1
    assert entry["topic_id"] == "77"
    assert store.get("telegram_dead_topic_ids", []) == []
    assert "topic_recovery_pending" not in entry


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


def test_fold_skipped_when_binding_lacks_bot_kind(monkeypatch):
    # A binding that doesn't record which bot sent the message must NOT be folded (a wrong-bot edit
    # 404s and would falsely mark the fold done). It just stays unfolded, honestly.
    store = _folded_store(monkeypatch)
    state.message_bindings(store)["400"].pop("bot_kind", None)
    telegram = _run(store, monkeypatch)
    assert not any(mid == "400" for _c, mid, _h in telegram.edited)
    assert state.message_bindings(store)["400"].get("folded") is None


def test_fold_per_pass_cap(monkeypatch):
    # Many superseded finals fold at most _FOLD_PASS_CAP per pass (self-healing sweep, no burst).
    monkeypatch.setenv("HERDRES_SOURCE_TOPIC_MODE", "worker")
    store = _store()
    turns = [{"id": "turn-new", "worker_id": "w1", "worker_fingerprint": "fp1",
              "assistant_final_text": "New answer", "complete": True}]
    _worker_key, worker, _created = state.upsert_worker_entry(
        store,
        _source_worker({
            "id": "w1",
            "name": "w1",
            "status": "idle",
            "space_id": "s1",
            "fingerprint": "fp1",
        }),
        topic_id="77",
    )
    worker["last_clean_message_id"] = "900"
    for i in range(6):   # six superseded finals with bindings
        tid = f"turn-old-{i}"
        turns.append({"id": tid, "worker_id": "w1", "worker_fingerprint": "fp1",
                      "assistant_final_text": f"Old {i}", "complete": True})
        state.mark_delivered(store, f"final:{tid}:x", {"worker_id": "w1", "turn_id": tid})
        state.bind_message_to_worker(store, str(400 + i), worker,
                                     topic_id="77", kind="final", turn_id=tid, bot_kind=MANAGER_BOT_KIND)
    state.mark_delivered(store, "final:turn-new:x", {"worker_id": "w1", "turn_id": "turn-new"})
    state.bind_message_to_worker(store, "900", worker, topic_id="77",
                                 kind="final", turn_id="turn-new", bot_kind=MANAGER_BOT_KIND)
    monkeypatch.setenv("HERDR_TELEGRAM_TOPICS_RESPONSE_COLLAPSE_PREVIOUS", "1")
    telegram = FakeTelegram()
    _sync_turns(store, {"turns": turns}, {"pending": []}, SyncRuntime(FakeTendwire(), telegram, with_outbox=False), chat_id="-100")
    assert len(telegram.edited) == source_sync._FOLD_PASS_CAP     # capped this pass
    folded = [m for m, b in state.message_bindings(store).items() if b.get("folded")]
    assert len(folded) == source_sync._FOLD_PASS_CAP              # rest fold on later passes
