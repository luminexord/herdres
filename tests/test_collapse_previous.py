"""Recipient-visible collapse of superseded rich Response details blocks."""
from __future__ import annotations

import json

from herdres_connector import config, source_sync, state
from herdres_connector.managed_bots import MANAGER_BOT_KIND
from herdres_connector.rich_delivery import (
    render_turn_item_html,
    send_feed_item,
)
from herdres_connector.source_sync import SyncRuntime, _sync_turns
from herdres_connector.telegram_delivery import TelegramError

from test_source_only import FakeTelegram, FakeTendwire, _source_worker, _store
from test_rich_delivery import _RichCardRecipientParser


import pytest


@pytest.fixture(autouse=True)
def _rich_delivery_by_default(monkeypatch):
    """Collapse is a sendRichMessage presentation and is tested as such."""

    monkeypatch.setenv("HERDRES_FORCE_PLAIN_DELIVERY", "0")


def _recipient_tree(html: str) -> dict[str, object]:
    parser = _RichCardRecipientParser()
    parser.feed(html)
    parser.close()
    return {
        "text": "".join(parser.text),
        "details": [
            {
                **detail,
                "summary": "".join(detail["summary"]),
                "body": "".join(detail["body"]),
            }
            for detail in parser.details
        ],
    }


class _ReadbackTelegram(FakeTelegram):
    """Parse accepted rich payloads into the recipient's block-tree state."""

    def __init__(self, token="fake", shared=None):
        super().__init__(token=token, shared=shared)
        self._shared.setdefault("recipient", {})
        self.recipient = self._shared["recipient"]

    def with_token(self, token):
        return _ReadbackTelegram(token=token, shared=self._shared)

    def seed(self, message_id: str, html: str) -> None:
        self.recipient[str(message_id)] = _recipient_tree(html)

    def api(self, method, payload):
        response = super().api(method, payload)
        rich_payload = payload.get("rich_message")
        if method in {"sendRichMessage", "editMessageText"} and rich_payload:
            rich = json.loads(rich_payload)
            message_id = (
                str(payload.get("message_id") or "")
                if method == "editMessageText"
                else str(response["result"]["message_id"])
            )
            self.seed(message_id, str(rich.get("html") or ""))
        return response


def _response_detail(tree: dict[str, object]) -> dict[str, object]:
    matches = [
        detail
        for detail in tree["details"]
        if "Response" in str(detail["summary"])
    ]
    assert len(matches) == 1
    return matches[0]


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


def test_delivered_newest_response_reads_back_as_open_details_once():
    telegram = _ReadbackTelegram()
    result = send_feed_item(
        telegram,
        "-100",
        _turn_item(),
        telegram={},
        thread_id="77",
    )

    tree = telegram.recipient[result["message_id"]]
    response = _response_detail(tree)
    assert response["type"] == "PageBlockDetails"
    assert response["open"] is True
    assert str(response["body"]).count("First response line") == 1
    assert str(tree["text"]).count("First response line") == 1


def test_render_collapsed_when_flagged():
    tree = _recipient_tree(
        render_turn_item_html(_turn_item(collapse_response=True))
    )
    response = _response_detail(tree)
    assert response["type"] == "PageBlockDetails"
    assert response["open"] is False
    html = str(response["body"])
    assert "Bold result" in html
    assert html.count("First response line") == 1


# --- the fold sweep in _sync_turns ---------------------------------------------

def _two_turn_payload():
    # rows are per-worker recency ordered: newest FIRST (pass 1 setdefault picks the latest)
    return {"turns": [
        {"id": "turn-new", "worker_id": "w1", "worker_fingerprint": "fp1",
         "assistant_final_text": "New answer", "complete": True,
         "content": {
             "schema_version": 1,
             "content_revision": "twrev1.new",
             "fields": {
                 "assistant_final_text": {"availability": "complete"}
             },
         }},
        {"id": "turn-old", "worker_id": "w1", "worker_fingerprint": "fp1",
         "assistant_final_text": _FORMATTED_RESPONSE, "complete": True,
         "content": {
             "schema_version": 1,
             "content_revision": "twrev1.old",
             "fields": {
                 "assistant_final_text": {"availability": "complete"}
             },
         }},
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
    telegram.sync_counts = _sync_turns(
        store,
        _two_turn_payload(),
        {"pending": []},
        runtime,
        chat_id="-100",
        list_finals_are_authoritative=False,
    )
    return telegram


def test_superseded_final_gets_folded(monkeypatch):
    store = _folded_store(monkeypatch)
    telegram = _ReadbackTelegram()
    telegram.seed("400", render_turn_item_html(_turn_item()))
    before = _response_detail(telegram.recipient["400"])
    assert before["open"] is True

    _run(store, monkeypatch, telegram=telegram)

    after = _response_detail(telegram.recipient["400"])
    assert after["open"] is False
    assert str(after["body"]).count("First response line") == 1
    assert str(telegram.recipient["400"]["text"]).count(
        "First response line"
    ) == 1
    assert not any(
        mid == "501" for _chat, mid, _html in telegram.edited
    )  # latest never folded
    assert state.message_bindings(store)["400"].get("folded") is True  # idempotency marker
    assert sum(
        binding.get("folded") is True
        for binding in state.message_bindings(store).values()
    ) > 0
    assert telegram.sync_counts["response_folded"] == 1


def test_schema_v2_source_final_reaches_fold_sweep(monkeypatch):
    store = _folded_store(monkeypatch)
    reached: list[str] = []
    original = source_sync._fold_superseded_final

    def recording_fold(store, item, entry, runtime, **kwargs):
        reached.append(str(item.get("id") or ""))
        return original(store, item, entry, runtime, **kwargs)

    monkeypatch.setattr(
        source_sync, "_fold_superseded_final", recording_fold
    )
    _run(store, monkeypatch, telegram=_ReadbackTelegram())

    assert reached == ["turn-old"]


def test_schema_v2_sweep_marks_a_binding_folded(monkeypatch):
    store = _folded_store(monkeypatch)

    _run(store, monkeypatch, telegram=_ReadbackTelegram())

    assert sum(
        binding.get("folded") is True
        for binding in state.message_bindings(store).values()
    ) > 0


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
    telegram = _ReadbackTelegram()

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
        _response_detail(telegram.recipient[message_id])["open"]
        is False
        for message_id in ("400", "401")
    )
    assert _response_detail(telegram.recipient["501"])["open"] is True
    assert all(
        state.message_bindings(store)[message_id]["folded"] is True
        for message_id in ("400", "401")
    )


def test_readable_noncollapsed_fallback_is_never_marked_folded(
    monkeypatch,
):
    monkeypatch.setenv("HERDRES_FORCE_PLAIN_DELIVERY", "1")
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

    telegram = _run(
        store, monkeypatch, telegram=PlainFallbackTelegram()
    )

    binding = state.message_bindings(store)["400"]
    assert binding.get("folded") is None
    assert binding["fold_attempts"] == 1
    assert "not applied" in binding["fold_error"]
    assert telegram.sync_counts["response_fold_failed"] == 1


def test_failed_superseded_fold_is_observable_in_state_and_sync_result(
    monkeypatch,
):
    store = _folded_store(monkeypatch)

    class FailingTelegram(FakeTelegram):
        def api(self, method, payload):
            if method == "editMessageText":
                raise TelegramError("recipient rejected fold")
            return super().api(method, payload)

        def edit_message(self, _chat_id, _message_id, _html):
            return {"ok": False, "error": "recipient rejected fold"}

    telegram = _run(store, monkeypatch, telegram=FailingTelegram())

    binding = state.message_bindings(store)["400"]
    assert binding.get("folded") is None
    assert binding["fold_attempts"] == 1
    assert binding["fold_error"] == "recipient rejected fold"
    assert telegram.sync_counts["response_fold_attempted"] == 1
    assert telegram.sync_counts["response_fold_failed"] == 1


def test_fold_topic_not_found_never_tombstones_rebound_live_topic(
    monkeypatch,
):
    monkeypatch.setenv("HERDRES_FORCE_PLAIN_DELIVERY", "1")
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
    monkeypatch.setenv("HERDRES_FORCE_PLAIN_DELIVERY", "1")
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
        _sync_turns(
            store,
            _two_turn_payload(),
            {"pending": []},
            runtime,
            chat_id="-100",
            list_finals_are_authoritative=False,
        )
    binding = state.message_bindings(store)["400"]
    assert binding.get("folded") is None
    assert int(binding.get("fold_attempts") or 0) == source_sync._FOLD_ATTEMPT_CAP   # gave up at the cap


def test_fold_skipped_when_binding_lacks_bot_kind(monkeypatch):
    # A binding that doesn't record which bot sent the message must NOT be folded (a wrong-bot edit
    # 404s and would falsely mark the fold done). It stays unfolded and exposes
    # the reason in both binding state and the structured sweep result.
    store = _folded_store(monkeypatch)
    state.message_bindings(store)["400"].pop("bot_kind", None)
    telegram = _run(store, monkeypatch)
    assert not any(mid == "400" for _c, mid, _h in telegram.edited)
    binding = state.message_bindings(store)["400"]
    assert binding.get("folded") is None
    assert "bot identity" in binding["fold_error"]
    assert telegram.sync_counts["response_fold_failed"] == 1


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
