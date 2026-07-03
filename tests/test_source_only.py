from __future__ import annotations

import json
from pathlib import Path

import herdres
import herdres_gateway
from herdres_connector import state
from herdres_connector.rich_delivery import MAX_RICH_HTML_CHARS, render_turn_item_html
from herdres_connector.safe import public_prune
from herdres_connector.source_sync import SyncRuntime, sync_once
from herdres_connector.telegram_delivery import TelegramClient, drain_outbox


class FakeTendwire:
    def __init__(self, *, turns=None, pending=None, workers=None, spaces=None):
        self.commands = []
        self._turns = turns if turns is not None else {"turns": []}
        self._pending = pending if pending is not None else {"pending_interactions": []}
        self._workers = workers if workers is not None else [
            {
                "id": "worker-1",
                "name": "Alpha",
                "status": "working",
                "space_id": "space-1",
                "fingerprint": "fp-1",
                "meta": {"agent": "codex"},
            }
        ]
        self._spaces = spaces if spaces is not None else [
            {
                "id": "space-1",
                "name": "Project",
                "status": "active",
                "fingerprint": "space-fp-1",
            }
        ]

    def snapshot(self):
        return {
            "ok": True,
            "spaces": self._spaces,
            "workers": self._workers,
        }

    def turns(self):
        return self._turns

    def pending(self):
        return self._pending

    def connector_poll(self, **_kwargs):
        return {"ok": True, "items": []}

    def command(self, request):
        self.commands.append(request)
        return {"ok": True, "status": "accepted", "result": {"delivery_state": "submitted"}}


class FakeTelegram:
    dry_run = False

    def __init__(self, token="fake", shared=None):
        self.token = token
        shared = shared or {
            "sent": [],
            "edited": [],
            "topics": [],
            "deleted_topics": [],
            "pins": [],
            "api_calls": [],
        }
        self._shared = shared
        self.sent = shared["sent"]
        self.edited = shared["edited"]
        self.topics = shared["topics"]
        self.deleted_topics = shared["deleted_topics"]
        self.pins = shared["pins"]
        self.api_calls = shared["api_calls"]

    def with_token(self, token):
        return FakeTelegram(token=token, shared=self._shared)

    def api(self, method, payload):
        self.api_calls.append((method, dict(payload), self.token))
        if method == "sendRichMessage":
            message_id = str(100 + len(self.sent))
            rich = json.loads(payload.get("rich_message") or "{}")
            kwargs = {
                "thread_id": str(payload.get("message_thread_id") or ""),
                "format": "rich",
                "token": self.token,
            }
            self.sent.append((str(payload.get("chat_id") or ""), str(rich.get("html") or ""), kwargs, message_id))
            return {"ok": True, "result": {"message_id": message_id}}
        if method == "editMessageText":
            rich_payload = payload.get("rich_message")
            rich = json.loads(rich_payload) if rich_payload else {}
            html = str(rich.get("html") or payload.get("text") or "")
            self.edited.append((str(payload.get("chat_id") or ""), str(payload.get("message_id") or ""), html))
            return {"ok": True, "result": {"message_id": str(payload.get("message_id") or "0")}}
        return {"ok": True, "result": {"message_id": 0}}

    def create_topic(self, _chat_id, name):
        self.topics.append(name)
        return {"ok": True, "topic_id": str(76 + len(self.topics))}

    def edit_topic_icon(self, *_args, **_kwargs):
        return {"ok": True}

    def delete_topic(self, _chat_id, thread_id):
        self.deleted_topics.append(str(thread_id))
        return {"ok": True}

    def send_message(self, chat_id, html, **kwargs):
        message_id = str(100 + len(self.sent))
        payload_kwargs = dict(kwargs)
        payload_kwargs["token"] = self.token
        self.sent.append((chat_id, html, payload_kwargs, message_id))
        return {"ok": True, "message_id": message_id}

    def edit_message(self, chat_id, message_id, html):
        self.edited.append((chat_id, str(message_id), html))
        return {"ok": True, "message_id": str(message_id)}

    def pin_message(self, chat_id, message_id):
        self.pins.append((chat_id, str(message_id)))
        return {"ok": True}


def _store():
    return {
        "enabled": True,
        "telegram": {"chat_id": "-100", "general_thread_id": "1"},
        "panes": {},
        "spaces": {},
        "tendwired_bootstrap_complete": True,
    }


def test_first_sync_bootstraps_current_turns_without_telegram_posts(monkeypatch):
    monkeypatch.setenv("HERDRES_TENDWIRE_MODE", "source")
    store = _store()
    store.pop("tendwired_bootstrap_complete", None)
    telegram = FakeTelegram()
    turns = {
        "turns": [
            {
                "id": "turn-0",
                "worker_id": "worker-1",
                "worker_fingerprint": "fp-1",
                "user_text": "Old prompt",
                "assistant_final_text": "Old final",
                "complete": True,
            }
        ]
    }

    result = sync_once(store, SyncRuntime(FakeTendwire(turns=turns), telegram, with_outbox=False))

    assert result["bootstrap_seen"] == 1
    assert result["feed_sent"] == 0
    assert not any("Old final" in sent[1] for sent in telegram.sent)
    assert store["tendwired_bootstrap_complete"] is True


def test_sync_delivers_final_turn_once(monkeypatch):
    monkeypatch.setenv("HERDRES_TENDWIRE_MODE", "source")
    store = _store()
    telegram = FakeTelegram()
    turns = {
        "turns": [
            {
                "id": "turn-1",
                "worker_id": "worker-1",
                "worker_fingerprint": "fp-1",
                "user_text": "Question",
                "assistant_final_text": "Full final answer",
                "complete": True,
            }
        ]
    }
    runtime = SyncRuntime(FakeTendwire(turns=turns), telegram, with_outbox=False)

    first = sync_once(store, runtime)
    second = sync_once(store, runtime)

    assert first["feed_sent"] == 1
    assert second["feed_sent"] == 0
    assert any("Full final answer" in sent[1] for sent in telegram.sent)
    assert any(sent[2]["thread_id"] == "77" for sent in telegram.sent if "Full final answer" in sent[1])
    assert len(store["tendwire_source_delivered_turns"]) == 1
    response_message_id = [sent[3] for sent in telegram.sent if "Full final answer" in sent[1]][0]
    binding = state.find_message_binding(store, response_message_id, topic_id="77")
    assert binding is not None
    assert binding["worker_id"] == "worker-1"


def test_source_final_uses_configured_managed_bot_voice(monkeypatch):
    monkeypatch.setenv("HERDRES_TENDWIRE_MODE", "source")
    monkeypatch.setenv("HERDR_TELEGRAM_TOPICS_MANAGED_BOTS", "1")
    store = _store()
    store["telegram"]["managed_bots"] = {"codex": {"enabled": True, "token": "codex-token"}}
    _key, stale, _created = state.upsert_worker_entry(
        store,
        {
            "id": "worker-1",
            "name": "codex",
            "status": "working",
            "space_id": "space-1",
            "fingerprint": "fp-1",
            "meta": {"agent": "codex"},
        },
    )
    stale["managed_bot_kind"] = "claude"
    telegram = FakeTelegram()
    turns = {
        "turns": [
            {
                "id": "turn-1",
                "worker_id": "worker-1",
                "worker_fingerprint": "fp-1",
                "assistant_final_text": "Codex final",
                "complete": True,
            }
        ]
    }

    result = sync_once(store, SyncRuntime(FakeTendwire(turns=turns), telegram, with_outbox=False))

    assert result["feed_sent"] == 1
    assert any(call[0] == "sendRichMessage" and call[2] == "codex-token" for call in telegram.api_calls)
    entry = next(iter(state.source_worker_entries(store).values()))
    assert entry["last_clean_bot_kind"] == "codex"
    binding = state.find_message_binding(store, entry["last_clean_message_id"], topic_id="77")
    assert binding is not None
    assert binding["bot_kind"] == "codex"


def test_sync_backfills_existing_message_bindings(monkeypatch):
    monkeypatch.setenv("HERDRES_TENDWIRE_MODE", "source")
    store = _store()
    _worker_key, worker, _created = state.upsert_worker_entry(
        store,
        {"id": "worker-1", "name": "Alpha", "status": "working", "space_id": "space-1", "fingerprint": "fp-1"},
    )
    worker["last_clean_message_id"] = "555"
    worker["last_turn_id"] = "turn-1"
    state.upsert_space_entry(
        store,
        {"id": "space-1", "name": "Project", "status": "active", "fingerprint": "space-fp"},
        topic_id="77",
    )

    result = sync_once(store, SyncRuntime(FakeTendwire(), FakeTelegram(), with_outbox=False))

    assert result["message_bindings"] == 1
    binding = state.find_message_binding(store, "555", topic_id="77")
    assert binding is not None
    assert binding["worker_id"] == "worker-1"


def test_sync_creates_one_topic_per_space_not_per_worker(monkeypatch):
    monkeypatch.setenv("HERDRES_TENDWIRE_MODE", "source")
    store = _store()
    telegram = FakeTelegram()
    workers = [
        {"id": "worker-1", "name": "codex", "status": "working", "space_id": "space-1", "fingerprint": "fp-1"},
        {"id": "worker-2", "name": "claude", "status": "done", "space_id": "space-1", "fingerprint": "fp-2"},
    ]
    turns = {
        "turns": [
            {"id": "turn-1", "worker_id": "worker-1", "assistant_final_text": "one", "complete": True},
            {"id": "turn-2", "worker_id": "worker-2", "assistant_final_text": "two", "complete": True},
        ]
    }

    result = sync_once(
        store,
        SyncRuntime(
            FakeTendwire(
                turns=turns,
                workers=workers,
                spaces=[{"id": "space-1", "name": "Project", "status": "active", "fingerprint": "space-fp"}],
            ),
            telegram,
            with_outbox=False,
        ),
    )

    assert result["spaces"] == 1
    assert result["panes"] == 2
    assert telegram.topics == ["Project"]
    assert len(state.source_entries(store)) == 1
    assert len(state.source_worker_entries(store)) == 2
    assert all(sent[2]["thread_id"] == "77" for sent in telegram.sent if sent[1].startswith("<b>Project"))


def test_worker_topic_mode_creates_one_topic_per_worker(monkeypatch):
    monkeypatch.setenv("HERDRES_TENDWIRE_MODE", "source")
    monkeypatch.setenv("HERDRES_SOURCE_TOPIC_MODE", "worker")
    store = _store()
    telegram = FakeTelegram()
    workers = [
        {"id": "worker-1", "name": "codex", "status": "working", "space_id": "space-1", "fingerprint": "fp-1"},
        {"id": "worker-2", "name": "claude", "status": "idle", "space_id": "space-1", "fingerprint": "fp-2"},
    ]

    result = sync_once(
        store,
        SyncRuntime(
            FakeTendwire(
                workers=workers,
                spaces=[{"id": "space-1", "name": "Project", "status": "active", "fingerprint": "space-fp"}],
            ),
            telegram,
            with_outbox=False,
        ),
    )

    assert result["panes"] == 2
    assert telegram.topics == ["codex", "claude"]
    assert len(state.source_entries(store)) == 2


def test_space_topic_reuses_existing_same_name_worker_topic(monkeypatch):
    monkeypatch.setenv("HERDRES_TENDWIRE_MODE", "source")
    store = _store()
    _key, legacy, _created = state.upsert_worker_entry(
        store,
        {"id": "worker-old", "name": "Project", "status": "idle", "space_id": "old-space", "fingerprint": "old-fp"},
        topic_id="123",
    )
    legacy["topic_name"] = "Project"
    telegram = FakeTelegram()

    sync_once(store, SyncRuntime(FakeTendwire(), telegram, with_outbox=False))

    assert telegram.topics == []
    entry = next(iter(state.source_entries(store).values()))
    assert entry["topic_id"] == "123"


def test_space_without_open_worker_is_not_telegram_visible(monkeypatch):
    monkeypatch.setenv("HERDRES_TENDWIRE_MODE", "source")
    store = _store()
    telegram = FakeTelegram()

    result = sync_once(
        store,
        SyncRuntime(
            FakeTendwire(
                workers=[],
                spaces=[{"id": "empty-space", "name": "Empty", "status": "active", "fingerprint": "space-fp"}],
            ),
            telegram,
            with_outbox=False,
        ),
    )

    assert result["spaces"] == 0
    assert telegram.topics == []
    assert state.source_entries(store) == {}


def test_space_mode_deletes_stale_worker_topics(monkeypatch):
    monkeypatch.setenv("HERDRES_TENDWIRE_MODE", "source")
    store = _store()
    _key, stale, _created = state.upsert_worker_entry(
        store,
        {"id": "worker-old", "name": "Old worker", "status": "idle", "space_id": "old-space", "fingerprint": "old-fp"},
        topic_id="88",
    )
    stale["topic_name"] = "Old worker"
    telegram = FakeTelegram()

    result = sync_once(store, SyncRuntime(FakeTendwire(), telegram, with_outbox=False))

    assert result["topic_cleanup"]["deleted"] == 1
    assert telegram.deleted_topics == ["88"]
    old = [entry for entry in state.source_worker_entries(store).values() if entry.get("tendwire_worker_id") == "worker-old"][0]
    assert not old.get("topic_id")


def test_finished_council_worker_topic_is_deleted(monkeypatch):
    monkeypatch.setenv("HERDRES_TENDWIRE_MODE", "source")
    monkeypatch.setenv("HERDRES_SOURCE_TOPIC_MODE", "worker")
    store = _store()
    state.upsert_worker_entry(
        store,
        {"id": "gm-1", "name": "gm-local-as", "status": "done", "space_id": "space-1", "fingerprint": "fp-1"},
        topic_id="88",
    )
    telegram = FakeTelegram()

    result = sync_once(
        store,
        SyncRuntime(
            FakeTendwire(
                workers=[{"id": "gm-1", "name": "gm-local-as", "status": "done", "space_id": "space-1", "fingerprint": "fp-1"}],
                spaces=[{"id": "space-1", "name": "Council", "status": "active", "fingerprint": "space-fp"}],
            ),
            telegram,
            with_outbox=False,
        ),
    )

    assert result["topic_cleanup"]["deleted"] == 1
    assert telegram.topics == []
    assert telegram.deleted_topics == ["88"]
    assert state.source_worker_entries(store) == {}


def test_finished_council_space_topic_is_deleted(monkeypatch):
    monkeypatch.setenv("HERDRES_TENDWIRE_MODE", "source")
    store = _store()
    state.upsert_space_entry(
        store,
        {"id": "space-1", "name": "gitmoot · local-as", "status": "active", "fingerprint": "space-fp"},
        topic_id="88",
    )
    telegram = FakeTelegram()

    result = sync_once(
        store,
        SyncRuntime(
            FakeTendwire(
                workers=[],
                spaces=[{"id": "space-1", "name": "gitmoot · local-as", "status": "active", "fingerprint": "space-fp"}],
            ),
            telegram,
            with_outbox=False,
        ),
    )

    assert result["topic_cleanup"]["deleted"] == 1
    assert result["topic_cleanup"]["pruned"] == 1
    assert telegram.deleted_topics == ["88"]
    assert state.source_entries(store) == {}


def test_finished_council_worker_and_space_topic_delete_once(monkeypatch):
    monkeypatch.setenv("HERDRES_TENDWIRE_MODE", "source")
    store = _store()
    state.upsert_worker_entry(
        store,
        {"id": "gm-1", "name": "gm-local-as", "status": "closed", "space_id": "space-1", "fingerprint": "fp-1"},
        topic_id="88",
    )
    state.upsert_space_entry(
        store,
        {"id": "space-1", "name": "gitmoot · local-as", "status": "active", "fingerprint": "space-fp"},
        topic_id="88",
    )
    telegram = FakeTelegram()

    result = sync_once(
        store,
        SyncRuntime(
            FakeTendwire(
                workers=[],
                spaces=[{"id": "space-1", "name": "gitmoot · local-as", "status": "active", "fingerprint": "space-fp"}],
            ),
            telegram,
            with_outbox=False,
        ),
    )

    assert result["topic_cleanup"]["deleted"] == 1
    assert telegram.deleted_topics == ["88"]
    assert state.source_entries(store) == {}
    assert state.source_worker_entries(store) == {}


def test_finished_council_worker_does_not_delete_active_space_topic(monkeypatch):
    monkeypatch.setenv("HERDRES_TENDWIRE_MODE", "source")
    store = _store()
    state.upsert_worker_entry(
        store,
        {"id": "gm-old", "name": "gm-local-as", "status": "done", "space_id": "space-1", "fingerprint": "fp-old"},
        topic_id="88",
    )
    state.upsert_space_entry(
        store,
        {"id": "space-1", "name": "gitmoot · local-as", "status": "active", "fingerprint": "space-fp"},
        topic_id="88",
    )
    telegram = FakeTelegram()

    result = sync_once(
        store,
        SyncRuntime(
            FakeTendwire(
                workers=[
                    {"id": "gm-new", "name": "gm-local-as", "status": "working", "space_id": "space-1", "fingerprint": "fp-new"}
                ],
                spaces=[{"id": "space-1", "name": "gitmoot · local-as", "status": "active", "fingerprint": "space-fp"}],
            ),
            telegram,
            with_outbox=False,
        ),
    )

    assert result["topic_cleanup"]["deleted"] == 0
    assert telegram.deleted_topics == []
    assert next(iter(state.source_entries(store).values()))["topic_id"] == "88"


def test_final_response_renders_common_markdown_as_telegram_html():
    html = render_turn_item_html(
        {
            "kind": "turn",
            "title": "Alpha",
            "user_text": "Question",
            "assistant_final_text": "## **Fix it**\n\n- keep **bold**\n- escape <tags>\n\nUse `code`.",
        }
    )

    assert "##" not in html
    assert "**" not in html
    # No redundant top worker title; the Response is the open top-level section.
    assert "<h3>Alpha</h3>" not in html
    assert html.startswith("<b>✅ Response</b>")
    assert "<h3>Fix it</h3>" in html
    assert "<ul>" in html
    assert "<li>keep <b>bold</b></li>" in html
    assert "escape &lt;tags&gt;" in html
    assert "<code>code</code>" in html
    assert "<p>Use <code>code</code>.</p>" in html
    # Prompt is a de-emphasized (<footer>) collapsible section; no quote bars.
    assert "<details open><summary>💬 <b>You</b></summary><footer>Question</footer></details>" in html
    assert "<blockquote>" not in html
    assert "</details><br><details" not in html


def test_long_final_response_uses_full_visible_response_section():
    html = render_turn_item_html(
        {
            "kind": "turn",
            "title": "Alpha",
            "user_text": "Question",
            "assistant_final_text": "## **Plan**\n\n" + "- keep **rich** sections\n" * 80,
        }
    )

    assert html.startswith("<b>✅ Response</b>")
    assert "<blockquote>" not in html
    assert "<blockquote expandable>" not in html
    assert "##" not in html
    assert "**" not in html
    assert "<h3>Plan</h3>" in html
    assert "<ul>" in html
    assert "<li>keep <b>rich</b> sections</li>" in html
    assert "</details><br><details" not in html


def test_oversize_rich_response_falls_back_without_raw_markdown_or_truncation(monkeypatch):
    monkeypatch.setenv("HERDRES_TENDWIRE_MODE", "source")
    store = _store()
    telegram = FakeTelegram()
    tail = "TAIL_MARKER_12345"
    turns = {
        "turns": [
            {
                "id": "turn-huge",
                "worker_id": "worker-1",
                "assistant_final_text": "## **Long**\n\n" + "- keep **rich** sections\n" * 450 + tail,
                "complete": True,
            }
        ]
    }

    result = sync_once(store, SyncRuntime(FakeTendwire(turns=turns), telegram, with_outbox=False))

    sent_text = "\n".join(sent[1] for sent in telegram.sent)
    assert result["feed_sent"] == 1
    assert len(render_turn_item_html({"kind": "turn", "assistant_final_text": turns["turns"][0]["assistant_final_text"]})) > MAX_RICH_HTML_CHARS
    assert any(sent[2].get("format") != "rich" for sent in telegram.sent)
    assert tail in sent_text
    assert "##" not in sent_text
    assert "**" not in sent_text


def test_sync_sends_all_long_final_response_parts(monkeypatch):
    monkeypatch.setenv("HERDRES_TENDWIRE_MODE", "source")
    store = _store()
    telegram = FakeTelegram()
    tail = "TAIL_MARKER_67890"
    turns = {
        "turns": [
            {
                "id": "turn-long",
                "worker_id": "worker-1",
                "assistant_final_text": "## **Long**\n\n" + "- keep **rich** sections\n" * 220 + tail,
                "complete": True,
            }
        ]
    }

    result = sync_once(store, SyncRuntime(FakeTendwire(turns=turns), telegram, with_outbox=False))

    response_messages = [sent[1] for sent in telegram.sent if "<b>✅ Response" in sent[1]]
    assert result["feed_sent"] == 1
    assert len(response_messages) >= 1
    assert any(tail in message for message in response_messages)
    entry = next(iter(state.source_worker_entries(store).values()))
    assert len(entry["last_clean_message_ids"]) == len(response_messages)


def test_expandable_blockquote_has_delivery_fallbacks():
    variants = TelegramClient(token="fake", dry_run=True)._html_variants(
        "<b>Response</b>\n<blockquote expandable>hello <b>there</b></blockquote>"
    )

    assert variants[0][0] == "html"
    assert variants[1] == (
        "html-no-expandable",
        "<b>Response</b>\n<blockquote>hello <b>there</b></blockquote>",
    )
    assert variants[-1] == ("plain", "Response\nhello there")


def test_outbox_attention_falls_back_when_general_thread_missing():
    class OutboxTendwire:
        def __init__(self):
            self.acked = []
            self.failed = []

        def connector_poll(self, **_kwargs):
            return {
                "ok": True,
                "items": [
                    {
                        "ref": "ref-1",
                        "key": "attention:1",
                        "attempt": 1,
                        "payload": {
                            "event_type": "attention_created",
                            "attention": {"severity": "warning", "reason": "Needs input"},
                        },
                    }
                ],
            }

        def connector_ack(self, ref, response, **_kwargs):
            self.acked.append((ref, response))
            return {"ok": True}

        def connector_fail(self, ref, error, **_kwargs):
            self.failed.append((ref, error))
            return {"ok": True}

    class TopicMissingTelegram(FakeTelegram):
        def send_message(self, chat_id, html, **kwargs):
            if kwargs.get("thread_id"):
                return {"ok": False, "error": "Bad Request: message thread not found"}
            return super().send_message(chat_id, html, **kwargs)

    store = _store()
    tendwire = OutboxTendwire()
    telegram = TopicMissingTelegram()

    result = drain_outbox(store, telegram, tendwire, chat_id="-100", max_sends=1)

    assert result["delivered"] == 1
    assert result["acked"] == 1
    assert result["failed"] == 0
    assert tendwire.failed == []
    assert tendwire.acked == [("ref-1", {"telegram": "delivered"})]
    assert telegram.sent[-1][2].get("thread_id") is None


def test_long_telegram_send_splits_instead_of_truncating():
    class CapturingTelegram(TelegramClient):
        def __init__(self):
            super().__init__(token="fake")
            object.__setattr__(self, "payloads", [])

        def api(self, method, payload):
            self.payloads.append((method, payload))
            return {"ok": True, "result": {"message_id": len(self.payloads)}}

    telegram = CapturingTelegram()
    tail = "TAIL_MARKER_TELEGRAM_SPLIT"
    result = telegram.send_message("-100", "<b>Long</b>\n" + ("word " * 1200) + tail, thread_id="77")

    assert result["ok"] is True
    assert result["format"] == "plain-split"
    assert len(result["message_ids"]) > 1
    assert all(len(payload["text"]) <= 3900 for _method, payload in telegram.payloads)
    assert all("parse_mode" not in payload for _method, payload in telegram.payloads)
    assert any(tail in payload["text"] for _method, payload in telegram.payloads)


def test_existing_final_message_is_not_reposted_for_render_version_churn(monkeypatch):
    monkeypatch.setenv("HERDRES_TENDWIRE_MODE", "source")
    store = _store()
    telegram = FakeTelegram()
    turns = {
        "turns": [
            {
                "id": "turn-1",
                "worker_id": "worker-1",
                "worker_fingerprint": "fp-1",
                "assistant_final_text": "## **Fixed**\n\n- now rich",
                "complete": True,
            }
        ]
    }
    runtime = SyncRuntime(FakeTendwire(turns=turns), telegram, with_outbox=False)

    assert sync_once(store, runtime)["feed_sent"] == 1
    entry = next(iter(state.source_worker_entries(store).values()))
    entry["last_render_version"] = "old"
    sent_before = list(telegram.sent)
    telegram.edited.clear()

    assert sync_once(store, runtime)["feed_sent"] == 0
    assert telegram.sent == sent_before
    assert telegram.edited == []


def test_completed_turn_content_churn_is_not_reposted(monkeypatch):
    monkeypatch.setenv("HERDRES_TENDWIRE_MODE", "source")
    store = _store()
    telegram = FakeTelegram()
    first_turns = {
        "turns": [
            {
                "id": "turn-1",
                "worker_id": "worker-1",
                "worker_fingerprint": "fp-1",
                "assistant_final_text": "First final",
                "complete": True,
            }
        ]
    }
    second_turns = {
        "turns": [
            {
                "id": "turn-1",
                "worker_id": "worker-1",
                "worker_fingerprint": "fp-1",
                "assistant_final_text": "First final with formatting changed",
                "complete": True,
            }
        ]
    }

    first = sync_once(store, SyncRuntime(FakeTendwire(turns=first_turns), telegram, with_outbox=False))
    sent_before = list(telegram.sent)
    second = sync_once(store, SyncRuntime(FakeTendwire(turns=second_turns), telegram, with_outbox=False))

    assert first["feed_sent"] == 1
    assert second["feed_sent"] == 0
    assert second["turn_updates"] == 1
    assert telegram.sent == sent_before
    entry = next(iter(state.source_worker_entries(store).values()))
    assert entry["last_turn_id"] == "turn-1"
    assert len([key for key in store["tendwire_source_delivered_turns"] if key.startswith("final:turn-1:")]) == 1


def test_delivered_final_turn_repairs_stale_entry_without_repost(monkeypatch):
    monkeypatch.setenv("HERDRES_TENDWIRE_MODE", "source")
    store = _store()
    _worker_key, worker, _created = state.upsert_worker_entry(
        store,
        {"id": "worker-1", "name": "Alpha", "status": "idle", "space_id": "space-1", "fingerprint": "fp-1"},
    )
    worker["last_turn_id"] = "old-turn"
    worker["last_clean_message_id"] = "old-message"
    state.bind_message_to_worker(store, "555", worker, topic_id="77", kind="final", turn_id="turn-1", bot_kind="codex")
    state.mark_delivered(store, "final:turn-1:oldhash", {"worker_id": "worker-1", "turn_id": "turn-1"})
    telegram = FakeTelegram()
    turns = {
        "turns": [
            {
                "id": "turn-1",
                "worker_id": "worker-1",
                "worker_fingerprint": "fp-1",
                "assistant_final_text": "Current final text",
                "complete": True,
            }
        ]
    }

    result = sync_once(store, SyncRuntime(FakeTendwire(turns=turns), telegram, with_outbox=False))

    assert result["feed_sent"] == 0
    assert result["turn_updates"] == 1
    assert not any("Current final text" in sent[1] for sent in telegram.sent)
    entry = next(iter(state.source_worker_entries(store).values()))
    assert entry["last_turn_id"] == "turn-1"
    assert entry["last_clean_message_id"] == "555"
    assert entry["last_clean_message_ids"] == ["555"]
    assert entry["last_clean_bot_kind"] == "codex"


def test_historical_same_worker_final_is_suppressed_without_churning_latest(monkeypatch):
    monkeypatch.setenv("HERDRES_TENDWIRE_MODE", "source")
    store = _store()
    telegram = FakeTelegram()
    turns = {
        "turns": [
            {
                "id": "turn-new",
                "worker_id": "worker-1",
                "worker_fingerprint": "fp-1",
                "assistant_final_text": "New final",
                "complete": True,
            },
            {
                "id": "turn-old",
                "worker_id": "worker-1",
                "worker_fingerprint": "fp-1",
                "assistant_final_text": "Old final",
                "complete": True,
            },
        ]
    }

    first = sync_once(store, SyncRuntime(FakeTendwire(turns=turns), telegram, with_outbox=False))
    second = sync_once(store, SyncRuntime(FakeTendwire(turns=turns), telegram, with_outbox=False))

    assert first["feed_sent"] == 1
    assert "New final" in "\n".join(sent[1] for sent in telegram.sent)
    assert "Old final" not in "\n".join(sent[1] for sent in telegram.sent)
    entry = next(iter(state.source_worker_entries(store).values()))
    assert entry["last_turn_id"] == "turn-new"
    assert second["feed_sent"] == 0
    assert second["turn_updates"] == 0
    assert entry["last_turn_id"] == "turn-new"
    assert any(key.startswith("final:turn-old:") for key in store["tendwire_source_delivered_turns"])


def test_only_latest_working_turn_per_worker_is_delivered(monkeypatch):
    monkeypatch.setenv("HERDRES_TENDWIRE_MODE", "source")
    store = _store()
    telegram = FakeTelegram()
    turns = {
        "turns": [
            {
                "id": "turn-old-open",
                "worker_id": "worker-1",
                "worker_fingerprint": "fp-1",
                "assistant_stream_text": "old working text",
                "complete": False,
            },
            {
                "id": "turn-new-open",
                "worker_id": "worker-1",
                "worker_fingerprint": "fp-1",
                "assistant_stream_text": "new working text",
                "complete": False,
                "updated_at": "2026-07-03T16:24:15+00:00",
            },
        ]
    }

    first = sync_once(store, SyncRuntime(FakeTendwire(turns=turns), telegram, with_outbox=False))
    sent_text = "\n".join(sent[1] for sent in telegram.sent)
    sent_before = list(telegram.sent)
    second = sync_once(store, SyncRuntime(FakeTendwire(turns=turns), telegram, with_outbox=False))

    assert first["feed_sent"] == 1
    assert "new working text" in sent_text
    assert "old working text" not in sent_text
    entry = next(iter(state.source_worker_entries(store).values()))
    assert entry["last_stream_turn_id"] == "turn-new-open"
    assert second["feed_sent"] == 0
    assert telegram.sent == sent_before


def test_working_update_edits_existing_message(monkeypatch):
    monkeypatch.setenv("HERDRES_TENDWIRE_MODE", "source")
    store = _store()
    telegram = FakeTelegram()
    first_turns = {"turns": [{"id": "turn-1", "worker_id": "worker-1", "assistant_stream_text": "first", "complete": False}]}
    second_turns = {"turns": [{"id": "turn-1", "worker_id": "worker-1", "assistant_stream_text": "second", "complete": False}]}

    sync_once(store, SyncRuntime(FakeTendwire(turns=first_turns), telegram, with_outbox=False))
    sync_once(store, SyncRuntime(FakeTendwire(turns=second_turns), telegram, with_outbox=False))

    assert len(telegram.sent) >= 1
    assert telegram.edited
    assert "second" in telegram.edited[-1][2]


def test_command_reply_uses_hashed_request_id_without_raw_telegram_ids(tmp_path, monkeypatch):
    monkeypatch.setenv("HERDR_TELEGRAM_TOPICS_STATE", str(tmp_path / "state.json"))
    store = _store()
    state.upsert_worker_entry(
        store,
        {"id": "worker-1", "name": "Alpha", "status": "idle", "space_id": "space-1", "fingerprint": "fp-1"},
    )
    _space_key, entry, _created = state.upsert_space_entry(
        store,
        {"id": "space-1", "name": "Project", "status": "active", "fingerprint": "space-fp"},
        topic_id="77",
    )
    entry["active_worker_id"] = "worker-1"
    entry["active_worker_fingerprint"] = "fp-1"
    state.save_state(store)
    fake = FakeTendwire()

    class ClientFactory:
        def __call__(self):
            return fake

    monkeypatch.setattr(herdres, "TendwireClient", ClientFactory())
    result = herdres.command_reply(
        {
            "chat_id": "-100",
            "topic_id": "77",
            "message_id": "12345",
            "reply_to_message_id": "12344",
            "text": "/send hello",
        }
    )

    assert result == {"handled": True, "reply": "Sent to Tendwire worker."}
    request = fake.commands[0]
    assert request["target"] == {"worker_id": "worker-1", "worker_fingerprint": "fp-1"}
    encoded = json.dumps(request, sort_keys=True)
    assert "12345" not in encoded
    assert "-100" not in encoded
    assert "topic_id" not in encoded


def test_command_reply_to_agent_message_targets_original_worker(tmp_path, monkeypatch):
    monkeypatch.setenv("HERDR_TELEGRAM_TOPICS_STATE", str(tmp_path / "state.json"))
    store = _store()
    state.upsert_worker_entry(
        store,
        {"id": "worker-codex", "name": "codex", "status": "idle", "space_id": "space-1", "fingerprint": "fp-codex"},
    )
    _worker_key, claude, _created = state.upsert_worker_entry(
        store,
        {"id": "worker-claude", "name": "claude", "status": "idle", "space_id": "space-1", "fingerprint": "fp-claude"},
    )
    _space_key, entry, _created = state.upsert_space_entry(
        store,
        {"id": "space-1", "name": "Project", "status": "active", "fingerprint": "space-fp"},
        topic_id="77",
    )
    entry["active_worker_id"] = "worker-codex"
    entry["active_worker_fingerprint"] = "fp-codex"
    state.bind_message_to_worker(store, "555", claude, topic_id="77", kind="final", turn_id="turn-claude")
    state.save_state(store)
    fake = FakeTendwire()

    class ClientFactory:
        def __call__(self):
            return fake

    monkeypatch.setattr(herdres, "TendwireClient", ClientFactory())
    result = herdres.command_reply(
        {
            "chat_id": "-100",
            "topic_id": "77",
            "message_id": "999",
            "reply_to_message_id": "555",
            "text": "/send reply to claude",
        }
    )

    assert result == {"handled": True, "reply": "Sent to Tendwire worker."}
    request = fake.commands[0]
    assert request["target"] == {"worker_id": "worker-claude", "worker_fingerprint": "fp-claude"}
    assert request["instruction"] == {"text": "reply to claude"}


def test_command_reply_at_alias_targets_worker_in_space(tmp_path, monkeypatch):
    monkeypatch.setenv("HERDR_TELEGRAM_TOPICS_STATE", str(tmp_path / "state.json"))
    store = _store()
    state.upsert_worker_entry(
        store,
        {"id": "worker-codex", "name": "codex", "status": "idle", "space_id": "space-1", "fingerprint": "fp-codex"},
    )
    state.upsert_worker_entry(
        store,
        {"id": "worker-claude", "name": "claude", "status": "idle", "space_id": "space-1", "fingerprint": "fp-claude"},
    )
    _space_key, entry, _created = state.upsert_space_entry(
        store,
        {"id": "space-1", "name": "Project", "status": "active", "fingerprint": "space-fp"},
        topic_id="77",
    )
    entry["active_worker_id"] = "worker-codex"
    entry["active_worker_fingerprint"] = "fp-codex"
    state.save_state(store)
    fake = FakeTendwire()

    class ClientFactory:
        def __call__(self):
            return fake

    monkeypatch.setattr(herdres, "TendwireClient", ClientFactory())
    result = herdres.command_reply(
        {
            "chat_id": "-100",
            "topic_id": "77",
            "message_id": "999",
            "text": "/send @claude hello there",
        }
    )

    assert result == {"handled": True, "reply": "Sent to Tendwire worker."}
    request = fake.commands[0]
    assert request["target"] == {"worker_id": "worker-claude", "worker_fingerprint": "fp-claude"}
    assert request["instruction"] == {"text": "hello there"}


def test_command_reply_unknown_at_alias_fails_safely(tmp_path, monkeypatch):
    monkeypatch.setenv("HERDR_TELEGRAM_TOPICS_STATE", str(tmp_path / "state.json"))
    store = _store()
    state.upsert_worker_entry(
        store,
        {"id": "worker-codex", "name": "codex", "status": "idle", "space_id": "space-1", "fingerprint": "fp-codex"},
    )
    _space_key, entry, _created = state.upsert_space_entry(
        store,
        {"id": "space-1", "name": "Project", "status": "active", "fingerprint": "space-fp"},
        topic_id="77",
    )
    entry["active_worker_id"] = "worker-codex"
    entry["active_worker_fingerprint"] = "fp-codex"
    state.save_state(store)
    fake = FakeTendwire()

    class ClientFactory:
        def __call__(self):
            return fake

    monkeypatch.setattr(herdres, "TendwireClient", ClientFactory())
    result = herdres.command_reply(
        {
            "chat_id": "-100",
            "topic_id": "77",
            "message_id": "999",
            "text": "/send @missing hello",
        }
    )

    assert result["handled"] is True
    assert result["status"] == "unknown_target_alias"
    assert fake.commands == []


def test_gateway_maps_only_source_topic(monkeypatch):
    store = _store()
    state.upsert_worker_entry(
        store,
        {"id": "worker-1", "name": "Alpha", "status": "idle", "space_id": "space-1", "fingerprint": "fp-1"},
        topic_id="78",
    )
    _space_key, entry, _created = state.upsert_space_entry(
        store,
        {"id": "space-1", "name": "Project", "status": "active", "fingerprint": "space-fp"},
        topic_id="77",
    )
    payload = herdres_gateway._payload_for_message(
        {
            "chat": {"id": "-100", "is_forum": True},
            "message_thread_id": 77,
            "message_id": 10,
            "from": {"id": "1", "is_bot": False},
            "text": "hello",
        },
        store,
    )
    assert payload is not None
    assert payload["topic_id"] == "77"
    assert herdres_gateway._payload_for_message({"chat": {"id": "-100"}, "message_thread_id": 78}, store) is None


def test_runtime_has_no_direct_herdr_pane_api_names():
    forbidden = [
        "pane_list",
        "pane_by_id",
        "pane_turn",
        "prefetch_pane_turns",
        "send_to_pane",
        "pane send-keys",
        "pane read",
    ]
    runtime_files = [Path("herdres.py"), Path("herdres_gateway.py"), *Path("herdres_connector").glob("*.py")]
    text = "\n".join(path.read_text(encoding="utf-8") for path in runtime_files)
    for needle in forbidden:
        assert needle not in text


def test_public_prune_removes_private_fields():
    payload = {
        "ok": True,
        "chat_id": "-100",
        "topic_id": "77",
        "message_id": "10",
        "token": "secret",
        "target": {"worker_id": "w", "backend_target": "raw"},
    }
    clean = public_prune(payload)
    encoded = json.dumps(clean)
    assert "-100" not in encoded
    assert "secret" not in encoded
    assert "backend_target" not in encoded
