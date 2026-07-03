from __future__ import annotations

import json
from pathlib import Path

import herdres
import herdres_gateway
from herdres_connector import state
from herdres_connector.rendering import render_final_turn
from herdres_connector.safe import public_prune
from herdres_connector.source_sync import SyncRuntime, sync_once
from herdres_connector.telegram_delivery import TelegramClient


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
    token = "fake"
    dry_run = False

    def __init__(self):
        self.sent = []
        self.edited = []
        self.topics = []
        self.deleted_topics = []
        self.pins = []

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
        self.sent.append((chat_id, html, kwargs, message_id))
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


def test_final_response_renders_common_markdown_as_telegram_html():
    html = render_final_turn(
        {
            "user_text": "Question",
            "assistant_final_text": "## **Fix it**\n\n- keep **bold**\n- escape <tags>\n\nUse `code`.",
        },
        {"topic_name": "Alpha", "tendwire_worker_id": "worker-1"},
    )

    assert "##" not in html
    assert "**" not in html
    assert "<b>Fix it</b>" in html
    assert "• keep <b>bold</b>" in html
    assert "escape &lt;tags&gt;" in html
    assert "<code>code</code>" in html
    assert "<b>Response</b>" in html
    assert "<blockquote>" in html


def test_long_final_response_uses_full_visible_response_section():
    html = render_final_turn(
        {
            "user_text": "Question",
            "assistant_final_text": "## **Plan**\n\n" + "- keep **rich** sections\n" * 80,
        },
        {"topic_name": "Alpha", "tendwire_worker_id": "worker-1"},
    )

    assert "<b>Response</b>" in html
    assert "<blockquote>" in html
    assert "<blockquote expandable>" not in html
    assert "##" not in html
    assert "**" not in html
    assert "• keep <b>rich</b> sections" in html


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


def test_existing_final_message_is_edited_to_current_rich_render(monkeypatch):
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
    telegram.edited.clear()

    assert sync_once(store, runtime)["feed_sent"] == 1
    assert telegram.edited
    assert "<b>Fixed</b>" in telegram.edited[-1][2]
    assert "##" not in telegram.edited[-1][2]


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

    assert result == {"handled": True, "reply": ""}
    request = fake.commands[0]
    assert request["target"] == {"worker_id": "worker-1", "worker_fingerprint": "fp-1"}
    encoded = json.dumps(request, sort_keys=True)
    assert "12345" not in encoded
    assert "-100" not in encoded
    assert "topic_id" not in encoded


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
