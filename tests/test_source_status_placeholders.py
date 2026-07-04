from __future__ import annotations

from herdres_connector import state
from herdres_connector.source_sync import SyncRuntime, sync_once


class FakeTendwire:
    def __init__(self, turns=None):
        self._turns = turns if turns is not None else {"turns": []}
        self._workers = [
            {
                "id": "worker-live",
                "name": "claude",
                "status": "active",
                "space_id": "space-workers",
                "fingerprint": "fp-live",
                "meta": {"agent": "claude", "raw_status": "working"},
            }
        ]
        self._spaces = [
            {
                "id": "space-workers",
                "name": "Workers",
                "status": "active",
                "fingerprint": "space-fp",
            }
        ]

    def snapshot(self):
        return {"ok": True, "workers": self._workers, "spaces": self._spaces}

    def turns(self):
        return self._turns

    def pending(self):
        return {"pending_interactions": []}

    def connector_poll(self, **_kwargs):
        return {"ok": True, "items": []}


class FakeTelegram:
    dry_run = False

    def __init__(self):
        self.sent = []
        self.edited = []
        self.icon_edits = []

    def with_token(self, _token):
        return self

    def api(self, method, payload):
        if method == "sendRichMessage":
            message_id = str(200 + len(self.sent))
            self.sent.append(
                (
                    str(payload.get("chat_id") or ""),
                    str(payload.get("rich_message") or ""),
                    {"thread_id": str(payload.get("message_thread_id") or "")},
                    message_id,
                )
            )
            return {"ok": True, "result": {"message_id": message_id}}
        if method == "editMessageText":
            self.edited.append((str(payload.get("chat_id") or ""), str(payload.get("message_id") or ""), str(payload.get("rich_message") or "")))
            return {"ok": True, "result": {"message_id": str(payload.get("message_id") or "0")}}
        if method == "getForumTopicIconStickers":
            return {
                "ok": True,
                "result": [
                    {"emoji": "⚡️", "custom_emoji_id": "icon-working"},
                    {"emoji": "✅", "custom_emoji_id": "icon-idle"},
                ],
            }
        return {"ok": True, "result": {"message_id": 0}}

    def send_message(self, chat_id, html, **kwargs):
        message_id = str(200 + len(self.sent))
        self.sent.append((str(chat_id), str(html), dict(kwargs), message_id))
        return {"ok": True, "message_id": message_id}

    def edit_message(self, chat_id, message_id, html):
        self.edited.append((str(chat_id), str(message_id), str(html)))
        return {"ok": True, "message_id": str(message_id)}

    def pin_message(self, _chat_id, _message_id):
        return {"ok": True}

    def create_topic(self, _chat_id, _name):
        return {"ok": True, "topic_id": "77"}

    def edit_topic_icon(self, chat_id, thread_id, emoji_id):
        self.icon_edits.append((str(chat_id), str(thread_id), str(emoji_id)))
        return {"ok": True}

    def delete_topic(self, _chat_id, _thread_id):
        return {"ok": True}


def _store():
    return {
        "enabled": True,
        "telegram": {"chat_id": "-100", "general_thread_id": "1"},
        "panes": {},
        "spaces": {},
        "tendwired_bootstrap_complete": True,
    }


def test_repaired_same_turn_id_with_changed_final_edits_existing_final(monkeypatch):
    monkeypatch.setenv("HERDRES_TENDWIRE_MODE", "source")
    store = _store()
    telegram = FakeTelegram()
    tendwire = FakeTendwire(
        turns={
            "turns": [
                {
                    "id": "turn-reused",
                    "worker_id": "worker-live",
                    "space_id": "space-workers",
                    "assistant_final_text": "old final",
                    "complete": True,
                }
            ]
        }
    )
    tendwire._workers[0]["status"] = "idle"
    tendwire._workers[0]["meta"] = {"agent": "claude"}

    first = sync_once(store, SyncRuntime(tendwire, telegram, with_outbox=False))
    worker = next(iter(state.source_worker_entries(store).values()))
    first_message_id = worker["last_clean_message_id"]
    tendwire._turns = {
        "turns": [
            {
                "id": "turn-reused",
                "worker_id": "worker-live",
                "space_id": "space-workers",
                "assistant_final_text": "new final",
                "complete": True,
            }
        ]
    }
    second = sync_once(store, SyncRuntime(tendwire, telegram, with_outbox=False))
    third = sync_once(store, SyncRuntime(tendwire, telegram, with_outbox=False))

    assert first["feed_sent"] == 1
    assert second["feed_sent"] == 0
    assert third["feed_sent"] == 1
    assert worker["last_clean_message_id"] == first_message_id
    assert any(edit[1] == first_message_id and "new final" in edit[2] for edit in telegram.edited)
