from __future__ import annotations

import herdres_gateway


def test_gateway_deletes_topic_icon_service_message(monkeypatch):
    deleted = []

    class FakeTelegram:
        def __init__(self, token):
            self.token = token

        def delete_message(self, chat_id, message_id):
            deleted.append((self.token, str(chat_id), str(message_id)))
            return {"ok": True}

    monkeypatch.setattr(herdres_gateway, "TelegramClient", FakeTelegram)
    monkeypatch.setenv("HERDR_TELEGRAM_TOPICS_DELETE_ICON_MESSAGES", "1")

    handled = herdres_gateway._delete_topic_icon_service_message(
        {
            "message_id": 123,
            "chat": {"id": "-100"},
            "forum_topic_edited": {"icon_custom_emoji_id": "emoji-id"},
        },
        {"telegram": {"chat_id": "-100"}},
        "token",
    )

    assert handled is True
    assert deleted == [("token", "-100", "123")]


def test_gateway_keeps_non_icon_topic_edit(monkeypatch):
    deleted = []

    class FakeTelegram:
        def __init__(self, token):
            self.token = token

        def delete_message(self, chat_id, message_id):
            deleted.append((self.token, str(chat_id), str(message_id)))
            return {"ok": True}

    monkeypatch.setattr(herdres_gateway, "TelegramClient", FakeTelegram)

    handled = herdres_gateway._delete_topic_icon_service_message(
        {
            "message_id": 123,
            "chat": {"id": "-100"},
            "forum_topic_edited": {"name": "Workers"},
        },
        {"telegram": {"chat_id": "-100"}},
        "token",
    )

    assert handled is False
    assert deleted == []
