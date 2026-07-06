from __future__ import annotations

from herdres_connector.rich_delivery import edit_rich_message, send_rich_message
from herdres_connector.telegram_delivery import TelegramError


class FakeTelegram:
    def __init__(self, error: str):
        self.error = error
        self.calls: list[tuple[str, str]] = []

    def with_token(self, _token):
        return self

    def api(self, method, _payload):
        self.calls.append(("api", method))
        raise TelegramError(self.error)

    def send_message(self, _chat_id, _html, **_kwargs):
        self.calls.append(("send_message", "legacy"))
        return {"ok": True, "message_id": "123", "format": "html"}

    def edit_message(self, _chat_id, _message_id, _html):
        self.calls.append(("edit_message", "legacy"))
        return {"ok": True, "message_id": "42", "format": "html"}


def test_transient_rich_send_error_retries_without_plain_fallback():
    telegram = {"rich_messages": {"supported": "yes"}}
    client = FakeTelegram("[SSL: UNEXPECTED_EOF_WHILE_READING] EOF occurred in violation of protocol")

    result = send_rich_message(client, "-100", "<p>Hello</p>", telegram=telegram, thread_id="77")

    assert result["ok"] is False
    assert result["format"] == "rich"
    assert result["kind"] == "transient"
    assert client.calls == [("api", "sendRichMessage")]
    assert telegram["rich_messages"]["supported"] == "yes"


def test_transient_rich_edit_error_retries_without_plain_fallback():
    telegram = {"rich_messages": {"supported": "yes"}}
    client = FakeTelegram("timed out while calling Telegram")

    result = edit_rich_message(client, "-100", "42", "<p>Hello</p>", telegram=telegram)

    assert result["ok"] is False
    assert result["format"] == "rich"
    assert result["kind"] == "transient"
    assert client.calls == [("api", "editMessageText")]
    assert telegram["rich_messages"]["supported"] == "yes"


def test_capability_rich_send_error_still_falls_back_and_disables_rich():
    telegram = {"rich_messages": {"supported": "unknown"}}
    client = FakeTelegram("Not Found: method not found")

    result = send_rich_message(client, "-100", "<p>Hello</p>", telegram=telegram, thread_id="77")

    assert result["ok"] is True
    assert result["fallback_reason"] == "capability"
    assert client.calls == [("api", "sendRichMessage"), ("send_message", "legacy")]
    assert telegram["rich_messages"]["supported"] == "no"
