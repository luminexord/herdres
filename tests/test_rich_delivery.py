from __future__ import annotations

import hashlib

from herdres_connector import source_sync, state
from herdres_connector.rich_delivery import (
    edit_rich_message,
    send_rich_message,
)
from herdres_connector.telegram_delivery import TelegramError


class FakeTelegram:
    def __init__(self, error: str):
        self.error = error
        self.calls: list[tuple[str, str]] = []
        self.sent_texts: list[str] = []
        self.plain_succeeded = False
        self.fail_plain = False

    def with_token(self, _token):
        return self

    def api(self, method, _payload):
        self.calls.append(("api", method))
        if method == "sendRichMessage" and not self.plain_succeeded:
            raise AssertionError("rich enhancement ran before canonical plain send")
        raise TelegramError(self.error)

    def send_message(self, _chat_id, html, **_kwargs):
        self.calls.append(("send_message", "legacy"))
        self.sent_texts.append(str(html))
        if self.fail_plain:
            return {"ok": False, "error": "plain rejected"}
        self.plain_succeeded = True
        return {"ok": True, "message_id": "123", "format": "html"}

    def edit_message(self, _chat_id, _message_id, _html):
        self.calls.append(("edit_message", "legacy"))
        return {"ok": True, "message_id": "42", "format": "html"}


def test_table_delivery_is_exactly_one_canonical_plain_send():
    telegram = {"rich_messages": {"supported": "yes"}}
    client = FakeTelegram("sendRichMessage must not run")

    result = send_rich_message(
        client,
        "-100",
        "<h3>Results</h3><table><tr><th>Name</th><th>Status</th></tr>"
        "<tr><td>Ada</td><td>Ready</td></tr></table>",
        telegram=telegram,
        thread_id="77",
    )

    assert result["ok"] is True
    assert result["message_id"] == "123"
    assert client.calls == [("send_message", "legacy")]
    assert client.sent_texts == ["Results\nName | Status\nAda | Ready"]
    assert telegram["rich_messages"]["supported"] == "yes"


def test_rich_edit_keeps_the_durable_message_readable():
    telegram = {"rich_messages": {"supported": "yes"}}
    client = FakeTelegram("timed out while calling Telegram")

    result = edit_rich_message(client, "-100", "42", "<p>Hello</p>", telegram=telegram)

    assert result["ok"] is True
    assert result["format"] == "html"
    assert client.calls == [("edit_message", "legacy")]
    assert telegram["rich_messages"]["supported"] == "yes"


def test_rich_capability_state_does_not_add_a_second_write():
    telegram = {"rich_messages": {"supported": "unknown"}}
    client = FakeTelegram("Not Found: method not found")

    result = send_rich_message(
        client,
        "-100",
        "<h3>Results</h3><table><tr><th>Name</th><th>Status</th></tr>"
        "<tr><td>Ada</td><td>Ready</td></tr></table>",
        telegram=telegram,
        thread_id="77",
    )

    assert result["ok"] is True
    assert client.calls == [("send_message", "legacy")]
    assert telegram["rich_messages"]["supported"] == "unknown"


def test_rich_capable_paragraph_is_delivered_as_nonempty_plain_text():
    telegram = {"rich_messages": {"supported": "yes"}}
    client = FakeTelegram("rich should not run")

    result = send_rich_message(
        client,
        "-100",
        "<p>The exact response the owner must read.</p>",
        telegram=telegram,
        thread_id="77",
    )

    assert result["ok"] is True
    assert client.sent_texts == ["The exact response the owner must read."]
    assert client.calls == [("send_message", "legacy")]


def test_supported_table_still_uses_one_physical_write():
    class SupportedRichTelegram(FakeTelegram):
        def api(self, method, _payload):
            raise AssertionError(f"unexpected second physical write: {method}")

    telegram = {"rich_messages": {"supported": "yes"}}
    client = SupportedRichTelegram("")

    result = send_rich_message(
        client,
        "-100",
        "<table><tr><th>Name</th><th>Status</th></tr>"
        "<tr><td>Ada</td><td>Ready</td></tr></table>",
        telegram=telegram,
        thread_id="77",
    )

    assert result["ok"] is True
    assert result["message_id"] == "123"
    assert client.calls == [("send_message", "legacy")]


def test_plain_failure_is_the_only_failed_write():
    telegram = {"rich_messages": {"supported": "yes"}}
    client = FakeTelegram("rich must not run")
    client.fail_plain = True

    result = send_rich_message(
        client,
        "-100",
        "<table><tr><th>Name</th><th>Status</th></tr>"
        "<tr><td>Ada</td><td>Ready</td></tr></table>",
        telegram=telegram,
        thread_id="77",
    )

    assert result["ok"] is False
    assert client.calls == [("send_message", "legacy")]


def test_table_plain_text_uses_pipe_delimited_rows():
    telegram = {"rich_messages": {"supported": "no"}}
    client = FakeTelegram("rich must not run")

    result = send_rich_message(
        client,
        "-100",
        "<h3>Results</h3><table><tr><th>Name</th><th>Status</th></tr>"
        "<tr><td>Ada</td><td>Ready</td></tr></table>",
        telegram=telegram,
        thread_id="77",
    )

    assert result["ok"] is True
    assert client.sent_texts == ["Results\nName | Status\nAda | Ready"]


def _guarded_store() -> dict:
    stable_key = "wsk1_" + hashlib.sha256(b"worker-1").hexdigest()
    return {
        "enabled": True,
        "telegram": {
            "chat_id": "-100",
            "rich_messages": {"supported": "unknown"},
        },
        "panes": {
            "worker-entry": {
                "source": "tendwire",
                "entry_type": "worker",
                "tendwire_worker_id": "worker-1",
                "tendwire_stable_key": stable_key,
                "tendwire_stable_key_version": 1,
                "topic_id": "77",
            }
        },
        "spaces": {},
    }


def _guarded_rich_send(current, telegram):
    entry = current["panes"]["worker-entry"]
    return source_sync._execute_entry_operation(
        current,
        source_sync._OfflockClient(telegram, current, "telegram"),
        source_sync._capture_entry_operation(current, entry),
        source_sync._provider_mutation(
            "telegram.send_feed_item",
            reason=(
                "telegram.send_feed_item: rich capability persistence "
                "regression"
            ),
            args=(
                "-100",
                {
                    "kind": "notice",
                    "title": "Test",
                    "summary": "| Name | Status |\n| --- | --- |\n| Ada | Ready |",
                },
            ),
            kwargs={
                "telegram": current["telegram"],
                "thread_id": "77",
            },
        ),
    )


def test_guarded_table_delivery_is_plain_only_and_leaves_rich_state_unchanged(
    tmp_path, monkeypatch
):
    state_path = tmp_path / "state.json"
    monkeypatch.setenv("HERDR_TELEGRAM_TOPICS_STATE", str(state_path))

    store = _guarded_store()
    state.save_state(store, state_path)
    with state.state_lock(state_path):
        current = state.load_state(state_path)
        client = FakeTelegram("sendRichMessage must not run")
        result = _guarded_rich_send(
            current, client
        )
        assert result.result["ok"] is True
        assert client.calls == [("send_message", "legacy")]
        assert current["telegram"]["rich_messages"]["supported"] == "unknown"
        state.save_state(current, state_path)
    assert state.load_state(state_path)["telegram"]["rich_messages"][
        "supported"
    ] == "unknown"
