from __future__ import annotations

import hashlib

from herdres_connector import source_sync, state
from herdres_connector.rich_delivery import (
    RICH_STATE_UPDATE_KEY,
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


def test_transient_rich_send_error_keeps_canonical_plain_success():
    telegram = {"rich_messages": {"supported": "yes"}}
    client = FakeTelegram("[SSL: UNEXPECTED_EOF_WHILE_READING] EOF occurred in violation of protocol")

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
    assert result["rich_enhancement"]["ok"] is False
    assert result["rich_enhancement"]["kind"] == "transient"
    assert client.calls == [
        ("send_message", "legacy"),
        ("api", "sendRichMessage"),
    ]
    assert telegram["rich_messages"]["supported"] == "yes"


def test_rich_edit_keeps_the_durable_message_readable():
    telegram = {"rich_messages": {"supported": "yes"}}
    client = FakeTelegram("timed out while calling Telegram")

    result = edit_rich_message(client, "-100", "42", "<p>Hello</p>", telegram=telegram)

    assert result["ok"] is True
    assert result["format"] == "html"
    assert client.calls == [("edit_message", "legacy")]
    assert telegram["rich_messages"]["supported"] == "yes"


def test_capability_rich_send_error_keeps_plain_and_disables_rich():
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
    assert result["rich_enhancement"]["kind"] == "capability"
    assert client.calls == [
        ("send_message", "legacy"),
        ("api", "sendRichMessage"),
    ]
    assert telegram["rich_messages"]["supported"] == "unknown"
    assert result[RICH_STATE_UPDATE_KEY]["transition"] == "disabled"


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


def test_table_rich_enhancement_runs_only_after_plain_acceptance():
    class SupportedRichTelegram(FakeTelegram):
        def api(self, method, _payload):
            self.calls.append(("api", method))
            assert self.plain_succeeded is True
            return {"ok": True, "result": {"message_id": 456}}

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
    assert result["message_ids"] == ["123", "456"]
    assert result["rich_message_id"] == "456"
    assert client.calls == [
        ("send_message", "legacy"),
        ("api", "sendRichMessage"),
    ]


def test_plain_failure_prevents_rich_enhancement():
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


def test_guarded_rich_capability_transitions_persist_after_reload(
    tmp_path, monkeypatch
):
    state_path = tmp_path / "state.json"
    monkeypatch.setenv("HERDR_TELEGRAM_TOPICS_STATE", str(state_path))

    capability = _guarded_store()
    state.save_state(capability, state_path)
    with state.state_lock(state_path):
        current = state.load_state(state_path)
        result = _guarded_rich_send(
            current, FakeTelegram("Not Found: method not found")
        )
        assert result.result["ok"] is True
        assert current["telegram"]["rich_messages"]["supported"] == "no"
        state.save_state(current, state_path)
    assert state.load_state(state_path)["telegram"]["rich_messages"][
        "supported"
    ] == "no"

    supported = _guarded_store()
    state.save_state(supported, state_path)

    class SupportedTelegram(FakeTelegram):
        def api(self, method, _payload):
            self.calls.append(("api", method))
            return {"ok": True, "result": {"message_id": 501}}

    with state.state_lock(state_path):
        current = state.load_state(state_path)
        result = _guarded_rich_send(current, SupportedTelegram(""))
        assert result.result["format"] == "plain+rich"
        assert current["telegram"]["rich_messages"]["supported"] == "yes"
        state.save_state(current, state_path)
    assert state.load_state(state_path)["telegram"]["rich_messages"][
        "supported"
    ] == "yes"

    bad_request = _guarded_store()
    state.save_state(bad_request, state_path)
    with state.state_lock(state_path):
        current = state.load_state(state_path)
        for _index in range(3):
            result = _guarded_rich_send(
                current, FakeTelegram("Bad Request: malformed rich payload")
            )
            assert result.result["ok"] is True
        rich = current["telegram"]["rich_messages"]
        assert rich["bad_request_streak"] == 3
        assert rich["supported"] == "no"
        assert "repeated bad_request" in rich["disabled_reason"]
        state.save_state(current, state_path)
    persisted = state.load_state(state_path)["telegram"]["rich_messages"]
    assert persisted["bad_request_streak"] == 3
    assert persisted["supported"] == "no"
