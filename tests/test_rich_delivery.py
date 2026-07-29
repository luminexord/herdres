from __future__ import annotations

import hashlib
from html.parser import HTMLParser

import pytest

from herdres_connector import source_sync, state
from herdres_connector.rendering import telegram_html
from herdres_connector.rich_delivery import (
    _replace_inline_links,
    edit_rich_message,
    send_feed_item,
    send_rich_message,
)
from herdres_connector.telegram_delivery import TelegramClient, TelegramError


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


class _RecipientHTMLParser(HTMLParser):
    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.text: list[str] = []
        self.entities: list[dict[str, str]] = []
        self.unsupported_tags: list[str] = []
        self.open_tags: list[str] = []

    def handle_starttag(self, tag, attrs):
        supported = {
            "a",
            "b",
            "blockquote",
            "code",
            "em",
            "i",
            "pre",
            "s",
            "strong",
            "u",
        }
        if tag not in supported:
            self.unsupported_tags.append(tag)
            return
        attributes = dict(attrs)
        entity_type = {
            "b": "Bold",
            "strong": "Bold",
            "i": "Italic",
            "em": "Italic",
            "u": "Underline",
            "s": "Strikethrough",
            "code": "Code",
            "pre": "Pre",
            "a": "TextUrl",
            "blockquote": "Blockquote",
        }.get(tag)
        suppressed = any(
            parent in {"code", "pre"} for parent in self.open_tags
        )
        if tag == "a" and "a" in self.open_tags:
            suppressed = True
        if tag == "blockquote" and "blockquote" in self.open_tags:
            suppressed = True
        href = str(attributes.get("href") or "")
        if tag == "a" and not href.lower().startswith(
            ("http://", "https://", "mailto:", "tg://")
        ):
            suppressed = True
        if entity_type and not suppressed:
            entity = {"type": entity_type}
            if tag == "a":
                entity["url"] = href
            self.entities.append(entity)
        self.open_tags.append(tag)

    def handle_endtag(self, tag):
        for index in range(len(self.open_tags) - 1, -1, -1):
            if self.open_tags[index] == tag:
                del self.open_tags[index]
                break

    def handle_data(self, data):
        self.text.append(data)


class RecipientTelegram(TelegramClient):
    """Telegram test adapter that records what a recipient can read."""

    def __init__(self, *, reject_html=False):
        super().__init__(token="test")
        object.__setattr__(self, "reject_html", reject_html)
        object.__setattr__(self, "recipient_messages", [])
        object.__setattr__(self, "attempts", [])

    def api(self, method, payload):
        assert method == "sendMessage"
        text = str(payload.get("text") or "")
        parse_mode = str(payload.get("parse_mode") or "")
        self.attempts.append({"text": text, "parse_mode": parse_mode})
        if parse_mode == "HTML" and self.reject_html:
            raise TelegramError("can't parse entities")
        parser = _RecipientHTMLParser()
        if parse_mode == "HTML":
            parser.feed(text)
            parser.close()
            if parser.unsupported_tags:
                raise TelegramError(
                    f"unsupported tag: {parser.unsupported_tags[0]}"
                )
            received_text = "".join(parser.text)
            entities = parser.entities
        else:
            received_text = text
            entities = []
        self.recipient_messages.append(
            {"text": received_text, "entities": entities}
        )
        return {
            "ok": True,
            "result": {
                "message_id": len(self.recipient_messages),
            },
        }


def test_canonical_send_preserves_recipient_formatting_entities():
    client = RecipientTelegram()

    result = send_feed_item(
        client,
        "-100",
        {
            "kind": "turn",
            "assistant_final_text": (
                "**Bold** `inline()` "
                "[link](https://example.test/path)"
            ),
        },
        telegram={},
        thread_id="77",
    )

    assert result["ok"] is True
    assert result["format"] == "html"
    entities = client.recipient_messages[0]["entities"]
    assert {"type": "Bold"} in entities
    assert {"type": "Code"} in entities
    assert {
        "type": "TextUrl",
        "url": "https://example.test/path",
    } in entities
    assert "Bold inline() link" in client.recipient_messages[0]["text"]


@pytest.mark.parametrize(
    ("markup", "entity"),
    [
        pytest.param("<b>bold</b>", {"type": "Bold"}, id="bold"),
        pytest.param("<i>italic</i>", {"type": "Italic"}, id="italic"),
        pytest.param(
            "<u>underline</u>", {"type": "Underline"}, id="underline"
        ),
        pytest.param(
            "<s>strike</s>",
            {"type": "Strikethrough"},
            id="strikethrough",
        ),
        pytest.param("<code>inline</code>", {"type": "Code"}, id="code"),
        pytest.param("<pre>block</pre>", {"type": "Pre"}, id="pre"),
        pytest.param(
            '<a href="https://example.test/path">link</a>',
            {
                "type": "TextUrl",
                "url": "https://example.test/path",
            },
            id="link",
        ),
        pytest.param(
            "<blockquote>quote</blockquote>",
            {"type": "Blockquote"},
            id="blockquote",
        ),
    ],
)
def test_each_promised_format_reaches_the_recipient(markup, entity):
    client = RecipientTelegram()

    result = send_rich_message(
        client,
        "-100",
        markup,
        telegram={},
    )

    assert result["ok"] is True
    assert result["format"] == "html"
    assert entity in client.recipient_messages[0]["entities"]


@pytest.mark.parametrize(
    ("markup", "expected_entities"),
    [
        pytest.param(
            "<b>Run <code>cmd</code></b>",
            [{"type": "Bold"}, {"type": "Code"}],
            id="bold-with-code",
        ),
        pytest.param(
            "<code>x <b>y</b> z</code>",
            [{"type": "Code"}],
            id="code-drops-inner",
        ),
        pytest.param(
            "<pre>x <b>y</b> z</pre>",
            [{"type": "Pre"}],
            id="pre-drops-inner",
        ),
        pytest.param(
            '<blockquote>q <a href="https://example.test/q">link</a>'
            "</blockquote>",
            [
                {"type": "Blockquote"},
                {
                    "type": "TextUrl",
                    "url": "https://example.test/q",
                },
            ],
            id="blockquote-with-link",
        ),
        pytest.param(
            '<a href="https://example.test/a"><b>linked</b></a>',
            [
                {
                    "type": "TextUrl",
                    "url": "https://example.test/a",
                },
                {"type": "Bold"},
            ],
            id="link-with-bold",
        ),
        pytest.param(
            '<a href="https://example.test/outer">outer '
            '<a href="https://example.test/inner">inner</a></a>',
            [
                {
                    "type": "TextUrl",
                    "url": "https://example.test/outer",
                }
            ],
            id="nested-anchor-keeps-outer",
        ),
        pytest.param(
            "<blockquote>outer <blockquote>inner</blockquote></blockquote>",
            [{"type": "Blockquote"}],
            id="nested-blockquote-flattens-inner",
        ),
        pytest.param(
            '<a href="javascript:alert(1)">unsafe</a>',
            [],
            id="javascript-href-stripped",
        ),
        pytest.param(
            '<a href="data:text/plain,unsafe">unsafe</a>',
            [],
            id="data-href-stripped",
        ),
        pytest.param(
            "<b>a <i>b</i> c</b>",
            [{"type": "Bold"}, {"type": "Italic"}],
            id="bold-with-italic",
        ),
    ],
)
def test_recipient_fake_matches_measured_telegram_entity_behavior(
    markup, expected_entities
):
    client = RecipientTelegram()

    result = client.send_message("-100", markup)

    assert result["ok"] is True
    assert result["format"] == "html"
    assert client.recipient_messages[0]["entities"] == expected_entities


@pytest.mark.parametrize(
    ("markdown", "expected_text", "expected_url"),
    [
        pytest.param(
            "[wiki](https://example.test/Foo_(bar))",
            "✅ Response\nwiki",
            "https://example.test/Foo_(bar)",
            id="balanced-parentheses",
        ),
        pytest.param(
            r"[wiki](https://example.test/Foo_\))",
            "✅ Response\nwiki",
            "https://example.test/Foo_)",
            id="escaped-unbalanced-closing-parenthesis",
        ),
        pytest.param(
            r"[wiki](https://example.test/\(Foo\))",
            "✅ Response\nwiki",
            "https://example.test/(Foo)",
            id="escaped-balanced-parentheses",
        ),
        pytest.param(
            "[wiki](https://example.test/Foo))",
            "✅ Response\nwiki)",
            "https://example.test/Foo",
            id="trailing-prose-parenthesis",
        ),
        pytest.param(
            r"[wiki](https://example.test/end\\)",
            "✅ Response\nwiki",
            "https://example.test/end\\",
            id="doubled-terminal-backslash",
        ),
        pytest.param(
            "[wiki](https://example.test/[one])",
            "✅ Response\nwiki",
            "https://example.test/[one]",
            id="destination-brackets",
        ),
    ],
)
def test_markdown_link_destination_is_preserved_exactly(
    markdown, expected_text, expected_url
):
    client = RecipientTelegram()

    result = send_feed_item(
        client,
        "-100",
        {"kind": "turn", "assistant_final_text": markdown},
        telegram={},
        thread_id="77",
    )

    assert result["ok"] is True
    assert result["format"] == "html"
    assert client.recipient_messages[0]["text"] == expected_text
    assert {
        "type": "TextUrl",
        "url": expected_url,
    } in client.recipient_messages[0]["entities"]


@pytest.mark.parametrize(
    "markdown",
    [
        pytest.param(
            "[wiki](https://example.test/Foo bar)",
            id="space",
        ),
        pytest.param(
            "[wiki](<https://example.test/Foo>)",
            id="angle-brackets",
        ),
        pytest.param(
            r"[wiki](https://example.test/end\)",
            id="terminal-backslash",
        ),
        pytest.param(
            "[wiki](https://example.test/(unterminated)",
            id="unterminated-parenthesis",
        ),
        pytest.param(
            "[[[[not a link",
            id="unmatched-bracket-run",
        ),
        pytest.param(
            "![image](https://example.test/image.png)",
            id="image-syntax",
        ),
    ],
)
def test_ambiguous_markdown_link_destination_stays_literal(markdown):
    client = RecipientTelegram()

    result = send_feed_item(
        client,
        "-100",
        {"kind": "turn", "assistant_final_text": markdown},
        telegram={},
        thread_id="77",
    )

    assert result["ok"] is True
    assert result["format"] == "html"
    assert client.recipient_messages[0]["text"] == (
        f"✅ Response\n{markdown}"
    )
    assert not any(
        entity["type"] == "TextUrl"
        for entity in client.recipient_messages[0]["entities"]
    )


def test_adjacent_markdown_links_preserve_both_destinations():
    client = RecipientTelegram()

    result = send_feed_item(
        client,
        "-100",
        {
            "kind": "turn",
            "assistant_final_text": (
                "[one](https://example.test/1)"
                "[two](https://example.test/2)"
            ),
        },
        telegram={},
        thread_id="77",
    )

    assert result["ok"] is True
    assert client.recipient_messages[0]["text"] == "✅ Response\nonetwo"
    assert [
        entity["url"]
        for entity in client.recipient_messages[0]["entities"]
        if entity["type"] == "TextUrl"
    ] == [
        "https://example.test/1",
        "https://example.test/2",
    ]


def test_pathological_64k_link_scan_has_bounded_linear_work():
    tail = "x](https://example.test)"
    source = "[" * (64000 - len(tail)) + tail
    work = [0]
    link_spans = []

    rendered = _replace_inline_links(
        source,
        link_spans,
        _work_counter=work,
    )

    assert rendered.endswith("\u00010\u0001")
    assert link_spans == [
        '<a href="https://example.test">x</a>'
    ]
    assert work[0] <= len(source) * 2


def test_unsupported_nested_tag_is_flattened_without_plain_fallback():
    client = RecipientTelegram()

    result = send_rich_message(
        client,
        "-100",
        "<h3>Result</h3><b>before <widget><code>x</code></widget> "
        "after</b><table><tr><th>Name</th><th>Status</th></tr>"
        "<tr><td>Ada</td><td>Ready</td></tr></table>",
        telegram={},
    )

    assert result["ok"] is True
    assert result["format"] == "html"
    assert len(client.attempts) == 1
    assert client.recipient_messages[0]["text"] == (
        "Result\nbefore x afterName | Status\nAda | Ready"
    )
    assert client.recipient_messages[0]["entities"] == [
        {"type": "Bold"},
        {"type": "Code"},
    ]


def test_telegram_html_allowlist_balances_and_escapes_edge_cases():
    sanitized = telegram_html(
        '<b class="ignored">bold<unknown> nested</unknown>'
        '<a href="https://example.test/?a=1&amp;b=2" title="drop">link</a>'
        '<pre><b>literal & raw</b></pre>'
        "<i>unclosed"
    )

    assert sanitized == (
        '<b>bold nested<a href="https://example.test/?a=1&amp;b=2">'
        "link</a><pre>&lt;b&gt;literal &amp; raw&lt;/b&gt;</pre>"
        "<i>unclosed</i></b>"
    )
    assert telegram_html("2 < 3 & 5") == "2 &lt; 3 &amp; 5"
    assert (
        telegram_html('<a href="javascript:alert(1)">unsafe</a>')
        == "unsafe"
    )
    assert (
        telegram_html('<a href="data:text/plain,unsafe">unsafe</a>')
        == "unsafe"
    )


def test_rejected_html_still_delivers_nonempty_readable_plain_text():
    client = RecipientTelegram(reject_html=True)

    result = client.send_message("-100", "<b>Readable fallback</b>")

    assert result["ok"] is True
    assert result["format"] == "plain"
    assert len(client.attempts) == 2
    assert client.recipient_messages == [
        {"text": "Readable fallback", "entities": []}
    ]


def test_html_rejection_plain_fallback_is_readable_and_recorded():
    client = RecipientTelegram(reject_html=True)
    store = {"telegram": {}}

    result = source_sync._execute_exact_provider_operation(
        client,
        mutation=source_sync._provider_mutation(
            "telegram.send_message",
            reason=(
                "telegram.send_message: test formatted delivery fallback "
                "observability"
            ),
            args=("-100", "<b>Readable fallback</b>"),
        ),
        store=store,
    )

    assert result["ok"] is True
    assert result["format"] == "plain"
    assert client.recipient_messages == [
        {"text": "Readable fallback", "entities": []}
    ]
    fallback_state = store["telegram"]["delivery_format_fallbacks"]
    assert fallback_state["sequence"] == 1
    assert fallback_state["last"]["method"] == "sendMessage"
    assert fallback_state["last"]["requested_format"] == "html"
    assert fallback_state["last"]["delivered_format"] == "plain"
    assert fallback_state["last"]["rejections"] == [
        {"format": "html", "error": "can't parse entities"}
    ]


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
