from __future__ import annotations

import hashlib
import json
from html.parser import HTMLParser

import pytest

from herdres_connector import source_sync, state
from herdres_connector.rendering import telegram_html
from herdres_connector.rich_delivery import (
    _replace_inline_links,
    edit_feed_item,
    edit_rich_message,
    send_feed_item,
    send_rich_message,
)
from herdres_connector.telegram_delivery import TelegramClient, TelegramError


@pytest.fixture(autouse=True)
def _plain_fallback_by_default(monkeypatch):
    """#213's focused assertions exercise the emergency plain path."""

    monkeypatch.setenv("HERDRES_FORCE_PLAIN_DELIVERY", "1")


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
        object.__setattr__(self, "recipient_edits", [])
        object.__setattr__(self, "attempts", [])

    def api(self, method, payload):
        assert method in {"sendMessage", "editMessageText"}
        text = str(payload.get("text") or "")
        parse_mode = str(payload.get("parse_mode") or "")
        self.attempts.append(
            {"method": method, "text": text, "parse_mode": parse_mode}
        )
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
        received = {"text": received_text, "entities": entities}
        if method == "editMessageText":
            self.recipient_edits.append(received)
            message_id = payload.get("message_id")
        else:
            self.recipient_messages.append(received)
            message_id = len(self.recipient_messages)
        return {
            "ok": True,
            "result": {
                "message_id": message_id,
            },
        }


class _RichCardRecipientParser(HTMLParser):
    """Small read-back model for the rich block types the owner consumes."""

    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.text: list[str] = []
        self.tags: list[str] = []
        self.details: list[dict[str, object]] = []
        self.table_rows: list[list[str]] = []
        self._row: list[str] | None = None
        self._cell: list[str] | None = None
        self._details_stack: list[dict[str, object]] = []
        self._in_summary = 0

    def handle_starttag(self, tag, attrs):
        self.tags.append(tag)
        if tag == "details":
            detail = {
                "type": "PageBlockDetails",
                "open": any(name == "open" for name, _value in attrs),
                "summary": [],
                "body": [],
            }
            self.details.append(detail)
            self._details_stack.append(detail)
        elif tag == "summary":
            self._in_summary += 1
        elif tag == "tr":
            self._row = []
        elif tag in {"th", "td"} and self._row is not None:
            self._cell = []

    def handle_endtag(self, tag):
        if tag == "summary":
            self._in_summary = max(0, self._in_summary - 1)
        elif tag == "details":
            if self._details_stack:
                self._details_stack.pop()
        elif tag in {"th", "td"} and self._row is not None:
            self._row.append("".join(self._cell or []))
            self._cell = None
        elif tag == "tr" and self._row is not None:
            self.table_rows.append(self._row)
            self._row = None

    def handle_data(self, data):
        self.text.append(data)
        if self._details_stack:
            target = (
                "summary" if self._in_summary else "body"
            )
            self._details_stack[-1][target].append(data)
        if self._cell is not None:
            self._cell.append(data)


class RichCardRecipientTelegram(RecipientTelegram):
    """Recipient read-back model for sendRichMessage plus plain fallback."""

    def __init__(
        self,
        *,
        reject_rich_at: int | None = None,
        rich_error: str = "network timeout",
    ):
        super().__init__()
        object.__setattr__(self, "reject_rich_at", reject_rich_at)
        object.__setattr__(self, "rich_error", rich_error)
        object.__setattr__(self, "rich_attempts", 0)

    def api(self, method, payload):
        if method not in {"sendRichMessage", "editMessageText"} or (
            method == "editMessageText"
            and "rich_message" not in payload
        ):
            return super().api(method, payload)
        self.rich_attempts += 1
        self.attempts.append(
            {"method": method, "rich_message": payload["rich_message"]}
        )
        if self.reject_rich_at == self.rich_attempts:
            raise TelegramError(self.rich_error)
        rich = json.loads(payload["rich_message"])
        parser = _RichCardRecipientParser()
        parser.feed(str(rich.get("html") or ""))
        parser.close()
        received = {
            "format": "rich",
            "text": "".join(parser.text),
            "blocks": list(parser.tags),
            "details": [
                {
                    **detail,
                    "summary": "".join(detail["summary"]),
                    "body": "".join(detail["body"]),
                }
                for detail in parser.details
            ],
            "table_rows": parser.table_rows,
        }
        if method == "editMessageText":
            message_id = str(payload.get("message_id") or "")
            self.recipient_edits.append(received)
        else:
            message_id = str(len(self.recipient_messages) + 1)
            self.recipient_messages.append(received)
        return {
            "ok": True,
            "result": {"message_id": message_id},
        }


def test_rich_primary_arrives_as_titled_card_with_table(monkeypatch):
    monkeypatch.setenv("HERDRES_FORCE_PLAIN_DELIVERY", "0")
    client = RichCardRecipientTelegram()

    result = send_feed_item(
        client,
        "-100",
        {
            "kind": "turn",
            "user_text": "Inspect this table",
            "worklog_text": "Checked the source",
            "assistant_final_text": (
                "| Name | Status |\n"
                "| --- | --- |\n"
                "| Ada | Ready |"
            ),
        },
        telegram={},
        thread_id="77",
    )

    assert result["ok"] is True
    assert result["format"] == "rich"
    assert result["physical_writes"] == 1
    assert [attempt["method"] for attempt in client.attempts] == [
        "sendRichMessage"
    ]
    received = client.recipient_messages[0]
    assert received["format"] == "rich"
    assert {"details", "summary", "table", "tr", "th", "td"} <= set(
        received["blocks"]
    )
    assert all(
        title in received["text"]
        for title in ("Response", "You", "Working")
    )
    assert received["table_rows"] == [
        ["Name", "Status"],
        ["Ada", "Ready"],
    ]


def test_rich_rejection_falls_back_to_readable_formatted_plain(monkeypatch):
    monkeypatch.setenv("HERDRES_FORCE_PLAIN_DELIVERY", "0")
    client = RichCardRecipientTelegram(
        reject_rich_at=1,
        rich_error="Bad Request: rich message rejected",
    )

    result = send_feed_item(
        client,
        "-100",
        {
            "kind": "turn",
            "assistant_final_text": (
                "**Readable** `fallback()` "
                "[reference](https://example.test/fallback)"
            ),
        },
        telegram={},
        thread_id="77",
    )

    assert result["ok"] is True
    assert result["format"] == "html"
    assert result["fallback_reason"] == "bad_request"
    assert result["physical_writes"] == 2
    assert [attempt["method"] for attempt in client.attempts] == [
        "sendRichMessage",
        "sendMessage",
    ]
    received = client.recipient_messages[0]
    assert received["text"].startswith("✅ Response")
    assert {"Bold", "Code", "TextUrl"} <= {
        entity["type"] for entity in received["entities"]
    }


def test_force_plain_switch_skips_rich_primary(monkeypatch):
    monkeypatch.setenv("HERDRES_FORCE_PLAIN_DELIVERY", "1")
    client = RichCardRecipientTelegram()

    result = send_feed_item(
        client,
        "-100",
        {"kind": "turn", "assistant_final_text": "**Plain override**"},
        telegram={},
        thread_id="77",
    )

    assert result["ok"] is True
    assert result["format"] == "html"
    assert all(
        attempt["method"] != "sendRichMessage"
        for attempt in client.attempts
    )


def test_failed_later_rich_part_keeps_honest_accepted_prefix(monkeypatch):
    monkeypatch.setenv("HERDRES_FORCE_PLAIN_DELIVERY", "0")
    client = RichCardRecipientTelegram(reject_rich_at=2)
    text = "first paragraph\n\n" * 500

    result = send_feed_item(
        client,
        "-100",
        {"kind": "turn", "assistant_final_text": text},
        telegram={},
        thread_id="77",
    )

    # This assertion used to require ``ok is True``, locking in the defect
    # where an accepted prefix made an incomplete logical final look complete.
    assert result["ok"] is False
    assert result["partial"] is True
    assert result["incomplete_logical_delivery"] is True
    assert result["message_ids"] == ["1"]
    assert result["canonical_message_id"] == "1"
    assert result["failed_part_index"] == 1
    assert result["terminal_outcome"] == "delivery_unknown"
    assert result["operator_attention_required"] is True
    assert result["automatic_replay_authorized"] is False
    assert len(client.recipient_messages) == 1


def test_exact_write_allowance_bounds_rich_and_plain_variant_ladders(
    monkeypatch,
):
    monkeypatch.setenv("HERDRES_FORCE_PLAIN_DELIVERY", "0")
    client = RichCardRecipientTelegram(
        reject_rich_at=1,
        rich_error="Bad Request: rich rejected",
    )
    object.__setattr__(client, "reject_html", True)

    one_write = send_rich_message(
        client,
        "-100",
        "<b>Readable</b>",
        telegram={},
        fallback_text="Readable",
        max_physical_writes=1,
    )

    assert one_write["ok"] is False
    assert one_write["kind"] == "operation_budget_exhausted"
    assert one_write["physical_writes"] == 1
    assert [attempt["method"] for attempt in client.attempts] == [
        "sendRichMessage"
    ]

    client = RichCardRecipientTelegram(
        reject_rich_at=1,
        rich_error="Bad Request: rich rejected",
    )
    object.__setattr__(client, "reject_html", True)
    two_writes = send_rich_message(
        client,
        "-100",
        "<b>Readable</b>",
        telegram={},
        fallback_text="Readable",
        max_physical_writes=2,
    )

    assert two_writes["ok"] is False
    assert two_writes["kind"] == "operation_budget_exhausted"
    assert two_writes["physical_writes"] == 2
    assert [attempt["method"] for attempt in client.attempts] == [
        "sendRichMessage",
        "sendMessage",
    ]
    assert client.recipient_messages == []


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


def test_quoted_reply_separates_author_label_from_recipient_text():
    client = RecipientTelegram()

    result = send_feed_item(
        client,
        "-100",
        {
            "kind": "turn",
            "user_text": "Testing message from telegram",
        },
        telegram={},
        thread_id="77",
    )

    assert result["ok"] is True
    assert result["format"] == "html"
    assert client.recipient_messages[0]["text"] == (
        "💬 You\nTesting message from telegram"
    )


@pytest.mark.parametrize(
    ("item", "expected_url"),
    [
        pytest.param(
            {
                "kind": "turn",
                "assistant_final_text": (
                    "First response line has **Bold result** and `inline()`.\n"
                    "Second response line has *italic detail* and "
                    "[reference](https://example.test/response).\n"
                    "Third response line makes the folded section meaningful."
                ),
                "collapse_response": True,
            },
            "https://example.test/response",
            id="response",
        ),
        pytest.param(
            {
                "kind": "turn",
                "assistant_final_text": "answer",
                "user_text": (
                    "First prompt line has **Bold request** and `inline()`.\n"
                    "Second prompt line has *italic detail* and "
                    "[reference](https://example.test/prompt).\n"
                    "Third prompt line makes the folded section meaningful."
                ),
                "collapse_response": True,
            },
            "https://example.test/prompt",
            id="prompt",
        ),
        pytest.param(
            {
                "kind": "turn",
                "assistant_final_text": "answer",
                "worklog_text": (
                    "First worklog line has **Bold progress** and `inline()`.\n"
                    "Second worklog line has *italic detail* and "
                    "[reference](https://example.test/worklog).\n"
                    "Third worklog line makes the folded section meaningful."
                ),
                "collapse_response": True,
            },
            "https://example.test/worklog",
            id="worklog",
        ),
    ],
)
def test_formatted_multiline_fold_fallback_is_readable_but_not_collapsed(
    item,
    expected_url,
):
    client = RecipientTelegram()

    result = edit_feed_item(
        client,
        "-100",
        "42",
        item,
        telegram={},
    )

    assert result["ok"] is True
    assert result["format"] == "html"
    assert result["collapse_applied"] is False
    assert len(client.attempts) == 1
    assert client.attempts[0]["method"] == "editMessageText"
    assert "<br>" not in client.attempts[0]["text"]
    received = client.recipient_edits[0]
    entity_types = [entity["type"] for entity in received["entities"]]
    assert entity_types.count("Bold") >= 2
    assert "Code" in entity_types
    assert "Italic" in entity_types
    assert {
        "type": "TextUrl",
        "url": expected_url,
    } in received["entities"]
    # Working keeps its established plain-text summary preview above the
    # formatted body; the response and prompt have no markdown-bearing preview.
    if expected_url != "https://example.test/worklog":
        assert "**" not in received["text"]
        assert "`" not in received["text"]
        assert "*italic detail*" not in received["text"]
        assert "[reference](" not in received["text"]


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


class _CountedText(str):
    """Test-only string that charges deterministic character inspections."""

    def __new__(cls, value):
        instance = super().__new__(cls, value)
        instance.inspections = 0
        return instance

    def __getitem__(self, key):
        if isinstance(key, slice):
            start, stop, step = key.indices(len(self))
            self.inspections += len(range(start, stop, step))
        else:
            self.inspections += 1
        return super().__getitem__(key)

    def startswith(self, prefix, start=0, end=None):
        stop = len(self) if end is None else min(len(self), end)
        self.inspections += min(len(prefix), max(0, stop - start))
        return super().startswith(prefix, start, stop)

    def find(self, sub, start=0, end=None):
        stop = len(self) if end is None else min(len(self), end)
        found = super().find(sub, start, stop)
        scan_stop = stop if found < 0 else found + len(sub)
        self.inspections += max(0, scan_stop - start)
        return found


def test_pathological_64k_link_scan_has_bounded_linear_work():
    tail = "x](https://example.test)"
    source = _CountedText("[" * (64000 - len(tail)) + tail)
    link_spans = []

    rendered = _replace_inline_links(source, link_spans)

    assert rendered.endswith("\u00010\u0001")
    assert link_spans == [
        '<a href="https://example.test">x</a>'
    ]
    assert source.inspections <= len(source) * 4 + 128


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
