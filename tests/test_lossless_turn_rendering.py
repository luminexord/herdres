from __future__ import annotations

from typing import Any

import pytest

from herdres_connector.rendering import split_text_chunks, split_text_spans
from herdres_connector.rich_delivery import (
    MAX_RICH_HTML_CHARS,
    RICH_FALLBACK_MAX_CHARS,
    RICH_MULTIPART_MAX_BYTES,
    TELEGRAM_RICH_BLOCK_LIMIT,
    TELEGRAM_RICH_TEXT_LIMIT,
    prepare_turn_delivery_parts,
    render_turn_delivery_part_html,
    render_turn_delivery_part_plain_text,
    render_turn_item_html,
    send_turn_delivery_part,
)
from herdres_connector.safe import canonical_text
from herdres_connector.telegram_delivery import TelegramClient, TelegramError


def _reconstruct(item: dict[str, Any], parts: list[dict[str, Any]], field: str) -> str:
    source = item[field]
    cursor = 0
    fragments: list[str] = []
    for ordinal, part in enumerate(parts):
        assert part["schema_version"] == 1
        assert part["ordinal"] == ordinal
        assert part["part_count"] == len(parts)
        for span in part["spans"]:
            if span["field"] != field:
                continue
            assert span["start_char"] == cursor
            assert span["end_char"] > span["start_char"]
            fragments.append(source[span["start_char"] : span["end_char"]])
            cursor = span["end_char"]
    assert cursor == len(source)
    return "".join(fragments)


def _long_canonical_turn() -> dict[str, Any]:
    user = (
        " \r\n\r\n"
        "## Prompt heading\r\n"
        "- keep the first prompt item\r\n"
        "  - nested-looking prompt item\r\n"
        "emoji: 👩🏽‍💻; CJK: 漢字; combining: e\u0301\r\n"
        "```text\r\n  prompt code keeps indentation\r\n```\r\n"
        + ("無" * 5_000)
        + "\r\n\r\n  "
    )
    final = (
        "\n\n  # Final heading\r\n\r\n"
        "- first item\r\n"
        "- second item with 👩🏽‍💻 漢字 e\u0301\r\n\r\n"
        "```python\r\nprint('keep fence')\r\n```\r\n"
        + ("L" * 70_000)
        + "\r\n\r\ntrailing blanks stay\r\n  \r\n"
    )
    return {"kind": "turn", "user_text": user, "assistant_final_text": final}


@pytest.mark.parametrize(
    "source,limit",
    [
        ("  outer blanks\n\n", 5),
        ("a\r\n\r\nb\r\nc", 4),
        ("👩🏽‍💻漢字e\u0301" * 9, 7),
        ("```py\nprint(1)\n```\n- one\n  - nested\n", 11),
        ("x" * 101, 13),
    ],
)
def test_split_text_spans_are_exact_code_point_ranges(source: str, limit: int):
    spans = split_text_spans(source, limit=limit)

    assert spans[0][0] == 0
    assert spans[-1][1] == len(source)
    assert all(left[1] == right[0] for left, right in zip(spans, spans[1:]))
    assert all(0 < end - start <= limit for start, end in spans)
    assert "".join(source[start:end] for start, end in spans) == source
    assert "".join(split_text_chunks(source, limit=limit)) == source


def test_turn_delivery_plan_is_deterministic_and_reconstructs_over_64k_exactly():
    item = _long_canonical_turn()

    first = prepare_turn_delivery_parts(item)
    second = prepare_turn_delivery_parts(dict(item))

    assert first == second
    assert len(first) > 1
    assert _reconstruct(item, first, "user_text") == item["user_text"]
    assert _reconstruct(item, first, "assistant_final_text") == item["assistant_final_text"]
    assert sum(
        span["end_char"] - span["start_char"]
        for part in first
        for span in part["spans"]
    ) == len(item["user_text"]) + len(item["assistant_final_text"])


def test_canonical_validation_and_prompt_only_plan_never_trim_or_cap():
    prompt = " \r\n\r\n" + ("漢e\u0301👩🏽‍💻" * 12_000) + "\r\n  "
    item = {"kind": "turn", "user_text": canonical_text(prompt, field="user_text"), "assistant_final_text": ""}

    parts = prepare_turn_delivery_parts(item)

    assert canonical_text(prompt) == prompt
    assert len(prompt) > 64_000
    assert _reconstruct(item, parts, "user_text") == prompt
    assert all(
        len(render_turn_delivery_part_html(item, part)) <= MAX_RICH_HTML_CHARS
        for part in parts
    )


def test_every_planned_render_and_fallback_is_bounded():
    item = _long_canonical_turn()
    parts = prepare_turn_delivery_parts(item)

    for part in parts:
        rich = render_turn_delivery_part_html(item, part)
        fallback = render_turn_delivery_part_plain_text(item, part)
        assert len(rich) <= MAX_RICH_HTML_CHARS
        assert len(fallback) <= TELEGRAM_RICH_TEXT_LIMIT
        assert rich.count("<li>") <= TELEGRAM_RICH_BLOCK_LIMIT

    assert render_turn_delivery_part_html(item, parts[0]).startswith(
        f"<b>✅ Response 1/{len(parts)}</b><br><br>"
    )
    assert "trailing blanks stay" in render_turn_delivery_part_html(item, parts[-1])


def test_long_markdown_list_word_is_split_before_inner_renderer_can_slice_it():
    item = {"kind": "turn", "user_text": "", "assistant_final_text": "- " + ("x" * 1_500) + "TAIL"}

    parts = prepare_turn_delivery_parts(item)
    rendered = "".join(render_turn_delivery_part_html(item, part) for part in parts)

    assert _reconstruct(item, parts, "assistant_final_text") == item["assistant_final_text"]
    assert rendered.count("x") == 1_500
    assert "TAIL" in rendered


def test_short_turn_plans_one_part_with_byte_identical_rendering():
    item = {
        "kind": "turn",
        "user_text": "Question",
        "assistant_final_text": "## **Fix it**\n\n- keep **bold**\n- escape <tags>\n\nUse `code`.",
    }
    existing = render_turn_item_html(item)

    parts = prepare_turn_delivery_parts(item)

    assert len(parts) == 1
    assert parts[0]["spans"] == [
        {"field": "user_text", "start_char": 0, "end_char": len(item["user_text"])},
        {
            "field": "assistant_final_text",
            "start_char": 0,
            "end_char": len(item["assistant_final_text"]),
        },
    ]
    assert render_turn_delivery_part_html(item, parts[0]) == existing
    assert existing.startswith("<b>✅ Response</b><br><br><h4>Fix it</h4>")
    assert "<details open><summary>💬 <b>You</b></summary><footer>Question</footer></details>" in existing


def test_planner_ignores_non_turn_items_and_renderer_rejects_ambiguous_parts():
    assert prepare_turn_delivery_parts({"assistant_final_text": "not a typed turn"}) == []
    item = {"kind": "turn", "user_text": "q", "assistant_final_text": "answer"}
    part = prepare_turn_delivery_parts(item)[0]

    bad_schema = dict(part, schema_version=2)
    with pytest.raises(ValueError, match="schema"):
        render_turn_delivery_part_html(item, bad_schema)

    bad_ordinal = dict(part, ordinal=True)
    with pytest.raises(ValueError, match="ordinal"):
        render_turn_delivery_part_html(item, bad_ordinal)

    duplicate = dict(part, spans=[part["spans"][0], part["spans"][0]])
    with pytest.raises(ValueError, match="duplicate"):
        render_turn_delivery_part_html(item, duplicate)


class RecordingTelegram(TelegramClient):
    def __init__(self) -> None:
        super().__init__(token="recording")
        object.__setattr__(self, "calls", [])

    def api(self, method: str, payload: dict[str, Any]) -> dict[str, Any]:
        self.calls.append((method, payload))
        return {"ok": True, "result": {"message_id": len(self.calls)}}


def test_one_part_executor_never_activates_telegram_hidden_split():
    item = _long_canonical_turn()
    parts = prepare_turn_delivery_parts(item)

    rich_client = RecordingTelegram()
    rich_state: dict[str, Any] = {"rich_messages": {"supported": "yes"}}
    for part in parts:
        before = len(rich_client.calls)
        result = send_turn_delivery_part(
            rich_client,
            "-100",
            item,
            part,
            telegram=rich_state,
            thread_id="7",
        )
        assert result["ok"] is True
        assert len(rich_client.calls) == before + 1
        assert rich_client.calls[-1][0] == "sendRichMessage"

    fallback_parts = prepare_turn_delivery_parts(item, rich_transport=False)
    fallback_client = RecordingTelegram()
    fallback_state = None
    for part in fallback_parts:
        before = len(fallback_client.calls)
        result = send_turn_delivery_part(
            fallback_client,
            "-100",
            item,
            part,
            telegram=fallback_state,
            thread_id="7",
        )
        assert result["ok"] is True
        assert len(fallback_client.calls) == before + 1
        method, payload = fallback_client.calls[-1]
        assert method == "sendMessage"
        assert len(payload["text"]) <= RICH_FALLBACK_MAX_CHARS
        assert result.get("message_ids") is None


def test_rich_planner_uses_current_32768_character_transport_bound():
    item = {
        "kind": "turn",
        "user_text": "u" * 18_000,
        "assistant_final_text": "f" * 15_799,
    }

    rich_parts = prepare_turn_delivery_parts(item)
    plain_parts = prepare_turn_delivery_parts(item, rich_transport=False)

    assert len(rich_parts) == 2
    assert len(plain_parts) > len(rich_parts)
    assert _reconstruct(item, rich_parts, "user_text") == item["user_text"]
    assert (
        _reconstruct(item, rich_parts, "assistant_final_text")
        == item["assistant_final_text"]
    )
    assert all(
        len(render_turn_delivery_part_html(item, part)) <= TELEGRAM_RICH_TEXT_LIMIT
        for part in rich_parts
    )
    assert all(
        len(render_turn_delivery_part_html(item, part).encode("utf-8"))
        <= RICH_MULTIPART_MAX_BYTES
        for part in rich_parts
    )
    assert all(
        len(render_turn_delivery_part_plain_text(item, part))
        <= RICH_FALLBACK_MAX_CHARS
        for part in plain_parts
    )


def test_two_part_rich_plan_keeps_provider_display_margin_without_content_loss():
    final = (("- delivery item " + ("x" * 110) + "\n") * 280)[:35_579]
    item = {
        "kind": "turn",
        "user_text": "continue",
        "assistant_final_text": final,
    }

    parts = prepare_turn_delivery_parts(item)

    assert len(parts) == 2
    assert _reconstruct(item, parts, "user_text") == item["user_text"]
    assert _reconstruct(item, parts, "assistant_final_text") == final
    assert all(
        len(render_turn_delivery_part_html(item, part).encode("utf-8"))
        <= RICH_MULTIPART_MAX_BYTES
        for part in parts
    )


def test_large_rich_part_never_falls_back_to_hidden_plain_siblings():
    class UnsupportedRichTelegram(RecordingTelegram):
        def api(self, method: str, payload: dict[str, Any]) -> dict[str, Any]:
            self.calls.append((method, payload))
            if method == "sendRichMessage":
                raise TelegramError("method not found")
            return {"ok": True, "result": {"message_id": len(self.calls)}}

    item = {
        "kind": "turn",
        "user_text": "",
        "assistant_final_text": "x" * 20_000,
    }
    part = prepare_turn_delivery_parts(item)[0]
    client = UnsupportedRichTelegram()
    telegram = {"rich_messages": {"supported": "yes"}}

    result = send_turn_delivery_part(
        client,
        "-100",
        item,
        part,
        telegram=telegram,
        thread_id="7",
    )

    assert result["ok"] is False
    assert result["kind"] == "presentation_transport_changed"
    assert [method for method, _payload in client.calls] == ["sendRichMessage"]
