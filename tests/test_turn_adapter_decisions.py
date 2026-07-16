from __future__ import annotations

import importlib.util
import json
from pathlib import Path

import pytest


_SPEC = importlib.util.spec_from_file_location(
    "herdr_turn_adapter_decisions",
    Path(__file__).resolve().parent.parent / "herdr_turn_adapter.py",
)
adapter = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(adapter)


def _ask_user_question(tool_input: dict) -> dict:
    """Round-trip a hook-shaped AskUserQuestion payload through real JSON."""

    return json.loads(
        json.dumps(
            {
                "name": "AskUserQuestion",
                "tool_use_id": "toolu_decision_1",
                "input": tool_input,
            }
        )
    )


def _options(count: int) -> list[dict[str, str]]:
    return [
        {"label": f"Option {index}", "description": f"Details {index}"}
        for index in range(1, count + 1)
    ]


def test_single_question_emits_button_decision_with_custom_write_in() -> None:
    tool = _ask_user_question(
        {
            "questions": [
                {
                    "header": "Release",
                    "question": "Which path should I use?",
                    "multiSelect": False,
                    "options": [
                        {"label": "Safe path", "description": "Lower risk"},
                        {"label": "Fast path", "description": "Lower latency"},
                    ],
                }
            ]
        }
    )

    assert adapter.claude_decision_turn_fields(tool) == {
        "pending_decision": {
            "decision_id": "toolu_decision_1",
            "prompt": "Release: Which path should I use?",
            "mode": "buttons",
            "options": [
                {"id": "1", "label": "Safe path", "send_text": "Safe path"},
                {"id": "2", "label": "Fast path", "send_text": "Fast path"},
                {
                    "id": "custom",
                    "label": "✍️ Write a different answer",
                    "send_text": "",
                },
            ],
        }
    }


def test_single_multi_select_emits_real_multi_decision_without_write_in() -> None:
    tool = _ask_user_question(
        {
            "questions": [
                {
                    "header": "Checks",
                    "question": "Which checks should run?",
                    "multiSelect": True,
                    "options": [
                        {"label": "Unit tests", "description": "Fast"},
                        {"label": "Integration tests", "description": "Thorough"},
                    ],
                }
            ]
        }
    )

    assert adapter.claude_decision_turn_fields(tool) == {
        "pending_decision": {
            "decision_id": "toolu_decision_1",
            "prompt": "Which checks should run?",
            "mode": "multi",
            "multi_select": True,
            "options": [
                {"id": "1", "label": "Unit tests"},
                {"id": "2", "label": "Integration tests"},
            ],
        }
    }


def test_multi_question_input_remains_a_read_only_pending_interaction() -> None:
    tool = _ask_user_question(
        {
            "questions": [
                {
                    "header": "Runtime",
                    "question": "Choose a runtime",
                    "multiSelect": False,
                    "options": _options(2),
                },
                {
                    "header": "Checks",
                    "question": "Choose checks",
                    "multiSelect": True,
                    "options": _options(3),
                },
            ]
        }
    )

    fields = adapter.claude_decision_turn_fields(tool)

    assert "pending_decision" not in fields
    interaction = fields["pending_interaction"]
    assert interaction["kind"] == "multi_question_form"
    assert [question["type"] for question in interaction["questions"]] == [
        "single_choice",
        "multi_choice",
    ]


@pytest.mark.parametrize("multi_select", [False, True])
def test_eleven_options_remain_remotely_answerable(multi_select: bool) -> None:
    tool = _ask_user_question(
        {
            "questions": [
                {
                    "header": "Target",
                    "question": "Choose targets",
                    "multiSelect": multi_select,
                    "options": _options(11),
                }
            ]
        }
    )

    decision = adapter.claude_decision_turn_fields(tool)["pending_decision"]

    assert decision["mode"] == ("multi" if multi_select else "buttons")
    assert [option["id"] for option in decision["options"][:11]] == [
        str(index) for index in range(1, 12)
    ]
    assert len(decision["options"]) == (11 if multi_select else 12)
    assert ("custom" in {option["id"] for option in decision["options"]}) is (
        not multi_select
    )


@pytest.mark.parametrize("multi_select", [False, True])
def test_twelve_options_fail_closed_to_read_only_without_truncation(
    multi_select: bool,
) -> None:
    tool = _ask_user_question(
        {
            "questions": [
                {
                    "header": "Target",
                    "question": "Choose targets",
                    "multiSelect": multi_select,
                    "options": _options(12),
                }
            ]
        }
    )

    fields = adapter.claude_decision_turn_fields(tool)

    assert "pending_decision" not in fields
    interaction = fields["pending_interaction"]
    assert interaction["kind"] == "single_question"
    assert interaction["questions"][0]["type"] == (
        "multi_choice" if multi_select else "single_choice"
    )
    assert len(interaction["questions"][0]["options"]) == 12
