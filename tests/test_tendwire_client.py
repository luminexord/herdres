from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from herdres_connector import tendwire_client
from herdres_connector.tendwire_client import TendwireClient


@pytest.fixture
def client_runner(monkeypatch):
    calls = []
    responses = []

    monkeypatch.setenv("HERDRES_TENDWIRE_BIN", "tw")

    def fake_run(argv, **kwargs):
        calls.append((argv, kwargs))
        response = responses.pop(0)
        return SimpleNamespace(
            returncode=response.get("returncode", 0),
            stdout=json.dumps(response["body"], ensure_ascii=False).encode("utf-8"),
            stderr=response.get("stderr", "").encode("utf-8"),
        )

    monkeypatch.setattr(tendwire_client.subprocess, "run", fake_run)
    return TendwireClient(), calls, responses


def test_turn_final_lease_seconds_default_invalid_and_bounds():
    lease_seconds = tendwire_client.config.tendwire_turn_final_lease_seconds

    assert lease_seconds(env={}) == 900
    assert lease_seconds(
        env={"HERDRES_TENDWIRE_TURN_FINAL_LEASE_SECONDS": ""}
    ) == 900
    assert lease_seconds(
        env={"HERDRES_TENDWIRE_TURN_FINAL_LEASE_SECONDS": "invalid"}
    ) == 900
    assert lease_seconds(
        env={"HERDRES_TENDWIRE_TURN_FINAL_LEASE_SECONDS": "120"}
    ) == 120
    assert lease_seconds(
        env={"HERDRES_TENDWIRE_TURN_FINAL_LEASE_SECONDS": "59"}
    ) == 60
    assert lease_seconds(
        env={"HERDRES_TENDWIRE_TURN_FINAL_LEASE_SECONDS": "3601"}
    ) == 3600


def test_turns_requests_and_requires_v2_content_schema(client_runner):
    client, calls, responses = client_runner
    responses.append(
        {
            "body": {
                "schema_version": 2,
                "turns": [
                    {
                        "id": "turn-1",
                        "content": {
                            "schema_version": 1,
                            "content_revision": "twrev1.public",
                            "fields": {},
                        },
                    }
                ],
            }
        }
    )

    result = client.turns()

    assert result["schema_version"] == 2
    assert calls[0][0] == ["tw", "turns", "--schema-version", "2", "--json"]
    assert calls[0][1]["input"] is None

    responses.extend(
        [
            {"body": {"schema_version": 1, "turns": []}},
            {"body": {"schema_version": 2, "turns": [{"id": "turn-1"}]}},
        ]
    )
    old = client.turns()
    missing_content = client.turns()

    assert old["ok"] is False
    assert old["status"] == "upgrade_required"
    assert old["required_turn_schema_version"] == 2
    assert missing_content["ok"] is False
    assert missing_content["status"] == "unsupported_content_schema"
    assert missing_content["supported_content_schema_version"] == 1


def test_turns_preserves_typed_upgrade_error_from_cli(client_runner):
    client, _calls, responses = client_runner
    responses.append(
        {
            "returncode": 1,
            "body": {
                "ok": False,
                "status": "upgrade_required",
                "required_turn_schema_version": 2,
                "error": "turn schema v2 required",
            },
        }
    )

    result = client.turns()

    assert result == {
        "ok": False,
        "status": "upgrade_required",
        "required_turn_schema_version": 2,
        "error": "turn schema v2 required",
    }


def test_turn_content_get_uses_exact_bounded_cli_and_preserves_page_text(client_runner):
    client, calls, responses = client_runner
    page = "\r\n  e\u0301 雪\t\n" + ("x" * (48 * 1024 - 20)) + "  \r\n"
    responses.append(
        {
            "body": {
                "schema_version": 1,
                "turn_id": "turn-1",
                "content_revision": "twrev1.public",
                "field": "assistant_final_text",
                "availability": "complete",
                "segment_id": "twseg1.public",
                "index": 1,
                "count": 2,
                "text": page,
                "segment_char_length": len(page),
                "segment_byte_length": len(page.encode("utf-8")),
                "total_char_length": 50000,
                "total_byte_length": 50003,
                "next_cursor": None,
            }
        }
    )

    result = client.turn_content_get(
        "turn-1",
        "twrev1.public",
        "assistant_final_text",
        cursor="twcur1.public",
    )

    assert result["text"] == page
    assert result["text"].encode("utf-8") == page.encode("utf-8")
    assert calls[0][0] == [
        "tw",
        "turn",
        "content",
        "get",
        "--json",
        "--turn-id",
        "turn-1",
        "--revision",
        "twrev1.public",
        "--field",
        "assistant_final_text",
        "--cursor",
        "twcur1.public",
    ]
    assert calls[0][1]["input"] is None


def test_turn_content_get_omits_null_cursor_and_rejects_schema(client_runner):
    client, calls, responses = client_runner
    responses.append({"body": {"schema_version": 2, "text": "not v1"}})

    result = client.turn_content_get("turn-1", "twrev1.public", "user_text")

    assert "--cursor" not in calls[0][0]
    assert result["ok"] is False
    assert result["status"] == "unsupported_content_schema"
    assert result["supported_content_schema_version"] == 1


def test_prepare_begin_part_commit_send_only_neutral_bounded_json(client_runner):
    client, calls, responses = client_runner
    responses.extend(
        [
            {"body": {"schema_version": 1, "ok": True, "plan_token": "twplan1.public", "state": "preparing", "part_count": 2, "accepted_parts": 0}},
            {"body": {"schema_version": 1, "ok": True, "plan_token": "twplan1.public", "ordinal": 0, "accepted_parts": 1}},
            {"body": {"schema_version": 1, "ok": True, "plan_token": "twplan1.public", "state": "active", "job_count": 2}},
        ]
    )

    begin = client.connector_prepare_begin(
        turn_id="turn-1",
        content_revision="twrev1.public",
        presentation_version="herdres-rich-v3",
        part_count=2,
    )
    part = client.connector_prepare_part(
        plan_token=begin["plan_token"],
        ordinal=0,
        spans=[
            {"field": "user_text", "start_char": 0, "end_char": 4},
            {"field": "assistant_final_text", "start_char": 0, "end_char": 3900},
        ],
    )
    commit = client.connector_prepare_commit(plan_token=part["plan_token"])

    expected_argv = ["tw", "connector", "prepare", "--name", "turn-final", "--json"]
    assert [call[0] for call in calls] == [expected_argv, expected_argv, expected_argv]
    payloads = [json.loads(call[1]["input"].decode("utf-8")) for call in calls]
    assert payloads == [
        {
            "schema_version": 1,
            "action": "begin",
            "name": "turn-final",
            "turn_id": "turn-1",
            "content_revision": "twrev1.public",
            "presentation_version": "herdres-rich-v3",
            "part_count": 2,
        },
        {
            "schema_version": 1,
            "action": "part",
            "name": "turn-final",
            "plan_token": "twplan1.public",
            "ordinal": 0,
            "spans": [
                {"field": "user_text", "start_char": 0, "end_char": 4},
                {"field": "assistant_final_text", "start_char": 0, "end_char": 3900},
            ],
        },
        {
            "schema_version": 1,
            "action": "commit",
            "name": "turn-final",
            "plan_token": "twplan1.public",
        },
    ]
    encoded = b" ".join(call[1]["input"] for call in calls)
    assert b'"text"' not in encoded
    assert b'"user_text"' in encoded  # coordinate enum only
    assert b'"assistant_final_text"' in encoded  # coordinate enum only
    assert begin["plan_token"] == part["plan_token"] == commit["plan_token"]



def test_prepare_source_ref_is_exact_and_optional_and_ack_prunes_provider_ids(client_runner):
    client, calls, responses = client_runner
    responses.extend(
        [
            {
                "body": {
                    "schema_version": 1,
                    "ok": True,
                    "plan_token": "twplan1.source",
                    "state": "preparing",
                }
            },
            {
                "body": {
                    "schema_version": 1,
                    "ok": True,
                    "plan_token": "twplan1.source",
                    "state": "active",
                    "job_count": 1,
                }
            },
            {
                "body": {
                    "schema_version": 1,
                    "ok": True,
                    "status": "acknowledged",
                }
            },
        ]
    )
    source_ref = "twref1.exact_live_source"

    client.connector_prepare_begin(
        turn_id="turn-source",
        content_revision="twrev1.source",
        presentation_version="turn-present-v27",
        part_count=1,
        source_ref=source_ref,
    )
    client.connector_prepare_commit(
        plan_token="twplan1.source",
        source_ref=source_ref,
    )
    client.turn_final_ack(
        "twref1.part",
        {
            "outcome": "applied",
            "message_id": "901",
            "nested": {
                "chat_id": "-100",
                "topic_id": "77",
                "job_key": "turn-final:twplan1.source:000000",
            },
        },
    )

    prepare_payloads = [
        json.loads(calls[index][1]["input"].decode("utf-8"))
        for index in (0, 1)
    ]
    assert prepare_payloads[0]["source_ref"] == source_ref
    assert prepare_payloads[1]["source_ref"] == source_ref
    assert json.loads(calls[2][0][-1]) == {
        "outcome": "applied",
        "nested": {
            "job_key": "turn-final:twplan1.source:000000",
        },
    }
    encoded = json.dumps(prepare_payloads).lower()
    assert all(
        forbidden not in encoded
        for forbidden in ("telegram", "chat_id", "topic_id", "message_id")
    )

def test_prepare_part_rejects_content_or_non_range_fields_without_calling_cli(client_runner):
    client, calls, _responses = client_runner

    result = client.connector_prepare_part(
        plan_token="twplan1.public",
        ordinal=0,
        spans=[
            {
                "field": "assistant_final_text",
                "start_char": 0,
                "end_char": 4,
                "text": "must never cross prepare",
            }
        ],
    )

    assert result["status"] == "invalid_prepare_part"
    assert calls == []


def test_turn_final_methods_use_dedicated_queue_and_preserve_public_plan_tokens(client_runner):
    client, calls, responses = client_runner
    responses.extend(
        [
            {
                "body": {
                    "schema_version": 1,
                    "ok": True,
                    "items": [
                        {
                            "ref": "twref1.lease",
                            "key": "turn-final:twplan1.public:000000",
                            "payload": {
                                "schema_version": 1,
                                "plan_token": "twplan1.public",
                                "replaces_plan_token": "twplan1.old",
                                "spans": [{"field": "assistant_final_text", "start_char": 0, "end_char": 4}],
                            },
                        }
                    ],
                }
            },
            {"body": {"schema_version": 1, "ok": True, "status": "delivered"}},
            {"body": {"schema_version": 1, "ok": True, "status": "retry"}},
            {"body": {"schema_version": 1, "ok": True, "status": "deferred"}},
        ]
    )

    poll = client.turn_final_poll(limit=2, lease_seconds=45)
    client.turn_final_ack("twref1.lease", {"outcome": "applied"})
    client.turn_final_fail("twref1.next", "temporary")
    client.turn_final_defer("twref1.next", "rate limited", delay_seconds=30)

    assert poll["items"][0]["payload"]["plan_token"] == "twplan1.public"
    assert poll["items"][0]["payload"]["replaces_plan_token"] == "twplan1.old"
    assert [call[0] for call in calls] == [
        ["tw", "connector", "poll", "--name", "turn-final", "--limit", "2", "--lease-seconds", "45"],
        ["tw", "connector", "ack", "--name", "turn-final", "--ref", "twref1.lease", "--response-json", '{"outcome":"applied"}'],
        ["tw", "connector", "fail", "--name", "turn-final", "--ref", "twref1.next", "--reason", "temporary"],
        ["tw", "connector", "defer", "--name", "turn-final", "--ref", "twref1.next", "--reason", "rate limited", "--delay-seconds", "30"],
    ]


def test_prepare_and_turn_final_refuse_unsupported_connector_schema(client_runner):
    client, _calls, responses = client_runner
    responses.extend(
        [
            {"body": {"schema_version": 2, "ok": True, "plan_token": "twplan2.unknown"}},
            {"body": {"ok": True, "items": []}},
        ]
    )

    prepare = client.connector_prepare_begin(
        turn_id="turn-1",
        content_revision="twrev1.public",
        presentation_version="herdres-rich-v3",
        part_count=1,
    )
    poll = client.turn_final_poll()

    assert prepare["ok"] is False
    assert prepare["status"] == "unsupported_content_schema"
    assert prepare["supported_content_schema_version"] == 1
    assert poll["ok"] is False
    assert poll["status"] == "unsupported_content_schema"
    assert poll["supported_content_schema_version"] == 1


def test_attention_connector_calls_are_unchanged(client_runner, monkeypatch):
    client, calls, responses = client_runner
    monkeypatch.setattr(tendwire_client.config, "tendwire_db_path", lambda: Path("/private/tendwire.db"))
    responses.extend(
        [
            {"body": {"ok": True, "items": []}},
            {"body": {"ok": True}},
            {"body": {"ok": True}},
        ]
    )

    client.connector_poll()
    client.connector_ack("twref1.attention", {"duplicate": True})
    client.connector_fail("twref1.attention", "failed")

    assert [call[0] for call in calls] == [
        ["tw", "connector", "poll", "--db-path", "/private/tendwire.db", "--name", "attention", "--limit", "3", "--lease-seconds", "60"],
        ["tw", "connector", "ack", "--db-path", "/private/tendwire.db", "--name", "attention", "--ref", "twref1.attention", "--response-json", '{"duplicate":true}'],
        ["tw", "connector", "fail", "--db-path", "/private/tendwire.db", "--name", "attention", "--ref", "twref1.attention", "--error", "failed"],
    ]


def test_recover_failed_turn_final_uses_exact_bounded_prepare_action(client_runner):
    client, calls, responses = client_runner
    responses.append(
        {
            "body": {
                "schema_version": 1,
                "ok": True,
                "status": "recovered",
                "failed_plan_token": "twplan1.failed",
                "plan_token": "twplan1.replacement",
                "generation": 2,
                "content_revision": "twrev1.public",
                "state": "active",
                "acknowledged_prefix_count": 1,
                "executable_job_count": 2,
                "retained_failed_job_count": 1,
                "prior_attempt_count": 4,
                "idempotent_replay": False,
            }
        }
    )

    result = client.connector_prepare_recover(
        failed_plan_token="twplan1.failed",
        request_id="operator-2026.07.11:1",
    )

    assert result["plan_token"] == "twplan1.replacement"
    assert calls[0][0] == [
        "tw",
        "connector",
        "prepare",
        "--name",
        "turn-final",
        "--json",
    ]
    assert json.loads(calls[0][1]["input"].decode("utf-8")) == {
        "schema_version": 1,
        "action": "recover",
        "name": "turn-final",
        "failed_plan_token": "twplan1.failed",
        "request_id": "operator-2026.07.11:1",
    }


@pytest.mark.parametrize(
    ("failed_plan_token", "request_id"),
    [
        ("not-a-plan", "request-1"),
        ("twplan1.failed", ""),
        ("twplan1.failed", "contains space"),
        ("twplan1.failed", "x" * 129),
    ],
)
def test_recover_failed_turn_final_rejects_invalid_public_coordinates_without_rpc(
    client_runner,
    failed_plan_token,
    request_id,
):
    client, calls, _responses = client_runner

    result = client.connector_prepare_recover(
        failed_plan_token=failed_plan_token,
        request_id=request_id,
    )

    assert result["ok"] is False
    assert result["status"] == "invalid_recovery_request"
    assert calls == []
