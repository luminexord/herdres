from __future__ import annotations

import hashlib
import json
from types import SimpleNamespace

import herdres
import herdres_gateway
from herdres_connector import decisions, source_sync, state, tendwire_client
from herdres_connector.ingress_identity import (
    derive_telegram_request_id,
    validate_request_id,
)
from herdres_connector.telegram_delivery import TelegramClient


REQUEST_KEY = bytes(range(32))


def _request_id(update_id: int = 100, message_id: int = 9001) -> str:
    return derive_telegram_request_id(
        REQUEST_KEY,
        receiver_id="manager",
        update_id=update_id,
        chat_id=-100,
        message_id=message_id,
    )


def _store() -> dict:
    stable_key = "wsk1_" + hashlib.sha256(b"worker-1").hexdigest()
    return {
        "enabled": True,
        "telegram": {
            "chat_id": "-100",
            "general_thread_id": "1",
            "owner_user_ids": ["7"],
        },
        "panes": {
            "worker-entry": {
                "source": "tendwire",
                "entry_type": "worker",
                "tendwire_worker_id": "worker-1",
                "worker_id": "worker-1",
                "tendwire_space_id": "space-1",
                "space_id": "space-1",
                "tendwire_fingerprint": "fp-1",
                "tendwire_stable_key": stable_key,
                "tendwire_stable_key_version": 1,
                "tendwire_raw_status": "idle",
                "topic_id": "77",
            }
        },
        "spaces": {
            "space-entry": {
                "source": "tendwire",
                "entry_type": "space",
                "tendwire_space_id": "space-1",
                "space_id": "space-1",
                "topic_id": "77",
                "active_worker_id": "worker-1",
                "active_worker_fingerprint": "fp-1",
                "active_worker_stable_key": stable_key,
                "active_worker_stable_key_version": 1,
            }
        },
        "tendwired_bootstrap_complete": True,
    }


def _pending(
    *,
    kind: str = "single",
    decision_ref: str = "decision-1",
    question_count: int = 1,
    worker_id: str = "worker-1",
) -> dict:
    return {
        "pending_interactions": [
            {
                "id": "pending-1",
                "worker_id": worker_id,
                "meta": {
                    "decision": {
                        "decision_ref": decision_ref,
                        "kind": kind,
                        "prompt": "Which release path should I use?",
                        "options": [
                            {"ref": "1", "label": "Use the safe path"},
                            {"ref": "2", "label": "Use the fast path"},
                        ],
                        "multi_select": kind == "multi",
                        "question_count": question_count,
                    }
                },
            }
        ]
    }


class FakeTelegram:
    dry_run = False

    def __init__(self, token: str = "fake") -> None:
        self.token = token
        self.sent: list[dict] = []
        self.edited: list[dict] = []
        self.markup_edits: list[dict] = []
        self.callback_answers: list[dict] = []

    def send_message(self, chat_id, html, **kwargs):
        message_id = str(101 + len(self.sent))
        self.sent.append(
            {
                "chat_id": str(chat_id),
                "html": str(html),
                "kwargs": dict(kwargs),
                "message_id": message_id,
            }
        )
        return {"ok": True, "message_id": message_id}

    def edit_message(self, chat_id, message_id, html):
        self.edited.append(
            {
                "chat_id": str(chat_id),
                "message_id": str(message_id),
                "html": str(html),
            }
        )
        return {"ok": True, "message_id": str(message_id)}

    def edit_message_reply_markup(self, chat_id, message_id, reply_markup):
        self.markup_edits.append(
            {
                "chat_id": str(chat_id),
                "message_id": str(message_id),
                "reply_markup": reply_markup,
            }
        )
        return {"ok": True, "message_id": str(message_id)}

    def answer_callback_query(self, callback_query_id, text="", **kwargs):
        self.callback_answers.append(
            {
                "callback_query_id": str(callback_query_id),
                "text": str(text),
                **kwargs,
            }
        )
        return {"ok": True}


class FakeTendwire:
    def __init__(self, response: dict | None = None) -> None:
        self.response = response or {"ok": True, "status": "accepted"}
        self.commands: list[dict] = []

    def command(self, request):
        self.commands.append(request)
        return dict(self.response)

    def command_json(self, request_json):
        request = json.loads(request_json)
        self.commands.append(request)
        return {
            "schema_version": 2,
            "action": "send_instruction",
            "request_id": request["request_id"],
            "ok": True,
            "dry_run": False,
            "status": "accepted",
            "disposition": "terminal_accepted",
            "result": {
                "target": {"worker_id": "worker-1"},
                "delivery_state": "submitted",
                "transport_state": "submitted",
                "target_state_at_send": "idle",
                "observed_turn_state": "pending_observation",
            },
            "error": None,
            "warnings": [],
        }


def _post(store: dict, telegram: FakeTelegram, payload: dict) -> dict:
    result = decisions.sync_decisions(
        store, payload, telegram, chat_id="-100"
    )
    assert result["posted"] == 1
    return store["decisions"]["active"]["77"]


def _button(record: dict, token: str) -> str:
    for row in decisions.inline_keyboard(record)["inline_keyboard"]:
        data = row[0]["callback_data"]
        if data.rsplit(":", 1)[-1] == token:
            return data
    raise AssertionError(f"button not found: {token}")


def test_resolve_decisions_joins_worker_and_fails_closed() -> None:
    store = _store()
    payload = _pending()
    payload["pending_interactions"].extend(
        [
            _pending(decision_ref="multi-question", question_count=2)[
                "pending_interactions"
            ][0],
            _pending(decision_ref="unknown-kind", kind="wizard")[
                "pending_interactions"
            ][0],
            _pending(decision_ref="unknown-worker", worker_id="worker-missing")[
                "pending_interactions"
            ][0],
            {"id": "plain-attention", "worker_id": "worker-1", "meta": {}},
        ]
    )

    resolved = decisions.resolve_decisions(store, payload)

    assert len(resolved) == 1
    assert resolved[0]["decision_id"] == "decision-1"
    assert resolved[0]["entry_key"] == "worker-entry"
    assert resolved[0]["topic_id"] == "77"


def test_inline_keyboard_is_bounded_and_has_kind_specific_controls() -> None:
    store = _store()
    telegram = FakeTelegram()
    record = _post(
        store,
        telegram,
        _pending(decision_ref="decision-" + "x" * 2000),
    )

    markup = telegram.sent[0]["kwargs"]["reply_markup"]
    callbacks = [row[0]["callback_data"] for row in markup["inline_keyboard"]]

    assert all(len(value.encode("utf-8")) <= 64 for value in callbacks)
    assert callbacks[-1].endswith(":custom")
    assert "Write a different answer" in markup["inline_keyboard"][-1][0]["text"]
    assert record["message_id"] == telegram.sent[0]["message_id"]

    plan_store = _store()
    plan_telegram = FakeTelegram()
    plan_record = _post(plan_store, plan_telegram, _pending(kind="plan"))
    plan_buttons = decisions.inline_keyboard(plan_record)["inline_keyboard"]
    assert len(plan_buttons) == 2
    assert all(not row[0]["callback_data"].endswith(":custom") for row in plan_buttons)
    assert all(not row[0]["callback_data"].endswith(":__submit__") for row in plan_buttons)


def test_post_is_idempotent_then_retracts_when_decision_leaves_pending() -> None:
    store = _store()
    telegram = FakeTelegram()
    payload = _pending()
    _post(store, telegram, payload)

    unchanged = decisions.sync_decisions(store, payload, telegram, chat_id="-100")
    retracted = decisions.sync_decisions(
        store, {"pending_interactions": []}, telegram, chat_id="-100"
    )

    assert unchanged["changed"] is False
    assert len(telegram.sent) == 1
    assert retracted["retracted"] == 1
    assert store["decisions"]["active"] == {}
    assert telegram.markup_edits[-1]["reply_markup"] == {"inline_keyboard": []}
    assert "✅ Answered." in telegram.edited[-1]["html"]


def test_multi_toggle_edits_keyboard_in_place_without_new_message() -> None:
    store = _store()
    telegram = FakeTelegram()
    record = _post(store, telegram, _pending(kind="multi"))

    result = decisions.handle_callback(
        store,
        callback_data=_button(record, "1"),
        topic_id="77",
        chat_id="-100",
        request_id=_request_id(),
        telegram=telegram,
        tendwire=FakeTendwire(),
    )

    assert result["status"] == "toggled"
    assert len(telegram.sent) == 1
    assert len(telegram.markup_edits) == 1
    assert telegram.markup_edits[0]["message_id"] == record["message_id"]
    assert store["decisions"]["active"]["77"]["selected"] == ["1"]
    assert telegram.markup_edits[0]["reply_markup"]["inline_keyboard"][0][0][
        "text"
    ].startswith("✅")


def test_submit_shape_uses_valid_derived_request_id() -> None:
    store = _store()
    telegram = FakeTelegram()
    tendwire = FakeTendwire()
    record = _post(store, telegram, _pending())

    result = decisions.handle_callback(
        store,
        callback_data=_button(record, "2"),
        topic_id="77",
        chat_id="-100",
        request_id=_request_id(),
        telegram=telegram,
        tendwire=tendwire,
    )

    request = tendwire.commands[0]
    assert result["status"] == "accepted"
    assert request == {
        "schema_version": 1,
        "action": "answer_decision",
        "request_id": request["request_id"],
        "target": {"worker_id": "worker-1"},
        "params": {
            "decision_ref": "decision-1",
            "selection": {"option_refs": ["2"]},
        },
    }
    assert validate_request_id(request["request_id"]) == request["request_id"]
    assert store["decisions"]["active"] == {}
    assert telegram.markup_edits[-1]["reply_markup"] == {"inline_keyboard": []}
    assert "✅ Answered." in telegram.edited[-1]["html"]


def test_failed_submit_keeps_record_and_reports_explicit_error() -> None:
    store = _store()
    telegram = FakeTelegram()
    tendwire = FakeTendwire({"ok": False, "status": "invalid_selection"})
    record = _post(store, telegram, _pending())

    result = decisions.handle_callback(
        store,
        callback_data=_button(record, "1"),
        topic_id="77",
        chat_id="-100",
        request_id=_request_id(),
        telegram=telegram,
        tendwire=tendwire,
    )

    assert result["status"] == "invalid_selection"
    assert store["decisions"]["active"]["77"] is record
    assert len(telegram.sent) == 2
    assert "Could not answer" in telegram.sent[-1]["html"]
    assert "invalid_selection" in telegram.sent[-1]["html"]
    assert telegram.markup_edits == []


def test_decision_not_pending_retracts_with_honest_note() -> None:
    store = _store()
    telegram = FakeTelegram()
    tendwire = FakeTendwire({"ok": False, "status": "decision_not_pending"})
    record = _post(store, telegram, _pending())

    result = decisions.handle_callback(
        store,
        callback_data=_button(record, "1"),
        topic_id="77",
        chat_id="-100",
        request_id=_request_id(),
        telegram=telegram,
        tendwire=tendwire,
    )

    assert result["status"] == "decision_not_pending"
    assert store["decisions"]["active"] == {}
    assert "no longer pending (answered at the desk?)" in telegram.edited[-1]["html"]
    assert "✅ Answered." not in telegram.edited[-1]["html"]


def test_write_in_arm_then_plain_text_submits_as_decision(
    tmp_path, monkeypatch
) -> None:
    store = _store()
    telegram = FakeTelegram()
    record = _post(store, telegram, _pending())
    armed = decisions.handle_callback(
        store,
        callback_data=_button(record, "custom"),
        topic_id="77",
        chat_id="-100",
        request_id=_request_id(),
        telegram=telegram,
        tendwire=FakeTendwire(),
    )
    assert armed["status"] == "await_freeform"
    assert record["await_freeform"] is True

    monkeypatch.setenv("HERDR_TELEGRAM_TOPICS_STATE", str(tmp_path / "state.json"))
    state.save_state(store)
    tendwire = FakeTendwire()
    monkeypatch.setattr(herdres, "TendwireClient", lambda: tendwire)
    monkeypatch.setattr(herdres, "TelegramClient", lambda **_kwargs: telegram)

    result = herdres.command_reply(
        {
            "request_id": _request_id(update_id=101, message_id=9002),
            "topic_id": "77",
            "message_id": "9002",
            "text": "Use the compatibility implementation",
        }
    )

    assert result["handled"] is True
    assert tendwire.commands[0]["action"] == "answer_decision"
    assert tendwire.commands[0]["params"]["selection"] == {
        "text": "Use the compatibility implementation"
    }
    assert state.load_state()["decisions"]["active"] == {}


def test_send_command_falls_through_even_when_write_in_is_armed(
    tmp_path, monkeypatch
) -> None:
    store = _store()
    telegram = FakeTelegram()
    record = _post(store, telegram, _pending())
    record["await_freeform"] = True
    monkeypatch.setenv("HERDR_TELEGRAM_TOPICS_STATE", str(tmp_path / "state.json"))
    state.save_state(store)
    tendwire = FakeTendwire()
    monkeypatch.setattr(herdres, "TendwireClient", lambda: tendwire)

    result = herdres.command_reply(
        {
            "request_id": _request_id(update_id=102, message_id=9003),
            "topic_id": "77",
            "message_id": "9003",
            "text": "/send deploy the compatibility implementation",
        }
    )

    assert result["handled"] is True
    assert tendwire.commands[0]["action"] == "send_instruction"
    assert tendwire.commands[0]["instruction"]["text"] == (
        "deploy the compatibility implementation"
    )
    assert state.load_state()["decisions"]["active"]["77"][
        "await_freeform"
    ] is True


def test_remote_decisions_flag_off_is_fully_inert(monkeypatch) -> None:
    monkeypatch.setenv("HERDRES_REMOTE_DECISIONS", "0")
    store = _store()
    telegram = FakeTelegram()
    tendwire = FakeTendwire()

    sync_result = decisions.sync_decisions(
        store, _pending(), telegram, chat_id="-100"
    )
    callback_result = decisions.handle_callback(
        store,
        callback_data="hdec:anything:1",
        topic_id="77",
        chat_id="-100",
        request_id=_request_id(),
        telegram=telegram,
        tendwire=tendwire,
    )

    assert sync_result["enabled"] is False
    assert callback_result["status"] == "disabled"
    assert "decisions" not in store
    assert telegram.sent == []
    assert tendwire.commands == []
    assert decisions.config.remote_decisions_enabled({}) is True
    assert decisions.config.remote_decisions_enabled({"HERDRES_REMOTE_DECISIONS": ""}) is True


def test_gateway_routes_owner_callback_and_answers_query(
    tmp_path, monkeypatch
) -> None:
    store = _store()
    telegram = FakeTelegram()
    record = _post(store, telegram, _pending())
    monkeypatch.setenv("HERDR_TELEGRAM_TOPICS_STATE", str(tmp_path / "state.json"))
    state.save_state(store)
    tendwire = FakeTendwire()
    monkeypatch.setattr(herdres_gateway, "TelegramClient", lambda **_kwargs: telegram)
    monkeypatch.setattr(herdres_gateway, "TendwireClient", lambda: tendwire)

    checkpoint = herdres_gateway.handle_update(
        {
            "update_id": 103,
            "callback_query": {
                "id": "callback-1",
                "from": {"id": 7, "is_bot": False},
                "data": _button(record, "1"),
                "message": {
                    "message_id": int(record["message_id"]),
                    "message_thread_id": 77,
                    "chat": {"id": -100, "is_forum": True},
                },
            },
        },
        "fake-token",
        receiver_id="manager",
        request_id_key=REQUEST_KEY,
    )

    assert checkpoint == herdres_gateway.CHECKPOINT_ADVANCE
    assert validate_request_id(tendwire.commands[0]["request_id"])
    assert state.load_state()["decisions"]["active"] == {}
    assert telegram.callback_answers[-1]["callback_query_id"] == "callback-1"
    assert telegram.callback_answers[-1]["text"] == "Answered."


def test_gateway_rejects_non_owner_callback_but_still_answers(
    tmp_path, monkeypatch
) -> None:
    store = _store()
    telegram = FakeTelegram()
    record = _post(store, telegram, _pending())
    monkeypatch.setenv("HERDR_TELEGRAM_TOPICS_STATE", str(tmp_path / "state.json"))
    state.save_state(store)
    tendwire = FakeTendwire()
    monkeypatch.setattr(herdres_gateway, "TelegramClient", lambda **_kwargs: telegram)
    monkeypatch.setattr(herdres_gateway, "TendwireClient", lambda: tendwire)

    checkpoint = herdres_gateway.handle_update(
        {
            "update_id": 104,
            "callback_query": {
                "id": "callback-denied",
                "from": {"id": 8, "is_bot": False},
                "data": _button(record, "1"),
                "message": {
                    "message_id": int(record["message_id"]),
                    "message_thread_id": 77,
                    "chat": {"id": -100, "is_forum": True},
                },
            },
        },
        "fake-token",
        receiver_id="manager",
        request_id_key=REQUEST_KEY,
    )

    assert checkpoint == herdres_gateway.CHECKPOINT_ADVANCE
    assert tendwire.commands == []
    assert "not allowed" in telegram.callback_answers[-1]["text"]
    assert "77" in state.load_state()["decisions"]["active"]


def test_decision_sync_failure_is_exception_isolated(monkeypatch) -> None:
    monkeypatch.setattr(
        source_sync.decisions,
        "sync_decisions",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("boom")),
    )
    runtime = SimpleNamespace(telegram=FakeTelegram(), dry_run=False)

    result = source_sync._deliver_decisions(
        _store(), _pending(), runtime, chat_id="-100"
    )

    assert result["status"] == "failed"
    assert result["changed"] is False


def test_dry_run_decision_sync_has_no_telegram_or_state_writes() -> None:
    store = _store()
    telegram = FakeTelegram()

    result = decisions.sync_decisions(
        store, _pending(), telegram, chat_id="-100", dry_run=True
    )

    assert result["dry_run"] is True
    assert result["resolved"] == 1
    assert "decisions" not in store
    assert telegram.sent == []


def test_tendwire_client_accepts_answer_decision_contract(monkeypatch) -> None:
    calls = []
    request = {
        "schema_version": 1,
        "action": "answer_decision",
        "request_id": _request_id(),
        "target": {"worker_id": "worker-1"},
        "params": {
            "decision_ref": "decision-1",
            "selection": {"option_refs": ["1"]},
        },
    }
    monkeypatch.setenv("HERDRES_TENDWIRE_BIN", "tw")

    def fake_run(argv, **kwargs):
        calls.append((argv, kwargs))
        return SimpleNamespace(
            returncode=0,
            stdout=json.dumps(
                {
                    "ok": True,
                    "status": "accepted",
                    "action": "answer_decision",
                    "request_id": request["request_id"],
                }
            ).encode(),
            stderr=b"",
        )

    monkeypatch.setattr(tendwire_client.subprocess, "run", fake_run)

    result = tendwire_client.TendwireClient().command(request)

    assert result["ok"] is True
    assert result["status"] == "accepted"
    assert json.loads(calls[0][1]["input"].decode()) == request


def test_send_message_attaches_markup_only_to_final_split_and_helpers_use_api() -> None:
    calls: list[tuple[str, dict]] = []

    class RecordingTelegram(TelegramClient):
        def api(self, method, payload):
            calls.append((method, dict(payload)))
            return {"ok": True, "result": {"message_id": len(calls)}}

    telegram = RecordingTelegram(token="fake")
    markup = {"inline_keyboard": [[{"text": "One", "callback_data": "hdec:x:1"}]]}

    result = telegram.send_message("-100", "x" * 8000, reply_markup=markup)
    telegram.edit_message_reply_markup("-100", "9", {"inline_keyboard": []})
    telegram.answer_callback_query("callback-9", "Updated")

    send_calls = [payload for method, payload in calls if method == "sendMessage"]
    assert result["ok"] is True
    assert len(send_calls) > 1
    assert all("reply_markup" not in payload for payload in send_calls[:-1])
    assert json.loads(send_calls[-1]["reply_markup"]) == markup
    assert result["reply_markup_message_id"] == str(len(send_calls))
    assert calls[-2][0] == "editMessageReplyMarkup"
    assert calls[-1] == (
        "answerCallbackQuery",
        {
            "callback_query_id": "callback-9",
            "show_alert": "false",
            "text": "Updated",
        },
    )
