from __future__ import annotations

import threading
import time
from dataclasses import replace
from pathlib import Path
from typing import Any

import pytest

from herdres_connector import decisions, ingress
from herdres_connector.ingress import IngressPorts
from herdres_connector.ingress_identity import derive_telegram_request_id
from herdres_connector.ingress_queue import IngressQueue
from herdres_connector.state import (
    DecisionIngressResult,
    DecisionMutationResult,
    DecisionMutationStatus,
    DecisionOption,
    DecisionStatus,
    IngressPolicy,
    IngressReceiver,
    PhysicalOwner,
    SecretStr,
    StableOwner,
    StateToken,
)


class _UnusedTendwire:
    def command_json(self, _request_json: str) -> dict[str, Any]:
        raise AssertionError("Tendwire must not be called")


class _CoordinatedTelegram:
    def __init__(self) -> None:
        self._counter_lock = threading.Lock()
        self.active = 0
        self.max_active = 0
        self.delete_entered = threading.Event()
        self.edit_entered = threading.Event()
        self.release_delete = threading.Event()

    def _begin(self) -> None:
        with self._counter_lock:
            self.active += 1
            self.max_active = max(self.max_active, self.active)

    def _end(self) -> None:
        with self._counter_lock:
            self.active -= 1

    def delete_message(self, _chat_id: str, _message_id: str) -> dict[str, Any]:
        self._begin()
        try:
            self.delete_entered.set()
            if not self.release_delete.wait(timeout=5):
                raise TimeoutError("test did not release presenter deletion")
            return {"ok": True}
        finally:
            self._end()

    def edit_message_reply_markup(
        self, _chat_id: str, message_id: str, _reply_markup: dict[str, Any]
    ) -> dict[str, Any]:
        self._begin()
        try:
            self.edit_entered.set()
            return {"ok": True, "message_id": message_id}
        finally:
            self._end()


@pytest.fixture
def guarded_state(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    state_parent = tmp_path / "guarded-state"
    state_parent.mkdir(mode=0o700)
    state_path = state_parent / "state.json"
    key_path = state_parent / "request-id.key"
    key_path.write_bytes(b"k" * 32)
    key_path.chmod(0o600)
    monkeypatch.setenv("HERDR_TELEGRAM_TOPICS_STATE", str(state_path))
    monkeypatch.setenv("HERDRES_REQUEST_ID_KEY_PATH", str(key_path))
    return state_path


def _decision(topic_id: str) -> DecisionIngressResult:
    options = (
        DecisionOption("one", "One"),
        DecisionOption("two", "Two"),
    )
    return DecisionIngressResult(
        DecisionStatus.ACTIVE,
        StateToken("st1.provider-ready"),
        "decision-1",
        "revision-1",
        "multi",
        "worker-1",
        StableOwner("wsk1_" + "a" * 64, 1, None),
        "binding-1",
        "-100",
        topic_id,
        "50",
        tuple(option.option_ref for option in options),
        options,
        ("one",),
        False,
        "render-1",
        PhysicalOwner("manager", "-100", topic_id),
    )


def _ready_local_toggle(
    queue: IngressQueue,
    state_path: Path,
    telegram: _CoordinatedTelegram,
    *,
    topic_id: str,
    update_id: int,
) -> tuple[Any, dict[str, Any], IngressPorts, DecisionIngressResult]:
    request_id = derive_telegram_request_id(
        b"k" * 32,
        receiver_id="manager",
        update_id=update_id,
        chat_id=-100,
        message_id=update_id,
    )
    accepted = queue.accept_update(
        {
            "receiver_id": "manager",
            "update_id": update_id,
            "request_id": request_id,
            "ordering_key": f'["topic","-100","{topic_id}"]',
            "kind": "decision",
            "input": {
                "chat_id": "-100",
                "topic_id": topic_id,
                "message_id": str(update_id),
                "callback_ref": "0123456789abcd",
            },
            "first_seen_at": 10.0,
            "deadline_at": 100.0,
            "retain_until": 200.0,
        }
    )
    assert accepted.status == "enqueued"
    item = queue.claim(f"local-{update_id}", 11.0, 80.0)
    assert item is not None
    snapshot = _decision(topic_id)
    markup, fingerprint = decisions.render_ingress_markup(snapshot, ("one",))
    action = {
        "schema_version": 1,
        "action": "TOGGLE_OPTION",
        "request_id": request_id,
        "callback_ref": "0123456789abcd",
        "decision_ref": snapshot.decision_ref,
        "revision_digest": snapshot.revision_digest,
        "option_ref": "one",
        "desired_selected_refs": ["one"],
        "desired_markup_fingerprint": fingerprint,
        "physical_owner": {
            "bot_identity": "manager",
            "chat_id": "-100",
            "topic_id": topic_id,
        },
        "message_binding_id": snapshot.message_binding_id,
        "message_id": snapshot.message_id,
        "reply_markup": markup,
    }
    stored = queue.store_local_action(
        item.seq,
        item.lease_owner,
        {
            "local_action": action,
            "expected_state_token": "st1.checkpointed",
            "now": 11.0,
        },
    )
    assert stored.status == "stored" and stored.digest
    assert queue.advance_local_phase(
        item.seq,
        item.lease_owner,
        {
            "operation_digest": stored.digest,
            "from_phase": "checkpointed",
            "to_phase": "state_applied",
            "expected_token": "st1.checkpointed",
            "state_token": "st1.state-applied",
            "now": 11.0,
        },
    ).status == "advanced"
    assert queue.advance_local_phase(
        item.seq,
        item.lease_owner,
        {
            "operation_digest": stored.digest,
            "from_phase": "state_applied",
            "to_phase": "provider_ready",
            "expected_token": "st1.state-applied",
            "state_token": snapshot.state_token.value,
            "now": 11.0,
        },
    ).status == "advanced"
    item = replace(
        item,
        local_action=action,
        operation_digest=stored.digest,
        local_phase="provider_ready",
        local_expected_state_token="st1.checkpointed",
        local_applied_state_token="st1.state-applied",
        local_provider_state_token=snapshot.state_token.value,
    )
    ports = IngressPorts(
        state_path=state_path,
        request_id_key=b"k" * 32,
        queue=queue,
        receivers=(),
        telegram_clients={"manager": telegram},
        tendwire=_UnusedTendwire(),
        now=lambda: 12.0,
        provider_timeout_seconds=3.0,
    )
    return item, action, ports, snapshot


def _old_delete_operation(topic_id: str) -> decisions.ProviderOperation:
    record = {
        "entry_key": "pane-1",
        "worker_id": "worker-1",
        "decision_id": "old-decision",
        "message_id": "49",
    }
    return decisions._operation(
        "telegram.delete_message",
        reason="telegram.delete_message: retire stale decision card",
        record=record,
        topic_id=topic_id,
        message_id="49",
        scope="exact",
        args=("-100", "49"),
    )


def _run_thread(call: Any, results: list[Any], errors: list[BaseException]) -> None:
    try:
        results.append(call())
    except BaseException as exc:  # noqa: BLE001 - surfaced by the test thread
        errors.append(exc)


def _install_local_projection(
    monkeypatch: pytest.MonkeyPatch, snapshot: DecisionIngressResult
) -> None:
    monkeypatch.setattr(
        ingress,
        "_read_policy_and_decision",
        lambda _ports, _input, _callback_ref: snapshot,
    )
    monkeypatch.setattr(
        ingress,
        "apply_decision_ingress",
        lambda _path, _mutation: DecisionMutationResult(
            DecisionMutationStatus.APPLIED,
            StateToken("st1.markup-recorded"),
            snapshot.decision_ref,
            snapshot.selected_refs,
            False,
            snapshot.message_binding_id,
            snapshot.message_id,
            "",
        ),
    )


def test_same_physical_owner_serializes_presenter_delete_and_h8_markup(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    guarded_state: Path,
) -> None:
    telegram = _CoordinatedTelegram()
    with IngressQueue.open_writer(tmp_path / "same-owner.db") as queue:
        item, action, ports, snapshot = _ready_local_toggle(
            queue, guarded_state, telegram, topic_id="77", update_id=701
        )
        _install_local_projection(monkeypatch, snapshot)
        old_results: list[Any] = []
        local_results: list[Any] = []
        errors: list[BaseException] = []
        old = threading.Thread(
            target=_run_thread,
            args=(
                lambda: decisions._execute(
                    _old_delete_operation("77"), telegram=telegram
                ),
                old_results,
                errors,
            ),
        )
        local = threading.Thread(
            target=_run_thread,
            args=(
                lambda: ingress.apply_local_decision(queue, item, action, ports),
                local_results,
                errors,
            ),
        )
        old.start()
        assert telegram.delete_entered.wait(timeout=3)
        local.start()
        try:
            assert not telegram.edit_entered.wait(timeout=0.2)
        finally:
            telegram.release_delete.set()
            old.join(timeout=3)
            local.join(timeout=3)

        assert not old.is_alive() and not local.is_alive()
        assert errors == []
        assert len(old_results) == len(local_results) == 1
        assert local_results[0].status == "settled"
        assert telegram.max_active == 1


def test_different_physical_owner_h8_markup_completes_while_presenter_is_blocked(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    guarded_state: Path,
) -> None:
    telegram = _CoordinatedTelegram()
    with IngressQueue.open_writer(tmp_path / "different-owner.db") as queue:
        item, action, ports, snapshot = _ready_local_toggle(
            queue, guarded_state, telegram, topic_id="88", update_id=702
        )
        _install_local_projection(monkeypatch, snapshot)
        old_results: list[Any] = []
        local_results: list[Any] = []
        errors: list[BaseException] = []
        old = threading.Thread(
            target=_run_thread,
            args=(
                lambda: decisions._execute(
                    _old_delete_operation("77"), telegram=telegram
                ),
                old_results,
                errors,
            ),
        )
        local = threading.Thread(
            target=_run_thread,
            args=(
                lambda: ingress.apply_local_decision(queue, item, action, ports),
                local_results,
                errors,
            ),
        )
        old.start()
        assert telegram.delete_entered.wait(timeout=3)
        local.start()
        try:
            assert telegram.edit_entered.wait(timeout=3)
            local.join(timeout=3)
            assert not local.is_alive()
            assert local_results[0].status == "settled"
            assert old.is_alive()
        finally:
            telegram.release_delete.set()
            old.join(timeout=3)
            local.join(timeout=3)

        assert not old.is_alive()
        assert errors == []
        assert len(old_results) == len(local_results) == 1
        assert telegram.max_active == 2


class _MediaPollingTelegram:
    def __init__(self, updates: list[dict[str, Any]], token: str) -> None:
        self.updates = updates
        self.token = token
        self.calls: list[tuple[str, dict[str, Any]]] = []

    def api(self, method: str, payload: dict[str, Any]) -> dict[str, Any]:
        self.calls.append((method, dict(payload)))
        return {"ok": True, "result": list(self.updates)}

    def answer_callback_query(self, *_args: Any, **_kwargs: Any) -> dict[str, Any]:
        raise AssertionError("media updates must not answer callbacks")


@pytest.mark.parametrize("media_field", ["voice", "audio", "document", "photo", "video"])
def test_voice_and_media_updates_advance_without_queueing_or_private_leakage(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    media_field: str,
) -> None:
    private_text = "PRIVATE_MEDIA_PAYLOAD_SHOULD_NOT_LEAK"
    private_token = "PRIVATE_TELEGRAM_TOKEN_SHOULD_NOT_LEAK"
    private_fields: Any = (
        [{"file_id": private_text, "file_unique_id": "PRIVATE_UNIQUE"}]
        if media_field == "photo"
        else {
            "file_id": private_text,
            "file_unique_id": "PRIVATE_UNIQUE",
            "file_name": "private-filename.bin",
            "mime_type": "application/private",
        }
    )
    update = {
        "update_id": 900,
        "message": {
            "message_id": 901,
            "message_thread_id": 77,
            "chat": {"id": -100, "is_forum": True},
            "from": {"id": 7, "username": "private-owner"},
            media_field: private_fields,
            "media_group_id": "PRIVATE_GROUP",
        },
    }
    policy = IngressPolicy(
        "-100",
        "1",
        frozenset({"7"}),
        (),
        StateToken("st1.media-policy"),
    )
    receiver = IngressReceiver(
        "manager", "manager", "manager_bot", SecretStr(private_token)
    )
    preview = ingress.preview_update(update, policy, receiver)
    assert (preview.disposition, preview.reason) == ("advance", "unsupported_message")
    assert preview.text == ""

    logs: list[str] = []
    telegram = _MediaPollingTelegram([update], private_token)
    monkeypatch.setattr(ingress, "read_ingress_policy", lambda _path: policy)
    with IngressQueue.open_writer(tmp_path / f"media-{media_field}.db") as queue:
        assert queue.initialize_cursor("manager", 900) == 900
        ports = IngressPorts(
            state_path=tmp_path / "unused-state.json",
            request_id_key=b"k" * 32,
            queue=queue,
            receivers=(receiver,),
            telegram_clients={"manager": telegram},
            tendwire=_UnusedTendwire(),
            poll_timeout_seconds=0,
            log=logs.append,
        )
        result = ingress.poll_receiver_once(receiver, queue, ports)
        assert (result.enqueued, result.advanced, result.errors) == (0, 1, 0)
        assert queue.cursor("manager") == 901
        assert queue._connection.execute(
            "SELECT COUNT(*) FROM requests"
        ).fetchone()[0] == 0

    captured = "\n".join(logs)
    for secret in (
        private_text,
        private_token,
        "PRIVATE_UNIQUE",
        "PRIVATE_GROUP",
        "private-filename.bin",
        "application/private",
        "private-owner",
    ):
        assert secret not in captured
    assert telegram.calls == [
        (
            "getUpdates",
            {
                "timeout": 0,
                "allowed_updates": '["message","callback_query"]',
                "offset": 900,
            },
        )
    ]
