from __future__ import annotations

import base64
import contextlib
import hashlib
import json
import multiprocessing
import os
import sqlite3
import stat
import threading
from dataclasses import replace
from pathlib import Path

import pytest

from herdres_connector import ingress
from herdres_connector.decisions import render_ingress_markup
from herdres_connector.ingress import IngressPorts
from herdres_connector.ingress_queue import IngressQueue, NOTICE_EVIDENCE_SECONDS, SCHEMA_VERSION, StoreResult
from herdres_connector.state import (
    DecisionIngressResult, DecisionMutationResult, DecisionMutationStatus,
    DecisionOption, DecisionStatus, IngressPolicy, IngressReceiver,
    IngressRouteResult, PhysicalOwner, RouteStatus, SecretStr, StableOwner,
    StateToken,
)


def _request_id(index: int) -> str:
    digest = hashlib.sha256(str(index).encode()).digest()
    return "hri1_" + base64.urlsafe_b64encode(digest).rstrip(b"=").decode()


def _accept(queue: IngressQueue, index: int, *, key: str = "topic", now: float = 10.0, deadline: float = 100.0, retain: float = 200.0, depth: int = 32):
    return queue.accept_update({
        "receiver_id": "manager", "update_id": index, "request_id": _request_id(index),
        "ordering_key": key, "kind": "decision" if index % 2 else "message",
        "input": {"chat_id": "10", "message_id": str(index)}, "first_seen_at": now,
        "deadline_at": deadline, "retain_until": retain, "depth_limit": depth,
    })


def _command(request_id: str, *, fingerprint: bool = True) -> dict:
    target = {"worker_id": "worker-1"}
    if fingerprint:
        target["worker_fingerprint"] = "fp-1"
    return {"schema_version": 1, "action": "send_instruction", "request_id": request_id, "target": target}


def _checkpoint(item, *, generation=None, fingerprint=True, now=12.0) -> dict:
    return {
        "command": _command(item.request_id, fingerprint=fingerprint),
        "target_stable_key": "owner-1", "target_stable_key_version": 1,
        "target_route_generation": generation, "target_worker_id": "worker-1",
        "target_space_id": "space-1", "target_bot_kind": "manager", "now": now,
    }


@pytest.fixture
def queue_path(tmp_path: Path) -> Path:
    return tmp_path / "inbound_spool.db"


def test_schema_security_and_exclusive_writer(queue_path: Path) -> None:
    with IngressQueue.open_writer(queue_path) as queue:
        assert queue._connection.execute("PRAGMA user_version").fetchone()[0] == SCHEMA_VERSION
        assert queue._connection.execute("PRAGMA journal_mode").fetchone()[0] == "wal"
        assert queue._connection.execute("PRAGMA synchronous").fetchone()[0] == 2
        columns = queue._connection.execute("PRAGMA table_info(requests)").fetchall()
        assert len(columns) == 44
        assert stat.S_IMODE(queue_path.stat().st_mode) == 0o600
        assert stat.S_IMODE(queue_path.with_name(queue_path.name + "-wal").stat().st_mode) == 0o600
        with pytest.raises(BlockingIOError):
            IngressQueue.open_writer(queue_path)


def test_unsafe_database_and_symlink_are_rejected(tmp_path: Path) -> None:
    unsafe = tmp_path / "unsafe.db"
    unsafe.write_bytes(b"not sqlite")
    unsafe.chmod(0o644)
    with pytest.raises(OSError):
        IngressQueue.open_writer(unsafe)
    target = tmp_path / "target.db"
    target.write_bytes(b"")
    target.chmod(0o600)
    link = tmp_path / "link.db"
    link.symlink_to(target)
    with pytest.raises(OSError):
        IngressQueue.open_writer(link)


@pytest.mark.parametrize("suffix", ("", "-wal", "-shm"))
def test_live_queue_rejects_file_family_inode_replacement(queue_path: Path, suffix: str) -> None:
    queue = IngressQueue.open_writer(queue_path)
    _accept(queue, 1)
    member = queue_path.with_name(queue_path.name + suffix)
    replacement = member.with_name(member.name + ".replacement")
    replacement.write_bytes(member.read_bytes()); replacement.chmod(0o600); os.replace(replacement, member)
    with pytest.raises(OSError, match="changed or is unsafe"):
        queue.cursor("manager")
    with pytest.raises(OSError):
        queue.close()


@pytest.mark.parametrize("operation", ("replace", "remove"))
def test_live_queue_rejects_writer_lock_leaf_change(
    queue_path: Path,
    operation: str,
) -> None:
    queue = IngressQueue.open_writer(queue_path)
    lock_path = queue_path.with_name(".inbound-spool.sqlite.writer.lock")
    if operation == "replace":
        replacement = lock_path.with_name(lock_path.name + ".replacement")
        replacement.write_bytes(b"")
        replacement.chmod(0o600)
        os.replace(replacement, lock_path)
    else:
        lock_path.unlink()
    with pytest.raises(OSError):
        queue.cursor("manager")
    with pytest.raises(OSError):
        queue.close()


def test_cursor_acceptance_duplicate_collision_and_overflow_throttle(queue_path: Path) -> None:
    with IngressQueue.open_writer(queue_path) as queue:
        assert queue.cursor("manager") is None
        assert queue.initialize_cursor("manager", 4) == 4
        first = _accept(queue, 5, key="same", depth=1)
        assert (first.status, first.next_update_id) == ("enqueued", 6)
        assert _accept(queue, 5, key="same", depth=1).status == "duplicate"
        with pytest.raises(RuntimeError, match="identity collision"):
            queue.accept_update({
                "receiver_id": "manager", "update_id": 5, "request_id": _request_id(99),
                "ordering_key": "same", "kind": "decision", "input": {},
                "first_seen_at": 10.0, "deadline_at": 100.0, "retain_until": 200.0,
            })
        assert queue.cursor("manager") == 6
        overflow = _accept(queue, 6, key="same", now=10.0, depth=1)
        throttled = _accept(queue, 7, key="same", now=69.999, depth=1)
        boundary = _accept(queue, 8, key="same", now=70.0, depth=1)
        states = queue._connection.execute("SELECT seq,notify_state FROM requests WHERE seq IN (?,?,?) ORDER BY seq", (overflow.seq, throttled.seq, boundary.seq)).fetchall()
        assert overflow.status == throttled.status == boundary.status == "overflow"
        assert [row["notify_state"] for row in states] == ["pending", "none", "pending"]


def test_cursor_and_enqueue_roll_back_together_on_commit_path_failure(queue_path: Path) -> None:
    with IngressQueue.open_writer(queue_path) as queue:
        queue._connection.execute("CREATE TEMP TRIGGER reject_cursor BEFORE INSERT ON receiver_cursors BEGIN SELECT RAISE(ABORT,'cursor failure'); END")
        with pytest.raises(sqlite3.IntegrityError, match="cursor failure"):
            _accept(queue, 1)
        queue._connection.execute("DROP TRIGGER reject_cursor")
        assert queue.cursor("manager") is None
        assert queue._connection.execute("SELECT COUNT(*) FROM requests").fetchone()[0] == 0


def test_fifo_unrelated_keys_and_claim_attempts(queue_path: Path) -> None:
    with IngressQueue.open_writer(queue_path) as queue:
        one, two, other = _accept(queue, 1, key="topic"), _accept(queue, 2, key="topic"), _accept(queue, 3, key="other")
        claimed = queue.claim("worker-a", 11.0, 10.0)
        assert (claimed.seq, claimed.attempts) == (one.seq, 1)
        concurrent = queue.claim("worker-b", 11.0, 10.0)
        assert concurrent.seq == other.seq
        assert queue.quarantine(claimed.seq, "wrong", {"reason": "bad", "now": 12.0, "notify": False}).status == "lost"
        assert queue.quarantine(claimed.seq, "worker-a", {"reason": "bad", "now": 12.0, "notify": False}).status == "quarantined"
        assert queue.claim("worker-a", 12.0, 10.0).seq == two.seq


def test_renew_expiry_reclaim_and_deadline_equality(queue_path: Path) -> None:
    with IngressQueue.open_writer(queue_path) as queue:
        first = _accept(queue, 1, deadline=30.0)
        item = queue.claim("worker", 11.0, 5.0)
        assert not queue.renew(first.seq, "other", 12.0, 10.0)
        assert queue.renew(first.seq, "worker", 12.0, 10.0)
        assert queue.renew(first.seq, "worker", 13.0, 1.0)
        assert queue._connection.execute("SELECT lease_until FROM requests WHERE seq=?", (first.seq,)).fetchone()[0] == 22.0
        assert queue.claim("other", 21.999, 5.0) is None
        assert not queue.renew(first.seq, "worker", 22.0, 1.0)
        reclaimed = queue.claim("other", 22.0, 5.0)
        assert (reclaimed.seq, reclaimed.attempts) == (first.seq, 2)
        assert queue.claim("third", 30.0, 5.0) is None
        row = queue._connection.execute("SELECT state,disposition FROM requests WHERE seq=?", (first.seq,)).fetchone()
        assert tuple(row) == ("quarantine", "deadline_expired")


def test_owner_mutations_expire_without_competing_claim(queue_path: Path) -> None:
    with IngressQueue.open_writer(queue_path) as queue:
        command_item = queue.claim("command", 11.0, 1.0) if _accept(
            queue, 20, key="command", deadline=30.0
        ) else None
        assert queue.store_command(
            command_item.seq,
            "command",
            _checkpoint(command_item, now=12.0),
        ).status == "lost"

        deadline_item = queue.claim("local-store", 11.0, 10.0) if _accept(
            queue, 21, key="local-store", deadline=12.0
        ) else None
        deadline_action = {
            "schema_version": 1,
            "action": "ARM_FREEFORM",
            "request_id": deadline_item.request_id,
            "callback_ref": "c",
            "decision_ref": "d",
            "revision_digest": "r",
        }
        assert queue.store_local_action(
            deadline_item.seq,
            "local-store",
            {
                "local_action": deadline_action,
                "expected_state_token": "s0",
                "now": 12.0,
            },
        ).status == "lost"

        local_item = queue.claim("phase", 11.0, 1.0) if _accept(
            queue, 22, key="phase", deadline=30.0
        ) else None
        local_action = {**deadline_action, "request_id": local_item.request_id}
        local = queue.store_local_action(
            local_item.seq,
            "phase",
            {
                "local_action": local_action,
                "expected_state_token": "s0",
                "now": 11.5,
            },
        )
        assert queue.advance_local_phase(
            local_item.seq,
            "phase",
            {
                "operation_digest": local.digest,
                "from_phase": "checkpointed",
                "to_phase": "state_applied",
                "expected_token": "s0",
                "state_token": "s1",
                "now": 12.0,
            },
        ).status == "lost"

        results = []
        for index, operation in enumerate(("settle", "retry", "quarantine"), 23):
            item = queue.claim(operation, 11.0, 1.0) if _accept(
                queue, index, key=operation, deadline=30.0
            ) else None
            stored = queue.store_command(
                item.seq,
                operation,
                _checkpoint(item, now=11.5),
            )
            if operation == "settle":
                result = queue.settle_receipt(
                    item.seq,
                    operation,
                    {
                        "operation_digest": stored.digest,
                        "receipt_kind": "daemon",
                        "receipt": {"status": "accepted"},
                        "disposition": "terminal_accepted",
                        "now": 12.0,
                    },
                )
            elif operation == "retry":
                result = queue.schedule_retry(
                    item.seq,
                    operation,
                    {
                        "operation_digest": stored.digest,
                        "disposition": "no_receipt",
                        "now": 12.0,
                        "next_attempt_at": 13.0,
                    },
                )
            else:
                result = queue.quarantine(
                    item.seq,
                    operation,
                    {
                        "operation_digest": stored.digest,
                        "reason": "uncertain",
                        "notify": False,
                        "now": 12.0,
                    },
                )
            results.append(result.status)
        assert results == ["lost", "lost", "lost"]

        rows = queue._connection.execute(
            "SELECT seq,state,disposition FROM requests WHERE seq>=? ORDER BY seq",
            (command_item.seq,),
        ).fetchall()
        assert [tuple(row)[1:] for row in rows] == [
            ("retry", "lease_expired"),
            ("quarantine", "deadline_expired"),
            ("retry", "lease_expired"),
            ("retry", "lease_expired"),
            ("retry", "lease_expired"),
            ("retry", "lease_expired"),
        ]


def test_command_checkpoint_replay_and_only_permitted_refresh(queue_path: Path) -> None:
    with IngressQueue.open_writer(queue_path) as queue:
        accepted = _accept(queue, 1)
        item = queue.claim("worker", 11.0, 30.0)
        stored = queue.store_command(item.seq, "worker", _checkpoint(item))
        assert stored.status == "stored"
        assert queue.store_command(item.seq, "worker", _checkpoint(item)).status == "existing"
        refreshed = queue.store_command(item.seq, "worker", _checkpoint(item, fingerprint=False))
        assert refreshed.status == "refreshed"
        assert refreshed.digest != stored.digest
        assert queue.store_command(item.seq, "worker", _checkpoint(item)).status == "conflict"
        row = queue._connection.execute("SELECT state,route_refresh_count FROM requests WHERE seq=?", (accepted.seq,)).fetchone()
        assert tuple(row) == ("quarantine", 1)


def test_route_generation_forbids_fingerprint_removal(queue_path: Path) -> None:
    with IngressQueue.open_writer(queue_path) as queue:
        item = queue.claim("worker", 11.0, 30.0) if _accept(queue, 1) else None
        generation = "twroute1." + "A" * 43
        assert queue.store_command(item.seq, "worker", _checkpoint(item, generation=generation)).status == "stored"
        assert queue.store_command(item.seq, "worker", _checkpoint(item, generation=generation, fingerprint=False)).status == "conflict"
        with pytest.raises(ValueError, match="route_generation"):
            IngressQueue._route_generation("twroute1.short")


def test_retry_digest_cas_and_terminal_daemon_receipt(queue_path: Path) -> None:
    with IngressQueue.open_writer(queue_path) as queue:
        item = queue.claim("worker", 11.0, 30.0) if _accept(queue, 1) else None
        stored = queue.store_command(item.seq, "worker", _checkpoint(item))
        retry = {"operation_digest": stored.digest, "disposition": "no_receipt", "now": 12.0, "next_attempt_at": 15.0}
        assert queue.schedule_retry(item.seq, "other", retry).status == "lost"
        assert queue.schedule_retry(item.seq, "worker", {**retry, "operation_digest": "x" * 43}).status == "lost"
        assert queue.schedule_retry(item.seq, "worker", retry).status == "retry"
        replay = queue.claim("worker-2", 15.0, 30.0)
        settlement = {
            "operation_digest": stored.digest, "receipt_kind": "daemon",
            "receipt": {"status": "rejected"}, "disposition": "terminal_rejected",
            "terminal_reply": "Request rejected.", "notify": True, "now": 16.0,
        }
        assert queue.settle_receipt(replay.seq, "worker-2", settlement).status == "settled"
        assert queue.settle_receipt(replay.seq, "worker-2", settlement).status == "existing"
        assert queue.claim("worker-3", 17.0, 30.0) is None


def test_arm_and_toggle_local_phase_tokens(queue_path: Path) -> None:
    with IngressQueue.open_writer(queue_path) as queue:
        arm_item = queue.claim("worker", 11.0, 40.0) if _accept(queue, 1, key="arm") else None
        arm = {"schema_version": 1, "action": "ARM_FREEFORM", "request_id": arm_item.request_id, "callback_ref": "c", "decision_ref": "d", "revision_digest": "r"}
        stored = queue.store_local_action(
            arm_item.seq,
            "worker",
            {"local_action": arm, "expected_state_token": "s0", "now": 11.0},
        )
        assert queue.advance_local_phase(arm_item.seq, "worker", {"operation_digest": stored.digest, "from_phase": "checkpointed", "to_phase": "state_applied", "expected_token": "s0", "state_token": "s1", "now": 12.0}).status == "advanced"
        settled = queue.settle_receipt(arm_item.seq, "worker", {"operation_digest": stored.digest, "receipt_kind": "local", "receipt": {"status": "applied"}, "disposition": "local_applied", "now": 13.0})
        assert settled.status == "settled"

        toggle_item = queue.claim("worker", 21.0, 5.0) if _accept(queue, 2, key="toggle", now=20.0, deadline=100.0, retain=200.0) else None
        toggle = {"schema_version": 1, "action": "TOGGLE_OPTION", "request_id": toggle_item.request_id, "callback_ref": "c", "decision_ref": "d", "revision_digest": "r", "option_ref": "o", "desired_selected_refs": ["o"], "desired_markup_fingerprint": "m", "physical_owner": {"bot_identity": "b", "chat_id": "1", "topic_id": "2"}, "message_binding_id": "mb", "message_id": "3", "reply_markup": {"inline_keyboard": []}}
        local = queue.store_local_action(
            toggle_item.seq,
            "worker",
            {"local_action": toggle, "expected_state_token": "t0", "now": 21.0},
        )
        transitions = (
            {"from_phase": "checkpointed", "to_phase": "state_applied", "expected_token": "t0", "state_token": "t1"},
            {"from_phase": "state_applied", "to_phase": "provider_ready", "expected_token": "t1", "state_token": "t2"},
            {"from_phase": "provider_ready", "to_phase": "provider_applied", "expected_token": "t2", "provider_outcome": "not_modified", "provider_at": 24.0},
            {"from_phase": "provider_applied", "to_phase": "markup_recorded", "expected_token": "t2", "state_token": "t3"},
        )
        for offset, transition in enumerate(transitions[:3]):
            transition.update(operation_digest=local.digest, now=22.0 + offset)
            assert queue.advance_local_phase(toggle_item.seq, "worker", transition).status == "advanced"
        resumed = queue.claim("resume", 26.0, 10.0)
        assert (resumed.local_phase, resumed.local_expected_state_token, resumed.local_applied_state_token) == ("provider_applied", "t0", "t1")
        assert (resumed.local_provider_state_token, resumed.local_markup_state_token) == ("t2", None)
        assert (resumed.local_provider_outcome, resumed.local_provider_at, resumed.attempts) == ("not_modified", 24.0, 2)
        final = transitions[3]
        final.update(operation_digest=local.digest, now=26.0)
        assert queue.advance_local_phase(toggle_item.seq, "resume", final).status == "advanced"
        assert queue.settle_receipt(toggle_item.seq, "resume", {"operation_digest": local.digest, "receipt_kind": "local", "receipt": {"status": "markup_recorded"}, "disposition": "local_markup_applied", "now": 27.0}).status == "settled"


def test_notice_at_most_once_and_claimed_evidence_retention(queue_path: Path) -> None:
    with IngressQueue.open_writer(queue_path) as queue:
        item = queue.claim("worker", 11.0, 30.0) if _accept(queue, 1, deadline=15.0, retain=20.0) else None
        assert queue.quarantine(item.seq, "worker", {"reason": "uncertain", "terminal_reply": "Outcome uncertain.", "now": 12.0}).status == "quarantined"
        claim = queue.claim_notice(item.seq, 13.0)
        assert claim is not None and claim.terminal_reply == "Outcome uncertain."
        assert queue.claim_notice(item.seq, 14.0) is None
        assert queue.prune(21.0) == 0
        assert queue.prune(13.0 + NOTICE_EVIDENCE_SECONDS) == 0
        assert queue.prune(13.0 + NOTICE_EVIDENCE_SECONDS + 0.001) == 1


def test_restart_claims_oldest_pending_notice_without_resend(queue_path: Path) -> None:
    with IngressQueue.open_writer(queue_path) as queue:
        for index in (1, 2):
            _accept(queue, index, key=str(index)); item = queue.claim("worker", 11.0, 30.0)
            queue.quarantine(item.seq, "worker", {"reason": "uncertain", "terminal_reply": "Outcome uncertain.", "now": 12.0})
    with IngressQueue.open_writer(queue_path) as restarted:
        first = restarted.claim_next_notice(13.0)
        assert first is not None and first.seq == 1
    with IngressQueue.open_writer(queue_path) as restarted_again:
        second = restarted_again.claim_next_notice(14.0)
        assert second is not None and second.seq == 2
        assert restarted_again.claim_next_notice(15.0) is None


def _observe_in_child(path: str, result) -> None:
    with IngressQueue.observe(path) as observer:
        health = observer.health_snapshot(12.0)
        result.put((health.pending, health.processing, len(observer.status_rows(12.0))))


def test_live_read_only_observer_while_writer_is_locked(queue_path: Path) -> None:
    with IngressQueue.open_writer(queue_path) as queue:
        _accept(queue, 1)
        queue.claim("worker", 11.0, 10.0)
        with IngressQueue.observe(queue_path) as observer:
            health = observer.health_snapshot(12.0)
            assert (health.pending, health.processing, health.expired_leases) == (0, 1, 0)
            assert observer.status_rows(12.0)[0].count == 1
            with pytest.raises(sqlite3.OperationalError):
                observer._connection.execute("DELETE FROM requests")
        context = multiprocessing.get_context("spawn")
        result = context.Queue()
        process = context.Process(target=_observe_in_child, args=(str(queue_path), result))
        process.start()
        process.join(10)
        assert process.exitcode == 0
        assert result.get(timeout=2) == (0, 1, 1)


class _Telegram:
    def __init__(self, updates=()) -> None:
        self.updates = list(updates); self.edits = []; self.notices = []; self.acks = []

    def api(self, method, payload):
        assert method == "getUpdates"
        return {"ok": True, "result": self.updates.pop(0) if self.updates else []}

    def answer_callback_query(self, callback_id, text="", *, show_alert=False):
        self.acks.append((callback_id, text, show_alert)); return {"ok": True}

    def edit_message_reply_markup(self, chat_id, message_id, reply_markup):
        self.edits.append((chat_id, message_id, reply_markup)); return {"ok": True, "message_id": message_id}

    def send_message(self, chat_id, text, **kwargs):
        self.notices.append((chat_id, text, kwargs)); return {"ok": True, "message_id": "77"}


class _Tendwire:
    def __init__(self, callback=None) -> None:
        self.calls = []; self.callback = callback

    def command_json(self, value):
        self.calls.append(value)
        if self.callback is not None:
            self.callback()
        request = json.loads(value)
        return {
            "schema_version": 3, "action": request["action"], "request_id": request["request_id"],
            "ok": True, "dry_run": False, "status": "accepted", "disposition": "terminal_accepted",
            "result": {"target": {"worker_id": "worker-1"}, "delivery_state": "submitted",
                       "transport_state": "submitted", "target_state_at_send": "idle",
                       "observed_turn_state": "pending_observation"}, "error": None, "warnings": [],
        }


def _policy(token="s0") -> IngressPolicy:
    return IngressPolicy("-100", "1", frozenset({"7"}),
                         (("codex_bot", "codex"), ("claude_bot", "claude")), StateToken(token))


def _receiver(kind="manager") -> IngressReceiver:
    return IngressReceiver(kind, kind, f"{kind}_bot", SecretStr("token"))


def _owner() -> StableOwner:
    return StableOwner("wsk1_" + "a" * 64, 1, None)


def _route(status=RouteStatus.RESOLVED, *, bot_kind="codex", binding=True) -> IngressRouteResult:
    return IngressRouteResult(status, StateToken("s0"), "-100", "9", "worker-1", "fp-1",
                              _owner(), "space-1", bot_kind, "44" if binding else None,
                              binding, status.value)


def _decision(token="s0", *, selected=(), mode="multi") -> DecisionIngressResult:
    options = (DecisionOption("one", "One"), DecisionOption("two", "Two"))
    return DecisionIngressResult(DecisionStatus.ACTIVE, StateToken(token), "decision-1", "revision-1",
                                 mode, "worker-1", _owner(), "binding-1", "-100", "9", "50",
                                 tuple(option.option_ref for option in options), options, tuple(selected),
                                 False, "render-1", PhysicalOwner("manager", "-100", "9"))


def _ports(queue, telegram=None, tendwire=None, *, now=lambda: 11.0) -> IngressPorts:
    return IngressPorts(Path("/unused/state.json"), b"k" * 32, queue, (_receiver(),),
                        {"manager": telegram or _Telegram()}, tendwire or _Tendwire(),
                        threading.Event(), now=now, poll_timeout_seconds=0)


def test_preview_receiver_precedence_and_ambiguous_binding_quarantine() -> None:
    update = {"update_id": 5, "message": {"message_id": 6, "message_thread_id": 9,
              "chat": {"id": -100, "is_forum": True}, "from": {"id": 7}, "text": "hello",
              "reply_to_message": {"message_id": 44, "from": {"username": "claude_bot"}}}}
    manager = ingress.preview_update(update, _policy(), _receiver(), _route(bot_kind="codex"))
    assert (manager.disposition, manager.explicit_bot_kind) == ("advance", "claude")
    child = ingress.preview_update(update, _policy(), _receiver("claude"), _route(bot_kind="codex"))
    assert child.disposition == "enqueue"
    update["message"]["reply_to_message"].pop("from")
    bound = ingress.preview_update(update, _policy(), _receiver("codex"), _route(bot_kind="codex"))
    assert bound.disposition == "enqueue"
    ambiguous = ingress.preview_update(update, _policy(), _receiver(),
                                       _route(RouteStatus.BINDING_AMBIGUOUS))
    assert ambiguous.disposition == "quarantine"
    assert json.loads(ingress.canonical_input(ambiguous))["preview_quarantine"]


def test_canonical_input_ordering_and_exact_command_builders() -> None:
    preview = ingress.Preview("enqueue", "accepted", 8, "manager", "manager", "message",
                              "-100", "9", "10", "7", "hello", general_topic=False)
    assert ingress.canonical_input(preview) == json.dumps(
        json.loads(ingress.canonical_input(preview)), ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    callback = replace(preview, kind="decision", text="", callback_query_id="c",
                       callback_data="hdec:0123456789abcd:one", callback_ref="0123456789abcd",
                       callback_token="one")
    assert ingress.ordering_key(preview) == ingress.ordering_key(callback)
    assert ingress.ordering_key(replace(preview, owner_command=True, receiver_id="child")) != ingress.ordering_key(preview)
    item = ingress.QueueItem(1, _request_id(8), "manager", 8, "k", "message", {"text": "hello"},
        "processing", 1, 100.0, "lease", 20.0, None, None, None, None,
        None, None, None, None, None, None)
    send = ingress.build_send_instruction(item, _route())
    assert set(send.value) == {"schema_version", "action", "request_id", "dry_run", "target",
                               "instruction", "response_schema_version"}
    answered = replace(item, input={"selection": {"option_refs": ["one"]}})
    answer = ingress.build_answer_decision(answered, _decision())
    assert set(answer.value) == {"schema_version", "action", "request_id", "dry_run", "target", "params"}
    with pytest.raises(ValueError):
        ingress.build_answer_decision(replace(item, input={"selection": {"option_refs": []}}), _decision())


def test_daemon_receipt_matrix_and_transport_markers(monkeypatch) -> None:
    item = ingress.QueueItem(1, _request_id(9), "manager", 9, "k", "message", {"text": "hello"},
        "processing", 1, 100.0, "lease", 20.0, None, None, None, None,
        None, None, None, None, None, None)
    command = ingress.build_send_instruction(item, _route())
    response = _Tendwire().command_json(command.json)
    assert ingress.reduce_daemon_receipt(command, response).reason == "terminal_accepted"
    monkeypatch.setattr(ingress, "command_process_not_started", lambda value: value == "not-started")
    monkeypatch.setattr(ingress, "command_process_ambiguous", lambda value: value == "ambiguous")
    assert ingress.reduce_daemon_receipt(command, "not-started").disposition == "retry"
    assert ingress.reduce_daemon_receipt(command, "ambiguous").disposition == "quarantine"
    uncertain = {**response, "ok": False, "status": "request_state_uncertain",
                 "disposition": "terminal_uncertain", "result": None,
                 "error": {"message": "unknown"}}
    assert ingress.reduce_daemon_receipt(command, uncertain).reason == "terminal_uncertain"


def test_dispatch_lease_loss_reuses_identical_command(queue_path: Path, monkeypatch) -> None:
    with IngressQueue.open_writer(queue_path) as queue:
        queue.accept_update({"receiver_id": "manager", "update_id": 2, "request_id": _request_id(2),
            "ordering_key": "topic", "kind": "message", "input": {"text": "hello"},
            "first_seen_at": 10.0, "deadline_at": 100.0, "retain_until": 200.0})
        first = queue.claim("first", 11.0, 1.0); reclaimed = []
        tendwire = _Tendwire(lambda: reclaimed.append(queue.claim("second", 12.0, 20.0)))
        ports = _ports(queue, tendwire=tendwire)
        missing = replace(_decision(), status=DecisionStatus.MISSING)
        monkeypatch.setattr(ingress, "_read_policy_and_decision", lambda ports, data, ref: missing)
        monkeypatch.setattr(ingress, "_route_for_item", lambda item, ports: _route())
        assert ingress.dispatch_one(queue, first, ports).status == "lost"
        tendwire.callback = None
        assert ingress.dispatch_one(queue, reclaimed[0], ports).status == "settled"
        assert tendwire.calls[0] == tendwire.calls[1]


def test_local_arm_crash_replays_composite_mutation(queue_path: Path, monkeypatch) -> None:
    with IngressQueue.open_writer(queue_path) as queue:
        accepted = _accept(queue, 1, key="arm"); item = queue.claim("worker", 11.0, 30.0)
        action = {"schema_version": 1, "action": "ARM_FREEFORM", "request_id": item.request_id,
                  "callback_ref": "0123456789abcd", "decision_ref": "decision-1", "revision_digest": "revision-1"}
        stored = queue.store_local_action(
            item.seq,
            "worker",
            {"local_action": action, "expected_state_token": "s0", "now": 11.0},
        )
        item = replace(item, local_action=action, operation_digest=stored.digest,
                       local_phase="checkpointed", local_expected_state_token="s0")
        outcomes = [DecisionMutationStatus.APPLIED, DecisionMutationStatus.ALREADY_APPLIED]
        monkeypatch.setattr(ingress, "apply_decision_ingress", lambda path, mutation:
            DecisionMutationResult(outcomes.pop(0), StateToken("s1"), "decision-1", (), True, "binding-1", "50", ""))
        original = queue.advance_local_phase; lost_once = [True]
        def lose_first(*args, **kwargs):
            if lost_once: lost_once.pop(); return StoreResult("lost", item.seq, stored.digest)
            return original(*args, **kwargs)
        monkeypatch.setattr(queue, "advance_local_phase", lose_first)
        assert ingress.apply_local_decision(queue, item, action, _ports(queue)).status == "lost"
        assert ingress.apply_local_decision(queue, item, action, _ports(queue)).status == "settled"
        assert queue._connection.execute("SELECT state FROM requests WHERE seq=?", (accepted.seq,)).fetchone()[0] == "terminal"


def test_local_toggle_runs_guarded_exact_edit_and_records_markup(queue_path: Path, monkeypatch) -> None:
    with IngressQueue.open_writer(queue_path) as queue:
        _accept(queue, 1, key="toggle"); item = queue.claim("worker", 11.0, 90.0)
        snapshot = _decision(); markup, fingerprint = render_ingress_markup(snapshot, ("one",))
        action = {"schema_version": 1, "action": "TOGGLE_OPTION", "request_id": item.request_id,
            "callback_ref": "0123456789abcd", "decision_ref": "decision-1", "revision_digest": "revision-1",
            "option_ref": "one", "desired_selected_refs": ["one"], "desired_markup_fingerprint": fingerprint,
            "physical_owner": {"bot_identity": "manager", "chat_id": "-100", "topic_id": "9"},
            "message_binding_id": "binding-1", "message_id": "50", "reply_markup": markup}
        stored = queue.store_local_action(
            item.seq,
            "worker",
            {"local_action": action, "expected_state_token": "s0", "now": 11.0},
        )
        item = replace(item, input={"chat_id": "-100", "topic_id": "9", "callback_ref": "0123456789abcd"},
                       local_action=action, operation_digest=stored.digest,
                       local_phase="checkpointed", local_expected_state_token="s0")
        mutations = []
        def mutate(path, mutation):
            mutations.append(mutation.kind.value); token = "s1" if len(mutations) == 1 else "s3"
            return DecisionMutationResult(DecisionMutationStatus.APPLIED, StateToken(token),
                "decision-1", ("one",), False, "binding-1", "50", fingerprint)
        telegram = _Telegram(); guarded = []
        @contextlib.contextmanager
        def guard(path, owner, deadline):
            guarded.append(owner); yield object()
        monkeypatch.setattr(ingress, "apply_decision_ingress", mutate)
        monkeypatch.setattr(ingress, "read_ingress_policy", lambda path: _policy("s2"))
        monkeypatch.setattr(ingress, "read_decision_ingress", lambda path, query: _decision("s2", selected=("one",)))
        monkeypatch.setattr(ingress, "provider_mutation_guard", guard)
        assert ingress.apply_local_decision(queue, item, action, _ports(queue, telegram=telegram)).status == "settled"
        assert mutations == ["TOGGLE_OPTION", "RECORD_LOCAL_MARKUP"]
        assert telegram.edits == [("-100", "50", markup)] and guarded == [PhysicalOwner("manager", "-100", "9")]


def test_notice_delivery_and_polling_cursor_acceptance(queue_path: Path, monkeypatch) -> None:
    with IngressQueue.open_writer(queue_path) as queue:
        item = queue.claim("worker", 11.0, 30.0) if _accept(queue, 1, key="notice") else None
        queue.quarantine(item.seq, "worker", {"reason": "uncertain", "terminal_reply": "Outcome uncertain.", "now": 12.0})
        claim = queue.claim_next_notice(13.0); telegram = _Telegram()
        assert ingress.send_terminal_notice(queue, claim, telegram, now=lambda: 14.0).status == "sent"
        assert queue.claim_next_notice(15.0) is None and len(telegram.notices) == 1
    second_path = queue_path.with_name("poll.db")
    historical = [{"update_id": 5, "message": {"message_id": 5, "message_thread_id": 9,
                  "chat": {"id": -100}, "from": {"id": 7}, "text": "old"}}]
    live = [{"update_id": 6, "message": {"message_id": 6, "message_thread_id": 9,
            "chat": {"id": -100}, "from": {"id": 7}, "text": "new"}}]
    telegram = _Telegram((historical, live))
    with IngressQueue.open_writer(second_path) as queue:
        ports = _ports(queue, telegram=telegram)
        monkeypatch.setattr(ingress, "read_ingress_policy", lambda path: _policy())
        assert ingress.poll_receiver_once(_receiver(), queue, ports).advanced == 1
        polled = ingress.poll_receiver_once(_receiver(), queue, ports)
        assert (polled.enqueued, queue.cursor("manager")) == (1, 7)
