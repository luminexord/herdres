from __future__ import annotations

import json
import multiprocessing
import os
import signal
import threading
import time
from pathlib import Path

import pytest

import herdres
import herdres_gateway
from herdres_connector import config, ingress_requests, state
from herdres_connector.ingress_identity import derive_telegram_request_id
from herdres_connector.ingress_lanes import IngressLaneSpool, lane_key

from test_source_only import (
    REQUEST_ID_KEY,
    _accepted_command_response,
    _failed_command_response,
    _source_worker,
    _store,
)


def _update(update_id: int, topic_id: int, text: str) -> dict[str, object]:
    return {
        "update_id": update_id,
        "message": {
            "chat": {"id": -100, "is_forum": True},
            "message_thread_id": topic_id,
            "message_id": update_id + 1000,
            "from": {"id": 1, "is_bot": False},
            "text": text,
        },
    }


def _request_id(update: dict[str, object], receiver: str = "manager") -> str:
    message = update["message"]
    assert isinstance(message, dict)
    chat = message["chat"]
    assert isinstance(chat, dict)
    return derive_telegram_request_id(
        REQUEST_ID_KEY,
        receiver_id=receiver,
        update_id=update["update_id"],
        chat_id=chat["id"],
        message_id=message["message_id"],
    )


def _enqueue(
    spool: IngressLaneSpool,
    update: dict[str, object],
    topic: str,
    *,
    first_seen_at: float | None = None,
    deadline_at: float | None = None,
) -> None:
    seen = time.time() if first_seen_at is None else first_seen_at
    spool.enqueue(
        request_id=_request_id(update),
        receiver_kind="manager",
        update_id=int(update["update_id"]),
        lane_key_value=lane_key("manager", topic),
        kind="message",
        update=update,
        route={"chat_id": "-100", "topic_id": topic},
        first_seen_at=seen,
        deadline_at=seen + 60 if deadline_at is None else deadline_at,
        depth_limit=32,
    )


def _wait_for(predicate, timeout: float = 3.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.01)
    assert predicate()


def _configured_state(path: Path) -> None:
    store = _store()
    for worker_id, topic in (("worker-a", "77"), ("worker-b", "88")):
        state.upsert_worker_entry(
            store,
            _source_worker(
                {
                    "id": worker_id,
                    "name": worker_id,
                    "status": "idle",
                    "space_id": "space-1",
                    "fingerprint": f"fp-{worker_id}",
                }
            ),
            topic_id=topic,
        )
    state.save_state(store, path=path)


def test_busy_lane_does_not_delay_another_agent_under_five_seconds(
    tmp_path, monkeypatch
) -> None:
    spool = IngressLaneSpool(tmp_path / "spool.db")
    _enqueue(spool, _update(1, 77, "A blocks"), "77")
    _enqueue(spool, _update(2, 88, "B flows"), "88")
    blocked = threading.Event()
    release = threading.Event()
    delivered_b = threading.Event()

    def handle(update, *_args, **_kwargs):
        topic = update["message"]["message_thread_id"]
        if topic == 77:
            blocked.set()
            release.wait(4.0)
        else:
            delivered_b.set()
        return herdres_gateway.CHECKPOINT_ADVANCE

    monkeypatch.setattr(herdres_gateway, "handle_update", handle)
    dispatcher = herdres_gateway._InboundLaneDispatcher(
        spool, REQUEST_ID_KEY, workers=2, backoff_seconds=0.01, lease_seconds=5
    )
    dispatcher.update_specs([("manager", "token", 0)])
    started_at = time.monotonic()
    dispatcher.start()
    try:
        assert blocked.wait(1.0)
        assert delivered_b.wait(4.0)
        assert time.monotonic() - started_at < 5.0
    finally:
        release.set()
        dispatcher.stop()


def test_same_lane_fifo_including_ack_while_other_lane_interleaves(
    tmp_path, monkeypatch
) -> None:
    spool = IngressLaneSpool(tmp_path / "spool.db")
    _enqueue(spool, _update(10, 77, "A1"), "77")
    _enqueue(spool, _update(11, 88, "B1"), "88")
    _enqueue(spool, _update(12, 77, "A2"), "77")
    a1_started = threading.Event()
    release_a1 = threading.Event()
    b_done = threading.Event()
    events: list[str] = []
    events_lock = threading.Lock()

    def record(value: str) -> None:
        with events_lock:
            events.append(value)

    def handle(update, *_args, **_kwargs):
        text = update["message"]["text"]
        record(f"start-{text}")
        if text == "A1":
            a1_started.set()
            release_a1.wait(2.0)
        if text == "B1":
            b_done.set()
        record(f"ack-{text}")
        return herdres_gateway.CHECKPOINT_ADVANCE

    monkeypatch.setattr(herdres_gateway, "handle_update", handle)
    dispatcher = herdres_gateway._InboundLaneDispatcher(
        spool, REQUEST_ID_KEY, workers=3, backoff_seconds=0.01, lease_seconds=5
    )
    dispatcher.update_specs([("manager", "token", 0)])
    dispatcher.start()
    try:
        assert a1_started.wait(1.0)
        assert b_done.wait(1.0)
        time.sleep(0.05)
        assert "start-A2" not in events
        release_a1.set()
        _wait_for(lambda: all(row["state"] == "done" for row in spool.rows()))
    finally:
        release_a1.set()
        dispatcher.stop()

    assert events.index("start-B1") < events.index("ack-A1")
    assert events.index("ack-A1") < events.index("start-A2")
    assert events.index("start-A2") < events.index("ack-A2")


def test_owner_commands_share_the_ordered_control_lane(tmp_path, monkeypatch) -> None:
    spool = IngressLaneSpool(tmp_path / "spool.db")
    first = _update(20, 77, "/status")
    second = _update(21, 88, "/help")
    _enqueue(spool, first, "__control__")
    _enqueue(spool, second, "__control__")
    first_started = threading.Event()
    release = threading.Event()
    calls: list[str] = []

    def handle(update, *_args, **_kwargs):
        text = update["message"]["text"]
        calls.append(text)
        if text == "/status":
            first_started.set()
            release.wait(2.0)
        return herdres_gateway.CHECKPOINT_ADVANCE

    monkeypatch.setattr(herdres_gateway, "handle_update", handle)
    dispatcher = herdres_gateway._InboundLaneDispatcher(
        spool, REQUEST_ID_KEY, workers=2, lease_seconds=5
    )
    dispatcher.update_specs([("manager", "token", 0)])
    dispatcher.start()
    try:
        assert first_started.wait(1.0)
        time.sleep(0.05)
        assert calls == ["/status"]
        release.set()
        _wait_for(lambda: len(calls) == 2)
    finally:
        release.set()
        dispatcher.stop()
    assert calls == ["/status", "/help"]


def test_poison_head_quarantines_visibly_without_delaying_other_lane(
    tmp_path, monkeypatch
) -> None:
    state_path = tmp_path / "state.json"
    _configured_state(state_path)
    monkeypatch.setenv("HERDR_TELEGRAM_TOPICS_STATE", str(state_path))
    monkeypatch.setenv("HERDRES_SOURCE_TOPIC_MODE", "worker")
    monkeypatch.setenv("HERDRES_ACK_ON_SEND", "1")
    monkeypatch.setattr(config, "command_retry_horizon_seconds", lambda _env=None: 0.5)
    monkeypatch.setattr(config, "command_request_retention_seconds", lambda _env=None: 1.0)
    spool = IngressLaneSpool(tmp_path / "spool.db")
    first_seen = time.time()
    _enqueue(
        spool,
        _update(30, 77, "poison A"),
        "77",
        first_seen_at=first_seen,
        deadline_at=first_seen + 0.5,
    )
    _enqueue(spool, _update(31, 88, "healthy B"), "88")
    backend_events: list[tuple[str, float]] = []
    notices: list[tuple[str, str, float]] = []

    class Client:
        def command_json(self, request_json):
            request = json.loads(request_json)
            worker_id = request["target"]["worker_id"]
            backend_events.append((worker_id, time.monotonic()))
            if worker_id == "worker-a":
                return _failed_command_response(
                    request, status="in_progress", disposition="in_progress"
                )
            return _accepted_command_response(request)

    class Telegram:
        def __init__(self, token):
            self.token = token

        def send_message(self, chat_id, reply, **kwargs):
            notices.append((str(kwargs.get("thread_id")), reply, time.monotonic()))
            return {"ok": True, "message_id": "1"}

    monkeypatch.setattr(herdres, "TendwireClient", Client)
    monkeypatch.setattr(
        herdres_gateway,
        "run_herdres_command",
        lambda payload: herdres.command_reply(payload),
    )
    monkeypatch.setattr(herdres_gateway, "TelegramClient", Telegram)
    dispatcher = herdres_gateway._InboundLaneDispatcher(
        spool, REQUEST_ID_KEY, workers=2, backoff_seconds=0.05, lease_seconds=2
    )
    dispatcher.update_specs([("manager", "token", 0)])
    dispatcher.start()
    try:
        _wait_for(lambda: all(row["state"] == "done" for row in spool.rows()))
    finally:
        dispatcher.stop()

    b_at = next(at for worker, at in backend_events if worker == "worker-b")
    quarantine = next(
        item for item in notices if item[1] == ingress_requests.QUARANTINE_REPLY
    )
    assert quarantine[0] == "77"
    assert b_at < quarantine[2]
    records = state.load_state()[ingress_requests.RECORDS_KEY]
    poison = records[_request_id(_update(30, 77, "poison A"))]
    assert poison["state"] == "quarantined"


def test_lane_overflow_notifies_once_and_advances_cursor(tmp_path, monkeypatch) -> None:
    spool = IngressLaneSpool(tmp_path / "spool.db")
    spool.initialize_cursor("manager", 41)
    _enqueue(spool, _update(40, 77, "already queued"), "77")
    notices: list[tuple[str, str]] = []
    mirrors: list[int] = []
    monkeypatch.setenv("HERDRES_INBOUND_LANE_DEPTH", "1")
    monkeypatch.setenv("HERDRES_SOURCE_TOPIC_MODE", "worker")
    store = _store()
    state.upsert_worker_entry(
        store,
        _source_worker(
            {
                "id": "worker-a",
                "name": "worker-a",
                "status": "idle",
                "space_id": "space-1",
                "fingerprint": "fp-a",
            }
        ),
        topic_id="77",
    )
    monkeypatch.setattr(herdres_gateway.state, "load_state", lambda: store)
    monkeypatch.setattr(
        herdres_gateway,
        "get_updates",
        lambda *_args, **_kwargs: [_update(41, 77, "overflow")],
    )
    monkeypatch.setattr(
        herdres_gateway,
        "_notify_lane_overflow",
        lambda _token, route, lane: notices.append((route["topic_id"], lane)),
    )
    monkeypatch.setattr(
        herdres_gateway, "_save_offset", lambda offset, _key: mirrors.append(offset)
    )

    herdres_gateway._poll_once_lanes(
        "manager",
        "token",
        timeout_seconds=0,
        request_id_key=REQUEST_ID_KEY,
        spool=spool,
    )

    assert spool.cursor("manager") == 42
    assert len(spool.rows()) == 1
    assert notices == [("77", lane_key("manager", "77"))]
    assert mirrors == [42]


def test_cursor_commit_survives_failure_before_legacy_mirror(tmp_path, monkeypatch) -> None:
    spool = IngressLaneSpool(tmp_path / "spool.db")
    spool.initialize_cursor("manager", 50)
    offsets_seen: list[int] = []
    update = _update(50, 77, "durable")
    monkeypatch.setattr(herdres_gateway.state, "load_state", _store)

    def updates(_token, offset, *, timeout_seconds):
        offsets_seen.append(offset)
        return [update] if offset == 50 else []

    monkeypatch.setattr(herdres_gateway, "get_updates", updates)
    monkeypatch.setattr(
        herdres_gateway,
        "_save_offset",
        lambda *_args: (_ for _ in ()).throw(OSError("simulated crash window")),
    )

    herdres_gateway._poll_once_lanes(
        "manager", "token", timeout_seconds=0, request_id_key=REQUEST_ID_KEY, spool=spool
    )
    herdres_gateway._poll_once_lanes(
        "manager", "token", timeout_seconds=0, request_id_key=REQUEST_ID_KEY, spool=spool
    )

    assert offsets_seen == [50, 51]
    assert spool.cursor("manager") == 51
    assert len(spool.rows()) == 1


def test_first_lane_start_migrates_legacy_receiver_cursor(tmp_path, monkeypatch) -> None:
    base = tmp_path / "gateway.offset"
    base.write_text("91", encoding="utf-8")
    spool = IngressLaneSpool(tmp_path / "spool.db")
    offsets: list[int] = []
    monkeypatch.setattr(herdres_gateway.config, "offset_path", lambda: base)
    monkeypatch.setattr(
        herdres_gateway,
        "get_updates",
        lambda _token, offset, *, timeout_seconds: offsets.append(offset) or [],
    )

    herdres_gateway._poll_once_lanes(
        "manager", "token", timeout_seconds=0, request_id_key=REQUEST_ID_KEY, spool=spool
    )

    assert offsets == [91]
    assert spool.cursor("manager") == 91


def test_terminal_ingress_cache_marks_refetched_update_done_without_dispatch(
    tmp_path, monkeypatch
) -> None:
    spool = IngressLaneSpool(tmp_path / "spool.db")
    spool.initialize_cursor("manager", 95)
    update = _update(95, 77, "already accepted")
    request_id = _request_id(update)
    store = _store()
    record, _created = ingress_requests.ensure_request_shell(
        store, request_id, now=10.0, retry_horizon=60, retention=120
    )
    request_json = ingress_requests.canonical_request_json(
        {
            "schema_version": 1,
            "action": "send_instruction",
            "request_id": request_id,
            "dry_run": False,
            "target": {"worker_id": "worker-a"},
            "instruction": {"text": "already accepted"},
        }
    )
    ingress_requests.attach_request_json(record, request_json, now=11.0)
    ingress_requests.mark_terminal(
        record,
        "terminal_accepted",
        now=12.0,
        reply="Sent to Tendwire worker.",
    )
    monkeypatch.setattr(herdres_gateway.state, "load_state", lambda: store)
    monkeypatch.setattr(
        herdres_gateway,
        "get_updates",
        lambda *_args, **_kwargs: [update],
    )
    monkeypatch.setattr(herdres_gateway, "_save_offset", lambda *_args: None)

    herdres_gateway._poll_once_lanes(
        "manager", "token", timeout_seconds=0, request_id_key=REQUEST_ID_KEY, spool=spool
    )

    rows = spool.rows()
    assert len(rows) == 1
    assert rows[0]["state"] == "done"
    assert rows[0]["notify_state"] == "cached"


def test_feature_flag_off_uses_legacy_synchronous_path(tmp_path, monkeypatch) -> None:
    monkeypatch.delenv("HERDRES_INBOUND_LANES", raising=False)
    handled: list[int] = []
    saved: list[int] = []
    monkeypatch.setattr(herdres_gateway, "_read_offset", lambda _key: 60)
    monkeypatch.setattr(
        herdres_gateway,
        "get_updates",
        lambda *_args, **_kwargs: [_update(60, 77, "legacy")],
    )
    monkeypatch.setattr(
        herdres_gateway,
        "handle_update",
        lambda update, *_args, **_kwargs: (
            handled.append(update["update_id"]) or herdres_gateway.CHECKPOINT_ADVANCE
        ),
    )
    monkeypatch.setattr(
        herdres_gateway, "_save_offset", lambda offset, _key: saved.append(offset)
    )
    monkeypatch.setattr(
        herdres_gateway,
        "IngressLaneSpool",
        lambda *_args, **_kwargs: pytest.fail("flag-off path opened the spool"),
    )

    herdres_gateway._poll_once(
        "manager", "token", timeout_seconds=0, request_id_key=REQUEST_ID_KEY
    )

    assert handled == [60]
    assert saved == [61]


def _kill_stage_child(
    spool_path: str,
    update: dict[str, object],
    stage: str,
    ready,
) -> None:
    spool = IngressLaneSpool(spool_path)
    if stage == "before_dispatch":
        _enqueue(spool, update, "77")
        ready.send("enqueued")
    else:
        item = spool.claim("killed-dispatcher", lease_seconds=120)
        assert item is not None
        checkpoint = herdres_gateway.handle_update(
            item.update,
            "token",
            receiver_id=item.receiver_kind,
            request_id_key=REQUEST_ID_KEY,
            bot_key="manager",
            ingress_first_seen_at=item.first_seen_at,
        )
        assert checkpoint == herdres_gateway.CHECKPOINT_ADVANCE
        ready.send("terminal-cached")
    time.sleep(60)


@pytest.mark.parametrize("stage", ["before_dispatch", "after_dispatch"])
def test_kill_9_restart_submits_each_request_exactly_once(
    stage, tmp_path, monkeypatch
) -> None:
    if "fork" not in multiprocessing.get_all_start_methods():
        pytest.skip("kill -9 regression requires fork")
    state_path = tmp_path / "state.json"
    _configured_state(state_path)
    monkeypatch.setenv("HERDR_TELEGRAM_TOPICS_STATE", str(state_path))
    monkeypatch.setenv("HERDRES_SOURCE_TOPIC_MODE", "worker")
    spool_path = tmp_path / "spool.db"
    spool = IngressLaneSpool(spool_path)
    update = _update(70 if stage == "before_dispatch" else 71, 77, stage)
    if stage == "after_dispatch":
        _enqueue(spool, update, "77")
    context = multiprocessing.get_context("fork")
    submits = context.Value("i", 0)

    class Client:
        def command_json(self, request_json):
            with submits.get_lock():
                submits.value += 1
            return _accepted_command_response(json.loads(request_json))

    class Telegram:
        def __init__(self, token):
            self.token = token

        def send_message(self, *_args, **_kwargs):
            return {"ok": True, "message_id": "1"}

    monkeypatch.setattr(herdres, "TendwireClient", Client)
    monkeypatch.setattr(
        herdres_gateway,
        "run_herdres_command",
        lambda payload: herdres.command_reply(payload),
    )
    monkeypatch.setattr(herdres_gateway, "TelegramClient", Telegram)
    parent, child = context.Pipe(duplex=False)
    process = context.Process(
        target=_kill_stage_child,
        args=(str(spool_path), update, stage, child),
    )
    process.start()
    assert parent.poll(5.0)
    assert parent.recv() in {"enqueued", "terminal-cached"}
    os.kill(process.pid, signal.SIGKILL)
    process.join(5.0)
    assert process.exitcode == -signal.SIGKILL
    dispatcher = herdres_gateway._InboundLaneDispatcher(
        IngressLaneSpool(spool_path),
        REQUEST_ID_KEY,
        workers=1,
        backoff_seconds=0.01,
        lease_seconds=1,
    )
    dispatcher.update_specs([("manager", "token", 0)])
    dispatcher.start()
    try:
        _wait_for(
            lambda: all(row["state"] == "done" for row in dispatcher.spool.rows())
        )
    finally:
        dispatcher.stop()

    assert submits.value == 1


def test_tendwire_submit_releases_state_lock_and_preserves_concurrent_write(
    tmp_path, monkeypatch
) -> None:
    state_path = tmp_path / "state.json"
    _configured_state(state_path)
    monkeypatch.setenv("HERDR_TELEGRAM_TOPICS_STATE", str(state_path))
    monkeypatch.setenv("HERDRES_SOURCE_TOPIC_MODE", "worker")
    entered = threading.Event()
    release = threading.Event()

    class Client:
        def command_json(self, request_json):
            entered.set()
            assert release.wait(2.0)
            return _accepted_command_response(json.loads(request_json))

    monkeypatch.setattr(herdres, "TendwireClient", Client)
    result: dict[str, object] = {}

    def submit() -> None:
        result.update(
            herdres.command_reply(
                {
                    "request_id": _request_id(_update(80, 77, "off lock")),
                    "topic_id": "77",
                    "message_id": "1080",
                    "text": "off lock",
                }
            )
        )

    thread = threading.Thread(target=submit)
    thread.start()
    assert entered.wait(1.0)
    acquired_at = time.monotonic()
    with state.state_lock():
        concurrent = state.load_state()
        concurrent["concurrent_write"] = True
        state.save_state(concurrent)
    assert time.monotonic() - acquired_at < 0.5
    release.set()
    thread.join(3.0)

    assert result["checkpoint"] == herdres_gateway.CHECKPOINT_ADVANCE
    final = state.load_state()
    assert final["concurrent_write"] is True
    assert final[ingress_requests.RECORDS_KEY][
        _request_id(_update(80, 77, "off lock"))
    ]["state"] == "terminal"


def test_lane_configuration_defaults_and_bounds() -> None:
    assert config.inbound_lanes_enabled({}) is False
    assert config.inbound_lanes_enabled({"HERDRES_INBOUND_LANES": "1"}) is True
    assert config.inbound_dispatch_workers({}) == 8
    assert config.inbound_dispatch_workers({"HERDRES_INBOUND_DISPATCH_WORKERS": "0"}) == 1
    assert config.inbound_lane_depth({}) == 32
    assert config.inbound_lane_depth({"HERDRES_INBOUND_LANE_DEPTH": "5000"}) == 4096
    assert config.inbound_lane_backoff_seconds({}) == 2.0
