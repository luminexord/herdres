"""Production-shaped regressions for pane/topic binding integrity."""

from __future__ import annotations

import inspect
from copy import deepcopy
from datetime import datetime, timedelta, timezone

import pytest

from herdres_connector import source_sync, state
from herdres_connector.source_sync import SyncRuntime, sync_once

from test_source_only import FakeTelegram, FakeTendwire, _store


STABLE_KEY = "wsk1_" + "d" * 64


@pytest.fixture(autouse=True)
def _source_worker_mode(monkeypatch):
    monkeypatch.setenv("HERDRES_TENDWIRE_MODE", "source")
    monkeypatch.setenv("HERDRES_SOURCE_TOPIC_MODE", "worker")
    monkeypatch.setenv("HERDRES_PINNED_STATUS", "0")
    monkeypatch.setenv("HERDR_TELEGRAM_TOPICS_STATUS_ICON", "0")
    monkeypatch.setenv("HERDR_TELEGRAM_TOPICS_RICH_MESSAGES", "0")


def _worker(
    worker_id: str = "claude-live",
    *,
    fingerprint: str = "fp-live",
) -> dict:
    return {
        "id": worker_id,
        "name": "Claude",
        "status": "working",
        "space_id": "workspace-1",
        "fingerprint": fingerprint,
        "last_seen_at": datetime.now(timezone.utc).isoformat(),
        "meta": {
            "agent": "claude",
            "label": "discovery-calls",
            "cwd": "/root/herdres",
            "foreground_cwd": "/root/herdres",
            "stable_key": STABLE_KEY,
            "stable_key_version": 1,
        },
    }


def _final(worker_id: str = "claude-live") -> dict:
    return {
        "id": "turn-next-final",
        "worker_id": worker_id,
        "space_id": "workspace-1",
        "complete": True,
        "user_text": "Finish the pane/topic repair.",
        "assistant_final_text": "Pane/topic binding repaired.",
    }


def _working(worker_id: str = "claude-live") -> dict:
    return {
        "id": "turn-working",
        "worker_id": worker_id,
        "space_id": "workspace-1",
        "assistant_stream_text": "Checking the pane/topic repair.",
        "complete": False,
    }


def _entry(
    store: dict,
    *,
    worker_id: str,
    fingerprint: str,
    topic_id: str | None,
) -> tuple[str, dict]:
    key, entry, _created = state.upsert_worker_entry(
        store,
        _worker(worker_id, fingerprint=fingerprint),
        topic_id=topic_id or "",
    )
    return key, entry


def _duplicate_entry(
    store: dict,
    source: dict,
    *,
    key: str,
    fingerprint: str,
    topic_id: str | None,
) -> tuple[str, dict]:
    duplicate = deepcopy(source)
    duplicate["tendwire_fingerprint"] = fingerprint
    if topic_id is None:
        duplicate.pop("topic_id", None)
    else:
        duplicate["topic_id"] = topic_id
    store["panes"][key] = duplicate
    return key, duplicate


class DeletedTopicTelegram(FakeTelegram):
    def __init__(self, dead_topic_id: str):
        super().__init__()
        self.dead_topic_id = dead_topic_id
        self.failed_topic_sends = 0

    def send_message(self, chat_id, html, **kwargs):
        if str(kwargs.get("thread_id") or "") == self.dead_topic_id:
            self.failed_topic_sends += 1
            return {
                "ok": False,
                "error": "Bad Request: message thread not found",
            }
        return super().send_message(chat_id, html, **kwargs)


class DeletedWorkingTopicTelegram(DeletedTopicTelegram):
    def __init__(self, dead_topic_id: str, stale_message_id: str):
        super().__init__(dead_topic_id)
        self.stale_message_id = stale_message_id
        self.failed_working_edits = 0

    def edit_message(self, chat_id, message_id, html):
        if str(message_id) == self.stale_message_id:
            self.failed_working_edits += 1
            return {
                "ok": False,
                "kind": "not_found",
                "error": "Bad Request: message to edit not found",
            }
        return super().edit_message(chat_id, message_id, html)


class MissingWorkingCardTelegram(FakeTelegram):
    def __init__(
        self,
        stale_message_id: str,
        *,
        kind: str = "not_found",
        error: str = "Bad Request: message to edit not found",
    ):
        super().__init__()
        self.stale_message_id = stale_message_id
        self.kind = kind
        self.error = error
        self.failed_working_edits = 0

    def edit_message(self, chat_id, message_id, html):
        if str(message_id) == self.stale_message_id:
            self.failed_working_edits += 1
            return {
                "ok": False,
                "kind": self.kind,
                "error": self.error,
            }
        return super().edit_message(chat_id, message_id, html)


class CleanupDeletedTopicTelegram(FakeTelegram):
    def __init__(self, *, already_missing: bool):
        super().__init__()
        self.already_missing = already_missing

    def delete_topic(self, chat_id, thread_id):
        result = super().delete_topic(chat_id, thread_id)
        if self.already_missing:
            return {
                "ok": False,
                "error": "Bad Request: message thread not found",
            }
        return result


class MissingRenameTopicTelegram(FakeTelegram):
    def __init__(self, dead_topic_id: str):
        super().__init__()
        self.dead_topic_id = dead_topic_id
        self.missing_renames = 0

    def rename_topic(self, chat_id, thread_id, name):
        if str(thread_id) == self.dead_topic_id:
            self.missing_renames += 1
            return {
                "ok": False,
                "error": "Bad Request: message thread not found",
            }
        return super().rename_topic(chat_id, thread_id, name)


class MissingIconTopicTelegram(FakeTelegram):
    def __init__(self, dead_topic_id: str):
        super().__init__()
        self.dead_topic_id = dead_topic_id
        self.missing_icon_edits = 0

    def edit_topic_icon(self, chat_id, thread_id, emoji_id):
        if str(thread_id) == self.dead_topic_id:
            self.missing_icon_edits += 1
            return {
                "ok": False,
                "error": "Bad Request: message thread not found",
            }
        return super().edit_topic_icon(
            chat_id, thread_id, emoji_id
        )


class MissingRetiredTopicTelegram(FakeTelegram):
    def __init__(self, dead_topic_id: str, path: str):
        super().__init__()
        self.dead_topic_id = dead_topic_id
        self.path = path
        self.missing_operations = 0

    def send_message(self, chat_id, html, **kwargs):
        if (
            self.path == "notice"
            and str(kwargs.get("thread_id") or "")
            == self.dead_topic_id
        ):
            self.missing_operations += 1
            return {
                "ok": False,
                "error": "Bad Request: message thread not found",
            }
        return super().send_message(chat_id, html, **kwargs)

    def rename_topic(self, chat_id, thread_id, name):
        if (
            self.path == "rename"
            and str(thread_id) == self.dead_topic_id
        ):
            self.missing_operations += 1
            return {
                "ok": False,
                "error": "Bad Request: message thread not found",
            }
        return super().rename_topic(chat_id, thread_id, name)


def _current_topic_refs(store: dict, topic_id: str) -> list[dict]:
    return [
        entry
        for entry in (
            list(state.source_worker_entries(store).values())
            + list(state.source_space_entries(store).values())
        )
        if str(entry.get("topic_id") or "") == topic_id
    ]


def test_provider_gone_repair_requires_explicit_keyword_topic_id():
    parameter = inspect.signature(
        source_sync._repair_provider_gone_topic
    ).parameters["topic_id"]

    assert parameter.kind is inspect.Parameter.KEYWORD_ONLY
    assert parameter.default is inspect.Parameter.empty
    with pytest.raises(TypeError):
        source_sync._repair_provider_gone_topic(
            {}, None, {"kind": "topic_not_found"}
        )




def test_missing_live_rename_tombstones_aliases_before_second_pass():
    store = _store()
    dead_topic_id = "15007"
    _key, entry = _entry(
        store,
        worker_id="claude-live",
        fingerprint="fp-live",
        topic_id=dead_topic_id,
    )
    entry["topic_name"] = "old-label"
    state.upsert_space_entry(
        store,
        {
            "id": "stale-alias",
            "name": "alias-label",
            "status": "active",
            "fingerprint": "alias-fp",
        },
        topic_id=dead_topic_id,
    )
    telegram = MissingRenameTopicTelegram(dead_topic_id)
    runtime = SyncRuntime(
        FakeTendwire(
            workers=[_worker()],
            turns={"turns": []},
            spaces=[],
        ),
        telegram,
        with_outbox=False,
    )

    sync_once(store, runtime)
    replacement_topic_id = entry["topic_id"]

    assert telegram.missing_renames == 1
    assert replacement_topic_id != dead_topic_id
    assert store["telegram_dead_topic_ids"] == [dead_topic_id]
    assert _current_topic_refs(store, dead_topic_id) == []
    assert telegram.topics == ["discovery-calls"]
    assert entry["last_topic_recovery"]["replacement_topic_id"] == (
        replacement_topic_id
    )

    alias_worker = _worker(
        "alias-worker", fingerprint="alias-worker-fp"
    )
    alias_worker["meta"]["label"] = "alias-label"
    alias_worker["meta"]["stable_key"] = "wsk1_" + "f" * 64
    runtime.tendwire = FakeTendwire(
        workers=[_worker(), alias_worker],
        turns={"turns": []},
        spaces=[],
    )

    sync_once(store, runtime)

    assert entry["topic_id"] == replacement_topic_id
    assert _current_topic_refs(store, dead_topic_id) == []
    assert all(
        worker.get("topic_id") != dead_topic_id
        for worker in state.source_worker_entries(store).values()
    )
    assert telegram.topics == ["discovery-calls", "alias-label"]


def test_live_rename_tombstones_requested_topic_after_concurrent_rebind(
    tmp_path, monkeypatch
):
    state_path = tmp_path / "state.json"
    monkeypatch.setenv(
        "HERDR_TELEGRAM_TOPICS_STATE", str(state_path)
    )
    requested_topic_id = "15007"
    rebound_topic_id = "16000"
    store = _store()
    _key, entry = _entry(
        store,
        worker_id="claude-live",
        fingerprint="fp-live",
        topic_id=requested_topic_id,
    )
    entry["topic_name"] = "old-label"
    state.save_state(store, state_path)

    class ConcurrentRebindRenameTelegram(FakeTelegram):
        def __init__(self):
            super().__init__()
            self.requested_topic_ids = []

        def rename_topic(self, chat_id, thread_id, name):
            self.requested_topic_ids.append(str(thread_id))
            concurrent = state.load_state(state_path)
            _entry_key, rebound = (
                state.find_worker_entry_by_stable_key(
                    concurrent, STABLE_KEY
                )
            )
            assert rebound is not None
            rebound["topic_id"] = rebound_topic_id
            concurrent["spaces"]["space:concurrent-old-alias"] = {
                "source": "tendwire",
                "entry_type": "space",
                "tendwire_space_id": "concurrent-old-alias",
                "topic_id": requested_topic_id,
                "topic_name": "Concurrent old alias",
            }
            state.save_state(concurrent, state_path)
            return {
                "ok": False,
                "kind": "topic_not_found",
                "error": "Bad Request: message thread not found",
            }

    telegram = ConcurrentRebindRenameTelegram()
    runtime = SyncRuntime(
        FakeTendwire(
            workers=[_worker()],
            turns={"turns": []},
            spaces=[],
        ),
        telegram,
        with_outbox=False,
    )

    with state.state_lock(path=state_path):
        current = state.load_state(state_path)
        sync_once(current, runtime)

    _entry_key, rebound = state.find_worker_entry_by_stable_key(
        current, STABLE_KEY
    )
    assert telegram.requested_topic_ids == [requested_topic_id]
    assert rebound is not None
    assert rebound["topic_id"] == rebound_topic_id
    assert state.topic_id_is_tombstoned(
        current, requested_topic_id
    )
    assert not state.topic_id_is_tombstoned(
        current, rebound_topic_id
    )
    assert _current_topic_refs(current, requested_topic_id) == []
    assert _current_topic_refs(current, rebound_topic_id) == [
        rebound
    ]
    assert telegram.topics == []


def test_concurrent_rebind_during_create_checkpoints_accepted_topic(
    tmp_path, monkeypatch
):
    state_path = tmp_path / "state.json"
    monkeypatch.setenv(
        "HERDR_TELEGRAM_TOPICS_STATE", str(state_path)
    )
    store = _store()
    store["panes"] = {}
    state.save_state(store, state_path)

    class RebindDuringCreateTelegram(FakeTelegram):
        def create_topic(self, chat_id, name, icon_color=None):
            created = super().create_topic(
                chat_id, name, icon_color=icon_color
            )
            concurrent = state.load_state(state_path)
            entry = next(
                iter(state.source_worker_entries(concurrent).values())
            )
            entry["topic_id"] = "16000"
            state.save_state(concurrent, state_path)
            return created

    telegram = RebindDuringCreateTelegram()
    runtime = SyncRuntime(
        FakeTendwire(
            workers=[_worker()],
            turns={"turns": []},
            spaces=[],
        ),
        telegram,
        with_outbox=False,
    )

    with state.state_lock(path=state_path):
        current = state.load_state(state_path)
        sync_once(current, runtime)

    orphaned = state.orphaned_created_topics(current)
    entry = next(iter(state.source_worker_entries(current).values()))
    assert entry["topic_id"] == "16000"
    assert [item["topic_id"] for item in orphaned] == ["77"]
    assert "77" not in state.dead_topic_ids(current)

    with state.state_lock(path=state_path):
        current = state.load_state(state_path)
        sync_once(current, runtime)

    assert telegram.deleted_topics == ["77"]
    assert state.orphaned_created_topics(current) == []
    assert state.topic_id_is_tombstoned(current, "77")
    assert next(iter(state.source_worker_entries(current).values()))[
        "topic_id"
    ] == "16000"


def test_accepted_topic_create_survives_crash_before_state_barrier(
    tmp_path, monkeypatch
):
    state_path = tmp_path / "state.json"
    monkeypatch.setenv(
        "HERDR_TELEGRAM_TOPICS_STATE", str(state_path)
    )
    store = _store()
    store["panes"] = {}
    state.save_state(store, state_path)

    class SequencedCreateTelegram(FakeTelegram):
        def __init__(self):
            super().__init__()
            self.created_topic_ids: list[str] = []

        def create_topic(self, chat_id, name, icon_color=None):
            topic_id = str(700 + len(self.created_topic_ids))
            self.created_topic_ids.append(topic_id)
            self.topics.append(name)
            return {"ok": True, "topic_id": topic_id}

    telegram = SequencedCreateTelegram()
    tendwire = FakeTendwire(
        workers=[_worker()],
        turns={"turns": []},
        spaces=[],
    )

    def crash_after_accept():
        raise RuntimeError("crash after accepted topic")

    with pytest.raises(
        RuntimeError, match="crash after accepted topic"
    ):
        with state.state_lock(path=state_path):
            current = state.load_state(state_path)
            sync_once(
                current,
                SyncRuntime(
                    tendwire,
                    telegram,
                    with_outbox=False,
                    after_provider_accept=crash_after_accept,
                ),
            )

    durable_before_restart = state.load_state(state_path)
    entry = next(
        iter(state.source_worker_entries(durable_before_restart).values())
    )
    assert not entry.get("topic_id")
    assert telegram.created_topic_ids == ["700"]
    assert state.accepted_notification_journal_path(
        state_path
    ).exists()

    with state.state_lock(path=state_path):
        current = state.load_state(state_path)
        assert len(
            current["telegram"]["accepted_created_topics"]
        ) == 1
        sync_once(
            current,
            SyncRuntime(
                tendwire,
                telegram,
                with_outbox=False,
            ),
        )

    entry = next(iter(state.source_worker_entries(current).values()))
    assert entry["topic_id"] == "700"
    assert telegram.created_topic_ids == ["700"]
    assert not current["telegram"].get("accepted_created_topics")
    assert not state.accepted_notification_journal_path(
        state_path
    ).exists()
    persisted = state.load_state(state_path)
    persisted_entry = next(
        iter(state.source_worker_entries(persisted).values())
    )
    assert persisted_entry["topic_id"] == "700"
    assert not persisted["telegram"].get("accepted_created_topics")


def test_missing_topic_icon_tombstones_aliases_and_remints_once(
    monkeypatch,
):
    monkeypatch.setenv("HERDR_TELEGRAM_TOPICS_STATUS_ICON", "1")
    store = _store()
    dead_topic_id = "15007"
    _key, entry = _entry(
        store,
        worker_id="claude-live",
        fingerprint="fp-live",
        topic_id=dead_topic_id,
    )
    state.upsert_space_entry(
        store,
        {
            "id": "stale-icon-alias",
            "name": "icon-alias",
            "status": "active",
            "fingerprint": "icon-alias-fp",
        },
        topic_id=dead_topic_id,
    )
    telegram = MissingIconTopicTelegram(dead_topic_id)
    runtime = SyncRuntime(
        FakeTendwire(
            workers=[_worker()],
            turns={"turns": []},
            spaces=[],
        ),
        telegram,
        with_outbox=False,
    )

    sync_once(store, runtime)

    assert telegram.missing_icon_edits == 1
    assert store["telegram_dead_topic_ids"] == [dead_topic_id]
    assert _current_topic_refs(store, dead_topic_id) == []
    assert telegram.topics == []

    sync_once(store, runtime)
    replacement_topic_id = entry["topic_id"]
    sync_once(store, runtime)

    assert replacement_topic_id != dead_topic_id
    assert telegram.topics == ["discovery-calls"]
    assert telegram.missing_icon_edits == 1
    assert _current_topic_refs(store, dead_topic_id) == []
    assert entry["last_topic_recovery"]["replacement_topic_id"] == (
        replacement_topic_id
    )


@pytest.mark.parametrize("path", ["notice", "rename"])
def test_missing_retired_topic_operation_tombstones_every_alias(path):
    store = _store()
    dead_topic_id = "15007"
    retired = {
        "source": "tendwire",
        "entry_type": "worker",
        "topic_id": dead_topic_id,
        "topic_name": "📁 retired pane",
        "routing_retired": True,
        "routing_retired_reason": "stable_key_duplicate_consolidated",
        "status": "closed",
        f"retired_topic_{path}_pending": True,
        f"retired_topic_{path}_error": "old error",
    }
    store["panes"]["worker:retired-provider-missing"] = retired
    state.upsert_space_entry(
        store,
        {
            "id": "retired-alias",
            "name": "retired-alias",
            "status": "active",
            "fingerprint": "retired-alias-fp",
        },
        topic_id=dead_topic_id,
    )
    telegram = MissingRetiredTopicTelegram(dead_topic_id, path)
    runtime = SyncRuntime(
        FakeTendwire(
            workers=[],
            turns={"turns": []},
            spaces=[],
        ),
        telegram,
        with_outbox=False,
    )

    sync_once(store, runtime)

    assert telegram.missing_operations == 1
    assert store["telegram_dead_topic_ids"] == [dead_topic_id]
    assert _current_topic_refs(store, dead_topic_id) == []
    assert "topic_id" not in retired
    assert retired["retired_topic_id"] == dead_topic_id
    assert retired["retired_topic_missing"] is True
    assert not any(
        field in retired
        for field in (
            "retired_topic_notice_pending",
            "retired_topic_notice_error",
            "retired_topic_rename_pending",
            "retired_topic_rename_error",
            "retired_topic_close_pending",
            "retired_topic_close_error",
        )
    )

    sync_once(store, runtime)

    assert telegram.missing_operations == 1
    assert telegram.topics == []


@pytest.mark.parametrize("already_missing", [False, True])
def test_cleanup_delete_tombstones_shared_live_space_and_retired_aliases(
    monkeypatch, already_missing
):
    monkeypatch.setenv("HERDRES_SOURCE_TOPIC_MODE", "space")
    store = _store()
    dead_topic_id = "15007"
    live_space = {
        "id": "workspace-1",
        "name": "discovery-calls",
        "status": "active",
        "fingerprint": "live-space-fp",
    }
    _live_key, live_entry, _created = state.upsert_space_entry(
        store,
        live_space,
        topic_id=dead_topic_id,
    )
    _stale_key, stale_entry, _created = state.upsert_space_entry(
        store,
        {
            "id": "stale-council",
            "name": "gitmoot · local-as",
            "status": "active",
            "fingerprint": "stale-space-fp",
        },
        topic_id=dead_topic_id,
    )
    stale_entry["stale_space_topic"] = True
    rebound_space = {
        "id": "rebound-space",
        "name": "active-rebound-space",
        "status": "active",
        "fingerprint": "rebound-space-fp",
    }
    _rebound_space_key, _rebound_space_entry, _created = (
        state.upsert_space_entry(
            store, rebound_space, topic_id="16000"
        )
    )
    retired = {
        "source": "tendwire",
        "entry_type": "worker",
        "topic_id": dead_topic_id,
        "topic_name": "📁 retired duplicate",
        "routing_retired": True,
        "routing_retired_reason": "stable_key_duplicate_consolidated",
        "status": "closed",
        "retired_topic_notice_pending": True,
        "retired_topic_notice_error": "old notice error",
        "retired_topic_rename_pending": True,
        "retired_topic_rename_error": "old rename error",
        "retired_topic_close_pending": True,
        "retired_topic_close_error": "old close error",
    }
    store["panes"]["worker:retired-alias"] = retired
    rebound = {
        "source": "tendwire",
        "entry_type": "worker",
        "topic_id": "16000",
        "deleted_topic_id": dead_topic_id,
        "topic_name": "Council · rebound",
        "worker_name": "gm-local-as",
        "status": "closed",
        "tendwire_raw_status": "closed",
    }
    store["panes"]["worker:rebound-council"] = rebound
    rebound_live_worker = _worker(
        "rebound-live", fingerprint="rebound-live-fp"
    )
    rebound_live_worker["space_id"] = "rebound-space"
    rebound_live_worker["meta"]["stable_key"] = "wsk1_" + "e" * 64
    telegram = CleanupDeletedTopicTelegram(
        already_missing=already_missing
    )
    runtime = SyncRuntime(
        FakeTendwire(
            workers=[_worker(), rebound_live_worker],
            turns={"turns": []},
            spaces=[live_space, rebound_space],
        ),
        telegram,
        with_outbox=False,
    )

    first = sync_once(store, runtime)

    assert telegram.deleted_topics == [dead_topic_id]
    assert store["telegram_dead_topic_ids"] == [dead_topic_id]
    assert store["telegram_deleted_topics"] == [
        {
            "topic_id": dead_topic_id,
            "name": "gitmoot · local-as",
            "reason": "done_council_space_topic",
        }
    ]
    assert store["panes"]["worker:rebound-council"] is rebound
    assert rebound["topic_id"] == "16000"
    assert rebound["deleted_topic_id"] == dead_topic_id
    assert "topic_id" not in live_entry
    assert "topic_id" not in retired
    assert retired["retired_topic_id"] == dead_topic_id
    assert retired["retired_topic_missing"] is True
    assert not any(
        field in retired
        for field in (
            "retired_topic_notice_pending",
            "retired_topic_notice_error",
            "retired_topic_rename_pending",
            "retired_topic_rename_error",
            "retired_topic_close_pending",
            "retired_topic_close_error",
        )
    )
    assert all(
        entry.get("tendwire_space_id") != "stale-council"
        for entry in state.source_space_entries(store).values()
    )

    second = sync_once(store, runtime)
    replacement_topic_id = live_entry["topic_id"]
    third = sync_once(store, runtime)

    assert first["topic_cleanup"]["failed"] == 0
    assert second["topic_cleanup"]["deleted"] == 0
    assert third["topic_cleanup"]["deleted"] == 0
    assert replacement_topic_id != dead_topic_id
    assert telegram.topics == ["discovery-calls"]
    assert telegram.deleted_topics == [dead_topic_id]
    assert store["telegram_dead_topic_ids"] == [dead_topic_id]
    assert len(store["telegram_deleted_topics"]) == 1


def test_deleted_topic_defers_durable_final_until_remint_then_acks_once():
    from test_turn_final_delivery import TurnFinalTendwire, _runtime, _turn_row

    row = _turn_row(
        "turn-durable-remint",
        "twrev1.durable_remint",
        "Durable final delivered after remint.",
        user="Recover the deleted topic.",
    )
    tendwire = TurnFinalTendwire(row)
    store = _store()
    worker = tendwire.snapshot()["workers"][0]
    _key, entry, _created = state.upsert_worker_entry(
        store, worker, topic_id="15007"
    )
    telegram = DeletedTopicTelegram("15007")

    first = sync_once(store, _runtime(tendwire, telegram))

    assert first["tendwire_turn_final"]["deferred"] == 1
    assert len(tendwire.defer_calls) == 1
    assert tendwire.defer_calls[0][1] == "transient_delivery"
    assert tendwire.defer_delay_seconds == [1]
    assert tendwire.ack_calls == []
    assert "topic_id" not in entry

    second = sync_once(store, _runtime(tendwire, telegram))
    replacement_topic_id = entry["topic_id"]

    assert second["tendwire_turn_final"]["acked"] == 1
    assert len(tendwire.ack_calls) == 1
    assert replacement_topic_id != "15007"
    assert (
        len(
            [
                sent
                for sent in telegram.sent
                if sent[2].get("thread_id") == replacement_topic_id
                and "Durable final delivered after remint." in sent[1]
            ]
        )
        == 1
    )

    third = sync_once(store, _runtime(tendwire, telegram))

    assert third["tendwire_turn_final"]["polled"] == 0
    assert len(tendwire.ack_calls) == 1


def test_one_topic_duplicate_live_claim_retires_loser_and_restores_route():
    store = _store()
    survivor_key, survivor = _entry(
        store,
        worker_id="claude-live",
        fingerprint="fp-topic",
        topic_id="16756",
    )
    loser_key, loser = _duplicate_entry(
        store,
        survivor,
        key="worker:claude-live:topicless-duplicate",
        fingerprint="fp-topicless",
        topic_id=None,
    )
    for key, entry in ((survivor_key, survivor), (loser_key, loser)):
        entry["tendwire_stable_key"] = STABLE_KEY
        entry["tendwire_stable_key_version"] = 1
        state.quarantine_worker_entry(
            store, key, reason="preflight_stable_key_conflict"
        )
    worker = _worker("claude-live", fingerprint="fp-topic")
    telegram = FakeTelegram()

    result = sync_once(
        store,
        SyncRuntime(
            FakeTendwire(
                workers=[worker],
                turns={"turns": []},
                spaces=[],
            ),
            telegram,
            with_outbox=False,
        ),
    )

    assert result["feed_sent"] == 0
    assert survivor["topic_id"] == "16756"
    assert not state.entry_is_quarantined(survivor)
    assert state.worker_entry_is_uniquely_routable(
        store, survivor_key, survivor
    )
    assert state.entry_is_retired(loser)
    assert loser["routing_retired_reason"] == "duplicate_live_claim"
    assert state.entry_stable_identity(loser) is None
    assert state.find_worker_entry_by_stable_key(store, STABLE_KEY) == (
        survivor_key,
        survivor,
    )


def test_recent_distinct_topic_claims_remain_fail_closed():
    store = _store()
    first_key, first = _entry(
        store,
        worker_id="claude-live",
        fingerprint="fp-a",
        topic_id="16756",
    )
    second_key, second = _duplicate_entry(
        store,
        first,
        key="worker:claude-live:second-topic",
        fingerprint="fp-b",
        topic_id="16910",
    )
    recent = datetime.now(timezone.utc).isoformat()
    for key, entry in ((first_key, first), (second_key, second)):
        entry["tendwire_stable_key"] = STABLE_KEY
        entry["tendwire_stable_key_version"] = 1
        entry["tendwire_last_seen_at"] = recent
        state.quarantine_worker_entry(
            store, key, reason="preflight_stable_key_conflict"
        )
    telegram = FakeTelegram()

    result = sync_once(
        store,
        SyncRuntime(
            FakeTendwire(
                workers=[_worker("claude-live", fingerprint="fp-a")],
                turns={"turns": [_final("claude-live")]},
                spaces=[],
            ),
            telegram,
            with_outbox=False,
        ),
    )

    assert result["feed_sent"] == 0
    assert not state.entry_is_retired(first)
    assert not state.entry_is_retired(second)
    assert state.entry_is_quarantined(first)
    assert state.entry_is_quarantined(second)
    assert state.find_worker_entry_by_stable_key(store, STABLE_KEY) == (
        None,
        None,
    )
    assert telegram.sent == []
    assert telegram.topics == []


@pytest.mark.parametrize("last_seen_at", [None, "", "not-a-timestamp"])
def test_unknown_distinct_live_topic_claims_fail_closed_across_passes(
    last_seen_at,
):
    store = _store()
    first_key, first = _entry(
        store,
        worker_id="claude-live",
        fingerprint="fp-a",
        topic_id="16756",
    )
    second_key, second = _duplicate_entry(
        store,
        first,
        key="worker:claude-live:second-topic",
        fingerprint="fp-b",
        topic_id="16910",
    )
    for key, entry in ((first_key, first), (second_key, second)):
        entry["tendwire_stable_key"] = STABLE_KEY
        entry["tendwire_stable_key_version"] = 1
        if last_seen_at is None:
            entry.pop("tendwire_last_seen_at", None)
        else:
            entry["tendwire_last_seen_at"] = last_seen_at
        state.quarantine_worker_entry(
            store, key, reason="preflight_stable_key_conflict"
        )
    runtime = SyncRuntime(
        FakeTendwire(
            workers=[_worker("claude-live", fingerprint="fp-a")],
            turns={"turns": [_final("claude-live")]},
            spaces=[],
        ),
        FakeTelegram(),
        with_outbox=False,
    )

    first_result = sync_once(store, runtime)
    after_first = deepcopy(store)
    second_result = sync_once(store, runtime)

    assert first_result["feed_sent"] == second_result["feed_sent"] == 0
    assert store == after_first
    assert not state.entry_is_retired(first)
    assert not state.entry_is_retired(second)
    assert state.entry_is_quarantined(first)
    assert state.entry_is_quarantined(second)
    assert runtime.telegram.sent == []
    assert runtime.telegram.topics == []


def test_future_dated_distinct_live_topic_claims_fail_closed():
    store = _store()
    first_key, first = _entry(
        store,
        worker_id="claude-live",
        fingerprint="fp-a",
        topic_id="16756",
    )
    second_key, second = _duplicate_entry(
        store,
        first,
        key="worker:claude-live:second-topic",
        fingerprint="fp-b",
        topic_id="16910",
    )
    future = (datetime.now(timezone.utc) + timedelta(days=1)).isoformat()
    for key, entry in ((first_key, first), (second_key, second)):
        entry["tendwire_stable_key"] = STABLE_KEY
        entry["tendwire_stable_key_version"] = 1
        entry["tendwire_last_seen_at"] = future
        state.quarantine_worker_entry(
            store, key, reason="preflight_stable_key_conflict"
        )
    telegram = FakeTelegram()

    result = sync_once(
        store,
        SyncRuntime(
            FakeTendwire(
                workers=[_worker("claude-live", fingerprint="fp-a")],
                turns={"turns": [_final("claude-live")]},
                spaces=[],
            ),
            telegram,
            with_outbox=False,
        ),
    )

    assert result["feed_sent"] == 0
    assert not state.entry_is_retired(first)
    assert not state.entry_is_retired(second)
    assert telegram.sent == []
    assert telegram.topics == []


def test_retired_distinct_topic_duplicate_with_unknown_liveness_consolidates():
    store = _store()
    first_key, first = _entry(
        store,
        worker_id="claude-live",
        fingerprint="fp-live",
        topic_id="16756",
    )
    second_key, second = _duplicate_entry(
        store,
        first,
        key="worker:claude-old-b:second-topic",
        fingerprint="fp-b",
        topic_id="16910",
    )
    for entry in (first, second):
        entry["tendwire_stable_key"] = STABLE_KEY
        entry["tendwire_stable_key_version"] = 1
        entry["tendwire_last_seen_at"] = ""
    second["routing_retired"] = True
    second["routing_retired_reason"] = "historical_duplicate"
    second["status"] = "closed"
    telegram = FakeTelegram()

    sync_once(
        store,
        SyncRuntime(
            FakeTendwire(
                workers=[_worker("claude-live", fingerprint="fp-live")],
                turns={"turns": []},
                spaces=[],
            ),
            telegram,
            with_outbox=False,
        ),
    )

    assert state.worker_entry_is_uniquely_routable(store, first_key, first)
    assert set(state.source_worker_entries(store)) == {first_key, second_key}
    assert state.entry_is_retired(second)
    assert second["routing_retired_reason"] == "historical_duplicate"
    assert state.entry_stable_identity(second) is None








def test_healthy_noop_sync_pass_does_not_write_state():
    store = _store()
    _entry(
        store,
        worker_id="claude-live",
        fingerprint="fp-live",
        topic_id="16756",
    )
    runtime = SyncRuntime(
        FakeTendwire(
            workers=[_worker()],
            turns={"turns": []},
            spaces=[],
        ),
        FakeTelegram(),
        with_outbox=False,
    )
    sync_once(store, runtime)
    sync_once(store, runtime)
    before = deepcopy(store)

    result = sync_once(store, runtime)

    assert result["changed"] is False
    assert store == before


def test_offlock_guard_rejects_replaced_working_message_id():
    store = _store()
    _key, entry = _entry(
        store,
        worker_id="claude-live",
        fingerprint="fp-live",
        topic_id="15007",
    )
    entry["last_stream_message_id"] = "400"
    operation = source_sync._capture_entry_operation(
        store,
        entry,
        topic_id="15007",
        message_id="400",
        observe=("last_stream_message_id",),
    )

    entry["last_stream_message_id"] = "401"

    resolution = source_sync._compare_and_apply_entry_operation(
        store, operation
    )
    assert resolution.disposition == "reconcile"
    assert resolution.entry is None


def test_offlock_guard_rejects_plan_revision_change():
    store = _store()
    _key, entry = _entry(
        store,
        worker_id="claude-live",
        fingerprint="fp-live",
        topic_id="15007",
    )
    entry["pending_plan_token"] = "twplan1.guard"
    entry["pending_content_revision"] = "twrev1.before"
    operation = source_sync._capture_entry_operation(
        store,
        entry,
        topic_id="15007",
        plan_token="twplan1.guard",
        revision="twrev1.before",
    )

    entry["pending_content_revision"] = "twrev1.after"

    resolution = source_sync._compare_and_apply_entry_operation(
        store, operation
    )
    assert resolution.disposition == "reconcile"
    assert resolution.entry is None


def test_offlock_guard_rejects_owner_generation_change_on_same_topic():
    store = _store()
    _key, entry = _entry(
        store,
        worker_id="claude-live",
        fingerprint="fp-live",
        topic_id="15007",
    )
    operation = source_sync._capture_entry_operation(
        store, entry, topic_id="15007"
    )

    entry["tendwire_fingerprint"] = "fp-rebound"

    resolution = source_sync._compare_and_apply_entry_operation(
        store, operation
    )
    assert entry["topic_id"] == "15007"
    assert resolution.disposition == "reconcile"
    assert resolution.entry is None
