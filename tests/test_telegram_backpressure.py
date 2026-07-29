from __future__ import annotations

import io
import json
import urllib.error

import pytest

from herdres_connector import source_sync
from herdres_connector import telegram_delivery
from herdres_connector.telegram_delivery import RateLimited, TelegramClient
from test_source_only import FakeTelegram, FakeTendwire, _store


def _worker(worker_id: str, space_id: str) -> dict[str, str]:
    return {
        "id": worker_id,
        "name": worker_id,
        "status": "working",
        "space_id": space_id,
        "fingerprint": f"fp-{worker_id}",
    }


def _space(space_id: str) -> dict[str, str]:
    return {
        "id": space_id,
        "name": space_id,
        "status": "active",
        "fingerprint": f"fp-{space_id}",
    }


def test_rate_limited_call_waits_exact_hint_and_pass_completes_later_work(
    monkeypatch,
):
    monkeypatch.setenv("HERDRES_TENDWIRE_MODE", "source")
    monkeypatch.setenv("HERDRES_PINNED_STATUS", "0")
    monkeypatch.setenv("HERDRES_SOURCE_TOPIC_MODE", "space")
    clock = [100.0]
    monkeypatch.setattr(source_sync.time, "monotonic", lambda: clock[0])
    monkeypatch.setattr(
        source_sync.time,
        "sleep",
        lambda seconds: clock.__setitem__(0, clock[0] + seconds),
    )

    class RateLimitedFirstIcon(FakeTelegram):
        def __init__(self):
            super().__init__()
            self.icon_attempts = []

        def edit_topic_icon(self, chat_id, thread_id, emoji_id):
            self.icon_attempts.append(
                (str(thread_id), clock[0])
            )
            if len(self.icon_attempts) == 1:
                raise RateLimited(
                    7,
                    "Too Many Requests: retry after 7",
                )
            return super().edit_topic_icon(
                chat_id, thread_id, emoji_id
            )

    telegram = RateLimitedFirstIcon()
    tendwire = FakeTendwire(
        workers=[
            _worker("worker-one", "space-one"),
            _worker("worker-two", "space-two"),
        ],
        spaces=[_space("space-one"), _space("space-two")],
        turns={"turns": []},
    )
    store = _store()

    result = source_sync.sync_once(
        store,
        source_sync.SyncRuntime(
            tendwire, telegram, with_outbox=False
        ),
    )

    assert result["ok"] is True
    assert result["icon_updated"] == 2
    assert len(telegram.icon_edits) == 2
    assert telegram.icon_attempts[:2] == [
        ("77", 100.0),
        ("77", 107.0),
    ]
    assert telegram.icon_attempts[-1][0] == "78"
    event = result["telegram_backpressure"]["events"][0]
    assert event["method"] == "editForumTopic"
    assert event["retry_after"] == 7
    assert event["observed_wait_seconds"] == 7.0
    assert event["outcome"] == "recovered"
    assert (
        store["telegram"]["rate_limit_backpressure"]["last"]
        == event
    )


def test_rate_limit_retry_is_bounded_and_reports_exhaustion(
    monkeypatch,
):
    monkeypatch.setenv("HERDRES_TENDWIRE_MODE", "source")
    monkeypatch.setenv("HERDRES_PINNED_STATUS", "0")
    monkeypatch.setenv("HERDRES_SOURCE_TOPIC_MODE", "space")
    clock = [20.0]
    monkeypatch.setattr(source_sync.time, "monotonic", lambda: clock[0])
    monkeypatch.setattr(
        source_sync.time,
        "sleep",
        lambda seconds: clock.__setitem__(0, clock[0] + seconds),
    )

    class LimitedFirstTopic(FakeTelegram):
        def __init__(self):
            super().__init__()
            self.icon_attempts = []

        def edit_topic_icon(self, chat_id, thread_id, emoji_id):
            self.icon_attempts.append(str(thread_id))
            if str(thread_id) == "77":
                raise RateLimited(
                    3,
                    "Too Many Requests: retry after 3",
                    method="editForumTopic",
                )
            return super().edit_topic_icon(
                chat_id, thread_id, emoji_id
            )

    telegram = LimitedFirstTopic()
    store = _store()
    result = source_sync.sync_once(
        store,
        source_sync.SyncRuntime(
            FakeTendwire(
                workers=[
                    _worker("worker-one", "space-one"),
                    _worker("worker-two", "space-two"),
                ],
                spaces=[_space("space-one"), _space("space-two")],
                turns={"turns": []},
            ),
            telegram,
            with_outbox=False,
        ),
    )

    assert telegram.icon_attempts == ["77", "77", "78"]
    assert clock[0] == 23.0
    assert result["ok"] is True
    assert result["changed"] is True
    assert result["icon_updated"] == 1
    assert telegram.icon_edits == [("-100", "78", "icon-fox")]
    assert result["telegram_backpressure"]["count"] == 2
    events = result["telegram_backpressure"]["events"]
    assert [event["outcome"] for event in events] == [
        "retrying",
        "retry_exhausted",
    ]
    assert {event["method"] for event in events} == {
        "editForumTopic"
    }


def test_rate_limit_above_wait_ceiling_does_not_hold_pass(
    monkeypatch,
):
    clock = [30.0]
    monkeypatch.setattr(source_sync.time, "monotonic", lambda: clock[0])
    monkeypatch.setattr(
        source_sync.time,
        "sleep",
        lambda seconds: clock.__setitem__(0, clock[0] + seconds),
    )

    class LongLimitedDelete:
        def __init__(self):
            self.calls = 0

        def delete_message(self, _chat_id, _message_id):
            self.calls += 1
            raise RateLimited(
                16,
                "Too Many Requests: retry after 16",
                method="deleteMessage",
            )

    telegram = LongLimitedDelete()
    store = _store()
    result = source_sync._execute_exact_provider_operation(
        telegram,
        store=store,
        mutation=source_sync._provider_mutation(
            "telegram.delete_message",
            reason=(
                "telegram.delete_message: wait ceiling regression"
            ),
            args=("-100", "42"),
        ),
    )

    assert telegram.calls == 1
    assert clock[0] == 30.0
    assert result["status"] == (
        "telegram_rate_limit_wait_exceeds_ceiling"
    )
    assert result["retry_after"] == 16
    assert result["method"] == "deleteMessage"


def test_outbound_message_rate_limit_is_recorded_but_never_retried(
    monkeypatch,
):
    clock = [40.0]
    monkeypatch.setattr(source_sync.time, "monotonic", lambda: clock[0])
    monkeypatch.setattr(
        source_sync.time,
        "sleep",
        lambda seconds: clock.__setitem__(0, clock[0] + seconds),
    )

    class LimitedSend:
        def __init__(self):
            self.calls = 0

        def send_message(self, _chat_id, _text, **_kwargs):
            self.calls += 1
            raise RateLimited(
                4,
                "Too Many Requests: retry after 4",
                method="sendMessage",
            )

    telegram = LimitedSend()
    store = _store()
    result = source_sync._execute_exact_provider_operation(
        telegram,
        store=store,
        mutation=source_sync._provider_mutation(
            "telegram.send_message",
            reason=(
                "telegram.send_message: no replay regression"
            ),
            args=("-100", "one owner-visible message"),
        ),
    )

    assert telegram.calls == 1
    assert clock[0] == 40.0
    assert result["ok"] is False
    assert result["status"] == "telegram_rate_limited"
    assert result["method"] == "sendMessage"
    assert (
        store["telegram"]["rate_limit_backpressure"]["last"][
            "outcome"
        ]
        == "not_retried_message_send"
    )


def test_bot_api_429_carries_exact_method_and_retry_hint(
    monkeypatch,
):
    payload = json.dumps(
        {
            "ok": False,
            "error_code": 429,
            "description": "Too Many Requests: retry after 6",
            "parameters": {"retry_after": 6},
        }
    ).encode()

    def limited(*_args, **_kwargs):
        raise urllib.error.HTTPError(
            "https://api.telegram.invalid",
            429,
            "Too Many Requests",
            hdrs=None,
            fp=io.BytesIO(payload),
        )

    monkeypatch.setattr(
        telegram_delivery.urllib.request, "urlopen", limited
    )

    with pytest.raises(RateLimited) as raised:
        TelegramClient(token="test").api(
            "editForumTopic",
            {"chat_id": "-100", "message_thread_id": "77"},
        )

    assert raised.value.retry_after == 6
    assert raised.value.method == "editForumTopic"
