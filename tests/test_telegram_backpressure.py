from __future__ import annotations

import io
import json
import urllib.error

import pytest

from herdres_connector import source_sync
from herdres_connector import telegram_delivery
from herdres_connector.telegram_delivery import (
    RateLimited,
    TelegramClient,
    TelegramError,
)
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


def test_send_voice_http_429_carries_method_and_retry_hint(
    tmp_path, monkeypatch
):
    voice = tmp_path / "reply.ogg"
    voice.write_bytes(b"OggS")
    payload = json.dumps(
        {
            "ok": False,
            "error_code": 429,
            "description": "Too Many Requests: retry after 8",
            "parameters": {"retry_after": 8},
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
        TelegramClient(token="test").send_voice("-100", voice)

    assert raised.value.retry_after == 8
    assert raised.value.method == "sendVoice"


def test_send_voice_json_429_carries_method_and_retry_hint(
    tmp_path, monkeypatch
):
    voice = tmp_path / "reply.ogg"
    voice.write_bytes(b"OggS")
    payload = json.dumps(
        {
            "ok": False,
            "error_code": 429,
            "description": "Too Many Requests: retry after 9",
            "parameters": {"retry_after": 9},
        }
    ).encode()

    class Response:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def read(self):
            return payload

    monkeypatch.setattr(
        telegram_delivery.urllib.request,
        "urlopen",
        lambda *_args, **_kwargs: Response(),
    )

    with pytest.raises(RateLimited) as raised:
        TelegramClient(token="test").send_voice("-100", voice)

    assert raised.value.retry_after == 9
    assert raised.value.method == "sendVoice"


def test_voice_batch_rate_limit_stops_and_keeps_accepted_ids(
    tmp_path, monkeypatch
):
    monkeypatch.setattr(
        source_sync.speech,
        "outbound_speech_dir",
        lambda *, prune=False: tmp_path,
    )
    monkeypatch.setattr(
        source_sync.speech,
        "speech_request",
        lambda _operation, payload: {
            "ok": True,
            "path": payload["dest"],
        },
    )

    class PartialVoice:
        def __init__(self):
            self.calls = 0

        def send_voice(self, *_args, **_kwargs):
            self.calls += 1
            if self.calls == 1:
                return {"ok": True, "message_id": "801"}
            raise RateLimited(
                5,
                "Too Many Requests: retry after 5",
                method="sendVoice",
            )

    telegram = PartialVoice()
    store = _store()
    result = source_sync._execute_exact_provider_operation(
        telegram,
        store=store,
        mutation=source_sync._provider_mutation(
            "telegram.send_voice_batch",
            reason=(
                "telegram.send_voice_batch: partial acceptance "
                "backpressure regression"
            ),
            args=(
                ("first", "second", "must not run"),
                "turn-1",
                "-100",
                "77",
                "42",
            ),
        ),
    )

    assert telegram.calls == 2
    assert result["ok"] is False
    assert result["status"] == "telegram_rate_limited"
    assert result["method"] == "sendVoice"
    assert result["retry_after"] == 5
    assert result["accepted_message_ids"] == ["801"]
    events = store["telegram"]["rate_limit_backpressure"]["events"]
    assert len(events) == 1
    assert events[0]["method"] == "sendVoice"
    assert events[0]["capability"] == "telegram.send_voice_batch"
    assert events[0]["outcome"] == "not_retried_message_send"


def test_voice_batch_finds_nested_rate_limit_and_stops(
    tmp_path, monkeypatch
):
    monkeypatch.setattr(
        source_sync.speech,
        "outbound_speech_dir",
        lambda *, prune=False: tmp_path,
    )
    monkeypatch.setattr(
        source_sync.speech,
        "speech_request",
        lambda _operation, payload: {
            "ok": True,
            "path": payload["dest"],
        },
    )

    class NestedLimit:
        def __init__(self):
            self.calls = 0

        def send_voice(self, *_args, **_kwargs):
            self.calls += 1
            if self.calls == 1:
                return {"ok": True, "message_id": "801"}
            try:
                raise RateLimited(
                    6,
                    "Too Many Requests: retry after 6",
                    method="sendVoice",
                )
            except RateLimited as cause:
                raise TelegramError("wrapped transport error") from cause

    telegram = NestedLimit()
    store = _store()
    result = source_sync._execute_exact_provider_operation(
        telegram,
        store=store,
        mutation=source_sync._provider_mutation(
            "telegram.send_voice_batch",
            reason=(
                "telegram.send_voice_batch: nested rate-limit "
                "regression"
            ),
            args=(
                ("first", "second", "must not run"),
                "turn-1",
                "-100",
                "77",
                "42",
            ),
        ),
    )

    assert telegram.calls == 2
    assert result["status"] == "telegram_rate_limited"
    assert result["retry_after"] == 6
    assert result["method"] == "sendVoice"
    assert result["accepted_message_ids"] == ["801"]
    events = store["telegram"]["rate_limit_backpressure"]["events"]
    assert len(events) == 1
    assert events[0]["method"] == "sendVoice"
    assert events[0]["outcome"] == "not_retried_message_send"


def test_voice_batch_generic_telegram_error_stops_as_unknown(
    tmp_path, monkeypatch
):
    monkeypatch.setattr(
        source_sync.speech,
        "outbound_speech_dir",
        lambda *, prune=False: tmp_path,
    )
    monkeypatch.setattr(
        source_sync.speech,
        "speech_request",
        lambda _operation, payload: {
            "ok": True,
            "path": payload["dest"],
        },
    )

    class AmbiguousVoice:
        def __init__(self):
            self.calls = 0

        def send_voice(self, *_args, **_kwargs):
            self.calls += 1
            if self.calls == 1:
                return {"ok": True, "message_id": "801"}
            raise TelegramError("connection closed after upload")

    telegram = AmbiguousVoice()
    result = source_sync._execute_exact_provider_operation(
        telegram,
        store=_store(),
        mutation=source_sync._provider_mutation(
            "telegram.send_voice_batch",
            reason=(
                "telegram.send_voice_batch: ambiguous transport "
                "regression"
            ),
            args=(
                ("first", "second", "must not run"),
                "turn-1",
                "-100",
                "77",
                "42",
            ),
        ),
    )

    assert telegram.calls == 2
    assert (
        result["status"]
        == "telegram_voice_batch_delivery_unknown"
    )
    assert result["accepted_message_ids"] == ["801"]
    assert result["automatic_replay_authorized"] is False


def test_voice_batch_programming_error_surfaces_loudly(
    tmp_path, monkeypatch
):
    monkeypatch.setattr(
        source_sync.speech,
        "outbound_speech_dir",
        lambda *, prune=False: tmp_path,
    )
    monkeypatch.setattr(
        source_sync.speech,
        "speech_request",
        lambda _operation, payload: {
            "ok": True,
            "path": payload["dest"],
        },
    )

    class BrokenVoice:
        def send_voice(self, *_args, **_kwargs):
            raise AttributeError("voice adapter typo")

    with pytest.raises(AttributeError, match="voice adapter typo"):
        source_sync._execute_exact_provider_operation(
            BrokenVoice(),
            store=_store(),
            mutation=source_sync._provider_mutation(
                "telegram.send_voice_batch",
                reason=(
                    "telegram.send_voice_batch: programming error "
                    "must remain loud"
                ),
                args=(
                    ("first", "must not run"),
                    "turn-1",
                    "-100",
                    "77",
                    "42",
                ),
            ),
        )
