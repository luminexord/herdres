"""Outbound voice / speak-back presenter coverage.

Covers the ported chain: state ring (record/detect voice-note message ids), TelegramClient.send_voice
(multipart + dry-run), speech trimming/triggers, chunking, and outbound file hygiene.
"""
from __future__ import annotations

import json
from unittest.mock import patch

import pytest
from herdres_connector import speech, state
from herdres_connector.telegram_delivery import (
    RateLimited,
    TelegramClient,
)

# --- state ring --------------------------------------------------------------

def test_voice_reply_ring_records_dedups_and_bounds():
    entry: dict = {}
    for i in range(35):
        state.record_voice_reply_message_id(entry, i)
    ids = entry["voice_reply_message_ids"]
    assert len(ids) == state.VOICE_REPLY_ID_HISTORY == 30      # bounded
    assert ids[-1] == "34" and ids[0] == "5"                   # newest kept, oldest dropped
    state.record_voice_reply_message_id(entry, 20)            # re-touch moves to newest, no dup
    assert entry["voice_reply_message_ids"].count("20") == 1
    assert entry["voice_reply_message_ids"][-1] == "20"


def test_message_is_voice_reply():
    entry = {"voice_reply_message_ids": ["901", "902"]}
    assert state.message_is_voice_reply(entry, "902") is True
    assert state.message_is_voice_reply(entry, 902) is True
    assert state.message_is_voice_reply(entry, "999") is False
    assert state.message_is_voice_reply(entry, None) is False
    assert state.message_is_voice_reply({}, "901") is False


# --- send_voice --------------------------------------------------------------

def test_send_voice_dry_run_is_noop_ok():
    client = TelegramClient(token="x", dry_run=True)
    assert client.send_voice("-100", "/nonexistent.ogg", thread_id="77")["ok"] is True


def test_send_voice_multipart_body_and_call(tmp_path):
    ogg = tmp_path / "reply.ogg"
    ogg.write_bytes(b"OggS-fake-opus")
    captured = {}

    class _Resp:
        def __enter__(self_):
            return self_
        def __exit__(self_, *a):
            return False
        def read(self_):
            return json.dumps({"ok": True, "result": {"message_id": 555}}).encode()

    def fake_urlopen(request, timeout=None):
        captured["url"] = request.full_url
        captured["ctype"] = request.headers.get("Content-type")
        captured["body"] = request.data
        return _Resp()

    with patch("urllib.request.urlopen", side_effect=fake_urlopen):
        out = TelegramClient(token="TOK").send_voice("-100", ogg, thread_id="77", reply_to_message_id="42")

    assert out == {"ok": True, "message_id": "555"}
    assert captured["url"].endswith("/botTOK/sendVoice")
    assert captured["ctype"].startswith("multipart/form-data; boundary=")
    body = captured["body"]
    assert b'name="chat_id"' in body and b"-100" in body
    assert b'name="message_thread_id"' in body and b"77" in body
    assert b'name="reply_parameters"' in body and b'"message_id":42' in body
    assert b'name="voice"; filename="reply.ogg"' in body and b"OggS-fake-opus" in body


@pytest.mark.parametrize(
    "result",
    [{}, {"message_id": 0}, {"message_id": "0"}],
    ids=["missing", "numeric-zero", "string-zero"],
)
def test_send_voice_does_not_synthesize_zero_message_id(
    tmp_path, result
):
    ogg = tmp_path / "reply.ogg"
    ogg.write_bytes(b"OggS-fake-opus")

    class _Resp:
        def __enter__(self_):
            return self_

        def __exit__(self_, *args):
            return False

        def read(self_):
            return json.dumps(
                {"ok": True, "result": result}
            ).encode()

    with patch(
        "urllib.request.urlopen", return_value=_Resp()
    ):
        sent = TelegramClient(token="TOK").send_voice(
            "-100", ogg
        )

    assert sent == {"ok": True, "message_id": ""}


# --- speech trim / triggers --------------------------------------------------

def test_trim_for_speech_strips_and_caps():
    out = speech.trim_for_speech("Run `ls` then ```rm -rf /``` see https://x.io **now**")
    assert "```" not in out and "`" not in out and "http" not in out and "*" not in out
    assert "code omitted" in out and "a link" in out


def test_speech_reply_triggers(monkeypatch):
    monkeypatch.delenv("HERDR_TELEGRAM_TOPICS_SPEECH_REPLY_TRIGGER", raising=False)
    assert speech.speech_reply_triggered("please reply by voice") is True
    assert speech.speech_reply_triggered("just text") is False
    monkeypatch.setenv("HERDR_TELEGRAM_TOPICS_SPEECH_REPLY_ON_VOICE_REPLY", "0")
    assert speech.speech_reply_on_voice_reply_enabled() is False
    monkeypatch.setenv("HERDR_TELEGRAM_TOPICS_SPEECH_REPLY_ON_VOICE_REPLY", "1")
    assert speech.speech_reply_on_voice_reply_enabled() is True


# --- Phase 2: chunking + off-lock synth --------------------------------------

def test_speech_reply_chunks(monkeypatch):
    monkeypatch.delenv("HERDR_TELEGRAM_TOPICS_SPEECH_REPLY_MAX_CHARS", raising=False)
    text = " ".join(f"Sentence number {i} here." for i in range(50))
    chunks = speech.speech_reply_chunks(text, max_chars=40, max_chunks=3)
    assert len(chunks) == 3                              # capped at max_chunks
    assert all(len(c) <= 40 for c in chunks)            # each within the size cap
    assert speech.speech_reply_chunks("Short answer.", max_chars=600) == ["Short answer."]
    assert speech.speech_reply_chunks("") == []












# --- outbound-speech dir hygiene ---------------------------------------------

def test_outbound_speech_dir_prunes_and_rejects_symlink(tmp_path, monkeypatch):
    monkeypatch.setenv("HERDR_TELEGRAM_TOPICS_STATE", str(tmp_path / "state.json"))
    base = tmp_path / "outbound-speech"
    base.mkdir()
    for i in range(70):
        (base / f"r{i:03d}.ogg").write_bytes(b"x")
    d = speech.outbound_speech_dir(prune=True)
    assert len(list(d.glob("*.ogg"))) == 64            # bounded to keep=64 (unbounded growth fixed)

    # symlinked dir is rejected (never followed)
    monkeypatch.setenv("HERDR_TELEGRAM_TOPICS_STATE", str(tmp_path / "sub" / "state.json"))
    (tmp_path / "sub").mkdir()
    (tmp_path / "sub" / "outbound-speech").symlink_to(tmp_path)
    try:
        speech.outbound_speech_dir()
        assert False, "expected symlink rejection"
    except RuntimeError:
        pass
