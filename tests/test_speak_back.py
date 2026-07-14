"""Outbound voice / speak-back (issue #4 Phase 1): reply-to-voice auto-mode.

Covers the ported chain: state ring (record/detect voice-note message ids), TelegramClient.send_voice
(multipart + dry-run), speech trim/triggers, the additive _deliver_final speak seam, and command_reply
setting speak_next_reply when the owner replies to a voice note.
"""
from __future__ import annotations

import json
from unittest.mock import patch

import herdres
from herdres_connector import source_sync, speech, state
from herdres_connector.source_sync import SyncRuntime
from herdres_connector.telegram_delivery import TelegramClient

from test_source_only import REQUEST_ID, FakeTelegram, FakeTendwire, _source_worker, _store


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


# --- the speak seam (lives in _sync_turns so ALL delivery branches speak) -----

def _final_item():
    return {"id": "t1", "worker_id": "w1", "worker_fingerprint": "fp1",
            "assistant_final_text": "All done deploying the branch.", "complete": True}


def _turns_of(item):
    return {"turns": [item]}


def _persist_worker(store, **entry_extra):
    key, entry, _created = state.upsert_worker_entry(store, _source_worker({
        "id": "w1",
        "name": "worker",
        "status": "working",
        "space_id": "s1",
        "fingerprint": "fp1",
    }), topic_id="77")
    entry.update(entry_extra)
    return key, entry


def _worker_store(monkeypatch, **entry_extra):
    monkeypatch.setenv("HERDRES_SOURCE_TOPIC_MODE", "worker")  # _entry_for_turn resolves worker entries
    store = _store()
    _key, entry = _persist_worker(store, **entry_extra)
    return store, entry


def _run_turns(store, item, runtime):
    return source_sync._sync_turns(store, _turns_of(item), {"pending": []}, runtime, chat_id="-100")


def test_speak_seam_speaks_when_flag_set_and_pops_it(monkeypatch):
    store, entry = _worker_store(monkeypatch, speak_next_reply=True)
    telegram = FakeTelegram()
    runtime = SyncRuntime(FakeTendwire(), telegram, with_outbox=False)
    monkeypatch.setattr(speech, "speech_request", lambda ep, pl: {"ok": True, "path": pl.get("dest")})
    _run_turns(store, _final_item(), runtime)
    assert len(telegram.sent) == 1                      # text turn delivered
    assert len(telegram.voice_notes) == 1              # + spoken back
    assert "speak_next_reply" not in entry             # one-shot flag consumed
    assert entry.get("voice_reply_message_ids") == ["900"]  # sent voice id remembered for next time


def test_speak_seam_silent_without_trigger(monkeypatch):
    store, entry = _worker_store(monkeypatch)   # no flag/trigger/force-all
    telegram = FakeTelegram()
    runtime = SyncRuntime(FakeTendwire(), telegram, with_outbox=False)
    monkeypatch.setattr(speech, "speech_request", lambda ep, pl: {"ok": True, "path": pl.get("dest")})
    _run_turns(store, _final_item(), runtime)
    assert len(telegram.sent) == 1
    assert telegram.voice_notes == []                  # text-only, no voice


def test_speak_seam_tts_failure_leaves_text_intact(monkeypatch):
    store, entry = _worker_store(monkeypatch, speak_next_reply=True)
    telegram = FakeTelegram()
    runtime = SyncRuntime(FakeTendwire(), telegram, with_outbox=False)
    monkeypatch.setattr(speech, "speech_request", lambda ep, pl: {"ok": False, "path": ""})
    _run_turns(store, _final_item(), runtime)
    assert len(telegram.sent) == 1 and telegram.voice_notes == []
    assert "speak_next_reply" not in entry             # still consumed (one-shot, no retry storm)


def test_speak_seam_fires_for_non_raw_delivery_branch(monkeypatch):
    # Regression: the seam must fire for the promote/replace branches too (the common streaming case),
    # not only the raw-send path. Simulate a non-raw delivery by stubbing _deliver_final to just
    # report success without going through send_feed_item; the seam (now in _sync_turns) must still speak.
    store, entry = _worker_store(monkeypatch, speak_next_reply=True)
    telegram = FakeTelegram()
    runtime = SyncRuntime(FakeTendwire(), telegram, with_outbox=False)
    monkeypatch.setattr(speech, "speech_request", lambda ep, pl: {"ok": True, "path": pl.get("dest")})
    monkeypatch.setattr(source_sync, "_deliver_final", lambda *a, **k: True)   # promote/replace-style
    _run_turns(store, _final_item(), runtime)
    assert len(telegram.voice_notes) == 1              # spoke despite no raw send
    assert "speak_next_reply" not in entry


def test_speak_seam_skipped_in_dry_run(monkeypatch):
    store, entry = _worker_store(monkeypatch, speak_next_reply=True)
    telegram = FakeTelegram()
    runtime = SyncRuntime(FakeTendwire(), telegram, with_outbox=False, dry_run=True)
    monkeypatch.setattr(speech, "speech_request", lambda ep, pl: {"ok": True, "path": pl.get("dest")})
    monkeypatch.setattr(source_sync, "_deliver_final", lambda *a, **k: True)
    _run_turns(store, _final_item(), runtime)
    assert telegram.voice_notes == []                  # no speak in a preview pass
    assert entry.get("speak_next_reply") is True       # flag preserved for the real send


# --- Phase 2: chunking + off-lock synth --------------------------------------

def test_speech_reply_chunks(monkeypatch):
    monkeypatch.delenv("HERDR_TELEGRAM_TOPICS_SPEECH_REPLY_MAX_CHARS", raising=False)
    text = " ".join(f"Sentence number {i} here." for i in range(50))
    chunks = speech.speech_reply_chunks(text, max_chars=40, max_chunks=3)
    assert len(chunks) == 3                              # capped at max_chunks
    assert all(len(c) <= 40 for c in chunks)            # each within the size cap
    assert speech.speech_reply_chunks("Short answer.", max_chars=600) == ["Short answer."]
    assert speech.speech_reply_chunks("") == []


def test_speak_seam_chunks_long_reply_into_multiple_notes(monkeypatch):
    store, entry = _worker_store(monkeypatch, speak_next_reply=True)
    monkeypatch.setenv("HERDR_TELEGRAM_TOPICS_SPEECH_REPLY_MAX_CHARS", "30")
    telegram = FakeTelegram()
    runtime = SyncRuntime(FakeTendwire(), telegram, with_outbox=False)
    monkeypatch.setattr(speech, "speech_request", lambda ep, pl: {"ok": True, "path": pl.get("dest")})
    monkeypatch.setattr(source_sync, "_deliver_final", lambda *a, **k: True)
    long_item = {"id": "t1", "worker_id": "w1", "worker_fingerprint": "fp1", "complete": True,
                 "assistant_final_text": "First part is done here. Second part also done. Third part finished."}
    _run_turns(store, long_item, runtime)
    assert len(telegram.voice_notes) >= 2                       # spoken as several voice notes
    assert len(entry.get("voice_reply_message_ids") or []) == len(telegram.voice_notes)  # all recorded


def test_speak_seam_offlock_synth_no_clobber(tmp_path, monkeypatch):
    # Phase 2: synth runs OFF the state lock. A competitor writing state.json DURING synth must survive
    # (the seam commits before releasing, reloads after) — no clobber, and the voice id still records.
    monkeypatch.setenv("HERDRES_SOURCE_TOPIC_MODE", "worker")
    statepath = tmp_path / "state.json"
    monkeypatch.setenv("HERDR_TELEGRAM_TOPICS_STATE", str(statepath))
    store = _store()
    worker_key, _entry = _persist_worker(store, speak_next_reply=True)
    state.save_state(store)
    telegram = FakeTelegram()
    runtime = SyncRuntime(FakeTendwire(), telegram, with_outbox=False)

    def fake_tts(_ep, pl):
        # runs inside the released (off-lock) window: a competitor grabs the freed lock and writes.
        disk = json.loads(statepath.read_text())
        disk["competitor_sentinel"] = "written-during-synth"
        statepath.write_text(json.dumps(disk))
        return {"ok": True, "path": pl.get("dest")}

    monkeypatch.setattr(speech, "speech_request", fake_tts)
    monkeypatch.setattr(source_sync, "_deliver_final", lambda *a, **k: True)
    with state.state_lock(path=statepath):
        source_sync._sync_turns(store, _turns_of(_final_item()), {"pending": []}, runtime, chat_id="-100")

    assert store.get("competitor_sentinel") == "written-during-synth"   # competitor write survived reload
    assert len(telegram.voice_notes) == 1
    assert store["panes"][worker_key].get("voice_reply_message_ids") == ["900"]   # recorded on fresh entry


def test_speak_seam_offlock_entry_pruned_during_synth(tmp_path, monkeypatch):
    # If a competitor prunes the entry during off-lock synth, the seam must not crash or resurrect it;
    # the voice notes were sent but their ids can't persist (entry gone) — handled gracefully.
    monkeypatch.setenv("HERDRES_SOURCE_TOPIC_MODE", "worker")
    statepath = tmp_path / "state.json"
    monkeypatch.setenv("HERDR_TELEGRAM_TOPICS_STATE", str(statepath))
    store = _store()
    worker_key, _entry = _persist_worker(store, speak_next_reply=True)
    state.save_state(store)
    telegram = FakeTelegram()
    runtime = SyncRuntime(FakeTendwire(), telegram, with_outbox=False)

    def fake_tts(_ep, pl):
        disk = json.loads(statepath.read_text())
        disk["panes"].pop(worker_key, None)   # competitor prunes the entry mid-synth
        statepath.write_text(json.dumps(disk))
        return {"ok": True, "path": pl.get("dest")}

    monkeypatch.setattr(speech, "speech_request", fake_tts)
    monkeypatch.setattr(source_sync, "_deliver_final", lambda *a, **k: True)
    with state.state_lock(path=statepath):
        source_sync._sync_turns(store, _turns_of(_final_item()), {"pending": []}, runtime, chat_id="-100")

    assert worker_key not in store["panes"]           # competitor prune survived, not resurrected
    assert len(telegram.voice_notes) == 1              # note was still sent, no crash


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


# --- command_reply sets the flag on a reply-to-voice -------------------------

def test_command_reply_sets_speak_next_reply_on_voice_reply(tmp_path, monkeypatch):
    monkeypatch.setenv("HERDRES_TENDWIRE_MODE", "source")
    monkeypatch.setenv("HERDRES_SOURCE_TOPIC_MODE", "worker")
    statepath = tmp_path / "state.json"
    monkeypatch.setenv("HERDR_TELEGRAM_TOPICS_STATE", str(statepath))
    store = _store()
    worker_key, _entry = _persist_worker(store, voice_reply_message_ids=["901"])
    state.save_state(store)

    with patch.object(herdres.TendwireClient, "command", return_value={"ok": True, "status": "accepted", "result": {"delivery_state": "submitted"}}):
        result = herdres.command_reply({
            "request_id": REQUEST_ID,
            "topic_id": "77", "user_id": "1", "text": "great, keep going",
            "reply_to_message_id": "901",   # replying to the pane's voice note 901
        })
    assert result["handled"] is True
    saved = state.load_state(statepath)
    assert saved["panes"][worker_key].get("speak_next_reply") is True


def test_command_reply_no_flag_for_plain_message(tmp_path, monkeypatch):
    monkeypatch.setenv("HERDRES_TENDWIRE_MODE", "source")
    monkeypatch.setenv("HERDRES_SOURCE_TOPIC_MODE", "worker")
    statepath = tmp_path / "state.json"
    monkeypatch.setenv("HERDR_TELEGRAM_TOPICS_STATE", str(statepath))
    store = _store()
    worker_key, _entry = _persist_worker(store, voice_reply_message_ids=["901"])
    state.save_state(store)
    with patch.object(herdres.TendwireClient, "command", return_value={"ok": True, "status": "accepted", "result": {"delivery_state": "submitted"}}):
        herdres.command_reply(
            {
                "request_id": REQUEST_ID,
                "topic_id": "77",
                "user_id": "1",
                "text": "hello",
                "reply_to_message_id": "555",
            }
        )
    saved = state.load_state(statepath)
    assert saved["panes"][worker_key].get("speak_next_reply") is None


# --- trigger phrase is a bridge directive: stripped before the pane sees it ---

def test_strip_speech_reply_trigger():
    assert speech.strip_speech_reply_trigger("reply by voice: say hello") == "say hello"
    assert speech.strip_speech_reply_trigger("Explain that to me? Reply by voice") == "Explain that to me"
    assert speech.strip_speech_reply_trigger("reply by voice") == ""
    assert speech.strip_speech_reply_trigger("no trigger here") == "no trigger here"


def test_command_reply_arms_flag_and_strips_trigger(tmp_path, monkeypatch):
    monkeypatch.setenv("HERDRES_TENDWIRE_MODE", "source")
    monkeypatch.setenv("HERDRES_SOURCE_TOPIC_MODE", "worker")
    statepath = tmp_path / "state.json"
    monkeypatch.setenv("HERDR_TELEGRAM_TOPICS_STATE", str(statepath))
    store = _store()
    worker_key, _entry = _persist_worker(store)
    state.save_state(store)
    sent = {}

    def fake_command(self, request):
        sent.update(request)
        return {"ok": True, "status": "accepted", "result": {"delivery_state": "submitted"}}

    with patch.object(herdres.TendwireClient, "command", fake_command):
        herdres.command_reply(
            {
                "request_id": REQUEST_ID,
                "topic_id": "77",
                "user_id": "1",
                "text": "Summarize the file? Reply by voice",
            }
        )
    assert sent["instruction"]["text"] == "Summarize the file"     # phrase never reaches the agent
    saved = state.load_state(statepath)
    assert saved["panes"][worker_key].get("speak_next_reply") is True  # bridge owns the voice


def test_command_reply_standalone_trigger_arms_without_submitting(tmp_path, monkeypatch):
    monkeypatch.setenv("HERDRES_TENDWIRE_MODE", "source")
    monkeypatch.setenv("HERDRES_SOURCE_TOPIC_MODE", "worker")
    statepath = tmp_path / "state.json"
    monkeypatch.setenv("HERDR_TELEGRAM_TOPICS_STATE", str(statepath))
    store = _store()
    worker_key, _entry = _persist_worker(store)
    state.save_state(store)
    with patch.object(herdres.TendwireClient, "command") as cmd:
        result = herdres.command_reply({"topic_id": "77", "user_id": "1", "text": "reply by voice"})
    cmd.assert_not_called()                                        # nothing submitted to the pane
    assert "spoken" in result["reply"]
    saved = state.load_state(statepath)
    assert saved["panes"][worker_key].get("speak_next_reply") is True
