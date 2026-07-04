"""Tests for the voice pipeline fixes (issue #4):
  * download uses the RECEIVING bot's token (managed-bot topics), not just the manager token;
  * long/quiet voice notes are chunked + volume-normalized for STT;
  * replying to one of the agent's voice notes auto-enables "reply by voice" (one-shot).
"""
from __future__ import annotations

import os
import unittest
from unittest.mock import Mock, patch

import herdres
import herdres_speech


TELEGRAM = {"managed_bots": {"claude": {"token": "CLAUDE_TOK", "enabled": True},
                             "codex": {"token": "CODEX_TOK", "enabled": True}}}


class DownloadTokenTests(unittest.TestCase):
    def test_candidate_order_receiving_bot_then_manager_then_others(self) -> None:
        cands = herdres.download_bot_token_candidates(TELEGRAM, "claude")
        self.assertEqual(cands[0], "CLAUDE_TOK")   # receiving bot first
        self.assertIn(None, cands)                 # manager (default) as fallback
        self.assertIn("CODEX_TOK", cands)          # other managed bots last

    def test_candidates_disabled_bot_skipped(self) -> None:
        tg = {"managed_bots": {"claude": {"token": "T", "enabled": False}}}
        self.assertEqual(herdres.download_bot_token_candidates(tg, "claude"), [None])

    def test_candidates_no_kind_is_manager_only_plus_managed(self) -> None:
        self.assertEqual(herdres.download_bot_token_candidates({}, ""), [None])

    def test_get_file_any_picks_the_only_token_that_works(self) -> None:
        def fake_api(method, payload, *, token=None):
            if method == "getFile" and token == "CLAUDE_TOK":
                return {"ok": True, "result": {"file_path": "voice/f.oga", "file_size": 5}}
            return {"ok": False, "description": "Bad Request: file not found"}
        with patch.object(herdres, "telegram_api", side_effect=fake_api):
            result, tok = herdres.telegram_get_file_any(
                "FID", herdres.download_bot_token_candidates(TELEGRAM, "claude"))
        self.assertEqual(tok, "CLAUDE_TOK")
        self.assertEqual(result["file_path"], "voice/f.oga")

    def test_get_file_any_raises_when_all_tokens_fail(self) -> None:
        with patch.object(herdres, "telegram_api",
                          return_value={"ok": False, "description": "file not found"}):
            with self.assertRaises(herdres.BridgeError):
                herdres.telegram_get_file_any("FID", ["A", None, "B"])

    def test_deliver_attachment_uses_winning_token_for_getfile_and_fetch(self) -> None:
        calls = {"getFile": [], "download": None}

        def fake_api(method, payload, *, token=None):
            calls["getFile"].append(token)
            if method == "getFile" and token == "CLAUDE_TOK":
                return {"ok": True, "result": {"file_path": "voice/f.oga", "file_size": 5}}
            return {"ok": False, "description": "file not found"}

        def fake_dl(file_path, dest, *, api_token=None, max_bytes=None):
            calls["download"] = api_token
            return 5

        with patch.object(herdres, "telegram_api", side_effect=fake_api), \
                patch.object(herdres, "download_telegram_file", side_effect=fake_dl), \
                patch.object(herdres, "attachment_dest_path", return_value=herdres.Path("/tmp/x")), \
                patch.object(herdres, "prune_attachment_dir", Mock()):
            ok, detail, dest = herdres.deliver_attachment(
                "pane-1", {"file_id": "FID", "file_size": 5},
                api_tokens=herdres.download_bot_token_candidates(TELEGRAM, "claude"))
        self.assertTrue(ok)
        self.assertEqual(calls["download"], "CLAUDE_TOK")


class VoiceReplyAutoModeTests(unittest.TestCase):
    def test_record_and_detect(self) -> None:
        entry: dict = {}
        herdres.record_voice_reply_message_id(entry, 5001)
        herdres.record_voice_reply_message_id(entry, "5002")
        self.assertTrue(herdres.message_is_voice_reply(entry, "5001"))
        self.assertTrue(herdres.message_is_voice_reply(entry, 5002))
        self.assertFalse(herdres.message_is_voice_reply(entry, "9999"))
        self.assertFalse(herdres.message_is_voice_reply(entry, ""))

    def test_bounded_ring_evicts_oldest(self) -> None:
        entry: dict = {}
        herdres.record_voice_reply_message_id(entry, "old")
        for i in range(herdres.VOICE_REPLY_ID_HISTORY + 5):
            herdres.record_voice_reply_message_id(entry, 6000 + i)
        self.assertEqual(len(entry["voice_reply_message_ids"]), herdres.VOICE_REPLY_ID_HISTORY)
        self.assertFalse(herdres.message_is_voice_reply(entry, "old"))

    def test_enable_toggle(self) -> None:
        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop("HERDR_TELEGRAM_TOPICS_SPEECH_REPLY_ON_VOICE_REPLY", None)
            self.assertTrue(herdres.speech_reply_on_voice_reply_enabled())
            os.environ["HERDR_TELEGRAM_TOPICS_SPEECH_REPLY_ON_VOICE_REPLY"] = "0"
            self.assertFalse(herdres.speech_reply_on_voice_reply_enabled())
            os.environ.pop("HERDR_TELEGRAM_TOPICS_SPEECH_REPLY_ON_VOICE_REPLY", None)

    def _state(self):
        key = "pane-1"
        entry = {"pane_key": key, "pane_id": "pane-1", "space_key": "workspace:one", "topic_id": "77",
                 "pane_root_message_id": "1001", "last_known_status": "idle",
                 "voice_reply_message_ids": ["3001"]}
        state = {"version": 1, "enabled": True,
                 "telegram": {"chat_id": "-1001", "general_thread_id": "1", "owner_user_ids": ["42"]},
                 "spaces": {"workspace:one": {"space_key": "workspace:one", "topic_id": "77", "pane_keys": [key]}},
                 "panes": {key: entry}}
        return state, entry

    def _command_reply(self, reply_to):
        state, entry = self._state()
        payload = {"chat_id": "-1001", "topic_id": "77", "user_id": "42", "message_id": "7",
                   "text": "go on then", "reply_to_message_id": reply_to}
        with patch.multiple(herdres, load_state=Mock(return_value=state), save_state=Mock(),
                            load_dotenv=Mock(), send_to_pane=Mock(return_value=(True, ""))):
            herdres.command_reply(payload)
        return entry.get("speak_next_reply")

    def test_command_reply_sets_flag_on_reply_to_voice(self) -> None:
        self.assertTrue(self._command_reply("3001"))

    def test_command_reply_no_flag_on_reply_to_text(self) -> None:
        self.assertIsNone(self._command_reply("9999"))

    def test_command_reply_no_flag_without_reply(self) -> None:
        self.assertIsNone(self._command_reply(""))


class SttChunkingTests(unittest.TestCase):
    def test_normalize_filter_default_on_and_toggle(self) -> None:
        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop("HERDR_TELEGRAM_TOPICS_SPEECH_NORMALIZE", None)
            self.assertIn("loudnorm", herdres_speech._stt_normalize_filter())
            os.environ["HERDR_TELEGRAM_TOPICS_SPEECH_NORMALIZE"] = "0"
            self.assertEqual(herdres_speech._stt_normalize_filter(), "")
            os.environ.pop("HERDR_TELEGRAM_TOPICS_SPEECH_NORMALIZE", None)

    def test_short_audio_single_pass(self) -> None:
        # ~5s of PCM (< 14s window) → one decode pass.
        pcm = b"\x00\x00\x00\x00" * (herdres_speech.STT_SAMPLE_RATE * 5)
        with patch.object(herdres_speech, "_load_stt", return_value=object()), \
                patch.object(herdres_speech, "_decode_to_pcm", return_value=pcm), \
                patch.object(herdres_speech, "_decode_samples", return_value="hello") as dec:
            out = herdres_speech.transcribe("x.ogg")
        self.assertEqual(out, "hello")
        self.assertEqual(dec.call_count, 1)

    def test_long_audio_is_chunked_and_joined(self) -> None:
        # ~50s of PCM with a 14s window → multiple decode passes, non-empty parts joined.
        pcm = b"\x00\x00\x00\x00" * (herdres_speech.STT_SAMPLE_RATE * 50)
        parts = iter(["one", "", "two", "three", ""])
        with patch.object(herdres_speech, "_load_stt", return_value=object()), \
                patch.object(herdres_speech, "_decode_to_pcm", return_value=pcm), \
                patch.object(herdres_speech, "_decode_samples", side_effect=lambda *a: next(parts, "")) as dec:
            out = herdres_speech.transcribe("x.ogg")
        self.assertGreater(dec.call_count, 1)          # chunked
        self.assertEqual(out, "one two three")         # empties dropped, rest joined


if __name__ == "__main__":
    unittest.main()
