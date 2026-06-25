"""Issue #4 v1: inbound voice — a Telegram voice note → local STT (parakeet) → the pane.

Covers:
  * attachment extraction adds a `voice` kind (dict + object forms); document/photo unchanged;
  * the command_reply voice arm: transcribes + echoes + delivers when enabled; degrades gracefully
    (a friendly reply, never send_to_pane, contract preserved) when disabled / engine-absent / empty;
  * herdres_speech engine: flags, trim_for_speech, transcribe is fail-open, speech_request falls
    back in-process when there's no sidecar socket.
"""

from __future__ import annotations

import unittest
import unittest.mock as mock
from pathlib import Path
from unittest.mock import Mock, patch

import herdres
import herdres_routing
import herdres_speech


# --- attachment extraction --------------------------------------------------------------------

class VoiceExtractionTests(unittest.TestCase):
    def test_dict_voice(self) -> None:
        att = herdres_routing.attachment_payload_dict(
            {"voice": {"file_id": "v1", "mime_type": "audio/ogg", "file_size": 5000, "duration": 4}})
        self.assertEqual(att["kind"], "voice")
        self.assertEqual(att["file_id"], "v1")
        self.assertEqual(att["duration"], 4)

    def test_obj_voice(self) -> None:
        voice = type("V", (), {"file_id": "v2", "mime_type": "", "file_size": 0, "duration": 9})()
        att = herdres_routing.attachment_payload_obj(type("M", (), {"voice": voice})())
        self.assertEqual(att["kind"], "voice")
        self.assertEqual(att["mime_type"], "audio/ogg")  # default when Telegram omits it
        self.assertEqual(att["duration"], 9)

    def test_document_and_photo_unchanged(self) -> None:
        doc = herdres_routing.attachment_payload_dict({"document": {"file_id": "d1", "file_name": "a.txt"}})
        self.assertEqual(doc["kind"], "document")
        photo = herdres_routing.attachment_payload_dict({"photo": [{"file_id": "p1"}]})
        self.assertEqual(photo["kind"], "photo")

    def test_no_attachment(self) -> None:
        self.assertIsNone(herdres_routing.attachment_payload_dict({"text": "hi"}))


# --- command_reply voice arm ------------------------------------------------------------------

def _state(agent: str = "claude") -> dict:
    return {
        "version": 1,
        "telegram": {"chat_id": "-1001", "general_thread_id": "1", "owner_user_ids": ["42"]},
        "spaces": {"sp": {"space_key": "sp", "topic_id": "77", "pane_keys": ["p1"], "message_routes": {}}},
        "panes": {"p1": {"pane_key": "p1", "pane_id": "p1", "agent": agent, "space_key": "sp",
                         "topic_id": "77", "last_known_status": "working"}},
    }


def _voice_payload(caption: str = "") -> dict:
    return {"chat_id": "-1001", "topic_id": "77", "user_id": "42", "text": "", "caption": caption,
            "attachment": {"kind": "voice", "file_id": "v1", "mime_type": "audio/ogg",
                           "file_size": 5000, "duration": 4}}


class VoiceCommandReplyTests(unittest.TestCase):
    def _run(self, *, speech, send_to_pane=None, deliver=None, payload=None):
        state = _state()
        send_to_pane = send_to_pane or Mock(return_value=(True, ""))
        deliver = deliver or Mock(return_value=(True, "", Path("/tmp/voice.ogg")))
        send_message = Mock(return_value="9001")
        with patch.multiple(
            herdres,
            load_dotenv=Mock(), load_state=Mock(return_value=state), save_state=Mock(),
            herdres_speech=speech, deliver_attachment=deliver,
            send_to_pane=send_to_pane, send_message=send_message,
        ):
            result = herdres.command_reply(payload or _voice_payload())
        return result, send_to_pane, deliver, send_message

    def test_enabled_transcribes_echoes_and_delivers(self) -> None:
        speech = Mock()
        speech.speech_input_enabled.return_value = True
        speech.speech_echo_transcript_enabled.return_value = True
        speech.speech_request.return_value = {"text": "deploy the staging branch please"}
        result, send_to_pane, deliver, send_message = self._run(speech=speech)
        deliver.assert_called_once()
        send_to_pane.assert_called_once()
        self.assertEqual(send_to_pane.call_args.args[1], "deploy the staging branch please")
        # echo went to the topic
        self.assertTrue(any("Heard" in str(a) for a in send_message.call_args.args))
        self.assertIn("Sent your voice message", result["reply"])

    def test_caption_appended(self) -> None:
        speech = Mock()
        speech.speech_input_enabled.return_value = True
        speech.speech_echo_transcript_enabled.return_value = False
        speech.speech_request.return_value = {"text": "the transcript"}
        result, send_to_pane, _, _ = self._run(speech=speech, payload=_voice_payload(caption="(also: be terse)"))
        self.assertIn("the transcript", send_to_pane.call_args.args[1])
        self.assertIn("be terse", send_to_pane.call_args.args[1])

    def test_disabled_is_graceful_no_delivery(self) -> None:
        speech = Mock()
        speech.speech_input_enabled.return_value = False
        result, send_to_pane, deliver, _ = self._run(speech=speech)
        self.assertIn("Voice transcription is off", result["reply"])
        send_to_pane.assert_not_called()
        deliver.assert_not_called()  # don't even download when disabled

    def test_module_absent_is_graceful(self) -> None:
        result, send_to_pane, _, _ = self._run(speech=None)  # herdres_speech import failed
        self.assertIn("Voice transcription is off", result["reply"])
        send_to_pane.assert_not_called()

    def test_disabled_with_caption_delivers_caption(self) -> None:
        # Speech off but the voice note has a caption: forward the caption (don't drop it).
        speech = Mock()
        speech.speech_input_enabled.return_value = False
        result, send_to_pane, deliver, _ = self._run(speech=speech, payload=_voice_payload(caption="ship it"))
        send_to_pane.assert_called_once()
        self.assertEqual(send_to_pane.call_args.args[1], "ship it")
        deliver.assert_not_called()  # no need to download audio we won't transcribe
        self.assertIn("sent your caption", result["reply"])

    def test_echo_failure_does_not_abort_delivery(self) -> None:
        # The cosmetic "Heard:" echo raising must NOT prevent the transcript reaching the pane.
        speech = Mock()
        speech.speech_input_enabled.return_value = True
        speech.speech_echo_transcript_enabled.return_value = True
        speech.speech_request.return_value = {"text": "do the thing"}
        send_to_pane = Mock(return_value=(True, ""))
        state = _state()
        with patch.multiple(
            herdres, load_dotenv=Mock(), load_state=Mock(return_value=state), save_state=Mock(),
            herdres_speech=speech, deliver_attachment=Mock(return_value=(True, "", Path("/tmp/v.ogg"))),
            send_to_pane=send_to_pane,
            send_message=Mock(side_effect=herdres.BridgeError("telegram blip")),  # echo fails
        ):
            result = herdres.command_reply(_voice_payload())
        send_to_pane.assert_called_once()
        self.assertEqual(send_to_pane.call_args.args[1], "do the thing")
        self.assertIn("Sent your voice message", result["reply"])

    def test_empty_transcript_is_graceful(self) -> None:
        speech = Mock()
        speech.speech_input_enabled.return_value = True
        speech.speech_echo_transcript_enabled.return_value = True
        speech.speech_request.return_value = {"text": ""}  # engine unavailable / silence
        result, send_to_pane, deliver, _ = self._run(speech=speech)
        self.assertIn("speech-to-text is unavailable", result["reply"])
        deliver.assert_called_once()        # it downloaded
        send_to_pane.assert_not_called()    # but never delivered an empty instruction

    def test_flag_read_exception_is_graceful(self) -> None:
        # A flag reader that somehow raises must be treated as "off", never abort the turn.
        speech = Mock()
        speech.speech_input_enabled.side_effect = RuntimeError("env blew up")
        result, send_to_pane, deliver, _ = self._run(speech=speech)
        self.assertIn("Voice transcription is off", result["reply"])
        send_to_pane.assert_not_called()
        deliver.assert_not_called()

    def test_echo_flag_exception_does_not_abort_delivery(self) -> None:
        speech = Mock()
        speech.speech_input_enabled.return_value = True
        speech.speech_echo_transcript_enabled.side_effect = RuntimeError("boom")
        speech.speech_request.return_value = {"text": "hi there"}
        result, send_to_pane, _, _ = self._run(speech=speech)
        send_to_pane.assert_called_once()
        self.assertEqual(send_to_pane.call_args.args[1], "hi there")

    def test_engine_exception_is_graceful(self) -> None:
        speech = Mock()
        speech.speech_input_enabled.return_value = True
        speech.speech_request.side_effect = RuntimeError("boom")
        result, send_to_pane, _, _ = self._run(speech=speech)
        self.assertIn("speech-to-text is unavailable", result["reply"])
        send_to_pane.assert_not_called()


# --- engine module ----------------------------------------------------------------------------

class SpeechEngineTests(unittest.TestCase):
    def test_flags_default_off_on(self) -> None:
        with patch.dict("os.environ", {}, clear=False):
            for k in ("HERDR_TELEGRAM_TOPICS_SPEECH_INPUT", "HERDR_TELEGRAM_TOPICS_SPEECH_REPLIES",
                      "HERDR_TELEGRAM_TOPICS_SPEECH_ECHO_TRANSCRIPT"):
                import os
                os.environ.pop(k, None)
            self.assertFalse(herdres_speech.speech_input_enabled())
            self.assertFalse(herdres_speech.speech_replies_enabled())
            self.assertTrue(herdres_speech.speech_echo_transcript_enabled())  # default on
        with patch.dict("os.environ", {"HERDR_TELEGRAM_TOPICS_SPEECH_INPUT": "1"}):
            self.assertTrue(herdres_speech.speech_input_enabled())

    def test_transcribe_failopen_without_sherpa(self) -> None:
        # No model/sherpa on the test host -> "" (never raises).
        self.assertEqual(herdres_speech.transcribe("/nonexistent/clip.ogg"), "")

    def test_trim_for_speech_strips_code_and_urls(self) -> None:
        out = herdres_speech.trim_for_speech(
            "Run ```bash\nrm -rf /\n``` then see https://example.com/x for **details**.", max_chars=200)
        self.assertNotIn("rm -rf", out)
        self.assertNotIn("```", out)
        self.assertNotIn("http", out)
        self.assertNotIn("**", out)

    def test_trim_caps_on_sentence_boundary(self) -> None:
        text = "First sentence is here. Second sentence is also here. Third runs over the limit now."
        out = herdres_speech.trim_for_speech(text, max_chars=40)
        self.assertTrue(out.endswith("."))
        self.assertLessEqual(len(out), 60)

    def test_speech_request_falls_back_in_process(self) -> None:
        # No sidecar socket -> speech_request("stt") calls transcribe() in-process (which fail-opens).
        with patch.object(herdres_speech, "speech_socket_path", return_value=Path("/no/such.sock")), \
             patch.object(herdres_speech, "transcribe", return_value="hello") as t:
            out = herdres_speech.speech_request("stt", {"path": "/tmp/x.ogg"})
        self.assertEqual(out, {"text": "hello"})
        t.assert_called_once()

    def test_load_stt_does_not_permanently_cache_when_model_absent(self) -> None:
        # Re-attempt cheaply so `herdres speech install` takes effect without a process restart:
        # a missing model must NOT set the permanent failure flag.
        with patch.object(herdres_speech, "_STT_RECOGNIZER", None), \
             patch.object(herdres_speech, "_STT_LOAD_FAILED", False), \
             patch.object(herdres_speech, "stt_model_dir", return_value=Path("/no/such/model")):
            self.assertIsNone(herdres_speech._load_stt())
            self.assertIsNone(herdres_speech._load_stt())
            self.assertFalse(herdres_speech._STT_LOAD_FAILED)  # still re-attemptable

    def test_sidecar_call_caps_response_size(self) -> None:
        # A runaway sidecar that streams > the cap must raise (so speech_request falls back), not OOM.
        big = b"x" * (herdres_speech._SIDECAR_MAX_BYTES + 1)
        fake = Mock()
        fake.recv.side_effect = [b"HTTP/1.0 200 OK\r\n\r\n", big, b""]
        cm = Mock(); cm.__enter__ = Mock(return_value=fake); cm.__exit__ = Mock(return_value=False)
        with patch("socket.socket", return_value=cm):
            with self.assertRaises(ValueError):
                herdres_speech._sidecar_call(Path("/x.sock"), "stt", {"path": "/a.ogg"})

    def test_speech_request_prefers_sidecar_when_socket_exists(self) -> None:
        with patch.object(herdres_speech, "speech_socket_path") as sp, \
             patch.object(herdres_speech, "_sidecar_call", return_value={"text": "from sidecar"}) as sc:
            sp.return_value = Mock(exists=lambda: True)
            out = herdres_speech.speech_request("stt", {"path": "/tmp/x.ogg"})
        self.assertEqual(out, {"text": "from sidecar"})
        sc.assert_called_once()


class SpeechCliTests(unittest.TestCase):
    def test_speech_check_returns_preflight(self) -> None:
        from types import SimpleNamespace
        result = herdres.speech_once(SimpleNamespace(action="check"))
        self.assertTrue(result["ok"])
        for k in ("sherpa_onnx", "ffmpeg", "stt_model", "stt_model_dir"):
            self.assertIn(k, result)

    def test_speech_install_dispatch(self) -> None:
        from types import SimpleNamespace
        with patch.object(herdres_speech, "install_stt_model", return_value=(True, "installed")), \
             patch.object(herdres_speech, "check", return_value={"sherpa_onnx": False, "ffmpeg": True}):
            result = herdres.speech_once(SimpleNamespace(action="install", force=False))
        self.assertTrue(result["ok"])
        self.assertEqual(result["model"], "installed")
        self.assertTrue(any("sherpa-onnx" in h for h in result["next_steps"]))  # sherpa missing -> hint

    def test_install_stt_model_downloads_verifies_extracts(self) -> None:
        import hashlib
        import io
        import tarfile
        import tempfile
        files = ["encoder.int8.onnx", "decoder.int8.onnx", "joiner.int8.onnx", "tokens.txt"]
        sub = "fake-model-dir"
        with tempfile.TemporaryDirectory() as d:
            base = Path(d)
            archive = base / "m.tar.bz2"
            with tarfile.open(archive, "w:bz2") as tar:
                for f in files:
                    p = base / sub / f
                    p.parent.mkdir(parents=True, exist_ok=True)
                    p.write_bytes(f.encode())
                    tar.add(p, arcname=f"{sub}/{f}")
            data = archive.read_bytes()
            sha = hashlib.sha256(data).hexdigest()
            models_dir = base / "models"
            spec = {"test-model": {"url": "https://x/test.tar.bz2", "sha256": sha,
                                   "archive_subdir": sub, "files": files}}

            class _Resp(io.BytesIO):
                def __enter__(self):
                    return self

                def __exit__(self, *a):
                    return False

            with patch.dict("os.environ", {"HERDR_TELEGRAM_TOPICS_SPEECH_STT_MODEL": "test-model",
                                           "HERDR_TELEGRAM_TOPICS_SPEECH_MODELS_DIR": str(models_dir)}), \
                 patch.object(herdres_speech, "STT_MODELS", spec), \
                 patch("urllib.request.urlopen", return_value=_Resp(data)):
                ok, msg = herdres_speech.install_stt_model(log=lambda *_: None)
                self.assertTrue(ok, msg)
                self.assertTrue(herdres_speech.stt_model_present())
            for f in files:
                self.assertTrue((models_dir / "test-model" / f).is_file())

    def test_install_stt_model_rejects_bad_checksum(self) -> None:
        import io
        import tempfile
        spec = {"test-model": {"url": "https://x/test.tar.bz2", "sha256": "0" * 64,
                               "archive_subdir": "x", "files": ["tokens.txt"]}}

        class _Resp(io.BytesIO):
            def __enter__(self):
                return self

            def __exit__(self, *a):
                return False

        with tempfile.TemporaryDirectory() as d:
            with patch.dict("os.environ", {"HERDR_TELEGRAM_TOPICS_SPEECH_STT_MODEL": "test-model",
                                           "HERDR_TELEGRAM_TOPICS_SPEECH_MODELS_DIR": str(d)}), \
                 patch.object(herdres_speech, "STT_MODELS", spec), \
                 patch("urllib.request.urlopen", return_value=_Resp(b"not the real archive")):
                ok, msg = herdres_speech.install_stt_model(log=lambda *_: None)
        self.assertFalse(ok)
        self.assertIn("checksum mismatch", msg)


if __name__ == "__main__":
    unittest.main()
