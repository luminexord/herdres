"""Re-port of the #118 inbound-STT fixes into the RC connector (herdres_connector/speech.py):
long-audio chunking (parakeet returns empty past ~15s) and pre-STT loudness normalization (quiet
Telegram voice notes transcribe to nothing without it), plus the loudnorm bare-retry.
"""
from __future__ import annotations

import subprocess
from unittest.mock import patch

from herdres_connector import speech
from herdres_connector.speech import STT_SAMPLE_RATE


# --- flag helpers ------------------------------------------------------------

def test_stt_chunk_samples_default_override_bad(monkeypatch):
    monkeypatch.delenv("HERDR_TELEGRAM_TOPICS_SPEECH_CHUNK_SECONDS", raising=False)
    assert speech._stt_chunk_samples() == 14 * STT_SAMPLE_RATE
    monkeypatch.setenv("HERDR_TELEGRAM_TOPICS_SPEECH_CHUNK_SECONDS", "20")
    assert speech._stt_chunk_samples() == 20 * STT_SAMPLE_RATE
    monkeypatch.setenv("HERDR_TELEGRAM_TOPICS_SPEECH_CHUNK_SECONDS", "bad")
    assert speech._stt_chunk_samples() == 14 * STT_SAMPLE_RATE


def test_stt_normalize_filter_default_off_custom(monkeypatch):
    monkeypatch.delenv("HERDR_TELEGRAM_TOPICS_SPEECH_NORMALIZE", raising=False)
    monkeypatch.delenv("HERDR_TELEGRAM_TOPICS_SPEECH_NORMALIZE_FILTER", raising=False)
    assert speech._stt_normalize_filter() == "loudnorm=I=-16:TP=-1.5:LRA=11"
    monkeypatch.setenv("HERDR_TELEGRAM_TOPICS_SPEECH_NORMALIZE", "0")
    assert speech._stt_normalize_filter() == ""
    monkeypatch.setenv("HERDR_TELEGRAM_TOPICS_SPEECH_NORMALIZE", "1")
    monkeypatch.setenv("HERDR_TELEGRAM_TOPICS_SPEECH_NORMALIZE_FILTER", "loudnorm=I=-20")
    assert speech._stt_normalize_filter() == "loudnorm=I=-20"


# --- chunking ----------------------------------------------------------------

class _FakeStream:
    def __init__(self, sink):
        self._sink = sink
        self.result = type("R", (), {"text": ""})()

    def accept_waveform(self, rate, samples):
        self._sink.append(len(samples))
        self.result = type("R", (), {"text": f"seg{len(self._sink)}"})()


class _FakeRecognizer:
    def __init__(self):
        self.window_lengths: list[int] = []

    def create_stream(self):
        return _FakeStream(self.window_lengths)

    def decode_stream(self, stream):
        return None


def _run_transcribe(monkeypatch, sample_count):
    rec = _FakeRecognizer()
    monkeypatch.setenv("HERDR_TELEGRAM_TOPICS_SPEECH_CHUNK_SECONDS", "14")
    # array.array("f") path (no numpy needed): _decode_to_pcm returns raw f32 bytes.
    pcm = bytearray(sample_count * 4)  # sample_count float32 samples
    with patch.object(speech, "_load_stt", return_value=rec), \
            patch.object(speech, "_decode_to_pcm", return_value=bytes(pcm)), \
            patch.dict("sys.modules", {"numpy": None}):   # force the array.array fallback branch
        text = speech.transcribe("ignored.ogg")
    return rec, text


def test_transcribe_short_clip_single_pass(monkeypatch):
    rec, text = _run_transcribe(monkeypatch, 10 * STT_SAMPLE_RATE)   # 10s <= 14s
    assert len(rec.window_lengths) == 1                              # one decode pass
    assert text == "seg1"


def test_transcribe_long_clip_is_windowed(monkeypatch):
    rec, text = _run_transcribe(monkeypatch, 40 * STT_SAMPLE_RATE)   # 40s -> multiple windows
    chunk = 14 * STT_SAMPLE_RATE
    assert len(rec.window_lengths) > 1                               # windowed, not one pass
    assert max(rec.window_lengths) <= chunk                          # no window exceeds the cap
    assert text.startswith("seg")                                    # joined non-empty parts


def test_transcribe_tail_window_is_full_size(monkeypatch):
    # Regression: a 32s note used to end with a ~5s trailing window, which parakeet transcribes to
    # EMPTY, silently dropping the end of the message. Every window must now be a full `chunk` so the
    # tail is captured (windows are spaced evenly and the last ends exactly at n).
    rec, _text = _run_transcribe(monkeypatch, 32 * STT_SAMPLE_RATE)
    chunk = 14 * STT_SAMPLE_RATE
    assert rec.window_lengths, "expected windowed decode"
    assert all(w == chunk for w in rec.window_lengths)              # NO short trailing sliver
    assert len(rec.window_lengths) >= 2


def test_join_transcript_chunks_dedups_overlap():
    # Consecutive windows re-decode a few overlapping seconds; the shared >=3-word run is trimmed.
    joined = speech._join_transcript_chunks([
        "the cat sat on the mat",
        "on the mat and then left",       # "on the mat" overlaps -> trimmed
    ])
    assert joined == "the cat sat on the mat and then left"
    # no clean overlap -> plain space-join (a little duplication beats a dropped tail)
    assert speech._join_transcript_chunks(["hello world", "foo bar"]) == "hello world foo bar"
    # empties skipped
    assert speech._join_transcript_chunks(["", "only this", ""]) == "only this"


# --- loudnorm bare-retry -----------------------------------------------------

def test_decode_to_pcm_retries_without_filter(monkeypatch, tmp_path):
    monkeypatch.setenv("HERDR_TELEGRAM_TOPICS_SPEECH_NORMALIZE", "1")
    calls = []

    def fake_run(cmd, **_kwargs):
        calls.append(cmd)
        filtered = "-af" in cmd
        if filtered:
            return subprocess.CompletedProcess(cmd, 1, stdout=b"", stderr=b"loudnorm boom")
        return subprocess.CompletedProcess(cmd, 0, stdout=b"PCMDATA", stderr=b"")

    with patch.object(speech, "_ffmpeg", return_value="/usr/bin/ffmpeg"), \
            patch("subprocess.run", side_effect=fake_run):
        out = speech._decode_to_pcm(tmp_path / "note.ogg")

    assert out == b"PCMDATA"
    assert len(calls) == 2
    assert "-af" in calls[0] and "loudnorm=I=-16:TP=-1.5:LRA=11" in calls[0]   # first: filtered
    assert "-af" not in calls[1]                                               # retry: bare


def test_decode_to_pcm_no_retry_when_normalize_off(monkeypatch, tmp_path):
    monkeypatch.setenv("HERDR_TELEGRAM_TOPICS_SPEECH_NORMALIZE", "0")
    calls = []

    def fake_run(cmd, **_kwargs):
        calls.append(cmd)
        return subprocess.CompletedProcess(cmd, 1, stdout=b"", stderr=b"boom")

    with patch.object(speech, "_ffmpeg", return_value="/usr/bin/ffmpeg"), \
            patch("subprocess.run", side_effect=fake_run):
        out = speech._decode_to_pcm(tmp_path / "note.ogg")

    assert out is None
    assert len(calls) == 1               # normalize off -> no filter -> no retry
    assert "-af" not in calls[0]
