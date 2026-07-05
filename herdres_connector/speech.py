"""Optional inbound speech-to-text for Telegram voice notes.

This is the slim source-mode extraction of old Herdres speech input:
Telegram audio is downloaded by the gateway with the bot token that received
the update, transcribed locally when enabled, then submitted to Tendwire as
plain text. Speech is strictly optional and fail-open.
"""

from __future__ import annotations

import array
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any

from . import config
from .safe import sanitize_text


DEFAULT_STT_MODEL = "parakeet-tdt-0.6b-v3-int8"
STT_SAMPLE_RATE = 16000
ATTACHMENT_MAX_BYTES = 20 * 1024 * 1024
ATTACHMENT_CHUNK_BYTES = 1024 * 1024
ATTACHMENT_DOWNLOAD_TIMEOUT = 45.0
ATTACHMENT_READ_TIMEOUT = 20.0

STT_MODELS: dict[str, dict[str, Any]] = {
    "parakeet-tdt-0.6b-v3-int8": {
        "url": "https://github.com/k2-fsa/sherpa-onnx/releases/download/asr-models/sherpa-onnx-nemo-parakeet-tdt-0.6b-v3-int8.tar.bz2",
        "sha256": "5793d0fd397c5778d2cf2126994d58e9d56b1be7c04d13c7a15bb1b4eafb16bf",
        "archive_subdir": "sherpa-onnx-nemo-parakeet-tdt-0.6b-v3-int8",
        "files": ["encoder.int8.onnx", "decoder.int8.onnx", "joiner.int8.onnx", "tokens.txt"],
    },
    "parakeet-tdt-0.6b-v2-int8": {
        "url": "https://github.com/k2-fsa/sherpa-onnx/releases/download/asr-models/sherpa-onnx-nemo-parakeet-tdt-0.6b-v2-int8.tar.bz2",
        "sha256": "157c157bc51155e03e37d2466522a3a737dd9c72bb25f36eb18912964161e1ad",
        "archive_subdir": "sherpa-onnx-nemo-parakeet-tdt-0.6b-v2-int8",
        "files": ["encoder.int8.onnx", "decoder.int8.onnx", "joiner.int8.onnx", "tokens.txt"],
    },
}

_STT_RECOGNIZER: object | None = None
_STT_LOAD_FAILED = False

# --- outbound TTS (issue #4: speak the agent's reply back as a Telegram voice note) ---
DEFAULT_TTS_MODEL = "kokoro-en-v0_19"
TTS_MODELS: dict[str, dict[str, Any]] = {
    "kokoro-en-v0_19": {
        "url": "https://github.com/k2-fsa/sherpa-onnx/releases/download/tts-models/kokoro-en-v0_19.tar.bz2",
        "sha256": "",  # not published for tts-models; the pinned URL is the trust anchor
        "archive_subdir": "kokoro-en-v0_19",
        "engine": "kokoro",
        "sample_rate": 24000,
        "present": ["model.onnx", "voices.bin", "tokens.txt", "espeak-ng-data"],
    },
}
_TTS_ENGINE: object | None = None
_TTS_LOAD_FAILED = False

_CODE_FENCE_RE = re.compile(r"```.*?```", re.DOTALL)
_INLINE_CODE_RE = re.compile(r"`[^`]*`")
_URL_RE = re.compile(r"https?://\S+")


def _flag(name: str, default: str = "0") -> bool:
    return str(os.getenv(name, default) or default).strip().lower() in {"1", "true", "yes", "on"}


def speech_input_enabled() -> bool:
    return _flag("HERDR_TELEGRAM_TOPICS_SPEECH_INPUT", "0")


def speech_models_dir() -> Path:
    override = os.getenv("HERDR_TELEGRAM_TOPICS_SPEECH_MODELS_DIR", "").strip()
    if override:
        return Path(override).expanduser()
    return config.state_path().parent / "speech-models"


def stt_model_dir() -> Path:
    model = os.getenv("HERDR_TELEGRAM_TOPICS_SPEECH_STT_MODEL", DEFAULT_STT_MODEL).strip() or DEFAULT_STT_MODEL
    return speech_models_dir() / model


def speech_temp_dir() -> Path:
    base = config.state_path().parent / "speech-input"
    base.mkdir(parents=True, exist_ok=True)
    try:
        base.chmod(0o700)
    except OSError:
        pass
    return base


def _ffmpeg() -> str | None:
    return shutil.which(os.getenv("HERDR_TELEGRAM_TOPICS_SPEECH_FFMPEG", "ffmpeg") or "ffmpeg")


def _stt_model_spec() -> dict[str, Any] | None:
    model = os.getenv("HERDR_TELEGRAM_TOPICS_SPEECH_STT_MODEL", DEFAULT_STT_MODEL).strip() or DEFAULT_STT_MODEL
    spec = STT_MODELS.get(model)
    return spec if isinstance(spec, dict) else None


def stt_model_present() -> bool:
    spec = _stt_model_spec()
    if not spec:
        return False
    return all((stt_model_dir() / str(name)).is_file() for name in spec.get("files") or ())


def sherpa_available() -> bool:
    try:
        import sherpa_onnx  # noqa: F401

        return True
    except Exception:
        return False


def _tts_model_spec() -> dict[str, Any] | None:
    model = os.getenv("HERDR_TELEGRAM_TOPICS_SPEECH_TTS_MODEL", DEFAULT_TTS_MODEL).strip() or DEFAULT_TTS_MODEL
    spec = TTS_MODELS.get(model)
    return spec if isinstance(spec, dict) else None


def tts_model_dir() -> Path:
    model = os.getenv("HERDR_TELEGRAM_TOPICS_SPEECH_TTS_MODEL", DEFAULT_TTS_MODEL).strip() or DEFAULT_TTS_MODEL
    return speech_models_dir() / model


def tts_model_present() -> bool:
    spec = _tts_model_spec()
    if not spec:
        return False
    d = tts_model_dir()
    return all((d / str(name)).exists() for name in spec.get("present") or ())


def _tts_sample_rate() -> int:
    return int((_tts_model_spec() or {}).get("sample_rate") or 24000)


def _tts_sid() -> int:
    try:
        return max(0, int(os.getenv("HERDR_TELEGRAM_TOPICS_SPEECH_TTS_SID", "0") or "0"))
    except ValueError:
        return 0


def _unlink_quietly(path: Path) -> None:
    try:
        path.unlink()
    except OSError:
        pass


def _prune_speech_dir(base: Path, *, keep: int = 64, part_stale_seconds: int = 600) -> None:
    """Bound the outbound-speech dir: keep the newest `keep` finished .ogg files and sweep only STALE
    .part files, so synthesized replies don't accumulate unbounded in the long-lived process."""
    try:
        entries = [p for p in base.iterdir() if p.is_file() and not p.is_symlink()]
    except OSError:
        return
    now = time.time()

    def _mtime(p: Path) -> float:
        try:
            return p.stat().st_mtime
        except OSError:
            return 0.0

    for part in (p for p in entries if p.name.endswith(".part")):
        if now - _mtime(part) > part_stale_seconds:
            _unlink_quietly(part)
    finals = [p for p in entries if p.name.endswith(".ogg")]
    for stale in sorted(finals, key=_mtime, reverse=True)[keep:]:
        _unlink_quietly(stale)


def outbound_speech_dir(*, prune: bool = False) -> Path:
    base = config.state_path().parent / "outbound-speech"
    if base.is_symlink():
        raise RuntimeError("outbound speech directory path is unsafe (symlink)")
    base.mkdir(parents=True, exist_ok=True)
    try:
        base.chmod(0o700)
    except OSError:
        pass
    if prune:
        _prune_speech_dir(base)
    return base


def speech_replies_enabled() -> bool:
    """Force EVERY reply to be spoken back (issue #4). Default off; the per-turn triggers below
    (reply-to-voice auto-mode, the trigger phrase) are the normal opt-in."""
    return _flag("HERDR_TELEGRAM_TOPICS_SPEECH_REPLIES", "0")


def speech_reply_on_voice_reply_enabled() -> bool:
    """When the owner replies to one of the agent's voice notes, speak the next reply back too."""
    return _flag("HERDR_TELEGRAM_TOPICS_SPEECH_REPLY_ON_VOICE_REPLY", "1")


def speech_reply_trigger() -> str:
    return os.getenv("HERDR_TELEGRAM_TOPICS_SPEECH_REPLY_TRIGGER", "reply by voice").strip()


def speech_reply_triggered(user_text: str | None) -> bool:
    """True when the owner's prompt contains the trigger phrase, so just THIS reply is spoken."""
    trig = speech_reply_trigger().lower()
    return bool(trig) and trig in str(user_text or "").lower()


def strip_speech_reply_trigger(text: str) -> str:
    """Remove one occurrence of the trigger phrase (plus adjacent punctuation) from an instruction.
    The phrase is a BRIDGE directive ("speak the reply back"), not part of the task — delivering it to
    the agent makes the agent think IT must produce audio (e.g. it starts installing TTS)."""
    trig = speech_reply_trigger()
    raw = str(text or "")
    if not trig:
        return raw.strip()
    pattern = re.compile(r"[\s,;:.!?\-–—]*" + re.escape(trig) + r"[\s,;:.!?\-–—]*", re.IGNORECASE)
    return pattern.sub(" ", raw, count=1).strip()


def speech_reply_max_chars() -> int:
    try:
        return max(1, int(os.getenv("HERDR_TELEGRAM_TOPICS_SPEECH_REPLY_MAX_CHARS", "600") or "600"))
    except ValueError:
        return 600


def _clean_for_speech(text: str) -> str:
    """Strip code blocks / inline code / URLs / markdown so TTS reads prose, not code aloud."""
    s = _CODE_FENCE_RE.sub(" (code omitted) ", str(text or ""))
    s = _INLINE_CODE_RE.sub(lambda m: m.group(0).strip("`"), s)
    s = _URL_RE.sub("a link", s)
    s = re.sub(r"[*_#>`]+", "", s)
    return re.sub(r"\s+", " ", s).strip()


def _cut_at_boundary(s: str, max_chars: int) -> tuple[str, str]:
    """Split `s` into a ≤max_chars head (preferring a sentence, else word, boundary) and the rest."""
    if len(s) <= max_chars:
        return s, ""
    cut = s[:max_chars]
    boundary = max(cut.rfind(". "), cut.rfind("! "), cut.rfind("? "))
    if boundary <= max_chars // 2:
        boundary = cut.rfind(" ")            # no sentence break — fall back to a word break
    idx = boundary + 1 if boundary > 0 else max_chars
    return s[:idx].strip(), s[idx:].strip()


def trim_for_speech(text: str, *, max_chars: int | None = None) -> str:
    """Clean an agent reply and cap it to a single speakable chunk on a sentence boundary."""
    if max_chars is None:
        max_chars = speech_reply_max_chars()
    head, _rest = _cut_at_boundary(_clean_for_speech(text), max_chars)
    return head


def speech_reply_max_chunks() -> int:
    """Cap the number of voice notes one reply is split into, so a long reply can't flood the topic."""
    try:
        return max(1, int(os.getenv("HERDR_TELEGRAM_TOPICS_SPEECH_REPLY_MAX_CHUNKS", "5") or "5"))
    except ValueError:
        return 5


def speech_reply_chunks(text: str, *, max_chars: int | None = None, max_chunks: int | None = None) -> list[str]:
    """Split a cleaned reply into up to `max_chunks` speakable chunks of ≤`max_chars`, so a long answer
    is spoken as several voice notes instead of being truncated to one."""
    if max_chars is None:
        max_chars = speech_reply_max_chars()
    if max_chunks is None:
        max_chunks = speech_reply_max_chunks()
    s = _clean_for_speech(text)
    chunks: list[str] = []
    while s and len(chunks) < max_chunks:
        head, s = _cut_at_boundary(s, max_chars)
        if head:
            chunks.append(head)
    return chunks


def _load_tts():
    """Return the warm Kokoro TTS engine, or None if anything is unavailable. Same re-attempt policy
    as _load_stt: cheap retries until a PRESENT-but-unloadable model is cached as failed."""
    global _TTS_ENGINE, _TTS_LOAD_FAILED
    if _TTS_ENGINE is not None:
        return _TTS_ENGINE
    if _TTS_LOAD_FAILED:
        return None
    try:
        import sherpa_onnx  # type: ignore
    except Exception:
        return None
    d = tts_model_dir()
    model, voices, tokens, data = (d / "model.onnx", d / "voices.bin", d / "tokens.txt", d / "espeak-ng-data")
    if not (model.is_file() and voices.is_file() and tokens.is_file() and data.is_dir()):
        return None  # not installed yet — re-check next call
    try:
        threads = max(1, int(os.getenv("HERDR_TELEGRAM_TOPICS_SPEECH_THREADS", "2") or "2"))
        cfg = sherpa_onnx.OfflineTtsConfig(
            model=sherpa_onnx.OfflineTtsModelConfig(
                kokoro=sherpa_onnx.OfflineTtsKokoroModelConfig(
                    model=str(model), voices=str(voices), tokens=str(tokens), data_dir=str(data)),
                num_threads=threads, provider="cpu",
            ),
        )
        _TTS_ENGINE = sherpa_onnx.OfflineTts(cfg)
    except Exception as exc:
        _TTS_LOAD_FAILED = True  # present-but-unloadable; log so the warm process isn't silent
        print(f"herdres speech: TTS model present but failed to load: {exc}", file=sys.stderr)
    return _TTS_ENGINE


def _encode_pcm_to_ogg(pcm: bytes, sample_rate: int, dest: Path) -> bool:
    """Encode raw float32-LE mono PCM to an OGG/Opus file (what Telegram sendVoice wants) via ffmpeg."""
    ff = _ffmpeg()
    if not ff:
        return False
    try:
        proc = subprocess.run(
            [ff, "-nostdin", "-hide_banner", "-loglevel", "error", "-y",
             "-f", "f32le", "-ar", str(sample_rate), "-ac", "1", "-i", "pipe:0",
             "-c:a", "libopus", "-b:a", "32k", "-f", "ogg", str(dest)],
            input=pcm, capture_output=True, timeout=60,
        )
    except (OSError, subprocess.SubprocessError):
        return False
    return proc.returncode == 0 and dest.is_file() and dest.stat().st_size > 0


def synthesize(text: str, dest: str | Path) -> bool:
    """Synthesize `text` to an OGG/Opus voice file at `dest`. Returns False on ANY failure (engine/
    model/ffmpeg absent or error) — the caller just skips the spoken reply; the text turn is untouched."""
    spoken = (text or "").strip()
    if not spoken:
        return False
    engine = _load_tts()
    if engine is None:
        return False
    try:
        audio = engine.generate(spoken, sid=_tts_sid(), speed=1.0)
        try:
            import numpy as np  # type: ignore

            pcm = np.asarray(audio.samples, dtype=np.float32).tobytes()
        except Exception:
            pcm = array.array("f", list(audio.samples)).tobytes()
        sr = int(getattr(audio, "sample_rate", 0) or _tts_sample_rate())
        return _encode_pcm_to_ogg(pcm, sr, Path(dest))
    except Exception:
        return False


def check() -> dict[str, Any]:
    return {
        "sherpa_onnx": sherpa_available(),
        "ffmpeg": bool(_ffmpeg()),
        "stt_model": stt_model_present(),
        "stt_model_dir": str(stt_model_dir()),
        "input_enabled": speech_input_enabled(),
        "tts_model": tts_model_present(),
        "tts_model_dir": str(tts_model_dir()),
    }


def _load_stt() -> object | None:
    global _STT_RECOGNIZER, _STT_LOAD_FAILED
    if _STT_RECOGNIZER is not None:
        return _STT_RECOGNIZER
    if _STT_LOAD_FAILED:
        return None
    try:
        import sherpa_onnx  # type: ignore
    except Exception:
        return None
    d = stt_model_dir()
    enc = d / "encoder.int8.onnx"
    dec = d / "decoder.int8.onnx"
    joiner = d / "joiner.int8.onnx"
    tokens = d / "tokens.txt"
    if not all(path.is_file() for path in (enc, dec, joiner, tokens)):
        return None
    try:
        threads = max(1, int(os.getenv("HERDR_TELEGRAM_TOPICS_SPEECH_THREADS", "2") or "2"))
        _STT_RECOGNIZER = sherpa_onnx.OfflineRecognizer.from_transducer(
            encoder=str(enc),
            decoder=str(dec),
            joiner=str(joiner),
            tokens=str(tokens),
            num_threads=threads,
            sample_rate=STT_SAMPLE_RATE,
            feature_dim=80,
            decoding_method="greedy_search",
            model_type="nemo_transducer",
        )
    except Exception:
        _STT_LOAD_FAILED = True
    return _STT_RECOGNIZER


def _max_decode_seconds() -> int:
    try:
        return max(1, int(os.getenv("HERDR_TELEGRAM_TOPICS_SPEECH_MAX_SECONDS", "600") or "600"))
    except ValueError:
        return 600


def _stt_normalize_filter() -> str:
    """ffmpeg audio filter applied before STT. Telegram voice notes are often recorded very quietly
    (mean loudness well below -40 dB), which parakeet transcribes to nothing; loudnorm brings the
    signal up to a consistent level. Set HERDR_TELEGRAM_TOPICS_SPEECH_NORMALIZE=0 to disable."""
    raw = str(os.getenv("HERDR_TELEGRAM_TOPICS_SPEECH_NORMALIZE", "1") or "1").strip().lower()
    if raw in {"0", "false", "no", "off"}:
        return ""
    return str(
        os.getenv("HERDR_TELEGRAM_TOPICS_SPEECH_NORMALIZE_FILTER", "loudnorm=I=-16:TP=-1.5:LRA=11") or ""
    ).strip()


def _decode_to_pcm(path: Path) -> bytes | None:
    ffmpeg = _ffmpeg()
    if not ffmpeg:
        return None
    norm = _stt_normalize_filter()

    def _cmd(with_filter: bool) -> list[str]:
        cmd = [ffmpeg, "-nostdin", "-hide_banner", "-loglevel", "error", "-i", str(path),
               "-ar", str(STT_SAMPLE_RATE), "-ac", "1", "-t", str(_max_decode_seconds())]
        if with_filter and norm:
            cmd += ["-af", norm]
        cmd += ["-f", "f32le", "-"]
        return cmd

    try:
        proc = subprocess.run(_cmd(True), capture_output=True, timeout=45, check=False)
    except (OSError, subprocess.SubprocessError):
        return None
    if proc.returncode != 0 or not proc.stdout:
        # loudnorm can fail on odd/short inputs; retry once without the filter before giving up.
        if not norm:
            return None
        try:
            proc = subprocess.run(_cmd(False), capture_output=True, timeout=45, check=False)
        except (OSError, subprocess.SubprocessError):
            return None
        if proc.returncode != 0 or not proc.stdout:
            return None
    return proc.stdout


def _stt_chunk_samples() -> int:
    """Window length (in samples) for long-audio chunking. parakeet-tdt returns an EMPTY result once a
    single decode pass exceeds ~15s, so anything longer must be windowed. Default 14s; override with
    HERDR_TELEGRAM_TOPICS_SPEECH_CHUNK_SECONDS."""
    try:
        secs = max(1, int(os.getenv("HERDR_TELEGRAM_TOPICS_SPEECH_CHUNK_SECONDS", "14") or "14"))
    except ValueError:
        secs = 14
    return secs * STT_SAMPLE_RATE


def _decode_samples(recognizer, samples) -> str:
    stream = recognizer.create_stream()
    stream.accept_waveform(STT_SAMPLE_RATE, samples)
    recognizer.decode_stream(stream)
    return str(getattr(stream.result, "text", "") or "").strip()


def transcribe(path: str | Path) -> str:
    recognizer = _load_stt()
    if recognizer is None:
        return ""
    pcm = _decode_to_pcm(Path(path))
    if not pcm:
        return ""
    try:
        try:
            import numpy as np  # type: ignore

            samples = np.frombuffer(pcm, dtype=np.float32)
        except Exception:
            samples = array.array("f")
            samples.frombytes(pcm)
        n = len(samples)
        chunk = _stt_chunk_samples()
        # Short clip: single pass (parakeet handles it, and one pass avoids boundary artifacts).
        if n <= chunk:
            return _decode_samples(recognizer, samples)
        # Long clip: window it. A ~1s overlap keeps a word from being clipped exactly on a boundary;
        # a duplicated boundary word is far less bad than a silently-empty transcript.
        overlap = STT_SAMPLE_RATE
        step = max(1, chunk - overlap)
        parts: list[str] = []
        i = 0
        while i < n:
            piece = _decode_samples(recognizer, samples[i:i + chunk])
            if piece:
                parts.append(piece)
            i += step
        return " ".join(parts).strip()
    except Exception:
        return ""


def speech_request(endpoint: str, payload: dict[str, Any]) -> dict[str, Any]:
    if endpoint == "stt":
        return {"text": transcribe(payload.get("path", ""))}
    if endpoint == "tts":
        # No speech sidecar in the source-only RC: synthesize in-process (Kokoro, ~1-3s). In Phase 1
        # this runs under the caller's state lock (source_sync._speak_reply); moving it off-lock is a
        # Phase 2 item (needs commit-before/reload-after vs the inter-turn yield barrier).
        dest = str(payload.get("dest") or "")
        ok = bool(dest) and synthesize(payload.get("text", ""), dest)
        return {"ok": ok, "path": dest if ok else ""}
    return {}


def voice_attachment_from_message(message: dict[str, Any]) -> dict[str, Any] | None:
    try:
        voice = message.get("voice")
        if isinstance(voice, dict):
            file_id = str(voice.get("file_id") or "")
            if file_id:
                return {
                    "kind": "voice",
                    "file_id": file_id,
                    "file_unique_id": str(voice.get("file_unique_id") or ""),
                    "file_name": "",
                    "mime_type": str(voice.get("mime_type") or "audio/ogg"),
                    "file_size": int(voice.get("file_size") or 0),
                    "duration": int(voice.get("duration") or 0),
                }
        audio = message.get("audio")
        if isinstance(audio, dict):
            file_id = str(audio.get("file_id") or "")
            if file_id:
                return {
                    "kind": "audio",
                    "file_id": file_id,
                    "file_unique_id": str(audio.get("file_unique_id") or ""),
                    "file_name": str(audio.get("file_name") or ""),
                    "mime_type": str(audio.get("mime_type") or "audio/mpeg"),
                    "file_size": int(audio.get("file_size") or 0),
                    "duration": int(audio.get("duration") or 0),
                }
    except Exception:
        return None
    return None


def is_voice_payload(payload: dict[str, Any]) -> bool:
    attachment = payload.get("attachment")
    return isinstance(attachment, dict) and str(attachment.get("kind") or "") in {"voice", "audio"} and bool(attachment.get("file_id"))


def telegram_get_file(bot_token: str, file_id: str) -> dict[str, Any]:
    data = urllib.parse.urlencode({"file_id": str(file_id)}).encode("utf-8")
    url = f"https://api.telegram.org/bot{bot_token}/getFile"
    with urllib.request.urlopen(url, data=data, timeout=20) as response:
        body = response.read().decode("utf-8", "replace")
    result = json.loads(body)
    if not isinstance(result, dict) or not result.get("ok"):
        raise RuntimeError(sanitize_text((result or {}).get("description") or "Telegram getFile failed", 240))
    file_info = result.get("result")
    if not isinstance(file_info, dict) or not str(file_info.get("file_path") or ""):
        raise RuntimeError("Telegram getFile returned no file path")
    return file_info


def download_telegram_file(bot_token: str, file_path: str, dest_path: Path, *, max_bytes: int = ATTACHMENT_MAX_BYTES) -> int:
    part_path = dest_path.with_name(dest_path.name + ".part")
    written = 0
    try:
        request = urllib.request.Request(
            f"https://api.telegram.org/file/bot{bot_token}/{urllib.parse.quote(str(file_path), safe='/')}",
            method="GET",
        )
        deadline = time.monotonic() + ATTACHMENT_DOWNLOAD_TIMEOUT
        with urllib.request.urlopen(request, timeout=ATTACHMENT_READ_TIMEOUT) as response, part_path.open("xb") as out:
            while True:
                if time.monotonic() > deadline:
                    raise RuntimeError("audio download exceeded the time budget")
                chunk = response.read(ATTACHMENT_CHUNK_BYTES)
                if not chunk:
                    break
                written += len(chunk)
                if written > max_bytes:
                    raise RuntimeError("audio is larger than the configured limit")
                out.write(chunk)
        os.replace(part_path, dest_path)
        return written
    except BaseException:
        for path in (part_path, dest_path):
            try:
                path.unlink()
            except OSError:
                pass
        raise


def pretranscribe_voice_payload(payload: dict[str, Any], *, bot_token: str) -> dict[str, Any]:
    if not is_voice_payload(payload):
        return payload
    try:
        if not speech_input_enabled():
            return payload
        attachment = payload.get("attachment")
        if not isinstance(attachment, dict):
            return payload
        declared = int(attachment.get("file_size") or 0)
        if declared > ATTACHMENT_MAX_BYTES:
            return payload
        fd, dest_name = tempfile.mkstemp(prefix="voice-", suffix=".ogg", dir=str(speech_temp_dir()))
        os.close(fd)
        dest = Path(dest_name)
        try:
            file_info = telegram_get_file(bot_token, str(attachment.get("file_id") or ""))
            confirmed = int(file_info.get("file_size") or 0)
            if confirmed > ATTACHMENT_MAX_BYTES:
                return payload
            download_telegram_file(bot_token, str(file_info.get("file_path") or ""), dest)
            transcript = sanitize_text(speech_request("stt", {"path": str(dest)}).get("text"), 12000).strip()
        finally:
            for path in (dest, dest.with_name(dest.name + ".part")):
                try:
                    path.unlink()
                except OSError:
                    pass
        out = dict(payload)
        out["_speech_pretranscribed"] = True
        out["_speech_transcript"] = transcript
        return out
    except (OSError, RuntimeError, urllib.error.URLError, urllib.error.HTTPError, ValueError, json.JSONDecodeError):
        return payload


def install_stt_model(*, force: bool = False, log=print) -> tuple[bool, str]:
    spec = _stt_model_spec()
    if not spec:
        return False, f"unknown STT model: {os.getenv('HERDR_TELEGRAM_TOPICS_SPEECH_STT_MODEL', DEFAULT_STT_MODEL)}"
    dest = stt_model_dir()
    if stt_model_present() and not force:
        return True, f"already present at {dest}"

    import hashlib
    import tarfile

    dest.mkdir(parents=True, exist_ok=True)
    url = str(spec["url"])
    expected_sha = str(spec["sha256"])
    log(f"downloading {url}")
    with tempfile.TemporaryDirectory() as tmp:
        archive_path = Path(tmp) / "model.tar.bz2"
        digest = hashlib.sha256()
        try:
            with urllib.request.urlopen(url, timeout=180) as response, archive_path.open("wb") as out:
                while True:
                    chunk = response.read(262144)
                    if not chunk:
                        break
                    digest.update(chunk)
                    out.write(chunk)
        except Exception as exc:
            return False, f"download failed: {sanitize_text(exc, 240)}"
        actual_sha = digest.hexdigest()
        if actual_sha != expected_sha:
            return False, f"checksum mismatch (expected {expected_sha}, got {actual_sha})"
        try:
            with tarfile.open(archive_path, "r:bz2") as tar:
                for name in spec.get("files") or ():
                    member_name = f"{spec['archive_subdir']}/{name}"
                    member = tar.getmember(member_name)
                    source = tar.extractfile(member)
                    if source is None:
                        return False, f"archive missing expected file: {name}"
                    (dest / str(name)).write_bytes(source.read())
        except Exception as exc:
            return False, f"extract failed: {sanitize_text(exc, 240)}"
    return True, f"installed {len(spec.get('files') or [])} files to {dest}"
