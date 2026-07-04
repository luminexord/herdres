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
import shutil
import subprocess
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


def check() -> dict[str, Any]:
    return {
        "sherpa_onnx": sherpa_available(),
        "ffmpeg": bool(_ffmpeg()),
        "stt_model": stt_model_present(),
        "stt_model_dir": str(stt_model_dir()),
        "input_enabled": speech_input_enabled(),
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


def _decode_to_pcm(path: Path) -> bytes | None:
    ffmpeg = _ffmpeg()
    if not ffmpeg:
        return None
    try:
        proc = subprocess.run(
            [
                ffmpeg,
                "-nostdin",
                "-hide_banner",
                "-loglevel",
                "error",
                "-i",
                str(path),
                "-ar",
                str(STT_SAMPLE_RATE),
                "-ac",
                "1",
                "-t",
                str(_max_decode_seconds()),
                "-f",
                "f32le",
                "-",
            ],
            capture_output=True,
            timeout=45,
            check=False,
        )
    except (OSError, subprocess.SubprocessError):
        return None
    if proc.returncode != 0 or not proc.stdout:
        return None
    return proc.stdout


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
        stream = recognizer.create_stream()
        stream.accept_waveform(STT_SAMPLE_RATE, samples)
        recognizer.decode_stream(stream)
        return str(getattr(stream.result, "text", "") or "").strip()
    except Exception:
        return ""


def speech_request(endpoint: str, payload: dict[str, Any]) -> dict[str, Any]:
    if endpoint == "stt":
        return {"text": transcribe(payload.get("path", ""))}
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
