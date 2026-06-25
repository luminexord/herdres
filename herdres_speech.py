"""Issue #4: local, machine-agnostic speech for herdres — speech-to-text (and, in a later phase,
text-to-speech) for the Telegram voice "call" feature.

This module is the single engine + client used by both the inbound path (`command_reply` transcribes
an owner's voice note) and, later, the outbound path (sync speaks the agent's reply). It runs the
**sherpa-onnx** Python package locally (NVIDIA Parakeet for STT, Kokoro for TTS) — no cloud. Audio is
decoded/encoded with `ffmpeg`.

Design invariants:
  * **Graceful, fail-open.** Every public call returns an empty/false result (never raises) when
    sherpa-onnx, the model, or ffmpeg is missing. Callers must treat that as "speech unavailable" and
    keep the existing text path working — a speech failure must NEVER break message routing or sync.
  * **Opt-in.** Nothing loads a model unless the relevant feature flag is on AND the call is made.
  * **Runtime flag reads.** Flags are read at call time (after load_dotenv), per the herdres
    import-time-flag gotcha (see working_badge_enabled in herdres.py).
  * **`SPEECH` namespace.** `voice` already means the per-agent bot persona (voice_mode/`/voice`);
    audio uses `HERDR_TELEGRAM_TOPICS_SPEECH_*` and a `speech-models/` dir to stay orthogonal.

herdres.py imports this module best-effort (`try: import herdres_speech`); if the file is absent the
speech features are simply off. A future `herdres-speech` sidecar imports the same module to hold the
models warm and serve `speech_request()` over a Unix socket.
"""

from __future__ import annotations

import json
import os
import re
import shutil
import socket
import subprocess
import time
from pathlib import Path

# Default model ids (match orca's catalog; downloaded by `herdres speech install`).
DEFAULT_STT_MODEL = "parakeet-tdt-0.6b-v3-int8"
DEFAULT_TTS_MODEL = "kokoro"
STT_SAMPLE_RATE = 16000

# STT model catalog — sherpa-onnx release archives (tar.bz2), SHA256-verified. Mirrors orca's
# model-catalog.ts. The archive extracts to `archive_subdir/<files>`; we flatten into the model dir.
STT_MODELS: dict[str, dict] = {
    "parakeet-tdt-0.6b-v3-int8": {
        "url": "https://github.com/k2-fsa/sherpa-onnx/releases/download/asr-models/sherpa-onnx-nemo-parakeet-tdt-0.6b-v3-int8.tar.bz2",
        "sha256": "5793d0fd397c5778d2cf2126994d58e9d56b1be7c04d13c7a15bb1b4eafb16bf",
        "archive_subdir": "sherpa-onnx-nemo-parakeet-tdt-0.6b-v3-int8",
        "files": ["encoder.int8.onnx", "decoder.int8.onnx", "joiner.int8.onnx", "tokens.txt"],
    },
    "parakeet-tdt-0.6b-v2-int8": {  # English-only, faster
        "url": "https://github.com/k2-fsa/sherpa-onnx/releases/download/asr-models/sherpa-onnx-nemo-parakeet-tdt-0.6b-v2-int8.tar.bz2",
        "sha256": "157c157bc51155e03e37d2466522a3a737dd9c72bb25f36eb18912964161e1ad",
        "archive_subdir": "sherpa-onnx-nemo-parakeet-tdt-0.6b-v2-int8",
        "files": ["encoder.int8.onnx", "decoder.int8.onnx", "joiner.int8.onnx", "tokens.txt"],
    },
}

# Lazy, process-global recognizer (kept warm in a long-lived process: the sidecar / embedded gateway).
# We re-attempt the (cheap) import + model-file checks every call so that `herdres speech install` /
# `pip install sherpa-onnx` take effect WITHOUT restarting the process; only a model that is present
# but fails to LOAD is cached as failed, to avoid repaying an expensive broken load every call.
_STT_RECOGNIZER: object | None = None
_STT_LOAD_FAILED = False


# --- config (read at call time) ---------------------------------------------------------------

def _flag(name: str, default: str = "0") -> bool:
    return os.getenv(name, default).strip().lower() in {"1", "true", "yes", "on"}


def speech_input_enabled() -> bool:
    """Inbound: transcribe an owner's Telegram voice note into the pane (issue #4 v1)."""
    return _flag("HERDR_TELEGRAM_TOPICS_SPEECH_INPUT", "0")


def speech_replies_enabled() -> bool:
    """Outbound: speak the agent's reply back as a Telegram voice message (issue #4 v2)."""
    return _flag("HERDR_TELEGRAM_TOPICS_SPEECH_REPLIES", "0")


def speech_echo_transcript_enabled() -> bool:
    """Echo "🎙️ Heard: …" into the topic so the owner can see/correct what was heard."""
    return _flag("HERDR_TELEGRAM_TOPICS_SPEECH_ECHO_TRANSCRIPT", "1")


def speech_models_dir() -> Path:
    override = os.getenv("HERDR_TELEGRAM_TOPICS_SPEECH_MODELS_DIR", "").strip()
    if override:
        return Path(override).expanduser()
    # Sibling of the attachments/ tree (state_path().parent), without importing herdres.py.
    state = os.getenv("HERDR_TELEGRAM_TOPICS_STATE", "").strip()
    base = Path(state).expanduser().parent if state else (Path.home() / ".local/share/herdres")
    return base / "speech-models"


def stt_model_dir() -> Path:
    model = os.getenv("HERDR_TELEGRAM_TOPICS_SPEECH_STT_MODEL", DEFAULT_STT_MODEL).strip() or DEFAULT_STT_MODEL
    return speech_models_dir() / model


def speech_socket_path() -> Path:
    override = os.getenv("HERDR_TELEGRAM_TOPICS_SPEECH_SOCKET", "").strip()
    if override:
        return Path(override).expanduser()
    return speech_models_dir().parent / "speech.sock"


def _ffmpeg() -> str | None:
    return shutil.which(os.getenv("HERDR_TELEGRAM_TOPICS_SPEECH_FFMPEG", "ffmpeg") or "ffmpeg")


# --- engine: speech-to-text -------------------------------------------------------------------

def _load_stt():
    """Return the warm offline Parakeet recognizer, building it on first success, or None if anything
    is unavailable. Re-attempts cheaply each call (so a later install takes effect) until a model that
    is PRESENT fails to load, which is cached to avoid re-paying the expensive broken load."""
    global _STT_RECOGNIZER, _STT_LOAD_FAILED
    if _STT_RECOGNIZER is not None:
        return _STT_RECOGNIZER
    if _STT_LOAD_FAILED:
        return None
    try:
        import sherpa_onnx  # type: ignore
    except Exception:
        return None  # not installed yet — re-check next call, do not permanently cache
    d = stt_model_dir()
    enc, dec, joiner, tokens = (d / "encoder.int8.onnx", d / "decoder.int8.onnx",
                                d / "joiner.int8.onnx", d / "tokens.txt")
    if not all(p.is_file() for p in (enc, dec, joiner, tokens)):
        return None  # not downloaded yet — re-check next call (post-`herdres speech install`)
    try:
        threads = max(1, int(os.getenv("HERDR_TELEGRAM_TOPICS_SPEECH_THREADS", "2") or "2"))
        _STT_RECOGNIZER = sherpa_onnx.OfflineRecognizer.from_transducer(
            encoder=str(enc), decoder=str(dec), joiner=str(joiner), tokens=str(tokens),
            num_threads=threads, sample_rate=STT_SAMPLE_RATE, feature_dim=80,
            decoding_method="greedy_search", model_type="nemo_transducer",
        )
    except Exception:
        _STT_LOAD_FAILED = True  # present-but-unloadable model: don't retry the costly load each call
    return _STT_RECOGNIZER


def _max_decode_seconds() -> int:
    try:
        return max(1, int(os.getenv("HERDR_TELEGRAM_TOPICS_SPEECH_MAX_SECONDS", "600") or "600"))
    except ValueError:
        return 600


def _decode_to_pcm(path: Path) -> bytes | None:
    """Decode any audio (Telegram voice = OGG/Opus) to 16 kHz mono float32 PCM via ffmpeg."""
    ff = _ffmpeg()
    if not ff:
        return None
    try:
        proc = subprocess.run(
            # -t caps decoded duration so a tiny-but-long Opus note can't expand to GBs of PCM in RAM.
            [ff, "-nostdin", "-hide_banner", "-loglevel", "error", "-i", str(path),
             "-ar", str(STT_SAMPLE_RATE), "-ac", "1", "-t", str(_max_decode_seconds()), "-f", "f32le", "-"],
            capture_output=True, timeout=45,  # safety bound on a pathological ffmpeg hang
        )
    except (OSError, subprocess.SubprocessError):
        return None
    if proc.returncode != 0 or not proc.stdout:
        return None
    return proc.stdout


def transcribe(path: str | Path) -> str:
    """Transcribe an audio file to text. Returns "" on ANY failure (engine/model/ffmpeg absent or
    error) — the caller degrades to "speech unavailable" and keeps the text path working."""
    rec = _load_stt()
    if rec is None:
        return ""
    pcm = _decode_to_pcm(Path(path))
    if not pcm:
        return ""
    try:
        # numpy is NOT a hard dependency of sherpa-onnx; prefer it (fast zero-copy) but fall back to a
        # stdlib float array (sherpa's accept_waveform accepts a buffer/sequence of float32) so a host
        # with sherpa but no numpy still works.
        try:
            import numpy as np
            samples = np.frombuffer(pcm, dtype=np.float32)
        except Exception:
            import array
            samples = array.array("f")
            samples.frombytes(pcm)
        stream = rec.create_stream()
        stream.accept_waveform(STT_SAMPLE_RATE, samples)
        rec.decode_stream(stream)
        return str(getattr(stream.result, "text", "") or "").strip()
    except Exception:
        return ""


# --- preflight / install ----------------------------------------------------------------------

def _stt_model_spec() -> dict | None:
    model = os.getenv("HERDR_TELEGRAM_TOPICS_SPEECH_STT_MODEL", DEFAULT_STT_MODEL).strip() or DEFAULT_STT_MODEL
    return STT_MODELS.get(model)


def stt_model_present() -> bool:
    spec = _stt_model_spec()
    if not spec:
        return False
    d = stt_model_dir()
    return all((d / f).is_file() for f in spec["files"])


def sherpa_available() -> bool:
    try:
        import sherpa_onnx  # noqa: F401
        return True
    except Exception:
        return False


def check() -> dict:
    """Read-only preflight for `herdres speech check`."""
    return {
        "sherpa_onnx": sherpa_available(),
        "ffmpeg": bool(_ffmpeg()),
        "stt_model": stt_model_present(),
        "stt_model_dir": str(stt_model_dir()),
        "socket_present": speech_socket_path().exists(),
        "input_enabled": speech_input_enabled(),
        "replies_enabled": speech_replies_enabled(),
    }


def install_stt_model(*, force: bool = False, log=print) -> tuple[bool, str]:
    """Download + SHA256-verify + extract the configured STT model into stt_model_dir(). Only the
    declared model files are copied out of the (pinned, verified) archive, so a malicious archive
    path can't escape the model dir. Returns (ok, message)."""
    spec = _stt_model_spec()
    if not spec:
        return (False, f"unknown STT model: {os.getenv('HERDR_TELEGRAM_TOPICS_SPEECH_STT_MODEL', DEFAULT_STT_MODEL)}")
    dest = stt_model_dir()
    if stt_model_present() and not force:
        return (True, f"already present at {dest}")
    import hashlib
    import tarfile
    import tempfile
    import urllib.request
    dest.mkdir(parents=True, exist_ok=True)
    log(f"downloading {spec['url']}")
    with tempfile.TemporaryDirectory() as td:
        archive = Path(td) / "model.tar.bz2"
        try:
            with urllib.request.urlopen(spec["url"], timeout=180) as resp, open(archive, "wb") as fh:
                h = hashlib.sha256()
                while True:
                    chunk = resp.read(262144)
                    if not chunk:
                        break
                    h.update(chunk)
                    fh.write(chunk)
        except Exception as exc:
            return (False, f"download failed: {exc}")
        if h.hexdigest() != spec["sha256"]:
            return (False, f"checksum mismatch (expected {spec['sha256']}, got {h.hexdigest()})")
        try:
            with tarfile.open(archive, "r:bz2") as tar:
                try:
                    tar.extractall(td, filter="data")  # py3.12+/backports: refuse unsafe members
                except TypeError:
                    tar.extractall(td)  # older Python without the filter kwarg
        except Exception as exc:
            return (False, f"extract failed: {exc}")
        src = Path(td) / spec["archive_subdir"]
        for fname in spec["files"]:
            s = src / fname
            if not s.is_file():
                return (False, f"archive missing expected file: {fname}")
            (dest / fname).write_bytes(s.read_bytes())
    return (True, f"installed {len(spec['files'])} files to {dest}")


# --- outbound speech helpers (used by the v2 reply path; pure/cheap, safe to ship now) ---------

_CODE_FENCE_RE = re.compile(r"```.*?```", re.S)
_INLINE_CODE_RE = re.compile(r"`[^`]*`")
_URL_RE = re.compile(r"https?://\S+")


def trim_for_speech(text: str, *, max_chars: int | None = None) -> str:
    """Strip code blocks / inline code / URLs / markdown emphasis from an agent reply and cap it to a
    speakable length on a sentence boundary, so TTS reads a short answer rather than code aloud."""
    if max_chars is None:
        try:
            max_chars = int(os.getenv("HERDR_TELEGRAM_TOPICS_SPEECH_REPLY_MAX_CHARS", "600") or "600")
        except ValueError:
            max_chars = 600
    s = _CODE_FENCE_RE.sub(" (code omitted) ", str(text or ""))
    s = _INLINE_CODE_RE.sub(lambda m: m.group(0).strip("`"), s)
    s = _URL_RE.sub("a link", s)
    s = re.sub(r"[*_#>`]+", "", s)
    s = re.sub(r"\s+", " ", s).strip()
    if len(s) <= max_chars:
        return s
    cut = s[:max_chars]
    boundary = max(cut.rfind(". "), cut.rfind("! "), cut.rfind("? "))
    return (cut[: boundary + 1] if boundary > max_chars // 2 else cut).strip()


# --- client: prefer the warm sidecar, fall back to in-process ---------------------------------

def speech_request(endpoint: str, payload: dict) -> dict:
    """Call the warm `herdres-speech` sidecar over its Unix socket; on ANY failure (no sidecar yet,
    refused, timeout) fall back to running the engine in-process. Returns a dict; never raises.

    Endpoints: "stt" -> {"text": str}. (v2 will add "tts" -> {"path": str}.)
    """
    sock_path = speech_socket_path()
    if sock_path.exists():
        try:
            return _sidecar_call(sock_path, endpoint, payload)
        except Exception:
            pass  # fall through to in-process
    if endpoint == "stt":
        return {"text": transcribe(payload.get("path", ""))}
    return {}


_SIDECAR_MAX_BYTES = 16 * 1024 * 1024


def _sidecar_call(sock_path: Path, endpoint: str, payload: dict, *, timeout: float = 120.0) -> dict:
    # A TOTAL wall-clock deadline (each recv's per-op timeout shrinks toward it) + a byte cap, so a
    # dribbling / half-open / runaway sidecar raises instead of hanging — speech_request then falls
    # back in-process. This matters because command_reply holds the global lock during the call.
    body = json.dumps(payload).encode("utf-8")
    deadline = time.monotonic() + timeout
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as s:
        s.settimeout(timeout)
        s.connect(str(sock_path))
        req = (
            f"POST /{endpoint} HTTP/1.0\r\n"
            f"Content-Type: application/json\r\nContent-Length: {len(body)}\r\n\r\n"
        ).encode("utf-8") + body
        s.sendall(req)
        chunks: list[bytes] = []
        total = 0
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError("sidecar response exceeded the total deadline")
            s.settimeout(remaining)
            chunk = s.recv(65536)
            if not chunk:
                break
            chunks.append(chunk)
            total += len(chunk)
            if total > _SIDECAR_MAX_BYTES:
                raise ValueError("sidecar response too large")
    raw = b"".join(chunks)
    _, _, payload_bytes = raw.partition(b"\r\n\r\n")
    return json.loads(payload_bytes.decode("utf-8") or "{}")
