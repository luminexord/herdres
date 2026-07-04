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
import sys
import time
from pathlib import Path

# Default model ids (match orca's catalog; downloaded by `herdres speech install`).
DEFAULT_STT_MODEL = "parakeet-tdt-0.6b-v3-int8"
DEFAULT_TTS_MODEL = "kokoro-en-v0_19"
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

# TTS model catalog — sherpa-onnx tts-models releases (tar.bz2). Kokoro needs the espeak-ng-data
# DIRECTORY (not just files), so these are extracted wholesale (the whole archive_subdir is moved
# into the model dir), not file-by-file. No SHA is published for the tts-models release, so the
# pinned HTTPS GitHub URL is the trust anchor (sha verified only if present in the spec).
TTS_MODELS: dict[str, dict] = {
    "kokoro-en-v0_19": {
        "url": "https://github.com/k2-fsa/sherpa-onnx/releases/download/tts-models/kokoro-en-v0_19.tar.bz2",
        "sha256": "",  # not published for tts-models; pinned URL is the trust anchor
        "archive_subdir": "kokoro-en-v0_19",
        "engine": "kokoro",
        "sample_rate": 24000,
        # files the model dir must contain after install (espeak-ng-data is a required directory)
        "present": ["model.onnx", "voices.bin", "tokens.txt", "espeak-ng-data"],
    },
}

# Lazy, process-global recognizer (kept warm in a long-lived process: the sidecar / embedded gateway).
# We re-attempt the (cheap) import + model-file checks every call so that `herdres speech install` /
# `pip install sherpa-onnx` take effect WITHOUT restarting the process; only a model that is present
# but fails to LOAD is cached as failed, to avoid repaying an expensive broken load every call.
_STT_RECOGNIZER: object | None = None
_STT_LOAD_FAILED = False
_TTS_ENGINE: object | None = None
_TTS_LOAD_FAILED = False


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


def speech_reply_trigger() -> str:
    """The phrase that opts a single reply into voice (per-message, no global flag). Default
    "reply by voice"; set HERDR_TELEGRAM_TOPICS_SPEECH_REPLY_TRIGGER to "" to disable the keyword."""
    return os.getenv("HERDR_TELEGRAM_TOPICS_SPEECH_REPLY_TRIGGER", "reply by voice").strip()


def speech_reply_triggered(user_text: str | None) -> bool:
    """True when the owner's prompt for this turn contains the trigger phrase (case-insensitive), so
    just THIS reply is spoken back. Default behaviour stays text when the phrase is absent."""
    trig = speech_reply_trigger().lower()
    return bool(trig) and trig in str(user_text or "").lower()


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


def tts_model_dir() -> Path:
    model = os.getenv("HERDR_TELEGRAM_TOPICS_SPEECH_TTS_MODEL", DEFAULT_TTS_MODEL).strip() or DEFAULT_TTS_MODEL
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
    except Exception as exc:
        _STT_LOAD_FAILED = True  # present-but-unloadable model: don't retry the costly load each call
        print(f"herdres_speech: STT model present but failed to load: {exc}", file=sys.stderr)
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
    return str(os.getenv("HERDR_TELEGRAM_TOPICS_SPEECH_NORMALIZE_FILTER", "loudnorm=I=-16:TP=-1.5:LRA=11")
               or "").strip()


def _decode_to_pcm(path: Path) -> bytes | None:
    """Decode any audio (Telegram voice = OGG/Opus) to 16 kHz mono float32 PCM via ffmpeg, with an
    optional loudness-normalization filter (rescues very quiet recordings)."""
    ff = _ffmpeg()
    if not ff:
        return None
    # -t caps decoded duration so a tiny-but-long Opus note can't expand to GBs of PCM in RAM.
    cmd = [ff, "-nostdin", "-hide_banner", "-loglevel", "error", "-i", str(path),
           "-ar", str(STT_SAMPLE_RATE), "-ac", "1", "-t", str(_max_decode_seconds())]
    norm = _stt_normalize_filter()
    if norm:
        cmd += ["-af", norm]
    cmd += ["-f", "f32le", "-"]
    try:
        proc = subprocess.run(cmd, capture_output=True, timeout=45)  # safety bound on a pathological ffmpeg hang
    except (OSError, subprocess.SubprocessError):
        return None
    if proc.returncode != 0 or not proc.stdout:
        # Normalization can fail on odd inputs; retry once without the filter before giving up.
        if norm:
            try:
                bare = [ff, "-nostdin", "-hide_banner", "-loglevel", "error", "-i", str(path),
                        "-ar", str(STT_SAMPLE_RATE), "-ac", "1", "-t", str(_max_decode_seconds()), "-f", "f32le", "-"]
                proc = subprocess.run(bare, capture_output=True, timeout=45)
            except (OSError, subprocess.SubprocessError):
                return None
        if proc.returncode != 0 or not proc.stdout:
            return None
    return proc.stdout


def _stt_chunk_samples() -> int:
    """Window length (in samples) for long-audio chunking. parakeet-tdt returns an EMPTY result once
    a single decode pass exceeds ~15s, so anything longer must be windowed. Default 14s; override with
    HERDR_TELEGRAM_TOPICS_SPEECH_CHUNK_SECONDS."""
    try:
        secs = max(1, int(os.getenv("HERDR_TELEGRAM_TOPICS_SPEECH_CHUNK_SECONDS", "14") or "14"))
    except ValueError:
        secs = 14
    return secs * STT_SAMPLE_RATE


def _decode_samples(rec, samples) -> str:
    stream = rec.create_stream()
    stream.accept_waveform(STT_SAMPLE_RATE, samples)
    rec.decode_stream(stream)
    return str(getattr(stream.result, "text", "") or "").strip()


def transcribe(path: str | Path) -> str:
    """Transcribe an audio file to text. Returns "" on ANY failure (engine/model/ffmpeg absent or
    error) — the caller degrades to "speech unavailable" and keeps the text path working.

    Long clips are windowed: parakeet-tdt returns an empty string once one decode pass runs past
    ~15s, so a 30s/60s voice note would otherwise transcribe to nothing. We split into overlapping
    windows, decode each, and join the non-empty parts."""
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
        n = len(samples)
        chunk = _stt_chunk_samples()
        # Short clip: single pass (parakeet handles it, and one pass avoids boundary artifacts).
        if n <= chunk:
            return _decode_samples(rec, samples)
        # Long clip: window it. A ~1s overlap keeps a word from being clipped exactly on a boundary;
        # duplicate boundary words are far less bad than a silently-empty transcript.
        overlap = STT_SAMPLE_RATE
        step = max(1, chunk - overlap)
        parts: list[str] = []
        i = 0
        while i < n:
            piece = _decode_samples(rec, samples[i:i + chunk])
            if piece:
                parts.append(piece)
            i += step
        return " ".join(parts).strip()
    except Exception:
        return ""


# --- engine: text-to-speech (issue #4 v2) -----------------------------------------------------

def _tts_model_spec() -> dict | None:
    model = os.getenv("HERDR_TELEGRAM_TOPICS_SPEECH_TTS_MODEL", DEFAULT_TTS_MODEL).strip() or DEFAULT_TTS_MODEL
    return TTS_MODELS.get(model)


def _tts_sample_rate() -> int:
    spec = _tts_model_spec()
    return int((spec or {}).get("sample_rate") or 24000)


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
        return None  # not downloaded yet — re-check next call
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
        _TTS_LOAD_FAILED = True  # present-but-unloadable; logged so the warm sidecar isn't silent
        print(f"herdres_speech: TTS model present but failed to load: {exc}", file=sys.stderr)
    return _TTS_ENGINE


def _tts_sid() -> int:
    try:
        return max(0, int(os.getenv("HERDR_TELEGRAM_TOPICS_SPEECH_TTS_SID", "0") or "0"))
    except ValueError:
        return 0


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
            import numpy as np
            samples = np.asarray(audio.samples, dtype=np.float32)
            pcm = samples.tobytes()
        except Exception:
            import array
            pcm = array.array("f", list(audio.samples)).tobytes()
        sr = int(getattr(audio, "sample_rate", 0) or _tts_sample_rate())
        return _encode_pcm_to_ogg(pcm, sr, Path(dest))
    except Exception:
        return False


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


def tts_model_present() -> bool:
    spec = _tts_model_spec()
    if not spec:
        return False
    d = tts_model_dir()
    for name in spec["present"]:
        p = d / name
        if p.is_dir():
            if not any(p.iterdir()):  # e.g. espeak-ng-data/ must be a NON-EMPTY dir, not just present
                return False
        elif not p.is_file():
            return False
    return True


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
        "tts_model": tts_model_present(),
        "tts_model_dir": str(tts_model_dir()),
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


def install_tts_model(*, force: bool = False, log=print) -> tuple[bool, str]:
    """Download + extract the configured TTS model (Kokoro) into tts_model_dir(). Unlike the STT
    model this copies the WHOLE archive subdir (Kokoro needs the espeak-ng-data/ directory). The
    tts-models release publishes no SHA, so verification runs only if a sha256 is pinned in the spec;
    the pinned HTTPS URL is the trust anchor. Extraction uses the data filter to refuse unsafe members."""
    spec = _tts_model_spec()
    if not spec:
        return (False, f"unknown TTS model: {os.getenv('HERDR_TELEGRAM_TOPICS_SPEECH_TTS_MODEL', DEFAULT_TTS_MODEL)}")
    dest = tts_model_dir()
    if tts_model_present() and not force:
        return (True, f"already present at {dest}")
    import hashlib
    import shutil
    import tarfile
    import tempfile
    import urllib.request
    log(f"downloading {spec['url']} (large — this is the Kokoro voice model)")
    with tempfile.TemporaryDirectory() as td:
        archive = Path(td) / "model.tar.bz2"
        try:
            with urllib.request.urlopen(spec["url"], timeout=600) as resp, open(archive, "wb") as fh:
                h = hashlib.sha256()
                while True:
                    chunk = resp.read(262144)
                    if not chunk:
                        break
                    h.update(chunk)
                    fh.write(chunk)
        except Exception as exc:
            return (False, f"download failed: {exc}")
        if spec.get("sha256") and h.hexdigest() != spec["sha256"]:
            return (False, f"checksum mismatch (expected {spec['sha256']}, got {h.hexdigest()})")
        try:
            with tarfile.open(archive, "r:bz2") as tar:
                try:
                    tar.extractall(td, filter="data")
                except TypeError:
                    tar.extractall(td)
        except Exception as exc:
            return (False, f"extract failed: {exc}")
        src = Path(td) / spec["archive_subdir"]
        if not src.is_dir():
            return (False, f"archive missing expected dir: {spec['archive_subdir']}")
        dest.mkdir(parents=True, exist_ok=True)
        for item in src.iterdir():  # copy the model files + the espeak-ng-data/ dir
            target = dest / item.name
            if item.is_dir():
                shutil.copytree(item, target, dirs_exist_ok=True)
            else:
                shutil.copy2(item, target)
    if not tts_model_present():
        return (False, "install incomplete (expected model files/dirs missing after extract)")
    return (True, f"installed to {dest}")


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
        return {"text": transcribe(payload.get("path", ""))}  # in-process fallback (v1 behaviour)
    if endpoint == "tts":
        # TTS goes ONLY through the warm sidecar (which synthesizes asynchronously, off the caller's
        # lock). With no sidecar we do NOT synthesize in-process — that would block the global sync
        # lock for seconds. So the spoken reply requires `herdres-speech.service` to be running.
        return {"ok": False}
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
