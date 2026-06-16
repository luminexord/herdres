#!/usr/bin/env python3
"""Sync Herdr panes to Telegram forum topics and handle pane-topic commands.

This is intentionally small and stdlib-only. Routine sync uses no LLM calls.
Secrets are read from environment/.env files, never persisted.
"""

from __future__ import annotations

import argparse
import datetime as _dt
import fcntl
import hashlib
import html
import json
import os
import re
import shlex
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any


DEFAULT_STATE = Path.home() / ".local/share/herdres/state.json"
DEFAULT_ENV = Path(os.getenv("HERDRES_ENV", str(Path.home() / ".config/herdres/herdres.env"))).expanduser()
DEFAULT_HERMES_ENV = Path.home() / ".hermes/.env"
DEFAULT_LOCK = Path.home() / ".local/share/herdres/sync.lock"
DEFAULT_CHAT_ID = ""
DEFAULT_GENERAL_THREAD_ID = "1"
DEFAULT_OWNER_ID = ""
DEFAULT_HERDR_BIN = "herdr"
DEFAULT_HERDR_TOPIC_ICON_COLOR = "9367192"  # 0x8EEE98, one of Telegram's allowed forum-topic colors.

MAX_CREATES_PER_RUN = int(os.getenv("HERDR_TELEGRAM_TOPICS_MAX_CREATES", "3"))
MAX_SENDS_PER_RUN = int(os.getenv("HERDR_TELEGRAM_TOPICS_MAX_SENDS", "8"))
MAX_STATUS_MARKERS_PER_RUN = int(os.getenv("HERDR_TELEGRAM_TOPICS_MAX_STATUS_MARKERS", "8"))
READ_LINES_STATUS = int(os.getenv("HERDR_TELEGRAM_TOPICS_STATUS_READ_LINES", "40"))
READ_LINES_COMMAND_DEFAULT = 80
READ_LINES_COMMAND_MAX = 160
MAX_REPLY_CHARS = 3200
MAX_STATUS_CHARS = 1500
MAX_RICH_HTML_CHARS = int(os.getenv("HERDR_TELEGRAM_TOPICS_RICH_MAX_CHARS", "14000"))
MAX_RICH_DETAIL_CHARS = int(os.getenv("HERDR_TELEGRAM_TOPICS_RICH_DETAIL_CHARS", "2400"))
PREFLIGHT_TTL_SECONDS = int(os.getenv("HERDR_TELEGRAM_TOPICS_PREFLIGHT_TTL", "900"))
PREFLIGHT_GRACE_SECONDS = int(os.getenv("HERDR_TELEGRAM_TOPICS_PREFLIGHT_GRACE", "86400"))
TOPIC_VERIFY_TTL_SECONDS = int(os.getenv("HERDR_TELEGRAM_TOPICS_VERIFY_TTL", "900"))
MAX_TOPIC_VERIFIES_PER_RUN = int(os.getenv("HERDR_TELEGRAM_TOPICS_MAX_VERIFIES", "3"))
HERDR_TOPIC_ICON_COLOR = int(os.getenv("HERDR_TELEGRAM_TOPICS_ICON_COLOR", DEFAULT_HERDR_TOPIC_ICON_COLOR))
HERDR_TOPIC_ICON_CUSTOM_EMOJI_ID = os.getenv("HERDR_TELEGRAM_TOPICS_ICON_CUSTOM_EMOJI_ID", "").strip()
STATUS_ICON_ENABLED = os.getenv("HERDR_TELEGRAM_TOPICS_STATUS_ICON", "1").lower() in {"1", "true", "yes", "on"}
STATUS_ICON_CACHE_TTL_SECONDS = int(os.getenv("HERDR_TELEGRAM_TOPICS_STATUS_ICON_CACHE_TTL", "86400"))
STATUS_ICON_RETRY_SECONDS = int(os.getenv("HERDR_TELEGRAM_TOPICS_STATUS_ICON_RETRY", "300"))
STATUS_MARKER_SUPPRESS_WHEN_ICON_OK = os.getenv(
    "HERDR_TELEGRAM_TOPICS_STATUS_MARKER_SUPPRESS_WHEN_ICON_OK",
    "1",
).lower() in {"1", "true", "yes", "on"}
CLEAN_FEED_ENABLED = os.getenv("HERDR_TELEGRAM_TOPICS_CLEAN_FEED", "1").lower() in {"1", "true", "yes", "on"}
TURN_FEED_ENABLED = os.getenv("HERDR_TELEGRAM_TOPICS_TURN_FEED", "0").lower() in {"1", "true", "yes", "on"}
RICH_MESSAGES_ENABLED = os.getenv("HERDR_TELEGRAM_TOPICS_RICH_MESSAGES", "1").lower() in {"1", "true", "yes", "on"}
LIVE_CARD_ENABLED = os.getenv("HERDR_TELEGRAM_TOPICS_LIVE_CARD", "1").lower() in {"1", "true", "yes", "on"}
STATUS_MARKER_ENABLED = os.getenv("HERDR_TELEGRAM_TOPICS_STATUS_MARKER", "1").lower() in {"1", "true", "yes", "on"}
STATUS_MARKER_DELETE_OLD = os.getenv("HERDR_TELEGRAM_TOPICS_STATUS_MARKER_DELETE_OLD", "1").lower() in {
    "1",
    "true",
    "yes",
    "on",
}
ALLOW_UNBOUNDED_REPORTS = os.getenv("HERDR_TELEGRAM_TOPICS_UNBOUNDED_REPORTS", "0").lower() in {
    "1",
    "true",
    "yes",
    "on",
}
RICH_RENDER_VERSION = 13
FEED_READ_LINES = int(os.getenv("HERDR_TELEGRAM_TOPICS_FEED_READ_LINES", "140"))
FEED_MAX_CHARS = int(os.getenv("HERDR_TELEGRAM_TOPICS_FEED_MAX_CHARS", "9000"))
FINAL_REPLY_MAX_CHARS = int(os.getenv("HERDR_TELEGRAM_TOPICS_FINAL_REPLY_MAX_CHARS", "16000"))
FINAL_REPLY_MAX_LINES = int(os.getenv("HERDR_TELEGRAM_TOPICS_FINAL_REPLY_MAX_LINES", "140"))
USER_PROMPT_MAX_CHARS = int(os.getenv("HERDR_TELEGRAM_TOPICS_USER_PROMPT_MAX_CHARS", "1200"))
DETAIL_REPLY_TIMEOUT_SECONDS = int(os.getenv("HERDR_TELEGRAM_TOPICS_DETAIL_TIMEOUT", "1800"))
CLEAN_ATTEMPT_TTL_SECONDS = int(os.getenv("HERDR_TELEGRAM_TOPICS_CLEAN_ATTEMPT_TTL", "1800"))
PANE_INPUT_FILE_CHARS = int(os.getenv("HERDR_TELEGRAM_TOPICS_INPUT_FILE_CHARS", "1200"))
PANE_INPUT_FILE_LINES = int(os.getenv("HERDR_TELEGRAM_TOPICS_INPUT_FILE_LINES", "6"))
PANE_INPUT_FILE_MAX_CHARS = int(os.getenv("HERDR_TELEGRAM_TOPICS_INPUT_FILE_MAX_CHARS", "120000"))
EVENT_SETTLE_SECONDS = float(os.getenv("HERDR_TELEGRAM_TOPICS_EVENT_SETTLE_SECONDS", "4"))
EVENT_SETTLE_INTERVAL_SECONDS = float(os.getenv("HERDR_TELEGRAM_TOPICS_EVENT_SETTLE_INTERVAL", "0.75"))
DUPLICATE_TOPIC_DELETE_LIMIT = int(os.getenv("HERDR_TELEGRAM_TOPICS_DUPLICATE_DELETE_LIMIT", "12"))
AUTO_FEED_SOURCES = ("recent-unwrapped",)
MANUAL_FEED_SOURCES = ("recent-unwrapped", "transcript", "visible")

SECRET_PATTERNS = [
    re.compile(r"(?i)\b(bot_token|token|api[_-]?key|secret|password|passwd|authorization)\s*[:=]\s*([^\s]+)"),
    re.compile(r"(?i)\bBearer\s+[A-Za-z0-9._~+/\-]+=*"),
    re.compile(r"\b\d{6,}:[A-Za-z0-9_-]{20,}\b"),
    re.compile(r"\bAKIA[0-9A-Z]{16}\b"),
    re.compile(r"\b[A-Za-z0-9+/]{40,}={0,2}\b"),
    re.compile(r"(?i)([?&](?:access_token|api[_-]?key|auth[_-]?token|token|signature|sig)=)([^&#\s]+)"),
]

ANSI_RE = re.compile(r"\x1b\[[0-9;?]*[ -/]*[@-~]")
TUI_LEADING_CHROME_RE = re.compile(r"^\s*[│┃└┌┐┘├┤╭╮╰╯⎿]\s*")
PROMPT_ONLY_RE = re.compile(r"^\s*(?:❯|›)\s*$")
PROMPT_WITH_TEXT_RE = re.compile(r"^\s*(?:❯|›)\s+\S+")
REPORT_BLOCK_RE = re.compile(r"(?ms)^\s*HERDRES_REPORT_START\s*$\s*(.*?)^\s*HERDRES_REPORT_END\s*$")
CHOICES_BLOCK_RE = re.compile(r"(?ms)^\s*HERDRES_CHOICES_START\s*$\s*(.*?)^\s*HERDRES_CHOICES_END\s*$")
REPORT_TITLE_RE = re.compile(r"^\s*HERDRES_REPORT_TITLE\s*:\s*(.{1,80})\s*$", re.IGNORECASE)
BAD_TITLE_WORDS_RE = re.compile(
    r"\b(first non-empty|becomes|because|should|could|would|which|that|etc)\b",
    re.IGNORECASE,
)
ACTION_QUESTION_RE = re.compile(
    r"(?i)\b("
    r"should\s+(?:i|we)\b|"
    r"do you want me to\b|"
    r"would you like(?: me)? to\b|"
    r"want me to\b|"
    r"approve\b|"
    r"choose\b|"
    r"select\b|"
    r"proceed\b|"
    r"continue\?\s*$|"
    r"deploy\?\s*$|"
    r"run it\?\s*$"
    r")",
    re.IGNORECASE,
)
RESUME_CONTROL_RE = re.compile(
    r"\b("
    r"conversation interrupted|"
    r"goal paused|"
    r"goal resumed|"
    r"conversation resumed|"
    r"transcript restored|"
    r"compacted conversation|"
    r"previous conversation state"
    r")\b",
    re.IGNORECASE,
)
STRUCTURED_SECTION_RE = re.compile(r"^\s*([A-Za-z][A-Za-z ]{0,40})\s*:\s*(.*?)\s*$")
INLINE_CODE_RE = re.compile(r"`([^`\n]{1,300})`")
COMMIT_LINE_RE = re.compile(r"^`?([0-9a-f]{7,12})\s+(.+?)`?$", re.IGNORECASE)
FENCE_START_RE = re.compile(r"^\s*```\s*([A-Za-z0-9_+-]{0,32})\s*$")
TOKEN_CODE_RE = re.compile(
    r"(?<![\w/])("
    r"(?:~|/)[A-Za-z0-9_.+-]+(?:/[A-Za-z0-9_.+-]+)+(?::\d+)?|"
    r"\b[A-Z][A-Z0-9_]{2,}\b|"
    r"\b[A-Za-z_][A-Za-z0-9_]*\.(?:py|json|toml|service|timer|sh|md|txt|yaml|yml)\b(?::\d+)?|"
    r"\b[A-Za-z_][A-Za-z0-9_]*=\S+\b|"
    r"\b(?:sendRichMessage|editForumTopic|editMessageText|createForumTopic)\b|"
    r"\b[0-9a-f]{7,12}\b"
    r")(?![\w/])"
)
SECTION_ALIASES = {
    "summary": "summary",
    "short summary": "summary",
    "table": "table",
    "status": "table",
    "status table": "table",
    "metrics": "table",
    "checklist": "checklist",
    "deployment checklist": "checklist",
    "next": "checklist",
    "details": "details",
    "risks": "details",
    "proof": "details",
    "logs": "details",
    "commands": "details",
    "diff": "details",
    "footer": "footer",
    "meta": "footer",
}
CODE_DETAILS_SECTIONS = {"proof", "logs", "commands", "diff"}


class BridgeError(RuntimeError):
    pass


class RateLimited(BridgeError):
    def __init__(self, retry_after: int):
        super().__init__(f"Telegram rate limited; retry after {retry_after}s")
        self.retry_after = retry_after


def utc_now() -> str:
    return _dt.datetime.now(tz=_dt.timezone.utc).replace(microsecond=0).isoformat()


def load_dotenv(path: Path = DEFAULT_ENV) -> None:
    paths = [path]
    if path != DEFAULT_HERMES_ENV:
        paths.append(DEFAULT_HERMES_ENV)
    for env_path in paths:
        _load_dotenv_file(env_path)


def _load_dotenv_file(path: Path) -> None:
    if not path.exists():
        return
    try:
        for raw in path.read_text(encoding="utf-8").splitlines():
            line = raw.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            key = key.strip()
            value = value.strip().strip('"').strip("'")
            if key and key not in os.environ:
                os.environ[key] = value
    except OSError:
        return


def state_path() -> Path:
    return Path(os.getenv("HERDR_TELEGRAM_TOPICS_STATE", str(DEFAULT_STATE))).expanduser()


def lock_path() -> Path:
    return Path(os.getenv("HERDR_TELEGRAM_TOPICS_LOCK", str(DEFAULT_LOCK))).expanduser()


def initial_state() -> dict[str, Any]:
    owners = [
        part.strip()
        for part in os.getenv("TELEGRAM_ALLOWED_USERS", DEFAULT_OWNER_ID).split(",")
        if part.strip()
    ]
    return {
        "version": 1,
        "enabled": os.getenv("HERDR_TELEGRAM_TOPICS_ENABLED", "1").lower() in {"1", "true", "yes", "on"},
        "plugin_event_enabled": os.getenv("HERDR_TELEGRAM_TOPICS_PLUGIN_EVENTS", "1").lower() in {"1", "true", "yes", "on"},
        "telegram": {
            "chat_id": os.getenv("HERDR_TELEGRAM_TOPICS_CHAT_ID", DEFAULT_CHAT_ID),
            "general_thread_id": os.getenv("HERDR_TELEGRAM_TOPICS_GENERAL_THREAD_ID", DEFAULT_GENERAL_THREAD_ID),
            "owner_user_ids": owners,
            "implicit_send_enabled": False,
        },
        "panes": {},
        "created_at": utc_now(),
        "updated_at": utc_now(),
    }


def load_state() -> dict[str, Any]:
    path = state_path()
    if not path.exists():
        return initial_state()
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        backup = path.with_suffix(path.suffix + ".bak")
        if backup.exists():
            try:
                data = json.loads(backup.read_text(encoding="utf-8"))
                if isinstance(data, dict) and data.get("version") == 1:
                    return data
            except Exception:
                pass
        backup = path.with_suffix(path.suffix + f".corrupt-{int(time.time())}.bak")
        try:
            path.replace(backup)
        except OSError:
            pass
        raise BridgeError(f"state file is corrupt: {exc}") from exc
    if not isinstance(data, dict) or data.get("version") != 1:
        raise BridgeError("unsupported state schema")
    data.setdefault("enabled", True)
    data.setdefault("plugin_event_enabled", True)
    data.setdefault("telegram", {})
    data.setdefault("panes", {})
    return data


def save_state(state: dict[str, Any]) -> None:
    path = state_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    state["updated_at"] = utc_now()
    payload = json.dumps(state, indent=2, sort_keys=True) + "\n"
    if path.exists():
        backup = path.with_suffix(path.suffix + ".bak")
        try:
            backup.write_text(path.read_text(encoding="utf-8"), encoding="utf-8")
        except OSError:
            pass
    fd, tmp_name = tempfile.mkstemp(prefix=path.name + ".", suffix=".tmp", dir=str(path.parent))
    with os.fdopen(fd, "w", encoding="utf-8") as fh:
        fh.write(payload)
        fh.flush()
        os.fsync(fh.fileno())
    os.replace(tmp_name, path)


def sanitize_text(text: str, max_chars: int = MAX_REPLY_CHARS) -> str:
    out = text or ""
    for pat in SECRET_PATTERNS:
        if pat.pattern.startswith("(?i)([?&]"):
            out = pat.sub(lambda m: f"{m.group(1)}***", out)
        elif "Bearer" in pat.pattern:
            out = pat.sub("Bearer ***", out)
        elif "bot_token" in pat.pattern:
            out = pat.sub(lambda m: f"{m.group(1)}=***", out)
        else:
            out = pat.sub("***", out)
    out = re.sub(r"[\x00-\x08\x0b\x0c\x0e-\x1f]", "", out)
    if len(out) > max_chars:
        out = out[: max_chars - 80].rstrip() + "\n...[truncated by herdr-topic bridge]"
    return out


def compact_path(path: str | None) -> str:
    if not path:
        return ""
    home = str(Path.home())
    value = str(path)
    if value.startswith(home):
        value = "~" + value[len(home):]
    return sanitize_text(value, max_chars=160)


def run_cmd(args: list[str], *, timeout: int = 10, input_text: str | None = None) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        args,
        input=input_text,
        text=True,
        capture_output=True,
        timeout=timeout,
        check=False,
    )


def herdr_bin() -> str:
    return os.getenv("HERDR_BIN", DEFAULT_HERDR_BIN)


def herdr_json(args: list[str], *, timeout: int = 10) -> Any:
    proc = run_cmd([herdr_bin(), *args], timeout=timeout)
    if proc.returncode != 0:
        raise BridgeError(sanitize_text((proc.stderr or proc.stdout or "").strip(), 500))
    try:
        return json.loads(proc.stdout)
    except json.JSONDecodeError as exc:
        raise BridgeError(f"herdr returned non-JSON for {' '.join(args)}") from exc


def herdr_text(args: list[str], *, timeout: int = 10) -> str:
    proc = run_cmd([herdr_bin(), *args], timeout=timeout)
    if proc.returncode != 0:
        raise BridgeError(sanitize_text((proc.stderr or proc.stdout or "").strip(), 500))
    return proc.stdout


def pane_list() -> list[dict[str, Any]]:
    data = herdr_json(["pane", "list"], timeout=8)
    if isinstance(data, dict):
        panes = data.get("result", {}).get("panes")
        if isinstance(panes, list):
            return panes
    raise BridgeError("unexpected herdr pane list response")


def pane_by_id(pane_id: str) -> dict[str, Any] | None:
    for pane in pane_list():
        if str(pane.get("pane_id")) == str(pane_id):
            return pane
    return None


def pane_turn(pane_id: str) -> dict[str, Any]:
    # Upgrade-safe optional interface: Herdres can consume this when Herdr
    # exposes it, but never scrapes pane output as a substitute.
    try:
        data = herdr_json(["pane", "turn", pane_id, "--last", "--format", "json"], timeout=8)
    except BridgeError as exc:
        return {
            "available": False,
            "reason": "no_structured_turn_source",
            "detail": sanitize_text(str(exc), 300),
        }
    if isinstance(data, dict):
        result_turn = data.get("result", {}).get("turn")
        if isinstance(result_turn, dict):
            return result_turn
        return data
    return {"available": False, "reason": "unexpected_turn_response"}


def pane_agent_session_id(pane: dict[str, Any]) -> str:
    sess = pane.get("agent_session")
    if isinstance(sess, dict):
        return str(sess.get("value") or "")
    return ""


def pane_key(pane: dict[str, Any]) -> str:
    parts = [
        str(pane.get("pane_id") or ""),
        str(pane.get("terminal_id") or ""),
        str(pane.get("workspace_id") or ""),
        str(pane.get("tab_id") or ""),
    ]
    raw = "|".join(parts)
    digest = hashlib.sha1(raw.encode("utf-8")).hexdigest()[:10]
    return f"{parts[0]}:{digest}"


def pane_handle_alias(value: str) -> str:
    text = str(value or "")
    match = re.match(r"^(w[0-9a-f]+)(?::p|-)(\d+)$", text, flags=re.IGNORECASE)
    if not match:
        return ""
    return f"{match.group(1)}:{int(match.group(2))}"


def entry_pane_alias(entry: dict[str, Any]) -> str:
    return pane_handle_alias(str(entry.get("pane_id") or ""))


def short_pane_id(pane_id: str) -> str:
    text = str(pane_id)
    if len(text) <= 18:
        return text
    return text[:8] + "-" + text[-6:]


def clean_topic_title(value: str, *, fallback: str = "Task") -> str:
    text = sanitize_text(value, 80)
    text = re.sub(r"\bHerdr\b", " ", text, flags=re.IGNORECASE)
    text = re.sub(r"\bw[0-9a-f]{8,}-\d+\b", " ", text, flags=re.IGNORECASE)
    text = re.sub(r"[_./:@-]+", " ", text)
    text = re.sub(r"[^A-Za-z0-9 ]+", " ", text)
    words = [w for w in text.strip().split() if w]
    stop = {"agent", "codex", "claude", "task", "session", "pane", "home", "deploy"}
    kept = [w for w in words if w.lower() not in stop]
    words = kept or words
    if not words:
        return fallback
    title = " ".join(words[:2]).strip()
    return title.title()[:32].strip() or fallback


def clean_label_topic_title(value: str, *, fallback: str = "Task") -> str:
    text = sanitize_text(value, 80)
    text = re.sub(r"\bHerdr\b", " ", text, flags=re.IGNORECASE)
    text = re.sub(r"\bw[0-9a-f]{8,}-\d+\b", " ", text, flags=re.IGNORECASE)
    text = re.sub(r"[_./:@-]+", " ", text)
    text = re.sub(r"[^A-Za-z0-9 ]+", " ", text)
    words = [w for w in text.strip().split() if w]
    if not words:
        return fallback
    title = " ".join(words[:2]).strip()
    return title.title()[:32].strip() or fallback


def title_from_text(text: str) -> str:
    lower = text.lower()
    rules = [
        (("topic name", "topic naming", "editforumtopic", "forum topic icon"), "Topic Names"),
        (("herdres", "createforumtopic", "herdr pane telegram", "topic sync"), "Topic Sync"),
        (("flightrecorder", "flight recorder"), "Flight Recorder"),
        (("italy ping",), "Italy Ping"),
        (("gitmoot", "code review", "review pass"), "Review"),
        (("summarize recent commits", "recent commits"), "Commits"),
    ]
    for needles, title in rules:
        if any(needle in lower for needle in needles):
            return title
    return ""


def pane_manual_label(pane: dict[str, Any]) -> str:
    label = str(pane.get("label") or "").strip()
    label = re.sub(r"\s+", " ", label)
    return sanitize_text(label, 120)


def topic_name_from_pane_label(label: str) -> str:
    label_title = title_from_text(label)
    if label_title:
        return label_title
    return clean_label_topic_title(label)


def topic_name_for_pane(pane: dict[str, Any]) -> str:
    label = pane_manual_label(pane)
    if label:
        return topic_name_from_pane_label(label)

    pane_id = str(pane.get("pane_id") or "")
    tail_title = title_from_text(recent_tail(pane_id, lines=50, max_chars=2000)) if pane_id else ""
    if tail_title:
        return tail_title

    cwd = Path(str(pane.get("foreground_cwd") or pane.get("cwd") or "")).name
    cwd = re.sub(r"^(x-|hermes-)", "", cwd)
    cwd = re.sub(r"\b(agent|deploy|reply)\b", " ", cwd, flags=re.IGNORECASE)
    cwd_title = clean_topic_title(cwd, fallback="")
    if cwd_title:
        return cwd_title

    return clean_topic_title(str(pane.get("agent") or "Task"))


def status_object(pane: dict[str, Any]) -> dict[str, Any]:
    return {
        "pane_id": str(pane.get("pane_id") or ""),
        "terminal_id": str(pane.get("terminal_id") or ""),
        "workspace": str(pane.get("workspace_id") or ""),
        "tab": str(pane.get("tab_id") or ""),
        "agent": str(pane.get("agent") or ""),
        "agent_session_id": pane_agent_session_id(pane),
        "status": str(pane.get("agent_status") or "unknown"),
        "cwd": compact_path(pane.get("cwd") or pane.get("foreground_cwd") or ""),
        "label": sanitize_text(str(pane.get("label") or ""), 120),
    }


def status_hash(obj: dict[str, Any]) -> str:
    return hashlib.sha256(json.dumps(obj, sort_keys=True).encode("utf-8")).hexdigest()


def stable_status_object(pane: dict[str, Any]) -> dict[str, Any]:
    obj = status_object(pane)
    obj.pop("label", None)
    return obj


def pane_output(
    pane_id: str,
    *,
    lines: int = READ_LINES_STATUS,
    max_chars: int = 700,
    source: str = "visible",
) -> str:
    try:
        raw = herdr_text(
            ["pane", "read", pane_id, "--source", source, "--lines", str(lines), "--format", "text"],
            timeout=8,
        )
    except Exception:
        return ""
    try:
        data = json.loads(raw)
        text = data.get("result", {}).get("text") or data.get("text") or raw
    except Exception:
        text = raw
    return sanitize_text(str(text), max_chars=max_chars)


def pane_feed_output(pane_id: str, *, manual: bool = False) -> str:
    sources = MANUAL_FEED_SOURCES if manual else AUTO_FEED_SOURCES
    for source in sources:
        text = pane_output(
            pane_id,
            lines=FEED_READ_LINES,
            max_chars=FEED_MAX_CHARS,
            source=source,
        )
        if text.strip():
            return text
    return ""


def recent_tail(pane_id: str, lines: int = READ_LINES_STATUS, max_chars: int = 700) -> str:
    clean_lines = [ln.rstrip() for ln in pane_output(pane_id, lines=lines, max_chars=max_chars).splitlines()]
    clean_lines = [ln for ln in clean_lines if ln.strip()]
    return "\n".join(clean_lines[-8:])


NOISE_PREFIXES = (
    "ran ",
    "explored",
    "read ",
    "edited ",
    "listed ",
    "search ",
    "open ",
    "find ",
    "chunk id:",
    "wall time:",
    "process exited",
    "original token count:",
    "output:",
    "gpt-",
    "claude code",
    "opus ",
    "sonnet ",
    "ctrl+",
    "esc to interrupt",
    "worked for ",
    "working (",
    "goal blocked",
)

TOOL_START_RE = re.compile(
    r"^\s*[•●]?\s*"
    r"(?:Bash|Read|Edit|Write|MultiEdit|Grep|Glob|LS|TodoWrite|Task|WebFetch|WebSearch)"
    r"\(",
    re.IGNORECASE,
)

TUI_STATUS_PREFIXES = (
    "bash(",
    "started task-",
    "running in the background",
    "tip: use /btw",
    "brewed for",
    "* brewed for",
    "... +",
    "… +",
)

TOOL_CONTEXT_STATUS_PREFIXES = (
    "job:",
    "state:",
    "repo:",
    "branch:",
)

PROCESS_OUTPUT_PREFIXES = (
    "commit ",
    "to https://",
    "ls-remote ",
    "--user is-enabled",
    "telegram topics",
)

PROCESS_OUTPUT_EXACT = {
    "enabled",
}

REPORT_PRIMARY_STARTS = {
    "what changed",
    "changes made",
}

REPORT_FALLBACK_STARTS = {
    "summary",
    "final",
    "final status",
    "verification",
    "verified with",
}

REPORT_VERIFICATION_STARTS = {
    "verification",
    "verified with",
}

QUESTION_MARKERS = (
    "would you like",
    "please choose",
    "choose ",
    "select ",
    "ready to execute",
    "needs approval",
    "waiting for owner",
    "requires owner",
)


def normalize_feed_line(line: str) -> str:
    text = ANSI_RE.sub("", line or "").rstrip()
    text = TUI_LEADING_CHROME_RE.sub("", text)
    text = re.sub(r"[\u2500-\u257f]+", " ", text)
    return sanitize_text(text, 500)


def noise_key(line: str) -> str:
    text = ANSI_RE.sub("", line or "")
    text = TUI_LEADING_CHROME_RE.sub("", text)
    text = re.sub(r"[\u2500-\u257f]+", " ", text)
    text = text.strip().lstrip(" \t-*>\u2022\u25cf\u25b8\u276f\u203a\u23bf\u273b\u23f5\u23f8").strip()
    return re.sub(r"\s+", " ", text).lower()


def is_composer_boundary(line: str) -> bool:
    raw = ANSI_RE.sub("", line or "").strip()
    low = noise_key(line)
    return bool(PROMPT_ONLY_RE.fullmatch(raw) or PROMPT_WITH_TEXT_RE.match(raw) or low.startswith("tip: use /btw"))


def strip_visible_composer(lines: list[str]) -> list[str]:
    search_from = max(0, len(lines) - 80)
    for idx in range(search_from, len(lines)):
        line = lines[idx]
        if option_match(line):
            continue
        if is_composer_boundary(line):
            return lines[:idx]
    return lines


def is_tui_status_noise(line: str, *, in_tool_block: bool = False) -> bool:
    low = noise_key(line)
    return any(low.startswith(prefix) for prefix in TUI_STATUS_PREFIXES) or (
        in_tool_block and any(low.startswith(prefix) for prefix in TOOL_CONTEXT_STATUS_PREFIXES)
    )


def drop_tui_tool_blocks(lines: list[str]) -> list[str]:
    out: list[str] = []
    skipping_tool = False
    for line in lines:
        clean = line.strip()
        if TOOL_START_RE.match(line) or is_tui_status_noise(line):
            skipping_tool = True
            continue

        if skipping_tool:
            if not clean:
                skipping_tool = False
                continue
            if is_tui_status_noise(line, in_tool_block=True) or TOOL_START_RE.match(line) or _is_codeish_line(line):
                continue
            skipping_tool = False

        out.append(line)
    return out


def is_noise_line(line: str) -> bool:
    if is_trivial_marker_line(line):
        return True
    low = noise_key(line)
    if not low:
        return True
    if low in {"herdres_report_start", "herdres_report_end", "herdres_choices_start", "herdres_choices_end"}:
        return True
    if is_tui_status_noise(line):
        return True
    if any(low.startswith(prefix) for prefix in NOISE_PREFIXES):
        return True
    if re.fullmatch(r"[-=_./\\|: ]{4,}", low):
        return True
    if low.startswith(("{", "[")) and low.endswith(("}", "]")):
        if re.search(r'"(?:ok|changed|message|created|sent|panes)"\s*:', low):
            return True
    if len(low) > 80 and low.startswith(("{", "[")) and low.endswith(("}", "]")):
        return True
    if low in PROCESS_OUTPUT_EXACT or any(low.startswith(prefix) for prefix in PROCESS_OUTPUT_PREFIXES):
        return True
    if " lines (ctrl +" in low or " to view transcript" in low:
        return True
    if low.startswith((
        "use /skills",
        "shift+tab",
        "bypass permissions on",
        "explain this codebase",
        "new task?",
    )):
        return True
    if low.startswith(("/compact", "compacted", "read ../.claude/", "read .claude/")):
        return True
    if low == "summary)":
        return True
    if re.fullmatch(r"(worked|crunched|simmered|sauteed|sautéed|thinking|processed) for \d+\s*[smh]", low):
        return True
    if any(fragment in low for fragment in (
        "ctrl+o",
        "shift+tab",
        "earning kickback",
    )):
        return True
    if "plan mode on" in low and "·" in str(line or ""):
        return True
    if "for agents" in low and ("·" in str(line or "") or "\u2190" in str(line or "")):
        return True
    if "bypass permissions" in low and (
        "·" in str(line or "") or "\u2190" in str(line or "") or low.startswith("bypass permissions on")
    ):
        return True
    return False


def clean_feed_lines(text: str) -> list[str]:
    prepared: list[str] = []
    for raw in (text or "").splitlines():
        clean = normalize_feed_line(raw)
        if not clean.strip():
            if prepared and prepared[-1] != "":
                prepared.append("")
            continue
        prepared.append(clean)

    prepared = strip_visible_composer(prepared)
    prepared = drop_tui_tool_blocks(prepared)

    lines: list[str] = []
    for clean in prepared:
        if not clean.strip():
            if lines and lines[-1] != "":
                lines.append("")
            continue
        if is_noise_line(clean):
            continue
        lines.append(clean)

    lines = drop_tui_tool_blocks(lines)

    while lines and lines[0] == "":
        lines.pop(0)
    while lines and lines[-1] == "":
        lines.pop()
    return lines[-180:]


def option_match(line: str) -> re.Match[str] | None:
    return re.match(r"^\s*(?:[\u276f>*-]\s*)?(\d{1,2})[.)]\s+(.{1,180})$", line)


def prompt_id_for(text: str, options: list[dict[str, str]]) -> str:
    payload = json.dumps({"text": text, "options": options}, sort_keys=True)
    return hashlib.sha1(payload.encode("utf-8")).hexdigest()[:12]


def compact_block(lines: list[str], *, max_lines: int = 10, max_chars: int = 1400) -> str:
    selected = [str(ln).rstrip() for ln in lines][-max_lines:]
    while selected and not selected[0].strip():
        selected.pop(0)
    while selected and not selected[-1].strip():
        selected.pop()
    text = "\n".join(selected[-max_lines:]).strip()
    return sanitize_text(text, max_chars=max_chars).strip()


def strip_outer_blank_lines(lines: list[str]) -> list[str]:
    out = [str(line).rstrip() for line in lines]
    while out and not out[0].strip():
        out.pop(0)
    while out and not out[-1].strip():
        out.pop()
    return out


def is_trivial_marker_line(line: str) -> bool:
    return bool(re.fullmatch(r"\s*[-*+\u2022]\s*", str(line or "")))


def heading_key(line: str) -> str:
    clean = str(line or "").strip()
    clean = re.sub(r"^\s*(?:[-*+\u2022]\s*)?", "", clean)
    clean = clean.rstrip(":").strip()
    return re.sub(r"\s+", " ", clean).lower()


def is_safe_report_title(line: str) -> bool:
    raw = str(line or "").strip().rstrip(":")
    if not raw:
        return False
    if len(raw) > 72 or len(raw.split()) > 7:
        return False
    if raw.endswith((".", "!", "?", ",")):
        return False
    if is_trivial_marker_line(raw):
        return False
    if _bullet_text(line) or _numbered_text(line):
        return False
    if _is_codeish_line(line):
        return False
    if BAD_TITLE_WORDS_RE.search(raw):
        return False
    return True


def section_alias(line: str) -> tuple[str, str, str] | None:
    match = STRUCTURED_SECTION_RE.match(str(line or ""))
    if not match:
        return None
    label = re.sub(r"\s+", " ", match.group(1).strip()).lower()
    kind = SECTION_ALIASES.get(label)
    if not kind:
        return None
    title = match.group(2).strip()
    if not title:
        title = label.title()
    return kind, title, label


def is_section_marker_line(line: str) -> bool:
    return section_alias(line) is not None


def parse_bounded_report_body(body_lines: list[str]) -> tuple[str, str] | None:
    body_lines = strip_outer_blank_lines(body_lines)
    if not body_lines:
        return None

    title = ""
    meta = REPORT_TITLE_RE.match(body_lines[0])
    if meta:
        title = sanitize_text(meta.group(1).strip(), 80)
        body_lines = body_lines[1:]
    elif is_section_marker_line(body_lines[0]):
        return None
    elif is_safe_report_title(body_lines[0]):
        title = sanitize_text(body_lines[0].strip().rstrip(":"), 80)
        body_lines = body_lines[1:]
    else:
        return None

    body_text = "\n".join(strip_outer_blank_lines(body_lines)).strip()
    if not title or not body_text:
        return None
    return title, body_text


def extract_bounded_report(lines: list[str]) -> tuple[str, str] | None:
    text = "\n".join(lines)
    matches = list(REPORT_BLOCK_RE.finditer(text))
    if not matches:
        return None
    return parse_bounded_report_body(matches[-1].group(1).splitlines())


def extract_bounded_report_from_raw(raw_text: str) -> tuple[str, str] | None:
    safe = ANSI_RE.sub("", sanitize_text(str(raw_text or ""), FEED_MAX_CHARS))
    matches = list(REPORT_BLOCK_RE.finditer(safe))
    if not matches:
        return None
    return parse_bounded_report_body(matches[-1].group(1).splitlines())


def is_report_primary_key(key: str) -> bool:
    return key in REPORT_PRIMARY_STARTS or any(key.startswith(start + " ") for start in REPORT_PRIMARY_STARTS)


def report_start_index(lines: list[str]) -> int | None:
    for idx in range(len(lines) - 1, -1, -1):
        line = lines[idx]
        key = heading_key(line)
        if is_report_primary_key(key):
            return idx
    for idx in range(len(lines) - 1, -1, -1):
        line = lines[idx]
        key = heading_key(line)
        if key in REPORT_FALLBACK_STARTS:
            if key in REPORT_VERIFICATION_STARTS and any(str(prev).strip() for prev in lines[:idx]):
                continue
            return idx
    return None


def slice_report_lines(lines: list[str]) -> list[str]:
    idx = report_start_index(lines)
    if idx is None:
        return lines
    return lines[idx:]


def report_title_and_body(lines: list[str]) -> tuple[str, str]:
    sliced = slice_report_lines(lines)
    if sliced:
        key = heading_key(sliced[0])
        if is_report_primary_key(key) or key in REPORT_FALLBACK_STARTS:
            title = sliced[0].strip().rstrip(":")
            body_lines = sliced[1:]
            return title, "\n".join(body_lines).strip()
    return "Update", "\n".join(sliced).strip()


def titled_feed_text(title: str, body: str) -> str:
    clean = body.strip()
    if clean.lower() == title.lower() or clean.lower().startswith(title.lower() + "\n"):
        return clean
    return f"{title}\n{clean}"


def feed_body_lines(title: str, body: str) -> list[str]:
    lines = [ln.rstrip() for ln in str(body or "").splitlines()]
    while lines and not lines[0].strip():
        lines.pop(0)
    while lines and not lines[-1].strip():
        lines.pop()
    if lines and lines[0].strip().lower() == title.lower():
        lines = lines[1:]
    while lines and not lines[0].strip():
        lines.pop(0)
    return lines


def make_feed_item(kind: str, title: str, body: str, *, notify: bool) -> dict[str, Any]:
    lines = feed_body_lines(title, body)
    line_cap = 80 if kind in {"report", "blocked", "error"} else 30
    detail_cap = 60 if kind in {"report", "blocked", "error"} else 24
    detail_chars = 4200 if kind in {"report", "blocked", "error"} else MAX_RICH_DETAIL_CHARS
    summary = compact_block(lines[:4], max_lines=4, max_chars=700) if lines else ""
    detail = compact_block(lines[4:], max_lines=detail_cap, max_chars=detail_chars) if len(lines) > 4 else ""
    text_body = "\n".join(lines).strip()
    text = titled_feed_text(title, text_body or body)
    return {
        "kind": kind,
        "title": title,
        "summary": summary or text_body or body.strip(),
        "detail": detail,
        "lines": lines[:line_cap],
        "text": text,
        "notify": notify,
    }


def _boolish(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    return str(value or "").strip().lower() in {"1", "true", "yes", "on"}


def _callback_id(value: str, fallback: str) -> str:
    clean = re.sub(r"[^A-Za-z0-9_-]+", "", str(value or "").strip())
    return (clean or fallback)[:32]


def _prompt_callback_id(value: str, fallback_payload: str, options: list[dict[str, str]]) -> str:
    clean = _callback_id(value, "")
    if clean and len(clean.encode("utf-8")) <= 16:
        return clean
    return prompt_id_for(fallback_payload, options)


def safe_callback_data(action: str, prompt_id: str, choice_id: str) -> str:
    clean_action = "d" if action == "d" else "c"
    clean_prompt = _callback_id(prompt_id, "prompt")[:16]
    clean_choice = _callback_id(choice_id, "choice")[:32]
    data = f"herdr:{clean_action}:{clean_prompt}:{clean_choice}"
    if len(data.encode("utf-8")) <= 64:
        return data
    short_choice = hashlib.sha1(clean_choice.encode("utf-8")).hexdigest()[:10]
    return f"herdr:{clean_action}:{clean_prompt}:{short_choice}"


def normalize_pending_decision(turn: dict[str, Any]) -> dict[str, Any] | None:
    pending = turn.get("pending_decision")
    if not isinstance(pending, dict):
        return None
    raw_options = pending.get("options")
    if not isinstance(raw_options, list) or not raw_options:
        return None

    prompt = sanitize_text(str(pending.get("prompt") or ""), 1200).strip()
    if not prompt:
        prompt = "Choose how to proceed."

    options: list[dict[str, str]] = []
    for idx, raw in enumerate(raw_options[:12], start=1):
        if isinstance(raw, dict):
            raw_id = str(raw.get("id") or raw.get("number") or idx).strip()
            label = str(raw.get("label") or raw.get("text") or raw.get("title") or raw_id).strip()
            if "send_text" in raw:
                send_text = str(raw.get("send_text") or "").strip()
            elif "value" in raw:
                send_text = str(raw.get("value") or "").strip()
            else:
                send_text = raw_id
            needs_detail = (
                _boolish(raw.get("needs_detail"))
                or _boolish(raw.get("requires_detail"))
                or _boolish(raw.get("custom"))
            )
        else:
            raw_id = str(idx)
            label = str(raw or "").strip()
            send_text = raw_id
            needs_detail = False

        raw_id = raw_id or str(idx)
        label = re.sub(r"^\s*\d{1,2}[.)]\s+", "", label).strip()
        label = sanitize_text(label or raw_id, 120)
        callback_id = _callback_id(raw_id, str(idx))
        if raw_id.lower() == "custom" or callback_id.lower() == "custom":
            needs_detail = True
        has_explicit_send_text = isinstance(raw, dict) and "send_text" in raw
        if has_explicit_send_text:
            needs_detail = needs_detail or not send_text
        option: dict[str, str] = {
            "number": callback_id,
            "callback_id": callback_id,
            "id": sanitize_text(raw_id, 80),
            "label": label,
            "send_text": sanitize_text(send_text, 500),
        }
        if needs_detail:
            option["needs_detail"] = "1"
        options.append(option)

    if not options:
        return None
    decision_id = sanitize_text(str(pending.get("decision_id") or turn.get("turn_id") or prompt_id_for(prompt, options)), 300)
    return {
        "decision_id": decision_id,
        "prompt": prompt,
        "mode": "buttons",
        "options": options,
    }


def make_decision_feed_item(turn: dict[str, Any], decision: dict[str, Any]) -> dict[str, Any] | None:
    prompt = sanitize_text(str(decision.get("prompt") or ""), 1200).strip()
    options = list(decision.get("options") or [])
    if not prompt or not options:
        return None
    user_text = sanitize_text(str(turn.get("user_text") or ""), USER_PROMPT_MAX_CHARS).strip()
    assistant_context = sanitize_text(str(turn.get("assistant_final_text") or ""), FINAL_REPLY_MAX_CHARS).strip()
    text_parts: list[str] = []
    if user_text:
        text_parts.extend(["You asked", user_text, ""])
    if assistant_context:
        text_parts.extend([assistant_context, ""])
    text_parts.append(prompt)
    text_parts.append("")
    text_parts.extend(f"{opt.get('number')}) {opt.get('label')}" for opt in options)
    return {
        "kind": "decision",
        "title": "Decision needed",
        "summary": prompt,
        "detail": assistant_context,
        "lines": prompt.splitlines()[:8],
        "text": "\n".join(text_parts).strip(),
        "turn_id": sanitize_text(str(turn.get("turn_id") or ""), 300),
        "decision_id": str(decision.get("decision_id") or ""),
        "user_text": user_text,
        "assistant_final_text": assistant_context,
        "options": options,
        "notify": True,
    }


def make_turn_feed_item(turn: dict[str, Any]) -> dict[str, Any] | None:
    if not isinstance(turn, dict):
        return None
    if turn.get("available") is not True:
        return None
    decision = normalize_pending_decision(turn)
    if decision and (turn.get("awaiting_input") is True or turn.get("complete") is not True):
        return make_decision_feed_item(turn, decision)
    if turn.get("complete") is not True:
        return None
    assistant_final = sanitize_text(str(turn.get("assistant_final_text") or ""), FINAL_REPLY_MAX_CHARS).strip()
    if not assistant_final:
        return None
    user_text = sanitize_text(str(turn.get("user_text") or ""), USER_PROMPT_MAX_CHARS).strip()
    text_parts: list[str] = []
    if user_text:
        text_parts.extend(["You asked", user_text, ""])
    text_parts.append(assistant_final)
    return {
        "kind": "turn",
        "title": "",
        "summary": compact_block(assistant_final.splitlines()[:4], max_lines=4, max_chars=700),
        "detail": "",
        "lines": assistant_final.splitlines()[:FINAL_REPLY_MAX_LINES],
        "text": "\n".join(text_parts).strip(),
        "turn_id": sanitize_text(str(turn.get("turn_id") or ""), 300),
        "user_text": user_text,
        "assistant_final_text": assistant_final,
        "notify": False,
    }


def extract_turn_feed_item(pane: dict[str, Any], entry: dict[str, Any]) -> dict[str, Any] | None:
    turn = pane_turn(str(pane.get("pane_id") or ""))
    available = bool(turn.get("available", True))
    reason = sanitize_text(str(turn.get("reason") or ""), 300)
    if entry.get("last_turn_available") != available or str(entry.get("last_turn_reason") or "") != reason:
        entry["last_turn_available"] = available
        if reason:
            entry["last_turn_reason"] = reason
        else:
            entry.pop("last_turn_reason", None)
    item = make_turn_feed_item(turn)
    if item:
        entry["last_turn_id"] = item.get("turn_id") or ""
    return item


def item_plain_text(item: dict[str, Any]) -> str:
    if str(item.get("kind") or "").lower() == "turn":
        user_text = str(item.get("user_text") or "").strip()
        assistant_final = str(item.get("assistant_final_text") or "").strip()
        parts: list[str] = []
        if user_text:
            parts.extend(["You asked", user_text, ""])
        if assistant_final:
            parts.append(assistant_final)
        return sanitize_text("\n".join(parts).strip(), FINAL_REPLY_MAX_CHARS)
    if str(item.get("kind") or "").lower() == "decision":
        user_text = str(item.get("user_text") or "").strip()
        assistant_context = str(item.get("assistant_final_text") or "").strip()
        prompt = str(item.get("summary") or item.get("text") or "").strip()
        options = list(item.get("options") or [])
        parts: list[str] = []
        if user_text:
            parts.extend(["You asked", user_text, ""])
        if assistant_context:
            parts.extend([assistant_context, ""])
        if prompt:
            parts.append(prompt)
        if options:
            parts.append("")
            parts.extend(f"{opt.get('number')}) {opt.get('label')}" for opt in options)
        return sanitize_text("\n".join(parts).strip(), FINAL_REPLY_MAX_CHARS)
    text = str(item.get("text") or "").strip()
    if text:
        return sanitize_text(text, MAX_REPLY_CHARS)
    title = str(item.get("title") or item.get("kind") or "Update").strip()
    lines = [title]
    summary = str(item.get("summary") or "").strip()
    detail = str(item.get("detail") or "").strip()
    if summary:
        lines.append(summary)
    options = list(item.get("options") or [])
    if options:
        lines.append("")
        lines.extend(f"{opt.get('number')}) {opt.get('label')}" for opt in options)
    if detail:
        lines.extend(["", detail])
    return sanitize_text("\n".join(lines).strip(), MAX_REPLY_CHARS)


def _html_text(value: Any, max_chars: int = MAX_REPLY_CHARS) -> str:
    return html.escape(sanitize_text(str(value or ""), max_chars), quote=False)


def _rich_paragraph(value: str) -> str:
    clean = _rich_inline(value, MAX_RICH_DETAIL_CHARS).strip()
    if not clean:
        return ""
    return f"<p>{clean}</p>"


def _bullet_text(line: str) -> str | None:
    match = re.match(r"^\s*(?:[-*+]|\u2022)\s+(.+)$", line or "")
    if match:
        return match.group(1).strip()
    return None


def _numbered_text(line: str) -> tuple[int, str] | None:
    match = re.match(r"^\s*(\d{1,2})[.)]\s+(.+)$", line or "")
    if not match:
        return None
    return int(match.group(1)), match.group(2).strip()


def _split_path_section(line: str) -> tuple[str, str] | None:
    clean = str(line or "").strip().rstrip(":")
    match = re.match(
        r"^(Changes made|Changed|Implemented|Updated|Modified|Edited|Touched)\s+(?:in|at)\s+(.+)$",
        clean,
        re.IGNORECASE,
    )
    if not match:
        return None
    title = match.group(1).strip()
    title = title[:1].upper() + title[1:]
    ref = match.group(2).strip().rstrip(":")
    if not ref or not re.search(r"[/\\.]|:\d+$", ref):
        return None
    return title, ref


def _is_codeish_line(line: str) -> bool:
    raw = str(line or "")
    stripped = str(line or "").strip()
    if not stripped or is_trivial_marker_line(stripped):
        return False
    return (
        raw.startswith(("    ", "\t"))
        or stripped.startswith(("$ ", "# ", "./"))
        or bool(re.match(r"^(cd|python3?|pip3?|npm|pnpm|yarn|node|git|gh|curl|ssh|systemctl|journalctl|herdr)\b", stripped))
        or bool(re.match(r"^[A-Z][A-Z0-9_]+=", stripped))
    )


def looks_like_path_or_symbol(value: str) -> bool:
    clean = str(value or "").strip()
    if not clean or len(clean.split()) > 3:
        return False
    if re.fullmatch(r"[A-Z][A-Z0-9_]{2,}", clean):
        return True
    if re.fullmatch(r"[0-9a-f]{7,40}", clean, re.IGNORECASE):
        return True
    if re.fullmatch(r"commit\s+[0-9a-f]{7,40}", clean, re.IGNORECASE):
        return True
    if re.search(r"(^|[~/\\])[\w.+-]+(?:/[\w.+-]+)+(?::\d+)?$", clean):
        return True
    if re.search(r"\b[\w.+-]+\.(?:py|js|ts|tsx|jsx|json|md|txt|yaml|yml|toml|sh|service|timer)(?::\d+)?$", clean):
        return True
    return False


def _rich_text_segment(value: str) -> str:
    text = str(value or "")
    parts: list[str] = []
    pos = 0
    for match in TOKEN_CODE_RE.finditer(text):
        parts.append(html.escape(text[pos:match.start()], quote=False).replace("`", ""))
        parts.append(f"<code>{html.escape(match.group(1), quote=False)}</code>")
        pos = match.end()
    parts.append(html.escape(text[pos:], quote=False).replace("`", ""))
    rendered = "".join(parts)
    rendered = re.sub(r"\*\*([^*\n]{1,300})\*\*", r"<b>\1</b>", rendered)
    rendered = re.sub(r"(?<!\*)\*([^*\n]{1,180})\*(?!\*)", r"<i>\1</i>", rendered)
    return rendered


def _rich_inline(value: str, max_chars: int = 500) -> str:
    clean = str(value or "").strip()
    if not clean:
        return ""
    if "`" not in clean and (_is_codeish_line(clean) or looks_like_path_or_symbol(clean)):
        return f"<code>{_html_text(clean, max_chars)}</code>"
    clean = sanitize_text(clean, max_chars)
    parts: list[str] = []
    pos = 0
    for match in INLINE_CODE_RE.finditer(clean):
        parts.append(_rich_text_segment(clean[pos:match.start()]))
        parts.append(f"<code>{_html_text(match.group(1), max_chars)}</code>")
        pos = match.end()
    parts.append(_rich_text_segment(clean[pos:]))
    return "".join(parts)


def _looks_like_section(line: str, next_line: str | None = None) -> bool:
    clean = str(line or "").strip()
    if not clean or len(clean) > 80:
        return False
    if _split_path_section(clean):
        return True
    if clean.endswith(":"):
        return True
    if next_line and (_bullet_text(next_line) or _numbered_text(next_line)):
        words = clean.split()
        return 1 <= len(words) <= 5 and not clean.endswith((".", "!", "?"))
    return False


def _limited_lines(value: str | list[str], *, max_chars: int, max_lines: int = 30) -> tuple[list[str], list[str]]:
    if isinstance(value, list):
        raw_lines = [str(ln).rstrip() for ln in value]
    else:
        raw_lines = [ln.rstrip() for ln in str(value or "").splitlines()]
    lines: list[str] = []
    overflow: list[str] = []
    used = 0
    content_count = 0
    for raw in raw_lines:
        clean = sanitize_text(raw, 500).rstrip()
        if not clean.strip():
            if lines and lines[-1] != "":
                lines.append("")
            continue
        next_len = used + len(clean) + 1
        if content_count >= max_lines or next_len > max_chars:
            overflow.append(clean)
            continue
        lines.append(clean)
        content_count += 1
        used = next_len
    while lines and lines[0] == "":
        lines.pop(0)
    while lines and lines[-1] == "":
        lines.pop()
    return lines, overflow


def _rich_structured_block(value: str | list[str], *, max_chars: int = MAX_RICH_DETAIL_CHARS, max_lines: int = 30) -> tuple[str, list[str]]:
    lines, overflow = _limited_lines(value, max_chars=max_chars, max_lines=max_lines)
    if not lines:
        return "", overflow
    parts: list[str] = []
    idx = 0
    while idx < len(lines):
        line = lines[idx]
        if not line.strip():
            idx += 1
            continue

        bullet = _bullet_text(line)
        if bullet:
            items: list[str] = []
            while idx < len(lines):
                item = _bullet_text(lines[idx])
                if not item:
                    break
                idx += 1
                while idx < len(lines):
                    continuation = lines[idx]
                    if (
                        not continuation.strip()
                        or _bullet_text(continuation)
                        or _numbered_text(continuation)
                        or (
                            not continuation.startswith((" ", "\t"))
                            and _looks_like_section(continuation, lines[idx + 1] if idx + 1 < len(lines) else None)
                        )
                    ):
                        break
                    if not continuation.startswith((" ", "\t")) and item.rstrip().endswith((".", "!", "?")):
                        break
                    item = f"{item.rstrip()} {continuation.strip()}"
                    idx += 1
                items.append(item)
            parts.append("<ul>\n" + "\n".join(f"<li>{_rich_inline(item, 500)}</li>" for item in items) + "\n</ul>")
            continue

        numbered = _numbered_text(line)
        if numbered:
            numbered_items: list[tuple[int, str]] = []
            while idx < len(lines):
                item = _numbered_text(lines[idx])
                if not item:
                    break
                number, text = item
                numbered_items.append(item)
                idx += 1
                while idx < len(lines):
                    continuation = lines[idx]
                    if (
                        not continuation.strip()
                        or _bullet_text(continuation)
                        or _numbered_text(continuation)
                        or (
                            not continuation.startswith((" ", "\t"))
                            and _looks_like_section(continuation, lines[idx + 1] if idx + 1 < len(lines) else None)
                        )
                    ):
                        break
                    if not continuation.startswith((" ", "\t")) and text.rstrip().endswith((".", "!", "?")):
                        break
                    text = f"{text.rstrip()} {continuation.strip()}"
                    numbered_items[-1] = (number, text)
                    idx += 1
            numbers = [num for num, _ in numbered_items]
            if numbers == list(range(1, len(numbers) + 1)):
                items = "\n".join(f"<li>{_rich_inline(text, 500)}</li>" for _, text in numbered_items)
                parts.append("<ol>\n" + items + "\n</ol>")
            else:
                parts.extend(_rich_paragraph(f"{num}) {text}") for num, text in numbered_items)
            continue

        if _is_codeish_line(line):
            code_lines = [line.strip()]
            idx += 1
            while idx < len(lines) and _is_codeish_line(lines[idx]):
                code_lines.append(lines[idx].strip())
                idx += 1
            code_text = _html_text("\n".join(code_lines), 1000)
            parts.append(f"<pre><code>{code_text}</code></pre>")
            continue

        next_line = lines[idx + 1] if idx + 1 < len(lines) else None
        path_section = _split_path_section(line)
        if path_section:
            heading, ref = path_section
            parts.append(f"<h4>{_html_text(heading, 100)}</h4>")
            parts.append(f"<p><code>{_html_text(ref, 300)}</code></p>")
            idx += 1
            continue

        if _looks_like_section(line, next_line):
            parts.append(f"<h4>{_html_text(line.rstrip(':'), 100)}</h4>")
            idx += 1
            continue

        paragraph = [line.strip()]
        idx += 1
        while idx < len(lines):
            candidate = lines[idx]
            if (
                not candidate.strip()
                or _bullet_text(candidate)
                or _numbered_text(candidate)
                or _is_codeish_line(candidate)
                or _looks_like_section(candidate, lines[idx + 1] if idx + 1 < len(lines) else None)
            ):
                break
            paragraph.append(candidate.strip())
            idx += 1
        parts.append(_rich_paragraph(" ".join(paragraph)))
    return "\n".join(part for part in parts if part), overflow


def _split_structured_sections(lines: list[str]) -> tuple[list[tuple[str, str, list[str]]], bool]:
    sections: list[tuple[str, str, list[str]]] = []
    current_kind = ""
    current_title = ""
    current_lines: list[str] = []
    has_structured = False

    def flush() -> None:
        nonlocal current_kind, current_title, current_lines
        if current_lines or current_kind:
            sections.append((current_kind, current_title, current_lines))
        current_kind = ""
        current_title = ""
        current_lines = []

    for raw in lines:
        section = section_alias(str(raw or ""))
        if section:
            flush()
            has_structured = True
            current_kind, current_title, _label = section
            current_lines = []
            continue
        current_lines.append(str(raw or "").rstrip())
    flush()
    return sections, has_structured


def _table_cells(line: str) -> list[str]:
    text = str(line or "").strip()
    if "|" not in text:
        return []
    if text.startswith("|"):
        text = text[1:]
    if text.endswith("|"):
        text = text[:-1]
    cells = [cell.strip() for cell in text.split("|")]
    return cells if len(cells) >= 2 and any(cells) else []


def _is_table_separator(cells: list[str]) -> bool:
    return bool(cells) and all(re.fullmatch(r":?-{2,}:?", cell or "") for cell in cells)


def _rich_table_section(lines: list[str], *, rich_cells: bool = False) -> str:
    rows: list[list[str]] = []
    for line in lines:
        if not line.strip():
            continue
        cells = _table_cells(line)
        if not cells:
            continue
        if _is_table_separator(cells):
            continue
        rows.append(cells[:20])
    if len(rows) < 2:
        return ""
    width = max(len(row) for row in rows[:20])
    normalized = [row + [""] * (width - len(row)) for row in rows[:20]]
    header = normalized[0]
    body = normalized[1:]
    cell_html = _rich_inline if rich_cells else _html_text
    html_rows = [
        "<tr>" + "".join(f"<th>{cell_html(cell, 160)}</th>" for cell in header) + "</tr>",
    ]
    html_rows.extend(
        "<tr>" + "".join(f"<td>{cell_html(cell, 160)}</td>" for cell in row) + "</tr>"
        for row in body
    )
    return "<table bordered striped>\n" + "\n".join(html_rows) + "\n</table>"


def _checklist_item(line: str) -> tuple[bool, str] | None:
    match = re.match(r"^\s*(?:[-*+\u2022]\s*)?\[(x|X| )\]\s+(.+)$", line or "")
    if not match:
        return None
    return match.group(1).lower() == "x", match.group(2).strip()


def _rich_checklist_section(lines: list[str]) -> str:
    items: list[tuple[bool, str]] = []
    for line in lines:
        parsed = _checklist_item(line)
        if parsed:
            items.append(parsed)
    if not items:
        return ""
    rendered = []
    for checked, text in items[:40]:
        attr = " checked" if checked else ""
        rendered.append(f"<li><input type=\"checkbox\"{attr}>{_rich_inline(text, 500)}</li>")
    return "<ul>\n" + "\n".join(rendered) + "\n</ul>"


def _looks_like_code_detail(title: str, lines: list[str]) -> bool:
    low = re.sub(r"\s+", " ", str(title or "").strip()).lower()
    if low in CODE_DETAILS_SECTIONS:
        return True
    content = [line for line in lines if line.strip()]
    if not content:
        return False
    codeish = 0
    for line in content:
        stripped = line.strip()
        if _is_codeish_line(line) or stripped.startswith(("{", "[")) or stripped.startswith(("diff ", "@@")):
            codeish += 1
    return codeish >= max(1, len(content) // 2)


def _rich_structured_report(lines: list[str]) -> str:
    sections, has_structured = _split_structured_sections(lines)
    if not has_structured:
        return ""
    parts: list[str] = []
    for kind, title, body in sections:
        body = strip_outer_blank_lines(body)
        if not kind:
            block, _ = _rich_structured_block(body, max_chars=1200, max_lines=20)
            if block:
                parts.append(block)
            continue
        if kind == "summary":
            summary = compact_block(body, max_lines=6, max_chars=900)
            if summary:
                label = title or "Summary"
                parts.append(f"<p><b>{_html_text(label, 80)}:</b> {_html_text(summary, 900)}</p>")
            continue
        if kind == "table":
            table_html = _rich_table_section(body)
            if title:
                parts.append(f"<h4>{_html_text(title, 100)}</h4>")
            if table_html:
                parts.append(table_html)
            elif body:
                block, _ = _rich_structured_block(body, max_chars=1200, max_lines=20)
                if block:
                    parts.append(block)
            continue
        if kind == "checklist":
            heading = title or "Checklist"
            checklist_html = _rich_checklist_section(body)
            parts.append(f"<h4>{_html_text(heading, 100)}</h4>")
            if checklist_html:
                parts.append(checklist_html)
            elif body:
                block, _ = _rich_structured_block(body, max_chars=1200, max_lines=30)
                if block:
                    parts.append(block)
            continue
        if kind == "details":
            summary = title or "Details"
            if _looks_like_code_detail(summary, body):
                proof = "\n".join(line.rstrip() for line in body if line.strip())
                block = f"<pre><code>{_html_text(proof, 1800)}</code></pre>" if proof else ""
            else:
                block, _ = _rich_structured_block(body, max_chars=1800, max_lines=40)
            if block:
                parts.append(f"<details><summary>{_html_text(summary, 100)}</summary>{block}</details>")
            continue
        if kind == "footer":
            footer = compact_block(body, max_lines=3, max_chars=500)
            if footer:
                parts.append(f"<footer>{_html_text(footer, 500)}</footer>")
            continue
    return "\n".join(part for part in parts if part)


def _rich_lines_block(value: str, *, max_chars: int = MAX_RICH_DETAIL_CHARS) -> str:
    block, overflow = _rich_structured_block(value, max_chars=max_chars, max_lines=12)
    if overflow:
        overflow_block, _ = _rich_structured_block(overflow, max_chars=900, max_lines=8)
        if overflow_block:
            block += f"\n<details><summary>More</summary>{overflow_block}</details>"
    return block


def _rich_options_block(options: list[dict[str, str]]) -> str:
    if not options:
        return ""
    numbered: list[tuple[int, str]] = []
    sequential = True
    for idx, opt in enumerate(options[:12], start=1):
        raw_number = str(opt.get("number") or idx)
        try:
            number = int(raw_number)
        except ValueError:
            sequential = False
            number = idx
        if number != idx:
            sequential = False
        numbered.append((number, str(opt.get("label") or "")))
    if sequential:
        items = "\n".join(f"<li>{_html_text(label, 180)}</li>" for _, label in numbered)
        return f"<ol>\n{items}\n</ol>"
    return "\n".join(_rich_paragraph(f"{number}) {label}") for number, label in numbered)


def line_is_question_heading(line: str) -> bool:
    low = noise_key(line)
    return (
        low == "question"
        or low.startswith("question:")
        or low.startswith("decision needed")
        or low.startswith("needs approval")
    )


def is_action_question(lines: list[str]) -> bool:
    tail = compact_block(lines[-6:], max_lines=6, max_chars=800)
    if "?" not in tail:
        return False
    return bool(ACTION_QUESTION_RE.search(tail))


def has_resume_control_noise(raw_text: str) -> bool:
    return bool(RESUME_CONTROL_RE.search(raw_text or ""))


def _blockquote_text(value: str, max_chars: int) -> str:
    lines = sanitize_text(value, max_chars).splitlines()
    return "<br>".join(_rich_inline(line, max_chars) for line in lines)


TURN_COLLAPSED_SECTION_KEYS = {"proof", "logs", "commands", "diff", "raw output", "raw"}
TURN_KNOWN_HEADING_KEYS = {
    "implemented",
    "pushed",
    "verification",
    "what changed",
    "what i did",
    "recommended follow-ups",
    "recommended follow ups",
    "next steps",
    "risks",
    "proof",
    "details",
    "deployment",
    "deployed",
    "summary",
    "result",
}


def _plain_heading_title(value: str) -> str:
    clean = str(value or "").strip()
    clean = re.sub(r"`([^`\n]{1,300})`", r"\1", clean)
    clean = re.sub(r"\*\*([^*\n]{1,300})\*\*", r"\1", clean)
    clean = re.sub(r"(?<!\*)\*([^*\n]{1,180})\*(?!\*)", r"\1", clean)
    clean = clean.strip(" -*_`")
    return re.sub(r"\s+", " ", clean).strip()


def _turn_heading_title(line: str) -> str:
    clean = re.sub(r"^\s{0,3}#{1,6}\s+", "", str(line or "").strip())
    clean = clean.rstrip(":").rstrip(".").strip()
    return re.sub(r"\s+", " ", clean)


def _next_nonempty_line(lines: list[str], start: int) -> str | None:
    for idx in range(start, len(lines)):
        if str(lines[idx] or "").strip():
            return str(lines[idx])
    return None


def _is_table_line(line: str) -> bool:
    return bool(_table_cells(line))


def _is_turn_heading_line(
    line: str,
    next_nonempty: str | None,
    *,
    first_block: bool = False,
    previous_blank: bool = False,
) -> bool:
    clean = str(line or "").strip()
    if not clean or len(clean) > 120:
        return False
    if FENCE_START_RE.match(clean) or _bullet_text(clean) or _numbered_text(clean) or _checklist_item(clean):
        return False
    if clean.startswith(">") or _is_table_line(clean) or _is_codeish_line(clean):
        return False
    words = _turn_heading_title(clean).split()
    if not 1 <= len(words) <= 6:
        return False
    if "`" in clean and len(words) > 3:
        return False
    key = _turn_heading_title(clean).lower()
    if key in TURN_KNOWN_HEADING_KEYS:
        return True
    if clean.startswith("#"):
        return True
    if clean.endswith(":"):
        return True
    if first_block and len(words) <= 4 and not clean.endswith(("!", "?")):
        return True
    if previous_blank and next_nonempty and len(words) <= 4 and not clean.endswith(("!", "?")):
        return True
    return False


def _rich_commit_line(line: str) -> str | None:
    match = COMMIT_LINE_RE.match(str(line or "").strip())
    if not match:
        return None
    return f"<p><code>{_html_text(match.group(1), 40)}</code> {_rich_inline(match.group(2), 500)}</p>"


def _lead_heading_split(line: str) -> tuple[str, str] | None:
    clean = str(line or "").strip()
    match = re.match(r"^(.{8,100}?)\s+[—–]\s+(.+)$", clean)
    if not match:
        return None
    raw_title = match.group(1).strip()
    rest = match.group(2).strip()
    meta_match = re.search(r"\s*\((`[^`\n]{1,120}`)\)\s*$", raw_title)
    if meta_match:
        raw_title = raw_title[:meta_match.start()].strip()
        rest = f"{meta_match.group(1)} — {rest}"
    title = _plain_heading_title(raw_title).rstrip(":").rstrip(".").strip()
    words = title.split()
    if not 2 <= len(words) <= 8:
        return None
    if title.lower() in {"yes", "no", "ok", "okay", "done"}:
        return None
    if title.endswith(("?", "!", ",")):
        return None
    if not rest:
        return None
    return title, rest


def _split_long_paragraph(value: str, *, max_chars: int = 360) -> list[str]:
    text = re.sub(r"\s+", " ", str(value or "").strip())
    if not text:
        return []
    if len(text) <= max_chars:
        return [text]
    sentences = re.split(r"(?<=[.!?])\s+(?=[A-Z0-9\"'`*(])", text)
    chunks: list[str] = []
    current = ""
    for sentence in sentences:
        sentence = sentence.strip()
        if not sentence:
            continue
        if not current:
            current = sentence
            continue
        if len(current) + 1 + len(sentence) <= max_chars:
            current = f"{current} {sentence}"
            continue
        chunks.append(current)
        current = sentence
    if current:
        chunks.append(current)

    expanded: list[str] = []
    for chunk in chunks:
        if len(chunk) <= max_chars + 120:
            expanded.append(chunk)
            continue
        parts = re.split(r"\s+(?:;\s+|→\s+)", chunk)
        if len(parts) <= 1:
            expanded.append(chunk)
            continue
        buf = ""
        for part in parts:
            part = part.strip()
            if not part:
                continue
            if not buf:
                buf = part
            elif len(buf) + 1 + len(part) <= max_chars:
                buf = f"{buf} {part}"
            else:
                expanded.append(buf)
                buf = part
        if buf:
            expanded.append(buf)
    return expanded or [text]


def _rich_paragraph_blocks(value: str) -> list[str]:
    return [block for block in (_rich_paragraph(chunk) for chunk in _split_long_paragraph(value)) if block]


def _render_final_reply_blocks(lines: list[str], *, seen_heading: bool = False) -> str:
    parts: list[str] = []
    idx = 0
    previous_blank = True
    while idx < len(lines):
        line = str(lines[idx] or "").rstrip()
        if not line.strip():
            previous_blank = True
            idx += 1
            continue

        fence = FENCE_START_RE.match(line)
        if fence:
            language = fence.group(1).strip()
            code_lines: list[str] = []
            idx += 1
            while idx < len(lines) and not str(lines[idx]).strip().startswith("```"):
                code_lines.append(str(lines[idx]).rstrip())
                idx += 1
            if idx < len(lines):
                idx += 1
            class_attr = f' class="language-{html.escape(language, quote=True)}"' if language else ""
            parts.append(f"<pre><code{class_attr}>{_html_text(chr(10).join(code_lines), 3000)}</code></pre>")
            previous_blank = False
            continue

        next_nonempty = _next_nonempty_line(lines, idx + 1)
        if _is_turn_heading_line(line, next_nonempty, first_block=not seen_heading, previous_blank=previous_blank):
            title = _turn_heading_title(line)
            key = title.lower()
            if key in TURN_COLLAPSED_SECTION_KEYS:
                body_lines: list[str] = []
                idx += 1
                while idx < len(lines):
                    candidate = str(lines[idx] or "").rstrip()
                    candidate_next = _next_nonempty_line(lines, idx + 1)
                    if candidate.strip() and _is_turn_heading_line(
                        candidate,
                        candidate_next,
                        first_block=False,
                        previous_blank=previous_blank,
                    ):
                        break
                    body_lines.append(candidate)
                    previous_blank = not candidate.strip()
                    idx += 1
                body_html = _render_final_reply_blocks(body_lines, seen_heading=True)
                if body_html:
                    parts.append(f"<details><summary>{_html_text(title, 100)}</summary>{body_html}</details>")
                previous_blank = False
                seen_heading = True
                continue
            tag = "h3" if not seen_heading else "h4"
            parts.append(f"<{tag}>{_html_text(title, 100)}</{tag}>")
            seen_heading = True
            previous_blank = False
            idx += 1
            continue

        if _is_table_line(line) and idx + 1 < len(lines) and _is_table_line(str(lines[idx + 1])):
            table_lines: list[str] = []
            while idx < len(lines) and _is_table_line(str(lines[idx])):
                table_lines.append(str(lines[idx]).rstrip())
                idx += 1
            table_html = _rich_table_section(table_lines, rich_cells=True)
            if table_html:
                parts.append(table_html)
            previous_blank = False
            continue

        checklist = _checklist_item(line)
        if checklist:
            checklist_lines: list[str] = []
            while idx < len(lines) and _checklist_item(str(lines[idx] or "")):
                checklist_lines.append(str(lines[idx]).rstrip())
                idx += 1
            checklist_html = _rich_checklist_section(checklist_lines)
            if checklist_html:
                parts.append(checklist_html)
            previous_blank = False
            continue

        bullet = _bullet_text(line)
        if bullet:
            items: list[str] = []
            while idx < len(lines):
                item = _bullet_text(str(lines[idx] or ""))
                if item is None:
                    break
                idx += 1
                while idx < len(lines):
                    continuation = str(lines[idx] or "")
                    if (
                        not continuation.strip()
                        or _bullet_text(continuation)
                        or _numbered_text(continuation)
                        or _checklist_item(continuation)
                    ):
                        break
                    if not continuation.startswith((" ", "\t")):
                        break
                    item = f"{item.rstrip()} {continuation.strip()}"
                    idx += 1
                items.append(item)
            parts.append("<ul>\n" + "\n".join(f"<li>{_rich_inline(item, 900)}</li>" for item in items) + "\n</ul>")
            previous_blank = False
            continue

        numbered = _numbered_text(line)
        if numbered:
            items: list[tuple[int, str]] = []
            while idx < len(lines):
                parsed = _numbered_text(str(lines[idx] or ""))
                if not parsed:
                    break
                number, text = parsed
                idx += 1
                while idx < len(lines):
                    continuation = str(lines[idx] or "")
                    if (
                        not continuation.strip()
                        or _bullet_text(continuation)
                        or _numbered_text(continuation)
                        or _checklist_item(continuation)
                    ):
                        break
                    if not continuation.startswith((" ", "\t")):
                        break
                    text = f"{text.rstrip()} {continuation.strip()}"
                    idx += 1
                items.append((number, text))
            parts.append("<ol>\n" + "\n".join(f"<li>{_rich_inline(text, 900)}</li>" for _, text in items) + "\n</ol>")
            previous_blank = False
            continue

        if line.strip().startswith(">"):
            quote_lines: list[str] = []
            while idx < len(lines) and str(lines[idx] or "").strip().startswith(">"):
                quote_lines.append(re.sub(r"^\s*>\s?", "", str(lines[idx]).rstrip()))
                idx += 1
            parts.append("<blockquote>" + "<br>".join(_rich_inline(quote, 900) for quote in quote_lines) + "</blockquote>")
            previous_blank = False
            continue

        commit_html = _rich_commit_line(line)
        if commit_html:
            parts.append(commit_html)
            previous_blank = False
            idx += 1
            continue

        lead_split = _lead_heading_split(line) if (previous_blank or not seen_heading) else None
        if lead_split:
            title, rest = lead_split
            tag = "h3" if not seen_heading else "h4"
            parts.append(f"<{tag}>{_html_text(title, 100)}</{tag}>")
            parts.extend(_rich_paragraph_blocks(rest))
            seen_heading = True
            previous_blank = False
            idx += 1
            continue

        if _is_codeish_line(line):
            code_lines = [line.strip()]
            idx += 1
            while idx < len(lines) and _is_codeish_line(str(lines[idx] or "")):
                code_lines.append(str(lines[idx]).strip())
                idx += 1
            parts.append(f"<pre><code>{_html_text(chr(10).join(code_lines), 1800)}</code></pre>")
            previous_blank = False
            continue

        paragraph = [line.strip()]
        idx += 1
        while idx < len(lines):
            candidate = str(lines[idx] or "").rstrip()
            if not candidate.strip():
                break
            candidate_next = _next_nonempty_line(lines, idx + 1)
            if (
                FENCE_START_RE.match(candidate)
                or _bullet_text(candidate)
                or _numbered_text(candidate)
                or _checklist_item(candidate)
                or candidate.strip().startswith(">")
                or _is_codeish_line(candidate)
                or (_is_table_line(candidate) and idx + 1 < len(lines) and _is_table_line(str(lines[idx + 1])))
                or _is_turn_heading_line(candidate, candidate_next, first_block=False, previous_blank=False)
            ):
                break
            paragraph.append(candidate.strip())
            idx += 1
        parts.extend(_rich_paragraph_blocks(" ".join(paragraph)))
        previous_blank = False
    return "\n".join(part for part in parts if part)


def render_final_reply_html(value: str) -> str:
    clean = sanitize_text(str(value or ""), FINAL_REPLY_MAX_CHARS).strip()
    if not clean:
        return ""
    return _render_final_reply_blocks(clean.splitlines())


def _turn_fallback_body_html(assistant_final: str, reserved_chars: int) -> str:
    budget = max(1200, min(FINAL_REPLY_MAX_CHARS, MAX_RICH_HTML_CHARS - reserved_chars - 500))
    while budget >= 900:
        fallback = sanitize_text(assistant_final, budget)
        body = render_final_reply_html(fallback) or _rich_paragraph(fallback)
        if len(body) <= max(900, MAX_RICH_HTML_CHARS - reserved_chars):
            return body
        budget = budget // 2
    return _rich_paragraph(sanitize_text(assistant_final, 900))


def render_turn_item_html(item: dict[str, Any]) -> str:
    user_text = str(item.get("user_text") or "").strip()
    assistant_final = str(item.get("assistant_final_text") or "").strip()
    parts: list[str] = []
    if user_text:
        parts.append(
            "<blockquote>"
            "<b>You asked</b><br>"
            f"{_blockquote_text(user_text, USER_PROMPT_MAX_CHARS)}"
            "</blockquote>"
        )
    body_html = render_final_reply_html(assistant_final)
    if body_html:
        parts.append(body_html)
    elif assistant_final:
        parts.append(_rich_paragraph(assistant_final))
    rendered = "\n".join(part for part in parts if part).strip()
    if len(rendered) > MAX_RICH_HTML_CHARS:
        quote_html = ""
        if user_text:
            quote_html = (
                "<blockquote><b>You asked</b><br>"
                f"{_blockquote_text(user_text, USER_PROMPT_MAX_CHARS)}"
                "</blockquote>"
            )
        body_html = _turn_fallback_body_html(assistant_final, len(quote_html))
        if quote_html:
            rendered = f"{quote_html}\n{body_html}"
        else:
            rendered = body_html
        if len(rendered) > MAX_RICH_HTML_CHARS:
            body_html = _rich_paragraph(sanitize_text(assistant_final, 900))
            return f"{quote_html}\n{body_html}" if quote_html else body_html
    return rendered


def render_decision_item_html(item: dict[str, Any]) -> str:
    user_text = str(item.get("user_text") or "").strip()
    assistant_context = str(item.get("assistant_final_text") or "").strip()
    prompt = str(item.get("summary") or "").strip()
    options = list(item.get("options") or [])
    parts: list[str] = []
    if user_text:
        parts.append(
            "<blockquote>"
            "<b>You asked</b><br>"
            f"{_blockquote_text(user_text, USER_PROMPT_MAX_CHARS)}"
            "</blockquote>"
        )
    if assistant_context:
        context_html = render_final_reply_html(assistant_context)
        if context_html:
            parts.append(context_html)
    parts.append("<h3>Decision needed</h3>")
    if prompt:
        parts.append(_rich_paragraph(prompt))
    if options:
        parts.append(_rich_options_block(options))
    rendered = "\n".join(part for part in parts if part).strip()
    if len(rendered) > MAX_RICH_HTML_CHARS:
        compact = dict(item)
        compact["assistant_final_text"] = ""
        compact["summary"] = sanitize_text(prompt, 900)
        return render_decision_item_html(compact)
    return rendered


def render_feed_item_html(item: dict[str, Any], *, live: bool = False) -> str:
    kind = str(item.get("kind") or "update").lower()
    if kind == "turn":
        return render_turn_item_html(item)
    if kind == "decision":
        return render_decision_item_html(item)
    title = str(item.get("title") or "").strip()
    if not title:
        title = {
            "choices": "Question",
            "question": "Question",
            "blocked": "Blocked",
            "error": "Error",
            "report": "Report",
        }.get(kind, "Update")
    if live:
        title = f"Latest {title}"

    parts = [f"<h3>{_html_text(title, 80)}</h3>"]
    summary = str(item.get("summary") or "").strip()
    detail = str(item.get("detail") or "").strip()
    options = list(item.get("options") or [])

    content_lines = item.get("lines") if isinstance(item.get("lines"), list) else []
    if not content_lines:
        content_lines = [ln for ln in summary.splitlines() if ln.strip()]
        content_lines.extend(ln for ln in detail.splitlines() if ln.strip())

    if options:
        if summary:
            parts.append(_rich_lines_block(summary, max_chars=700))
        parts.append(_rich_options_block(options))
        if detail:
            detail_html = _rich_lines_block(detail, max_chars=1200)
            if detail_html:
                parts.append(f"<details><summary>Details</summary>{detail_html}</details>")
    elif content_lines:
        body_max_lines = 80 if kind in {"report", "blocked", "error"} else 30
        body_max_chars = 5000 if kind in {"report", "blocked", "error"} else MAX_RICH_DETAIL_CHARS
        body_html = _rich_structured_report(content_lines) if kind == "report" else ""
        overflow: list[str] = []
        if not body_html:
            body_html, overflow = _rich_structured_block(
                content_lines,
                max_chars=body_max_chars,
                max_lines=body_max_lines,
            )
        if body_html:
            parts.append(body_html)
        if overflow:
            overflow_html, _ = _rich_structured_block(overflow, max_chars=900, max_lines=10)
            if overflow_html:
                parts.append(f"<details><summary>More</summary>{overflow_html}</details>")

    rendered = "\n".join(part for part in parts if part).strip()
    if len(rendered) > MAX_RICH_HTML_CHARS:
        compact = dict(item)
        compact["detail"] = ""
        compact["summary"] = sanitize_text(str(compact.get("summary") or item_plain_text(item)), 900)
        rendered = render_feed_item_html(compact, live=live)
    if len(rendered) > MAX_RICH_HTML_CHARS:
        title_only = str(item.get("title") or item.get("kind") or "Update")
        return f"<h3>{_html_text(title_only, 80)}</h3>\n{_rich_paragraph(item_plain_text(item)[:900])}"
    return rendered


def render_notice_html(title: str, body: str) -> str:
    return f"<h3>{_html_text(title, 80)}</h3>\n{_rich_lines_block(body, max_chars=900)}"


def contains_marker(text: str, markers: tuple[str, ...]) -> bool:
    low = text.lower()
    for marker in markers:
        escaped = re.escape(marker.lower())
        if " " in marker:
            if marker.lower() in low:
                return True
        elif re.search(rf"\b{escaped}\b", low):
            return True
    return False


def choice_context_lines(lines: list[str], start: int) -> list[str]:
    context: list[str] = []
    for line in lines[max(0, start - 5):start]:
        low = line.lower()
        if (
            line_is_question_heading(line)
            or contains_marker(low, QUESTION_MARKERS)
            or re.search(r"\b(choose|select|pick|approve|which option)\b", low)
            or is_action_question([line])
        ):
            context.append(line)
        elif context:
            context.append(line)
    return context


def has_choice_context(lines: list[str]) -> bool:
    text = compact_block(lines, max_lines=5, max_chars=700)
    low = text.lower()
    return (
        any(line_is_question_heading(line) for line in lines)
        or contains_marker(low, QUESTION_MARKERS)
        or bool(re.search(r"\b(choose|select|pick|approve|which option)\b", low))
        or is_action_question(lines)
    )


def extract_choices(lines: list[str], *, explicit: bool = False) -> dict[str, Any] | None:
    best: tuple[int, int, list[dict[str, str]]] | None = None
    idx = 0
    while idx < len(lines):
        match = option_match(lines[idx])
        if not match:
            idx += 1
            continue
        start = idx
        options: list[dict[str, str]] = []
        seen = set()
        while idx < len(lines):
            item = option_match(lines[idx])
            if not item:
                break
            number = item.group(1)
            label = sanitize_text(item.group(2).strip(), 120)
            if number in seen or not label:
                break
            seen.add(number)
            options.append({"number": number, "label": label})
            idx += 1
        if 2 <= len(options) <= 12:
            best = (start, idx, options)
        idx += 1
    if not best:
        return None
    start, end, options = best
    context = choice_context_lines(lines, start)
    if not explicit and not has_choice_context(context):
        return None
    question = compact_block(context, max_lines=3, max_chars=500) or "Choose a response."
    body = "\n".join(f"{opt['number']}) {opt['label']}" for opt in options)
    text = f"Question\n{question}\n\n{body}"
    prompt_id = prompt_id_for(text, options)
    return {
        "kind": "choices",
        "title": "Question",
        "summary": question,
        "detail": "",
        "text": text,
        "options": options,
        "prompt_id": prompt_id,
        "notify": True,
    }


def extract_choices_from_raw(raw_text: str) -> dict[str, Any] | None:
    safe = ANSI_RE.sub("", sanitize_text(str(raw_text or ""), FEED_MAX_CHARS))
    matches = list(CHOICES_BLOCK_RE.finditer(safe))
    if not matches:
        return None
    body_lines = strip_outer_blank_lines(matches[-1].group(1).splitlines())
    return extract_choices(body_lines, explicit=True)


def extract_clean_feed_item(
    pane: dict[str, Any],
    entry: dict[str, Any],
    raw_text: str,
    *,
    allow_unbounded_reports: bool = ALLOW_UNBOUNDED_REPORTS,
) -> dict[str, Any] | None:
    status = str(pane.get("agent_status") or "").lower()
    bounded_report = extract_bounded_report_from_raw(raw_text)
    if bounded_report and status in {"done", "idle"}:
        title, body = bounded_report
        if body.strip():
            return make_feed_item("report", title, body, notify=False)
        return None

    lines = clean_feed_lines(raw_text)
    if not lines:
        return None

    tail = compact_block(lines, max_lines=80, max_chars=5000)
    if not tail:
        return None

    if allow_unbounded_reports:
        report_idx = report_start_index(lines)
        if report_idx is not None and status in {"done", "idle"}:
            title, body = report_title_and_body(lines)
            if body.strip():
                return make_feed_item("report", title, body, notify=False)
            return None

    choices = extract_choices_from_raw(raw_text) or extract_choices(lines)
    if choices:
        return choices
    if is_action_question(lines):
        return make_feed_item("question", "Question", tail, notify=True)
    if status in {"blocked", "error"}:
        heading = "Blocked" if status == "blocked" else "Error"
        return make_feed_item(status, heading, tail, notify=True)
    return None


def clean_feed_hash(item: dict[str, Any], *, include_render_version: bool = True) -> str:
    payload = {
        "kind": item.get("kind"),
        "text": item.get("text"),
        "title": item.get("title"),
        "summary": item.get("summary"),
        "detail": item.get("detail"),
        "lines": item.get("lines") or [],
        "options": item.get("options") or [],
        "turn_id": item.get("turn_id"),
        "decision_id": item.get("decision_id"),
        "user_text": item.get("user_text"),
        "assistant_final_text": item.get("assistant_final_text"),
    }
    if include_render_version:
        payload["render_version"] = RICH_RENDER_VERSION
    return hashlib.sha256(json.dumps(payload, sort_keys=True).encode("utf-8")).hexdigest()


def recent_attempt(entry: dict[str, Any], item_hash: str, ttl_seconds: int = CLEAN_ATTEMPT_TTL_SECONDS) -> bool:
    if entry.get("last_clean_attempt_hash") != item_hash:
        return False
    try:
        then = _dt.datetime.fromisoformat(str(entry.get("last_clean_attempt_at", "")).replace("Z", "+00:00"))
    except Exception:
        return False
    return (_dt.datetime.now(tz=_dt.timezone.utc) - then).total_seconds() < ttl_seconds


def feed_text_has_ui_noise(text: str) -> bool:
    for raw in str(text or "").splitlines():
        if not raw.strip():
            continue
        low = noise_key(raw)
        if not low:
            continue
        if TOOL_START_RE.match(raw):
            return True
        if is_tui_status_noise(raw):
            return True
        if low.startswith((
            "bash(",
            "started task-",
            "running in the background",
            "tip: use /btw",
            "brewed for",
            "* brewed for",
        )):
            return True
        if low in PROCESS_OUTPUT_EXACT or any(low.startswith(prefix) for prefix in PROCESS_OUTPUT_PREFIXES):
            return True
        if low.startswith(("{", "[")) and low.endswith(("}", "]")):
            if re.search(r'"(?:ok|changed|message|created|sent|panes)"\s*:', low):
                return True
    return False


def clear_clean_feed_state(entry: dict[str, Any]) -> None:
    for key in (
        "last_clean_hash",
        "last_clean_semantic_hash",
        "last_clean_render_hash",
        "last_clean_message_id",
        "last_clean_kind",
        "last_clean_text",
        "last_clean_item",
        "last_clean_sent_at",
        "last_clean_send_error",
        "last_clean_attempt_hash",
        "last_clean_attempt_at",
        "last_turn_id",
        "last_turn_available",
        "last_turn_reason",
        "active_prompt",
        "awaiting_detail",
    ):
        entry.pop(key, None)


def clear_topic_mapping(entry: dict[str, Any], reason: str = "") -> None:
    """Drop Telegram objects tied to a deleted forum topic, preserving pane identity."""
    old_topic_id = str(entry.get("topic_id") or "")
    for key in (
        "topic_id",
        "card_message_id",
        "card_hash",
        "card_status_hash",
        "card_format",
        "status_marker_message_id",
        "status_marker_hash",
        "status_marker_text",
        "status_marker_sent_at",
        "last_status_hash",
        "last_notified_status",
        "last_sent_at",
        "last_topic_verified_at",
        "last_topic_verify_attempt_at",
        "last_topic_verify_error",
        "last_topic_verify_error_at",
        "topic_rename_pending_at",
        "topic_rename_from",
        "topic_rename_to",
    ):
        entry.pop(key, None)
    clear_clean_feed_state(entry)
    entry["topic_missing_at"] = utc_now()
    if old_topic_id:
        entry["topic_missing_id"] = old_topic_id
    if reason:
        entry["topic_missing_reason"] = sanitize_text(reason, 500)


def choice_needs_detail(option: dict[str, str]) -> bool:
    if _boolish(option.get("needs_detail")):
        return True
    label = str(option.get("label") or "").lower()
    number = str(option.get("number") or "")
    if number.lower() == "custom" or str(option.get("id") or "").lower() == "custom":
        return True
    if "send_text" in option and not str(option.get("send_text") or "").strip():
        return True
    return number == "4" or any(word in label for word in ("detail", "feedback", "other", "refine", "custom"))


def choices_reply_markup(prompt_id: str, options: list[dict[str, str]]) -> dict[str, Any]:
    rows: list[list[dict[str, str]]] = []
    has_custom_button = False
    for idx, opt in enumerate(options[:12], start=1):
        number = str(opt.get("number") or idx)
        callback_id = str(opt.get("callback_id") or _callback_id(number, str(idx)))
        label = re.sub(r"\s+", " ", str(opt.get("label") or "")).strip()
        is_custom = number.lower() == "custom" or callback_id.lower() == "custom" or str(opt.get("id") or "").lower() == "custom"
        if is_custom:
            has_custom_button = True
            button_text = label or "Custom reply"
        else:
            display_number = number if number.isdigit() else str(idx)
            button_text = f"{display_number}. {label}" if label else display_number
        action = "d" if choice_needs_detail(opt) else "c"
        rows.append([{"text": button_text[:64], "callback_data": safe_callback_data(action, prompt_id, callback_id)}])
    if not has_custom_button:
        rows.append([{"text": "Tell me differently", "callback_data": safe_callback_data("d", prompt_id, "custom")}])
    return {"inline_keyboard": rows}


def prompt_delivery_state(item: dict[str, Any]) -> tuple[dict[str, Any] | None, dict[str, Any] | None, bool]:
    if str(item.get("kind") or "").lower() not in {"choices", "decision"}:
        return None, None, True
    options = list(item.get("options") or [])
    if not options:
        return None, None, True
    plain_text = item_plain_text(item)
    normalized_options: list[dict[str, str]] = []
    for idx, raw in enumerate(options[:12], start=1):
        opt = dict(raw)
        raw_id = str(opt.get("callback_id") or opt.get("number") or opt.get("id") or idx)
        opt["callback_id"] = _callback_id(raw_id, str(idx))
        normalized_options.append(opt)
    options = normalized_options
    prompt_id = _prompt_callback_id(str(item.get("prompt_id") or ""), plain_text, options)
    item["prompt_id"] = prompt_id
    item["options"] = options
    active_prompt = {
        "id": prompt_id,
        "text": plain_text,
        "item": item,
        "options": options,
        "created_at": utc_now(),
    }
    if item.get("decision_id"):
        active_prompt["decision_id"] = str(item.get("decision_id") or "")
    return choices_reply_markup(prompt_id, options), active_prompt, False


def format_status(
    pane: dict[str, Any],
    *,
    include_recent: bool = False,
    include_commands: bool = False,
) -> str:
    obj = status_object(pane)
    lines = [
        f"Herdr pane {obj['pane_id']}",
        f"Status: {obj['status'] or 'unknown'}",
    ]
    if obj["agent"]:
        lines.append(f"Agent: {obj['agent']}")
    if obj["label"]:
        lines.append(f"Label: {obj['label']}")
    if obj["cwd"]:
        lines.append(f"Path: {obj['cwd']}")
    lines.append(f"Workspace/tab: {obj['workspace']} / {obj['tab']}")
    if include_recent:
        tail = recent_tail(obj["pane_id"])
        if tail:
            lines.append("")
            lines.append("Recent visible output:")
            lines.append(tail)
    if include_commands:
        lines.append("")
        lines.append("Commands: /status, /read [lines], /send <text>, /keys <keys>")
    return sanitize_text("\n".join(lines), max_chars=MAX_STATUS_CHARS)


def format_debug(pane: dict[str, Any] | None, entry: dict[str, Any]) -> str:
    lines = [
        "Debug",
        f"pane_id: {entry.get('pane_id') or ''}",
        f"topic_id: {entry.get('topic_id') or ''}",
        f"topic_name: {entry.get('topic_name') or ''}",
        f"last_known_status: {entry.get('last_known_status') or ''}",
        f"workspace: {entry.get('workspace') or ''}",
        f"tab: {entry.get('tab') or ''}",
        f"terminal_id: {entry.get('terminal_id') or ''}",
        f"agent_session_id: {entry.get('agent_session_id') or ''}",
        f"last_clean_kind: {entry.get('last_clean_kind') or ''}",
        f"last_clean_hash: {entry.get('last_clean_hash') or ''}",
        f"last_clean_sent_at: {entry.get('last_clean_sent_at') or ''}",
        f"last_turn_id: {entry.get('last_turn_id') or ''}",
        f"last_turn_available: {entry.get('last_turn_available')}",
        f"last_turn_reason: {entry.get('last_turn_reason') or ''}",
        f"card_message_id: {entry.get('card_message_id') or ''}",
        f"card_hash: {entry.get('card_hash') or ''}",
        f"card_status_hash: {entry.get('card_status_hash') or ''}",
        f"card_format: {entry.get('card_format') or ''}",
        f"status_marker_message_id: {entry.get('status_marker_message_id') or ''}",
        f"status_marker_hash: {entry.get('status_marker_hash') or ''}",
        f"last_seen_at: {entry.get('last_seen_at') or ''}",
    ]
    if pane:
        lines.extend([
            f"agent: {pane.get('agent') or ''}",
            f"agent_status: {pane.get('agent_status') or ''}",
            f"cwd: {compact_path(pane.get('cwd') or pane.get('foreground_cwd') or '')}",
        ])
    return sanitize_text("\n".join(lines), max_chars=MAX_REPLY_CHARS)


def latest_clean_report(entry: dict[str, Any], pane: dict[str, Any] | None = None) -> str:
    item = entry.get("last_clean_item") if isinstance(entry.get("last_clean_item"), dict) else {}
    if item:
        text = item_plain_text(item)
        if text:
            return text
    text = str(entry.get("last_clean_text") or "").strip()
    if text:
        return text
    if pane:
        raw = pane_feed_output(str(pane.get("pane_id") or ""), manual=True)
        item = extract_clean_feed_item(pane, entry, raw, allow_unbounded_reports=True)
        if item:
            return str(item.get("text") or "").strip()
    return "No clean report is available yet."


def latest_clean_item(entry: dict[str, Any], pane: dict[str, Any] | None = None) -> dict[str, Any] | None:
    item = entry.get("last_clean_item") if isinstance(entry.get("last_clean_item"), dict) else {}
    if item:
        return dict(item)
    text = str(entry.get("last_clean_text") or "").strip()
    if text:
        kind = str(entry.get("last_clean_kind") or "report")
        title = {
            "choices": "Question",
            "question": "Question",
            "blocked": "Blocked",
            "error": "Error",
            "report": "Report",
        }.get(kind, "Report")
        return make_feed_item(kind, title, text, notify=False)
    if pane:
        raw = pane_feed_output(str(pane.get("pane_id") or ""), manual=True)
        return extract_clean_feed_item(pane, entry, raw, allow_unbounded_reports=True)
    return None


def latest_turn_item(entry: dict[str, Any], pane: dict[str, Any] | None = None) -> dict[str, Any] | None:
    if pane:
        item = extract_turn_feed_item(pane, entry)
        if item:
            return item
    item = entry.get("last_clean_item") if isinstance(entry.get("last_clean_item"), dict) else {}
    if str(item.get("kind") or "").lower() == "turn":
        return dict(item)
    return None


def latest_turn_report(entry: dict[str, Any], pane: dict[str, Any] | None = None) -> str:
    item = latest_turn_item(entry, pane)
    if item:
        return item_plain_text(item)
    return "No structured turn is available yet."


def live_status_item(pane: dict[str, Any]) -> dict[str, Any]:
    status = str(pane.get("agent_status") or "unknown").lower()
    if status in {"blocked"}:
        return make_feed_item("status", "Waiting", "This pane is waiting for input or is blocked.", notify=False)
    if status in {"error"}:
        return make_feed_item("status", "Error", "This pane reported an error.", notify=False)
    if status in {"done"}:
        return make_feed_item("status", "Done", "Latest work appears complete.", notify=False)
    if status in {"idle"}:
        return make_feed_item("status", "Idle", "No active change.", notify=False)
    if status in {"unknown"}:
        return make_feed_item("status", "Status", "Current pane state is unclear.", notify=False)
    return make_feed_item("status", "Working", "Work is in progress.", notify=False)


def workflow_summary(pane: dict[str, Any]) -> str:
    counts = pane.get("workflow_counts")
    if isinstance(counts, dict):
        total = int(counts.get("total") or counts.get("count") or 0)
        done = int(counts.get("done") or counts.get("completed") or counts.get("succeeded") or 0)
        active = int(counts.get("active") or counts.get("running") or counts.get("working") or 0)
        if total:
            if active:
                return f"Working on {done}/{total} workflows; {active} active."
            return f"Workflows {done}/{total}."
    workflows = pane.get("workflows")
    if isinstance(workflows, list) and workflows:
        total = len(workflows)
        done = 0
        active = 0
        for workflow in workflows:
            if not isinstance(workflow, dict):
                continue
            status = str(workflow.get("status") or workflow.get("state") or "").lower()
            if status in {"done", "complete", "completed", "succeeded", "success"}:
                done += 1
            elif status in {"active", "running", "working", "in_progress", "pending"}:
                active += 1
        if active:
            return f"Working on {done}/{total} workflows; {active} active."
        return f"Workflows {done}/{total}."
    total_raw = pane.get("workflow_total") or pane.get("workflows_total")
    done_raw = pane.get("workflow_done") or pane.get("workflows_done") or pane.get("workflow_completed")
    try:
        total = int(total_raw or 0)
        done = int(done_raw or 0)
    except Exception:
        return ""
    if total:
        return f"Working on {done}/{total} workflows."
    return ""


def status_marker_content(pane: dict[str, Any]) -> tuple[str, str]:
    status = str(pane.get("agent_status") or "unknown").lower()
    workflows = workflow_summary(pane)
    if status == "working":
        return "🟡 Working", workflows or "Work is in progress."
    if status == "idle":
        return "🟢 Idle", workflows or "No active work."
    if status == "done":
        return "✅ Done", workflows or "Latest work finished."
    if status == "blocked":
        return "🟠 Waiting", workflows or "Waiting for input or blocked."
    if status == "error":
        return "🔴 Error", workflows or "This pane reported an error."
    return "⚪ Status", workflows or "Current pane state is unclear."


def status_marker_hash(pane: dict[str, Any]) -> str:
    title, body = status_marker_content(pane)
    payload = {
        "version": 1,
        "status": str(pane.get("agent_status") or "unknown").lower(),
        "title": title,
        "body": body,
    }
    return hashlib.sha256(json.dumps(payload, sort_keys=True).encode("utf-8")).hexdigest()


STATUS_ICON_ENV_KEYS = {
    "working": "HERDR_TELEGRAM_TOPICS_STATUS_ICON_WORKING",
    "idle": "HERDR_TELEGRAM_TOPICS_STATUS_ICON_IDLE",
    "done": "HERDR_TELEGRAM_TOPICS_STATUS_ICON_DONE",
    "blocked": "HERDR_TELEGRAM_TOPICS_STATUS_ICON_BLOCKED",
    "error": "HERDR_TELEGRAM_TOPICS_STATUS_ICON_ERROR",
    "workflow": "HERDR_TELEGRAM_TOPICS_STATUS_ICON_WORKFLOW",
    "unknown": "HERDR_TELEGRAM_TOPICS_STATUS_ICON_UNKNOWN",
}

STATUS_ICON_EMOJI_ENV_KEYS = {
    "working": "HERDR_TELEGRAM_TOPICS_STATUS_ICON_WORKING_EMOJI",
    "idle": "HERDR_TELEGRAM_TOPICS_STATUS_ICON_IDLE_EMOJI",
    "done": "HERDR_TELEGRAM_TOPICS_STATUS_ICON_DONE_EMOJI",
    "blocked": "HERDR_TELEGRAM_TOPICS_STATUS_ICON_BLOCKED_EMOJI",
    "error": "HERDR_TELEGRAM_TOPICS_STATUS_ICON_ERROR_EMOJI",
    "workflow": "HERDR_TELEGRAM_TOPICS_STATUS_ICON_WORKFLOW_EMOJI",
    "unknown": "HERDR_TELEGRAM_TOPICS_STATUS_ICON_UNKNOWN_EMOJI",
}

STATUS_ICON_DEFAULT_EMOJI = {
    "working": "⚡️",
    "idle": "☕️",
    "done": "✅",
    "blocked": "❗️",
    "error": "‼️",
    "workflow": "📈",
    "unknown": "❓",
}


def status_icon_key(pane: dict[str, Any]) -> str:
    status = str(pane.get("agent_status") or "unknown").lower()
    if status == "working" and workflow_summary(pane):
        return "workflow"
    if status in {"working", "idle", "done", "blocked", "error"}:
        return status
    return "unknown"


def status_icon_emoji(key: str) -> str:
    env_key = STATUS_ICON_EMOJI_ENV_KEYS.get(key, "")
    return (os.getenv(env_key, "") if env_key else "").strip() or STATUS_ICON_DEFAULT_EMOJI.get(key, "❓")


def status_icon_explicit_id(key: str) -> str:
    env_key = STATUS_ICON_ENV_KEYS.get(key, "")
    return (os.getenv(env_key, "") if env_key else "").strip()


def cache_fresh(value: str, ttl_seconds: int) -> bool:
    try:
        then = _dt.datetime.fromisoformat(value.replace("Z", "+00:00"))
    except Exception:
        return False
    return (_dt.datetime.now(tz=_dt.timezone.utc) - then).total_seconds() <= ttl_seconds


def forum_icon_cache(telegram: dict[str, Any]) -> dict[str, Any]:
    cache = telegram.setdefault("forum_topic_icons", {})
    if not isinstance(cache, dict):
        cache = {}
        telegram["forum_topic_icons"] = cache
    cache.setdefault("by_emoji", {})
    return cache


def refresh_forum_icon_cache(telegram: dict[str, Any]) -> dict[str, Any]:
    cache = forum_icon_cache(telegram)
    fetched_at = str(cache.get("fetched_at") or "")
    if cache.get("by_emoji") and cache_fresh(fetched_at, STATUS_ICON_CACHE_TTL_SECONDS):
        return cache
    response = telegram_api("getForumTopicIconStickers", {})
    by_emoji: dict[str, str] = {}
    for sticker in response.get("result") or []:
        if not isinstance(sticker, dict):
            continue
        emoji = str(sticker.get("emoji") or "").strip()
        custom_emoji_id = str(sticker.get("custom_emoji_id") or "").strip()
        if emoji and custom_emoji_id and emoji not in by_emoji:
            by_emoji[emoji] = custom_emoji_id
    cache["by_emoji"] = by_emoji
    cache["fetched_at"] = utc_now()
    cache.pop("last_error", None)
    cache.pop("last_error_at", None)
    return cache


def status_icon_custom_emoji_id(telegram: dict[str, Any], pane: dict[str, Any]) -> tuple[str, str, str]:
    key = status_icon_key(pane)
    keys = [key]
    if key == "workflow":
        keys.append("working")
    keys.append("unknown")
    for candidate in keys:
        explicit = status_icon_explicit_id(candidate)
        if explicit:
            return explicit, candidate, status_icon_emoji(candidate)
    try:
        cache = refresh_forum_icon_cache(telegram)
    except Exception as exc:
        cache = forum_icon_cache(telegram)
        cache["last_error"] = sanitize_text(str(exc), 500)
        cache["last_error_at"] = utc_now()
        return "", key, status_icon_emoji(key)
    by_emoji = cache.get("by_emoji") if isinstance(cache.get("by_emoji"), dict) else {}
    for candidate in keys:
        emoji = status_icon_emoji(candidate)
        custom_emoji_id = str(by_emoji.get(emoji) or "").strip()
        if custom_emoji_id:
            return custom_emoji_id, candidate, emoji
    return "", key, status_icon_emoji(key)


def edit_topic_icon(chat_id: str, topic_id: str | int, icon_custom_emoji_id: str) -> bool:
    payload: dict[str, Any] = {
        "chat_id": chat_id,
        "message_thread_id": str(topic_id),
        "icon_custom_emoji_id": icon_custom_emoji_id,
    }
    return bool(telegram_api("editForumTopic", payload).get("result"))


def update_topic_status_icon(
    chat_id: str,
    entry: dict[str, Any],
    pane: dict[str, Any],
    *,
    telegram: dict[str, Any],
) -> dict[str, Any]:
    if not STATUS_ICON_ENABLED:
        return {"ok": False, "attempted": False, "kind": "disabled"}
    topic_id = str(entry.get("topic_id") or "")
    if not topic_id:
        return {"ok": False, "attempted": False, "kind": "missing_topic"}
    icon_id, icon_key, emoji = status_icon_custom_emoji_id(telegram, pane)
    if not icon_id:
        entry["last_topic_status_icon_missing"] = icon_key
        entry["last_topic_status_icon_missing_emoji"] = emoji
        entry["last_topic_status_icon_missing_at"] = utc_now()
        return {"ok": False, "attempted": False, "kind": "no_icon", "icon_key": icon_key, "emoji": emoji}
    if str(entry.get("topic_status_icon_custom_emoji_id") or "") == icon_id:
        return {"ok": True, "attempted": False, "kind": "unchanged", "icon_key": icon_key, "emoji": emoji}
    retry_key = f"{icon_id}:{icon_key}"
    if str(entry.get("last_topic_status_icon_attempt_key") or "") == retry_key:
        last_attempt = str(entry.get("last_topic_status_icon_attempt_at") or "")
        if last_attempt and cache_fresh(last_attempt, STATUS_ICON_RETRY_SECONDS):
            return {"ok": False, "attempted": False, "kind": "retry_deferred", "icon_key": icon_key, "emoji": emoji}
    entry["last_topic_status_icon_attempt_key"] = retry_key
    entry["last_topic_status_icon_attempt_at"] = utc_now()
    try:
        ok = edit_topic_icon(chat_id, topic_id, icon_id)
    except RateLimited:
        raise
    except BridgeError as exc:
        kind = classify_telegram_error(exc)
        entry["last_topic_status_icon_error"] = sanitize_text(str(exc), 500)
        entry["last_topic_status_icon_error_at"] = utc_now()
        result = {"ok": False, "attempted": True, "kind": kind, "error": str(exc), "icon_key": icon_key, "emoji": emoji}
        if kind == "topic_not_found":
            result["topic_missing"] = True
        return result
    if ok:
        entry["topic_status_icon_key"] = icon_key
        entry["topic_status_icon_emoji"] = emoji
        entry["topic_status_icon_custom_emoji_id"] = icon_id
        entry["topic_status_icon_updated_at"] = utc_now()
        entry.pop("last_topic_status_icon_error", None)
        entry.pop("last_topic_status_icon_error_at", None)
        entry.pop("last_topic_status_icon_missing", None)
        entry.pop("last_topic_status_icon_missing_emoji", None)
        entry.pop("last_topic_status_icon_missing_at", None)
    return {"ok": bool(ok), "attempted": True, "kind": "updated" if ok else "failed", "icon_key": icon_key, "emoji": emoji}


def pane_input_needs_file(text: str) -> bool:
    value = str(text or "")
    if len(value) >= PANE_INPUT_FILE_CHARS:
        return True
    return value.count("\n") + 1 >= PANE_INPUT_FILE_LINES


def safe_file_component(value: str, fallback: str = "pane") -> str:
    text = re.sub(r"[^A-Za-z0-9_.-]+", "-", str(value or "").strip())
    text = text.strip(".-")
    return text[:80] or fallback


def write_inbound_pane_message(pane_id: str, text: str) -> Path:
    root = state_path().parent / "inbound" / safe_file_component(pane_id)
    root.mkdir(parents=True, exist_ok=True)
    try:
        root.chmod(0o700)
    except OSError:
        pass
    content = str(text or "")
    if len(content) > PANE_INPUT_FILE_MAX_CHARS:
        content = content[:PANE_INPUT_FILE_MAX_CHARS] + "\n\n[Herdres truncated this inbound Telegram message locally.]"
    digest = hashlib.sha256(content.encode("utf-8")).hexdigest()[:12]
    stamp = _dt.datetime.now(tz=_dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    path = root / f"{stamp}-{digest}.txt"
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(fd, "w", encoding="utf-8") as fh:
        fh.write(content)
        if not content.endswith("\n"):
            fh.write("\n")
    return path


def pane_input_file_instruction(path: Path, text: str) -> str:
    preview = re.sub(r"\s+", " ", str(text or "").strip())
    preview = sanitize_text(preview, 420)
    line_count = str(text or "").count("\n") + 1 if text else 0
    char_count = len(str(text or ""))
    suffix = f" Preview: {preview}" if preview else ""
    return (
        "Telegram topic message received. "
        f"The full owner message is saved at {path}. "
        "Read that file and treat its contents as the user's instruction; then respond to the owner. "
        f"It has {line_count} lines and {char_count} chars."
        f"{suffix}"
    )


def send_to_pane(pane_id: str, text: str, *, timeout: int = 8) -> tuple[bool, str]:
    pane = pane_by_id(pane_id)
    if not pane:
        return False, "Herdr pane is not currently live."
    outbound = str(text or "")
    if pane_input_needs_file(outbound):
        try:
            inbound_path = write_inbound_pane_message(pane_id, outbound)
        except OSError as exc:
            return False, f"Could not write inbound message file: {sanitize_text(str(exc), 300)}"
        outbound = pane_input_file_instruction(inbound_path, outbound)
    proc = run_cmd([herdr_bin(), "pane", "run", pane_id, outbound], timeout=timeout)
    if proc.returncode != 0:
        return False, sanitize_text(proc.stderr or proc.stdout, 800)
    return True, ""


def telegram_token() -> str:
    load_dotenv()
    token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
    if not token:
        raise BridgeError("TELEGRAM_BOT_TOKEN is not available")
    return token


def dry_run_enabled() -> bool:
    return os.getenv("HERDR_TELEGRAM_TOPICS_DRY_RUN", "").lower() in {"1", "true", "yes", "on"}


def dry_run_result(method: str, payload: dict[str, Any]) -> dict[str, Any]:
    now_id = int(time.time() * 1000) % 100000000
    safe_payload = dict(payload)
    if "rich_message" in safe_payload:
        safe_payload["rich_message"] = "[rich_message]"
    if "reply_markup" in safe_payload:
        safe_payload["reply_markup"] = "[reply_markup]"
    print(json.dumps({"dry_run_method": method, "payload": safe_payload}, sort_keys=True), file=sys.stderr)
    if method == "getChat":
        return {"ok": True, "result": {"type": "supergroup", "is_forum": True}}
    if method == "getMe":
        return {"ok": True, "result": {"id": 1}}
    if method == "getChatMember":
        return {"ok": True, "result": {"status": "administrator", "can_manage_topics": True}}
    if method == "createForumTopic":
        return {"ok": True, "result": {"message_thread_id": now_id}}
    if method == "getForumTopicIconStickers":
        return {
            "ok": True,
            "result": [
                {"emoji": "⚡️", "custom_emoji_id": "dry-working"},
                {"emoji": "☕️", "custom_emoji_id": "dry-idle"},
                {"emoji": "✅", "custom_emoji_id": "dry-done"},
                {"emoji": "❗️", "custom_emoji_id": "dry-blocked"},
                {"emoji": "‼️", "custom_emoji_id": "dry-error"},
                {"emoji": "📈", "custom_emoji_id": "dry-workflow"},
                {"emoji": "❓", "custom_emoji_id": "dry-unknown"},
            ],
        }
    if method in {"sendMessage", "sendRichMessage", "editMessageText"}:
        return {"ok": True, "result": {"message_id": now_id}}
    return {"ok": True, "result": True}


def telegram_api(method: str, payload: dict[str, Any]) -> dict[str, Any]:
    if dry_run_enabled():
        return dry_run_result(method, payload)
    token = telegram_token()
    url = f"https://api.telegram.org/bot{token}/{method}"
    data = urllib.parse.urlencode(payload).encode("utf-8")
    request = urllib.request.Request(url, data=data, method="POST")
    try:
        with urllib.request.urlopen(request, timeout=20) as resp:
            body = resp.read().decode("utf-8")
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        try:
            parsed = json.loads(body)
        except Exception:
            raise BridgeError(f"Telegram {method} failed: HTTP {exc.code}") from exc
        params = parsed.get("parameters") or {}
        if exc.code == 429 and params.get("retry_after"):
            raise RateLimited(int(params["retry_after"])) from exc
        desc = sanitize_text(str(parsed.get("description") or f"HTTP {exc.code}"), 500)
        raise BridgeError(f"Telegram {method} failed: {desc}") from exc
    except Exception as exc:
        raise BridgeError(f"Telegram {method} failed: {exc}") from exc
    parsed = json.loads(body)
    if not parsed.get("ok"):
        params = parsed.get("parameters") or {}
        if params.get("retry_after"):
            raise RateLimited(int(params["retry_after"]))
        raise BridgeError(f"Telegram {method} failed: {sanitize_text(str(parsed.get('description')), 500)}")
    return parsed


def telegram_message_id(response: dict[str, Any]) -> str | None:
    result = response.get("result") if isinstance(response, dict) else None
    if isinstance(result, dict) and result.get("message_id") is not None:
        return str(result.get("message_id"))
    return None


def classify_telegram_error(exc: Exception) -> str:
    text = str(exc).lower()
    if isinstance(exc, RateLimited):
        return "rate_limited"
    if (
        "message thread not found" in text
        or "message_thread_id_invalid" in text
        or "message thread invalid" in text
        or "thread not found" in text
        or "forum topic not found" in text
        or "topic not found" in text
        or "topic_deleted" in text
    ):
        return "topic_not_found"
    if (
        "sendrichmessage" in text
        and ("not found" in text or "does not exist" in text or "no such method" in text or "http 404" in text)
    ):
        return "capability"
    if "method" in text and ("not found" in text or "does not exist" in text):
        return "capability"
    if "message is not modified" in text:
        return "not_modified"
    if "topic_not_modified" in text:
        return "not_modified"
    if (
        "message to edit not found" in text
        or "message_id_invalid" in text
        or "message can't be edited" in text
    ):
        return "not_found"
    if "bad request" in text or "can't parse" in text or "entity" in text or "unsupported" in text:
        return "bad_request"
    if any(fragment in text for fragment in ("timed out", "timeout", "temporarily", "network", "connection", "http 5")):
        return "transient"
    return "transient"


def result_topic_missing(result: dict[str, Any] | None) -> bool:
    return isinstance(result, dict) and (
        bool(result.get("topic_missing")) or str(result.get("kind") or "") == "topic_not_found"
    )


def topic_verify_due(entry: dict[str, Any], ttl_seconds: int = TOPIC_VERIFY_TTL_SECONDS) -> bool:
    if not entry.get("topic_id"):
        return False
    try:
        checked = _dt.datetime.fromisoformat(
            str(entry.get("last_topic_verified_at", "")).replace("Z", "+00:00")
        )
    except Exception:
        try:
            checked = _dt.datetime.fromisoformat(
                str(entry.get("last_topic_verify_attempt_at", "")).replace("Z", "+00:00")
            )
        except Exception:
            return True
    return (_dt.datetime.now(tz=_dt.timezone.utc) - checked).total_seconds() > ttl_seconds


def verify_topic_mapping(chat_id: str, entry: dict[str, Any]) -> dict[str, Any]:
    topic_id = str(entry.get("topic_id") or "")
    if not topic_id:
        return {"ok": False, "kind": "missing_local_topic"}
    name = str(entry.get("topic_name") or "Task")
    try:
        edit_topic(chat_id, topic_id, name)
    except RateLimited:
        raise
    except BridgeError as exc:
        kind = classify_telegram_error(exc)
        if kind == "not_modified":
            entry["last_topic_verified_at"] = utc_now()
            entry.pop("last_topic_verify_attempt_at", None)
            entry.pop("last_topic_verify_error", None)
            entry.pop("last_topic_verify_error_at", None)
            entry.pop("topic_missing_at", None)
            entry.pop("topic_missing_id", None)
            entry.pop("topic_missing_reason", None)
            entry.pop("topic_rename_pending_at", None)
            entry.pop("topic_rename_from", None)
            entry.pop("topic_rename_to", None)
            return {"ok": True, "kind": kind}
        if kind == "topic_not_found":
            return {"ok": False, "kind": kind, "topic_missing": True, "error": str(exc)}
        entry["last_topic_verify_attempt_at"] = utc_now()
        return {"ok": False, "kind": kind, "error": str(exc), "transient": kind == "transient"}
    entry["last_topic_verified_at"] = utc_now()
    entry.pop("last_topic_verify_attempt_at", None)
    entry.pop("last_topic_verify_error", None)
    entry.pop("last_topic_verify_error_at", None)
    entry.pop("topic_missing_at", None)
    entry.pop("topic_missing_id", None)
    entry.pop("topic_missing_reason", None)
    entry.pop("topic_rename_pending_at", None)
    entry.pop("topic_rename_from", None)
    entry.pop("topic_rename_to", None)
    return {"ok": True}


def rich_telegram_state(telegram: dict[str, Any] | None) -> dict[str, Any]:
    if not isinstance(telegram, dict):
        return {}
    rich = telegram.setdefault("rich_messages", {})
    if not isinstance(rich, dict):
        rich = {}
        telegram["rich_messages"] = rich
    rich.setdefault("supported", "unknown")
    return rich


def rich_enabled(telegram: dict[str, Any] | None) -> bool:
    if not RICH_MESSAGES_ENABLED:
        return False
    rich = rich_telegram_state(telegram)
    return str(rich.get("supported") or "unknown") != "no"


def mark_rich_supported(telegram: dict[str, Any] | None) -> None:
    rich = rich_telegram_state(telegram)
    if rich:
        rich["supported"] = "yes"
        rich.pop("disabled_reason", None)
        rich["last_ok_at"] = utc_now()


def mark_rich_disabled(telegram: dict[str, Any] | None, reason: str) -> None:
    rich = rich_telegram_state(telegram)
    if rich:
        rich["supported"] = "no"
        rich["disabled_reason"] = sanitize_text(reason, 300)
        rich["disabled_at"] = utc_now()


def _thread_payload(thread_id: str | int | None) -> dict[str, str]:
    tid = str(thread_id or "")
    if tid and tid != DEFAULT_GENERAL_THREAD_ID:
        return {"message_thread_id": tid}
    return {}


def _reply_markup_payload(reply_markup: dict[str, Any] | None) -> dict[str, str]:
    if not reply_markup:
        return {}
    return {"reply_markup": json.dumps(reply_markup, separators=(",", ":"))}


def send_message(
    chat_id: str,
    text: str,
    *,
    thread_id: str | int | None = None,
    notify: bool = False,
    reply_markup: dict[str, Any] | None = None,
    reply_to_message_id: str | int | None = None,
) -> str | None:
    payload: dict[str, Any] = {
        "chat_id": chat_id,
        "text": sanitize_text(text, MAX_REPLY_CHARS),
    }
    if not notify:
        payload["disable_notification"] = "true"
    payload.update(_thread_payload(thread_id))
    payload.update(_reply_markup_payload(reply_markup))
    if reply_to_message_id:
        payload["reply_to_message_id"] = str(reply_to_message_id)
    return telegram_message_id(telegram_api("sendMessage", payload))


def send_legacy_message_result(
    chat_id: str,
    text: str,
    *,
    thread_id: str | int | None = None,
    notify: bool = False,
    reply_markup: dict[str, Any] | None = None,
    reply_to_message_id: str | int | None = None,
) -> dict[str, Any]:
    try:
        mid = send_message(
            chat_id,
            text,
            thread_id=thread_id,
            notify=notify,
            reply_markup=reply_markup,
            reply_to_message_id=reply_to_message_id,
        )
    except RateLimited:
        raise
    except BridgeError as exc:
        kind = classify_telegram_error(exc)
        if kind == "topic_not_found":
            return {"ok": False, "format": "legacy", "kind": kind, "topic_missing": True, "error": str(exc)}
        return {"ok": False, "format": "legacy", "kind": kind, "transient": kind == "transient", "error": str(exc)}
    return {"ok": True, "format": "legacy", "message_id": mid}


def edit_message_text(
    chat_id: str,
    message_id: str | int,
    text: str,
    *,
    reply_markup: dict[str, Any] | None = None,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "chat_id": chat_id,
        "message_id": str(message_id),
        "text": sanitize_text(text, MAX_REPLY_CHARS),
    }
    payload.update(_reply_markup_payload(reply_markup))
    try:
        response = telegram_api("editMessageText", payload)
    except RateLimited:
        raise
    except BridgeError as exc:
        kind = classify_telegram_error(exc)
        result = {"ok": kind == "not_modified", "kind": kind, "error": str(exc)}
        if kind == "not_found":
            result["not_found"] = True
        if kind == "topic_not_found":
            result["topic_missing"] = True
        return result
    return {"ok": True, "kind": "edited", "message_id": telegram_message_id(response)}


def send_rich_message(
    chat_id: str,
    html_text: str,
    *,
    telegram: dict[str, Any] | None = None,
    fallback_text: str = "",
    thread_id: str | int | None = None,
    notify: bool = False,
    reply_markup: dict[str, Any] | None = None,
    reply_to_message_id: str | int | None = None,
) -> dict[str, Any]:
    fallback = fallback_text or sanitize_text(html.unescape(re.sub(r"<[^>]+>", "", html_text)), MAX_REPLY_CHARS)
    if not rich_enabled(telegram):
        return send_legacy_message_result(
            chat_id,
            fallback,
            thread_id=thread_id,
            notify=notify,
            reply_markup=reply_markup,
            reply_to_message_id=reply_to_message_id,
        )

    payload: dict[str, Any] = {
        "chat_id": chat_id,
        "rich_message": json.dumps(
            {"html": sanitize_text(html_text, MAX_RICH_HTML_CHARS), "skip_entity_detection": True},
            separators=(",", ":"),
        ),
    }
    if not notify:
        payload["disable_notification"] = "true"
    payload.update(_thread_payload(thread_id))
    payload.update(_reply_markup_payload(reply_markup))
    if reply_to_message_id:
        try:
            payload["reply_parameters"] = json.dumps({"message_id": int(reply_to_message_id)}, separators=(",", ":"))
        except (TypeError, ValueError):
            pass

    try:
        response = telegram_api("sendRichMessage", payload)
    except RateLimited:
        raise
    except BridgeError as exc:
        kind = classify_telegram_error(exc)
        if kind == "capability":
            mark_rich_disabled(telegram, str(exc))
            result = send_legacy_message_result(
                chat_id,
                fallback,
                thread_id=thread_id,
                notify=notify,
                reply_markup=reply_markup,
                reply_to_message_id=reply_to_message_id,
            )
            result["fallback_reason"] = kind
            return result
        if kind == "bad_request":
            result = send_legacy_message_result(
                chat_id,
                fallback,
                thread_id=thread_id,
                notify=notify,
                reply_markup=reply_markup,
                reply_to_message_id=reply_to_message_id,
            )
            result["fallback_reason"] = kind
            return result
        if kind == "topic_not_found":
            return {"ok": False, "format": "rich", "kind": kind, "topic_missing": True, "error": str(exc)}
        return {"ok": False, "format": "rich", "kind": kind, "transient": True, "error": str(exc)}
    mark_rich_supported(telegram)
    return {"ok": True, "format": "rich", "message_id": telegram_message_id(response)}


def edit_rich_message(
    chat_id: str,
    message_id: str | int,
    html_text: str,
    *,
    telegram: dict[str, Any] | None = None,
    fallback_text: str = "",
    reply_markup: dict[str, Any] | None = None,
) -> dict[str, Any]:
    fallback = fallback_text or sanitize_text(html.unescape(re.sub(r"<[^>]+>", "", html_text)), MAX_REPLY_CHARS)
    if not rich_enabled(telegram):
        return edit_message_text(chat_id, message_id, fallback, reply_markup=reply_markup)

    payload: dict[str, Any] = {
        "chat_id": chat_id,
        "message_id": str(message_id),
        "rich_message": json.dumps(
            {"html": sanitize_text(html_text, MAX_RICH_HTML_CHARS), "skip_entity_detection": True},
            separators=(",", ":"),
        ),
    }
    payload.update(_reply_markup_payload(reply_markup))
    try:
        response = telegram_api("editMessageText", payload)
    except RateLimited:
        raise
    except BridgeError as exc:
        kind = classify_telegram_error(exc)
        if kind == "not_modified":
            return {"ok": True, "kind": kind}
        if kind == "not_found":
            return {"ok": False, "kind": kind, "not_found": True, "error": str(exc)}
        if kind == "topic_not_found":
            return {"ok": False, "kind": kind, "topic_missing": True, "error": str(exc)}
        if kind == "capability":
            mark_rich_disabled(telegram, str(exc))
            legacy = edit_message_text(chat_id, message_id, fallback, reply_markup=reply_markup)
            legacy["fallback_reason"] = kind
            return legacy
        if kind == "bad_request":
            legacy = edit_message_text(chat_id, message_id, fallback, reply_markup=reply_markup)
            legacy["fallback_reason"] = kind
            return legacy
        return {"ok": False, "kind": kind, "transient": True, "error": str(exc)}
    mark_rich_supported(telegram)
    return {"ok": True, "kind": "edited", "message_id": telegram_message_id(response)}


def send_feed_item(
    chat_id: str,
    item: dict[str, Any],
    *,
    telegram: dict[str, Any] | None,
    thread_id: str | int | None,
    notify: bool = False,
    reply_markup: dict[str, Any] | None = None,
    reply_to_message_id: str | int | None = None,
    live: bool = False,
) -> dict[str, Any]:
    return send_rich_message(
        chat_id,
        render_feed_item_html(item, live=live),
        telegram=telegram,
        fallback_text=item_plain_text(item),
        thread_id=thread_id,
        notify=notify,
        reply_markup=reply_markup,
        reply_to_message_id=reply_to_message_id,
    )


def edit_feed_item(
    chat_id: str,
    message_id: str | int,
    item: dict[str, Any],
    *,
    telegram: dict[str, Any] | None,
    reply_markup: dict[str, Any] | None = None,
    live: bool = False,
) -> dict[str, Any]:
    return edit_rich_message(
        chat_id,
        message_id,
        render_feed_item_html(item, live=live),
        telegram=telegram,
        fallback_text=item_plain_text(item),
        reply_markup=reply_markup,
    )


def send_notice(
    chat_id: str,
    title: str,
    body: str,
    *,
    telegram: dict[str, Any] | None,
    thread_id: str | int | None,
    notify: bool = False,
    reply_markup: dict[str, Any] | None = None,
    reply_to_message_id: str | int | None = None,
) -> dict[str, Any]:
    plain = sanitize_text(f"{title}\n{body}".strip(), MAX_REPLY_CHARS)
    return send_rich_message(
        chat_id,
        render_notice_html(title, body),
        telegram=telegram,
        fallback_text=plain,
        thread_id=thread_id,
        notify=notify,
        reply_markup=reply_markup,
        reply_to_message_id=reply_to_message_id,
    )


def update_live_card(
    chat_id: str,
    entry: dict[str, Any],
    item: dict[str, Any],
    *,
    telegram: dict[str, Any],
) -> dict[str, Any]:
    html_text = render_feed_item_html(item, live=True)
    plain = item_plain_text(item)
    card_hash = hashlib.sha256(
        json.dumps(
            {
                "html": html_text,
                "plain": plain,
                "reply_markup": None,
            },
            sort_keys=True,
        ).encode("utf-8")
    ).hexdigest()
    if card_hash == entry.get("card_hash") and entry.get("card_message_id"):
        return {"ok": True, "kind": "unchanged", "attempted": False}

    message_id = str(entry.get("card_message_id") or "")
    if message_id:
        result = edit_rich_message(
            chat_id,
            message_id,
            html_text,
            telegram=telegram,
            fallback_text=plain,
        )
        if result.get("ok"):
            entry["card_hash"] = card_hash
            entry["card_format"] = str(result.get("format") or ("legacy" if not rich_enabled(telegram) else "rich"))
            return {**result, "attempted": True}
        if not result.get("not_found"):
            return {**result, "attempted": True}

    result = send_rich_message(
        chat_id,
        html_text,
        telegram=telegram,
        fallback_text=plain,
        thread_id=entry.get("topic_id"),
        notify=False,
    )
    if result.get("ok"):
        if result.get("message_id"):
            entry["card_message_id"] = str(result["message_id"])
        entry["card_hash"] = card_hash
        entry["card_format"] = str(result.get("format") or "rich")
    return {**result, "attempted": True}


def update_status_marker(
    chat_id: str,
    entry: dict[str, Any],
    pane: dict[str, Any],
    *,
    telegram: dict[str, Any],
) -> dict[str, Any]:
    marker_hash = status_marker_hash(pane)
    if marker_hash == entry.get("status_marker_hash") and entry.get("status_marker_message_id"):
        return {"ok": True, "kind": "unchanged", "attempted": False}
    title, body = status_marker_content(pane)
    old_message_id = str(entry.get("status_marker_message_id") or "")
    result = send_notice(
        chat_id,
        title,
        body,
        telegram=telegram,
        thread_id=entry.get("topic_id"),
        notify=False,
    )
    if result.get("ok"):
        new_message_id = str(result.get("message_id") or "")
        if old_message_id and new_message_id and old_message_id != new_message_id and STATUS_MARKER_DELETE_OLD:
            try:
                delete_message(chat_id, old_message_id)
            except Exception:
                entry["last_status_marker_delete_error"] = utc_now()
        if new_message_id:
            entry["status_marker_message_id"] = new_message_id
        entry["status_marker_hash"] = marker_hash
        entry["status_marker_text"] = sanitize_text(f"{title}\n{body}", 500)
        entry["status_marker_sent_at"] = utc_now()
    return {**result, "attempted": True}


def clear_status_marker_for_icon(chat_id: str, entry: dict[str, Any]) -> bool:
    old_message_id = str(entry.get("status_marker_message_id") or "")
    if not old_message_id:
        return False
    if STATUS_MARKER_DELETE_OLD:
        try:
            delete_message(chat_id, old_message_id)
        except Exception:
            entry["last_status_marker_delete_error"] = utc_now()
    for key in (
        "status_marker_message_id",
        "status_marker_hash",
        "status_marker_text",
        "status_marker_sent_at",
    ):
        entry.pop(key, None)
    entry["status_marker_cleared_for_icon_at"] = utc_now()
    return True


def create_topic(chat_id: str, name: str, *, icon_custom_emoji_id: str = "") -> str:
    payload: dict[str, Any] = {"chat_id": chat_id, "name": name}
    if icon_custom_emoji_id:
        payload["icon_custom_emoji_id"] = icon_custom_emoji_id
    elif HERDR_TOPIC_ICON_CUSTOM_EMOJI_ID:
        payload["icon_custom_emoji_id"] = HERDR_TOPIC_ICON_CUSTOM_EMOJI_ID
    else:
        payload["icon_color"] = str(HERDR_TOPIC_ICON_COLOR)
    result = telegram_api("createForumTopic", payload).get("result") or {}
    topic_id = result.get("message_thread_id")
    if topic_id is None:
        raise BridgeError("createForumTopic returned no message_thread_id")
    return str(topic_id)


def edit_topic(chat_id: str, topic_id: str | int, name: str, *, icon_custom_emoji_id: str | None = None) -> bool:
    payload: dict[str, Any] = {
        "chat_id": chat_id,
        "message_thread_id": str(topic_id),
        "name": name,
    }
    if icon_custom_emoji_id:
        payload["icon_custom_emoji_id"] = icon_custom_emoji_id
    return bool(telegram_api("editForumTopic", payload).get("result"))


def delete_topic(chat_id: str, topic_id: str | int) -> bool:
    payload = {"chat_id": chat_id, "message_thread_id": str(topic_id)}
    return bool(telegram_api("deleteForumTopic", payload).get("result"))


def delete_message(chat_id: str, message_id: str | int) -> bool:
    payload = {"chat_id": chat_id, "message_id": str(message_id)}
    return bool(telegram_api("deleteMessage", payload).get("result"))


def preflight(chat_id: str) -> None:
    if not chat_id:
        raise BridgeError("HERDR_TELEGRAM_TOPICS_CHAT_ID is required")
    chat = telegram_api("getChat", {"chat_id": chat_id}).get("result") or {}
    if chat.get("type") != "supergroup" or not chat.get("is_forum"):
        raise BridgeError("Telegram chat must be a forum-enabled supergroup")
    me = telegram_api("getMe", {}).get("result") or {}
    bot_id = me.get("id")
    if not bot_id:
        raise BridgeError("getMe returned no bot id")
    member = telegram_api("getChatMember", {"chat_id": chat_id, "user_id": str(bot_id)}).get("result") or {}
    if member.get("status") not in {"administrator", "creator"}:
        raise BridgeError("bot is not an administrator in the Telegram forum group")
    if member.get("status") != "creator" and not member.get("can_manage_topics", False):
        raise BridgeError("bot lacks can_manage_topics in the Telegram forum group")


def preflight_is_fresh(telegram: dict[str, Any]) -> bool:
    try:
        checked = _dt.datetime.fromisoformat(
            str(telegram.get("last_preflight_ok_at", "")).replace("Z", "+00:00")
        )
    except Exception:
        return False
    return (_dt.datetime.now(tz=_dt.timezone.utc) - checked).total_seconds() < PREFLIGHT_TTL_SECONDS


def preflight_ok_within(telegram: dict[str, Any], seconds: int = PREFLIGHT_GRACE_SECONDS) -> bool:
    try:
        checked = _dt.datetime.fromisoformat(
            str(telegram.get("last_preflight_ok_at", "")).replace("Z", "+00:00")
        )
    except Exception:
        return False
    return (_dt.datetime.now(tz=_dt.timezone.utc) - checked).total_seconds() < seconds


def is_transient_telegram_error(error_text: str) -> bool:
    low = str(error_text or "").lower()
    markers = (
        "urlopen error",
        "timed out",
        "timeout",
        "temporary failure",
        "connection reset",
        "connection aborted",
        "connection refused",
        "network is unreachable",
        "name or service not known",
        "unexpected_eof_while_reading",
        "eof occurred in violation of protocol",
        "ssl:",
    )
    return any(marker in low for marker in markers)


def preflight_alert_text(error_text: str) -> str:
    base = "Herdr topic sync preflight could not verify Telegram access."
    if is_transient_telegram_error(error_text):
        return (
            f"{base}\n"
            f"Reason: {error_text}\n\n"
            "This looks like a transient Telegram network/TLS failure, not a bot permission problem. "
            "Sync will continue if a recent permission check succeeded."
        )
    return (
        "Herdr topic sync is blocked before topic creation.\n"
        f"Reason: {error_text}\n\n"
        "Grant the bot admin permission to manage topics in the Telegram forum group, then run the sync again."
    )


def configure_telegram_state(state: dict[str, Any]) -> tuple[dict[str, Any], str]:
    telegram = state.setdefault("telegram", {})
    chat_id = str(telegram.get("chat_id") or os.getenv("HERDR_TELEGRAM_TOPICS_CHAT_ID") or DEFAULT_CHAT_ID)
    telegram["chat_id"] = chat_id
    telegram.setdefault("general_thread_id", os.getenv("HERDR_TELEGRAM_TOPICS_GENERAL_THREAD_ID", DEFAULT_GENERAL_THREAD_ID))
    telegram.setdefault(
        "owner_user_ids",
        [p.strip() for p in os.getenv("TELEGRAM_ALLOWED_USERS", DEFAULT_OWNER_ID).split(",") if p.strip()],
    )
    telegram.setdefault("implicit_send_enabled", False)
    return telegram, chat_id


def preflight_for_event(state: dict[str, Any], chat_id: str, telegram: dict[str, Any]) -> tuple[bool, str]:
    try:
        if not preflight_is_fresh(telegram):
            preflight(chat_id)
            telegram["last_preflight_ok_at"] = utc_now()
        telegram.pop("last_preflight_error", None)
        telegram.pop("last_event_preflight_error", None)
        return True, ""
    except Exception as exc:
        error_text = sanitize_text(str(exc), 500)
        if is_transient_telegram_error(error_text) and preflight_ok_within(telegram):
            telegram["last_preflight_warning"] = error_text
            telegram["last_preflight_warning_at"] = utc_now()
            return True, error_text
        telegram["last_event_preflight_error"] = error_text
        telegram["last_event_preflight_error_at"] = utc_now()
        return False, error_text


def duplicate_match_score(left: dict[str, Any], right: dict[str, Any]) -> int:
    score = 0
    left_session = str(left.get("agent_session_id") or "")
    right_session = str(right.get("agent_session_id") or "")
    if left_session and left_session == right_session:
        score += 100
    left_alias = entry_pane_alias(left)
    right_alias = entry_pane_alias(right)
    if left_alias and left_alias == right_alias:
        score += 70
    if str(left.get("workspace") or "") and str(left.get("workspace") or "") == str(right.get("workspace") or ""):
        score += 10
    left_name = str(left.get("pane_label_topic_name") or left.get("topic_name") or "").lower()
    right_name = str(right.get("pane_label_topic_name") or right.get("topic_name") or "").lower()
    if left_name and left_name == right_name:
        score += 20
    return score


def find_reusable_closed_entry(panes: dict[str, Any], current_key: str, pane: dict[str, Any]) -> tuple[str, dict[str, Any]] | None:
    candidate = {
        "pane_id": str(pane.get("pane_id") or ""),
        "terminal_id": str(pane.get("terminal_id") or ""),
        "agent_session_id": pane_agent_session_id(pane),
        "workspace": str(pane.get("workspace_id") or ""),
        "tab": str(pane.get("tab_id") or ""),
        "pane_label_topic_name": topic_name_from_pane_label(pane_manual_label(pane)) if pane_manual_label(pane) else "",
    }
    matches: list[tuple[int, str, dict[str, Any]]] = []
    for key, entry in panes.items():
        if key == current_key or not isinstance(entry, dict):
            continue
        if str(entry.get("last_known_status") or "").lower() != "closed":
            continue
        if not entry.get("topic_id"):
            continue
        score = duplicate_match_score(entry, candidate)
        if score >= 90:
            matches.append((score, key, entry))
    if not matches:
        return None
    matches.sort(key=lambda item: (item[0], str(item[2].get("last_seen_at") or "")), reverse=True)
    _score, key, entry = matches[0]
    return key, entry


def ensure_pane_entry(state: dict[str, Any], pane: dict[str, Any]) -> tuple[str, dict[str, Any], bool]:
    key = pane_key(pane)
    panes = state.setdefault("panes", {})
    entry = panes.get(key)
    created = False
    if not isinstance(entry, dict):
        reusable = find_reusable_closed_entry(panes, key, pane)
        if reusable:
            old_key, entry = reusable
            panes.pop(old_key, None)
            entry["pane_key"] = key
            entry["reused_from_pane_key"] = old_key
            entry["reused_topic_mapping_at"] = utc_now()
            entry.pop("closed_at", None)
            panes[key] = entry
            created = True
        else:
            entry = {"pane_key": key, "created_at": utc_now()}
            panes[key] = entry
            created = True
        created = True
    entry.update({
        "pane_id": str(pane.get("pane_id") or ""),
        "terminal_id": str(pane.get("terminal_id") or ""),
        "agent_session_id": pane_agent_session_id(pane),
        "workspace": str(pane.get("workspace_id") or ""),
        "tab": str(pane.get("tab_id") or ""),
    })
    manual_label = pane_manual_label(pane)
    previous_label = str(entry.get("pane_label_raw") or "")
    previous_label_topic_name = str(entry.get("pane_label_topic_name") or "")
    if manual_label:
        label_topic_name = topic_name_from_pane_label(manual_label)
        entry["pane_label_raw"] = manual_label
        entry["pane_label_topic_name"] = label_topic_name
        old_topic_name = str(entry.get("topic_name") or "")
        if created or not entry.get("topic_name"):
            entry["topic_name"] = label_topic_name
            entry["topic_title_source"] = "pane-label"
        elif label_topic_name and old_topic_name != label_topic_name:
            should_rename = (
                previous_label != manual_label
                or previous_label_topic_name != label_topic_name
                or str(entry.get("topic_title_source") or "") != "pane-label"
            )
            if should_rename:
                entry["topic_name"] = label_topic_name
                entry["topic_title_source"] = "pane-label"
                entry["topic_rename_pending_at"] = utc_now()
                entry["topic_rename_from"] = old_topic_name
                entry["topic_rename_to"] = label_topic_name
        elif not previous_label:
            entry.setdefault("pane_label_baselined_at", utc_now())
    else:
        entry.pop("pane_label_topic_name", None)
        if previous_label:
            entry["pane_label_raw"] = ""
            entry["pane_label_cleared_at"] = utc_now()
    if not entry.get("topic_name"):
        entry["topic_name"] = topic_name_for_pane(pane)
    return key, entry, created


def should_send_status(entry: dict[str, Any], obj_hash: str, pane: dict[str, Any], new_entry: bool) -> bool:
    status = str(pane.get("agent_status") or "").lower()
    previous_status = str(entry.get("last_notified_status") or "").lower()
    if new_entry or not entry.get("last_status_hash"):
        return True
    if previous_status != status:
        return True
    if status in {"blocked", "error"}:
        last_sent = entry.get("last_sent_at") or ""
        try:
            then = _dt.datetime.fromisoformat(last_sent.replace("Z", "+00:00"))
            return (_dt.datetime.now(tz=_dt.timezone.utc) - then).total_seconds() > 1800
        except Exception:
            return True
    return False


def sync_pane_once(
    state: dict[str, Any],
    chat_id: str,
    telegram: dict[str, Any],
    pane: dict[str, Any],
    counters: dict[str, int],
    caps: dict[str, int],
    *,
    turn_only: bool = False,
) -> bool:
    key, entry, new_entry = ensure_pane_entry(state, pane)
    changed = bool(new_entry)
    entry["last_seen_at"] = utc_now()
    entry["last_known_status"] = str(pane.get("agent_status") or "unknown")
    max_creates = int(caps.get("max_creates", MAX_CREATES_PER_RUN))
    max_sends = int(caps.get("max_sends", MAX_SENDS_PER_RUN))
    max_feed_sends = int(caps.get("max_feed_sends", max_sends))
    max_marker_sends = int(caps.get("max_marker_sends", MAX_STATUS_MARKERS_PER_RUN))
    max_verifies = int(caps.get("max_verifies", MAX_TOPIC_VERIFIES_PER_RUN))
    feed_delivered_this_pane = False

    if not entry.get("topic_id") and counters.get("creates", 0) < max_creates:
        topic_name = str(entry.get("topic_name") or topic_name_for_pane(pane))
        topic_icon_id = ""
        topic_icon_key = ""
        topic_icon_emoji = ""
        if STATUS_ICON_ENABLED:
            topic_icon_id, topic_icon_key, topic_icon_emoji = status_icon_custom_emoji_id(telegram, pane)
        create_kwargs = {"icon_custom_emoji_id": topic_icon_id} if topic_icon_id else {}
        topic_id = create_topic(chat_id, topic_name, **create_kwargs)
        counters["creates"] = counters.get("creates", 0) + 1
        entry["topic_id"] = topic_id
        entry["topic_name"] = topic_name
        if topic_icon_id:
            entry["topic_status_icon_key"] = topic_icon_key
            entry["topic_status_icon_emoji"] = topic_icon_emoji
            entry["topic_status_icon_custom_emoji_id"] = topic_icon_id
            entry["topic_status_icon_updated_at"] = utc_now()
        entry["last_topic_verified_at"] = utc_now()
        entry.pop("topic_missing_at", None)
        entry.pop("topic_missing_id", None)
        entry.pop("topic_missing_reason", None)
        entry.pop("topic_rename_pending_at", None)
        entry.pop("topic_rename_from", None)
        entry.pop("topic_rename_to", None)
        save_state(state)
        changed = True
        if not CLEAN_FEED_ENABLED and counters.get("sends", 0) < max_sends:
            send_message(
                chat_id,
                f"Linked Telegram topic to Herdr pane.\nPane key: {key}\n\n{format_status(pane, include_commands=True)}",
                thread_id=topic_id,
            )
            counters["sends"] = counters.get("sends", 0) + 1
    if not entry.get("topic_id"):
        return changed

    rename_pending = bool(entry.get("topic_rename_pending_at"))
    if rename_pending or (counters.get("verifies", 0) < max_verifies and topic_verify_due(entry)):
        verify_result = verify_topic_mapping(chat_id, entry)
        if rename_pending:
            counters["renames"] = counters.get("renames", 0) + 1
        else:
            counters["verifies"] = counters.get("verifies", 0) + 1
        if verify_result.get("ok"):
            changed = True
        elif result_topic_missing(verify_result):
            clear_topic_mapping(entry, str(verify_result.get("error") or verify_result))
            save_state(state)
            return True
        else:
            verify_error = sanitize_text(str(verify_result), 500)
            if entry.get("last_topic_verify_error") != verify_error:
                entry["last_topic_verify_error"] = verify_error
                entry["last_topic_verify_error_at"] = utc_now()
                changed = True

    stable_obj_hash = status_hash(stable_status_object(pane))
    live_item = live_status_item(pane)
    live_card_hash = clean_feed_hash(live_item)
    if LIVE_CARD_ENABLED and not STATUS_MARKER_ENABLED and counters.get("sends", 0) < max_sends and (
        not entry.get("card_message_id") or entry.get("card_status_hash") != live_card_hash
    ):
        card_result = update_live_card(chat_id, entry, live_item, telegram=telegram)
        if card_result.get("attempted"):
            counters["sends"] = counters.get("sends", 0) + 1
        if result_topic_missing(card_result):
            clear_topic_mapping(entry, str(card_result.get("error") or card_result))
            save_state(state)
            return True
        if card_result.get("ok"):
            entry["card_status_hash"] = live_card_hash
            changed = True

    if CLEAN_FEED_ENABLED:
        item = None
        if TURN_FEED_ENABLED:
            before_turn_state = (
                entry.get("last_turn_available"),
                entry.get("last_turn_reason"),
                entry.get("last_turn_id"),
            )
            item = extract_turn_feed_item(pane, entry)
            after_turn_state = (
                entry.get("last_turn_available"),
                entry.get("last_turn_reason"),
                entry.get("last_turn_id"),
            )
            if before_turn_state != after_turn_state:
                changed = True
        elif not turn_only:
            raw = pane_feed_output(str(pane.get("pane_id") or ""))
            bounded_report = extract_bounded_report_from_raw(raw)
            if bounded_report:
                if entry.pop("suppress_auto_feed_until_bounded_report", None) is not None:
                    changed = True
                item = extract_clean_feed_item(
                    pane,
                    entry,
                    raw,
                    allow_unbounded_reports=ALLOW_UNBOUNDED_REPORTS,
                )
            elif has_resume_control_noise(raw):
                if entry.get("last_clean_hash") or not entry.get("suppress_auto_feed_until_bounded_report"):
                    clear_clean_feed_state(entry)
                    changed = True
                if not entry.get("suppress_auto_feed_until_bounded_report"):
                    entry["suppress_auto_feed_until_bounded_report"] = True
                    changed = True
            else:
                if entry.pop("suppress_auto_feed_until_bounded_report", None) is not None:
                    changed = True
                item = extract_clean_feed_item(
                    pane,
                    entry,
                    raw,
                    allow_unbounded_reports=ALLOW_UNBOUNDED_REPORTS,
                )

        old_clean_has_noise = feed_text_has_ui_noise(str(entry.get("last_clean_text") or ""))
        if item:
            item_render_hash = clean_feed_hash(item)
            item_semantic_hash = clean_feed_hash(item, include_render_version=False)
            previous_semantic_hash = str(entry.get("last_clean_semantic_hash") or "")
            previous_render_hash = str(entry.get("last_clean_render_hash") or entry.get("last_clean_hash") or "")
            same_semantic = bool(previous_semantic_hash and previous_semantic_hash == item_semantic_hash)
            render_changed = item_render_hash != previous_render_hash
            content_changed = not same_semantic
            should_deliver = old_clean_has_noise or content_changed or render_changed
            if counters.get("feed_sends", 0) < max_feed_sends and should_deliver and not recent_attempt(entry, item_render_hash):
                reply_markup, pending_active_prompt, clear_active_prompt = prompt_delivery_state(item)
                entry["last_clean_attempt_hash"] = item_render_hash
                entry["last_clean_attempt_at"] = utc_now()
                changed = True
                did_edit = False
                message_id = str(entry.get("last_clean_message_id") or "")
                if same_semantic and (render_changed or old_clean_has_noise) and message_id:
                    result = edit_feed_item(
                        chat_id,
                        message_id,
                        item,
                        telegram=telegram,
                        reply_markup=reply_markup,
                    )
                    if result.get("ok"):
                        did_edit = True
                    elif result.get("not_found"):
                        result = send_feed_item(
                            chat_id,
                            item,
                            telegram=telegram,
                            thread_id=entry["topic_id"],
                            notify=bool(item.get("notify")),
                            reply_markup=reply_markup,
                        )
                    else:
                        result = {**result, "edit_failed": True}
                else:
                    result = send_feed_item(
                        chat_id,
                        item,
                        telegram=telegram,
                        thread_id=entry["topic_id"],
                        notify=bool(item.get("notify")),
                        reply_markup=reply_markup,
                    )
                if result.get("ok"):
                    counters["sends"] = counters.get("sends", 0) + 1
                    counters["feed_sends"] = counters.get("feed_sends", 0) + 1
                    feed_delivered_this_pane = True
                    if pending_active_prompt:
                        entry["active_prompt"] = pending_active_prompt
                    elif clear_active_prompt:
                        entry.pop("active_prompt", None)
                    entry["last_clean_hash"] = item_render_hash
                    entry["last_clean_semantic_hash"] = item_semantic_hash
                    entry["last_clean_render_hash"] = item_render_hash
                    if result.get("message_id"):
                        entry["last_clean_message_id"] = str(result["message_id"])
                    elif did_edit and message_id:
                        entry["last_clean_message_id"] = message_id
                    entry["last_clean_kind"] = str(item.get("kind") or "")
                    entry["last_clean_text"] = item_plain_text(item)
                    entry["last_clean_item"] = item
                    entry["last_clean_sent_at"] = utc_now()
                    entry.pop("last_clean_send_error", None)
                    changed = True
                elif result_topic_missing(result):
                    clear_topic_mapping(entry, str(result.get("error") or result))
                    save_state(state)
                    return True
                else:
                    entry["last_clean_send_error"] = sanitize_text(str(result), 500)
                    changed = True
        elif old_clean_has_noise:
            clear_clean_feed_state(entry)
            changed = True
        entry["last_status_hash"] = stable_obj_hash
    elif counters.get("sends", 0) < max_sends and should_send_status(entry, stable_obj_hash, pane, new_entry):
        pane_status = str(pane.get("agent_status") or "").lower()
        include_recent = pane_status in {"blocked", "unknown"}
        status_result = send_legacy_message_result(
            chat_id,
            format_status(pane, include_recent=include_recent),
            thread_id=entry["topic_id"],
            notify=pane_status in {"blocked", "error"},
        )
        if status_result.get("ok"):
            counters["sends"] = counters.get("sends", 0) + 1
            entry["last_status_hash"] = stable_obj_hash
            entry["last_notified_status"] = pane_status
            entry["last_sent_at"] = utc_now()
            changed = True
        elif result_topic_missing(status_result):
            clear_topic_mapping(entry, str(status_result.get("error") or status_result))
            save_state(state)
            return True
    status_icon_ok = False
    if STATUS_ICON_ENABLED and entry.get("topic_id"):
        icon_result = update_topic_status_icon(chat_id, entry, pane, telegram=telegram)
        if icon_result.get("attempted"):
            counters["icon_updates"] = counters.get("icon_updates", 0) + 1
        if result_topic_missing(icon_result):
            clear_topic_mapping(entry, str(icon_result.get("error") or icon_result))
            save_state(state)
            return True
        if icon_result.get("ok"):
            status_icon_ok = True
            if icon_result.get("attempted"):
                changed = True
            if STATUS_MARKER_SUPPRESS_WHEN_ICON_OK and clear_status_marker_for_icon(chat_id, entry):
                changed = True

    if (
        STATUS_MARKER_ENABLED
        and not feed_delivered_this_pane
        and not (STATUS_MARKER_SUPPRESS_WHEN_ICON_OK and status_icon_ok)
        and entry.get("topic_id")
        and counters.get("marker_sends", 0) < max_marker_sends
    ):
        marker_result = update_status_marker(chat_id, entry, pane, telegram=telegram)
        if marker_result.get("attempted"):
            counters["sends"] = counters.get("sends", 0) + 1
            counters["marker_sends"] = counters.get("marker_sends", 0) + 1
        if result_topic_missing(marker_result):
            clear_topic_mapping(entry, str(marker_result.get("error") or marker_result))
            save_state(state)
            return True
        if marker_result.get("ok") and marker_result.get("attempted"):
            changed = True
    return changed


def sync_once() -> dict[str, Any]:
    load_dotenv()
    state = load_state()
    if not state.get("enabled", True):
        return {"ok": True, "changed": False, "message": "disabled"}
    telegram, chat_id = configure_telegram_state(state)

    all_panes = pane_list()
    include_shells = os.getenv("HERDR_TELEGRAM_TOPICS_INCLUDE_SHELLS", "").lower() in {"1", "true", "yes", "on"}
    panes = [pane for pane in all_panes if include_shells or pane.get("agent")]
    live_keys = {pane_key(pane) for pane in panes}
    sends = 0
    changed = False

    for key, entry in list(state.get("panes", {}).items()):
        if key in live_keys or entry.get("last_known_status") == "closed":
            continue
        entry["last_known_status"] = "closed"
        entry["closed_at"] = utc_now()
        changed = True
        if entry.get("topic_id") and sends < MAX_SENDS_PER_RUN:
            send_notice(
                chat_id,
                "Closed",
                "This Herdr pane is no longer live.",
                telegram=telegram,
                thread_id=entry["topic_id"],
                notify=True,
            )
            sends += 1

    if not panes:
        state["last_sync_empty_at"] = utc_now()
        save_state(state)
        return {"ok": True, "changed": changed, "panes": 0, "sent": sends, "message": "no agent panes"}

    try:
        if not preflight_is_fresh(telegram):
            preflight(chat_id)
            telegram["last_preflight_ok_at"] = utc_now()
        telegram.pop("last_preflight_error", None)
    except BridgeError as exc:
        error_text = sanitize_text(str(exc), 500)
        if is_transient_telegram_error(error_text) and preflight_ok_within(telegram):
            telegram["last_preflight_warning"] = error_text
            telegram["last_preflight_warning_at"] = utc_now()
            save_state(state)
        else:
            should_alert = telegram.get("last_preflight_error") != error_text
            if not should_alert:
                try:
                    last_alert = _dt.datetime.fromisoformat(
                        str(telegram.get("last_preflight_alert_at", "")).replace("Z", "+00:00")
                    )
                    should_alert = (_dt.datetime.now(tz=_dt.timezone.utc) - last_alert).total_seconds() > 3600
                except Exception:
                    should_alert = True
            if should_alert and chat_id:
                try:
                    send_message(
                        chat_id,
                        preflight_alert_text(error_text),
                        thread_id=telegram.get("general_thread_id", DEFAULT_GENERAL_THREAD_ID),
                        notify=not is_transient_telegram_error(error_text),
                    )
                    telegram["last_preflight_alert_at"] = utc_now()
                except Exception:
                    pass
            telegram["last_preflight_error"] = error_text
            save_state(state)
            raise
    counters = {
        "sends": sends,
        "creates": 0,
        "verifies": 0,
        "renames": 0,
        "feed_sends": 0,
        "marker_sends": 0,
        "icon_updates": 0,
    }
    caps = {
        "max_sends": MAX_SENDS_PER_RUN,
        "max_feed_sends": MAX_SENDS_PER_RUN,
        "max_marker_sends": MAX_STATUS_MARKERS_PER_RUN,
        "max_creates": MAX_CREATES_PER_RUN,
        "max_verifies": MAX_TOPIC_VERIFIES_PER_RUN,
    }
    for pane in panes:
        if sync_pane_once(state, chat_id, telegram, pane, counters, caps):
            changed = True
    sends = counters["sends"]
    creates = counters["creates"]
    verifies = counters["verifies"]
    renames = counters["renames"]

    save_state(state)
    return {
        "ok": True,
        "changed": changed,
        "panes": len(panes),
        "created": creates,
        "verified": verifies,
        "renamed": renames,
        "sent": sends,
        "feed_sent": counters["feed_sends"],
        "marker_sent": counters["marker_sends"],
        "icon_updated": counters["icon_updates"],
    }


def duplicate_topic_records(state: dict[str, Any]) -> list[dict[str, Any]]:
    panes = state.get("panes") if isinstance(state.get("panes"), dict) else {}
    active = {
        key: entry
        for key, entry in panes.items()
        if isinstance(entry, dict)
        and str(entry.get("last_known_status") or "").lower() != "closed"
        and entry.get("topic_id")
    }
    records: list[dict[str, Any]] = []
    for closed_key, closed in panes.items():
        if not isinstance(closed, dict):
            continue
        if str(closed.get("last_known_status") or "").lower() != "closed":
            continue
        if not closed.get("topic_id"):
            continue
        best: tuple[int, str, dict[str, Any]] | None = None
        for active_key, active_entry in active.items():
            if str(closed.get("topic_id")) == str(active_entry.get("topic_id")):
                continue
            score = duplicate_match_score(closed, active_entry)
            if score >= 90 and (best is None or score > best[0]):
                best = (score, active_key, active_entry)
        if best:
            score, active_key, active_entry = best
            records.append({
                "closed_key": closed_key,
                "active_key": active_key,
                "score": score,
                "topic_id": str(closed.get("topic_id") or ""),
                "topic_name": str(closed.get("topic_name") or ""),
                "active_topic_id": str(active_entry.get("topic_id") or ""),
                "active_topic_name": str(active_entry.get("topic_name") or ""),
                "pane_id": str(closed.get("pane_id") or ""),
                "active_pane_id": str(active_entry.get("pane_id") or ""),
                "agent_session_id": str(closed.get("agent_session_id") or ""),
            })
    return records


def cleanup_duplicates_once(*, delete: bool = False) -> dict[str, Any]:
    load_dotenv()
    state = load_state()
    telegram, chat_id = configure_telegram_state(state)
    records = duplicate_topic_records(state)
    if not delete:
        return {"ok": True, "changed": False, "duplicates": records, "count": len(records)}
    try:
        preflight(chat_id)
        telegram["last_preflight_ok_at"] = utc_now()
    except Exception as exc:
        save_state(state)
        return {"ok": False, "changed": False, "error": sanitize_text(str(exc), 500), "duplicates": records}
    deleted: list[dict[str, Any]] = []
    failed: list[dict[str, Any]] = []
    panes = state.setdefault("panes", {})
    for record in records[:DUPLICATE_TOPIC_DELETE_LIMIT]:
        topic_id = record["topic_id"]
        try:
            ok = delete_topic(chat_id, topic_id)
        except Exception as exc:
            failed.append({**record, "error": sanitize_text(str(exc), 500)})
            continue
        if not ok:
            failed.append({**record, "error": "deleteForumTopic returned false"})
            continue
        archived = dict(panes.pop(record["closed_key"], {}) or {})
        archived["deleted_duplicate_topic_at"] = utc_now()
        archived["deleted_duplicate_topic_id"] = topic_id
        archived["active_duplicate_pane_key"] = record["active_key"]
        state.setdefault("deleted_duplicate_topics", []).append(archived)
        deleted.append(record)
    changed = bool(deleted)
    if changed or failed:
        state["last_duplicate_cleanup_at"] = utc_now()
        state["last_duplicate_cleanup_deleted"] = len(deleted)
        state["last_duplicate_cleanup_failed"] = len(failed)
        save_state(state)
    return {
        "ok": not failed,
        "changed": changed,
        "duplicates": records,
        "deleted": deleted,
        "failed": failed,
        "deleted_count": len(deleted),
        "failed_count": len(failed),
    }


def parse_plugin_json_env(name: str) -> dict[str, Any]:
    raw = os.getenv(name, "").strip()
    if not raw:
        return {}
    try:
        value = json.loads(raw)
    except json.JSONDecodeError:
        return {}
    return value if isinstance(value, dict) else {}


def _first_string_value(obj: Any, keys: set[str]) -> str:
    if isinstance(obj, dict):
        for key, value in obj.items():
            key_l = str(key).lower()
            if key_l in keys and value not in (None, ""):
                return str(value)
        for value in obj.values():
            found = _first_string_value(value, keys)
            if found:
                return found
    elif isinstance(obj, list):
        for value in obj:
            found = _first_string_value(value, keys)
            if found:
                return found
    return ""


def event_pane_id(context: dict[str, Any], event: dict[str, Any]) -> str:
    for root in (event, context):
        if not isinstance(root, dict):
            continue
        pane_container = root.get("pane")
        if isinstance(pane_container, str) and pane_container:
            return pane_container
        found = _first_string_value(pane_container, {"pane_id", "paneid", "id"})
        if found:
            return found
        for container_key in ("agent", "resource", "payload", "data"):
            container = root.get(container_key)
            found = _first_string_value(container, {"pane_id", "paneid"})
            if found:
                return found
        found = _first_string_value(root, {"pane_id", "paneid"})
        if found:
            return found
    return ""


def event_status(context: dict[str, Any], event: dict[str, Any]) -> str:
    for root in (event, context):
        found = _first_string_value(root, {"agent_status", "status", "state"})
        if found:
            return found.lower()
    return ""


def should_settle_event(pane: dict[str, Any], context: dict[str, Any], event: dict[str, Any]) -> bool:
    status = event_status(context, event) or str(pane.get("agent_status") or "").lower()
    return status in {"done", "idle", "blocked", "error"}


def plugin_enable_once(enabled: bool) -> dict[str, Any]:
    load_dotenv()
    state = load_state()
    state["plugin_event_enabled"] = bool(enabled)
    state["plugin_event_enabled_at"] = utc_now()
    save_state(state)
    return {"ok": True, "plugin_event_enabled": bool(enabled)}


def event_once() -> dict[str, Any]:
    load_dotenv()
    state = load_state()
    if not state.get("enabled", True):
        return {"ok": True, "changed": False, "message": "disabled"}
    if not state.get("plugin_event_enabled", True):
        return {"ok": True, "changed": False, "message": "plugin events disabled"}

    telegram, chat_id = configure_telegram_state(state)
    context = parse_plugin_json_env("HERDR_PLUGIN_CONTEXT_JSON")
    event = parse_plugin_json_env("HERDR_PLUGIN_EVENT_JSON")
    pane_id = event_pane_id(context, event)
    if not pane_id:
        state["last_plugin_event_missing_pane_at"] = utc_now()
        save_state(state)
        return {"ok": True, "changed": False, "message": "no pane id in plugin event"}

    pane = pane_by_id(pane_id)
    if not pane or not pane.get("agent"):
        state["last_plugin_event_unknown_pane_at"] = utc_now()
        state["last_plugin_event_unknown_pane_id"] = sanitize_text(pane_id, 120)
        save_state(state)
        return {"ok": True, "changed": False, "pane_id": pane_id, "message": "pane not found or not an agent"}

    preflight_ok, preflight_error = preflight_for_event(state, chat_id, telegram)
    if not preflight_ok:
        save_state(state)
        return {
            "ok": True,
            "changed": False,
            "pane_id": pane_id,
            "message": "telegram preflight failed",
            "error": preflight_error,
        }

    counters = {
        "sends": 0,
        "creates": 0,
        "verifies": 0,
        "renames": 0,
        "feed_sends": 0,
        "marker_sends": 0,
        "icon_updates": 0,
    }
    caps = {
        "max_sends": min(MAX_SENDS_PER_RUN, 2),
        "max_feed_sends": min(MAX_SENDS_PER_RUN, 2),
        "max_marker_sends": 1,
        "max_creates": min(MAX_CREATES_PER_RUN, 1),
        "max_verifies": min(MAX_TOPIC_VERIFIES_PER_RUN, 1),
    }
    attempts = 0
    changed = False
    settle = should_settle_event(pane, context, event)
    deadline = time.monotonic() + max(0.0, EVENT_SETTLE_SECONDS)
    try:
        while True:
            attempts += 1
            before_feed_sends = counters.get("feed_sends", 0)
            changed = sync_pane_once(state, chat_id, telegram, pane, counters, caps, turn_only=True) or changed
            if counters.get("feed_sends", 0) > before_feed_sends:
                break
            entry = (state.get("panes") or {}).get(pane_key(pane), {})
            if TURN_FEED_ENABLED and entry.get("last_turn_available") is False and not settle:
                break
            if not settle or time.monotonic() >= deadline or counters.get("sends", 0) >= caps["max_sends"]:
                break
            time.sleep(max(0.1, EVENT_SETTLE_INTERVAL_SECONDS))
            refreshed = pane_by_id(pane_id)
            if not refreshed or not refreshed.get("agent"):
                break
            pane = refreshed
    except Exception as exc:
        state["last_plugin_event_error"] = sanitize_text(str(exc), 500)
        state["last_plugin_event_error_at"] = utc_now()
        save_state(state)
        return {"ok": True, "changed": False, "pane_id": pane_id, "message": "event sync failed"}

    state["last_plugin_event_at"] = utc_now()
    state["last_plugin_event_pane_id"] = pane_id
    save_state(state)
    return {
        "ok": True,
        "changed": changed,
        "pane_id": pane_id,
        "sent": counters["sends"],
        "feed_sent": counters["feed_sends"],
        "marker_sent": counters["marker_sends"],
        "icon_updated": counters["icon_updates"],
        "attempts": attempts,
        "created": counters["creates"],
        "verified": counters["verifies"],
        "renamed": counters["renames"],
    }


def parse_command(text: str) -> tuple[str, str]:
    stripped = (text or "").strip()
    if not stripped:
        return "", ""
    if not stripped.startswith("/"):
        return "plain", stripped
    first, _, rest = stripped.partition(" ")
    command = first[1:].split("@", 1)[0].strip().lower().replace("_", "-")
    return command, rest.strip()


def topic_entry(state: dict[str, Any], chat_id: str, topic_id: str) -> dict[str, Any] | None:
    telegram = state.get("telegram") or {}
    if str(telegram.get("chat_id")) != str(chat_id):
        return None
    if str(topic_id) == str(telegram.get("general_thread_id", DEFAULT_GENERAL_THREAD_ID)):
        return None
    for entry in (state.get("panes") or {}).values():
        if str(entry.get("topic_id") or "") == str(topic_id):
            return entry
    return None


def command_reply(payload: dict[str, Any]) -> dict[str, Any]:
    load_dotenv()
    state = load_state()
    chat_id = str(payload.get("chat_id") or "")
    topic_id = str(payload.get("topic_id") or "")
    user_id = str(payload.get("user_id") or "")
    text = str(payload.get("text") or "")
    telegram = state.setdefault("telegram", {})

    entry = topic_entry(state, chat_id, topic_id)
    if not entry:
        return {"handled": False}

    owners = {str(x) for x in (state.get("telegram") or {}).get("owner_user_ids", [])}
    if not owners:
        owners = {p.strip() for p in os.getenv("TELEGRAM_ALLOWED_USERS", DEFAULT_OWNER_ID).split(",") if p.strip()}
    if payload.get("edited"):
        return {"handled": True, "reply": ""}
    if payload.get("from_bot"):
        return {"handled": True, "reply": ""}
    if user_id not in owners:
        return {"handled": True, "reply": ""}
    if payload.get("forwarded"):
        return {"handled": True, "reply": "Ignored non-direct owner message in pane topic."}

    pane_id = str(entry.get("pane_id") or "")
    if not pane_id or entry.get("last_known_status") == "closed":
        return {"handled": True, "reply": "This topic is mapped to a closed or unavailable Herdr pane."}

    command, arg = parse_command(text)
    if command == "plain":
        awaiting = entry.get("awaiting_detail") if isinstance(entry.get("awaiting_detail"), dict) else {}
        if awaiting and str(awaiting.get("user_id") or "") == user_id:
            try:
                created_at = _dt.datetime.fromisoformat(str(awaiting.get("created_at", "")).replace("Z", "+00:00"))
                expired = (_dt.datetime.now(tz=_dt.timezone.utc) - created_at).total_seconds() > DETAIL_REPLY_TIMEOUT_SECONDS
            except Exception:
                expired = True
            if expired:
                entry.pop("awaiting_detail", None)
                save_state(state)
                return {"handled": True, "reply": "That detail request expired. Use /choices to resend the choices."}
            force_reply_message_id = str(awaiting.get("force_reply_message_id") or "")
            reply_to_message_id = str(payload.get("reply_to_message_id") or "")
            if force_reply_message_id and reply_to_message_id != force_reply_message_id:
                return {"handled": True, "reply": "Reply directly to the detail prompt, or tap the button again."}
            choice = str(awaiting.get("choice") or "").strip()
            outbound = f"{choice}\n{arg}" if choice else arg
            ok, detail = send_to_pane(pane_id, outbound)
            if not ok:
                return {"handled": True, "reply": f"Send failed: {detail}"}
            entry.pop("awaiting_detail", None)
            entry.pop("active_prompt", None)
            save_state(state)
            return {"handled": True, "reply": "Sent details."}
        implicit = bool((state.get("telegram") or {}).get("implicit_send_enabled", False))
        if implicit:
            payload = dict(payload)
            payload["text"] = "/send " + arg
            return command_reply(payload)
        return {"handled": True, "reply": "This is a mapped Herdr pane topic. Use /send <text> to forward to this pane, or /help."}

    if command in {"help", "start"}:
        implicit = bool((state.get("telegram") or {}).get("implicit_send_enabled", False))
        plain_text_help = (
            "Plain text from you is forwarded directly to this pane."
            if implicit
            else "Plain text is not forwarded unless implicit send is enabled."
        )
        return {
            "handled": True,
            "reply": (
                "Pane topic commands:\n"
                "/report or /status - latest clean report/question\n"
                "/choices - resend active choices or decision buttons\n"
                "/raw [lines] - sanitized raw visible output\n"
                "/debug - technical mapping details\n"
                "/send <text> - send instruction to this pane\n"
                "/keys <keys> - send explicit keys\n"
                f"{plain_text_help}"
            ),
        }
    if command in {"status", "report"}:
        pane = pane_by_id(pane_id)
        if TURN_FEED_ENABLED:
            before_turn_state = (
                entry.get("last_turn_available"),
                entry.get("last_turn_reason"),
                entry.get("last_turn_id"),
            )
            item = latest_turn_item(entry, pane)
            after_turn_state = (
                entry.get("last_turn_available"),
                entry.get("last_turn_reason"),
                entry.get("last_turn_id"),
            )
            state_changed = before_turn_state != after_turn_state
            if item:
                reply_markup, pending_active_prompt, clear_active_prompt = prompt_delivery_state(item)
                result = send_feed_item(
                    chat_id,
                    item,
                    telegram=telegram,
                    thread_id=topic_id,
                    notify=False,
                    reply_markup=reply_markup,
                )
                if result.get("ok"):
                    if pending_active_prompt:
                        entry["active_prompt"] = pending_active_prompt
                    elif clear_active_prompt:
                        entry.pop("active_prompt", None)
                    entry["last_clean_hash"] = clean_feed_hash(item)
                    entry["last_clean_semantic_hash"] = clean_feed_hash(item, include_render_version=False)
                    entry["last_clean_render_hash"] = clean_feed_hash(item)
                    if result.get("message_id"):
                        entry["last_clean_message_id"] = str(result["message_id"])
                    entry["last_clean_kind"] = str(item.get("kind") or "turn")
                    entry["last_clean_text"] = item_plain_text(item)
                    entry["last_clean_item"] = item
                    entry["last_clean_sent_at"] = utc_now()
                    entry.pop("last_clean_send_error", None)
                    save_state(state)
                    return {"handled": True, "reply": ""}
                entry["last_clean_send_error"] = sanitize_text(str(result), 500)
                save_state(state)
                return {"handled": True, "reply": item_plain_text(item)}
            if state_changed:
                save_state(state)
            return {"handled": True, "reply": latest_turn_report(entry, None)}
        item = latest_clean_item(entry, pane)
        if item:
            reply_markup, pending_active_prompt, clear_active_prompt = prompt_delivery_state(item)
            result = send_feed_item(
                chat_id,
                item,
                telegram=telegram,
                thread_id=topic_id,
                notify=False,
                reply_markup=reply_markup,
            )
            if result.get("ok"):
                if pending_active_prompt:
                    entry["active_prompt"] = pending_active_prompt
                elif clear_active_prompt:
                    entry.pop("active_prompt", None)
            save_state(state)
            if not result.get("ok"):
                return {"handled": True, "reply": latest_clean_report(entry, pane)}
            return {"handled": True, "reply": ""}
        return {"handled": True, "reply": latest_clean_report(entry, pane)}
    if command == "choices":
        prompt = entry.get("active_prompt") if isinstance(entry.get("active_prompt"), dict) else {}
        options = list(prompt.get("options") or [])
        prompt_id = str(prompt.get("id") or "")
        prompt_text = str(prompt.get("text") or "")
        if not prompt_id or not options or not prompt_text:
            return {"handled": True, "reply": "No active choices for this pane."}
        prompt_item = dict(prompt.get("item") or {})
        if not prompt_item:
            question_lines = [ln for ln in feed_body_lines("Question", prompt_text) if not option_match(ln)]
            question = compact_block(question_lines, max_lines=3, max_chars=500) or "Choose a response."
            prompt_item = {
                "kind": "choices",
                "title": "Question",
                "summary": question,
                "detail": "",
                "text": prompt_text,
                "notify": True,
            }
            prompt_item["options"] = options
            prompt_item["prompt_id"] = prompt_id
        send_feed_item(
            chat_id,
            prompt_item,
            telegram=telegram,
            thread_id=topic_id,
            notify=True,
            reply_markup=choices_reply_markup(prompt_id, options),
        )
        save_state(state)
        return {"handled": True, "reply": ""}
    if command in {"raw", "read"}:
        try:
            lines = int(arg.strip() or READ_LINES_COMMAND_DEFAULT)
        except ValueError:
            lines = READ_LINES_COMMAND_DEFAULT
        lines = max(1, min(lines, READ_LINES_COMMAND_MAX))
        text_out = recent_tail(pane_id, lines=lines, max_chars=MAX_REPLY_CHARS - 300)
        return {"handled": True, "reply": text_out or "No visible output available."}
    if command == "debug":
        pane = pane_by_id(pane_id)
        return {"handled": True, "reply": format_debug(pane, entry)}
    if command == "send":
        if not arg:
            return {"handled": True, "reply": "Usage: /send <instruction for this pane>"}
        ok, detail = send_to_pane(pane_id, arg)
        if not ok:
            return {"handled": True, "reply": f"Send failed: {detail}"}
        return {"handled": True, "reply": "Sent."}
    if command == "keys":
        if not arg:
            return {"handled": True, "reply": "Usage: /keys <key> [key ...]"}
        try:
            keys = shlex.split(arg)
        except ValueError as exc:
            return {"handled": True, "reply": f"Could not parse keys: {exc}"}
        if not keys:
            return {"handled": True, "reply": "Usage: /keys <key> [key ...]"}
        proc = run_cmd([herdr_bin(), "pane", "send-keys", pane_id, *keys], timeout=8)
        if proc.returncode != 0:
            return {"handled": True, "reply": f"Keys failed: {sanitize_text(proc.stderr or proc.stdout, 800)}"}
        return {"handled": True, "reply": f"Sent keys: {' '.join(keys)}"}
    return {"handled": True, "reply": f"Unknown pane command: /{command}. Use /help."}


def callback_reply(payload: dict[str, Any]) -> dict[str, Any]:
    load_dotenv()
    state = load_state()
    chat_id = str(payload.get("chat_id") or "")
    topic_id = str(payload.get("topic_id") or "")
    user_id = str(payload.get("user_id") or "")
    data = str(payload.get("data") or "")
    message_id = str(payload.get("message_id") or "")
    telegram = state.setdefault("telegram", {})

    if not data.startswith("herdr:"):
        return {"handled": False}
    entry = topic_entry(state, chat_id, topic_id)
    if not entry:
        return {"handled": False}

    owners = {str(x) for x in (state.get("telegram") or {}).get("owner_user_ids", [])}
    if not owners:
        owners = {p.strip() for p in os.getenv("TELEGRAM_ALLOWED_USERS", DEFAULT_OWNER_ID).split(",") if p.strip()}
    if user_id not in owners:
        return {"handled": True, "answer": "Not authorized.", "show_alert": True}

    parts = data.split(":")
    if len(parts) != 4 or parts[1] not in {"c", "d"}:
        return {"handled": True, "answer": "Unknown Herdr action."}
    action = parts[1]
    prompt_id = parts[2]
    choice_number = parts[3]
    prompt = entry.get("active_prompt") if isinstance(entry.get("active_prompt"), dict) else {}
    if str(prompt.get("id") or "") != prompt_id:
        return {"handled": True, "answer": "Those choices are no longer active."}
    options = list(prompt.get("options") or [])

    pane_id = str(entry.get("pane_id") or "")
    if not pane_id or entry.get("last_known_status") == "closed":
        return {"handled": True, "answer": "This pane is no longer live.", "show_alert": True}

    option = next(
        (
            opt
            for opt in options
            if str(opt.get("number") or "") == choice_number
            or str(opt.get("callback_id") or "") == choice_number
            or str(opt.get("id") or "") == choice_number
        ),
        None,
    )

    if action == "d":
        choice_text = ""
        option_label = "custom"
        if option:
            option_label = str(option.get("label") or option.get("id") or choice_number)
            if str(option.get("id") or "").lower() != "custom" and choice_number.lower() != "custom":
                choice_text = str(option.get("send_text") if "send_text" in option else option.get("number") or choice_number)
        entry["awaiting_detail"] = {
            "user_id": user_id,
            "prompt_id": prompt_id,
            "choice": sanitize_text(choice_text, 500).strip(),
            "option": sanitize_text(option_label, 160),
            "created_at": utc_now(),
        }
        notice_title = "Custom reply" if not choice_text else f"Details for {choice_number}"
        notice_body = "Write the instruction to send to this pane." if not choice_text else "Write the details to send with this choice."
        notice = send_notice(
            chat_id,
            notice_title,
            notice_body,
            telegram=telegram,
            thread_id=topic_id,
            notify=True,
            reply_markup={
                "force_reply": True,
                "selective": True,
                "input_field_placeholder": "Instruction for this pane" if not choice_text else f"Details for {choice_number}",
            },
            reply_to_message_id=message_id,
        )
        if notice.get("message_id"):
            entry["awaiting_detail"]["force_reply_message_id"] = str(notice["message_id"])
        save_state(state)
        return {"handled": True, "answer": "Write the instruction in this topic." if not choice_text else "Write the details in this topic."}

    if not option:
        return {"handled": True, "answer": "Choice not found."}

    if choice_needs_detail(option):
        choice_text = str(option.get("send_text") if "send_text" in option else choice_number).strip()
        entry["awaiting_detail"] = {
            "user_id": user_id,
            "prompt_id": prompt_id,
            "choice": sanitize_text(choice_text, 500),
            "option": str(option.get("label") or ""),
            "created_at": utc_now(),
        }
        notice = send_notice(
            chat_id,
            f"Details for option {choice_number}",
            "Write what should change or what to send with this choice.",
            telegram=telegram,
            thread_id=topic_id,
            notify=True,
            reply_markup={
                "force_reply": True,
                "selective": True,
                "input_field_placeholder": f"Details for option {choice_number}",
            },
            reply_to_message_id=message_id,
        )
        if notice.get("message_id"):
            entry["awaiting_detail"]["force_reply_message_id"] = str(notice["message_id"])
        save_state(state)
        return {"handled": True, "answer": "Write the details in this topic."}

    outbound = str(option.get("send_text") if "send_text" in option else choice_number).strip()
    if not outbound:
        return {"handled": True, "answer": "This choice needs details.", "show_alert": True}
    ok, detail = send_to_pane(pane_id, outbound)
    if not ok:
        return {"handled": True, "answer": f"Send failed: {detail}", "show_alert": True}
    entry.pop("active_prompt", None)
    entry.pop("awaiting_detail", None)
    save_state(state)
    send_notice(
        chat_id,
        "Selected",
        f"{choice_number}) {option.get('label')}",
        telegram=telegram,
        thread_id=topic_id,
        notify=False,
    )
    return {"handled": True, "answer": f"Selected {choice_number}."}


def probe_rich(thread_id: str | None = None) -> dict[str, Any]:
    load_dotenv()
    state = load_state()
    telegram = state.setdefault("telegram", {})
    chat_id = str(telegram.get("chat_id") or os.getenv("HERDR_TELEGRAM_TOPICS_CHAT_ID") or DEFAULT_CHAT_ID)
    if not chat_id:
        raise BridgeError("HERDR_TELEGRAM_TOPICS_CHAT_ID is required")
    topic_id = (
        thread_id
        or os.getenv("HERDR_TELEGRAM_TOPICS_PROBE_THREAD_ID", "").strip()
        or str(telegram.get("general_thread_id") or DEFAULT_GENERAL_THREAD_ID)
    )
    result = send_notice(
        chat_id,
        "Rich Probe",
        "This message verifies Herdr rich-message delivery and will be deleted when possible.",
        telegram=telegram,
        thread_id=topic_id,
        notify=False,
    )
    message_id = result.get("message_id")
    deleted = False
    if message_id:
        try:
            deleted = bool(telegram_api("deleteMessage", {"chat_id": chat_id, "message_id": str(message_id)}).get("result"))
        except Exception:
            deleted = False
    save_state(state)
    return {"ok": bool(result.get("ok")), "format": result.get("format"), "message_id": message_id, "deleted": deleted}


def with_lock(fn, *, blocking: bool = False):
    path = lock_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as lock_fh:
        try:
            flags = fcntl.LOCK_EX if blocking else fcntl.LOCK_EX | fcntl.LOCK_NB
            fcntl.flock(lock_fh.fileno(), flags)
        except BlockingIOError:
            return {"ok": True, "changed": False, "message": "another sync is running"}
        return fn()


def main() -> int:
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="cmd", required=True)
    sub.add_parser("sync")
    sub.add_parser("event")
    sub.add_parser("plugin-enable")
    sub.add_parser("plugin-disable")
    cleanup = sub.add_parser("cleanup-duplicates")
    cleanup.add_argument("--delete", action="store_true")
    sub.add_parser("command")
    sub.add_parser("callback")
    probe = sub.add_parser("probe")
    probe.add_argument("--thread-id", default=None)
    args = parser.parse_args()
    try:
        if args.cmd == "sync":
            result = with_lock(sync_once)
        elif args.cmd == "event":
            result = with_lock(event_once, blocking=True)
        elif args.cmd == "plugin-enable":
            result = with_lock(lambda: plugin_enable_once(True), blocking=True)
        elif args.cmd == "plugin-disable":
            result = with_lock(lambda: plugin_enable_once(False), blocking=True)
        elif args.cmd == "cleanup-duplicates":
            result = with_lock(lambda: cleanup_duplicates_once(delete=args.delete), blocking=True)
        elif args.cmd == "command":
            payload = json.loads(sys.stdin.read() or "{}")
            result = with_lock(lambda: command_reply(payload), blocking=True)
        elif args.cmd == "callback":
            payload = json.loads(sys.stdin.read() or "{}")
            result = with_lock(lambda: callback_reply(payload), blocking=True)
        else:
            result = with_lock(lambda: probe_rich(args.thread_id), blocking=True)
        print(json.dumps(result, sort_keys=True))
        return 0
    except RateLimited as exc:
        print(json.dumps({"ok": False, "rate_limited": True, "retry_after": exc.retry_after, "error": str(exc)}))
        return 75
    except Exception as exc:
        print(json.dumps({"ok": False, "error": sanitize_text(str(exc), 1000)}))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
