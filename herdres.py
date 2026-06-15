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
READ_LINES_STATUS = int(os.getenv("HERDR_TELEGRAM_TOPICS_STATUS_READ_LINES", "40"))
READ_LINES_COMMAND_DEFAULT = 80
READ_LINES_COMMAND_MAX = 160
MAX_REPLY_CHARS = 3200
MAX_STATUS_CHARS = 1500
MAX_RICH_HTML_CHARS = int(os.getenv("HERDR_TELEGRAM_TOPICS_RICH_MAX_CHARS", "6000"))
MAX_RICH_DETAIL_CHARS = int(os.getenv("HERDR_TELEGRAM_TOPICS_RICH_DETAIL_CHARS", "2400"))
PREFLIGHT_TTL_SECONDS = int(os.getenv("HERDR_TELEGRAM_TOPICS_PREFLIGHT_TTL", "900"))
HERDR_TOPIC_ICON_COLOR = int(os.getenv("HERDR_TELEGRAM_TOPICS_ICON_COLOR", DEFAULT_HERDR_TOPIC_ICON_COLOR))
HERDR_TOPIC_ICON_CUSTOM_EMOJI_ID = os.getenv("HERDR_TELEGRAM_TOPICS_ICON_CUSTOM_EMOJI_ID", "").strip()
CLEAN_FEED_ENABLED = os.getenv("HERDR_TELEGRAM_TOPICS_CLEAN_FEED", "1").lower() in {"1", "true", "yes", "on"}
RICH_MESSAGES_ENABLED = os.getenv("HERDR_TELEGRAM_TOPICS_RICH_MESSAGES", "1").lower() in {"1", "true", "yes", "on"}
LIVE_CARD_ENABLED = os.getenv("HERDR_TELEGRAM_TOPICS_LIVE_CARD", "1").lower() in {"1", "true", "yes", "on"}
ALLOW_UNBOUNDED_REPORTS = os.getenv("HERDR_TELEGRAM_TOPICS_UNBOUNDED_REPORTS", "0").lower() in {
    "1",
    "true",
    "yes",
    "on",
}
RICH_RENDER_VERSION = 9
FEED_READ_LINES = int(os.getenv("HERDR_TELEGRAM_TOPICS_FEED_READ_LINES", "140"))
FEED_MAX_CHARS = int(os.getenv("HERDR_TELEGRAM_TOPICS_FEED_MAX_CHARS", "9000"))
DETAIL_REPLY_TIMEOUT_SECONDS = int(os.getenv("HERDR_TELEGRAM_TOPICS_DETAIL_TIMEOUT", "1800"))
CLEAN_ATTEMPT_TTL_SECONDS = int(os.getenv("HERDR_TELEGRAM_TOPICS_CLEAN_ATTEMPT_TTL", "1800"))
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
REPORT_TITLE_RE = re.compile(r"^\s*HERDRES_REPORT_TITLE\s*:\s*(.{1,80})\s*$", re.IGNORECASE)
BAD_TITLE_WORDS_RE = re.compile(
    r"\b(first non-empty|becomes|because|should|could|would|which|that|etc)\b",
    re.IGNORECASE,
)
ACTION_QUESTION_RE = re.compile(
    r"\b(should i|should we|do you want me to|would you like me to|would you like|approve|choose|select|run|deploy|continue|proceed)\b",
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
STRUCTURED_SECTION_RE = re.compile(r"^\s*(SUMMARY|TABLE|CHECKLIST|DETAILS|FOOTER)\s*:\s*(.*?)\s*$", re.IGNORECASE)


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


def title_from_text(text: str) -> str:
    lower = text.lower()
    rules = [
        (("topic name", "topic naming", "editforumtopic", "forum topic icon"), "Topic Names"),
        (("herdres", "createforumtopic", "herdr pane telegram", "topic sync"), "Topic Sync"),
        (("flightrecorder", "flight recorder"), "Flight Recorder"),
        (("gitmoot", "code review", "review pass"), "Review"),
        (("summarize recent commits", "recent commits"), "Commits"),
    ]
    for needles, title in rules:
        if any(needle in lower for needle in needles):
            return title
    return ""


def topic_name_for_pane(pane: dict[str, Any]) -> str:
    label = str(pane.get("label") or "").strip()
    label_title = title_from_text(label)
    if label_title:
        return label_title
    if label:
        return clean_topic_title(label)

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


def extract_bounded_report(lines: list[str]) -> tuple[str, str] | None:
    text = "\n".join(lines)
    matches = list(REPORT_BLOCK_RE.finditer(text))
    if not matches:
        return None

    body = matches[-1].group(1).strip()
    body_lines = strip_outer_blank_lines(body.splitlines())
    if not body_lines:
        return None

    title = ""
    meta = REPORT_TITLE_RE.match(body_lines[0])
    if meta:
        title = sanitize_text(meta.group(1).strip(), 80)
        body_lines = body_lines[1:]
    elif is_safe_report_title(body_lines[0]):
        title = sanitize_text(body_lines[0].strip().rstrip(":"), 80)
        body_lines = body_lines[1:]
    else:
        return None

    body_text = "\n".join(strip_outer_blank_lines(body_lines)).strip()
    if not title or not body_text:
        return None
    return title, body_text


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


def item_plain_text(item: dict[str, Any]) -> str:
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
    clean = _html_text(value, MAX_RICH_DETAIL_CHARS).strip()
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


def _rich_inline(value: str, max_chars: int = 500) -> str:
    clean = str(value or "").strip()
    if _is_codeish_line(clean):
        return f"<code>{_html_text(clean, max_chars)}</code>"
    return _html_text(clean, max_chars)


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
        match = STRUCTURED_SECTION_RE.match(str(raw or ""))
        if match:
            flush()
            has_structured = True
            current_kind = match.group(1).lower()
            current_title = match.group(2).strip()
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


def _rich_table_section(lines: list[str]) -> str:
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
    html_rows = [
        "<tr>" + "".join(f"<th>{_html_text(cell, 160)}</th>" for cell in header) + "</tr>",
    ]
    html_rows.extend(
        "<tr>" + "".join(f"<td>{_html_text(cell, 160)}</td>" for cell in row) + "</tr>"
        for row in body
    )
    return "<table>\n" + "\n".join(html_rows) + "\n</table>"


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


def render_feed_item_html(item: dict[str, Any], *, live: bool = False) -> str:
    kind = str(item.get("kind") or "update").lower()
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


def extract_choices(lines: list[str], *, allow_trailing_without_context: bool = False) -> dict[str, Any] | None:
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
    context = []
    for line in lines[max(0, start - 4):start]:
        low = line.lower()
        if line_is_question_heading(line) or contains_marker(low, QUESTION_MARKERS) or line.endswith("?"):
            context.append(line)
        elif context:
            context.append(line)
    has_context = bool(context)
    is_trailing = end >= len(lines) - 2
    if not has_context and not (allow_trailing_without_context and is_trailing):
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


def extract_clean_feed_item(
    pane: dict[str, Any],
    entry: dict[str, Any],
    raw_text: str,
    *,
    allow_unbounded_reports: bool = ALLOW_UNBOUNDED_REPORTS,
) -> dict[str, Any] | None:
    lines = clean_feed_lines(raw_text)
    if not lines:
        return None

    status = str(pane.get("agent_status") or "").lower()
    tail = compact_block(lines, max_lines=80, max_chars=5000)
    if not tail:
        return None

    bounded_report = extract_bounded_report(lines)
    if bounded_report and status in {"done", "idle"}:
        title, body = bounded_report
        if body.strip():
            return make_feed_item("report", title, body, notify=False)
        return None

    if allow_unbounded_reports:
        report_idx = report_start_index(lines)
        if report_idx is not None and status in {"done", "idle"}:
            title, body = report_title_and_body(lines)
            if body.strip():
                return make_feed_item("report", title, body, notify=False)
            return None

    choices = extract_choices(lines, allow_trailing_without_context=status in {"blocked", "error", "unknown"})
    if choices:
        return choices
    if is_action_question(lines):
        return make_feed_item("question", "Question", tail, notify=True)
    if status in {"blocked", "error"}:
        heading = "Blocked" if status == "blocked" else "Error"
        return make_feed_item(status, heading, tail, notify=True)
    return None


def clean_feed_hash(item: dict[str, Any]) -> str:
    payload = {
        "render_version": RICH_RENDER_VERSION,
        "kind": item.get("kind"),
        "text": item.get("text"),
        "title": item.get("title"),
        "summary": item.get("summary"),
        "detail": item.get("detail"),
        "lines": item.get("lines") or [],
        "options": item.get("options") or [],
    }
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
        "last_clean_kind",
        "last_clean_text",
        "last_clean_item",
        "last_clean_sent_at",
        "last_clean_send_error",
        "last_clean_attempt_hash",
        "last_clean_attempt_at",
        "active_prompt",
        "awaiting_detail",
    ):
        entry.pop(key, None)


def choice_needs_detail(option: dict[str, str]) -> bool:
    label = str(option.get("label") or "").lower()
    number = str(option.get("number") or "")
    return number == "4" or any(word in label for word in ("detail", "feedback", "change", "other", "refine"))


def choices_reply_markup(prompt_id: str, options: list[dict[str, str]]) -> dict[str, Any]:
    rows: list[list[dict[str, str]]] = []
    for idx, opt in enumerate(options[:12], start=1):
        number = str(opt.get("number") or idx)
        label = re.sub(r"\s+", " ", str(opt.get("label") or "")).strip()
        button_text = f"{number}. {label}" if label else number
        rows.append([{"text": button_text[:64], "callback_data": f"herdr:c:{prompt_id}:{number}"}])
    rows.append([{"text": "Custom reply", "callback_data": f"herdr:d:{prompt_id}:custom"}])
    return {"inline_keyboard": rows}


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
        f"card_message_id: {entry.get('card_message_id') or ''}",
        f"card_hash: {entry.get('card_hash') or ''}",
        f"card_status_hash: {entry.get('card_status_hash') or ''}",
        f"card_format: {entry.get('card_format') or ''}",
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


def send_to_pane(pane_id: str, text: str, *, timeout: int = 8) -> tuple[bool, str]:
    pane = pane_by_id(pane_id)
    if not pane:
        return False, "Herdr pane is not currently live."
    if pane.get("agent"):
        proc = run_cmd([herdr_bin(), "agent", "send", pane_id, text], timeout=timeout)
    else:
        proc = run_cmd([herdr_bin(), "pane", "send-text", pane_id, text], timeout=timeout)
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
        "sendrichmessage" in text
        and ("not found" in text or "does not exist" in text or "no such method" in text or "http 404" in text)
    ):
        return "capability"
    if "method" in text and ("not found" in text or "does not exist" in text):
        return "capability"
    if "message is not modified" in text:
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
        mid = send_message(
            chat_id,
            fallback,
            thread_id=thread_id,
            notify=notify,
            reply_markup=reply_markup,
            reply_to_message_id=reply_to_message_id,
        )
        return {"ok": True, "format": "legacy", "message_id": mid}

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
            mid = send_message(
                chat_id,
                fallback,
                thread_id=thread_id,
                notify=notify,
                reply_markup=reply_markup,
                reply_to_message_id=reply_to_message_id,
            )
            return {"ok": True, "format": "legacy", "message_id": mid, "fallback_reason": kind}
        if kind == "bad_request":
            mid = send_message(
                chat_id,
                fallback,
                thread_id=thread_id,
                notify=notify,
                reply_markup=reply_markup,
                reply_to_message_id=reply_to_message_id,
            )
            return {"ok": True, "format": "legacy", "message_id": mid, "fallback_reason": kind}
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


def create_topic(chat_id: str, name: str) -> str:
    payload: dict[str, Any] = {"chat_id": chat_id, "name": name}
    if HERDR_TOPIC_ICON_CUSTOM_EMOJI_ID:
        payload["icon_custom_emoji_id"] = HERDR_TOPIC_ICON_CUSTOM_EMOJI_ID
    else:
        payload["icon_color"] = str(HERDR_TOPIC_ICON_COLOR)
    result = telegram_api("createForumTopic", payload).get("result") or {}
    topic_id = result.get("message_thread_id")
    if topic_id is None:
        raise BridgeError("createForumTopic returned no message_thread_id")
    return str(topic_id)


def edit_topic(chat_id: str, topic_id: str | int, name: str) -> bool:
    payload: dict[str, Any] = {
        "chat_id": chat_id,
        "message_thread_id": str(topic_id),
        "name": name,
    }
    if HERDR_TOPIC_ICON_CUSTOM_EMOJI_ID:
        payload["icon_custom_emoji_id"] = HERDR_TOPIC_ICON_CUSTOM_EMOJI_ID
    return bool(telegram_api("editForumTopic", payload).get("result"))


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


def ensure_pane_entry(state: dict[str, Any], pane: dict[str, Any]) -> tuple[str, dict[str, Any], bool]:
    key = pane_key(pane)
    panes = state.setdefault("panes", {})
    entry = panes.get(key)
    created = False
    if not isinstance(entry, dict):
        entry = {"pane_key": key, "created_at": utc_now()}
        panes[key] = entry
        created = True
    entry.update({
        "pane_id": str(pane.get("pane_id") or ""),
        "terminal_id": str(pane.get("terminal_id") or ""),
        "agent_session_id": pane_agent_session_id(pane),
        "workspace": str(pane.get("workspace_id") or ""),
        "tab": str(pane.get("tab_id") or ""),
        "topic_name": entry.get("topic_name") or topic_name_for_pane(pane),
    })
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


def sync_once() -> dict[str, Any]:
    load_dotenv()
    state = load_state()
    if not state.get("enabled", True):
        return {"ok": True, "changed": False, "message": "disabled"}
    telegram = state.setdefault("telegram", {})
    chat_id = str(telegram.get("chat_id") or os.getenv("HERDR_TELEGRAM_TOPICS_CHAT_ID") or DEFAULT_CHAT_ID)
    telegram["chat_id"] = chat_id
    telegram.setdefault("general_thread_id", os.getenv("HERDR_TELEGRAM_TOPICS_GENERAL_THREAD_ID", DEFAULT_GENERAL_THREAD_ID))
    telegram.setdefault(
        "owner_user_ids",
        [p.strip() for p in os.getenv("TELEGRAM_ALLOWED_USERS", DEFAULT_OWNER_ID).split(",") if p.strip()],
    )
    telegram.setdefault("implicit_send_enabled", False)

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
                    "Herdr topic sync is blocked before topic creation.\n"
                    f"Reason: {error_text}\n\n"
                    "Grant the bot admin permission to manage topics in the Telegram forum group, then run the sync again.",
                    thread_id=telegram.get("general_thread_id", DEFAULT_GENERAL_THREAD_ID),
                    notify=True,
                )
                telegram["last_preflight_alert_at"] = utc_now()
            except Exception:
                pass
        telegram["last_preflight_error"] = error_text
        save_state(state)
        raise
    creates = 0

    for pane in panes:
        key, entry, new_entry = ensure_pane_entry(state, pane)
        entry["last_seen_at"] = utc_now()
        entry["last_known_status"] = str(pane.get("agent_status") or "unknown")
        if not entry.get("topic_id") and creates < MAX_CREATES_PER_RUN:
            topic_name = str(entry.get("topic_name") or topic_name_for_pane(pane))
            topic_id = create_topic(chat_id, topic_name)
            creates += 1
            entry["topic_id"] = topic_id
            entry["topic_name"] = topic_name
            save_state(state)
            changed = True
            if not CLEAN_FEED_ENABLED and sends < MAX_SENDS_PER_RUN:
                send_message(
                    chat_id,
                    f"Linked Telegram topic to Herdr pane.\nPane key: {key}\n\n{format_status(pane, include_commands=True)}",
                    thread_id=topic_id,
                )
                sends += 1
        if not entry.get("topic_id"):
            continue
        stable_obj_hash = status_hash(stable_status_object(pane))
        live_item = live_status_item(pane)
        live_card_hash = clean_feed_hash(live_item)
        if LIVE_CARD_ENABLED and sends < MAX_SENDS_PER_RUN and (
            not entry.get("card_message_id") or entry.get("card_status_hash") != live_card_hash
        ):
            card_result = update_live_card(chat_id, entry, live_item, telegram=telegram)
            if card_result.get("attempted"):
                sends += 1
            if card_result.get("ok"):
                entry["card_status_hash"] = live_card_hash
                changed = True
        if CLEAN_FEED_ENABLED:
            raw = pane_feed_output(str(pane.get("pane_id") or ""))
            clean_lines = clean_feed_lines(raw)
            bounded_report = extract_bounded_report(clean_lines)
            item = None
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
                item_hash = clean_feed_hash(item)
                if (
                    sends < MAX_SENDS_PER_RUN
                    and (old_clean_has_noise or item_hash != entry.get("last_clean_hash"))
                    and not recent_attempt(entry, item_hash)
                ):
                    reply_markup = None
                    pending_active_prompt = None
                    clear_active_prompt = False
                    if item.get("kind") == "choices":
                        options = list(item.get("options") or [])
                        prompt_id = str(item.get("prompt_id") or prompt_id_for(item_plain_text(item), options))
                        item["prompt_id"] = prompt_id
                        reply_markup = choices_reply_markup(prompt_id, options)
                        pending_active_prompt = {
                            "id": prompt_id,
                            "text": item_plain_text(item),
                            "item": item,
                            "options": options,
                            "created_at": utc_now(),
                        }
                    else:
                        clear_active_prompt = True
                    entry["last_clean_attempt_hash"] = item_hash
                    entry["last_clean_attempt_at"] = utc_now()
                    changed = True
                    result = send_feed_item(
                        chat_id,
                        item,
                        telegram=telegram,
                        thread_id=entry["topic_id"],
                        notify=bool(item.get("notify")),
                        reply_markup=reply_markup,
                    )
                    if result.get("ok"):
                        sends += 1
                        if pending_active_prompt:
                            entry["active_prompt"] = pending_active_prompt
                        elif clear_active_prompt:
                            entry.pop("active_prompt", None)
                        entry["last_clean_hash"] = item_hash
                        entry["last_clean_kind"] = str(item.get("kind") or "")
                        entry["last_clean_text"] = item_plain_text(item)
                        entry["last_clean_item"] = item
                        entry["last_clean_sent_at"] = utc_now()
                        entry.pop("last_clean_send_error", None)
                        changed = True
                    else:
                        entry["last_clean_send_error"] = sanitize_text(str(result), 500)
                        changed = True
            elif old_clean_has_noise:
                clear_clean_feed_state(entry)
                changed = True
            entry["last_status_hash"] = stable_obj_hash
        elif sends < MAX_SENDS_PER_RUN and should_send_status(entry, stable_obj_hash, pane, new_entry):
            pane_status = str(pane.get("agent_status") or "").lower()
            include_recent = pane_status in {"blocked", "unknown"}
            send_message(
                chat_id,
                format_status(pane, include_recent=include_recent),
                thread_id=entry["topic_id"],
                notify=pane_status in {"blocked", "error"},
            )
            sends += 1
            entry["last_status_hash"] = stable_obj_hash
            entry["last_notified_status"] = pane_status
            entry["last_sent_at"] = utc_now()
            changed = True

    save_state(state)
    return {"ok": True, "changed": changed, "panes": len(panes), "created": creates, "sent": sends}


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
            choice = str(awaiting.get("choice") or "").strip()
            outbound = f"{choice}\n{arg}" if choice else arg
            ok, detail = send_to_pane(pane_id, outbound)
            if not ok:
                return {"handled": True, "reply": f"Send failed: {detail}"}
            entry.pop("awaiting_detail", None)
            save_state(state)
            return {"handled": True, "reply": "Sent details."}
        implicit = bool((state.get("telegram") or {}).get("implicit_send_enabled", False))
        if implicit:
            payload = dict(payload)
            payload["text"] = "/send " + arg
            return command_reply(payload)
        return {"handled": True, "reply": "This is a mapped Herdr pane topic. Use /send <text> to forward to this pane, or /help."}

    if command in {"help", "start"}:
        return {
            "handled": True,
            "reply": (
                "Pane topic commands:\n"
                "/report or /status - latest clean report/question\n"
                "/choices - resend active choices\n"
                "/raw [lines] - sanitized raw visible output\n"
                "/debug - technical mapping details\n"
                "/send <text> - send instruction to this pane\n"
                "/keys <keys> - send explicit keys\n"
                "Plain text is not forwarded unless implicit send is enabled."
            ),
        }
    if command in {"status", "report"}:
        pane = pane_by_id(pane_id)
        item = latest_clean_item(entry, pane)
        if item:
            result = send_feed_item(
                chat_id,
                item,
                telegram=telegram,
                thread_id=topic_id,
                notify=False,
            )
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

    if action == "d":
        entry["awaiting_detail"] = {
            "user_id": user_id,
            "prompt_id": prompt_id,
            "choice": "",
            "option": "custom",
            "created_at": utc_now(),
        }
        save_state(state)
        send_notice(
            chat_id,
            "Custom reply",
            "Write the instruction to send to this pane.",
            telegram=telegram,
            thread_id=topic_id,
            notify=True,
            reply_markup={
                "force_reply": True,
                "selective": True,
                "input_field_placeholder": "Instruction for this pane",
            },
            reply_to_message_id=message_id,
        )
        return {"handled": True, "answer": "Write the instruction in this topic."}

    option = next((opt for opt in options if str(opt.get("number")) == choice_number), None)
    if not option:
        return {"handled": True, "answer": "Choice not found."}

    if choice_needs_detail(option):
        entry["awaiting_detail"] = {
            "user_id": user_id,
            "prompt_id": prompt_id,
            "choice": choice_number,
            "option": str(option.get("label") or ""),
            "created_at": utc_now(),
        }
        save_state(state)
        send_notice(
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
        return {"handled": True, "answer": "Write the details in this topic."}

    ok, detail = send_to_pane(pane_id, choice_number)
    if not ok:
        return {"handled": True, "answer": f"Send failed: {detail}", "show_alert": True}
    send_notice(
        chat_id,
        "Selected",
        f"{choice_number}) {option.get('label')}",
        telegram=telegram,
        thread_id=topic_id,
        notify=False,
    )
    save_state(state)
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
    sub.add_parser("command")
    sub.add_parser("callback")
    probe = sub.add_parser("probe")
    probe.add_argument("--thread-id", default=None)
    args = parser.parse_args()
    try:
        if args.cmd == "sync":
            result = with_lock(sync_once)
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
