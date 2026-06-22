#!/usr/bin/env python3
"""Local Herdr wrapper that adds `pane turn` from agent session logs.

All non-`pane turn` commands are delegated to the real Herdr binary. This keeps
the integration upgrade-safe: Herdr itself is never patched.
"""

from __future__ import annotations

import contextlib
import json
import hashlib
import os
import re
import subprocess
import sys
import unicodedata
from pathlib import Path
from typing import Any, Callable


DEFAULT_REAL_HERDR = "herdr"
MAX_TEXT_CHARS = int(os.getenv("HERDRES_TURN_ADAPTER_MAX_TEXT_CHARS", "12000"))
CLAUDE_FALLBACK_MAX_FILES = int(os.getenv("HERDRES_CLAUDE_FALLBACK_MAX_FILES", "24"))
CLAUDE_FALLBACK_READ_LINES = int(os.getenv("HERDRES_CLAUDE_FALLBACK_READ_LINES", "160"))
# How many recent completed turns to expose so the bridge can catch up on a
# burst of completions (e.g. a reply immediately followed by an auto-pursued
# turn) instead of collapsing to only the newest one.
RECENT_TURNS = int(os.getenv("HERDRES_RECENT_TURNS", "12"))
# Tail-read window for turn extraction so a huge rollout (1+ GB) isn't fully parsed every
# sync. Files <= TURN_TAIL_BYTES are read whole (byte-identical to a full read); larger
# files read only the recent tail starting at a turn boundary. TURN_TAIL_MAX_BYTES caps
# the grow-the-window loop. Both env-overridable.
TURN_TAIL_BYTES = int(os.getenv("HERDRES_TURN_TAIL_BYTES", str(512 * 1024)))
TURN_TAIL_MAX_BYTES = int(os.getenv("HERDRES_TURN_TAIL_MAX_BYTES", str(8 * 1024 * 1024)))
DEVIN_SESSION_START_SKEW_SECONDS = int(os.getenv("HERDRES_DEVIN_SESSION_START_SKEW", "30"))
# Per-turn worklog: summarize the intermediate tool_use + interim assistant text so a
# COMPLETED turn carries a worklog the bridge renders under the Response. Previously the
# worklog was live-only (open turns) and cleared on completion; this persists it. Toggle
# off with HERDRES_TURN_ADAPTER_WORKLOG=0.
WORKLOG_ENABLED = os.getenv("HERDRES_TURN_ADAPTER_WORKLOG", "1").strip().lower() not in (
    "0",
    "false",
    "no",
    "off",
)
WORKLOG_MAX_CHARS = int(os.getenv("HERDRES_TURN_ADAPTER_WORKLOG_MAX_CHARS", "4000"))
WORKLOG_LINE_MAX_CHARS = int(os.getenv("HERDRES_TURN_ADAPTER_WORKLOG_LINE_MAX_CHARS", "160"))
# Cap the accumulator itself (not just the emitted join) so a very long turn can't grow
# worklog_parts without bound; we keep the most recent lines (closest to the answer).
WORKLOG_MAX_LINES = int(os.getenv("HERDRES_TURN_ADAPTER_WORKLOG_MAX_LINES", "400"))

# Redact obvious secrets before a tool arg / interim line reaches Telegram: KEY=value or
# "Header: value" for auth-ish keys, and URL embedded credentials. Best-effort, not a
# guarantee — worklog content is a summary, so over-redaction is preferred to a leak.
_SECRET_KV_RE = re.compile(
    r"(?i)\b(authorization|bearer|api[_-]?key|access[_-]?key|client[_-]?secret|secret|token|password|passwd|passphrase)\b"
    r"([\"']?\s*[:=]\s*[\"']?)(\S+)"
)
_URL_CRED_RE = re.compile(r"://[^/\s:@]+:[^/\s@]+@")


def _redact_secrets(text: str) -> str:
    redacted = _SECRET_KV_RE.sub(lambda m: f"{m.group(1)}{m.group(2)}***", text)
    return _URL_CRED_RE.sub("://***@", redacted)


def _strip_control(text: str) -> str:
    # Drop control + format chars (incl. NUL, bidi overrides like U+202E, zero-width) that
    # could spoof or corrupt the rendered worklog; keep tabs.
    return "".join(ch for ch in text if ch == "\t" or unicodedata.category(ch) not in ("Cc", "Cf"))


def _clean_worklog_line(text: str) -> str:
    return _redact_secrets(_strip_control(text)).strip()


def _join_worklog(parts: list[str]) -> str:
    """Join worklog lines keeping the MOST RECENT that fit WORKLOG_MAX_CHARS.

    Truncation keeps the tail (the steps closest to the final answer), not the oldest
    boilerplate — and the [...] marker shows when earlier steps were dropped.
    """
    kept: list[str] = []
    total = 0
    dropped = False
    for line in reversed(parts):
        add = len(line) + 1
        if kept and total + add > WORKLOG_MAX_CHARS:
            dropped = True
            break
        kept.append(line)
        total += add
    kept.reverse()
    if dropped:
        kept.insert(0, "[...]")
    return "\n".join(kept).strip()


def real_herdr_bin() -> str:
    return os.getenv("HERDR_REAL_BIN", DEFAULT_REAL_HERDR)


def exec_real_herdr() -> None:
    real = real_herdr_bin()
    os.execvp(real, [real, *sys.argv[1:]])


def run_real_herdr_json(args: list[str]) -> dict[str, Any]:
    proc = subprocess.run(
        [real_herdr_bin(), *args],
        text=True,
        capture_output=True,
        timeout=8,
        check=False,
    )
    if proc.returncode != 0:
        raise RuntimeError((proc.stderr or proc.stdout or "").strip() or "real herdr failed")
    return json.loads(proc.stdout)


def run_real_herdr_text(args: list[str]) -> str:
    proc = subprocess.run(
        [real_herdr_bin(), *args],
        text=True,
        capture_output=True,
        timeout=8,
        check=False,
    )
    if proc.returncode != 0:
        return ""
    return proc.stdout


def sanitize_text(value: str, max_chars: int = MAX_TEXT_CHARS) -> str:
    text = str(value or "").replace("\x00", "")
    if len(text) > max_chars:
        return text[: max_chars - 20].rstrip() + "\n[truncated]"
    return text


def unavailable(reason: str, **extra: Any) -> dict[str, Any]:
    turn = {"available": False, "reason": reason}
    turn.update(extra)
    return {"ok": True, "result": {"turn": turn}}


def result_turn(turn: dict[str, Any]) -> dict[str, Any]:
    return {"ok": True, "result": {"turn": turn}}


def stream_revision(text: str) -> str:
    return hashlib.sha256(str(text or "").encode("utf-8")).hexdigest()[:16]


def add_stream_fields(
    turn: dict[str, Any],
    text: str,
    source: str,
    updated_at: Any = "",
) -> dict[str, Any]:
    stream_text = sanitize_text(str(text or "").strip())
    if not stream_text:
        return turn
    turn["assistant_stream_text"] = stream_text
    turn["stream_revision"] = stream_revision(stream_text)
    turn["stream_updated_at"] = updated_at or ""
    turn["stream_source"] = source
    return turn


def content_text(content: Any) -> str:
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        parts: list[str] = []
        for item in content:
            if not isinstance(item, dict):
                continue
            text = item.get("text")
            if isinstance(text, str):
                parts.append(text)
        return "\n".join(part for part in parts if part)
    return ""


def _jsonl_tail_lines(
    path: Path,
    is_turn_start: Callable[[str], bool],
    is_turn_end: Callable[[str], bool],
    cap: int,
    min_turns: int,
) -> list[str]:
    """Lines of a large JSONL file from the first turn-start boundary within a bounded tail.

    Reads the last ``cap`` bytes from EOF, drops the partial first record, and starts at the
    first line where ``is_turn_start`` holds. Sufficiency is measured by COMPLETED turns
    (``is_turn_end`` markers — task_complete / end_turn), NOT turn-starts: a tail dominated by
    aborted / open / internal-prompt markers would otherwise stop growing before it holds the
    recent COMPLETED turns and surface fewer than a full read. If fewer than ``min_turns + 1``
    completions are present and the window can still grow, doubles it up to TURN_TAIL_MAX_BYTES.
    Returns ``[]`` (no usable start boundary, e.g. one turn larger than the ceiling) so the
    caller falls back to a full read.
    """
    size = path.stat().st_size
    window = cap
    while True:
        start = max(0, size - window)
        with path.open("rb") as fh:
            fh.seek(start)
            chunk = fh.read()
        lines = chunk.decode("utf-8", errors="replace").split("\n")
        if start > 0:
            lines = lines[1:]  # drop the partial record we seeked into the middle of
        first_idx: int | None = None
        completed = 0
        for i, ln in enumerate(lines):
            if not ln:
                continue
            if first_idx is None and is_turn_start(ln):
                first_idx = i
            if is_turn_end(ln):
                completed += 1
        at_ceiling = start == 0 or window >= TURN_TAIL_MAX_BYTES
        if first_idx is not None and (completed >= min_turns + 1 or at_ceiling):
            return lines[first_idx:]
        if at_ceiling:
            return []  # no start boundary even at the ceiling -> caller does a full read
        window = min(window * 2, size, TURN_TAIL_MAX_BYTES)


@contextlib.contextmanager
def open_jsonl_tail(
    path: Path,
    is_turn_start: Callable[[str], bool],
    is_turn_end: Callable[[str], bool],
    *,
    cap: int | None = None,
    min_turns: int = RECENT_TURNS,
):
    """Yield an iterable of JSONL lines for the recent tail of ``path``.

    A file <= the cap is read whole (a real file handle — byte-identical to the prior
    ``with path.open(...) as handle``). A larger file is read from a bounded tail starting
    at a clean turn-start boundary, so a multi-GB transcript is never fully parsed every
    sync. Both turn parsers reset all per-turn state at a boundary, so the exposed recent
    turns are identical to a full read. A drop-in for ``with path.open(...) as handle:``.
    """
    cap = TURN_TAIL_BYTES if cap is None else cap
    try:
        size = path.stat().st_size
    except OSError:
        yield iter(())
        return
    if size <= cap:
        with path.open(encoding="utf-8", errors="replace") as handle:
            yield handle
        return
    lines = _jsonl_tail_lines(path, is_turn_start, is_turn_end, cap, min_turns)
    if not lines:
        # One turn larger than the ceiling: full read so correctness is never compromised.
        with path.open(encoding="utf-8", errors="replace") as handle:
            yield handle
        return
    yield iter(lines)


def _codex_is_turn_start(raw: str) -> bool:
    try:
        event = json.loads(raw)
    except Exception:
        return False
    payload = event.get("payload") if isinstance(event.get("payload"), dict) else {}
    return event.get("type") == "event_msg" and payload.get("type") == "task_started"


def _claude_is_turn_start(raw: str) -> bool:
    try:
        event = json.loads(raw)
    except Exception:
        return False
    return str(event.get("type") or "") == "user"


def _codex_is_turn_end(raw: str) -> bool:
    """A COMPLETED codex turn (task_complete) — used to size the tail window."""
    try:
        event = json.loads(raw)
    except Exception:
        return False
    payload = event.get("payload") if isinstance(event.get("payload"), dict) else {}
    return event.get("type") == "event_msg" and payload.get("type") == "task_complete"


def _claude_is_turn_end(raw: str) -> bool:
    """A COMPLETED claude turn (assistant end_turn) — used to size the tail window."""
    try:
        event = json.loads(raw)
    except Exception:
        return False
    if str(event.get("type") or "") != "assistant":
        return False
    msg = event.get("message") if isinstance(event.get("message"), dict) else {}
    return msg.get("stop_reason") == "end_turn"


# File-path-ish tool args are shown as a basename; everything else as a first-line brief.
_TOOL_PATH_KEYS = ("file_path", "path", "notebook_path")
_TOOL_ARG_KEYS = (
    "command",
    "cmd",  # codex exec_command / shell
    "file_path",
    "path",
    "notebook_path",
    "pattern",
    "query",
    "url",
    "description",
    "prompt",
    "subagent_type",
    "input",  # custom_tool_call / generic
)


def summarize_tool_use(name: Any, tool_input: Any) -> str:
    """One concise worklog line for a tool call: the tool name plus a short arg.

    Handles claude tool_use input dicts and codex/devin arg dicts. A list value
    (e.g. a codex shell ``command: ["bash","-lc","..."]``) is joined to a string.
    """
    label = sanitize_text(str(name or "tool").strip(), 40) or "tool"
    if not isinstance(tool_input, dict):
        return label
    for key in _TOOL_ARG_KEYS:
        val = tool_input.get(key)
        if isinstance(val, list):
            val = " ".join(str(c) for c in val if isinstance(c, (str, int, float)))
        if isinstance(val, str) and val.strip():
            brief = val.strip().splitlines()[0]
            if key in _TOOL_PATH_KEYS:
                brief = brief.rstrip("/").rsplit("/", 1)[-1]
            brief = sanitize_text(brief, WORKLOG_LINE_MAX_CHARS)
            return f"{label} {brief}".strip()
    return label


def _tool_call_worklog_line(name: Any, arguments: Any) -> str:
    """Worklog line for a codex/devin tool call. ``arguments`` may be a JSON string,
    a dict, or a raw string (e.g. an apply_patch body); fall back to a first-line brief."""
    if isinstance(arguments, str):
        s = arguments.strip()
        if s[:1] in "{[":
            try:
                arguments = json.loads(s)
            except Exception:
                pass
    if isinstance(arguments, dict):
        return summarize_tool_use(name, arguments)
    label = sanitize_text(str(name or "tool").strip(), 40) or "tool"
    if isinstance(arguments, list):
        joined = " ".join(str(c) for c in arguments if isinstance(c, (str, int, float))).strip()
        return f"{label} {sanitize_text(joined.splitlines()[0], WORKLOG_LINE_MAX_CHARS)}".strip() if joined else label
    if isinstance(arguments, str) and arguments.strip():
        return f"{label} {sanitize_text(arguments.strip().splitlines()[0], WORKLOG_LINE_MAX_CHARS)}".strip()
    return label


def codex_worklog_line(payload: dict[str, Any]) -> str:
    """One worklog line for a codex intermediate response_item (tool call)."""
    pt = payload.get("type")
    if pt == "function_call":
        return _tool_call_worklog_line(payload.get("name"), payload.get("arguments"))
    if pt == "custom_tool_call":
        return _tool_call_worklog_line(payload.get("name"), payload.get("input"))
    return ""


def devin_tool_call_line(call: Any) -> str:
    """Worklog line for one Devin tool_call entry (OpenAI-style ``function`` or flat)."""
    if not isinstance(call, dict):
        return ""
    fn = call.get("function") if isinstance(call.get("function"), dict) else {}
    name = fn.get("name") or call.get("name") or call.get("tool") or call.get("type") or "tool"
    args = fn.get("arguments")
    if args is None:
        args = call.get("arguments") or call.get("args") or call.get("input") or call.get("parameters")
    return _tool_call_worklog_line(name, args)


def _accumulate_worklog(parts: list[str], raw_lines: list[str]) -> None:
    """Clean (strip control chars + redact secrets), append, and bound the accumulator
    to WORKLOG_MAX_LINES (keeping the most recent lines)."""
    for ln in raw_lines:
        cleaned = _clean_worklog_line(ln)
        if cleaned:
            parts.append(sanitize_text(cleaned, WORKLOG_LINE_MAX_CHARS))
    if len(parts) > WORKLOG_MAX_LINES:
        del parts[: len(parts) - WORKLOG_MAX_LINES]


def claude_worklog_lines(content: Any) -> list[str]:
    """Worklog lines for one assistant message: tool_use summaries + interim text.

    Unlike ``content_text`` (which keeps only text blocks), this also surfaces the
    tool calls — the intermediate steps that make up the worklog under the Response.
    """
    raw: list[str] = []
    if not isinstance(content, list):
        raw = content_text(content).splitlines()
    else:
        for item in content:
            if not isinstance(item, dict):
                continue
            itype = item.get("type")
            if itype == "tool_use":
                raw.append(summarize_tool_use(item.get("name"), item.get("input")))
            elif itype == "text":
                raw.extend(str(item.get("text") or "").splitlines())
    lines: list[str] = []
    for ln in raw:
        cleaned = _clean_worklog_line(ln)  # strip control chars + redact secrets
        if cleaned:
            lines.append(sanitize_text(cleaned, WORKLOG_LINE_MAX_CHARS))
    return lines


def is_internal_codex_user_text(text: str) -> bool:
    stripped = text.lstrip()
    return stripped.startswith((
        "<environment_context>",
        "<turn_aborted>",
        "<codex_internal_context>",
        "<codex_internal_context ",
        "<permissions instructions>",
        "<collaboration_mode>",
        "<skills_instructions>",
        "<subagent_notification>",
        "<model_switch>",
    ))


def is_internal_claude_user_text(text: str) -> bool:
    stripped = text.lstrip()
    return stripped.startswith((
        "<task-notification>",
        "<local-command-stdout>",
        "<local-command-stderr>",
        "<system-reminder>",
        "<ide_context>",
        "<command-name>",
    ))


def codex_session_path(session_id: str) -> Path | None:
    base = Path(os.getenv("CODEX_HOME", str(Path.home() / ".codex"))).expanduser()
    sessions = base / "sessions"
    if not sessions.exists():
        return None
    matches = sorted(sessions.glob(f"**/*{session_id}.jsonl"), key=lambda p: p.stat().st_mtime, reverse=True)
    return matches[0] if matches else None


def claude_session_path(session_id: str) -> Path | None:
    base = Path(os.getenv("CLAUDE_PROJECTS_DIR", str(Path.home() / ".claude/projects"))).expanduser()
    if not base.exists():
        return None
    matches = sorted(base.glob(f"**/{session_id}.jsonl"), key=lambda p: p.stat().st_mtime, reverse=True)
    return matches[0] if matches else None


def devin_transcripts_dir() -> Path:
    return Path(os.getenv("DEVIN_TRANSCRIPTS_DIR", str(Path.home() / ".local/share/devin/cli/transcripts"))).expanduser()


def devin_bin() -> str:
    """Return the devin CLI path, falling back to ~/.local/bin/devin.

    The launchd environment has a minimal PATH that doesn't include
    ~/.local/bin, so shutil.which() fails under launchd.
    """
    explicit = os.getenv("DEVIN_BIN")
    if explicit:
        return explicit
    for candidate in ("devin", str(Path.home() / ".local/bin/devin")):
        try:
            if subprocess.run([candidate, "--version"], capture_output=True, timeout=2).returncode == 0:
                return candidate
        except Exception:
            continue
    return str(Path.home() / ".local/bin/devin")


def pane_devin_process_started_at(pane_id: str) -> int:
    if not pane_id:
        return 0
    try:
        data = run_real_herdr_json(["pane", "process-info", "--pane", pane_id])
    except Exception:
        return 0
    info = data.get("result", {}).get("process_info")
    if not isinstance(info, dict):
        return 0
    starts: list[int] = []
    for proc in info.get("foreground_processes") or []:
        if not isinstance(proc, dict):
            continue
        name = str(proc.get("name") or "").lower()
        cmdline = str(proc.get("cmdline") or "").lower()
        if "devin" not in name and "devin" not in cmdline:
            continue
        try:
            pid = int(proc.get("pid") or 0)
        except (TypeError, ValueError):
            continue
        if pid <= 0:
            continue
        try:
            starts.append(int(os.stat(f"/proc/{pid}").st_ctime))
        except OSError:
            continue
    return min(starts) if starts else 0


def devin_resolve_session_id(pane: dict[str, Any]) -> str:
    """Resolve the Devin session ID for a pane when agent_session is None.

    Uses `devin list --format json` and matches by working directory.
    Prefers sessions that have either a transcript JSON file or rows in
    sessions.db (most active sessions store data in the DB).
    """
    cwd = str(pane.get("cwd") or pane.get("foreground_cwd") or "")
    pane_started_at = pane_devin_process_started_at(str(pane.get("pane_id") or ""))
    try:
        proc = subprocess.run(
            [devin_bin(), "list", "--format", "json"],
            cwd=cwd or None,
            capture_output=True,
            text=True,
            timeout=3,
            check=False,
        )
        if proc.returncode == 0 and proc.stdout.strip():
            entries = json.loads(proc.stdout)
            if isinstance(entries, list):
                cwd_real = os.path.realpath(cwd) if cwd else ""
                matching = [
                    e for e in entries
                    if isinstance(e, dict)
                    and os.path.realpath(str(e.get("working_directory") or "")) == cwd_real
                ]
                if matching:
                    matching.sort(key=lambda e: int(e.get("last_activity_at") or 0), reverse=True)
                    transcripts_dir = devin_transcripts_dir()
                    db_path = devin_sessions_db()
                    valid: list[str] = []
                    for e in matching:
                        sid = str(e.get("id") or "")
                        if not sid:
                            continue
                        try:
                            last_activity_at = int(e.get("last_activity_at") or 0)
                        except (TypeError, ValueError):
                            last_activity_at = 0
                        if pane_started_at and last_activity_at < pane_started_at - DEVIN_SESSION_START_SKEW_SECONDS:
                            continue
                        if (transcripts_dir / f"{sid}.json").exists() or devin_db_has_session(db_path, sid):
                            if sid not in valid:
                                valid.append(sid)
                    # FAIL CLOSED on an ambiguous cwd. A single working directory shared
                    # by more than one live Devin session — e.g. one GLM seat per space
                    # all rooted at /home/smith — cannot be attributed to a specific
                    # pane by cwd alone. Returning the newest (the old behavior) made
                    # ONE pane's turn resolve for EVERY same-cwd pane, broadcasting it to
                    # every space's topic. Resolve only when exactly one session matches;
                    # otherwise return "" so the caller treats the pane as having no
                    # agent session (no delivery) rather than delivering a wrong/shared
                    # turn. A pane with a unique cwd still resolves normally.
                    if len(valid) == 1:
                        return valid[0]
    except Exception:
        pass
    return ""


def devin_session_path(session_id: str) -> Path | None:
    path = devin_transcripts_dir() / f"{session_id}.json"
    return path if path.exists() else None


def devin_sessions_db() -> Path:
    return Path(os.getenv("DEVIN_SESSIONS_DB", str(Path.home() / ".local/share/devin/cli/sessions.db"))).expanduser()


def devin_db_has_session(db_path: Path, session_id: str) -> bool:
    if not db_path.exists():
        return False
    try:
        import sqlite3
        conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True, timeout=1)
        cur = conn.execute("SELECT 1 FROM sessions WHERE id = ? LIMIT 1", (session_id,))
        found = cur.fetchone() is not None
        conn.close()
        return found
    except Exception:
        return False


def extract_devin_turn_from_db(db_path: Path, pane_id: str, session_id: str) -> dict[str, Any]:
    """Extract the latest turn from Devin's sessions.db (SQLite).

    Mirrors extract_devin_turn but reads message_nodes rows instead of
    transcript JSON steps. Each chat_message is JSON with role/content/tool_calls.
    """
    try:
        import sqlite3
        conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True, timeout=2)
        rows = conn.execute(
            "SELECT node_id, chat_message, created_at FROM message_nodes "
            "WHERE session_id = ? ORDER BY node_id ASC",
            (session_id,),
        ).fetchall()
        conn.close()
    except Exception:
        return {"available": False, "reason": "db_read_error", "pane_id": pane_id, "agent": "devin"}

    if not rows:
        return {"available": False, "reason": "no_steps", "pane_id": pane_id, "agent": "devin"}

    completed: list[dict[str, Any]] = []
    current_turn_id = ""
    current_started_at: Any = None
    current_user_text = ""
    last_agent_text = ""
    open_turn = False
    worklog_parts: list[str] = []  # devin tool calls + interim narration for the open turn

    def _devin_turn(final_text: str, completed_at: Any) -> dict[str, Any]:
        turn = {
            "available": True,
            "pane_id": pane_id,
            "agent": "devin",
            "agent_session_id": session_id,
            "turn_id": str(current_turn_id),
            "turn_index": None,
            "complete": True,
            "complete_reason": "done",
            "started_at": current_started_at,
            "completed_at": completed_at,
            "user_text": current_user_text,
            "assistant_final_text": final_text,
        }
        if WORKLOG_ENABLED and worklog_parts:
            worklog = _join_worklog(worklog_parts)
            if worklog:
                turn["worklog_text"] = worklog
        return turn

    for node_id, chat_message_json, created_at in rows:
        try:
            msg = json.loads(chat_message_json)
        except (json.JSONDecodeError, TypeError):
            continue
        role = str(msg.get("role") or "")
        content = str(msg.get("content") or "")
        tool_calls = msg.get("tool_calls") or []
        timestamp = created_at

        if role == "user" and content.strip() and not is_internal_devin_user_text(content):
            if open_turn and current_user_text and last_agent_text:
                completed.append(_devin_turn(last_agent_text, timestamp))
            current_turn_id = str(node_id)
            current_started_at = timestamp
            current_user_text = sanitize_text(content)
            last_agent_text = ""
            open_turn = True
            worklog_parts = []
            continue

        if role == "assistant" and open_turn:
            if content.strip():
                last_agent_text = sanitize_text(content)
            if tool_calls:
                # Tool steps only (see extract_devin_turn): the message can become the
                # final reply when closed by the next prompt, so don't dup it here.
                if WORKLOG_ENABLED:
                    _accumulate_worklog(worklog_parts, [l for l in (devin_tool_call_line(c) for c in tool_calls) if l])
            elif content.strip():
                if current_user_text and last_agent_text:
                    completed.append(_devin_turn(last_agent_text, timestamp))
                    open_turn = False
            continue

    if completed:
        recent = completed[-RECENT_TURNS:]
        latest = dict(recent[-1])
        if open_turn:
            latest["has_open_turn"] = True
            latest["open_turn_id"] = current_turn_id
            if current_user_text:
                latest["open_user_text"] = current_user_text
            add_stream_fields(latest, last_agent_text, "devin")
        latest["recent_turns"] = recent
        return latest
    if open_turn:
        turn = {
            "available": True,
            "pane_id": pane_id,
            "agent": "devin",
            "agent_session_id": session_id,
            "complete": False,
            "turn_id": current_turn_id,
            "user_text": current_user_text,
            "assistant_final_text": "",
        }
        return add_stream_fields(turn, last_agent_text, "devin")
    return {
        "available": True,
        "pane_id": pane_id,
        "agent": "devin",
        "agent_session_id": session_id,
        "complete": False,
        "reason": "no_completed_turn",
    }


def claude_project_dir_for_cwd(cwd: str) -> Path | None:
    base = Path(os.getenv("CLAUDE_PROJECTS_DIR", str(Path.home() / ".claude/projects"))).expanduser()
    if not base.exists() or not cwd:
        return None
    encoded = str(Path(cwd)).replace("/", "-")
    candidate = base / encoded
    return candidate if candidate.exists() else None


def claude_candidate_paths_for_pane(pane: dict[str, Any]) -> list[Path]:
    cwd = str(pane.get("foreground_cwd") or pane.get("cwd") or "")
    project_dir = claude_project_dir_for_cwd(cwd)
    if not project_dir:
        return []
    paths = sorted(project_dir.glob("*.jsonl"), key=lambda p: p.stat().st_mtime, reverse=True)
    return paths[:CLAUDE_FALLBACK_MAX_FILES]


def normalize_for_match(value: str) -> str:
    return re.sub(r"\s+", " ", str(value or "").lower()).strip()


def match_chunks(value: str) -> list[str]:
    words = normalize_for_match(value).split()
    chunks: list[str] = []
    for size in (12, 20, 32):
        if len(words) >= size:
            chunks.append(" ".join(words[:size]))
            chunks.append(" ".join(words[-size:]))
    if len(words) >= 48:
        mid = len(words) // 2
        chunks.append(" ".join(words[max(0, mid - 10):mid + 10]))
    return [chunk for chunk in chunks if len(chunk) >= 40]


def turn_visible_match_score(turn: dict[str, Any], pane_text: str) -> int:
    visible = normalize_for_match(pane_text)
    if not visible:
        return 0
    score = 0
    for chunk in match_chunks(str(turn.get("assistant_final_text") or "")):
        if chunk in visible:
            score += 1
    user_text = str(turn.get("user_text") or "")
    if user_text and not is_internal_claude_user_text(user_text):
        for chunk in match_chunks(user_text)[:2]:
            if chunk in visible:
                score += 1
    return score


def pane_recent_text(pane_id: str) -> str:
    for source in ("recent-unwrapped", "visible"):
        text = run_real_herdr_text(
            ["pane", "read", pane_id, "--source", source, "--lines", str(CLAUDE_FALLBACK_READ_LINES), "--format", "text"]
        )
        if text.strip():
            return text
    return ""


def claude_sibling_count(pane: dict[str, Any]) -> int:
    """Number of claude panes sharing this pane's foreground cwd (including it).

    1 means this pane is the only claude session in its cwd, so the newest
    session file in that project dir is unambiguously this pane's — no need to
    scrape/match the visible pane. On any uncertainty we return 2 so the caller
    falls back to visible-text disambiguation.
    """
    cwd = str(pane.get("foreground_cwd") or pane.get("cwd") or "")
    if not cwd:
        return 2
    try:
        data = run_real_herdr_json(["pane", "list"])
    except (RuntimeError, json.JSONDecodeError, subprocess.TimeoutExpired, OSError):
        return 2
    panes = data.get("result", {}).get("panes")
    if not isinstance(panes, list):
        return 2
    count = 0
    for other in panes:
        if (
            isinstance(other, dict)
            and str(other.get("agent") or "").lower() == "claude"
            and str(other.get("foreground_cwd") or other.get("cwd") or "") == cwd
        ):
            count += 1
    return count or 1


def proc_starttime(pid: int) -> str | None:
    """Field 22 (starttime, in clock ticks) of /proc/<pid>/stat, or None.

    comm (field 2) can contain spaces and parentheses, so we split on the LAST
    ')' and index the remainder: field 22 is index 19 of what follows ") ".
    Used as a PID-reuse guard against Claude's pid->session mapping file.
    """
    try:
        with open(f"/proc/{pid}/stat", encoding="utf-8", errors="replace") as handle:
            data = handle.read()
        rest = data[data.rfind(")") + 2:]
        return rest.split()[19]
    except (OSError, IndexError, ValueError):
        return None


def proc_ppid(pid: int) -> int | None:
    """Parent PID (field 4 of /proc/<pid>/stat), or None.

    Same comm-aware parse as proc_starttime: state is index 0 after ") ",
    ppid is index 1. Used to walk up from a tool child to its claude parent.
    """
    try:
        with open(f"/proc/{pid}/stat", encoding="utf-8", errors="replace") as handle:
            data = handle.read()
        rest = data[data.rfind(")") + 2:]
        return int(rest.split()[1])
    except (OSError, IndexError, ValueError):
        return None


def proc_is_claude(pid: int) -> bool:
    """True if /proc/<pid>/comm or its argv0 names the claude binary.

    comm is truncated to 15 chars (fine for "claude") and reads "node" for a
    wrapper install, so we also check the argv0 basename as a fallback.
    """
    try:
        with open(f"/proc/{pid}/comm", encoding="utf-8", errors="replace") as handle:
            if handle.read().strip() == "claude":
                return True
    except OSError:
        pass
    try:
        with open(f"/proc/{pid}/cmdline", "rb") as handle:
            argv0 = handle.read().split(b"\x00", 1)[0].decode("utf-8", "replace")
    except OSError:
        return False
    return bool(argv0) and os.path.basename(argv0) == "claude"


def claude_pid_for_pane(pane_id: str) -> int | None:
    """The live claude PID for a pane via `herdr pane process-info`.

    Deterministic, in priority order: (1) a foreground process that IS claude;
    (2) the claude ancestor of a foreground tool child (claude stays the pane's
    process while a Bash/node child is the momentary foreground leaf); (3) the
    foreground process-group leader as a last resort. Whatever PID is returned
    is still validated against Claude's own pid->session map (procStart + cwd +
    pid) by the caller, so a wrong guess fails closed rather than mis-attributes.
    """
    try:
        data = run_real_herdr_json(["pane", "process-info", "--pane", pane_id])
        info = data.get("result", {}).get("process_info", {})
        procs = info.get("foreground_processes")
        leaf_pids: list[int] = []
        if isinstance(procs, list):
            for proc in procs:
                if not isinstance(proc, dict):
                    continue
                try:
                    pid = int(proc["pid"])
                except (KeyError, TypeError, ValueError):
                    continue  # one malformed entry must not abort resolution.
                argv = proc.get("argv")
                argv0 = str(argv[0]) if isinstance(argv, list) and argv else ""
                if str(proc.get("name") or "") == "claude" or argv0 == "claude":
                    return pid
                leaf_pids.append(pid)
        # Walk up from each foreground leaf to a claude parent (bounded).
        for leaf in leaf_pids:
            pid = leaf
            for _ in range(8):
                ppid = proc_ppid(pid)
                if ppid is None or ppid <= 1:
                    break
                if proc_is_claude(ppid):
                    return ppid
                pid = ppid
        pgid = info.get("foreground_process_group_id")
        if pgid is not None:
            return int(pgid)
    except (RuntimeError, json.JSONDecodeError, subprocess.TimeoutExpired, OSError, KeyError, TypeError, ValueError):
        return None
    return None


def claude_sessions_dir() -> Path:
    return Path(os.getenv("CLAUDE_SESSIONS_DIR", str(Path.home() / ".claude/sessions"))).expanduser()


def claude_session_record_for_pid(pid: int, expected_cwd: str = "") -> dict[str, Any] | None:
    """Read Claude's own ~/.claude/sessions/<PID>.json pid->sessionId record.

    This is the authoritative binding Codex exposes via agent_session.value but
    Claude does not surface to Herdr. Guarded against PID reuse (procStart must
    match /proc) and a definite cwd mismatch. Any broken hop returns None so the
    caller falls through to the existing heuristics — never worse than today.
    """
    try:
        record = json.loads((claude_sessions_dir() / f"{pid}.json").read_text(encoding="utf-8", errors="replace"))
    except (OSError, json.JSONDecodeError, ValueError):
        return None
    if not isinstance(record, dict):
        return None
    session_id = str(record.get("sessionId") or "")
    if not session_id:
        return None
    try:
        if int(record.get("pid")) != pid:
            return None  # map file is for a different PID (corrupt/misnamed).
    except (TypeError, ValueError):
        return None
    started = proc_starttime(pid)
    if started is not None and str(record.get("procStart")) != str(started):
        return None  # PID was recycled; this mapping file is stale.
    record_cwd = str(record.get("cwd") or "")
    if expected_cwd and record_cwd and record_cwd != expected_cwd:
        return None
    return {"sessionId": session_id, "cwd": record_cwd, "procStart": record.get("procStart")}


def resolve_claude_session_via_pid(pane: dict[str, Any], pane_id: str) -> tuple[Path | None, str]:
    """Deterministic pane -> PID -> sessionId -> session file. ('', None) on any miss."""
    expected_cwd = str(pane.get("foreground_cwd") or pane.get("cwd") or "")
    pid = claude_pid_for_pane(pane_id)
    if pid is None:
        return (None, "")
    record = claude_session_record_for_pid(pid, expected_cwd)
    if record is None:
        return (None, "")
    session_id = record["sessionId"]
    project_dir = claude_project_dir_for_cwd(expected_cwd)
    if project_dir:
        candidate = project_dir / f"{session_id}.jsonl"
        if candidate.exists():
            return (candidate, session_id)
    path = claude_session_path(session_id)
    if path is None or not path.exists():
        return (None, "")
    return (path, session_id)


def infer_claude_turn_from_visible_pane(pane: dict[str, Any], pane_id: str) -> dict[str, Any]:
    # PRIMARY: deterministic pane -> PID -> sessionId via Claude's own pid map.
    # Reads exactly one session file (no project-dir glob, no sibling pane-list
    # call, no visible-text scrape), which also removes the timeout class for
    # large project dirs. Falls through to the heuristics only on a broken hop.
    sess = pane.get("agent_session")
    herdr_sid = str(sess.get("value") or "") if isinstance(sess, dict) else ""
    pid_path, pid_sid = resolve_claude_session_via_pid(pane, pane_id)
    if pid_path is not None:
        turn = extract_claude_turn(pid_path, pane_id, pid_sid)
        turn["session_match_source"] = "pid_session_map"
        if herdr_sid and herdr_sid != pid_sid:
            # Herdr's cached agent_session.value went stale (e.g. /resume after a
            # 529); the live pid map wins. Surfaced for observability only.
            turn["herdr_session_id"] = herdr_sid
        return result_turn(turn)

    # SECONDARY: Herdr's cached agent_session.value. A definite id, but cached at
    # pane-bind and stale across a resume/restart, so it ranks below the live pid
    # map. Used only when the pid hop is broken AND the file still exists.
    if herdr_sid:
        path = claude_session_path(herdr_sid)
        if path is not None and path.exists():
            turn = extract_claude_turn(path, pane_id, herdr_sid)
            turn["session_match_source"] = "herdr_agent_session"
            return result_turn(turn)

    candidates: list[tuple[float, Path, dict[str, Any]]] = []
    for path in claude_candidate_paths_for_pane(pane):
        turn = extract_claude_turn(path, pane_id, path.stem)
        if turn.get("complete") is not True or not turn.get("assistant_final_text"):
            continue
        try:
            mtime = path.stat().st_mtime
        except OSError:
            continue
        candidates.append((mtime, path, turn))
    if not candidates:
        return unavailable("no_completed_claude_turn", pane_id=pane_id, agent="claude")
    candidates.sort(key=lambda item: item[0], reverse=True)

    if claude_sibling_count(pane) <= 1:
        # Only one claude session in this cwd: the most recently written session
        # file is this pane's. Reliable and avoids the slow/fragile pane scrape.
        _mtime, path, turn = candidates[0]
        turn["agent_session_id"] = path.stem
        turn["session_match_source"] = "exclusive_cwd_mtime"
        return result_turn(turn)

    # Shared cwd: disambiguate by matching the visible pane against each session.
    pane_text = pane_recent_text(pane_id)
    if not pane_text.strip():
        return unavailable("claude_pane_text_unavailable", pane_id=pane_id, agent="claude")
    matches: list[tuple[int, float, Path, dict[str, Any]]] = []
    for mtime, path, turn in candidates:
        score = turn_visible_match_score(turn, pane_text)
        if score:
            matches.append((score, mtime, path, turn))
    matches.sort(key=lambda item: (item[0], item[1]), reverse=True)
    if not matches or matches[0][0] < 2:
        return unavailable("no_unique_claude_session_match", pane_id=pane_id, agent="claude")
    if len(matches) > 1 and matches[0][0] < matches[1][0] + 2:
        return unavailable("ambiguous_claude_session_match", pane_id=pane_id, agent="claude")
    score, _mtime, path, turn = matches[0]
    turn["agent_session_id"] = path.stem
    turn["session_match_source"] = "pane_visible_match"
    turn["session_match_score"] = score
    return result_turn(turn)


def extract_codex_turn(path: Path, pane_id: str, session_id: str) -> dict[str, Any]:
    current_turn_id = ""
    current_started_at: Any = None
    current_user_text = ""
    last_assistant_text = ""
    completed: list[dict[str, Any]] = []
    open_turn = False
    worklog_parts: list[str] = []  # codex tool calls for the open turn

    with open_jsonl_tail(path, _codex_is_turn_start, _codex_is_turn_end) as handle:
        for raw in handle:
            try:
                event = json.loads(raw)
            except Exception:
                continue
            payload = event.get("payload") if isinstance(event.get("payload"), dict) else {}
            if event.get("type") == "event_msg" and payload.get("type") == "task_started":
                current_turn_id = str(payload.get("turn_id") or "")
                current_started_at = payload.get("started_at")
                current_user_text = ""
                last_assistant_text = ""
                open_turn = True
                worklog_parts = []
                continue
            if event.get("type") == "response_item" and payload.get("type") == "message":
                role = str(payload.get("role") or "")
                text = content_text(payload.get("content")).strip()
                if role == "user" and text and not is_internal_codex_user_text(text):
                    current_user_text = sanitize_text(text)
                elif role == "assistant" and text:
                    last_assistant_text = sanitize_text(text)
                continue
            # Tool calls between task_started and task_complete are the worklog. (codex
            # reasoning summaries are encrypted/empty, and the final reply is itself an
            # agent_message, so only the tool steps are surfaced here.)
            if (
                WORKLOG_ENABLED
                and open_turn
                and event.get("type") == "response_item"
                and payload.get("type") in ("function_call", "custom_tool_call")
            ):
                line = codex_worklog_line(payload)
                if line:
                    _accumulate_worklog(worklog_parts, [line])
                continue
            if event.get("type") == "event_msg" and payload.get("type") == "task_complete":
                final_text = sanitize_text(str(payload.get("last_agent_message") or "").strip())
                if not final_text:
                    final_text = last_assistant_text
                if final_text and current_user_text:
                    turn = {
                        "available": True,
                        "pane_id": pane_id,
                        "agent": "codex",
                        "agent_session_id": session_id,
                        "turn_id": str(payload.get("turn_id") or current_turn_id),
                        "turn_index": None,
                        "complete": True,
                        "complete_reason": "done",
                        "started_at": current_started_at,
                        "completed_at": payload.get("completed_at"),
                        "user_text": current_user_text,
                        "assistant_final_text": final_text,
                    }
                    if WORKLOG_ENABLED and worklog_parts:
                        worklog = _join_worklog(worklog_parts)
                        if worklog:
                            turn["worklog_text"] = worklog
                    completed.append(turn)
                open_turn = False
                continue
            if event.get("type") == "event_msg" and payload.get("type") == "turn_aborted":
                open_turn = False

    if completed:
        recent = completed[-RECENT_TURNS:]
        latest = dict(recent[-1])
        if open_turn:
            latest["has_open_turn"] = True
            latest["open_turn_id"] = current_turn_id
            if current_user_text:
                latest["open_user_text"] = current_user_text
            add_stream_fields(latest, last_assistant_text, "codex")
        latest["recent_turns"] = recent
        return latest
    if open_turn:
        turn = {
            "available": True,
            "pane_id": pane_id,
            "agent": "codex",
            "agent_session_id": session_id,
            "complete": False,
            "turn_id": current_turn_id,
            "user_text": current_user_text,
            "assistant_final_text": "",
        }
        return add_stream_fields(turn, last_assistant_text, "codex")
    return {
        "available": True,
        "pane_id": pane_id,
        "agent": "codex",
        "agent_session_id": session_id,
        "complete": False,
        "reason": "no_completed_turn",
    }


def extract_claude_turn(path: Path, pane_id: str, session_id: str) -> dict[str, Any]:
    pending_user_text = ""
    pending_user_uuid = ""
    consumed_user_uuid = ""  # uuid of the last real prompt already paired to an end_turn
    completed: list[dict[str, Any]] = []
    incomplete_user = False
    pending_api_error: dict[str, Any] | None = None  # set if an API error is the latest unresolved event
    latest_stream_text = ""
    latest_stream_updated_at: Any = ""
    worklog_parts: list[str] = []  # intermediate tool_use + text for the open turn

    with open_jsonl_tail(path, _claude_is_turn_start, _claude_is_turn_end) as handle:
        for raw in handle:
            try:
                event = json.loads(raw)
            except Exception:
                continue
            event_type = str(event.get("type") or "")
            msg = event.get("message") if isinstance(event.get("message"), dict) else {}
            if event_type == "user":
                text = content_text(msg.get("content")).strip()
                uuid = str(event.get("uuid") or "")
                if text and not is_internal_claude_user_text(text):
                    # A real human prompt: it opens a new turn boundary. It also
                    # supersedes any prior API error (the owner has responded /
                    # is driving it forward), so don't keep warning about it.
                    pending_user_text = sanitize_text(text)
                    pending_user_uuid = uuid
                    incomplete_user = True
                    pending_api_error = None
                    latest_stream_text = ""
                    latest_stream_updated_at = ""
                    worklog_parts = []
                else:
                    # Internal (<task-notification> etc.) or empty user event. It
                    # still marks a turn boundary, but must NOT destroy a real
                    # prompt that has not yet been answered — otherwise the reply
                    # to that prompt would post with an empty "You asked" block.
                    real_prompt_armed = bool(pending_user_text) and pending_user_uuid != consumed_user_uuid
                    if not real_prompt_armed:
                        pending_user_text = ""
                        pending_user_uuid = uuid
                        incomplete_user = True
                        latest_stream_text = ""
                        latest_stream_updated_at = ""
                        worklog_parts = []
                continue
            if event_type == "assistant":
                if event.get("isApiErrorMessage") is True:
                    # Claude logs the API error (retries exhausted) as an
                    # assistant message; the turn stops here. Record it as the
                    # latest unresolved state — cleared below when a real
                    # completion supersedes it (recovery).
                    pending_api_error = {
                        "id": str(event.get("uuid") or ""),
                        "code": sanitize_text(str(event.get("error") or ""), 80),
                        "text": sanitize_text(content_text(msg.get("content")).strip(), 600),
                        "at": event.get("timestamp"),
                    }
                    continue
                content = msg.get("content")
                text = content_text(content).strip()
                if text and msg.get("stop_reason") == "end_turn" and pending_user_uuid:
                    turn = {
                        "available": True,
                        "pane_id": pane_id,
                        "agent": "claude",
                        "agent_session_id": session_id,
                        "turn_id": str(event.get("uuid") or ""),
                        "complete": True,
                        "complete_reason": "done",
                        "started_at": None,
                        "completed_at": event.get("timestamp"),
                        "user_text": pending_user_text,
                        "assistant_final_text": sanitize_text(text),
                        "_prompt_uuid": pending_user_uuid,
                    }
                    if WORKLOG_ENABLED and worklog_parts:
                        worklog = _join_worklog(worklog_parts)
                        if worklog:
                            turn["worklog_text"] = worklog
                    # Coalesce consecutive end_turns under the same prompt (the
                    # last non-empty assistant message wins) rather than emitting
                    # a duplicate turn for the same prompt.
                    if completed and completed[-1].get("_prompt_uuid") == pending_user_uuid:
                        completed[-1] = turn
                    else:
                        completed.append(turn)
                    consumed_user_uuid = pending_user_uuid
                    incomplete_user = False
                    pending_api_error = None  # a real completion supersedes any prior API error
                    latest_stream_text = ""
                    latest_stream_updated_at = ""
                elif pending_user_text:
                    # Intermediate step (tool_use and/or interim text) inside an open
                    # prompt. Gated on pending_user_text (NOT incomplete_user) so steps
                    # between coalesced consecutive end_turns of the same prompt are
                    # captured too; tool_use-only messages have no text but still count.
                    if WORKLOG_ENABLED:
                        worklog_parts.extend(claude_worklog_lines(content))
                        if len(worklog_parts) > WORKLOG_MAX_LINES:
                            del worklog_parts[: len(worklog_parts) - WORKLOG_MAX_LINES]
                    if text and incomplete_user:
                        latest_stream_text = sanitize_text(text)
                        latest_stream_updated_at = event.get("timestamp") or ""

    if completed:
        recent = [{k: v for k, v in t.items() if k != "_prompt_uuid"} for t in completed[-RECENT_TURNS:]]
        latest = dict(recent[-1])
        if incomplete_user:
            latest["has_open_turn"] = True
            latest["open_turn_id"] = pending_user_uuid
            if pending_user_text:
                latest["open_user_text"] = pending_user_text
            add_stream_fields(latest, latest_stream_text, "claude", latest_stream_updated_at)
        latest["recent_turns"] = recent
        if pending_api_error:
            latest["api_error"] = pending_api_error
        return latest
    if incomplete_user:
        result = {
            "available": True,
            "pane_id": pane_id,
            "agent": "claude",
            "agent_session_id": session_id,
            "complete": False,
            "turn_id": pending_user_uuid,
            "user_text": pending_user_text,
            "assistant_final_text": "",
        }
        if pending_api_error:
            result["api_error"] = pending_api_error
        add_stream_fields(result, latest_stream_text, "claude", latest_stream_updated_at)
        return result
    result = {
        "available": True,
        "pane_id": pane_id,
        "agent": "claude",
        "agent_session_id": session_id,
        "complete": False,
        "reason": "no_completed_turn",
    }
    if pending_api_error:
        result["api_error"] = pending_api_error
    return result


def is_internal_devin_user_text(text: str) -> bool:
    stripped = text.lstrip()
    return stripped.startswith((
        "<system_info>",
        "<rules",
        "<available_skills>",
        "<user-prompt-submit-hook>",
        "<subagent_completion_notification>",
        "<system_guidance>",
    ))


def extract_devin_turn(path: Path, pane_id: str, session_id: str) -> dict[str, Any]:
    """Extract the latest turn from a Devin CLI transcript (ATIF JSON format).

    Devin transcripts are a single JSON object with a ``steps`` array.  Each
    step has ``source`` (system/user/agent), ``message`` (text), and optional
    ``tool_calls``.  A turn is a user message followed by agent steps; the
    turn is "complete" when the agent emits a step with non-empty ``message``
    text and no tool calls (the final summary), or when a new user prompt
    arrives (marking the previous turn complete by implication).
    """
    try:
        data = json.loads(path.read_text(encoding="utf-8", errors="replace"))
    except (OSError, json.JSONDecodeError):
        return {"available": False, "reason": "transcript_read_error", "pane_id": pane_id, "agent": "devin"}
    steps = data.get("steps") if isinstance(data.get("steps"), list) else []
    if not steps:
        return {"available": False, "reason": "no_steps", "pane_id": pane_id, "agent": "devin"}

    completed: list[dict[str, Any]] = []
    current_turn_id = ""
    current_started_at: Any = None
    current_user_text = ""
    last_agent_text = ""
    open_turn = False
    worklog_parts: list[str] = []  # devin tool calls + interim narration for the open turn

    def _devin_turn(final_text: str, completed_at: Any) -> dict[str, Any]:
        turn = {
            "available": True,
            "pane_id": pane_id,
            "agent": "devin",
            "agent_session_id": session_id,
            "turn_id": str(current_turn_id),
            "turn_index": None,
            "complete": True,
            "complete_reason": "done",
            "started_at": current_started_at,
            "completed_at": completed_at,
            "user_text": current_user_text,
            "assistant_final_text": final_text,
        }
        if WORKLOG_ENABLED and worklog_parts:
            worklog = _join_worklog(worklog_parts)
            if worklog:
                turn["worklog_text"] = worklog
        return turn

    for step in steps:
        source = str(step.get("source") or "")
        msg = str(step.get("message") or "")
        step_id = step.get("step_id")
        timestamp = step.get("timestamp")
        tool_calls = step.get("tool_calls") or []

        if source == "user" and msg.strip() and not is_internal_devin_user_text(msg):
            # New user prompt: close out any open turn
            if open_turn and current_user_text and last_agent_text:
                completed.append(_devin_turn(last_agent_text, timestamp))
            current_turn_id = str(step_id)
            current_started_at = timestamp
            current_user_text = sanitize_text(msg)
            last_agent_text = ""
            open_turn = True
            worklog_parts = []
            continue

        if source == "agent" and open_turn:
            if msg.strip():
                last_agent_text = sanitize_text(msg)
            if tool_calls:
                # Tool-using step: record each tool call. The step's message is NOT added —
                # it also becomes last_agent_text and, when the turn is closed by the next
                # prompt, the final reply, which would duplicate the response into the
                # worklog. (Mirrors codex: tool steps only.)
                if WORKLOG_ENABLED:
                    _accumulate_worklog(worklog_parts, [l for l in (devin_tool_call_line(c) for c in tool_calls) if l])
            elif msg.strip():
                # text with no tool calls -> final response
                if current_user_text and last_agent_text:
                    completed.append(_devin_turn(last_agent_text, timestamp))
                    open_turn = False
            continue

    if completed:
        recent = completed[-RECENT_TURNS:]
        latest = dict(recent[-1])
        if open_turn:
            latest["has_open_turn"] = True
            latest["open_turn_id"] = current_turn_id
            if current_user_text:
                latest["open_user_text"] = current_user_text
            add_stream_fields(latest, last_agent_text, "devin")
        latest["recent_turns"] = recent
        return latest
    if open_turn:
        turn = {
            "available": True,
            "pane_id": pane_id,
            "agent": "devin",
            "agent_session_id": session_id,
            "complete": False,
            "turn_id": current_turn_id,
            "user_text": current_user_text,
            "assistant_final_text": "",
        }
        return add_stream_fields(turn, last_agent_text, "devin")
    return {
        "available": True,
        "pane_id": pane_id,
        "agent": "devin",
        "agent_session_id": session_id,
        "complete": False,
        "reason": "no_completed_turn",
    }


def pane_from_list(pane_id: str) -> dict[str, Any] | None:
    try:
        data = run_real_herdr_json(["pane", "list"])
    except (RuntimeError, json.JSONDecodeError, subprocess.TimeoutExpired, OSError):
        return {"_adapter_error": "herdr_list_failed"}
    panes = data.get("result", {}).get("panes")
    if not isinstance(panes, list):
        return None
    for pane in panes:
        if isinstance(pane, dict) and str(pane.get("pane_id") or "") == pane_id:
            return pane
    return None


def pane_turn(pane_id: str) -> dict[str, Any]:
    pane = pane_from_list(pane_id)
    if not pane:
        return unavailable("pane_not_found")
    if pane.get("_adapter_error"):
        return unavailable(str(pane.get("_adapter_error")))
    agent = str(pane.get("agent") or "").lower()
    # Claude resolves through the deterministic pid map FIRST, regardless of
    # whether Herdr supplied agent_session.value: that value is cached at
    # pane-bind and goes stale across a /resume or restart (e.g. after an API
    # 529), so trusting it here would read a dead session and stop delivery.
    if agent == "claude":
        return infer_claude_turn_from_visible_pane(pane, pane_id)
    session = pane.get("agent_session") if isinstance(pane.get("agent_session"), dict) else {}
    session_id = str(session.get("value") or "")
    if not session_id:
        if agent == "devin":
            session_id = devin_resolve_session_id(pane)
            if not session_id:
                return unavailable("no_agent_session_id", pane_id=pane_id, agent=agent)
        else:
            return unavailable("no_agent_session_id", pane_id=pane_id, agent=agent or None)

    if agent == "codex":
        path = codex_session_path(session_id)
        if not path:
            return unavailable("session_file_not_found", pane_id=pane_id, agent=agent, agent_session_id=session_id)
        return result_turn(extract_codex_turn(path, pane_id, session_id))

    if agent == "devin":
        path = devin_session_path(session_id)
        if path:
            return result_turn(extract_devin_turn(path, pane_id, session_id))
        db_path = devin_sessions_db()
        if db_path.exists() and devin_db_has_session(db_path, session_id):
            return result_turn(extract_devin_turn_from_db(db_path, pane_id, session_id))
        return unavailable("session_file_not_found", pane_id=pane_id, agent=agent, agent_session_id=session_id)

    return unavailable("unsupported_agent", pane_id=pane_id, agent=agent or None)


def main() -> int:
    args = sys.argv[1:]
    if len(args) >= 3 and args[0] == "pane" and args[1] == "turn":
        pane_id = args[2]
        print(json.dumps(pane_turn(pane_id), separators=(",", ":")))
        return 0
    exec_real_herdr()
    return 127


if __name__ == "__main__":
    raise SystemExit(main())
