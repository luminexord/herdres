#!/usr/bin/env python3
"""Local Herdr wrapper that adds `pane turn` from agent session logs.

All non-`pane turn` commands are delegated to the real Herdr binary. This keeps
the integration upgrade-safe: Herdr itself is never patched.
"""

from __future__ import annotations

import json
import hashlib
import os
import re
import subprocess
import sys
from pathlib import Path
from typing import Any


DEFAULT_REAL_HERDR = "herdr"
MAX_TEXT_CHARS = int(os.getenv("HERDRES_TURN_ADAPTER_MAX_TEXT_CHARS", "12000"))
CLAUDE_FALLBACK_MAX_FILES = int(os.getenv("HERDRES_CLAUDE_FALLBACK_MAX_FILES", "24"))
CLAUDE_FALLBACK_READ_LINES = int(os.getenv("HERDRES_CLAUDE_FALLBACK_READ_LINES", "160"))
# How many recent completed turns to expose so the bridge can catch up on a
# burst of completions (e.g. a reply immediately followed by an auto-pursued
# turn) instead of collapsing to only the newest one.
RECENT_TURNS = int(os.getenv("HERDRES_RECENT_TURNS", "12"))
DEVIN_SESSION_START_SKEW_SECONDS = int(os.getenv("HERDRES_DEVIN_SESSION_START_SKEW", "30"))


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
                        if (transcripts_dir / f"{sid}.json").exists():
                            return sid
                        if devin_db_has_session(db_path, sid):
                            return sid
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
                completed.append({
                    "available": True,
                    "pane_id": pane_id,
                    "agent": "devin",
                    "agent_session_id": session_id,
                    "turn_id": str(current_turn_id),
                    "turn_index": None,
                    "complete": True,
                    "complete_reason": "done",
                    "started_at": current_started_at,
                    "completed_at": timestamp,
                    "user_text": current_user_text,
                    "assistant_final_text": last_agent_text,
                })
            current_turn_id = str(node_id)
            current_started_at = timestamp
            current_user_text = sanitize_text(content)
            last_agent_text = ""
            open_turn = True
            continue

        if role == "assistant" and open_turn:
            if content.strip():
                last_agent_text = sanitize_text(content)
            if content.strip() and not tool_calls:
                if current_user_text and last_agent_text:
                    completed.append({
                        "available": True,
                        "pane_id": pane_id,
                        "agent": "devin",
                        "agent_session_id": session_id,
                        "turn_id": str(current_turn_id),
                        "turn_index": None,
                        "complete": True,
                        "complete_reason": "done",
                        "started_at": current_started_at,
                        "completed_at": timestamp,
                        "user_text": current_user_text,
                        "assistant_final_text": last_agent_text,
                    })
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


def claude_pid_for_pane(pane_id: str) -> int | None:
    """The live claude PID for a pane via `herdr pane process-info`.

    Deterministic: Herdr reports the pane's foreground process. claude is its
    own process-group leader, so foreground_process_group_id is a safe fallback
    when a child (bash/node) is momentarily the named foreground process.
    """
    try:
        data = run_real_herdr_json(["pane", "process-info", "--pane", pane_id])
        info = data.get("result", {}).get("process_info", {})
        procs = info.get("foreground_processes")
        if isinstance(procs, list):
            for proc in procs:
                if not isinstance(proc, dict):
                    continue
                argv = proc.get("argv")
                argv0 = str(argv[0]) if isinstance(argv, list) and argv else ""
                if str(proc.get("name") or "") == "claude" or argv0 == "claude":
                    return int(proc["pid"])
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
    pid_path, pid_sid = resolve_claude_session_via_pid(pane, pane_id)
    if pid_path is not None:
        turn = extract_claude_turn(pid_path, pane_id, pid_sid)
        turn["session_match_source"] = "pid_session_map"
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

    with path.open(encoding="utf-8", errors="replace") as handle:
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
                continue
            if event.get("type") == "response_item" and payload.get("type") == "message":
                role = str(payload.get("role") or "")
                text = content_text(payload.get("content")).strip()
                if role == "user" and text and not is_internal_codex_user_text(text):
                    current_user_text = sanitize_text(text)
                elif role == "assistant" and text:
                    last_assistant_text = sanitize_text(text)
                continue
            if event.get("type") == "event_msg" and payload.get("type") == "task_complete":
                final_text = sanitize_text(str(payload.get("last_agent_message") or "").strip())
                if not final_text:
                    final_text = last_assistant_text
                if final_text and current_user_text:
                    completed.append({
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
                    })
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

    with path.open(encoding="utf-8", errors="replace") as handle:
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
                text = content_text(msg.get("content")).strip()
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
                elif text and incomplete_user and pending_user_text:
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

    for step in steps:
        source = str(step.get("source") or "")
        msg = str(step.get("message") or "")
        step_id = step.get("step_id")
        timestamp = step.get("timestamp")
        tool_calls = step.get("tool_calls") or []

        if source == "user" and msg.strip() and not is_internal_devin_user_text(msg):
            # New user prompt: close out any open turn
            if open_turn and current_user_text and last_agent_text:
                completed.append({
                    "available": True,
                    "pane_id": pane_id,
                    "agent": "devin",
                    "agent_session_id": session_id,
                    "turn_id": str(current_turn_id),
                    "turn_index": None,
                    "complete": True,
                    "complete_reason": "done",
                    "started_at": current_started_at,
                    "completed_at": timestamp,
                    "user_text": current_user_text,
                    "assistant_final_text": last_agent_text,
                })
            current_turn_id = str(step_id)
            current_started_at = timestamp
            current_user_text = sanitize_text(msg)
            last_agent_text = ""
            open_turn = True
            continue

        if source == "agent" and open_turn:
            if msg.strip():
                last_agent_text = sanitize_text(msg)
            # If the agent step has text but no tool calls, it's likely a final response
            if msg.strip() and not tool_calls:
                if current_user_text and last_agent_text:
                    completed.append({
                        "available": True,
                        "pane_id": pane_id,
                        "agent": "devin",
                        "agent_session_id": session_id,
                        "turn_id": str(current_turn_id),
                        "turn_index": None,
                        "complete": True,
                        "complete_reason": "done",
                        "started_at": current_started_at,
                        "completed_at": timestamp,
                        "user_text": current_user_text,
                        "assistant_final_text": last_agent_text,
                    })
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
    session = pane.get("agent_session") if isinstance(pane.get("agent_session"), dict) else {}
    session_id = str(session.get("value") or "")
    if not session_id:
        if agent == "claude":
            return infer_claude_turn_from_visible_pane(pane, pane_id)
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

    if agent == "claude":
        path = claude_session_path(session_id)
        if not path:
            return unavailable("session_file_not_found", pane_id=pane_id, agent=agent, agent_session_id=session_id)
        return result_turn(extract_claude_turn(path, pane_id, session_id))

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
