#!/usr/bin/env python3
"""Local Herdr wrapper that adds `pane turn` from agent session logs.

All non-`pane turn` commands are delegated to the real Herdr binary. This keeps
the integration upgrade-safe: Herdr itself is never patched.
"""

from __future__ import annotations

import json
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
        "<permissions instructions>",
        "<collaboration_mode>",
        "<skills_instructions>",
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


def infer_claude_turn_from_visible_pane(pane: dict[str, Any], pane_id: str) -> dict[str, Any]:
    pane_text = pane_recent_text(pane_id)
    if not pane_text.strip():
        return unavailable("claude_pane_text_unavailable", pane_id=pane_id, agent="claude")
    matches: list[tuple[int, float, Path, dict[str, Any]]] = []
    for path in claude_candidate_paths_for_pane(pane):
        turn = extract_claude_turn(path, pane_id, path.stem)
        if turn.get("complete") is not True or not turn.get("assistant_final_text"):
            continue
        score = turn_visible_match_score(turn, pane_text)
        if score:
            matches.append((score, path.stat().st_mtime, path, turn))
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
    latest_complete: dict[str, Any] | None = None
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
                    latest_complete = {
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
                open_turn = False
                continue
            if event.get("type") == "event_msg" and payload.get("type") == "turn_aborted":
                open_turn = False

    if open_turn and latest_complete:
        latest_complete["has_open_turn"] = True
        latest_complete["open_turn_id"] = current_turn_id
        return latest_complete
    if open_turn:
        return {
            "available": True,
            "pane_id": pane_id,
            "agent": "codex",
            "agent_session_id": session_id,
            "complete": False,
            "turn_id": current_turn_id,
            "user_text": current_user_text,
            "assistant_final_text": "",
        }
    if latest_complete:
        return latest_complete
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
    latest_complete: dict[str, Any] | None = None
    incomplete_user = False

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
                if text:
                    pending_user_text = "" if is_internal_claude_user_text(text) else sanitize_text(text)
                    pending_user_uuid = str(event.get("uuid") or "")
                    incomplete_user = True
                continue
            if event_type == "assistant":
                text = content_text(msg.get("content")).strip()
                if text and msg.get("stop_reason") == "end_turn" and pending_user_uuid:
                    latest_complete = {
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
                    }
                    incomplete_user = False

    if latest_complete:
        if incomplete_user:
            latest_complete["has_open_turn"] = True
            latest_complete["open_turn_id"] = pending_user_uuid
        return latest_complete
    if incomplete_user:
        return {
            "available": True,
            "pane_id": pane_id,
            "agent": "claude",
            "agent_session_id": session_id,
            "complete": False,
            "turn_id": pending_user_uuid,
            "user_text": pending_user_text,
            "assistant_final_text": "",
        }
    return {
        "available": True,
        "pane_id": pane_id,
        "agent": "claude",
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
