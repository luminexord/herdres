#!/usr/bin/env python3
"""herdres Claude Code hook — record a PENDING AskUserQuestion / ExitPlanMode prompt so herdres
can mirror it to Telegram as tappable buttons (issue #36).

Claude Code does NOT persist a pending tool_use to the session transcript until it is answered,
but it DOES fire PreToolUse for these tools the moment the prompt is shown (verified). This hook
captures that PreToolUse payload to a per-session file and clears it on PostToolUse / SessionEnd.

Passive recorder: reads the hook JSON on stdin, writes/removes one small file, emits nothing, and
fails closed (any error -> exit 0) so it can never block or alter a Claude turn.

Installed (by herdres) for PreToolUse + PostToolUse (matcher AskUserQuestion|ExitPlanMode) and
SessionEnd (cleanup). The herdres turn adapter reads the file keyed by session_id.
"""
import hashlib
import json
import os
import sys
import time
from pathlib import Path

DECISION_TOOLS = {"askuserquestion", "exitplanmode"}


def pending_dir() -> Path:
    base = os.environ.get("HERDRES_PENDING_DIR")
    return Path(base) if base else (Path.home() / ".local" / "share" / "herdres" / "pending")


def _safe(value: str) -> str:
    cleaned = "".join(c for c in str(value) if c.isalnum() or c in "-_.")
    return cleaned[:120] or "session"


def main() -> None:
    raw = sys.stdin.read()
    payload = json.loads(raw) if raw.strip() else {}
    if not isinstance(payload, dict):
        return
    session_id = str(payload.get("session_id") or "").strip()
    if not session_id:
        return
    event = str(payload.get("hook_event_name") or "")
    path = pending_dir() / f"{_safe(session_id)}.json"

    if event == "PreToolUse":
        name = str(payload.get("tool_name") or "").strip()
        if name.lower() not in DECISION_TOOLS:
            return
        tool_input = payload.get("tool_input") if isinstance(payload.get("tool_input"), dict) else {}
        # PreToolUse carries no tool_use_id, so synthesize a STABLE id from the prompt content:
        # re-fires for the same prompt produce the same decision_id (no duplicate buttons).
        digest = hashlib.sha256((session_id + json.dumps(tool_input, sort_keys=True)).encode("utf-8")).hexdigest()[:16]
        record = {
            "tool_use_id": f"hookdec-{digest}",
            "name": name,
            "input": tool_input,
            "session_id": session_id,
            "ts": time.time(),
        }
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.parent / f"{path.name}.{os.getpid()}.tmp"
        tmp.write_text(json.dumps(record), encoding="utf-8")
        os.replace(tmp, path)
    elif event in ("PostToolUse", "SessionEnd", "Stop"):
        try:
            path.unlink()
        except OSError:
            pass


if __name__ == "__main__":
    try:
        main()
    except Exception:
        pass
    sys.exit(0)
