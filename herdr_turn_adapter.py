#!/usr/bin/env python3
"""Local Herdr wrapper that adds `pane turn` from agent session logs.

All non-`pane turn` commands are delegated to the real Herdr binary. This keeps
the integration upgrade-safe: Herdr itself is never patched.
"""

from __future__ import annotations

import calendar
import contextlib
import json
import hashlib
import os
import re
import subprocess
import sys
import time
import unicodedata
from datetime import datetime, timezone
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

# Issue #36: surface Claude Code AskUserQuestion / ExitPlanMode prompts as structured
# pending_decision/pending_interaction turns, so herdres renders tappable buttons instead of
# a read-only visible-screen prompt. Toggle off with HERDRES_TURN_ADAPTER_DECISIONS=0.
DECISIONS_ENABLED = os.getenv("HERDRES_TURN_ADAPTER_DECISIONS", "1").strip().lower() not in (
    "0",
    "false",
    "no",
    "off",
)
DECISION_TOOL_NAMES = {"askuserquestion", "exitplanmode"}
# ExitPlanMode is a TUI approve/reject dialog (not a freeform-text prompt), so the text sent
# to the pane when the owner taps "Approve & proceed" is a tunable knob validated by the e2e.
# "Keep planning / revise" uses an empty send_text -> herdres opens a ForceReply so the owner
# types revision feedback that is sent to the pane.
PLAN_APPROVE_SEND_TEXT = os.getenv("HERDRES_PLAN_APPROVE_SEND_TEXT", "1")
PLAN_REVISE_SEND_TEXT = os.getenv("HERDRES_PLAN_REVISE_SEND_TEXT", "")
# A pending AskUserQuestion/ExitPlanMode is recorded by the herdres Claude hook
# (herdres_decision_hook.py, PreToolUse) to a per-session file here — the transcript never
# contains it while pending. Cleared by the hook on PostToolUse/SessionEnd; TTL bounds an
# abandoned file (missed clear / crash).
PENDING_DECISION_TTL_SECONDS = int(os.getenv("HERDRES_PENDING_TTL_SECONDS", "3600"))

# Kimi Code 0.26+ stores authoritative session events below a workspace-scoped
# directory. Resolution is deliberately bounded and requires a unique live Kimi
# pane for the workspace; mutable newest-file selection across workspaces is not
# allowed.
KIMI_WORKSPACE_MAX_ENTRIES = 4096
KIMI_SESSION_MAX_ENTRIES = 256
KIMI_STATE_MAX_BYTES = 64 * 1024
_KIMI_SESSION_RE = re.compile(
    r"^session_([0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12})$"
)

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


PANE_LIST_CACHE = os.getenv(
    "HERDR_ADAPTER_PANE_LIST_CACHE",
    os.path.expanduser("~/.local/share/herdres/turn-adapter-pane-list.json"),
)


def _pane_list_ttl() -> float:
    try:
        return max(0.0, float(os.getenv("HERDR_ADAPTER_PANE_LIST_TTL", "8") or "8"))
    except ValueError:
        return 8.0


def cached_pane_list_json() -> dict[str, Any]:
    """`herdr pane list` behind a short-TTL file cache. Tendwire captures turns for ~15 panes
    concurrently each cycle and every capture used to run its own pane list (twice); under load the
    herdr CLI takes seconds per call, so the fan-out snowballed into a storm that wedged the tendwire
    daemon. All concurrent captures can share one listing: panes don't change within a few seconds.
    Set HERDR_ADAPTER_PANE_LIST_TTL=0 to disable. Fail-open: any cache error falls back to the CLI."""
    ttl = _pane_list_ttl()
    path = PANE_LIST_CACHE
    if ttl > 0:
        try:
            st = os.stat(path)
            if (time.time() - st.st_mtime) <= ttl:
                with open(path, "r", encoding="utf-8") as fh:
                    data = json.load(fh)
                if isinstance(data, dict):
                    return data
        except (OSError, ValueError):
            pass
    data = run_real_herdr_json(["pane", "list"])
    if ttl > 0:
        try:
            os.makedirs(os.path.dirname(path), exist_ok=True)
            tmp = f"{path}.{os.getpid()}.tmp"
            with open(tmp, "w", encoding="utf-8") as fh:
                json.dump(data, fh)
            os.replace(tmp, path)
        except (OSError, ValueError):
            pass
    return data


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


def sanitize_canonical_text(value: str) -> str:
    """Sanitize lossless prompt/final content without applying a presentation bound."""
    return str(value or "").replace("\x00", "")


def sanitize_bounded_text(value: str, max_chars: int = MAX_TEXT_CHARS) -> str:
    """Sanitize text for bounded transient/status fields."""
    text = sanitize_canonical_text(value)
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
    stream_text = sanitize_bounded_text(str(text or "").strip())
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
    label = sanitize_bounded_text(str(name or "tool").strip(), 40) or "tool"
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
            brief = sanitize_bounded_text(brief, WORKLOG_LINE_MAX_CHARS)
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
    label = sanitize_bounded_text(str(name or "tool").strip(), 40) or "tool"
    if isinstance(arguments, list):
        joined = " ".join(str(c) for c in arguments if isinstance(c, (str, int, float))).strip()
        return f"{label} {sanitize_bounded_text(joined.splitlines()[0], WORKLOG_LINE_MAX_CHARS)}".strip() if joined else label
    if isinstance(arguments, str) and arguments.strip():
        return f"{label} {sanitize_bounded_text(arguments.strip().splitlines()[0], WORKLOG_LINE_MAX_CHARS)}".strip()
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
            parts.append(sanitize_bounded_text(cleaned, WORKLOG_LINE_MAX_CHARS))
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
            lines.append(sanitize_bounded_text(cleaned, WORKLOG_LINE_MAX_CHARS))
    return lines


def pending_decision_dir() -> Path:
    base = os.environ.get("HERDRES_PENDING_DIR")
    return Path(base) if base else (Path.home() / ".local" / "share" / "herdres" / "pending")


def _safe_session_id(session_id: str) -> str:
    """Per-session pending filename stem. MUST stay byte-identical to herdres_decision_hook.py's
    `_safe` — the hook writes the file and this consumer reads it; any drift silently breaks the
    button path. tests/test_decision_hook.py pins the two together with a contract test."""
    cleaned = "".join(c for c in str(session_id) if c.isalnum() or c in "-_.")
    return cleaned[:120] or "session"


def read_pending_decision(session_id: str) -> dict[str, Any] | None:
    """The pending AskUserQuestion/ExitPlanMode for this session, as recorded by the herdres
    Claude hook (issue #36). None if absent, malformed, or stale (TTL)."""
    sid = str(session_id or "").strip()
    if not sid:
        return None
    path = pending_decision_dir() / f"{_safe_session_id(sid)}.json"
    try:
        if not path.exists():
            return None
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError, ValueError):
        return None
    if not isinstance(data, dict) or not isinstance(data.get("input"), dict):
        return None
    if str(data.get("name") or "").strip().lower() not in DECISION_TOOL_NAMES:
        return None  # only the two decision tools — ignore a stray/foreign file
    try:
        if PENDING_DECISION_TTL_SECONDS > 0 and (time.time() - float(data.get("ts") or 0)) > PENDING_DECISION_TTL_SECONDS:
            return None  # abandoned (missed clear / crash) — bounded by TTL
    except (TypeError, ValueError):
        pass
    return data


def claude_decision_turn_fields(tool: dict[str, Any]) -> dict[str, Any] | None:
    """Map a pending AskUserQuestion / ExitPlanMode tool_use to the turn fields herdres needs
    to render tappable buttons (pending_decision) or a read-only form (pending_interaction).
    Returns the dict to merge onto the open turn, or None to fall back to normal handling."""
    name = str(tool.get("name") or "").strip().lower()
    tool_id = str(tool.get("tool_use_id") or "")
    if not tool_id:
        return None
    tool_input = tool.get("input") if isinstance(tool.get("input"), dict) else {}

    if name == "exitplanmode":
        fields: dict[str, Any] = {
            "pending_decision": {
                "decision_id": tool_id,
                "prompt": "Approve this plan?",
                "mode": "buttons",
                "options": [
                    {"id": "approve", "label": "✅ Approve & proceed", "send_text": PLAN_APPROVE_SEND_TEXT},
                    {"id": "revise", "label": "✍️ Keep planning / revise", "send_text": PLAN_REVISE_SEND_TEXT},
                ],
            }
        }
        plan = sanitize_canonical_text(str(tool_input.get("plan") or "")).strip()
        if plan:
            fields["assistant_final_text"] = plan
        return fields

    if name == "askuserquestion":
        questions = tool_input.get("questions")
        if not isinstance(questions, list) or not questions:
            return None
        first = questions[0] if isinstance(questions[0], dict) else {}
        single = len(questions) == 1 and not bool(first.get("multiSelect"))
        if single:
            options: list[dict[str, str]] = []
            raw_opts = first.get("options") if isinstance(first.get("options"), list) else []
            for opt in raw_opts[:11]:
                label = sanitize_bounded_text(str((opt.get("label") or opt.get("text") or opt.get("title") or "") if isinstance(opt, dict) else (opt or "")), 180).strip()
                if label:
                    options.append({"id": str(len(options) + 1), "label": label, "send_text": label})
            if not options:
                return None
            options.append({"id": "custom", "label": "✍️ Write a different answer", "send_text": ""})
            header = sanitize_bounded_text(str(first.get("header") or ""), 80).strip()
            question = sanitize_bounded_text(str(first.get("question") or "Choose an option."), 1200).strip() or "Choose an option."
            prompt = f"{header}: {question}" if header and header.lower() not in question.lower() else question
            fields = {
                "pending_decision": {
                    "decision_id": tool_id,
                    "prompt": prompt,
                    "mode": "buttons",
                    "options": options,
                }
            }
            # The hook's PreToolUse payload carries only the structured tool_input, not the
            # assistant's surrounding prose, so there is no preamble to attach here; the question
            # text itself is the card body.
            return fields

        # Multi-question or multi-select -> read-only structured form (owner answers in the pane).
        norm_questions: list[dict[str, Any]] = []
        for qi, q in enumerate(questions[:12], start=1):
            if not isinstance(q, dict):
                continue
            opts: list[dict[str, str]] = []
            raw_opts = q.get("options") if isinstance(q.get("options"), list) else []
            for opt in raw_opts[:12]:
                label = sanitize_bounded_text(str((opt.get("label") or opt.get("text") or opt.get("title") or "") if isinstance(opt, dict) else (opt or "")), 180).strip()
                if label:
                    opts.append({"option_id": str(len(opts) + 1), "label": label, "value": label})
            if not opts:
                continue
            norm_questions.append(
                {
                    "question_id": f"q{qi}",
                    "title": sanitize_bounded_text(str(q.get("question") or f"Question {qi}"), 500).strip() or f"Question {qi}",
                    "type": "multi_choice" if bool(q.get("multiSelect")) else "single_choice",
                    "options": opts,
                }
            )
        if not norm_questions:
            return None
        return {
            "pending_interaction": {
                "interaction_id": tool_id,
                "revision": "1",
                "kind": "multi_question_form" if len(norm_questions) > 1 else "single_question",
                "prompt": "Input needed.",
                "questions": norm_questions,
                "answers": {},
            }
        }
    return None


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
        epoch = _proc_start_epoch(pid)
        if epoch is not None:
            starts.append(epoch)
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
                    # all rooted at the user home directory and cannot be attributed to a specific
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
            current_user_text = sanitize_canonical_text(content)
            last_agent_text = ""
            open_turn = True
            worklog_parts = []
            continue

        if role == "assistant" and open_turn:
            if content.strip():
                last_agent_text = sanitize_canonical_text(content)
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
        data = cached_pane_list_json()
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


# --- Process introspection: portable across Linux (/proc) and macOS (ps) ------
# The Linux path reads /proc/<pid>/* directly, which
# does not exist on macOS. These helpers branch on platform. On macOS `ps -o
# lstart=` returns the same ctime-format string that Claude Code writes as
# `procStart` in ~/.claude/sessions/<PID>.json, so the PID-reuse guard keeps full
# fidelity. Any miss returns None and the caller falls back to cwd heuristics.
_IS_DARWIN = sys.platform == "darwin"

# Keep each `ps` spawn short: the ancestor walk issues many per capture and
# tendwired's capture window is only a few seconds, so one slow `ps` must not
# starve the whole turn. The walk itself is already bounded (<=8 levels).
_PS_TIMEOUT_SECONDS = 1.0

# `ps -o lstart=` under LC_ALL=C, C-locale month names, one per index+1.
_MONTHS = {
    "Jan": 1, "Feb": 2, "Mar": 3, "Apr": 4, "May": 5, "Jun": 6,
    "Jul": 7, "Aug": 8, "Sep": 9, "Oct": 10, "Nov": 11, "Dec": 12,
}


def _ps_field(pid: int, fmt: str) -> str | None:
    """`ps -o <fmt>= -p <pid>` output (stripped) or None. Used on macOS/BSD.

    LC_ALL=C/TZ=UTC are pinned to match the env Claude Code uses when it writes
    `procStart` (`ps ... {LC_ALL:"C",TZ:"UTC"}`). Without this, `lstart` here is in
    the local timezone/locale and differs from the recorded ctime string, so the
    PID-reuse guard's strict compare rejects every valid record on a non-UTC Mac.
    """
    try:
        out = subprocess.run(
            ["ps", "-o", f"{fmt}=", "-p", str(pid)],
            capture_output=True,
            text=True,
            check=False,
            timeout=_PS_TIMEOUT_SECONDS,
            env={**os.environ, "LC_ALL": "C", "TZ": "UTC"},
        )
    except (OSError, subprocess.SubprocessError):
        return None
    if out.returncode != 0:
        return None
    value = out.stdout.strip()
    return value or None


def _lstart_to_epoch(value: str) -> int | None:
    """`ps -o lstart=` ("Www Mmm D HH:MM:SS YYYY", C-locale/UTC) -> Unix epoch int.

    Parsed with a fixed month map and timegm (not strptime %a/%b + mktime): the
    former is LC_TIME-sensitive on the Python side and the latter assumes local
    time, but _ps_field pins TZ=UTC so the fields are UTC.
    """
    parts = value.split()
    if len(parts) != 5:
        return None
    _wday, mon, day, clock, year = parts
    month = _MONTHS.get(mon)
    if month is None:
        return None
    hms = clock.split(":")
    if len(hms) != 3:
        return None
    try:
        hour, minute, second = (int(part) for part in hms)
        return calendar.timegm((int(year), month, int(day), hour, minute, second, 0, 0, 0))
    except (ValueError, OverflowError):
        return None


def _proc_start_epoch(pid: int) -> int | None:
    """Process start time as a Unix epoch int, or None (PID-order tie-breaker)."""
    if _IS_DARWIN:
        value = _ps_field(pid, "lstart")
        if not value:
            return None
        return _lstart_to_epoch(value)
    try:
        return int(os.stat(f"/proc/{pid}").st_ctime)
    except OSError:
        return None


def proc_starttime(pid: int) -> str | None:
    """Stable process-start token, or None. PID-reuse guard vs Claude's map file.

    Linux: field 22 (starttime, clock ticks) of /proc/<pid>/stat (comm at field 2
    can hold spaces/parens, so split on the LAST ')' and index the remainder).
    macOS: `ps -o lstart=`, which matches Claude's `procStart` ctime string.
    """
    if _IS_DARWIN:
        return _ps_field(pid, "lstart")
    try:
        with open(f"/proc/{pid}/stat", encoding="utf-8", errors="replace") as handle:
            data = handle.read()
        rest = data[data.rfind(")") + 2:]
        return rest.split()[19]
    except (OSError, IndexError, ValueError):
        return None


def proc_ppid(pid: int) -> int | None:
    """Parent PID, or None. Used to walk up from a tool child to its claude parent.

    Linux: field 4 of /proc/<pid>/stat (state index 0 after ") ", ppid index 1).
    macOS: `ps -o ppid=`.
    """
    if _IS_DARWIN:
        value = _ps_field(pid, "ppid")
        try:
            return int(value) if value is not None else None
        except ValueError:
            return None
    try:
        with open(f"/proc/{pid}/stat", encoding="utf-8", errors="replace") as handle:
            data = handle.read()
        rest = data[data.rfind(")") + 2:]
        return int(rest.split()[1])
    except (OSError, IndexError, ValueError):
        return None


def proc_is_claude(pid: int) -> bool:
    """True if the process names the claude binary (comm or argv0 basename).

    Linux: /proc/<pid>/comm (truncated to 15 chars; reads "node" for a wrapper
    install, so argv0 from /proc/<pid>/cmdline is a fallback).
    macOS: `ps -o comm=` and `ps -o args=` (argv0 basename).
    """
    if _IS_DARWIN:
        comm = _ps_field(pid, "comm") or ""
        if comm and os.path.basename(comm) == "claude":
            return True
        args = _ps_field(pid, "args") or ""
        argv0 = args.split(" ", 1)[0] if args else ""
        return bool(argv0) and os.path.basename(argv0) == "claude"
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
    if _IS_DARWIN and started is None:
        # A flaky `ps` for a *live* PID (an ordinary failure on macOS, unlike
        # /proc on Linux) must not silently disable the reuse guard: fail closed
        # so we fall through to cwd heuristics rather than trust an unverified
        # (possibly stale, PID-recycled) record.
        return None
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
    current_model = ""  # latest model seen (turn_context recurs ~per turn, so it is in the tail)
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
            if event.get("type") in ("turn_context", "session_meta"):
                model = str(payload.get("model") or "").strip()
                if model:
                    current_model = model
                continue
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
                    current_user_text = sanitize_canonical_text(text)
                elif role == "assistant" and text:
                    last_assistant_text = sanitize_canonical_text(text)
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
                final_text = sanitize_canonical_text(str(payload.get("last_agent_message") or "").strip())
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
        if current_model:
            latest["model"] = current_model
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
        if current_model:
            turn["model"] = current_model
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
    current_model = ""  # latest model seen (every assistant message carries message.model)
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
                if re.match(r"^\[Request interrupted by user[^\]]*\]$", text):
                    # Issue #3: an interrupt ends the open turn WITHOUT an end_turn. The marker is a
                    # control message, never a human prompt — the exact bracketed whole-message form
                    # avoids swallowing a prompt that merely starts with the phrase — so it only acts
                    # as a boundary here, never a visible "[Request interrupted…]" prompt.
                    if DECISIONS_ENABLED and read_pending_decision(session_id):
                        # A #36 decision is pending (tappable buttons). Leave the open turn intact so
                        # the post-loop decision path surfaces it with the REAL prompt; don't finalize
                        # and don't arm the marker as a prompt.
                        continue
                    if (
                        pending_user_uuid
                        and pending_user_uuid != consumed_user_uuid
                    ):
                        # Finalize the open turn as a completed (interrupted) turn so its worklog/stream
                        # tail lands on Telegram instead of being discarded. We finalize even with NO
                        # accumulated content (a pure-reasoning interrupt): the resulting turn EDITS the
                        # prompt message, which clears the "Working…" reasoning indicator (issue #3) —
                        # without it, a bare prompt message would keep "Working…" forever after an Esc.
                        final_text = sanitize_canonical_text(latest_stream_text).strip() or "(interrupted)"
                        turn = {
                            "available": True,
                            "pane_id": pane_id,
                            "agent": "claude",
                            "agent_session_id": session_id,
                            "turn_id": pending_user_uuid,
                            "complete": True,
                            "complete_reason": "interrupted",
                            "started_at": None,
                            "completed_at": event.get("timestamp"),
                            "user_text": pending_user_text,
                            "assistant_final_text": final_text,
                            "_prompt_uuid": pending_user_uuid,
                        }
                        if WORKLOG_ENABLED and worklog_parts:
                            worklog = _join_worklog(worklog_parts)
                            if worklog:
                                turn["worklog_text"] = worklog
                        completed.append(turn)
                        consumed_user_uuid = pending_user_uuid
                    # Boundary reset: drop the marker and any content-less open turn.
                    pending_user_text = ""
                    pending_user_uuid = ""
                    incomplete_user = False
                    pending_api_error = None
                    latest_stream_text = ""
                    latest_stream_updated_at = ""
                    worklog_parts = []
                    continue
                if text and not is_internal_claude_user_text(text):
                    # A real human prompt: it opens a new turn boundary. It also
                    # supersedes any prior API error (the owner has responded /
                    # is driving it forward), so don't keep warning about it.
                    pending_user_text = sanitize_canonical_text(text)
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
                model = str(msg.get("model") or "").strip()
                if model and model.lower() != "<synthetic>":
                    current_model = model
                if event.get("isApiErrorMessage") is True:
                    # Claude logs the API error (retries exhausted) as an
                    # assistant message; the turn stops here. Record it as the
                    # latest unresolved state — cleared below when a real
                    # completion supersedes it (recovery).
                    pending_api_error = {
                        "id": str(event.get("uuid") or ""),
                        "code": sanitize_bounded_text(str(event.get("error") or ""), 80),
                        "text": sanitize_bounded_text(content_text(msg.get("content")).strip(), 600),
                        "at": event.get("timestamp"),
                    }
                    continue
                content = msg.get("content")
                text = content_text(content).strip()
                if text and msg.get("stop_reason") == "end_turn" and pending_user_uuid:
                    # A real prompt owns the turn identity from its first open
                    # projection through completion. Internal automation user
                    # records still delimit parsing, but must not materialize a
                    # public final with an empty user prompt.
                    if pending_user_text:
                        turn = {
                            "available": True,
                            "pane_id": pane_id,
                            "agent": "claude",
                            "agent_session_id": session_id,
                            "turn_id": pending_user_uuid,
                            "complete": True,
                            "complete_reason": "done",
                            "started_at": None,
                            "completed_at": event.get("timestamp"),
                            "user_text": pending_user_text,
                            "assistant_final_text": sanitize_canonical_text(text),
                            "_prompt_uuid": pending_user_uuid,
                        }
                        if WORKLOG_ENABLED and worklog_parts:
                            worklog = _join_worklog(worklog_parts)
                            if worklog:
                                turn["worklog_text"] = worklog
                        # Coalesce consecutive end_turns under the same prompt
                        # rather than emitting a duplicate turn.
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
                        latest_stream_text = sanitize_canonical_text(text)
                        latest_stream_updated_at = event.get("timestamp") or ""

    if DECISIONS_ENABLED and incomplete_user:
        # A pending AskUserQuestion/ExitPlanMode prompt is recorded by the herdres Claude hook
        # (PreToolUse) keyed by session_id — the transcript never contains it while pending.
        # Deliver it as a structured decision turn so herdres renders tappable buttons (issue #36),
        # preserving recent_turns for history.
        pending_decision_tool = read_pending_decision(session_id)
        if pending_decision_tool is not None:
            decision_fields = claude_decision_turn_fields(pending_decision_tool)
            if decision_fields:
                result: dict[str, Any] = {
                    "available": True,
                    "pane_id": pane_id,
                    "agent": "claude",
                    "agent_session_id": session_id,
                    "turn_id": pending_user_uuid or str(pending_decision_tool.get("tool_use_id") or ""),
                    "complete": False,
                    "awaiting_input": True,
                    "user_text": pending_user_text,
                    "assistant_final_text": "",
                    **decision_fields,
                }
                if completed:
                    result["recent_turns"] = [
                        {k: v for k, v in t.items() if k != "_prompt_uuid"} for t in completed[-RECENT_TURNS:]
                    ]
                if current_model:
                    result["model"] = current_model
                return result
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
        if current_model:
            latest["model"] = current_model
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
        if current_model:
            result["model"] = current_model
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
            current_user_text = sanitize_canonical_text(msg)
            last_agent_text = ""
            open_turn = True
            worklog_parts = []
            continue

        if source == "agent" and open_turn:
            if msg.strip():
                last_agent_text = sanitize_canonical_text(msg)
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


def kimi_code_home() -> Path:
    return Path(
        os.getenv("KIMI_CODE_HOME", str(Path.home() / ".kimi-code"))
    ).expanduser()


def _kimi_workspace_suffix(cwd: str) -> str:
    normalized = os.path.realpath(cwd)
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()[:12]


def _kimi_regular_file(path: Path, root: Path, *, max_bytes: int | None = None) -> bool:
    try:
        if path.is_symlink() or not path.is_file():
            return False
        resolved = path.resolve(strict=True)
        resolved.relative_to(root)
        size = resolved.stat().st_size
    except (OSError, RuntimeError, ValueError):
        return False
    return max_bytes is None or size <= max_bytes


def _kimi_bounded_entries(path: Path, limit: int) -> list[Path] | None:
    entries: list[Path] = []
    try:
        with os.scandir(path) as iterator:
            for item in iterator:
                if len(entries) >= limit:
                    return None
                entries.append(Path(item.path))
    except OSError:
        return None
    return entries


def _kimi_session_path_for_pane(pane: dict[str, Any]) -> tuple[Path, str] | None:
    cwd = str(pane.get("foreground_cwd") or pane.get("cwd") or "").strip()
    if not cwd:
        return None
    try:
        canonical_cwd = str(Path(cwd).resolve(strict=True))
        root = (kimi_code_home() / "sessions").resolve(strict=True)
    except (OSError, RuntimeError, ValueError):
        return None
    workspace_entries = _kimi_bounded_entries(root, KIMI_WORKSPACE_MAX_ENTRIES)
    if workspace_entries is None:
        return None
    suffix = "_" + _kimi_workspace_suffix(canonical_cwd)
    workspaces = [
        item
        for item in workspace_entries
        if item.name.startswith("wd_")
        and item.name.endswith(suffix)
        and not item.is_symlink()
        and item.is_dir()
    ]
    if len(workspaces) != 1:
        return None
    workspace = workspaces[0].resolve(strict=True)
    try:
        workspace.relative_to(root)
    except (OSError, RuntimeError, ValueError):
        return None
    session_entries = _kimi_bounded_entries(workspace, KIMI_SESSION_MAX_ENTRIES)
    if session_entries is None:
        return None

    candidates: list[tuple[int, Path, str]] = []
    for session_dir in session_entries:
        match = _KIMI_SESSION_RE.fullmatch(session_dir.name)
        if match is None or session_dir.is_symlink() or not session_dir.is_dir():
            continue
        try:
            resolved_session = session_dir.resolve(strict=True)
            resolved_session.relative_to(workspace)
        except (OSError, RuntimeError, ValueError):
            continue
        state_path = resolved_session / "state.json"
        wire_path = resolved_session / "agents" / "main" / "wire.jsonl"
        if not _kimi_regular_file(state_path, root, max_bytes=KIMI_STATE_MAX_BYTES):
            continue
        if not _kimi_regular_file(wire_path, root):
            continue
        try:
            state = json.loads(state_path.read_text(encoding="utf-8"))
            if not isinstance(state, dict):
                continue
            state_cwd = str(Path(str(state.get("workDir") or "")).resolve(strict=True))
            wire_mtime = wire_path.stat().st_mtime_ns
        except (OSError, RuntimeError, TypeError, ValueError, json.JSONDecodeError):
            continue
        if state_cwd != canonical_cwd:
            continue
        candidates.append((wire_mtime, wire_path, match.group(1)))
    if not candidates:
        return None
    candidates.sort(key=lambda item: (item[0], item[2]), reverse=True)
    if len(candidates) > 1 and candidates[0][0] == candidates[1][0]:
        return None
    return candidates[0][1], candidates[0][2]


def _kimi_is_turn_start(raw: str) -> bool:
    try:
        event = json.loads(raw)
    except Exception:
        return False
    origin = event.get("origin") if isinstance(event.get("origin"), dict) else {}
    return event.get("type") == "turn.prompt" and origin.get("kind") == "user"


def _kimi_is_turn_end(raw: str) -> bool:
    try:
        record = json.loads(raw)
    except Exception:
        return False
    event = record.get("event") if isinstance(record.get("event"), dict) else {}
    return (
        record.get("type") == "context.append_loop_event"
        and event.get("type") == "step.end"
        and event.get("finishReason") == "end_turn"
    )


def _kimi_timestamp(value: Any) -> str | None:
    if type(value) not in (int, float) or not float(value) >= 0:
        return None
    try:
        return datetime.fromtimestamp(float(value) / 1000.0, tz=timezone.utc).isoformat()
    except (OverflowError, OSError, ValueError):
        return None


def _kimi_progress_text(value: Any) -> str:
    if not isinstance(value, str):
        return ""
    cleaned = _redact_secrets(_strip_control(value)).strip()
    return sanitize_bounded_text(cleaned)


def extract_kimi_turn(path: Path, pane_id: str, session_id: str) -> dict[str, Any]:
    completed: list[dict[str, Any]] = []
    current: dict[str, Any] | None = None
    step_text: dict[str, list[str]] = {}
    progress = ""

    with open_jsonl_tail(path, _kimi_is_turn_start, _kimi_is_turn_end) as handle:
        for raw in handle:
            try:
                record = json.loads(raw)
            except Exception:
                continue
            record_type = str(record.get("type") or "")
            origin = record.get("origin") if isinstance(record.get("origin"), dict) else {}
            if record_type == "turn.prompt" and origin.get("kind") == "user":
                prompt = sanitize_canonical_text(content_text(record.get("input")))
                started_at = _kimi_timestamp(record.get("time"))
                if not prompt.strip() or started_at is None:
                    current = None
                    step_text = {}
                    progress = ""
                    continue
                prompt_time = record.get("time")
                current = {
                    "available": True,
                    "pane_id": pane_id,
                    "agent": "kimi",
                    "agent_session_id": session_id,
                    "turn_id": f"{session_id}:{prompt_time}",
                    "complete": False,
                    "started_at": started_at,
                    "user_text": prompt,
                    "assistant_final_text": "",
                }
                step_text = {}
                progress = ""
                continue
            if current is None:
                continue
            if record_type == "turn.steer" and origin.get("kind") == "user":
                steering = sanitize_canonical_text(content_text(record.get("input")))
                if steering.strip():
                    current["user_text"] = str(current["user_text"]) + "\n\n" + steering
                continue
            if record_type != "context.append_loop_event":
                continue
            event = record.get("event") if isinstance(record.get("event"), dict) else {}
            event_type = str(event.get("type") or "")
            step = str(event.get("step") or "")
            if event_type == "content.part":
                part = event.get("part") if isinstance(event.get("part"), dict) else {}
                if part.get("type") == "text" and isinstance(part.get("text"), str):
                    text = sanitize_canonical_text(part["text"])
                    step_text.setdefault(step, []).append(text)
                    if text:
                        progress = "".join(step_text[step])
                elif part.get("type") == "think":
                    candidate = _kimi_progress_text(part.get("think"))
                    if candidate:
                        progress = candidate
                continue
            if event_type == "tool.call":
                description = event.get("description") or event.get("name")
                candidate = _kimi_progress_text(description)
                if candidate:
                    progress = candidate
                continue
            if event_type != "step.end" or event.get("finishReason") != "end_turn":
                continue
            final_text = sanitize_canonical_text("".join(step_text.get(step, [])))
            if not final_text.strip():
                continue
            final = dict(current)
            final["complete"] = True
            final["complete_reason"] = "done"
            final["completed_at"] = _kimi_timestamp(record.get("time"))
            final["assistant_final_text"] = final_text
            if progress:
                final["worklog_text"] = progress
            completed.append(final)
            current = None
            step_text = {}
            progress = ""

    recent = completed[-RECENT_TURNS:]
    if current is not None:
        if recent:
            latest = dict(recent[-1])
            latest["recent_turns"] = recent
            latest["has_open_turn"] = True
            latest["open_turn_id"] = current["turn_id"]
            latest["open_user_text"] = current["user_text"]
            return add_stream_fields(latest, progress, "kimi")
        current["recent_turns"] = []
        return add_stream_fields(current, progress, "kimi")
    if recent:
        latest = dict(recent[-1])
        latest["recent_turns"] = recent
        return latest
    return {
        "available": True,
        "pane_id": pane_id,
        "agent": "kimi",
        "agent_session_id": session_id,
        "complete": False,
        "reason": "no_completed_turn",
    }


def infer_kimi_turn_from_workspace(pane: dict[str, Any], pane_id: str) -> dict[str, Any]:
    cwd = str(pane.get("foreground_cwd") or pane.get("cwd") or "").strip()
    if not cwd:
        return unavailable("kimi_workspace_unavailable", pane_id=pane_id, agent="kimi")
    try:
        listing = cached_pane_list_json()
    except (RuntimeError, json.JSONDecodeError, subprocess.TimeoutExpired, OSError):
        return unavailable("herdr_list_failed", pane_id=pane_id, agent="kimi")
    panes = listing.get("result", {}).get("panes")
    peers = [
        item
        for item in panes
        if isinstance(item, dict)
        and str(item.get("agent") or "").lower() == "kimi"
        and str(item.get("foreground_cwd") or item.get("cwd") or "") == cwd
    ] if isinstance(panes, list) else []
    if len(peers) != 1 or str(peers[0].get("pane_id") or "") != pane_id:
        return unavailable("ambiguous_kimi_workspace", pane_id=pane_id, agent="kimi")
    resolved = _kimi_session_path_for_pane(pane)
    if resolved is None:
        return unavailable("kimi_session_not_found", pane_id=pane_id, agent="kimi")
    path, session_id = resolved
    return result_turn(extract_kimi_turn(path, pane_id, session_id))


def pane_from_list(pane_id: str) -> dict[str, Any] | None:
    try:
        data = cached_pane_list_json()
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
    if agent == "kimi":
        return infer_kimi_turn_from_workspace(pane, pane_id)
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
