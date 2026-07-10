"""Who-am-I + remaining-quota footer for the pinned boards.

Reads plan metadata from the local CLI credential files (named fields only — NEVER the
tokens themselves) and the REMAINING rate-limit headroom per window, rendered as e.g.

    🔑 Max 20x · 5h 76% left (1h10m) · wk 68% left (Sun)
    🔑 ChatGPT Pro · 5h 86% left (2h05m) · wk 98% left (Fri)

Sources:
- Claude: the OAuth usage endpoint the in-app /usage screen uses (bearer = the access
  token from ~/.claude/.credentials.json, read in-process and never rendered/logged).
- Codex: the `rate_limits` events the codex CLI writes into its session JSONL files
  (primary = 5h window, secondary = weekly) — local, no network.

The snapshot is disk-cached with a coarse TTL: every value change re-edits every pinned
board, so the TTL (config.usage_refresh_seconds) is the pin-edit rate limiter, and reset
countdowns are rounded to 5 minutes for the same reason.
"""
from __future__ import annotations

import base64
import glob
import json
import os
import time
import urllib.request
from datetime import datetime
from pathlib import Path
from typing import Any

from . import config

CLAUDE_CREDENTIALS_PATH = Path.home() / ".claude/.credentials.json"
CODEX_AUTH_PATH = Path.home() / ".codex/auth.json"
CODEX_SESSIONS_DIR = Path.home() / ".codex/sessions"
USAGE_CACHE_PATH = Path.home() / ".local/share/herdres/usage_cache.json"

CLAUDE_USAGE_URL = "https://api.anthropic.com/api/oauth/usage"
HTTP_TIMEOUT_SECONDS = 15
_SESSION_TAIL_BYTES = 400_000
_SESSION_FILES_TO_SCAN = 3

# Process-local memo so one sync pass touching many topics reads/refreshes at most once,
# even when the sources fail (a failed refresh would otherwise retry per topic).
_MEMO: dict[str, Any] | None = None


def _read_json(path: Any) -> dict[str, Any]:
    try:
        data = json.loads(Path(path).read_text(encoding="utf-8"))
    except Exception:
        return {}
    return data if isinstance(data, dict) else {}


# --- identity (plan metadata only; tokens are never rendered or logged) -----------------


def agent_kind(value: Any) -> str:
    """Map an entry's agent/worker name to the account it runs on ('claude'/'codex'/'')."""
    clean = str(value or "").strip().lower()
    if clean.startswith("claude"):
        return "claude"
    if clean.startswith(("codex", "gpt", "openai")):
        return "codex"
    return ""


def claude_identity(path: Any = None) -> dict[str, str]:
    oauth = _read_json(path or CLAUDE_CREDENTIALS_PATH).get("claudeAiOauth")
    if not isinstance(oauth, dict):
        return {}
    identity: dict[str, str] = {}
    for key, field in (("plan", "subscriptionType"), ("tier", "rateLimitTier")):
        value = str(oauth.get(field) or "").strip()
        if value:
            identity[key] = value
    return identity


def _jwt_claims(token: str) -> dict[str, Any]:
    try:
        payload = token.split(".")[1]
        payload += "=" * (-len(payload) % 4)
        claims = json.loads(base64.urlsafe_b64decode(payload))
    except Exception:
        return {}
    return claims if isinstance(claims, dict) else {}


def codex_identity(path: Any = None) -> dict[str, str]:
    data = _read_json(path or CODEX_AUTH_PATH)
    identity: dict[str, str] = {}
    mode = str(data.get("auth_mode") or "").strip()
    if mode:
        identity["mode"] = mode
    tokens = data.get("tokens")
    id_token = str(tokens.get("id_token") or "") if isinstance(tokens, dict) else ""
    auth_claim = _jwt_claims(id_token).get("https://api.openai.com/auth")
    if isinstance(auth_claim, dict):
        plan = str(auth_claim.get("chatgpt_plan_type") or "").strip()
        if plan:
            identity["plan"] = plan
    return identity


def _pretty_claude_plan(identity: dict[str, str]) -> str:
    tier = identity.get("tier", "")
    token = tier.rsplit("claude_", 1)[-1] if "claude_" in tier else tier
    words = [word for word in token.split("_") if word and word != "default"]
    if words:
        return " ".join(word.capitalize() if word.isalpha() else word for word in words)
    plan = identity.get("plan", "")
    return plan.capitalize() if plan else ""


def _pretty_codex_plan(identity: dict[str, str]) -> str:
    plan = identity.get("plan", "")
    if identity.get("mode") == "chatgpt" or plan:
        return f"ChatGPT {plan.capitalize()}".strip() if plan else "ChatGPT"
    mode = identity.get("mode", "")
    return "API key" if "api" in mode else (mode.capitalize() if mode else "")


# --- remaining-quota collection ----------------------------------------------------------


def _window(used_percent: Any, resets_epoch: Any) -> dict[str, Any]:
    window: dict[str, Any] = {}
    if isinstance(used_percent, (int, float)):
        window["used_percent"] = float(used_percent)
    if isinstance(resets_epoch, (int, float)) and resets_epoch > 0:
        window["resets_at"] = float(resets_epoch)
    return window


def _iso_epoch(value: Any) -> float | None:
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00")).timestamp()
    except Exception:
        return None


def _http_get_json(url: str, headers: dict[str, str]) -> dict[str, Any]:
    request = urllib.request.Request(url, headers=headers)
    try:
        with urllib.request.urlopen(request, timeout=HTTP_TIMEOUT_SECONDS) as response:
            data = json.loads(response.read().decode("utf-8", "replace"))
    except Exception:
        return {}
    return data if isinstance(data, dict) else {}


def _claude_limits() -> dict[str, Any]:
    """5h/weekly utilization from the OAuth usage endpoint (what in-app /usage shows)."""
    oauth = _read_json(CLAUDE_CREDENTIALS_PATH).get("claudeAiOauth")
    token = str(oauth.get("accessToken") or "") if isinstance(oauth, dict) else ""
    if not token:
        return {}
    payload = _http_get_json(CLAUDE_USAGE_URL, {
        "Authorization": f"Bearer {token}",
        "anthropic-beta": "oauth-2025-04-20",
        "Content-Type": "application/json",
    })
    limits: dict[str, Any] = {}
    for key, field in (("five_hour", "five_hour"), ("weekly", "seven_day"), ("weekly_opus", "seven_day_opus")):
        raw = payload.get(field)
        if isinstance(raw, dict):
            window = _window(raw.get("utilization"), _iso_epoch(raw.get("resets_at")))
            if window:
                limits[key] = window
    return limits


def _last_rate_limits_line(path: Path) -> dict[str, Any]:
    try:
        with open(path, "rb") as handle:
            handle.seek(0, os.SEEK_END)
            size = handle.tell()
            handle.seek(max(0, size - _SESSION_TAIL_BYTES))
            tail = handle.read().decode("utf-8", "replace")
    except Exception:
        return {}
    for line in reversed(tail.splitlines()):
        if '"rate_limits"' not in line:
            continue
        try:
            found = _dig_rate_limits(json.loads(line))
        except Exception:
            continue
        if found:
            return found
    return {}


def _dig_rate_limits(obj: Any) -> dict[str, Any]:
    if isinstance(obj, dict):
        limits = obj.get("rate_limits")
        if isinstance(limits, dict):
            return limits
        for value in obj.values():
            found = _dig_rate_limits(value)
            if found:
                return found
    return {}


def _codex_limits() -> dict[str, Any]:
    """5h/weekly windows from the newest `rate_limits` event in the codex session logs
    (primary = 5h, secondary = weekly). Local files only — no network."""
    try:
        files = sorted(
            glob.glob(str(Path(CODEX_SESSIONS_DIR) / "**" / "*.jsonl"), recursive=True),
            key=os.path.getmtime,
        )
    except Exception:
        return {}
    for path in reversed(files[-_SESSION_FILES_TO_SCAN:] if files else []):
        raw = _last_rate_limits_line(Path(path))
        if not raw:
            continue
        limits: dict[str, Any] = {}
        for key, field in (("five_hour", "primary"), ("weekly", "secondary")):
            window_raw = raw.get(field)
            if isinstance(window_raw, dict):
                window = _window(window_raw.get("used_percent"), window_raw.get("resets_at"))
                if window:
                    limits[key] = window
        if limits:
            return limits
    return {}


def _collect_usage(now: float) -> dict[str, Any]:
    usage: dict[str, Any] = {}
    claude = _claude_limits()
    if claude:
        usage["claude"] = claude
    codex = _codex_limits()
    if codex:
        usage["codex"] = codex
    return usage


def usage_snapshot(*, cache_path: Any = None, now: float | None = None, env: Any = None) -> dict[str, Any]:
    """The remaining-quota numbers behind the account line, refreshed at most once per TTL.

    Serves, in order: the process memo, the disk cache (shared across processes), then a
    fresh collection. A failed refresh keeps serving the last good snapshot (and backs off
    a full TTL) rather than blanking the boards."""
    global _MEMO
    current = time.time() if now is None else now
    ttl = config.usage_refresh_seconds(env)
    if _MEMO is not None:
        fetched = _MEMO.get("fetched_at")
        if isinstance(fetched, (int, float)) and current - fetched < ttl:
            return _MEMO
    path = Path(cache_path or USAGE_CACHE_PATH)
    cached = _read_json(path)
    fetched = cached.get("fetched_at")
    if isinstance(fetched, (int, float)) and current - fetched < ttl:
        _MEMO = cached
        return cached
    fresh = _collect_usage(current)
    if not fresh and cached:
        snapshot = dict(cached)
        snapshot["fetched_at"] = current  # back off for a full TTL before retrying the sources
    else:
        snapshot = {"fetched_at": current, **fresh}
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(snapshot), encoding="utf-8")
    except Exception:
        pass
    _MEMO = snapshot
    return snapshot


# --- the rendered line -------------------------------------------------------------------


def _fmt_countdown(resets_at: Any, now: float) -> str:
    if not isinstance(resets_at, (int, float)):
        return ""
    minutes = int((resets_at - now) // 60)
    if minutes <= 0:
        return ""
    minutes -= minutes % 5  # coarse on purpose: every tick of this string re-edits every pin
    hours, mins = divmod(minutes, 60)
    if hours >= 24:
        return time.strftime("%a", time.localtime(resets_at))
    return f"{hours}h{mins:02d}m" if hours else f"{mins}m"


def _fmt_window(label: str, window: Any, now: float) -> str:
    if not isinstance(window, dict):
        return ""
    used = window.get("used_percent")
    if not isinstance(used, (int, float)):
        return ""
    left = max(0, min(100, round(100 - used)))
    part = f"{label} {left}% left"
    if left <= 10:
        part = f"⚠️ {part}"
    countdown = _fmt_countdown(window.get("resets_at"), now)
    return f"{part} ({countdown})" if countdown else part


def account_line(kind: str, *, snapshot: dict[str, Any] | None = None, now: float | None = None, env: Any = None) -> str:
    """The plain-text account/remaining-quota line for one agent kind ('' when unknown)."""
    kind = agent_kind(kind)
    if not kind:
        return ""
    current = time.time() if now is None else now
    snapshot = usage_snapshot(env=env) if snapshot is None else snapshot
    limits = snapshot.get(kind) if isinstance(snapshot.get(kind), dict) else {}
    plan = _pretty_claude_plan(claude_identity()) if kind == "claude" else _pretty_codex_plan(codex_identity())
    parts = [plan] if plan else []
    for label, key in (("5h", "five_hour"), ("wk", "weekly"), ("opus wk", "weekly_opus")):
        part = _fmt_window(label, limits.get(key), current)
        if part:
            parts.append(part)
    return "🔑 " + " · ".join(parts) if parts else ""
