"""Who-am-I + usage for the pinned boards.

Reads plan metadata from the local CLI credential files (named fields only — NEVER the
tokens themselves) and local usage from `ccusage`, and renders the compact account line
appended to the pinned status boards, e.g.

    🔑 Max 20x · block $37 (1h35m) · today $593
    🔑 ChatGPT Pro · today $371 · 215M tok

The ccusage snapshot is disk-cached with a coarse TTL: every value change re-edits every
pinned board, so the TTL (config.usage_refresh_seconds) is the pin-edit rate limiter, and
all numbers are rounded coarsely for the same reason.
"""
from __future__ import annotations

import base64
import json
import subprocess
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from . import config

CLAUDE_CREDENTIALS_PATH = Path.home() / ".claude/.credentials.json"
CODEX_AUTH_PATH = Path.home() / ".codex/auth.json"
USAGE_CACHE_PATH = Path.home() / ".local/share/herdres/usage_cache.json"

# ccusage answers in ~2s with a --since filter; the cap only bounds a wedged run, which
# would otherwise stall the whole sync pass (refresh happens at most once per TTL).
CCUSAGE_TIMEOUT_SECONDS = 15

# Process-local memo so one sync pass touching many topics reads/refreshes at most once,
# even when ccusage is missing (a failed refresh would otherwise retry per topic).
_MEMO: dict[str, Any] | None = None


def _read_json(path: Any) -> dict[str, Any]:
    try:
        data = json.loads(Path(path).read_text(encoding="utf-8"))
    except Exception:
        return {}
    return data if isinstance(data, dict) else {}


# --- identity (plan metadata only; tokens are never read out of these files) -----------


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


# --- usage snapshot (ccusage, disk-cached) ----------------------------------------------


def _run_ccusage(args: list[str]) -> dict[str, Any]:
    try:
        proc = subprocess.run(
            ["ccusage", *args, "--json"], capture_output=True, timeout=CCUSAGE_TIMEOUT_SECONDS
        )
    except Exception:
        return {}
    if proc.returncode != 0:
        return {}
    try:
        data = json.loads(proc.stdout.decode("utf-8", "replace") or "{}")
    except Exception:
        return {}
    return data if isinstance(data, dict) else {}


def _daily_totals(payload: dict[str, Any]) -> dict[str, Any]:
    rows = payload.get("daily")
    if not isinstance(rows, list) or not rows:
        return {}
    row = rows[-1]
    if not isinstance(row, dict):
        return {}
    totals: dict[str, Any] = {}
    cost = row.get("totalCost") if row.get("totalCost") is not None else row.get("costUSD")
    if isinstance(cost, (int, float)):
        totals["today_cost"] = float(cost)
    tokens = row.get("totalTokens")
    if isinstance(tokens, (int, float)):
        totals["today_tokens"] = int(tokens)
    return totals


def _collect_usage(now: float) -> dict[str, Any]:
    since = time.strftime("%Y%m%d", time.localtime(now))
    usage: dict[str, Any] = {}
    claude = _daily_totals(_run_ccusage(["claude", "daily", "--since", since]))
    blocks = _run_ccusage(["blocks", "--active"]).get("blocks")
    block = blocks[0] if isinstance(blocks, list) and blocks and isinstance(blocks[0], dict) else {}
    if isinstance(block.get("costUSD"), (int, float)):
        claude["block_cost"] = float(block["costUSD"])
    if block.get("endTime"):
        claude["block_end"] = str(block["endTime"])
    if claude:
        usage["claude"] = claude
    codex = _daily_totals(_run_ccusage(["codex", "daily", "--since", since]))
    if codex:
        usage["codex"] = codex
    return usage


def usage_snapshot(*, cache_path: Any = None, now: float | None = None, env: Any = None) -> dict[str, Any]:
    """The ccusage numbers behind the account line, refreshed at most once per TTL.

    Serves, in order: the process memo, the disk cache (shared across the timer's
    one-process-per-tick runs), then a fresh ccusage collection. A failed refresh keeps
    serving the last good snapshot rather than blanking the boards."""
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
        snapshot["fetched_at"] = current  # back off for a full TTL before retrying ccusage
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


def _fmt_dollars(value: Any) -> str:
    if not isinstance(value, (int, float)):
        return ""
    return f"${round(float(value)):,}"


def _fmt_tokens(value: Any) -> str:
    if not isinstance(value, (int, float)) or value <= 0:
        return ""
    count = float(value)
    if count >= 1_000_000:
        return f"{count / 1_000_000:.0f}M tok"
    if count >= 1_000:
        return f"{count / 1_000:.0f}k tok"
    return f"{count:.0f} tok"


def _fmt_block_left(end_iso: str, now: float) -> str:
    try:
        end = datetime.fromisoformat(end_iso.replace("Z", "+00:00"))
        minutes = int((end.timestamp() - now) // 60)
    except Exception:
        return ""
    if minutes <= 0:
        return ""
    minutes -= minutes % 5  # coarse on purpose: every tick of this string re-edits every pin
    hours, mins = divmod(minutes, 60)
    return f"{hours}h{mins:02d}m" if hours else f"{mins}m"


def account_line(kind: str, *, snapshot: dict[str, Any] | None = None, now: float | None = None, env: Any = None) -> str:
    """The plain-text account/usage line for one agent kind ('' when unknown/unavailable)."""
    kind = agent_kind(kind)
    if not kind:
        return ""
    current = time.time() if now is None else now
    snapshot = usage_snapshot(env=env) if snapshot is None else snapshot
    usage = snapshot.get(kind) if isinstance(snapshot.get(kind), dict) else {}
    parts: list[str] = []
    if kind == "claude":
        plan = _pretty_claude_plan(claude_identity())
        if plan:
            parts.append(plan)
        block_cost = _fmt_dollars(usage.get("block_cost"))
        if block_cost:
            left = _fmt_block_left(str(usage.get("block_end") or ""), current)
            parts.append(f"block {block_cost} ({left})" if left else f"block {block_cost}")
    else:
        plan = _pretty_codex_plan(codex_identity())
        if plan:
            parts.append(plan)
    today_cost = _fmt_dollars(usage.get("today_cost"))
    if today_cost:
        parts.append(f"today {today_cost}")
    tokens = _fmt_tokens(usage.get("today_tokens"))
    if tokens:
        parts.append(tokens)
    return "🔑 " + " · ".join(parts) if parts else ""
