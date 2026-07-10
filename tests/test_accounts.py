"""The pinned who-am-I/quota line: plan metadata comes from the CLI credential files
(named fields only — tokens must never surface), remaining rate-limit headroom from the
Claude OAuth usage endpoint / the codex session logs, behind a disk-cached TTL (the
pin-edit rate limiter), rendered as a compact footer on the pinned boards."""
from __future__ import annotations

import base64
import json

from herdres_connector import accounts, state
from herdres_connector.source_sync import SyncRuntime, sync_once

from test_source_only import FakeTelegram, FakeTendwire, _store


def _fake_jwt(claims: dict) -> str:
    def b64(obj: dict) -> str:
        return base64.urlsafe_b64encode(json.dumps(obj).encode()).decode().rstrip("=")

    return f"{b64({'alg': 'none'})}.{b64(claims)}.sig"


# --- identity ----------------------------------------------------------------


def test_agent_kind():
    assert accounts.agent_kind("claude-1-1-1") == "claude"
    assert accounts.agent_kind("Claude") == "claude"
    assert accounts.agent_kind("codex-2") == "codex"
    assert accounts.agent_kind("gpt-5-codex") == "codex"
    assert accounts.agent_kind("kimi") == ""
    assert accounts.agent_kind(None) == ""


def test_claude_identity_reads_only_plan_fields(tmp_path):
    creds = tmp_path / "creds.json"
    creds.write_text(json.dumps({"claudeAiOauth": {
        "subscriptionType": "max",
        "rateLimitTier": "default_claude_max_20x",
        "accessToken": "sk-SECRET",
        "refreshToken": "rt-SECRET",
    }}))
    assert accounts.claude_identity(creds) == {"plan": "max", "tier": "default_claude_max_20x"}


def test_codex_identity_decodes_plan_claim(tmp_path):
    auth = tmp_path / "auth.json"
    auth.write_text(json.dumps({"auth_mode": "chatgpt", "tokens": {
        "id_token": _fake_jwt({"https://api.openai.com/auth": {"chatgpt_plan_type": "pro"}}),
        "access_token": "at-SECRET",
    }}))
    assert accounts.codex_identity(auth) == {"mode": "chatgpt", "plan": "pro"}


def test_identity_missing_or_malformed_files(tmp_path):
    assert accounts.claude_identity(tmp_path / "nope.json") == {}
    assert accounts.codex_identity(tmp_path / "nope.json") == {}
    bad = tmp_path / "bad.json"
    bad.write_text("not json")
    assert accounts.claude_identity(bad) == {}


def test_pretty_plans():
    assert accounts._pretty_claude_plan({"plan": "max", "tier": "default_claude_max_20x"}) == "Max 20x"
    assert accounts._pretty_claude_plan({"plan": "pro"}) == "Pro"
    assert accounts._pretty_claude_plan({}) == ""
    assert accounts._pretty_codex_plan({"mode": "chatgpt", "plan": "pro"}) == "ChatGPT Pro"
    assert accounts._pretty_codex_plan({"mode": "chatgpt"}) == "ChatGPT"
    assert accounts._pretty_codex_plan({"mode": "apikey"}) == "API key"
    assert accounts._pretty_codex_plan({}) == ""


# --- usage snapshot cache ------------------------------------------------------


def test_usage_snapshot_ttl_memo_and_stale(monkeypatch, tmp_path):
    cache = tmp_path / "usage_cache.json"
    calls: list[float] = []

    def fake_collect(now):
        calls.append(now)
        return {"claude": {"today_cost": 10.0}}

    monkeypatch.setattr(accounts, "_collect_usage", fake_collect)
    monkeypatch.setattr(accounts, "_MEMO", None)

    first = accounts.usage_snapshot(cache_path=cache, now=1000.0, env={})
    assert first["claude"]["today_cost"] == 10.0 and len(calls) == 1
    # within TTL: memo serves, no re-collect
    assert accounts.usage_snapshot(cache_path=cache, now=1100.0, env={}) == first
    assert len(calls) == 1
    # memo gone but the disk cache is fresh (a new timer-tick process): still no re-collect
    monkeypatch.setattr(accounts, "_MEMO", None)
    assert accounts.usage_snapshot(cache_path=cache, now=1200.0, env={})["claude"]["today_cost"] == 10.0
    assert len(calls) == 1
    # TTL expired -> re-collect
    accounts.usage_snapshot(cache_path=cache, now=1400.0, env={})
    assert len(calls) == 2
    # a failed refresh serves the stale snapshot and backs off a full TTL before retrying
    monkeypatch.setattr(accounts, "_collect_usage", lambda now: {})
    monkeypatch.setattr(accounts, "_MEMO", None)
    stale = accounts.usage_snapshot(cache_path=cache, now=2000.0, env={})
    assert stale["claude"]["today_cost"] == 10.0
    assert stale["fetched_at"] == 2000.0


# --- the rendered line ---------------------------------------------------------


def test_account_line_claude_format(monkeypatch, tmp_path):
    creds = tmp_path / "creds.json"
    creds.write_text(json.dumps({"claudeAiOauth": {
        "subscriptionType": "max", "rateLimitTier": "default_claude_max_20x",
        "accessToken": "sk-SECRET"}}))
    monkeypatch.setattr(accounts, "CLAUDE_CREDENTIALS_PATH", creds)
    now = 1_700_000_000.0
    snapshot = {"claude": {
        "five_hour": {"used_percent": 24.0, "resets_at": now + 73 * 60},
        "weekly": {"used_percent": 32.0, "resets_at": now + 30 * 3600},
    }}
    line = accounts.account_line("claude", snapshot=snapshot, now=now)
    day = accounts.time.strftime("%a", accounts.time.localtime(now + 30 * 3600))
    assert line == f"🔑 Max 20x · 5h 76% left (1h10m) · wk 68% left ({day})"
    assert "SECRET" not in line


def test_account_line_codex_format(monkeypatch, tmp_path):
    auth = tmp_path / "auth.json"
    auth.write_text(json.dumps({"auth_mode": "chatgpt", "tokens": {
        "id_token": _fake_jwt({"https://api.openai.com/auth": {"chatgpt_plan_type": "pro"}})}}))
    monkeypatch.setattr(accounts, "CODEX_AUTH_PATH", auth)
    snapshot = {"codex": {"five_hour": {"used_percent": 14.0}, "weekly": {"used_percent": 2.0}}}
    assert accounts.account_line("codex-1", snapshot=snapshot, now=0.0) == \
        "🔑 ChatGPT Pro · 5h 86% left · wk 98% left"


def test_account_line_degrades_gracefully(monkeypatch, tmp_path):
    monkeypatch.setattr(accounts, "CLAUDE_CREDENTIALS_PATH", tmp_path / "nope.json")
    assert accounts.account_line("kimi", snapshot={}, now=0.0) == ""          # unknown kind
    assert accounts.account_line("claude", snapshot={}, now=0.0) == ""        # no identity, no limits
    # limits without identity still render the meter
    assert accounts.account_line("claude", snapshot={"claude": {"five_hour": {"used_percent": 50}}},
                                 now=0.0) == "🔑 5h 50% left"


def test_window_warns_when_nearly_exhausted():
    line = accounts._fmt_window("5h", {"used_percent": 93.0}, 0.0)
    assert line == "⚠️ 5h 7% left"
    assert accounts._fmt_window("5h", {"used_percent": 120.0}, 0.0) == "⚠️ 5h 0% left"   # clamped
    assert accounts._fmt_window("5h", {}, 0.0) == ""                                     # no data, no part
    assert accounts._fmt_window("5h", None, 0.0) == ""


def test_countdown_rounds_and_switches_to_day():
    now = 1_700_000_000.0
    assert accounts._fmt_countdown(now + 47 * 60, now) == "45m"               # 47 -> 45, damps pin churn
    assert accounts._fmt_countdown(now - 60, now) == ""
    assert accounts._fmt_countdown("garbage", now) == ""
    day = accounts.time.strftime("%a", accounts.time.localtime(now + 40 * 3600))
    assert accounts._fmt_countdown(now + 40 * 3600, now) == day               # >24h away -> weekday name


# --- the collectors --------------------------------------------------------------


def test_claude_limits_parses_oauth_usage(monkeypatch, tmp_path):
    creds = tmp_path / "creds.json"
    creds.write_text(json.dumps({"claudeAiOauth": {"accessToken": "sk-SECRET"}}))
    monkeypatch.setattr(accounts, "CLAUDE_CREDENTIALS_PATH", creds)
    seen: dict = {}

    def fake_get(url, headers):
        seen["url"] = url
        seen["auth"] = headers.get("Authorization", "")
        return {
            "five_hour": {"utilization": 24.0, "resets_at": "2026-07-10T22:00:00+00:00"},
            "seven_day": {"utilization": 32.0, "resets_at": "2026-07-12T00:00:00+00:00"},
            "seven_day_opus": None,
        }

    monkeypatch.setattr(accounts, "_http_get_json", fake_get)
    limits = accounts._claude_limits()
    assert seen["url"] == accounts.CLAUDE_USAGE_URL and seen["auth"] == "Bearer sk-SECRET"
    assert limits["five_hour"]["used_percent"] == 24.0
    assert limits["weekly"]["used_percent"] == 32.0
    assert limits["five_hour"]["resets_at"] > 0
    assert "weekly_opus" not in limits                                        # null claim -> omitted


def test_claude_limits_without_token_makes_no_request(monkeypatch, tmp_path):
    monkeypatch.setattr(accounts, "CLAUDE_CREDENTIALS_PATH", tmp_path / "nope.json")
    monkeypatch.setattr(accounts, "_http_get_json",
                        lambda *a, **k: (_ for _ in ()).throw(AssertionError("must not call out without a token")))
    assert accounts._claude_limits() == {}


def test_codex_limits_reads_session_tail(monkeypatch, tmp_path):
    sessions = tmp_path / "sessions" / "2026" / "07"
    sessions.mkdir(parents=True)
    log = sessions / "rollout-x.jsonl"
    stale = {"payload": {"rate_limits": {"primary": {"used_percent": 90.0}}}}
    fresh = {"payload": {"rate_limits": {
        "primary": {"used_percent": 14.0, "window_minutes": 300, "resets_at": 1_783_723_274},
        "secondary": {"used_percent": 2.0, "window_minutes": 10080, "resets_at": 1_784_310_074},
    }}}
    log.write_text(json.dumps(stale) + "\n" + json.dumps({"other": 1}) + "\n" + json.dumps(fresh) + "\n")
    monkeypatch.setattr(accounts, "CODEX_SESSIONS_DIR", tmp_path / "sessions")
    limits = accounts._codex_limits()
    assert limits["five_hour"] == {"used_percent": 14.0, "resets_at": 1_783_723_274.0}   # LAST event wins
    assert limits["weekly"]["used_percent"] == 2.0
    monkeypatch.setattr(accounts, "CODEX_SESSIONS_DIR", tmp_path / "missing")
    assert accounts._codex_limits() == {}


# --- pinned-board integration ---------------------------------------------------


def test_pinned_boards_include_account_line(monkeypatch, tmp_path):
    monkeypatch.setenv("HERDRES_TENDWIRE_MODE", "source")
    monkeypatch.setenv("HERDRES_SOURCE_TOPIC_MODE", "worker")
    monkeypatch.setenv("HERDRES_PINNED_ACCOUNT", "1")                          # opt back in (conftest defaults off)
    creds = tmp_path / "creds.json"
    creds.write_text(json.dumps({"claudeAiOauth": {"subscriptionType": "max",
                                                    "rateLimitTier": "default_claude_max_20x"}}))
    monkeypatch.setattr(accounts, "CLAUDE_CREDENTIALS_PATH", creds)
    monkeypatch.setattr(accounts, "CODEX_AUTH_PATH", tmp_path / "no-codex-auth.json")
    monkeypatch.setattr(accounts, "usage_snapshot",
                        lambda **_: {"fetched_at": 0.0, "claude": {"five_hour": {"used_percent": 24.0}}})
    telegram = FakeTelegram()
    store = _store()
    tendwire = FakeTendwire(
        workers=[{"id": "claude-1", "name": "Gitmoot", "status": "working", "space_id": "space-1",
                  "fingerprint": "fp-1", "meta": {"agent": "claude"}}],
        turns={"turns": [
            {"id": "t1", "worker_id": "claude-1", "worker_fingerprint": "fp-1",
             "assistant_final_text": "done", "complete": True},
        ]},
    )
    sync_once(store, SyncRuntime(tendwire, telegram, with_outbox=False))
    assert state.source_worker_entries(store), "sync did not create a worker entry"
    boards = [html for (_chat, html, _kwargs, _mid) in telegram.sent if "🔑" in html]
    assert boards, f"no board carried the account line; sent={[s[1][:80] for s in telegram.sent]}"
    assert any("Max 20x" in html and "5h 76% left" in html for html in boards)


def test_pinned_account_disabled_means_no_line(monkeypatch):
    monkeypatch.setenv("HERDRES_TENDWIRE_MODE", "source")
    monkeypatch.setenv("HERDRES_SOURCE_TOPIC_MODE", "worker")
    monkeypatch.setenv("HERDRES_PINNED_ACCOUNT", "0")
    monkeypatch.setattr(accounts, "usage_snapshot",
                        lambda **_: (_ for _ in ()).throw(AssertionError("must not probe usage when disabled")))
    telegram = FakeTelegram()
    tendwire = FakeTendwire(turns={"turns": [
        {"id": "t1", "worker_id": "claude-1", "worker_fingerprint": "fp-1",
         "assistant_final_text": "done", "complete": True},
    ]})
    sync_once(_store(), SyncRuntime(tendwire, telegram, with_outbox=False))
    assert not any("🔑" in html for (_chat, html, _kwargs, _mid) in telegram.sent)
