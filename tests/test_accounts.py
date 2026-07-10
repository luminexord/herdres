"""The pinned who-am-I/usage line: plan metadata comes from the CLI credential files
(named fields only — tokens must never surface), usage from ccusage behind a disk-cached
TTL (the pin-edit rate limiter), rendered as a compact footer on the pinned boards."""
from __future__ import annotations

import base64
import json
from datetime import datetime, timezone

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
    end = datetime.fromtimestamp(now + 100 * 60, tz=timezone.utc).isoformat()
    snapshot = {"claude": {"today_cost": 592.88, "today_tokens": 411_792_393,
                           "block_cost": 37.2, "block_end": end}}
    line = accounts.account_line("claude", snapshot=snapshot, now=now)
    assert line == "🔑 Max 20x · block $37 (1h40m) · today $593 · 412M tok"
    assert "SECRET" not in line


def test_account_line_codex_format(monkeypatch, tmp_path):
    auth = tmp_path / "auth.json"
    auth.write_text(json.dumps({"auth_mode": "chatgpt", "tokens": {
        "id_token": _fake_jwt({"https://api.openai.com/auth": {"chatgpt_plan_type": "pro"}})}}))
    monkeypatch.setattr(accounts, "CODEX_AUTH_PATH", auth)
    snapshot = {"codex": {"today_cost": 371.44, "today_tokens": 215_249_312}}
    assert accounts.account_line("codex-1", snapshot=snapshot, now=0.0) == "🔑 ChatGPT Pro · today $371 · 215M tok"


def test_account_line_degrades_gracefully(monkeypatch, tmp_path):
    monkeypatch.setattr(accounts, "CLAUDE_CREDENTIALS_PATH", tmp_path / "nope.json")
    assert accounts.account_line("kimi", snapshot={}, now=0.0) == ""          # unknown kind
    assert accounts.account_line("claude", snapshot={}, now=0.0) == ""        # no identity, no usage
    # usage without identity still renders the meter
    assert accounts.account_line("claude", snapshot={"claude": {"today_cost": 3.2}}, now=0.0) == "🔑 today $3"


def test_block_left_rounds_to_five_minutes():
    now = 1_700_000_000.0
    end = datetime.fromtimestamp(now + 47 * 60, tz=timezone.utc).isoformat()
    assert accounts._fmt_block_left(end, now) == "45m"                        # 47 -> 45, damps pin churn
    past = datetime.fromtimestamp(now - 60, tz=timezone.utc).isoformat()
    assert accounts._fmt_block_left(past, now) == ""
    assert accounts._fmt_block_left("garbage", now) == ""


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
    monkeypatch.setattr(accounts, "usage_snapshot", lambda **_: {"fetched_at": 0.0, "claude": {"today_cost": 5.0}})
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
    assert any("Max 20x" in html and "today $5" in html for html in boards)


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
