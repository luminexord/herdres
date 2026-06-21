#!/usr/bin/env python3
"""Sync Herdr panes to Telegram forum topics and handle pane-topic commands.

This is intentionally small and stdlib-only. Routine sync uses no LLM calls.
Secrets are read from environment/.env files, never persisted.
"""

from __future__ import annotations

import argparse
import datetime as _dt
import fcntl
import getpass
import hashlib
import html
import json
import os
import re
import shlex
import subprocess
import sys
import tarfile
import tempfile
import threading
import http.client
import time
import urllib.error
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Callable, Iterator


HERDRES_VERSION = "0.3.0"

DEFAULT_STATE = Path.home() / ".local/share/herdres/state.json"
DEFAULT_ENV = Path(os.getenv("HERDRES_ENV", str(Path.home() / ".config/herdres/herdres.env"))).expanduser()
DEFAULT_HERMES_ENV = Path.home() / ".hermes/.env"
DEFAULT_LOCK = Path.home() / ".local/share/herdres/sync.lock"
DEFAULT_CHAT_ID = ""
DEFAULT_GENERAL_THREAD_ID = "1"
DEFAULT_OWNER_ID = ""
DEFAULT_HERDR_BIN = "herdr"
DEFAULT_HERDR_TOPIC_ICON_COLOR = "9367192"  # 0x8EEE98, one of Telegram's allowed forum-topic colors.


def parse_bool_env(key: str, default: str = "1") -> bool:
    """Parse a boolean environment variable.

    Accepts 1/true/yes/on (case-insensitive) as truthy; everything else is
    falsy. Centralizes the pattern repeated across feature flags.
    """
    return os.getenv(key, default).lower() in {"1", "true", "yes", "on"}


MAX_CREATES_PER_RUN = int(os.getenv("HERDR_TELEGRAM_TOPICS_MAX_CREATES", "3"))
MAX_SENDS_PER_RUN = int(os.getenv("HERDR_TELEGRAM_TOPICS_MAX_SENDS", "8"))
MAX_STATUS_MARKERS_PER_RUN = int(os.getenv("HERDR_TELEGRAM_TOPICS_MAX_STATUS_MARKERS", "8"))
READ_LINES_STATUS = int(os.getenv("HERDR_TELEGRAM_TOPICS_STATUS_READ_LINES", "40"))
READ_LINES_COMMAND_DEFAULT = 80
READ_LINES_COMMAND_MAX = 160
MAX_REPLY_CHARS = 3200
MAX_STATUS_CHARS = 1500
MAX_RICH_HTML_CHARS = int(os.getenv("HERDR_TELEGRAM_TOPICS_RICH_MAX_CHARS", "14000"))
# Telegram clients render rich messages only up to a size limit; past it the
# whole message shows "This message is not supported in your version of
# Telegram" (the API accepts it regardless, so there's no error to catch).
# Empirically ~7KB of HTML still renders, so split well under that and deliver
# long content as multiple messages instead of one that silently breaks.
RICH_SAFE_CHARS = int(os.getenv("HERDR_TELEGRAM_TOPICS_RICH_SAFE_CHARS", "6000"))
MAX_RICH_DETAIL_CHARS = int(os.getenv("HERDR_TELEGRAM_TOPICS_RICH_DETAIL_CHARS", "2400"))
PREFLIGHT_TTL_SECONDS = int(os.getenv("HERDR_TELEGRAM_TOPICS_PREFLIGHT_TTL", "900"))
PREFLIGHT_GRACE_SECONDS = int(os.getenv("HERDR_TELEGRAM_TOPICS_PREFLIGHT_GRACE", "86400"))
TOPIC_VERIFY_TTL_SECONDS = int(os.getenv("HERDR_TELEGRAM_TOPICS_VERIFY_TTL", "900"))
MAX_TOPIC_VERIFIES_PER_RUN = int(os.getenv("HERDR_TELEGRAM_TOPICS_MAX_VERIFIES", "3"))
HERDR_TOPIC_ICON_COLOR = int(os.getenv("HERDR_TELEGRAM_TOPICS_ICON_COLOR", DEFAULT_HERDR_TOPIC_ICON_COLOR))
HERDR_TOPIC_ICON_CUSTOM_EMOJI_ID = os.getenv("HERDR_TELEGRAM_TOPICS_ICON_CUSTOM_EMOJI_ID", "").strip()
STATUS_ICON_ENABLED = parse_bool_env("HERDR_TELEGRAM_TOPICS_STATUS_ICON", "1")
STATUS_ICON_CACHE_TTL_SECONDS = int(os.getenv("HERDR_TELEGRAM_TOPICS_STATUS_ICON_CACHE_TTL", "86400"))
STATUS_ICON_RETRY_SECONDS = int(os.getenv("HERDR_TELEGRAM_TOPICS_STATUS_ICON_RETRY", "30"))
STATUS_MARKER_SUPPRESS_WHEN_ICON_OK = parse_bool_env(
    "HERDR_TELEGRAM_TOPICS_STATUS_MARKER_SUPPRESS_WHEN_ICON_OK", "1"
)
CLEAN_FEED_ENABLED = parse_bool_env("HERDR_TELEGRAM_TOPICS_CLEAN_FEED", "1")
TURN_FEED_ENABLED = parse_bool_env("HERDR_TELEGRAM_TOPICS_TURN_FEED", "1")
STREAMING_DRAFTS_ENABLED = parse_bool_env("HERDR_TELEGRAM_TOPICS_STREAMING", "1")
STREAM_MIN_INTERVAL_SECONDS = float(os.getenv("HERDR_TELEGRAM_TOPICS_STREAM_MIN_INTERVAL", "2"))
STREAM_MIN_CHARS = int(os.getenv("HERDR_TELEGRAM_TOPICS_STREAM_MIN_CHARS", "80"))
MAX_STREAM_DRAFTS = int(os.getenv("HERDR_TELEGRAM_TOPICS_MAX_DRAFTS", "8"))
MANAGED_BOTS_ENABLED = parse_bool_env("HERDR_TELEGRAM_TOPICS_MANAGED_BOTS", "0")
MANAGER_BOT_KIND = "manager"
MANAGER_BOT_COMMANDS = [
    ("status", "Latest report or question"),
    ("report", "Latest clean report"),
    ("choices", "Resend active choices"),
    ("raw", "Sanitized raw visible output"),
    ("send", "Send text to a pane"),
    ("keys", "Send explicit keystrokes"),
    ("agents", "Pick which agent to address"),
    ("voice", "Switch shared/per-agent voice"),
    ("new", "Start a new agent pane"),
    ("debug", "Pane mapping details"),
    ("help", "Show commands"),
]
MANAGED_BOT_ROUTE_KIND_FIELDS = (
    "pane_root_bot_kind",
    "status_marker_bot_kind",
    "last_clean_bot_kind",
    "last_stream_bot_kind",
    "last_prompt_bot_kind",
)
MANAGED_BOT_REISSUE_RETRY_SECONDS = int(os.getenv("HERDR_TELEGRAM_TOPICS_MANAGED_BOT_REISSUE_RETRY_SECONDS", "300"))
DEVIN_GLM_SEAT_DEFAULT_MODEL = "glm-5.2"
DEVIN_GLM_SEAT_DEFAULT_PERMISSION_MODE = "dangerous"
DEVIN_GLM_SEAT_DEFAULT_LABEL = "GLM Devin"
DEVIN_GLM_SEAT_PENDING_TTL_SECONDS = int(os.getenv("HERDR_TELEGRAM_TOPICS_DEVIN_GLM_SEAT_PENDING_TTL", "1800"))
DEVIN_GLM_SEAT_ERROR_RETRY_SECONDS = int(os.getenv("HERDR_TELEGRAM_TOPICS_DEVIN_GLM_SEAT_ERROR_RETRY", "300"))
DEVIN_SUPPORTED_MODELS: tuple[str, ...] = (
    "adaptive",
    "claude-haiku-4.5",
    "claude-opus-4.5",
    "claude-opus-4.6",
    "claude-opus-4.7",
    "claude-opus-4.8",
    "claude-sonnet-4.5",
    "claude-sonnet-4.6",
    "deepseek-v4-pro",
    "gemini-3-flash",
    "gemini-3.1-pro",
    "gemini-3.5-flash",
    "glm-5.1",
    "glm-5.2",
    "gpt-5.2",
    "gpt-5.3-codex",
    "gpt-5.4",
    "gpt-5.4-mini",
    "gpt-5.5",
    "kimi-k2.6",
    "kimi-k2.7",
    "swe-1.5",
    "swe-1.6",
    "swe-1.6-fast",
)
def devin_model_env_prefix(model: str) -> str:
    clean = re.sub(r"[^A-Za-z0-9]+", "_", str(model or "").strip()).strip("_").upper()
    return f"DEVIN_{clean or 'MODEL'}"


def devin_model_default_label(model: str) -> str:
    clean = str(model or "").strip()
    family_labels = {
        "adaptive": "Adaptive",
        "claude": "Claude",
        "deepseek": "DeepSeek",
        "gemini": "Gemini",
        "glm": "GLM",
        "gpt": "GPT",
        "kimi": "Kimi",
        "swe": "SWE",
    }
    family = re.split(r"[-_]+", clean, maxsplit=1)[0].lower()
    return f"{family_labels.get(family, clean or 'Model')} Devin"


def devin_model_alias(model: str, *, label: str | None = None, env: str | None = None) -> dict[str, str]:
    clean = str(model or "").strip()
    return {
        "model": clean,
        "label": label or devin_model_default_label(clean),
        "env": env or devin_model_env_prefix(clean),
    }


DEVIN_MODEL_ALIASES: dict[str, dict[str, str]] = {
    model: devin_model_alias(model)
    for model in DEVIN_SUPPORTED_MODELS
}
DEVIN_MODEL_ALIASES.update({
    "glm": devin_model_alias("glm-5.2", label="GLM Devin", env="DEVIN_GLM"),
    "glm-5.2": devin_model_alias("glm-5.2", label="GLM Devin", env="DEVIN_GLM"),
    "glm5.2": devin_model_alias("glm-5.2", label="GLM Devin", env="DEVIN_GLM"),
    "glm-devin": devin_model_alias("glm-5.2", label="GLM Devin", env="DEVIN_GLM"),
    "kimi": devin_model_alias("kimi-k2.7", env="DEVIN_KIMI"),
    "kimi-k2.7": devin_model_alias("kimi-k2.7", env="DEVIN_KIMI"),
    "kimi-k27": devin_model_alias("kimi-k2.7", env="DEVIN_KIMI"),
    "kimi-devin": devin_model_alias("kimi-k2.7", env="DEVIN_KIMI"),
    "opus": devin_model_alias("claude-opus-4.8"),
    "claude-opus": devin_model_alias("claude-opus-4.8"),
    "gpt": devin_model_alias("gpt-5.5"),
    "gemini": devin_model_alias("gemini-3.1-pro"),
    "deepseek": devin_model_alias("deepseek-v4-pro"),
    "swe": devin_model_alias("swe-1.6"),
})
RICH_MESSAGES_ENABLED = parse_bool_env("HERDR_TELEGRAM_TOPICS_RICH_MESSAGES", "1")
RICH_BAD_REQUEST_LIMIT = int(os.getenv("HERDR_TELEGRAM_TOPICS_RICH_BAD_REQUEST_LIMIT", "3"))
LIVE_CARD_ENABLED = parse_bool_env("HERDR_TELEGRAM_TOPICS_LIVE_CARD", "0")
STATUS_MARKER_ENABLED = parse_bool_env("HERDR_TELEGRAM_TOPICS_STATUS_MARKER", "0")
PANE_ROOT_MESSAGES_ENABLED = parse_bool_env("HERDR_TELEGRAM_TOPICS_PANE_ROOT_MESSAGES", "0")
PINNED_STATUS_ENABLED = parse_bool_env("HERDR_TELEGRAM_TOPICS_PINNED_STATUS", "1")
# Per-agent topic grouping is read at runtime via per_agent_topics_enabled(),
# NOT as an import-time constant: the Herdr plugin and bare CLI invocations have
# no systemd EnvironmentFile, so the flag must be read after load_dotenv() runs
# (see per_agent_topics_enabled()).
VISIBLE_CHOICE_BUTTONS_ENABLED = parse_bool_env("HERDR_TELEGRAM_TOPICS_VISIBLE_CHOICE_BUTTONS", "0")
VISIBLE_READONLY_PROMPTS_ENABLED = parse_bool_env("HERDR_TELEGRAM_TOPICS_VISIBLE_READONLY_PROMPTS", "1")
LEGACY_CHOICES_ENABLED = parse_bool_env("HERDR_TELEGRAM_TOPICS_LEGACY_CHOICES", "0")
STRUCTURED_INTERACTIONS_ENABLED = parse_bool_env("HERDR_TELEGRAM_TOPICS_STRUCTURED_INTERACTIONS", "1")
STATUS_MARKER_DELETE_OLD = parse_bool_env("HERDR_TELEGRAM_TOPICS_STATUS_MARKER_DELETE_OLD", "1")
ALLOW_UNBOUNDED_REPORTS = parse_bool_env("HERDR_TELEGRAM_TOPICS_UNBOUNDED_REPORTS", "0")
RICH_RENDER_VERSION = 20
USER_PROMPT_LABEL = "User:"
WORKLOG_LABEL = "Worklog"
RESPONSE_LABEL = "Response"
FEED_READ_LINES = int(os.getenv("HERDR_TELEGRAM_TOPICS_FEED_READ_LINES", "140"))
FEED_MAX_CHARS = int(os.getenv("HERDR_TELEGRAM_TOPICS_FEED_MAX_CHARS", "9000"))
FINAL_REPLY_MAX_CHARS = int(os.getenv("HERDR_TELEGRAM_TOPICS_FINAL_REPLY_MAX_CHARS", "16000"))
FINAL_REPLY_MAX_LINES = int(os.getenv("HERDR_TELEGRAM_TOPICS_FINAL_REPLY_MAX_LINES", "140"))
USER_PROMPT_MAX_CHARS = int(os.getenv("HERDR_TELEGRAM_TOPICS_USER_PROMPT_MAX_CHARS", "1200"))
# Default per-space prompt-collapse threshold (see space_prompt_collapse_chars):
# 0 = never collapse, 1 = always collapse, N>1 = collapse when the prompt exceeds N
# chars. Default to a threshold so long agent-to-agent echoes collapse while short
# human one-liners stay expanded.
PROMPT_COLLAPSE_CHARS_DEFAULT = int(os.getenv("HERDR_TELEGRAM_TOPICS_PROMPT_COLLAPSE_CHARS", "200"))
PROMPT_PREVIEW_CHARS = int(os.getenv("HERDR_TELEGRAM_TOPICS_PROMPT_PREVIEW_CHARS", "60"))
INTERACTION_READONLY_WARNING_TITLE = "⚠️ Manual action required"
INTERACTION_READONLY_WARNING_BODY = (
    "This structured prompt cannot be answered from Telegram yet. Open Herdr and answer it there."
)
DETAIL_REPLY_TIMEOUT_SECONDS = int(os.getenv("HERDR_TELEGRAM_TOPICS_DETAIL_TIMEOUT", "1800"))
ACTIVE_PROMPT_TTL_SECONDS = int(os.getenv("HERDR_TELEGRAM_TOPICS_ACTIVE_PROMPT_TTL", "900"))
ACTIVE_PANE_TTL_SECONDS = int(os.getenv("HERDR_TELEGRAM_TOPICS_ACTIVE_PANE_TTL", "600"))
# Cooldown before re-attempting a failed multibot-offer card, so an undeliverable
# space (deleted topic / kicked bot) can't burn a send slot every sync.
MULTIBOT_OFFER_RETRY_SECONDS = int(os.getenv("HERDR_TELEGRAM_TOPICS_MULTIBOT_OFFER_RETRY", "3600"))
CLEAN_ATTEMPT_TTL_SECONDS = int(os.getenv("HERDR_TELEGRAM_TOPICS_CLEAN_ATTEMPT_TTL", "1800"))
PANE_INPUT_FILE_CHARS = int(os.getenv("HERDR_TELEGRAM_TOPICS_INPUT_FILE_CHARS", "1200"))
PANE_INPUT_FILE_LINES = int(os.getenv("HERDR_TELEGRAM_TOPICS_INPUT_FILE_LINES", "6"))
PANE_INPUT_FILE_MAX_CHARS = int(os.getenv("HERDR_TELEGRAM_TOPICS_INPUT_FILE_MAX_CHARS", "120000"))
# Wall-time budget for one send_to_pane() call. Kept well under the gateway
# COMMAND_TIMEOUT (herdres_gateway.py: 60s) so the function self-bounds and
# returns a real reply before the gateway SIGKILLs the subprocess. Invariant:
# BUDGET + PER_CALL_CAP <= COMMAND_TIMEOUT - margin (40 + 5 = 45 <= 60 - 15).
SEND_TO_PANE_BUDGET_SECONDS = float(os.getenv("HERDR_TELEGRAM_TOPICS_SEND_BUDGET", "40"))
SEND_TO_PANE_PER_CALL_CAP = float(os.getenv("HERDR_TELEGRAM_TOPICS_SEND_CALL_CAP", "5"))
# Never START a new herdr call (esp. the main `pane run`) with less than this many
# seconds left — reserve enough for one full-cap call so the actual delivery runs.
SEND_TO_PANE_MIN_CALL_SECONDS = SEND_TO_PANE_PER_CALL_CAP
# Inbound Telegram attachments (documents/photos). 20MB is the Bot API getFile
# hard ceiling; the download timeout is kept under the bridge's 25s subprocess
# kill so a slow/huge fetch fails cleanly instead of wedging the state lock.
ATTACHMENT_MAX_BYTES = int(os.getenv("HERDR_TELEGRAM_TOPICS_ATTACHMENT_MAX_BYTES", str(20 * 1024 * 1024)))
ATTACHMENT_FILE_HOST = os.getenv("HERDR_TELEGRAM_TOPICS_FILE_HOST", "https://api.telegram.org")
# Socket timeout AND a wall-clock total budget for the download, kept well under
# the bridge's 25s subprocess kill so a slow fetch fails cleanly (no orphan
# "complete" file: we stream to a .part and atomically rename only on success).
ATTACHMENT_DOWNLOAD_TIMEOUT = int(os.getenv("HERDR_TELEGRAM_TOPICS_ATTACHMENT_TIMEOUT", "12"))
# Per-read socket timeout, kept below the total budget so a single stalled read
# cannot overrun it: worst case ~= DOWNLOAD_TIMEOUT + READ_TIMEOUT, still < 25s.
ATTACHMENT_READ_TIMEOUT = int(os.getenv("HERDR_TELEGRAM_TOPICS_ATTACHMENT_READ_TIMEOUT", "8"))
ATTACHMENT_CHUNK_BYTES = 65536
ATTACHMENT_KEEP_PER_PANE = int(os.getenv("HERDR_TELEGRAM_TOPICS_ATTACHMENT_KEEP", "20"))
VISIBLE_CHOICE_SELECT_MODE = os.getenv("HERDR_TELEGRAM_TOPICS_VISIBLE_CHOICE_SELECT_MODE", "number").strip().lower()
VISIBLE_CHOICE_NUMBER_ENTER = parse_bool_env("HERDR_TELEGRAM_TOPICS_VISIBLE_CHOICE_NUMBER_ENTER", "1")
VISIBLE_CHOICE_VERIFY_SECONDS = float(os.getenv("HERDR_TELEGRAM_TOPICS_VISIBLE_CHOICE_VERIFY_SECONDS", "2.5"))
EVENT_SETTLE_SECONDS = float(os.getenv("HERDR_TELEGRAM_TOPICS_EVENT_SETTLE_SECONDS", "4"))
EVENT_SETTLE_INTERVAL_SECONDS = float(os.getenv("HERDR_TELEGRAM_TOPICS_EVENT_SETTLE_INTERVAL", "0.75"))
DUPLICATE_TOPIC_DELETE_LIMIT = int(os.getenv("HERDR_TELEGRAM_TOPICS_DUPLICATE_DELETE_LIMIT", "12"))
AUTO_FEED_SOURCES = ("recent-unwrapped",)
MANUAL_FEED_SOURCES = ("recent-unwrapped", "transcript", "visible")

SECRET_PATTERNS = [
    re.compile(r"(?i)\b(bot_token|token|api[_-]?key|secret|password|passwd|authorization)\s*[:=]\s*([^\s]+)"),
    re.compile(r"(?i)\bBearer\s+[A-Za-z0-9._~+/\-]+=*"),
    re.compile(r"\b\d{6,}:[A-Za-z0-9_-]{20,}\b"),
    re.compile(r"(?i)bot\d{6,}:[A-Za-z0-9_-]{20,}"),
    re.compile(r"\bAKIA[0-9A-Z]{16}\b"),
    re.compile(r"\b[A-Za-z0-9+/]{40,}={0,2}\b"),
    re.compile(r"(?i)([?&](?:access_token|api[_-]?key|auth[_-]?token|token|signature|sig)=)([^&#\s]+)"),
]

ANSI_RE = re.compile(r"\x1b\[[0-9;?]*[ -/]*[@-~]")
TUI_LEADING_CHROME_RE = re.compile(r"^\s*[│┃└┌┐┘├┤╭╮╰╯⎿]\s*")
PROMPT_ONLY_RE = re.compile(r"^\s*(?:❯|›)\s*$")
PROMPT_WITH_TEXT_RE = re.compile(r"^\s*(?:❯|›)\s+\S+")
PROMPT_PLACEHOLDER_RE = re.compile(r"^\s*(?:❯|›)\s+Write tests for @filename\s*$", re.IGNORECASE)
CODEX_GOAL_USAGE_FOOTER_RE = re.compile(r"\bgoal\s+hit\s+usage limits?\b", re.IGNORECASE)
REPORT_BLOCK_RE = re.compile(r"(?ms)^\s*HERDRES_REPORT_START\s*$\s*(.*?)^\s*HERDRES_REPORT_END\s*$")
CHOICES_BLOCK_RE = re.compile(r"(?ms)^\s*HERDRES_CHOICES_START\s*$\s*(.*?)^\s*HERDRES_CHOICES_END\s*$")
REPORT_TITLE_RE = re.compile(r"^\s*HERDRES_REPORT_TITLE\s*:\s*(.{1,80})\s*$", re.IGNORECASE)
BAD_TITLE_WORDS_RE = re.compile(
    r"\b(first non-empty|becomes|because|should|could|would|which|that|etc)\b",
    re.IGNORECASE,
)
ACTION_QUESTION_RE = re.compile(
    r"(?i)\b("
    r"should\s+(?:i|we)\b|"
    r"do you want me to\b|"
    r"would you like(?: me)? to\b|"
    r"want me to\b|"
    r"approve\b|"
    r"choose\b|"
    r"select\b|"
    r"proceed\b|"
    r"continue\?\s*$|"
    r"deploy\?\s*$|"
    r"run it\?\s*$"
    r")",
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
STRUCTURED_SECTION_RE = re.compile(r"^\s*([A-Za-z][A-Za-z ]{0,40})\s*:\s*(.*?)\s*$")
INLINE_CODE_RE = re.compile(r"`([^`\n]{1,300})`")
COMMIT_LINE_RE = re.compile(r"^`?([0-9a-f]{7,12})\s+(.+?)`?$", re.IGNORECASE)
FENCE_START_RE = re.compile(r"^\s*(`{3,}|~{3,})\s*([A-Za-z0-9_+-]{0,32})\s*$")
# Claude Code working-spinner status line, e.g. "✻ Baked for 4m 47s",
# "✻ Brewed for 28s", or "✻ Brewing… (4s · esc to interrupt)". It rotates
# through many verbs and the duration can be multi-unit, so match the SHAPE.
# Anchored on the spinner GLYPH (matched against the raw, ANSI-stripped line)
# so legitimate prose/bullets like "Waited for 3s" or "- Compiled for 2m" are
# NOT swallowed. The working-status gate is the primary defense; this only
# keeps a stray spinner out of a genuine prompt scrape.
SPINNER_GLYPHS = "✶✷✸✹✺✻✼✽✾"  # Claude Code spinner star family (NOT decorative bullets)
SPINNER_STATUS_RE = re.compile(
    rf"^[{SPINNER_GLYPHS}]\s+\S.*?\besc to interrupt\b[)\s]*$"
    rf"|^[{SPINNER_GLYPHS}]\s+\S+\s+for\s+\d+\s*[smhd](?:\s+\d+\s*[smhd])*\s*$",
    re.IGNORECASE,
)
# Agent statuses that mean the pane is actively producing output (never a
# genuine awaiting-input prompt) — used to suppress visible-screen scraping.
ACTIVE_AGENT_STATUSES = {"working", "running", "active", "in_progress", "pending"}
# Footer marker the agent shows near the input while pursuing a goal, e.g.
# "◎ /goal active (3h)". Match the literal slash-marker (not bare "goal active")
# so prose can't trigger a false positive. Used only to give an idle-but-mid-goal
# pane a distinct topic icon. "Goal achieved" (done) deliberately does NOT match.
GOAL_ACTIVE_RE = re.compile(r"/goal active\b", re.IGNORECASE)
# Number of footer (tail) lines of the visible screen to scan for the marker.
GOAL_MARKER_READ_LINES = int(os.getenv("HERDR_TELEGRAM_TOPICS_GOAL_MARKER_LINES", "16"))
MANAGED_BOT_REQUEST_BASE_ID = 41000
MANAGED_BOT_SPECS: dict[str, dict[str, Any]] = {
    "codex": {
        "request_id": MANAGED_BOT_REQUEST_BASE_ID + 1,
        "label": "Codex",
        "name": "Herdr Codex",
        "suggested_username": "herdr_codex_bot",
        "description": "Herdr Codex pane bot for Herdres.",
        "short_description": "Codex panes in Herdr.",
        "aliases": ("codex", "gpt", "openai"),
    },
    "claude": {
        "request_id": MANAGED_BOT_REQUEST_BASE_ID + 2,
        "label": "Claude",
        "name": "Herdr Claude",
        "suggested_username": "herdr_claude_bot",
        "description": "Herdr Claude pane bot for Herdres.",
        "short_description": "Claude panes in Herdr.",
        "aliases": ("claude", "anthropic"),
    },
    "kimi": {
        "request_id": MANAGED_BOT_REQUEST_BASE_ID + 3,
        "label": "Kimi",
        "name": "Herdr Kimi",
        "suggested_username": "herdr_kimi_bot",
        "description": "Herdr Kimi pane bot for Herdres.",
        "short_description": "Kimi panes in Herdr.",
        "aliases": ("kimi", "moonshot"),
    },
    "omp": {
        "request_id": MANAGED_BOT_REQUEST_BASE_ID + 4,
        "label": "OMP",
        "name": "Herdr OMP",
        "suggested_username": "herdr_omp_bot",
        "description": "Herdr OMP pane bot for Herdres.",
        "short_description": "OMP panes in Herdr.",
        "aliases": ("omp",),
    },
    "devin": {
        "request_id": MANAGED_BOT_REQUEST_BASE_ID + 5,
        "label": "Devin",
        "name": "Herdr Devin",
        "suggested_username": "herdr_devin_bot",
        "description": "Herdr Devin pane bot for Herdres.",
        "short_description": "Devin panes in Herdr.",
        "aliases": ("devin", "cognition"),
    },
    "glm": {
        "request_id": MANAGED_BOT_REQUEST_BASE_ID + 6,
        "label": "GLM Devin",
        "name": "Guremi",
        "suggested_username": "Guremi_bot",
        "description": "GLM Devin pane bot for Herdres, running through Devin.",
        "short_description": "GLM Devin panes.",
        "aliases": ("glm", "glm5", "glm5.2", "glm-5.2", "guremi"),
    },
}
NEW_PANE_AGENT_COMMANDS = {kind: kind for kind in MANAGED_BOT_SPECS}
CODE_FILE_EXTENSIONS = ("py", "json", "toml", "service", "timer", "sh", "md", "txt", "yaml", "yml")
PATH_OR_SYMBOL_FILE_EXTENSIONS = (
    "py", "js", "ts", "tsx", "jsx", "json", "md", "txt", "yaml", "yml", "toml", "sh", "service", "timer"
)
CODE_FILE_EXT_RE = "|".join(CODE_FILE_EXTENSIONS)
PATH_OR_SYMBOL_FILE_EXT_RE = "|".join(PATH_OR_SYMBOL_FILE_EXTENSIONS)
CODE_PATH_BRANCH = r"(?:~|/)[A-Za-z0-9_.+-]+(?:/[A-Za-z0-9_.+-]+)+(?::\d+)?"
CODE_SYMBOL_BRANCH = r"[A-Z][A-Z0-9_]*[_0-9][A-Z0-9_]*"
CODE_FILE_BRANCH = rf"[A-Za-z_][A-Za-z0-9_]*\.(?:{CODE_FILE_EXT_RE})(?::\d+)?"
# Env-style assignment ONLY (uppercase key, >=2 chars): DEBUG=true, FOO_BAR=1.
# NOT statistical notation like N=16, f=2, w=15, p=0.05 (common in prose).
CODE_ENV_ASSIGN_BRANCH = r"[A-Z][A-Z0-9_]+=\S+"
CODE_API_BRANCH = r"(?:sendRichMessage|editForumTopic|editMessageText|createForumTopic)"
INLINE_CODE_HASH_BRANCH = r"[0-9a-f]{7,12}"
SYMBOL_CODE_HASH_BRANCH = r"[0-9a-f]{7,40}"
TOKEN_CODE_RE = re.compile(
    r"(?<![\w/])("
    rf"{CODE_PATH_BRANCH}|"
    rf"\b{CODE_SYMBOL_BRANCH}\b|"
    rf"\b{CODE_FILE_BRANCH}\b|"
    rf"\b{CODE_ENV_ASSIGN_BRANCH}|"
    rf"\b{CODE_API_BRANCH}\b|"
    rf"\b{INLINE_CODE_HASH_BRANCH}\b"
    r")(?![\w/])"
)
TOKEN_CODE_TRAILING_PUNCT_RE = re.compile(r"[.,;:!?)\]}]+$")
MANAGED_BOT_MENTION_RE = re.compile(r"@([A-Za-z0-9_]{3,64})")
# Markdown link / image / bare-URL / math spans, masked before inline-code detection
# so a URL query string or an equation is never shattered into a <code> box.
MD_IMAGE_RE = re.compile(r"!\[([^\]\n]*)\]\(\s*(https?://[^)\s]+|/[^)\s]+)\s*\)")
MD_LINK_RE = re.compile(r"\[([^\]\n]{1,400})\]\(\s*(https?://[^)\s]+|mailto:[^)\s]+)\s*\)")
BARE_URL_RE = re.compile(r"(?i)(?<![\"'>=/\w])(?:https?://|www\.)[^\s<>()\[\]]+[^\s<>()\[\].,;:!?'\"]")
MATH_SPAN_RE = re.compile(r"\\\([^\n]{1,400}?\\\)|\\\[[^\n]{1,400}?\\\]|\$\$[^\n]{1,400}?\$\$")
HRULE_RE = re.compile(r"^\s*([-*_])(?:[ \t]*\1){2,}[ \t]*$")
_PH_OPEN = chr(0xE000)
_PH_CLOSE = chr(0xE001)
_PH_RE = re.compile(_PH_OPEN + r"(\d+)" + _PH_CLOSE)
SECTION_ALIASES = {
    "summary": "summary",
    "short summary": "summary",
    "table": "table",
    "status": "table",
    "status table": "table",
    "metrics": "table",
    "checklist": "checklist",
    "deployment checklist": "checklist",
    "next": "checklist",
    "details": "details",
    "risks": "details",
    "proof": "details",
    "logs": "details",
    "commands": "details",
    "diff": "details",
    "footer": "footer",
    "meta": "footer",
}
CODE_DETAILS_SECTIONS = {"proof", "logs", "commands", "diff"}


class BridgeError(RuntimeError):
    pass


class RateLimited(BridgeError):
    def __init__(self, retry_after: int):
        super().__init__(f"Telegram rate limited; retry after {retry_after}s")
        self.retry_after = retry_after


def utc_now() -> str:
    return _dt.datetime.now(tz=_dt.timezone.utc).replace(microsecond=0).isoformat()


def parse_utc_datetime(value: str) -> _dt.datetime | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        parsed = _dt.datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=_dt.timezone.utc)
    return parsed.astimezone(_dt.timezone.utc)


def format_elapsed_minutes(total_seconds: float) -> str:
    safe_seconds = max(0, int(total_seconds))
    total_minutes = max(1, safe_seconds // 60)
    hours, minutes = divmod(total_minutes, 60)
    if hours <= 0:
        return f"{minutes}m"
    if minutes <= 0:
        return f"{hours}h"
    return f"{hours}h {minutes}m"


def request_elapsed_label(started_at: str, *, now: _dt.datetime | None = None) -> str:
    started = parse_utc_datetime(started_at)
    if started is None:
        return ""
    current = now or _dt.datetime.now(tz=_dt.timezone.utc)
    if current.tzinfo is None:
        current = current.replace(tzinfo=_dt.timezone.utc)
    elapsed_seconds = (current.astimezone(_dt.timezone.utc) - started).total_seconds()
    return format_elapsed_minutes(elapsed_seconds)


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


def per_agent_topics_enabled() -> bool:
    """Whether to map one Telegram topic per Herdr agent (pane) instead of per space.

    Read at call time rather than frozen as an import-time constant. Entry points
    (sync_once/event_once/command/callback/...) call load_dotenv() before touching
    grouping logic, so this reflects herdres.env even when the process has no
    systemd EnvironmentFile (the Herdr plugin runs `herdres event` directly). A
    module-level constant would be False under the plugin and would flip topic
    grouping back to per-space, collapsing all agents onto one topic.
    """
    return os.getenv("HERDR_TELEGRAM_TOPICS_PER_AGENT", "0").lower() in {"1", "true", "yes", "on"}


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
        "enabled": parse_bool_env("HERDR_TELEGRAM_TOPICS_ENABLED", "1"),
        "plugin_event_enabled": parse_bool_env("HERDR_TELEGRAM_TOPICS_PLUGIN_EVENTS", "1"),
        "telegram": {
            "chat_id": os.getenv("HERDR_TELEGRAM_TOPICS_CHAT_ID", DEFAULT_CHAT_ID),
            "general_thread_id": os.getenv("HERDR_TELEGRAM_TOPICS_GENERAL_THREAD_ID", DEFAULT_GENERAL_THREAD_ID),
            "owner_user_ids": owners,
            "implicit_send_enabled": False,
            "managed_bots": {},
        },
        "spaces": {},
        "panes": {},
        "created_at": utc_now(),
        "updated_at": utc_now(),
    }


def normalize_state(data: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(data, dict) or data.get("version") != 1:
        raise BridgeError("unsupported state schema")
    data.setdefault("enabled", True)
    data.setdefault("plugin_event_enabled", True)
    data.setdefault("telegram", {})
    data.setdefault("spaces", {})
    data.setdefault("panes", {})
    migrate_legacy_pane_topics(data)
    migrate_space_voice_mode(data)
    clear_disabled_visible_choice_state(data)
    return data


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
                return normalize_state(data)
            except Exception:
                pass
        backup = path.with_suffix(path.suffix + f".corrupt-{int(time.time())}.bak")
        try:
            path.replace(backup)
        except OSError:
            pass
        raise BridgeError(f"state file is corrupt: {exc}") from exc
    return normalize_state(data)


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
    out = ANSI_RE.sub("", out)
    out = re.sub(r"[\x00-\x08\x0b\x0c\x0e-\x1f]", " ", out)
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


def managed_bot_specs() -> dict[str, dict[str, Any]]:
    return MANAGED_BOT_SPECS


def managed_bot_setup_enabled() -> bool:
    return MANAGED_BOTS_ENABLED


def sync_env_managed_bot_tokens(telegram: dict[str, Any]) -> bool:
    bots = telegram.get("managed_bots") if isinstance(telegram.get("managed_bots"), dict) else {}
    if not isinstance(bots, dict):
        bots = {}
        telegram["managed_bots"] = bots
    changed = False
    for kind, spec in managed_bot_specs().items():
        env_prefix = f"HERDR_TELEGRAM_TOPICS_MANAGED_BOT_{kind.upper()}"
        token = os.getenv(f"{env_prefix}_TOKEN", "").strip()
        if not token:
            continue
        username = os.getenv(f"{env_prefix}_USERNAME", "").strip().lstrip("@")
        if not username:
            username = str(spec.get("suggested_username") or "").strip().lstrip("@")
        name = os.getenv(f"{env_prefix}_NAME", "").strip() or str(spec.get("name") or spec.get("label") or kind)
        current = bots.get(kind) if isinstance(bots.get(kind), dict) else {}
        if (
            str(current.get("token") or "") == token
            and str(current.get("username") or "") == username
            and current.get("enabled") is True
        ):
            continue
        bots[kind] = {
            "kind": kind,
            "username": username,
            "name": name,
            "token": token,
            "enabled": True,
            "source": "manual-env",
            "updated_at": utc_now(),
        }
        changed = True
    return changed


def space_voice_mode(state: dict[str, Any], pane_or_entry: dict[str, Any] | None) -> str:
    if not isinstance(pane_or_entry, dict):
        return "shared"
    key = str(pane_or_entry.get("space_key") or "")
    if not key:
        key = space_key(pane_or_entry)
    spaces = state.get("spaces") if isinstance(state.get("spaces"), dict) else {}
    space = spaces.get(key)
    if isinstance(space, dict):
        return str(space.get("voice_mode") or "shared")
    return "shared"


def managed_voice_enabled_for_space(state: dict[str, Any], pane_or_entry: dict[str, Any] | None) -> bool:
    return space_voice_mode(state, pane_or_entry) == "per_agent"


def space_prompt_collapse_chars(state: dict[str, Any], pane_or_entry: dict[str, Any] | None) -> int:
    """Per-space threshold controlling whether the echoed "User:" prompt block is
    collapsed by default in a turn message: 0 = never collapse (always expanded),
    1 = always collapse, N>1 = collapse only when the prompt is longer than N chars.
    Defaults to PROMPT_COLLAPSE_CHARS_DEFAULT (a threshold) so long agent-to-agent
    prompt echoes collapse while short human one-liners stay expanded."""
    default = PROMPT_COLLAPSE_CHARS_DEFAULT
    if not isinstance(pane_or_entry, dict):
        return default
    key = str(pane_or_entry.get("space_key") or "") or space_key(pane_or_entry)
    spaces = state.get("spaces") if isinstance(state.get("spaces"), dict) else {}
    space = spaces.get(key)
    if isinstance(space, dict) and space.get("prompt_collapse_chars") is not None:
        try:
            return max(0, int(space.get("prompt_collapse_chars")))
        except (TypeError, ValueError):
            return default
    return default


def refresh_entry_managed_voice(state: dict[str, Any], entry: dict[str, Any], pane: dict[str, Any] | None = None) -> None:
    if isinstance(entry, dict):
        entry["managed_voice_active"] = managed_voice_enabled_for_space(
            state,
            pane if isinstance(pane, dict) and pane.get("space_key") else entry,
        )


def managed_bot_newbot_url(manager_username: str, spec: dict[str, Any]) -> str:
    manager = str(manager_username or "").strip().lstrip("@")
    suggested = str(spec.get("suggested_username") or "").strip().lstrip("@")
    name = urllib.parse.quote(str(spec.get("name") or ""), safe="")
    return f"https://t.me/newbot/{manager}/{suggested}?name={name}"


def managed_bot_kinds_for_panes(panes: list[dict[str, Any]]) -> list[str]:
    seen: set[str] = set()
    for pane in panes:
        if str(pane.get("agent_status") or "").lower() == "closed":
            continue
        kind = managed_bot_kind_for_entry(pane, pane)
        if kind:
            seen.add(kind)
    return [kind for kind in managed_bot_specs() if kind in seen]


def devin_model_managed_bot_kind_from_label(value: str | None) -> str:
    text = str(value or "").strip().lower()
    if not text:
        return ""
    if "glm" in text or "guremi" in text:
        return "glm"
    if "kimi" in text:
        return "kimi"
    return ""


def pane_agent_status_label(pane: dict[str, Any]) -> str:
    kind = managed_bot_kind_for_agent(str(pane.get("agent") or ""))
    if kind == "devin":
        label = str(pane.get("label") or pane.get("name") or pane.get("title") or "").strip()
        if label and label.lower() not in {"devin", "herdr devin"}:
            return sanitize_text(label, 80)
    spec = managed_bot_specs().get(kind) if kind else None
    if spec:
        return str(spec.get("label") or kind.title())
    agent = clean_label_topic_title(str(pane.get("agent") or ""), fallback="")
    return agent or pane_thread_name(pane)


# NOTE: the per-space pin and the global dashboard now share render_pinned_status()
# (defined later, with the canonical severity dot/sort). The old divergent helpers
# pane_pinned_status_emoji / PINNED_STATUS_SORT_ORDER / pane_pinned_status_sort_key /
# space_pinned_status_text were removed in favor of that single renderer.


def open_panes_by_space(panes: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for pane in panes:
        if str(pane.get("agent_status") or "").strip().lower() == "closed":
            continue
        grouped.setdefault(space_key(pane), []).append(pane)
    return grouped


def managed_bot_setup_reply_markup(manager_username: str, *, kinds: list[str] | None = None) -> dict[str, Any]:
    selected = kinds if kinds is not None else list(managed_bot_specs().keys())
    buttons: list[dict[str, str]] = []
    for kind in selected:
        spec = managed_bot_specs().get(kind)
        if not spec:
            continue
        buttons.append({
            "text": str(spec.get("label") or spec.get("name") or "Bot"),
            "url": managed_bot_newbot_url(manager_username, spec),
        })
    return {"inline_keyboard": [buttons[index : index + 2] for index in range(0, len(buttons), 2)]}


def managed_bot_username_for_kind(telegram: dict[str, Any], kind: str) -> str:
    bots = telegram.get("managed_bots") if isinstance(telegram.get("managed_bots"), dict) else {}
    record = bots.get(kind) if isinstance(bots, dict) else None
    if isinstance(record, dict):
        username = str(record.get("username") or "").strip().lstrip("@")
        if username:
            return username
    spec = managed_bot_specs().get(kind) or {}
    return str(spec.get("suggested_username") or "").strip().lstrip("@")


def managed_bot_kind_for_username(telegram: dict[str, Any] | None, username: str) -> str:
    clean_username = str(username or "").strip().lstrip("@").lower()
    if not clean_username:
        return ""
    telegram_data = telegram if isinstance(telegram, dict) else {}
    bots = telegram_data.get("managed_bots") if isinstance(telegram_data.get("managed_bots"), dict) else {}
    for kind, spec in managed_bot_specs().items():
        candidates = {str(spec.get("suggested_username") or "").strip().lstrip("@").lower()}
        record = bots.get(kind) if isinstance(bots, dict) else None
        if isinstance(record, dict):
            candidates.add(str(record.get("username") or "").strip().lstrip("@").lower())
        if clean_username in candidates:
            return kind
    return ""


def mentioned_managed_bot_kind(telegram: dict[str, Any] | None, text: str) -> str:
    for match in MANAGED_BOT_MENTION_RE.finditer(str(text or "")):
        kind = managed_bot_kind_for_username(telegram, match.group(1))
        if kind:
            return kind
    return ""


def strip_managed_bot_mentions(telegram: dict[str, Any] | None, text: str) -> str:
    def replacement(match: re.Match[str]) -> str:
        if managed_bot_kind_for_username(telegram, match.group(1)):
            return ""
        return match.group(0)

    without_mentions = MANAGED_BOT_MENTION_RE.sub(replacement, str(text or ""))
    return re.sub(r"[ \t]{2,}", " ", without_mentions).strip()


def entry_matches_managed_bot_kind(entry: dict[str, Any], kind: str) -> bool:
    clean_kind = str(kind or "").strip().lower()
    if not clean_kind:
        return True
    return managed_bot_kind_for_entry(entry) == clean_kind


def managed_bot_group_url(username: str) -> str:
    clean_username = str(username or "").strip().lstrip("@")
    return f"https://t.me/{clean_username}?startgroup=herdres"


def managed_bot_group_access_reply_markup(telegram: dict[str, Any], kinds: list[str]) -> dict[str, Any]:
    buttons: list[dict[str, str]] = []
    for kind in kinds:
        spec = managed_bot_specs().get(kind)
        username = managed_bot_username_for_kind(telegram, kind)
        if not spec or not username:
            continue
        buttons.append({
            "text": f"Add {spec.get('label') or spec.get('name') or 'Bot'}",
            "url": managed_bot_group_url(username),
        })
    return {"inline_keyboard": [buttons[index : index + 2] for index in range(0, len(buttons), 2)]}


def managed_bot_request_keyboard(*, kinds: list[str] | None = None) -> dict[str, Any]:
    selected = kinds if kinds is not None else list(managed_bot_specs().keys())
    keyboard: list[list[dict[str, Any]]] = []
    for kind in selected:
        spec = managed_bot_specs().get(kind)
        if not spec:
            continue
        keyboard.append([
            {
                "text": f"Create {spec.get('label')} bot",
                "request_managed_bot": {
                    "request_id": int(spec.get("request_id") or 0),
                    "suggested_name": str(spec.get("name") or ""),
                    "suggested_username": str(spec.get("suggested_username") or ""),
                },
            }
        ])
    return {
        "keyboard": keyboard,
        "resize_keyboard": True,
        "one_time_keyboard": False,
        "is_persistent": True,
    }


def managed_bot_kind_for_agent(value: str | None) -> str:
    text = re.sub(r"[^a-z0-9]+", " ", str(value or "").lower())
    words = set(text.split())
    for kind, spec in managed_bot_specs().items():
        aliases = {str(alias).lower() for alias in spec.get("aliases") or ()}
        if kind in words or aliases.intersection(words):
            return kind
        if any(alias and alias in text for alias in aliases):
            return kind
    return ""


def managed_bot_kind_for_payload(bot: dict[str, Any]) -> str:
    text = " ".join(
        [
            str(bot.get("username") or ""),
            str(bot.get("first_name") or ""),
            str(bot.get("name") or ""),
        ]
    ).lower()
    for kind, spec in managed_bot_specs().items():
        if kind in text:
            return kind
        username = str(spec.get("suggested_username") or "").lower()
        if username and username in text:
            return kind
        aliases = {str(alias).lower() for alias in spec.get("aliases") or ()}
        if any(alias and alias in text for alias in aliases):
            return kind
    return ""


def managed_bot_kind_for_entry(entry: dict[str, Any], pane: dict[str, Any] | None = None) -> str:
    explicit = str(entry.get("managed_bot_kind") or "").strip().lower()
    if explicit in managed_bot_specs():
        return explicit
    if pane:
        if managed_bot_kind_for_agent(str(pane.get("agent") or "")) == "devin":
            model_kind = devin_model_managed_bot_kind_from_label(
                " ".join(str(pane.get(key) or "") for key in ("label", "name", "title", "pane_thread_name"))
            )
            if model_kind:
                return model_kind
        kind = managed_bot_kind_for_agent(str(pane.get("agent") or ""))
        if kind:
            return kind
    if managed_bot_kind_for_agent(str(entry.get("agent") or "")) == "devin":
        model_kind = devin_model_managed_bot_kind_from_label(
            " ".join(str(entry.get(key) or "") for key in ("pane_label_raw", "pane_thread_name", "topic_name", "label"))
        )
        if model_kind:
            return model_kind
    return managed_bot_kind_for_agent(str(entry.get("agent") or ""))


def managed_bot_token_for_entry(
    telegram: dict[str, Any] | None,
    entry: dict[str, Any],
    pane: dict[str, Any] | None = None,
) -> str | None:
    enabled = entry.get("managed_voice_active") if isinstance(entry, dict) and "managed_voice_active" in entry else managed_bot_setup_enabled()
    if not enabled or not isinstance(telegram, dict):
        return None
    kind = managed_bot_kind_for_entry(entry, pane)
    if not kind:
        return None
    bots = telegram.get("managed_bots") if isinstance(telegram.get("managed_bots"), dict) else {}
    record = bots.get(kind) if isinstance(bots, dict) else None
    if not isinstance(record, dict) or record.get("enabled") is False:
        return None
    token = str(record.get("token") or "").strip()
    return token or None


def desired_message_bot_kind(
    telegram: dict[str, Any] | None,
    entry: dict[str, Any],
    pane: dict[str, Any] | None = None,
) -> str:
    if managed_bot_token_for_entry(telegram, entry, pane):
        kind = managed_bot_kind_for_entry(entry, pane)
        if kind:
            return kind
    return MANAGER_BOT_KIND


def sent_message_bot_kind(
    telegram: dict[str, Any] | None,
    entry: dict[str, Any],
    pane: dict[str, Any] | None,
    result: dict[str, Any],
) -> str:
    if result.get("managed_bot_fallback"):
        return MANAGER_BOT_KIND
    return desired_message_bot_kind(telegram, entry, pane)


def _iso_age_seconds(value: str) -> float | None:
    try:
        then = _dt.datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    return (_dt.datetime.now(tz=_dt.timezone.utc) - then).total_seconds()


def message_bot_reissue_due(entry: dict[str, Any], bot_kind_field: str, desired_kind: str) -> bool:
    current = str(entry.get(bot_kind_field) or "")
    if current == desired_kind:
        return False
    if not current and desired_kind == MANAGER_BOT_KIND:
        return False
    retry_kind = str(entry.get(f"{bot_kind_field}_retry_kind") or "")
    retry_at = str(entry.get(f"{bot_kind_field}_retry_at") or "")
    age = _iso_age_seconds(retry_at) if retry_at else None
    if current == MANAGER_BOT_KIND and retry_kind == desired_kind and age is not None:
        return age >= MANAGED_BOT_REISSUE_RETRY_SECONDS
    return True


def record_message_bot_kind(
    entry: dict[str, Any],
    bot_kind_field: str,
    bot_kind: str,
    desired_kind: str,
) -> None:
    entry[bot_kind_field] = bot_kind
    if bot_kind == desired_kind:
        entry.pop(f"{bot_kind_field}_retry_kind", None)
        entry.pop(f"{bot_kind_field}_retry_at", None)
        return
    entry[f"{bot_kind_field}_retry_kind"] = desired_kind
    entry[f"{bot_kind_field}_retry_at"] = utc_now()


def note_managed_bot_access_required(entry: dict[str, Any], bot_kind_field: str, desired_kind: str) -> None:
    if not desired_kind or desired_kind == MANAGER_BOT_KIND:
        return
    if not entry.get(bot_kind_field):
        entry[bot_kind_field] = MANAGER_BOT_KIND
    entry[f"{bot_kind_field}_retry_kind"] = desired_kind
    entry[f"{bot_kind_field}_retry_at"] = utc_now()
    entry[f"{bot_kind_field}_access_required"] = True


def managed_bot_access_retry_waiting(entry: dict[str, Any], bot_kind_field: str, desired_kind: str) -> bool:
    if not desired_kind or desired_kind == MANAGER_BOT_KIND:
        return False
    if str(entry.get(f"{bot_kind_field}_retry_kind") or "") != desired_kind:
        return False
    retry_at = str(entry.get(f"{bot_kind_field}_retry_at") or "")
    age = _iso_age_seconds(retry_at) if retry_at else None
    return age is not None and age < MANAGED_BOT_REISSUE_RETRY_SECONDS


def managed_bot_profile_photo_path(kind: str) -> Path | None:
    env_name = f"HERDR_TELEGRAM_TOPICS_MANAGED_BOT_{kind.upper()}_PHOTO"
    configured = os.getenv(env_name, "").strip()
    if configured:
        return Path(configured).expanduser()
    default_path = Path.home() / ".config/herdres/managed-bots" / f"{kind}.jpg"
    return default_path if default_path.exists() else None


def configure_managed_bot_profile(kind: str, token: str, *, photo_path: Path | None = None) -> dict[str, Any]:
    spec = managed_bot_specs().get(kind)
    if not spec:
        return {"ok": False, "error": f"unknown managed bot kind: {kind}"}
    errors: list[str] = []
    for method, field, value in (
        ("setMyName", "name", spec.get("name")),
        ("setMyDescription", "description", spec.get("description")),
        ("setMyShortDescription", "short_description", spec.get("short_description")),
    ):
        try:
            telegram_api(method, {field: str(value or "")}, token=token)
        except Exception as exc:
            errors.append(f"{method}: {sanitize_text(str(exc), 240)}")
    selected_photo = photo_path if photo_path is not None else managed_bot_profile_photo_path(kind)
    photo_status = "not_configured"
    if selected_photo:
        if selected_photo.exists():
            try:
                telegram_api_multipart(
                    "setMyProfilePhoto",
                    {
                        "photo": json.dumps(
                            {"type": "static", "photo": "attach://profile_photo"},
                            separators=(",", ":"),
                        )
                    },
                    {"profile_photo": selected_photo},
                    token=token,
                )
                photo_status = "updated"
            except Exception as exc:
                photo_status = "failed"
                errors.append(f"setMyProfilePhoto: {sanitize_text(str(exc), 240)}")
        else:
            photo_status = "missing"
            errors.append(f"profile photo not found: {selected_photo}")
    return {"ok": not errors, "errors": errors, "photo": photo_status}


def extract_managed_bot_update(payload: dict[str, Any]) -> dict[str, Any] | None:
    if not isinstance(payload, dict):
        return None
    if isinstance(payload.get("managed_bot"), dict):
        obj = payload["managed_bot"]
        if isinstance(obj.get("bot"), dict):
            return obj
    message = payload.get("message") if isinstance(payload.get("message"), dict) else payload
    created = message.get("managed_bot_created") if isinstance(message, dict) else None
    if isinstance(created, dict) and isinstance(created.get("bot"), dict):
        return {"bot": created["bot"], "user": message.get("from") or {}}
    if isinstance(payload.get("bot"), dict):
        return payload
    return None


def managed_bot_update(payload: dict[str, Any]) -> dict[str, Any]:
    load_dotenv()
    state = load_state()
    telegram = state.setdefault("telegram", {})
    bots = telegram.setdefault("managed_bots", {})
    if not isinstance(bots, dict):
        bots = {}
        telegram["managed_bots"] = bots
    update = extract_managed_bot_update(payload)
    if not update:
        return {"ok": False, "handled": False, "error": "no managed bot payload"}
    bot = update.get("bot") if isinstance(update.get("bot"), dict) else {}
    user = update.get("user") if isinstance(update.get("user"), dict) else {}
    kind = managed_bot_kind_for_payload(bot)
    if not kind:
        return {"ok": False, "handled": True, "error": "could not infer managed bot kind"}
    bot_id = str(bot.get("id") or "")
    if not bot_id:
        return {"ok": False, "handled": True, "kind": kind, "error": "managed bot update had no bot id"}
    token_result = telegram_api("getManagedBotToken", {"user_id": bot_id})
    child_token = str(token_result.get("result") or "").strip()
    if not child_token:
        return {"ok": False, "handled": True, "kind": kind, "error": "getManagedBotToken returned no token"}
    profile = configure_managed_bot_profile(kind, child_token)
    bots[kind] = {
        "kind": kind,
        "bot_id": bot_id,
        "username": str(bot.get("username") or ""),
        "name": str(bot.get("first_name") or bot.get("name") or ""),
        "owner_user_id": str(user.get("id") or ""),
        "token": child_token,
        "enabled": True,
        "profile": profile,
        "updated_at": utc_now(),
    }
    telegram.pop("managed_bot_setup_message_error", None)
    telegram["managed_bot_last_update_at"] = utc_now()
    save_state(state)
    return {"ok": bool(profile.get("ok")), "handled": True, "kind": kind, "profile": profile}


def manager_bot_username(telegram: dict[str, Any]) -> str:
    cached = str(telegram.get("bot_username") or "").strip().lstrip("@")
    if cached:
        return cached
    result = telegram_api("getMe", {}).get("result") or {}
    username = str(result.get("username") or "").strip().lstrip("@")
    if username:
        telegram["bot_username"] = username
    if "can_manage_bots" in result:
        telegram["can_manage_bots"] = bool(result.get("can_manage_bots"))
    return username


def managed_bot_group_access_kinds(state: dict[str, Any], open_panes: list[dict[str, Any]]) -> list[str]:
    panes = state.get("panes") if isinstance(state.get("panes"), dict) else {}
    telegram = state.get("telegram") if isinstance(state.get("telegram"), dict) else {}
    bots = telegram.get("managed_bots") if isinstance(telegram.get("managed_bots"), dict) else {}
    open_kinds = set(managed_bot_kinds_for_panes(open_panes))
    seen: set[str] = set()
    for pane in open_panes:
        key = pane_key(pane)
        entry = panes.get(key) if isinstance(panes, dict) else None
        if not isinstance(entry, dict):
            continue
        for field in MANAGED_BOT_ROUTE_KIND_FIELDS:
            kind = str(entry.get(f"{field}_retry_kind") or "")
            if kind not in open_kinds:
                continue
            record = bots.get(kind) if isinstance(bots, dict) else None
            if isinstance(record, dict) and record.get("token") and record.get("enabled") is not False:
                seen.add(kind)
    return [kind for kind in managed_bot_specs() if kind in seen]


def ensure_managed_bot_group_access_message(
    state: dict[str, Any],
    chat_id: str,
    telegram: dict[str, Any],
    counters: dict[str, int],
    max_sends: int,
    open_panes: list[dict[str, Any]],
) -> bool:
    if not managed_bot_setup_enabled() or not chat_id:
        return False
    missing_access = managed_bot_group_access_kinds(state, open_panes)
    if not missing_access:
        return False
    access_kinds = {str(kind) for kind in telegram.get("managed_bot_group_access_kinds") or []}
    if telegram.get("managed_bot_group_access_message_id") and set(missing_access).issubset(access_kinds):
        return False
    if counters.get("sends", 0) >= max_sends:
        return False
    labels = ", ".join(str(managed_bot_specs()[kind]["label"]) for kind in missing_access)
    body = (
        f"Telegram is rejecting pane messages from these managed bots in this forum group: {labels}. "
        "Add each bot to the group, then Herdres will send pane threads and replies from the matching bot."
    )
    result = send_notice(
        chat_id,
        "Add pane bots to this group",
        body,
        telegram=telegram,
        thread_id=telegram.get("general_thread_id", DEFAULT_GENERAL_THREAD_ID),
        notify=True,
        reply_markup=managed_bot_group_access_reply_markup(telegram, missing_access),
    )
    counters["sends"] = counters.get("sends", 0) + 1
    if result.get("ok") and result.get("message_id"):
        telegram["managed_bot_group_access_message_id"] = str(result["message_id"])
        telegram["managed_bot_group_access_kinds"] = missing_access
        telegram["managed_bot_group_access_sent_at"] = utc_now()
        telegram.pop("managed_bot_group_access_message_error", None)
    else:
        telegram["managed_bot_group_access_message_error"] = sanitize_text(str(result), 500)
    save_state(state)
    return True


def ensure_multibot_offer_message(
    state: dict[str, Any],
    chat_id: str,
    telegram: dict[str, Any],
    counters: dict[str, int],
    max_sends: int,
    panes: list[dict[str, Any]],
) -> bool:
    if not managed_bot_setup_enabled() or not chat_id:
        return False
    if telegram.get("can_manage_bots") is not True or per_agent_topics_enabled():
        return False
    spaces = state.get("spaces") if isinstance(state.get("spaces"), dict) else {}
    changed = False
    for space_key_value, space in spaces.items():
        if not isinstance(space, dict):
            continue
        if (
            str(space.get("voice_mode") or "shared") == "per_agent"
            or space.get("multibot_offer_dismissed")
            or space.get("multibot_offer_message_id")
            or int(space.get("multibot_offer_signal") or 0) < 1
        ):
            continue
        last_err_at = str(space.get("multibot_offer_error_at") or "")
        if last_err_at:
            err_age = _iso_age_seconds(last_err_at)
            if err_age is not None and err_age < MULTIBOT_OFFER_RETRY_SECONDS:
                continue
        space_open_panes = [
            pane
            for pane in panes
            if str(pane.get("space_key") or space_key(pane)) == str(space_key_value)
            and str(pane.get("agent_status") or "").strip().lower() != "closed"
        ]
        kinds = managed_bot_kinds_for_panes(space_open_panes)
        if len(kinds) < 2:
            continue
        if counters.get("sends", 0) >= max_sends:
            return False
        space_token = _callback_id(str(space.get("space_key") or space_key_value), "space")[:16]
        reply_markup = {"inline_keyboard": [[
            {"text": "Upgrade", "callback_data": f"herdr:mb:{space_token}:up"},
            {"text": "Dismiss", "callback_data": f"herdr:mb:{space_token}:no"},
        ]]}
        result = send_notice(
            chat_id,
            "Give each agent its own bot (optional)",
            "This space can use a separate Telegram bot voice for each agent. It is optional and reversible with /voice.",
            telegram=telegram,
            thread_id=space.get("topic_id"),
            notify=True,
            reply_markup=reply_markup,
        )
        counters["sends"] = counters.get("sends", 0) + 1
        changed = True
        if result.get("ok") and result.get("message_id"):
            space["multibot_offer_message_id"] = str(result["message_id"])
            space["multibot_offer_sent_at"] = utc_now()
            space.pop("multibot_offer_error", None)
        else:
            space["multibot_offer_error"] = sanitize_text(str(result), 500)
            space["multibot_offer_error_at"] = utc_now()
        save_state(state)
        return True
    return changed


def ensure_managed_bot_setup_message(
    state: dict[str, Any],
    chat_id: str,
    telegram: dict[str, Any],
    counters: dict[str, int],
    max_sends: int,
    open_panes: list[dict[str, Any]],
) -> bool:
    if not managed_bot_setup_enabled() or not chat_id:
        return False
    if telegram.get("can_manage_bots") is not True:
        return False
    bots = telegram.get("managed_bots") if isinstance(telegram.get("managed_bots"), dict) else {}
    open_kinds = managed_bot_kinds_for_panes(open_panes)
    if not open_kinds:
        return False
    missing = [kind for kind in open_kinds if not isinstance(bots.get(kind), dict) or not bots[kind].get("token")]
    if not missing:
        return False
    setup_kinds = {str(kind) for kind in telegram.get("managed_bot_setup_kinds") or []}
    if telegram.get("managed_bot_setup_message_id") and set(missing).issubset(setup_kinds):
        return False
    if counters.get("sends", 0) >= max_sends:
        return False
    try:
        username = manager_bot_username(telegram)
    except Exception as exc:
        telegram["managed_bot_setup_message_error"] = sanitize_text(str(exc), 300)
        return True
    if not username:
        telegram["managed_bot_setup_message_error"] = "getMe returned no manager bot username"
        return True
    labels = ", ".join(str(managed_bot_specs()[kind]["label"]) for kind in missing)
    body = (
        f"Create managed pane bots for the currently open pane types: {labels}. "
        "After creation, add each child bot to this forum group so direct replies to that bot can be received."
    )
    result = send_notice(
        chat_id,
        "Managed pane bots",
        body,
        telegram=telegram,
        thread_id=telegram.get("general_thread_id", DEFAULT_GENERAL_THREAD_ID),
        notify=True,
        reply_markup=managed_bot_setup_reply_markup(username, kinds=missing),
    )
    counters["sends"] = counters.get("sends", 0) + 1
    if result.get("ok") and result.get("message_id"):
        telegram["managed_bot_setup_message_id"] = str(result["message_id"])
        telegram["managed_bot_setup_kinds"] = missing
        telegram["managed_bot_setup_sent_at"] = utc_now()
        telegram.pop("managed_bot_setup_message_error", None)
    else:
        telegram["managed_bot_setup_message_error"] = sanitize_text(str(result), 500)
    save_state(state)
    return True


def run_cmd(args: list[str], *, timeout: int = 10, input_text: str | None = None) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        args,
        input=input_text,
        text=True,
        capture_output=True,
        timeout=timeout,
        check=False,
    )


def _send_deadline(deadline: float | None) -> float:
    """Resolve a deadline, defaulting to now + the send budget when unset."""
    if deadline is not None:
        return deadline
    return time.monotonic() + SEND_TO_PANE_BUDGET_SECONDS


def _remaining(deadline: float | None) -> float:
    if deadline is None:
        return float("inf")
    return deadline - time.monotonic()


def _bounded_timeout(deadline: float | None, cap: float = SEND_TO_PANE_PER_CALL_CAP) -> int:
    """Per-call herdr timeout = min(cap, remaining), floored at 1s, int for run_cmd."""
    if deadline is None:
        return int(cap)
    rem = _remaining(deadline)
    return max(1, int(min(cap, rem)))


def _deadline_sleep(seconds: float, deadline: float | None) -> bool:
    """Sleep at most `seconds`, clamped to remaining budget. Returns True if
    budget remains afterward (caller may continue), False if exhausted."""
    rem = _remaining(deadline)
    if rem <= 0:
        return False
    time.sleep(min(seconds, rem))
    return _remaining(deadline) > 0


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


def workspace_label_map(deadline: float | None = None) -> dict[str, str]:
    if deadline is not None and _remaining(deadline) <= SEND_TO_PANE_MIN_CALL_SECONDS:
        return {}
    try:
        data = herdr_json(["workspace", "list"], timeout=(8 if deadline is None else _bounded_timeout(deadline)))
    except BridgeError:
        return {}
    if not isinstance(data, dict):
        return {}
    workspaces = data.get("result", {}).get("workspaces")
    if not isinstance(workspaces, list):
        return {}
    labels: dict[str, str] = {}
    for workspace in workspaces:
        if not isinstance(workspace, dict):
            continue
        workspace_id = str(workspace.get("workspace_id") or "")
        label = clean_space_topic_title(str(workspace.get("label") or ""), fallback="")
        if workspace_id and label:
            labels[workspace_id] = label
    return labels


def pane_list(deadline: float | None = None) -> list[dict[str, Any]]:
    if deadline is not None and _remaining(deadline) <= SEND_TO_PANE_MIN_CALL_SECONDS:
        raise BridgeError("herdr pane list skipped because send budget is exhausted")
    data = herdr_json(["pane", "list"], timeout=(8 if deadline is None else _bounded_timeout(deadline)))
    if isinstance(data, dict):
        panes = data.get("result", {}).get("panes")
        if isinstance(panes, list):
            labels = workspace_label_map(deadline=deadline)
            if not labels:
                return panes
            enriched: list[dict[str, Any]] = []
            for pane in panes:
                if not isinstance(pane, dict):
                    continue
                item = dict(pane)
                label = labels.get(str(item.get("workspace_id") or ""))
                if label:
                    item["space_name"] = label
                    item["workspace_label"] = label
                enriched.append(item)
            return enriched
    raise BridgeError("unexpected herdr pane list response")


def pane_by_id(pane_id: str, deadline: float | None = None) -> dict[str, Any] | None:
    if deadline is not None and _remaining(deadline) <= SEND_TO_PANE_MIN_CALL_SECONDS:
        return None
    for pane in pane_list(deadline=deadline):
        if str(pane.get("pane_id")) == str(pane_id):
            return pane
    return None


_turn_cache: dict[str, dict[str, Any]] = {}
_turn_cache_lock = threading.Lock()

# Per-sync ephemeral cache for pane read output (visible-screen scrapes).
_pane_read_cache: dict[str, str] = {}
_pane_read_cache_lock = threading.Lock()


def pane_turn(pane_id: str) -> dict[str, Any]:
    cached = _turn_cache.get(pane_id)
    if cached is not None:
        return cached
    # Upgrade-safe optional interface: Herdres can consume this when Herdr
    # exposes it, but never scrapes pane output as a substitute.
    try:
        data = herdr_json(["pane", "turn", pane_id, "--last", "--format", "json"], timeout=8)
    except BridgeError as exc:
        result = {
            "available": False,
            "reason": "no_structured_turn_source",
            "detail": sanitize_text(str(exc), 300),
        }
        with _turn_cache_lock:
            _turn_cache[pane_id] = result
        return result
    if isinstance(data, dict):
        result_turn = data.get("result", {}).get("turn")
        if isinstance(result_turn, dict):
            with _turn_cache_lock:
                _turn_cache[pane_id] = result_turn
            return result_turn
        with _turn_cache_lock:
            _turn_cache[pane_id] = data
        return data
    result = {"available": False, "reason": "unexpected_turn_response"}
    with _turn_cache_lock:
        _turn_cache[pane_id] = result
    return result


def prefetch_pane_turns(pane_ids: list[str], max_workers: int = 6) -> None:
    """Fetch all pane turns in parallel and populate _turn_cache."""
    uncached = [pid for pid in pane_ids if pid and pid not in _turn_cache]
    if not uncached:
        return
    with ThreadPoolExecutor(max_workers=min(max_workers, len(uncached))) as pool:
        futures = {pool.submit(pane_turn, pid): pid for pid in uncached}
        for future in as_completed(futures):
            try:
                result = future.result(timeout=12)
                if isinstance(result, dict):
                    with _turn_cache_lock:
                        _turn_cache[futures[future]] = result
            except Exception:
                pass


def cached_pane_turn(pane_id: str) -> dict[str, Any]:
    """Check the per-sync turn cache before calling pane_turn."""
    cached = _turn_cache.get(pane_id)
    if cached is not None:
        return cached
    return pane_turn(pane_id)


def clear_sync_caches() -> None:
    """Clear per-sync ephemeral caches. Called at the start of each sync."""
    with _turn_cache_lock:
        _turn_cache.clear()
    with _pane_read_cache_lock:
        _pane_read_cache.clear()


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


def _space_component(value: str) -> str:
    text = sanitize_text(value, 120).strip()
    text = re.sub(r"\s+", "-", text)
    text = re.sub(r"[^A-Za-z0-9_.:-]+", "-", text)
    text = re.sub(r"-+", "-", text).strip("-")
    return text


def space_key(pane: dict[str, Any]) -> str:
    if per_agent_topics_enabled():
        # One topic per Herdr agent: group by the stable pane id (which both live
        # panes and the reconstructed pane_like dicts in migrate carry), not by
        # pane_key, whose digest folds in terminal_id that migrate does not have.
        pane_id = _space_component(str(pane.get("pane_id") or ""))
        if pane_id:
            return f"agent:{pane_id}"
        return f"agent:{pane_key(pane)}"
    explicit_space = _space_component(str(pane.get("space_id") or ""))
    if explicit_space:
        return f"space:{explicit_space}"
    workspace = _space_component(str(pane.get("workspace_id") or pane.get("workspace") or ""))
    if workspace:
        return f"workspace:{workspace}"
    cwd_value = str(pane.get("foreground_cwd") or pane.get("cwd") or "")
    cwd_name = _space_component(Path(cwd_value).name if cwd_value else "")
    if cwd_name:
        return f"cwd:{cwd_name}"
    return "default"


def clean_space_topic_title(value: str, *, fallback: str = "Herdr Space") -> str:
    text = sanitize_text(value, 80)
    text = re.sub(r"\s+", " ", text).strip()
    return text[:32].strip() or fallback


def agent_topic_name_for_pane(pane: dict[str, Any]) -> str:
    # Per-agent topic title. A manual pane label is the user's explicit intent,
    # so it wins; otherwise name the topic "<agent> · <folder>".
    manual = pane_manual_label(pane)
    if manual:
        labelled = topic_name_from_pane_label(manual)
        if labelled:
            return labelled
    agent = str(pane.get("agent") or "").strip().lower()
    cwd_value = str(pane.get("foreground_cwd") or pane.get("cwd") or "")
    folder = sanitize_text(Path(cwd_value).name if cwd_value else "", 40).strip()
    if agent and folder:
        return f"{agent} · {folder}"[:60]
    if agent:
        return agent
    if folder:
        return folder
    return "Herdr Agent"


def space_name_for_pane(pane: dict[str, Any]) -> str:
    if per_agent_topics_enabled():
        return agent_topic_name_for_pane(pane)
    explicit_name = clean_space_topic_title(
        str(pane.get("space_name") or pane.get("workspace_label") or pane.get("workspace_name") or ""),
        fallback="",
    )
    if explicit_name:
        return explicit_name
    explicit_space = clean_label_topic_title(str(pane.get("space_id") or ""), fallback="")
    if explicit_space:
        return explicit_space
    workspace = clean_label_topic_title(str(pane.get("workspace_id") or pane.get("workspace") or ""), fallback="")
    if workspace:
        return workspace
    cwd_value = str(pane.get("foreground_cwd") or pane.get("cwd") or "")
    cwd_name = clean_label_topic_title(Path(cwd_value).name if cwd_value else "", fallback="")
    if cwd_name:
        return cwd_name
    return "Herdr Space"


def pane_handle_alias(value: str) -> str:
    text = str(value or "")
    match = re.match(r"^(w[0-9a-f]+)(?::p|-)(\d+)$", text, flags=re.IGNORECASE)
    if not match:
        return ""
    return f"{match.group(1)}:{int(match.group(2))}"


def entry_pane_alias(entry: dict[str, Any]) -> str:
    return pane_handle_alias(str(entry.get("pane_id") or ""))


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
    title = format_topic_title_words(words[:2])
    return title[:32].strip() or fallback


TOPIC_TITLE_ACRONYMS = {
    "api": "API",
    "cli": "CLI",
    "glm": "GLM",
    "llm": "LLM",
    "omp": "OMP",
    "tg": "TG",
    "ui": "UI",
}


def format_topic_title_words(words: list[str]) -> str:
    formatted: list[str] = []
    for word in words:
        clean = str(word or "")
        replacement = TOPIC_TITLE_ACRONYMS.get(clean.lower())
        formatted.append(replacement if replacement else clean.title())
    return " ".join(formatted).strip()


def clean_label_topic_title(value: str, *, fallback: str = "Task") -> str:
    text = sanitize_text(value, 80)
    text = re.sub(r"\bHerdr\b", " ", text, flags=re.IGNORECASE)
    text = re.sub(r"\bw[0-9a-f]{8,}-\d+\b", " ", text, flags=re.IGNORECASE)
    text = re.sub(r"[_./:@-]+", " ", text)
    text = re.sub(r"[^A-Za-z0-9 ]+", " ", text)
    words = [w for w in text.strip().split() if w]
    if not words:
        return fallback
    title = format_topic_title_words(words[:2])
    return title[:32].strip() or fallback


def title_from_text(text: str) -> str:
    lower = text.lower()
    rules = [
        (("topic name", "topic naming", "editforumtopic", "forum topic icon"), "Topic Names"),
        (("herdres", "createforumtopic", "herdr pane telegram", "topic sync"), "Topic Sync"),
        (("flightrecorder", "flight recorder"), "Flight Recorder"),
        (("italy ping",), "Italy Ping"),
        (("gitmoot", "code review", "review pass"), "Review"),
        (("summarize recent commits", "recent commits"), "Commits"),
    ]
    for needles, title in rules:
        if any(needle in lower for needle in needles):
            return title
    return ""


def pane_manual_label(pane: dict[str, Any]) -> str:
    label = str(pane.get("label") or "").strip()
    label = re.sub(r"\s+", " ", label)
    return sanitize_text(label, 120)


def topic_name_from_pane_label(label: str) -> str:
    # A manual pane label is the user's explicit intent: clean it literally.
    # Do NOT run it through title_from_text's content-keyword mapper, which would
    # e.g. rewrite any label containing "herdres" to "Topic Sync".
    return clean_label_topic_title(label)


def topic_name_for_pane(pane: dict[str, Any]) -> str:
    label = pane_manual_label(pane)
    if label:
        return topic_name_from_pane_label(label)

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


def pane_thread_name(pane: dict[str, Any]) -> str:
    label = pane_manual_label(pane)
    if label:
        return label
    pane_id = str(pane.get("pane_id") or "")
    alias = pane_handle_alias(pane_id)
    if alias:
        return alias
    short_id = short_pane_id(pane_id) if pane_id else ""
    agent = clean_label_topic_title(str(pane.get("agent") or ""), fallback="")
    if agent and short_id:
        return f"{agent} {short_id}"
    if short_id:
        return short_id
    if agent:
        return agent
    return "Pane"


def pane_entry_iter(state: dict[str, Any]) -> Iterator[tuple[str, dict[str, Any]]]:
    panes = state.setdefault("panes", {})
    for key, entry in panes.items():
        if isinstance(entry, dict):
            yield str(key), entry


def ensure_space_entry(state: dict[str, Any], pane: dict[str, Any]) -> tuple[str, dict[str, Any], bool]:
    key = space_key(pane)
    spaces = state.setdefault("spaces", {})
    entry = spaces.get(key)
    changed = False
    if not isinstance(entry, dict):
        preserved = state.get("_preserved_voice_mode") if isinstance(state.get("_preserved_voice_mode"), dict) else {}
        vm = preserved.pop(key, None) or "shared"
        entry = {"space_key": key, "created_at": utc_now(), "pane_keys": [], "voice_mode": vm}
        spaces[key] = entry
        changed = True
    if entry.get("space_key") != key:
        entry["space_key"] = key
        changed = True
    space_id = str(pane.get("space_id") or pane.get("workspace_id") or pane.get("workspace") or "")
    if space_id and entry.get("space_id") != space_id:
        entry["space_id"] = space_id
        changed = True
    workspace_label = clean_space_topic_title(
        str(pane.get("space_name") or pane.get("workspace_label") or pane.get("workspace_name") or ""),
        fallback="",
    )
    if workspace_label and entry.get("workspace_name") != workspace_label:
        entry["workspace_name"] = workspace_label
        changed = True
    topic_name = space_name_for_pane(pane)
    current_topic_name = str(entry.get("topic_name") or "")
    if not current_topic_name:
        entry["topic_name"] = topic_name
        changed = True
    elif workspace_label and current_topic_name != topic_name:
        entry["topic_name"] = topic_name
        entry["topic_title_source"] = "workspace-label"
        if entry.get("topic_id"):
            entry["topic_rename_pending_at"] = utc_now()
            entry["topic_rename_from"] = current_topic_name
            entry["topic_rename_to"] = topic_name
            entry.pop("last_topic_verified_at", None)
        changed = True
    pane_keys = entry.setdefault("pane_keys", [])
    key_for_pane = pane_key(pane)
    if key_for_pane not in pane_keys:
        pane_keys.append(key_for_pane)
        changed = True
    return key, entry, changed


def migrate_legacy_pane_topics(state: dict[str, Any]) -> bool:
    changed = False
    spaces = state.setdefault("spaces", {})
    for key, entry in pane_entry_iter(state):
        pane_like = {
            "pane_id": entry.get("pane_id") or key,
            "workspace_id": entry.get("workspace_id") or entry.get("workspace") or "",
            "tab_id": entry.get("tab") or "",
            "label": entry.get("pane_label_raw") or entry.get("topic_name") or "",
            "agent": entry.get("agent") or "",
            "foreground_cwd": entry.get("foreground_cwd") or entry.get("cwd") or "",
        }
        key_for_space = space_key(pane_like)
        space_entry = spaces.get(key_for_space)
        if not isinstance(space_entry, dict):
            space_entry = {
                "space_key": key_for_space,
                "space_id": str(pane_like.get("workspace_id") or ""),
                "topic_name": space_name_for_pane(pane_like),
                "pane_keys": [],
            }
            spaces[key_for_space] = space_entry
            changed = True
        pane_keys = space_entry.setdefault("pane_keys", [])
        if key not in pane_keys:
            pane_keys.append(key)
            changed = True
        if entry.get("space_key") != key_for_space:
            entry["space_key"] = key_for_space
            changed = True
        thread_name = pane_thread_name(pane_like)
        if entry.get("pane_thread_name") != thread_name:
            entry["pane_thread_name"] = thread_name
            changed = True
        legacy_topic_id = str(entry.get("topic_id") or "")
        if legacy_topic_id and not space_entry.get("topic_id"):
            space_entry["topic_id"] = legacy_topic_id
            for verify_key in ("last_topic_verified_at", "last_topic_verify_attempt_at"):
                verify_value = entry.get(verify_key)
                if verify_value and not space_entry.get(verify_key):
                    space_entry[verify_key] = verify_value
            legacy_name = str(entry.get("topic_name") or "")
            space_name = space_name_for_pane(pane_like)
            if not space_entry.get("topic_name"):
                space_entry["topic_name"] = space_name
            if legacy_name and legacy_name != space_name:
                space_entry["topic_rename_pending_at"] = utc_now()
                space_entry["topic_rename_from"] = legacy_name
                space_entry["topic_rename_to"] = space_name
            changed = True
        space_topic_id = str(space_entry.get("topic_id") or "")
        if legacy_topic_id and space_topic_id and legacy_topic_id != space_topic_id:
            if entry.get("legacy_topic_id") != legacy_topic_id:
                entry["legacy_topic_id"] = legacy_topic_id
                changed = True
            if entry.get("topic_name") and entry.get("legacy_topic_name") != entry.get("topic_name"):
                entry["legacy_topic_name"] = entry.get("topic_name")
                changed = True
            entry["topic_id"] = space_topic_id
            changed = True
        elif space_topic_id and entry.get("topic_id") != space_topic_id:
            entry["topic_id"] = space_topic_id
            changed = True
        space_topic_name = str(space_entry.get("topic_name") or space_name_for_pane(pane_like))
        current_topic_name = str(entry.get("topic_name") or "")
        if current_topic_name and current_topic_name != space_topic_name:
            if entry.get("legacy_topic_name") != current_topic_name:
                entry["legacy_topic_name"] = current_topic_name
                changed = True
        if space_topic_name and current_topic_name != space_topic_name:
            entry["topic_name"] = space_topic_name
            entry["topic_title_source"] = "space"
            changed = True
        for rename_key in ("topic_rename_pending_at", "topic_rename_from", "topic_rename_to"):
            if rename_key in entry:
                entry.pop(rename_key, None)
                changed = True
    return changed


def migrate_space_voice_mode(state: dict[str, Any]) -> bool:
    changed = False
    telegram = state.get("telegram") if isinstance(state.get("telegram"), dict) else {}
    bots = telegram.get("managed_bots") if isinstance(telegram.get("managed_bots"), dict) else {}
    panes = state.get("panes") if isinstance(state.get("panes"), dict) else {}
    spaces = state.get("spaces") if isinstance(state.get("spaces"), dict) else {}
    for space in spaces.values():
        if not isinstance(space, dict) or space.get("voice_mode"):
            continue
        for pane_key_value in space.get("pane_keys") or []:
            entry = panes.get(str(pane_key_value))
            if not isinstance(entry, dict):
                continue
            kind = managed_bot_kind_for_entry(entry)
            if not kind:
                continue
            record = bots.get(kind)
            if isinstance(record, dict) and record.get("token") and record.get("enabled") is not False:
                space["voice_mode"] = "per_agent"
                changed = True
                break
    return changed


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
    deadline: float | None = None,
) -> str:
    try:
        raw = herdr_text(
            ["pane", "read", pane_id, "--source", source, "--lines", str(lines), "--format", "text"],
            timeout=(8 if deadline is None else _bounded_timeout(deadline)),
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
    cache_key = f"{pane_id}:{manual}"
    cached = _pane_read_cache.get(cache_key)
    if cached is not None:
        return cached
    sources = MANUAL_FEED_SOURCES if manual else AUTO_FEED_SOURCES
    for source in sources:
        text = pane_output(
            pane_id,
            lines=FEED_READ_LINES,
            max_chars=FEED_MAX_CHARS,
            source=source,
        )
        if text.strip():
            with _pane_read_cache_lock:
                _pane_read_cache[cache_key] = text
            return text
    result = ""
    with _pane_read_cache_lock:
        _pane_read_cache[cache_key] = result
    return result


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
        if is_composer_boundary(line) and visible_composer_tail(lines[idx + 1:]):
            return lines[:idx]
    return lines


def visible_composer_tail(lines: list[str]) -> bool:
    for line in lines:
        raw = ANSI_RE.sub("", line or "").strip()
        low = noise_key(line)
        if not raw:
            continue
        if raw.startswith("●") or option_match(raw):
            return False
        if low.startswith("tip: use /btw"):
            continue
        if PROMPT_ONLY_RE.fullmatch(raw):
            continue
        if choice_ui_chrome_line(raw):
            continue
        continue
    return True


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
    if low in {"herdres_report_start", "herdres_report_end", "herdres_choices_start", "herdres_choices_end"}:
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
    if SPINNER_STATUS_RE.match(ANSI_RE.sub("", line or "").strip()):
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


def choice_continuation_line(line: str) -> bool:
    stripped = str(line or "").strip()
    if not stripped:
        return False
    if option_match(stripped):
        return False
    if choice_ui_chrome_line(stripped):
        return False
    return True


def choice_separator_line(line: str) -> bool:
    stripped = str(line or "").strip()
    return bool(stripped.startswith(("─", "━")))


def choice_ui_chrome_line(line: str) -> bool:
    stripped = str(line or "").strip()
    if not stripped:
        return False
    if stripped.startswith(("─", "━", "Enter to select", "Tab/", "Esc to cancel", "←", "→")):
        return True
    if "✔ Submit" in stripped or "☐" in stripped:
        return True
    return False


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


def section_alias(line: str) -> tuple[str, str, str] | None:
    match = STRUCTURED_SECTION_RE.match(str(line or ""))
    if not match:
        return None
    label = re.sub(r"\s+", " ", match.group(1).strip()).lower()
    kind = SECTION_ALIASES.get(label)
    if not kind:
        return None
    title = match.group(2).strip()
    if not title:
        title = label.title()
    return kind, title, label


def is_section_marker_line(line: str) -> bool:
    return section_alias(line) is not None


def parse_bounded_report_body(body_lines: list[str]) -> tuple[str, str] | None:
    body_lines = strip_outer_blank_lines(body_lines)
    if not body_lines:
        return None

    title = ""
    meta = REPORT_TITLE_RE.match(body_lines[0])
    if meta:
        title = sanitize_text(meta.group(1).strip(), 80)
        body_lines = body_lines[1:]
    elif is_section_marker_line(body_lines[0]):
        return None
    elif is_safe_report_title(body_lines[0]):
        title = sanitize_text(body_lines[0].strip().rstrip(":"), 80)
        body_lines = body_lines[1:]
    else:
        return None

    body_text = "\n".join(strip_outer_blank_lines(body_lines)).strip()
    if not title or not body_text:
        return None
    return title, body_text


def extract_bounded_report(lines: list[str]) -> tuple[str, str] | None:
    text = "\n".join(lines)
    matches = list(REPORT_BLOCK_RE.finditer(text))
    if not matches:
        return None
    return parse_bounded_report_body(matches[-1].group(1).splitlines())


def extract_bounded_report_from_raw(raw_text: str) -> tuple[str, str] | None:
    safe = ANSI_RE.sub("", sanitize_text(str(raw_text or ""), FEED_MAX_CHARS))
    matches = list(REPORT_BLOCK_RE.finditer(safe))
    if not matches:
        return None
    return parse_bounded_report_body(matches[-1].group(1).splitlines())


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


def _boolish(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    return str(value or "").strip().lower() in {"1", "true", "yes", "on"}


def _callback_id(value: str, fallback: str) -> str:
    clean = re.sub(r"[^A-Za-z0-9_-]+", "", str(value or "").strip())
    return (clean or fallback)[:32]


def _prompt_callback_id(value: str, fallback_payload: str, options: list[dict[str, str]]) -> str:
    clean = _callback_id(value, "")
    if clean and len(clean.encode("utf-8")) <= 16:
        return clean
    return prompt_id_for(fallback_payload, options)


def safe_callback_data(action: str, prompt_id: str, choice_id: str) -> str:
    clean_action = "d" if action == "d" else "c"
    clean_prompt = _callback_id(prompt_id, "prompt")[:16]
    clean_choice = _callback_id(choice_id, "choice")[:32]
    data = f"herdr:{clean_action}:{clean_prompt}:{clean_choice}"
    if len(data.encode("utf-8")) <= 64:
        return data
    short_choice = hashlib.sha1(clean_choice.encode("utf-8")).hexdigest()[:10]
    return f"herdr:{clean_action}:{clean_prompt}:{short_choice}"


def normalize_pending_interaction(turn: dict[str, Any]) -> dict[str, Any] | None:
    pending = turn.get("pending_interaction")
    if not isinstance(pending, dict):
        return None
    interaction_id = sanitize_text(str(pending.get("interaction_id") or ""), 300).strip()
    if not interaction_id:
        return None
    kind = str(pending.get("kind") or pending.get("type") or "").strip().lower()
    kind = kind.replace("-", "_")
    if kind not in {"single_question", "multi_question_form"}:
        return None
    revision = sanitize_text(str(pending.get("revision") or "1"), 40).strip() or "1"
    raw_questions = pending.get("questions")
    if not isinstance(raw_questions, list) or not raw_questions:
        return None

    questions: list[dict[str, Any]] = []
    for q_idx, raw_question in enumerate(raw_questions[:12], start=1):
        if not isinstance(raw_question, dict):
            return None
        question_id = sanitize_text(str(raw_question.get("question_id") or raw_question.get("id") or f"q{q_idx}"), 80)
        title = sanitize_text(
            str(raw_question.get("title") or raw_question.get("prompt") or raw_question.get("question") or f"Question {q_idx}"),
            500,
        ).strip()
        raw_options = raw_question.get("options")
        if not question_id or not title or not isinstance(raw_options, list) or not raw_options:
            return None
        options: list[dict[str, str]] = []
        for opt_idx, raw_option in enumerate(raw_options[:12], start=1):
            if isinstance(raw_option, dict):
                option_id = sanitize_text(
                    str(raw_option.get("option_id") or raw_option.get("id") or raw_option.get("number") or opt_idx),
                    80,
                )
                label = sanitize_text(
                    str(raw_option.get("label") or raw_option.get("text") or raw_option.get("title") or option_id),
                    180,
                ).strip()
                value = sanitize_text(str(raw_option.get("value") or raw_option.get("send_text") or option_id), 500)
                description = sanitize_text(
                    str(raw_option.get("description") or raw_option.get("detail") or raw_option.get("help") or ""),
                    500,
                ).strip()
                needs_detail = (
                    _boolish(raw_option.get("needs_detail"))
                    or _boolish(raw_option.get("requires_detail"))
                    or _boolish(raw_option.get("custom"))
                    or not value.strip()
                )
            else:
                option_id = sanitize_text(str(opt_idx), 80)
                label = sanitize_text(str(raw_option or ""), 180).strip()
                value = option_id
                description = ""
                needs_detail = False
            if not option_id or not label:
                return None
            option: dict[str, str] = {
                "option_id": option_id,
                "label": label,
                "value": value,
            }
            if description:
                option["description"] = description
            if needs_detail:
                option["needs_detail"] = "1"
            options.append(option)
        questions.append(
            {
                "question_id": question_id,
                "title": title,
                "type": sanitize_text(str(raw_question.get("type") or "single_choice"), 80),
                "required": "1" if _boolish(raw_question.get("required", True)) else "",
                "options": options,
            }
        )

    answers = pending.get("answers") if isinstance(pending.get("answers"), dict) else {}
    return {
        "interaction_id": interaction_id,
        "revision": revision,
        "kind": kind,
        "prompt": sanitize_text(str(pending.get("prompt") or "Input needed."), 1200).strip() or "Input needed.",
        "questions": questions,
        "answers": answers,
        "source": "pending_interaction",
    }


def normalize_pending_decision(turn: dict[str, Any]) -> dict[str, Any] | None:
    if normalize_pending_interaction(turn):
        return None
    pending = turn.get("pending_decision")
    if not isinstance(pending, dict):
        return None
    if pending_decision_looks_multi_question(pending):
        return None
    raw_options = pending.get("options")
    if not isinstance(raw_options, list) or not raw_options:
        return None

    prompt = sanitize_text(str(pending.get("prompt") or ""), 1200).strip()
    if not prompt:
        prompt = "Choose how to proceed."

    options: list[dict[str, str]] = []
    for idx, raw in enumerate(raw_options[:12], start=1):
        if isinstance(raw, dict):
            raw_id = str(raw.get("id") or raw.get("number") or idx).strip()
            label = str(raw.get("label") or raw.get("text") or raw.get("title") or raw_id).strip()
            if "send_text" in raw:
                send_text = str(raw.get("send_text") or "").strip()
            elif "value" in raw:
                send_text = str(raw.get("value") or "").strip()
            else:
                send_text = raw_id
            needs_detail = (
                _boolish(raw.get("needs_detail"))
                or _boolish(raw.get("requires_detail"))
                or _boolish(raw.get("custom"))
            )
        else:
            raw_id = str(idx)
            label = str(raw or "").strip()
            send_text = raw_id
            needs_detail = False

        raw_id = raw_id or str(idx)
        label = re.sub(r"^\s*\d{1,2}[.)]\s+", "", label).strip()
        label = sanitize_text(label or raw_id, 120)
        callback_id = _callback_id(raw_id, str(idx))
        if raw_id.lower() == "custom" or callback_id.lower() == "custom":
            needs_detail = True
        has_explicit_send_text = isinstance(raw, dict) and "send_text" in raw
        if has_explicit_send_text:
            needs_detail = needs_detail or not send_text
        option: dict[str, str] = {
            "number": callback_id,
            "callback_id": callback_id,
            "id": sanitize_text(raw_id, 80),
            "label": label,
            "send_text": sanitize_text(send_text, 500),
        }
        if needs_detail:
            option["needs_detail"] = "1"
        options.append(option)

    if not options:
        return None
    decision_id = sanitize_text(str(pending.get("decision_id") or turn.get("turn_id") or prompt_id_for(prompt, options)), 300)
    return {
        "decision_id": decision_id,
        "prompt": prompt,
        "mode": "buttons",
        "options": options,
        "source": "pending_decision",
    }


def pending_decision_looks_multi_question(pending: dict[str, Any]) -> bool:
    for key in ("questions", "answers", "review", "interactions", "forms"):
        value = pending.get(key)
        if isinstance(value, (list, dict)) and value:
            return True
    kind = str(pending.get("kind") or pending.get("type") or "").strip().lower()
    if kind in {"multi_question_form", "multi-question-form", "wizard", "form", "review"}:
        return True
    raw_options = pending.get("options")
    if isinstance(raw_options, list):
        question_like = 0
        for raw in raw_options:
            if isinstance(raw, dict) and (
                raw.get("question_id")
                or raw.get("question")
                or raw.get("questions")
                or raw.get("options")
            ):
                question_like += 1
        if question_like > 1:
            return True
    return False


def make_decision_feed_item(turn: dict[str, Any], decision: dict[str, Any]) -> dict[str, Any] | None:
    prompt = sanitize_text(str(decision.get("prompt") or ""), 1200).strip()
    options = list(decision.get("options") or [])
    if not prompt or not options:
        return None
    user_text = sanitize_text(str(turn.get("user_text") or ""), USER_PROMPT_MAX_CHARS).strip()
    assistant_context = sanitize_text(str(turn.get("assistant_final_text") or ""), FINAL_REPLY_MAX_CHARS).strip()
    text_parts: list[str] = []
    if user_text:
        text_parts.extend([USER_PROMPT_LABEL, user_text, ""])
    if assistant_context:
        text_parts.extend([assistant_context, ""])
    text_parts.append(prompt)
    text_parts.append("")
    text_parts.extend(f"{opt.get('number')}) {opt.get('label')}" for opt in options)
    return {
        "kind": "decision",
        "title": "Decision needed",
        "summary": prompt,
        "detail": assistant_context,
        "lines": prompt.splitlines()[:8],
        "text": "\n".join(text_parts).strip(),
        "turn_id": sanitize_text(str(turn.get("turn_id") or ""), 300),
        "decision_id": str(decision.get("decision_id") or ""),
        "choice_source": str(decision.get("source") or "pending_decision"),
        "user_text": user_text,
        "assistant_final_text": assistant_context,
        "options": options,
        "notify": True,
    }


def interaction_answer_text(question: dict[str, Any], answer: Any) -> str:
    if not isinstance(answer, dict):
        return sanitize_text(str(answer or ""), 500).strip()
    text = sanitize_text(str(answer.get("text") or answer.get("value") or ""), 500).strip()
    option_id = str(answer.get("option_id") or answer.get("id") or "").strip()
    if option_id:
        for option in list(question.get("options") or []):
            if str(option.get("option_id") or "") == option_id:
                label = str(option.get("label") or option_id).strip()
                return f"{label}: {text}" if text else label
        return f"{option_id}: {text}" if text else option_id
    return text


def make_interaction_feed_item(turn: dict[str, Any], interaction: dict[str, Any]) -> dict[str, Any] | None:
    questions = list(interaction.get("questions") or [])
    if not questions:
        return None
    user_text = sanitize_text(str(turn.get("user_text") or ""), USER_PROMPT_MAX_CHARS).strip()
    assistant_context = sanitize_text(str(turn.get("assistant_final_text") or ""), FINAL_REPLY_MAX_CHARS).strip()
    prompt = sanitize_text(str(interaction.get("prompt") or "Input needed."), 1200).strip()
    answers = interaction.get("answers") if isinstance(interaction.get("answers"), dict) else {}
    note = f"{INTERACTION_READONLY_WARNING_TITLE}\n{INTERACTION_READONLY_WARNING_BODY}"
    text_parts: list[str] = []
    if user_text:
        text_parts.extend([USER_PROMPT_LABEL, user_text, ""])
    if assistant_context:
        text_parts.extend([assistant_context, ""])
    text_parts.append(prompt)
    text_parts.append("")
    for idx, question in enumerate(questions, start=1):
        title = str(question.get("title") or f"Question {idx}").strip()
        text_parts.append(f"{idx}. {title}")
        answer_text = interaction_answer_text(question, answers.get(str(question.get("question_id") or "")))
        if answer_text:
            text_parts.append(f"Answer: {answer_text}")
        for option in list(question.get("options") or []):
            label = str(option.get("label") or "").strip()
            option_id = str(option.get("option_id") or "").strip()
            description = str(option.get("description") or "").strip()
            line = f"- {option_id}. {label}" if option_id else f"- {label}"
            if description:
                line = f"{line}: {description}"
            text_parts.append(line)
        text_parts.append("")
    text_parts.append(note)
    return {
        "kind": "interaction_readonly",
        "title": "Input needed",
        "summary": prompt,
        "detail": note,
        "lines": [],
        "text": "\n".join(text_parts).strip(),
        "turn_id": sanitize_text(str(turn.get("turn_id") or ""), 300),
        "interaction_id": str(interaction.get("interaction_id") or ""),
        "interaction_revision": str(interaction.get("revision") or "1"),
        "choice_source": "pending_interaction",
        "user_text": user_text,
        "assistant_final_text": assistant_context,
        "questions": questions,
        "answers": answers,
        "notify": True,
    }


def make_turn_feed_item(turn: dict[str, Any]) -> dict[str, Any] | None:
    if not isinstance(turn, dict):
        return None
    if turn.get("available") is not True:
        return None
    interaction = normalize_pending_interaction(turn) if STRUCTURED_INTERACTIONS_ENABLED else None
    if interaction and (turn.get("awaiting_input") is True or turn.get("complete") is not True):
        return make_interaction_feed_item(turn, interaction)
    decision = normalize_pending_decision(turn) if STRUCTURED_INTERACTIONS_ENABLED else None
    if decision and (turn.get("awaiting_input") is True or turn.get("complete") is not True):
        return make_decision_feed_item(turn, decision)
    if turn.get("has_open_turn") is True:
        return None
    if turn.get("complete") is not True:
        return None
    assistant_final = sanitize_text(str(turn.get("assistant_final_text") or ""), FINAL_REPLY_MAX_CHARS).strip()
    if not assistant_final:
        return None
    user_text = sanitize_text(str(turn.get("user_text") or ""), USER_PROMPT_MAX_CHARS).strip()
    raw_worklog = turn.get("worklog_text") or ("" if turn.get("has_open_turn") is True else turn.get("assistant_stream_text"))
    worklog_text = sanitize_text(str(raw_worklog or ""), FINAL_REPLY_MAX_CHARS).strip()
    text_parts: list[str] = []
    if user_text:
        text_parts.extend([USER_PROMPT_LABEL, user_text, ""])
    if worklog_text:
        text_parts.extend([WORKLOG_LABEL, worklog_text, ""])
    text_parts.append(assistant_final)
    return {
        "kind": "turn",
        "title": "",
        "summary": compact_block(assistant_final.splitlines()[:4], max_lines=4, max_chars=700),
        "detail": "",
        "lines": assistant_final.splitlines()[:FINAL_REPLY_MAX_LINES],
        "text": "\n".join(text_parts).strip(),
        "turn_id": sanitize_text(str(turn.get("turn_id") or ""), 300),
        "user_text": user_text,
        "worklog_text": worklog_text,
        "assistant_final_text": assistant_final,
        "notify": False,
    }


def ensure_request_started(entry: dict[str, Any], turn_id: str) -> str:
    clean_turn_id = sanitize_text(str(turn_id or ""), 300)
    if not clean_turn_id:
        return ""
    if str(entry.get("request_turn_id") or "") != clean_turn_id:
        entry["request_turn_id"] = clean_turn_id
        entry["request_started_at"] = utc_now()
        return str(entry["request_started_at"])
    started_at = str(entry.get("request_started_at") or "").strip()
    if not started_at:
        entry["request_started_at"] = utc_now()
        return str(entry["request_started_at"])
    return started_at


def request_started_at_for_turn(entry: dict[str, Any], turn_id: str) -> str:
    clean_turn_id = str(turn_id or "")
    if clean_turn_id and str(entry.get("request_turn_id") or "") == clean_turn_id:
        return str(entry.get("request_started_at") or "")
    if clean_turn_id and str(entry.get("last_prompt_turn_id") or "") == clean_turn_id:
        return str(entry.get("last_prompt_sent_at") or "")
    return ""


def worklog_label_for_turn(entry: dict[str, Any], turn_id: str) -> str:
    elapsed = request_elapsed_label(request_started_at_for_turn(entry, turn_id))
    if not elapsed:
        return WORKLOG_LABEL
    return f"{WORKLOG_LABEL} ({elapsed})"


def stream_user_text_for_turn(entry: dict[str, Any], turn_id: str) -> str:
    clean_turn_id = str(turn_id or "")
    if not clean_turn_id:
        return ""
    if str(entry.get("pending_prompt_turn_id") or "") == clean_turn_id:
        return sanitize_text(str(entry.get("pending_prompt_text") or ""), USER_PROMPT_MAX_CHARS).strip()
    if str(entry.get("last_prompt_turn_id") or "") == clean_turn_id:
        return sanitize_text(str(entry.get("last_prompt_text") or ""), USER_PROMPT_MAX_CHARS).strip()
    return ""


def apply_worklog_label(item: dict[str, Any] | None, entry: dict[str, Any]) -> dict[str, Any] | None:
    if not item or str(item.get("kind") or "").lower() != "turn":
        return item
    if not str(item.get("worklog_text") or item.get("assistant_stream_text") or "").strip():
        return item
    item["worklog_label"] = worklog_label_for_turn(entry, str(item.get("turn_id") or ""))
    return item


def turn_open_prompt(turn: dict[str, Any]) -> tuple[str, str]:
    if not isinstance(turn, dict) or turn.get("available") is not True:
        return "", ""
    if turn.get("has_open_turn") is True:
        turn_id = str(turn.get("open_turn_id") or "")
        user_text = str(turn.get("open_user_text") or "")
    elif turn.get("complete") is False:
        turn_id = str(turn.get("turn_id") or "")
        user_text = str(turn.get("user_text") or "")
    else:
        return "", ""
    user_text = sanitize_text(user_text, USER_PROMPT_MAX_CHARS).strip()
    if not turn_id or not user_text:
        return "", ""
    return sanitize_text(turn_id, 300), user_text


def make_prompt_feed_item(turn_id: str, user_text: str) -> dict[str, Any]:
    prompt = sanitize_text(user_text, USER_PROMPT_MAX_CHARS).strip()
    return {
        "kind": "prompt",
        "title": USER_PROMPT_LABEL,
        "summary": prompt,
        "detail": "",
        "lines": prompt.splitlines(),
        "text": f"{USER_PROMPT_LABEL}\n{prompt}".strip(),
        "turn_id": sanitize_text(str(turn_id or ""), 300),
        "user_text": prompt,
        "notify": False,
    }


def choice_options_compatible(left: dict[str, Any], right: dict[str, Any]) -> bool:
    left_options = list(left.get("options") or [])
    right_options = list(right.get("options") or [])
    if not left_options or not right_options:
        return False
    if len(left_options) != len(right_options):
        return False
    for left_opt, right_opt in zip(left_options[:4], right_options[:4]):
        left_label = re.sub(r"\s+", " ", str(left_opt.get("label") or "").strip()).lower()
        right_label = re.sub(r"\s+", " ", str(right_opt.get("label") or "").strip()).lower()
        if left_label and right_label and left_label != right_label:
            return False
    return True


def merge_visible_choice_item(visible_item: dict[str, Any], recent_item: dict[str, Any]) -> dict[str, Any]:
    if not choice_options_compatible(visible_item, recent_item):
        return visible_item
    merged = dict(visible_item)
    for field in ("summary", "detail", "text", "prompt_id"):
        value = recent_item.get(field)
        if isinstance(value, str) and value.strip():
            merged[field] = value
    recent_options = list(recent_item.get("options") or [])
    visible_options = list(visible_item.get("options") or [])
    if len(recent_options) == len(visible_options):
        merged_options: list[dict[str, str]] = []
        for visible_opt, recent_opt in zip(visible_options, recent_options):
            option = dict(visible_opt)
            if str(recent_opt.get("description") or "").strip():
                option["description"] = str(recent_opt.get("description") or "")
            merged_options.append(option)
        merged["options"] = merged_options
    return merged


def extract_visible_choice_feed_item(pane: dict[str, Any]) -> dict[str, Any] | None:
    pane_id = str(pane.get("pane_id") or "")
    if not pane_id:
        return None
    raw = pane_output(pane_id, lines=READ_LINES_COMMAND_MAX, max_chars=FEED_MAX_CHARS, source="visible")
    if not raw.strip():
        return None
    item = extract_choices(clean_feed_lines(raw))
    if not item:
        return None
    recent_raw = pane_output(pane_id, lines=READ_LINES_COMMAND_MAX, max_chars=FEED_MAX_CHARS, source="recent-unwrapped")
    if recent_raw.strip():
        recent_item = extract_choices(clean_feed_lines(recent_raw))
        if recent_item:
            item = merge_visible_choice_item(item, recent_item)
    prompt_id = str(item.get("prompt_id") or "")
    if prompt_id:
        item["turn_id"] = f"visible-choice:{prompt_id}"
        item["decision_id"] = prompt_id
    item["choice_source"] = "visible_scrape"
    item["title"] = "Decision needed"
    return item


def visible_readonly_prompt_note() -> str:
    return (
        "Visible-screen prompt only. Telegram buttons are disabled for safety. "
        "Answer in Herdr directly; use /send only for simple text replies."
    )


def mark_visible_prompt_readonly(item: dict[str, Any]) -> dict[str, Any]:
    readonly = dict(item)
    prompt_id = str(readonly.get("prompt_id") or prompt_id_for(item_plain_text(readonly), list(readonly.get("options") or [])))
    readonly["prompt_id"] = prompt_id
    readonly["turn_id"] = f"visible-readonly:{prompt_id}"
    readonly["choice_source"] = "visible_readonly"
    readonly["title"] = "Input needed"
    readonly["notify"] = True
    readonly.pop("decision_id", None)
    note = visible_readonly_prompt_note()
    detail = str(readonly.get("detail") or "").strip()
    if note not in detail:
        readonly["detail"] = f"{detail}\n\n{note}".strip() if detail else note
    text = str(readonly.get("text") or "").strip()
    if text and note not in text:
        readonly["text"] = f"{text}\n\n{note}"
    return readonly


def visible_action_question_text(lines: list[str]) -> str:
    semantic_lines = [strip_assistant_reply_marker(line).strip() for line in lines if str(line or "").strip()]
    search_from = max(0, len(semantic_lines) - 10)
    for idx in range(len(semantic_lines) - 1, search_from - 1, -1):
        current = semantic_lines[idx]
        if "?" in current and ACTION_QUESTION_RE.search(current):
            return sanitize_text(current, 700)
        window_lines = semantic_lines[max(search_from, idx - 2):idx + 1]
        window = compact_block(window_lines, max_lines=3, max_chars=700)
        if "?" in window and ACTION_QUESTION_RE.search(window):
            return window
    tail = compact_block(semantic_lines[-6:], max_lines=6, max_chars=800)
    return tail if "?" in tail and ACTION_QUESTION_RE.search(tail) else ""


def extract_visible_readonly_question_item(pane: dict[str, Any]) -> dict[str, Any] | None:
    pane_id = str(pane.get("pane_id") or "")
    if not pane_id:
        return None
    raw = pane_output(pane_id, lines=READ_LINES_COMMAND_MAX, max_chars=FEED_MAX_CHARS, source="visible")
    if not raw.strip():
        return None
    lines = clean_feed_lines(raw)
    if not lines or not is_action_question(lines):
        return None
    question = visible_action_question_text(lines)
    if not question:
        return None
    note = visible_readonly_prompt_note()
    item = make_feed_item("question", "Input needed", f"{question}\n\n{note}", notify=True)
    item["choice_source"] = "visible_readonly"
    item["turn_id"] = f"visible-readonly-question:{hashlib.sha256(question.encode('utf-8')).hexdigest()[:16]}"
    return item


def extract_visible_readonly_feed_item(pane: dict[str, Any]) -> dict[str, Any] | None:
    item = extract_visible_choice_feed_item(pane)
    if item:
        return mark_visible_prompt_readonly(item)
    return extract_visible_readonly_question_item(pane)


def select_turn_feed_item(turn: dict[str, Any], entry: dict[str, Any]) -> dict[str, Any] | None:
    """Pick the feed item for this turn, catching up on undelivered turns.

    When the adapter exposes recent completed turns and we have already
    delivered one of them, emit the OLDEST not-yet-delivered turn so a burst of
    completions (e.g. a human-prompted reply immediately followed by an
    auto-pursued turn) is delivered in order across successive sync cycles
    instead of collapsing to only the newest one. The cursor is the last
    *confirmed-delivered* turn (last_clean_item), so a failed send re-selects the
    same turn rather than skipping it. Falls back to the latest turn when there
    is nothing to catch up on or the delivered cursor is no longer in the window.
    """
    if isinstance(turn, dict):
        recent = turn.get("recent_turns")
        if isinstance(recent, list) and len(recent) >= 2:
            last_clean_turn = str((entry.get("last_clean_item") or {}).get("turn_id") or "")
            streamed = str(entry.get("last_stream_turn_id") or entry.get("pending_stream_turn_id") or "")
            legacy_turn = str(entry.get("last_turn_id") or "")
            delivered = last_clean_turn or ("" if streamed and legacy_turn == streamed else legacy_turn)
            ids = [str(t.get("turn_id") or "") for t in recent]
            if delivered and delivered in ids:
                idx = ids.index(delivered)
                if idx < len(recent) - 1:
                    catch_up = make_turn_feed_item(recent[idx + 1])
                    if catch_up:
                        return catch_up
            if not delivered and streamed and streamed in ids:
                streamed_item = make_turn_feed_item(recent[ids.index(streamed)])
                if streamed_item:
                    return streamed_item
    return make_turn_feed_item(turn)


def turn_item_precedes_prompt(item: dict[str, Any], turn: dict[str, Any], prompt_turn_id: str) -> bool:
    item_turn_id = str(item.get("turn_id") or "")
    prompt_id = str(prompt_turn_id or "")
    if not item_turn_id or not prompt_id or item_turn_id == prompt_id:
        return False
    if turn.get("has_open_turn") is True and prompt_id == str(turn.get("open_turn_id") or ""):
        return True
    recent = turn.get("recent_turns") if isinstance(turn, dict) else None
    if not isinstance(recent, list):
        return False
    ids = [str(candidate.get("turn_id") or "") for candidate in recent if isinstance(candidate, dict)]
    if item_turn_id not in ids or prompt_id not in ids:
        return False
    return ids.index(item_turn_id) < ids.index(prompt_id)


def record_suppressed_clean_item(entry: dict[str, Any], item: dict[str, Any], reason: str) -> None:
    item_render_hash = clean_feed_hash(item)
    item_semantic_hash = clean_feed_hash(item, include_render_version=False)
    entry["last_clean_hash"] = item_render_hash
    entry["last_clean_semantic_hash"] = item_semantic_hash
    entry["last_clean_render_hash"] = item_render_hash
    entry["last_clean_text"] = item_plain_text(item)
    entry["last_clean_item"] = item
    entry["last_clean_suppressed_reason"] = sanitize_text(reason, 120)
    entry["last_clean_suppressed_at"] = utc_now()
    entry.pop("last_clean_send_error", None)


def native_session_turn_owner_key(state: dict[str, Any], entry: dict[str, Any]) -> str:
    current_key = str(entry.get("pane_key") or "")
    space_key_value = str(entry.get("space_key") or "").strip()
    topic_id = str(entry.get("topic_id") or "").strip()
    agent = str(entry.get("agent") or "").strip().lower()
    session_id = str(entry.get("agent_session_id") or "").strip()
    if not (current_key and space_key_value and topic_id and agent and session_id):
        return current_key
    panes = state.get("panes") if isinstance(state.get("panes"), dict) else {}
    spaces = state.get("spaces") if isinstance(state.get("spaces"), dict) else {}
    space = spaces.get(space_key_value) if isinstance(spaces.get(space_key_value), dict) else {}
    ordered_keys: list[str] = []
    for key in space.get("pane_keys") or []:
        clean_key = str(key)
        if clean_key and clean_key not in ordered_keys:
            ordered_keys.append(clean_key)
    for key in sorted(str(key) for key in panes):
        if key not in ordered_keys:
            ordered_keys.append(key)
    for key in ordered_keys:
        other = panes.get(key)
        if not isinstance(other, dict):
            continue
        if str(other.get("last_known_status") or "").lower() == "closed":
            continue
        if str(other.get("space_key") or "").strip() != space_key_value:
            continue
        if str(other.get("topic_id") or "").strip() != topic_id:
            continue
        if str(other.get("agent") or "").strip().lower() != agent:
            continue
        if str(other.get("agent_session_id") or "").strip() != session_id:
            continue
        return key
    return current_key


def duplicate_native_session_owner_key(state: dict[str, Any], entry: dict[str, Any]) -> str:
    current_key = str(entry.get("pane_key") or "")
    owner_key = native_session_turn_owner_key(state, entry)
    if owner_key and current_key and owner_key != current_key:
        return owner_key
    return ""


def suppress_duplicate_native_session_prompt(entry: dict[str, Any], owner_key: str) -> bool:
    turn_id = str(entry.get("pending_prompt_turn_id") or "")
    if not turn_id:
        return False
    prompt_text = sanitize_text(str(entry.get("pending_prompt_text") or ""), USER_PROMPT_MAX_CHARS).strip()
    prompt_hash = str(entry.get("pending_prompt_hash") or (stream_text_hash(prompt_text) if prompt_text else ""))
    entry["last_prompt_turn_id"] = turn_id
    if prompt_hash:
        entry["last_prompt_hash"] = prompt_hash
    if prompt_text:
        entry["last_prompt_text"] = prompt_text
    entry["last_prompt_suppressed_reason"] = f"duplicate_native_session:{owner_key}"
    entry["last_prompt_suppressed_at"] = utc_now()
    for key in ("pending_prompt_turn_id", "pending_prompt_text", "pending_prompt_hash", "last_prompt_message_id"):
        entry.pop(key, None)
    return True


def suppress_duplicate_native_session_stream(entry: dict[str, Any], owner_key: str) -> bool:
    turn_id = str(entry.get("pending_stream_turn_id") or "")
    if not turn_id:
        return False
    stream_text = sanitize_text(str(entry.get("pending_stream_text") or ""), MAX_REPLY_CHARS).strip()
    user_text = stream_user_text_for_turn(entry, turn_id)
    stream_hash = stream_render_hash(stream_text, user_text) if stream_text else str(entry.get("pending_stream_revision") or "")
    same_turn = str(entry.get("last_stream_turn_id") or "") == turn_id
    try:
        update_count = int(entry.get("last_stream_update_count") or 0) if same_turn else 0
    except (TypeError, ValueError):
        update_count = 0
    entry["last_stream_turn_id"] = turn_id
    if stream_hash:
        entry["last_stream_hash"] = stream_hash
    if stream_text:
        entry["last_stream_text"] = stream_text
    entry["last_stream_update_count"] = update_count + 1
    entry["last_stream_suppressed_reason"] = f"duplicate_native_session:{owner_key}"
    entry["last_stream_suppressed_at"] = utc_now()
    for key in ("pending_stream_turn_id", "pending_stream_text", "pending_stream_revision", "last_stream_message_id", "last_stream_draft_id"):
        entry.pop(key, None)
    return True


def suppress_duplicate_native_session_item(entry: dict[str, Any], item: dict[str, Any] | None, owner_key: str) -> bool:
    if not item or str(item.get("kind") or "").lower() != "turn":
        return False
    if not str(item.get("turn_id") or "").strip():
        return False
    record_suppressed_clean_item(entry, item, f"duplicate_native_session:{owner_key}")
    clear_stream_state(entry)
    return True


def should_baseline_new_pane_turn(entry: dict[str, Any], item: dict[str, Any] | None, new_entry: bool) -> bool:
    if not new_entry or not item:
        return False
    if str(item.get("kind") or "").lower() != "turn":
        return False
    if not str(item.get("turn_id") or "").strip():
        return False
    if entry.get("last_clean_hash") or entry.get("last_clean_item"):
        return False
    if entry.get("last_prompt_message_id") or entry.get("last_stream_message_id"):
        return False
    return True


def attach_stream_worklog(item: dict[str, Any] | None, entry: dict[str, Any], turn: dict[str, Any]) -> dict[str, Any] | None:
    if not item or str(item.get("kind") or "").lower() != "turn":
        return item
    if item.get("worklog_text"):
        return apply_worklog_label(item, entry)
    item_turn_id = str(item.get("turn_id") or "")
    stream_turn_id = str(entry.get("last_stream_turn_id") or entry.get("pending_stream_turn_id") or "")
    if not stream_turn_id and turn.get("has_open_turn") is True:
        stream_turn_id = str(turn.get("open_turn_id") or "")
    if not stream_turn_id and turn.get("assistant_stream_text"):
        stream_turn_id = str(turn.get("turn_id") or "")
    if stream_turn_id and item_turn_id and item_turn_id != stream_turn_id:
        return item
    worklog_text = sanitize_text(
        str(turn.get("assistant_stream_text") or entry.get("last_stream_text") or entry.get("pending_stream_text") or ""),
        FINAL_REPLY_MAX_CHARS,
    ).strip()
    if worklog_text:
        item["worklog_text"] = worklog_text
    return apply_worklog_label(item, entry)


API_ERROR_NOTICE_TITLE = "⚠️ API error — pane stopped"


def api_error_notice_body(api_error: dict[str, Any]) -> str:
    code = sanitize_text(str(api_error.get("code") or "").strip(), 80)
    detail = sanitize_text(str(api_error.get("text") or "").strip(), 600)
    if not detail:
        detail = f"The model API returned an error ({code})." if code else "The model API returned an error."
    return (
        f"{detail}\n\n"
        "The agent stopped and is waiting on this. Reply in this topic (or use /send …) to tell it to continue."
    )


def apply_api_error_warning(
    chat_id: str,
    telegram: dict[str, Any] | None,
    entry: dict[str, Any],
    counters: dict[str, int],
    max_sends: int,
) -> dict[str, Any]:
    """Send a one-time ⚠️ warning for an unresolved model-API-error stall.

    The dedup id (last_api_error_id) is set ONLY after a successful send, so a
    capped/failed send retries next cycle. It is cleared ONLY on a reliable
    recovery (no pending error AND the turn is available again) — never on a
    transient adapter miss. Returns {'changed', 'topic_missing'}.
    """
    pending = entry.get("pending_api_error") if isinstance(entry.get("pending_api_error"), dict) else None
    if pending and entry.get("topic_id"):
        err_id = str(pending.get("id") or "")
        if err_id and err_id != str(entry.get("last_api_error_id") or "") and counters.get("sends", 0) < max_sends:
            notice = send_notice(
                chat_id,
                API_ERROR_NOTICE_TITLE,
                api_error_notice_body(pending),
                telegram=telegram,
                thread_id=entry["topic_id"],
                notify=True,
                reply_to_message_id=pane_root_reply_target(entry),
                api_token=managed_bot_token_for_entry(telegram, entry),
            )
            if notice.get("ok"):
                entry["last_api_error_id"] = err_id
                counters["sends"] = counters.get("sends", 0) + 1
                return {
                    "changed": True,
                    "topic_missing": False,
                    "message_id": str(notice.get("message_id") or ""),
                }
            if result_topic_missing(notice):
                return {"changed": False, "topic_missing": True}
            if result_pane_root_missing(notice):
                return {
                    "changed": False,
                    "topic_missing": False,
                    "pane_root_missing": True,
                    "error": str(notice.get("error") or notice),
                }
        return {"changed": False, "topic_missing": False}
    if not pending and entry.get("last_api_error_id") and entry.get("last_turn_available") is True:
        entry.pop("last_api_error_id", None)
        return {"changed": True, "topic_missing": False}
    return {"changed": False, "topic_missing": False}


def extract_turn_feed_item(
    pane: dict[str, Any], entry: dict[str, Any], *, allow_visible_fallback: bool = True
) -> dict[str, Any] | None:
    turn = cached_pane_turn(str(pane.get("pane_id") or ""))
    available = bool(turn.get("available", True))
    reason = sanitize_text(str(turn.get("reason") or ""), 300)
    if entry.get("last_turn_available") != available or str(entry.get("last_turn_reason") or "") != reason:
        entry["last_turn_available"] = available
        if reason:
            entry["last_turn_reason"] = reason
        else:
            entry.pop("last_turn_reason", None)
    status = str(pane.get("agent_status") or "").strip().lower()
    # Record an unresolved model-API-error stall (detected from the transcript:
    # Claude logs isApiErrorMessage even when agent_status doesn't reflect the
    # stop). sync_pane_once posts a one-time ⚠️ warning from this. We trust the
    # transcript rather than agent_status; the adapter clears it on a completed
    # turn or a new user prompt, so it won't linger or false-alarm.
    api_error = turn.get("api_error") if isinstance(turn, dict) else None
    if api_error:
        entry["pending_api_error"] = api_error
    else:
        entry.pop("pending_api_error", None)
    item = attach_stream_worklog(select_turn_feed_item(turn, entry), entry, turn)
    # Prefer the actual completed turn over any visible-screen scrape. This
    # covers the auto-continue case AND the done->working status-lag race: even
    # if the status momentarily reads non-working while the terminal has already
    # started the next spinner, we deliver the real completed message (deduped
    # downstream) rather than scraping in-progress screen output. Only a
    # genuinely blocked pane (a real on-screen prompt awaiting the user) bypasses
    # this to the visible path.
    if not item and status != "blocked" and turn.get("complete") is True and turn.get("has_open_turn") is True:
        item = attach_stream_worklog(make_turn_feed_item({**turn, "has_open_turn": False, "assistant_stream_text": ""}), entry, turn)
    prompt_turn_id, prompt_text = turn_open_prompt(turn)
    if item and str(item.get("turn_id") or "") == prompt_turn_id and str(item.get("kind") or "").lower() in {
        "decision",
        "interaction_readonly",
    }:
        prompt_turn_id = ""
        prompt_text = ""
    newest_prompt_turn_id = prompt_turn_id or str(entry.get("last_prompt_turn_id") or "")
    item_suppressed_for_newer_prompt = False
    if item and turn_item_precedes_prompt(item, turn, newest_prompt_turn_id):
        record_suppressed_clean_item(entry, item, "newer_prompt_already_visible")
        item = None
        item_suppressed_for_newer_prompt = True
    prompt_hash = stream_text_hash(prompt_text) if prompt_turn_id and prompt_text else ""
    if prompt_turn_id and prompt_text:
        ensure_request_started(entry, prompt_turn_id)
    prompt_already_sent = str(entry.get("last_prompt_turn_id") or "") == prompt_turn_id and (
        not str(entry.get("last_prompt_hash") or "") or str(entry.get("last_prompt_hash") or "") == prompt_hash
    )
    if prompt_turn_id and not prompt_already_sent:
        entry["pending_prompt_turn_id"] = prompt_turn_id
        entry["pending_prompt_text"] = prompt_text
        entry["pending_prompt_hash"] = prompt_hash
    else:
        entry.pop("pending_prompt_turn_id", None)
        entry.pop("pending_prompt_text", None)
        entry.pop("pending_prompt_hash", None)
    stream_text = sanitize_text(str(turn.get("assistant_stream_text") or "").strip(), MAX_REPLY_CHARS)
    stream_turn_id = ""
    if stream_text and turn.get("complete") is False:
        stream_turn_id = str(turn.get("turn_id") or "")
    elif stream_text and turn.get("has_open_turn") is True:
        if not item and item_suppressed_for_newer_prompt:
            stream_turn_id = str(turn.get("open_turn_id") or turn.get("turn_id") or "")
        elif item:
            item_semantic_hash = clean_feed_hash(item, include_render_version=False)
            if str(item.get("kind") or "").lower() == "turn" and same_delivered_content(entry, item, item_semantic_hash):
                item = None
                stream_turn_id = str(turn.get("open_turn_id") or turn.get("turn_id") or "")
    if not item and stream_turn_id:
        entry["pending_stream_turn_id"] = stream_turn_id
        entry["pending_stream_text"] = stream_text
        entry["pending_stream_revision"] = str(turn.get("stream_revision") or stream_text_hash(stream_text))
        entry["last_turn_id"] = stream_turn_id
        return None
    entry.pop("pending_stream_turn_id", None)
    entry.pop("pending_stream_text", None)
    entry.pop("pending_stream_revision", None)
    # Visible-screen prompt fallback: never scrape an actively-working pane — its
    # screen is its own in-progress output (spinner, tool noise, the echo of an
    # already-delivered reply), which produced the "Input needed" spam and
    # whole-screen blobs. Genuine prompts surface in blocked/idle states.
    if not item and allow_visible_fallback and status not in ACTIVE_AGENT_STATUSES:
        if VISIBLE_CHOICE_BUTTONS_ENABLED:
            item = extract_visible_choice_feed_item(pane)
        elif VISIBLE_READONLY_PROMPTS_ENABLED:
            item = extract_visible_readonly_feed_item(pane)
    return item


def clear_delivered_stream_state(entry: dict[str, Any]) -> None:
    for key in (
        "last_stream_hash",
        "last_stream_turn_id",
        "last_stream_draft_id",
        "last_stream_message_id",
        "last_stream_text",
        "last_stream_sent_at",
        "last_stream_error",
        "last_stream_transport",
        "last_stream_update_count",
        "last_stream_bot_kind",
        "last_stream_bot_kind_retry_kind",
        "last_stream_bot_kind_retry_at",
    ):
        entry.pop(key, None)


def clear_stream_state(entry: dict[str, Any]) -> None:
    for key in (
        "pending_stream_turn_id",
        "pending_stream_text",
        "pending_stream_revision",
    ):
        entry.pop(key, None)
    clear_delivered_stream_state(entry)


def item_plain_text(item: dict[str, Any]) -> str:
    if str(item.get("kind") or "").lower() == "turn":
        user_text = str(item.get("user_text") or "").strip()
        worklog_text = str(item.get("worklog_text") or "").strip()
        assistant_final = str(item.get("assistant_final_text") or "").strip()
        parts: list[str] = []
        if user_text:
            parts.extend([USER_PROMPT_LABEL, user_text, ""])
        if worklog_text:
            parts.extend([WORKLOG_LABEL, worklog_text, ""])
        if assistant_final:
            parts.append(assistant_final)
        return sanitize_text("\n".join(parts).strip(), FINAL_REPLY_MAX_CHARS)
    if str(item.get("kind") or "").lower() == "decision":
        user_text = str(item.get("user_text") or "").strip()
        assistant_context = str(item.get("assistant_final_text") or "").strip()
        prompt = str(item.get("summary") or item.get("text") or "").strip()
        options = list(item.get("options") or [])
        parts: list[str] = []
        if user_text:
            parts.extend([USER_PROMPT_LABEL, user_text, ""])
        if assistant_context:
            parts.extend([assistant_context, ""])
        if prompt:
            parts.append(prompt)
        if options:
            parts.append("")
            parts.extend(f"{opt.get('number')}) {opt.get('label')}" for opt in options)
        return sanitize_text("\n".join(parts).strip(), FINAL_REPLY_MAX_CHARS)
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
    clean = _rich_inline(value, MAX_RICH_DETAIL_CHARS).strip()
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
    if re.match(r"^#{1,6}\s+\S", stripped):
        # Markdown ATX heading (e.g. "# Title"), not a shell comment.
        return False
    if raw.startswith(("    ", "\t")) or stripped.startswith(("$ ", "./")):
        return True
    if re.match(r"^(cd|python3?|pip3?|npm|pnpm|yarn|node|git|gh|curl|ssh|systemctl|journalctl|herdr)\b", stripped):
        # Treat as a command only when it looks command-shaped (short, or has a
        # flag/path/operator) so prose like "git handles version control" stays prose.
        return len(stripped.split()) <= 3 or bool(re.search(r"(?:^|\s)[-/]|--|[=|<>]", stripped))
    if re.fullmatch(CODE_ENV_ASSIGN_BRANCH, stripped):
        return True
    return False


def looks_like_path_or_symbol(value: str) -> bool:
    clean = str(value or "").strip()
    if not clean or len(clean.split()) > 3:
        return False
    if re.fullmatch(CODE_SYMBOL_BRANCH, clean):
        return True
    if re.fullmatch(SYMBOL_CODE_HASH_BRANCH, clean, re.IGNORECASE):
        return True
    if re.fullmatch(rf"commit\s+{SYMBOL_CODE_HASH_BRANCH}", clean, re.IGNORECASE):
        return True
    if re.search(r"(^|[~/\\])[\w.+-]+(?:/[\w.+-]+)+(?::\d+)?$", clean):
        return True
    if re.search(rf"\b[\w.+-]+\.(?:{PATH_OR_SYMBOL_FILE_EXT_RE})(?::\d+)?$", clean):
        return True
    return False


def _apply_emphasis(rendered: str) -> str:
    rendered = re.sub(r"\*\*\*([^\n]+?)\*\*\*", r"<b><i>\1</i></b>", rendered)
    rendered = re.sub(r"\*\*([^\n]+?)\*\*", r"<b>\1</b>", rendered)
    rendered = re.sub(r"(?<![\w*])\*(?!\s)([^*\n]+?)(?<!\s)\*(?![\w*])", r"<i>\1</i>", rendered)
    rendered = re.sub(r"___([^\n]+?)___", r"<b><i>\1</i></b>", rendered)
    rendered = re.sub(r"__([^\n]+?)__", r"<b>\1</b>", rendered)
    rendered = re.sub(r"(?<![\w_])_(?!\s)([^_\n]+?)(?<!\s)_(?![\w_])", r"<i>\1</i>", rendered)
    rendered = re.sub(r"~~([^\s~][^\n]*?)~~", r"<s>\1</s>", rendered)
    return rendered


def _rich_code_and_escape(text: str) -> str:
    parts: list[str] = []
    pos = 0
    for match in TOKEN_CODE_RE.finditer(text):
        parts.append(html.escape(text[pos:match.start()], quote=False).replace("`", ""))
        code = match.group(1)
        trailing = TOKEN_CODE_TRAILING_PUNCT_RE.search(code)
        code_end = match.end()
        if trailing:
            code_end -= len(trailing.group(0))
            code = code[: trailing.start()]
        parts.append(f"<code>{html.escape(code, quote=False)}</code>")
        pos = code_end
    parts.append(html.escape(text[pos:], quote=False).replace("`", ""))
    return "".join(parts)


def _rich_text_segment(value: str) -> str:
    text = str(value or "")
    stash: list[str] = []

    def _keep(fragment: str) -> str:
        stash.append(fragment)
        return f"{_PH_OPEN}{len(stash) - 1}{_PH_CLOSE}"

    def _image(match: "re.Match[str]") -> str:
        url = match.group(2).strip()
        label = match.group(1).strip() or url
        return _keep(f'<a href="{html.escape(url, quote=True)}">{html.escape(label, quote=False)}</a>')

    def _link(match: "re.Match[str]") -> str:
        label = _apply_emphasis(_rich_code_and_escape(match.group(1)))
        url = match.group(2).strip()
        return _keep(f'<a href="{html.escape(url, quote=True)}">{label}</a>')

    def _bare(match: "re.Match[str]") -> str:
        url = match.group(0)
        return _keep(f'<a href="{html.escape(url, quote=True)}">{html.escape(url, quote=False)}</a>')

    # Mask inline `code` first so emphasis can span it (e.g. **bold `x` more**)
    # and so its content is never touched by link/emphasis passes.
    text = INLINE_CODE_RE.sub(lambda m: _keep(f"<code>{_html_text(m.group(1))}</code>"), text)
    text = MD_IMAGE_RE.sub(_image, text)
    text = MD_LINK_RE.sub(_link, text)
    text = MATH_SPAN_RE.sub(lambda m: _keep(html.escape(m.group(0), quote=False)), text)
    text = BARE_URL_RE.sub(_bare, text)

    rendered = _apply_emphasis(_rich_code_and_escape(text))
    if stash:
        rendered = _PH_RE.sub(lambda m: stash[int(m.group(1))], rendered)
    return rendered


def _rich_inline(value: str, max_chars: int = 500) -> str:
    clean = str(value or "").strip()
    if not clean:
        return ""
    if "`" not in clean and len(clean) <= 80 and (looks_like_path_or_symbol(clean) or _is_codeish_line(clean)):
        return f"<code>{_html_text(clean, max_chars)}</code>"
    return _rich_text_segment(sanitize_text(clean, max_chars))


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


def _atx_heading_title(line: str) -> str | None:
    s = str(line or "").strip()
    if re.match(r"^#{1,6}\s+\S", s):
        return re.sub(r"^#{1,6}\s+", "", s).strip().rstrip("#").strip()
    return None


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
                        or _atx_heading_title(continuation)
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
                        or _atx_heading_title(continuation)
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

        atx_title = _atx_heading_title(line)
        if atx_title:
            parts.append(f"<b>{_html_text(atx_title, 120)}</b>")
            idx += 1
            continue

        next_line = lines[idx + 1] if idx + 1 < len(lines) else None
        path_section = _split_path_section(line)
        if path_section:
            heading, ref = path_section
            parts.append(f"<b>{_html_text(heading, 100)}</b>")
            parts.append(f"<p><code>{_html_text(ref, 300)}</code></p>")
            idx += 1
            continue

        if _looks_like_section(line, next_line):
            parts.append(f"<b>{_html_text(line.rstrip(':'), 100)}</b>")
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
    return _join_blocks(parts), overflow


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
        section = section_alias(str(raw or ""))
        if section:
            flush()
            has_structured = True
            current_kind, current_title, _label = section
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
    if text.endswith("|") and not text.endswith("\\|"):
        text = text[:-1]
    cells: list[str] = []
    buf: list[str] = []
    in_code = False
    i = 0
    while i < len(text):
        ch = text[i]
        if ch == "\\" and i + 1 < len(text) and text[i + 1] == "|":
            buf.append("|")
            i += 2
            continue
        if ch == "`":
            in_code = not in_code
            buf.append(ch)
        elif ch == "|" and not in_code:
            cells.append("".join(buf).strip())
            buf = []
        else:
            buf.append(ch)
        i += 1
    cells.append("".join(buf).strip())
    return cells if len(cells) >= 2 and any(cells) else []


def _is_table_separator(cells: list[str]) -> bool:
    return bool(cells) and all(re.fullmatch(r":?-{2,}:?", cell or "") for cell in cells)


def _rich_table_section(lines: list[str], *, rich_cells: bool = False) -> str:
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
    cell_html = _rich_inline if rich_cells else _html_text
    html_rows = [
        "<tr>" + "".join(f"<th>{cell_html(cell, 160)}</th>" for cell in header) + "</tr>",
    ]
    html_rows.extend(
        "<tr>" + "".join(f"<td>{cell_html(cell, 160)}</td>" for cell in row) + "</tr>"
        for row in body
    )
    return "<table bordered striped>\n" + "\n".join(html_rows) + "\n</table>"


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


def _looks_like_code_detail(title: str, lines: list[str]) -> bool:
    low = re.sub(r"\s+", " ", str(title or "").strip()).lower()
    if low in CODE_DETAILS_SECTIONS:
        return True
    content = [line for line in lines if line.strip()]
    if not content:
        return False
    codeish = 0
    for line in content:
        stripped = line.strip()
        if _is_codeish_line(line) or stripped.startswith(("{", "[")) or stripped.startswith(("diff ", "@@")):
            codeish += 1
    return codeish >= max(1, len(content) // 2)


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
                parts.append(f"<b>{_html_text(title, 100)}</b>")
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
            parts.append(f"<b>{_html_text(heading, 100)}</b>")
            if checklist_html:
                parts.append(checklist_html)
            elif body:
                block, _ = _rich_structured_block(body, max_chars=1200, max_lines=30)
                if block:
                    parts.append(block)
            continue
        if kind == "details":
            summary = title or "Details"
            if _looks_like_code_detail(summary, body):
                proof = "\n".join(line.rstrip() for line in body if line.strip())
                block = f"<pre><code>{_html_text(proof, 1800)}</code></pre>" if proof else ""
            else:
                block, _ = _rich_structured_block(body, max_chars=1800, max_lines=40)
            if block:
                parts.append(f"<details><summary>{_html_text(summary, 100)}</summary>{block}</details>")
            continue
        if kind == "footer":
            footer = compact_block(body, max_lines=3, max_chars=500)
            if footer:
                parts.append(f"<footer>{_html_text(footer, 500)}</footer>")
            continue
    return _join_blocks(parts)


def _rich_lines_block(value: str, *, max_chars: int = MAX_RICH_DETAIL_CHARS) -> str:
    block, overflow = _rich_structured_block(value, max_chars=max_chars, max_lines=12)
    if overflow:
        overflow_block, _ = _rich_structured_block(overflow, max_chars=900, max_lines=8)
        if overflow_block:
            block += f"<br>{overflow_block}"
    return block


def _rich_options_block(options: list[dict[str, str]]) -> str:
    if not options:
        return ""
    numbered: list[tuple[int, str, str]] = []
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
        numbered.append((number, str(opt.get("label") or ""), str(opt.get("description") or "")))
    if sequential:
        rendered_items: list[str] = []
        for _number, label, description in numbered:
            body = _html_text(label, 180)
            if description.strip():
                body += f"<br><small>{_rich_inline(description, 500)}</small>"
            rendered_items.append(f"<li>{body}</li>")
        items = "\n".join(rendered_items)
        return f"<ol>\n{items}\n</ol>"
    return "\n".join(
        _rich_paragraph(f"{number}) {label}" + (f"\n{description}" if description else ""))
        for number, label, description in numbered
    )


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


def _blockquote_text(value: str, max_chars: int) -> str:
    lines = sanitize_text(value, max_chars).splitlines()
    return "<br>".join(_rich_inline(line, max_chars) for line in lines)


def _prompt_should_collapse(user_text: str, collapse_chars: int) -> bool:
    if collapse_chars <= 0:
        return False  # 0 (or negative) = never collapse
    if collapse_chars == 1:
        return True   # 1 = always collapse
    return len(str(user_text or "")) > collapse_chars  # N>1 = threshold


def _item_prompt_collapse_chars(item: dict[str, Any]) -> int:
    try:
        return max(0, int(item.get("prompt_collapse_chars") or 0))
    except (TypeError, ValueError):
        return 0


def _prompt_preview(user_text: str) -> str:
    # First non-empty line of the prompt, trimmed to a short scannable preview.
    for line in str(user_text or "").splitlines():
        line = line.strip()
        if line:
            return line[:PROMPT_PREVIEW_CHARS]
    return ""


def render_user_prompt_quote_html(user_text: str, collapse_chars: int = 0) -> str:
    body = _blockquote_text(user_text, USER_PROMPT_MAX_CHARS).strip()
    if not body:
        return ""
    collapse = _prompt_should_collapse(user_text, collapse_chars)
    # When collapsed, surface a short preview in the summary so the prompt is
    # recognizable without expanding (e.g. "User: look into the emoji…").
    preview = _prompt_preview(user_text) if collapse else ""
    return _rich_details_quote_html(
        USER_PROMPT_LABEL,
        body,
        summary_max_chars=20,
        open_by_default=not collapse,
        preview=preview,
    )


def _rich_details_quote_html(
    summary: str,
    body_html: str,
    *,
    summary_max_chars: int = 80,
    open_by_default: bool = True,
    quote: bool = True,
    preview: str = "",
) -> str:
    body = str(body_html or "").strip()
    if not body:
        return ""
    label = _html_text(summary, summary_max_chars)
    open_attr = " open" if open_by_default else ""
    # An optional preview rides AFTER the bold label inside the summary so a
    # collapsed block is recognizable at a glance: <summary><b>User:</b> preview…</summary>
    preview_html = ""
    preview = str(preview or "").strip()
    if preview:
        preview_html = " " + _html_text(preview, PROMPT_PREVIEW_CHARS + 6)
    # quote=True wraps the body in a <blockquote> (the blue quote) — used for the
    # user prompt and worklog. quote=False keeps the body un-flattened so rich
    # content (headings/tables/code) renders properly — used for the response.
    inner = f"<blockquote>{body}</blockquote>" if quote else body
    return f"<details{open_attr}><summary><b>{label}</b>{preview_html}</summary>{inner}</details>"


TURN_COLLAPSED_SECTION_KEYS = {"proof", "logs", "commands", "diff", "raw output", "raw"}
TURN_KNOWN_HEADING_KEYS = {
    "implemented",
    "pushed",
    "verification",
    "what changed",
    "what i did",
    "recommended follow-ups",
    "recommended follow ups",
    "next steps",
    "risks",
    "proof",
    "details",
    "deployment",
    "deployed",
    "summary",
    "result",
}
TURN_STATUS_HEADING_KEYS = {
    "fixed",
    "done",
    "failed",
    "resolved",
    "blocked",
    "reverted",
    "shipped",
    "deployed",
}
TURN_INLINE_SECTION_LABELS = {
    "what happened": "What happened",
    "why it happened": "Why it happened",
    "impact": "Impact",
    "fix": "Fix",
    "current status": "Current status",
    "next step": "Next step",
    "result": "Result",
    "verification": "Verification",
}
TURN_INLINE_SECTION_RE = re.compile(
    r"^("
    + "|".join(re.escape(label) for label in sorted(TURN_INLINE_SECTION_LABELS, key=len, reverse=True))
    + r"):\s+(.+)$",
    re.IGNORECASE,
)


def _plain_heading_title(value: str) -> str:
    clean = str(value or "").strip()
    clean = re.sub(r"`([^`\n]{1,300})`", r"\1", clean)
    clean = re.sub(r"\*\*\*([^\n]+?)\*\*\*", r"\1", clean)
    clean = re.sub(r"\*\*([^\n]+?)\*\*", r"\1", clean)
    clean = re.sub(r"(?<!\*)\*([^*\n]{1,180})\*(?!\*)", r"\1", clean)
    clean = re.sub(r"__([^\n]+?)__", r"\1", clean)
    clean = re.sub(r"~~([^\n]+?)~~", r"\1", clean)
    clean = clean.strip(" -*_`~")
    return re.sub(r"\s+", " ", clean).strip()


def _turn_heading_title(line: str) -> str:
    clean = re.sub(r"^\s{0,3}#{1,6}\s+", "", str(line or "").strip())
    clean = clean.rstrip(":").rstrip(".").strip()
    return re.sub(r"\s+", " ", clean)


def _next_nonempty_line(lines: list[str], start: int) -> str | None:
    for idx in range(start, len(lines)):
        if str(lines[idx] or "").strip():
            return str(lines[idx])
    return None


def _is_table_line(line: str) -> bool:
    return bool(_table_cells(line))


def _is_turn_heading_line(
    line: str,
    next_nonempty: str | None,
    *,
    first_block: bool = False,
    previous_blank: bool = False,
) -> bool:
    clean = str(line or "").strip()
    if not clean or len(clean) > 120:
        return False
    if HRULE_RE.match(clean):
        return False
    if FENCE_START_RE.match(clean) or _bullet_text(clean) or _numbered_text(clean) or _checklist_item(clean):
        return False
    if re.match(r"^#{1,6}\s+\S", clean):
        # Explicit Markdown heading: always a heading, regardless of length/inline code.
        return True
    if clean.startswith(">") or _is_table_line(clean) or _is_codeish_line(clean):
        return False
    words = _turn_heading_title(clean).split()
    if not 1 <= len(words) <= 6:
        return False
    if "`" in clean and len(words) > 3:
        return False
    key = _turn_heading_title(clean).lower()
    if key in TURN_KNOWN_HEADING_KEYS:
        return True
    if clean.endswith(":"):
        return True
    if first_block and next_nonempty and len(words) <= 4 and clean[:1].isupper() and not clean.endswith(("!", "?")) and not (len(words) > 1 and clean.endswith(".")):
        return True
    if previous_blank and next_nonempty and len(words) <= 4 and clean[:1].isupper() and not clean.endswith(("!", "?")) and not (len(words) > 1 and clean.endswith(".")):
        return True
    return False


def _rich_commit_line(line: str) -> str | None:
    match = COMMIT_LINE_RE.match(str(line or "").strip())
    if not match:
        return None
    return f"<p><code>{_html_text(match.group(1), 40)}</code> {_rich_inline(match.group(2), 500)}</p>"


def _lead_heading_split(line: str, *, allow_status_title: bool = False) -> tuple[str, str] | None:
    clean = str(line or "").strip()
    match = re.match(r"^(.{2,100}?)\s+[—–]\s+(.+)$", clean)
    if not match:
        return None
    raw_title = match.group(1).strip()
    rest = match.group(2).strip()
    meta_match = re.search(r"\s*\((`[^`\n]{1,120}`)\)\s*$", raw_title)
    if meta_match:
        raw_title = raw_title[:meta_match.start()].strip()
        rest = f"{meta_match.group(1)} — {rest}"
    title = _plain_heading_title(raw_title).rstrip(":").rstrip(".").strip()
    words = title.split()
    title_key = title.lower()
    if len(words) == 1 and allow_status_title and title_key in TURN_STATUS_HEADING_KEYS:
        pass
    elif not 2 <= len(words) <= 5 or ":" in title:
        return None
    if title_key in {"yes", "no", "ok", "okay"}:
        return None
    if title.endswith(("?", "!", ",")):
        return None
    if not rest:
        return None
    return title, rest


def _inline_section_split(line: str) -> tuple[str, str] | None:
    clean = str(line or "").strip()
    match = TURN_INLINE_SECTION_RE.match(clean)
    if not match:
        return None
    title = TURN_INLINE_SECTION_LABELS.get(match.group(1).lower())
    body = match.group(2).strip()
    if not title or not body:
        return None
    return title, body


def _split_long_paragraph(value: str, *, max_chars: int = 360) -> list[str]:
    text = re.sub(r"\s+", " ", str(value or "").strip())
    if not text:
        return []
    if len(text) <= max_chars:
        return [text]
    sentences = re.split(r"(?<=[.!?])\s+(?=[A-Z0-9\"'`*(])", text)
    chunks: list[str] = []
    current = ""
    for sentence in sentences:
        sentence = sentence.strip()
        if not sentence:
            continue
        if not current:
            current = sentence
            continue
        if len(current) + 1 + len(sentence) <= max_chars:
            current = f"{current} {sentence}"
            continue
        chunks.append(current)
        current = sentence
    if current:
        chunks.append(current)

    expanded: list[str] = []
    for chunk in chunks:
        if len(chunk) <= max_chars + 120:
            expanded.append(chunk)
            continue
        parts = re.split(r"\s+(?:;\s+|→\s+)", chunk)
        if len(parts) <= 1:
            expanded.append(chunk)
            continue
        buf = ""
        for part in parts:
            part = part.strip()
            if not part:
                continue
            if not buf:
                buf = part
            elif len(buf) + 1 + len(part) <= max_chars:
                buf = f"{buf} {part}"
            else:
                expanded.append(buf)
                buf = part
        if buf:
            expanded.append(buf)
    return expanded or [text]


def _rich_paragraph_blocks(value: str) -> list[str]:
    return [block for block in (_rich_paragraph(chunk) for chunk in _split_long_paragraph(value)) if block]


TEXT_FENCE_LANGS = {"text", "txt", "plain", "plaintext", "markdown", "md"}


def _text_fence_is_preformatted(code_lines: list[str]) -> bool:
    # A ```text fence usually holds prose / a numbered list (render as readable
    # text), but sometimes holds aligned tables, command syntax, or indented
    # output that NEEDS monospace. Keep it as a code block in that case.
    lines = [ln for ln in code_lines if ln.strip()]
    if not lines:
        return False
    listish = sum(1 for ln in lines if re.match(r"^\s*(?:\d{1,2}[.)]|[-*+•])\s", ln))
    if listish * 2 >= len(lines):
        return False
    pre = 0
    for ln in lines:
        if re.search(r"\S {2,}\S", ln) or "|" in ln:
            pre += 1
        elif re.search(r"[{};]|=>|::|\b(?:def|class|import|function|sudo|systemctl|journalctl|curl|ssh|export|chmod|mkdir)\b", ln):
            pre += 1
        elif ln[:1] in (" ", "\t"):
            pre += 1
    return pre * 2 >= len(lines)


def _render_text_fence(code_lines: list[str]) -> str:
    # Codex wraps prose / numbered steps / field lists in ```text fences. Render
    # them as readable text (line breaks preserved via <br>, blank lines as gaps),
    # not a monospace code block.
    groups: list[list[str]] = [[]]
    for ln in code_lines:
        if not ln.strip():
            if groups[-1]:
                groups.append([])
            continue
        rendered = _rich_inline(ln, 900)
        if rendered:
            groups[-1].append(rendered)
    blocks = ["<br>".join(g) for g in groups if g]
    return "<br><br>".join(blocks)


_RICH_TOP_BLOCK_TAGS = ("p", "h[1-6]", "table", "ul", "ol", "blockquote", "pre", "details", "footer", "b")
_RICH_SPACIOUS_BLOCK_TAGS = ("pre", "h[1-6]", "ul", "ol", "blockquote", "details", "table")
_RICH_PLAIN_BREAK_EXTRA_TAGS = ("li", "tr", "summary")


def _rich_tag_pattern(tags: tuple[str, ...]) -> str:
    return "|".join(tags)


_RICH_TOP_BLOCK_TAG_RE = _rich_tag_pattern(_RICH_TOP_BLOCK_TAGS)
_RICH_SPACIOUS_BLOCK_TAG_RE = _rich_tag_pattern(_RICH_SPACIOUS_BLOCK_TAGS)
_RICH_PLAIN_BREAK_TAG_RE = _rich_tag_pattern(
    tuple(tag for tag in _RICH_TOP_BLOCK_TAGS if tag != "b") + _RICH_PLAIN_BREAK_EXTRA_TAGS
)
_SPACIOUS_END = re.compile(rf"</(?:{_RICH_SPACIOUS_BLOCK_TAG_RE})>$")
_SPACIOUS_START = re.compile(rf"^<(?:{_RICH_SPACIOUS_BLOCK_TAG_RE})\b")


def _join_blocks(parts: list[str]) -> str:
    # The gateway renders block elements (<pre>, <h3>, <ul>, <blockquote>...) with
    # their own vertical margins, but flows plain text / <p> / inline <b> tightly.
    # So only insert a <br> between two flow blocks; never next to a block that
    # already carries its own margin (adding one there double-spaces).
    kept = [p for p in parts if p and p.strip()]
    if not kept:
        return ""
    result = kept[0]
    for part in kept[1:]:
        prev = result.rstrip()
        nxt = part.lstrip()
        if _SPACIOUS_END.search(prev) or _SPACIOUS_START.match(nxt):
            sep = ""               # block element carries its own margin
        elif prev.endswith(">"):
            sep = "<br>"           # flow block already breaks one line -> one <br> = blank line
        else:
            sep = "<br><br>"       # plain-text-ending block (e.g. text-fenced list) -> two <br>
        result += sep + part
    return result


def _render_final_reply_blocks(lines: list[str], *, seen_heading: bool = False) -> str:
    parts: list[str] = []
    idx = 0
    previous_blank = True
    while idx < len(lines):
        line = str(lines[idx] or "").rstrip()
        if not line.strip():
            previous_blank = True
            idx += 1
            continue

        if HRULE_RE.match(line.strip()):
            # Thematic break: render as vertical spacing, never a "---" heading.
            previous_blank = True
            idx += 1
            continue

        fence = FENCE_START_RE.match(line)
        if fence:
            marker = fence.group(1)[0]
            language = fence.group(2).strip()
            code_lines: list[str] = []
            idx += 1
            while idx < len(lines) and not str(lines[idx]).strip().startswith(marker * 3):
                code_lines.append(str(lines[idx]).rstrip())
                idx += 1
            if idx < len(lines):
                idx += 1
            if language.lower() in TEXT_FENCE_LANGS and not _text_fence_is_preformatted(code_lines):
                rendered_fence = _render_text_fence(code_lines)
                if rendered_fence:
                    parts.append(rendered_fence)
                previous_blank = False
                continue
            class_attr = f' class="language-{html.escape(language, quote=True)}"' if language else ""
            parts.append(f"<pre><code{class_attr}>{_html_text(chr(10).join(code_lines), 3000)}</code></pre>")
            previous_blank = False
            continue

        next_nonempty = _next_nonempty_line(lines, idx + 1)
        if _is_turn_heading_line(line, next_nonempty, first_block=not seen_heading, previous_blank=previous_blank):
            title = _turn_heading_title(line)
            key = title.lower()
            if key in TURN_COLLAPSED_SECTION_KEYS:
                body_lines: list[str] = []
                idx += 1
                while idx < len(lines):
                    candidate = str(lines[idx] or "").rstrip()
                    candidate_next = _next_nonempty_line(lines, idx + 1)
                    if candidate.strip() and _is_turn_heading_line(
                        candidate,
                        candidate_next,
                        first_block=False,
                        previous_blank=previous_blank,
                    ):
                        break
                    body_lines.append(candidate)
                    previous_blank = not candidate.strip()
                    idx += 1
                body_html = _render_final_reply_blocks(body_lines, seen_heading=True)
                if body_html:
                    parts.append(f"<details><summary>{_html_text(title, 100)}</summary>{body_html}</details>")
                previous_blank = False
                seen_heading = True
                continue
            tag = "h3" if not seen_heading else "b"
            parts.append(f"<{tag}>{_html_text(title, 100)}</{tag}>")
            seen_heading = True
            previous_blank = False
            idx += 1
            continue

        if _is_table_line(line) and idx + 1 < len(lines) and _is_table_line(str(lines[idx + 1])):
            table_lines: list[str] = []
            while idx < len(lines) and _is_table_line(str(lines[idx])):
                table_lines.append(str(lines[idx]).rstrip())
                idx += 1
            table_html = _rich_table_section(table_lines, rich_cells=True)
            if table_html:
                parts.append(table_html)
            previous_blank = False
            continue

        checklist = _checklist_item(line)
        if checklist:
            checklist_lines: list[str] = []
            while idx < len(lines) and _checklist_item(str(lines[idx] or "")):
                checklist_lines.append(str(lines[idx]).rstrip())
                idx += 1
            checklist_html = _rich_checklist_section(checklist_lines)
            if checklist_html:
                parts.append(checklist_html)
            previous_blank = False
            continue

        bullet = _bullet_text(line)
        if bullet:
            items: list[str] = []
            while idx < len(lines):
                item = _bullet_text(str(lines[idx] or ""))
                if item is None:
                    break
                idx += 1
                while idx < len(lines):
                    continuation = str(lines[idx] or "")
                    if (
                        not continuation.strip()
                        or _bullet_text(continuation)
                        or _numbered_text(continuation)
                        or _checklist_item(continuation)
                    ):
                        break
                    if not continuation.startswith((" ", "\t")):
                        break
                    item = f"{item.rstrip()} {continuation.strip()}"
                    idx += 1
                items.append(item)
            parts.append("<ul>\n" + "\n".join(f"<li>{_rich_inline(item, 900)}</li>" for item in items) + "\n</ul>")
            previous_blank = False
            continue

        numbered = _numbered_text(line)
        if numbered:
            items: list[tuple[int, str]] = []
            while idx < len(lines):
                parsed = _numbered_text(str(lines[idx] or ""))
                if not parsed:
                    break
                number, text = parsed
                idx += 1
                while idx < len(lines):
                    continuation = str(lines[idx] or "")
                    if (
                        not continuation.strip()
                        or _bullet_text(continuation)
                        or _numbered_text(continuation)
                        or _checklist_item(continuation)
                    ):
                        break
                    if not continuation.startswith((" ", "\t")):
                        break
                    text = f"{text.rstrip()} {continuation.strip()}"
                    idx += 1
                items.append((number, text))
            parts.append("<ol>\n" + "\n".join(f"<li>{_rich_inline(text, 900)}</li>" for _, text in items) + "\n</ol>")
            previous_blank = False
            continue

        if line.strip().startswith(">"):
            quote_lines: list[str] = []
            while idx < len(lines) and str(lines[idx] or "").strip().startswith(">"):
                quote_lines.append(re.sub(r"^\s*>\s?", "", str(lines[idx]).rstrip()))
                idx += 1
            parts.append("<blockquote>" + "<br>".join(_rich_inline(quote, 900) for quote in quote_lines) + "</blockquote>")
            previous_blank = False
            continue

        commit_html = _rich_commit_line(line)
        if commit_html:
            parts.append(commit_html)
            previous_blank = False
            idx += 1
            continue

        lead_split = _lead_heading_split(line, allow_status_title=not seen_heading) if (previous_blank or not seen_heading) else None
        if lead_split:
            title, rest = lead_split
            tag = "h3" if not seen_heading else "b"
            parts.append(f"<{tag}>{_html_text(title, 100)}</{tag}>")
            parts.extend(_rich_paragraph_blocks(rest))
            seen_heading = True
            previous_blank = False
            idx += 1
            continue

        inline_section = _inline_section_split(line)
        if inline_section:
            title, body = inline_section
            tag = "h3" if not seen_heading else "b"
            parts.append(f"<{tag}>{_html_text(title, 100)}</{tag}>")
            parts.extend(_rich_paragraph_blocks(body))
            seen_heading = True
            previous_blank = False
            idx += 1
            continue

        if _is_codeish_line(line):
            code_lines = [line.strip()]
            idx += 1
            while idx < len(lines) and _is_codeish_line(str(lines[idx] or "")):
                code_lines.append(str(lines[idx]).strip())
                idx += 1
            parts.append(f"<pre><code>{_html_text(chr(10).join(code_lines), 1800)}</code></pre>")
            previous_blank = False
            continue

        paragraph = [line.strip()]
        idx += 1
        while idx < len(lines):
            candidate = str(lines[idx] or "").rstrip()
            if not candidate.strip():
                break
            candidate_next = _next_nonempty_line(lines, idx + 1)
            if (
                FENCE_START_RE.match(candidate)
                or _bullet_text(candidate)
                or _numbered_text(candidate)
                or _checklist_item(candidate)
                or candidate.strip().startswith(">")
                or _is_codeish_line(candidate)
                or (_is_table_line(candidate) and idx + 1 < len(lines) and _is_table_line(str(lines[idx + 1])))
                or _is_turn_heading_line(candidate, candidate_next, first_block=False, previous_blank=False)
                or _inline_section_split(candidate)
            ):
                break
            paragraph.append(candidate.strip())
            idx += 1
        parts.extend(_rich_paragraph_blocks(" ".join(paragraph)))
        previous_blank = False
    return _join_blocks(parts)


def render_final_reply_html(value: str) -> str:
    clean = sanitize_text(str(value or ""), FINAL_REPLY_MAX_CHARS).strip()
    if not clean:
        return ""
    return _render_final_reply_blocks(clean.splitlines())


def _turn_fallback_body_html(assistant_final: str, reserved_chars: int) -> str:
    budget = max(1200, min(FINAL_REPLY_MAX_CHARS, MAX_RICH_HTML_CHARS - reserved_chars - 500))
    while budget >= 900:
        fallback = sanitize_text(assistant_final, budget)
        body = render_final_reply_html(fallback) or _rich_paragraph(fallback)
        if len(body) <= max(900, MAX_RICH_HTML_CHARS - reserved_chars):
            return body
        budget = budget // 2
    return _rich_paragraph(sanitize_text(assistant_final, 900))


def render_assistant_response_quote_html(assistant_final: str) -> str:
    clean = str(assistant_final or "").strip()
    if not clean:
        return ""
    body_html = render_final_reply_html(clean) or _rich_paragraph(clean)
    # Collapsible (open) like before, but NOT blockquoted — the blue quote bar
    # flattened the rich response. Prompt + worklog keep their blockquote.
    return _rich_details_quote_html(RESPONSE_LABEL, body_html, quote=False)


def render_worklog_quote_html(
    worklog_text: str,
    *,
    response_available: bool,
    label: str = WORKLOG_LABEL,
) -> str:
    clean = str(worklog_text or "").strip()
    if not clean:
        return ""
    body_html = render_final_reply_html(clean) or _rich_paragraph(clean)
    return _rich_details_quote_html(label or WORKLOG_LABEL, body_html, open_by_default=not response_available)


def render_turn_item_html(item: dict[str, Any]) -> str:
    user_text = str(item.get("user_text") or "").strip()
    worklog_text = str(item.get("worklog_text") or item.get("assistant_stream_text") or "").strip()
    worklog_label = str(item.get("worklog_label") or WORKLOG_LABEL).strip() or WORKLOG_LABEL
    assistant_final = str(item.get("assistant_final_text") or "").strip()
    collapse_chars = _item_prompt_collapse_chars(item)
    parts: list[str] = []
    if user_text:
        parts.append(render_user_prompt_quote_html(user_text, collapse_chars))
    worklog_html = render_worklog_quote_html(worklog_text, response_available=bool(assistant_final), label=worklog_label)
    if worklog_html:
        parts.append(worklog_html)
    response_html = render_assistant_response_quote_html(assistant_final)
    if response_html:
        parts.append(response_html)
    rendered = _join_blocks(parts).strip()
    if len(rendered) > MAX_RICH_HTML_CHARS:
        quote_html = ""
        if user_text:
            quote_html = render_user_prompt_quote_html(user_text, collapse_chars)
        worklog_html = ""
        if worklog_text:
            worklog_body = _turn_fallback_body_html(worklog_text, len(quote_html) + 120)
            worklog_html = _rich_details_quote_html(worklog_label, worklog_body, open_by_default=not bool(assistant_final))
        response_html = ""
        if assistant_final:
            body_html = _turn_fallback_body_html(assistant_final, len(quote_html) + len(worklog_html) + 120)
            response_html = _rich_details_quote_html(RESPONSE_LABEL, body_html)
        rendered = _join_blocks([quote_html, worklog_html, response_html]).strip()
        if len(rendered) > MAX_RICH_HTML_CHARS:
            worklog_html = ""
            if worklog_text:
                worklog_html = _rich_details_quote_html(
                    worklog_label,
                    _rich_paragraph(sanitize_text(worklog_text, 900)),
                    open_by_default=not bool(assistant_final),
                )
            response_html = ""
            if assistant_final:
                body_html = _rich_paragraph(sanitize_text(assistant_final, 900))
                response_html = _rich_details_quote_html(RESPONSE_LABEL, body_html)
            return _join_blocks([quote_html, worklog_html, response_html]).strip()
    return rendered


def render_stream_turn_html(user_text: str, worklog_text: str, *, worklog_label: str = WORKLOG_LABEL, collapse_chars: int = 0) -> str:
    clean_user = sanitize_text(str(user_text or ""), USER_PROMPT_MAX_CHARS).strip()
    clean_worklog = sanitize_text(str(worklog_text or ""), MAX_REPLY_CHARS).strip()
    if clean_user:
        return render_turn_item_html(
            {
                "kind": "turn",
                "user_text": clean_user,
                "worklog_text": clean_worklog,
                "worklog_label": worklog_label,
                "assistant_final_text": "",
                "prompt_collapse_chars": collapse_chars,
            }
        )
    return render_worklog_quote_html(clean_worklog, response_available=False, label=worklog_label)


def render_decision_item_html(item: dict[str, Any]) -> str:
    user_text = str(item.get("user_text") or "").strip()
    assistant_context = str(item.get("assistant_final_text") or "").strip()
    prompt = str(item.get("summary") or "").strip()
    options = list(item.get("options") or [])
    parts: list[str] = []
    if user_text:
        parts.append(render_user_prompt_quote_html(user_text, _item_prompt_collapse_chars(item)))
    if assistant_context:
        context_html = render_assistant_response_quote_html(assistant_context)
        if context_html:
            parts.append(context_html)
    parts.append("<h3>Decision needed</h3>")
    if prompt:
        parts.append(_rich_paragraph(prompt))
    if options:
        parts.append(_rich_options_block(options))
    rendered = _join_blocks(parts).strip()
    if len(rendered) > MAX_RICH_HTML_CHARS:
        compact = dict(item)
        compact["assistant_final_text"] = ""
        compact["summary"] = sanitize_text(prompt, 900)
        return render_decision_item_html(compact)
    return rendered


def render_interaction_readonly_item_html(item: dict[str, Any]) -> str:
    user_text = str(item.get("user_text") or "").strip()
    assistant_context = str(item.get("assistant_final_text") or "").strip()
    prompt = str(item.get("summary") or "Input needed.").strip()
    questions = list(item.get("questions") or [])
    answers = item.get("answers") if isinstance(item.get("answers"), dict) else {}
    parts: list[str] = []
    if user_text:
        parts.append(render_user_prompt_quote_html(user_text, _item_prompt_collapse_chars(item)))
    if assistant_context:
        context_html = render_assistant_response_quote_html(assistant_context)
        if context_html:
            parts.append(context_html)
    parts.append("<h3>Input needed</h3>")
    if prompt:
        parts.append(_rich_paragraph(prompt))
    parts.append(
        "<blockquote>"
        f"<b>{_html_text(INTERACTION_READONLY_WARNING_TITLE, 80)}</b><br>"
        f"{_rich_inline(INTERACTION_READONLY_WARNING_BODY, 300)}"
        "</blockquote>"
    )
    for idx, question in enumerate(questions, start=1):
        title = str(question.get("title") or f"Question {idx}").strip()
        parts.append(f"<b>{idx}. {_rich_inline(title, 180)}</b>")
        answer = interaction_answer_text(question, answers.get(str(question.get("question_id") or "")))
        if answer:
            parts.append(f"<p><b>Current answer:</b> {_rich_inline(answer, 500)}</p>")
        option_items: list[str] = []
        for option in list(question.get("options") or []):
            option_id = str(option.get("option_id") or "").strip()
            label = str(option.get("label") or "").strip()
            description = str(option.get("description") or "").strip()
            prefix = f"{option_id}. " if option_id else ""
            body = _rich_inline(f"{prefix}{label}", 240)
            if description:
                body += f"<br><small>{_rich_inline(description, 500)}</small>"
            option_items.append(f"<li>{body}</li>")
        if option_items:
            parts.append("<ul>\n" + "\n".join(option_items) + "\n</ul>")
    rendered = _join_blocks(parts).strip()
    if len(rendered) > MAX_RICH_HTML_CHARS:
        compact = dict(item)
        compact["assistant_final_text"] = ""
        compact["questions"] = questions[:4]
        return render_interaction_readonly_item_html(compact)
    return rendered


def render_feed_item_html(item: dict[str, Any], *, live: bool = False) -> str:
    kind = str(item.get("kind") or "update").lower()
    if kind == "prompt":
        prompt = str(item.get("user_text") or item.get("summary") or "").strip()
        return render_user_prompt_quote_html(prompt, _item_prompt_collapse_chars(item))
    if kind == "turn":
        return render_turn_item_html(item)
    if kind == "decision":
        return render_decision_item_html(item)
    if kind == "interaction_readonly":
        return render_interaction_readonly_item_html(item)
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
        if detail:
            detail_html = render_final_reply_html(detail) or _rich_lines_block(detail, max_chars=5500)
            if detail_html:
                parts.append(detail_html)
        if summary:
            if detail:
                parts.append("<b>Question</b>")
            parts.append(_rich_lines_block(summary, max_chars=700))
        parts.append(_rich_options_block(options))
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

    rendered = _join_blocks(parts).strip()
    if len(rendered) > MAX_RICH_HTML_CHARS:
        compact = dict(item)
        compact["detail"] = ""
        compact["summary"] = sanitize_text(str(compact.get("summary") or item_plain_text(item)), 900)
        rendered = render_feed_item_html(compact, live=live)
    if len(rendered) > MAX_RICH_HTML_CHARS:
        title_only = str(item.get("title") or item.get("kind") or "Update")
        return f"<h3>{_html_text(title_only, 80)}</h3><br>{_rich_paragraph(item_plain_text(item)[:900])}"
    return rendered


def render_notice_html(title: str, body: str) -> str:
    title_html = f"<h3>{_html_text(title, 80)}</h3>"
    clean_body = str(body or "").strip()
    if not clean_body:
        return title_html
    return f"{title_html}<br>{_rich_lines_block(clean_body, max_chars=900)}"


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


CHOICE_CONTEXT_RE = re.compile(
    r"\b(choose|select|pick|approve)\b"
    r"|\bwhich\s+[^?.!\n]{0,100}?\bdo\s+you\s+want\?"
    r"|\bwhich\s+[^?.!\n]{0,120}?\bshould\b[^?.!\n]{0,120}?\?"
    r"|\bwhich\s+(?:is\s+it|one|path|option)\b",
    re.IGNORECASE,
)
WRAPPED_CHOICE_CONTEXT_RE = re.compile(
    r"\bwhich\s+[^?.!]{0,100}?\bdo\s+you\s+want\?"
    r"|\bwhich\s+[^?.!]{0,120}?\bshould\b[^?.!]{0,120}?\?"
    r"|\bwhich\s+(?:is\s+it|one|path|option)\b",
    re.IGNORECASE,
)


def has_choice_context_hint(text: str) -> bool:
    raw = str(text or "")
    for line in raw.splitlines() or [raw]:
        if CHOICE_CONTEXT_RE.search(re.sub(r"[ \t]+", " ", line)):
            return True
    return bool(WRAPPED_CHOICE_CONTEXT_RE.search(re.sub(r"\s+", " ", raw)))


def choice_context_lines(lines: list[str], start: int) -> list[str]:
    context: list[str] = []
    for line in lines[max(0, start - 5):start]:
        low = line.lower()
        if (
            line_is_question_heading(line)
            or contains_marker(low, QUESTION_MARKERS)
            or has_choice_context_hint(line)
            or is_action_question([line])
        ):
            context.append(line)
        elif context:
            context.append(line)
    return context


def has_choice_context(lines: list[str]) -> bool:
    text = compact_block(lines, max_lines=5, max_chars=700)
    low = text.lower()
    return (
        any(line_is_question_heading(line) for line in lines)
        or contains_marker(low, QUESTION_MARKERS)
        or has_choice_context_hint(text)
        or is_action_question(lines)
    )


ASSISTANT_REPLY_START_RE = re.compile(r"^\s*[●]\s+(.+)$")
FINAL_REPLY_OPENER_RE = re.compile(
    r"^\s*(?:Codex|Claude|Here(?:'s| is)|Both reviewers|Both agree|My recommendation|Implemented|Done|Fixed)\b",
    re.IGNORECASE,
)


def final_reply_opener_line(line: str) -> bool:
    clean = strip_assistant_reply_marker(line).strip()
    if not clean:
        return False
    low = clean.lower()
    if re.match(r"^(wrote|read|edited|opened)\s+\d+\s+lines\b", low):
        return False
    if re.match(r"^\d+\s+", clean):
        return False
    if re.search(r"\.(?:py|js|ts|json|md|toml):\d+:", clean):
        return False
    if clean.startswith(("/", "./", "../", "timeout ", "(timeout")):
        return False
    return bool(FINAL_REPLY_OPENER_RE.match(clean))


def strip_assistant_reply_marker(line: str) -> str:
    match = ASSISTANT_REPLY_START_RE.match(str(line or ""))
    if match and not TOOL_START_RE.match(line):
        return match.group(1).strip()
    return str(line or "").rstrip()


def trim_choice_context_start(lines: list[str]) -> list[str]:
    if not lines:
        return []
    for idx in range(len(lines) - 1, -1, -1):
        line = lines[idx]
        if ASSISTANT_REPLY_START_RE.match(line) and not TOOL_START_RE.match(line):
            return lines[idx:]
    for idx, line in enumerate(lines):
        if final_reply_opener_line(line):
            return lines[idx:]
    return lines[-140:]


def visible_choice_question_context(lines: list[str], start: int) -> tuple[str, str]:
    before: list[str] = []
    for line in lines[:start]:
        if choice_ui_chrome_line(line):
            continue
        if str(line or "").strip():
            before.append(strip_assistant_reply_marker(line))
        elif before and before[-1] != "":
            before.append("")
    before = strip_outer_blank_lines(before)
    if not before:
        return "", ""
    question_idx: int | None = None
    question_pattern = re.compile(r"\b(which (?:is it|one|path|option)|choose|select|pick|approve)\b", re.IGNORECASE)
    for idx in range(len(before) - 1, -1, -1):
        window = re.sub(r"\s+", " ", " ".join(before[idx:min(len(before), idx + 6)]))
        if "?" in window or question_pattern.search(window):
            question_idx = idx
            while question_idx > 0 and before[question_idx - 1].strip():
                question_idx -= 1
            break
    if question_idx is None:
        context_lines = trim_choice_context_start(before)
        return compact_block(context_lines, max_lines=140, max_chars=8500), ""
    context_lines = trim_choice_context_start(before[:question_idx])
    question_lines = before[question_idx:]
    context = compact_block(context_lines, max_lines=140, max_chars=8500)
    question = compact_block(question_lines, max_lines=12, max_chars=1800)
    return context, question


def choice_question_is_self_contained(question: str, options: list[dict[str, str]]) -> bool:
    flat = re.sub(r"\s+", " ", str(question or "").strip()).lower()
    if not flat:
        return False
    if not re.search(r"\b(how should i proceed|which path should i take|what should i do|how do you want me to proceed)\b", flat):
        return False
    described = sum(1 for opt in options if str(opt.get("description") or "").strip())
    return described >= max(1, min(3, len(options) - 1))


def extract_choices(lines: list[str], *, explicit: bool = False) -> dict[str, Any] | None:
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
        current_option: dict[str, str] | None = None
        continuation_lines: list[str] = []

        def flush_continuation() -> None:
            nonlocal continuation_lines, current_option
            if current_option is not None and continuation_lines:
                current_option["description"] = sanitize_text(
                    re.sub(r"\s+", " ", " ".join(line.strip() for line in continuation_lines)).strip(),
                    700,
                )
            continuation_lines = []

        while idx < len(lines):
            item = option_match(lines[idx])
            if not item:
                if options and not str(lines[idx] or "").strip() and idx + 1 < len(lines) and option_match(lines[idx + 1]):
                    idx += 1
                    continue
                if options and choice_separator_line(lines[idx]):
                    idx += 1
                    continue
                if options and choice_continuation_line(lines[idx]):
                    continuation_lines.append(lines[idx])
                    idx += 1
                    continue
                break
            flush_continuation()
            number = item.group(1)
            label = sanitize_text(item.group(2).strip(), 120)
            if number in seen or not label:
                break
            seen.add(number)
            current_option = {"number": number, "label": label}
            options.append(current_option)
            idx += 1
        flush_continuation()
        if 2 <= len(options) <= 12:
            best = (start, idx, options)
        idx += 1
    if not best:
        return None
    start, end, options = best
    context = choice_context_lines(lines, start)
    if not context:
        nearby_context = lines[max(0, start - 8):start]
        if has_choice_context(nearby_context):
            context = nearby_context
    if not explicit and not has_choice_context(context):
        return None
    intro, question = visible_choice_question_context(lines, start)
    question = question or compact_block(context, max_lines=8, max_chars=1000) or "Choose a response."
    if choice_question_is_self_contained(question, options):
        intro = ""
    body_lines: list[str] = []
    for opt in options:
        body_lines.append(f"{opt['number']}) {opt['label']}")
        if opt.get("description"):
            body_lines.append(f"   {opt['description']}")
    body = "\n".join(body_lines)
    text = f"Question\n{question}\n\n{body}"
    prompt_id = prompt_id_for(text, options)
    return {
        "kind": "choices",
        "title": "Question",
        "summary": question,
        "detail": intro,
        "text": text,
        "options": options,
        "prompt_id": prompt_id,
        "notify": True,
    }


def extract_choices_from_raw(raw_text: str) -> dict[str, Any] | None:
    safe = ANSI_RE.sub("", sanitize_text(str(raw_text or ""), FEED_MAX_CHARS))
    matches = list(CHOICES_BLOCK_RE.finditer(safe))
    if not matches:
        return None
    body_lines = strip_outer_blank_lines(matches[-1].group(1).splitlines())
    item = extract_choices(body_lines, explicit=True)
    if item:
        item["choice_source"] = "explicit_block"
    return item


def extract_clean_feed_item(
    pane: dict[str, Any],
    entry: dict[str, Any],
    raw_text: str,
    *,
    allow_unbounded_reports: bool = ALLOW_UNBOUNDED_REPORTS,
) -> dict[str, Any] | None:
    status = str(pane.get("agent_status") or "").lower()
    bounded_report = extract_bounded_report_from_raw(raw_text)
    if bounded_report and status in {"done", "idle"}:
        title, body = bounded_report
        if body.strip():
            return make_feed_item("report", title, body, notify=False)
        return None

    lines = clean_feed_lines(raw_text)
    if not lines:
        return None

    tail = compact_block(lines, max_lines=80, max_chars=5000)
    if not tail:
        return None

    if allow_unbounded_reports:
        report_idx = report_start_index(lines)
        if report_idx is not None and status in {"done", "idle"}:
            title, body = report_title_and_body(lines)
            if body.strip():
                return make_feed_item("report", title, body, notify=False)
            return None

    choices = extract_choices_from_raw(raw_text) or extract_choices(lines)
    if choices:
        choices.setdefault("choice_source", "explicit_block" if "HERDRES_CHOICES_START" in raw_text else "legacy_clean_feed")
        return choices
    if is_action_question(lines):
        return make_feed_item("question", "Question", tail, notify=True)
    if status in {"blocked", "error"}:
        heading = "Blocked" if status == "blocked" else "Error"
        return make_feed_item(status, heading, tail, notify=True)
    return None


def clean_feed_hash(item: dict[str, Any], *, include_render_version: bool = True) -> str:
    payload = {
        "kind": item.get("kind"),
        "text": item.get("text"),
        "title": item.get("title"),
        "summary": item.get("summary"),
        "detail": item.get("detail"),
        "lines": item.get("lines") or [],
        "options": item.get("options") or [],
        "turn_id": item.get("turn_id"),
        "decision_id": item.get("decision_id"),
        "interaction_id": item.get("interaction_id"),
        "interaction_revision": item.get("interaction_revision"),
        "user_text": item.get("user_text"),
        "worklog_text": item.get("worklog_text"),
        "assistant_final_text": item.get("assistant_final_text"),
        "questions": item.get("questions") or [],
        "answers": item.get("answers") or {},
    }
    if include_render_version:
        payload["render_version"] = RICH_RENDER_VERSION
        payload["worklog_label"] = item.get("worklog_label")
    return hashlib.sha256(json.dumps(payload, sort_keys=True).encode("utf-8")).hexdigest()


def same_delivered_content(entry: dict[str, Any], item: dict[str, Any], item_semantic_hash: str) -> bool:
    previous_semantic_hash = str(entry.get("last_clean_semantic_hash") or "")
    if previous_semantic_hash and previous_semantic_hash == item_semantic_hash:
        return True
    turn_id = str(item.get("turn_id") or "")
    previous_turn_id = str(entry.get("last_turn_id") or (entry.get("last_clean_item") or {}).get("turn_id") or "")
    if turn_id and previous_turn_id and turn_id == previous_turn_id:
        previous_text = str(entry.get("last_clean_text") or "").strip()
        if previous_text and previous_text == item_plain_text(item).strip():
            return True
    return False


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
        "last_clean_semantic_hash",
        "last_clean_render_hash",
        "last_clean_message_id",
        "last_clean_kind",
        "last_clean_text",
        "last_clean_item",
        "last_clean_sent_at",
        "last_clean_send_error",
        "last_clean_attempt_hash",
        "last_clean_attempt_at",
        "last_turn_id",
        "last_turn_available",
        "last_turn_reason",
        "active_prompt",
        "awaiting_detail",
    ):
        entry.pop(key, None)


def awaiting_detail_source(awaiting: dict[str, Any]) -> str:
    source = str(awaiting.get("choice_source") or awaiting.get("source") or "").strip()
    if source:
        return source
    if awaiting.get("decision_id"):
        return "pending_decision"
    if str(awaiting.get("visible_choice") or "").strip():
        return "visible_scrape"
    return ""


def prompt_interaction_disabled(item_or_prompt: dict[str, Any]) -> bool:
    source = prompt_source(item_or_prompt)
    if source == "visible_readonly":
        return True
    if source == "visible_scrape":
        return not VISIBLE_CHOICE_BUTTONS_ENABLED
    if source == "legacy_clean_feed":
        return (not VISIBLE_CHOICE_BUTTONS_ENABLED) or (not LEGACY_CHOICES_ENABLED)
    if source == "legacy":
        return not LEGACY_CHOICES_ENABLED
    if source == "pending_decision":
        return not STRUCTURED_INTERACTIONS_ENABLED
    return False


def clear_disabled_visible_choice_state(state: dict[str, Any]) -> bool:
    changed = False
    for entry in (state.get("panes") or {}).values():
        active = entry.get("active_prompt") if isinstance(entry.get("active_prompt"), dict) else {}
        awaiting = entry.get("awaiting_detail") if isinstance(entry.get("awaiting_detail"), dict) else {}
        active_disabled = bool(active) and prompt_interaction_disabled(active)
        active_unbound = bool(active) and not str(active.get("message_id") or "").strip()
        awaiting_source = awaiting_detail_source(awaiting)
        awaiting_disabled = bool(awaiting_source) and prompt_interaction_disabled({"choice_source": awaiting_source})
        if active_disabled or active_unbound:
            entry.pop("active_prompt", None)
            entry.pop("awaiting_detail", None)
            entry["last_visible_choice_cleared_at"] = utc_now()
            changed = True
        if awaiting_disabled:
            entry["last_disabled_awaiting_detail"] = {
                "user_id": str(awaiting.get("user_id") or ""),
                "force_reply_message_id": str(awaiting.get("force_reply_message_id") or ""),
                "cleared_at": utc_now(),
            }
            entry.pop("awaiting_detail", None)
            entry["last_visible_choice_cleared_at"] = utc_now()
            changed = True
    return changed


def clear_topic_mapping(entry: dict[str, Any], reason: str = "") -> None:
    """Drop Telegram objects tied to a deleted forum topic, preserving pane identity."""
    old_topic_id = str(entry.get("topic_id") or "")
    for key in (
        "topic_id",
        "card_message_id",
        "card_hash",
        "card_status_hash",
        "card_format",
        "status_marker_message_id",
        "status_marker_hash",
        "status_marker_text",
        "status_marker_sent_at",
        "last_status_hash",
        "last_notified_status",
        "last_sent_at",
        "last_topic_verified_at",
        "last_topic_verify_attempt_at",
        "last_topic_verify_error",
        "last_topic_verify_error_at",
        "topic_rename_pending_at",
        "topic_rename_from",
        "topic_rename_to",
    ):
        entry.pop(key, None)
    clear_clean_feed_state(entry)
    entry["topic_missing_at"] = utc_now()
    if old_topic_id:
        entry["topic_missing_id"] = old_topic_id
    if reason:
        entry["topic_missing_reason"] = sanitize_text(reason, 500)


def clear_space_topic_mapping(state: dict[str, Any], space_entry: dict[str, Any], reason: str = "") -> None:
    old_topic_id = str(space_entry.get("topic_id") or "")
    for key in (
        "topic_id",
        "last_topic_verified_at",
        "last_topic_verify_attempt_at",
        "last_topic_verify_error",
        "last_topic_verify_error_at",
        "topic_rename_pending_at",
        "topic_rename_from",
        "topic_rename_to",
        # Aggregate topic-icon state lives on the space record now; drop it so a
        # remap to a new topic id re-applies the icon instead of dedup-skipping it.
        "topic_status_icon_key",
        "topic_status_icon_emoji",
        "topic_status_icon_custom_emoji_id",
        "topic_status_icon_updated_at",
        "last_topic_status_icon_attempt_key",
        "last_topic_status_icon_attempt_at",
        "last_topic_status_icon_missing",
        "last_topic_status_icon_missing_emoji",
        "last_topic_status_icon_missing_at",
        "last_topic_status_icon_error",
        "last_topic_status_icon_error_at",
    ):
        space_entry.pop(key, None)
    space_entry["topic_missing_at"] = utc_now()
    if old_topic_id:
        space_entry["topic_missing_id"] = old_topic_id
    if reason:
        space_entry["topic_missing_reason"] = sanitize_text(reason, 500)

    panes = state.get("panes") if isinstance(state.get("panes"), dict) else {}
    for pane_key_value in space_entry.get("pane_keys") or []:
        entry = panes.get(str(pane_key_value))
        if isinstance(entry, dict) and str(entry.get("topic_id") or "") == old_topic_id:
            clear_topic_mapping(entry, reason)


def clear_entry_topic_mapping(state: dict[str, Any], entry: dict[str, Any], reason: str = "") -> None:
    spaces = state.get("spaces") if isinstance(state.get("spaces"), dict) else {}
    space = spaces.get(str(entry.get("space_key") or ""))
    if isinstance(space, dict) and str(space.get("topic_id") or "") == str(entry.get("topic_id") or ""):
        clear_space_topic_mapping(state, space, reason)
        return
    clear_topic_mapping(entry, reason)


# Topic-mapping fields cleared from a pane entry on a grouping-mode switch so
# fresh topics are created from scratch (clean slate). Pane identity is kept.
_GROUPING_RESET_ENTRY_FIELDS = (
    "topic_id",
    "space_key",
    "topic_name",
    "legacy_topic_name",
    "topic_title_source",
    "card_message_id",
    "card_hash",
    "card_status_hash",
    "card_format",
    "status_marker_message_id",
    "status_marker_hash",
    "status_marker_text",
    "status_marker_sent_at",
    "last_status_hash",
    "last_notified_status",
    "last_sent_at",
    "last_topic_verified_at",
    "last_topic_verify_attempt_at",
    "last_topic_verify_error",
    "last_topic_verify_error_at",
    "topic_rename_pending_at",
    "topic_rename_from",
    "topic_rename_to",
    "topic_missing_at",
    "topic_missing_id",
    "topic_missing_reason",
    "pane_root_message_id",
    "pane_root_message_sent_at",
    "pane_root_message_error",
    "pane_root_message_missing_at",
    "pane_root_message_missing_id",
    "pane_root_message_missing_reason",
    # Legacy per-pane status-icon cache. The aggregate topic icon now lives on the
    # SPACE record (update_topic_status_icon dedups against it; clear_space_topic_mapping
    # clears it on remap), so these per-entry pops are harmless no-ops kept for cleanup of
    # any pre-migration state.
    "topic_status_icon_key",
    "topic_status_icon_emoji",
    "topic_status_icon_custom_emoji_id",
    "topic_status_icon_updated_at",
    # Last-prompt / last-message caches reference message ids in the old topic;
    # clearing them lets the new topic re-post the active prompt and re-anchor.
    "last_prompt_message_id",
    "last_prompt_hash",
    "last_prompt_turn_id",
    "last_prompt_bot_kind",
    "last_pane_message_id",
    "legacy_topic_id",
)


def reset_topic_grouping(state: dict[str, Any], mode: str, reason: str = "") -> None:
    """Clean-slate reset when the topic grouping mode changes.

    Drops every space->topic mapping (which also clears per-space pinned-status
    message ids and message routes) and every pane entry's topic linkage, so the
    next sync recreates topics for the new grouping. The old Telegram topics are
    left untouched on Telegram's side; Herdres simply forgets them.
    """
    old_spaces = state.get("spaces") if isinstance(state.get("spaces"), dict) else {}
    preserved = {
        k: s.get("voice_mode")
        for k, s in old_spaces.items()
        if isinstance(s, dict) and s.get("voice_mode")
    }
    if preserved:
        preserved_voice = state.get("_preserved_voice_mode")
        if not isinstance(preserved_voice, dict):
            preserved_voice = {}
            state["_preserved_voice_mode"] = preserved_voice
        preserved_voice.update(preserved)
    state["spaces"] = {}
    panes = state.get("panes") if isinstance(state.get("panes"), dict) else {}
    for entry in panes.values():
        if not isinstance(entry, dict):
            continue
        for field in _GROUPING_RESET_ENTRY_FIELDS:
            entry.pop(field, None)
        clear_clean_feed_state(entry)
        clear_stream_state(entry)
    state["topic_grouping_mode"] = mode
    state["topic_grouping_reset_at"] = utc_now()
    if reason:
        state["topic_grouping_reset_reason"] = sanitize_text(reason, 200)


def reconcile_topic_grouping(state: dict[str, Any]) -> bool:
    """Ensure state matches the configured topic grouping mode; reset on change.

    Called by every entry point that can create or adopt topics (sync_once and
    event_once) so a flag flip is honored no matter which path observes it first.
    Without this in event_once, a plugin-triggered event after a mode change would
    keep cross-wiring topics until a sync happened to run. Returns True if a
    clean-slate reset fired. Relies on load_dotenv() having run first.
    """
    desired = "agent" if per_agent_topics_enabled() else "space"
    if (str(state.get("topic_grouping_mode") or "space")) != desired:
        reset_topic_grouping(state, desired, reason=f"grouping mode -> {desired}")
        return True
    if state.get("topic_grouping_mode") != desired:
        state["topic_grouping_mode"] = desired
    return False


def choice_needs_detail(option: dict[str, str]) -> bool:
    if _boolish(option.get("needs_detail")):
        return True
    label = str(option.get("label") or "").lower()
    number = str(option.get("number") or "")
    if number.lower() == "custom" or str(option.get("id") or "").lower() == "custom":
        return True
    if "send_text" in option and not str(option.get("send_text") or "").strip():
        return True
    return number == "4" or any(word in label for word in ("detail", "feedback", "other", "refine", "custom"))


def prompt_source(item_or_prompt: dict[str, Any]) -> str:
    source = str(item_or_prompt.get("choice_source") or item_or_prompt.get("source") or "").strip()
    if source:
        return source
    item = item_or_prompt.get("item") if isinstance(item_or_prompt.get("item"), dict) else {}
    source = str(item.get("choice_source") or item.get("source") or "").strip()
    if source:
        return source
    turn_id = str(item_or_prompt.get("turn_id") or item.get("turn_id") or "")
    if turn_id.startswith("visible-readonly:") or turn_id.startswith("visible-readonly-question:"):
        return "visible_readonly"
    if turn_id.startswith("visible-choice:"):
        return "visible_scrape"
    if item_or_prompt.get("decision_id") or item.get("decision_id"):
        return "pending_decision"
    return "legacy"


def visible_choice_prompt_blocked(item_or_prompt: dict[str, Any]) -> bool:
    return prompt_interaction_disabled(item_or_prompt)


def choices_reply_markup(prompt_id: str, options: list[dict[str, str]]) -> dict[str, Any]:
    rows: list[list[dict[str, str]]] = []
    has_custom_button = False
    for idx, opt in enumerate(options[:12], start=1):
        number = str(opt.get("number") or idx)
        callback_id = str(opt.get("callback_id") or _callback_id(number, str(idx)))
        label = re.sub(r"\s+", " ", str(opt.get("label") or "")).strip()
        is_custom = number.lower() == "custom" or callback_id.lower() == "custom" or str(opt.get("id") or "").lower() == "custom"
        if is_custom:
            has_custom_button = True
            button_text = label or "Custom reply"
        elif choice_needs_detail(opt):
            has_custom_button = True
            display_number = number if number.isdigit() else str(idx)
            button_text = f"{display_number}. {label}" if label else display_number
        else:
            display_number = number if number.isdigit() else str(idx)
            button_text = f"{display_number}. {label}" if label else display_number
        action = "d" if choice_needs_detail(opt) else "c"
        rows.append([{"text": button_text[:64], "callback_data": safe_callback_data(action, prompt_id, callback_id)}])
    if not has_custom_button:
        rows.append([{"text": "Tell me differently", "callback_data": safe_callback_data("d", prompt_id, "custom")}])
    return {"inline_keyboard": rows}


def onboarding_reply_markup(space_token: str, kinds: list[str], selected: list[str]) -> dict[str, Any]:
    selected_set = set(selected)
    rows: list[list[dict[str, str]]] = []
    for kind in kinds:
        label = str((managed_bot_specs().get(kind) or {}).get("label") or kind.title())
        mark = "\u2705 " if kind in selected_set else "\u25ab\ufe0f "
        rows.append([{"text": (mark + label)[:64], "callback_data": f"herdr:ob:{space_token}:{kind}"}])
    rows.append([{"text": "Done", "callback_data": f"herdr:ob:{space_token}:_done"}])
    return {"inline_keyboard": rows}


def agent_picker_pane_tokens(live_entries: list[tuple[str, dict[str, Any]]]) -> dict[str, str]:
    base_counts: dict[str, int] = {}
    for pane_key_value, _entry in live_entries:
        base = _callback_id(str(pane_key_value), "pane")[:24]
        base_counts[base] = base_counts.get(base, 0) + 1
    used: set[str] = set()
    tokens: dict[str, str] = {}
    for pane_key_value, _entry in live_entries:
        pane_key_text = str(pane_key_value)
        base = _callback_id(pane_key_text, "pane")[:24]
        token = base
        if base_counts.get(base, 0) > 1:
            digest = hashlib.sha1(pane_key_text.encode("utf-8")).hexdigest()[:8]
            token = f"{base[:23]}-{digest}"
        attempt = 0
        while token in used:
            attempt += 1
            digest = hashlib.sha1(f"{pane_key_text}:{attempt}".encode("utf-8")).hexdigest()[:8]
            token = f"{base[:23]}-{digest}"
        used.add(token)
        tokens[pane_key_text] = token
    return tokens


def agents_picker_reply_markup(space_token: str, live_entries: list[tuple[str, dict[str, Any]]]) -> dict[str, Any]:
    rows: list[list[dict[str, str]]] = []
    tokens = agent_picker_pane_tokens(live_entries[:12])
    for pane_key_value, entry in live_entries[:12]:
        kind = managed_bot_kind_for_entry(entry)
        label = str((managed_bot_specs().get(kind) or {}).get("label") or entry.get("agent") or entry.get("pane_id") or "pane")
        pane_token = tokens[str(pane_key_value)]
        rows.append([{
            "text": (str(label) + " — " + str(entry.get("pane_id") or ""))[:64],
            "callback_data": f"herdr:ag:{space_token}:{pane_token}",
        }])
    return {"inline_keyboard": rows}


def new_pane_picker_reply_markup(space_token: str) -> dict[str, Any]:
    rows: list[list[dict[str, str]]] = []
    current: list[dict[str, str]] = []
    for model in DEVIN_SUPPORTED_MODELS:
        spec = DEVIN_MODEL_ALIASES.get(model) or devin_model_alias(model)
        current.append({
            "text": str(spec.get("label") or model)[:64],
            "callback_data": f"herdr:np:{space_token}:{model}",
        })
        if len(current) == 2:
            rows.append(current)
            current = []
    if current:
        rows.append(current)
    rows.append([
        {"text": "Codex", "callback_data": f"herdr:np:{space_token}:codex"},
        {"text": "Claude", "callback_data": f"herdr:np:{space_token}:claude"},
    ])
    return {"inline_keyboard": rows}


def prompt_delivery_state(item: dict[str, Any]) -> tuple[dict[str, Any] | None, dict[str, Any] | None, bool]:
    if str(item.get("kind") or "").lower() not in {"choices", "decision"}:
        return None, None, True
    source = prompt_source(item)
    if prompt_interaction_disabled(item):
        return None, None, True
    options = list(item.get("options") or [])
    if not options:
        return None, None, True
    plain_text = item_plain_text(item)
    normalized_options: list[dict[str, str]] = []
    for idx, raw in enumerate(options[:12], start=1):
        opt = dict(raw)
        raw_id = str(opt.get("callback_id") or opt.get("number") or opt.get("id") or idx)
        opt["callback_id"] = _callback_id(raw_id, str(idx))
        normalized_options.append(opt)
    options = normalized_options
    prompt_id = _prompt_callback_id(str(item.get("prompt_id") or ""), plain_text, options)
    item["prompt_id"] = prompt_id
    item["options"] = options
    active_prompt = {
        "id": prompt_id,
        "text": plain_text,
        "item": item,
        "options": options,
        "choice_source": source,
        "created_at": utc_now(),
    }
    if item.get("decision_id"):
        active_prompt["decision_id"] = str(item.get("decision_id") or "")
    return choices_reply_markup(prompt_id, options), active_prompt, False


def active_prompt_expired(prompt: dict[str, Any], ttl_seconds: int = ACTIVE_PROMPT_TTL_SECONDS) -> bool:
    try:
        created_at = _dt.datetime.fromisoformat(str(prompt.get("created_at", "")).replace("Z", "+00:00"))
    except Exception:
        return True
    return (_dt.datetime.now(tz=_dt.timezone.utc) - created_at).total_seconds() > ttl_seconds


def bind_active_prompt_message(
    entry: dict[str, Any],
    active_prompt: dict[str, Any],
    message_id: str | int | None,
) -> bool:
    prompt = dict(active_prompt)
    bound_message_id = str(message_id or "").strip()
    if not bound_message_id:
        entry.pop("active_prompt", None)
        entry.pop("awaiting_detail", None)
        entry["last_prompt_bind_error"] = "button message did not include a Telegram message_id"
        entry["last_prompt_bind_error_at"] = utc_now()
        return False
    prompt["message_id"] = bound_message_id
    prompt["created_at"] = utc_now()
    entry["active_prompt"] = prompt
    entry.pop("last_prompt_bind_error", None)
    entry.pop("last_prompt_bind_error_at", None)
    return True


def record_delivered_feed_item(
    entry: dict[str, Any],
    item: dict[str, Any],
    result: dict[str, Any],
    *,
    pending_active_prompt: dict[str, Any] | None,
    clear_active_prompt: bool,
    item_render_hash: str | None = None,
    item_semantic_hash: str | None = None,
    fallback_message_id: str | int | None = None,
) -> None:
    if pending_active_prompt:
        bind_active_prompt_message(
            entry,
            pending_active_prompt,
            result.get("message_id") or fallback_message_id or "",
        )
    elif clear_active_prompt:
        entry.pop("active_prompt", None)
        entry.pop("awaiting_detail", None)
    # The dedup hashes MUST be the PRE-delivery values: prompt_delivery_state()
    # mutates choice/decision items (rewrites options + prompt_id), so recomputing
    # here would store a post-mutation hash that never matches the pre-mutation
    # hash the next sync cycle compares against — causing duplicate re-sends.
    if item_render_hash is None:
        item_render_hash = clean_feed_hash(item)
    if item_semantic_hash is None:
        item_semantic_hash = clean_feed_hash(item, include_render_version=False)
    entry["last_clean_hash"] = item_render_hash
    entry["last_clean_semantic_hash"] = item_semantic_hash
    entry["last_clean_render_hash"] = item_render_hash
    if result.get("message_id"):
        entry["last_clean_message_id"] = str(result["message_id"])
    elif fallback_message_id:
        entry["last_clean_message_id"] = str(fallback_message_id)
    entry["last_clean_kind"] = str(item.get("kind") or "")
    entry["last_clean_text"] = item_plain_text(item)
    entry["last_clean_item"] = item
    entry["last_clean_sent_at"] = utc_now()
    if item.get("turn_id"):
        entry["last_turn_id"] = str(item.get("turn_id") or "")
    entry.pop("last_clean_send_error", None)


def active_prompt_message_rejection(prompt: dict[str, Any], callback_message_id: str) -> str:
    bound_message_id = str(prompt.get("message_id") or "").strip()
    callback_message_id = str(callback_message_id or "").strip()
    if active_prompt_expired(prompt):
        return "expired"
    if bound_message_id:
        if callback_message_id != bound_message_id:
            return "stale_message"
        return ""
    return ""


def current_visible_choice_item_for_entry(entry: dict[str, Any]) -> dict[str, Any] | None:
    if not VISIBLE_CHOICE_BUTTONS_ENABLED:
        return None
    pane_id = str(entry.get("pane_id") or "")
    if not pane_id:
        return None
    pane = pane_by_id(pane_id) or {"pane_id": pane_id}
    return extract_visible_choice_feed_item(pane)


def refresh_stale_visible_prompt(
    state: dict[str, Any],
    entry: dict[str, Any],
    chat_id: str,
    topic_id: str,
    telegram: dict[str, Any],
    prompt_id: str,
) -> bool:
    current_item = current_visible_choice_item_for_entry(entry)
    if not current_item:
        return False
    current_prompt_id = str(current_item.get("prompt_id") or "")
    if not current_prompt_id or current_prompt_id == prompt_id:
        return False
    reply_markup, active_prompt, _clear = prompt_delivery_state(current_item)
    result = send_feed_item(
        chat_id,
        current_item,
        telegram=telegram,
        thread_id=topic_id,
        notify=bool(current_item.get("notify")),
        reply_markup=reply_markup,
        api_token=managed_bot_token_for_entry(telegram, entry),
    )
    if result.get("ok"):
        if active_prompt:
            bind_active_prompt_message(entry, active_prompt, result.get("message_id"))
        entry["last_clean_hash"] = clean_feed_hash(current_item)
        entry["last_clean_semantic_hash"] = clean_feed_hash(current_item, include_render_version=False)
        entry["last_clean_render_hash"] = clean_feed_hash(current_item)
        if result.get("message_id"):
            entry["last_clean_message_id"] = str(result["message_id"])
        entry["last_clean_kind"] = str(current_item.get("kind") or "choices")
        entry["last_clean_text"] = item_plain_text(current_item)
        entry["last_clean_item"] = current_item
        entry["last_clean_sent_at"] = utc_now()
        entry["last_turn_id"] = current_item.get("turn_id") or ""
        entry.pop("last_clean_send_error", None)
        save_state(state)
    return True


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
        lines.append("Commands: /status, /read [lines], /send <text>, /send! <text>, /keys <keys>")
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
        f"last_turn_id: {entry.get('last_turn_id') or ''}",
        f"last_turn_available: {entry.get('last_turn_available')}",
        f"last_turn_reason: {entry.get('last_turn_reason') or ''}",
        f"card_message_id: {entry.get('card_message_id') or ''}",
        f"card_hash: {entry.get('card_hash') or ''}",
        f"card_status_hash: {entry.get('card_status_hash') or ''}",
        f"card_format: {entry.get('card_format') or ''}",
        f"status_marker_message_id: {entry.get('status_marker_message_id') or ''}",
        f"status_marker_hash: {entry.get('status_marker_hash') or ''}",
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


def latest_turn_item(entry: dict[str, Any], pane: dict[str, Any] | None = None) -> dict[str, Any] | None:
    if pane:
        item = extract_turn_feed_item(pane, dict(entry), allow_visible_fallback=False)
        if item:
            return item
    item = entry.get("last_clean_item") if isinstance(entry.get("last_clean_item"), dict) else {}
    if str(item.get("kind") or "").lower() == "turn":
        return dict(item)
    return None


def latest_turn_report(entry: dict[str, Any], pane: dict[str, Any] | None = None) -> str:
    item = latest_turn_item(entry, pane)
    if item:
        return item_plain_text(item)
    return "No structured turn is available yet."


def revalidate_pending_decision_prompt(
    pane_id: str,
    prompt: dict[str, Any],
) -> tuple[str, dict[str, Any] | None]:
    if not TURN_FEED_ENABLED or not STRUCTURED_INTERACTIONS_ENABLED:
        return "unknown", None
    if prompt_source(prompt) != "pending_decision":
        return "unknown", None
    decision_id = str(prompt.get("decision_id") or "").strip()
    if not decision_id:
        item = prompt.get("item") if isinstance(prompt.get("item"), dict) else {}
        decision_id = str(item.get("decision_id") or "").strip()
    if not pane_id or not decision_id:
        return "unknown", None

    turn = cached_pane_turn(pane_id)
    if turn.get("available") is not True:
        return "unknown", None

    decision = normalize_pending_decision(turn)
    if not decision or str(decision.get("decision_id") or "").strip() != decision_id:
        return "stale", None

    item = make_decision_feed_item(turn, decision)
    if not item:
        return "unknown", None
    return "fresh", item


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


def workflow_summary(pane: dict[str, Any]) -> str:
    counts = pane.get("workflow_counts")
    if isinstance(counts, dict):
        total = int(counts.get("total") or counts.get("count") or 0)
        done = int(counts.get("done") or counts.get("completed") or counts.get("succeeded") or 0)
        active = int(counts.get("active") or counts.get("running") or counts.get("working") or 0)
        if total:
            if active:
                return f"Working on {done}/{total} workflows; {active} active."
            return f"Workflows {done}/{total}."
    workflows = pane.get("workflows")
    if isinstance(workflows, list) and workflows:
        total = len(workflows)
        done = 0
        active = 0
        for workflow in workflows:
            if not isinstance(workflow, dict):
                continue
            status = str(workflow.get("status") or workflow.get("state") or "").lower()
            if status in {"done", "complete", "completed", "succeeded", "success"}:
                done += 1
            elif status in {"active", "running", "working", "in_progress", "pending"}:
                active += 1
        if active:
            return f"Working on {done}/{total} workflows; {active} active."
        return f"Workflows {done}/{total}."
    total_raw = pane.get("workflow_total") or pane.get("workflows_total")
    done_raw = pane.get("workflow_done") or pane.get("workflows_done") or pane.get("workflow_completed")
    try:
        total = int(total_raw or 0)
        done = int(done_raw or 0)
    except Exception:
        return ""
    if total:
        return f"Working on {done}/{total} workflows."
    return ""


def status_marker_content(pane: dict[str, Any]) -> tuple[str, str]:
    status = str(pane.get("agent_status") or "unknown").lower()
    workflows = workflow_summary(pane)
    if status == "working":
        return "🟡 Working", workflows or "Work is in progress."
    if status == "idle":
        return "🟢 Idle", workflows or "No active work."
    if status == "done":
        return "✅ Done", workflows or "Latest work finished."
    if status == "blocked":
        return "🟠 Waiting", workflows or "Waiting for input or blocked."
    if status == "error":
        return "🔴 Error", workflows or "This pane reported an error."
    return "⚪ Status", workflows or "Current pane state is unclear."


def status_marker_hash(pane: dict[str, Any]) -> str:
    title, body = status_marker_content(pane)
    payload = {
        "version": 1,
        "status": str(pane.get("agent_status") or "unknown").lower(),
        "title": title,
        "body": body,
    }
    return hashlib.sha256(json.dumps(payload, sort_keys=True).encode("utf-8")).hexdigest()


STATUS_ICON_TABLE = (
    ("working", "HERDR_TELEGRAM_TOPICS_STATUS_ICON_WORKING", "HERDR_TELEGRAM_TOPICS_STATUS_ICON_WORKING_EMOJI", "⚡️"),
    ("idle", "HERDR_TELEGRAM_TOPICS_STATUS_ICON_IDLE", "HERDR_TELEGRAM_TOPICS_STATUS_ICON_IDLE_EMOJI", "☕️"),
    ("done", "HERDR_TELEGRAM_TOPICS_STATUS_ICON_DONE", "HERDR_TELEGRAM_TOPICS_STATUS_ICON_DONE_EMOJI", "✅"),
    ("blocked", "HERDR_TELEGRAM_TOPICS_STATUS_ICON_BLOCKED", "HERDR_TELEGRAM_TOPICS_STATUS_ICON_BLOCKED_EMOJI", "❗️"),
    ("error", "HERDR_TELEGRAM_TOPICS_STATUS_ICON_ERROR", "HERDR_TELEGRAM_TOPICS_STATUS_ICON_ERROR_EMOJI", "‼️"),
    ("workflow", "HERDR_TELEGRAM_TOPICS_STATUS_ICON_WORKFLOW", "HERDR_TELEGRAM_TOPICS_STATUS_ICON_WORKFLOW_EMOJI", "📈"),
    ("unknown", "HERDR_TELEGRAM_TOPICS_STATUS_ICON_UNKNOWN", "HERDR_TELEGRAM_TOPICS_STATUS_ICON_UNKNOWN_EMOJI", "❓"),
    ("closed", "HERDR_TELEGRAM_TOPICS_STATUS_ICON_CLOSED", "HERDR_TELEGRAM_TOPICS_STATUS_ICON_CLOSED_EMOJI", "📁"),
    ("goal", "HERDR_TELEGRAM_TOPICS_STATUS_ICON_GOAL", "HERDR_TELEGRAM_TOPICS_STATUS_ICON_GOAL_EMOJI", "🧠"),
)

STATUS_ICON_ENV_KEYS = {key: env_key for key, env_key, _emoji_env_key, _emoji in STATUS_ICON_TABLE}
STATUS_ICON_EMOJI_ENV_KEYS = {key: emoji_env_key for key, _env_key, emoji_env_key, _emoji in STATUS_ICON_TABLE}
STATUS_ICON_DEFAULT_EMOJI = {key: emoji for key, _env_key, _emoji_env_key, emoji in STATUS_ICON_TABLE}


def pane_goal_active(pane: dict[str, Any]) -> bool:
    """True when the pane footer shows an active goal (e.g. "◎ /goal active").

    Read once per pane per sync (memoised on the pane dict) and only consulted for
    idle/done panes, so the cost is at most one small visible read per idle-or-done pane.
    """
    if "_goal_active" in pane:
        return bool(pane["_goal_active"])
    result = False
    pane_id = str(pane.get("pane_id") or "")
    if pane_id:
        # `--source visible` returns the whole screen; the marker lives in the
        # footer (last line). Use a generous max_chars so the tail isn't dropped
        # (sanitize_text truncates from the head), then scan only the footer.
        raw = pane_output(pane_id, lines=GOAL_MARKER_READ_LINES, max_chars=12000, source="visible")
        if raw.strip():
            footer = ANSI_RE.sub("", raw).splitlines()[-GOAL_MARKER_READ_LINES:]
            result = bool(GOAL_ACTIVE_RE.search("\n".join(footer)))
    pane["_goal_active"] = result
    return result


def status_icon_key(pane: dict[str, Any]) -> str:
    """Canonical pane-status classifier (alias-folding). The single source of the
    status key consumed by the topic-aggregate icon AND the pinned-status dot/sort."""
    status = str(pane.get("agent_status") or "unknown").lower().replace("-", "_")
    if pane.get("closed") or pane.get("exited") or pane.get("process_exited") or status in {"closed", "exited"}:
        return "closed"
    if status in {"error", "failed", "failure"}:
        return "error"
    if status in {"blocked", "waiting"}:
        return "blocked"
    if status in {"working", "busy"} or status in ACTIVE_AGENT_STATUSES:
        # A tracked multi-step run reads as "workflow" (📈); ad-hoc work as ⚡️.
        return "workflow" if workflow_summary(pane) else "working"
    if status == "idle":
        # An idle pane still pursuing a goal reads as "on a goal" (🧠), not a break.
        return "goal" if pane_goal_active(pane) else "idle"
    if status in {"done", "complete", "completed", "success", "succeeded"}:
        # A finished turn that still has a committed /goal also reads as "on a goal"
        # (🧠): the agent ended its turn — often while background work it kicked off is
        # still running — but the goal marks the work as ongoing. herdr only sees the
        # agent's generate-state (done), so the goal is the signal that work continues.
        return "goal" if pane_goal_active(pane) else "done"
    return "unknown"


# Canonical status severity (higher = worse / needs more attention). ONE ordering
# shared by the topic-aggregate icon, the pinned-status dot, and the pin sort — so the
# icon a topic shows, the dot a pane gets, and the order they sort can never disagree.
STATUS_SEVERITY = {
    "error": 8, "blocked": 7, "workflow": 6, "working": 5,
    "goal": 4, "idle": 3, "done": 2, "unknown": 1, "closed": 0,
}


def status_severity(key: str) -> int:
    return STATUS_SEVERITY.get(str(key or "unknown"), STATUS_SEVERITY["unknown"])


def topic_status_key(open_panes: list[dict[str, Any]]) -> str:
    """Aggregate status key for a (possibly shared) topic: the worst-severity status
    among its open panes — so panes in one topic no longer fight over the icon. Returns
    "closed" when the topic has no open panes."""
    keys = [status_icon_key(p) for p in open_panes if pinned_status_pane_open(p)]
    if not keys:
        return "closed"
    return max(keys, key=status_severity)


def status_icon_emoji(key: str) -> str:
    env_key = STATUS_ICON_EMOJI_ENV_KEYS.get(key, "")
    return (os.getenv(env_key, "") if env_key else "").strip() or STATUS_ICON_DEFAULT_EMOJI.get(key, "❓")


PINNED_STATUS_DOTS = {
    "idle": "🟢",
    "done": "🟢",
    "working": "🟡",
    "workflow": "🟡",
    "blocked": "🔴",
    "error": "🔴",
    "goal": "🧠",
    "unknown": "⬜",
}


def pinned_status_enabled() -> bool:
    return os.getenv("HERDR_TELEGRAM_TOPICS_PINNED_STATUS", "0").strip() == "1"


def pinned_status_dot(pane: dict[str, Any]) -> str:
    # Derive PURELY from the canonical classifier so the dot, the topic icon, and the
    # sort all agree. status_icon_key already maps an idle pane pursuing a goal to
    # "goal" (🧠); a WORKING pane stays "working" (🟡) even if a goal is active — the
    # old goal-first check wrongly showed 🧠 for working panes and re-read the pane.
    return PINNED_STATUS_DOTS.get(status_icon_key(pane), "⬜")


def pinned_status_pane_open(pane: dict[str, Any]) -> bool:
    status = str(pane.get("agent_status") or "").lower()
    if status in {"closed", "exited"}:
        return False
    if pane.get("closed") or pane.get("exited") or pane.get("process_exited"):
        return False
    return True


def pinned_status_pane_label(state: dict[str, Any], pane: dict[str, Any]) -> str:
    entries = state.get("panes") if isinstance(state.get("panes"), dict) else {}
    entry = entries.get(pane_key(pane)) if isinstance(entries, dict) else None
    raw = ""
    if isinstance(entry, dict):
        raw = str(entry.get("topic_name") or "")
    if not raw:
        raw = str(pane.get("topic_name") or pane.get("name") or pane_manual_label(pane) or pane.get("pane_id") or "pane")
    label = re.sub(r"\s+", " ", raw).strip()
    return sanitize_text(label or "pane", 80)


def render_pinned_status(
    state: dict[str, Any],
    panes: list[dict[str, Any]],
    *,
    label_fn: Callable[[dict[str, Any]], str] | None = None,
) -> str:
    """Unified pinned-status renderer for BOTH surfaces. One row per open pane:
    "<label> <dot>", sorted worst-severity first then label. Dot and sort both derive
    from the canonical status severity, so a pane's dot, its topic icon (A), and its
    sort order can never disagree. No elapsed time — the pin is a glance board, and
    including it would force an edit every tick.

    The DOT/sort/structure are unified; only the label differs by surface: the per-space
    topic pin uses the agent label (default) so panes sharing one topic stay
    distinguishable, while the global dashboard passes the topic label."""
    if label_fn is None:
        label_fn = pane_agent_status_label
    rows: list[tuple[int, str, str]] = []
    for pane in panes:
        if not pinned_status_pane_open(pane):
            continue
        label = label_fn(pane)
        rows.append((status_severity(status_icon_key(pane)), label, f"{label} {pinned_status_dot(pane)}"))
    if not rows:
        return "No active panes."
    rows.sort(key=lambda r: (-r[0], r[1]))
    return sanitize_text(" | ".join(r[2] for r in rows), MAX_REPLY_CHARS)


def status_icon_explicit_id(key: str) -> str:
    env_key = STATUS_ICON_ENV_KEYS.get(key, "")
    return (os.getenv(env_key, "") if env_key else "").strip()


def cache_fresh(value: str, ttl_seconds: int) -> bool:
    try:
        then = _dt.datetime.fromisoformat(value.replace("Z", "+00:00"))
    except Exception:
        return False
    return (_dt.datetime.now(tz=_dt.timezone.utc) - then).total_seconds() <= ttl_seconds


def forum_icon_cache(telegram: dict[str, Any]) -> dict[str, Any]:
    cache = telegram.setdefault("forum_topic_icons", {})
    if not isinstance(cache, dict):
        cache = {}
        telegram["forum_topic_icons"] = cache
    cache.setdefault("by_emoji", {})
    return cache


def refresh_forum_icon_cache(telegram: dict[str, Any]) -> dict[str, Any]:
    cache = forum_icon_cache(telegram)
    fetched_at = str(cache.get("fetched_at") or "")
    if cache.get("by_emoji") and cache_fresh(fetched_at, STATUS_ICON_CACHE_TTL_SECONDS):
        return cache
    response = telegram_api("getForumTopicIconStickers", {})
    by_emoji: dict[str, str] = {}
    for sticker in response.get("result") or []:
        if not isinstance(sticker, dict):
            continue
        emoji = str(sticker.get("emoji") or "").strip()
        custom_emoji_id = str(sticker.get("custom_emoji_id") or "").strip()
        if emoji and custom_emoji_id and emoji not in by_emoji:
            by_emoji[emoji] = custom_emoji_id
    cache["by_emoji"] = by_emoji
    cache["fetched_at"] = utc_now()
    cache.pop("last_error", None)
    cache.pop("last_error_at", None)
    return cache


def status_icon_id_for_keys(telegram: dict[str, Any], keys: list[str]) -> tuple[str, str, str]:
    """Resolve (custom_emoji_id, matched_key, emoji) for the first of `keys` that
    resolves — via an explicit env id, else the cached forum-icon set."""
    primary = keys[0] if keys else "unknown"
    for candidate in keys:
        explicit = status_icon_explicit_id(candidate)
        if explicit:
            return explicit, candidate, status_icon_emoji(candidate)
    try:
        cache = refresh_forum_icon_cache(telegram)
    except Exception as exc:
        cache = forum_icon_cache(telegram)
        cache["last_error"] = sanitize_text(str(exc), 500)
        cache["last_error_at"] = utc_now()
        return "", primary, status_icon_emoji(primary)
    by_emoji = cache.get("by_emoji") if isinstance(cache.get("by_emoji"), dict) else {}
    for candidate in keys:
        emoji = status_icon_emoji(candidate)
        custom_emoji_id = str(by_emoji.get(emoji) or "").strip()
        if custom_emoji_id:
            return custom_emoji_id, candidate, emoji
    return "", primary, status_icon_emoji(primary)


def status_icon_id_for_status_key(telegram: dict[str, Any], key: str) -> tuple[str, str, str]:
    keys = [key]
    if key == "workflow":
        keys.append("working")
    keys.append("unknown")
    return status_icon_id_for_keys(telegram, keys)


def status_icon_custom_emoji_id(telegram: dict[str, Any], pane: dict[str, Any]) -> tuple[str, str, str]:
    return status_icon_id_for_status_key(telegram, status_icon_key(pane))


def edit_topic_icon(
    chat_id: str,
    topic_id: str | int,
    icon_custom_emoji_id: str,
    *,
    name: str = "",
) -> bool:
    payload: dict[str, Any] = {
        "chat_id": chat_id,
        "message_thread_id": str(topic_id),
        "icon_custom_emoji_id": icon_custom_emoji_id,
    }
    title = clean_space_topic_title(str(name or ""), fallback="")
    if title:
        payload["name"] = title
    return bool(telegram_api("editForumTopic", payload).get("result"))


# Fire-and-forget icon updates: the next sync retries on failure.
_icon_fire_threads: list[threading.Thread] = []


def edit_topic_icon_async(
    chat_id: str,
    topic_id: str | int,
    icon_custom_emoji_id: str,
    *,
    name: str = "",
) -> None:
    def _fire() -> None:
        try:
            edit_topic_icon(chat_id, topic_id, icon_custom_emoji_id, name=name)
        except Exception:
            pass

    t = threading.Thread(target=_fire, daemon=True)
    _icon_fire_threads.append(t)
    t.start()


def update_topic_status_icon(
    chat_id: str,
    space_record: dict[str, Any],
    open_panes: list[dict[str, Any]],
    *,
    telegram: dict[str, Any],
) -> dict[str, Any]:
    """Set a topic's forum icon from the AGGREGATE status of all open panes sharing
    that topic (worst-severity wins), tracked on the shared space record — so panes in
    one topic no longer fight over the icon. Escalations apply immediately; a
    de-escalation (toward a less-severe status) is coalesced for
    STATUS_ICON_RETRY_SECONDS to absorb the working→idle→working flicker herdr reports
    during brief output pauses."""
    if not STATUS_ICON_ENABLED:
        return {"ok": False, "attempted": False, "kind": "disabled"}
    topic_id = str(space_record.get("topic_id") or "")
    if not topic_id:
        return {"ok": False, "attempted": False, "kind": "missing_topic"}
    icon_key = topic_status_key(open_panes)
    if icon_key == "closed":
        # No open panes — leave the icon alone (topic-close cleanup owns closed topics).
        return {"ok": True, "attempted": False, "kind": "no_open_panes"}
    icon_id, icon_key, emoji = status_icon_id_for_status_key(telegram, icon_key)
    if not icon_id:
        space_record["last_topic_status_icon_missing"] = icon_key
        space_record["last_topic_status_icon_missing_emoji"] = emoji
        space_record["last_topic_status_icon_missing_at"] = utc_now()
        return {"ok": False, "attempted": False, "kind": "no_icon", "icon_key": icon_key, "emoji": emoji}
    if str(space_record.get("topic_status_icon_custom_emoji_id") or "") == icon_id:
        return {"ok": True, "attempted": False, "kind": "unchanged", "icon_key": icon_key, "emoji": emoji}
    current_key = str(space_record.get("topic_status_icon_key") or "")
    last_attempt = str(space_record.get("last_topic_status_icon_attempt_at") or "")
    is_deescalation = bool(current_key) and status_severity(icon_key) < status_severity(current_key)
    if is_deescalation and last_attempt and cache_fresh(last_attempt, STATUS_ICON_RETRY_SECONDS):
        return {"ok": False, "attempted": False, "kind": "retry_deferred", "icon_key": icon_key, "emoji": emoji}
    space_record["last_topic_status_icon_attempt_key"] = f"{icon_id}:{icon_key}"
    space_record["last_topic_status_icon_attempt_at"] = utc_now()
    # Fire-and-forget: update state optimistically, let the next sync retry on failure.
    edit_topic_icon_async(chat_id, topic_id, icon_id, name=str(space_record.get("topic_name") or ""))
    space_record["topic_status_icon_key"] = icon_key
    space_record["topic_status_icon_emoji"] = emoji
    space_record["topic_status_icon_custom_emoji_id"] = icon_id
    space_record["topic_status_icon_updated_at"] = utc_now()
    for stale in (
        "last_topic_status_icon_error", "last_topic_status_icon_error_at",
        "last_topic_status_icon_missing", "last_topic_status_icon_missing_emoji",
        "last_topic_status_icon_missing_at",
    ):
        space_record.pop(stale, None)
    return {"ok": True, "attempted": True, "kind": "updated", "icon_key": icon_key, "emoji": emoji}


def update_topic_icons_for_spaces(
    state: dict[str, Any],
    chat_id: str,
    telegram: dict[str, Any],
    panes: list[dict[str, Any]],
    counters: dict[str, Any],
) -> None:
    """Per-topic icon pass: one aggregate icon update per space/topic (replaces the old
    per-pane updates that let same-topic panes fight). Call after the pane sync loop."""
    if not STATUS_ICON_ENABLED:
        return
    spaces = state.get("spaces") if isinstance(state.get("spaces"), dict) else {}
    for space_key_value, space_panes in open_panes_by_space(panes).items():
        space_record = spaces.get(str(space_key_value))
        if not isinstance(space_record, dict) or not space_record.get("topic_id"):
            continue
        result = update_topic_status_icon(chat_id, space_record, space_panes, telegram=telegram)
        if result.get("attempted"):
            counters["icon_updates"] = counters.get("icon_updates", 0) + 1


def pane_input_needs_file(text: str) -> bool:
    value = str(text or "")
    if len(value) >= PANE_INPUT_FILE_CHARS:
        return True
    return value.count("\n") + 1 >= PANE_INPUT_FILE_LINES


def safe_file_component(value: str, fallback: str = "pane") -> str:
    text = re.sub(r"[^A-Za-z0-9_.-]+", "-", str(value or "").strip())
    text = text.strip(".-")
    return text[:80] or fallback


def write_inbound_pane_message(pane_id: str, text: str) -> Path:
    root = state_path().parent / "inbound" / safe_file_component(pane_id)
    root.mkdir(parents=True, exist_ok=True)
    try:
        root.chmod(0o700)
    except OSError:
        pass
    content = str(text or "")
    if len(content) > PANE_INPUT_FILE_MAX_CHARS:
        content = content[:PANE_INPUT_FILE_MAX_CHARS] + "\n\n[Herdres truncated this inbound Telegram message locally.]"
    digest = hashlib.sha256(content.encode("utf-8")).hexdigest()[:12]
    stamp = _dt.datetime.now(tz=_dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    path = root / f"{stamp}-{digest}.txt"
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(fd, "w", encoding="utf-8") as fh:
        fh.write(content)
        if not content.endswith("\n"):
            fh.write("\n")
    return path


def pane_input_file_instruction(path: Path, text: str) -> str:
    preview = re.sub(r"\s+", " ", str(text or "").strip())
    preview = sanitize_text(preview, 420)
    line_count = str(text or "").count("\n") + 1 if text else 0
    char_count = len(str(text or ""))
    suffix = f" Preview: {preview}" if preview else ""
    return (
        "Telegram topic message received. "
        f"The full owner message is saved at {path}. "
        "Read that file and treat its contents as the user's instruction; then respond to the owner. "
        f"It has {line_count} lines and {char_count} chars."
        f"{suffix}"
    )


def is_staged_input_refusal(detail: str) -> bool:
    lower = str(detail or "").lower()
    return "staged pane input" in lower or "refusing to append telegram text" in lower


def send_text_and_enter_to_pane(
    pane_id: str,
    text: str,
    *,
    timeout: int = 8,
    deadline: float | None = None,
) -> tuple[bool, str]:
    if _remaining(deadline) <= SEND_TO_PANE_MIN_CALL_SECONDS:
        return False, "Send timed out before delivery (staged input could not be cleared)."
    call_timeout = timeout if deadline is None else _bounded_timeout(deadline)
    text_proc = run_cmd([herdr_bin(), "pane", "send-text", pane_id, text], timeout=call_timeout)
    if text_proc.returncode != 0:
        return False, sanitize_text(text_proc.stderr or text_proc.stdout, 800)
    if _remaining(deadline) <= SEND_TO_PANE_MIN_CALL_SECONDS:
        return False, "Send timed out before delivery (staged input could not be cleared)."
    call_timeout = timeout if deadline is None else _bounded_timeout(deadline)
    enter_proc = run_cmd([herdr_bin(), "pane", "send-keys", pane_id, "enter"], timeout=call_timeout)
    if enter_proc.returncode != 0:
        return False, sanitize_text(enter_proc.stderr or enter_proc.stdout, 800)
    return True, ""


def send_to_pane(
    pane_id: str,
    text: str,
    *,
    timeout: int = 8,
    submit_staged: bool = True,
    deadline: float | None = None,
) -> tuple[bool, str]:
    deadline = _send_deadline(deadline)
    # Reassembly of split Telegram prompts is handled upstream in
    # herdres_gateway.py (buffer_or_assemble); text is already a whole prompt here.
    if _remaining(deadline) <= SEND_TO_PANE_MIN_CALL_SECONDS:
        return False, "Send timed out before delivery (pane busy or unresponsive)."
    pane = pane_by_id(pane_id, deadline=deadline)
    if not pane:
        return False, "Herdr pane is not currently live."
    outbound = str(text or "")
    if pane_input_needs_file(outbound):
        try:
            inbound_path = write_inbound_pane_message(pane_id, outbound)
        except OSError as exc:
            return False, f"Could not write inbound message file: {sanitize_text(str(exc), 300)}"
        outbound = pane_input_file_instruction(inbound_path, outbound)
    window = max(16, outbound.count("\n") + 8)
    clear_ok, clear_detail = clear_staged_pane_input_if_needed(pane_id, timeout=timeout, deadline=deadline, window=window)
    if not clear_ok:
        return False, clear_detail
    if _remaining(deadline) <= SEND_TO_PANE_MIN_CALL_SECONDS:
        return False, "Send timed out before delivery (pane busy or unresponsive)."
    proc = run_cmd([herdr_bin(), "pane", "run", pane_id, outbound], timeout=_bounded_timeout(deadline))
    if proc.returncode != 0:
        detail = sanitize_text(proc.stderr or proc.stdout, 800)
        if not is_staged_input_refusal(detail):
            return False, detail
        clear_ok, clear_detail = clear_staged_pane_input(
            pane_id, timeout=timeout, verify=False, deadline=deadline, window=window
        )
        if not clear_ok:
            return False, clear_detail
        if _remaining(deadline) <= SEND_TO_PANE_MIN_CALL_SECONDS:
            return False, "Send timed out before delivery (staged input could not be cleared)."
        return send_text_and_enter_to_pane(pane_id, outbound, timeout=timeout, deadline=deadline)
    if submit_staged:
        submit_ok, submit_detail = submit_staged_pane_input_if_needed(
            pane_id,
            timeout=timeout,
            agent_status=str(pane.get("agent_status") or ""),
            deadline=deadline,
            window=window,
        )
        if not submit_ok:
            return False, submit_detail
        return True, submit_detail
    return True, ""


def interrupt_and_send_to_pane(pane_id: str, text: str, *, timeout: int = 8) -> tuple[bool, str]:
    deadline = _send_deadline(None)
    # Halt the agent's current turn (Esc) so the message runs now instead of
    # queueing behind it, then deliver via the normal send path. Only interrupt a
    # pane that is actually working — Esc on an idle pane is needless and, on
    # Codex, pops its "edit previous message" recall preview.
    pane = pane_by_id(pane_id, deadline=deadline)
    if pane and str(pane.get("agent_status") or "").strip().lower() == "working":
        if _remaining(deadline) > SEND_TO_PANE_MIN_CALL_SECONDS:
            run_cmd([herdr_bin(), "pane", "send-keys", pane_id, "escape"], timeout=_bounded_timeout(deadline))
        for delay in (0.3, 0.5, 0.7, 1.0):
            if not _deadline_sleep(delay, deadline):
                break
            if _remaining(deadline) <= SEND_TO_PANE_MIN_CALL_SECONDS:
                break
            refreshed = pane_by_id(pane_id, deadline=deadline)
            if not refreshed or str(refreshed.get("agent_status") or "").strip().lower() != "working":
                break
    return send_to_pane(pane_id, text, timeout=timeout, deadline=deadline)


def interrupt_and_send_response(pane_id: str, text: str) -> dict[str, Any]:
    outbound = str(text or "").strip()
    if not outbound:
        return {"handled": True, "reply": "Usage: /send! <instruction> — interrupts the current turn and sends now"}
    ok, detail = interrupt_and_send_to_pane(pane_id, outbound)
    if not ok:
        return {"handled": True, "reply": f"Send failed: {sanitize_text(detail, 300)}"}
    if detail:
        # Esc didn't fully stop the turn — the message queued instead; say so.
        return {"handled": True, "reply": sanitize_text(detail, 300)}
    return {"handled": True, "reply": "⏹️ Interrupted the current turn and sent your message."}


def pane_input_ansi(pane_id: str, *, lines: int = 16, deadline: float | None = None) -> str:
    # Raw ANSI read (NOT via pane_output/sanitize_text, which strip escapes) so
    # styling survives: Codex renders its empty-box placeholder suggestions in
    # SGR "dim" (\x1b[2m), which is how we tell a placeholder from real input.
    try:
        raw = herdr_text(
            ["pane", "read", pane_id, "--source", "recent-unwrapped", "--lines", str(lines), "--format", "ansi"],
            timeout=(8 if deadline is None else _bounded_timeout(deadline)),
        )
    except Exception:
        return ""
    try:
        data = json.loads(raw)
        return str(data.get("result", {}).get("text") or data.get("text") or raw)
    except Exception:
        return raw


def pane_input_looks_staged(pane_id: str, *, deadline: float | None = None, window: int = 16) -> bool:
    window = max(16, int(window))
    ansi_raw = pane_input_ansi(pane_id, lines=window, deadline=deadline)
    if ansi_raw.strip():
        ansi_lines = ansi_raw.splitlines()[-window:]
    else:
        # Fall back to the sanitized text read (no dim info) if ANSI is unavailable.
        ansi_lines = pane_output(
            pane_id, lines=window, max_chars=4000, source="recent-unwrapped", deadline=deadline
        ).splitlines()[-window:]
    clean_lines = [ANSI_RE.sub("", str(line or "")).strip() for line in ansi_lines]
    if not any(clean_lines):
        return False
    if any(CODEX_GOAL_USAGE_FOOTER_RE.search(line) for line in clean_lines):
        return False
    for ansi_line, clean in zip(reversed(ansi_lines), reversed(clean_lines)):
        if PROMPT_PLACEHOLDER_RE.fullmatch(clean):
            return False
        if PROMPT_WITH_TEXT_RE.match(clean):
            # A greyed placeholder suggestion (empty box) is rendered dim; real
            # typed input is not. The truecolor background \x1b[48;2;…m is not the
            # standalone dim code \x1b[2m, so this literal check is unambiguous.
            if "\x1b[2m" in str(ansi_line):
                return False
            return True
        if PROMPT_ONLY_RE.fullmatch(clean):
            return False
        if re.search(r"\[Pasted text #\d+", clean, re.IGNORECASE):
            return True
    return False


CLEAR_STAGED_INPUT_KEY_SEQUENCES = (
    ("ctrl+u",),
    ("ctrl+a", "ctrl+k"),
    ("escape", "ctrl+u"),
    ("ctrl+a", "backspace"),
)
CLEAR_STAGED_INPUT_FORCE_KEY_SEQUENCES = (
    ("escape", "ctrl+u", "ctrl+u"),
    ("ctrl+a", "ctrl+k", "ctrl+k", "ctrl+k"),
    ("cmd+a", "backspace"),
    ("ctrl+e", *("backspace",) * 80),
)
CLEAR_STAGED_INPUT_POLL_DELAYS = (0.1, 0.2, 0.35, 0.6)


def clear_staged_pane_input(
    pane_id: str,
    *,
    timeout: int = 8,
    verify: bool = True,
    deadline: float | None = None,
    window: int = 16,
) -> tuple[bool, str]:
    last_error = ""
    successful_clear_attempt = False
    for keys in CLEAR_STAGED_INPUT_KEY_SEQUENCES + CLEAR_STAGED_INPUT_FORCE_KEY_SEQUENCES:
        if _remaining(deadline) <= SEND_TO_PANE_MIN_CALL_SECONDS:
            break
        call_timeout = timeout if deadline is None else _bounded_timeout(deadline)
        proc = run_cmd([herdr_bin(), "pane", "send-keys", pane_id, *keys], timeout=call_timeout)
        if proc.returncode != 0:
            last_error = sanitize_text(proc.stderr or proc.stdout, 800)
            continue
        successful_clear_attempt = True
        if verify:
            for delay in CLEAR_STAGED_INPUT_POLL_DELAYS:
                if not _deadline_sleep(delay, deadline):
                    break
                if _remaining(deadline) <= SEND_TO_PANE_MIN_CALL_SECONDS:
                    break
                if not pane_input_looks_staged(pane_id, deadline=deadline, window=window):
                    return True, ""
    if last_error and not successful_clear_attempt:
        return False, last_error
    return True, ""


def clear_staged_pane_input_if_needed(
    pane_id: str,
    *,
    timeout: int = 8,
    deadline: float | None = None,
    window: int = 16,
) -> tuple[bool, str]:
    if _remaining(deadline) <= SEND_TO_PANE_MIN_CALL_SECONDS:
        return True, ""
    if not pane_input_looks_staged(pane_id, deadline=deadline, window=window):
        return True, ""
    return clear_staged_pane_input(pane_id, timeout=timeout, deadline=deadline, window=window)


def submit_staged_pane_input_if_needed(
    pane_id: str,
    *,
    timeout: int = 8,
    agent_status: str = "",
    deadline: float | None = None,
    window: int = 16,
) -> tuple[bool, str]:
    # `herdr pane run` is *supposed* to submit the input itself, but in some TUI
    # states it leaves the text staged in the input box (we observed an inbound
    # Telegram message sitting in the box, never sent). Press Enter when we can
    # see staged input. The check is conditional, so a command that herdr already
    # submitted leaves an empty box (which never matches) and is not double-sent.
    # Poll briefly to tolerate terminal render lag before giving up.
    staged = False
    for delay in (0.15, 0.35, 0.6):
        if not _deadline_sleep(delay, deadline):
            break
        if _remaining(deadline) <= SEND_TO_PANE_MIN_CALL_SECONDS:
            break
        if pane_input_looks_staged(pane_id, deadline=deadline, window=window):
            staged = True
            break
    if not staged:
        return True, ""
    if _remaining(deadline) <= SEND_TO_PANE_MIN_CALL_SECONDS:
        return True, "Sent — but I couldn't confirm it submitted within the time budget. Check the pane."
    call_timeout = timeout if deadline is None else _bounded_timeout(deadline)
    proc = run_cmd([herdr_bin(), "pane", "send-keys", pane_id, "enter"], timeout=call_timeout)
    if proc.returncode != 0:
        if _remaining(deadline) <= SEND_TO_PANE_MIN_CALL_SECONDS:
            detail = sanitize_text(proc.stderr or proc.stdout, 800)
            return False, detail
        call_timeout = timeout if deadline is None else _bounded_timeout(deadline)
        fallback = run_cmd([herdr_bin(), "pane", "send-text", pane_id, "\r"], timeout=call_timeout)
        if fallback.returncode != 0:
            detail = sanitize_text(proc.stderr or proc.stdout or fallback.stderr or fallback.stdout, 800)
            return False, detail
    # A zero return code only means the keystroke was delivered, not that the TUI
    # accepted it as a submit (a raw "\r" via send-text in particular is ignored
    # in some states). Confirm the input box actually cleared, polling to absorb
    # render lag, so we never report success while the text still sits unsent.
    for delay in (0.15, 0.35, 0.6):
        if not _deadline_sleep(delay, deadline):
            break
        if _remaining(deadline) <= SEND_TO_PANE_MIN_CALL_SECONDS:
            break
        if not pane_input_looks_staged(pane_id, deadline=deadline, window=window):
            return True, ""
    # Still staged. A working agent queues typed input and won't accept Enter as a
    # submit until its turn finishes, so tell the sender it's queued (not lost).
    # Otherwise trust the delivered Enter rather than refusing — Herdres does not
    # refuse after a delivered submit.
    if str(agent_status or "").strip().lower() == "working":
        return True, "Queued — the agent is busy; your message will run when the current turn finishes."
    if _remaining(deadline) <= SEND_TO_PANE_MIN_CALL_SECONDS:
        return True, "Sent — but I couldn't confirm it submitted within the time budget. Check the pane."
    return True, ""


def telegram_get_file(file_id: str) -> dict[str, Any]:
    response = telegram_api("getFile", {"file_id": str(file_id)})
    result = response.get("result") if isinstance(response, dict) else None
    if not isinstance(result, dict) or not str(result.get("file_path") or ""):
        raise BridgeError("Telegram getFile returned no file_path")
    return result


def download_telegram_file(file_path: str, dest_path: Path, *, max_bytes: int = ATTACHMENT_MAX_BYTES) -> int:
    # Stream to a sibling <name>.part then atomically rename to the final name on
    # success, so a SIGKILL (the bridge kills the subprocess at ~25s) can only
    # leave a .part that is never handed to the agent. O_EXCL|O_NOFOLLOW refuses
    # to follow/overwrite a pre-planted symlink or existing file at the .part
    # name. The byte cap and a wall-clock deadline are both enforced on the bytes
    # actually read (Content-Length / Telegram's declared size are never trusted).
    part_path = dest_path.with_name(dest_path.name + ".part")
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0)
    fd = os.open(part_path, flags, 0o600)
    written = 0
    try:
        with os.fdopen(fd, "wb") as out:
            if dry_run_enabled():
                placeholder = b"[dry-run telegram attachment]\n"
                out.write(placeholder)
                written = len(placeholder)
            else:
                token = telegram_token()
                url = f"{ATTACHMENT_FILE_HOST}/file/bot{token}/{urllib.parse.quote(str(file_path), safe='/')}"
                deadline = time.monotonic() + ATTACHMENT_DOWNLOAD_TIMEOUT
                try:
                    request = urllib.request.Request(url, method="GET")
                    with urllib.request.urlopen(request, timeout=ATTACHMENT_READ_TIMEOUT) as resp:
                        while True:
                            if time.monotonic() > deadline:
                                raise BridgeError("attachment download exceeded the time budget")
                            chunk = resp.read(ATTACHMENT_CHUNK_BYTES)
                            if not chunk:
                                break
                            written += len(chunk)
                            if written > max_bytes:
                                raise BridgeError("attachment exceeds the size cap")
                            out.write(chunk)
                except urllib.error.HTTPError as exc:
                    if exc.code == 429:
                        try:
                            retry_after = int(exc.headers.get("Retry-After") or 1)
                        except (TypeError, ValueError):
                            retry_after = 1
                        raise RateLimited(retry_after) from exc
                    raise BridgeError(f"attachment download failed: HTTP {exc.code}") from exc
                except (urllib.error.URLError, http.client.HTTPException, OSError, ValueError) as exc:
                    # Scrub the token explicitly (the URL embeds it) before it can
                    # reach any reply or log, in addition to SECRET_PATTERNS.
                    detail = sanitize_text(str(exc).replace(token, "REDACTED"), 200)
                    raise BridgeError(f"attachment download failed: {detail}") from exc
        os.replace(part_path, dest_path)
        return written
    except BaseException:
        _unlink_quietly(part_path)
        _unlink_quietly(dest_path)
        raise


def _unlink_quietly(path: Path) -> None:
    try:
        path.unlink()
    except OSError:
        pass


def attachment_dest_dir(pane_id: str) -> Path:
    base = state_path().parent / "attachments"
    root = base / safe_file_component(pane_id)
    # Reject a pre-planted symlink at any level so writes cannot be redirected
    # outside the attachment tree (O_EXCL only guards the final leaf name).
    for ancestor in (base, root):
        if ancestor.is_symlink():
            raise BridgeError("attachment directory path is unsafe (symlink)")
    base.mkdir(parents=True, exist_ok=True)
    root.mkdir(parents=True, exist_ok=True)
    for directory in (base, root):
        try:
            directory.chmod(0o700)
        except OSError:
            pass
    return root


def prune_attachment_dir(root: Path, *, keep: int = ATTACHMENT_KEEP_PER_PANE) -> None:
    """Bound a pane's attachment dir: drop stale .part files and keep only the
    most recent `keep` finished files so a sub-cap flood cannot fill the disk."""
    try:
        entries = [p for p in root.iterdir() if p.is_file() and not p.is_symlink()]
    except OSError:
        return
    for part in (p for p in entries if p.name.endswith(".part")):
        _unlink_quietly(part)
    finals = [p for p in entries if not p.name.endswith(".part")]

    def _mtime(path: Path) -> float:
        try:
            return path.stat().st_mtime
        except OSError:
            return 0.0

    for stale in sorted(finals, key=_mtime, reverse=True)[keep:]:
        _unlink_quietly(stale)


def attachment_dest_path(pane_id: str, attachment: dict[str, Any]) -> Path:
    # The attacker-controlled file_name is NEVER joined as a path: basename strips
    # any directory part, safe_file_component allowlists [A-Za-z0-9_.-], and a
    # UTC stamp + sha256(file_id)[:12] prefix guarantees a unique, traversal-free
    # leaf so O_EXCL never collides on legitimate traffic.
    kind = str(attachment.get("kind") or "")
    raw_name = os.path.basename(str(attachment.get("file_name") or ""))
    safe = safe_file_component(raw_name, fallback=("photo" if kind == "photo" else "attachment"))
    if kind == "photo" and not safe.lower().endswith((".jpg", ".jpeg", ".png", ".webp", ".gif")):
        safe = f"{safe}.jpg"
    digest = hashlib.sha256(str(attachment.get("file_id") or "").encode("utf-8")).hexdigest()[:12]
    # Microseconds + a random nonce make every destination unique, so re-sending
    # the same file_id twice in one second cannot collide (which would let a
    # failed retry's cleanup unlink the earlier, successfully delivered file).
    stamp = _dt.datetime.now(tz=_dt.timezone.utc).strftime("%Y%m%dT%H%M%S_%fZ")
    nonce = os.urandom(4).hex()
    return attachment_dest_dir(pane_id) / f"{stamp}-{nonce}-{digest}-{safe}"


def pane_attachment_instruction(path: Path, attachment: dict[str, Any], caption: str) -> str:
    kind = str(attachment.get("kind") or "")
    mime = sanitize_text(str(attachment.get("mime_type") or ""), 120).strip()
    size = int(attachment.get("file_size") or 0)
    caption_clean = sanitize_text(re.sub(r"\s+", " ", str(caption or "").strip()), 420)
    descriptor: list[str] = []
    if kind != "photo":
        raw_name = sanitize_text(str(attachment.get("file_name") or ""), 200).strip()
        if raw_name:
            descriptor.append(f"original name: {raw_name}")
        if mime:
            descriptor.append(f"type: {mime}")
    else:
        descriptor.append(f"type: {mime or 'image/jpeg'} (photo)")
    descriptor.append(f"{size} bytes")
    head = (
        "Telegram topic attachment received. "
        f"A file the owner sent is saved at {path} ({', '.join(descriptor)}). "
    )
    if caption_clean:
        return head + (
            "Read that file and treat the caption below as the owner's instruction about it; "
            "then respond to the owner.\n"
            f"Caption: {caption_clean}"
        )
    return head + "Read that file and treat its contents as the owner's instruction; then respond to the owner."


def deliver_attachment(pane_id: str, attachment: dict[str, Any]) -> tuple[bool, str, Path | None]:
    cap_mb = ATTACHMENT_MAX_BYTES // (1024 * 1024)
    declared = int(attachment.get("file_size") or 0)
    if declared > ATTACHMENT_MAX_BYTES:
        return (False, f"too large ({declared // (1024 * 1024)} MB); Telegram bots can only fetch files up to {cap_mb} MB.", None)
    try:
        result = telegram_get_file(str(attachment.get("file_id") or ""))
        confirmed = int(result.get("file_size") or 0)
        if confirmed > ATTACHMENT_MAX_BYTES:
            return (False, f"too large ({confirmed // (1024 * 1024)} MB); Telegram bots can only fetch files up to {cap_mb} MB.", None)
        dest = attachment_dest_path(pane_id, attachment)
        written = download_telegram_file(str(result.get("file_path") or ""), dest)
        if confirmed > 0 and written != confirmed and not dry_run_enabled():
            _unlink_quietly(dest)
            return (False, "the download was incomplete (size mismatch); please resend.", None)
        prune_attachment_dir(dest.parent)
    except RateLimited:
        raise
    except (BridgeError, OSError) as exc:
        return (False, sanitize_text(str(exc), 300), None)
    return (True, "", dest)


def visible_choice_selection_keys(choice: str) -> list[str]:
    choice = str(choice or "").strip()
    if VISIBLE_CHOICE_SELECT_MODE in {"number", "numbers", "digit", "digits"}:
        if not choice:
            return ["enter"]
        return [choice, "enter"] if VISIBLE_CHOICE_NUMBER_ENTER else [choice]
    try:
        displayed_number = int(choice)
    except ValueError:
        displayed_number = 0
    if displayed_number < 1:
        return [choice, "enter"] if choice else ["enter"]
    return ["up"] * 24 + ["down"] * (displayed_number - 1) + ["enter"]


def visible_custom_detail_ready_text(raw: str) -> bool:
    lines = clean_feed_lines(raw)
    last_option_idx = -1
    for idx, line in enumerate(lines):
        if option_match(line):
            last_option_idx = idx
    if last_option_idx >= 0:
        lines = lines[last_option_idx + 1 :]
    if not any(str(line or "").strip() for line in lines):
        return False
    low = re.sub(r"\s+", " ", " ".join(lines)).lower()
    return bool(
        re.search(
            r"\b("
            r"type (?:your |an? )?(?:answer|response|instruction)|"
            r"write (?:your |an? )?(?:answer|response|instruction)|"
            r"provide (?:details|an? answer|a response|your response)|"
            r"paste (?:your |an? )?(?:answer|response|instruction)|"
            r"custom (?:answer|response|instruction)|"
            r"(?:answer|response|instruction) now|"
            r"details? (?:for|to send|now)"
            r")\b",
            low,
        )
    )


def wait_for_visible_custom_detail_field(
    pane_id: str,
    *,
    timeout_seconds: float = VISIBLE_CHOICE_VERIFY_SECONDS,
) -> bool:
    deadline = time.time() + max(0.2, timeout_seconds)
    while time.time() < deadline:
        raw = pane_output(pane_id, lines=READ_LINES_COMMAND_MAX, max_chars=FEED_MAX_CHARS, source="visible")
        if raw.strip() and visible_custom_detail_ready_text(raw):
            return True
        time.sleep(0.25)
    return False


def visible_prompt_matches_awaiting(entry: dict[str, Any], awaiting: dict[str, Any]) -> bool:
    current = current_visible_choice_item_for_entry(entry)
    if not current:
        return False
    expected_prompt_id = str(awaiting.get("prompt_id") or "")
    current_prompt_id = str(current.get("prompt_id") or "")
    if expected_prompt_id and current_prompt_id and expected_prompt_id != current_prompt_id:
        return False
    choice = str(awaiting.get("visible_choice") or "").strip()
    if not choice:
        return False
    current_options = list(current.get("options") or [])
    current_match = next((opt for opt in current_options if str(opt.get("number") or "") == choice), None)
    if not current_match:
        return False
    expected_options = awaiting.get("visible_options")
    if isinstance(expected_options, list) and expected_options:
        expected_match = next(
            (opt for opt in expected_options if str(opt.get("number") or "") == choice),
            None,
        )
        if isinstance(expected_match, dict):
            expected_label = re.sub(r"\s+", " ", str(expected_match.get("label") or "").strip()).lower()
            current_label = re.sub(r"\s+", " ", str(current_match.get("label") or "").strip()).lower()
            if expected_label and current_label and expected_label != current_label:
                return False
    return True


def send_choice_detail_to_pane(
    pane_id: str,
    choice: str,
    detail_text: str,
    *,
    timeout: int = 8,
    deadline: float | None = None,
) -> tuple[bool, str]:
    deadline = _send_deadline(deadline)
    choice = str(choice or "").strip()
    if not choice:
        return send_to_pane(pane_id, detail_text, timeout=timeout, deadline=deadline)
    if _remaining(deadline) <= SEND_TO_PANE_MIN_CALL_SECONDS:
        return False, "Send timed out before delivery (pane busy or unresponsive)."
    pane = pane_by_id(pane_id, deadline=deadline)
    if not pane:
        return False, "Herdr pane is not currently live."
    if _remaining(deadline) <= SEND_TO_PANE_MIN_CALL_SECONDS:
        return False, "Send timed out before delivery (pane busy or unresponsive)."
    proc = run_cmd([herdr_bin(), "pane", "send-keys", pane_id, choice, "enter"], timeout=_bounded_timeout(deadline))
    if proc.returncode != 0:
        return False, sanitize_text(proc.stderr or proc.stdout, 800)
    _deadline_sleep(0.2, deadline)
    return send_to_pane(pane_id, detail_text, timeout=timeout, deadline=deadline)


def send_visible_choice_detail_to_pane(
    pane_id: str,
    choice: str,
    detail_text: str,
    *,
    timeout: int = 8,
    deadline: float | None = None,
) -> tuple[bool, str]:
    deadline = _send_deadline(deadline)
    choice = str(choice or "").strip()
    if not choice:
        return False, "This custom option is no longer mapped to a visible choice."
    if _remaining(deadline) <= SEND_TO_PANE_MIN_CALL_SECONDS:
        return False, "Send timed out before delivery (pane busy or unresponsive)."
    pane = pane_by_id(pane_id, deadline=deadline)
    if not pane:
        return False, "Herdr pane is not currently live."
    keys = visible_choice_selection_keys(choice)
    if _remaining(deadline) <= SEND_TO_PANE_MIN_CALL_SECONDS:
        return False, "Send timed out before delivery (pane busy or unresponsive)."
    proc = run_cmd([herdr_bin(), "pane", "send-keys", pane_id, *keys], timeout=_bounded_timeout(deadline))
    if proc.returncode != 0:
        return False, sanitize_text(proc.stderr or proc.stdout, 800)
    if not wait_for_visible_custom_detail_field(pane_id):
        return (
            False,
            "I selected the custom option, but Herdr did not show a custom-answer field. "
            "I did not send your text to avoid answering the wrong prompt.",
        )
    return send_to_pane(pane_id, detail_text, timeout=timeout, submit_staged=True, deadline=deadline)


def telegram_token() -> str:
    load_dotenv()
    # Prefer a herdres-specific outbound token. Nothing else sets it, so it always
    # comes from herdres.env and wins even when TELEGRAM_BOT_TOKEN is pre-injected
    # into the environment (e.g. Hermes's EnvironmentFile on the timer service),
    # which load_dotenv would otherwise refuse to override.
    token = os.getenv("HERDRES_OUTBOUND_BOT_TOKEN", "").strip() or os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
    if not token:
        raise BridgeError("TELEGRAM_BOT_TOKEN is not available")
    return token


def dry_run_enabled() -> bool:
    return parse_bool_env("HERDR_TELEGRAM_TOPICS_DRY_RUN", "")


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
    if method == "getForumTopicIconStickers":
        return {
            "ok": True,
            "result": [
                {"emoji": emoji, "custom_emoji_id": f"dry-{key}"}
                for key, emoji in STATUS_ICON_DEFAULT_EMOJI.items()
            ],
        }
    if method == "getFile":
        return {"ok": True, "result": {"file_id": str(payload.get("file_id", "")), "file_path": "documents/file_0.bin", "file_size": 11}}
    if method in {"sendMessage", "sendRichMessage", "editMessageText"}:
        return {"ok": True, "result": {"message_id": now_id}}
    if method in {"sendMessageDraft", "sendRichMessageDraft"}:
        return {"ok": True, "result": True}
    return {"ok": True, "result": True}


def _parse_telegram_response(method: str, body: str, *, http_code: int | None = None) -> dict[str, Any]:
    """Decode a Telegram API response body and raise on failure.

    Shared by telegram_api and telegram_api_multipart so both paths classify
    429 rate limits, surface description text, and reject non-ok payloads the
    same way. When http_code is given the body came from an HTTPError.
    """
    try:
        parsed = json.loads(body)
    except Exception:
        if http_code is not None:
            raise BridgeError(f"Telegram {method} failed: HTTP {http_code}") from None
        raise BridgeError(f"Telegram {method} failed: invalid JSON") from None
    params = parsed.get("parameters") or {}
    if http_code == 429 and params.get("retry_after"):
        raise RateLimited(int(params["retry_after"]))
    if http_code is not None:
        desc = sanitize_text(str(parsed.get("description") or f"HTTP {http_code}"), 500)
        raise BridgeError(f"Telegram {method} failed: {desc}")
    if not parsed.get("ok"):
        if params.get("retry_after"):
            raise RateLimited(int(params["retry_after"]))
        raise BridgeError(f"Telegram {method} failed: {sanitize_text(str(parsed.get('description')), 500)}")
    return parsed


def telegram_api(method: str, payload: dict[str, Any], *, token: str | None = None) -> dict[str, Any]:
    if dry_run_enabled():
        return dry_run_result(method, payload)
    api_token = token or telegram_token()
    url = f"https://api.telegram.org/bot{api_token}/{method}"
    data = urllib.parse.urlencode(payload).encode("utf-8")
    request = urllib.request.Request(url, data=data, method="POST")
    try:
        with urllib.request.urlopen(request, timeout=20) as resp:
            body = resp.read().decode("utf-8")
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        return _parse_telegram_response(method, body, http_code=exc.code)
    except Exception as exc:
        raise BridgeError(f"Telegram {method} failed: {exc}") from exc
    return _parse_telegram_response(method, body)


def telegram_api_multipart(
    method: str,
    fields: dict[str, Any],
    files: dict[str, Path],
    *,
    token: str | None = None,
) -> dict[str, Any]:
    if dry_run_enabled():
        payload = dict(fields)
        payload["files"] = {key: str(path) for key, path in files.items()}
        return dry_run_result(method, payload)
    api_token = token or telegram_token()
    boundary = "------------------------" + hashlib.sha256(os.urandom(16)).hexdigest()[:16]
    chunks: list[bytes] = []
    for key, value in fields.items():
        chunks.extend(
            [
                f"--{boundary}\r\n".encode("utf-8"),
                f'Content-Disposition: form-data; name="{key}"\r\n\r\n'.encode("utf-8"),
                str(value).encode("utf-8"),
                b"\r\n",
            ]
        )
    for key, path in files.items():
        file_path = Path(path)
        chunks.extend(
            [
                f"--{boundary}\r\n".encode("utf-8"),
                (
                    f'Content-Disposition: form-data; name="{key}"; '
                    f'filename="{file_path.name}"\r\n'
                    "Content-Type: image/jpeg\r\n\r\n"
                ).encode("utf-8"),
                file_path.read_bytes(),
                b"\r\n",
            ]
        )
    chunks.append(f"--{boundary}--\r\n".encode("utf-8"))
    body = b"".join(chunks)
    request = urllib.request.Request(
        f"https://api.telegram.org/bot{api_token}/{method}",
        data=body,
        headers={
            "Content-Type": f"multipart/form-data; boundary={boundary}",
            "Content-Length": str(len(body)),
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=30) as resp:
            response_body = resp.read().decode("utf-8")
    except urllib.error.HTTPError as exc:
        response_body = exc.read().decode("utf-8", errors="replace")
        return _parse_telegram_response(method, response_body, http_code=exc.code)
    except Exception as exc:
        raise BridgeError(f"Telegram {method} failed: {exc}") from exc
    return _parse_telegram_response(method, response_body)


def telegram_api_for_token(method: str, payload: dict[str, Any], api_token: str | None) -> dict[str, Any]:
    if api_token:
        return telegram_api(method, payload, token=api_token)
    return telegram_api(method, payload)


def telegram_message_id(response: dict[str, Any]) -> str | None:
    result = response.get("result") if isinstance(response, dict) else None
    if isinstance(result, dict) and result.get("message_id") is not None:
        return str(result.get("message_id"))
    return None


def _telegram_error_is_bot_access(exc: Exception) -> bool:
    text = str(exc).lower()
    return (
        "forbidden" in text
        or "bot was kicked" in text
        or "bot is not a member" in text
        or "chat not found" in text
        or "not enough rights" in text
        or "need administrator rights" in text
    )


def classify_telegram_error(exc: Exception, *, managed_bot_context: bool = False) -> str:
    text = str(exc).lower()
    if isinstance(exc, RateLimited):
        return "rate_limited"
    if managed_bot_context and _telegram_error_is_bot_access(exc):
        return "bot_access"
    if (
        "message thread not found" in text
        or "message_thread_id_invalid" in text
        or "message thread invalid" in text
        or "thread not found" in text
        or "forum topic not found" in text
        or "topic not found" in text
        or "topic_deleted" in text
    ):
        return "topic_not_found"
    if (
        "sendrichmessage" in text
        and ("not found" in text or "does not exist" in text or "no such method" in text or "http 404" in text)
    ):
        return "capability"
    if "method" in text and ("not found" in text or "does not exist" in text):
        return "capability"
    if "message is not modified" in text:
        return "not_modified"
    if "topic_not_modified" in text:
        return "not_modified"
    if (
        "message to edit not found" in text
        or "message_id_invalid" in text
        or "message can't be edited" in text
        or "replied message not found" in text
        or "reply message not found" in text
        or "message to reply not found" in text
        or "message to be replied not found" in text
        or "reply_to_message_id_invalid" in text
    ):
        return "not_found"
    if (
        "unauthorized" in text
        or "forbidden" in text
        or "bot was blocked by the user" in text
        or "bot blocked" in text
        or "user is deactivated" in text
        or "chat not found" in text
        or "bot is not a member" in text
        or "was kicked" in text
    ):
        return "permanent"
    if "bad request" in text or "can't parse" in text or "entity" in text or "unsupported" in text:
        return "bad_request"
    if any(fragment in text for fragment in ("timed out", "timeout", "temporarily", "network", "connection", "http 5")):
        return "transient"
    return "transient"


def result_topic_missing(result: dict[str, Any] | None) -> bool:
    return isinstance(result, dict) and (
        bool(result.get("topic_missing")) or str(result.get("kind") or "") == "topic_not_found"
    )


def result_pane_root_missing(result: dict[str, Any] | None) -> bool:
    return isinstance(result, dict) and bool(result.get("not_found")) and str(result.get("kind") or "") == "not_found"


def clear_pane_root_message(state: dict[str, Any], entry: dict[str, Any], reason: str = "") -> None:
    old_message_id = str(entry.get("pane_root_message_id") or "")
    for key in ("pane_root_message_id", "pane_root_message_sent_at", "pane_root_message_error"):
        entry.pop(key, None)
    entry["pane_root_message_missing_at"] = utc_now()
    if old_message_id:
        entry["pane_root_message_missing_id"] = old_message_id
    if reason:
        entry["pane_root_message_missing_reason"] = sanitize_text(reason, 500)
    spaces = state.get("spaces") if isinstance(state.get("spaces"), dict) else {}
    space = spaces.get(str(entry.get("space_key") or ""))
    routes = space.get("message_routes") if isinstance(space, dict) and isinstance(space.get("message_routes"), dict) else {}
    if old_message_id and isinstance(routes, dict):
        routes.pop(old_message_id, None)


def pane_root_reply_target(entry: dict[str, Any]) -> str | int | None:
    if not PANE_ROOT_MESSAGES_ENABLED:
        return None
    target = entry.get("pane_root_message_id")
    if isinstance(target, (str, int)):
        return target
    return None


def topic_verify_due(entry: dict[str, Any], ttl_seconds: int = TOPIC_VERIFY_TTL_SECONDS) -> bool:
    if not entry.get("topic_id"):
        return False
    try:
        checked = _dt.datetime.fromisoformat(
            str(entry.get("last_topic_verified_at", "")).replace("Z", "+00:00")
        )
    except Exception:
        try:
            checked = _dt.datetime.fromisoformat(
                str(entry.get("last_topic_verify_attempt_at", "")).replace("Z", "+00:00")
            )
        except Exception:
            return True
    return (_dt.datetime.now(tz=_dt.timezone.utc) - checked).total_seconds() > ttl_seconds


def verify_topic_mapping(chat_id: str, entry: dict[str, Any]) -> dict[str, Any]:
    topic_id = str(entry.get("topic_id") or "")
    if not topic_id:
        return {"ok": False, "kind": "missing_local_topic"}
    name = str(entry.get("topic_name") or "Task")
    try:
        edit_topic(chat_id, topic_id, name)
    except RateLimited:
        raise
    except BridgeError as exc:
        kind = classify_telegram_error(exc)
        if kind == "not_modified":
            entry["last_topic_verified_at"] = utc_now()
            entry.pop("last_topic_verify_attempt_at", None)
            entry.pop("last_topic_verify_error", None)
            entry.pop("last_topic_verify_error_at", None)
            entry.pop("topic_missing_at", None)
            entry.pop("topic_missing_id", None)
            entry.pop("topic_missing_reason", None)
            entry.pop("topic_rename_pending_at", None)
            entry.pop("topic_rename_from", None)
            entry.pop("topic_rename_to", None)
            return {"ok": True, "kind": kind}
        if kind == "topic_not_found":
            return {"ok": False, "kind": kind, "topic_missing": True, "error": str(exc)}
        entry["last_topic_verify_attempt_at"] = utc_now()
        return {"ok": False, "kind": kind, "error": str(exc), "transient": kind == "transient"}
    entry["last_topic_verified_at"] = utc_now()
    entry.pop("last_topic_verify_attempt_at", None)
    entry.pop("last_topic_verify_error", None)
    entry.pop("last_topic_verify_error_at", None)
    entry.pop("topic_missing_at", None)
    entry.pop("topic_missing_id", None)
    entry.pop("topic_missing_reason", None)
    entry.pop("topic_rename_pending_at", None)
    entry.pop("topic_rename_from", None)
    entry.pop("topic_rename_to", None)
    return {"ok": True}


def rich_telegram_state(telegram: dict[str, Any] | None) -> dict[str, Any]:
    if not isinstance(telegram, dict):
        return {}
    rich = telegram.setdefault("rich_messages", {})
    if not isinstance(rich, dict):
        rich = {}
        telegram["rich_messages"] = rich
    rich.setdefault("supported", "unknown")
    return rich


def _rich_disabled_reason_is_capability(reason: str) -> bool:
    text = str(reason or "").lower()
    return any(marker in text for marker in ("method not found", "no such method", "does not exist", "http 404"))


def rich_enabled(telegram: dict[str, Any] | None) -> bool:
    if not RICH_MESSAGES_ENABLED:
        return False
    rich = rich_telegram_state(telegram)
    if str(rich.get("supported") or "unknown") != "no":
        return True
    if _rich_disabled_reason_is_capability(str(rich.get("disabled_reason") or "")):
        return False
    disabled_version_text = str(rich.get("disabled_render_version") or "").strip()
    disabled_version = int(disabled_version_text) if disabled_version_text.isdigit() else 0
    if disabled_version == RICH_RENDER_VERSION:
        return False
    rich["supported"] = "unknown"
    rich.pop("disabled_reason", None)
    rich.pop("disabled_at", None)
    rich.pop("bad_request_streak", None)
    rich.pop("disabled_render_version", None)
    return True


def rich_message_send_enabled(telegram: dict[str, Any] | None) -> bool:
    if not isinstance(telegram, dict):
        return False
    if not rich_enabled(telegram):
        return False
    rich = rich_telegram_state(telegram)
    return str(rich.get("supported") or "unknown") == "yes"


def mark_rich_supported(telegram: dict[str, Any] | None) -> None:
    rich = rich_telegram_state(telegram)
    if rich:
        rich["supported"] = "yes"
        rich.pop("disabled_reason", None)
        rich.pop("bad_request_streak", None)
        rich["last_ok_at"] = utc_now()


def mark_rich_disabled(telegram: dict[str, Any] | None, reason: str) -> None:
    rich = rich_telegram_state(telegram)
    if rich:
        rich["supported"] = "no"
        rich["disabled_reason"] = sanitize_text(reason, 300)
        rich["disabled_at"] = utc_now()
        rich["disabled_render_version"] = RICH_RENDER_VERSION


def note_rich_bad_request(telegram: dict[str, Any] | None, reason: str) -> None:
    # A structural rejection of our HTML repeats on every send; after a few in a
    # row, latch rich off so we stop hammering the API and looping on the fallback.
    rich = rich_telegram_state(telegram)
    if not rich:
        return
    try:
        streak = int(rich.get("bad_request_streak") or 0)
    except (TypeError, ValueError):
        streak = 0
    streak += 1
    rich["bad_request_streak"] = streak
    if streak >= RICH_BAD_REQUEST_LIMIT:
        mark_rich_disabled(telegram, f"repeated bad_request: {reason}")


def telegram_streaming_state(telegram: dict[str, Any] | None) -> dict[str, Any]:
    if not isinstance(telegram, dict):
        return {}
    streaming = telegram.setdefault("streaming_drafts", {})
    if not isinstance(streaming, dict):
        streaming = {}
        telegram["streaming_drafts"] = streaming
    streaming.setdefault("supported", "unknown")
    return streaming


def streaming_enabled(telegram: dict[str, Any] | None) -> bool:
    if not STREAMING_DRAFTS_ENABLED:
        return False
    streaming = telegram_streaming_state(telegram)
    return str(streaming.get("supported") or "unknown") != "no"


def stream_config_enabled() -> bool:
    return STREAMING_DRAFTS_ENABLED


def mark_streaming_supported(telegram: dict[str, Any] | None) -> None:
    streaming = telegram_streaming_state(telegram)
    if streaming:
        streaming["supported"] = "yes"
        streaming["last_ok_at"] = utc_now()
        streaming.pop("disabled_reason", None)


def mark_streaming_disabled(telegram: dict[str, Any] | None, reason: str) -> None:
    streaming = telegram_streaming_state(telegram)
    if streaming:
        streaming["supported"] = "no"
        streaming["disabled_reason"] = sanitize_text(reason, 300)
        streaming["disabled_at"] = utc_now()


def stream_text_hash(text: str) -> str:
    return hashlib.sha256(str(text or "").encode("utf-8")).hexdigest()[:16]


def stream_render_hash(text: str, user_text: str = "") -> str:
    clean_user = sanitize_text(str(user_text or ""), USER_PROMPT_MAX_CHARS).strip()
    clean_text = sanitize_text(str(text or ""), MAX_REPLY_CHARS).strip()
    if clean_user:
        return stream_text_hash(f"{clean_user}\n\n{clean_text}")
    return stream_text_hash(clean_text)


def stream_draft_id(chat_id: str, space_key_value: str, pane_key_value: str, turn_id: str) -> int:
    raw = "|".join([str(chat_id), str(space_key_value), str(pane_key_value), str(turn_id)])
    value = int(hashlib.sha256(raw.encode("utf-8")).hexdigest()[:15], 16) % 2147483647
    return value or 1


def reusable_stream_state_for_turn(entry: dict[str, Any], turn_id: str) -> bool:
    if str(entry.get("last_stream_turn_id") or "") != str(turn_id):
        return False
    message_id = str(entry.get("last_stream_message_id") or "")
    return not message_id or pane_message_is_latest(entry, message_id)


def stream_throttle_reason(entry: dict[str, Any], turn_id: str, *, max_updates: int | None = None) -> str:
    if not reusable_stream_state_for_turn(entry, turn_id):
        return ""
    try:
        update_count = int(entry.get("last_stream_update_count") or 0)
    except (TypeError, ValueError):
        update_count = 0
    if max_updates is not None and max_updates > 0 and update_count >= max_updates:
        return "max_stream_updates"
    if STREAM_MIN_INTERVAL_SECONDS > 0 and entry.get("last_stream_sent_at"):
        try:
            sent_at = _dt.datetime.fromisoformat(str(entry.get("last_stream_sent_at") or "").replace("Z", "+00:00"))
            elapsed = (_dt.datetime.now(tz=_dt.timezone.utc) - sent_at).total_seconds()
        except Exception:
            elapsed = STREAM_MIN_INTERVAL_SECONDS
        if elapsed < STREAM_MIN_INTERVAL_SECONDS:
            return "below_min_interval"
    return ""


def record_stream_sent(
    entry: dict[str, Any],
    turn_id: str,
    text_hash: str,
    *,
    draft_id: str = "",
    transport: str = "",
    text: str = "",
) -> None:
    same_turn = str(entry.get("last_stream_turn_id") or "") == str(turn_id)
    try:
        update_count = int(entry.get("last_stream_update_count") or 0) if same_turn else 0
    except (TypeError, ValueError):
        update_count = 0
    entry["last_stream_hash"] = text_hash
    entry["last_stream_turn_id"] = str(turn_id)
    if text:
        entry["last_stream_text"] = sanitize_text(text, MAX_REPLY_CHARS)
    if draft_id:
        entry["last_stream_draft_id"] = str(draft_id)
    if transport:
        entry["last_stream_transport"] = transport
    entry["last_stream_sent_at"] = utc_now()
    entry["last_stream_update_count"] = update_count + 1
    entry.pop("last_stream_error", None)


def html_to_plain(html_text: str) -> str:
    # Clean plain-text projection of rich HTML for the legacy sendMessage fallback:
    # keeps line structure, never leaks raw markdown.
    text = re.sub(r"</t[dh]>\s*<t[dh]\b[^>]*>", " | ", html_text, flags=re.IGNORECASE)
    text = re.sub(rf"</(?:{_RICH_PLAIN_BREAK_TAG_RE})>", "\n", text, flags=re.IGNORECASE)
    text = re.sub(r"<br\s*/?>", "\n", text)
    text = re.sub(r"<[^>]+>", "", text)
    text = html.unescape(text)
    return re.sub(r"\n{3,}", "\n\n", text).strip()


def _thread_payload(thread_id: str | int | None) -> dict[str, str]:
    tid = str(thread_id or "")
    if tid and tid != DEFAULT_GENERAL_THREAD_ID:
        return {"message_thread_id": tid}
    return {}


def _reply_markup_payload(reply_markup: dict[str, Any] | None) -> dict[str, str]:
    if not reply_markup:
        return {}
    return {"reply_markup": json.dumps(reply_markup, separators=(",", ":"))}


def _message_common_payload(
    chat_id: str,
    *,
    thread_id: str | int | None,
    notify: bool,
    reply_markup: dict[str, Any] | None,
) -> dict[str, Any]:
    payload: dict[str, Any] = {"chat_id": chat_id}
    if not notify:
        payload["disable_notification"] = "true"
    payload.update(_thread_payload(thread_id))
    payload.update(_reply_markup_payload(reply_markup))
    return payload


def _reply_parameters_payload(reply_to_message_id: str | int | None) -> dict[str, str]:
    if not reply_to_message_id:
        return {}
    try:
        return {"reply_parameters": json.dumps({"message_id": int(reply_to_message_id)}, separators=(",", ":"))}
    except (TypeError, ValueError):
        return {}


def send_message_draft(
    chat_id: str,
    text: str,
    *,
    telegram: dict[str, Any] | None,
    draft_id: str | int,
    thread_id: str | int | None = None,
    reply_to_message_id: str | int | None = None,
    api_token: str | None = None,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "chat_id": chat_id,
        "draft_id": str(draft_id),
        "text": sanitize_text(text, MAX_REPLY_CHARS),
    }
    payload.update(_thread_payload(thread_id))
    payload.update(_reply_parameters_payload(reply_to_message_id))
    try:
        telegram_api_for_token("sendMessageDraft", payload, api_token)
    except RateLimited:
        raise
    except BridgeError as exc:
        kind = classify_telegram_error(exc, managed_bot_context=bool(api_token))
        if kind in {"capability", "bad_request"}:
            mark_streaming_disabled(telegram, str(exc))
        if kind == "not_found":
            return {"ok": False, "format": "draft", "kind": kind, "not_found": True, "error": str(exc)}
        return {"ok": False, "format": "draft", "kind": kind, "transient": kind == "transient", "error": str(exc)}
    mark_streaming_supported(telegram)
    return {"ok": True, "format": "draft", "draft_id": str(draft_id)}


def send_rich_message_draft(
    chat_id: str,
    html_text: str,
    *,
    telegram: dict[str, Any] | None,
    fallback_text: str = "",
    draft_id: str | int,
    thread_id: str | int | None = None,
    reply_to_message_id: str | int | None = None,
    api_token: str | None = None,
) -> dict[str, Any]:
    fallback = fallback_text or sanitize_text(html_to_plain(html_text), MAX_REPLY_CHARS)
    if not rich_message_send_enabled(telegram):
        return send_message_draft(
            chat_id,
            fallback,
            telegram=telegram,
            draft_id=draft_id,
            thread_id=thread_id,
            reply_to_message_id=reply_to_message_id,
            api_token=api_token,
        )
    payload: dict[str, Any] = {
        "chat_id": chat_id,
        "draft_id": str(draft_id),
        "rich_message": json.dumps(
            {"html": sanitize_text(html_text, MAX_RICH_HTML_CHARS), "skip_entity_detection": True},
            separators=(",", ":"),
        ),
    }
    payload.update(_thread_payload(thread_id))
    payload.update(_reply_parameters_payload(reply_to_message_id))
    try:
        telegram_api_for_token("sendRichMessageDraft", payload, api_token)
    except RateLimited:
        raise
    except BridgeError as exc:
        kind = classify_telegram_error(exc, managed_bot_context=bool(api_token))
        if kind in {"capability", "bad_request"}:
            mark_streaming_disabled(telegram, str(exc))
        if kind == "not_found":
            return {"ok": False, "format": "rich-draft", "kind": kind, "not_found": True, "error": str(exc)}
        return {"ok": False, "format": "rich-draft", "kind": kind, "transient": kind == "transient", "error": str(exc)}
    mark_streaming_supported(telegram)
    return {"ok": True, "format": "rich-draft", "draft_id": str(draft_id)}


def send_stream_draft(
    chat_id: str,
    telegram: dict[str, Any],
    entry: dict[str, Any],
    *,
    turn_id: str,
    text: str,
    user_text: str = "",
) -> dict[str, Any]:
    if not streaming_enabled(telegram):
        return {"ok": False, "skipped": True, "reason": "streaming_disabled"}
    clean_text = sanitize_text(str(text or "").strip(), MAX_REPLY_CHARS)
    if not clean_text:
        return {"ok": False, "skipped": True, "reason": "empty_stream"}
    if STREAM_MIN_CHARS and len(clean_text) < STREAM_MIN_CHARS and entry.get("last_stream_hash"):
        return {"ok": True, "skipped": True, "reason": "below_min_chars"}
    text_hash = stream_render_hash(clean_text, user_text)
    if entry.get("last_stream_hash") == text_hash and reusable_stream_state_for_turn(entry, str(turn_id)):
        return {"ok": True, "skipped": True, "reason": "unchanged_hash", "hash": text_hash}
    throttle_reason = stream_throttle_reason(entry, str(turn_id), max_updates=MAX_STREAM_DRAFTS)
    if throttle_reason:
        return {"ok": True, "skipped": True, "reason": throttle_reason, "hash": text_hash}
    draft_id = stream_draft_id(
        chat_id,
        str(entry.get("space_key") or ""),
        str(entry.get("pane_key") or ""),
        str(turn_id),
    )
    html_text = render_stream_turn_html(user_text, clean_text, worklog_label=worklog_label_for_turn(entry, str(turn_id)), collapse_chars=int(entry.get("prompt_collapse_chars") or 0))
    result = send_rich_message_draft(
        chat_id,
        html_text,
        telegram=telegram,
        fallback_text=html_to_plain(html_text),
        draft_id=draft_id,
        thread_id=entry.get("topic_id"),
        reply_to_message_id=pane_root_reply_target(entry),
        api_token=managed_bot_token_for_entry(telegram, entry),
    )
    if result.get("ok"):
        record_stream_sent(entry, str(turn_id), text_hash, draft_id=str(draft_id), text=clean_text)
    else:
        entry["last_stream_error"] = sanitize_text(str(result), 500)
    result["hash"] = text_hash
    result["draft_id"] = str(draft_id)
    return result


def send_stream_message(
    chat_id: str,
    telegram: dict[str, Any],
    entry: dict[str, Any],
    *,
    turn_id: str,
    text: str,
    user_text: str = "",
) -> dict[str, Any]:
    if not stream_config_enabled():
        return {"ok": False, "skipped": True, "reason": "streaming_disabled"}
    clean_text = sanitize_text(str(text or "").strip(), MAX_REPLY_CHARS)
    if not clean_text:
        return {"ok": False, "skipped": True, "reason": "empty_stream"}
    desired_bot_kind = desired_message_bot_kind(telegram, entry)
    if managed_bot_access_retry_waiting(entry, "last_stream_bot_kind", desired_bot_kind):
        return {"ok": True, "skipped": True, "reason": "managed_bot_access_pending", "format": "message"}
    if STREAM_MIN_CHARS and len(clean_text) < STREAM_MIN_CHARS and entry.get("last_stream_hash"):
        return {"ok": True, "skipped": True, "reason": "below_min_chars", "format": "message"}
    text_hash = stream_render_hash(clean_text, user_text)
    if (
        entry.get("last_stream_hash") == text_hash
        and reusable_stream_state_for_turn(entry, str(turn_id))
        and not message_bot_reissue_due(entry, "last_stream_bot_kind", desired_bot_kind)
    ):
        return {"ok": True, "skipped": True, "reason": "unchanged_hash", "hash": text_hash, "format": "message"}
    throttle_reason = stream_throttle_reason(entry, str(turn_id))
    if throttle_reason:
        return {"ok": True, "skipped": True, "reason": throttle_reason, "hash": text_hash, "format": "message"}

    html_text = render_stream_turn_html(user_text, clean_text, worklog_label=worklog_label_for_turn(entry, str(turn_id)), collapse_chars=int(entry.get("prompt_collapse_chars") or 0))
    fallback_text = html_to_plain(html_text)
    message_id = ""
    stream_message_id = str(entry.get("last_stream_message_id") or "")
    if stream_message_id and reusable_stream_state_for_turn(entry, str(turn_id)):
        message_id = stream_message_id
    if not message_id:
        message_id = turn_visible_anchor_message_id(entry, str(turn_id))
    if (
        message_id
        and not entry.get("last_stream_bot_kind")
        and message_id == str(entry.get("last_prompt_message_id") or "")
        and entry.get("last_prompt_bot_kind")
    ):
        entry["last_stream_bot_kind"] = str(entry.get("last_prompt_bot_kind") or "")
    if message_id and message_bot_reissue_due(entry, "last_stream_bot_kind", desired_bot_kind):
        message_id = ""
    if message_id:
        result = edit_rich_message(
            chat_id,
            message_id,
            html_text,
            telegram=telegram,
            fallback_text=fallback_text,
            api_token=managed_bot_token_for_entry(telegram, entry),
            rich_payload=True,
        )
        if result.get("not_found"):
            entry.pop("last_stream_message_id", None)
            entry.pop("last_stream_bot_kind", None)
            message_id = ""
        elif result.get("ok"):
            entry["last_stream_message_id"] = message_id
            record_stream_sent(entry, str(turn_id), text_hash, transport="message", text=clean_text)
            record_message_bot_kind(entry, "last_stream_bot_kind", sent_message_bot_kind(telegram, entry, None, result), desired_bot_kind)
            result.update({"format": "message-edit", "hash": text_hash, "message_id": message_id})
            return result
        else:
            if result.get("kind") == "bot_access":
                note_managed_bot_access_required(entry, "last_stream_bot_kind", desired_bot_kind)
            entry["last_stream_error"] = sanitize_text(str(result), 500)
            result.update({"format": "message-edit", "hash": text_hash, "message_id": message_id})
            return result

    result = send_rich_message(
        chat_id,
        html_text,
        telegram=telegram,
        fallback_text=fallback_text,
        thread_id=entry.get("topic_id"),
        notify=False,
        reply_to_message_id=pane_root_reply_target(entry),
        api_token=managed_bot_token_for_entry(telegram, entry),
    )
    result["hash"] = text_hash
    if result.get("ok"):
        mid = str(result.get("message_id") or "")
        if mid:
            entry["last_stream_message_id"] = mid
        record_message_bot_kind(entry, "last_stream_bot_kind", sent_message_bot_kind(telegram, entry, None, result), desired_bot_kind)
        record_stream_sent(entry, str(turn_id), text_hash, transport="message", text=clean_text)
        result["format"] = "message"
        result["sent_message"] = True
    elif result.get("kind") == "bot_access":
        note_managed_bot_access_required(entry, "last_stream_bot_kind", desired_bot_kind)
        entry["last_stream_error"] = sanitize_text(str(result), 500)
    else:
        entry["last_stream_error"] = sanitize_text(str(result), 500)
    return result


def send_stream_update(
    chat_id: str,
    telegram: dict[str, Any],
    entry: dict[str, Any],
    *,
    turn_id: str,
    text: str,
    user_text: str = "",
) -> dict[str, Any]:
    if not stream_config_enabled():
        return {"ok": False, "skipped": True, "reason": "streaming_disabled"}
    if streaming_enabled(telegram):
        draft_result = send_stream_draft(chat_id, telegram, entry, turn_id=turn_id, text=text, user_text=user_text)
        if draft_result.get("ok") or draft_result.get("skipped") or draft_result.get("transient"):
            return draft_result
        if result_topic_missing(draft_result) or draft_result.get("kind") not in {"capability", "bad_request", "bot_access"}:
            return draft_result
    return send_stream_message(chat_id, telegram, entry, turn_id=turn_id, text=text, user_text=user_text)


def send_message(
    chat_id: str,
    text: str,
    *,
    thread_id: str | int | None = None,
    notify: bool = False,
    reply_markup: dict[str, Any] | None = None,
    reply_to_message_id: str | int | None = None,
    api_token: str | None = None,
) -> str | None:
    payload = _message_common_payload(chat_id, thread_id=thread_id, notify=notify, reply_markup=reply_markup)
    payload["text"] = sanitize_text(text, MAX_REPLY_CHARS)
    if reply_to_message_id:
        try:
            payload["reply_parameters"] = json.dumps({"message_id": int(reply_to_message_id)}, separators=(",", ":"))
        except (TypeError, ValueError):
            payload["reply_to_message_id"] = str(reply_to_message_id)
    return telegram_message_id(telegram_api_for_token("sendMessage", payload, api_token))


def send_legacy_message_result(
    chat_id: str,
    text: str,
    *,
    thread_id: str | int | None = None,
    notify: bool = False,
    reply_markup: dict[str, Any] | None = None,
    reply_to_message_id: str | int | None = None,
    api_token: str | None = None,
    allow_managed_bot_fallback: bool = False,
) -> dict[str, Any]:
    try:
        mid = send_message(
            chat_id,
            text,
            thread_id=thread_id,
            notify=notify,
            reply_markup=reply_markup,
            reply_to_message_id=reply_to_message_id,
            api_token=api_token,
        )
    except RateLimited:
        raise
    except BridgeError as exc:
        kind = classify_telegram_error(exc, managed_bot_context=bool(api_token))
        if kind == "topic_not_found":
            return {"ok": False, "format": "legacy", "kind": kind, "topic_missing": True, "error": str(exc)}
        if kind == "not_found":
            return {"ok": False, "format": "legacy", "kind": kind, "not_found": True, "error": str(exc)}
        if kind == "bot_access" and api_token and allow_managed_bot_fallback:
            result = send_legacy_message_result(
                chat_id,
                text,
                thread_id=thread_id,
                notify=notify,
                reply_markup=reply_markup,
                reply_to_message_id=reply_to_message_id,
                api_token=None,
                allow_managed_bot_fallback=False,
            )
            result["fallback_reason"] = "managed_bot_access"
            result["managed_bot_fallback"] = True
            return result
        return {"ok": False, "format": "legacy", "kind": kind, "transient": kind == "transient", "error": str(exc)}
    return {"ok": True, "format": "legacy", "message_id": mid}


def edit_message_text(
    chat_id: str,
    message_id: str | int,
    text: str,
    *,
    reply_markup: dict[str, Any] | None = None,
    api_token: str | None = None,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "chat_id": chat_id,
        "message_id": str(message_id),
        "text": sanitize_text(text, MAX_REPLY_CHARS),
    }
    payload.update(_reply_markup_payload(reply_markup))
    try:
        response = telegram_api_for_token("editMessageText", payload, api_token)
    except RateLimited:
        raise
    except BridgeError as exc:
        kind = classify_telegram_error(exc, managed_bot_context=bool(api_token))
        result = {"ok": kind == "not_modified", "format": "legacy", "kind": kind, "error": str(exc)}
        if kind == "not_found":
            result["not_found"] = True
        if kind == "topic_not_found":
            result["topic_missing"] = True
        return result
    return {"ok": True, "format": "legacy", "kind": "edited", "message_id": telegram_message_id(response)}


def pin_chat_message(chat_id: str, message_id: str | int) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "chat_id": chat_id,
        "message_id": str(message_id),
        "disable_notification": "true",
    }
    try:
        response = telegram_api("pinChatMessage", payload)
    except RateLimited:
        raise
    except BridgeError as exc:
        return {"ok": False, "error": str(exc)}
    if not response.get("ok", True):
        return {"ok": False, "error": sanitize_text(str(response), 500)}
    return {"ok": bool(response.get("result", True))}


def warn_pinned_status(message: str) -> None:
    print(f"herdres pinned status warning: {message}", file=sys.stderr)


def _pinned_status_env_topic_id() -> str:
    raw = os.getenv("HERDR_TELEGRAM_TOPICS_PINNED_STATUS_TOPIC_ID")
    if raw is None:
        return ""
    raw = raw.strip()
    if not raw:
        return ""
    try:
        return str(int(raw))
    except ValueError:
        return ""


def pinned_status_topic_id(state: dict[str, Any]) -> str:
    # PURE: resolve the dashboard topic id from env or persisted state only.
    # We do NOT auto-create a topic — the operator must point this at an existing
    # forum topic via HERDR_TELEGRAM_TOPICS_PINNED_STATUS_TOPIC_ID (no surprise topic creation).
    env_topic_id = _pinned_status_env_topic_id()
    if env_topic_id:
        return env_topic_id
    telegram = state.get("telegram") if isinstance(state.get("telegram"), dict) else {}
    return str(telegram.get("pinned_status_topic_id") or "").strip()


def recreate_pinned_status_message(
    state: dict[str, Any],
    chat_id: str,
    topic_id: str,
    text: str,
) -> bool:
    telegram = state.setdefault("telegram", {})
    if not isinstance(telegram, dict):
        return False
    msg_id = send_message(chat_id, text, thread_id=topic_id, notify=False)
    if not msg_id:
        telegram["pinned_status_last_error"] = "sendMessage returned no message_id"
        telegram["pinned_status_last_error_at"] = utc_now()
        return False
    telegram["pinned_status_msg_id"] = str(msg_id)
    telegram["pinned_status_text"] = text
    telegram["pinned_status_topic_id"] = str(topic_id)
    telegram.pop("pinned_status_last_error", None)
    telegram.pop("pinned_status_last_error_at", None)
    pin_result = pin_chat_message(chat_id, msg_id)
    if not pin_result.get("ok"):
        warning = sanitize_text(str(pin_result.get("error") or pin_result), 500)
        telegram["pinned_status_pin_error"] = warning
        telegram["pinned_status_pin_error_at"] = utc_now()
        warn_pinned_status(warning)
    else:
        telegram.pop("pinned_status_pin_error", None)
        telegram.pop("pinned_status_pin_error_at", None)
        telegram["pinned_status_pinned_at"] = utc_now()
    return True


def sync_pinned_status_overview(
    state: dict[str, Any],
    bot_token: str,
    chat_id: str,
    panes: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    del bot_token
    if not pinned_status_enabled():
        return {"ok": True, "changed": False, "skipped": "disabled"}
    if panes is None:
        panes = pane_list()
    # Global dashboard: label rows by topic (distinguishes panes across spaces).
    text = render_pinned_status(state, panes, label_fn=lambda p: pinned_status_pane_label(state, p))
    telegram = state.setdefault("telegram", {})
    if not isinstance(telegram, dict):
        return {"ok": False, "changed": False, "error": "telegram state is not a dict"}
    # Read the previously-stored topic id BEFORE resolving, so a changed env id is
    # detected (the resolver is pure and does not mutate state).
    old_topic_id = str(telegram.get("pinned_status_topic_id") or "").strip()
    topic_id = pinned_status_topic_id(state)
    general_id = str(telegram.get("general_thread_id", DEFAULT_GENERAL_THREAD_ID) or DEFAULT_GENERAL_THREAD_ID)
    if str(topic_id or "").strip() in {"", "0", general_id}:
        warn_pinned_status("no dashboard topic set (HERDR_TELEGRAM_TOPICS_PINNED_STATUS_TOPIC_ID unset or General); skipping")
        return {"ok": True, "changed": False, "skipped": "no_topic"}
    if old_topic_id and old_topic_id != str(topic_id):
        telegram.pop("pinned_status_msg_id", None)
        telegram.pop("pinned_status_text", None)
    telegram["pinned_status_topic_id"] = str(topic_id)
    msg_id = telegram.get("pinned_status_msg_id")
    if msg_id and str(telegram.get("pinned_status_text") or "") == text:
        return {"ok": True, "changed": False, "skipped": "unchanged"}
    if not msg_id:
        return {"ok": True, "changed": recreate_pinned_status_message(state, chat_id, str(topic_id), text)}
    result = edit_message_text(chat_id, msg_id, text)
    if result.get("ok"):
        telegram["pinned_status_text"] = text
        telegram["pinned_status_topic_id"] = str(topic_id)
        telegram.pop("pinned_status_last_error", None)
        telegram.pop("pinned_status_last_error_at", None)
        return {"ok": True, "changed": result.get("kind") != "not_modified"}
    if result.get("not_found") or result.get("kind") in {"not_found", "message_not_found"}:
        telegram.pop("pinned_status_msg_id", None)
        telegram.pop("pinned_status_text", None)
        return {"ok": True, "changed": recreate_pinned_status_message(state, chat_id, str(topic_id), text)}
    telegram["pinned_status_last_error"] = sanitize_text(str(result.get("error") or result), 500)
    telegram["pinned_status_last_error_at"] = utc_now()
    return {"ok": False, "changed": False, "error": telegram["pinned_status_last_error"]}


_RICH_TOP_BLOCK_RE = re.compile(
    rf"<({_RICH_TOP_BLOCK_TAG_RE})\b[^>]*>.*?</\1>|<br\s*/?>",
    re.DOTALL | re.IGNORECASE,
)


_DETAILS_WRAPPER_RE = re.compile(
    r"^\s*(<(ol|ul|table|blockquote|pre)\b[^>]*>)(.*)(</\2>)\s*\Z",
    re.DOTALL | re.IGNORECASE,
)
_SUMMARY_RE = re.compile(r"^\s*<summary\b[^>]*>.*?</summary>\s*", re.DOTALL | re.IGNORECASE)


def _split_top_level_units(inner: str, delim_re: str, container_re: str | None) -> list[str]:
    """Split `inner` into units, each ending at a TOP-LEVEL delimiter match.

    When `container_re` is given (e.g. "ol|ul" for the `</li>` delimiter, or
    "table" for `</tr>`), a delimiter nested inside another such container is NOT
    treated as a boundary — so a nested sub-list/table inside an item keeps the
    whole parent item in one unit and every emitted chunk stays tag-balanced.
    When `container_re` is None (e.g. `<br>`/newline) every delimiter is a
    boundary. Delimiters stay attached to the unit they close."""
    if container_re is None:
        units, cur = [], ""
        for part in re.split("(" + delim_re + ")", inner, flags=re.IGNORECASE):
            if not part:
                continue
            cur += part
            if re.fullmatch(delim_re, part, flags=re.IGNORECASE):
                units.append(cur)
                cur = ""
        if cur:
            units.append(cur)
        return [u for u in units if u]
    token_re = re.compile(
        r"<(/?)(" + container_re + r")\b[^>]*>|(" + delim_re + r")", re.IGNORECASE
    )
    units, depth, start = [], 0, 0
    for tok in token_re.finditer(inner):
        if tok.group(3) is not None:  # boundary delimiter (e.g. </li>, </tr>)
            if depth == 0:
                units.append(inner[start:tok.end()])
                start = tok.end()
        elif tok.group(1):  # nested container close
            depth = max(0, depth - 1)
        else:  # nested container open
            depth += 1
    if start < len(inner):
        units.append(inner[start:])
    return [u for u in units if u]


# An "atom" for inline-aware breaking: a COMPLETE non-nested inline element
# (kept whole so we never split mid-<b>/<code>/<a>/<td>), a lone/void tag, a run
# of non-space non-`<` text, or a whitespace run.
_INLINE_ATOM_RE = re.compile(
    r"<([a-zA-Z0-9]+)\b[^>]*>.*?</\1\s*>|<[^>]+>|[^<\s]+|\s+", re.DOTALL | re.IGNORECASE
)


def _break_inline(html: str, budget: int) -> list[str]:
    """Break inline/flow HTML into fragments each <= budget WITHOUT ever splitting
    inside a tag or an inline element. A complete element (e.g. <td>…</td>,
    <code>…</code>) is atomic; an oversize element is recursed into and re-wrapped;
    an oversize bare text token is char-sliced as a last resort (never loops).

    Contract: every fragment is <= budget AND tag-balanced, EXCEPT a single
    indivisible atom whose own tag overhead exceeds the budget (only reachable via
    an absurdly long <a href="…"> URL — herdres emits no other attribute-bearing
    inline tag). Such an atom is emitted whole and VALID rather than char-sliced
    through its tag (which would corrupt it); its size is bounded by the one atom
    and stays well under the hard MAX_RICH_HTML_CHARS ceiling, so Telegram still
    accepts it. Balance always wins over the soft size target."""
    if len(html) <= budget:
        return [html]
    pieces, cur = [], ""
    for atom in (m.group(0) for m in _INLINE_ATOM_RE.finditer(html)):
        if len(atom) > budget:
            # An atom that alone exceeds the budget: flush whatever is buffered
            # first, then subdivide the atom itself (recurse into an element so its
            # tags stay balanced; char-slice an indivisible bare token).
            if cur:
                pieces.append(cur)
                cur = ""
            em = re.match(r"(<([a-zA-Z0-9]+)\b[^>]*>)(.*)(</\2\s*>)\Z", atom, re.DOTALL | re.IGNORECASE)
            if em:
                open_t, _name, inner, close_t = em.groups()
                sub_budget = max(40, budget - len(open_t) - len(close_t))
                pieces.extend(open_t + frag + close_t for frag in _break_inline(inner, sub_budget))
            else:
                pieces.extend(atom[k:k + budget] for k in range(0, len(atom), budget))
            continue
        if cur and len(cur) + len(atom) > budget:
            pieces.append(cur)
            cur = atom
        else:
            cur += atom
    if cur:
        pieces.append(cur)
    return [p for p in pieces if p] or [html]


def _hard_break_unit(unit: str, budget: int) -> list[str]:
    """Break a single over-budget unit so no emitted piece exceeds `budget`, while
    keeping every piece tag-balanced.

    A unit containing a nested BLOCK wrapper (<ol>/<ul>/<table>/<blockquote>/<pre>)
    is kept whole — breaking it would sever the nested structure (the GAP-1 bug);
    balance wins over size for that rare case. A `<li>`/`<tr>` is broken via the
    inline-aware breaker on its inner content (inline tags and table cells stay
    intact, oversize text is char-sliced) and each fragment re-wrapped in the same
    item tag. Anything else is inline-broken directly."""
    if len(unit) <= budget:
        return [unit]
    if re.search(r"<(ol|ul|table|blockquote|pre|details)\b", unit, re.IGNORECASE):
        return [unit]
    item = re.match(r"(<(li|tr)\b[^>]*>)(.*)(</\2>)\Z", unit, re.DOTALL | re.IGNORECASE)
    if item:
        open_t, _name, text, close_t = item.groups()
        inner_budget = max(80, budget - len(open_t) - len(close_t))
        return [open_t + frag + close_t for frag in _break_inline(text, inner_budget)]
    return _break_inline(unit, budget)


def _hard_split_rich_block(block: str, limit: int) -> list[str]:
    # A single top-level block bigger than the limit (e.g. a long <details> turn
    # block whose inner <ol>/<table>/<blockquote> overflows). Split the inner
    # content at TOP-LEVEL row/list/line boundaries (nested sub-lists/tables stay
    # intact) and re-wrap each piece in BOTH the outer tag AND the nested wrapper
    # so every chunk is tag-balanced; the <summary> rides the first chunk only.
    m = re.match(r"(<([a-zA-Z0-9]+)\b[^>]*>)(.*)(</\2>)\Z", block, re.DOTALL)
    if not m:
        # No clean outer wrapper (inline text / <br>): inline-aware break so inline
        # tags stay intact and an indivisible whitespace-free token is char-sliced
        # (every piece <= limit).
        return _break_inline(block, limit)
    open_tag, name, inner, close_tag = m.groups()
    # Peel a leading <summary>…</summary> off the inner HTML (only meaningful for
    # <details>, harmless to check for other tags since they won't have one).
    summary_html = ""
    body = inner
    if name.lower() == "details":
        sm = _SUMMARY_RE.match(inner)
        if sm:
            summary_html = sm.group(0).strip()
            body = inner[sm.end():]

    def _delims_for(html: str) -> tuple[str, str | None]:
        if "</tr>" in html:
            return r"</tr>", r"table"
        if "</li>" in html:
            return r"</li>", r"ol|ul"
        return r"<br\s*/?>|\n", None

    wm = _DETAILS_WRAPPER_RE.match(body)
    if not wm:
        # No recognizable single nested wrapper: split body directly (no nested
        # wrapper to sever), but still depth-aware + hard-break over-budget units.
        budget = max(200, limit - len(open_tag) - len(close_tag) - len(summary_html))
        delim_re, container_re = _delims_for(body)
        units = []
        for u in _split_top_level_units(body, delim_re, container_re):
            units.extend(_hard_break_unit(u, budget))
        pieces, cur, first = [], "", True
        for u in units:
            if cur and len(cur) + len(u) > budget:
                pieces.append(open_tag + (summary_html if first else "") + cur + close_tag)
                cur, first = u, False
            else:
                cur += u
        if cur:
            pieces.append(open_tag + (summary_html if first else "") + cur + close_tag)
        return pieces or [block]
    w_open, w_name, w_inner, w_close = wm.groups()
    # Pick the structural delimiter by wrapper type so a chunk is never cut
    # mid-row / mid-item, and the nesting container so nested sub-lists/tables
    # are never treated as top-level boundaries.
    if w_name.lower() == "table":
        delim_re, container_re = r"</tr>", r"table"
    elif w_name.lower() in ("ol", "ul"):
        delim_re, container_re = r"</li>", r"ol|ul"
    else:  # blockquote / pre — no nestable item boundary
        delim_re, container_re = r"<br\s*/?>|\n", None
    # Budget must account for outer tag + summary + wrapper open/close per chunk.
    per_chunk_overhead = len(open_tag) + len(close_tag) + len(w_open) + len(w_close)
    budget = max(200, limit - per_chunk_overhead - len(summary_html))
    # Top-level units (nested sub-lists/tables stay whole), then hard-break any
    # single unit that alone exceeds the budget so no chunk silently overflows.
    units = []
    for u in _split_top_level_units(w_inner, delim_re, container_re):
        units.extend(_hard_break_unit(u, budget))
    pieces, cur, first = [], "", True
    for u in units:
        if cur and len(cur) + len(u) > budget:
            pieces.append(
                open_tag + (summary_html if first else "") + w_open + cur + w_close + close_tag
            )
            cur, first = u, False
        else:
            cur += u
    if cur:
        pieces.append(
            open_tag + (summary_html if first else "") + w_open + cur + w_close + close_tag
        )
    return pieces or [block]


def split_rich_html(html_text: str, limit: int) -> list[str]:
    """Split rich HTML into chunks each <= limit, breaking only between top-level
    blocks (paragraphs, tables, lists, quotes, ...) so no tag is ever cut in half."""
    if len(html_text) <= limit:
        return [html_text]
    segments: list[str] = []
    pos = 0
    for match in _RICH_TOP_BLOCK_RE.finditer(html_text):
        if match.start() > pos:
            between = html_text[pos:match.start()]
            if between.strip():
                segments.append(between)
        segments.append(match.group(0))
        pos = match.end()
    if pos < len(html_text):
        tail = html_text[pos:]
        if tail.strip():
            segments.append(tail)
    if not segments:
        return _hard_split_rich_block(html_text, limit)
    chunks: list[str] = []
    cur = ""
    for seg in segments:
        if len(seg) > limit:
            if cur:
                chunks.append(cur)
                cur = ""
            chunks.extend(_hard_split_rich_block(seg, limit))
            continue
        if cur and len(cur) + len(seg) > limit:
            chunks.append(cur)
            cur = seg
        else:
            cur += seg
    if cur:
        chunks.append(cur)
    return chunks


def _record_rich_failure(kind: str, telegram: dict[str, Any] | None, exc: Exception) -> None:
    if kind == "capability":
        mark_rich_disabled(telegram, str(exc))
    elif kind == "bad_request":
        note_rich_bad_request(telegram, str(exc))


def _send_rich_chunk(
    chat_id: str,
    html_text: str,
    *,
    telegram: dict[str, Any] | None,
    fallback: str,
    thread_id: str | int | None,
    notify: bool,
    reply_markup: dict[str, Any] | None,
    reply_to_message_id: str | int | None,
    api_token: str | None = None,
    allow_managed_bot_fallback: bool = False,
) -> dict[str, Any]:
    if not rich_message_send_enabled(telegram):
        return send_legacy_message_result(
            chat_id,
            fallback,
            thread_id=thread_id,
            notify=notify,
            reply_markup=reply_markup,
            reply_to_message_id=reply_to_message_id,
            api_token=api_token,
            allow_managed_bot_fallback=allow_managed_bot_fallback,
        )

    payload = _message_common_payload(chat_id, thread_id=thread_id, notify=notify, reply_markup=reply_markup)
    payload["rich_message"] = json.dumps(
        {"html": html_text, "skip_entity_detection": True},
        separators=(",", ":"),
    )
    if reply_to_message_id:
        try:
            payload["reply_parameters"] = json.dumps({"message_id": int(reply_to_message_id)}, separators=(",", ":"))
        except (TypeError, ValueError):
            pass

    try:
        response = telegram_api_for_token("sendRichMessage", payload, api_token)
    except RateLimited:
        raise
    except BridgeError as exc:
        kind = classify_telegram_error(exc, managed_bot_context=bool(api_token))
        if kind == "capability":
            _record_rich_failure(kind, telegram, exc)
            result = send_legacy_message_result(
                chat_id,
                fallback,
                thread_id=thread_id,
                notify=notify,
                reply_markup=reply_markup,
                reply_to_message_id=reply_to_message_id,
                api_token=api_token,
                allow_managed_bot_fallback=allow_managed_bot_fallback,
            )
            result["fallback_reason"] = kind
            return result
        if kind == "bad_request":
            _record_rich_failure(kind, telegram, exc)
            result = send_legacy_message_result(
                chat_id,
                fallback,
                thread_id=thread_id,
                notify=notify,
                reply_markup=reply_markup,
                reply_to_message_id=reply_to_message_id,
                api_token=api_token,
                allow_managed_bot_fallback=allow_managed_bot_fallback,
            )
            result["fallback_reason"] = kind
            return result
        if kind == "topic_not_found":
            return {"ok": False, "format": "rich", "kind": kind, "topic_missing": True, "error": str(exc)}
        if kind == "not_found":
            return {"ok": False, "format": "rich", "kind": kind, "not_found": True, "error": str(exc)}
        if kind == "bot_access" and api_token and allow_managed_bot_fallback:
            result = _send_rich_chunk(
                chat_id,
                html_text,
                telegram=telegram,
                fallback=fallback,
                thread_id=thread_id,
                notify=notify,
                reply_markup=reply_markup,
                reply_to_message_id=reply_to_message_id,
                api_token=None,
                allow_managed_bot_fallback=False,
            )
            result["fallback_reason"] = "managed_bot_access"
            result["managed_bot_fallback"] = True
            return result
        return {"ok": False, "format": "rich", "kind": kind, "transient": kind == "transient", "error": str(exc)}
    mark_rich_supported(telegram)
    return {"ok": True, "format": "rich", "message_id": telegram_message_id(response)}


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
    api_token: str | None = None,
    allow_managed_bot_fallback: bool = False,
) -> dict[str, Any]:
    fallback = fallback_text or sanitize_text(html_to_plain(html_text), MAX_REPLY_CHARS)
    if not rich_message_send_enabled(telegram):
        return send_legacy_message_result(
            chat_id,
            fallback,
            thread_id=thread_id,
            notify=notify,
            reply_markup=reply_markup,
            reply_to_message_id=reply_to_message_id,
            api_token=api_token,
            allow_managed_bot_fallback=allow_managed_bot_fallback,
        )

    sanitized = sanitize_text(html_text, MAX_RICH_HTML_CHARS)
    chunks = split_rich_html(sanitized, RICH_SAFE_CHARS)
    # Drop chunks whose plain-text projection is empty (a hard-split bug used to
    # emit e.g. <details></ol></details> with no visible content). Recompute the
    # first/last indices over the survivors so notify/reply_markup still attach
    # to the real first/last chunk rather than a blank one.
    chunks = [c for c in chunks if html_to_plain(c).strip()]
    if not chunks:
        return {"ok": False, "format": "rich", "kind": "empty"}
    # Common case: one chunk, identical behaviour to before.
    if len(chunks) <= 1:
        return _send_rich_chunk(
            chat_id,
            chunks[0],
            telegram=telegram,
            fallback=fallback,
            thread_id=thread_id,
            notify=notify,
            reply_markup=reply_markup,
            reply_to_message_id=reply_to_message_id,
            api_token=api_token,
            allow_managed_bot_fallback=allow_managed_bot_fallback,
        )
    # Oversize: deliver as sequential messages. The buttons (reply_markup) ride the
    # last chunk; reply_to ties the first. First chunk's result is the anchor.
    first_result: dict[str, Any] | None = None
    last = len(chunks) - 1
    for idx, chunk in enumerate(chunks):
        result = _send_rich_chunk(
            chat_id,
            chunk,
            telegram=telegram,
            fallback=sanitize_text(html_to_plain(chunk), MAX_REPLY_CHARS),
            thread_id=thread_id,
            notify=notify if idx == 0 else False,
            reply_markup=reply_markup if idx == last else None,
            reply_to_message_id=reply_to_message_id if idx == 0 else None,
            api_token=api_token,
            allow_managed_bot_fallback=allow_managed_bot_fallback,
        )
        if idx == 0:
            first_result = result
            if not result.get("ok"):
                return result
        elif not result.get("ok"):
            result["partial_sent"] = True
            result["failed_chunk_index"] = idx
            if first_result.get("message_id"):
                result["message_id"] = first_result["message_id"]
            return result
    return first_result or {"ok": False, "format": "rich", "kind": "empty"}


def edit_rich_message(
    chat_id: str,
    message_id: str | int,
    html_text: str,
    *,
    telegram: dict[str, Any] | None = None,
    fallback_text: str = "",
    reply_markup: dict[str, Any] | None = None,
    api_token: str | None = None,
    rich_payload: bool = False,
) -> dict[str, Any]:
    fallback = fallback_text or sanitize_text(html_to_plain(html_text), MAX_REPLY_CHARS)
    if not rich_payload or not rich_message_send_enabled(telegram):
        return edit_message_text(chat_id, message_id, fallback, reply_markup=reply_markup, api_token=api_token)

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
        response = telegram_api_for_token("editMessageText", payload, api_token)
    except RateLimited:
        raise
    except BridgeError as exc:
        kind = classify_telegram_error(exc, managed_bot_context=bool(api_token))
        if kind == "not_modified":
            return {"ok": True, "format": "rich", "kind": kind}
        if kind == "not_found":
            return {"ok": False, "format": "rich", "kind": kind, "not_found": True, "error": str(exc)}
        if kind == "topic_not_found":
            return {"ok": False, "format": "rich", "kind": kind, "topic_missing": True, "error": str(exc)}
        if kind == "capability":
            mark_rich_disabled(telegram, str(exc))
            legacy = edit_message_text(chat_id, message_id, fallback, reply_markup=reply_markup, api_token=api_token)
            legacy["fallback_reason"] = kind
            return legacy
        if kind == "bad_request":
            note_rich_bad_request(telegram, str(exc))
            legacy = edit_message_text(chat_id, message_id, fallback, reply_markup=reply_markup, api_token=api_token)
            legacy["fallback_reason"] = kind
            return legacy
        return {"ok": False, "format": "rich", "kind": kind, "transient": kind == "transient", "error": str(exc)}
    mark_rich_supported(telegram)
    return {"ok": True, "format": "rich", "kind": "edited", "message_id": telegram_message_id(response)}


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
    api_token: str | None = None,
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
        api_token=api_token,
    )


def edit_feed_item(
    chat_id: str,
    message_id: str | int,
    item: dict[str, Any],
    *,
    telegram: dict[str, Any] | None,
    reply_markup: dict[str, Any] | None = None,
    live: bool = False,
    api_token: str | None = None,
) -> dict[str, Any]:
    return edit_rich_message(
        chat_id,
        message_id,
        render_feed_item_html(item, live=live),
        telegram=telegram,
        fallback_text=item_plain_text(item),
        reply_markup=reply_markup,
        api_token=api_token,
        rich_payload=True,
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
    api_token: str | None = None,
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
        api_token=api_token,
    )


def update_live_card(
    chat_id: str,
    entry: dict[str, Any],
    item: dict[str, Any],
    *,
    telegram: dict[str, Any],
) -> dict[str, Any]:
    api_token = managed_bot_token_for_entry(telegram, entry)
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
        return {"ok": True, "format": str(entry.get("card_format") or "legacy"), "kind": "unchanged", "attempted": False}

    message_id = str(entry.get("card_message_id") or "")
    if message_id:
        result = edit_rich_message(
            chat_id,
            message_id,
            html_text,
            telegram=telegram,
            fallback_text=plain,
            api_token=api_token,
            rich_payload=True,
        )
        if result.get("ok"):
            entry["card_hash"] = card_hash
            entry["card_format"] = str(result.get("format") or "legacy")
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
        reply_to_message_id=pane_root_reply_target(entry),
        api_token=api_token,
    )
    if result.get("ok"):
        if result.get("message_id"):
            entry["card_message_id"] = str(result["message_id"])
        entry["card_hash"] = card_hash
        entry["card_format"] = str(result.get("format") or "rich")
    return {**result, "attempted": True}


def update_status_marker(
    chat_id: str,
    entry: dict[str, Any],
    pane: dict[str, Any],
    *,
    telegram: dict[str, Any],
) -> dict[str, Any]:
    api_token = managed_bot_token_for_entry(telegram, entry, pane)
    desired_bot_kind = desired_message_bot_kind(telegram, entry, pane)
    marker_hash = status_marker_hash(pane)
    if managed_bot_access_retry_waiting(entry, "status_marker_bot_kind", desired_bot_kind):
        return {"ok": True, "kind": "managed_bot_access_pending", "attempted": False}
    if (
        marker_hash == entry.get("status_marker_hash")
        and entry.get("status_marker_message_id")
        and not message_bot_reissue_due(entry, "status_marker_bot_kind", desired_bot_kind)
    ):
        return {"ok": True, "kind": "unchanged", "attempted": False}
    title, body = status_marker_content(pane)
    old_message_id = str(entry.get("status_marker_message_id") or "")
    old_bot_kind = str(entry.get("status_marker_bot_kind") or MANAGER_BOT_KIND)
    result = send_notice(
        chat_id,
        title,
        body,
        telegram=telegram,
        thread_id=entry.get("topic_id"),
        notify=False,
        reply_to_message_id=pane_root_reply_target(entry),
        api_token=api_token,
    )
    if result.get("ok"):
        new_message_id = str(result.get("message_id") or "")
        if old_message_id and new_message_id and old_message_id != new_message_id and STATUS_MARKER_DELETE_OLD:
            try:
                if api_token and old_bot_kind == desired_bot_kind:
                    delete_message(chat_id, old_message_id, api_token=api_token)
                else:
                    delete_message(chat_id, old_message_id)
            except Exception:
                entry["last_status_marker_delete_error"] = utc_now()
        if new_message_id:
            entry["status_marker_message_id"] = new_message_id
        record_message_bot_kind(
            entry,
            "status_marker_bot_kind",
            sent_message_bot_kind(telegram, entry, pane, result),
            desired_bot_kind,
        )
        entry["status_marker_hash"] = marker_hash
        entry["status_marker_text"] = sanitize_text(f"{title}\n{body}", 500)
        entry["status_marker_sent_at"] = utc_now()
    elif result.get("kind") == "bot_access":
        note_managed_bot_access_required(entry, "status_marker_bot_kind", desired_bot_kind)
    return {**result, "attempted": True}


def clear_status_marker_for_icon(chat_id: str, entry: dict[str, Any], *, api_token: str | None = None) -> bool:
    old_message_id = str(entry.get("status_marker_message_id") or "")
    if not old_message_id:
        return False
    if STATUS_MARKER_DELETE_OLD:
        try:
            if api_token:
                delete_message(chat_id, old_message_id, api_token=api_token)
            else:
                delete_message(chat_id, old_message_id)
        except Exception:
            entry["last_status_marker_delete_error"] = utc_now()
    for key in (
        "status_marker_message_id",
        "status_marker_hash",
        "status_marker_text",
        "status_marker_sent_at",
    ):
        entry.pop(key, None)
    entry["status_marker_cleared_for_icon_at"] = utc_now()
    return True


def create_topic(chat_id: str, name: str, *, icon_custom_emoji_id: str = "") -> str:
    payload: dict[str, Any] = {"chat_id": chat_id, "name": name}
    if icon_custom_emoji_id:
        payload["icon_custom_emoji_id"] = icon_custom_emoji_id
    elif HERDR_TOPIC_ICON_CUSTOM_EMOJI_ID:
        payload["icon_custom_emoji_id"] = HERDR_TOPIC_ICON_CUSTOM_EMOJI_ID
    else:
        payload["icon_color"] = str(HERDR_TOPIC_ICON_COLOR)
    result = telegram_api("createForumTopic", payload).get("result") or {}
    topic_id = result.get("message_thread_id")
    if topic_id is None:
        raise BridgeError("createForumTopic returned no message_thread_id")
    return str(topic_id)


def edit_topic(chat_id: str, topic_id: str | int, name: str, *, icon_custom_emoji_id: str | None = None) -> bool:
    payload: dict[str, Any] = {
        "chat_id": chat_id,
        "message_thread_id": str(topic_id),
        "name": name,
    }
    if icon_custom_emoji_id:
        payload["icon_custom_emoji_id"] = icon_custom_emoji_id
    return bool(telegram_api("editForumTopic", payload).get("result"))


def delete_topic(chat_id: str, topic_id: str | int) -> bool:
    payload = {"chat_id": chat_id, "message_thread_id": str(topic_id)}
    return bool(telegram_api("deleteForumTopic", payload).get("result"))


def delete_message(chat_id: str, message_id: str | int, *, api_token: str | None = None) -> bool:
    payload = {"chat_id": chat_id, "message_id": str(message_id)}
    return bool(telegram_api_for_token("deleteMessage", payload, api_token).get("result"))


def pin_chat_message(chat_id: str, message_id: str | int) -> dict[str, Any]:
    payload = {
        "chat_id": chat_id,
        "message_id": str(message_id),
        "disable_notification": "true",
    }
    try:
        response = telegram_api_for_token("pinChatMessage", payload, None)
    except RateLimited:
        raise
    except BridgeError as exc:
        text = str(exc).lower()
        if "already pinned" in text or "message is pinned" in text:
            return {"ok": True, "kind": "unchanged"}
        kind = classify_telegram_error(exc)
        return {"ok": False, "kind": kind, "transient": kind == "transient", "error": str(exc)}
    if not response.get("ok", True):
        description = str(response.get("description") or response.get("error") or response)
        text = description.lower()
        if "already pinned" in text or "message is pinned" in text:
            return {"ok": True, "kind": "unchanged"}
        return {"ok": False, "kind": "bad_request", "transient": False, "error": sanitize_text(description, 500)}
    return {"ok": True, "kind": "pinned"}


def space_pinned_status_hash(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def record_space_pinned_status(
    space_entry: dict[str, Any],
    *,
    message_id: str,
    text: str,
    text_hash: str,
    sent: bool,
) -> None:
    space_entry["pinned_status_message_id"] = message_id
    space_entry["pinned_status_text"] = text
    space_entry["pinned_status_hash"] = text_hash
    space_entry["pinned_status_updated_at"] = utc_now()
    if sent:
        space_entry["pinned_status_sent_at"] = utc_now()
    space_entry.pop("pinned_status_error", None)


def clear_space_pinned_status_message(space_entry: dict[str, Any], reason: str = "") -> None:
    for key in (
        "pinned_status_message_id",
        "pinned_status_text",
        "pinned_status_hash",
        "pinned_status_sent_at",
        "pinned_status_updated_at",
        "pinned_status_pinned_at",
    ):
        space_entry.pop(key, None)
    if reason:
        space_entry["pinned_status_error"] = sanitize_text(reason, 500)


def ensure_space_pinned_status(
    state: dict[str, Any],
    chat_id: str,
    space_entry: dict[str, Any],
    panes: list[dict[str, Any]],
    counters: dict[str, int],
    max_sends: int,
) -> dict[str, Any]:
    topic_id = str(space_entry.get("topic_id") or "")
    if not PINNED_STATUS_ENABLED or not topic_id:
        return {"changed": False, "updated": False, "skipped": True}
    text = render_pinned_status(state, panes)
    if not text:
        return {"changed": False, "updated": False, "skipped": True}
    text_hash = space_pinned_status_hash(text)
    message_id = str(space_entry.get("pinned_status_message_id") or "")
    if message_id and space_entry.get("pinned_status_hash") == text_hash:
        if space_entry.get("pinned_status_pinned_at"):
            return {"changed": False, "updated": False, "skipped": True, "reason": "unchanged"}
        pin_result = pin_chat_message(chat_id, message_id)
        if pin_result.get("ok"):
            space_entry["pinned_status_pinned_at"] = utc_now()
            space_entry.pop("pinned_status_pin_error", None)
            return {"changed": True, "updated": True, "pin": pin_result}
        space_entry["pinned_status_pin_error"] = sanitize_text(str(pin_result), 500)
        return {"changed": True, "updated": False, "pin": pin_result}

    if message_id:
        edit_result = edit_message_text(chat_id, message_id, text)
        if edit_result.get("ok"):
            record_space_pinned_status(
                space_entry,
                message_id=message_id,
                text=text,
                text_hash=text_hash,
                sent=False,
            )
            return {"changed": True, "updated": True, "edit": edit_result}
        if result_topic_missing(edit_result):
            return {"changed": False, "updated": False, "topic_missing": True, "error": str(edit_result)}
        if not edit_result.get("not_found"):
            space_entry["pinned_status_error"] = sanitize_text(str(edit_result), 500)
            return {"changed": True, "updated": False, "edit": edit_result}
        clear_space_pinned_status_message(space_entry, str(edit_result))

    if counters.get("sends", 0) >= max_sends:
        return {"changed": False, "updated": False, "skipped": True, "reason": "send_cap"}
    send_result = send_legacy_message_result(chat_id, text, thread_id=topic_id, notify=False)
    if result_topic_missing(send_result):
        return {"changed": False, "updated": False, "topic_missing": True, "error": str(send_result)}
    if not send_result.get("ok"):
        space_entry["pinned_status_error"] = sanitize_text(str(send_result), 500)
        return {"changed": True, "updated": False, "send": send_result}
    new_message_id = str(send_result.get("message_id") or "")
    if not new_message_id:
        space_entry["pinned_status_error"] = "Telegram returned no message id for pinned status"
        return {"changed": True, "updated": False, "send": send_result}
    counters["sends"] = counters.get("sends", 0) + 1
    record_space_pinned_status(
        space_entry,
        message_id=new_message_id,
        text=text,
        text_hash=text_hash,
        sent=True,
    )
    pin_result = pin_chat_message(chat_id, new_message_id)
    if pin_result.get("ok"):
        space_entry["pinned_status_pinned_at"] = utc_now()
        space_entry.pop("pinned_status_pin_error", None)
    else:
        space_entry["pinned_status_pin_error"] = sanitize_text(str(pin_result), 500)
    return {"changed": True, "updated": True, "send": send_result, "pin": pin_result}


def sync_space_pinned_statuses(
    state: dict[str, Any],
    chat_id: str,
    panes: list[dict[str, Any]],
    counters: dict[str, int],
    max_sends: int,
) -> dict[str, Any]:
    if not PINNED_STATUS_ENABLED:
        return {"changed": False, "updated": 0}
    spaces = state.get("spaces") if isinstance(state.get("spaces"), dict) else {}
    grouped = open_panes_by_space(panes)
    changed = False
    updated = 0
    for space_key_value, space_entry in spaces.items():
        if not isinstance(space_entry, dict):
            continue
        space_panes = grouped.get(str(space_key_value), [])
        if not space_panes and not space_entry.get("pinned_status_message_id"):
            continue
        result = ensure_space_pinned_status(state, chat_id, space_entry, space_panes, counters, max_sends)
        if result.get("topic_missing"):
            clear_space_topic_mapping(state, space_entry, str(result.get("error") or result))
            changed = True
            continue
        if result.get("changed"):
            changed = True
        if result.get("updated"):
            updated += 1
    return {"changed": changed, "updated": updated}


def observed_agent_panes() -> list[dict[str, Any]]:
    all_panes = pane_list()
    include_shells = parse_bool_env("HERDR_TELEGRAM_TOPICS_INCLUDE_SHELLS", "")
    return [pane for pane in all_panes if include_shells or pane.get("agent")]


def state_has_pane_id(state: dict[str, Any], pane_id: str) -> bool:
    panes = state.get("panes") if isinstance(state.get("panes"), dict) else {}
    for key, entry in panes.items():
        if str(key) == pane_id:
            return True
        if isinstance(entry, dict) and str(entry.get("pane_id") or "") == pane_id:
            return True
    return False


def sync_closed_pane_records(
    state: dict[str, Any],
    chat_id: str,
    telegram: dict[str, Any],
    panes: list[dict[str, Any]],
    *,
    sends: int,
    max_sends: int,
) -> dict[str, Any]:
    live_keys = {pane_key(pane) for pane in panes}
    state_panes = state.get("panes") if isinstance(state.get("panes"), dict) else {}
    changed = False
    sent = sends
    for key, entry in list(state_panes.items()):
        if key in live_keys or not isinstance(entry, dict):
            continue
        if remove_pane_from_space_memberships(state, str(key)):
            changed = True
        newly_closed = entry.get("last_known_status") != "closed"
        if newly_closed:
            entry["last_known_status"] = "closed"
            entry["closed_at"] = utc_now()
            changed = True
        topic_id = entry.get("topic_id")
        if topic_id and entry_uses_space_topic(state, entry) and not entry.get("closed_topic_finalized"):
            entry["closed_topic_finalized"] = True
            entry["closed_space_topic_preserved_at"] = utc_now()
            changed = True
        elif topic_id and not entry.get("closed_topic_finalized"):
            entry["closed_topic_finalized"] = True
            entry["closed_topic_preserved_at"] = utc_now()
            changed = True
        if newly_closed and topic_id and sent < max_sends and not closed_notice_already_sent(state, str(key), entry):
            closed_notice = send_notice(
                chat_id,
                "Closed by User",
                "",
                telegram=telegram,
                thread_id=topic_id,
                notify=True,
                reply_to_message_id=pane_root_reply_target(entry),
                api_token=managed_bot_token_for_entry(telegram, entry),
            )
            if closed_notice.get("ok") and closed_notice.get("message_id"):
                entry["closed_notice_message_id"] = str(closed_notice["message_id"])
                entry["closed_notice_sent_at"] = utc_now()
                record_pane_message_route(
                    state,
                    str(entry.get("space_key") or ""),
                    str(entry.get("pane_key") or key),
                    str(closed_notice["message_id"]),
                )
            sent += 1
    return {"changed": changed, "sent": sent}


def make_sync_counters(*, sends: int = 0) -> dict[str, int]:
    return {
        "sends": sends,
        "creates": 0,
        "verifies": 0,
        "renames": 0,
        "feed_sends": 0,
        "marker_sends": 0,
        "icon_updates": 0,
    }


def make_sync_caps(*, event: bool = False) -> dict[str, int]:
    if event:
        return {
            "max_sends": min(MAX_SENDS_PER_RUN, 2),
            "max_feed_sends": min(MAX_SENDS_PER_RUN, 2),
            "max_marker_sends": 1,
            "max_creates": min(MAX_CREATES_PER_RUN, 1),
            "max_verifies": min(MAX_TOPIC_VERIFIES_PER_RUN, 1),
        }
    return {
        "max_sends": MAX_SENDS_PER_RUN,
        "max_feed_sends": MAX_SENDS_PER_RUN,
        "max_marker_sends": MAX_STATUS_MARKERS_PER_RUN,
        "max_creates": MAX_CREATES_PER_RUN,
        "max_verifies": MAX_TOPIC_VERIFIES_PER_RUN,
    }


def reconcile_pinned_status_views(
    state: dict[str, Any],
    chat_id: str,
    panes: list[dict[str, Any]],
    counters: dict[str, int],
    max_sends: int,
) -> dict[str, bool | int]:
    result = sync_space_pinned_statuses(state, chat_id, panes, counters, max_sends)
    changed = bool(result.get("changed"))
    updated = int(result.get("updated") or 0)
    if pinned_status_enabled():
        overview = sync_pinned_status_overview(state, "", chat_id, panes)
        changed = changed or bool(overview.get("changed"))
    return {"changed": changed, "updated": updated}


def add_pinned_reconcile_result(
    changed: bool,
    updated: int,
    result: dict[str, bool | int],
) -> tuple[bool, int]:
    return changed or bool(result.get("changed")), updated + int(result.get("updated") or 0)


def record_plugin_event_seen(state: dict[str, Any], pane_id: str) -> None:
    state["last_plugin_event_at"] = utc_now()
    state["last_plugin_event_pane_id"] = pane_id


def reconcile_missing_event_pane(
    state: dict[str, Any],
    chat_id: str,
    telegram: dict[str, Any],
    pane_id: str,
) -> dict[str, Any]:
    state["last_plugin_event_unknown_pane_at"] = utc_now()
    state["last_plugin_event_unknown_pane_id"] = sanitize_text(pane_id, 120)
    if not state_has_pane_id(state, pane_id):
        save_state(state)
        return {"ok": True, "changed": False, "pane_id": pane_id, "message": "pane not found or not an agent"}

    preflight_ok, preflight_error = preflight_for_event(state, chat_id, telegram)
    if not preflight_ok:
        save_state(state)
        return {
            "ok": True,
            "changed": False,
            "pane_id": pane_id,
            "message": "telegram preflight failed",
            "error": preflight_error,
        }

    panes = observed_agent_panes()
    caps = make_sync_caps(event=True)
    closed_result = sync_closed_pane_records(
        state,
        chat_id,
        telegram,
        panes,
        sends=0,
        max_sends=caps["max_sends"],
    )
    counters = make_sync_counters(sends=int(closed_result.get("sent") or 0))
    pinned_result = reconcile_pinned_status_views(state, chat_id, panes, counters, caps["max_sends"])
    changed = bool(closed_result.get("changed") or pinned_result.get("changed"))
    record_plugin_event_seen(state, pane_id)
    save_state(state)
    return {
        "ok": True,
        "changed": changed,
        "pane_id": pane_id,
        "sent": counters["sends"],
        "pinned_status_updated": int(pinned_result.get("updated") or 0),
        "message": "pane not found; reconciled stored pane state",
    }


def ensure_manager_commands(telegram: dict[str, Any]) -> None:
    payload_commands = [{"command": c, "description": d} for c, d in MANAGER_BOT_COMMANDS]
    digest = hashlib.sha1(
        json.dumps(payload_commands, separators=(",", ":"), sort_keys=True).encode("utf-8")
    ).hexdigest()
    if telegram.get("manager_commands_digest") == digest:
        return
    try:
        telegram_api("setMyCommands", {"commands": json.dumps(payload_commands, separators=(",", ":"))})
    except Exception as exc:
        telegram["manager_commands_error"] = sanitize_text(str(exc), 300)
        return
    telegram["manager_commands_digest"] = digest
    telegram.pop("manager_commands_error", None)


def preflight(chat_id: str, telegram: dict[str, Any] | None = None) -> None:
    if not chat_id:
        raise BridgeError("HERDR_TELEGRAM_TOPICS_CHAT_ID is required")
    chat = telegram_api("getChat", {"chat_id": chat_id}).get("result") or {}
    if chat.get("type") != "supergroup" or not chat.get("is_forum"):
        raise BridgeError("Telegram chat must be a forum-enabled supergroup")
    me = telegram_api("getMe", {}).get("result") or {}
    if telegram is not None:
        username = str(me.get("username") or "").strip().lstrip("@")
        if username:
            telegram["bot_username"] = username
        if "can_manage_bots" in me:
            telegram["can_manage_bots"] = bool(me.get("can_manage_bots"))
        ensure_manager_commands(telegram)
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


def preflight_ok_within(telegram: dict[str, Any], seconds: int = PREFLIGHT_GRACE_SECONDS) -> bool:
    try:
        checked = _dt.datetime.fromisoformat(
            str(telegram.get("last_preflight_ok_at", "")).replace("Z", "+00:00")
        )
    except Exception:
        return False
    return (_dt.datetime.now(tz=_dt.timezone.utc) - checked).total_seconds() < seconds


def is_transient_telegram_error(error_text: str) -> bool:
    low = str(error_text or "").lower()
    markers = (
        "urlopen error",
        "timed out",
        "timeout",
        "temporary failure",
        "connection reset",
        "connection aborted",
        "connection refused",
        "network is unreachable",
        "name or service not known",
        "unexpected_eof_while_reading",
        "eof occurred in violation of protocol",
        "ssl:",
    )
    return any(marker in low for marker in markers)


def preflight_alert_text(error_text: str) -> str:
    base = "Herdr topic sync preflight could not verify Telegram access."
    if is_transient_telegram_error(error_text):
        return (
            f"{base}\n"
            f"Reason: {error_text}\n\n"
            "This looks like a transient Telegram network/TLS failure, not a bot permission problem. "
            "Sync will continue if a recent permission check succeeded."
        )
    return (
        "Herdr topic sync is blocked before topic creation.\n"
        f"Reason: {error_text}\n\n"
        "Grant the bot admin permission to manage topics in the Telegram forum group, then run the sync again."
    )


def configure_telegram_state(state: dict[str, Any]) -> tuple[dict[str, Any], str]:
    telegram = state.setdefault("telegram", {})
    chat_id = str(telegram.get("chat_id") or os.getenv("HERDR_TELEGRAM_TOPICS_CHAT_ID") or DEFAULT_CHAT_ID)
    telegram["chat_id"] = chat_id
    telegram.setdefault("general_thread_id", os.getenv("HERDR_TELEGRAM_TOPICS_GENERAL_THREAD_ID", DEFAULT_GENERAL_THREAD_ID))
    telegram.setdefault(
        "owner_user_ids",
        [p.strip() for p in os.getenv("TELEGRAM_ALLOWED_USERS", DEFAULT_OWNER_ID).split(",") if p.strip()],
    )
    telegram.setdefault("implicit_send_enabled", False)
    telegram.setdefault("managed_bots", {})
    if sync_env_managed_bot_tokens(telegram):
        telegram["managed_bot_env_sync_at"] = utc_now()
    return telegram, chat_id


def preflight_for_event(state: dict[str, Any], chat_id: str, telegram: dict[str, Any]) -> tuple[bool, str]:
    try:
        if not preflight_is_fresh(telegram):
            preflight(chat_id, telegram)
            telegram["last_preflight_ok_at"] = utc_now()
        telegram.pop("last_preflight_error", None)
        telegram.pop("last_event_preflight_error", None)
        return True, ""
    except Exception as exc:
        error_text = sanitize_text(str(exc), 500)
        if is_transient_telegram_error(error_text) and preflight_ok_within(telegram):
            telegram["last_preflight_warning"] = error_text
            telegram["last_preflight_warning_at"] = utc_now()
            return True, error_text
        telegram["last_event_preflight_error"] = error_text
        telegram["last_event_preflight_error_at"] = utc_now()
        return False, error_text


def duplicate_match_score(left: dict[str, Any], right: dict[str, Any]) -> int:
    score = 0
    left_session = str(left.get("agent_session_id") or "")
    right_session = str(right.get("agent_session_id") or "")
    if left_session and left_session == right_session:
        score += 100
    left_alias = entry_pane_alias(left)
    right_alias = entry_pane_alias(right)
    if left_alias and left_alias == right_alias:
        score += 70
    if str(left.get("workspace") or "") and str(left.get("workspace") or "") == str(right.get("workspace") or ""):
        score += 10
    left_name = str(left.get("pane_label_topic_name") or left.get("topic_name") or "").lower()
    right_name = str(right.get("pane_label_topic_name") or right.get("topic_name") or "").lower()
    if left_name and left_name == right_name:
        score += 20
    return score


def find_reusable_closed_entry(panes: dict[str, Any], current_key: str, pane: dict[str, Any]) -> tuple[str, dict[str, Any]] | None:
    candidate = {
        "pane_id": str(pane.get("pane_id") or ""),
        "terminal_id": str(pane.get("terminal_id") or ""),
        "agent_session_id": pane_agent_session_id(pane),
        "workspace": str(pane.get("workspace_id") or ""),
        "tab": str(pane.get("tab_id") or ""),
        "pane_label_topic_name": topic_name_from_pane_label(pane_manual_label(pane)) if pane_manual_label(pane) else "",
    }
    matches: list[tuple[int, str, dict[str, Any]]] = []
    for key, entry in panes.items():
        if key == current_key or not isinstance(entry, dict):
            continue
        if str(entry.get("last_known_status") or "").lower() != "closed":
            continue
        if not entry.get("topic_id"):
            continue
        score = duplicate_match_score(entry, candidate)
        if score >= 90:
            matches.append((score, key, entry))
    if not matches:
        return None
    matches.sort(key=lambda item: (item[0], str(item[2].get("last_seen_at") or "")), reverse=True)
    _score, key, entry = matches[0]
    return key, entry


def ensure_pane_entry(state: dict[str, Any], pane: dict[str, Any]) -> tuple[str, dict[str, Any], bool]:
    key = pane_key(pane)
    panes = state.setdefault("panes", {})
    entry = panes.get(key)
    created = False
    if not isinstance(entry, dict):
        reusable = find_reusable_closed_entry(panes, key, pane)
        if reusable:
            old_key, entry = reusable
            panes.pop(old_key, None)
            entry["pane_key"] = key
            entry["reused_from_pane_key"] = old_key
            entry["reused_topic_mapping_at"] = utc_now()
            entry.pop("closed_at", None)
            entry.pop("closed_topic_finalized", None)
            entry.pop("status_icon_key", None)
            entry.pop("topic_status_icon_key", None)
            entry.pop("topic_status_icon_emoji", None)
            entry.pop("topic_status_icon_custom_emoji_id", None)
            entry.pop("topic_status_icon_updated_at", None)
            panes[key] = entry
            created = True
        else:
            entry = {"pane_key": key, "created_at": utc_now()}
            panes[key] = entry
            created = True
        created = True
    entry.update({
        "pane_id": str(pane.get("pane_id") or ""),
        "terminal_id": str(pane.get("terminal_id") or ""),
        "agent_session_id": pane_agent_session_id(pane),
        "agent": str(pane.get("agent") or ""),
        "workspace": str(pane.get("workspace_id") or ""),
        "tab": str(pane.get("tab_id") or ""),
        # Persist the cwd so migrate_legacy_pane_topics() can reconstruct a
        # faithful pane_like; per-agent topic names ("<agent> · <folder>") are
        # derived from it and would degrade to a bare "<agent>" without it.
        "foreground_cwd": str(pane.get("foreground_cwd") or pane.get("cwd") or ""),
    })
    manual_label = pane_manual_label(pane)
    previous_label = str(entry.get("pane_label_raw") or "")
    previous_label_topic_name = str(entry.get("pane_label_topic_name") or "")
    if manual_label:
        label_topic_name = topic_name_from_pane_label(manual_label)
        entry["pane_label_raw"] = manual_label
        entry["pane_label_topic_name"] = label_topic_name
        if not previous_label:
            entry.setdefault("pane_label_baselined_at", utc_now())
        elif previous_label != manual_label or previous_label_topic_name != label_topic_name:
            entry["pane_label_changed_at"] = utc_now()
    else:
        entry.pop("pane_label_topic_name", None)
        if previous_label:
            entry["pane_label_raw"] = ""
            entry["pane_label_cleared_at"] = utc_now()
    key_for_space, space_entry, _space_changed = ensure_space_entry(state, pane)
    entry["space_key"] = key_for_space
    entry["pane_thread_name"] = pane_thread_name(pane)
    if entry.get("topic_id") and not space_entry.get("topic_id"):
        legacy_topic_name = str(entry.get("topic_name") or "")
        space_topic_name = str(space_entry.get("topic_name") or space_name_for_pane(pane))
        space_entry["topic_id"] = str(entry.get("topic_id") or "")
        for verify_key in ("last_topic_verified_at", "last_topic_verify_attempt_at"):
            verify_value = entry.get(verify_key)
            if verify_value and not space_entry.get(verify_key):
                space_entry[verify_key] = verify_value
        space_entry.setdefault("topic_name", space_topic_name)
        if legacy_topic_name and legacy_topic_name != space_topic_name:
            space_entry["topic_rename_pending_at"] = utc_now()
            space_entry["topic_rename_from"] = legacy_topic_name
            space_entry["topic_rename_to"] = space_topic_name
            space_entry.pop("last_topic_verified_at", None)
    if space_entry.get("topic_id"):
        entry["topic_id"] = str(space_entry.get("topic_id") or "")
    space_topic_name = str(space_entry.get("topic_name") or space_name_for_pane(pane))
    current_topic_name = str(entry.get("topic_name") or "")
    if current_topic_name and current_topic_name != space_topic_name:
        entry["legacy_topic_name"] = current_topic_name
    if space_topic_name:
        entry["topic_name"] = space_topic_name
        entry["topic_title_source"] = "space"
    for rename_key in ("topic_rename_pending_at", "topic_rename_from", "topic_rename_to"):
        entry.pop(rename_key, None)
    return key, entry, created


def pane_root_message_text(pane: dict[str, Any], entry: dict[str, Any]) -> str:
    title = str(entry.get("pane_thread_name") or pane_thread_name(pane))
    pane_id = str(pane.get("pane_id") or entry.get("pane_id") or "")
    agent = str(pane.get("agent") or "")
    cwd = compact_path(pane.get("cwd") or pane.get("foreground_cwd") or "")
    lines = [
        f"Pane thread: {title}",
        f"Pane key: {entry.get('pane_key') or pane_key(pane)}",
    ]
    if pane_id:
        lines.append(f"Pane id: {pane_id}")
    if agent:
        lines.append(f"Agent: {agent}")
    if cwd:
        lines.append(f"Cwd: {cwd}")
    lines.append("")
    lines.append(format_status(pane, include_commands=True))
    return "\n".join(lines).strip()


def pane_root_message_html(pane: dict[str, Any], entry: dict[str, Any]) -> str:
    title = html.escape(str(entry.get("pane_thread_name") or pane_thread_name(pane)))
    body = html.escape(pane_root_message_text(pane, entry)).replace("\n", "<br>")
    return f"<b>{title}</b><br>{body}"


def send_pane_root_message(
    chat_id: str,
    telegram: dict[str, Any],
    pane: dict[str, Any],
    entry: dict[str, Any],
    thread_id: str | int,
) -> dict[str, Any]:
    return send_rich_message(
        chat_id,
        pane_root_message_html(pane, entry),
        telegram=telegram,
        fallback_text=pane_root_message_text(pane, entry),
        thread_id=thread_id,
        notify=False,
        api_token=managed_bot_token_for_entry(telegram, entry, pane),
    )


def ensure_space_topic(
    state: dict[str, Any],
    chat_id: str,
    telegram: dict[str, Any],
    pane: dict[str, Any],
    counters: dict[str, int],
    max_creates: int,
) -> tuple[dict[str, Any], bool]:
    key_for_space, space_entry, space_changed = ensure_space_entry(state, pane)
    if space_entry.get("topic_id"):
        return space_entry, space_changed
    if counters.get("creates", 0) >= max_creates:
        return space_entry, space_changed
    topic_name = str(space_entry.get("topic_name") or space_name_for_pane(pane))
    topic_id = create_topic(chat_id, topic_name)
    counters["creates"] = counters.get("creates", 0) + 1
    space_entry["space_key"] = key_for_space
    space_entry["topic_id"] = topic_id
    space_entry["topic_name"] = topic_name
    space_entry["last_topic_verified_at"] = utc_now()
    space_entry.pop("topic_missing_at", None)
    space_entry.pop("topic_missing_id", None)
    space_entry.pop("topic_missing_reason", None)
    # The "which agents work here?" onboarding card only makes sense in per-space
    # grouping, where one topic holds several agents. In per-agent mode each topic
    # IS a single agent, so the question is moot — skip the card entirely.
    if not per_agent_topics_enabled():
        live = live_entries_for_space(state, space_entry)
        detected_kinds = []
        seen_kinds: set[str] = set()
        for _pk, live_entry in live:
            k = managed_bot_kind_for_entry(live_entry)
            if k and k not in seen_kinds:
                seen_kinds.add(k)
                detected_kinds.append(k)
        pane_kind = managed_bot_kind_for_entry(pane if isinstance(pane, dict) else {}, pane)
        if pane_kind and pane_kind not in seen_kinds:
            detected_kinds.append(pane_kind)
        space_entry["onboarding_selected"] = detected_kinds
        space_entry["onboarding_status"] = "pending"
        space_token = _callback_id(key_for_space, "space")[:16]
        all_kinds = list(managed_bot_specs().keys())
        try:
            ob_message_id = send_message(
                chat_id,
                "Which agents work in this space? Tap to toggle, then Done. "
                "(If you skip this, the detected agents are kept.)",
                thread_id=topic_id,
                reply_markup=onboarding_reply_markup(space_token, all_kinds, detected_kinds),
            )
            if ob_message_id:
                space_entry["onboarding_message_id"] = str(ob_message_id)
        except Exception as exc:
            space_entry["onboarding_error"] = sanitize_text(str(exc), 300)
    save_state(state)
    return space_entry, True


def ensure_pane_root_message(
    state: dict[str, Any],
    chat_id: str,
    telegram: dict[str, Any],
    pane: dict[str, Any],
    entry: dict[str, Any],
    counters: dict[str, int],
    max_sends: int,
) -> tuple[bool, dict[str, Any]]:
    topic_id = str(entry.get("topic_id") or "")
    desired_bot_kind = desired_message_bot_kind(telegram, entry, pane)
    if not topic_id:
        return False, {"ok": True, "skipped": True}
    if not PANE_ROOT_MESSAGES_ENABLED:
        return False, {"ok": True, "skipped": True, "reason": "pane_root_messages_disabled"}
    if managed_bot_access_retry_waiting(entry, "pane_root_bot_kind", desired_bot_kind):
        return False, {"ok": True, "skipped": True, "reason": "managed_bot_access_pending"}
    if entry.get("pane_root_message_id") and not message_bot_reissue_due(
        entry,
        "pane_root_bot_kind",
        desired_bot_kind,
    ):
        return False, {"ok": True, "skipped": True}
    if counters.get("sends", 0) >= max_sends:
        return False, {"ok": False, "skipped": True, "reason": "send_cap"}
    result = send_pane_root_message(chat_id, telegram, pane, entry, topic_id)
    if result.get("ok"):
        counters["sends"] = counters.get("sends", 0) + 1
        if result.get("message_id"):
            entry["pane_root_message_id"] = str(result["message_id"])
            record_pane_message_route(
                state,
                str(entry.get("space_key") or ""),
                str(entry.get("pane_key") or ""),
                str(result["message_id"]),
            )
        record_message_bot_kind(
            entry,
            "pane_root_bot_kind",
            sent_message_bot_kind(telegram, entry, pane, result),
            desired_bot_kind,
        )
        entry["pane_root_message_sent_at"] = utc_now()
        entry.pop("pane_root_message_error", None)
        save_state(state)
        return True, result
    if result.get("kind") == "bot_access":
        note_managed_bot_access_required(entry, "pane_root_bot_kind", desired_bot_kind)
    entry["pane_root_message_error"] = sanitize_text(str(result), 500)
    save_state(state)
    return True, result


def record_pane_message_route(state: dict[str, Any], space_key_value: str, pane_key_value: str, message_id: str | int) -> bool:
    space = state.setdefault("spaces", {}).get(space_key_value)
    if not isinstance(space, dict):
        return False
    message_key = str(message_id or "").strip()
    if not message_key:
        return False
    # Topic-scoped high-water mark: the newest message id seen in this space's shared
    # topic, across ALL panes. Unlike last_pane_message_id this is not per-pane, so a
    # sibling pane posting can be detected as having buried another pane's turn anchor.
    # Advanced independent of the pane entry — a closed/removed sibling's message still
    # buries. (Inbound owner messages do NOT pass through here — they only carry the
    # bot's own outbound ids — so command_reply advances the mark for those separately.)
    note_topic_high_water_mark(space, message_key)
    panes = state.setdefault("panes", {})
    entry = panes.get(pane_key_value)
    if not isinstance(entry, dict):
        return False
    routes = space.get("message_routes")
    if not isinstance(routes, dict):
        routes = {}
        space["message_routes"] = routes
    routes[message_key] = pane_key_value
    current_latest = str(entry.get("last_pane_message_id") or "")
    if not current_latest or message_id_index(message_key) >= message_id_index(current_latest):
        entry["last_pane_message_id"] = message_key
        entry["last_pane_message_at"] = utc_now()
    while len(routes) > 100:
        oldest = next(iter(routes))
        routes.pop(oldest, None)
    return True


def message_id_index(message_id: str | int) -> int:
    try:
        return int(str(message_id or "").strip())
    except (TypeError, ValueError):
        return -1


def pane_message_is_latest(entry: dict[str, Any], message_id: str | int) -> bool:
    message_key = str(message_id or "").strip()
    if not message_key:
        return False
    latest = str(entry.get("last_pane_message_id") or "").strip()
    return not latest or latest == message_key


def topic_message_is_latest(space: dict[str, Any] | None, message_id: str | int) -> bool:
    """Is `message_id` still the newest message in the space's shared topic?

    In a shared topic (per_agent voice, or any topic grouping several panes) a turn's
    edit-in-place anchor is only safe to edit when nothing — another pane's message or
    an inbound owner message — has been posted after it; otherwise Telegram keeps the
    edited message at its original (now buried) position. When the space is unknown we
    degrade to True so callers fall back to the pane-scoped check (no regression for
    single-pane topics or pre-existing state without a high-water mark)."""
    if not isinstance(space, dict):
        return True
    message_key = str(message_id or "").strip()
    if not message_key:
        return False
    latest = str(space.get("last_topic_message_id") or "").strip()
    return not latest or message_id_index(message_key) >= message_id_index(latest)


def note_topic_high_water_mark(space: dict[str, Any] | None, message_id: str | int) -> bool:
    """Advance the space's shared-topic high-water mark (the newest message id seen in the
    topic) if `message_id` is newer than the current mark. Fed by both outbound pane
    messages (record_pane_message_route) and inbound owner messages (command_reply) so the
    turn-finalize gate can detect when an in-place anchor has been buried. Returns True if
    the mark advanced (so callers can decide whether to persist state)."""
    if not isinstance(space, dict):
        return False
    message_key = str(message_id or "").strip()
    if not message_key:
        return False
    latest = str(space.get("last_topic_message_id") or "").strip()
    if latest and message_id_index(message_key) <= message_id_index(latest):
        return False
    space["last_topic_message_id"] = message_key
    return True


def turn_visible_anchor_message_id(entry: dict[str, Any], turn_id: str) -> str:
    clean_turn_id = str(turn_id or "")
    if not clean_turn_id:
        return ""
    stream_message_id = str(entry.get("last_stream_message_id") or "")
    stream_turn_id = str(entry.get("last_stream_turn_id") or "")
    if stream_message_id and stream_turn_id == clean_turn_id and pane_message_is_latest(entry, stream_message_id):
        return stream_message_id
    prompt_message_id = str(entry.get("last_prompt_message_id") or "")
    prompt_turn_id = str(entry.get("last_prompt_turn_id") or "")
    if prompt_message_id and prompt_turn_id == clean_turn_id and pane_message_is_latest(entry, prompt_message_id):
        return prompt_message_id
    return ""


def route_message_to_pane(
    state: dict[str, Any],
    chat_id: str,
    topic_id: str | int,
    message_id: str | int,
) -> tuple[str, dict[str, Any]] | None:
    telegram = state.get("telegram") if isinstance(state.get("telegram"), dict) else {}
    configured_chat = str(telegram.get("chat_id") or "")
    if configured_chat and str(chat_id) != configured_chat:
        return None
    message_key = str(message_id or "").strip()
    if not message_key:
        return None
    panes = state.get("panes") if isinstance(state.get("panes"), dict) else {}
    spaces = state.get("spaces") if isinstance(state.get("spaces"), dict) else {}
    for space in spaces.values():
        if not isinstance(space, dict) or str(space.get("topic_id") or "") != str(topic_id):
            continue
        routes = space.get("message_routes") if isinstance(space.get("message_routes"), dict) else {}
        routed_key = str(routes.get(message_key) or "")
        routed_entry = panes.get(routed_key)
        if routed_key and isinstance(routed_entry, dict):
            return routed_key, routed_entry
        for pane_key_value in space.get("pane_keys") or []:
            entry = panes.get(str(pane_key_value))
            if isinstance(entry, dict) and str(entry.get("pane_root_message_id") or "") == message_key:
                return str(pane_key_value), entry
    return None


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


def send_pending_prompt_message(
    state: dict[str, Any],
    chat_id: str,
    telegram: dict[str, Any],
    entry: dict[str, Any],
    counters: dict[str, int],
    max_sends: int,
    max_feed_sends: int,
) -> dict[str, Any]:
    turn_id = str(entry.get("pending_prompt_turn_id") or "")
    prompt_text = sanitize_text(str(entry.get("pending_prompt_text") or ""), USER_PROMPT_MAX_CHARS).strip()
    if not turn_id or not prompt_text:
        return {"changed": False, "topic_missing": False}
    prompt_hash = str(entry.get("pending_prompt_hash") or stream_text_hash(prompt_text))
    ensure_request_started(entry, turn_id)
    last_prompt_hash = str(entry.get("last_prompt_hash") or "")
    if str(entry.get("last_prompt_turn_id") or "") == turn_id and (
        not last_prompt_hash or last_prompt_hash == prompt_hash
    ):
        entry.pop("pending_prompt_turn_id", None)
        entry.pop("pending_prompt_text", None)
        entry.pop("pending_prompt_hash", None)
        return {"changed": True, "topic_missing": False}
    if counters.get("sends", 0) >= max_sends or counters.get("feed_sends", 0) >= max_feed_sends:
        return {"changed": False, "topic_missing": False, "skipped": True}
    desired_bot_kind = desired_message_bot_kind(telegram, entry)
    if managed_bot_access_retry_waiting(entry, "last_prompt_bot_kind", desired_bot_kind):
        return {
            "changed": False,
            "topic_missing": False,
            "skipped": True,
            "reason": "managed_bot_access_pending",
        }

    item = make_prompt_feed_item(turn_id, prompt_text)
    result = send_feed_item(
        chat_id,
        item,
        telegram=telegram,
        thread_id=entry["topic_id"],
        notify=False,
        reply_to_message_id=pane_root_reply_target(entry),
        api_token=managed_bot_token_for_entry(telegram, entry),
    )
    if result_topic_missing(result):
        return {"changed": False, "topic_missing": True, "error": str(result.get("error") or result)}
    if result_pane_root_missing(result):
        return {
            "changed": False,
            "topic_missing": False,
            "pane_root_missing": True,
            "error": str(result.get("error") or result),
        }
    if result.get("ok"):
        counters["sends"] = counters.get("sends", 0) + 1
        counters["feed_sends"] = counters.get("feed_sends", 0) + 1
        entry["last_prompt_turn_id"] = turn_id
        entry["last_prompt_hash"] = prompt_hash
        entry["last_prompt_text"] = prompt_text
        entry["last_prompt_sent_at"] = utc_now()
        if result.get("message_id"):
            entry["last_prompt_message_id"] = str(result["message_id"])
            record_pane_message_route(
                state,
                str(entry.get("space_key") or ""),
                str(entry.get("pane_key") or ""),
                str(result["message_id"]),
            )
        record_message_bot_kind(
            entry,
            "last_prompt_bot_kind",
            sent_message_bot_kind(telegram, entry, None, result),
            desired_bot_kind,
        )
        clear_delivered_stream_state(entry)
        entry.pop("pending_prompt_turn_id", None)
        entry.pop("pending_prompt_text", None)
        entry.pop("pending_prompt_hash", None)
        entry.pop("last_prompt_error", None)
        return {"changed": True, "topic_missing": False, "message_id": result.get("message_id")}

    if result.get("kind") == "bot_access":
        note_managed_bot_access_required(entry, "last_prompt_bot_kind", desired_bot_kind)
    entry["last_prompt_error"] = sanitize_text(str(result), 500)
    entry["last_prompt_error_at"] = utc_now()
    return {"changed": True, "topic_missing": False}


def entry_uses_space_topic(state: dict[str, Any], entry: dict[str, Any]) -> bool:
    space_key_value = str(entry.get("space_key") or "")
    topic_id = str(entry.get("topic_id") or "")
    if not space_key_value or not topic_id:
        return False
    spaces = state.get("spaces") if isinstance(state.get("spaces"), dict) else {}
    space = spaces.get(space_key_value)
    return isinstance(space, dict) and str(space.get("topic_id") or "") == topic_id


def remove_pane_from_space_memberships(state: dict[str, Any], pane_key_value: str) -> bool:
    changed = False
    spaces = state.get("spaces") if isinstance(state.get("spaces"), dict) else {}
    for space in spaces.values():
        if not isinstance(space, dict):
            continue
        pane_keys = space.get("pane_keys")
        if not isinstance(pane_keys, list) or pane_key_value not in pane_keys:
            continue
        space["pane_keys"] = [str(key) for key in pane_keys if str(key) != pane_key_value]
        changed = True
    return changed


def closed_notice_identity(entry: dict[str, Any], fallback_key: str) -> str:
    for field in ("agent_session_id", "pane_id", "terminal_id"):
        value = str(entry.get(field) or "").strip()
        if value:
            return f"{field}:{value}"
    return f"pane_key:{fallback_key}"


def closed_notice_already_sent(state: dict[str, Any], current_key: str, entry: dict[str, Any]) -> bool:
    if entry.get("closed_notice_sent_at"):
        return True
    identity = closed_notice_identity(entry, current_key)
    panes = state.get("panes") if isinstance(state.get("panes"), dict) else {}
    for key, other in panes.items():
        if str(key) == str(current_key) or not isinstance(other, dict):
            continue
        if not other.get("closed_notice_sent_at"):
            continue
        if closed_notice_identity(other, str(key)) == identity:
            return True
    return False


def _sync_pane_clean_feed(
    state: dict[str, Any],
    chat_id: str,
    telegram: dict[str, Any],
    pane: dict[str, Any],
    entry: dict[str, Any],
    counters: dict[str, int],
    *,
    pane_api_token: str | None,
    turn_only: bool,
    new_entry: bool,
    max_sends: int,
    max_feed_sends: int,
    stable_obj_hash: str,
    changed: bool,
) -> dict[str, Any]:
    """Clean-feed delivery for one pane: turn feed, pending prompt, stream,
    and clean-feed item delivery.

    Extracted from sync_pane_once. Mutates state/entry/counters in place.
    Returns {"early_return", "changed", "feed_delivered", "stream_active"}.
    early_return is True when the topic or pane-root went missing (caller
    must return True immediately); None means continue.
    """
    feed_delivered_this_pane = False
    stream_active_this_pane = False
    item = None
    if TURN_FEED_ENABLED:
        before_turn_state = (
            entry.get("last_turn_available"),
            entry.get("last_turn_reason"),
            entry.get("last_turn_id"),
        )
        # The plugin/event path (turn_only) must never scrape the visible
        # screen: a status-change event can fire before the turn is flushed,
        # and scraping a transiently-done/idle pane sends a malformed blob and
        # breaks the settle loop. Visible prompts surface on the timer path.
        item = extract_turn_feed_item(pane, entry, allow_visible_fallback=not turn_only)
        if isinstance(item, dict):
            # Carry the per-space prompt-collapse setting (resolved onto the entry in
            # sync_pane_once) onto the feed item so the renderer collapses the echoed
            # prompt accordingly.
            item["prompt_collapse_chars"] = int(entry.get("prompt_collapse_chars") or 0)
        after_turn_state = (
            entry.get("last_turn_available"),
            entry.get("last_turn_reason"),
            entry.get("last_turn_id"),
        )
        if before_turn_state != after_turn_state:
            changed = True
    elif not turn_only and str(pane.get("agent_status") or "").strip().lower() not in ACTIVE_AGENT_STATUSES:
        raw = pane_feed_output(str(pane.get("pane_id") or ""))
        bounded_report = extract_bounded_report_from_raw(raw)
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

    duplicate_owner_key = duplicate_native_session_owner_key(state, entry)
    if duplicate_owner_key:
        if suppress_duplicate_native_session_item(entry, item, duplicate_owner_key):
            item = None
            changed = True
        if suppress_duplicate_native_session_prompt(entry, duplicate_owner_key):
            changed = True
        if suppress_duplicate_native_session_stream(entry, duplicate_owner_key):
            changed = True
    pending_stream_text = str(entry.get("pending_stream_text") or "")
    pending_stream_turn_id = str(entry.get("pending_stream_turn_id") or "")
    pending_stream_user_text = stream_user_text_for_turn(entry, pending_stream_turn_id)
    prompt_result = send_pending_prompt_message(
        state,
        chat_id,
        telegram,
        entry,
        counters,
        max_sends,
        max_feed_sends,
    )
    if prompt_result.get("topic_missing"):
        clear_entry_topic_mapping(state, entry, str(prompt_result.get("error") or prompt_result))
        save_state(state)
        return {"early_return": True, "changed": changed, "feed_delivered": feed_delivered_this_pane, "stream_active": stream_active_this_pane}
    if prompt_result.get("pane_root_missing"):
        clear_pane_root_message(state, entry, str(prompt_result.get("error") or prompt_result))
        save_state(state)
        return {"early_return": True, "changed": changed, "feed_delivered": feed_delivered_this_pane, "stream_active": stream_active_this_pane}
    if prompt_result.get("changed"):
        changed = True
    if not item and pending_stream_text and pending_stream_turn_id and stream_config_enabled():
        stream_result = send_stream_update(
            chat_id,
            telegram,
            entry,
            turn_id=pending_stream_turn_id,
            text=pending_stream_text,
            user_text=pending_stream_user_text,
        )
        if result_topic_missing(stream_result):
            clear_entry_topic_mapping(state, entry, str(stream_result.get("error") or stream_result))
            save_state(state)
            return {"early_return": True, "changed": changed, "feed_delivered": feed_delivered_this_pane, "stream_active": stream_active_this_pane}
        if result_pane_root_missing(stream_result):
            clear_pane_root_message(state, entry, str(stream_result.get("error") or stream_result))
            save_state(state)
            return {"early_return": True, "changed": changed, "feed_delivered": feed_delivered_this_pane, "stream_active": stream_active_this_pane}
        if stream_result.get("ok"):
            stream_active_this_pane = True
            if stream_result.get("sent_message"):
                counters["sends"] = counters.get("sends", 0) + 1
                if stream_result.get("message_id"):
                    record_pane_message_route(
                        state,
                        str(entry.get("space_key") or ""),
                        str(entry.get("pane_key") or ""),
                        str(stream_result["message_id"]),
                    )
            changed = True
        elif not stream_result.get("skipped"):
            entry["last_stream_error"] = sanitize_text(str(stream_result), 500)
            changed = True

    old_clean_has_noise = feed_text_has_ui_noise(str(entry.get("last_clean_text") or ""))
    if should_baseline_new_pane_turn(entry, item, new_entry):
        record_suppressed_clean_item(entry, item, "new_pane_initial_turn_baseline")
        item = None
        changed = True
    if item:
        item_render_hash = clean_feed_hash(item)
        item_semantic_hash = clean_feed_hash(item, include_render_version=False)
        previous_render_hash = str(entry.get("last_clean_render_hash") or entry.get("last_clean_hash") or "")
        same_semantic = same_delivered_content(entry, item, item_semantic_hash)
        render_changed = item_render_hash != previous_render_hash
        content_changed = not same_semantic
        should_deliver = old_clean_has_noise or content_changed or render_changed
        desired_bot_kind = desired_message_bot_kind(telegram, entry)
        message_id = str(entry.get("last_clean_message_id") or "")
        if same_semantic and render_changed and not content_changed and not old_clean_has_noise and not message_id:
            entry["last_clean_render_hash"] = item_render_hash
            entry["last_clean_hash"] = item_render_hash
            entry["last_clean_semantic_hash"] = item_semantic_hash
            entry["last_clean_text"] = item_plain_text(item)
            entry["last_clean_item"] = item
            entry["last_clean_render_only_skipped_at"] = utc_now()
            entry.pop("last_clean_send_error", None)
            changed = True
            should_deliver = False
        if (
            counters.get("feed_sends", 0) < max_feed_sends
            and should_deliver
            and not recent_attempt(entry, item_render_hash)
            and not managed_bot_access_retry_waiting(entry, "last_clean_bot_kind", desired_bot_kind)
        ):
            reply_markup, pending_active_prompt, clear_active_prompt = prompt_delivery_state(item)
            entry["last_clean_attempt_hash"] = item_render_hash
            entry["last_clean_attempt_at"] = utc_now()
            changed = True
            did_edit = False
            lifecycle_message_id = ""
            if str(item.get("kind") or "").lower() == "turn":
                lifecycle_message_id = turn_visible_anchor_message_id(entry, str(item.get("turn_id") or ""))
                if lifecycle_message_id:
                    space_entry = (state.get("spaces") or {}).get(str(entry.get("space_key") or ""))
                    if not topic_message_is_latest(space_entry, lifecycle_message_id):
                        # A sibling pane (or the owner) posted to this shared topic after the
                        # turn's anchor was placed. Editing it in place would leave the final
                        # completion buried mid-feed where the owner never sees it (the actual
                        # bug: a long codex turn finalized into a 17-min-old message that the
                        # newly-added Devin pane's messages had since buried). Drop the anchor
                        # so the turn is sent as a fresh message at the bottom of the topic.
                        lifecycle_message_id = ""
            if lifecycle_message_id and not entry.get("last_clean_bot_kind"):
                if lifecycle_message_id == str(entry.get("last_stream_message_id") or "") and entry.get("last_stream_bot_kind"):
                    entry["last_clean_bot_kind"] = str(entry.get("last_stream_bot_kind") or "")
                elif lifecycle_message_id == str(entry.get("last_prompt_message_id") or "") and entry.get("last_prompt_bot_kind"):
                    entry["last_clean_bot_kind"] = str(entry.get("last_prompt_bot_kind") or "")
            if (
                lifecycle_message_id
                and not message_bot_reissue_due(entry, "last_clean_bot_kind", desired_bot_kind)
            ):
                result = edit_feed_item(
                    chat_id,
                    lifecycle_message_id,
                    item,
                    telegram=telegram,
                    reply_markup=reply_markup,
                    api_token=pane_api_token,
                )
                if result.get("ok"):
                    did_edit = True
                    message_id = lifecycle_message_id
                elif result.get("not_found"):
                    result = send_feed_item(
                        chat_id,
                        item,
                        telegram=telegram,
                        thread_id=entry["topic_id"],
                        notify=bool(item.get("notify")),
                        reply_markup=reply_markup,
                        reply_to_message_id=pane_root_reply_target(entry),
                        api_token=pane_api_token,
                    )
                else:
                    result = {**result, "edit_failed": True}
            elif same_semantic and (render_changed or old_clean_has_noise) and message_id:
                result = edit_feed_item(
                    chat_id,
                    message_id,
                    item,
                    telegram=telegram,
                    reply_markup=reply_markup,
                    api_token=pane_api_token,
                )
                if result.get("ok"):
                    did_edit = True
                elif result.get("not_found"):
                    if content_changed:
                        result = send_feed_item(
                            chat_id,
                            item,
                            telegram=telegram,
                            thread_id=entry["topic_id"],
                            notify=bool(item.get("notify")),
                            reply_markup=reply_markup,
                            reply_to_message_id=pane_root_reply_target(entry),
                            api_token=pane_api_token,
                        )
                    else:
                        entry["last_clean_render_hash"] = item_render_hash
                        entry["last_clean_hash"] = item_render_hash
                        entry["last_clean_semantic_hash"] = item_semantic_hash
                        entry["last_clean_text"] = item_plain_text(item)
                        entry["last_clean_item"] = item
                        entry["last_clean_message_missing_at"] = utc_now()
                        entry.pop("last_clean_send_error", None)
                        changed = True
                        result = {"ok": False, "kind": "not_found", "skipped_stale_repost": True}
                else:
                    result = {**result, "edit_failed": True}
            else:
                result = send_feed_item(
                    chat_id,
                    item,
                    telegram=telegram,
                    thread_id=entry["topic_id"],
                    notify=bool(item.get("notify")),
                    reply_markup=reply_markup,
                    reply_to_message_id=pane_root_reply_target(entry),
                    api_token=pane_api_token,
                )
            if result.get("ok"):
                counters["sends"] = counters.get("sends", 0) + 1
                counters["feed_sends"] = counters.get("feed_sends", 0) + 1
                feed_delivered_this_pane = True
                record_delivered_feed_item(
                    entry,
                    item,
                    result,
                    pending_active_prompt=pending_active_prompt,
                    clear_active_prompt=clear_active_prompt,
                    item_render_hash=item_render_hash,
                    item_semantic_hash=item_semantic_hash,
                    fallback_message_id=message_id if did_edit else None,
                )
                record_message_bot_kind(
                    entry,
                    "last_clean_bot_kind",
                    sent_message_bot_kind(telegram, entry, None, result),
                    desired_bot_kind,
                )
                route_id = str(result.get("message_id") or (message_id if did_edit else ""))
                if route_id:
                    record_pane_message_route(
                        state,
                        str(entry.get("space_key") or ""),
                        str(entry.get("pane_key") or ""),
                        route_id,
                    )
                if str(item.get("kind") or "").lower() == "turn":
                    clear_stream_state(entry)
                changed = True
            elif result_topic_missing(result):
                clear_entry_topic_mapping(state, entry, str(result.get("error") or result))
                save_state(state)
                return {"early_return": True, "changed": changed, "feed_delivered": feed_delivered_this_pane, "stream_active": stream_active_this_pane}
            elif result_pane_root_missing(result):
                clear_pane_root_message(state, entry, str(result.get("error") or result))
                save_state(state)
                return {"early_return": True, "changed": changed, "feed_delivered": feed_delivered_this_pane, "stream_active": stream_active_this_pane}
            elif result.get("skipped_stale_repost"):
                changed = True
            elif result.get("kind") == "bot_access":
                note_managed_bot_access_required(
                    entry,
                    "last_clean_bot_kind",
                    desired_message_bot_kind(telegram, entry),
                )
                entry["last_clean_send_error"] = sanitize_text(str(result), 500)
                changed = True
            else:
                entry["last_clean_send_error"] = sanitize_text(str(result), 500)
                changed = True
    elif old_clean_has_noise:
        clear_clean_feed_state(entry)
        changed = True
    entry["last_status_hash"] = stable_obj_hash
    return {"early_return": None, "changed": changed, "feed_delivered": feed_delivered_this_pane, "stream_active": stream_active_this_pane}


def _sync_pane_legacy_status(
    state: dict[str, Any],
    chat_id: str,
    telegram: dict[str, Any],
    pane: dict[str, Any],
    entry: dict[str, Any],
    counters: dict[str, int],
    *,
    pane_api_token: str | None,
    new_entry: bool,
    max_sends: int,
    stable_obj_hash: str,
    changed: bool,
) -> dict[str, Any]:
    """Legacy status-message delivery for one pane (when clean feed is off).

    Extracted from sync_pane_once. Mutates state/entry/counters in place.
    Returns {"early_return", "changed"}. early_return is True when the topic
    or pane-root went missing (caller must return True immediately).
    """
    pane_status = str(pane.get("agent_status") or "").lower()
    include_recent = pane_status in {"blocked", "unknown"}
    status_result = send_legacy_message_result(
        chat_id,
        format_status(pane, include_recent=include_recent),
        thread_id=entry["topic_id"],
        notify=pane_status in {"blocked", "error"},
        reply_to_message_id=pane_root_reply_target(entry),
        api_token=pane_api_token,
    )
    if status_result.get("ok"):
        desired_bot_kind = desired_message_bot_kind(telegram, entry)
        counters["sends"] = counters.get("sends", 0) + 1
        if status_result.get("message_id"):
            record_pane_message_route(
                state,
                str(entry.get("space_key") or ""),
                str(entry.get("pane_key") or ""),
                str(status_result["message_id"]),
            )
        record_message_bot_kind(
            entry,
            "last_status_bot_kind",
            sent_message_bot_kind(telegram, entry, None, status_result),
            desired_bot_kind,
        )
        entry["last_status_hash"] = stable_obj_hash
        entry["last_notified_status"] = pane_status
        entry["last_sent_at"] = utc_now()
        changed = True
    elif result_topic_missing(status_result):
        clear_entry_topic_mapping(state, entry, str(status_result.get("error") or status_result))
        save_state(state)
        return {"early_return": True, "changed": changed}
    elif result_pane_root_missing(status_result):
        clear_pane_root_message(state, entry, str(status_result.get("error") or status_result))
        save_state(state)
        return {"early_return": True, "changed": changed}
    return {"early_return": None, "changed": changed}


def sync_pane_once(
    state: dict[str, Any],
    chat_id: str,
    telegram: dict[str, Any],
    pane: dict[str, Any],
    counters: dict[str, int],
    caps: dict[str, int],
    *,
    turn_only: bool = False,
) -> bool:
    key, entry, new_entry = ensure_pane_entry(state, pane)
    changed = bool(new_entry)
    refresh_entry_managed_voice(state, entry, pane)
    entry["prompt_collapse_chars"] = space_prompt_collapse_chars(state, entry)
    entry["last_seen_at"] = utc_now()
    entry["last_known_status"] = str(pane.get("agent_status") or "unknown")
    max_creates = int(caps.get("max_creates", MAX_CREATES_PER_RUN))
    max_sends = int(caps.get("max_sends", MAX_SENDS_PER_RUN))
    max_feed_sends = int(caps.get("max_feed_sends", max_sends))
    max_marker_sends = int(caps.get("max_marker_sends", MAX_STATUS_MARKERS_PER_RUN))
    max_verifies = int(caps.get("max_verifies", MAX_TOPIC_VERIFIES_PER_RUN))
    feed_delivered_this_pane = False
    stream_active_this_pane = False
    pane_api_token = managed_bot_token_for_entry(telegram, entry, pane)

    space_entry, space_changed = ensure_space_topic(state, chat_id, telegram, pane, counters, max_creates)
    if space_changed:
        changed = True
    if space_entry.get("topic_id"):
        entry["topic_id"] = str(space_entry.get("topic_id") or "")
        if space_entry.get("last_topic_verified_at"):
            entry["last_topic_verified_at"] = str(space_entry.get("last_topic_verified_at") or "")
        entry.pop("topic_missing_at", None)
        entry.pop("topic_missing_id", None)
        entry.pop("topic_missing_reason", None)
    if not entry.get("topic_id"):
        return changed
    if space_entry.get("topic_rename_pending_at"):
        rename_result = verify_topic_mapping(chat_id, space_entry)
        counters["renames"] = counters.get("renames", 0) + 1
        if rename_result.get("ok"):
            if space_entry.get("last_topic_verified_at"):
                entry["last_topic_verified_at"] = str(space_entry.get("last_topic_verified_at") or "")
            changed = True
        elif result_topic_missing(rename_result):
            clear_space_topic_mapping(state, space_entry, str(rename_result.get("error") or rename_result))
            save_state(state)
            return True
        else:
            space_entry["last_topic_verify_error"] = sanitize_text(str(rename_result), 500)
            space_entry["last_topic_verify_error_at"] = utc_now()
            changed = True
    elif counters.get("verifies", 0) < max_verifies and topic_verify_due(space_entry):
        verify_result = verify_topic_mapping(chat_id, space_entry)
        counters["verifies"] = counters.get("verifies", 0) + 1
        if verify_result.get("ok"):
            if space_entry.get("last_topic_verified_at"):
                entry["last_topic_verified_at"] = str(space_entry.get("last_topic_verified_at") or "")
            changed = True
        elif result_topic_missing(verify_result):
            clear_space_topic_mapping(state, space_entry, str(verify_result.get("error") or verify_result))
            save_state(state)
            return True
        else:
            space_entry["last_topic_verify_error"] = sanitize_text(str(verify_result), 500)
            space_entry["last_topic_verify_error_at"] = utc_now()
            changed = True
    root_changed, root_result = ensure_pane_root_message(
        state,
        chat_id,
        telegram,
        pane,
        entry,
        counters,
        max_sends,
    )
    if root_changed:
        changed = True
    if result_topic_missing(root_result):
        clear_entry_topic_mapping(state, entry, str(root_result.get("error") or root_result))
        save_state(state)
        return True

    stable_obj_hash = status_hash(stable_status_object(pane))
    live_item = live_status_item(pane)
    live_card_hash = clean_feed_hash(live_item)
    if LIVE_CARD_ENABLED and not STATUS_MARKER_ENABLED and counters.get("sends", 0) < max_sends and (
        not entry.get("card_message_id") or entry.get("card_status_hash") != live_card_hash
    ):
        card_result = update_live_card(chat_id, entry, live_item, telegram=telegram)
        if card_result.get("attempted"):
            counters["sends"] = counters.get("sends", 0) + 1
        if result_topic_missing(card_result):
            clear_entry_topic_mapping(state, entry, str(card_result.get("error") or card_result))
            save_state(state)
            return True
        if result_pane_root_missing(card_result):
            clear_pane_root_message(state, entry, str(card_result.get("error") or card_result))
            save_state(state)
            return True
        if card_result.get("ok"):
            entry["card_status_hash"] = live_card_hash
            if card_result.get("message_id"):
                record_pane_message_route(
                    state,
                    str(entry.get("space_key") or ""),
                    str(entry.get("pane_key") or ""),
                    str(card_result["message_id"]),
                )
            changed = True

    if CLEAN_FEED_ENABLED:
        feed_result = _sync_pane_clean_feed(
            state,
            chat_id,
            telegram,
            pane,
            entry,
            counters,
            pane_api_token=pane_api_token,
            turn_only=turn_only,
            new_entry=new_entry,
            max_sends=max_sends,
            max_feed_sends=max_feed_sends,
            stable_obj_hash=stable_obj_hash,
            changed=changed,
        )
        if feed_result["early_return"] is not None:
            return feed_result["early_return"]
        changed = feed_result["changed"]
        feed_delivered_this_pane = feed_result["feed_delivered"]
        stream_active_this_pane = feed_result["stream_active"]
    elif counters.get("sends", 0) < max_sends and should_send_status(entry, stable_obj_hash, pane, new_entry):
        status_result = _sync_pane_legacy_status(
            state,
            chat_id,
            telegram,
            pane,
            entry,
            counters,
            pane_api_token=pane_api_token,
            new_entry=new_entry,
            max_sends=max_sends,
            stable_obj_hash=stable_obj_hash,
            changed=changed,
        )
        if status_result["early_return"] is not None:
            return status_result["early_return"]
        changed = status_result["changed"]
    # One-time ⚠️ warning when a pane's agent stalls on a model-API error.
    api_warn = apply_api_error_warning(chat_id, telegram, entry, counters, max_sends)
    if api_warn["topic_missing"]:
        clear_entry_topic_mapping(state, entry, "api-error notice: topic missing")
        save_state(state)
        return True
    if api_warn.get("pane_root_missing"):
        clear_pane_root_message(state, entry, str(api_warn.get("error") or "api-error notice: pane root missing"))
        save_state(state)
        return True
    if api_warn["changed"]:
        if api_warn.get("message_id"):
            record_pane_message_route(
                state,
                str(entry.get("space_key") or ""),
                str(entry.get("pane_key") or ""),
                str(api_warn["message_id"]),
            )
        changed = True

    # The topic status icon is now set once per topic (aggregate of ALL open panes) in
    # update_topic_icons_for_spaces after the pane loop — not per pane — so same-topic
    # panes no longer fight over it. Here we only read whether the topic already has an
    # icon, to drive the status-marker suppression below.
    status_icon_ok = False
    if STATUS_ICON_ENABLED and entry.get("topic_id"):
        _space_rec = (state.get("spaces") or {}).get(str(entry.get("space_key") or ""))
        status_icon_ok = bool(isinstance(_space_rec, dict) and _space_rec.get("topic_status_icon_custom_emoji_id"))
        # When the topic icon already conveys status, retire any leftover text marker.
        if status_icon_ok and STATUS_MARKER_SUPPRESS_WHEN_ICON_OK and clear_status_marker_for_icon(chat_id, entry, api_token=pane_api_token):
            changed = True

    if (
        STATUS_MARKER_ENABLED
        and not feed_delivered_this_pane
        and not stream_active_this_pane
        and not (STATUS_MARKER_SUPPRESS_WHEN_ICON_OK and status_icon_ok)
        and entry.get("topic_id")
        and counters.get("marker_sends", 0) < max_marker_sends
    ):
        marker_result = update_status_marker(chat_id, entry, pane, telegram=telegram)
        if marker_result.get("attempted"):
            counters["sends"] = counters.get("sends", 0) + 1
            counters["marker_sends"] = counters.get("marker_sends", 0) + 1
        if result_topic_missing(marker_result):
            clear_entry_topic_mapping(state, entry, str(marker_result.get("error") or marker_result))
            save_state(state)
            return True
        if result_pane_root_missing(marker_result):
            clear_pane_root_message(state, entry, str(marker_result.get("error") or marker_result))
            save_state(state)
            return True
        if marker_result.get("ok") and marker_result.get("attempted"):
            if marker_result.get("message_id"):
                record_pane_message_route(
                    state,
                    str(entry.get("space_key") or ""),
                    str(entry.get("pane_key") or ""),
                    str(marker_result["message_id"]),
                )
            changed = True
    return changed


def sync_once() -> dict[str, Any]:
    load_dotenv()
    state = load_state()
    clear_sync_caches()
    changed = clear_disabled_visible_choice_state(state)
    if not state.get("enabled", True):
        if changed:
            save_state(state)
        return {"ok": True, "changed": changed, "message": "disabled"}
    telegram, chat_id = configure_telegram_state(state)

    # Switch topic grouping (per-space vs per-agent) cleanly: when the mode
    # changes, forget all existing topic mappings so fresh topics are created.
    if reconcile_topic_grouping(state):
        changed = True

    panes = observed_agent_panes()
    closed_result = sync_closed_pane_records(
        state,
        chat_id,
        telegram,
        panes,
        sends=0,
        max_sends=MAX_SENDS_PER_RUN,
    )
    if closed_result.get("changed"):
        changed = True
    sends = int(closed_result.get("sent") or 0)
    counters = make_sync_counters(sends=sends)
    caps = make_sync_caps()
    pinned_status_updated = 0

    if not panes:
        pinned_result = reconcile_pinned_status_views(state, chat_id, panes, counters, caps["max_sends"])
        changed, pinned_status_updated = add_pinned_reconcile_result(changed, pinned_status_updated, pinned_result)
        state["last_sync_empty_at"] = utc_now()
        save_state(state)
        return {
            "ok": True,
            "changed": changed,
            "panes": 0,
            "sent": counters["sends"],
            "pinned_status_updated": pinned_status_updated,
            "message": "no agent panes",
        }

    try:
        if not preflight_is_fresh(telegram):
            preflight(chat_id, telegram)
            telegram["last_preflight_ok_at"] = utc_now()
        telegram.pop("last_preflight_error", None)
    except BridgeError as exc:
        error_text = sanitize_text(str(exc), 500)
        if is_transient_telegram_error(error_text) and preflight_ok_within(telegram):
            telegram["last_preflight_warning"] = error_text
            telegram["last_preflight_warning_at"] = utc_now()
            save_state(state)
        else:
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
                        preflight_alert_text(error_text),
                        thread_id=telegram.get("general_thread_id", DEFAULT_GENERAL_THREAD_ID),
                        notify=not is_transient_telegram_error(error_text),
                    )
                    telegram["last_preflight_alert_at"] = utc_now()
                except Exception:
                    pass
            telegram["last_preflight_error"] = error_text
            save_state(state)
            raise
    # Reconcile pinned status BEFORE the (possibly slow) per-pane turn loop so a
    # working/blocked status surfaces promptly, then again AFTER for the authoritative
    # post-sync view. (Status events also reconcile in event_once for immediacy.)
    pinned_result = reconcile_pinned_status_views(state, chat_id, panes, counters, caps["max_sends"])
    changed, pinned_status_updated = add_pinned_reconcile_result(changed, pinned_status_updated, pinned_result)
    if ensure_managed_bot_setup_message(state, chat_id, telegram, counters, caps["max_sends"], panes):
        changed = True
    if ensure_managed_bot_group_access_message(state, chat_id, telegram, counters, caps["max_sends"], panes):
        changed = True
    if ensure_multibot_offer_message(state, chat_id, telegram, counters, caps["max_sends"], panes):
        changed = True
    if TURN_FEED_ENABLED:
        prefetch_pane_turns([str(p.get("pane_id") or "") for p in panes if p.get("pane_id")])
    # One aggregate topic-icon update per topic (worst-severity of its open panes),
    # BEFORE the per-pane loop so the per-pane status-marker suppression sees this sync's
    # icon for already-mapped topics. (A brand-new topic is created inside the loop, so
    # its icon lands this sync but one cycle after its first marker — self-corrects next
    # sync.) Agent statuses don't change during the loop, so pre-loop is accurate.
    update_topic_icons_for_spaces(state, chat_id, telegram, panes, counters)
    for pane in panes:
        if sync_pane_once(state, chat_id, telegram, pane, counters, caps):
            changed = True
    devin_glm_started = 0
    devin_glm_result = ensure_devin_glm_space_seats(state, panes)
    if devin_glm_result.get("changed"):
        changed = True
        devin_glm_started = int(devin_glm_result.get("started") or 0)
        if devin_glm_started:
            panes = observed_agent_panes()
    pinned_result = reconcile_pinned_status_views(state, chat_id, panes, counters, caps["max_sends"])
    changed, pinned_status_updated = add_pinned_reconcile_result(changed, pinned_status_updated, pinned_result)
    sends = counters["sends"]
    creates = counters["creates"]
    verifies = counters["verifies"]
    renames = counters["renames"]

    save_state(state)
    return {
        "ok": True,
        "changed": changed,
        "panes": len(panes),
        "created": creates,
        "verified": verifies,
        "renamed": renames,
        "sent": sends,
        "feed_sent": counters["feed_sends"],
        "marker_sent": counters["marker_sends"],
        "icon_updated": counters["icon_updates"],
        "pinned_status_updated": pinned_status_updated,
        "devin_glm_started": devin_glm_started,
    }


def duplicate_topic_records(state: dict[str, Any]) -> list[dict[str, Any]]:
    panes = state.get("panes") if isinstance(state.get("panes"), dict) else {}
    active = {
        key: entry
        for key, entry in panes.items()
        if isinstance(entry, dict)
        and str(entry.get("last_known_status") or "").lower() != "closed"
        and entry.get("topic_id")
    }
    records: list[dict[str, Any]] = []
    for closed_key, closed in panes.items():
        if not isinstance(closed, dict):
            continue
        if str(closed.get("last_known_status") or "").lower() != "closed":
            continue
        if not closed.get("topic_id"):
            continue
        best: tuple[int, str, dict[str, Any]] | None = None
        for active_key, active_entry in active.items():
            if str(closed.get("topic_id")) == str(active_entry.get("topic_id")):
                continue
            score = duplicate_match_score(closed, active_entry)
            if score >= 90 and (best is None or score > best[0]):
                best = (score, active_key, active_entry)
        if best:
            score, active_key, active_entry = best
            records.append({
                "closed_key": closed_key,
                "active_key": active_key,
                "score": score,
                "topic_id": str(closed.get("topic_id") or ""),
                "topic_name": str(closed.get("topic_name") or ""),
                "active_topic_id": str(active_entry.get("topic_id") or ""),
                "active_topic_name": str(active_entry.get("topic_name") or ""),
                "pane_id": str(closed.get("pane_id") or ""),
                "active_pane_id": str(active_entry.get("pane_id") or ""),
                "agent_session_id": str(closed.get("agent_session_id") or ""),
            })
    return records


def cleanup_duplicates_once(*, delete: bool = False) -> dict[str, Any]:
    load_dotenv()
    state = load_state()
    telegram, chat_id = configure_telegram_state(state)
    records = duplicate_topic_records(state)
    if not delete:
        return {"ok": True, "changed": False, "duplicates": records, "count": len(records)}
    try:
        preflight(chat_id, telegram)
        telegram["last_preflight_ok_at"] = utc_now()
    except Exception as exc:
        save_state(state)
        return {"ok": False, "changed": False, "error": sanitize_text(str(exc), 500), "duplicates": records}
    deleted: list[dict[str, Any]] = []
    failed: list[dict[str, Any]] = []
    panes = state.setdefault("panes", {})
    for record in records[:DUPLICATE_TOPIC_DELETE_LIMIT]:
        topic_id = record["topic_id"]
        try:
            ok = delete_topic(chat_id, topic_id)
        except Exception as exc:
            failed.append({**record, "error": sanitize_text(str(exc), 500)})
            continue
        if not ok:
            failed.append({**record, "error": "deleteForumTopic returned false"})
            continue
        archived = dict(panes.pop(record["closed_key"], {}) or {})
        archived["deleted_duplicate_topic_at"] = utc_now()
        archived["deleted_duplicate_topic_id"] = topic_id
        archived["active_duplicate_pane_key"] = record["active_key"]
        state.setdefault("deleted_duplicate_topics", []).append(archived)
        deleted.append(record)
    changed = bool(deleted)
    if changed or failed:
        state["last_duplicate_cleanup_at"] = utc_now()
        state["last_duplicate_cleanup_deleted"] = len(deleted)
        state["last_duplicate_cleanup_failed"] = len(failed)
        save_state(state)
    return {
        "ok": not failed,
        "changed": changed,
        "duplicates": records,
        "deleted": deleted,
        "failed": failed,
        "deleted_count": len(deleted),
        "failed_count": len(failed),
    }


def parse_plugin_json_env(name: str) -> dict[str, Any]:
    raw = os.getenv(name, "").strip()
    if not raw:
        return {}
    try:
        value = json.loads(raw)
    except json.JSONDecodeError:
        return {}
    return value if isinstance(value, dict) else {}


def _first_string_value(obj: Any, keys: set[str]) -> str:
    if isinstance(obj, dict):
        for key, value in obj.items():
            key_l = str(key).lower()
            if key_l in keys and value not in (None, ""):
                return str(value)
        for value in obj.values():
            found = _first_string_value(value, keys)
            if found:
                return found
    elif isinstance(obj, list):
        for value in obj:
            found = _first_string_value(value, keys)
            if found:
                return found
    return ""


def event_pane_id(context: dict[str, Any], event: dict[str, Any]) -> str:
    for root in (event, context):
        if not isinstance(root, dict):
            continue
        pane_container = root.get("pane")
        if isinstance(pane_container, str) and pane_container:
            return pane_container
        found = _first_string_value(pane_container, {"pane_id", "paneid", "id"})
        if found:
            return found
        for container_key in ("agent", "resource", "payload", "data"):
            container = root.get(container_key)
            found = _first_string_value(container, {"pane_id", "paneid"})
            if found:
                return found
        found = _first_string_value(root, {"pane_id", "paneid"})
        if found:
            return found
    return ""


def event_status(context: dict[str, Any], event: dict[str, Any]) -> str:
    for root in (event, context):
        found = _first_string_value(root, {"agent_status", "status", "state"})
        if found:
            return found.lower()
    return ""


def should_settle_event(pane: dict[str, Any], context: dict[str, Any], event: dict[str, Any]) -> bool:
    status = event_status(context, event) or str(pane.get("agent_status") or "").lower()
    return status in {"done", "idle", "blocked", "error"}


def plugin_enable_once(enabled: bool) -> dict[str, Any]:
    load_dotenv()
    state = load_state()
    state["plugin_event_enabled"] = bool(enabled)
    state["plugin_event_enabled_at"] = utc_now()
    save_state(state)
    return {"ok": True, "plugin_event_enabled": bool(enabled)}


def event_once() -> dict[str, Any]:
    load_dotenv()
    state = load_state()
    clear_sync_caches()
    if not state.get("enabled", True):
        return {"ok": True, "changed": False, "message": "disabled"}
    if not state.get("plugin_event_enabled", True):
        return {"ok": True, "changed": False, "message": "plugin events disabled"}

    telegram, chat_id = configure_telegram_state(state)
    # Honor a topic-grouping flag flip even when a plugin event observes it first,
    # so this path can never cross-wire topics against a stale grouping mode. A
    # reset mutates (and we persist) state, so it counts toward `changed`.
    grouping_reset = reconcile_topic_grouping(state)
    context = parse_plugin_json_env("HERDR_PLUGIN_CONTEXT_JSON")
    event = parse_plugin_json_env("HERDR_PLUGIN_EVENT_JSON")
    pane_id = event_pane_id(context, event)
    if not pane_id:
        state["last_plugin_event_missing_pane_at"] = utc_now()
        save_state(state)
        return {"ok": True, "changed": grouping_reset, "message": "no pane id in plugin event"}

    pane = pane_by_id(pane_id)
    if not pane or not pane.get("agent"):
        return reconcile_missing_event_pane(state, chat_id, telegram, pane_id)

    preflight_ok, preflight_error = preflight_for_event(state, chat_id, telegram)
    if not preflight_ok:
        save_state(state)
        return {
            "ok": True,
            "changed": grouping_reset,
            "pane_id": pane_id,
            "message": "telegram preflight failed",
            "error": preflight_error,
        }

    counters = make_sync_counters()
    caps = make_sync_caps(event=True)
    attempts = 0
    changed = grouping_reset
    settle = should_settle_event(pane, context, event)
    deadline = time.monotonic() + max(0.0, EVENT_SETTLE_SECONDS)
    try:
        while True:
            attempts += 1
            before_feed_sends = counters.get("feed_sends", 0)
            changed = sync_pane_once(state, chat_id, telegram, pane, counters, caps, turn_only=True) or changed
            if counters.get("feed_sends", 0) > before_feed_sends:
                break
            entry = (state.get("panes") or {}).get(pane_key(pane), {})
            if TURN_FEED_ENABLED and entry.get("last_turn_available") is False and not settle:
                break
            if not settle or time.monotonic() >= deadline or counters.get("sends", 0) >= caps["max_sends"]:
                break
            time.sleep(max(0.1, EVENT_SETTLE_INTERVAL_SECONDS))
            refreshed = pane_by_id(pane_id)
            if not refreshed or not refreshed.get("agent"):
                break
            pane = refreshed
    except RateLimited:
        raise
    except Exception as exc:
        state["last_plugin_event_error"] = sanitize_text(str(exc), 500)
        state["last_plugin_event_error_at"] = utc_now()
        save_state(state)
        return {"ok": True, "changed": False, "pane_id": pane_id, "message": "event sync failed"}

    # Reflect this status event in the topic icon + pinned status NOW, instead of
    # waiting for the next timer sync — so a working/blocked/idle transition shows
    # promptly (Codex cadence fix). One pane-list read drives both aggregate passes.
    try:
        event_panes = observed_agent_panes()
        update_topic_icons_for_spaces(state, chat_id, telegram, event_panes, counters)
        reconcile_pinned_status_views(state, chat_id, event_panes, counters, caps["max_sends"])
    except Exception:
        pass
    record_plugin_event_seen(state, pane_id)
    save_state(state)
    return {
        "ok": True,
        "changed": changed,
        "pane_id": pane_id,
        "sent": counters["sends"],
        "feed_sent": counters["feed_sends"],
        "marker_sent": counters["marker_sends"],
        "icon_updated": counters["icon_updates"],
        "attempts": attempts,
        "created": counters["creates"],
        "verified": counters["verifies"],
        "renamed": counters["renames"],
    }


def parse_command(text: str) -> tuple[str, str]:
    stripped = (text or "").strip()
    if not stripped:
        return "", ""
    if not stripped.startswith("/"):
        return "plain", stripped
    # Split on the first run of ANY whitespace (not just a space) so a command
    # whose argument starts on the next line — e.g. "/send\n<multi-line>" or
    # "/goal\n<goal>" — is still recognized as that command.
    parts = stripped.split(None, 1)
    command = parts[0][1:].split("@", 1)[0].strip().lower().replace("_", "-")
    rest = parts[1] if len(parts) > 1 else ""
    return command, rest.strip()


AMBIGUOUS_PANE_THREAD_REPLY = "Reply inside a pane thread so I know which Herdr pane to control."


def topic_space_entry(state: dict[str, Any], chat_id: str, topic_id: str) -> tuple[str, dict[str, Any]] | None:
    telegram = state.get("telegram") or {}
    if str(telegram.get("chat_id")) != str(chat_id):
        return None
    if str(topic_id) == str(telegram.get("general_thread_id", DEFAULT_GENERAL_THREAD_ID)):
        return None
    spaces = state.get("spaces") if isinstance(state.get("spaces"), dict) else {}
    for space_key_value, space in spaces.items():
        if isinstance(space, dict) and str(space.get("topic_id") or "") == str(topic_id):
            return str(space_key_value), space
    return None


def live_entries_for_space(state: dict[str, Any], space: dict[str, Any]) -> list[tuple[str, dict[str, Any]]]:
    panes = state.get("panes") if isinstance(state.get("panes"), dict) else {}
    entries: list[tuple[str, dict[str, Any]]] = []
    for pane_key_value in space.get("pane_keys") or []:
        key = str(pane_key_value)
        entry = panes.get(key)
        if isinstance(entry, dict) and str(entry.get("last_known_status") or "").lower() != "closed":
            entries.append((key, entry))
    return entries


def resolve_topic_entry(
    state: dict[str, Any],
    chat_id: str,
    topic_id: str,
    *,
    message_id: str = "",
    reply_to_message_id: str = "",
    prefer_message_id: bool = False,
    target_bot_kind: str = "",
) -> tuple[str, dict[str, Any]] | None:
    clean_target_bot_kind = str(target_bot_kind or "").strip().lower()
    if prefer_message_id and message_id:
        routed = route_message_to_pane(state, chat_id, topic_id, message_id)
        if routed and entry_matches_managed_bot_kind(routed[1], clean_target_bot_kind):
            return routed
    if reply_to_message_id:
        routed = route_message_to_pane(state, chat_id, topic_id, reply_to_message_id)
        if routed and entry_matches_managed_bot_kind(routed[1], clean_target_bot_kind):
            return routed
    mapped_space = topic_space_entry(state, chat_id, topic_id)
    if mapped_space:
        _space_key_value, space = mapped_space
        live_entries = [
            candidate
            for candidate in live_entries_for_space(state, space)
            if entry_matches_managed_bot_kind(candidate[1], clean_target_bot_kind)
        ]
        if len(live_entries) == 1:
            return live_entries[0]
        return None
    telegram = state.get("telegram") or {}
    if str(telegram.get("chat_id")) != str(chat_id):
        return None
    if str(topic_id) == str(telegram.get("general_thread_id", DEFAULT_GENERAL_THREAD_ID)):
        return None
    matching_entries: list[tuple[str, dict[str, Any]]] = []
    for entry in (state.get("panes") or {}).values():
        if str(entry.get("topic_id") or "") == str(topic_id):
            pane_key_value = str(entry.get("pane_key") or "")
            if not clean_target_bot_kind:
                return pane_key_value, entry
            if entry_matches_managed_bot_kind(entry, clean_target_bot_kind):
                matching_entries.append((pane_key_value, entry))
    if len(matching_entries) == 1:
        return matching_entries[0]
    return None


def topic_entry(
    state: dict[str, Any],
    chat_id: str,
    topic_id: str,
    *,
    message_id: str = "",
    reply_to_message_id: str = "",
    prefer_message_id: bool = False,
    target_bot_kind: str = "",
) -> dict[str, Any] | None:
    resolved = resolve_topic_entry(
        state,
        chat_id,
        topic_id,
        message_id=message_id,
        reply_to_message_id=reply_to_message_id,
        prefer_message_id=prefer_message_id,
        target_bot_kind=target_bot_kind,
    )
    if not resolved:
        return None
    _pane_key_value, entry = resolved
    return entry


def forward_text_to_pane_response(pane_id: str, text: str, *, usage: str = "") -> dict[str, Any]:
    outbound = str(text or "").strip()
    if not outbound:
        return {"handled": True, "reply": usage}
    ok, detail = send_to_pane(pane_id, outbound)
    if not ok:
        return {"handled": True, "reply": f"Send failed: {sanitize_text(detail, 300)}"}
    # On a normal submit `detail` is empty (silent success); when the agent was
    # busy it carries the "Queued …" note, which we surface so the sender knows.
    return {"handled": True, "reply": sanitize_text(detail, 300)}


def is_single_live_space_pane(state: dict[str, Any], chat_id: str, topic_id: str) -> bool:
    mapped_space = topic_space_entry(state, chat_id, topic_id)
    if not mapped_space:
        return False
    _space_key_value, space = mapped_space
    return len(live_entries_for_space(state, space)) == 1


def set_active_pane(space: dict[str, Any], pane_key_value: str, user_id: str) -> None:
    records = space.get("active_pane")
    if not isinstance(records, dict):
        records = {}
        space["active_pane"] = records
    records[str(user_id)] = {"pane_key": str(pane_key_value), "set_at": utc_now()}


def get_active_pane_entry(state: dict[str, Any], space: dict[str, Any], user_id: str) -> tuple[str, dict[str, Any]] | None:
    records = space.get("active_pane")
    if not isinstance(records, dict):
        return None
    record = records.get(str(user_id))
    if not isinstance(record, dict):
        return None
    age = _iso_age_seconds(str(record.get("set_at") or ""))
    if age is None or age > ACTIVE_PANE_TTL_SECONDS:
        records.pop(str(user_id), None)
        return None
    pane_key_value = str(record.get("pane_key") or "")
    panes = state.get("panes") if isinstance(state.get("panes"), dict) else {}
    entry = panes.get(pane_key_value)
    if not isinstance(entry, dict) or str(entry.get("last_known_status") or "").lower() == "closed":
        records.pop(str(user_id), None)
        return None
    return pane_key_value, entry


def clear_active_pane(space: dict[str, Any], user_id: str) -> None:
    records = space.get("active_pane")
    if isinstance(records, dict):
        records.pop(str(user_id), None)


def new_pane_usage() -> str:
    examples = " | ".join(DEVIN_SUPPORTED_MODELS)
    return (
        "Usage: /new codex|claude|kimi|omp|devin|<devin-model-id>\n"
        f"Devin model IDs: {examples}"
    )


def new_pane_agent_kind(arg: str) -> str:
    first = str(arg or "").strip().split(None, 1)[0] if str(arg or "").strip() else ""
    kind = managed_bot_kind_for_agent(first)
    return kind if kind in NEW_PANE_AGENT_COMMANDS else ""


def new_pane_agent_command(kind: str) -> str:
    env_name = f"HERDR_TELEGRAM_TOPICS_NEW_PANE_{kind.upper()}_COMMAND"
    configured = os.getenv(env_name, "").strip()
    return configured or NEW_PANE_AGENT_COMMANDS.get(kind, "")


def devin_model_alias_key(arg: str) -> str:
    first = str(arg or "").strip().split(None, 1)[0].lower() if str(arg or "").strip() else ""
    return first.replace("_", "-")


def devin_model_alias_spec(arg: str) -> dict[str, str] | None:
    alias = devin_model_alias_key(arg)
    spec = DEVIN_MODEL_ALIASES.get(alias)
    return dict(spec) if spec else None


def devin_model_command(model: str, env_prefix: str) -> str:
    configured = os.getenv(f"HERDR_TELEGRAM_TOPICS_NEW_PANE_{env_prefix}_COMMAND", "").strip()
    if configured:
        return configured
    configured = os.getenv(f"HERDR_TELEGRAM_TOPICS_{env_prefix}_COMMAND", "").strip()
    if configured:
        return configured
    args = ["devin", "--model", model]
    permission_mode = devin_glm_seat_permission_mode()
    if permission_mode:
        args.extend(["--permission-mode", permission_mode])
    extra = os.getenv(f"HERDR_TELEGRAM_TOPICS_{env_prefix}_EXTRA_ARGS", "").strip()
    if extra:
        args.extend(shlex.split(extra))
    return shlex.join(args)


def new_pane_launch_spec(arg: str) -> dict[str, str] | None:
    model_spec = devin_model_alias_spec(arg)
    if model_spec:
        model = model_spec["model"]
        label = os.getenv(f"HERDR_TELEGRAM_TOPICS_{model_spec['env']}_LABEL", model_spec["label"]).strip() or model_spec["label"]
        return {
            "kind": "devin",
            "label": label,
            "command": devin_model_command(model, model_spec["env"]),
            "rename_label": label,
            "via": "Devin",
        }
    kind = new_pane_agent_kind(arg)
    if not kind:
        return None
    label = str((managed_bot_specs().get(kind) or {}).get("label") or kind.title())
    return {"kind": kind, "label": label, "command": new_pane_agent_command(kind), "rename_label": "", "via": ""}


def devin_glm_seat_enabled() -> bool:
    return parse_bool_env("HERDR_TELEGRAM_TOPICS_DEVIN_GLM_SEAT", "0")


def devin_glm_seat_model() -> str:
    return os.getenv("HERDR_TELEGRAM_TOPICS_DEVIN_GLM_MODEL", DEVIN_GLM_SEAT_DEFAULT_MODEL).strip() or DEVIN_GLM_SEAT_DEFAULT_MODEL


def devin_glm_seat_permission_mode() -> str:
    return (
        os.getenv("HERDR_TELEGRAM_TOPICS_DEVIN_GLM_PERMISSION_MODE", DEVIN_GLM_SEAT_DEFAULT_PERMISSION_MODE).strip()
        or DEVIN_GLM_SEAT_DEFAULT_PERMISSION_MODE
    )


def devin_glm_seat_label() -> str:
    return os.getenv("HERDR_TELEGRAM_TOPICS_DEVIN_GLM_LABEL", DEVIN_GLM_SEAT_DEFAULT_LABEL).strip() or DEVIN_GLM_SEAT_DEFAULT_LABEL


def devin_glm_seat_max_per_run() -> int:
    try:
        return max(0, int(os.getenv("HERDR_TELEGRAM_TOPICS_DEVIN_GLM_SEAT_MAX_PER_RUN", "1")))
    except ValueError:
        return 1


def devin_glm_seat_command() -> str:
    configured = os.getenv("HERDR_TELEGRAM_TOPICS_DEVIN_GLM_COMMAND", "").strip()
    if configured:
        return configured
    return devin_model_command(devin_glm_seat_model(), "DEVIN_GLM")


def devin_glm_seat_cwd(space_key_value: str) -> str:
    """A stable, UNIQUE-per-space working directory for a Devin GLM seat.

    The Devin turn adapter resolves a pane's session by working directory; seats
    that share one cwd (e.g. every space's seat split from a /home/smith anchor)
    collide onto a single Devin session and broadcast one pane's turn to every
    space's topic. Giving each space's seat its own cwd makes that resolution
    exact. Stable per space so a restarted/recreated seat reuses the same dir."""
    key = str(space_key_value or "space")
    # A readable prefix PLUS a hash of the full key. The hash guarantees the dir is
    # unique per DISTINCT space key (a plain sanitizer collapses ":" and "_" to the
    # same char, so "workspace:a:b" and "workspace:a_b" would otherwise collide onto
    # one cwd and re-create the broadcast); it also makes the component traversal-safe
    # (no bare ".."). Strip leading dots/separators from the prefix as defense-in-depth.
    prefix = re.sub(r"[^A-Za-z0-9_-]", "_", key)[:40].strip("._-") or "space"
    digest = hashlib.sha1(key.encode("utf-8")).hexdigest()[:12]
    base = os.getenv("HERDR_TELEGRAM_TOPICS_DEVIN_GLM_SEAT_BASE", str(state_path().parent / "devin-glm-seats"))
    return str(Path(base) / f"{prefix}-{digest}")


def pane_closed_or_exited(pane: dict[str, Any]) -> bool:
    status = str(pane.get("agent_status") or "").strip().lower()
    return status in {"closed", "exited"} or bool(pane.get("closed") or pane.get("exited") or pane.get("process_exited"))


def pane_is_devin_glm_seat(pane: dict[str, Any]) -> bool:
    if pane_closed_or_exited(pane):
        return False
    agent_kind = managed_bot_kind_for_agent(str(pane.get("agent") or ""))
    labelish = " ".join(
        str(pane.get(key) or "")
        for key in ("label", "name", "title", "custom_status", "display_agent")
    ).lower()
    return agent_kind == "devin" and ("glm" in labelish or "5.2" in labelish)


def pane_in_space(pane: dict[str, Any], space_key_value: str) -> bool:
    try:
        return str(space_key(pane)) == str(space_key_value)
    except Exception:
        return False


def space_has_devin_glm_seat(space_entry: dict[str, Any], space_key_value: str, all_panes: list[dict[str, Any]]) -> bool:
    for pane in all_panes:
        if pane_in_space(pane, space_key_value) and pane_is_devin_glm_seat(pane):
            return True
    pending_pane_id = str(space_entry.get("devin_glm_seat_pane_id") or "")
    if not pending_pane_id:
        return False
    for pane in all_panes:
        if str(pane.get("pane_id") or "") == pending_pane_id and not pane_closed_or_exited(pane):
            return True
    if cache_fresh(str(space_entry.get("devin_glm_seat_created_at") or ""), DEVIN_GLM_SEAT_PENDING_TTL_SECONDS):
        return True
    return False


def devin_glm_anchor_pane(space_panes: list[dict[str, Any]]) -> dict[str, Any] | None:
    candidates = [pane for pane in space_panes if not pane_closed_or_exited(pane) and not pane_is_devin_glm_seat(pane)]
    if not candidates:
        return None
    status_rank = {"idle": 0, "done": 1, "blocked": 2, "unknown": 3, "working": 4}
    return sorted(
        candidates,
        key=lambda pane: (
            status_rank.get(str(pane.get("agent_status") or "").strip().lower(), 3),
            str(pane.get("pane_id") or ""),
        ),
    )[0]


def start_devin_glm_seat_for_space(
    state: dict[str, Any],
    space_key_value: str,
    space_entry: dict[str, Any],
    anchor: dict[str, Any],
) -> dict[str, Any]:
    anchor_pane_id = str(anchor.get("pane_id") or "").strip()
    if not anchor_pane_id:
        return {"ok": False, "error": "anchor pane has no pane_id"}
    split_args = [herdr_bin(), "pane", "split", anchor_pane_id, "--direction", "right"]
    # Give the seat a UNIQUE-per-space cwd (not the anchor's shared /home/smith) so
    # the Devin turn adapter resolves this pane's session unambiguously — same-cwd
    # seats otherwise collide onto one session and broadcast across all topics.
    seat_cwd = devin_glm_seat_cwd(space_key_value)
    try:
        os.makedirs(seat_cwd, exist_ok=True)
    except OSError as exc:
        space_entry["devin_glm_seat_error"] = sanitize_text(f"seat cwd mkdir failed: {exc}", 500)
        space_entry["devin_glm_seat_error_at"] = utc_now()
        return {"ok": False, "error": f"seat cwd mkdir failed: {exc}"}
    split_args.extend(["--cwd", seat_cwd])
    space_entry["devin_glm_seat_cwd"] = seat_cwd
    split_args.append("--no-focus")
    split_proc = run_cmd(split_args, timeout=10)
    if split_proc.returncode != 0:
        error = sanitize_text(split_proc.stderr or split_proc.stdout, 500)
        space_entry["devin_glm_seat_error"] = error
        space_entry["devin_glm_seat_error_at"] = utc_now()
        return {"ok": False, "error": error}
    try:
        split_data = json.loads(split_proc.stdout)
    except json.JSONDecodeError:
        split_data = {}
    new_pane_id = split_result_pane_id(split_data)
    if not new_pane_id:
        error = "Herdr did not return the new Devin GLM pane id."
        space_entry["devin_glm_seat_error"] = error
        space_entry["devin_glm_seat_error_at"] = utc_now()
        return {"ok": False, "error": error}

    label = devin_glm_seat_label()
    rename_proc = run_cmd([herdr_bin(), "pane", "rename", new_pane_id, label], timeout=5)
    if rename_proc.returncode != 0:
        space_entry["devin_glm_seat_rename_error"] = sanitize_text(rename_proc.stderr or rename_proc.stdout, 500)
        space_entry["devin_glm_seat_rename_error_at"] = utc_now()

    command = devin_glm_seat_command()
    run_proc = run_cmd([herdr_bin(), "pane", "run", new_pane_id, command], timeout=10)
    if run_proc.returncode != 0:
        error = sanitize_text(run_proc.stderr or run_proc.stdout, 500)
        space_entry["devin_glm_seat_error"] = error
        space_entry["devin_glm_seat_error_at"] = utc_now()
        space_entry["devin_glm_seat_pane_id"] = new_pane_id
        space_entry["devin_glm_seat_created_at"] = utc_now()
        return {"ok": False, "pane_id": new_pane_id, "error": error}

    space_entry["devin_glm_seat_pane_id"] = new_pane_id
    space_entry["devin_glm_seat_created_at"] = utc_now()
    space_entry["devin_glm_seat_command"] = command
    space_entry["devin_glm_seat_model"] = devin_glm_seat_model()
    space_entry["devin_glm_seat_permission_mode"] = devin_glm_seat_permission_mode()
    space_entry.pop("devin_glm_seat_error", None)
    space_entry.pop("devin_glm_seat_error_at", None)
    return {"ok": True, "pane_id": new_pane_id, "space_key": space_key_value}


def ensure_devin_glm_space_seats(state: dict[str, Any], panes: list[dict[str, Any]], max_starts: int | None = None) -> dict[str, Any]:
    if not devin_glm_seat_enabled():
        return {"changed": False, "started": 0, "skipped": True}
    max_allowed = devin_glm_seat_max_per_run() if max_starts is None else max(0, int(max_starts))
    if max_allowed <= 0:
        return {"changed": False, "started": 0, "skipped": True, "reason": "start_cap"}
    spaces = state.get("spaces") if isinstance(state.get("spaces"), dict) else {}
    if not spaces:
        return {"changed": False, "started": 0}
    try:
        all_panes = pane_list()
    except Exception as exc:
        state["last_devin_glm_seat_error"] = sanitize_text(str(exc), 500)
        state["last_devin_glm_seat_error_at"] = utc_now()
        return {"changed": True, "started": 0, "error": str(exc)}

    grouped = open_panes_by_space(panes)
    all_grouped = open_panes_by_space(all_panes)
    started = 0
    changed = False
    for space_key_value in sorted(grouped):
        if started >= max_allowed:
            break
        space_entry = spaces.get(str(space_key_value))
        if not isinstance(space_entry, dict):
            continue
        if space_has_devin_glm_seat(space_entry, str(space_key_value), all_panes):
            continue
        if cache_fresh(str(space_entry.get("devin_glm_seat_error_at") or ""), DEVIN_GLM_SEAT_ERROR_RETRY_SECONDS):
            continue
        anchor = devin_glm_anchor_pane(all_grouped.get(str(space_key_value), []) or grouped.get(str(space_key_value), []))
        if not anchor:
            continue
        result = start_devin_glm_seat_for_space(state, str(space_key_value), space_entry, anchor)
        changed = True
        if result.get("ok"):
            started += 1
    if changed:
        state["last_devin_glm_seat_sync_at"] = utc_now()
    return {"changed": changed, "started": started}


def split_result_pane_id(data: Any) -> str:
    if not isinstance(data, dict):
        return ""
    for key in ("pane_id", "new_pane_id"):
        value = str(data.get(key) or "").strip()
        if value:
            return value
    result = data.get("result")
    if not isinstance(result, dict):
        return ""
    for key in ("pane_id", "new_pane_id"):
        value = str(result.get(key) or "").strip()
        if value:
            return value
    for key in ("pane", "new_pane"):
        pane = result.get(key)
        if isinstance(pane, dict):
            value = str(pane.get("pane_id") or "").strip()
            if value:
                return value
    return ""


def new_pane_anchor_entry(
    state: dict[str, Any],
    chat_id: str,
    topic_id: str,
    *,
    message_id: str = "",
    reply_to_message_id: str = "",
) -> dict[str, Any] | None:
    resolved = resolve_topic_entry(
        state,
        chat_id,
        topic_id,
        message_id=message_id,
        reply_to_message_id=reply_to_message_id,
    )
    if resolved:
        _pane_key_value, entry = resolved
        return entry
    mapped_space = topic_space_entry(state, chat_id, topic_id)
    if not mapped_space:
        return None
    _space_key_value, space = mapped_space
    live_entries = live_entries_for_space(state, space)
    if not live_entries:
        return None
    _pane_key_value, entry = live_entries[0]
    return entry


def new_pane_picker_response(state: dict[str, Any], chat_id: str, topic_id: str) -> dict[str, Any]:
    mapped = topic_space_entry(state, chat_id, topic_id)
    if not mapped:
        return {"handled": True, "reply": new_pane_usage()}
    _space_key_value, space = mapped
    space_token = _callback_id(str(space.get("space_key") or _space_key_value), "space")[:16]
    result = send_notice(
        chat_id,
        "Open a model pane",
        "Choose the model/agent pane to open in this space. Labels ending in Devin run through the Devin CLI.",
        telegram=state.setdefault("telegram", {}),
        thread_id=topic_id,
        notify=True,
        reply_markup=new_pane_picker_reply_markup(space_token),
    )
    if result.get("ok") and result.get("message_id"):
        space["new_pane_picker_message_id"] = str(result["message_id"])
        space["new_pane_picker_sent_at"] = utc_now()
    else:
        space["new_pane_picker_error"] = sanitize_text(str(result), 500)
        space["new_pane_picker_error_at"] = utc_now()
    save_state(state)
    return {"handled": True, "reply": ""}


def new_agent_pane_response(
    state: dict[str, Any],
    chat_id: str,
    topic_id: str,
    payload: dict[str, Any],
    arg: str,
) -> dict[str, Any]:
    if not str(arg or "").strip():
        return new_pane_picker_response(state, chat_id, topic_id)
    spec = new_pane_launch_spec(arg)
    if not spec:
        return {"handled": True, "reply": new_pane_usage()}
    kind = spec["kind"]
    command = spec["command"]
    if not command:
        return {"handled": True, "reply": f"No command configured for {spec['label']}."}
    anchor = new_pane_anchor_entry(
        state,
        chat_id,
        topic_id,
        message_id=str(payload.get("message_id") or ""),
        reply_to_message_id=str(payload.get("reply_to_message_id") or ""),
    )
    if not anchor:
        return {"handled": True, "reply": "No open Herdr pane found in this space."}
    anchor_pane_id = str(anchor.get("pane_id") or "").strip()
    if not anchor_pane_id:
        return {"handled": True, "reply": "No open Herdr pane found in this space."}
    split_args = [herdr_bin(), "pane", "split", anchor_pane_id, "--direction", "right"]
    cwd = str(anchor.get("foreground_cwd") or anchor.get("cwd") or "").strip()
    if cwd:
        split_args.extend(["--cwd", cwd])
    split_args.append("--focus")
    split_proc = run_cmd(split_args, timeout=10)
    if split_proc.returncode != 0:
        detail = sanitize_text(split_proc.stderr or split_proc.stdout, 500)
        return {"handled": True, "reply": f"New pane failed: {detail}"}
    try:
        split_data = json.loads(split_proc.stdout)
    except json.JSONDecodeError:
        split_data = {}
    new_pane_id = split_result_pane_id(split_data)
    if not new_pane_id:
        return {"handled": True, "reply": "New pane failed: Herdr did not return the new pane id."}
    rename_label = str(spec.get("rename_label") or "").strip()
    if rename_label:
        rename_proc = run_cmd([herdr_bin(), "pane", "rename", new_pane_id, rename_label], timeout=5)
        if rename_proc.returncode != 0:
            detail = sanitize_text(rename_proc.stderr or rename_proc.stdout, 300)
            return {"handled": True, "reply": f"Started pane {new_pane_id}, but naming it failed: {detail}"}
    run_proc = run_cmd([herdr_bin(), "pane", "run", new_pane_id, command], timeout=10)
    if run_proc.returncode != 0:
        detail = sanitize_text(run_proc.stderr or run_proc.stdout, 500)
        return {"handled": True, "reply": f"Started pane {new_pane_id}, but launching {spec['label']} failed: {detail}"}
    via = f" through {spec['via']}" if spec.get("via") else ""
    return {"handled": True, "reply": f"Started {spec['label']}{via} in pane {new_pane_id}."}


def voice_mode_response(state: dict[str, Any], space: dict[str, Any], live: list[tuple[str, dict[str, Any]]], arg: str) -> dict[str, Any]:
    requested = str(arg or "").strip().lower().replace("-", "_")
    if requested in {"", "status"}:
        mode = str(space.get("voice_mode") or "shared")
        return {"handled": True, "reply": f"Voice mode for this space is {mode}."}
    if requested not in {"shared", "per_agent"}:
        return {"handled": True, "reply": "Usage: /voice shared|per_agent"}
    space["voice_mode"] = requested
    for _key, entry in live:
        refresh_entry_managed_voice(state, entry, None)
    save_state(state)
    label = "shared manager bot" if requested == "shared" else "per-agent bots"
    return {"handled": True, "reply": f"Voice mode for this space is now {label}."}


def command_reply(payload: dict[str, Any]) -> dict[str, Any]:
    load_dotenv()
    state = load_state()
    state_changed = clear_disabled_visible_choice_state(state)
    chat_id = str(payload.get("chat_id") or "")
    topic_id = str(payload.get("topic_id") or "")
    user_id = str(payload.get("user_id") or "")
    text = str(payload.get("text") or "")
    telegram = state.setdefault("telegram", {})
    owners = {str(x) for x in (state.get("telegram") or {}).get("owner_user_ids", [])}
    if not owners:
        owners = {p.strip() for p in os.getenv("TELEGRAM_ALLOWED_USERS", DEFAULT_OWNER_ID).split(",") if p.strip()}
    # Advance the shared-topic high-water mark for ANY new owner message that lands in a
    # mapped pane topic — INCLUDING forwarded or bot-mismatch messages that are rejected
    # by the gates below — because the message physically occupies a feed position and
    # buries any in-place turn anchor placed before it, so the next finalize must re-anchor
    # to the bottom instead of editing a now-buried message. Edited messages are skipped
    # (they create no new feed position); from_bot is skipped (the bot's own outbound sends
    # are tracked via record_pane_message_route). Owner inbound ids never reach
    # record_pane_message_route, so advance + persist here (many branches below return
    # without a trailing save_state).
    inbound_message_id = str(payload.get("message_id") or "")
    if (
        inbound_message_id
        and topic_id
        and user_id in owners
        and not payload.get("edited")
        and not payload.get("from_bot")
    ):
        mapped_topic_space = topic_space_entry(state, chat_id, topic_id)
        if mapped_topic_space and note_topic_high_water_mark(mapped_topic_space[1], inbound_message_id):
            save_state(state)
            state_changed = False
    payload_target_bot_kind = str(payload.get("target_bot_kind") or "").strip().lower()
    if payload_target_bot_kind not in managed_bot_specs():
        payload_target_bot_kind = ""
    mentioned_bot_kind = mentioned_managed_bot_kind(telegram, text)
    if payload_target_bot_kind and mentioned_bot_kind and payload_target_bot_kind != mentioned_bot_kind:
        return {"handled": True, "reply": "That message targets two different Herdr pane bots."}
    target_bot_kind = payload_target_bot_kind or mentioned_bot_kind
    if target_bot_kind:
        text = strip_managed_bot_mentions(telegram, text)

    command, arg = parse_command(text)
    if payload.get("edited"):
        return {"handled": True, "reply": ""}
    if payload.get("from_bot"):
        return {"handled": True, "reply": ""}
    if user_id not in owners:
        return {"handled": True, "reply": ""}
    if payload.get("forwarded"):
        return {"handled": True, "reply": "Ignored non-direct owner message in pane topic."}

    if command == "new":
        if state_changed:
            save_state(state)
        return new_agent_pane_response(state, chat_id, topic_id, payload, arg)

    resolved_active_entry = False
    entry = topic_entry(
        state,
        chat_id,
        topic_id,
        message_id=str(payload.get("message_id") or ""),
        reply_to_message_id=str(payload.get("reply_to_message_id") or ""),
        target_bot_kind=target_bot_kind,
    )
    if not entry:
        mapped_space = topic_space_entry(state, chat_id, topic_id)
        if mapped_space:
            _sk, space = mapped_space
            live = live_entries_for_space(state, space)
            if command == "voice":
                return voice_mode_response(state, space, live, arg)
            if command != "agents":
                if not str(payload.get("reply_to_message_id") or "").strip():
                    if (
                        not per_agent_topics_enabled()
                        and str(space.get("voice_mode") or "shared") != "per_agent"
                        and not space.get("multibot_offer_dismissed")
                        and len(managed_bot_kinds_for_panes([e for _k, e in live])) >= 2
                    ):
                        space["multibot_offer_signal"] = int(space.get("multibot_offer_signal") or 0) + 1
                        space["multibot_offer_last_signal_at"] = utc_now()
                # Consult the active pane even when this is a reply. A reply to a real
                # pane message already routed via topic_entry above (entry set, so this
                # branch is unreached); only an UNRESOLVED reply — e.g. replying to the
                # bot's own picker/confirmation message — falls through here, and it must
                # honor the active pane instead of re-showing the "which agent?" picker.
                resolved_active = get_active_pane_entry(state, space, user_id)
                if resolved_active:
                    save_state(state)
                    entry = resolved_active[1]
                    resolved_active_entry = True
            if not entry:
                if command in {"agents", "plain"}:
                    if not live:
                        return {"handled": True, "reply": "No live Herdr panes in this topic."}
                    if command == "plain" and arg.strip():
                        pend = space.setdefault("pending_pick", {})
                        if isinstance(pend, dict):
                            pend[str(user_id)] = {"text": arg, "set_at": utc_now()}
                    save_state(state)
                    space_token = _callback_id(str(space.get("space_key") or ""), "space")[:16]
                    mins = max(1, ACTIVE_PANE_TTL_SECONDS // 60)
                    send_message(
                        chat_id,
                        f"Send to which agent? Tap one — this topic then routes to them for {mins} min (no need to reply or @).",
                        thread_id=topic_id,
                        reply_markup=agents_picker_reply_markup(space_token, live),
                    )
                    return {"handled": True, "reply": ""}
                save_state(state)
                return {"handled": True, "reply": AMBIGUOUS_PANE_THREAD_REPLY}
        if not entry:
            return {"handled": False}
    if state_changed:
        save_state(state)
    refresh_entry_managed_voice(state, entry, None)
    if command == "voice":
        spaces = state.get("spaces") if isinstance(state.get("spaces"), dict) else {}
        entry_space_key = str(entry.get("space_key") or "")
        space = spaces.get(entry_space_key) if isinstance(spaces, dict) else None
        if isinstance(space, dict):
            return voice_mode_response(state, space, live_entries_for_space(state, space), arg)
        return {"handled": True, "reply": "This pane has no shared space voice setting."}

    pane_id = str(entry.get("pane_id") or "")
    if not pane_id or entry.get("last_known_status") == "closed":
        return {"handled": True, "reply": "This topic is mapped to a closed or unavailable Herdr pane."}
    pane_api_token = managed_bot_token_for_entry(telegram, entry)
    if command == "agents":
        label = str((managed_bot_specs().get(managed_bot_kind_for_entry(entry)) or {}).get("label")
                    or entry.get("agent") or entry.get("pane_id") or "pane")
        return {"handled": True, "reply": f"Only one agent here ({label}) — your messages already route to it."}

    attachment = payload.get("attachment")
    if isinstance(attachment, dict) and attachment.get("kind") in {"document", "photo"} and attachment.get("file_id"):
        caption = str(payload.get("caption") or "")
        ok, detail, dest = deliver_attachment(pane_id, attachment)
        if not ok or dest is None:
            return {"handled": True, "reply": f"Could not deliver that attachment: {detail}"}
        instruction = pane_attachment_instruction(dest, attachment, caption or text)
        sent_ok, sent_detail = send_to_pane(pane_id, instruction)
        if not sent_ok:
            return {"handled": True, "reply": f"Send failed: {sanitize_text(sent_detail, 300)}"}
        return {"handled": True, "reply": "Sent attachment to this pane."}

    if command == "plain":
        awaiting = entry.get("awaiting_detail") if isinstance(entry.get("awaiting_detail"), dict) else {}
        if awaiting and str(awaiting.get("user_id") or "") == user_id:
            awaiting_source = awaiting_detail_source(awaiting)
            if awaiting_source and prompt_interaction_disabled({"choice_source": awaiting_source}):
                entry.pop("awaiting_detail", None)
                save_state(state)
                return {
                    "handled": True,
                    "reply": "That choice prompt is no longer safe from Telegram. Use /send or answer in Herdr.",
                }
            try:
                created_at = _dt.datetime.fromisoformat(str(awaiting.get("created_at", "")).replace("Z", "+00:00"))
                expired = (_dt.datetime.now(tz=_dt.timezone.utc) - created_at).total_seconds() > DETAIL_REPLY_TIMEOUT_SECONDS
            except Exception:
                expired = True
            if expired:
                entry.pop("awaiting_detail", None)
                save_state(state)
                return {"handled": True, "reply": "That detail request expired. Use /choices to resend the choices."}
            force_reply_message_id = str(awaiting.get("force_reply_message_id") or "")
            reply_to_message_id = str(payload.get("reply_to_message_id") or "")
            if force_reply_message_id and reply_to_message_id != force_reply_message_id:
                return {"handled": True, "reply": "Reply directly to the detail prompt, or tap the button again."}
            choice = str(awaiting.get("choice") or "").strip()
            select_choice = str(awaiting.get("select_choice") or "").strip()
            visible_choice = str(awaiting.get("visible_choice") or "").strip()
            if visible_choice:
                if not visible_prompt_matches_awaiting(entry, awaiting):
                    entry.pop("awaiting_detail", None)
                    save_state(state)
                    return {
                        "handled": True,
                        "reply": "Those choices changed before I could send your answer. Use /choices to resend the current choices.",
                    }
                ok, detail = send_visible_choice_detail_to_pane(
                    pane_id,
                    visible_choice,
                    arg,
                )
            elif select_choice:
                ok, detail = send_choice_detail_to_pane(pane_id, select_choice, arg)
            else:
                outbound = f"{choice}\n{arg}" if choice else arg
                ok, detail = send_to_pane(pane_id, outbound)
            if not ok:
                return {"handled": True, "reply": f"Send failed: {detail}"}
            entry.pop("awaiting_detail", None)
            entry.pop("active_prompt", None)
            save_state(state)
            return {"handled": True, "reply": "Sent details."}
        implicit = bool((state.get("telegram") or {}).get("implicit_send_enabled", False))
        if str(payload.get("reply_to_message_id") or "").strip():
            disabled_awaiting = (
                entry.get("last_disabled_awaiting_detail")
                if isinstance(entry.get("last_disabled_awaiting_detail"), dict)
                else {}
            )
            if (
                disabled_awaiting
                and str(disabled_awaiting.get("user_id") or "") == user_id
                and str(disabled_awaiting.get("force_reply_message_id") or "")
                == str(payload.get("reply_to_message_id") or "")
            ):
                entry.pop("last_disabled_awaiting_detail", None)
                save_state(state)
                return {
                    "handled": True,
                    "reply": "That choice prompt is no longer safe from Telegram. Use /send or answer in Herdr.",
                }
            return forward_text_to_pane_response(pane_id, arg)
        if resolved_active_entry or implicit or target_bot_kind or is_single_live_space_pane(state, chat_id, topic_id):
            return forward_text_to_pane_response(pane_id, arg)
        return {"handled": True, "reply": "This is a mapped Herdr pane topic. Use /send <text> to forward to this pane, or /help."}

    if command in {"help", "start"}:
        implicit = bool((state.get("telegram") or {}).get("implicit_send_enabled", False))
        plain_text_help = (
            "Plain text from you is forwarded directly to this pane."
            if implicit
            else "Plain text is not forwarded unless implicit send is enabled."
        )
        return {
            "handled": True,
            "reply": (
                "Pane topic commands:\n"
                "/report or /status - latest clean report/question\n"
                "/choices - resend active choices or decision buttons\n"
                "/raw [lines] - sanitized raw visible output\n"
                "/debug - technical mapping details\n"
                "/send <text> - send instruction to this pane\n"
                "/send! <text> - interrupt the current turn and send now\n"
                "/keys <keys> - send explicit keys\n"
                "/agents - pick which agent to address in a shared topic\n"
                "/voice shared|per_agent - switch this space's Telegram voice\n"
                f"{plain_text_help}"
            ),
        }
    if command in {"status", "report"}:
        pane = pane_by_id(pane_id)
        if TURN_FEED_ENABLED:
            item = latest_turn_item(entry, pane)
            if item:
                report_render_hash = clean_feed_hash(item)
                report_semantic_hash = clean_feed_hash(item, include_render_version=False)
                reply_markup, pending_active_prompt, clear_active_prompt = prompt_delivery_state(item)
                result = send_feed_item(
                    chat_id,
                    item,
                    telegram=telegram,
                    thread_id=topic_id,
                    notify=False,
                    reply_markup=reply_markup,
                    reply_to_message_id=pane_root_reply_target(entry),
                    api_token=pane_api_token,
                )
                if result.get("ok"):
                    record_delivered_feed_item(
                        entry,
                        item,
                        result,
                        pending_active_prompt=pending_active_prompt,
                        clear_active_prompt=clear_active_prompt,
                        item_render_hash=report_render_hash,
                        item_semantic_hash=report_semantic_hash,
                    )
                    record_message_bot_kind(
                        entry,
                        "last_clean_bot_kind",
                        sent_message_bot_kind(telegram, entry, None, result),
                        desired_message_bot_kind(telegram, entry),
                    )
                    if result.get("message_id"):
                        record_pane_message_route(
                            state,
                            str(entry.get("space_key") or ""),
                            str(entry.get("pane_key") or ""),
                            str(result["message_id"]),
                        )
                    save_state(state)
                    return {"handled": True, "reply": ""}
                entry["last_clean_send_error"] = sanitize_text(str(result), 500)
                save_state(state)
                return {"handled": True, "reply": item_plain_text(item)}
            return {"handled": True, "reply": latest_turn_report(entry, None)}
        item = latest_clean_item(entry, pane)
        if item:
            reply_markup, pending_active_prompt, clear_active_prompt = prompt_delivery_state(item)
            result = send_feed_item(
                chat_id,
                item,
                telegram=telegram,
                thread_id=topic_id,
                notify=False,
                reply_markup=reply_markup,
                reply_to_message_id=pane_root_reply_target(entry),
                api_token=pane_api_token,
            )
            if result.get("ok"):
                if result.get("message_id"):
                    record_pane_message_route(
                        state,
                        str(entry.get("space_key") or ""),
                        str(entry.get("pane_key") or ""),
                        str(result["message_id"]),
                    )
                if pending_active_prompt:
                    bind_active_prompt_message(entry, pending_active_prompt, result.get("message_id"))
                elif clear_active_prompt:
                    entry.pop("active_prompt", None)
                    entry.pop("awaiting_detail", None)
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
        if prompt and prompt_interaction_disabled(prompt):
            entry.pop("active_prompt", None)
            entry.pop("awaiting_detail", None)
            save_state(state)
            return {"handled": True, "reply": "No active choices for this pane."}
        if not prompt_id or not options or not prompt_text:
            return {"handled": True, "reply": "No active choices for this pane."}
        revalidation, fresh_prompt_item = revalidate_pending_decision_prompt(pane_id, prompt)
        if revalidation == "stale":
            entry.pop("active_prompt", None)
            entry.pop("awaiting_detail", None)
            save_state(state)
            return {"handled": True, "reply": "No active choices for this pane."}
        if fresh_prompt_item:
            reply_markup, pending_active_prompt, clear_active_prompt = prompt_delivery_state(fresh_prompt_item)
            result = send_feed_item(
                chat_id,
                fresh_prompt_item,
                telegram=telegram,
                thread_id=topic_id,
                notify=True,
                reply_markup=reply_markup,
                reply_to_message_id=pane_root_reply_target(entry),
                api_token=pane_api_token,
            )
            if result.get("ok") and result.get("message_id"):
                record_pane_message_route(
                    state,
                    str(entry.get("space_key") or ""),
                    str(entry.get("pane_key") or ""),
                    str(result["message_id"]),
                )
            if result.get("ok") and pending_active_prompt:
                bind_active_prompt_message(entry, pending_active_prompt, result.get("message_id"))
            elif result.get("ok") and clear_active_prompt:
                entry.pop("active_prompt", None)
                entry.pop("awaiting_detail", None)
            save_state(state)
            return {"handled": True, "reply": ""}
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
        result = send_feed_item(
            chat_id,
            prompt_item,
            telegram=telegram,
            thread_id=topic_id,
            notify=True,
            reply_markup=choices_reply_markup(prompt_id, options),
            reply_to_message_id=pane_root_reply_target(entry),
            api_token=pane_api_token,
        )
        if result.get("ok"):
            if result.get("message_id"):
                record_pane_message_route(
                    state,
                    str(entry.get("space_key") or ""),
                    str(entry.get("pane_key") or ""),
                    str(result["message_id"]),
                )
            bind_active_prompt_message(entry, prompt, result.get("message_id"))
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
        return forward_text_to_pane_response(pane_id, arg, usage="Usage: /send <instruction for this pane>")
    if command in ("send!", "interrupt", "isend"):
        return interrupt_and_send_response(pane_id, arg)
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
    # Not one of herdres' own meta-commands: forward it to the pane so agent
    # slash-commands (e.g. /goal, /clear, /compact, /model) reach the CLI agent
    # as commands instead of being rejected as "unknown". Only the leading
    # @botname (added by Telegram in groups) is stripped.
    forward_text = re.sub(r"^(/\S+?)@\S+", r"\1", text.strip())
    if pane_input_needs_file(forward_text):
        # Long/multiline command: the command token MUST stay on a short single
        # line so the agent registers it as a slash-command — a long/multiline
        # paste becomes an opaque "[Pasted text]" block and the command is lost.
        # So keep "/cmd …" short and stage the bulk argument to a file the agent
        # reads (rather than replacing the whole command with a file instruction,
        # which dropped the command and truncated a preview).
        parts = forward_text.split(None, 1)
        cmd_token = parts[0]
        arg = parts[1] if len(parts) > 1 else ""  # keep the argument intact (no extra trimming)
        try:
            staged_path = write_inbound_pane_message(pane_id, arg or forward_text)
        except OSError as exc:
            return {"handled": True, "reply": f"Could not stage that command: {sanitize_text(str(exc), 300)}"}
        forward_text = (
            f"{cmd_token} The full input for this command is saved at {staged_path} — read that "
            f"file and use its complete contents as the {cmd_token.lstrip('/')} input, then proceed."
        )
        if pane_input_needs_file(forward_text):
            # Pathological (e.g. an enormous state path): refuse rather than let
            # send_to_pane silently file-convert and drop the slash-command.
            return {"handled": True, "reply": f"Could not forward /{command}: the command reference is too long."}
    return forward_text_to_pane_response(pane_id, forward_text)


def handle_onboarding_callback(state, telegram, chat_id, topic_id, message_id, space, parts):
    if len(parts) != 4:
        return {"handled": True, "answer": "Unknown Herdr action."}
    target = parts[3]
    selected = [k for k in (space.get("onboarding_selected") or []) if isinstance(k, str)]
    space_token = _callback_id(str(space.get("space_key") or ""), "space")[:16]
    all_kinds = list(managed_bot_specs().keys())
    if target == "_done":
        space["onboarding_status"] = "committed"
        save_state(state)
        try:
            telegram_api("editMessageText", {
                "chat_id": chat_id,
                "message_id": int(message_id),
                "text": "Agents set: " + (", ".join(
                    str((managed_bot_specs().get(k) or {}).get("label") or k) for k in selected
                ) or "none"),
            })
        except Exception:
            pass
        return {"handled": True, "answer": "Agents set."}
    if target not in managed_bot_specs():
        return {"handled": True, "answer": "Unknown agent."}
    if target in selected:
        selected = [k for k in selected if k != target]
    else:
        selected.append(target)
    space["onboarding_selected"] = selected
    save_state(state)
    try:
        telegram_api("editMessageReplyMarkup", {
            "chat_id": chat_id,
            "message_id": int(message_id),
            "reply_markup": json.dumps(onboarding_reply_markup(space_token, all_kinds, selected), separators=(",", ":")),
        })
    except Exception:
        pass
    return {"handled": True, "answer": "Updated."}


def handle_agent_pick_callback(state, telegram, chat_id, topic_id, message_id, user_id, space, parts):
    if len(parts) != 4:
        return {"handled": True, "answer": "Unknown Herdr action."}
    pane_token = parts[3]
    live = live_entries_for_space(state, space)
    tokens = agent_picker_pane_tokens(live)
    match = next(
        (pk for pk, _e in live if tokens.get(str(pk)) == pane_token),
        None,
    )
    if not match:
        return {"handled": True, "answer": "That pane is no longer live.", "show_alert": True}
    set_active_pane(space, match, user_id)
    panes = state.get("panes") or {}
    entry = panes.get(match) if isinstance(panes, dict) else None
    pane_id = str((entry or {}).get("pane_id") or "")
    delivered = False
    deliver_note = ""
    pend = space.get("pending_pick") if isinstance(space.get("pending_pick"), dict) else {}
    rec = pend.get(str(user_id)) if isinstance(pend, dict) else None
    if isinstance(rec, dict):
        pend.pop(str(user_id), None)
        text = str(rec.get("text") or "").strip()
        age = _iso_age_seconds(str(rec.get("set_at") or ""))
        if text and pane_id and (age is None or age <= ACTIVE_PANE_TTL_SECONDS):
            try:
                ok, detail = send_to_pane(pane_id, text)
            except Exception as exc:
                # send_to_pane shells out (subprocess timeout=8) and can RAISE, e.g.
                # TimeoutExpired on a stalled pane. Treat a raise as a failed send so
                # save_state still runs (active pane persists; pending stays consumed)
                # and the callback still answers (the inline button clears).
                ok, detail = False, sanitize_text(str(exc), 200)
            delivered = ok
            deliver_note = sanitize_text(detail, 200) if ok else ""
    save_state(state)
    label = str((managed_bot_specs().get(managed_bot_kind_for_entry(entry or {})) or {}).get("label")
                or (entry or {}).get("agent") or "pane")
    mins = max(1, ACTIVE_PANE_TTL_SECONDS // 60)
    if delivered:
        body = f"Sent to {label}. Messages here go to {label} for {mins} min (no reply or @ needed)."
        ans = f"Sent to {label}."
    else:
        body = f"Messages here go to {label} for {mins} min (no reply or @ needed)."
        ans = f"Sending to {label}."
    if deliver_note:
        body = f"{body}\n\n{deliver_note}"
    try:
        telegram_api("editMessageText", {
            "chat_id": chat_id,
            "message_id": int(message_id),
            "text": body,
        })
    except Exception:
        pass
    return {"handled": True, "answer": ans}


def handle_new_pane_picker_callback(state, telegram, chat_id, topic_id, message_id, space, parts):
    if len(parts) != 4:
        return {"handled": True, "answer": "Unknown Herdr action."}
    target = str(parts[3] or "")
    space_token = _callback_id(str(space.get("space_key") or ""), "space")[:16]
    if str(parts[2] or "") != space_token:
        return {"handled": True, "answer": "That picker expired. Use /new again.", "show_alert": True}
    result = new_agent_pane_response(
        state,
        chat_id,
        topic_id,
        {"message_id": message_id, "reply_to_message_id": ""},
        target,
    )
    reply = str(result.get("reply") or "").strip()
    if reply:
        try:
            telegram_api("editMessageText", {
                "chat_id": chat_id,
                "message_id": int(message_id),
                "text": reply,
            })
        except Exception:
            pass
    return {"handled": True, "answer": reply or "Started."}


def handle_multibot_offer_callback(state, telegram, chat_id, topic_id, message_id, space, parts):
    if len(parts) != 4:
        return {"handled": True, "answer": "Unknown Herdr action."}
    action = parts[3]
    if action == "no":
        space["multibot_offer_dismissed"] = True
        space.pop("multibot_offer_signal", None)
        save_state(state)
        try:
            telegram_api("editMessageText", {
                "chat_id": chat_id,
                "message_id": int(message_id),
                "text": "Kept shared voice. You can upgrade later with /voice.",
            })
        except Exception:
            pass
        return {"handled": True, "answer": "Dismissed."}
    if action == "up":
        live = live_entries_for_space(state, space)
        space["voice_mode"] = "per_agent"
        for _key, entry in live:
            refresh_entry_managed_voice(state, entry, None)
        scoped_kinds = managed_bot_kinds_for_panes([entry for _key, entry in live])
        try:
            manager_username = manager_bot_username(telegram)
        except Exception as exc:
            space["multibot_offer_error"] = sanitize_text(str(exc), 500)
            save_state(state)
            return {"handled": True, "answer": "Could not start upgrade — try again."}
        try:
            telegram_api("editMessageText", {
                "chat_id": chat_id,
                "message_id": int(message_id),
                "text": "Per-agent voice on for this space. Create a bot for each agent (BotFather opens in Telegram); I'll wire each one as you finish.",
            })
            telegram_api("editMessageReplyMarkup", {
                "chat_id": chat_id,
                "message_id": int(message_id),
                "reply_markup": json.dumps(
                    managed_bot_setup_reply_markup(manager_username, kinds=scoped_kinds),
                    separators=(",", ":"),
                ),
            })
        except Exception:
            pass
        space["multibot_offer_status"] = "upgrading"
        save_state(state)
        return {"handled": True, "answer": "Per-agent voice enabled."}
    return {"handled": True, "answer": "Unknown Herdr action."}


def callback_reply(payload: dict[str, Any]) -> dict[str, Any]:
    load_dotenv()
    state = load_state()
    state_changed = clear_disabled_visible_choice_state(state)
    chat_id = str(payload.get("chat_id") or "")
    topic_id = str(payload.get("topic_id") or "")
    user_id = str(payload.get("user_id") or "")
    data = str(payload.get("data") or "")
    message_id = str(payload.get("message_id") or "")
    telegram = state.setdefault("telegram", {})

    if not data.startswith("herdr:"):
        return {"handled": False}
    if (
        data.startswith("herdr:ob:")
        or data.startswith("herdr:ag:")
        or data.startswith("herdr:mb:")
        or data.startswith("herdr:np:")
    ):
        owners = {str(x) for x in (state.get("telegram") or {}).get("owner_user_ids", [])}
        if not owners:
            owners = {p.strip() for p in os.getenv("TELEGRAM_ALLOWED_USERS", DEFAULT_OWNER_ID).split(",") if p.strip()}
        if user_id not in owners:
            return {"handled": True, "answer": "Not authorized.", "show_alert": True}
        mapped_space = topic_space_entry(state, chat_id, topic_id)
        if not mapped_space:
            return {"handled": True, "answer": "This space is no longer active.", "show_alert": True}
        space_key_value, space = mapped_space
        parts = data.split(":")
        if data.startswith("herdr:mb:"):
            return handle_multibot_offer_callback(state, telegram, chat_id, topic_id, message_id, space, parts)
        if data.startswith("herdr:ob:"):
            return handle_onboarding_callback(state, telegram, chat_id, topic_id, message_id, space, parts)
        if data.startswith("herdr:np:"):
            return handle_new_pane_picker_callback(state, telegram, chat_id, topic_id, message_id, space, parts)
        return handle_agent_pick_callback(state, telegram, chat_id, topic_id, message_id, user_id, space, parts)
    entry = topic_entry(state, chat_id, topic_id, message_id=message_id, prefer_message_id=True)
    if not entry:
        return {"handled": False}
    if state_changed:
        save_state(state)

    owners = {str(x) for x in (state.get("telegram") or {}).get("owner_user_ids", [])}
    if not owners:
        owners = {p.strip() for p in os.getenv("TELEGRAM_ALLOWED_USERS", DEFAULT_OWNER_ID).split(",") if p.strip()}
    if user_id not in owners:
        return {"handled": True, "answer": "Not authorized.", "show_alert": True}
    refresh_entry_managed_voice(state, entry, None)

    parts = data.split(":")
    if len(parts) != 4 or parts[1] not in {"c", "d"}:
        return {"handled": True, "answer": "Unknown Herdr action."}
    action = parts[1]
    prompt_id = parts[2]
    choice_number = parts[3]
    prompt = entry.get("active_prompt") if isinstance(entry.get("active_prompt"), dict) else {}
    if str(prompt.get("id") or "") != prompt_id:
        return {"handled": True, "answer": "Those choices are no longer active."}
    if prompt_interaction_disabled(prompt):
        entry.pop("active_prompt", None)
        entry.pop("awaiting_detail", None)
        save_state(state)
        return {
            "handled": True,
            "answer": "These choices are no longer active from Telegram. Use /choices to refresh, /raw to inspect, or answer in Herdr.",
            "show_alert": True,
        }
    message_rejection = active_prompt_message_rejection(prompt, message_id)
    if message_rejection == "stale_message":
        return {
            "handled": True,
            "answer": "Those buttons are from an older Telegram message. Use /choices to refresh.",
            "show_alert": True,
        }
    if message_rejection == "expired":
        entry.pop("awaiting_detail", None)
        save_state(state)
        return {
            "handled": True,
            "answer": "Those choices expired. Use /choices to refresh.",
            "show_alert": True,
        }
    prompt_item = prompt.get("item") if isinstance(prompt.get("item"), dict) else {}
    if str(prompt_item.get("turn_id") or "").startswith("visible-choice:"):
        if refresh_stale_visible_prompt(state, entry, chat_id, topic_id, telegram, prompt_id):
            return {"handled": True, "answer": "Those choices changed. I sent the current prompt.", "show_alert": True}
    options = list(prompt.get("options") or [])

    pane_id = str(entry.get("pane_id") or "")
    if not pane_id or entry.get("last_known_status") == "closed":
        return {"handled": True, "answer": "This pane is no longer live.", "show_alert": True}
    pane_api_token = managed_bot_token_for_entry(telegram, entry)

    option = next(
        (
            opt
            for opt in options
            if str(opt.get("number") or "") == choice_number
            or str(opt.get("callback_id") or "") == choice_number
            or str(opt.get("id") or "") == choice_number
        ),
        None,
    )

    if action == "d":
        choice_text = ""
        select_choice = ""
        visible_choice = ""
        visible_choice_index = 0
        option_label = "custom"
        if option:
            option_label = str(option.get("label") or option.get("id") or choice_number)
            if str(option.get("id") or "").lower() != "custom" and choice_number.lower() != "custom":
                if "send_text" in option:
                    choice_text = str(option.get("send_text") or "")
                else:
                    visible_choice = str(option.get("number") or choice_number).strip()
                    for idx, candidate in enumerate(options, start=1):
                        if candidate is option:
                            visible_choice_index = idx
                            break
        entry["awaiting_detail"] = {
            "user_id": user_id,
            "prompt_id": prompt_id,
            "choice_source": prompt_source(prompt),
            "decision_id": sanitize_text(str(prompt.get("decision_id") or ""), 300),
            "choice": sanitize_text(choice_text, 500).strip(),
            "select_choice": sanitize_text(select_choice, 40).strip(),
            "visible_choice": sanitize_text(visible_choice, 40).strip(),
            "visible_choice_index": visible_choice_index,
            "visible_options": [
                {
                    "number": sanitize_text(str(opt.get("number") or ""), 40),
                    "label": sanitize_text(str(opt.get("label") or ""), 160),
                }
                for opt in options
            ],
            "option": sanitize_text(option_label, 160),
            "created_at": utc_now(),
        }
        notice_title = "Custom reply" if not choice_text and not select_choice and not visible_choice else f"Details for {choice_number}"
        notice_body = (
            "Write the instruction to send to this pane."
            if not choice_text and not select_choice and not visible_choice
            else "Write the details to send with this choice."
        )
        notice = send_notice(
            chat_id,
            notice_title,
            notice_body,
            telegram=telegram,
            thread_id=topic_id,
            notify=True,
            reply_markup={
                "force_reply": True,
                "selective": True,
                "input_field_placeholder": "Instruction for this pane" if not choice_text else f"Details for {choice_number}",
            },
            reply_to_message_id=message_id,
            api_token=pane_api_token,
        )
        if notice.get("message_id"):
            entry["awaiting_detail"]["force_reply_message_id"] = str(notice["message_id"])
        save_state(state)
        return {
            "handled": True,
            "answer": (
                "Write the instruction in this topic."
                if not choice_text and not select_choice and not visible_choice
                else "Write the details in this topic."
            ),
        }

    if not option:
        return {"handled": True, "answer": "Choice not found."}

    if choice_needs_detail(option):
        choice_text = str(option.get("send_text") or "").strip() if "send_text" in option else ""
        select_choice = ""
        visible_choice = ""
        visible_choice_index = 0
        if "send_text" not in option:
            visible_choice = str(option.get("number") or choice_number).strip()
            for idx, candidate in enumerate(options, start=1):
                if candidate is option:
                    visible_choice_index = idx
                    break
        entry["awaiting_detail"] = {
            "user_id": user_id,
            "prompt_id": prompt_id,
            "choice": sanitize_text(choice_text, 500),
            "select_choice": sanitize_text(select_choice, 40),
            "visible_choice": sanitize_text(visible_choice, 40),
            "visible_choice_index": visible_choice_index,
            "visible_options": [
                {
                    "number": sanitize_text(str(opt.get("number") or ""), 40),
                    "label": sanitize_text(str(opt.get("label") or ""), 160),
                }
                for opt in options
            ],
            "option": str(option.get("label") or ""),
            "created_at": utc_now(),
        }
        notice = send_notice(
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
            api_token=pane_api_token,
        )
        if notice.get("message_id"):
            entry["awaiting_detail"]["force_reply_message_id"] = str(notice["message_id"])
        save_state(state)
        return {"handled": True, "answer": "Write the details in this topic."}

    outbound = str(option.get("send_text") if "send_text" in option else choice_number).strip()
    if not outbound:
        return {"handled": True, "answer": "This choice needs details.", "show_alert": True}
    ok, detail = send_to_pane(pane_id, outbound)
    if not ok:
        return {"handled": True, "answer": f"Send failed: {detail}", "show_alert": True}
    entry.pop("active_prompt", None)
    entry.pop("awaiting_detail", None)
    save_state(state)
    send_notice(
        chat_id,
        "Selected",
        f"{choice_number}) {option.get('label')}",
        telegram=telegram,
        thread_id=topic_id,
        notify=False,
        api_token=pane_api_token,
    )
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


# --- setup wizard ------------------------------------------------------------
#
# `herdres setup` is the real credential gate. A SKILL.md instruction can only
# *bias* an agent; here the binary itself does the asking, validates each value,
# verifies it against Telegram, and only then writes herdres.env at 0o600. It
# refuses to run unattended without explicit flags and never silently adopts
# another app's bot token (e.g. Hermes's), which would break the one-getUpdates-
# consumer-per-token rule.

SETUP_TOKEN_RE = re.compile(r"^\d+:[A-Za-z0-9_-]{30,}$")
SETUP_CHAT_ID_RE = re.compile(r"^-100\d+$")
SETUP_ALLOWED_USERS_RE = re.compile(r"^\s*\d+(\s*,\s*\d+)*\s*$")


def _read_env_value(path: Path, key: str) -> str:
    """Return the bare value of ``key`` in a dotenv file, or '' if absent."""
    if not path.exists():
        return ""
    try:
        for raw in path.read_text(encoding="utf-8").splitlines():
            line = raw.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            name, value = line.split("=", 1)
            if name.strip() == key:
                return value.strip().strip('"').strip("'")
    except OSError:
        return ""
    return ""


def _setup_resolve(
    *,
    label: str,
    flag_value: str,
    validator: re.Pattern[str],
    interactive: bool,
    secret: bool,
    invalid_hint: str,
) -> str:
    """Resolve one setup value: flag wins, else prompt (interactive only).

    Re-prompts on invalid input in a TTY; raises BridgeError on an invalid flag
    or on a missing value when not interactive.
    """
    value = str(flag_value or "").strip()
    if value:
        if not validator.match(value):
            raise BridgeError(f"invalid {label}: {invalid_hint}")
        return value
    if not interactive:
        raise BridgeError(
            "herdres setup needs an interactive terminal, or pass "
            "--bot-token/--chat-id/--allowed-users"
        )
    while True:
        if secret:
            entered = getpass.getpass(f"{label}: ", stream=sys.stderr).strip()
        else:
            print(f"{label}: ", end="", file=sys.stderr, flush=True)
            entered = input().strip()
        if validator.match(entered):
            return entered
        print(f"  invalid {label}: {invalid_hint}", file=sys.stderr)


def _setup_confirm_hermes_reuse(interactive: bool) -> bool:
    """Require an explicit, typed confirmation before reusing the Hermes token."""
    print(
        "WARNING: this bot token is already configured for Hermes "
        f"({compact_path(str(DEFAULT_HERMES_ENV))}).\n"
        "Telegram allows only ONE getUpdates consumer per bot token, so sharing "
        "it with Hermes can make inbound commands land randomly on either "
        "process. A dedicated bot for herdres is strongly recommended.",
        file=sys.stderr,
    )
    if not interactive:
        return False
    print("Type 'reuse' to share the Hermes token anyway: ", end="", file=sys.stderr, flush=True)
    return input().strip().lower() == "reuse"


def setup_once(args: Any) -> dict[str, Any]:
    """Interactive credential wizard. Validates, preflights, then writes 0o600."""
    load_dotenv()
    interactive = sys.stdin.isatty()
    hermes_token = _read_env_value(DEFAULT_HERMES_ENV, "TELEGRAM_BOT_TOKEN")
    reuse_hermes = bool(getattr(args, "reuse_hermes_token", False))
    bot_flag = str(getattr(args, "bot_token", "") or "").strip()

    # A blank --bot-token together with --reuse-hermes-token is the explicit
    # "adopt the Hermes token" path: seed it from the Hermes env (still validated
    # below). Without that flag we never default to it — no silent scavenging.
    if not bot_flag and reuse_hermes and hermes_token:
        bot_flag = hermes_token

    token = _setup_resolve(
        label="Telegram bot token",
        flag_value=bot_flag,
        validator=SETUP_TOKEN_RE,
        interactive=interactive,
        secret=True,
        invalid_hint="expected <digits>:<30+ token chars>, e.g. 123456:ABC...",
    )
    chat_id = _setup_resolve(
        label="Forum supergroup chat id",
        flag_value=str(getattr(args, "chat_id", "") or "").strip(),
        validator=SETUP_CHAT_ID_RE,
        interactive=interactive,
        secret=False,
        invalid_hint="expected a -100... supergroup id",
    )
    allowed_users = _setup_resolve(
        label="Allowed user ids (comma-separated)",
        flag_value=str(getattr(args, "allowed_users", "") or "").strip(),
        validator=SETUP_ALLOWED_USERS_RE,
        interactive=interactive,
        secret=False,
        invalid_hint="expected numeric id(s), e.g. 123456789 or 123,456",
    )
    # Normalize to bare comma-joined ids ("123, 456" -> "123,456") so the stored
    # value matches what downstream split-on-comma parsers expect.
    allowed_users = ",".join(part.strip() for part in allowed_users.split(",") if part.strip())

    # No scavenging: if the resolved token is the Hermes token, require explicit
    # opt-in (the --reuse-hermes-token flag or an interactive 'reuse' confirm).
    if hermes_token and token == hermes_token and not reuse_hermes:
        if not _setup_confirm_hermes_reuse(interactive):
            raise BridgeError(
                "refusing to reuse the Hermes bot token without --reuse-hermes-token; "
                "use a dedicated bot for herdres (only one getUpdates consumer per token)"
            )

    # Verify before write: set the env in-process so preflight() uses these
    # exact values, then run getChat/getMe/getChatMember. Surface its error.
    saved_env = {
        key: os.environ.get(key)
        for key in ("TELEGRAM_BOT_TOKEN", "HERDRES_OUTBOUND_BOT_TOKEN", "HERDR_TELEGRAM_TOPICS_CHAT_ID")
    }
    os.environ["TELEGRAM_BOT_TOKEN"] = token
    os.environ["HERDRES_OUTBOUND_BOT_TOKEN"] = token
    os.environ["HERDR_TELEGRAM_TOPICS_CHAT_ID"] = chat_id
    try:
        preflight(chat_id)
    finally:
        for key, value in saved_env.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value

    path = setup_write_env(
        {
            "TELEGRAM_BOT_TOKEN": token,
            "HERDR_TELEGRAM_TOPICS_CHAT_ID": chat_id,
            "TELEGRAM_ALLOWED_USERS": allowed_users,
        }
    )

    print(
        "\nWrote " + compact_path(str(path)) + " (mode 0600). Next steps:\n"
        "  - enable the service: systemctl --user enable --now herdres.timer "
        "(Linux) or bootstrap the launchd agent (macOS)\n"
        "  - link the Herdr plugin: herdr plugin link "
        "~/.local/share/herdres/herdres-plugin && herdres plugin-enable\n"
        "  - verify: herdres sync",
        file=sys.stderr,
    )
    # Never echo the token back.
    return {
        "ok": True,
        "result": {
            "env_path": str(path),
            "chat_id": chat_id,
            "allowed_users": allowed_users,
            "preflight": "ok",
            "reused_hermes_token": bool(hermes_token and token == hermes_token),
        },
    }


def setup_write_env(updates: dict[str, str]) -> Path:
    """Atomically write the 3 target keys into herdres.env at 0o600.

    Preserves every existing non-target key/line in the file. Drops ALL existing
    lines for a target key (so a stale duplicate of e.g. TELEGRAM_BOT_TOKEN can't
    survive) and appends each new value exactly once. Writes via a temp file in
    the same dir + os.replace so the file is never half-written.
    """
    path = DEFAULT_ENV
    path.parent.mkdir(parents=True, exist_ok=True)
    targets = set(updates)
    lines: list[str] = []
    if path.exists():
        for raw in path.read_text(encoding="utf-8").splitlines():
            stripped = raw.strip()
            if stripped and not stripped.startswith("#") and "=" in stripped:
                key = stripped.split("=", 1)[0].strip()
                # Drop every existing line for a target key (even duplicates), so a
                # stale second copy can't survive; the new values are appended below.
                if key in targets:
                    continue
            lines.append(raw)
    for key, value in updates.items():
        lines.append(f"{key}={value}")
    payload = "\n".join(lines).rstrip("\n") + "\n"

    fd, tmp_name = tempfile.mkstemp(prefix=path.name + ".", suffix=".tmp", dir=str(path.parent))
    try:
        os.fchmod(fd, 0o600)
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            fh.write(payload)
            fh.flush()
            os.fsync(fh.fileno())
    except BaseException:
        os.unlink(tmp_name)
        raise
    os.replace(tmp_name, path)
    os.chmod(path, 0o600)
    return path


# ---------------------------------------------------------------------------
# `herdres update` — env-safe self-update (Issue #13 Phase 1+2, edge channel)
# ---------------------------------------------------------------------------

# Cross-task source marker (see GOALS/issue-13/GOAL.md): the installers write the
# absolute checkout path here so `update --edge` knows where to `git pull`.
SOURCE_MARKER = Path.home() / ".local/share/herdres/source"

# GitHub "owner/repo" the stable channel pulls signed release tarballs from. The
# RELEASE-ASSET contract (Issue #13 Phase 3): each release publishes
# `herdres-<tag>.tar.gz` + `herdres-<tag>.tar.gz.sha256`; the tarball expands to a
# source checkout (a tree containing herdres.py + the install-set files). Overridable
# via HERDRES_REPO so a fork can self-host releases.
HERDRES_REPO = os.getenv("HERDRES_REPO", "luminexord/herdres")

# Install destination roots. Module-level so tests can monkeypatch them onto a
# temp filesystem without touching the real ~/.local or ~/.config.
INSTALL_BIN_DIR = Path.home() / ".local/bin"
INSTALL_SHARE_DIR = Path.home() / ".local/share/herdres"
INSTALL_SYSTEMD_DIR = Path.home() / ".config/systemd/user"
BACKUP_DIR = Path.home() / ".local/share/herdres/backups"
KEEP_BACKUPS = 5

# launchd labels (macOS); only restarted if currently loaded. The cockpit label is
# included so a macOS update does not leave a stale cockpit running on old code.
# NOTE: this restart only re-leases already-loaded agents onto the new scripts. A
# macOS *deep* update (re-pinning the python shebang on the installed launchers, or
# refreshing the cockpit npm deps under ssh/server) is NOT done here — re-run
# install-macos.sh for that.
LAUNCHD_LABELS = (
    "com.gaijinjoe.herdres",
    "com.gaijinjoe.herdres-gateway",
    "com.gaijinjoe.herdres-cockpit",
)
# The gateway launchd label, kept distinct for the active-before/active-after health
# check that guards against a silently-failed re-enable (dead inbound).
GATEWAY_LAUNCHD_LABEL = "com.gaijinjoe.herdres-gateway"


def _installed_bin() -> Path:
    """Absolute path of the installed `herdres` launcher (for verify + plugin sed)."""
    return INSTALL_BIN_DIR / "herdres"


def _update_files_plan() -> list[dict[str, Any]]:
    """The install-user.sh code set: which source file maps to which dest + mode.

    Returns a list of plan entries. ``transform`` (optional) is applied to the
    source text before writing (used to sed the absolute herdres path into the
    plugin manifest). ``glob`` entries fan out to every match under the source
    systemd dir. ``herdres.env`` is intentionally NOT in this set.
    """
    return [
        {"src": "herdres.py", "dest": _installed_bin(), "mode": 0o755},
        {"src": "herdres_gateway.py", "dest": INSTALL_BIN_DIR / "herdres-gateway", "mode": 0o755},
        {"src": "herdres_routing.py", "dest": INSTALL_BIN_DIR / "herdres_routing.py", "mode": 0o644},
        {"src": "herdr_topic_bridge.py", "dest": INSTALL_SHARE_DIR / "herdr_topic_bridge.py", "mode": 0o644},
        {
            "src": "herdres-plugin/herdr-plugin.toml",
            "dest": INSTALL_SHARE_DIR / "herdres-plugin" / "herdr-plugin.toml",
            "mode": 0o644,
            "transform": _plugin_manifest_transform,
        },
        # systemd units: each herdres*.{service,timer} -> ~/.config/systemd/user/.
        {"src": "systemd/user/herdres.service", "dest": INSTALL_SYSTEMD_DIR / "herdres.service", "mode": 0o644},
        {"src": "systemd/user/herdres.timer", "dest": INSTALL_SYSTEMD_DIR / "herdres.timer", "mode": 0o644},
        {
            "src": "systemd/user/herdres-gateway.service",
            "dest": INSTALL_SYSTEMD_DIR / "herdres-gateway.service",
            "mode": 0o644,
        },
    ]


def _plugin_manifest_transform(text: str) -> str:
    """Rewrite the plugin manifest's bare ``["herdres", `` to the absolute path.

    Mirrors install-user.sh: ``sed 's#\\["herdres", #\\["$HOME/.local/bin/herdres", #g'``.
    """
    return text.replace('["herdres", ', f'["{_installed_bin()}", ')


def _atomic_install(dest: Path, text: str, mode: int) -> None:
    """Write ``text`` to ``dest`` via a temp file in the dest dir + os.replace.

    Mirrors the save_state/setup_write_env pattern so a dest is never half-written.
    """
    dest.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(prefix=dest.name + ".", suffix=".tmp", dir=str(dest.parent))
    try:
        os.fchmod(fd, mode)
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            fh.write(text)
            fh.flush()
            os.fsync(fh.fileno())
    except BaseException:
        try:
            os.unlink(tmp_name)
        except OSError:
            pass
        raise
    os.replace(tmp_name, dest)
    os.chmod(dest, mode)


def _parse_version(text: str) -> str | None:
    """Pull ``HERDRES_VERSION = "..."`` out of herdres.py source text."""
    match = re.search(r'^HERDRES_VERSION\s*=\s*["\']([^"\']+)["\']', text, re.MULTILINE)
    return match.group(1) if match else None


def _read_version_from(path: Path) -> str | None:
    """Parse ``HERDRES_VERSION = "..."`` from a herdres.py source file."""
    try:
        text = path.read_text(encoding="utf-8")
    except OSError:
        return None
    return _parse_version(text)


def _read_remote_version(repo: Path) -> str | None:
    """Read HERDRES_VERSION from the upstream herdres.py without touching the tree.

    Tries the tracked upstream branch first, then origin/HEAD, via
    ``git show <ref>:herdres.py``. Returns None when there is no upstream (so the
    caller can report "unknown" rather than pretend no update is available).
    """
    for ref in ("@{u}", "origin/HEAD"):
        proc = _run_git(repo, "show", f"{ref}:herdres.py")
        if proc.returncode == 0:
            version = _parse_version(proc.stdout)
            if version:
                return version
    return None


def _resolve_source(args: Any) -> Path:
    """Resolve the source checkout: --repo > HERDRES_SRC > marker > error."""
    repo = str(getattr(args, "repo", "") or "").strip()
    if not repo:
        repo = (os.environ.get("HERDRES_SRC") or "").strip()
    if not repo and SOURCE_MARKER.exists():
        repo = SOURCE_MARKER.read_text(encoding="utf-8").strip()
    if not repo:
        raise BridgeError(
            "herdres update: source checkout not found; pass --repo or set HERDRES_SRC"
        )
    path = Path(repo).expanduser()
    if not (path / "herdres.py").exists():
        raise BridgeError(f"herdres update: {compact_path(str(path))} is not a herdres checkout")
    return path


def _run_git(repo: Path, *git_args: str) -> subprocess.CompletedProcess:
    """Run ``git -C <repo> <args>`` capturing output; never raises on nonzero."""
    return subprocess.run(
        ["git", "-C", str(repo), *git_args],
        capture_output=True,
        text=True,
        check=False,
    )


def _git_rev(repo: Path, ref: str = "HEAD") -> str:
    proc = _run_git(repo, "rev-parse", "--short", ref)
    return proc.stdout.strip() if proc.returncode == 0 else ""


def _platform_is_macos() -> bool:
    return sys.platform == "darwin"


def _systemd_unit_active(unit: str) -> bool:
    """True if a systemd --user unit is enabled or loaded (so we should restart it)."""
    proc = subprocess.run(
        ["systemctl", "--user", "is-enabled", unit],
        capture_output=True,
        text=True,
        check=False,
    )
    state = proc.stdout.strip()
    # enabled / static / linked all mean "the operator wants this unit"; a missing
    # or disabled unit we leave alone rather than force-enabling it.
    return state in {"enabled", "static", "linked", "enabled-runtime"}


def _launchd_label_loaded(label: str) -> bool:
    uid = os.getuid()
    proc = subprocess.run(
        ["launchctl", "print", f"gui/{uid}/{label}"],
        capture_output=True,
        text=True,
        check=False,
    )
    return proc.returncode == 0


def _gateway_is_active() -> bool:
    """True if the inbound gateway is currently running.

    Linux: ``systemctl --user is-active herdres-gateway.service`` returns 0/"active".
    macOS: the gateway launchd label is loaded. Used to detect a silently-failed
    re-enable (a dead inbound) so the updater can roll back instead of leaving the
    gateway stopped.
    """
    if _platform_is_macos():
        return _launchd_label_loaded(GATEWAY_LAUNCHD_LABEL)
    proc = subprocess.run(
        ["systemctl", "--user", "is-active", "herdres-gateway.service"],
        capture_output=True,
        text=True,
        check=False,
    )
    return proc.returncode == 0 and proc.stdout.strip() == "active"


def _restart_services() -> list[str]:
    """Reload + restart the timer and re-lease the gateway. Returns actions taken.

    Linux/systemd: daemon-reload; restart herdres.timer; gateway is disabled then
    re-enabled (disable --now then enable --now) so the single getUpdates lease is
    released before the new process grabs it. macOS/launchd: bootout then bootstrap
    the herdres + gateway + cockpit agents. Only units currently active/loaded are
    touched.

    Gateway-health guard: if the gateway was active BEFORE the restart but is not
    active after it (e.g. a silently-failed re-enable), raise BridgeError so callers
    roll back instead of leaving a dead inbound. If it was not active to begin with,
    no health assertion is made.
    """
    # Capture the gateway's pre-restart liveness so we can assert it returns.
    gateway_was_active = _gateway_is_active()
    actions: list[str] = []
    if _platform_is_macos():
        uid = os.getuid()
        for label in LAUNCHD_LABELS:
            if not _launchd_label_loaded(label):
                continue
            plist = Path.home() / "Library/LaunchAgents" / f"{label}.plist"
            subprocess.run(
                ["launchctl", "bootout", f"gui/{uid}/{label}"],
                capture_output=True, text=True, check=False,
            )
            subprocess.run(
                ["launchctl", "bootstrap", f"gui/{uid}", str(plist)],
                capture_output=True, text=True, check=False,
            )
            actions.append(f"launchctl re-bootstrap {label}")
        _assert_gateway_health(gateway_was_active)
        return actions

    subprocess.run(["systemctl", "--user", "daemon-reload"], capture_output=True, text=True, check=False)
    actions.append("systemctl --user daemon-reload")
    if _systemd_unit_active("herdres.timer"):
        subprocess.run(
            ["systemctl", "--user", "restart", "herdres.timer"],
            capture_output=True, text=True, check=False,
        )
        actions.append("systemctl --user restart herdres.timer")
    if _systemd_unit_active("herdres-gateway.service"):
        # disable --now releases the getUpdates lease, enable --now re-acquires it.
        subprocess.run(
            ["systemctl", "--user", "disable", "--now", "herdres-gateway.service"],
            capture_output=True, text=True, check=False,
        )
        enable = subprocess.run(
            ["systemctl", "--user", "enable", "--now", "herdres-gateway.service"],
            capture_output=True, text=True, check=False,
        )
        actions.append("systemctl --user disable->enable herdres-gateway.service")
        # A nonzero enable means the re-enable silently failed; the gateway is now
        # disabled+stopped. The is-active check below catches both that and a unit
        # that enabled but failed to start.
        if enable.returncode != 0:
            actions.append(f"WARNING: gateway enable exited {enable.returncode}")
    _assert_gateway_health(gateway_was_active)
    return actions


def _assert_gateway_health(was_active: bool) -> None:
    """Raise if the gateway was active before a restart but isn't active after.

    Only asserts when the gateway was up to begin with — we never force-start a
    gateway the operator had stopped. A failure here means inbound is dead, so the
    caller should roll back (apply path) or surface a clear error (rollback path).
    """
    if was_active and not _gateway_is_active():
        raise BridgeError(
            "herdres update: gateway failed to come back active after restart "
            "(inbound is down); the gateway re-enable did not take"
        )


def _backup_install_set() -> Path:
    """Copy the currently-installed code set into backups/<UTC-ts>/, prune to KEEP_BACKUPS.

    The backup dir mirrors the dest filename per entry plus a manifest. The manifest
    records both the backed-up files (mapping each to its install destination, so
    --rollback can restore it) and the planned dests that did NOT exist pre-apply
    (so rollback can delete files newly created during a partial apply rather than
    leaving them behind). The dir name is a microsecond-precision UTC timestamp so
    lexical order == chronological order and same-second collisions can't happen.
    """
    BACKUP_DIR.mkdir(parents=True, exist_ok=True)
    # Microsecond precision: lexical sort == chronological, collisions effectively
    # impossible, so _latest_backup/_prune_backups stay chronologically correct.
    ts = _dt.datetime.now(tz=_dt.timezone.utc).strftime("%Y%m%dT%H%M%S.%fZ")
    dest_dir = BACKUP_DIR / ts
    # Defensive: a duplicate microsecond would be a clock anomaly; still don't clobber.
    suffix = 0
    while dest_dir.exists():
        suffix += 1
        dest_dir = BACKUP_DIR / f"{ts}-{suffix}"
    dest_dir.mkdir(parents=True)

    files: dict[str, str] = {}
    created: list[str] = []
    for idx, entry in enumerate(_update_files_plan()):
        installed = entry["dest"]
        if not installed.exists():
            # Dest absent pre-apply: a successful apply will create it, so record it
            # for deletion on rollback (otherwise the new file is orphaned).
            created.append(str(installed))
            continue
        # Flat, index-prefixed names keep distinct dirs' same-named files apart.
        backup_name = f"{idx:02d}_{installed.name}"
        (dest_dir / backup_name).write_bytes(installed.read_bytes())
        try:
            os.chmod(dest_dir / backup_name, installed.stat().st_mode & 0o777)
        except OSError:
            pass
        files[backup_name] = str(installed)
    manifest = {"files": files, "created": created}
    (dest_dir / "manifest.json").write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )

    _prune_backups()
    return dest_dir


def _backup_dirs() -> list[Path]:
    """Backup dirs sorted chronologically (microsecond UTC names sort lexically)."""
    if not BACKUP_DIR.exists():
        return []
    return sorted(
        (p for p in BACKUP_DIR.iterdir() if p.is_dir()),
        key=lambda p: p.name,
    )


def _read_manifest(backup_dir: Path) -> dict[str, Any]:
    """Load a backup manifest, tolerating the legacy flat ``{name: dest}`` form."""
    manifest = json.loads((backup_dir / "manifest.json").read_text(encoding="utf-8"))
    if "files" in manifest or "created" in manifest:
        return {"files": manifest.get("files", {}), "created": manifest.get("created", [])}
    # Legacy: the whole dict was the name->dest map, with no created tracking.
    return {"files": manifest, "created": []}


def _prune_backups() -> None:
    for old in _backup_dirs()[:-KEEP_BACKUPS]:
        for child in sorted(old.iterdir(), reverse=True):
            try:
                child.unlink()
            except OSError:
                pass
        try:
            old.rmdir()
        except OSError:
            pass


def _restore_backup(backup_dir: Path) -> None:
    """Fully revert an apply: restore backed-up dests AND delete newly-created ones.

    Restores every file recorded in the manifest to its dest (atomic) and deletes
    any dest that did not exist before the apply (so a partial apply that created a
    brand-new file is fully reverted rather than leaving the new file behind).
    """
    if not (backup_dir / "manifest.json").exists():
        raise BridgeError(f"herdres update: backup {compact_path(str(backup_dir))} has no manifest")
    manifest = _read_manifest(backup_dir)
    for backup_name, dest_str in manifest["files"].items():
        src = backup_dir / backup_name
        if not src.exists():
            continue
        dest = Path(dest_str)
        mode = src.stat().st_mode & 0o777
        _atomic_install(dest, src.read_text(encoding="utf-8"), mode)
    for dest_str in manifest["created"]:
        try:
            Path(dest_str).unlink()
        except OSError:
            pass


def _latest_backup() -> Path | None:
    backups = [p for p in _backup_dirs() if (p / "manifest.json").exists()]
    return backups[-1] if backups else None


def _apply_install_set(repo: Path) -> list[str]:
    """Atomically replace each install-set file from the source checkout.

    Returns the list of dest paths written. Raises (caller rolls back) on failure.
    """
    written: list[str] = []
    for entry in _update_files_plan():
        src = repo / entry["src"]
        if not src.exists():
            raise BridgeError(f"herdres update: source file missing: {entry['src']}")
        text = src.read_text(encoding="utf-8")
        transform = entry.get("transform")
        if transform is not None:
            text = transform(text)
        _atomic_install(entry["dest"], text, entry["mode"])
        written.append(str(entry["dest"]))
    return written


def _verify_install(expected_version: str, gateway_was_active: bool) -> dict[str, Any]:
    """Verify the applied update; return ``{"version": ..., "warnings": [...]}``.

    HARD success criterion (raises BridgeError -> caller rolls back): the new binary
    runs (`herdres version` exits 0) AND reports the expected NEW HERDRES_VERSION.
    That proves the code update actually landed and is loadable.

    Gateway-health (hard): if the gateway was active before the update, assert it is
    active again now (a silently-failed re-enable means inbound is dead -> roll back).

    SOFT check (warning only, never rolls back): a dry-run `herdres sync`. A nonzero
    sync can fail for ENVIRONMENTAL reasons (corrupt state, a pane probe, disk) that
    do not mean the code update is bad, so we record a warning and keep the update.
    """
    binary = _installed_bin()
    ver = subprocess.run(
        [str(binary), "version"],
        capture_output=True, text=True, check=False,
    )
    if ver.returncode != 0:
        raise BridgeError(f"herdres update: verify failed (`herdres version` exit {ver.returncode})")
    new_version = ""
    try:
        new_version = json.loads(ver.stdout or "{}").get("version", "")
    except (ValueError, AttributeError):
        pass
    if not new_version or new_version != expected_version:
        raise BridgeError(
            "herdres update: verify failed (new binary reports version "
            f"{new_version!r}, expected {expected_version!r}); update did not take"
        )

    # Hard: the gateway must be back if it was up before (catches a dead inbound).
    _assert_gateway_health(gateway_was_active)

    warnings: list[str] = []
    sync_env = dict(os.environ)
    sync_env["HERDR_TELEGRAM_TOPICS_DRY_RUN"] = "1"
    syn = subprocess.run(
        [str(binary), "sync"],
        capture_output=True, text=True, check=False, env=sync_env,
    )
    if syn.returncode != 0:
        # Soft: a dry-run sync can fail for environmental reasons that don't
        # invalidate the code update. Warn, but do NOT roll back a good update.
        warnings.append(
            f"post-update dry-run `herdres sync` exited {syn.returncode} "
            "(environmental? not rolled back; check state/panes)"
        )
    return {"version": new_version, "warnings": warnings}


# ---------------------------------------------------------------------------
# stable channel — signed GitHub-release tarballs (Issue #13 Phase 3)
# ---------------------------------------------------------------------------

# RELEASE-ASSET contract: a release publishes exactly these two assets, named off
# the tag, so the fetcher and the release CI agree on the names.
def _release_tarball_name(tag: str) -> str:
    return f"herdres-{tag}.tar.gz"


def _release_sha_name(tag: str) -> str:
    return f"{_release_tarball_name(tag)}.sha256"


def _http_get(url: str, *, accept: str = "application/octet-stream") -> bytes:
    """GET a URL, returning the raw body. stdlib-only; raises BridgeError on failure.

    A User-Agent is required by the GitHub API (it 403s a missing one); an optional
    GITHUB_TOKEN is forwarded so rate-limited / private-fork fetches still work.
    """
    req = urllib.request.Request(url, headers={
        "Accept": accept,
        "User-Agent": f"herdres/{HERDRES_VERSION}",
    })
    token = (os.environ.get("GITHUB_TOKEN") or os.environ.get("GH_TOKEN") or "").strip()
    if token:
        req.add_header("Authorization", f"Bearer {token}")
    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            return resp.read()
    except urllib.error.HTTPError as exc:
        raise BridgeError(
            f"herdres update: GET {url} failed (HTTP {exc.code} {exc.reason})"
        ) from exc
    except (urllib.error.URLError, OSError) as exc:
        raise BridgeError(f"herdres update: GET {url} failed ({exc})") from exc


def _release_metadata(tag: str | None) -> dict[str, Any]:
    """Fetch a release's JSON from the GitHub Releases API.

    ``tag`` pins a specific release (``--version``); ``None`` resolves "latest".
    Returns the parsed release object (which carries ``tag_name`` + ``assets``).
    """
    base = f"https://api.github.com/repos/{HERDRES_REPO}/releases"
    url = f"{base}/tags/{tag}" if tag else f"{base}/latest"
    body = _http_get(url, accept="application/vnd.github+json")
    try:
        data = json.loads(body.decode("utf-8"))
    except (ValueError, UnicodeDecodeError) as exc:
        raise BridgeError(f"herdres update: bad release JSON from {url} ({exc})") from exc
    if not isinstance(data, dict) or not data.get("tag_name"):
        raise BridgeError(
            f"herdres update: no release found ({'tag ' + tag if tag else 'latest'}) "
            f"for {HERDRES_REPO}"
        )
    return data


def _asset_url(release: dict[str, Any], name: str) -> str:
    """The browser_download_url for an asset named ``name`` in ``release``.

    Falls back to the conventional release-download URL if the API listing does not
    surface the asset (so a correctly-named-but-unlisted asset still resolves).
    """
    for asset in release.get("assets") or []:
        if isinstance(asset, dict) and asset.get("name") == name and asset.get("browser_download_url"):
            return str(asset["browser_download_url"])
    tag = release["tag_name"]
    return f"https://github.com/{HERDRES_REPO}/releases/download/{tag}/{name}"


def _verify_sha256(blob: bytes, sha_text: str, name: str) -> str:
    """Verify ``blob``'s sha256 against the ``.sha256`` asset; return the digest.

    The .sha256 asset is a ``sha256sum`` line (``<hex>  <filename>``) or a bare hex
    digest. Raises BridgeError on mismatch so a corrupted/tampered tarball is never
    extracted.
    """
    expected = (sha_text or "").strip().split()[0].lower() if sha_text.strip() else ""
    if not re.fullmatch(r"[0-9a-f]{64}", expected):
        raise BridgeError(f"herdres update: malformed sha256 for {name}: {sha_text!r}")
    actual = hashlib.sha256(blob).hexdigest()
    if actual != expected:
        raise BridgeError(
            f"herdres update: sha256 mismatch for {name} "
            f"(expected {expected}, got {actual}) — refusing to install"
        )
    return actual


def _safe_extract(tar: tarfile.TarFile, dest: Path) -> None:
    """Extract a tarball, refusing any member that escapes ``dest`` (zip-slip guard).

    Rejects absolute paths, ``..`` traversal, and symlink/hardlink members pointing
    outside the destination, so a hostile tarball cannot write outside the temp dir.
    """
    dest = dest.resolve()
    for member in tar.getmembers():
        target = (dest / member.name).resolve()
        if target != dest and dest not in target.parents:
            raise BridgeError(f"herdres update: refusing unsafe tar member {member.name!r}")
        if member.issym() or member.islnk():
            link_target = (target.parent / member.linkname).resolve()
            if link_target != dest and dest not in link_target.parents:
                raise BridgeError(f"herdres update: refusing unsafe tar link {member.name!r}")
    # filter="data" (Python 3.12+) is the hardened extraction filter: it strips
    # setuid/dev/absolute paths as defense-in-depth on top of the checks above. Fall
    # back to a bare extractall on older interpreters that lack the keyword.
    try:
        tar.extractall(dest, filter="data")
    except TypeError:  # pragma: no cover - only on Python < 3.12
        tar.extractall(dest)


def _source_root(extract_dir: Path) -> Path:
    """Locate the checkout root inside an extracted tarball (the dir with herdres.py).

    A release tarball commonly wraps everything in a single top-level prefix dir
    (e.g. ``herdres-0.3.0/``); accept either that or a flat layout.
    """
    if (extract_dir / "herdres.py").exists():
        return extract_dir
    for child in sorted(extract_dir.iterdir()):
        if child.is_dir() and (child / "herdres.py").exists():
            return child
    raise BridgeError("herdres update: release tarball does not contain herdres.py")


def _fetch_stable(tag: str | None, work_dir: Path) -> tuple[Path, str]:
    """Download + verify + extract a stable release into ``work_dir``.

    Resolves the release (pinned ``tag`` or latest), downloads the
    ``herdres-<tag>.tar.gz`` + ``.sha256`` assets, verifies the SHA256, and extracts
    the tarball under ``work_dir``. Returns ``(source_root, resolved_tag)`` where
    ``source_root`` is a checkout the edge apply path can consume unchanged.
    """
    release = _release_metadata(tag)
    resolved_tag = str(release["tag_name"])
    tarball_name = _release_tarball_name(resolved_tag)
    sha_name = _release_sha_name(resolved_tag)

    tarball = _http_get(_asset_url(release, tarball_name))
    sha_blob = _http_get(_asset_url(release, sha_name))
    _verify_sha256(tarball, sha_blob.decode("utf-8", "replace"), tarball_name)

    tar_path = work_dir / tarball_name
    tar_path.write_bytes(tarball)
    extract_dir = work_dir / "extract"
    extract_dir.mkdir(parents=True, exist_ok=True)
    try:
        with tarfile.open(tar_path, "r:gz") as tar:
            _safe_extract(tar, extract_dir)
    except tarfile.TarError as exc:
        raise BridgeError(f"herdres update: cannot read release tarball {tarball_name} ({exc})") from exc
    return _source_root(extract_dir), resolved_tag


def _apply_from_source(
    repo: Path,
    *,
    channel: str,
    current: str,
    available: str,
    no_restart: bool,
    head: str,
) -> dict[str, Any]:
    """Backup -> apply -> restart -> verify, rolling back on any failure.

    Shared by the edge (git checkout) and stable (extracted release) apply paths:
    once a source ``repo`` dir exists, the env-safe replace + service re-lease +
    verify + rollback are identical. ``head`` is the source revision/tag recorded in
    the result. Returns the standard update result dict.
    """
    # Snapshot gateway liveness before we touch anything so verify can assert it
    # comes back (a dead inbound after the restart means we must roll back).
    gateway_was_active = False if no_restart else _gateway_is_active()

    backup = _backup_install_set()
    try:
        written = _apply_install_set(repo)
        # _restart_services raises if a previously-active gateway can't be brought
        # back; that propagates here and rolls back. (--no-restart skips it; the
        # operator restarts manually later.)
        if no_restart:
            service_actions = ["(restart skipped: --no-restart)"]
        else:
            service_actions = _restart_services()
        verify = _verify_install(available, gateway_was_active)
    except Exception as exc:
        # Roll back when: a file replace errors, the new binary fails to run / version
        # mismatches, or the gateway fails to come back. (A soft dry-run sync failure
        # only warns and never reaches here.)
        try:
            _restore_backup(backup)
            if not no_restart:
                _restart_services()
        except Exception as restore_exc:  # pragma: no cover - best-effort restore
            raise BridgeError(
                f"herdres update FAILED and rollback also failed: {exc}; rollback: {restore_exc}"
            ) from exc
        raise BridgeError(f"herdres update failed (rolled back): {exc}") from exc

    result = {
        "ok": True,
        "action": "update",
        "channel": channel,
        "source": str(repo),
        "previous_version": current,
        "version": verify["version"] or available,
        "backup": str(backup),
        "files": written,
        "services": service_actions,
        "head": head,
    }
    warnings = list(verify["warnings"])
    if no_restart:
        warnings.append(
            "services not restarted (--no-restart): run 'systemctl --user daemon-reload "
            "&& systemctl --user restart herdres.timer' and re-enable the gateway to apply"
        )
    if warnings:
        result["warnings"] = warnings
    return result


def _update_stable(args: Any, *, no_restart: bool) -> dict[str, Any]:
    """The stable channel: --check / --dry-run / apply against a signed release.

    ``--version <tag>`` pins a release; otherwise "latest" is used. The release
    tarball is fetched + sha256-verified + extracted to a temp dir, then the SAME
    env-safe apply/restart/verify/rollback as edge runs against it.
    """
    current = HERDRES_VERSION
    pinned = str(getattr(args, "version", "") or "").strip() or None

    # --check: resolve the release version only; download nothing, apply nothing.
    if getattr(args, "check", False):
        release = _release_metadata(pinned)
        available_version = str(release["tag_name"]).lstrip("v")
        return {
            "ok": True,
            "action": "check",
            "channel": "stable",
            "source": HERDRES_REPO,
            "current_version": current,
            "available_version": available_version,
            "tag": str(release["tag_name"]),
            "update_available": available_version != current,
            "needs_upstream": False,
            "fetch_ok": True,
        }

    # --dry-run: describe the plan (resolve the tag so the operator sees the target),
    # but download/extract/apply nothing.
    if getattr(args, "dry_run", False):
        release = _release_metadata(pinned)
        files = [str(entry["dest"]) for entry in _update_files_plan()]
        service_actions = (
            ["launchctl bootout+bootstrap herdres + gateway + cockpit agents"]
            if _platform_is_macos()
            else [
                "systemctl --user daemon-reload",
                "systemctl --user restart herdres.timer",
                "systemctl --user disable --now then enable --now herdres-gateway.service",
            ]
        )
        return {
            "ok": True,
            "action": "dry-run",
            "channel": "stable",
            "source": HERDRES_REPO,
            "current_version": current,
            "target_version": str(release["tag_name"]).lstrip("v"),
            "tag": str(release["tag_name"]),
            "files": files,
            "services": service_actions,
        }

    # --- stable apply: fetch + verify + extract into a temp dir, then apply. The temp
    # dir is removed once the install-set files have been copied into place.
    with tempfile.TemporaryDirectory(prefix="herdres-stable-") as work:
        repo, resolved_tag = _fetch_stable(pinned, Path(work))
        available = _read_version_from(repo / "herdres.py") or resolved_tag.lstrip("v")
        return _apply_from_source(
            repo,
            channel="stable",
            current=current,
            available=available,
            no_restart=no_restart,
            head=resolved_tag,
        )


def update_once(args: Any) -> dict[str, Any]:
    """Self-update entry point. Returns the standard ``{"ok": ...}`` dict."""
    channel = str(getattr(args, "channel", "edge") or "edge").strip()
    if channel not in {"edge", "stable"}:
        raise BridgeError(f"herdres update: unknown channel {channel!r} (use edge or stable)")

    # --no-restart (or HERDRES_UPDATE_SKIP_RESTART=1): swap files but skip the service
    # restart — useful for "update now, restart later" and for sandboxed e2e tests
    # (systemctl/launchctl are not HOME-scoped, so skipping them keeps the test isolated).
    no_restart = bool(getattr(args, "no_restart", False)) or \
        os.environ.get("HERDRES_UPDATE_SKIP_RESTART") == "1"

    # --rollback short-circuits: restore the latest backup and restart, no git.
    if getattr(args, "rollback", False):
        backup = _latest_backup()
        if backup is None:
            raise BridgeError("herdres update: no backup to roll back to")
        _restore_backup(backup)
        # _restart_services re-checks gateway health and raises if a gateway that was
        # active can't be brought back, surfacing a clear error if rollback can't
        # restore inbound.
        if no_restart:
            actions = ["(restart skipped: --no-restart)"]
        else:
            try:
                actions = _restart_services()
            except BridgeError as exc:
                raise BridgeError(f"herdres rollback restored files but {exc}") from exc
        return {
            "ok": True,
            "action": "rollback",
            "restored_from": str(backup),
            "services": actions,
        }

    # stable channel: source is a signed GitHub release, not a local git checkout.
    if channel == "stable":
        return _update_stable(args, no_restart=no_restart)

    # --- edge channel: source is a local git checkout we `git pull --ff-only`. ---
    repo = _resolve_source(args)
    current = HERDRES_VERSION
    available = _read_version_from(repo / "herdres.py") or current

    # --check: fetch + compare the LOCAL version against the REMOTE version, apply
    # nothing. Side-effect-free except the `git fetch` (which is required to learn
    # the upstream version). A missing upstream is reported, not silently ignored.
    if getattr(args, "check", False):
        fetch = _run_git(repo, "fetch")
        local_rev = _git_rev(repo, "HEAD")
        remote_rev = _git_rev(repo, "@{u}") or _git_rev(repo, "origin/HEAD")
        remote_version = _read_remote_version(repo)
        if remote_version is None:
            # No upstream / couldn't read origin's herdres.py: don't pretend there's
            # nothing newer — report "unknown" and flag that upstream is needed.
            available_version = "unknown"
            update_available = False
            needs_upstream = True
        else:
            available_version = remote_version
            update_available = remote_version != current
            needs_upstream = False
        return {
            "ok": True,
            "action": "check",
            "channel": channel,
            "source": str(repo),
            "current_version": current,
            "available_version": available_version,
            "local_rev": local_rev,
            "remote_rev": remote_rev,
            "update_available": update_available,
            "needs_upstream": needs_upstream,
            "fetch_ok": fetch.returncode == 0,
        }

    # --dry-run: describe the plan, change nothing.
    if getattr(args, "dry_run", False):
        files = [str(entry["dest"]) for entry in _update_files_plan()]
        service_actions = (
            ["launchctl bootout+bootstrap herdres + gateway + cockpit agents"]
            if _platform_is_macos()
            else [
                "systemctl --user daemon-reload",
                "systemctl --user restart herdres.timer",
                "systemctl --user disable --now then enable --now herdres-gateway.service",
            ]
        )
        return {
            "ok": True,
            "action": "dry-run",
            "channel": channel,
            "source": str(repo),
            "current_version": current,
            "target_version": available,
            "files": files,
            "services": service_actions,
        }

    # --- edge apply ---
    pull = _run_git(repo, "pull", "--ff-only")
    if pull.returncode != 0:
        detail = (pull.stderr or pull.stdout or "").strip()
        raise BridgeError(
            "herdres update: `git pull --ff-only` failed (dirty or diverged checkout?): "
            + sanitize_text(detail, 400)
        )
    # Re-read the target version after the fast-forward.
    available = _read_version_from(repo / "herdres.py") or available

    return _apply_from_source(
        repo,
        channel="edge",
        current=current,
        available=available,
        no_restart=no_restart,
        head=_git_rev(repo, "HEAD"),
    )


def version_once(args: Any) -> dict[str, Any]:  # noqa: ARG001 - uniform handler signature
    return {"ok": True, "version": HERDRES_VERSION}


def main() -> int:
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="cmd", required=True)
    sub.add_parser("sync")
    sub.add_parser("event")
    sub.add_parser("plugin-enable")
    sub.add_parser("plugin-disable")
    cleanup = sub.add_parser("cleanup-duplicates")
    cleanup.add_argument("--delete", action="store_true")
    sub.add_parser("command")
    sub.add_parser("callback")
    sub.add_parser("managed-bot")
    probe = sub.add_parser("probe")
    probe.add_argument("--thread-id", default=None)
    setup = sub.add_parser("setup")
    setup.add_argument("--bot-token", default="")
    setup.add_argument("--chat-id", default="")
    setup.add_argument("--allowed-users", default="")
    setup.add_argument("--reuse-hermes-token", action="store_true")
    sub.add_parser("version")
    update = sub.add_parser("update")
    update.add_argument("--channel", default="edge")
    update.add_argument("--edge", action="store_const", const="edge", dest="channel")
    update.add_argument("--stable", action="store_const", const="stable", dest="channel")
    update.add_argument("--version", dest="version", default="",
                        help="(stable channel) pin a release tag instead of latest")
    update.add_argument("--repo", default="")
    update.add_argument("--check", action="store_true")
    update.add_argument("--rollback", action="store_true")
    update.add_argument("--dry-run", dest="dry_run", action="store_true")
    update.add_argument("--no-restart", dest="no_restart", action="store_true")
    args = parser.parse_args()
    try:
        if args.cmd == "sync":
            result = with_lock(sync_once)
        elif args.cmd == "event":
            result = with_lock(event_once, blocking=True)
        elif args.cmd == "plugin-enable":
            result = with_lock(lambda: plugin_enable_once(True), blocking=True)
        elif args.cmd == "plugin-disable":
            result = with_lock(lambda: plugin_enable_once(False), blocking=True)
        elif args.cmd == "cleanup-duplicates":
            result = with_lock(lambda: cleanup_duplicates_once(delete=args.delete), blocking=True)
        elif args.cmd == "command":
            payload = json.loads(sys.stdin.read() or "{}")
            result = with_lock(lambda: command_reply(payload), blocking=True)
        elif args.cmd == "callback":
            payload = json.loads(sys.stdin.read() or "{}")
            result = with_lock(lambda: callback_reply(payload), blocking=True)
        elif args.cmd == "managed-bot":
            payload = json.loads(sys.stdin.read() or "{}")
            result = with_lock(lambda: managed_bot_update(payload), blocking=True)
        elif args.cmd == "setup":
            result = setup_once(args)
        elif args.cmd == "version":
            result = version_once(args)
        elif args.cmd == "update":
            result = update_once(args)
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
