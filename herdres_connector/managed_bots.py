"""Managed bot voice selection for source-mode Telegram delivery.

This is the source-only extraction of the old Herdres managed-bot routing
helpers: resolve a worker/entry to a generic bot kind, then pick the operator's
private token from env or state. Public code never contains user bot names.
"""

from __future__ import annotations

import re
from typing import Any

from . import config


MANAGER_BOT_KIND = "manager"

MANAGED_BOT_SPECS: dict[str, dict[str, Any]] = {
    "codex": {"aliases": ("codex",)},
    "claude": {"aliases": ("claude",)},
    "glm": {"aliases": ("glm",)},
    "kimi": {"aliases": ("kimi",)},
    "omp": {"aliases": ("omp",)},
    "devin": {"aliases": ("devin",)},
}


def managed_bot_specs() -> dict[str, dict[str, Any]]:
    return MANAGED_BOT_SPECS


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


def managed_bot_kind_for_entry(entry: dict[str, Any] | None) -> str:
    if not isinstance(entry, dict):
        return ""
    explicit = str(entry.get("bot_kind") or "").strip().lower()
    if explicit in managed_bot_specs():
        return explicit
    for key in (
        "agent",
        "worker_name",
        "active_worker_name",
        "topic_name",
        "space_topic_name",
        "tendwire_worker_id",
        "worker_id",
    ):
        kind = managed_bot_kind_for_agent(str(entry.get(key) or ""))
        if kind:
            return kind
    return ""


def managed_bot_record(telegram: dict[str, Any] | None, kind: str) -> dict[str, Any] | None:
    telegram_data = telegram if isinstance(telegram, dict) else {}
    bots = telegram_data.get("managed_bots") if isinstance(telegram_data.get("managed_bots"), dict) else {}
    record = bots.get(kind) if isinstance(bots, dict) else None
    return record if isinstance(record, dict) else None


def managed_bot_token_for_entry(
    telegram: dict[str, Any] | None,
    entry: dict[str, Any] | None,
) -> str | None:
    if not config.managed_bots_enabled():
        return None
    kind = managed_bot_kind_for_entry(entry)
    if not kind:
        return None
    record = managed_bot_record(telegram, kind) or {}
    if record.get("enabled") is False:
        return None
    token = config.managed_bot_token(kind) or str(record.get("token") or "").strip()
    return token or None


def desired_message_bot_kind(telegram: dict[str, Any] | None, entry: dict[str, Any] | None) -> str:
    token = managed_bot_token_for_entry(telegram, entry)
    if token:
        kind = managed_bot_kind_for_entry(entry)
        if kind:
            return kind
    return MANAGER_BOT_KIND
