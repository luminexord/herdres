"""Small public-safety helpers for the source-only Herdres connector."""

from __future__ import annotations

import hashlib
import html
import json
import re
from typing import Any


FORBIDDEN_PUBLIC_KEYS = {
    "argv",
    "backend_target",
    "bot_token",
    "chat_id",
    "env",
    "message_id",
    "pane_id",
    "private_fingerprint",
    "socket_path",
    "stderr",
    "stdout",
    "target_value",
    "terminal_id",
    "token",
    "topic_id",
}

SECRET_RE = re.compile(r"\b\d{6,}:[A-Za-z0-9_-]{20,}\b")

# public_prune guards keys and secrets; it must not destroy turn content.
# Rendering enforces its own per-message size limits downstream.
PRUNE_TEXT_LIMIT = 64000


def sanitize_text(value: Any, limit: int = 4000) -> str:
    text = str(value or "")
    text = SECRET_RE.sub("[redacted-token]", text)
    text = "".join(ch for ch in text if ch in "\n\t" or ord(ch) >= 32)
    if limit <= 0:
        return ""
    return text[:limit]


def html_escape(value: Any, limit: int = 4000) -> str:
    return html.escape(sanitize_text(value, limit), quote=False)


def short_hash(value: Any, length: int = 16) -> str:
    encoded = json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()[: max(4, int(length))]


def compact_ws(value: Any, limit: int = 160) -> str:
    return re.sub(r"\s+", " ", sanitize_text(value, limit * 4)).strip()[:limit]


def public_prune(value: Any) -> Any:
    if isinstance(value, dict):
        result: dict[str, Any] = {}
        for key, item in value.items():
            clean_key = str(key)
            if clean_key in FORBIDDEN_PUBLIC_KEYS or "token" in clean_key.lower() or "secret" in clean_key.lower():
                continue
            result[clean_key] = public_prune(item)
        return result
    if isinstance(value, list):
        return [public_prune(item) for item in value]
    if isinstance(value, str):
        return sanitize_text(value, PRUNE_TEXT_LIMIT)
    return value
