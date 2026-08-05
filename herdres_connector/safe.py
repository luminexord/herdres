"""Small public-safety helpers for the source-only Herdres connector."""

from __future__ import annotations

import hashlib
import html
import json
import re
from typing import Any


FORBIDDEN_PUBLIC_KEYS = {
    "_meta",
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

# ACP reserves ``_meta`` for implementation-specific data and carries raw
# reasoning/tool material outside its message stream. Tendwire is the primary
# sanitizer, but the connector boundary drops the unmistakably private fields
# again so an upstream regression cannot copy them into Herdres state or audit
# output. Keep this list narrow: ``meta`` (without the leading underscore) is
# Tendwire's public stable-worker metadata and ``plan_token`` is an opaque
# connector protocol value.
PRIVATE_AGENT_FIELD_NAMES = frozenset(
    {
        "agentsessionid",
        "agentsessionids",
        "chainofthought",
        "controlevent",
        "permissionrequest",
        "rawinput",
        "rawoutput",
        "sessionid",
        "sessionids",
        "sourcesessionid",
        "sourcesessionids",
        "toolcall",
        "toolcallid",
        "toolcallids",
        "toolcalls",
        "toolcallupdate",
        "toolcallupdates",
        "tooluseid",
        "tooluseids",
    }
)
PRIVATE_AGENT_CONTAINER_FIELD_NAMES = frozenset({"acpevent", "agentevent"})

# ACP v1/v2 session/update envelopes use a discriminator beside their content,
# so deleting only the discriminator would leave the private sibling payload
# behind. Match the exact protocol shape and stable values rather than words
# inside ordinary user-visible metadata.
ACP_SESSION_UPDATE_KINDS = frozenset(
    {
        "agentmessage",
        "agentmessagechunk",
        "agentthought",
        "agentthoughtchunk",
        "availablecommandsupdate",
        "configoptionupdate",
        "currentmodeupdate",
        "plan",
        "planremoved",
        "planupdate",
        "sessioninfoupdate",
        "stateupdate",
        "terminaloutputchunk",
        "terminalupdate",
        "toolcall",
        "toolcallcontentchunk",
        "toolcallupdate",
        "usageupdate",
        "usermessage",
        "usermessagechunk",
    }
)
ACP_AGENT_EVENT_KINDS = frozenset(
    {
        "agentmessage",
        "extension",
        "plan",
        "sessioninfo",
        "thought",
        "toolcall",
        "toolcallupdate",
        "usage",
        "usermessage",
    }
)
PRIVATE_STRUCTURE_MAX_DEPTH = 128
PRIVATE_STRUCTURE_MAX_ITEMS = 100_000

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


def canonical_text(value: Any, *, field: str = "content") -> str:
    """Return already-canonical turn content without changing a code point.

    Tendwire owns canonical public sanitization.  Herdres may validate the
    resulting value's type, but must not trim, normalize, redact again, or apply
    the generic public-response size cap while planning Telegram presentation.
    """
    if value is None:
        return ""
    if not isinstance(value, str):
        raise TypeError(f"{field} must be a string")
    return value


def html_escape(value: Any, limit: int = 4000) -> str:
    return html.escape(sanitize_text(value, limit), quote=False)


def short_hash(value: Any, length: int = 16) -> str:
    encoded = json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()[: max(4, int(length))]


def compact_ws(value: Any, limit: int = 160) -> str:
    return re.sub(r"\s+", " ", sanitize_text(value, limit * 4)).strip()[:limit]


def normalized_public_key(value: Any) -> str:
    return re.sub(r"[^a-z0-9]+", "", str(value).lower())


def mapping_has_private_agent_discriminator(value: dict[Any, Any]) -> bool:
    """Recognize exact ACP event envelopes without scanning prose values."""

    normalized_items = [
        (normalized_public_key(key), item) for key, item in value.items()
    ]
    if any(
        key == "sessionupdate"
        and isinstance(item, str)
        and normalized_public_key(item) in ACP_SESSION_UPDATE_KINDS
        for key, item in normalized_items
    ):
        return True

    # A raw ACP JSON-RPC notification/request is private even when a malformed
    # or future peer omits the tool-call field that the recursive key checks
    # below would otherwise recognize.  Requiring both method and params avoids
    # treating ordinary prose metadata that merely names a protocol method as
    # an ACP envelope.
    keys = {key for key, _item in normalized_items}
    if "params" in keys and any(
        key == "method"
        and isinstance(item, str)
        and item.strip().lower()
        in {"session/update", "session/request_permission"}
        for key, item in normalized_items
    ):
        return True

    kind_names = {
        normalized_public_key(item)
        for key, item in normalized_items
        if key == "kind" and isinstance(item, str)
    }
    if not kind_names & ACP_AGENT_EVENT_KINDS or "payload" not in keys:
        return False
    return any(
        key == "visibility"
        and isinstance(item, str)
        and item.strip().lower() == "private"
        for key, item in normalized_items
    ) or any(
        marker in keys
        for marker in (
            "eventid",
            "sourceeventid",
            "sourcesessionid",
            "sourcesequence",
        )
    )


def public_prune(
    value: Any,
    *,
    _depth: int = 0,
    _budget: list[int] | None = None,
    _seen: set[int] | None = None,
) -> Any:
    if _budget is None:
        _budget = [PRIVATE_STRUCTURE_MAX_ITEMS]
    if _seen is None:
        _seen = set()
    _budget[0] -= 1
    if _budget[0] < 0 or _depth > PRIVATE_STRUCTURE_MAX_DEPTH:
        return None
    if isinstance(value, dict):
        identity = id(value)
        if identity in _seen:
            return None
        _seen.add(identity)
        if mapping_has_private_agent_discriminator(value):
            return {}
        result: dict[str, Any] = {}
        for key, item in value.items():
            clean_key = str(key)
            normalized_key = normalized_public_key(clean_key)
            if (
                clean_key.lower() == "_meta"
                or clean_key in FORBIDDEN_PUBLIC_KEYS
                or normalized_key in PRIVATE_AGENT_CONTAINER_FIELD_NAMES
                or normalized_key in PRIVATE_AGENT_FIELD_NAMES
                or "token" in clean_key.lower()
                or "secret" in clean_key.lower()
            ):
                continue
            result[clean_key] = public_prune(
                item,
                _depth=_depth + 1,
                _budget=_budget,
                _seen=_seen,
            )
        return result
    if isinstance(value, list):
        identity = id(value)
        if identity in _seen:
            return None
        _seen.add(identity)
        return [
            public_prune(
                item,
                _depth=_depth + 1,
                _budget=_budget,
                _seen=_seen,
            )
            for item in value
        ]
    if isinstance(value, str):
        return sanitize_text(value, PRUNE_TEXT_LIMIT)
    return value
