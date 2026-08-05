"""Pure managed-receiver kind and username normalization."""

from __future__ import annotations

import re
# H8-SLOC-BEGIN managed-typing-imports
from collections.abc import Iterable, Mapping
from typing import Protocol, TypeVar
# H8-SLOC-END managed-typing-imports


MANAGER_BOT_KIND = "manager"
_MANAGED_KIND_ORDER = ("codex", "claude", "glm", "kimi", "omp", "devin")
MANAGED_BOT_KINDS = frozenset(_MANAGED_KIND_ORDER)
_ENTRY_KIND_FIELDS = (
    "agent",
    "worker_name",
    "active_worker_name",
    "topic_name",
    "space_topic_name",
    "tendwire_worker_id",
    "worker_id",
)


# H8-SLOC-BEGIN managed-input-protocols
class ReceiverInput(Protocol):
    receiver_id: str
    bot_kind: str
    username: str


class PolicyInput(Protocol):
    managed_usernames: tuple[tuple[str, str], ...]


ReceiverT = TypeVar("ReceiverT", bound=ReceiverInput)
# H8-SLOC-END managed-input-protocols


def normalize_bot_kind(value: object, *, allow_manager: bool = True) -> str:
    """Return one canonical configured kind, or an empty string."""

    kind = str(value or "").strip().lower()
    allowed = MANAGED_BOT_KINDS | ({MANAGER_BOT_KIND} if allow_manager else set())
    return kind if kind in allowed else ""


def normalize_username(value: object) -> str:
    username = str(value or "").strip().lstrip("@").lower()
    return username if username and "@" not in username and not username.isspace() else ""


def managed_bot_kind_for_agent(value: str | None) -> str:
    words = re.sub(r"[^a-z0-9]+", " ", str(value or "").lower()).split()
    return next((kind for kind in _MANAGED_KIND_ORDER if kind in words), "")


def managed_bot_kind_for_entry(entry: Mapping[str, object] | None) -> str:
    if not isinstance(entry, Mapping):
        return ""
    explicit = normalize_bot_kind(entry.get("bot_kind"), allow_manager=False)
    if explicit:
        return explicit
    for field in _ENTRY_KIND_FIELDS:
        kind = managed_bot_kind_for_agent(str(entry.get(field) or ""))
        if kind:
            return kind
    return ""


def managed_bot_kind_for_username(policy: PolicyInput, username: str) -> str:
    wanted = normalize_username(username)
    if not wanted:
        return ""
    for configured, raw_kind in policy.managed_usernames:
        kind = normalize_bot_kind(raw_kind, allow_manager=False)
        if kind and normalize_username(configured) == wanted:
            return kind
    return ""


# H8-SLOC-BEGIN managed-receiver-lookups
def receiver_for_id(
    receivers: Iterable[ReceiverT], receiver_id: str
) -> ReceiverT | None:
    wanted = str(receiver_id or "").strip()
    matches = [receiver for receiver in receivers if receiver.receiver_id == wanted]
    return matches[0] if len(matches) == 1 else None


def receiver_for_kind(
    receivers: Iterable[ReceiverT], bot_kind: str
) -> ReceiverT | None:
    wanted = normalize_bot_kind(bot_kind)
    matches = [
        receiver
        for receiver in receivers
        if normalize_bot_kind(receiver.bot_kind) == wanted
    ]
    return matches[0] if wanted and len(matches) == 1 else None


def receiver_kind(receiver: ReceiverInput | None) -> str:
    return normalize_bot_kind(receiver.bot_kind if receiver is not None else "")
# H8-SLOC-END managed-receiver-lookups
