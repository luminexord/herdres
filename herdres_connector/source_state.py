"""Source-mode turn delivery state helpers."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class SourceTurnRuntime:
    delivery_seen: Callable[[dict[str, Any] | None, dict[str, Any] | None, dict[str, Any] | None, dict[str, Any] | None], bool]
    record_suppressed: Callable[[dict[str, Any], dict[str, Any], str], None]
    record_identity: Callable[[dict[str, Any], dict[str, Any]], bool]
    note_delivery: Callable[[dict[str, Any] | None, dict[str, Any] | None, dict[str, Any] | None, dict[str, Any] | None], bool]


def already_clean_delivered(entry: dict[str, Any], item: dict[str, Any] | None) -> bool:
    if not isinstance(item, dict) or str(item.get("kind") or "").lower() != "turn":
        return False
    turn_id = str(item.get("turn_id") or "").strip()
    if not turn_id:
        return False
    if turn_id != str(entry.get("last_turn_id") or "").strip():
        return False
    if not str(entry.get("last_clean_message_id") or "").strip():
        return False
    last_item = entry.get("last_clean_item") if isinstance(entry.get("last_clean_item"), dict) else {}
    last_kind = str(entry.get("last_clean_kind") or last_item.get("kind") or "turn").lower()
    return last_kind == "turn"


def suppress_globally_delivered_turn(
    state: dict[str, Any] | None,
    pane: dict[str, Any] | None,
    entry: dict[str, Any],
    item: dict[str, Any] | None,
    *,
    runtime: SourceTurnRuntime,
) -> bool:
    if not isinstance(item, dict) or str(item.get("kind") or "").lower() != "turn":
        return False
    if runtime.delivery_seen(state, pane, entry, item):
        runtime.record_suppressed(entry, item, "source_turn_global_delivered")
        runtime.record_identity(entry, item)
        if item.get("turn_id"):
            entry["last_turn_id"] = str(item.get("turn_id") or "")
        runtime.note_delivery(state, pane, entry, item)
        return True
    if already_clean_delivered(entry, item):
        runtime.record_suppressed(entry, item, "source_turn_already_delivered")
        runtime.record_identity(entry, item)
        runtime.note_delivery(state, pane, entry, item)
        return True
    return False

