"""Telegram formatting helpers for Tendwire connector items."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any
import html

import herdres_tendwire


def _source_v2_layout(layout: str) -> bool:
    return str(layout or "").strip().lower() == "source_v2"


def attention_notice_text(payload: dict[str, Any], *, sanitize: Callable[[str, int], str], layout: str = "source_v1") -> str:
    attention = payload.get("attention") if isinstance(payload.get("attention"), dict) else {}
    event_type = herdres_tendwire.outbox_event_type(payload, sanitize=sanitize)
    title = "Tendwire attention escalated" if event_type == "attention_escalated" else "Tendwire attention"
    if _source_v2_layout(layout):
        title = f"⚠️ {title}"
    lines = [title]
    for label, key, limit in (
        ("Severity", "severity", 80),
        ("Status", "status", 80),
        ("Kind", "kind", 120),
        ("Reason", "reason", 500),
        ("Updated", "last_changed_at", 80),
        ("Signals", "signal_count", 40),
    ):
        value = attention.get(key) if isinstance(attention, dict) else None
        text = sanitize(str(value or ""), limit).strip()
        if text:
            lines.append(f"{label}: {text}")
    transition_at = sanitize(str(payload.get("transition_at") or ""), 80).strip()
    if transition_at:
        lines.append(f"Observed: {transition_at}")
    return "\n".join(lines)


def attention_notice_html(payload: dict[str, Any], *, sanitize: Callable[[str, int], str], layout: str = "source_v1") -> str:
    plain = attention_notice_text(payload, sanitize=sanitize, layout=layout)
    lines = plain.splitlines()
    if not lines:
        return "<b>Tendwire attention</b>"
    head, rest = lines[0], lines[1:]
    blocks = [f"<h3>{html.escape(head)}</h3>"]
    for line in rest:
        label, sep, value = line.partition(":")
        if sep:
            blocks.append(f"<p><b>{html.escape(label.strip())}</b>: {html.escape(value.strip())}</p>")
        else:
            blocks.append(f"<p>{html.escape(line)}</p>")
    return "".join(blocks)
