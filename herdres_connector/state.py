"""State helpers for the source-only Telegram connector."""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

from . import config
from .safe import compact_ws, short_hash


def load_state(path: Path | None = None) -> dict[str, Any]:
    state_file = path or config.state_path()
    if not state_file.exists():
        return {"version": 2, "enabled": True, "telegram": {}, "panes": {}, "spaces": {}}
    try:
        data = json.loads(state_file.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {"version": 2, "enabled": True, "telegram": {}, "panes": {}, "spaces": {}}
    if not isinstance(data, dict):
        return {"version": 2, "enabled": True, "telegram": {}, "panes": {}, "spaces": {}}
    data.setdefault("version", 2)
    data.setdefault("enabled", True)
    data.setdefault("telegram", {})
    data.setdefault("panes", {})
    data.setdefault("spaces", {})
    return data


def save_state(data: dict[str, Any], path: Path | None = None) -> None:
    state_file = path or config.state_path()
    state_file.parent.mkdir(parents=True, exist_ok=True)
    tmp = state_file.with_suffix(state_file.suffix + ".tmp")
    tmp.write_text(json.dumps(data, indent=2, sort_keys=True, ensure_ascii=False) + "\n", encoding="utf-8")
    os.replace(tmp, state_file)


def source_worker_entries(data: dict[str, Any]) -> dict[str, dict[str, Any]]:
    panes = data.get("panes") if isinstance(data.get("panes"), dict) else {}
    return {
        str(key): value
        for key, value in panes.items()
        if isinstance(value, dict)
        and str(value.get("source") or "") == "tendwire"
        and str(value.get("entry_type") or "") == "worker"
    }


def source_space_entries(data: dict[str, Any]) -> dict[str, dict[str, Any]]:
    spaces = data.get("spaces") if isinstance(data.get("spaces"), dict) else {}
    return {
        str(key): value
        for key, value in spaces.items()
        if isinstance(value, dict)
        and str(value.get("source") or "") == "tendwire"
        and str(value.get("entry_type") or "") == "space"
    }


def source_entries(data: dict[str, Any]) -> dict[str, dict[str, Any]]:
    return source_worker_entries(data) if config.source_topic_mode() == "worker" else source_space_entries(data)


def find_entry_key_by_worker(data: dict[str, Any], worker_id: str) -> str | None:
    for key, entry in source_worker_entries(data).items():
        if str(entry.get("tendwire_worker_id") or entry.get("worker_id") or "") == worker_id:
            return key
    return None


def find_entry_key_by_space(data: dict[str, Any], space_id: str) -> str | None:
    for key, entry in source_space_entries(data).items():
        if str(entry.get("tendwire_space_id") or entry.get("space_id") or "") == space_id:
            return key
    return None


def find_space_entry_by_id(data: dict[str, Any], space_id: str | None) -> tuple[str, dict[str, Any]] | tuple[None, None]:
    if not space_id:
        return None, None
    key = find_entry_key_by_space(data, str(space_id))
    if key is None:
        return None, None
    return key, source_space_entries(data).get(key)


def find_entry_by_thread(data: dict[str, Any], thread_id: str | None) -> tuple[str, dict[str, Any]] | tuple[None, None]:
    if not thread_id:
        return None, None
    for key, entry in source_entries(data).items():
        if str(entry.get("topic_id") or "") == str(thread_id):
            return key, entry
    return None, None


def worker_agent(worker: dict[str, Any]) -> str:
    meta = worker.get("meta") if isinstance(worker.get("meta"), dict) else {}
    for key in ("agent", "kind", "bot_kind"):
        value = compact_ws(meta.get(key) or worker.get(key), 40).lower()
        if value:
            return value
    name = compact_ws(worker.get("name") or worker.get("id"), 40).lower()
    for candidate in ("codex", "claude", "kimi", "glm", "omp", "devin"):
        if candidate in name:
            return candidate
    return "agent"


def topic_name_for_worker(worker: dict[str, Any]) -> str:
    meta = worker.get("meta") if isinstance(worker.get("meta"), dict) else {}
    value = compact_ws(meta.get("space_name") or meta.get("cwd_name") or worker.get("name") or worker.get("id"), 120)
    return value or "Worker"


def topic_name_for_space(space: dict[str, Any]) -> str:
    meta = space.get("meta") if isinstance(space.get("meta"), dict) else {}
    value = compact_ws(space.get("name") or meta.get("name") or space.get("id"), 120)
    return value or "Space"


def find_legacy_topic_id_by_name(data: dict[str, Any], name: str) -> str:
    wanted = compact_ws(name, 120).casefold()
    if not wanted:
        return ""
    for entry in source_space_entries(data).values():
        if compact_ws(entry.get("topic_name"), 120).casefold() == wanted and entry.get("topic_id"):
            return str(entry["topic_id"])
    for entry in source_worker_entries(data).values():
        if compact_ws(entry.get("topic_name"), 120).casefold() == wanted and entry.get("topic_id"):
            return str(entry["topic_id"])
    return ""


def upsert_space_entry(data: dict[str, Any], space: dict[str, Any], *, topic_id: str = "") -> tuple[str, dict[str, Any], bool]:
    space_id = compact_ws(space.get("id"), 160)
    fingerprint = compact_ws(space.get("fingerprint"), 160)
    key = find_entry_key_by_space(data, space_id)
    created = False
    if key is None:
        key = f"space:{space_id}:{short_hash(fingerprint or space_id, 10)}"
        created = True
    spaces = data.setdefault("spaces", {})
    entry = spaces.get(key) if isinstance(spaces.get(key), dict) else {}
    topic_name = topic_name_for_space(space)
    entry.update(
        {
            "source": "tendwire",
            "entry_type": "space",
            "tendwire_space_id": space_id,
            "space_id": space_id,
            "tendwire_space_fingerprint": fingerprint,
            "tendwire_status_line": compact_ws(space.get("status_line") or space.get("status"), 240),
            "tendwire_last_seen_at": str(space.get("updated_at") or ""),
            "topic_name": entry.get("topic_name") or topic_name,
        }
    )
    if topic_id:
        entry["topic_id"] = str(topic_id)
    spaces[key] = entry
    return key, entry, created


def upsert_worker_entry(data: dict[str, Any], worker: dict[str, Any], *, topic_id: str = "") -> tuple[str, dict[str, Any], bool]:
    worker_id = compact_ws(worker.get("id"), 160)
    fingerprint = compact_ws(worker.get("fingerprint"), 160)
    space_id = compact_ws(worker.get("space_id"), 160)
    key = find_entry_key_by_worker(data, worker_id)
    created = False
    if key is None:
        key = f"worker:{worker_id}:{short_hash(fingerprint or worker_id, 10)}"
        created = True
    panes = data.setdefault("panes", {})
    entry = panes.get(key) if isinstance(panes.get(key), dict) else {}
    entry.update(
        {
            "source": "tendwire",
            "entry_type": "worker",
            "tendwire_worker_id": worker_id,
            "worker_id": worker_id,
            "tendwire_space_id": space_id,
            "space_id": space_id,
            "tendwire_fingerprint": fingerprint,
            "agent": worker_agent(worker),
            "worker_name": compact_ws(worker.get("name") or worker_id, 80),
            "tendwire_raw_status": compact_ws(worker.get("status"), 80),
            "tendwire_status_line": compact_ws(worker.get("summary") or worker.get("status"), 240),
            "tendwire_last_seen_at": str(worker.get("last_seen_at") or ""),
            "topic_name": entry.get("topic_name") or topic_name_for_worker(worker),
        }
    )
    if topic_id:
        entry["topic_id"] = str(topic_id)
    panes[key] = entry
    return key, entry, created


def delivered_turns(data: dict[str, Any]) -> dict[str, Any]:
    ledger = data.get("tendwire_source_delivered_turns")
    if not isinstance(ledger, dict):
        ledger = {}
        data["tendwire_source_delivered_turns"] = ledger
    return ledger


def mark_delivered(data: dict[str, Any], identity: str, record: dict[str, Any]) -> bool:
    ledger = delivered_turns(data)
    if identity in ledger:
        return False
    ledger[identity] = record
    if len(ledger) > 1000:
        for key in list(ledger)[: len(ledger) - 1000]:
            ledger.pop(key, None)
    return True
