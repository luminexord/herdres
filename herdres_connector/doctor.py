"""Source-only Herdres diagnostics."""

from __future__ import annotations

import sqlite3
import subprocess
import time
from pathlib import Path
from typing import Any

from . import config, state
from .safe import sanitize_text
from .tendwire_client import TendwireClient


def _systemctl_is_active(unit: str) -> dict[str, Any]:
    proc = subprocess.run(["systemctl", "--user", "is-active", unit], capture_output=True, text=True, check=False)
    status = sanitize_text(proc.stdout.strip() or proc.stderr.strip(), 80)
    return {"unit": unit, "active": proc.returncode == 0, "status": status, "returncode": proc.returncode}


def source_services() -> dict[str, Any]:
    services = {unit: _systemctl_is_active(unit) for unit in config.SOURCE_SERVICES}
    return {"ok": all(item["active"] for item in services.values()), "services": services}


def legacy_timer() -> dict[str, Any]:
    status = _systemctl_is_active(config.LEGACY_TIMER)
    return {"ok": not status["active"], "legacy_timer": status}


def sqlite_integrity(path: Path | None = None) -> dict[str, Any]:
    db_path = path or config.tendwire_db_path()
    if not db_path.exists():
        return {"ok": False, "path_configured": True, "status": "missing"}
    try:
        with sqlite3.connect(str(db_path)) as conn:
            row = conn.execute("PRAGMA integrity_check").fetchone()
    except sqlite3.Error as exc:
        return {"ok": False, "status": "error", "error": sanitize_text(str(exc), 200)}
    integrity = str(row[0] if row else "")
    return {"ok": integrity == "ok", "status": "ok" if integrity == "ok" else "failed", "integrity": integrity}


def tendwire_backend(client: TendwireClient | None = None) -> dict[str, Any]:
    data = (client or TendwireClient(timeout=10)).doctor()
    if str(data.get("status") or "").strip().lower() == "ok":
        return {"ok": True, "status": "healthy"}
    if not data.get("ok") and data.get("status"):
        return {"ok": False, "status": data.get("status"), "error": data.get("error", "")}
    health = data.get("backend_health") if isinstance(data.get("backend_health"), list) else []
    ok = any(isinstance(item, dict) and item.get("name") == "herdr" and item.get("status") == "healthy" for item in health)
    return {"ok": bool(ok), "status": "healthy" if ok else "unhealthy"}


def tendwire_delta_feed() -> dict[str, Any]:
    try:
        store = state.load_state()
    except RuntimeError as exc:
        return {
            "ok": False,
            "state": "fallback",
            "watermark_age_seconds": None,
            "last_batch": {},
            "health_flag": sanitize_text(str(exc), 80),
        }
    delta = store.get("tendwire_delta_sync")
    if not isinstance(delta, dict):
        return {
            "ok": True,
            "state": "bootstrapping",
            "watermark_age_seconds": None,
            "last_batch": {},
        }
    status = str(delta.get("status") or "bootstrapping")
    if status not in {"active", "fallback", "bootstrapping"}:
        status = "bootstrapping"
    updated_at = delta.get("watermark_updated_at")
    age: int | None = None
    if isinstance(updated_at, (int, float)) and not isinstance(updated_at, bool):
        age = max(0, int(time.time() - float(updated_at)))
    raw_batch = delta.get("last_batch")
    batch: dict[str, Any] = {}
    if isinstance(raw_batch, dict):
        for key in (
            "mode",
            "changes_returned",
            "upserts",
            "removals",
            "journal_rows_scanned",
            "projection_rows_read",
            "duration_ms",
        ):
            value = raw_batch.get(key)
            if key == "mode" and isinstance(value, str):
                batch[key] = sanitize_text(value, 24)
            elif isinstance(value, int) and not isinstance(value, bool) and value >= 0:
                batch[key] = value
    result: dict[str, Any] = {
        "ok": True,
        "state": status,
        "watermark_age_seconds": age,
        "last_batch": batch,
    }
    flag = delta.get("health_flag")
    if isinstance(flag, str) and flag:
        result["health_flag"] = sanitize_text(flag, 80)
    return result


def run_doctor(client: TendwireClient | None = None) -> dict[str, Any]:
    checks = {
        "source_services": source_services(),
        "legacy_topic_timer": legacy_timer(),
        "sqlite_integrity": sqlite_integrity(),
        "tendwire_backend": tendwire_backend(client),
        "tendwire_delta_feed": tendwire_delta_feed(),
    }
    return {"ok": all(item.get("ok") for item in checks.values()), "checks": checks}
