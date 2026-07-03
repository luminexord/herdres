"""Source-only Herdres diagnostics."""

from __future__ import annotations

import sqlite3
import subprocess
from pathlib import Path
from typing import Any

from . import config
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


def run_doctor(client: TendwireClient | None = None) -> dict[str, Any]:
    checks = {
        "source_services": source_services(),
        "legacy_topic_timer": legacy_timer(),
        "sqlite_integrity": sqlite_integrity(),
        "tendwire_backend": tendwire_backend(client),
    }
    return {"ok": all(item.get("ok") for item in checks.values()), "checks": checks}
