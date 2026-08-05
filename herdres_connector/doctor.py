"""Source-only Herdres diagnostics."""

from __future__ import annotations

import sqlite3
import subprocess
import time
from dataclasses import asdict
from pathlib import Path
from typing import Any

from . import config, state
from .ingress_queue import IngressQueue
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
            "state": "error",
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
    if status not in {"active", "bootstrapping"}:
        return {
            "ok": False,
            "state": "error",
            "watermark_age_seconds": None,
            "last_batch": {},
            "health_flag": "invalid_delta_state",
        }
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


def inbound_queue(
    path: Path | None = None,
    *,
    now: float | None = None,
) -> dict[str, Any]:
    """Expose aggregate queue health while the gateway writer is live."""

    db_path = path or config.inbound_spool_path()
    if not db_path.exists():
        return {"ok": True, "status": "bootstrapping"}
    try:
        observed_at = time.time() if now is None else float(now)
        with IngressQueue.observe(db_path) as observer:
            snapshot = observer.health_snapshot(observed_at)
            statuses = observer.status_rows(observed_at)
    except (OSError, RuntimeError, sqlite3.Error, ValueError) as exc:
        return {
            "ok": False,
            "status": "error",
            "signal": "inbound_queue_probe_failed",
            "error": sanitize_text(str(exc), 200),
        }
    attention = (
        snapshot.quarantine
        + snapshot.claimed_notices
        + snapshot.expired_leases
        + snapshot.overdue_open
    )
    return {
        "ok": attention == 0,
        "status": "healthy" if attention == 0 else "attention_required",
        "signal": "" if attention == 0 else "inbound_queue_attention_required",
        "attention_required": attention,
        "health": asdict(snapshot),
        "statuses": [asdict(row) for row in statuses],
    }


def outbound_response_folds(
    store: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Expose durable failures to collapse superseded Responses."""

    try:
        current = state.load_state() if store is None else store
    except RuntimeError as exc:
        return {
            "ok": False,
            "status": "error",
            "signal": "outbound_response_fold_probe_failed",
            "error": sanitize_text(str(exc), 200),
        }
    return state.response_fold_health(current)


def outbound_unbound_live_panes(
    store: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Expose live panes that have no usable owner-visible topic."""

    try:
        current = state.load_state() if store is None else store
    except RuntimeError as exc:
        return {
            "ok": False,
            "status": "error",
            "signal": "outbound_unbound_live_pane_probe_failed",
            "error": sanitize_text(str(exc), 200),
        }
    rows = state.live_unbound_worker_entries(current)
    if not rows:
        return {
            "ok": True,
            "status": "healthy",
            "signal": "",
            "unbound_count": 0,
        }
    key, entry = rows[0]
    return {
        "ok": False,
        "status": "live_panes_unbound",
        "signal": "outbound_live_panes_unbound",
        "unbound_count": len(rows),
        "first_unbound": {
            "entry_key": key,
            "worker_id": str(
                entry.get("tendwire_worker_id")
                or entry.get("worker_id")
                or ""
            ),
            "pane_uuid": str(entry.get("pane_uuid") or ""),
            "binding_state": str(
                entry.get("binding_state") or "pending_create"
            ),
        },
    }


def run_doctor(client: TendwireClient | None = None) -> dict[str, Any]:
    checks = {
        "source_services": source_services(),
        "legacy_topic_timer": legacy_timer(),
        "tendwire_backend": tendwire_backend(client),
        "tendwire_delta_feed": tendwire_delta_feed(),
        "inbound_queue": inbound_queue(),
        "outbound_unbound_live_panes": outbound_unbound_live_panes(),
        "outbound_response_folds": outbound_response_folds(),
    }
    return {"ok": all(item.get("ok") for item in checks.values()), "checks": checks}
