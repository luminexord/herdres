"""Doctor/service helpers for Herdres Tendwire source mode."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any
import os
import subprocess

import herdres_tendwire


def source_services_doctor(
    *,
    fix: bool = False,
    env: Any | None = None,
    is_macos: Callable[[], bool],
    runner: Callable[..., subprocess.CompletedProcess[str]],
    sanitize: Callable[[str, int], str],
    warn_invalid: Callable[[Any], None],
) -> dict[str, Any]:
    return herdres_tendwire.source_services_doctor(
        fix=fix,
        env=os.environ if env is None else env,
        is_macos=is_macos,
        runner=runner,
        sanitize=sanitize,
        warn_invalid=warn_invalid,
    )


def legacy_topic_timer_doctor(
    *,
    fix: bool = False,
    env: Any | None = None,
    is_macos: Callable[[], bool],
    runner: Callable[..., subprocess.CompletedProcess[str]],
    sanitize: Callable[[str, int], str],
    warn_invalid: Callable[[Any], None],
) -> dict[str, Any]:
    return herdres_tendwire.legacy_topic_timer_doctor(
        fix=fix,
        env=os.environ if env is None else env,
        is_macos=is_macos,
        runner=runner,
        sanitize=sanitize,
        warn_invalid=warn_invalid,
    )


def doctor_report(
    *,
    legacy_timer: dict[str, Any],
    source_services: dict[str, Any],
    tendwire_backend: dict[str, Any],
    sqlite_integrity: dict[str, Any],
) -> dict[str, Any]:
    return {
        "ok": (
            bool(legacy_timer.get("ok"))
            and bool(source_services.get("ok"))
            and bool(tendwire_backend.get("ok"))
            and bool(sqlite_integrity.get("ok"))
        ),
        "checks": {
            "legacy_topic_timer": legacy_timer,
            "source_services": source_services,
            "tendwire_backend": tendwire_backend,
            "sqlite_integrity": sqlite_integrity,
        },
    }

