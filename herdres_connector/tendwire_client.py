"""Tendwire public CLI client for the source-only Herdres connector."""

from __future__ import annotations

import json
import os
import shlex
import shutil
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from . import config
from .safe import public_prune, sanitize_text


class TendwireError(RuntimeError):
    pass


@dataclass(frozen=True)
class TendwireClient:
    timeout: float = 30.0

    def _explicit_parts(self) -> tuple[list[str] | None, dict[str, str]]:
        explicit = os.getenv("HERDRES_TENDWIRE_BIN") or os.getenv("TENDWIRE_BIN")
        if not explicit:
            return None, {}
        parts = shlex.split(os.path.expandvars(os.path.expanduser(explicit)))
        overrides: dict[str, str] = {}
        if parts and parts[0] == "env":
            parts = parts[1:]
            while parts and "=" in parts[0] and not parts[0].startswith("-"):
                key, value = parts.pop(0).split("=", 1)
                overrides[key] = value
        return parts or None, overrides

    def _base(self) -> list[str]:
        explicit, _overrides = self._explicit_parts()
        if explicit:
            return explicit
        found = shutil.which("tendwire")
        if found:
            return [found]
        source = Path(os.getenv("TENDWIRE_SOURCE_DIR", str(Path.home() / "tendwire" / "src"))).expanduser()
        return [sys.executable, "-m", "tendwire.cli"] if source.exists() else ["tendwire"]

    def _env(self) -> dict[str, str]:
        env = os.environ.copy()
        _explicit, overrides = self._explicit_parts()
        env.update(overrides)
        source = Path(os.getenv("TENDWIRE_SOURCE_DIR", str(Path.home() / "tendwire" / "src"))).expanduser()
        if source.exists():
            current = env.get("PYTHONPATH", "")
            env["PYTHONPATH"] = str(source) if not current else f"{source}{os.pathsep}{current}"
        env.setdefault("TENDWIRE_DB_PATH", str(config.tendwire_db_path()))
        env.setdefault("TENDWIRE_HERDR_BACKEND", os.getenv("TENDWIRE_HERDR_BACKEND", "socket"))
        return env

    def call(self, args: list[str], *, input_json: dict[str, Any] | None = None, timeout: float | None = None) -> dict[str, Any]:
        stdin = None
        if input_json is not None:
            stdin = json.dumps(input_json, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
        try:
            proc = subprocess.run(
                [*self._base(), *args],
                input=stdin,
                capture_output=True,
                env=self._env(),
                timeout=timeout or self.timeout,
                check=False,
            )
        except subprocess.TimeoutExpired as exc:
            return {"ok": False, "status": "timeout", "error": f"tendwire {' '.join(args[:2])} timed out after {exc.timeout}s"}
        except Exception as exc:  # noqa: BLE001
            return {"ok": False, "status": "subprocess_failed", "error": sanitize_text(str(exc), 300)}
        stdout = proc.stdout.decode("utf-8", "replace")
        stderr = proc.stderr.decode("utf-8", "replace")
        try:
            data = json.loads(stdout or "{}")
        except json.JSONDecodeError:
            detail = sanitize_text(stderr or stdout or "non-json Tendwire response", 300)
            return {"ok": False, "status": "non_json_stdout", "error": detail}
        if not isinstance(data, dict):
            return {"ok": False, "status": "non_object_json", "error": "Tendwire returned non-object JSON"}
        if proc.returncode != 0 and data.get("ok") is not True:
            clean = public_prune(data)
            clean.setdefault("ok", False)
            clean.setdefault("status", "nonzero_exit")
            clean.setdefault("error", sanitize_text(stderr or data.get("error") or "Tendwire command failed", 300))
            return clean
        return public_prune(data)

    def snapshot(self) -> dict[str, Any]:
        return self.call(["snapshot", "--json"])

    def turns(self) -> dict[str, Any]:
        return self.call(["turns", "--json"])

    def pending(self) -> dict[str, Any]:
        return self.call(["pending", "--json"])

    def doctor(self) -> dict[str, Any]:
        return self.call(["doctor", "--json"], timeout=10)

    def command(self, request: dict[str, Any]) -> dict[str, Any]:
        return self.call(["command", "--json"], input_json=request, timeout=60)

    def connector_poll(self, *, name: str = "attention", limit: int = 3, lease_seconds: int = 60) -> dict[str, Any]:
        return self.call(
            [
                "connector",
                "poll",
                "--db-path",
                str(config.tendwire_db_path()),
                "--name",
                name,
                "--limit",
                str(limit),
                "--lease-seconds",
                str(lease_seconds),
            ],
            timeout=10,
        )

    def connector_ack(self, ref: str, response: dict[str, Any] | None = None, *, name: str = "attention") -> dict[str, Any]:
        args = [
            "connector",
            "ack",
            "--db-path",
            str(config.tendwire_db_path()),
            "--name",
            name,
            "--ref",
            str(ref),
        ]
        if response is not None:
            args.extend(["--response-json", json.dumps(public_prune(response), separators=(",", ":"))])
        return self.call(args, timeout=10)

    def connector_fail(self, ref: str, error: str, *, name: str = "attention") -> dict[str, Any]:
        return self.call(
            [
                "connector",
                "fail",
                "--db-path",
                str(config.tendwire_db_path()),
                "--name",
                name,
                "--ref",
                str(ref),
                "--error",
                sanitize_text(error, 240),
            ],
            timeout=10,
        )
