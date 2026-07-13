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
from .safe import FORBIDDEN_PUBLIC_KEYS, PRUNE_TEXT_LIMIT, public_prune, sanitize_text


class TendwireError(RuntimeError):
    pass


TURN_SCHEMA_VERSION = 2
TURN_CONTENT_SCHEMA_VERSION = 1
CONNECTOR_PREPARE_SCHEMA_VERSION = 1
TURN_FINAL_CONNECTOR = "turn-final"
CONNECTOR_PREPARE_MAX_SPANS = 256
CONNECTOR_PREPARE_MAX_REQUEST_BYTES = 64 * 1024
_PUBLIC_PROTOCOL_TOKEN_KEYS = {
    "failed_plan_token",
    "plan_token",
    "replaces_plan_token",
}


def _protocol_prune(value: Any) -> Any:
    """Prune public protocol metadata while retaining its opaque public tokens."""
    if isinstance(value, dict):
        result: dict[str, Any] = {}
        for key, item in value.items():
            clean_key = str(key)
            forbidden = clean_key in FORBIDDEN_PUBLIC_KEYS or "secret" in clean_key.lower()
            token_key = "token" in clean_key.lower()
            if forbidden or (token_key and clean_key not in _PUBLIC_PROTOCOL_TOKEN_KEYS):
                continue
            result[clean_key] = _protocol_prune(item)
        return result
    if isinstance(value, list):
        return [_protocol_prune(item) for item in value]
    if isinstance(value, str):
        return sanitize_text(value, PRUNE_TEXT_LIMIT)
    return value


def _schema_error(
    status: str,
    message: str,
    *,
    received: Any,
    required_key: str,
    required_version: int,
) -> dict[str, Any]:
    return {
        "ok": False,
        "status": status,
        "error": message,
        required_key: required_version,
        "received_schema_version": received,
    }


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

    def call(
        self,
        args: list[str],
        *,
        input_json: dict[str, Any] | None = None,
        timeout: float | None = None,
        protocol: bool = False,
        preserve_page_text: bool = False,
    ) -> dict[str, Any]:
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
        try:
            stdout = proc.stdout.decode("utf-8") if preserve_page_text else proc.stdout.decode("utf-8", "replace")
        except UnicodeDecodeError:
            return {"ok": False, "status": "invalid_utf8_stdout", "error": "Tendwire returned invalid UTF-8"}
        stderr = proc.stderr.decode("utf-8", "replace")
        try:
            data = json.loads(stdout or "{}")
        except json.JSONDecodeError:
            detail = sanitize_text(stderr or stdout or "non-json Tendwire response", 300)
            return {"ok": False, "status": "non_json_stdout", "error": detail}
        if not isinstance(data, dict):
            return {"ok": False, "status": "non_object_json", "error": "Tendwire returned non-object JSON"}

        page_text: str | None = None
        prune_source = data
        if preserve_page_text and isinstance(data.get("text"), str):
            page_text = data["text"]
            prune_source = dict(data)
            del prune_source["text"]
        clean = _protocol_prune(prune_source) if protocol else public_prune(prune_source)
        if page_text is not None:
            clean["text"] = page_text
        if proc.returncode != 0 and data.get("ok") is not True:
            clean.setdefault("ok", False)
            clean.setdefault("status", "nonzero_exit")
            clean.setdefault("error", sanitize_text(stderr or data.get("error") or "Tendwire command failed", 300))
        return clean

    def snapshot(self) -> dict[str, Any]:
        return self.call(["snapshot", "--json"])

    def turns(self) -> dict[str, Any]:
        result = self.call(["turns", "--schema-version", str(TURN_SCHEMA_VERSION), "--json"])
        if result.get("ok") is False:
            return result
        received = result.get("schema_version")
        if type(received) is not int or received != TURN_SCHEMA_VERSION:
            return _schema_error(
                "upgrade_required",
                "Tendwire turn.list schema v2 is required",
                received=received,
                required_key="required_turn_schema_version",
                required_version=TURN_SCHEMA_VERSION,
            )
        turns = result.get("turns")
        if not isinstance(turns, list):
            return _schema_error(
                "unsupported_content_schema",
                "Tendwire turn.list v2 must contain a turns list",
                received=None,
                required_key="supported_content_schema_version",
                required_version=TURN_CONTENT_SCHEMA_VERSION,
            )
        for row in turns:
            content = row.get("content") if isinstance(row, dict) else None
            content_schema = content.get("schema_version") if isinstance(content, dict) else None
            if type(content_schema) is not int or content_schema != TURN_CONTENT_SCHEMA_VERSION:
                return _schema_error(
                    "unsupported_content_schema",
                    "Every Tendwire turn.list v2 row requires content schema v1",
                    received=content_schema,
                    required_key="supported_content_schema_version",
                    required_version=TURN_CONTENT_SCHEMA_VERSION,
                )
        return result

    def turn_content_get(
        self,
        turn_id: str,
        content_revision: str,
        field: str,
        cursor: str | None = None,
    ) -> dict[str, Any]:
        args = [
            "turn",
            "content",
            "get",
            "--json",
            "--turn-id",
            str(turn_id),
            "--revision",
            str(content_revision),
            "--field",
            str(field),
        ]
        if cursor is not None:
            args.extend(["--cursor", str(cursor)])
        result = self.call(args, timeout=10, protocol=True, preserve_page_text=True)
        if result.get("ok") is False:
            return result
        received = result.get("schema_version")
        if type(received) is not int or received != TURN_CONTENT_SCHEMA_VERSION or not isinstance(result.get("text"), str):
            return _schema_error(
                "unsupported_content_schema",
                "Tendwire turn.content.get schema v1 with exact text is required",
                received=received,
                required_key="supported_content_schema_version",
                required_version=TURN_CONTENT_SCHEMA_VERSION,
            )
        return result

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

    @staticmethod
    def _validate_protocol_schema(result: dict[str, Any]) -> dict[str, Any]:
        if result.get("ok") is False:
            return result
        received = result.get("schema_version")
        if type(received) is not int or received != CONNECTOR_PREPARE_SCHEMA_VERSION:
            return _schema_error(
                "unsupported_content_schema",
                "Tendwire connector schema v1 is required",
                received=received,
                required_key="supported_content_schema_version",
                required_version=CONNECTOR_PREPARE_SCHEMA_VERSION,
            )
        return result

    def _connector_prepare(self, request: dict[str, Any]) -> dict[str, Any]:
        encoded = json.dumps(request, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
        if len(encoded) > CONNECTOR_PREPARE_MAX_REQUEST_BYTES:
            return {
                "ok": False,
                "status": "prepare_request_too_large",
                "error": "connector.prepare request exceeds the Herdres client bound",
                "max_request_bytes": CONNECTOR_PREPARE_MAX_REQUEST_BYTES,
            }
        result = self.call(
            ["connector", "prepare", "--name", TURN_FINAL_CONNECTOR, "--json"],
            input_json=request,
            timeout=10,
            protocol=True,
        )
        return self._validate_protocol_schema(result)

    def connector_prepare_begin(
        self,
        *,
        turn_id: str,
        content_revision: str,
        presentation_version: str,
        part_count: int,
        source_ref: str | None = None,
    ) -> dict[str, Any]:
        request: dict[str, Any] = {
            "schema_version": CONNECTOR_PREPARE_SCHEMA_VERSION,
            "action": "begin",
            "name": TURN_FINAL_CONNECTOR,
            "turn_id": str(turn_id),
            "content_revision": str(content_revision),
            "presentation_version": str(presentation_version),
            "part_count": part_count,
        }
        if source_ref is not None:
            request["source_ref"] = str(source_ref)
        return self._connector_prepare(request)

    def connector_prepare_part(
        self,
        *,
        plan_token: str,
        ordinal: int,
        spans: list[dict[str, Any]],
    ) -> dict[str, Any]:
        if not isinstance(spans, list) or not spans or len(spans) > CONNECTOR_PREPARE_MAX_SPANS:
            return {
                "ok": False,
                "status": "invalid_prepare_part",
                "error": f"spans must contain 1..{CONNECTOR_PREPARE_MAX_SPANS} canonical ranges",
            }
        ranges: list[dict[str, Any]] = []
        for span in spans:
            if not isinstance(span, dict) or set(span) != {"field", "start_char", "end_char"}:
                return {
                    "ok": False,
                    "status": "invalid_prepare_part",
                    "error": "each span must contain only field, start_char, and end_char",
                }
            field = span.get("field")
            start_char = span.get("start_char")
            end_char = span.get("end_char")
            if (
                field not in {"user_text", "assistant_final_text"}
                or type(start_char) is not int
                or type(end_char) is not int
                or start_char < 0
                or end_char <= start_char
            ):
                return {
                    "ok": False,
                    "status": "invalid_prepare_part",
                    "error": "span must be a non-empty canonical user/final range",
                }
            ranges.append({"field": field, "start_char": start_char, "end_char": end_char})
        return self._connector_prepare(
            {
                "schema_version": CONNECTOR_PREPARE_SCHEMA_VERSION,
                "action": "part",
                "name": TURN_FINAL_CONNECTOR,
                "plan_token": str(plan_token),
                "ordinal": ordinal,
                "spans": ranges,
            }
        )

    def connector_prepare_commit(
        self, *, plan_token: str, source_ref: str | None = None
    ) -> dict[str, Any]:
        request = {
            "schema_version": CONNECTOR_PREPARE_SCHEMA_VERSION,
            "action": "commit",
            "name": TURN_FINAL_CONNECTOR,
            "plan_token": str(plan_token),
        }
        if source_ref is not None:
            request["source_ref"] = str(source_ref)
        return self._connector_prepare(request)
    def connector_prepare_recover(
        self,
        *,
        failed_plan_token: str,
        request_id: str,
    ) -> dict[str, Any]:
        """Request one explicit replacement generation for an exhausted plan."""
        if (
            not isinstance(failed_plan_token, str)
            or not failed_plan_token.startswith("twplan1.")
            or len(failed_plan_token) > 264
            or not failed_plan_token[8:]
            or not all(
                char.isascii() and (char.isalnum() or char in "_-")
                for char in failed_plan_token[8:]
            )
        ):
            return {
                "ok": False,
                "status": "invalid_recovery_request",
                "error": "failed_plan_token must be an opaque turn-final plan token",
            }
        if (
            not isinstance(request_id, str)
            or not 1 <= len(request_id) <= 128
            or not all(
                char.isascii() and (char.isalnum() or char in "._:-")
                for char in request_id
            )
        ):
            return {
                "ok": False,
                "status": "invalid_recovery_request",
                "error": "request_id must be 1..128 public-safe ASCII characters",
            }
        return self._connector_prepare(
            {
                "schema_version": CONNECTOR_PREPARE_SCHEMA_VERSION,
                "action": "recover",
                "name": TURN_FINAL_CONNECTOR,
                "failed_plan_token": failed_plan_token,
                "request_id": request_id,
            }
        )

    def turn_final_poll(self, *, limit: int = 1, lease_seconds: int = 60) -> dict[str, Any]:
        result = self.call(
            [
                "connector",
                "poll",
                "--name",
                TURN_FINAL_CONNECTOR,
                "--limit",
                str(limit),
                "--lease-seconds",
                str(lease_seconds),
            ],
            timeout=10,
            protocol=True,
        )
        return self._validate_protocol_schema(result)

    def turn_final_ack(self, ref: str, response: dict[str, Any] | None = None) -> dict[str, Any]:
        args = ["connector", "ack", "--name", TURN_FINAL_CONNECTOR, "--ref", str(ref)]
        if response is not None:
            args.extend(
                [
                    "--response-json",
                    json.dumps(_protocol_prune(response), separators=(",", ":"), ensure_ascii=False),
                ]
            )
        return self._validate_protocol_schema(self.call(args, timeout=10, protocol=True))

    def turn_final_fail(self, ref: str, reason: str) -> dict[str, Any]:
        result = self.call(
            [
                "connector",
                "fail",
                "--name",
                TURN_FINAL_CONNECTOR,
                "--ref",
                str(ref),
                "--reason",
                sanitize_text(reason, 240),
            ],
            timeout=10,
            protocol=True,
        )
        return self._validate_protocol_schema(result)

    def turn_final_defer(
        self,
        ref: str,
        reason: str = "",
        *,
        available_at: str | None = None,
        delay_seconds: int | None = None,
    ) -> dict[str, Any]:
        args = [
            "connector",
            "defer",
            "--name",
            TURN_FINAL_CONNECTOR,
            "--ref",
            str(ref),
            "--reason",
            sanitize_text(reason, 240),
        ]
        if available_at is not None:
            args.extend(["--available-at", str(available_at)])
        if delay_seconds is not None:
            args.extend(["--delay-seconds", str(delay_seconds)])
        return self._validate_protocol_schema(self.call(args, timeout=10, protocol=True))
