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
from .ingress_identity import validate_request_id
from .safe import FORBIDDEN_PUBLIC_KEYS, PRUNE_TEXT_LIMIT, public_prune, sanitize_text


class TendwireError(RuntimeError):
    pass


TURN_SCHEMA_VERSION = 2
TURN_CONTENT_SCHEMA_VERSION = 1
# Use Tendwire's documented maximum row limit. The daemon still enforces its
# fixed response-byte budget, while minimizing subprocess round trips on
# retained-history stores.
TURN_LIST_PAGE_LIMIT = 250
TURN_LIST_MAX_PAGES = 256
TURN_DELTA_SCHEMA_VERSION = 1
TURN_DELTA_PROJECTION_SCHEMA_VERSION = 2
CONNECTOR_PREPARE_SCHEMA_VERSION = 1
TURN_FINAL_CONNECTOR = "turn-final"
CONNECTOR_PREPARE_MAX_SPANS = 256
CONNECTOR_PREPARE_MAX_REQUEST_BYTES = 64 * 1024
CONNECTOR_PROCESS_TIMEOUT_SECONDS = 20
_PUBLIC_PROTOCOL_TOKEN_KEYS = {
    "failed_plan_token",
    "plan_token",
    "replaces_plan_token",
}
_SEND_COMMAND_REQUEST_FIELDS = {
    "schema_version",
    "action",
    "request_id",
    "dry_run",
    "target",
    "instruction",
}
_SEND_COMMAND_V3_REQUEST_FIELDS = _SEND_COMMAND_REQUEST_FIELDS | {
    "response_schema_version"
}
_DECISION_COMMAND_REQUEST_FIELDS = {
    "schema_version",
    "action",
    "request_id",
    "dry_run",
    "target",
    "params",
}
_COMMAND_TARGET_SHAPES = {
    frozenset({"worker_id"}),
    frozenset({"worker_id", "worker_fingerprint"}),
    frozenset({"space_id"}),
    frozenset({"name"}),
    frozenset({"name", "space_id"}),
}
_COMMAND_RESPONSE_FIELDS = {
    "schema_version",
    "action",
    "request_id",
    "ok",
    "dry_run",
    "status",
    "disposition",
    "result",
    "error",
    "warnings",
}
_COMMAND_ACCEPTED_RESULT_FIELDS = frozenset(
    {
        "target",
        "delivery_state",
        "transport_state",
        "target_state_at_send",
        "observed_turn_state",
    }
)
_COMMAND_ACCEPTED_OPTIONAL_RESULT_FIELDS = frozenset(
    {"submission_id", "turn_id"}
)
_COMMAND_ACCEPTED_OBSERVED_TURN_STATES = frozenset(
    {"pending_observation", "observed", "complete", "linked"}
)
_DECISION_ACCEPTED_RESULT_FIELDS = frozenset(
    {
        "target",
        "decision",
        "delivery_state",
        "transport_state",
        "observed_pending_state",
    }
)
_COMMAND_TERMINAL_REJECTION_STATUSES = frozenset(
    {
        "rejected",
        "stale_target",
        "backend_unavailable",
        "backend_unsupported",
        "ambiguous_backend_target",
        "backend_failed",
        "duplicate_request",
    }
)
_COMMAND_PRE_RECEIPT_STATUSES = frozenset(
    {
        "invalid_request",
        "rejected",
        "not_found",
        "ambiguous_target",
        "stale_target",
        "backend_unavailable",
        "backend_unsupported",
        "ambiguous_backend_target",
        "backend_failed",
    }
)
_COMMAND_DISPOSITIONS = frozenset(
    {
        "no_receipt",
        "in_progress",
        "terminal_accepted",
        "terminal_rejected",
        "terminal_uncertain",
    }
)
_DECISION_FAILURE_STATUSES = frozenset(
    {
        "decision_not_pending",
        "invalid_selection",
        "unsupported_decision",
        "unknown_worker",
    }
)
_DECISION_IN_PROGRESS_STATUS = "answer_in_progress"
_PRIVATE_INGRESS_ENV_KEYS = frozenset(
    {
        "BOT_TOKEN",
        "HERDRES_ENV_FILE",
        "HERDRES_OUTBOUND_BOT_TOKEN",
        "TELEGRAM_BOT_TOKEN",
        "HERDRES_TENDWIRE_BIN",
        "TENDWIRE_BIN",
    }
)
_PROCESS_NOT_STARTED = object()
_PROCESS_AMBIGUITY = object()


class _CommandProcessResult(dict[str, Any]):
    """A JSON-safe command result with identity-only private process evidence."""

    __slots__ = (
        "_process_ambiguity",
        "_process_not_started",
        "_process_returncode",
    )


def command_process_ambiguous(result: Any) -> bool:
    """Return whether a command result carries private post-start ambiguity."""
    return (
        isinstance(result, _CommandProcessResult)
        and getattr(result, "_process_ambiguity", None) is _PROCESS_AMBIGUITY
    )


def command_process_not_started(result: Any) -> bool:
    """Return whether a command result carries definite process-spawn failure."""
    return (
        isinstance(result, _CommandProcessResult)
        and getattr(result, "_process_not_started", None) is _PROCESS_NOT_STARTED
    )


def _invalid_command_request() -> dict[str, Any]:
    return {
        "ok": False,
        "status": "invalid_request",
        "error": "Herdres command request is not an exact public command object",
    }


def _request_state_uncertain(request: dict[str, Any] | None = None) -> dict[str, Any]:
    result = _CommandProcessResult(
        {
            "ok": False,
            "status": "request_state_uncertain",
            "error": "Tendwire command result was lost after request start",
        }
    )
    result._process_ambiguity = _PROCESS_AMBIGUITY
    if isinstance(request, dict):
        if isinstance(request.get("request_id"), str):
            result["request_id"] = request["request_id"]
        if request.get("action") in {"send_instruction", "answer_decision"}:
            result["action"] = request["action"]
    return result


def _exact_send_command_request(request: Any) -> dict[str, Any] | None:
    fields = set(request) if isinstance(request, dict) else set()
    if (
        not isinstance(request, dict)
        or (
            fields != _SEND_COMMAND_REQUEST_FIELDS
            and fields != _SEND_COMMAND_V3_REQUEST_FIELDS
        )
    ):
        return None
    if type(request.get("schema_version")) is not int or request["schema_version"] != 1:
        return None
    if request.get("action") != "send_instruction" or request.get("dry_run") is not False:
        return None
    if (
        "response_schema_version" in request
        and request.get("response_schema_version") != 3
    ):
        return None
    try:
        validate_request_id(request.get("request_id"))
    except ValueError:
        return None
    target = request.get("target")
    if not isinstance(target, dict) or frozenset(target) not in _COMMAND_TARGET_SHAPES:
        return None
    if any(not isinstance(value, str) or not value.strip() for value in target.values()):
        return None
    instruction = request.get("instruction")
    if (
        not isinstance(instruction, dict)
        or set(instruction) != {"text"}
        or not isinstance(instruction.get("text"), str)
        or not instruction["text"]
    ):
        return None
    return request


def _exact_decision_command_request(request: Any) -> dict[str, Any] | None:
    if not isinstance(request, dict) or set(request) != _DECISION_COMMAND_REQUEST_FIELDS:
        return None
    if type(request.get("schema_version")) is not int or request["schema_version"] != 1:
        return None
    if request.get("action") != "answer_decision" or request.get("dry_run") is not False:
        return None
    try:
        validate_request_id(request.get("request_id"))
    except ValueError:
        return None
    target = request.get("target")
    if (
        not isinstance(target, dict)
        or set(target) != {"worker_id"}
        or not isinstance(target.get("worker_id"), str)
        or not target["worker_id"].strip()
    ):
        return None
    params = request.get("params")
    if not isinstance(params, dict) or set(params) != {"decision_ref", "selection"}:
        return None
    if not isinstance(params.get("decision_ref"), str) or not params["decision_ref"].strip():
        return None
    selection = params.get("selection")
    if not isinstance(selection, dict):
        return None
    if set(selection) == {"text"}:
        return (
            request
            if isinstance(selection.get("text"), str) and bool(selection["text"].strip())
            else None
        )
    if set(selection) != {"option_refs"} or not isinstance(selection.get("option_refs"), list):
        return None
    refs = selection["option_refs"]
    if any(not isinstance(value, str) or not value.strip() for value in refs):
        return None
    if len(set(refs)) != len(refs):
        return None
    return request


def _exact_public_command_request(request: Any) -> dict[str, Any] | None:
    if not isinstance(request, dict):
        return None
    if request.get("action") == "answer_decision":
        return _exact_decision_command_request(request)
    return _exact_send_command_request(request)


def _valid_accepted_command_result(
    value: Any,
    request: dict[str, Any],
    *,
    response_schema_version: int,
) -> bool:
    if not isinstance(value, dict):
        return False
    fields = set(value)
    if not _COMMAND_ACCEPTED_RESULT_FIELDS <= fields or not fields <= (
        _COMMAND_ACCEPTED_RESULT_FIELDS
        | _COMMAND_ACCEPTED_OPTIONAL_RESULT_FIELDS
    ):
        return False
    turn_id = value.get("turn_id")
    if "turn_id" in value:
        if turn_id is None:
            if response_schema_version != 3:
                return False
        elif not isinstance(turn_id, str) or not turn_id.strip():
            return False
    submission_id = value.get("submission_id")
    if "submission_id" in value and (
        not isinstance(submission_id, str) or not submission_id.strip()
    ):
        return False
    if response_schema_version == 2 and "submission_id" in value:
        return False
    target = value.get("target")
    if not isinstance(target, dict) or set(target) != {"worker_id"}:
        return False
    worker_id = target.get("worker_id")
    requested_worker_id = request["target"].get("worker_id")
    return (
        isinstance(worker_id, str)
        and bool(worker_id.strip())
        and (
            not isinstance(requested_worker_id, str)
            or worker_id == requested_worker_id
        )
        and value.get("delivery_state") == "submitted"
        and value.get("transport_state") == "submitted"
        and isinstance(value.get("target_state_at_send"), str)
        and bool(value["target_state_at_send"].strip())
        and value.get("observed_turn_state")
        in _COMMAND_ACCEPTED_OBSERVED_TURN_STATES
    )


def _validated_command_response(
    response: dict[str, Any],
    request: dict[str, Any],
) -> dict[str, Any] | None:
    if (
        set(response) != _COMMAND_RESPONSE_FIELDS
        or type(response.get("schema_version")) is not int
        or response["schema_version"] not in {2, 3}
        or (
            response["schema_version"] == 3
            and request.get("response_schema_version") != 3
        )
        or response.get("request_id") != request["request_id"]
        or response.get("action") != "send_instruction"
        or response.get("dry_run") is not False
        or type(response.get("ok")) is not bool
        or not isinstance(response.get("status"), str)
        or not response["status"]
        or response.get("disposition") not in _COMMAND_DISPOSITIONS
        or (
            response.get("result") is not None
            and not isinstance(response.get("result"), dict)
        )
        or (
            response.get("error") is not None
            and not isinstance(response.get("error"), dict)
        )
        or not isinstance(response.get("warnings"), list)
        or any(not isinstance(item, str) for item in response["warnings"])
        or public_prune(response) != response
    ):
        return None

    status = response["status"]
    disposition = response["disposition"]
    if disposition == "terminal_accepted":
        if (
            status != "accepted"
            or response["ok"] is not True
            or response.get("error") is not None
            or not _valid_accepted_command_result(
                response.get("result"),
                request,
                response_schema_version=response["schema_version"],
            )
        ):
            return None
        return response

    error = response.get("error")
    if (
        response["ok"] is not False
        or not isinstance(error, dict)
        or (error.get("code") is not None and error.get("code") != status)
        or not isinstance(error.get("message"), str)
        or not error["message"]
    ):
        return None
    if disposition == "in_progress":
        return response if status == "pending" else None
    if disposition == "terminal_uncertain":
        return response if status == "request_state_uncertain" else None
    if disposition == "terminal_rejected":
        return (
            response
            if status in _COMMAND_TERMINAL_REJECTION_STATUSES
            else None
        )
    if disposition == "no_receipt":
        return response if status in _COMMAND_PRE_RECEIPT_STATUSES else None
    return None


def _validated_decision_response(
    response: dict[str, Any],
    request: dict[str, Any],
) -> dict[str, Any] | None:
    if (
        set(response) != _COMMAND_RESPONSE_FIELDS
        or type(response.get("schema_version")) is not int
        or response["schema_version"] != 2
        or response.get("request_id") != request["request_id"]
        or response.get("action") != "answer_decision"
        or response.get("dry_run") is not False
        or type(response.get("ok")) is not bool
        or not isinstance(response.get("status"), str)
        or not response["status"]
        or response.get("disposition") not in _COMMAND_DISPOSITIONS
        or not isinstance(response.get("warnings"), list)
        or any(not isinstance(item, str) for item in response["warnings"])
        or public_prune(response) != response
    ):
        return None
    status = response["status"]
    disposition = response["disposition"]
    if response["ok"] is True:
        result = response.get("result")
        target = result.get("target") if isinstance(result, dict) else None
        decision = result.get("decision") if isinstance(result, dict) else None
        if (
            status != "accepted"
            or disposition != "terminal_accepted"
            or response.get("error") is not None
            or not isinstance(result, dict)
            or set(result) != _DECISION_ACCEPTED_RESULT_FIELDS
            or not isinstance(target, dict)
            or set(target) != {"worker_id"}
            or not isinstance(target.get("worker_id"), str)
            or not target["worker_id"].strip()
            or target["worker_id"] != request["target"]["worker_id"]
            or not isinstance(decision, dict)
            or set(decision) != {"decision_ref"}
            or decision.get("decision_ref") != request["params"]["decision_ref"]
            or result.get("delivery_state") != "submitted"
            or result.get("transport_state") != "submitted"
            or result.get("observed_pending_state") != "pending_observation"
        ):
            return None
        return response
    error = response.get("error")
    if (
        response.get("result") is not None
        or not isinstance(error, dict)
        or error.get("code") != status
        or not isinstance(error.get("message"), str)
        or not error["message"]
    ):
        return None
    if status == _DECISION_IN_PROGRESS_STATUS:
        return response if disposition in {"no_receipt", "in_progress"} else None
    if (
        status in _DECISION_FAILURE_STATUSES
        and disposition in {"no_receipt", "terminal_rejected"}
    ):
        return response
    return None


def _is_private_ingress_env_key(key: str) -> bool:
    upper = str(key).upper()
    return (
        upper in _PRIVATE_INGRESS_ENV_KEYS
        or "TELEGRAM" in upper
        or upper.startswith("HERDRES_GATEWAY_")
        or upper.startswith("HERDRES_MANAGED_BOT_")
        or upper.startswith("HERDRES_PRIVATE_INGRESS_")
        or upper.startswith("HERDRES_REQUEST_ID_")
    )


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
    timeout: float | None = None

    def _timeout_seconds(self) -> float:
        if self.timeout is not None:
            return max(1.0, float(self.timeout))
        return config.tendwire_timeout_seconds()

    def _explicit_parts(self) -> tuple[list[str] | None, dict[str, str]]:
        explicit = os.getenv("HERDRES_TENDWIRE_BIN") or os.getenv("TENDWIRE_BIN")
        if not explicit:
            return None, {}
        parts = shlex.split(os.path.expandvars(os.path.expanduser(explicit)))
        overrides: dict[str, str] = {}
        if parts and Path(parts[0]).name == "env":
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
        explicit, overrides = self._explicit_parts()
        env.update(overrides)
        env = {
            key: value
            for key, value in env.items()
            if not _is_private_ingress_env_key(key)
        }
        source = Path(os.getenv("TENDWIRE_SOURCE_DIR", str(Path.home() / "tendwire" / "src"))).expanduser()
        if explicit is None and source.exists():
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
        input_bytes: bytes | None = None,
        timeout: float | None = None,
        protocol: bool = False,
        preserve_page_text: bool = False,
        strict_stdout: bool = False,
        post_start_uncertain: bool = False,
    ) -> dict[str, Any]:
        if input_json is not None and input_bytes is not None:
            raise ValueError("input_json and input_bytes are mutually exclusive")
        stdin = input_bytes
        if input_json is not None:
            stdin = json.dumps(input_json, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
        try:
            proc = subprocess.run(
                [*self._base(), *args],
                input=stdin,
                capture_output=True,
                env=self._env(),
                timeout=timeout if timeout is not None else self._timeout_seconds(),
                check=False,
            )
        except subprocess.TimeoutExpired as exc:
            if post_start_uncertain:
                return _request_state_uncertain(input_json)
            return {"ok": False, "status": "timeout", "error": f"tendwire {' '.join(args[:2])} timed out after {exc.timeout}s"}
        except OSError as exc:
            failure = {
                "ok": False,
                "status": "subprocess_failed",
                "error": sanitize_text(str(exc), 300),
            }
            if post_start_uncertain:
                process_failure = _CommandProcessResult(failure)
                process_failure._process_not_started = _PROCESS_NOT_STARTED
                return process_failure
            return failure
        except Exception as exc:  # noqa: BLE001
            if post_start_uncertain:
                return _request_state_uncertain(input_json)
            return {"ok": False, "status": "subprocess_failed", "error": sanitize_text(str(exc), 300)}
        try:
            if preserve_page_text or strict_stdout or post_start_uncertain:
                stdout = proc.stdout.decode("utf-8")
            else:
                stdout = proc.stdout.decode("utf-8", "replace")
        except UnicodeDecodeError:
            if post_start_uncertain:
                return _request_state_uncertain(input_json)
            return {"ok": False, "status": "invalid_utf8_stdout", "error": "Tendwire returned invalid UTF-8"}
        stderr = proc.stderr.decode("utf-8", "replace")
        try:
            data = json.loads(stdout or "{}")
        except json.JSONDecodeError:
            if post_start_uncertain:
                return _request_state_uncertain(input_json)
            detail = sanitize_text(stderr or stdout or "non-json Tendwire response", 300)
            return {"ok": False, "status": "non_json_stdout", "error": detail}
        if not isinstance(data, dict):
            if post_start_uncertain:
                return _request_state_uncertain(input_json)
            return {"ok": False, "status": "non_object_json", "error": "Tendwire returned non-object JSON"}
        if post_start_uncertain:
            process_result = _CommandProcessResult(data)
            process_result._process_returncode = proc.returncode
            return process_result

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
        merged: dict[str, Any] | None = None
        all_turns: list[dict[str, Any]] = []
        cursor: str | None = None
        seen_cursors: set[str] = set()
        for _page_index in range(TURN_LIST_MAX_PAGES):
            args = [
                "turns",
                "--schema-version",
                str(TURN_SCHEMA_VERSION),
                "--limit",
                str(TURN_LIST_PAGE_LIMIT),
                "--json",
            ]
            if cursor is not None:
                args.extend(("--cursor", cursor))
            result = self.call(args)
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
            if merged is None:
                merged = dict(result)
            all_turns.extend(row for row in turns if isinstance(row, dict))
            next_cursor = result.get("next_cursor")
            has_more = result.get("has_more") is True
            if next_cursor is None and not has_more:
                merged["turns"] = all_turns
                merged["next_cursor"] = None
                merged["has_more"] = False
                return merged
            if (
                not isinstance(next_cursor, str)
                or not next_cursor
                or next_cursor in seen_cursors
            ):
                return _schema_error(
                    "unsupported_content_schema",
                    "Tendwire turn.list pagination is invalid",
                    received=None,
                    required_key="supported_content_schema_version",
                    required_version=TURN_CONTENT_SCHEMA_VERSION,
                )
            seen_cursors.add(next_cursor)
            cursor = next_cursor
        return _schema_error(
            "unsupported_content_schema",
            "Tendwire turn.list pagination exceeds the supported bound",
            received=None,
            required_key="supported_content_schema_version",
            required_version=TURN_CONTENT_SCHEMA_VERSION,
        )

    def turn_delta(
        self,
        *,
        watermark: str | None = None,
        cursor: str | None = None,
        limit: int | None = None,
    ) -> dict[str, Any]:
        """Read exactly one bounded turn-change page.

        This method deliberately never retries. Once the child process starts,
        timeout, EOF, invalid UTF-8, and malformed output are transport
        ambiguity rather than permission to observe the source a second time.
        """
        if watermark is not None and cursor is not None:
            raise ValueError("watermark and cursor are mutually exclusive")
        page_limit = config.tendwire_delta_limit() if limit is None else int(limit)
        if not 1 <= page_limit <= 500:
            raise ValueError("turn delta limit must be between 1 and 500")
        args = [
            "turn",
            "delta",
            "--json",
            "--limit",
            str(page_limit),
        ]
        # The shipped Goal 13 Tendwire CLI fixes projection schema v2 for this
        # subcommand; unlike turn.list it does not accept --schema-version.
        if watermark is not None:
            args.extend(("--watermark", str(watermark)))
        elif cursor is not None:
            args.extend(("--cursor", str(cursor)))
        result = self.call(args, protocol=True, strict_stdout=True)
        error = result.get("error")
        error_code = (
            str(error.get("code") or "").strip().lower()
            if isinstance(error, dict)
            else ""
        )
        status = str(result.get("status") or "").strip().lower()
        if error_code and status in {"", "nonzero_exit"}:
            # Nonzero CLI exits synthesize nonzero_exit in call(). The public
            # nested error code remains the authoritative explicit outcome.
            result["status"] = error_code
            status = error_code
        if status in {"unsupported_method", "unknown_method"} or error_code in {
            "unsupported_method",
            "unknown_method",
        }:
            return {
                "ok": False,
                "status": "unsupported_method",
                "schema_version": TURN_DELTA_SCHEMA_VERSION,
                "projection_schema_version": TURN_DELTA_PROJECTION_SCHEMA_VERSION,
            }
        if status in {
            "timeout",
            "subprocess_failed",
            "invalid_utf8_stdout",
            "non_json_stdout",
            "non_object_json",
            "daemon_timeout",
            "daemon_protocol_error",
        }:
            return {
                "ok": False,
                "status": "transport_ambiguous",
                "transport_status": status,
                "error": sanitize_text(result.get("error") or status, 300),
            }
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
        result = self.call(args, protocol=True, preserve_page_text=True)
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

    def command_json(self, request_json: str) -> dict[str, Any]:
        if not isinstance(request_json, str):
            return _invalid_command_request()
        try:
            request = json.loads(request_json)
            request_bytes = request_json.encode("utf-8")
        except (json.JSONDecodeError, TypeError, ValueError, UnicodeError):
            return _invalid_command_request()
        public_request = _exact_public_command_request(request)
        if public_request is None:
            return _invalid_command_request()
        result = self.call(
            ["command", "--json"],
            input_bytes=request_bytes,
            timeout=60,
            post_start_uncertain=True,
        )
        status = result.get("status")
        if (
            status == "subprocess_failed"
            and command_process_not_started(result)
        ):
            return result
        process_returncode = getattr(result, "_process_returncode", None)
        if command_process_ambiguous(result):
            return _request_state_uncertain(public_request)
        validated = (
            _validated_decision_response(result, public_request)
            if public_request.get("action") == "answer_decision"
            else _validated_command_response(result, public_request)
        )
        if validated is None:
            return _request_state_uncertain(public_request)
        if type(process_returncode) is int and (
            (
                process_returncode == 0
                and validated["ok"] is True
            )
            or (
                process_returncode == 1
                and validated["ok"] is False
            )
        ):
            return public_prune(validated)
        return _request_state_uncertain(public_request)

    def command(self, request: dict[str, Any]) -> dict[str, Any]:
        try:
            request_json = json.dumps(
                request,
                separators=(",", ":"),
                ensure_ascii=False,
            )
        except (TypeError, ValueError):
            return _invalid_command_request()
        return self.command_json(request_json)

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
            timeout=CONNECTOR_PROCESS_TIMEOUT_SECONDS,
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
        return self.call(
            args,
            timeout=CONNECTOR_PROCESS_TIMEOUT_SECONDS,
        )

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
            timeout=CONNECTOR_PROCESS_TIMEOUT_SECONDS,
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
            timeout=CONNECTOR_PROCESS_TIMEOUT_SECONDS,
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
            timeout=CONNECTOR_PROCESS_TIMEOUT_SECONDS,
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
        return self._validate_protocol_schema(
            self.call(
                args,
                timeout=CONNECTOR_PROCESS_TIMEOUT_SECONDS,
                protocol=True,
            )
        )

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
            timeout=CONNECTOR_PROCESS_TIMEOUT_SECONDS,
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
        return self._validate_protocol_schema(
            self.call(
                args,
                timeout=CONNECTOR_PROCESS_TIMEOUT_SECONDS,
                protocol=True,
            )
        )
