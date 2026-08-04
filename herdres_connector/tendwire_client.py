"""Bounded AF_UNIX client for Tendwire's public daemon API."""

from __future__ import annotations

import json
import os
import secrets
import socket
import stat
import struct
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from . import config
from .ingress_identity import validate_request_id
from .safe import (
    FORBIDDEN_PUBLIC_KEYS,
    PRIVATE_STRUCTURE_MAX_DEPTH,
    PRIVATE_STRUCTURE_MAX_ITEMS,
    PRUNE_TEXT_LIMIT,
    public_prune,
    sanitize_text,
)


class TendwireError(RuntimeError):
    pass


TURN_SCHEMA_VERSION = 2
TURN_CONTENT_SCHEMA_VERSION = 1
TURN_LIST_PAGE_LIMIT = 250
TURN_LIST_MAX_PAGES = 256
TURN_DELTA_SCHEMA_VERSION = 1
TURN_DELTA_PROJECTION_SCHEMA_VERSION = 2
CONNECTOR_SCHEMA_VERSION = 1
TURN_FINAL_CONNECTOR = "turn-final"
CONNECTOR_PREPARE_MAX_SPANS = 256
CONNECTOR_PREPARE_MAX_REQUEST_BYTES = 64 * 1024
CONNECTOR_PROCESS_TIMEOUT_SECONDS = 20
DAEMON_API_SCHEMA_VERSION = 1
DAEMON_MAX_FRAME_BYTES = 1024 * 1024

_SEND_FIELDS = {"schema_version", "action", "request_id", "dry_run", "target", "instruction"}
_DECISION_FIELDS = {"schema_version", "action", "request_id", "dry_run", "target", "params"}
_RESPONSE_FIELDS = {
    "schema_version", "action", "request_id", "ok", "dry_run", "status",
    "disposition", "result", "error", "warnings",
}
_SEND_RESULT_REQUIRED = {
    "target", "delivery_state", "transport_state", "target_state_at_send",
    "observed_turn_state",
}
_SEND_RESULT_OPTIONAL = {"submission_id", "submission_verdict", "turn_id"}
_SEND_REJECTIONS = {
    "rejected", "stale_target", "backend_unavailable", "backend_unsupported",
    "ambiguous_backend_target", "backend_failed", "duplicate_request",
}
_SEND_NO_RECEIPT = {
    "invalid_request", "rejected", "not_found", "ambiguous_target", "stale_target",
    "backend_unavailable", "backend_unsupported", "ambiguous_backend_target", "backend_failed",
}
_DECISION_FAILURES = {
    "decision_not_pending", "invalid_selection", "unsupported_decision", "unknown_worker",
}
_DISPOSITIONS = {
    "no_receipt", "in_progress", "terminal_accepted", "terminal_rejected", "terminal_uncertain",
}
_EXACT_PROTOCOL_TEXT_KEYS = {
    "user_text", "assistant_final_text", "text", "plan_token", "failed_plan_token",
    "recovered_plan_token", "replaces_plan_token", "recovers_plan_token", "final_identity",
    "content_revision", "key",
}
_ALLOWED_PROTOCOL_TOKEN_KEYS = {
    "plan_token", "failed_plan_token", "recovered_plan_token", "replaces_plan_token",
    "recovers_plan_token",
}
_NOT_STARTED = object()
_AMBIGUOUS = object()


class _TransportResult(dict[str, Any]):
    __slots__ = ("_not_started", "_ambiguous")


def command_process_ambiguous(result: Any) -> bool:
    return (
        isinstance(result, _TransportResult)
        and getattr(result, "_ambiguous", None) is _AMBIGUOUS
    )


def command_process_not_started(result: Any) -> bool:
    return (
        isinstance(result, _TransportResult)
        and getattr(result, "_not_started", None) is _NOT_STARTED
    )


def _invalid_command_request() -> dict[str, Any]:
    return {
        "ok": False,
        "status": "invalid_request",
        "error": "Herdres command request is not an exact public command object",
    }


def _request_state_uncertain(request: dict[str, Any]) -> dict[str, Any]:
    result = _TransportResult({
        "ok": False,
        "status": "request_state_uncertain",
        "error": "Tendwire command result was lost after request start",
        "request_id": request["request_id"],
        "action": request["action"],
    })
    result._ambiguous = _AMBIGUOUS
    return result


def _exact_command_request(value: Any) -> dict[str, Any] | None:
    if (
        not isinstance(value, dict)
        or type(value.get("schema_version")) is not int
        or value["schema_version"] != 1
        or value.get("dry_run") is not False
    ):
        return None
    try:
        validate_request_id(value.get("request_id"))
    except ValueError:
        return None
    target = value.get("target")
    if not isinstance(target, dict) or any(
        not isinstance(item, str) or not item.strip()
        for item in target.values()
    ):
        return None
    if value.get("action") == "send_instruction":
        if set(value) not in (_SEND_FIELDS, _SEND_FIELDS | {"response_schema_version"}):
            return None
        if set(target) not in ({"worker_id"}, {"worker_id", "worker_fingerprint"}, {"space_id"}):
            return None
        instruction = value.get("instruction")
        if (
            not isinstance(instruction, dict)
            or set(instruction) != {"text"}
            or not isinstance(instruction.get("text"), str)
            or not instruction["text"]
        ):
            return None
        if "response_schema_version" in value and value["response_schema_version"] != 3:
            return None
        return value
    if (
        value.get("action") != "answer_decision"
        or set(value) != _DECISION_FIELDS
        or set(target) != {"worker_id"}
    ):
        return None
    params = value.get("params")
    if (
        not isinstance(params, dict)
        or set(params) != {"decision_ref", "selection"}
        or not isinstance(params.get("decision_ref"), str)
        or not params["decision_ref"].strip()
    ):
        return None
    selection = params.get("selection")
    if not isinstance(selection, dict):
        return None
    if set(selection) == {"text"}:
        text = selection["text"]
        return value if isinstance(text, str) and bool(text.strip()) else None
    refs = selection.get("option_refs")
    valid_refs = set(selection) == {"option_refs"} and isinstance(refs, list)
    if valid_refs:
        valid_refs = all(
            isinstance(item, str) and bool(item.strip()) for item in refs
        )
    if valid_refs:
        valid_refs = len(refs) == len(set(refs))
    return value if valid_refs else None


def _valid_response_shell(response: Any, request: dict[str, Any], schemas: set[int]) -> bool:
    return (
        isinstance(response, dict)
        and set(response) == _RESPONSE_FIELDS
        and type(response.get("schema_version")) is int
        and response["schema_version"] in schemas
        and response.get("request_id") == request["request_id"]
        and response.get("action") == request["action"]
        and response.get("dry_run") is False
        and type(response.get("ok")) is bool
        and isinstance(response.get("status"), str) and bool(response["status"])
        and response.get("disposition") in _DISPOSITIONS
        and isinstance(response.get("warnings"), list)
        and all(isinstance(v, str) for v in response["warnings"])
        and public_prune(response) == response
    )


def _valid_send_result(result: Any, request: dict[str, Any], schema: int) -> bool:
    if (
        not isinstance(result, dict)
        or not _SEND_RESULT_REQUIRED
        <= set(result)
        <= _SEND_RESULT_REQUIRED | _SEND_RESULT_OPTIONAL
    ):
        return False
    target = result.get("target")
    requested_worker = request["target"].get("worker_id")
    turn_id = result.get("turn_id")
    submission_id = result.get("submission_id")
    return (
        isinstance(target, dict) and set(target) == {"worker_id"}
        and isinstance(target.get("worker_id"), str) and bool(target["worker_id"].strip())
        and (not isinstance(requested_worker, str) or target["worker_id"] == requested_worker)
        and result.get("delivery_state") == result.get("transport_state") == "submitted"
        and isinstance(result.get("target_state_at_send"), str)
        and bool(result["target_state_at_send"].strip())
        and result.get("observed_turn_state")
        in {"pending_observation", "observed", "complete", "linked"}
        and (
            "turn_id" not in result
            or turn_id is None
            or isinstance(turn_id, str) and bool(turn_id.strip())
        )
        and (
            "submission_id" not in result
            or isinstance(submission_id, str) and bool(submission_id.strip())
        )
        and result.get("submission_verdict", "submitted") in {"submitted", "written_to_pty"}
        and not (schema == 2 and "submission_id" in result)
    )


def _validated_command_response(response: Any, request: dict[str, Any]) -> dict[str, Any] | None:
    schemas = {2, 3} if request.get("response_schema_version") == 3 else {2}
    if not _valid_response_shell(response, request, schemas):
        return None
    status, disposition = response["status"], response["disposition"]
    if response["ok"] is True:
        accepted = (
            status == "accepted"
            and disposition == "terminal_accepted"
            and response.get("error") is None
            and _valid_send_result(
                response.get("result"), request, response["schema_version"]
            )
        )
        return response if accepted else None
    error = response.get("error")
    if (
        not isinstance(error, dict)
        or not isinstance(error.get("message"), str)
        or not error["message"]
        or error.get("code") not in {None, status}
    ):
        return None
    allowed = (
        disposition == "in_progress" and status == "pending"
        or disposition == "terminal_uncertain" and status == "request_state_uncertain"
        or disposition == "terminal_rejected" and status in _SEND_REJECTIONS
        or disposition == "no_receipt" and status in _SEND_NO_RECEIPT
    )
    return response if allowed else None


def _validated_decision_response(response: Any, request: dict[str, Any]) -> dict[str, Any] | None:
    if not _valid_response_shell(response, request, {2}):
        return None
    status, disposition = response["status"], response["disposition"]
    if response["ok"] is True:
        result = response.get("result")
        target = result.get("target") if isinstance(result, dict) else None
        decision = result.get("decision") if isinstance(result, dict) else None
        valid = (
            status == "accepted"
            and disposition == "terminal_accepted"
            and response.get("error") is None
            and isinstance(result, dict)
            and set(result)
            == {
                "target",
                "decision",
                "delivery_state",
                "transport_state",
                "observed_pending_state",
            }
            and target == request["target"]
            and decision == {"decision_ref": request["params"]["decision_ref"]}
            and result.get("delivery_state") == result.get("transport_state") == "submitted"
            and result.get("observed_pending_state") == "pending_observation"
        )
        return response if valid else None
    error = response.get("error")
    if (
        response.get("result") is not None
        or not isinstance(error, dict)
        or error.get("code") != status
        or not isinstance(error.get("message"), str)
        or not error["message"]
    ):
        return None
    allowed = (
        status == "answer_in_progress"
        and disposition in {"no_receipt", "in_progress"}
    ) or (
        status in _DECISION_FAILURES
        and disposition in {"no_receipt", "terminal_rejected"}
    )
    return response if allowed else None


def _protocol_prune(
    value: Any,
    *,
    _depth: int = 0,
    _budget: list[int] | None = None,
    _seen: set[int] | None = None,
    _exact: bool = False,
) -> Any:
    if _budget is None:
        _budget = [PRIVATE_STRUCTURE_MAX_ITEMS]
    if _seen is None:
        _seen = set()
    _budget[0] -= 1
    if _budget[0] < 0 or _depth > PRIVATE_STRUCTURE_MAX_DEPTH:
        return None
    if isinstance(value, dict):
        if id(value) in _seen:
            return None
        _seen.add(id(value))
        clean: dict[str, Any] = {}
        for raw_key, item in value.items():
            key = str(raw_key)
            forbidden = (
                key in FORBIDDEN_PUBLIC_KEYS
                or "secret" in key.lower()
                or (
                    "token" in key.lower()
                    and key not in _ALLOWED_PROTOCOL_TOKEN_KEYS
                )
            )
            if forbidden:
                continue
            clean[key] = _protocol_prune(
                item,
                _depth=_depth + 1,
                _budget=_budget,
                _seen=_seen,
                _exact=key in _EXACT_PROTOCOL_TEXT_KEYS,
            )
        return clean
    if isinstance(value, list):
        if id(value) in _seen:
            return None
        _seen.add(id(value))
        return [
            _protocol_prune(
                item,
                _depth=_depth + 1,
                _budget=_budget,
                _seen=_seen,
                _exact=_exact,
            )
            for item in value
        ]
    if isinstance(value, str):
        return value if _exact else sanitize_text(value, PRUNE_TEXT_LIMIT)
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
    socket_path: str | Path | None = None

    def _timeout_seconds(self) -> float:
        if self.timeout is not None:
            return max(1.0, float(self.timeout))
        return config.tendwire_timeout_seconds()

    def _path(self) -> Path:
        return Path(self.socket_path or config.tendwire_socket_path()).expanduser()

    @staticmethod
    def _transport_error(
        status: str, message: str, *, started: bool
    ) -> dict[str, Any]:
        result = _TransportResult(
            {
                "ok": False,
                "status": status,
                "error": sanitize_text(message, 300),
            }
        )
        if started:
            result._ambiguous = _AMBIGUOUS
        else:
            result._not_started = _NOT_STARTED
        return result

    @staticmethod
    def _pin_socket(path: Path) -> tuple[int, int, str, tuple[int, int]]:
        if not path.is_absolute() or path.name in {"", ".", ".."}:
            raise OSError("Tendwire socket path must be an absolute leaf")
        flags = os.O_RDONLY | os.O_DIRECTORY | getattr(os, "O_CLOEXEC", 0)
        nofollow = getattr(os, "O_NOFOLLOW", 0)
        parent_fd = os.open("/", flags)
        pin_fd = -1
        try:
            for part in path.parts[1:-1]:
                next_fd = os.open(part, flags | nofollow, dir_fd=parent_fd)
                os.close(parent_fd)
                parent_fd = next_fd
            parent = os.fstat(parent_fd)
            insecure_parent = stat.S_IMODE(parent.st_mode) & (
                stat.S_IWGRP | stat.S_IWOTH
            )
            if parent.st_uid != os.geteuid() or insecure_parent:
                raise OSError("Tendwire socket parent is not private and owned")
            current = os.stat(path.name, dir_fd=parent_fd, follow_symlinks=False)
            insecure_socket = stat.S_IMODE(current.st_mode) & ~0o600
            if (
                not stat.S_ISSOCK(current.st_mode)
                or current.st_uid != os.geteuid()
                or insecure_socket
            ):
                raise OSError("Tendwire endpoint is not a private owned socket")
            path_flag = getattr(os, "O_PATH", os.O_RDONLY)
            pin_fd = os.open(
                path.name,
                path_flag | nofollow | getattr(os, "O_CLOEXEC", 0),
                dir_fd=parent_fd,
            )
            pinned = os.fstat(pin_fd)
            identity = (current.st_dev, current.st_ino)
            if (pinned.st_dev, pinned.st_ino) != identity or not stat.S_ISSOCK(pinned.st_mode):
                raise OSError("Tendwire socket changed while pinning")
            return parent_fd, pin_fd, path.name, identity
        except Exception:
            if pin_fd >= 0:
                os.close(pin_fd)
            os.close(parent_fd)
            raise

    @staticmethod
    def _anchored_socket_address(parent_fd: int, leaf: str) -> str:
        """Name ``leaf`` through the retained parent, never through its ancestors."""

        if os.name != "posix" or not isinstance(parent_fd, int) or parent_fd < 0:
            raise OSError("Tendwire socket anchoring is unsupported")
        expected = os.fstat(parent_fd)
        anchor = f"/proc/self/fd/{parent_fd}"
        try:
            current = os.stat(anchor)
        except OSError:
            raise OSError("Tendwire socket anchoring is unavailable") from None
        if (
            not stat.S_ISDIR(expected.st_mode)
            or (current.st_dev, current.st_ino) != (expected.st_dev, expected.st_ino)
        ):
            raise OSError("Tendwire socket anchor is invalid")
        return f"{anchor}/{leaf}"

    @staticmethod
    def _validate_peer(conn: socket.socket) -> None:
        if not hasattr(socket, "SO_PEERCRED"):
            raise OSError("Tendwire daemon peer validation is unsupported")
        credentials = struct.Struct("3i")
        try:
            raw = conn.getsockopt(
                socket.SOL_SOCKET,
                socket.SO_PEERCRED,
                credentials.size,
            )
            peer_pid, peer_uid, peer_gid = credentials.unpack(raw)
        except (OSError, struct.error):
            raise OSError("Tendwire daemon peer validation failed") from None
        if peer_pid <= 0 or peer_uid < 0 or peer_gid < 0:
            raise OSError("Tendwire daemon peer validation failed")
        if peer_uid != os.geteuid():
            raise OSError("Tendwire daemon peer is not owned by this user")

    @staticmethod
    def _read_frame(conn: socket.socket, deadline: float) -> bytes:
        chunks: list[bytes] = []
        size = 0
        complete = False
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError("Tendwire daemon request timed out")
            conn.settimeout(remaining)
            chunk = conn.recv(4096)
            if not chunk:
                if complete:
                    return b"".join(chunks)
                raise OSError("Tendwire daemon closed before a complete response")
            if complete:
                if chunk.strip():
                    raise ValueError("Tendwire daemon returned multiple responses")
                continue
            head, separator, tail = chunk.partition(b"\n")
            chunks.append(head)
            size += len(head)
            if size > DAEMON_MAX_FRAME_BYTES:
                raise ValueError("Tendwire daemon response exceeds the size bound")
            if separator:
                if tail.strip():
                    raise ValueError("Tendwire daemon returned multiple responses")
                complete = True

    @staticmethod
    def _outer_result(response: Any, request_id: str) -> dict[str, Any]:
        fields = {"schema_version", "ok", "status", "result", "error", "id"}
        invalid_shell = (
            not isinstance(response, dict)
            or set(response) != fields
            or response.get("schema_version") != DAEMON_API_SCHEMA_VERSION
            or isinstance(response.get("schema_version"), bool)
            or response.get("id") != request_id
            or type(response.get("ok")) is not bool
        )
        if invalid_shell:
            raise ValueError("Tendwire daemon returned an invalid response envelope")
        if response["ok"] is True:
            if (
                response.get("status") != "ok"
                or not isinstance(response.get("result"), dict)
                or response.get("error") is not None
            ):
                raise ValueError("Tendwire daemon returned an invalid success envelope")
            return response["result"]
        error = response.get("error")
        if (
            response.get("status") != "error"
            or response.get("result") is not None
            or not isinstance(error, dict)
            or not isinstance(error.get("code"), str)
            or not error["code"]
            or not isinstance(error.get("message"), str)
            or not error["message"]
        ):
            raise ValueError("Tendwire daemon returned an invalid error envelope")
        return {"ok": False, "status": error["code"], "error": public_prune(error)}

    def _request(
        self,
        method: str,
        params: dict[str, Any] | None = None,
        *,
        timeout: float | None = None,
    ) -> dict[str, Any]:
        request_id = f"r{secrets.token_hex(12)}"
        try:
            payload = {
                "id": request_id,
                "method": method,
                "params": dict(params or {}),
            }
            raw = json.dumps(
                payload,
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
                allow_nan=False,
            ).encode("utf-8") + b"\n"
        except (TypeError, ValueError, UnicodeError):
            return self._transport_error(
                "invalid_request",
                "Tendwire request is not bounded JSON",
                started=False,
            )
        if len(raw) > DAEMON_MAX_FRAME_BYTES:
            return self._transport_error(
                "request_too_large",
                "Tendwire request exceeds the size bound",
                started=False,
            )
        timeout_seconds = (
            self._timeout_seconds()
            if timeout is None
            else max(1.0, float(timeout))
        )
        deadline = time.monotonic() + timeout_seconds
        started = False
        parent_fd = pin_fd = -1
        try:
            path = self._path()
            parent_fd, pin_fd, leaf, identity = self._pin_socket(path)
            address = self._anchored_socket_address(parent_fd, leaf)
            with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as conn:
                conn.settimeout(max(0.001, deadline - time.monotonic()))
                conn.connect(address)
                current = os.stat(leaf, dir_fd=parent_fd, follow_symlinks=False)
                pinned = os.fstat(pin_fd)
                if (
                    (current.st_dev, current.st_ino) != identity
                    or (pinned.st_dev, pinned.st_ino) != identity
                    or not stat.S_ISSOCK(current.st_mode)
                    or not stat.S_ISSOCK(pinned.st_mode)
                ):
                    raise OSError("Tendwire socket changed during connection")
                self._validate_peer(conn)
                started = True
                conn.sendall(raw)
                frame = self._read_frame(conn, deadline)
            return self._outer_result(json.loads(frame.decode("utf-8")), request_id)
        except (TimeoutError, socket.timeout):
            return self._transport_error(
                "daemon_timeout",
                "Tendwire daemon request timed out",
                started=started,
            )
        except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as exc:
            return self._transport_error("daemon_protocol_error", str(exc), started=started)
        except OSError as exc:
            status = "daemon_protocol_error" if started else "daemon_unavailable"
            return self._transport_error(status, str(exc), started=started)
        finally:
            if pin_fd >= 0:
                os.close(pin_fd)
            if parent_fd >= 0:
                os.close(parent_fd)

    @staticmethod
    def _clean(result: dict[str, Any], *, protocol: bool = False) -> dict[str, Any]:
        clean = _protocol_prune(result) if protocol else public_prune(result)
        if isinstance(clean, dict):
            return clean
        return {
            "ok": False,
            "status": "daemon_protocol_error",
            "error": "invalid Tendwire result",
        }

    def snapshot(self) -> dict[str, Any]:
        return self._clean(self._request("snapshot.get"))

    def turns(self) -> dict[str, Any]:
        merged: dict[str, Any] | None = None
        rows: list[dict[str, Any]] = []
        cursor: str | None = None
        seen: set[str] = set()
        for _ in range(TURN_LIST_MAX_PAGES):
            params: dict[str, Any] = {"schema_version": 2, "limit": TURN_LIST_PAGE_LIMIT}
            if cursor is not None:
                params["cursor"] = cursor
            result = self._clean(self._request("turn.list", params), protocol=True)
            if result.get("ok") is False:
                return result
            turns = result.get("turns")
            if result.get("schema_version") != 2 or not isinstance(turns, list):
                return _schema_error(
                    "upgrade_required",
                    "Tendwire turn.list schema v2 is required",
                    received=result.get("schema_version"),
                    required_key="required_turn_schema_version",
                    required_version=2,
                )
            invalid_content = any(
                not isinstance(row, dict)
                or not isinstance(row.get("content"), dict)
                or row["content"].get("schema_version") != 1
                for row in turns
            )
            if invalid_content:
                return _schema_error(
                    "unsupported_content_schema",
                    "Every turn requires content schema v1",
                    received=None,
                    required_key="supported_content_schema_version",
                    required_version=1,
                )
            merged = dict(result) if merged is None else merged
            rows.extend(turns)
            cursor = result.get("next_cursor")
            if cursor is None and result.get("has_more") is not True:
                merged.update({"turns": rows, "next_cursor": None, "has_more": False})
                return merged
            if not isinstance(cursor, str) or not cursor or cursor in seen:
                break
            seen.add(cursor)
        return _schema_error(
            "unsupported_content_schema",
            "Tendwire turn.list pagination is invalid",
            received=None,
            required_key="supported_content_schema_version",
            required_version=1,
        )

    def turn_delta(
        self,
        *,
        watermark: str | None = None,
        cursor: str | None = None,
        limit: int | None = None,
    ) -> dict[str, Any]:
        if watermark is not None and cursor is not None:
            raise ValueError("watermark and cursor are mutually exclusive")
        page_limit = config.tendwire_delta_limit() if limit is None else int(limit)
        if not 1 <= page_limit <= 500:
            raise ValueError("turn delta limit must be between 1 and 500")
        params: dict[str, Any] = {"limit": page_limit}
        if watermark is not None:
            params["watermark"] = str(watermark)
        elif cursor is not None:
            params["cursor"] = str(cursor)
        result = self._clean(self._request("turn.delta", params), protocol=True)
        error = result.get("error")
        error_code = str(error.get("code") or "").lower() if isinstance(error, dict) else ""
        status = str(result.get("status") or "").lower()
        unsupported = {"unsupported_method", "unknown_method"}
        if status in unsupported or error_code in unsupported:
            return {
                "ok": False,
                "status": "unsupported_method",
                "schema_version": 1,
                "projection_schema_version": 2,
            }
        if status in {"daemon_timeout", "daemon_unavailable", "daemon_protocol_error"}:
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
        params: dict[str, Any] = {
            "schema_version": 1,
            "turn_id": str(turn_id),
            "content_revision": str(content_revision),
            "field": str(field),
        }
        if cursor is not None:
            params["cursor"] = str(cursor)
        result = self._clean(self._request("turn.content.get", params), protocol=True)
        if result.get("ok") is False:
            return result
        if result.get("schema_version") != 1 or not isinstance(result.get("text"), str):
            return _schema_error(
                "unsupported_content_schema",
                "Tendwire turn.content.get schema v1 with exact text is required",
                received=result.get("schema_version"),
                required_key="supported_content_schema_version",
                required_version=1,
            )
        return result

    def pending(self) -> dict[str, Any]:
        return self._clean(self._request("pending.list"))

    def doctor(self) -> dict[str, Any]:
        return self._clean(self._request("health.get", timeout=10))

    def command_json(self, request_json: str) -> dict[str, Any]:
        try:
            request = json.loads(request_json) if isinstance(request_json, str) else None
            if isinstance(request_json, str):
                request_json.encode("utf-8")
        except (json.JSONDecodeError, UnicodeError):
            return _invalid_command_request()
        public_request = _exact_command_request(request)
        if public_request is None:
            return _invalid_command_request()
        result = self._request("command.submit", public_request, timeout=60)
        if command_process_not_started(result):
            return result
        if command_process_ambiguous(result):
            return _request_state_uncertain(public_request)
        if public_request["action"] == "answer_decision":
            validated = _validated_decision_response(result, public_request)
        else:
            validated = _validated_command_response(result, public_request)
        if validated is None:
            return _request_state_uncertain(public_request)
        return public_prune(validated)

    def command(self, request: dict[str, Any]) -> dict[str, Any]:
        try:
            return self.command_json(json.dumps(request, separators=(",", ":"), ensure_ascii=False))
        except (TypeError, ValueError):
            return _invalid_command_request()

    def _connector(
        self,
        method: str,
        params: dict[str, Any],
        *,
        protocol: bool = False,
    ) -> dict[str, Any]:
        result = self._request(
            f"connector.{method}",
            params,
            timeout=CONNECTOR_PROCESS_TIMEOUT_SECONDS,
        )
        clean = self._clean(result, protocol=protocol)
        if isinstance(result, _TransportResult):
            return clean
        if self._valid_connector_result(method, params, clean):
            return clean
        return _schema_error(
            "invalid_connector_response",
            "Tendwire connector returned a malformed schema-v1 envelope",
            received=clean.get("schema_version"),
            required_key="supported_content_schema_version",
            required_version=CONNECTOR_SCHEMA_VERSION,
        )

    @staticmethod
    def _valid_connector_result(
        method: str,
        params: dict[str, Any],
        result: dict[str, Any],
    ) -> bool:
        status = result.get("status")
        ok = result.get("ok")
        name = str(params.get("name") or "")
        common = (
            type(result.get("schema_version")) is int
            and result["schema_version"] == CONNECTOR_SCHEMA_VERSION
            and type(ok) is bool
            and isinstance(status, str)
            and bool(status)
        )
        if not common:
            return False
        if ok is False:
            error = result.get("error")
            return (
                isinstance(result.get("host_id"), str)
                and bool(result["host_id"])
                and result.get("name") == name
                and isinstance(error, dict)
                and error.get("code") == status
                and isinstance(error.get("message"), str)
                and bool(error["message"])
            )
        if result.get("error") is not None:
            return False
        if method == "prepare":
            return TendwireClient._valid_prepare_result(params, result)
        if (
            not isinstance(result.get("host_id"), str)
            or not result["host_id"]
            or result.get("name") != name
        ):
            return False
        if method == "poll":
            items = result.get("items")
            return status == "ok" and isinstance(items, list) and all(
                TendwireClient._valid_connector_item(item) for item in items
            )
        if method in {"ack", "fail", "defer"}:
            expected = {
                "ack": {"acknowledged"},
                "fail": {"retry_scheduled", "attempts_exhausted", "superseded"},
                "defer": {"deferred", "superseded"},
            }[method]
            return (
                status in expected
                and result.get("ref") == str(params.get("ref") or "")
                and TendwireClient._valid_opaque(result.get("ref"), "twref1.")
                and isinstance(result.get("key"), str)
                and bool(result["key"])
                and type(result.get("attempt")) is int
                and result["attempt"] > 0
                and (
                    status not in {"retry_scheduled", "deferred"}
                    or isinstance(result.get("available_at"), str)
                    and bool(result["available_at"])
                )
            )
        return False

    @staticmethod
    def _valid_prepare_result(
        params: dict[str, Any], result: dict[str, Any]
    ) -> bool:
        action = params.get("action")
        if action == "recover":
            integer_fields = (
                "generation",
                "acknowledged_prefix_count",
                "executable_job_count",
                "retained_failed_job_count",
                "prior_attempt_count",
            )
            return (
                result.get("status") == "recovered"
                and result.get("failed_plan_token") == params.get("failed_plan_token")
                and TendwireClient._valid_opaque(result.get("failed_plan_token"), "twplan1.")
                and TendwireClient._valid_opaque(result.get("plan_token"), "twplan1.")
                and TendwireClient._valid_opaque(result.get("content_revision"), "twrev1.")
                and isinstance(result.get("state"), str)
                and bool(result["state"])
                and all(type(result.get(key)) is int for key in integer_fields)
                and type(result.get("idempotent_replay")) is bool
            )
        valid_plan = (
            result.get("status") == "ok"
            and isinstance(result.get("host_id"), str)
            and bool(result["host_id"])
            and result.get("name") == str(params.get("name") or "")
            and TendwireClient._valid_opaque(result.get("plan_token"), "twplan1.")
        )
        if action == "part":
            return (
                valid_plan
                and result.get("ordinal") == params.get("ordinal")
                and type(result.get("accepted_parts")) is int
                and result["accepted_parts"] > 0
            )
        count_key = "part_count" if action == "begin" else "job_count"
        return (
            valid_plan
            and action in {"begin", "commit"}
            and isinstance(result.get("state"), str)
            and bool(result["state"])
            and type(result.get("generation")) is int
            and type(result.get(count_key)) is int
            and result[count_key] >= (1 if action == "begin" else 0)
            and (
                action != "begin"
                or type(result.get("accepted_parts")) is int
                and result["accepted_parts"] >= 0
            )
        )

    @staticmethod
    def _valid_connector_item(item: Any) -> bool:
        return (
            isinstance(item, dict)
            and TendwireClient._valid_opaque(item.get("ref"), "twref1.")
            and isinstance(item.get("key"), str)
            and bool(item["key"])
            and type(item.get("attempt")) is int
            and item["attempt"] > 0
            and isinstance(item.get("leased_until"), str)
            and bool(item["leased_until"])
            and isinstance(item.get("available_at"), str)
            and bool(item["available_at"])
            and isinstance(item.get("payload"), dict)
        )

    @staticmethod
    def _valid_opaque(value: Any, prefix: str) -> bool:
        if not isinstance(value, str) or not value.startswith(prefix):
            return False
        body = value[len(prefix) :]
        return bool(body) and len(value) <= 264 and all(
            char.isascii() and (char.isalnum() or char in "_-")
            for char in body
        )

    def connector_poll(
        self,
        *,
        name: str = "attention",
        limit: int = 3,
        lease_seconds: int = 60,
    ) -> dict[str, Any]:
        return self._connector(
            "poll",
            {"name": name, "limit": limit, "lease_seconds": lease_seconds},
            protocol=name == TURN_FINAL_CONNECTOR,
        )

    def connector_ack(
        self,
        ref: str,
        response: dict[str, Any] | None = None,
        *,
        name: str = "attention",
    ) -> dict[str, Any]:
        params: dict[str, Any] = {"name": name, "ref": str(ref)}
        if response is not None:
            params["response"] = (
                _protocol_prune(response)
                if name == TURN_FINAL_CONNECTOR
                else public_prune(response)
            )
        return self._connector("ack", params, protocol=name == TURN_FINAL_CONNECTOR)

    def connector_fail(
        self,
        ref: str,
        error: str,
        *,
        name: str = "attention",
    ) -> dict[str, Any]:
        return self._connector(
            "fail",
            {
                "name": name,
                "ref": str(ref),
                "reason": sanitize_text(error, 240),
            },
            protocol=name == TURN_FINAL_CONNECTOR,
        )

    def _prepare(self, request: dict[str, Any]) -> dict[str, Any]:
        encoded = json.dumps(
            request, separators=(",", ":"), ensure_ascii=False
        ).encode("utf-8")
        if len(encoded) > CONNECTOR_PREPARE_MAX_REQUEST_BYTES:
            return {
                "ok": False,
                "status": "prepare_request_too_large",
                "error": "connector.prepare request exceeds the Herdres client bound",
                "max_request_bytes": CONNECTOR_PREPARE_MAX_REQUEST_BYTES,
            }
        return self._connector("prepare", request, protocol=True)

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
            "schema_version": 1,
            "action": "begin",
            "name": TURN_FINAL_CONNECTOR,
            "turn_id": str(turn_id),
            "content_revision": str(content_revision),
            "presentation_version": str(presentation_version),
            "part_count": part_count,
        }
        if source_ref is not None:
            request["source_ref"] = str(source_ref)
        return self._prepare(request)

    def connector_prepare_part(
        self,
        *,
        plan_token: str,
        ordinal: int,
        spans: list[dict[str, Any]],
    ) -> dict[str, Any]:
        if not isinstance(spans, list) or not 1 <= len(spans) <= CONNECTOR_PREPARE_MAX_SPANS:
            return {
                "ok": False,
                "status": "invalid_prepare_part",
                "error": (
                    f"spans must contain 1..{CONNECTOR_PREPARE_MAX_SPANS} "
                    "canonical ranges"
                ),
            }

        def invalid_span(span: Any) -> bool:
            return (
                not isinstance(span, dict)
                or set(span) != {"field", "start_char", "end_char"}
                or span.get("field")
                not in {"user_text", "assistant_final_text"}
                or type(span.get("start_char")) is not int
                or type(span.get("end_char")) is not int
                or span["start_char"] < 0
                or span["end_char"] <= span["start_char"]
            )

        if any(invalid_span(span) for span in spans):
            return {
                "ok": False,
                "status": "invalid_prepare_part",
                "error": "each span must be a non-empty canonical user/final range",
            }
        return self._prepare(
            {
                "schema_version": 1,
                "action": "part",
                "name": TURN_FINAL_CONNECTOR,
                "plan_token": str(plan_token),
                "ordinal": ordinal,
                "spans": spans,
            }
        )

    def connector_prepare_commit(
        self,
        *,
        plan_token: str,
        source_ref: str | None = None,
    ) -> dict[str, Any]:
        request: dict[str, Any] = {
            "schema_version": 1,
            "action": "commit",
            "name": TURN_FINAL_CONNECTOR,
            "plan_token": str(plan_token),
        }
        if source_ref is not None:
            request["source_ref"] = str(source_ref)
        return self._prepare(request)

    def connector_prepare_recover(
        self,
        *,
        failed_plan_token: str,
        request_id: str,
    ) -> dict[str, Any]:
        token_ok = (
            isinstance(failed_plan_token, str)
            and failed_plan_token.startswith("twplan1.")
            and 8 < len(failed_plan_token) <= 264
            and all(
                char.isascii() and (char.isalnum() or char in "_-")
                for char in failed_plan_token[8:]
            )
        )
        request_ok = (
            isinstance(request_id, str)
            and 1 <= len(request_id) <= 128
            and all(
                char.isascii() and (char.isalnum() or char in "._-")
                for char in request_id
            )
        )
        if not token_ok or not request_ok:
            return {
                "ok": False,
                "status": "invalid_recovery_request",
                "error": "recovery coordinates must be public-safe opaque values",
            }
        return self._prepare(
            {
                "schema_version": 1,
                "action": "recover",
                "name": TURN_FINAL_CONNECTOR,
                "failed_plan_token": failed_plan_token,
                "request_id": request_id,
            }
        )

    def turn_final_poll(self, *, limit: int = 1, lease_seconds: int = 60) -> dict[str, Any]:
        return self.connector_poll(
            name=TURN_FINAL_CONNECTOR,
            limit=limit,
            lease_seconds=lease_seconds,
        )

    def turn_final_ack(self, ref: str, response: dict[str, Any] | None = None) -> dict[str, Any]:
        return self.connector_ack(ref, response, name=TURN_FINAL_CONNECTOR)

    def turn_final_fail(self, ref: str, reason: str) -> dict[str, Any]:
        return self.connector_fail(ref, reason, name=TURN_FINAL_CONNECTOR)

    def turn_final_defer(
        self,
        ref: str,
        reason: str = "",
        *,
        available_at: str | None = None,
        delay_seconds: int | None = None,
    ) -> dict[str, Any]:
        params: dict[str, Any] = {
            "name": TURN_FINAL_CONNECTOR,
            "ref": str(ref),
            "reason": sanitize_text(reason, 240),
        }
        if available_at is not None:
            params["available_at"] = str(available_at)
        if delay_seconds is not None:
            params["delay_seconds"] = int(delay_seconds)
        return self._connector("defer", params, protocol=True)
