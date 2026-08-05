"""Strict, bounded AF_UNIX client for Tendwire's public daemon API."""

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
from typing import Any, Mapping

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
TURN_LIST_PAGE_LIMIT = 250
TURN_LIST_MAX_PAGES = 256
TURN_FINAL_CONNECTOR = "turn-final"
CONNECTOR_PREPARE_MAX_SPANS = 256
CONNECTOR_PREPARE_MAX_REQUEST_BYTES = 64 * 1024
CONNECTOR_PROCESS_TIMEOUT_SECONDS = 20
DAEMON_API_SCHEMA_VERSION = 1
DAEMON_MAX_FRAME_BYTES = 1024 * 1024

_COMMAND_FIELDS = {
    "send_instruction": (
        {
            "schema_version", "action", "request_id", "dry_run", "target",
            "instruction", "response_schema_version",
        },
        set(),
    ),
    "answer_decision": (
        {"schema_version", "action", "request_id", "dry_run", "target", "params"},
        set(),
    ),
}
_RESPONSE_FIELDS = {
    "schema_version", "action", "request_id", "ok", "dry_run",
    "status", "disposition", "result", "error", "warnings",
}
_SEND_RESULT_FIELDS = {
    "target", "delivery_state", "transport_state", "target_state_at_send",
    "observed_turn_state",
}
_SEND_RESULT_OPTIONAL = {"submission_id", "submission_verdict", "turn_id"}
_SEND_REJECTIONS = {
    "rejected", "stale_target", "backend_unavailable", "backend_unsupported",
    "ambiguous_backend_target", "backend_failed", "duplicate_request",
}
_SEND_NO_RECEIPT = _SEND_REJECTIONS | {"invalid_request", "not_found", "ambiguous_target"}
_DECISION_FAILURES = {
    "decision_not_pending", "invalid_selection", "unsupported_decision", "unknown_worker",
}
_DISPOSITIONS = {
    "no_receipt", "in_progress", "terminal_accepted", "terminal_rejected",
    "terminal_uncertain",
}
_EXACT_PROTOCOL_TEXT_KEYS = {
    "user_text", "assistant_final_text", "text", "plan_token", "failed_plan_token",
    "recovered_plan_token", "replaces_plan_token", "recovers_plan_token",
    "final_identity", "content_revision", "key",
}
_ALLOWED_PROTOCOL_TOKEN_KEYS = {
    "plan_token", "failed_plan_token", "recovered_plan_token", "replaces_plan_token",
    "recovers_plan_token",
}
_PUBLIC_CHARS = frozenset(
    "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789._-"
)
_OPAQUE_CHARS = _PUBLIC_CHARS - {"."}
_NOT_STARTED = object()
_AMBIGUOUS = object()


class _RPCResult(dict[str, Any]):
    __slots__ = ("origin", "_not_started", "_ambiguous")

    def __init__(self, value: Mapping[str, Any], *, origin: str) -> None:
        super().__init__(value)
        self.origin = origin
        self._not_started = None
        self._ambiguous = None


def command_process_ambiguous(result: Any) -> bool:
    return isinstance(result, _RPCResult) and result._ambiguous is _AMBIGUOUS


def command_process_not_started(result: Any) -> bool:
    return isinstance(result, _RPCResult) and result._not_started is _NOT_STARTED


def _keys(
    value: Any,
    required: set[str],
    optional: set[str] | frozenset[str] = frozenset(),
) -> bool:
    return (
        isinstance(value, dict)
        and required <= set(value) <= required | set(optional)
    )


def _text(value: Any) -> bool:
    return isinstance(value, str) and bool(value.strip())


def _error(status: str, message: str, **extra: Any) -> dict[str, Any]:
    return {"ok": False, "status": status, "error": message, **extra}


def _opaque(value: Any, prefix: str, *, limit: int = 264) -> bool:
    if not isinstance(value, str) or not value.startswith(prefix):
        return False
    suffix = value[len(prefix) :]
    return bool(suffix) and len(value) <= limit and all(
        char in _OPAQUE_CHARS for char in suffix
    )


def _invalid_command_request() -> dict[str, Any]:
    return _error(
        "invalid_request", "Herdres command request is not an exact public command object"
    )


def _request_state_uncertain(request: dict[str, Any]) -> _RPCResult:
    result = _RPCResult(
        {
            "ok": False,
            "status": "request_state_uncertain",
            "error": "Tendwire command result was lost after request start",
            "request_id": request["request_id"],
            "action": request["action"],
        },
        origin="transport",
    )
    result._ambiguous = _AMBIGUOUS
    return result


def _exact_command_request(value: Any) -> dict[str, Any] | None:
    if not isinstance(value, dict) or value.get("action") not in _COMMAND_FIELDS:
        return None
    required, optional = _COMMAND_FIELDS[value["action"]]
    if not _keys(value, required, optional) or value.get("schema_version") != 1:
        return None
    if isinstance(value.get("schema_version"), bool) or value.get("dry_run") is not False:
        return None
    try:
        validate_request_id(value.get("request_id"))
    except ValueError:
        return None
    target = value.get("target")
    if not isinstance(target, dict) or not all(
        _text(item) for item in target.values()
    ):
        return None
    if value["action"] == "send_instruction":
        allowed_targets = (
            {"worker_id"},
            {"worker_id", "worker_fingerprint"},
            {"space_id"},
        )
        instruction = value.get("instruction")
        valid = set(target) in allowed_targets and _keys(instruction, {"text"})
        valid = valid and isinstance(instruction["text"], str) and bool(instruction["text"])
        valid = valid and type(value.get("response_schema_version")) is int
        valid = valid and value["response_schema_version"] == 3
        return value if valid else None
    if set(target) != {"worker_id"} or not _keys(
        value.get("params"), {"decision_ref", "selection"}
    ):
        return None
    params = value["params"]
    selection = params["selection"]
    if not _text(params["decision_ref"]) or not isinstance(selection, dict):
        return None
    if set(selection) == {"text"}:
        return value if _text(selection["text"]) else None
    refs = selection.get("option_refs")
    valid = set(selection) == {"option_refs"} and isinstance(refs, list) and bool(refs)
    valid = (
        valid
        and all(_text(item) for item in refs)
        and len(refs) == len(set(refs))
    )
    return value if valid else None


def _valid_response_shell(
    response: Any,
    request: dict[str, Any],
    schemas: set[int],
) -> bool:
    if not isinstance(response, dict) or set(response) != _RESPONSE_FIELDS:
        return False
    return all(
        (
            type(response.get("schema_version")) is int,
            response["schema_version"] in schemas,
            response.get("request_id") == request["request_id"],
            response.get("action") == request["action"],
            response.get("dry_run") is False,
            type(response.get("ok")) is bool,
            _text(response.get("status")),
            response.get("disposition") in _DISPOSITIONS,
            isinstance(response.get("warnings"), list),
            all(isinstance(item, str) for item in response["warnings"]),
            public_prune(response) == response,
        )
    )


def _valid_send_result(result: Any, request: dict[str, Any], schema: int) -> bool:
    if not _keys(result, _SEND_RESULT_FIELDS, _SEND_RESULT_OPTIONAL):
        return False
    target = result["target"]
    requested = request["target"].get("worker_id")
    if not _keys(target, {"worker_id"}):
        return False
    return all(
        (
            _text(target.get("worker_id")),
            not isinstance(requested, str) or target.get("worker_id") == requested,
            result.get("delivery_state") == result.get("transport_state") == "submitted",
            _text(result.get("target_state_at_send")),
            result.get("observed_turn_state")
            in {"pending_observation", "observed", "complete", "linked"},
            result.get("turn_id") is None or _text(result.get("turn_id")),
            "submission_id" not in result or _text(result.get("submission_id")),
            result.get("submission_verdict", "submitted")
            in {"submitted", "written_to_pty"},
            schema != 2 or "submission_id" not in result,
        )
    )


def _valid_failure(response: dict[str, Any]) -> bool:
    error = response.get("error")
    return bool(
        response.get("result") is None
        and isinstance(error, dict)
        and error.get("code") in {None, response["status"]}
        and _text(error.get("message"))
    )


def _validated_command_response(
    response: Any,
    request: dict[str, Any],
) -> dict[str, Any] | None:
    if not _valid_response_shell(response, request, {2, 3}):
        return None
    status, disposition = response["status"], response["disposition"]
    if response["ok"] is True:
        valid = all(
            (
                status == "accepted",
                disposition == "terminal_accepted",
                response["schema_version"] == 3,
                response.get("error") is None,
                _valid_send_result(
                    response.get("result"), request, response["schema_version"]
                ),
            )
        )
        return response if valid else None
    allowed_pairs = {
        "in_progress": {"pending"},
        "terminal_uncertain": {"request_state_uncertain"},
        "terminal_rejected": _SEND_REJECTIONS,
        "no_receipt": _SEND_NO_RECEIPT,
    }
    allowed = status in allowed_pairs.get(disposition, set())
    return response if allowed and _valid_failure(response) else None


def _validated_decision_response(
    response: Any,
    request: dict[str, Any],
) -> dict[str, Any] | None:
    if not _valid_response_shell(response, request, {2}):
        return None
    status, disposition = response["status"], response["disposition"]
    if response["ok"] is True:
        result = response.get("result")
        fields = {
            "target", "decision", "delivery_state", "transport_state",
            "observed_pending_state",
        }
        if not _keys(result, fields):
            return None
        valid = all(
            (
                status == "accepted",
                disposition == "terminal_accepted",
                response.get("error") is None,
                result.get("target") == request["target"],
                result.get("decision")
                == {"decision_ref": request["params"]["decision_ref"]},
                result.get("delivery_state")
                == result.get("transport_state")
                == "submitted",
                result.get("observed_pending_state") == "pending_observation",
            )
        )
        return response if valid else None
    allowed = (
        status == "answer_in_progress" and disposition in {"no_receipt", "in_progress"}
    ) or (
        status in _DECISION_FAILURES
        and disposition in {"no_receipt", "terminal_rejected"}
    )
    return response if allowed and _valid_failure(response) else None


def _protocol_prune(
    value: Any,
    *,
    _depth: int = 0,
    _budget: list[int] | None = None,
    _seen: set[int] | None = None,
    _exact: bool = False,
) -> Any:
    budget = [PRIVATE_STRUCTURE_MAX_ITEMS] if _budget is None else _budget
    seen = set() if _seen is None else _seen
    budget[0] -= 1
    if budget[0] < 0 or _depth > PRIVATE_STRUCTURE_MAX_DEPTH:
        return None
    if isinstance(value, (dict, list)):
        if id(value) in seen:
            return None
        seen.add(id(value))
    if isinstance(value, dict):
        clean: dict[str, Any] = {}
        for raw_key, item in value.items():
            key = str(raw_key)
            forbidden = key in FORBIDDEN_PUBLIC_KEYS or "secret" in key.lower()
            forbidden |= "token" in key.lower() and key not in _ALLOWED_PROTOCOL_TOKEN_KEYS
            if not forbidden:
                clean[key] = _protocol_prune(
                    item,
                    _depth=_depth + 1,
                    _budget=budget,
                    _seen=seen,
                    _exact=key in _EXACT_PROTOCOL_TEXT_KEYS,
                )
        return clean
    if isinstance(value, list):
        return [
            _protocol_prune(item, _depth=_depth + 1, _budget=budget, _seen=seen,
                            _exact=_exact)
            for item in value
        ]
    if isinstance(value, str):
        return value if _exact else sanitize_text(value, PRUNE_TEXT_LIMIT)
    return value


def _schema_error(
    status: str,
    message: str,
    received: Any,
    key: str,
    version: int,
) -> dict[str, Any]:
    return _error(
        status, message, **{key: version, "received_schema_version": received}
    )


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
    def _transport_error(status: str, message: str, *, started: bool) -> _RPCResult:
        result = _RPCResult(
            {
                "ok": False,
                "status": status,
                "error": sanitize_text(message, 300),
            },
            origin="transport",
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
        parent_fd, pin_fd = os.open("/", flags), -1
        try:
            for part in path.parts[1:-1]:
                next_fd = os.open(part, flags | nofollow, dir_fd=parent_fd)
                os.close(parent_fd)
                parent_fd = next_fd
            parent = os.fstat(parent_fd)
            parent_is_private = not stat.S_IMODE(parent.st_mode) & 0o022
            if parent.st_uid != os.geteuid() or not parent_is_private:
                raise OSError("Tendwire socket parent is not private and owned")
            current = os.stat(path.name, dir_fd=parent_fd, follow_symlinks=False)
            socket_is_private = not stat.S_IMODE(current.st_mode) & ~0o600
            if not stat.S_ISSOCK(current.st_mode) or current.st_uid != os.geteuid():
                raise OSError("Tendwire endpoint is not a private owned socket")
            if not socket_is_private:
                raise OSError("Tendwire endpoint is not a private owned socket")
            pin_flags = getattr(os, "O_PATH", os.O_RDONLY) | nofollow
            pin_flags |= getattr(os, "O_CLOEXEC", 0)
            pin_fd = os.open(path.name, pin_flags, dir_fd=parent_fd)
            pinned = os.fstat(pin_fd)
            identity = (current.st_dev, current.st_ino)
            if (pinned.st_dev, pinned.st_ino) != identity:
                raise OSError("Tendwire socket changed while pinning")
            if not stat.S_ISSOCK(pinned.st_mode):
                raise OSError("Tendwire socket changed while pinning")
            return parent_fd, pin_fd, path.name, identity
        except Exception:
            if pin_fd >= 0:
                os.close(pin_fd)
            os.close(parent_fd)
            raise

    @staticmethod
    def _anchored_socket_address(parent_fd: int, leaf: str) -> str:
        if os.name != "posix" or parent_fd < 0:
            raise OSError("Tendwire socket anchoring is unsupported")
        expected = os.fstat(parent_fd)
        anchor = f"/proc/self/fd/{parent_fd}"
        try:
            current = os.stat(anchor)
        except OSError:
            raise OSError("Tendwire socket anchoring is unavailable") from None
        if not stat.S_ISDIR(expected.st_mode):
            raise OSError("Tendwire socket anchor is invalid")
        if (current.st_dev, current.st_ino) != (expected.st_dev, expected.st_ino):
            raise OSError("Tendwire socket anchor is invalid")
        return f"{anchor}/{leaf}"

    @staticmethod
    def _validate_peer(conn: socket.socket) -> None:
        if not hasattr(socket, "SO_PEERCRED"):
            raise OSError("Tendwire daemon peer validation is unsupported")
        credentials = struct.Struct("3i")
        try:
            raw = conn.getsockopt(socket.SOL_SOCKET, socket.SO_PEERCRED, credentials.size)
            pid, uid, gid = credentials.unpack(raw)
        except (OSError, struct.error):
            raise OSError("Tendwire daemon peer validation failed") from None
        if pid <= 0 or uid != os.geteuid() or gid < 0:
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
    def _outer_result(response: Any, request_id: str) -> _RPCResult:
        fields = {"schema_version", "ok", "status", "result", "error", "id"}
        valid = bool(
            isinstance(response, dict)
            and set(response) == fields
            and type(response.get("schema_version")) is int
            and response["schema_version"] == DAEMON_API_SCHEMA_VERSION
            and response.get("id") == request_id
            and type(response.get("ok")) is bool
        )
        if not valid:
            raise ValueError("Tendwire daemon returned an invalid response envelope")
        if response["ok"] is True:
            invalid_success = response.get("status") != "ok"
            invalid_success |= not isinstance(response.get("result"), dict)
            invalid_success |= response.get("error") is not None
            if invalid_success:
                raise ValueError("Tendwire daemon returned an invalid success envelope")
            return _RPCResult(response["result"], origin="inner")
        error = response.get("error")
        if (
            response.get("status") != "error"
            or response.get("result") is not None
            or not isinstance(error, dict)
            or not _text(error.get("code"))
            or not _text(error.get("message"))
        ):
            raise ValueError("Tendwire daemon returned an invalid error envelope")
        return _RPCResult({
            "ok": False, "status": error["code"], "error": public_prune(error),
        }, origin="outer")

    def _request(
        self,
        method: str,
        params: dict[str, Any] | None = None,
        *,
        timeout: float | None = None,
    ) -> _RPCResult:
        request_id = f"r{secrets.token_hex(12)}"
        try:
            payload = {"id": request_id, "method": method, "params": dict(params or {})}
            raw = json.dumps(
                payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"),
                allow_nan=False,
            ).encode("utf-8") + b"\n"
        except (TypeError, ValueError, UnicodeError):
            return self._transport_error(
                "invalid_request", "Tendwire request is not bounded JSON", started=False
            )
        if len(raw) > DAEMON_MAX_FRAME_BYTES:
            return self._transport_error(
                "request_too_large", "Tendwire request exceeds the size bound",
                started=False,
            )
        request_timeout = self._timeout_seconds()
        if timeout is not None:
            request_timeout = max(1.0, float(timeout))
        deadline = time.monotonic() + request_timeout
        started = False
        parent_fd = pin_fd = -1
        try:
            parent_fd, pin_fd, leaf, identity = self._pin_socket(self._path())
            with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as conn:
                conn.settimeout(max(0.001, deadline - time.monotonic()))
                conn.connect(self._anchored_socket_address(parent_fd, leaf))
                current = os.stat(leaf, dir_fd=parent_fd, follow_symlinks=False)
                pinned = os.fstat(pin_fd)
                if (current.st_dev, current.st_ino) != identity:
                    raise OSError("Tendwire socket changed during connection")
                if (pinned.st_dev, pinned.st_ino) != identity:
                    raise OSError("Tendwire socket changed during connection")
                self._validate_peer(conn)
                started = True
                conn.sendall(raw)
                frame = self._read_frame(conn, deadline)
            return self._outer_result(json.loads(frame.decode("utf-8")), request_id)
        except (TimeoutError, socket.timeout):
            return self._transport_error(
                "daemon_timeout", "Tendwire daemon request timed out", started=started
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
    def _clean(result: Mapping[str, Any], *, protocol: bool = False) -> dict[str, Any]:
        clean = _protocol_prune(result) if protocol else public_prune(result)
        if isinstance(clean, dict):
            return clean
        return _error("daemon_protocol_error", "invalid Tendwire result")

    def snapshot(self) -> dict[str, Any]:
        return self._clean(self._request("snapshot.get"))

    def pending(self) -> dict[str, Any]:
        return self._clean(self._request("pending.list"))

    def doctor(self) -> dict[str, Any]:
        return self._clean(self._request("health.get", timeout=10))

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
                    "upgrade_required", "Tendwire turn.list schema v2 is required",
                    result.get("schema_version"), "required_turn_schema_version", 2,
                )
            bad_content = any(
                not isinstance(row, dict)
                or not isinstance(row.get("content"), dict)
                or row["content"].get("schema_version") != 1
                for row in turns
            )
            if bad_content:
                return _schema_error(
                    "unsupported_content_schema", "Every turn requires content schema v1",
                    None, "supported_content_schema_version", 1,
                )
            merged = dict(result) if merged is None else merged
            rows.extend(turns)
            cursor = result.get("next_cursor")
            if cursor is None and result.get("has_more") is not True:
                merged.update({"turns": rows, "next_cursor": None, "has_more": False})
                return merged
            if not _text(cursor) or cursor in seen:
                break
            seen.add(cursor)
        return _schema_error(
            "unsupported_content_schema", "Tendwire turn.list pagination is invalid",
            None, "supported_content_schema_version", 1,
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
        code = str(error.get("code") or "").lower() if isinstance(error, dict) else ""
        status = str(result.get("status") or "").lower()
        unsupported = {"unsupported_method", "unknown_method"}
        if status in unsupported or code in unsupported:
            return {
                "ok": False, "status": "unsupported_method", "schema_version": 1,
                "projection_schema_version": 2,
            }
        if status in {"daemon_timeout", "daemon_unavailable", "daemon_protocol_error"}:
            return {
                "ok": False, "status": "transport_ambiguous",
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
        params = {
            "schema_version": 1, "turn_id": str(turn_id),
            "content_revision": str(content_revision), "field": str(field),
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
                result.get("schema_version"), "supported_content_schema_version", 1,
            )
        return result

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
        if isinstance(result, _RPCResult) and result.origin == "outer":
            return _request_state_uncertain(public_request)
        validator = (
            _validated_decision_response
            if public_request["action"] == "answer_decision"
            else _validated_command_response
        )
        validated = validator(result, public_request)
        if validated is None:
            return _request_state_uncertain(public_request)
        return public_prune(validated)

    def command(self, request: dict[str, Any]) -> dict[str, Any]:
        try:
            request_json = json.dumps(request, separators=(",", ":"), ensure_ascii=False)
            return self.command_json(request_json)
        except (TypeError, ValueError):
            return _invalid_command_request()

    def _connector(
        self,
        method: str,
        params: dict[str, Any],
        *,
        protocol: bool = False,
    ) -> dict[str, Any]:
        raw = self._request(
            f"connector.{method}", params, timeout=CONNECTOR_PROCESS_TIMEOUT_SECONDS
        )
        clean = self._clean(raw, protocol=protocol)
        if isinstance(raw, _RPCResult) and raw.origin in {"transport", "outer"}:
            return clean
        if self._valid_connector_result(method, params, clean):
            return clean
        return _schema_error(
            "invalid_connector_response",
            "Tendwire connector returned a malformed schema-v1 envelope",
            clean.get("schema_version"), "supported_content_schema_version", 1,
        )

    @staticmethod
    def _valid_connector_result(
        method: str,
        params: dict[str, Any],
        result: dict[str, Any],
    ) -> bool:
        status, ok = result.get("status"), result.get("ok")
        name = str(params.get("name") or "")
        if type(result.get("schema_version")) is not int or result["schema_version"] != 1:
            return False
        if type(ok) is not bool or not _text(status):
            return False
        if ok is False:
            error = result.get("error")
            return bool(
                _text(result.get("host_id"))
                and result.get("name") == name
                and isinstance(error, dict)
                and error.get("code") == status
                and _text(error.get("message"))
            )
        if result.get("error") is not None:
            return False
        if method == "prepare":
            return TendwireClient._valid_prepare_result(params, result)
        if not _text(result.get("host_id")) or result.get("name") != name:
            return False
        if method == "poll":
            items = result.get("items")
            return (
                status == "ok"
                and isinstance(items, list)
                and all(TendwireClient._valid_connector_item(item) for item in items)
            )
        expected = {
            "ack": {"acknowledged"},
            "fail": {"retry_scheduled", "attempts_exhausted", "superseded"},
            "defer": {"deferred", "superseded"},
        }.get(method)
        if not expected or status not in expected:
            return False
        if result.get("ref") != str(params.get("ref") or ""):
            return False
        if not _opaque(result.get("ref"), "twref1.") or not _text(result.get("key")):
            return False
        if type(result.get("attempt")) is not int or result["attempt"] <= 0:
            return False
        if status in {"retry_scheduled", "deferred"}:
            return _text(result.get("available_at"))
        return True

    @staticmethod
    def _valid_prepare_result(params: dict[str, Any], result: dict[str, Any]) -> bool:
        action = params.get("action")
        valid_plan = bool(
            result.get("status") == "ok"
            and _text(result.get("host_id"))
            and result.get("name") == str(params.get("name") or "")
            and _opaque(result.get("plan_token"), "twplan1.")
        )
        if action == "part":
            return bool(
                valid_plan
                and result.get("ordinal") == params.get("ordinal")
                and type(result.get("accepted_parts")) is int
                and result["accepted_parts"] > 0
            )
        if not valid_plan or action not in {"begin", "commit"}:
            return False
        count = "part_count" if action == "begin" else "job_count"
        if not _text(result.get("state")):
            return False
        if type(result.get("generation")) is not int:
            return False
        if type(result.get(count)) is not int:
            return False
        if result[count] < (1 if action == "begin" else 0):
            return False
        return bool(
            action != "begin"
            or (
                type(result.get("accepted_parts")) is int
                and result["accepted_parts"] >= 0
            )
        )

    @staticmethod
    def _valid_connector_item(item: Any) -> bool:
        if not isinstance(item, dict):
            return False
        attempt = item.get("attempt")
        if type(attempt) is not int:
            return False
        return all(
            (
                _opaque(item.get("ref"), "twref1."),
                _text(item.get("key")),
                attempt > 0,
                _text(item.get("leased_until")),
                _text(item.get("available_at")),
                isinstance(item.get("payload"), dict),
            )
        )

    def connector_poll(
        self, *, name: str = "attention", limit: int = 3, lease_seconds: int = 60
    ) -> dict[str, Any]:
        return self._connector(
            "poll",
            {"name": name, "limit": limit, "lease_seconds": lease_seconds},
            protocol=name == TURN_FINAL_CONNECTOR,
        )

    def connector_ack(
        self, ref: str, response: dict[str, Any] | None = None, *,
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
        self, ref: str, error: str, *, name: str = "attention"
    ) -> dict[str, Any]:
        return self._connector(
            "fail",
            {"name": name, "ref": str(ref), "reason": sanitize_text(error, 240)},
            protocol=name == TURN_FINAL_CONNECTOR,
        )

    def _prepare(self, request: dict[str, Any]) -> dict[str, Any]:
        encoded = json.dumps(
            request, separators=(",", ":"), ensure_ascii=False
        ).encode("utf-8")
        if len(encoded) > CONNECTOR_PREPARE_MAX_REQUEST_BYTES:
            return _error(
                "prepare_request_too_large",
                "connector.prepare request exceeds the Herdres client bound",
                max_request_bytes=CONNECTOR_PREPARE_MAX_REQUEST_BYTES,
            )
        return self._connector("prepare", request, protocol=True)

    def _prepare_action(self, action: str, **params: Any) -> dict[str, Any]:
        request = {
            "schema_version": 1, "action": action, "name": TURN_FINAL_CONNECTOR,
            **params,
        }
        return self._prepare(request)

    def connector_prepare_begin(
        self, *, turn_id: str, content_revision: str,
        presentation_version: str, part_count: int,
        source_ref: str | None = None,
    ) -> dict[str, Any]:
        params: dict[str, Any] = {
            "turn_id": str(turn_id), "content_revision": str(content_revision),
            "presentation_version": str(presentation_version), "part_count": part_count,
        }
        if source_ref is not None:
            params["source_ref"] = str(source_ref)
        return self._prepare_action("begin", **params)

    def connector_prepare_part(
        self, *, plan_token: str, ordinal: int, spans: list[dict[str, Any]]
    ) -> dict[str, Any]:
        valid = isinstance(spans, list) and 1 <= len(spans) <= CONNECTOR_PREPARE_MAX_SPANS
        valid = valid and all(
            _keys(span, {"field", "start_char", "end_char"})
            and span["field"] in {"user_text", "assistant_final_text"}
            and type(span["start_char"]) is int
            and type(span["end_char"]) is int
            and 0 <= span["start_char"] < span["end_char"]
            for span in spans
        )
        if not valid:
            return _error(
                "invalid_prepare_part",
                "spans must contain bounded non-empty canonical ranges",
            )
        return self._prepare_action(
            "part", plan_token=str(plan_token), ordinal=ordinal, spans=spans
        )

    def connector_prepare_commit(
        self, *, plan_token: str, source_ref: str | None = None
    ) -> dict[str, Any]:
        params = {"plan_token": str(plan_token)}
        if source_ref is not None:
            params["source_ref"] = str(source_ref)
        return self._prepare_action("commit", **params)

    def turn_final_poll(
        self, *, limit: int = 1, lease_seconds: int = 60
    ) -> dict[str, Any]:
        return self.connector_poll(
            name=TURN_FINAL_CONNECTOR, limit=limit, lease_seconds=lease_seconds
        )

    def turn_final_ack(
        self, ref: str, response: dict[str, Any] | None = None
    ) -> dict[str, Any]:
        return self.connector_ack(ref, response, name=TURN_FINAL_CONNECTOR)

    def turn_final_fail(self, ref: str, reason: str) -> dict[str, Any]:
        return self.connector_fail(ref, reason, name=TURN_FINAL_CONNECTOR)

    def turn_final_defer(
        self, ref: str, reason: str = "", *,
        available_at: str | None = None,
        delay_seconds: int | None = None,
    ) -> dict[str, Any]:
        params: dict[str, Any] = {
            "name": TURN_FINAL_CONNECTOR, "ref": str(ref),
            "reason": sanitize_text(reason, 240),
        }
        if available_at is not None:
            params["available_at"] = str(available_at)
        if delay_seconds is not None:
            params["delay_seconds"] = int(delay_seconds)
        return self._connector("defer", params, protocol=True)
