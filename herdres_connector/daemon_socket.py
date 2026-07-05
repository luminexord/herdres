"""Direct Unix-socket client for the Tendwire daemon.

In source mode herdres reads the neutral snapshot/turns on every sync AND every plugin event. Spawning
the `tendwire` CLI for that (Python startup + package import, ~0.6-2s) is far too slow to run under the
global sync lock — at a high event rate the `herdres event` processes pile up behind the lock. The
daemon already holds the warm store and answers its Unix socket in ~20ms, so we query that directly
and fall back to the CLI only when the socket is unavailable.

Protocol (matches tendwire.daemon_api.UnixSocketJSONServer): one newline-framed JSON request
`{"id","method","params"}` -> one newline-framed JSON response `{"ok": bool, "result"|"error": ...}`.
The daemon's `snapshot.get`/`turn.list` results are byte-for-byte the same payloads the CLI prints, so
callers consume them identically. Every failure raises DaemonUnavailable so the caller can fall back.
"""
from __future__ import annotations

import json
import socket
import time
from typing import Any

_MAX_RESPONSE_BYTES = 16 * 1024 * 1024
_RECV = 65536


class DaemonUnavailable(Exception):
    """The Tendwire daemon socket could not be reached or returned an error."""


def request(socket_path: str, method: str, params: dict[str, Any] | None = None, *, timeout: float = 2.0) -> dict[str, Any]:
    """Send one request to the daemon socket and return the `result` object. Raises DaemonUnavailable
    on any connect/timeout/protocol/error-response failure (never returns partial or error state)."""
    if not socket_path:
        raise DaemonUnavailable("no daemon socket path configured")
    if not hasattr(socket, "AF_UNIX"):
        raise DaemonUnavailable("Unix domain sockets are not supported on this platform")
    deadline = time.monotonic() + max(0.05, float(timeout))
    payload = json.dumps({"id": "herdres", "method": str(method), "params": params or {}}).encode("utf-8") + b"\n"
    buf = bytearray()
    sock: socket.socket | None = None
    try:
        # Socket creation is inside the try so fd exhaustion (EMFILE) becomes DaemonUnavailable and
        # the caller falls back to the CLI, rather than escaping as a raw OSError.
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        sock.settimeout(max(0.01, deadline - time.monotonic()))
        sock.connect(socket_path)
        sock.sendall(payload)
        while b"\n" not in buf:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise DaemonUnavailable("daemon socket timed out")
            sock.settimeout(remaining)
            chunk = sock.recv(_RECV)
            if not chunk:
                break
            buf.extend(chunk)
            if len(buf) > _MAX_RESPONSE_BYTES:
                raise DaemonUnavailable("daemon response exceeded the size cap")
    except DaemonUnavailable:
        raise
    except (OSError, socket.timeout) as exc:
        raise DaemonUnavailable(f"daemon socket error: {exc}") from exc
    finally:
        if sock is not None:
            try:
                sock.close()
            except OSError:
                pass
    line = bytes(buf).split(b"\n", 1)[0]
    if not line:
        raise DaemonUnavailable("empty daemon response")
    try:
        response = json.loads(line.decode("utf-8"))
    except (ValueError, UnicodeDecodeError) as exc:
        raise DaemonUnavailable(f"malformed daemon response: {exc}") from exc
    if not isinstance(response, dict) or not response.get("ok"):
        err = response.get("error") if isinstance(response, dict) else response
        raise DaemonUnavailable(f"daemon returned an error: {err}")
    result = response.get("result")
    return result if isinstance(result, dict) else {}


def snapshot(socket_path: str, *, timeout: float = 2.0) -> dict[str, Any]:
    """Neutral snapshot (same shape as `tendwire snapshot --json`)."""
    return request(socket_path, "snapshot.get", timeout=timeout)


def turns(socket_path: str, *, timeout: float = 2.0) -> dict[str, Any]:
    """Turn feed (same shape as `tendwire turns --json`)."""
    return request(socket_path, "turn.list", timeout=timeout)


def connector(socket_path: str, action: str, params: dict[str, Any] | None = None, *, timeout: float = 2.0) -> dict[str, Any]:
    """Neutral connector-outbox op (poll/ack/fail/defer/reclaim) — same payloads as
    `tendwire connector <action> --json`. `action` maps to the `connector.<action>` daemon method."""
    return request(socket_path, f"connector.{str(action).strip().lower()}", params, timeout=timeout)
