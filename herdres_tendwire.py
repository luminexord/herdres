"""Tendwire integration helpers for Herdres.

This module intentionally stays Telegram-free. It owns mode/config parsing and
public Tendwire snapshot conversion; herdres.py keeps Telegram formatting,
delivery, topic state, and state-file mutation.
"""

from __future__ import annotations

import math
import os
import re
import shlex
import hashlib
import json
import subprocess
import threading
from typing import Any, Callable


MODE_VALUES = ("off", "enrich", "commands", "source-read", "source")
LEGACY_TIMER_CONFLICT_MODES = {"commands", "source-read", "source"}
SOURCE_ROUTE_BLOCK_MODES = LEGACY_TIMER_CONFLICT_MODES
LEGACY_HERDR_TOPIC_TIMER = "herdr-telegram-topics.timer"
OPTIONAL_ENV_KEYS = {
    "HERDRES_TENDWIRE_DATA_DIR": "TENDWIRE_DATA_DIR",
    "HERDRES_TENDWIRE_DB_PATH": "TENDWIRE_DB_PATH",
    "HERDRES_TENDWIRE_HOST_ID": "TENDWIRE_HOST_ID",
}
OPTIONAL_PATH_KEYS = {"HERDRES_TENDWIRE_DATA_DIR", "HERDRES_TENDWIRE_DB_PATH"}
HERDR_TIMEOUT_DEFAULT = 1.0
DEFAULT_HERDR_BIN = "herdr"
_CONFIG_ENV_VAR_RE = re.compile(r"\$(\w+)|\$\{([^}]+)\}")

Sanitizer = Callable[[str, int], str]
RawSpacePredicate = Callable[[Any], bool]
SourceEntryPredicate = Callable[[dict[str, Any] | None], bool]
PaneKey = Callable[[dict[str, Any]], str]
Runner = Callable[..., Any]
FeedItemFactory = Callable[[dict[str, Any]], dict[str, Any] | None]
TextHash = Callable[[str], str]


class TendwireCallError(RuntimeError):
    """Public-safe Tendwire CLI failure for Herdres to translate at the boundary."""


def _default_sanitize(value: str, limit: int) -> str:
    return str(value or "")[: max(0, int(limit))]


def bool_env(env: Any, key: str) -> bool:
    return str(env.get(key, "0")).strip().lower() in {"1", "true", "yes", "on"}


def parse_mode(
    env: Any | None = None,
    *,
    diagnose_invalid: bool = False,
    warn_invalid: Callable[[Any], None] | None = None,
) -> str:
    source = os.environ if env is None else env
    raw_mode = source.get("HERDRES_TENDWIRE_MODE")
    if raw_mode is not None:
        mode = str(raw_mode).strip().lower()
        if mode in MODE_VALUES:
            return mode
        if diagnose_invalid and warn_invalid is not None:
            warn_invalid(raw_mode)
        return "off"
    if bool_env(source, "HERDRES_TENDWIRE_HYBRID") or bool_env(source, "HERDRES_TENDWIRE_SNAPSHOT"):
        return "enrich"
    return "off"


def mode_at_least(
    mode: str,
    env: Any | None = None,
    *,
    diagnose_invalid: bool = False,
    warn_invalid: Callable[[Any], None] | None = None,
) -> bool:
    try:
        wanted = MODE_VALUES.index(str(mode or "").strip().lower())
    except ValueError:
        return False
    current = parse_mode(env, diagnose_invalid=diagnose_invalid, warn_invalid=warn_invalid)
    return MODE_VALUES.index(current) >= wanted


def mode_enables_commands(mode: str) -> bool:
    normalized = str(mode or "").strip().lower()
    return normalized in MODE_VALUES and MODE_VALUES.index(normalized) >= MODE_VALUES.index("commands")


def mode_enables_source_inventory(mode: str) -> bool:
    return str(mode or "").strip().lower() in {"source-read", "source"}


def source_mode_blocks_closed_direct_routes(env: Any | None = None) -> bool:
    return parse_mode(env) in SOURCE_ROUTE_BLOCK_MODES


def connector_outbox_enabled(env: Any | None = None) -> bool:
    source = os.environ if env is None else env
    if source.get("HERDRES_TENDWIRE_CONNECTOR_OUTBOX") is not None:
        return bool_env(source, "HERDRES_TENDWIRE_CONNECTOR_OUTBOX")
    return parse_mode(source) == "source"


def bounded_int_env(
    key: str,
    default: int,
    *,
    minimum: int = 1,
    maximum: int = 100,
    env: Any | None = None,
) -> int:
    source = os.environ if env is None else env
    try:
        parsed = int(float(str(source.get(key, str(default)) or str(default))))
    except (TypeError, ValueError):
        parsed = default
    return max(minimum, min(parsed, maximum))


def connector_name(env: Any | None = None) -> str:
    source = os.environ if env is None else env
    raw = str(source.get("HERDRES_TENDWIRE_CONNECTOR_NAME", "attention") or "attention").strip() or "attention"
    clean = re.sub(r"[^A-Za-z0-9_.:-]+", "-", raw).strip(".:-_")
    return clean[:64] or "attention"


def connector_limit(env: Any | None = None) -> int:
    return bounded_int_env("HERDRES_TENDWIRE_CONNECTOR_LIMIT", 3, minimum=1, maximum=20, env=env)


def connector_lease_seconds(env: Any | None = None) -> int:
    return bounded_int_env("HERDRES_TENDWIRE_CONNECTOR_LEASE_SECONDS", 60, minimum=1, maximum=86400, env=env)


def connector_failure_delay_seconds(env: Any | None = None) -> int:
    return bounded_int_env(
        "HERDRES_TENDWIRE_CONNECTOR_FAILURE_DELAY_SECONDS",
        60,
        minimum=0,
        maximum=86400,
        env=env,
    )


def outbox_item_payload(item: dict[str, Any]) -> dict[str, Any]:
    payload = item.get("payload") if isinstance(item.get("payload"), dict) else {}
    return dict(payload) if isinstance(payload, dict) else {}


def outbox_event_type(payload: dict[str, Any], *, sanitize: Sanitizer = _default_sanitize) -> str:
    return sanitize(str(payload.get("event_type") or "attention"), 80).strip() or "attention"


def outbox_item_identity(item: dict[str, Any]) -> str:
    body = {"key": str(item.get("key") or ""), "payload": outbox_item_payload(item)}
    return hashlib.sha256(json.dumps(body, sort_keys=True, separators=(",", ":")).encode("utf-8")).hexdigest()[:24]


def outbox_worker_route_entry(
    state: dict[str, Any],
    payload: dict[str, Any],
    *,
    sanitize: Sanitizer = _default_sanitize,
) -> dict[str, Any] | None:
    attention = payload.get("attention") if isinstance(payload.get("attention"), dict) else {}
    meta = attention.get("meta") if isinstance(attention.get("meta"), dict) else {}
    worker_id = sanitize(str(meta.get("worker_id") or ""), 160).strip()
    if not worker_id:
        source = sanitize(str(attention.get("source") or ""), 160).strip()
        if source.startswith("worker:"):
            worker_id = source.split(":", 1)[1].strip()
    if not worker_id:
        return None

    space_id = sanitize(str(meta.get("space_id") or ""), 160).strip()
    acceptable_space_keys = {space_id, f"workspace:{space_id}"} if space_id else set()
    panes = state.get("panes") if isinstance(state.get("panes"), dict) else {}
    matches: list[dict[str, Any]] = []
    for entry in panes.values():
        if not isinstance(entry, dict):
            continue
        entry_worker_id = str(entry.get("worker_id") or entry.get("tendwire_worker_id") or "").strip()
        if entry_worker_id != worker_id:
            continue
        if not is_source_entry(entry):
            continue
        matches.append(entry)
    if not matches:
        return None
    if acceptable_space_keys:
        for entry in matches:
            if str(entry.get("space_key") or "") in acceptable_space_keys:
                return entry
    return matches[0]


def outbox_delivered_identities(state: dict[str, Any]) -> set[str]:
    audit = state.get("tendwire_outbox") if isinstance(state.get("tendwire_outbox"), dict) else {}
    identities = audit.get("delivered_identities") if isinstance(audit.get("delivered_identities"), list) else []
    return {str(value) for value in identities if str(value)}


def note_outbox_audit(
    state: dict[str, Any],
    event: dict[str, Any],
    *,
    checked_at: str,
    recent_limit: int = 50,
    delivered_limit: int = 200,
) -> None:
    audit = state.setdefault("tendwire_outbox", {})
    if not isinstance(audit, dict):
        audit = {}
        state["tendwire_outbox"] = audit
    audit["last_checked_at"] = str(checked_at or "")
    deliveries = audit.get("recent") if isinstance(audit.get("recent"), list) else []
    deliveries.append(dict(event))
    audit["recent"] = deliveries[-max(1, int(recent_limit)):]
    identity = str(event.get("identity") or "")
    if identity and str(event.get("status") or "") == "delivered":
        identities = audit.get("delivered_identities") if isinstance(audit.get("delivered_identities"), list) else []
        identities.append(identity)
        audit["delivered_identities"] = list(
            dict.fromkeys(str(value) for value in identities if str(value))
        )[-max(1, int(delivered_limit)):]


def outbox_drain_result(enabled: bool) -> dict[str, Any]:
    return {
        "enabled": bool(enabled),
        "polled": 0,
        "delivered": 0,
        "acked": 0,
        "failed": 0,
        "deferred": 0,
        "changed": False,
    }


def outbox_preflight_status(
    *,
    delivery_configured: bool,
    remaining_sends: int,
) -> str:
    if not delivery_configured:
        return "telegram_unconfigured"
    if int(remaining_sends) <= 0:
        return "send_cap_exhausted"
    return ""


def outbox_prepare_drain(
    *,
    enabled: bool,
    max_sends: int,
    sent_count: int,
    delivery_configured: bool,
    limit: int | None = None,
    env: Any | None = None,
) -> dict[str, Any]:
    result = outbox_drain_result(bool(enabled))
    if not enabled:
        return {"result": result, "should_poll": False, "remaining_sends": 0, "poll_params": None}
    remaining = max(0, int(max_sends) - int(sent_count))
    preflight_status = outbox_preflight_status(
        delivery_configured=delivery_configured,
        remaining_sends=remaining,
    )
    if preflight_status:
        result["status"] = preflight_status
        return {"result": result, "should_poll": False, "remaining_sends": remaining, "poll_params": None}
    return {
        "result": result,
        "should_poll": True,
        "remaining_sends": remaining,
        "poll_params": outbox_poll_params(remaining_sends=remaining, limit=limit, env=env),
    }


def outbox_apply_poll_response(
    result: dict[str, Any],
    poll: dict[str, Any],
    *,
    sanitize: Sanitizer = _default_sanitize,
) -> tuple[list[dict[str, Any]], dict[str, Any] | None]:
    if not poll.get("ok"):
        status = str(poll.get("status") or "poll_failed")
        result["status"] = status
        result["error"] = sanitize(str(poll.get("error") or ""), 300)
        result["changed"] = True
        return [], {"status": status}
    items = [item for item in poll.get("items", []) if isinstance(item, dict)]
    result["polled"] = len(items)
    return items, None


def outbox_poll_params(
    *,
    remaining_sends: int,
    limit: int | None = None,
    env: Any | None = None,
) -> dict[str, Any]:
    remaining = max(1, int(remaining_sends))
    requested_limit = connector_limit(env) if limit is None else max(1, int(limit))
    return {
        "name": connector_name(env),
        "limit": min(requested_limit, remaining),
        "lease_seconds": connector_lease_seconds(env),
    }


def outbox_ack_params(
    ref: str,
    item: dict[str, Any],
    *,
    sent: bool = True,
    deduplicated: bool = False,
    sanitize: Sanitizer = _default_sanitize,
    env: Any | None = None,
) -> dict[str, Any]:
    response: dict[str, Any] = {
        "sent": bool(sent),
        "event_type": outbox_event_type(outbox_item_payload(item), sanitize=sanitize),
    }
    if deduplicated:
        response["deduplicated"] = True
    return {"name": connector_name(env), "ref": str(ref or ""), "response": response}


def outbox_defer_params(
    ref: str,
    *,
    reason: str,
    delay_seconds: int,
    sent: bool = False,
    sanitize: Sanitizer = _default_sanitize,
    env: Any | None = None,
) -> dict[str, Any]:
    return {
        "name": connector_name(env),
        "ref": str(ref or ""),
        "reason": sanitize(str(reason or "deferred"), 120).strip() or "deferred",
        "delay_seconds": max(0, int(delay_seconds)),
        "response": {"sent": bool(sent)},
    }


def outbox_fail_params(
    ref: str,
    *,
    reason: str = "send_failed",
    delay_seconds: int | None = None,
    sent: bool = False,
    sanitize: Sanitizer = _default_sanitize,
    env: Any | None = None,
) -> dict[str, Any]:
    delay = connector_failure_delay_seconds(env) if delay_seconds is None else max(0, int(delay_seconds))
    return {
        "name": connector_name(env),
        "ref": str(ref or ""),
        "reason": sanitize(str(reason or "send_failed"), 120).strip() or "send_failed",
        "delay_seconds": delay,
        "response": {"sent": bool(sent)},
    }


def _env_source(env: Any | None = None) -> Any:
    return os.environ if env is None else env


def _env_get_str(env: Any, key: str, default: str = "") -> str:
    value = env.get(key, default)
    return "" if value is None else str(value)


def _expand_config_path(value: str, env: Any | None = None) -> str:
    source = _env_source(env)

    def repl(match: re.Match[str]) -> str:
        name = match.group(1) or match.group(2) or ""
        replacement = source.get(name)
        return match.group(0) if replacement is None else str(replacement)

    expanded = _CONFIG_ENV_VAR_RE.sub(repl, str(value))
    if expanded == "~" or expanded.startswith("~/"):
        home = _env_get_str(source, "HOME").strip()
        if home:
            return home.rstrip("/") + expanded[1:]
    return os.path.expanduser(expanded)


def _command_token_path_like(token: str) -> bool:
    return token.startswith("~") or "/" in token or "\\" in token or "$" in token


def command_base(env: Any | None = None) -> list[str]:
    source = _env_source(env)
    raw = _env_get_str(source, "HERDRES_TENDWIRE_BIN", "tendwire").strip()
    try:
        parts = shlex.split(raw)
    except ValueError:
        parts = []
    if parts and _command_token_path_like(parts[0]):
        parts[0] = _expand_config_path(parts[0], source)
    return parts or ["tendwire"]


def timeout_seconds(env: Any | None = None) -> int:
    source = _env_source(env)
    try:
        return max(1, int(float(_env_get_str(source, "HERDRES_TENDWIRE_TIMEOUT_SECONDS", "5"))))
    except (TypeError, ValueError):
        return 5


def herdr_bin(env: Any | None = None, *, default: str = DEFAULT_HERDR_BIN) -> str:
    source = _env_source(env)
    real_bin = _env_get_str(source, "HERDR_REAL_BIN").strip()
    if real_bin:
        return real_bin
    configured = _env_get_str(source, "HERDR_BIN").strip()
    if configured and "\x00" not in configured:
        return configured
    return default


def herdr_timeout_seconds(env: Any | None = None) -> float:
    source = _env_source(env)
    try:
        value = float(_env_get_str(source, "HERDRES_TENDWIRE_HERDR_TIMEOUT_SECONDS", str(HERDR_TIMEOUT_DEFAULT)))
    except (TypeError, ValueError):
        return HERDR_TIMEOUT_DEFAULT
    if not math.isfinite(value) or value <= 0:
        return HERDR_TIMEOUT_DEFAULT
    return value


def _timeout_env_value(value: float) -> str:
    return str(float(value))


def optional_config_value(key: str, env: Any | None = None) -> str | None:
    source = _env_source(env)
    raw = source.get(key)
    if raw is None or str(raw).strip() == "":
        return None
    value = str(raw).strip()
    if key in OPTIONAL_PATH_KEYS:
        return _expand_config_path(value, source)
    return value


def env_overrides(env: Any | None = None, *, default_herdr_bin: str = DEFAULT_HERDR_BIN) -> dict[str, str]:
    source = _env_source(env)
    overrides = {
        "TENDWIRE_HERDR_BIN": herdr_bin(source, default=default_herdr_bin),
        "TENDWIRE_HERDR_TIMEOUT_SECONDS": _timeout_env_value(herdr_timeout_seconds(source)),
    }
    for herdres_key, tendwire_key in OPTIONAL_ENV_KEYS.items():
        value = optional_config_value(herdres_key, source)
        if value is not None:
            overrides[tendwire_key] = value
    return overrides


def child_env(env: Any | None = None, *, default_herdr_bin: str = DEFAULT_HERDR_BIN) -> dict[str, str]:
    source = _env_source(env)
    child = {str(k): str(v) for k, v in dict(source).items()}
    for tendwire_key in OPTIONAL_ENV_KEYS.values():
        child.pop(tendwire_key, None)
    child.update(env_overrides(source, default_herdr_bin=default_herdr_bin))
    return child


def config_status(env: Any | None = None, *, default_herdr_bin: str = DEFAULT_HERDR_BIN) -> dict[str, Any]:
    source = _env_source(env)
    outer_timeout = timeout_seconds(source)
    herdr_timeout = herdr_timeout_seconds(source)
    warnings: list[str] = []
    if herdr_timeout >= float(outer_timeout):
        warnings.append("TENDWIRE_HERDR_TIMEOUT_SECONDS is greater than or equal to the outer Tendwire timeout")
    return {
        "tendwire_mode": parse_mode(source),
        "tendwire_bin": shlex.join(command_base(source)),
        "tendwire_db_path": optional_config_value("HERDRES_TENDWIRE_DB_PATH", source),
        "tendwire_data_dir": optional_config_value("HERDRES_TENDWIRE_DATA_DIR", source),
        "tendwire_host_id": optional_config_value("HERDRES_TENDWIRE_HOST_ID", source),
        "tendwire_herdr_bin": herdr_bin(source, default=default_herdr_bin),
        "tendwire_timeout_seconds": outer_timeout,
        "tendwire_herdr_timeout_seconds": herdr_timeout,
        "tendwire_connector_outbox": connector_outbox_enabled(source),
        "tendwire_connector_name": connector_name(source),
        "tendwire_connector_limit": connector_limit(source),
        "tendwire_connector_lease_seconds": connector_lease_seconds(source),
        "tendwire_connector_failure_delay_seconds": connector_failure_delay_seconds(source),
        "warnings": warnings,
    }


def json_object_from_stdout(stdout: str, source: str) -> tuple[dict[str, Any] | None, str]:
    text = str(stdout or "")
    stripped = text.strip()
    if not stripped:
        return None, f"{source} returned empty stdout"
    try:
        decoder = json.JSONDecoder()
        value, idx = decoder.raw_decode(stripped)
    except json.JSONDecodeError:
        if stripped[:1] in {"{", "["}:
            return None, f"{source} returned malformed JSON"
        return None, f"{source} returned non-JSON stdout"
    if stripped[idx:].strip():
        return None, f"{source} returned more than one JSON value"
    if not isinstance(value, dict):
        return None, f"{source} returned non-object JSON"
    return value, ""


def _json_error_status(error: str) -> str:
    if "malformed JSON" in error:
        return "malformed_json"
    if "non-object JSON" in error:
        return "non_object_json"
    return "non_json_stdout"


def command_submit(
    request: dict[str, Any],
    *,
    runner: Runner,
    sanitize: Sanitizer = _default_sanitize,
    env: Any | None = None,
    default_herdr_bin: str = DEFAULT_HERDR_BIN,
) -> dict[str, Any]:
    try:
        input_text = json.dumps(request, separators=(",", ":")) + "\n"
        proc = runner(
            [*command_base(env), "command", "--json"],
            timeout=timeout_seconds(env),
            input_text=input_text,
            env=child_env(env, default_herdr_bin=default_herdr_bin),
        )
    except subprocess.TimeoutExpired:
        return {"ok": False, "status": "timeout", "error": "tendwire command timed out"}
    except Exception as exc:
        return {"ok": False, "status": "subprocess_failed", "error": sanitize(str(exc), 300)}
    if proc.returncode != 0:
        detail = sanitize(proc.stderr or proc.stdout or "tendwire command failed", 500)
        return {"ok": False, "status": "nonzero_exit", "error": detail}
    data, error = json_object_from_stdout(proc.stdout, "tendwire command")
    if error:
        return {"ok": False, "status": _json_error_status(error), "error": error}
    return data


def connector_call(
    action: str,
    params: dict[str, Any] | None = None,
    *,
    runner: Runner,
    sanitize: Sanitizer = _default_sanitize,
    env: Any | None = None,
    default_herdr_bin: str = DEFAULT_HERDR_BIN,
) -> dict[str, Any]:
    data = dict(params or {})
    clean_action = str(action or "").strip().lower()
    name = str(data.get("name") or connector_name(env)).strip() or "attention"
    args = [*command_base(env), "connector", clean_action, "--name", name]
    if clean_action == "poll":
        try:
            poll_limit = max(1, int(data.get("limit") or connector_limit(env)))
        except (TypeError, ValueError):
            poll_limit = connector_limit(env)
        args.extend(["--limit", str(poll_limit)])
        lease_seconds = data.get("lease_seconds")
        if lease_seconds is not None:
            try:
                lease_value = max(1, int(lease_seconds))
            except (TypeError, ValueError):
                lease_value = connector_lease_seconds(env)
            args.extend(["--lease-seconds", str(lease_value)])
    elif clean_action in {"ack", "fail", "defer"}:
        ref = str(data.get("ref") or "").strip()
        if not ref:
            return {"ok": False, "status": "invalid_ref", "error": "missing connector ref"}
        args.extend(["--ref", ref])
        response = data.get("response")
        if isinstance(response, dict) and response:
            args.extend(["--response-json", json.dumps(response, sort_keys=True, separators=(",", ":"))])
        if clean_action in {"fail", "defer"}:
            reason = sanitize(str(data.get("reason") or ""), 120).strip()
            if reason:
                args.extend(["--reason", reason])
            if data.get("available_at"):
                args.extend(["--available-at", str(data.get("available_at"))])
            if data.get("delay_seconds") is not None:
                try:
                    delay_value = max(0, int(data.get("delay_seconds") or 0))
                except (TypeError, ValueError):
                    delay_value = connector_failure_delay_seconds(env)
                args.extend(["--delay-seconds", str(delay_value)])
    elif clean_action != "reclaim":
        return {"ok": False, "status": "unknown_method", "error": "unknown connector action"}
    try:
        proc = runner(
            args,
            timeout=timeout_seconds(env),
            env=child_env(env, default_herdr_bin=default_herdr_bin),
        )
    except subprocess.TimeoutExpired:
        return {"ok": False, "status": "timeout", "error": "tendwire connector call timed out"}
    except Exception as exc:
        return {"ok": False, "status": "subprocess_failed", "error": sanitize(str(exc), 300)}
    if proc.returncode != 0:
        return {
            "ok": False,
            "status": "nonzero_exit",
            "error": sanitize(proc.stderr or proc.stdout or "tendwire connector call failed", 500),
        }
    parsed, error = json_object_from_stdout(proc.stdout, f"tendwire connector {clean_action}")
    if error:
        return {"ok": False, "status": _json_error_status(error), "error": error}
    return parsed


def _json_command_payload(
    command: str,
    source: str,
    failure_message: str,
    *,
    runner: Runner,
    sanitize: Sanitizer = _default_sanitize,
    env: Any | None = None,
    default_herdr_bin: str = DEFAULT_HERDR_BIN,
) -> dict[str, Any]:
    proc = runner(
        [*command_base(env), command, "--json"],
        timeout=timeout_seconds(env),
        env=child_env(env, default_herdr_bin=default_herdr_bin),
    )
    if proc.returncode != 0:
        raise TendwireCallError(sanitize(proc.stderr or proc.stdout or failure_message, 500))
    try:
        data = json.loads(proc.stdout)
    except json.JSONDecodeError as exc:
        raise TendwireCallError(f"{source} returned non-JSON") from exc
    if not isinstance(data, dict):
        raise TendwireCallError(f"{source} returned non-object JSON")
    return data


def snapshot_payload(
    *,
    runner: Runner,
    sanitize: Sanitizer = _default_sanitize,
    env: Any | None = None,
    default_herdr_bin: str = DEFAULT_HERDR_BIN,
) -> dict[str, Any]:
    return _json_command_payload(
        "snapshot",
        "tendwire snapshot",
        "tendwire snapshot failed",
        runner=runner,
        sanitize=sanitize,
        env=env,
        default_herdr_bin=default_herdr_bin,
    )


def turns_payload(
    *,
    runner: Runner,
    sanitize: Sanitizer = _default_sanitize,
    env: Any | None = None,
    default_herdr_bin: str = DEFAULT_HERDR_BIN,
) -> dict[str, Any]:
    return _json_command_payload(
        "turns",
        "tendwire turns",
        "tendwire turns failed",
        runner=runner,
        sanitize=sanitize,
        env=env,
        default_herdr_bin=default_herdr_bin,
    )


_TURNS_PAYLOAD_CACHE: dict[str, Any] | None = None
_TURNS_PAYLOAD_CACHE_LOCK = threading.Lock()


def clear_turns_payload_cache() -> None:
    global _TURNS_PAYLOAD_CACHE
    with _TURNS_PAYLOAD_CACHE_LOCK:
        _TURNS_PAYLOAD_CACHE = None


def cached_turns_payload(
    *,
    runner: Runner,
    sanitize: Sanitizer = _default_sanitize,
    env: Any | None = None,
    default_herdr_bin: str = DEFAULT_HERDR_BIN,
) -> dict[str, Any]:
    global _TURNS_PAYLOAD_CACHE
    with _TURNS_PAYLOAD_CACHE_LOCK:
        if _TURNS_PAYLOAD_CACHE is not None:
            return _TURNS_PAYLOAD_CACHE
    data = turns_payload(
        runner=runner,
        sanitize=sanitize,
        env=env,
        default_herdr_bin=default_herdr_bin,
    )
    with _TURNS_PAYLOAD_CACHE_LOCK:
        _TURNS_PAYLOAD_CACHE = data
    return data


COMMAND_SUCCESS_STATUSES = {
    "accepted",
    "duplicate_instruction",
    "queued",
    "sent",
    "submitted",
    "ok",
    "success",
}
COMMAND_FAILURE_STATUSES = {
    "stale_target",
    "ambiguous_target",
    "ambiguous_backend_target",
    "backend_unavailable",
    "request_state_uncertain",
    "backend_unsupported",
    "backend_failed",
    "malformed_response",
    "non_json_stdout",
    "malformed_json",
    "non_object_json",
    "nonzero_exit",
    "timeout",
    "subprocess_failed",
}
ENTRY_METADATA_KEYS = (
    "tendwire_worker_id",
    "tendwire_fingerprint",
    "tendwire_status_line",
    "tendwire_last_seen_at",
)
COMMAND_SUBMISSION_LEDGER_LIMIT = 500


def is_source_entry(entry: dict[str, Any] | None) -> bool:
    if not isinstance(entry, dict):
        return False
    if str(entry.get("source") or "") != "tendwire":
        return False
    if str(entry.get("entry_type") or "") == "worker":
        return True
    if str(entry.get("pane_id") or "").startswith("tendwire:"):
        return True
    return bool(str(entry.get("tendwire_worker_id") or "").strip())


def source_entry_commands_allowed(
    entry: dict[str, Any] | None,
    *,
    source_read_enabled: bool,
    commands_enabled: bool,
) -> bool:
    return is_source_entry(entry) and bool(source_read_enabled) and bool(commands_enabled)


def entry_metadata_state(
    entry: dict[str, Any] | None,
    *,
    source_read_enabled: bool,
    commands_enabled: bool,
) -> str:
    if not isinstance(entry, dict):
        return "none"
    if is_source_entry(entry) and not source_entry_commands_allowed(
        entry,
        source_read_enabled=source_read_enabled,
        commands_enabled=commands_enabled,
    ):
        return "none"
    has_metadata = any(key in entry for key in ENTRY_METADATA_KEYS)
    if not has_metadata:
        return "none"
    worker_id = str(entry.get("tendwire_worker_id") or "").strip()
    fingerprint = str(entry.get("tendwire_fingerprint") or "").strip()
    return "valid" if worker_id and fingerprint else "partial"


def response_status(response: dict[str, Any]) -> str:
    for container in (response, response.get("result") if isinstance(response.get("result"), dict) else {}):
        for key in ("status", "state", "result_status"):
            value = str(container.get(key) or "").strip().lower().replace("-", "_")
            if value:
                return value
    return ""


def duplicate_payload_matches(response: dict[str, Any]) -> bool:
    containers = [response]
    if isinstance(response.get("result"), dict):
        containers.append(response["result"])
    for container in containers:
        for mismatch_key in ("payload_mismatch", "mismatched_payload", "request_mismatch"):
            if bool(container.get(mismatch_key)):
                return False
        for match_key in ("payload_matches", "payload_match", "same_payload", "request_matches"):
            if container.get(match_key) is True:
                return True
    return False


def command_succeeded(response: dict[str, Any]) -> bool:
    status = response_status(response)
    if status == "duplicate_request":
        return duplicate_payload_matches(response)
    if status in COMMAND_FAILURE_STATUSES:
        return False
    if status in COMMAND_SUCCESS_STATUSES:
        return True
    if response.get("ok") is True and not status:
        return True
    return False


def success_reply(
    response: dict[str, Any],
    *,
    sanitize: Sanitizer = _default_sanitize,
    limit: int = 300,
) -> str:
    status = response_status(response)
    result = response.get("result") if isinstance(response.get("result"), dict) else {}
    if str(result.get("delivery_state") or "").strip().lower() == "queued":
        return "Queued for Tendwire worker."
    if str(result.get("delivery_state") or "").strip().lower() == "duplicate_suppressed":
        return ""
    if status == "queued":
        return "Queued for Tendwire worker."
    for container in (response, result):
        message = str(container.get("reply") or container.get("message") or "").strip()
        if message:
            return sanitize(message, limit)
    return ""


def send_text_policy(
    *,
    source_inventory_enabled: bool,
    source_entry: bool,
    source_entry_commands_allowed: bool,
    commands_enabled: bool,
    metadata_state: str,
    direct_fallback_enabled: bool,
) -> str:
    """Classify a pane text send without touching Herdr, Tendwire, or Telegram state."""
    normalized_metadata = str(metadata_state or "none").strip().lower()
    if source_inventory_enabled and not source_entry:
        return "legacy_source_block"
    if source_entry and not source_entry_commands_allowed:
        return "source_commands_disabled"
    if commands_enabled:
        if normalized_metadata == "valid":
            return "tendwire"
        if normalized_metadata == "partial":
            if direct_fallback_enabled and not source_entry:
                return "direct"
            return "safe_failure"
        if source_inventory_enabled:
            return "safe_failure"
    if source_entry:
        return "safe_failure"
    return "direct"


def entry_send_text_policy(
    entry: dict[str, Any] | None,
    env: Any | None = None,
    *,
    diagnose_invalid: bool = False,
    warn_invalid: Callable[[Any], None] | None = None,
) -> str:
    """Classify a text send for one stored pane/source entry from Tendwire mode state."""
    mode = parse_mode(env, diagnose_invalid=diagnose_invalid, warn_invalid=warn_invalid)
    source_inventory = mode_enables_source_inventory(mode)
    commands = mode_enables_commands(mode)
    source_entry = is_source_entry(entry)
    commands_allowed = source_entry_commands_allowed(
        entry,
        source_read_enabled=source_inventory,
        commands_enabled=commands,
    )
    metadata = entry_metadata_state(
        entry,
        source_read_enabled=source_inventory,
        commands_enabled=commands,
    )
    source = os.environ if env is None else env
    return send_text_policy(
        source_inventory_enabled=source_inventory,
        source_entry=source_entry,
        source_entry_commands_allowed=commands_allowed,
        commands_enabled=commands,
        metadata_state=metadata,
        direct_fallback_enabled=bool_env(source, "HERDRES_TENDWIRE_DIRECT_FALLBACK"),
    )


def callback_choice_preflight_policy(
    *,
    source_inventory_enabled: bool,
    source_entry: bool,
    pane_id: str,
    last_known_status: str,
    metadata_state: str,
) -> str:
    """Classify callback choice safety before any Telegram/state side effects."""
    status = str(last_known_status or "").strip().lower()
    if source_inventory_enabled and not source_entry:
        return "legacy_source_block"
    if not source_entry and (not str(pane_id or "").strip() or status == "closed"):
        return "pane_not_live"
    if source_entry and status == "closed":
        return "source_not_live"
    if source_entry and str(metadata_state or "").strip().lower() != "valid":
        return "safe_failure"
    return "ok"


def same_worker_stale_target_candidate(response: dict[str, Any], worker_id: str) -> dict[str, str] | None:
    if response_status(response) != "stale_target":
        return None
    result = response.get("result") if isinstance(response.get("result"), dict) else {}
    candidates = result.get("candidates") if isinstance(result, dict) else None
    if not isinstance(candidates, list):
        return None
    matches: list[dict[str, str]] = []
    for item in candidates:
        if not isinstance(item, dict):
            continue
        candidate_worker_id = str(item.get("worker_id") or "").strip()
        candidate_fingerprint = str(item.get("worker_fingerprint") or "").strip()
        if candidate_worker_id == worker_id and candidate_fingerprint:
            matches.append(
                {
                    "worker_id": candidate_worker_id,
                    "worker_fingerprint": candidate_fingerprint,
                }
            )
    if len(matches) != 1:
        return None
    return matches[0]


def retry_request_id(base_request_id: str, candidate: dict[str, str]) -> str:
    base = str(base_request_id or "herdres:retry").strip() or "herdres:retry"
    fingerprint = str(candidate.get("worker_fingerprint") or "")
    digest = hashlib.sha256(f"{base}:{fingerprint}".encode("utf-8")).hexdigest()[:10]
    return f"{base}:retry:{digest}"


def legacy_direct_archive_record(
    pane_key_value: str,
    entry: dict[str, Any],
    *,
    now: str,
    is_source_entry: SourceEntryPredicate,
) -> dict[str, Any] | None:
    """Build a public-safe audit record for a legacy direct pane archived in source mode."""
    if is_source_entry(entry):
        return None
    return {
        "pane_key_hash": hashlib.sha256(str(pane_key_value).encode("utf-8")).hexdigest()[:16],
        "source": str(entry.get("source") or "herdr"),
        "entry_type": str(entry.get("entry_type") or ""),
        "status": str(entry.get("last_known_status") or ""),
        "space_key": str(entry.get("space_key") or ""),
        "had_topic": bool(str(entry.get("topic_id") or "")),
        "had_private_pane": bool(str(entry.get("pane_id") or "")),
        "removed_at": now,
    }


def source_pane_delete_record(pane_key_value: str, entry: dict[str, Any], *, now: str) -> dict[str, Any]:
    return {
        "pane_key": str(pane_key_value),
        "pane_id": str(entry.get("pane_id") or ""),
        "entry_type": str(entry.get("entry_type") or ""),
        "worker_id": str(entry.get("worker_id") or entry.get("tendwire_worker_id") or ""),
        "space_key": str(entry.get("space_key") or ""),
        "topic_id": str(entry.get("topic_id") or ""),
        "removed_at": now,
    }


def closed_source_prune_record(pane_key_value: str, entry: dict[str, Any], *, now: str) -> dict[str, Any]:
    return {
        "pane_key": str(pane_key_value),
        "entry_type": str(entry.get("entry_type") or ""),
        "worker_id": str(entry.get("worker_id") or entry.get("tendwire_worker_id") or ""),
        "space_key": str(entry.get("space_key") or ""),
        "topic_id": str(entry.get("topic_id") or ""),
        "removed_at": now,
    }


def append_bounded_audit(
    state: dict[str, Any],
    key: str,
    records: list[dict[str, Any]],
    *,
    limit: int,
) -> bool:
    if not isinstance(state, dict) or not records:
        return False
    prior = state.get(key)
    existing = prior if isinstance(prior, list) else []
    state[key] = (existing + records)[-max(1, int(limit)):]
    return True


def request_component(
    value: Any,
    limit: int = 48,
    *,
    sanitize: Sanitizer = _default_sanitize,
) -> str:
    text = sanitize(str(value or ""), limit).strip()
    text = re.sub(r"[^A-Za-z0-9_.:-]+", "-", text).strip(".")
    return text[:limit] or "none"


def instruction_text_hash(text: str) -> str:
    return hashlib.sha256(str(text or "").encode("utf-8")).hexdigest()[:16]


def instruction_request_id(
    *,
    chat_id: str = "",
    topic_id: str = "",
    message_id: str = "",
    reply_to_message_id: str = "",
    callback_message_id: str = "",
    worker_id: str = "",
    origin: str = "send",
    text: str = "",
    sanitize: Sanitizer = _default_sanitize,
) -> str:
    text_hash = instruction_text_hash(text)
    context = {
        "source": "telegram",
        "origin": str(origin or ""),
        "chat_id": str(chat_id or ""),
        "topic_id": str(topic_id or ""),
        "message_id": str(message_id or ""),
        "reply_to_message_id": str(reply_to_message_id or ""),
        "callback_message_id": str(callback_message_id or ""),
        "worker_id": str(worker_id or ""),
        "text_hash": text_hash,
    }
    digest = hashlib.sha256(json.dumps(context, sort_keys=True, separators=(",", ":")).encode("utf-8")).hexdigest()[:16]
    return ":".join(
        (
            "herdres",
            request_component(worker_id, 48, sanitize=sanitize),
            text_hash[:12],
            digest,
        )
    )


def instruction_submission_identity(
    *,
    chat_id: str = "",
    topic_id: str = "",
    message_id: str = "",
    reply_to_message_id: str = "",
    callback_message_id: str = "",
    worker_id: str = "",
    origin: str = "send",
    text: str = "",
) -> str:
    context = {
        "source": "telegram",
        "origin": str(origin or ""),
        "chat_id": str(chat_id or ""),
        "topic_id": str(topic_id or ""),
        "message_id": str(message_id or ""),
        "reply_to_message_id": str(reply_to_message_id or ""),
        "callback_message_id": str(callback_message_id or ""),
        "worker_id": str(worker_id or ""),
        "text_hash": instruction_text_hash(text),
    }
    return hashlib.sha256(json.dumps(context, sort_keys=True, separators=(",", ":")).encode("utf-8")).hexdigest()[:24]


def command_submission_identity_for_entry(
    entry: dict[str, Any],
    text: str,
    *,
    origin: str = "send",
    chat_id: str = "",
    topic_id: str = "",
    message_id: str = "",
    reply_to_message_id: str = "",
    callback_message_id: str = "",
) -> str:
    if not (str(message_id or "").strip() or str(callback_message_id or "").strip()):
        return ""
    worker_id = str(entry.get("tendwire_worker_id") or "").strip()
    if not worker_id:
        return ""
    return instruction_submission_identity(
        chat_id=chat_id,
        topic_id=topic_id,
        message_id=message_id,
        reply_to_message_id=reply_to_message_id,
        callback_message_id=callback_message_id,
        worker_id=worker_id,
        origin=origin,
        text=text,
    )


def command_submission_ledger(state: dict[str, Any]) -> dict[str, Any]:
    ledger = state.get("tendwire_command_submissions")
    if not isinstance(ledger, dict):
        ledger = {}
        state["tendwire_command_submissions"] = ledger
    return ledger


def command_submission_seen(state: dict[str, Any] | None, identity: str) -> bool:
    if not isinstance(state, dict) or not identity:
        return False
    ledger = state.get("tendwire_command_submissions")
    return isinstance(ledger, dict) and identity in ledger


def note_command_submission(
    state: dict[str, Any] | None,
    identity: str,
    *,
    request_id: str = "",
    worker_id: str = "",
    origin: str = "",
    text: str = "",
    status: str = "",
    now: str,
    sanitize: Sanitizer = _default_sanitize,
    limit: int = COMMAND_SUBMISSION_LEDGER_LIMIT,
) -> bool:
    if not isinstance(state, dict) or not identity:
        return False
    ledger = command_submission_ledger(state)
    record = ledger.get(identity)
    if not isinstance(record, dict):
        record = {"submitted_at": str(now or "")}
        ledger[identity] = record
        changed = True
    else:
        changed = False
    values = {
        "request_id": sanitize(str(request_id or ""), 200).strip(),
        "worker_id": sanitize(str(worker_id or ""), 120).strip(),
        "origin": sanitize(str(origin or ""), 40).strip(),
        "text_hash": instruction_text_hash(text),
        "status": sanitize(str(status or "attempted"), 80).strip() or "attempted",
        "updated_at": str(now or ""),
    }
    for key, value in values.items():
        if record.get(key) != value:
            record[key] = value
            changed = True
    max_records = max(1, int(limit))
    while len(ledger) > max_records:
        oldest_key = min(
            ledger,
            key=lambda item: str((ledger.get(item) if isinstance(ledger.get(item), dict) else {}).get("updated_at") or ""),
        )
        ledger.pop(oldest_key, None)
        changed = True
    return changed


def build_send_instruction_request(
    entry: dict[str, Any],
    text: str,
    *,
    origin: str = "send",
    chat_id: str = "",
    topic_id: str = "",
    message_id: str = "",
    reply_to_message_id: str = "",
    callback_message_id: str = "",
    request_id: str = "",
    sanitize: Sanitizer = _default_sanitize,
) -> dict[str, Any]:
    worker_id = str(entry.get("tendwire_worker_id") or "").strip()
    fingerprint = str(entry.get("tendwire_fingerprint") or "").strip()
    final_request_id = request_id or instruction_request_id(
        chat_id=chat_id,
        topic_id=topic_id,
        message_id=message_id,
        reply_to_message_id=reply_to_message_id,
        callback_message_id=callback_message_id,
        worker_id=worker_id,
        origin=origin,
        text=text,
        sanitize=sanitize,
    )
    params: dict[str, Any] = {
        "origin": "telegram",
        "telegram_origin": sanitize(str(origin or "send"), 40),
    }
    for key, value in (
        ("entry_type", entry.get("entry_type")),
        ("pane_key", entry.get("pane_key")),
    ):
        clean = sanitize(str(value or ""), 300).strip()
        if clean:
            params[key] = clean
    return {
        "schema_version": 1,
        "action": "send_instruction",
        "request_id": final_request_id,
        "dry_run": False,
        "target": {
            "worker_id": worker_id,
            "worker_fingerprint": fingerprint,
        },
        "instruction": {"text": str(text or "")},
        "params": params,
    }


def retry_send_instruction_request(
    entry: dict[str, Any],
    text: str,
    response: dict[str, Any],
    *,
    origin: str = "send",
    chat_id: str = "",
    topic_id: str = "",
    message_id: str = "",
    reply_to_message_id: str = "",
    callback_message_id: str = "",
    base_request_id: str = "",
    sanitize: Sanitizer = _default_sanitize,
) -> tuple[dict[str, Any], dict[str, Any]] | None:
    worker_id = str(entry.get("tendwire_worker_id") or "").strip()
    candidate = same_worker_stale_target_candidate(response, worker_id)
    if candidate is None:
        return None
    retry_entry = dict(entry)
    retry_entry["tendwire_fingerprint"] = candidate["worker_fingerprint"]
    retry_request = build_send_instruction_request(
        retry_entry,
        text,
        origin=origin,
        chat_id=chat_id,
        topic_id=topic_id,
        message_id=message_id,
        reply_to_message_id=reply_to_message_id,
        callback_message_id=callback_message_id,
        request_id=retry_request_id(base_request_id, candidate),
        sanitize=sanitize,
    )
    return retry_entry, retry_request


def submit_send_instruction_attempt(
    entry: dict[str, Any],
    text: str,
    *,
    state: dict[str, Any] | None = None,
    command_call: Callable[[dict[str, Any]], dict[str, Any]],
    now: Callable[[], str],
    origin: str = "send",
    chat_id: str = "",
    topic_id: str = "",
    message_id: str = "",
    reply_to_message_id: str = "",
    callback_message_id: str = "",
    request_id: str = "",
    sanitize: Sanitizer = _default_sanitize,
) -> dict[str, Any]:
    """Submit one Tendwire send_instruction command and handle public-safe retry/ledger state."""
    outbound = str(text or "")
    worker_id = str(entry.get("tendwire_worker_id") or "").strip()
    submission_identity = command_submission_identity_for_entry(
        entry,
        outbound,
        origin=origin,
        chat_id=chat_id,
        topic_id=topic_id,
        message_id=message_id,
        reply_to_message_id=reply_to_message_id,
        callback_message_id=callback_message_id,
    )
    if command_submission_seen(state, submission_identity):
        return {
            "duplicate": True,
            "ledger_changed": False,
            "response": {},
            "request_id": "",
            "submission_identity": submission_identity,
            "outbound": outbound,
        }
    request = build_send_instruction_request(
        entry,
        outbound,
        origin=origin,
        chat_id=chat_id,
        topic_id=topic_id,
        message_id=message_id,
        reply_to_message_id=reply_to_message_id,
        callback_message_id=callback_message_id,
        request_id=request_id,
        sanitize=sanitize,
    )
    current_request_id = str(request.get("request_id") or request_id)
    ledger_changed = note_command_submission(
        state,
        submission_identity,
        request_id=current_request_id,
        worker_id=worker_id,
        origin=origin,
        text=outbound,
        status="attempted",
        now=now(),
        sanitize=sanitize,
    )
    response = command_call(request)
    if response_status(response) == "stale_target":
        retry = retry_send_instruction_request(
            entry,
            outbound,
            response,
            origin=origin,
            chat_id=chat_id,
            topic_id=topic_id,
            message_id=message_id,
            reply_to_message_id=reply_to_message_id,
            callback_message_id=callback_message_id,
            base_request_id=current_request_id,
            sanitize=sanitize,
        )
        if retry is not None:
            retry_entry, retry_request = retry
            response = command_call(retry_request)
            current_request_id = str(retry_request.get("request_id") or current_request_id)
            ledger_changed = note_command_submission(
                state,
                submission_identity,
                request_id=current_request_id,
                worker_id=worker_id,
                origin=origin,
                text=outbound,
                status=response_status(response) or "attempted",
                now=now(),
                sanitize=sanitize,
            ) or ledger_changed
            if command_succeeded(response):
                entry["tendwire_fingerprint"] = str(retry_entry.get("tendwire_fingerprint") or "")
    else:
        ledger_changed = note_command_submission(
            state,
            submission_identity,
            request_id=current_request_id,
            worker_id=worker_id,
            origin=origin,
            text=outbound,
            status=response_status(response) or "attempted",
            now=now(),
            sanitize=sanitize,
        ) or ledger_changed
    return {
        "duplicate": False,
        "ledger_changed": ledger_changed,
        "response": response,
        "request_id": current_request_id,
        "submission_identity": submission_identity,
        "outbound": outbound,
    }


def send_instruction_attempt_result(
    attempt: dict[str, Any] | None,
    *,
    safe_failure_reply: str,
    sanitize: Sanitizer = _default_sanitize,
) -> dict[str, Any]:
    """Translate a submitted command attempt into public send response state."""
    data = attempt if isinstance(attempt, dict) else {}
    response = data.get("response") if isinstance(data.get("response"), dict) else {}
    if data.get("duplicate"):
        return {
            "duplicate": True,
            "succeeded": True,
            "ledger_changed": False,
            "reply": "",
            "response": response,
            "status": "duplicate",
        }
    ledger_changed = bool(data.get("ledger_changed"))
    if command_succeeded(response):
        return {
            "duplicate": False,
            "succeeded": True,
            "ledger_changed": ledger_changed,
            "reply": success_reply(response, sanitize=sanitize),
            "response": response,
            "status": response_status(response) or "accepted",
        }
    return {
        "duplicate": False,
        "succeeded": False,
        "ledger_changed": ledger_changed,
        "reply": str(safe_failure_reply or ""),
        "response": response,
        "status": response_status(response) or "failed",
    }


def worker_status_for_herdres(status: str) -> str:
    value = str(status or "unknown").strip().lower().replace("-", "_")
    if value in {"active", "running", "working", "busy"}:
        return "working"
    if value in {"failed", "failure", "error"}:
        return "error"
    if value in {"done", "complete", "completed", "success", "succeeded"}:
        return "done"
    if value in {"closed", "exited", "terminated", "stopped"}:
        return "closed"
    if value in {"blocked", "waiting", "idle", "warning", "unknown"}:
        return value
    return "unknown"


def normalized_label(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "").strip().lower())


def normalized_path(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    return os.path.normpath(os.path.expanduser(text))


def worker_agent(worker: dict[str, Any], *, sanitize: Sanitizer = _default_sanitize) -> str:
    meta = worker.get("meta") if isinstance(worker.get("meta"), dict) else {}
    return sanitize(str(meta.get("agent") or worker.get("name") or ""), 80)


def looks_like_raw_herdr_space_id(value: Any) -> bool:
    return bool(re.fullmatch(r"w[0-9a-f]{8,}", str(value or "").strip(), flags=re.IGNORECASE))


def worker_match_keys(worker: dict[str, Any], *, sanitize: Sanitizer = _default_sanitize) -> list[tuple[str, str, str, str]]:
    meta = worker.get("meta") if isinstance(worker.get("meta"), dict) else {}
    space_id = str(worker.get("space_id") or "").strip()
    tab_id = str(meta.get("tab_id") or "").strip()
    agent = normalized_label(worker_agent(worker, sanitize=sanitize))
    cwd = normalized_path(meta.get("foreground_cwd") or meta.get("cwd") or "")
    keys: list[tuple[str, str, str, str]] = []
    if space_id and tab_id and agent:
        keys.append(("space_tab_agent", space_id, tab_id, agent))
    if space_id and tab_id:
        keys.append(("space_tab", space_id, tab_id, ""))
    if space_id and cwd and agent:
        keys.append(("space_cwd_agent", space_id, cwd, agent))
    return keys


def pane_match_keys(pane: dict[str, Any]) -> list[tuple[str, str, str, str]]:
    workspace_id = str(pane.get("workspace_id") or pane.get("space_id") or "").strip()
    tab_id = str(pane.get("tab_id") or "").strip()
    agent = normalized_label(pane.get("agent") or pane.get("label") or pane.get("name") or "")
    cwd = normalized_path(pane.get("foreground_cwd") or pane.get("cwd") or "")
    keys: list[tuple[str, str, str, str]] = []
    if workspace_id and tab_id and agent:
        keys.append(("space_tab_agent", workspace_id, tab_id, agent))
    if workspace_id and tab_id:
        keys.append(("space_tab", workspace_id, tab_id, ""))
    if workspace_id and cwd and agent:
        keys.append(("space_cwd_agent", workspace_id, cwd, agent))
    return keys


def worker_index(
    snapshot: dict[str, Any],
    *,
    sanitize: Sanitizer = _default_sanitize,
) -> dict[tuple[str, str, str, str], list[dict[str, Any]]]:
    index: dict[tuple[str, str, str, str], list[dict[str, Any]]] = {}
    for worker in snapshot.get("workers") or []:
        if not isinstance(worker, dict):
            continue
        worker_id = str(worker.get("id") or "").strip()
        if not worker_id:
            continue
        for key in worker_match_keys(worker, sanitize=sanitize):
            index.setdefault(key, []).append(worker)
    return index


def match_worker(
    pane: dict[str, Any],
    index: dict[tuple[str, str, str, str], list[dict[str, Any]]],
) -> dict[str, Any] | None:
    for key in pane_match_keys(pane):
        matches = index.get(key) or []
        if len(matches) == 1:
            return matches[0]
        if len(matches) > 1:
            return None
    return None


def enrich_pane(
    pane: dict[str, Any],
    worker: dict[str, Any],
    *,
    sanitize: Sanitizer = _default_sanitize,
) -> dict[str, Any]:
    item = dict(pane)
    worker_id = str(worker.get("id") or "").strip()
    item["_tendwire_enriched"] = True
    item["_tendwire_worker_id"] = worker_id
    item["_tendwire_fingerprint"] = str(worker.get("fingerprint") or "")
    status_line = sanitize(str(worker.get("status_line") or worker.get("summary") or ""), 500)
    if status_line:
        item["summary"] = status_line
        item["_tendwire_status_line"] = status_line
    status = worker_status_for_herdres(str(worker.get("status") or "unknown"))
    if status and status != "unknown" and str(item.get("agent_status") or "").strip().lower() in {"", "unknown"}:
        item["agent_status"] = status
    last_seen_at = str(worker.get("last_seen_at") or "").strip()
    if last_seen_at:
        item["_tendwire_last_seen_at"] = last_seen_at
    return item


def enrich_panes(
    panes: list[dict[str, Any]],
    snapshot: dict[str, Any],
    *,
    sanitize: Sanitizer = _default_sanitize,
) -> list[dict[str, Any]]:
    index = worker_index(snapshot, sanitize=sanitize)
    enriched: list[dict[str, Any]] = []
    for pane in panes:
        worker = match_worker(pane, index)
        enriched.append(enrich_pane(pane, worker, sanitize=sanitize) if worker else pane)
    return enriched


def source_read_panes(
    snapshot: dict[str, Any],
    *,
    sanitize: Sanitizer = _default_sanitize,
    raw_space_id_predicate: RawSpacePredicate | None = None,
) -> list[dict[str, Any]]:
    """Build read-only pane-like records from Tendwire's public snapshot."""
    raw_id_predicate = raw_space_id_predicate or looks_like_raw_herdr_space_id
    spaces = {
        str(space.get("id") or ""): space
        for space in snapshot.get("spaces") or []
        if isinstance(space, dict) and str(space.get("id") or "")
    }
    panes: list[dict[str, Any]] = []
    for worker in snapshot.get("workers") or []:
        if not isinstance(worker, dict):
            continue
        worker_id = str(worker.get("id") or "").strip()
        if not worker_id:
            continue
        meta = worker.get("meta") if isinstance(worker.get("meta"), dict) else {}
        space_id = str(worker.get("space_id") or "").strip()
        space = spaces.get(space_id) or {}
        space_name = sanitize(str(space.get("name") or space.get("label") or ""), 120).strip()
        if raw_id_predicate(space_name):
            space_name = ""
        agent = worker_agent(worker, sanitize=sanitize) or sanitize(str(worker.get("name") or "worker"), 80)
        status_line = sanitize(str(worker.get("status_line") or worker.get("summary") or ""), 500)
        fingerprint = str(worker.get("fingerprint") or "")
        worker_status = worker_status_for_herdres(str(worker.get("status") or "unknown"))
        if worker_status == "closed":
            continue
        panes.append(
            {
                "entry_type": "worker",
                "worker_id": worker_id,
                "worker_fingerprint": fingerprint,
                "pane_id": "",
                "terminal_id": "",
                "workspace_id": space_id,
                "space_id": "",
                "tab_id": sanitize(str(meta.get("tab_id") or ""), 120).strip(),
                "agent": agent,
                "agent_status": worker_status,
                "label": sanitize(str(worker.get("name") or agent or "Tendwire Worker"), 120),
                "name": sanitize(str(worker.get("name") or agent or "Tendwire Worker"), 120),
                "foreground_cwd": str(meta.get("foreground_cwd") or meta.get("cwd") or ""),
                "space_name": space_name,
                "workspace_label": space_name,
                "summary": status_line,
                "source": "tendwire",
                "_tendwire_source_read": True,
                "_tendwire_enriched": True,
                "_tendwire_worker_id": worker_id,
                "_tendwire_fingerprint": fingerprint,
                "_tendwire_status_line": status_line,
                "_tendwire_last_seen_at": str(worker.get("last_seen_at") or ""),
            }
        )
    return panes


def snapshot_backend_degraded(snapshot: dict[str, Any]) -> bool:
    """True when Tendwire says the public snapshot is not freshly authoritative."""
    health_items = snapshot.get("backend_health") if isinstance(snapshot.get("backend_health"), list) else []
    for item in health_items:
        if not isinstance(item, dict):
            continue
        status = str(item.get("status") or "").strip().lower()
        if status and status != "healthy":
            return True
    status = str(snapshot.get("status") or "").strip().lower()
    return status in {"degraded", "unavailable"}


def source_entry_as_pane(
    pane_key_value: str,
    entry: dict[str, Any],
    *,
    is_source_entry: SourceEntryPredicate,
) -> dict[str, Any] | None:
    """Reconstruct a source worker pane from Herdres state when Tendwire is degraded."""
    if not is_source_entry(entry):
        return None
    worker_id = str(entry.get("worker_id") or entry.get("tendwire_worker_id") or "").strip()
    pane_id = str(entry.get("pane_id") or "").strip()
    if not worker_id and pane_id.startswith("tendwire:"):
        worker_id = pane_id.split(":", 1)[1].strip()
    if not worker_id:
        return None
    fingerprint = str(entry.get("worker_fingerprint") or entry.get("tendwire_fingerprint") or "").strip()
    space_id = str(entry.get("workspace") or "").strip()
    return {
        "_preserved_pane_key": str(pane_key_value),
        "entry_type": "worker",
        "worker_id": worker_id,
        "worker_fingerprint": fingerprint,
        "pane_id": "",
        "terminal_id": "",
        "workspace_id": space_id,
        "space_id": space_id,
        "tab_id": str(entry.get("tab") or ""),
        "agent": str(entry.get("agent") or "worker"),
        "agent_status": str(entry.get("last_known_status") or "unknown"),
        "label": str(entry.get("pane_thread_name") or entry.get("agent") or "Tendwire Worker"),
        "name": str(entry.get("pane_thread_name") or entry.get("agent") or "Tendwire Worker"),
        "foreground_cwd": str(entry.get("foreground_cwd") or ""),
        "space_name": str(entry.get("topic_name") or ""),
        "workspace_label": str(entry.get("topic_name") or ""),
        "summary": str(entry.get("tendwire_status_line") or ""),
        "source": "tendwire",
        "_tendwire_source_read": True,
        "_tendwire_enriched": True,
        "_tendwire_worker_id": worker_id,
        "_tendwire_fingerprint": fingerprint,
        "_tendwire_status_line": str(entry.get("tendwire_status_line") or ""),
        "_tendwire_last_seen_at": str(entry.get("tendwire_last_seen_at") or ""),
        "_tendwire_preserved_from_state": True,
    }


def source_state_panes(
    state: dict[str, Any],
    *,
    is_source_entry: SourceEntryPredicate,
) -> list[dict[str, Any]]:
    panes = state.get("panes") if isinstance(state.get("panes"), dict) else {}
    preserved: list[dict[str, Any]] = []
    for key, entry in panes.items():
        if not isinstance(entry, dict):
            continue
        if str(entry.get("last_known_status") or "").strip().lower() == "closed":
            continue
        pane = source_entry_as_pane(
            str(key),
            entry,
            is_source_entry=is_source_entry,
        )
        if pane is not None:
            preserved.append(pane)
    return preserved


def merge_preserved_source_panes(
    panes: list[dict[str, Any]],
    preserved_panes: list[dict[str, Any]],
    *,
    pane_key: PaneKey,
) -> tuple[list[dict[str, Any]], int]:
    merged = list(panes)
    live_keys = {pane_key(pane) for pane in merged}
    preserved_count = 0
    for pane in preserved_panes:
        key = pane_key(pane)
        if key in live_keys:
            continue
        merged.append(pane)
        live_keys.add(key)
        preserved_count += 1
    return merged, preserved_count


def source_inventory_panes(
    snapshot: dict[str, Any],
    *,
    preserved_panes: list[dict[str, Any]] | None = None,
    pane_key: PaneKey | None = None,
    sanitize: Sanitizer = _default_sanitize,
    raw_space_id_predicate: RawSpacePredicate | None = None,
) -> dict[str, Any]:
    panes = source_read_panes(
        snapshot,
        sanitize=sanitize,
        raw_space_id_predicate=raw_space_id_predicate,
    )
    degraded = snapshot_backend_degraded(snapshot)
    preserved_count = 0
    if degraded and preserved_panes:
        if pane_key is None:
            raise ValueError("pane_key is required when preserved_panes are supplied")
        panes, preserved_count = merge_preserved_source_panes(
            panes,
            preserved_panes,
            pane_key=pane_key,
        )
    return {
        "panes": panes,
        "degraded": degraded,
        "preserved_count": preserved_count,
    }


def _state_set(state: dict[str, Any], key: str, value: Any) -> bool:
    if state.get(key) == value:
        return False
    state[key] = value
    return True


def _state_pop(state: dict[str, Any], key: str) -> bool:
    if key not in state:
        return False
    state.pop(key, None)
    return True


def note_source_inventory_snapshot_failure(
    state: dict[str, Any] | None,
    error: Any,
    preserved_panes: list[dict[str, Any]],
    *,
    now: str,
    sanitize: Sanitizer = _default_sanitize,
) -> bool:
    """Record that source inventory fell back to preserved panes."""
    if not isinstance(state, dict) or not preserved_panes:
        return False
    changed = _state_set(
        state,
        "tendwire_source_inventory_last_error",
        sanitize(str(error), 500),
    )
    changed = _state_set(state, "tendwire_source_inventory_preserved_at", str(now or "")) or changed
    return changed


def note_source_inventory_result(
    state: dict[str, Any] | None,
    inventory: dict[str, Any],
    *,
    now: str,
) -> bool:
    """Update source inventory health bookkeeping after a snapshot result."""
    if not isinstance(state, dict):
        return False
    changed = _state_pop(state, "tendwire_source_inventory_last_error")
    changed = _state_pop(state, "tendwire_source_inventory_preserved_at") or changed
    if bool(inventory.get("degraded")):
        changed = _state_set(state, "tendwire_source_inventory_degraded_at", str(now or "")) or changed
        changed = _state_set(
            state,
            "tendwire_source_inventory_preserved",
            int(inventory.get("preserved_count") or 0),
        ) or changed
    else:
        changed = _state_pop(state, "tendwire_source_inventory_degraded_at") or changed
        changed = _state_pop(state, "tendwire_source_inventory_preserved") or changed
    return changed


def source_inventory_from_snapshot_loader(
    state: dict[str, Any] | None,
    *,
    load_snapshot: Callable[[], dict[str, Any]],
    pane_key: PaneKey,
    is_source_entry: SourceEntryPredicate,
    now: str,
    sanitize: Sanitizer = _default_sanitize,
    raw_space_id_predicate: RawSpacePredicate | None = None,
) -> list[dict[str, Any]]:
    """Return source inventory panes while preserving state on backend failure/degrade."""
    try:
        snapshot = load_snapshot()
    except Exception as exc:
        if isinstance(state, dict):
            preserved = source_state_panes(state, is_source_entry=is_source_entry)
            if preserved:
                note_source_inventory_snapshot_failure(
                    state,
                    exc,
                    preserved,
                    now=now,
                    sanitize=sanitize,
                )
                return preserved
        raise
    preserved = source_state_panes(state, is_source_entry=is_source_entry) if isinstance(state, dict) else []
    inventory = source_inventory_panes(
        snapshot,
        preserved_panes=preserved,
        pane_key=pane_key,
        sanitize=sanitize,
        raw_space_id_predicate=raw_space_id_predicate,
    )
    note_source_inventory_result(state, inventory, now=now)
    return list(inventory.get("panes") or [])


def is_source_read_pane(pane: dict[str, Any] | None) -> bool:
    return isinstance(pane, dict) and bool(pane.get("_tendwire_source_read"))


def entry_delivered_turn_identities(entry: dict[str, Any]) -> set[str]:
    raw = entry.get("delivered_turn_identities")
    if not isinstance(raw, list):
        return set()
    identities: set[str] = set()
    for value in raw:
        clean = str(value or "").strip()
        if clean:
            identities.add(clean)
    return identities


def source_turn_delivery_ledger(state: dict[str, Any] | None) -> dict[str, Any]:
    if not isinstance(state, dict):
        return {}
    ledger = state.get("tendwire_source_delivered_turns")
    if not isinstance(ledger, dict):
        ledger = {}
        state["tendwire_source_delivered_turns"] = ledger
    return ledger


def source_turn_delivery_key(
    worker_id: str,
    identity: str,
    *,
    sanitize: Sanitizer = _default_sanitize,
) -> str:
    clean_worker = sanitize(str(worker_id or ""), 120).strip()
    clean_identity = sanitize(str(identity or ""), 300).strip()
    if not clean_worker or not clean_identity:
        return ""
    payload = {"source": "tendwire", "worker_id": clean_worker, "turn_identity": clean_identity}
    return "source-turn:" + hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()[:24]


def source_turn_worker_id(
    pane: dict[str, Any] | None,
    entry: dict[str, Any] | None,
    *,
    sanitize: Sanitizer = _default_sanitize,
) -> str:
    for obj in (entry, pane):
        if not isinstance(obj, dict):
            continue
        value = str(obj.get("tendwire_worker_id") or obj.get("worker_id") or obj.get("_tendwire_worker_id") or "").strip()
        if value:
            return sanitize(value, 120).strip()
    return ""


def source_turn_delivery_seen(
    state: dict[str, Any] | None,
    pane: dict[str, Any] | None,
    entry: dict[str, Any] | None,
    identity: str,
    *,
    sanitize: Sanitizer = _default_sanitize,
) -> bool:
    if not isinstance(state, dict) or not identity:
        return False
    key = source_turn_delivery_key(
        source_turn_worker_id(pane, entry, sanitize=sanitize),
        identity,
        sanitize=sanitize,
    )
    ledger = state.get("tendwire_source_delivered_turns")
    return bool(key and isinstance(ledger, dict) and key in ledger)


def prune_source_turn_delivery_ledger(ledger: dict[str, Any], *, cap: int) -> None:
    if len(ledger) <= int(cap):
        return

    def sort_key(item: tuple[str, Any]) -> str:
        record = item[1]
        if isinstance(record, dict):
            return str(record.get("updated_at") or record.get("delivered_at") or "")
        return ""

    kept = dict(sorted(ledger.items(), key=sort_key)[-max(1, int(cap)):])
    ledger.clear()
    ledger.update(kept)


def note_source_turn_delivery_identity(
    state: dict[str, Any] | None,
    pane: dict[str, Any] | None,
    entry: dict[str, Any] | None,
    identity: str,
    *,
    turn_id: str = "",
    semantic_hash: str = "",
    now: str,
    refresh_updated_at: bool = True,
    sanitize: Sanitizer = _default_sanitize,
    cap: int,
) -> bool:
    worker_id = source_turn_worker_id(pane, entry, sanitize=sanitize)
    key = source_turn_delivery_key(worker_id, identity, sanitize=sanitize)
    if not isinstance(state, dict) or not key:
        return False
    ledger = source_turn_delivery_ledger(state)
    record = ledger.get(key)
    if not isinstance(record, dict):
        record = {"delivered_at": str(now or "")}
        ledger[key] = record
        changed = True
    else:
        changed = False
    values = {
        "worker_id": worker_id,
        "turn_identity": sanitize(str(identity or ""), 300).strip(),
        "turn_id": sanitize(str(turn_id or ""), 200).strip(),
        "semantic_hash": str(semantic_hash or ""),
    }
    if refresh_updated_at or not record.get("updated_at"):
        values["updated_at"] = str(now or "")
    for field, value in values.items():
        if record.get(field) != value:
            record[field] = value
            changed = True
    prune_source_turn_delivery_ledger(ledger, cap=cap)
    return changed


def note_source_turn_delivery(
    state: dict[str, Any] | None,
    pane: dict[str, Any] | None,
    entry: dict[str, Any] | None,
    item: dict[str, Any] | None,
    *,
    identity: str,
    semantic_hash: str = "",
    now: str,
    sanitize: Sanitizer = _default_sanitize,
    cap: int,
) -> bool:
    if not isinstance(item, dict) or str(item.get("kind") or "").lower() != "turn":
        return False
    return note_source_turn_delivery_identity(
        state,
        pane,
        entry,
        identity,
        turn_id=str(item.get("turn_id") or ""),
        semantic_hash=semantic_hash,
        now=now,
        refresh_updated_at=True,
        sanitize=sanitize,
        cap=cap,
    )


def seed_source_turn_delivery_ledger_from_entries(
    state: dict[str, Any] | None,
    *,
    is_source_entry: SourceEntryPredicate,
    identities_for_entry: Callable[[dict[str, Any]], set[str]],
    now: str,
    sanitize: Sanitizer = _default_sanitize,
    cap: int,
) -> bool:
    if not isinstance(state, dict):
        return False
    panes = state.get("panes") if isinstance(state.get("panes"), dict) else {}
    changed = False
    for entry in panes.values():
        if not isinstance(entry, dict) or not is_source_entry(entry):
            continue
        worker_id = source_turn_worker_id(None, entry, sanitize=sanitize)
        if not worker_id:
            continue
        for identity in identities_for_entry(entry):
            changed = note_source_turn_delivery_identity(
                state,
                None,
                entry,
                identity,
                now=now,
                refresh_updated_at=False,
                sanitize=sanitize,
                cap=cap,
            ) or changed
    return changed


def record_entry_delivered_turn_identity(
    entry: dict[str, Any],
    identity: str,
    *,
    cap: int,
) -> bool:
    clean_identity = str(identity or "").strip()
    if not clean_identity:
        return False
    raw = entry.get("delivered_turn_identities")
    identities = [str(value or "").strip() for value in raw] if isinstance(raw, list) else []
    identities = [value for value in identities if value and value != clean_identity]
    identities.append(clean_identity)
    if len(identities) > int(cap):
        identities = identities[-max(1, int(cap)):]
    if entry.get("delivered_turn_identities") == identities:
        return False
    entry["delivered_turn_identities"] = identities
    return True


def source_turn_for_pane(
    pane: dict[str, Any],
    turns_payload: dict[str, Any],
) -> dict[str, Any] | None:
    worker_id = str(pane.get("worker_id") or pane.get("_tendwire_worker_id") or "").strip()
    if not worker_id:
        return None
    fingerprint = str(pane.get("worker_fingerprint") or pane.get("_tendwire_fingerprint") or "").strip()
    turns = turns_payload.get("turns") if isinstance(turns_payload.get("turns"), list) else []
    fallback: dict[str, Any] | None = None
    for turn in turns:
        if not isinstance(turn, dict):
            continue
        if str(turn.get("worker_id") or "").strip() != worker_id:
            continue
        if fingerprint and str(turn.get("worker_fingerprint") or "").strip() == fingerprint:
            return turn
        if fallback is None:
            fallback = turn
    return fallback


def source_turn_feed_source(
    turn: dict[str, Any],
    *,
    sanitize: Sanitizer = _default_sanitize,
    final_reply_max_chars: int,
    user_prompt_max_chars: int,
) -> dict[str, Any]:
    assistant_final = sanitize(str(turn.get("assistant_final_text") or ""), final_reply_max_chars).strip()
    assistant_stream = sanitize(str(turn.get("assistant_stream_text") or ""), final_reply_max_chars).strip()
    user_text = sanitize(str(turn.get("user_text") or ""), user_prompt_max_chars).strip()
    complete = turn.get("complete") if isinstance(turn.get("complete"), bool) else bool(assistant_final)
    has_open_turn = turn.get("has_open_turn") if isinstance(turn.get("has_open_turn"), bool) else False
    feed_source = {
        "available": True,
        "turn_id": str(turn.get("id") or turn.get("turn_id") or turn.get("fingerprint") or ""),
        "user_text": user_text,
        "assistant_final_text": assistant_final,
        "assistant_stream_text": assistant_stream,
        "complete": complete,
        "has_open_turn": has_open_turn,
    }
    if isinstance(turn.get("awaiting_input"), bool):
        feed_source["awaiting_input"] = turn["awaiting_input"]
    for key in ("pending_interaction", "pending_decision"):
        value = turn.get(key)
        if isinstance(value, dict):
            feed_source[key] = dict(value)
    return feed_source


def note_source_turn_unavailable(
    entry: dict[str, Any],
    reason: str,
    *,
    sanitize: Sanitizer = _default_sanitize,
) -> None:
    entry["last_turn_available"] = False
    entry["last_turn_reason"] = sanitize(str(reason or "no_tendwire_turn"), 300) or "no_tendwire_turn"


def source_turn_feed_item(
    turn: dict[str, Any] | None,
    entry: dict[str, Any],
    *,
    make_feed_item: FeedItemFactory,
    text_hash: TextHash,
    sanitize: Sanitizer = _default_sanitize,
    final_reply_max_chars: int,
    user_prompt_max_chars: int,
    max_reply_chars: int,
) -> dict[str, Any] | None:
    if not isinstance(turn, dict):
        note_source_turn_unavailable(entry, "no_tendwire_turn", sanitize=sanitize)
        return None
    entry["last_turn_available"] = True
    entry.pop("last_turn_reason", None)
    feed_source = source_turn_feed_source(
        turn,
        sanitize=sanitize,
        final_reply_max_chars=final_reply_max_chars,
        user_prompt_max_chars=user_prompt_max_chars,
    )
    stream_text = sanitize(str(feed_source.get("assistant_stream_text") or ""), max_reply_chars).strip()
    stream_turn_id = sanitize(str(feed_source.get("turn_id") or ""), 300).strip()
    if stream_text and stream_turn_id and feed_source.get("has_open_turn") is True:
        entry["pending_stream_turn_id"] = stream_turn_id
        entry["pending_stream_text"] = stream_text
        entry["pending_stream_revision"] = text_hash(stream_text)
    item = make_feed_item(feed_source)
    if not item and feed_source.get("complete") is True and feed_source.get("has_open_turn") is True:
        item = make_feed_item({**feed_source, "has_open_turn": False, "assistant_stream_text": ""})
    if isinstance(item, dict):
        item["prompt_collapse_chars"] = int(entry.get("prompt_collapse_chars") or 0)
    return item
