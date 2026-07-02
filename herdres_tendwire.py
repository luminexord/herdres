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
from typing import Any, Callable


MODE_VALUES = ("off", "enrich", "commands", "source-read", "source")
LEGACY_TIMER_CONFLICT_MODES = {"commands", "source-read", "source"}
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


def snapshot_payload(
    *,
    runner: Runner,
    sanitize: Sanitizer = _default_sanitize,
    env: Any | None = None,
    default_herdr_bin: str = DEFAULT_HERDR_BIN,
) -> dict[str, Any]:
    proc = runner(
        [*command_base(env), "snapshot", "--json"],
        timeout=timeout_seconds(env),
        env=child_env(env, default_herdr_bin=default_herdr_bin),
    )
    if proc.returncode != 0:
        raise TendwireCallError(sanitize(proc.stderr or proc.stdout or "tendwire snapshot failed", 500))
    try:
        data = json.loads(proc.stdout)
    except json.JSONDecodeError as exc:
        raise TendwireCallError("tendwire snapshot returned non-JSON") from exc
    if not isinstance(data, dict):
        raise TendwireCallError("tendwire snapshot returned non-object JSON")
    return data


def turns_payload(
    *,
    runner: Runner,
    sanitize: Sanitizer = _default_sanitize,
    env: Any | None = None,
    default_herdr_bin: str = DEFAULT_HERDR_BIN,
) -> dict[str, Any]:
    proc = runner(
        [*command_base(env), "turns", "--json"],
        timeout=timeout_seconds(env),
        env=child_env(env, default_herdr_bin=default_herdr_bin),
    )
    if proc.returncode != 0:
        raise TendwireCallError(sanitize(proc.stderr or proc.stdout or "tendwire turns failed", 500))
    try:
        data = json.loads(proc.stdout)
    except json.JSONDecodeError as exc:
        raise TendwireCallError("tendwire turns returned non-JSON") from exc
    if not isinstance(data, dict):
        raise TendwireCallError("tendwire turns returned non-object JSON")
    return data


COMMAND_SUCCESS_STATUSES = {"accepted", "queued", "sent", "submitted", "ok", "success"}
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
    if status == "queued":
        return "Queued for Tendwire worker."
    for container in (response, response.get("result") if isinstance(response.get("result"), dict) else {}):
        message = str(container.get("reply") or container.get("message") or "").strip()
        if message:
            return sanitize(message, limit)
    return ""


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
        if raw_space_id_predicate is not None and raw_space_id_predicate(space_name):
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


def is_source_read_pane(pane: dict[str, Any] | None) -> bool:
    return isinstance(pane, dict) and bool(pane.get("_tendwire_source_read"))


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
    return {
        "available": True,
        "turn_id": str(turn.get("id") or turn.get("turn_id") or turn.get("fingerprint") or ""),
        "user_text": user_text,
        "assistant_final_text": assistant_final,
        "assistant_stream_text": assistant_stream,
        "complete": complete,
        "has_open_turn": has_open_turn,
    }
