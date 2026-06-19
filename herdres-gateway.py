#!/usr/bin/env python3
"""Standalone Telegram getUpdates gateway for Herdres pane-topic control."""

from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
import time
import urllib.parse
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from herdres_routing import (
    attachment_payload_dict,
    build_callback_payload_dict,
    build_command_payload_dict,
    mapped_entry_for_dict,
    mapped_topic_entry,
    message_thread_id_dict,
    owner_ids,
)

DEFAULT_ENV_PATH = Path.home() / ".config/herdres/herdres.env"
DEFAULT_STATE_PATH = Path.home() / ".local/share/herdres/state.json"
DEFAULT_SCRIPT_PATH = Path.home() / ".local/bin/herdres"
DEFAULT_OFFSET_PATH = Path.home() / ".local/share/herdres/gateway.offset"
ALLOWED_UPDATES = json.dumps(["message", "callback_query"])
STOP = False


@dataclass(frozen=True)
class GatewayConfig:
    token: str
    state_path: Path = DEFAULT_STATE_PATH
    script_path: Path = DEFAULT_SCRIPT_PATH
    offset_path: Path = DEFAULT_OFFSET_PATH
    long_poll_seconds: int = 50
    command_timeout: float = 25.0
    error_backoff: float = 3.0


def log(message: str) -> None:
    print(f"[herdres-gateway] {message}", file=sys.stderr, flush=True)


def load_env_file(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    try:
        lines = path.expanduser().read_text(encoding="utf-8").splitlines()
    except FileNotFoundError:
        return values
    except OSError as exc:
        log(f"could not read env file {path}: {exc}")
        return values
    for raw in lines:
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip("'\"")
        if key:
            values[key] = value
    return values


def merged_env() -> dict[str, str]:
    env_file = Path(os.environ.get("HERDRES_ENV_FILE", str(DEFAULT_ENV_PATH))).expanduser()
    values = load_env_file(env_file)
    values.update(os.environ)
    return values


def config_from_env(env: dict[str, str] | None = None) -> GatewayConfig:
    values = env if env is not None else merged_env()
    token = (values.get("HERDRES_GATEWAY_BOT_TOKEN") or values.get("TELEGRAM_BOT_TOKEN") or "").strip()
    return GatewayConfig(
        token=token,
        state_path=Path(values.get("HERDR_TELEGRAM_TOPICS_STATE", str(DEFAULT_STATE_PATH))).expanduser(),
        script_path=Path(values.get("HERDR_TELEGRAM_TOPICS_SCRIPT", str(DEFAULT_SCRIPT_PATH))).expanduser(),
        offset_path=Path(values.get("HERDR_TELEGRAM_TOPICS_GATEWAY_OFFSET", str(DEFAULT_OFFSET_PATH))).expanduser(),
        long_poll_seconds=int(values.get("HERDRES_GATEWAY_LONG_POLL_SECONDS", "50") or "50"),
        command_timeout=float(values.get("HERDRES_GATEWAY_COMMAND_TIMEOUT", "25") or "25"),
        error_backoff=float(values.get("HERDRES_GATEWAY_ERROR_BACKOFF", "3") or "3"),
    )


def load_state(path: Path) -> dict[str, Any] | None:
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        log(f"state is unreadable: {exc}")
        return None
    if not isinstance(data, dict) or data.get("version") != 1 or not data.get("enabled", True):
        return None
    return data


def telegram_api(token: str, method: str, params: dict[str, Any] | None = None, *, timeout: float = 30.0) -> dict[str, Any]:
    url = f"https://api.telegram.org/bot{token}/{method}"
    data = urllib.parse.urlencode(params or {}).encode("utf-8")
    request = urllib.request.Request(url, data=data, method="POST")
    with urllib.request.urlopen(request, timeout=timeout) as response:
        body = response.read().decode("utf-8")
    parsed = json.loads(body)
    if not isinstance(parsed, dict):
        raise RuntimeError("Telegram API returned non-object JSON")
    return parsed


def delete_webhook(config: GatewayConfig) -> None:
    try:
        telegram_api(config.token, "deleteWebhook", {"drop_pending_updates": "false"}, timeout=15.0)
    except Exception as exc:
        log(f"deleteWebhook failed: {exc}")


def read_offset(path: Path) -> int | None:
    try:
        return int(path.read_text(encoding="utf-8").strip())
    except Exception:
        return None


def write_offset_atomic(path: Path, offset: int) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    with open(temp, "w", encoding="utf-8") as handle:
        handle.write(f"{int(offset)}\n")
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temp, path)
    try:
        directory = os.open(path.parent, os.O_DIRECTORY)
    except OSError:
        return
    try:
        os.fsync(directory)
    finally:
        os.close(directory)


def drain_backlog(config: GatewayConfig) -> int | None:
    response = telegram_api(
        config.token,
        "getUpdates",
        {"timeout": 0, "allowed_updates": ALLOWED_UPDATES},
        timeout=20.0,
    )
    updates = response.get("result") if response.get("ok", True) else []
    if not isinstance(updates, list) or not updates:
        return None
    next_offset = max(int(update["update_id"]) for update in updates if "update_id" in update) + 1
    write_offset_atomic(config.offset_path, next_offset)
    log(f"drained {len(updates)} backlog update(s); starting at offset {next_offset}")
    return next_offset


def get_updates(config: GatewayConfig, offset: int | None) -> list[dict[str, Any]]:
    params: dict[str, Any] = {"timeout": config.long_poll_seconds, "allowed_updates": ALLOWED_UPDATES}
    if offset is not None:
        params["offset"] = offset
    response = telegram_api(
        config.token,
        "getUpdates",
        params,
        timeout=max(20.0, config.long_poll_seconds + 15.0),
    )
    if not response.get("ok", True):
        raise RuntimeError(f"getUpdates returned not ok: {str(response)[:200]}")
    updates = response.get("result") or []
    if not isinstance(updates, list):
        raise RuntimeError("getUpdates returned non-list result")
    return [update for update in updates if isinstance(update, dict)]


def command_error_result(mode: str, text: str) -> dict[str, Any]:
    if mode == "callback":
        return {"handled": True, "answer": text, "show_alert": True}
    return {"handled": True, "reply": text}


def run_herdres(config: GatewayConfig, mode: str, payload: dict[str, Any]) -> dict[str, Any]:
    if not config.script_path.exists():
        return command_error_result(
            mode,
            "Herdr callback could not start." if mode == "callback" else "Herdr topic command script is missing.",
        )
    env = os.environ.copy()
    env["HERDR_TELEGRAM_TOPICS_STATE"] = str(config.state_path)
    try:
        proc = subprocess.run(
            [str(config.script_path), mode],
            input=json.dumps(payload),
            text=True,
            capture_output=True,
            env=env,
            timeout=config.command_timeout,
        )
    except subprocess.TimeoutExpired:
        log(f"{mode} timed out for topic {payload.get('topic_id')}")
        return command_error_result(
            mode,
            "Herdr callback timed out." if mode == "callback" else "Herdr topic command timed out before completion.",
        )
    except Exception as exc:
        log(f"{mode} failed to start: {exc}")
        return command_error_result(
            mode,
            "Herdr callback could not start." if mode == "callback" else "Herdr topic command could not start. Check host logs.",
        )
    if proc.returncode != 0:
        detail = (proc.stderr or proc.stdout or "").strip()
        log(f"{mode} exited {proc.returncode}: {detail[:500]}")
        return command_error_result(
            mode,
            "Herdr callback failed." if mode == "callback" else "Herdr topic command failed. Check host logs.",
        )
    try:
        parsed = json.loads(proc.stdout)
    except Exception:
        log(f"{mode} returned invalid JSON: {proc.stdout[:500]}")
        return command_error_result(
            mode,
            "Herdr callback returned invalid output." if mode == "callback" else "Herdr topic command returned invalid output.",
        )
    if not isinstance(parsed, dict):
        return command_error_result(
            mode,
            "Herdr callback returned invalid output." if mode == "callback" else "Herdr topic command returned invalid output.",
        )
    return parsed


def send_message(
    config: GatewayConfig,
    chat_id: str,
    text: str,
    *,
    topic_id: str | None = None,
    reply_to_message_id: str | None = None,
) -> None:
    params: dict[str, Any] = {"chat_id": chat_id, "text": text}
    if topic_id:
        params["message_thread_id"] = topic_id
    if reply_to_message_id:
        params["reply_to_message_id"] = reply_to_message_id
    try:
        telegram_api(config.token, "sendMessage", params, timeout=30.0)
    except Exception as exc:
        log(f"sendMessage failed: {exc}")


def answer_callback(config: GatewayConfig, callback_query_id: str, result: dict[str, Any]) -> None:
    params: dict[str, Any] = {"callback_query_id": callback_query_id}
    answer = str(result.get("answer") or "").strip()
    if answer:
        params["text"] = answer
    if result.get("show_alert"):
        params["show_alert"] = "true"
    try:
        telegram_api(config.token, "answerCallbackQuery", params, timeout=15.0)
    except Exception as exc:
        log(f"answerCallbackQuery failed: {exc}")


def handle_message(config: GatewayConfig, message: dict[str, Any]) -> None:
    attachment = attachment_payload_dict(message)
    if not (message.get("text") or attachment):
        return
    state = load_state(config.state_path)
    if not state or not mapped_entry_for_dict(state, message):
        return
    payload = build_command_payload_dict(message)
    owners = owner_ids(state)
    if payload["from_bot"] or (owners and payload["user_id"] not in owners):
        return
    result = run_herdres(config, "command", payload)
    if not result.get("handled", True):
        return
    reply = str(result.get("reply") or "").strip()
    if reply:
        send_message(
            config,
            payload["chat_id"],
            reply,
            topic_id=payload["topic_id"],
            reply_to_message_id=payload["message_id"],
        )


def handle_callback(config: GatewayConfig, query: dict[str, Any]) -> None:
    data = str(query.get("data") or "")
    if not data.startswith("herdr:"):
        return
    state = load_state(config.state_path)
    if not state:
        return
    message = query.get("message") if isinstance(query.get("message"), dict) else {}
    chat = message.get("chat") if isinstance(message.get("chat"), dict) else {}
    if not chat:
        return
    chat_id = str(chat.get("id") or "")
    thread_id = message_thread_id_dict(message)
    if not mapped_topic_entry(state, chat_id, thread_id):
        return
    # Do NOT owner-prefilter callbacks: herdres callback_reply enforces auth and
    # returns the answer text. Dropping non-owner callbacks here would leave the
    # Telegram button spinner spinning forever (no answerCallbackQuery).
    payload = build_callback_payload_dict(query)
    result = run_herdres(config, "callback", payload)
    if not result.get("handled", True):
        return
    answer_callback(config, str(query.get("id") or ""), result)


def handle_update(config: GatewayConfig, update: dict[str, Any]) -> None:
    message = update.get("message")
    if isinstance(message, dict):
        handle_message(config, message)
        return
    query = update.get("callback_query")
    if isinstance(query, dict):
        handle_callback(config, query)


def poll_once(config: GatewayConfig) -> None:
    offset = read_offset(config.offset_path)
    if offset is None:
        try:
            offset = drain_backlog(config)
        except Exception as exc:
            log(f"initial backlog drain failed: {exc}")
            time.sleep(config.error_backoff)
            return
        if offset is not None:
            return
    try:
        updates = get_updates(config, offset)
    except Exception as exc:
        log(f"getUpdates failed: {exc}")
        time.sleep(config.error_backoff)
        return
    for update in updates:
        update_id = update.get("update_id")
        if update_id is None:
            continue
        try:
            handle_update(config, update)
        except Exception as exc:
            log(f"handler failed for update {update_id}: {exc}")
        write_offset_atomic(config.offset_path, int(update_id) + 1)


def _stop(_signum: int, _frame: Any) -> None:
    global STOP
    STOP = True


def install_signal_handlers() -> None:
    signal.signal(signal.SIGTERM, _stop)
    signal.signal(signal.SIGINT, _stop)


def poll_loop(config: GatewayConfig) -> int:
    while not STOP:
        poll_once(config)
    return 0


def main() -> int:
    config = config_from_env()
    if not config.token:
        log("HERDRES_GATEWAY_BOT_TOKEN or TELEGRAM_BOT_TOKEN is required")
        return 1
    install_signal_handlers()
    delete_webhook(config)
    return poll_loop(config)


if __name__ == "__main__":
    raise SystemExit(main())
