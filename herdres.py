#!/usr/bin/env python3
"""Tiny source-mode-only Herdres connector.

Herdres no longer observes or controls Herdr directly on this branch. It owns
Telegram transport/state and delegates observation, command routing, turns,
pending interactions, backend health, and connector outbox to Tendwire.
"""

from __future__ import annotations

import argparse
import copy
import json
import sys
from typing import Any

from herdres_connector import config, doctor, speech, state
from herdres_connector.managed_bots import managed_bot_kind_for_username
from herdres_connector.safe import compact_ws, public_prune, sanitize_text, short_hash
from herdres_connector.source_sync import SyncRuntime, sync_once
from herdres_connector.telegram_delivery import TelegramClient
from herdres_connector.tendwire_client import TendwireClient

VERSION = "tendwired-source-only"
SAFE_SEND_FAILURE_REPLY = "Could not send safely. Refresh status and choose the target again."


def _json(data: dict[str, Any]) -> int:
    print(json.dumps(public_prune(data), ensure_ascii=False, sort_keys=True))
    return 0 if data.get("ok", True) else 1


def _runtime(*, dry_run: bool = False, with_outbox: bool = True) -> SyncRuntime:
    token = config.telegram_token()
    return SyncRuntime(
        tendwire=TendwireClient(),
        telegram=TelegramClient(token=token, dry_run=dry_run),
        dry_run=dry_run,
        with_outbox=with_outbox,
    )


def _send_text_from_payload(payload: dict[str, Any]) -> str:
    text = str(payload.get("text") or payload.get("caption") or "").strip()
    if text.startswith("/send"):
        return text[5:].strip()
    if text.startswith("/"):
        return ""
    return text


def _raw_text_from_payload(payload: dict[str, Any]) -> str:
    return str(payload.get("text") or payload.get("caption") or "").strip()


def _split_target_alias(text: str) -> tuple[str, str]:
    parts = str(text or "").strip().split(maxsplit=1)
    if not parts or not parts[0].startswith("@"):
        return "", str(text or "").strip()
    alias = parts[0].strip("@:,. ")
    rest = parts[1].strip() if len(parts) > 1 else ""
    if rest.startswith("/send"):
        rest = rest[5:].strip()
    return alias, rest


def _clean_voice_caption(caption: Any) -> str:
    text = str(caption or "").strip()
    if text.startswith("/send"):
        text = text[5:].strip()
    if text.startswith("/"):
        return ""
    alias, rest = _split_target_alias(text)
    return rest if alias else text


def _voice_transcript_from_payload(payload: dict[str, Any]) -> str:
    if not speech.is_voice_payload(payload) or not payload.get("_speech_pretranscribed"):
        return ""
    return sanitize_text(payload.get("_speech_transcript"), 12000).strip()


def _voice_submission_text(payload: dict[str, Any], alias_body: str = "") -> str:
    transcript = _voice_transcript_from_payload(payload)
    if not transcript:
        return ""
    caption = _clean_voice_caption(alias_body or payload.get("caption") or payload.get("text") or "")
    if caption and caption != transcript:
        return f"{transcript}\n\n{caption}"
    return transcript


def _voice_unavailable_reply(payload: dict[str, Any]) -> str:
    if payload.get("_speech_pretranscribed"):
        return "Got your voice note, but speech-to-text is unavailable on this host. Send text, or run `herdres speech check`."
    try:
        enabled = speech.speech_input_enabled()
    except Exception:
        enabled = False
    if not enabled:
        return "Voice transcription is off. Enable `HERDR_TELEGRAM_TOPICS_SPEECH_INPUT=1` and run `herdres speech install`, or send text."
    return "Got your voice note, but it could not be transcribed. Send text, or run `herdres speech check`."


def _worker_entry_from_reply(store: dict[str, Any], payload: dict[str, Any]) -> tuple[str, dict[str, Any]] | tuple[None, None]:
    binding = state.find_message_binding(
        store,
        payload.get("reply_to_message_id"),
        topic_id=payload.get("topic_id"),
    )
    if not binding:
        return None, None
    return state.find_worker_entry_by_id(store, str(binding.get("worker_id") or ""))


def _worker_entry_from_alias(store: dict[str, Any], alias: str, entry: dict[str, Any]) -> tuple[str, dict[str, Any]] | tuple[None, None]:
    return state.find_worker_entry_by_alias(
        store,
        alias,
        space_id=str(entry.get("tendwire_space_id") or entry.get("space_id") or ""),
    )


def _space_entry_for_entry(store: dict[str, Any], entry: dict[str, Any]) -> tuple[str, dict[str, Any]] | tuple[None, None]:
    if str(entry.get("entry_type") or "") == "space":
        key = state.find_entry_key_by_space(store, str(entry.get("tendwire_space_id") or entry.get("space_id") or ""))
        return (key, entry) if key else (None, None)
    return state.find_space_entry_by_id(store, str(entry.get("tendwire_space_id") or entry.get("space_id") or ""))


def _normalize_voice_mode(value: Any) -> str:
    clean = str(value or "").strip().lower().replace("-", "_")
    if clean in {"per_agent", "peragent", "agent", "agents", "voice"}:
        return "per_agent"
    if clean in {"shared", "manager", "single"}:
        return "shared"
    return ""


def _voice_mode_reply(store: dict[str, Any], entry: dict[str, Any], payload: dict[str, Any]) -> dict[str, Any] | None:
    raw = _raw_text_from_payload(payload)
    if not raw.startswith("/voice"):
        return None
    command, _, rest = raw.partition(" ")
    command_name = command[1:].split("@", 1)[0].strip().lower().replace("-", "_")
    if command_name != "voice":
        return None
    _space_key, space_entry = _space_entry_for_entry(store, entry)
    if space_entry is None:
        return {"handled": True, "reply": SAFE_SEND_FAILURE_REPLY, "status": "missing_space"}
    requested = _normalize_voice_mode(rest or "status")
    if requested:
        space_entry["voice_mode"] = requested
        space_entry["managed_voice_active"] = requested == "per_agent"
        for worker in state.source_worker_entries(store).values():
            if str(worker.get("tendwire_space_id") or worker.get("space_id") or "") == str(space_entry.get("tendwire_space_id") or space_entry.get("space_id") or ""):
                worker["voice_mode"] = requested
                worker["managed_voice_active"] = requested == "per_agent"
    current = _normalize_voice_mode(space_entry.get("voice_mode")) or ("per_agent" if config.managed_bots_enabled() else "shared")
    label = "per-agent" if current == "per_agent" else "shared"
    return {"handled": True, "reply": f"Voice mode: {label}.", "status": "voice_mode", "voice_mode": current}


def _managed_bot_kind_for_alias(store: dict[str, Any], alias: str) -> str:
    telegram = store.get("telegram") if isinstance(store.get("telegram"), dict) else {}
    return managed_bot_kind_for_username(telegram, alias)


def _request_id(entry: dict[str, Any], payload: dict[str, Any], text: str) -> str:
    material = {
        "message": payload.get("message_id"),
        "reply": payload.get("reply_to_message_id"),
        "text": text,
        "worker": entry.get("active_worker_id") or entry.get("tendwire_worker_id"),
        "space": entry.get("tendwire_space_id") or entry.get("space_id"),
    }
    target = entry.get("active_worker_id") or entry.get("tendwire_worker_id") or entry.get("tendwire_space_id") or "space"
    return f"herdres:{target}:{short_hash(material, 20)}"


def _target_for_entry(entry: dict[str, Any]) -> dict[str, str]:
    worker_id = str(entry.get("active_worker_id") or entry.get("tendwire_worker_id") or "").strip()
    fingerprint = str(entry.get("active_worker_fingerprint") or entry.get("tendwire_fingerprint") or "").strip()
    if worker_id:
        target = {"worker_id": worker_id}
        if fingerprint:
            target["worker_fingerprint"] = fingerprint
        return target
    space_id = str(entry.get("tendwire_space_id") or entry.get("space_id") or "").strip()
    return {"space_id": space_id} if space_id else {}


def _command_request(entry: dict[str, Any], payload: dict[str, Any], text: str) -> dict[str, Any]:
    return {
        "schema_version": 1,
        "action": "send_instruction",
        "request_id": _request_id(entry, payload, text),
        "dry_run": False,
        "target": _target_for_entry(entry),
        "instruction": {"text": text},
        "params": {"origin": "telegram", "telegram_origin": "topic"},
    }


def _success_reply(response: dict[str, Any]) -> str:
    status = str(response.get("status") or "").strip().lower()
    result = response.get("result") if isinstance(response.get("result"), dict) else {}
    delivery = str(result.get("delivery_state") or "").strip().lower()
    if status == "duplicate_instruction" or delivery == "duplicate_suppressed":
        return "Already sent to Tendwire worker."
    if delivery == "queued":
        return "Queued for Tendwire worker."
    if (
        str(result.get("transport_state") or "").strip().lower() == "submitted"
        and str(result.get("target_state_at_send") or "").strip().lower() == "working"
    ):
        return "Submitted to busy Tendwire worker."
    if status in {"accepted", "submitted", "sent", "ok", "success"}:
        return "Sent to Tendwire worker."
    return ""


def _command_succeeded(response: dict[str, Any]) -> bool:
    status = str(response.get("status") or "").strip().lower()
    if status in {"accepted", "duplicate_instruction", "queued", "sent", "submitted", "ok", "success"}:
        return True
    return bool(response.get("ok") is True and not status)


def command_reply(payload: dict[str, Any]) -> dict[str, Any]:
    with state.state_lock():
        store = state.load_state()
        _key, entry = state.find_entry_by_thread(store, str(payload.get("topic_id") or ""))
        if entry is None:
            return {"handled": False}
        voice_reply = _voice_mode_reply(store, entry, payload)
        if voice_reply is not None:
            state.save_state(store)
            return voice_reply
        text = _send_text_from_payload(payload)
        voice_payload = speech.is_voice_payload(payload)
        alias_source = text if text else _clean_voice_caption(payload.get("caption") or payload.get("text") or "")
        alias, clean_text = _split_target_alias(alias_source)
        if alias:
            _alias_key, alias_entry = _worker_entry_from_alias(store, alias, entry)
            if alias_entry is not None:
                entry = alias_entry
                text = clean_text
            else:
                alias_kind = _managed_bot_kind_for_alias(store, alias)
                _kind_key, kind_entry = _worker_entry_from_alias(store, alias_kind, entry)
                if alias_kind and kind_entry is not None:
                    entry = kind_entry
                    text = clean_text
                else:
                    return {"handled": True, "reply": SAFE_SEND_FAILURE_REPLY, "status": "unknown_target_alias"}
        else:
            _reply_key, reply_entry = _worker_entry_from_reply(store, payload)
            if reply_entry is not None:
                entry = reply_entry
            else:
                target_bot_kind = str(payload.get("target_bot_kind") or "").strip().lower()
                if target_bot_kind:
                    _kind_key, kind_entry = _worker_entry_from_alias(store, target_bot_kind, entry)
                    if kind_entry is not None:
                        entry = kind_entry
                    else:
                        return {"handled": True, "reply": SAFE_SEND_FAILURE_REPLY, "status": "unknown_target_bot"}
        voice_text = _voice_submission_text(payload, clean_text if alias else "")
        if voice_text:
            text = voice_text
        if not text:
            if voice_payload:
                return {"handled": True, "reply": _voice_unavailable_reply(payload)}
            return {"handled": True, "reply": "Send a message in this topic or use /send <instruction>."}
        request = _command_request(entry, payload, text)
        response = TendwireClient().command(request)
        ledger = store.setdefault("tendwire_command_submissions", {})
        identity = short_hash({"request": request["request_id"], "worker": entry.get("tendwire_worker_id")}, 20)
        ledger[identity] = {
            "worker_id": entry.get("active_worker_id") or entry.get("tendwire_worker_id"),
            "space_id": entry.get("tendwire_space_id") or entry.get("space_id"),
            "status": response.get("status") or "unknown",
        }
        state.save_state(store)
        if _command_succeeded(response):
            return {"handled": True, "reply": _success_reply(response)}
        return {"handled": True, "reply": SAFE_SEND_FAILURE_REPLY, "status": response.get("status") or "failed"}


def callback_reply(_payload: dict[str, Any]) -> dict[str, Any]:
    return {"handled": True, "reply": "This source-only Herdres branch does not use Telegram callbacks."}


def _sync_pass() -> dict[str, Any]:
    with state.state_lock():
        store = state.load_state()
        result = sync_once(store, _runtime(dry_run=False, with_outbox=True))
        if result.get("changed"):
            state.save_state(store)
    return result


def cmd_sync(args: argparse.Namespace) -> int:
    config.load_env_file()
    config.require_source_mode()
    interval = float(getattr(args, "loop", 0) or 0)
    if interval <= 0:
        return _json(_sync_pass())
    import time as _time

    while True:
        started = _time.monotonic()
        try:
            _sync_pass()
        except Exception as exc:  # noqa: BLE001 - keep the loop alive across transient failures
            print(json.dumps({"ok": False, "status": "sync_pass_failed", "error": sanitize_text(str(exc), 300)}), flush=True)
        _time.sleep(max(0.5, interval - (_time.monotonic() - started)))


def cmd_command(_args: argparse.Namespace) -> int:
    config.load_env_file()
    try:
        payload = json.loads(sys.stdin.read() or "{}")
    except json.JSONDecodeError:
        payload = {}
    return _json(command_reply(payload if isinstance(payload, dict) else {}))


def cmd_callback(_args: argparse.Namespace) -> int:
    config.load_env_file()
    try:
        payload = json.loads(sys.stdin.read() or "{}")
    except json.JSONDecodeError:
        payload = {}
    return _json(callback_reply(payload if isinstance(payload, dict) else {}))


def cmd_doctor(_args: argparse.Namespace) -> int:
    config.load_env_file()
    return _json(doctor.run_doctor())


def cmd_speech(args: argparse.Namespace) -> int:
    config.load_env_file()
    action = str(args.action or "check")
    if action == "check":
        return _json({"ok": True, "speech": speech.check()})
    if action == "install":
        logs: list[str] = []
        ok, detail = speech.install_stt_model(force=bool(args.force), log=lambda msg: logs.append(str(msg)))
        result = {
            "ok": bool(ok),
            "status": "ok" if ok else "failed",
            "stt_model": detail,
            "speech": speech.check(),
        }
        if logs:
            result["log"] = logs[-3:]
        if not speech.sherpa_available():
            result["next_step"] = "Install the sherpa-onnx Python package, then enable HERDR_TELEGRAM_TOPICS_SPEECH_INPUT=1."
        return _json(result)
    return _json({"ok": False, "status": "failed", "error": f"unknown speech action: {action}"})


def cmd_source_smoke(args: argparse.Namespace) -> int:
    config.load_env_file()
    config.require_source_mode()
    with state.state_lock():
        store = copy.deepcopy(state.load_state())
    result = sync_once(store, _runtime(dry_run=True, with_outbox=bool(args.with_outbox)))
    payload = {
        "ok": bool(result.get("ok")),
        "status": "ok" if result.get("ok") else "failed",
        "mode": "source",
        "dry_run": True,
        "with_outbox": bool(args.with_outbox),
        "direct_herdr_calls": 0,
        "sync_result": result,
        "delivery_evidence": {
            "source_entry_count": len(state.source_entries(store)),
            "delivered_turn_count": len(store.get("tendwire_source_delivered_turns") or {}),
        },
    }
    return _json(payload)


def cmd_outbox(args: argparse.Namespace) -> int:
    config.load_env_file()
    with state.state_lock():
        store = state.load_state()
        runtime = _runtime(dry_run=bool(args.dry_run), with_outbox=True)
        chat_id = config.telegram_chat_id(store)
        from herdres_connector.telegram_delivery import drain_outbox

        result = drain_outbox(store, runtime.telegram, runtime.tendwire, chat_id=chat_id, max_sends=int(args.limit), dry_run=bool(args.dry_run))
        if result.get("changed") and not args.dry_run:
            state.save_state(store)
    return _json({"ok": True, **result})


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="herdres")
    sub = parser.add_subparsers(dest="cmd", required=True)
    sync_parser = sub.add_parser("sync")
    sync_parser.add_argument("--loop", type=float, default=0.0, help="run continuously, one pass every N seconds")
    sync_parser.set_defaults(func=cmd_sync)
    sub.add_parser("command").set_defaults(func=cmd_command)
    sub.add_parser("callback").set_defaults(func=cmd_callback)
    sub.add_parser("doctor").set_defaults(func=cmd_doctor)
    sub.add_parser("version").set_defaults(func=lambda _args: (print(VERSION), 0)[1])
    speech_parser = sub.add_parser("speech")
    speech_parser.add_argument("action", nargs="?", default="check", choices=["check", "install"])
    speech_parser.add_argument("--force", action="store_true")
    speech_parser.set_defaults(func=cmd_speech)
    tendwire = sub.add_parser("tendwire")
    tendwire_sub = tendwire.add_subparsers(dest="tendwire_cmd", required=True)
    smoke = tendwire_sub.add_parser("source-smoke")
    smoke.add_argument("--with-outbox", action="store_true")
    smoke.set_defaults(func=cmd_source_smoke)
    outbox = tendwire_sub.add_parser("outbox")
    outbox.add_argument("--limit", type=int, default=3)
    outbox.add_argument("--dry-run", action="store_true")
    outbox.set_defaults(func=cmd_outbox)
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        return int(args.func(args) or 0)
    except Exception as exc:  # noqa: BLE001 - command boundary returns public-safe JSON
        return _json({"ok": False, "status": "failed", "error": sanitize_text(str(exc), 300)})


if __name__ == "__main__":
    raise SystemExit(main())
