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
from typing import Any, Callable

from herdres_connector import config, doctor, speech, state
from herdres_connector.managed_bots import managed_bot_kind_for_username
from herdres_connector.safe import compact_ws, public_prune, sanitize_text, short_hash
from herdres_connector.source_sync import SyncRuntime, sync_once
from herdres_connector.telegram_delivery import TelegramClient
from herdres_connector.tendwire_client import TendwireClient

VERSION = "0.6.0-tendwired-source-only"
SAFE_SEND_FAILURE_REPLY = "Could not send safely. Refresh status and choose the target again."


def _json(data: dict[str, Any]) -> int:
    output = public_prune(data)
    if isinstance(output, dict):
        for key in ("failed_plan_token", "plan_token"):
            value = data.get(key)
            if (
                isinstance(value, str)
                and value.startswith("twplan1.")
                and 9 <= len(value) <= 264
                and all(
                    char.isascii() and (char.isalnum() or char in "_-")
                    for char in value[8:]
                )
            ):
                output[key] = value
    print(json.dumps(output, ensure_ascii=False, sort_keys=True))
    return 0 if data.get("ok", True) else 1


def _runtime(
    *,
    dry_run: bool = False,
    with_outbox: bool = True,
    checkpoint: Callable[[], None] | None = None,
) -> SyncRuntime:
    token = config.telegram_token()
    runtime = SyncRuntime(
        tendwire=TendwireClient(),
        telegram=TelegramClient(token=token, dry_run=dry_run),
        dry_run=dry_run,
        with_outbox=with_outbox,
    )
    # SyncRuntime intentionally remains constructible by old callers. The source executor consumes
    # this optional seam when it needs a durable receipt before acknowledging a Tendwire job.
    runtime.checkpoint = checkpoint
    return runtime


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


def _worker_entry_matches_binding_topic(
    store: dict[str, Any],
    binding: dict[str, Any],
    entry: dict[str, Any],
) -> bool:
    binding_topic = str(binding.get("topic_id") or "")
    return bool(
        binding_topic
        and binding_topic in state.worker_entry_allowed_topic_ids(store, entry)
    )


def _worker_entry_from_reply(store: dict[str, Any], payload: dict[str, Any]) -> tuple[str, dict[str, Any]] | tuple[None, None]:
    binding = state.find_message_binding(
        store,
        payload.get("reply_to_message_id"),
        topic_id=payload.get("topic_id"),
    )
    if not binding or "routing_quarantined" in binding:
        return None, None
    identity = state.message_binding_stable_identity(binding)
    has_stable_fields = "stable_key" in binding or "stable_key_version" in binding
    if identity is not None:
        key, entry = state.find_worker_entry_by_stable_key(store, identity[0])
    elif has_stable_fields:
        return None, None
    else:
        key, entry = state.find_worker_entry_by_id(store, str(binding.get("worker_id") or ""))
        if entry is None:
            return None, None
        bound_fingerprint = str(binding.get("worker_fingerprint") or "")
        bound_space = str(binding.get("space_id") or "")
        if bound_fingerprint and bound_fingerprint != str(entry.get("tendwire_fingerprint") or ""):
            return None, None
        if bound_space and bound_space != str(entry.get("tendwire_space_id") or entry.get("space_id") or ""):
            return None, None
    if (
        key is None
        or entry is None
        or not state.worker_entry_is_uniquely_routable(store, key, entry)
        or not _worker_entry_matches_binding_topic(store, binding, entry)
    ):
        return None, None
    return key, entry


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


# Choices whose selection needs the owner to then TYPE something (the picker's "write your own"
# option): the turn adapter stamps these ids on AskUserQuestion's custom option and ExitPlanMode's
# revise option. A bare number selecting one of these is refused so we never send a digit that leaves
# the pane waiting for text the owner didn't provide.
_FREETEXT_CHOICE_IDS = {"custom", "revise"}


def _pending_number_reply(entry: dict[str, Any], text: str) -> tuple[str, str] | None:
    """Validate a bare-number reply against the worker's LIVE pending prompt (backend-captured
    question + choices). Returns (text_to_send, "") when valid — the digit itself, which the pane's
    picker interprets natively — or ("", error_reply) to fail closed (stale prompt, out of range,
    custom choice). None = not a number-reply situation; the text passes through unchanged."""
    clean = str(text or "").strip()
    if not clean.isdigit() or len(clean) > 2:
        return None
    try:
        payload = TendwireClient().pending()
    except Exception:
        return None  # pending unavailable: don't block, pass the number through
    worker_id = str(entry.get("active_worker_id") or entry.get("tendwire_worker_id") or "")
    for row in payload.get("pending_interactions", []) if isinstance(payload, dict) else []:
        if not isinstance(row, dict) or str(row.get("worker_id") or "") != worker_id:
            continue
        if str(row.get("status") or "open") != "open":
            continue
        choices = row.get("choices") if isinstance(row.get("choices"), list) else []
        if not choices:
            return None  # synthetic/choice-less pending: nothing to validate against
        index = int(clean)
        if not 1 <= index <= len(choices):
            return ("", f"That prompt has {len(choices)} choices — reply 1–{len(choices)}, or type your answer.")
        choice = choices[index - 1] if isinstance(choices[index - 1], dict) else {}
        # tendwire dropped the private send_text 'value' from public pending.list (PR #3 review
        # hardening), so a free-text option is now identified by its stable choice_id. The old
        # empty-value check stays as a backstop for pre-sync daemons that still publish 'value'.
        choice_id = str(choice.get("choice_id") or "").strip().lower()
        value = choice.get("value")
        needs_custom_text = choice_id in _FREETEXT_CHOICE_IDS or (
            value is not None and not str(value).strip()
        )
        if needs_custom_text:
            return ("", "That choice takes a custom answer — just type it as text.")
        return (clean, "")
    return None  # no live pending with choices for this worker: pass through


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
            elif payload.get("reply_to_message_id") and state.find_message_binding(
                store,
                payload.get("reply_to_message_id"),
                topic_id=payload.get("topic_id"),
            ):
                return {"handled": True, "reply": SAFE_SEND_FAILURE_REPLY, "status": "ambiguous_reply_target"}
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
        # "reply by voice" is a BRIDGE directive: arm the one-shot speak flag and STRIP the phrase so
        # the agent never sees it (an agent reading it thinks it must produce audio itself and goes
        # off installing TTS). A standalone trigger just arms the flag without submitting a turn.
        if text and speech.speech_reply_triggered(text):
            entry["speak_next_reply"] = True
            text = speech.strip_speech_reply_trigger(text)
            if not text:
                state.save_state(store)
                return {"handled": True, "reply": "🎙️ Got it — the next reply will be spoken."}
        if not text:
            if voice_payload:
                return {"handled": True, "reply": _voice_unavailable_reply(payload)}
            return {"handled": True, "reply": "Send a message in this topic or use /send <instruction>."}
        # A bare number answering a live captured prompt: validate against the pending's choices and
        # fail closed on stale/out-of-range/custom, else send the digit (the picker's native input).
        number_reply = _pending_number_reply(entry, text)
        if number_reply is not None:
            mapped, error_reply = number_reply
            if error_reply:
                return {"handled": True, "reply": error_reply}
            text = mapped
        # Reply-to-voice auto-mode (#4): replying to one of this pane's voice notes speaks the next
        # reply back. One-shot flag consumed at delivery (_speak_reply in source_sync).
        if speech.speech_reply_on_voice_reply_enabled() and state.message_is_voice_reply(
            entry, payload.get("reply_to_message_id")
        ):
            entry["speak_next_reply"] = True
        request = _command_request(entry, payload, text)
        client = TendwireClient()
        response = client.command(request)
        if (
            str(response.get("status") or "") == "stale_target"
            and isinstance(request.get("target"), dict)
            and request["target"].get("worker_fingerprint")
        ):
            # Worker fingerprints churn with status/summary, so a cached
            # fingerprint can be seconds stale. Retry once pinned by id only.
            retry = json.loads(json.dumps(request))
            retry["target"].pop("worker_fingerprint", None)
            retry["request_id"] = f"{request['request_id']}-r2"
            response = client.command(retry)
        ledger = store.setdefault("tendwire_command_submissions", {})
        identity = short_hash({"request": request["request_id"], "worker": entry.get("tendwire_worker_id")}, 20)
        ledger[identity] = {
            "worker_id": entry.get("active_worker_id") or entry.get("tendwire_worker_id"),
            "space_id": entry.get("tendwire_space_id") or entry.get("space_id"),
            "status": response.get("status") or "unknown",
        }
        state.save_state(store)
        if _command_succeeded(response):
            return {"handled": True, "reply": _success_reply(response) if config.ack_on_send() else ""}
        return {"handled": True, "reply": SAFE_SEND_FAILURE_REPLY, "status": response.get("status") or "failed"}


def callback_reply(_payload: dict[str, Any]) -> dict[str, Any]:
    return {"handled": True, "reply": "This source-only Herdres branch does not use Telegram callbacks."}


def _sync_pass() -> dict[str, Any]:
    with state.state_lock():
        store = state.load_state()

        def checkpoint() -> None:
            if not state.lock_held():
                raise RuntimeError("state checkpoint requires the held state lock")
            state.save_state(store)

        result = sync_once(
            store,
            _runtime(
                dry_run=False,
                with_outbox=True,
                checkpoint=checkpoint,
            ),
        )
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


_TURN_FINAL_RECOVERY_AUDIT_LIMIT = 100
_TURN_FINAL_RECOVERY_RESPONSE_KEYS = {
    "schema_version",
    "ok",
    "status",
    "failed_plan_token",
    "plan_token",
    "generation",
    "content_revision",
    "state",
    "acknowledged_prefix_count",
    "executable_job_count",
    "retained_failed_job_count",
    "prior_attempt_count",
    "idempotent_replay",
}
_TURN_FINAL_RECOVERY_AUDIT_KEYS = _TURN_FINAL_RECOVERY_RESPONSE_KEYS - {
    "schema_version",
    "ok",
    "status",
    "idempotent_replay",
}


def _recovery_error(status: str, error: str) -> dict[str, Any]:
    return {
        "ok": False,
        "status": status,
        "error": sanitize_text(error, 240),
    }


def _valid_recovery_plan_token(value: Any) -> bool:
    return bool(
        isinstance(value, str)
        and value.startswith("twplan1.")
        and 9 <= len(value) <= 264
        and all(
            char.isascii() and (char.isalnum() or char in "_-")
            for char in value[8:]
        )
    )


def _valid_recovery_request_id(value: Any) -> bool:
    return bool(
        isinstance(value, str)
        and 1 <= len(value) <= 128
        and all(
            char.isascii() and (char.isalnum() or char in "._:-")
            for char in value
        )
    )


def _recovery_audits(store: dict[str, Any]) -> dict[str, Any]:
    audits = store.get("tendwire_turn_final_recoveries")
    return audits if isinstance(audits, dict) else {}
def _recovery_request_key(request_id: str) -> str:
    return short_hash(
        {"turn_final_recovery_request_id": request_id},
        32,
    )


def _recovery_request_bindings(store: dict[str, Any]) -> dict[str, Any]:
    bindings = store.get("tendwire_turn_final_recovery_requests")
    return bindings if isinstance(bindings, dict) else {}
def _inherited_recovery_audit(
    store: dict[str, Any],
    *,
    failed_plan_token: str,
    expected_predecessor_plan_token: str | None,
    content_revision: str,
    prior_generation: int,
    expected_job_count: int,
) -> dict[str, Any]:
    audits = _recovery_audits(store)
    request_bindings = _recovery_request_bindings(store)
    candidate_keys = {
        key
        for key, binding in request_bindings.items()
        if isinstance(binding, dict)
        and binding.get("plan_token") == failed_plan_token
    }
    candidate_keys.update(
        key
        for key, audit in audits.items()
        if isinstance(audit, dict)
        and audit.get("plan_token") == failed_plan_token
    )
    if prior_generation == 1:
        if candidate_keys:
            return _recovery_error(
                "recovery_state_invalid",
                "first-generation failed plan has conflicting inherited recovery state",
            )
        return {
            "ok": True,
            "identity": None,
            "retained_failed_job_count": 0,
        }
    if len(candidate_keys) != 1:
        return _recovery_error(
            "recovery_state_invalid",
            "failed plan does not have one inherited recovery audit",
        )

    request_key = next(iter(candidate_keys))
    audit = audits.get(request_key)
    binding = request_bindings.get(request_key)
    integer_fields = (
        "generation",
        "acknowledged_prefix_count",
        "executable_job_count",
        "retained_failed_job_count",
        "prior_attempt_count",
    )
    if (
        not isinstance(audit, dict)
        or set(audit) != _TURN_FINAL_RECOVERY_AUDIT_KEYS
        or not isinstance(binding, dict)
        or set(binding) != {"failed_plan_token", "plan_token", "generation"}
        or not _valid_recovery_plan_token(audit.get("failed_plan_token"))
        or (
            prior_generation > 1
            and not _valid_recovery_plan_token(
                expected_predecessor_plan_token
            )
        )
        or (
            prior_generation > 1
            and audit.get("failed_plan_token")
            != expected_predecessor_plan_token
        )
        or (
            prior_generation > 1
            and binding.get("failed_plan_token")
            != expected_predecessor_plan_token
        )
        or audit.get("failed_plan_token") == failed_plan_token
        or audit.get("plan_token") != failed_plan_token
        or audit.get("content_revision") != content_revision
        or audit.get("state") != "active"
        or any(
            type(audit.get(field)) is not int or int(audit[field]) < 0
            for field in integer_fields
        )
        or int(audit["generation"]) != prior_generation
        or int(audit["generation"]) < 2
        or int(audit["acknowledged_prefix_count"])
        + int(audit["executable_job_count"])
        != expected_job_count
        or int(audit["retained_failed_job_count"]) <= 0
        or binding.get("failed_plan_token") != audit.get("failed_plan_token")
        or binding.get("plan_token") != failed_plan_token
        or type(binding.get("generation")) is not int
        or int(binding["generation"]) != prior_generation
    ):
        return _recovery_error(
            "recovery_state_invalid",
            "inherited recovery audit does not identify the failed plan generation",
        )
    retained_failed_job_count = int(audit["retained_failed_job_count"])
    return {
        "ok": True,
        "identity": (
            request_key,
            expected_predecessor_plan_token,
            str(audit["failed_plan_token"]),
            failed_plan_token,
            prior_generation,
            retained_failed_job_count,
        ),
        "retained_failed_job_count": retained_failed_job_count,
    }
def _recovery_audit_eviction_key(
    store: dict[str, Any],
    *,
    released_plan_token: str | None = None,
) -> str | None:
    audits = _recovery_audits(store)
    request_bindings = _recovery_request_bindings(store)
    protected_plan_tokens = {
        str(entry["pending_plan_token"])
        for entry in state.source_worker_entries(store).values()
        if isinstance(entry, dict)
        and _valid_recovery_plan_token(entry.get("pending_plan_token"))
        and entry.get("pending_plan_token") != released_plan_token
    }
    for request_key, audit in audits.items():
        audit_plan_token = (
            audit.get("plan_token") if isinstance(audit, dict) else None
        )
        binding = request_bindings.get(request_key)
        binding_plan_token = (
            binding.get("plan_token")
            if isinstance(binding, dict)
            else None
        )
        if (
            audit_plan_token not in protected_plan_tokens
            and binding_plan_token not in protected_plan_tokens
        ):
            return request_key
    return None






def _valid_acknowledged_recovery_receipt(
    job_key: str,
    receipt: dict[str, Any],
    *,
    failed_plan_token: str,
    content_revision: str,
) -> bool:
    sequence = receipt.get("sequence_index")
    operation = receipt.get("operation")
    ordinal = receipt.get("part_ordinal")
    part_count = receipt.get("part_count")
    message_id = receipt.get("telegram_message_id")
    prior_message_id = receipt.get("prior_message_id")
    checkpoint = receipt.get("checkpoint_sequence")
    if (
        type(sequence) is not int
        or sequence < 0
        or job_key
        != f"turn-final:{failed_plan_token}:{sequence:06d}"
        or receipt.get("plan_token") != failed_plan_token
        or receipt.get("content_revision") != content_revision
        or operation not in {"upsert", "retire"}
        or type(ordinal) is not int
        or ordinal < 0
        or type(part_count) is not int
        or part_count <= 0
        or type(checkpoint) is not int
        or checkpoint <= 0
    ):
        return False
    if operation == "upsert":
        return bool(
            ordinal < part_count
            and isinstance(message_id, str)
            and message_id
            and message_id != "0"
            and len(message_id) <= 80
            and (
                prior_message_id in (None, "")
                or (
                    isinstance(prior_message_id, str)
                    and 0 < len(prior_message_id) <= 80
                )
            )
        )
    return bool(
        ordinal >= part_count
        and isinstance(prior_message_id, str)
        and 0 < len(prior_message_id) <= 80
        and message_id in (None, "")
    )


def _turn_final_recovery_preflight(
    store: dict[str, Any],
    failed_plan_token: str,
    request_id: str,
) -> dict[str, Any]:
    request_key = _recovery_request_key(request_id)
    request_bindings = _recovery_request_bindings(store)
    prior_binding = request_bindings.get(request_key)
    prior_audit = _recovery_audits(store).get(request_key)
    if prior_binding is not None:
        if (
            not isinstance(prior_binding, dict)
            or prior_binding.get("failed_plan_token") != failed_plan_token
            or not _valid_recovery_plan_token(
                prior_binding.get("plan_token")
            )
            or not isinstance(prior_audit, dict)
            or prior_audit.get("failed_plan_token") != failed_plan_token
            or prior_audit.get("plan_token")
            != prior_binding.get("plan_token")
        ):
            return _recovery_error(
                "recovery_request_conflict",
                "request_id is already bound or its replay detail expired",
            )
        return {
            "ok": True,
            "replay": True,
            "audit": copy.deepcopy(prior_audit),
        }
    if prior_audit is not None:
        return _recovery_error(
            "recovery_request_conflict",
            "recovery request detail lacks its durable request binding",
        )
    if len(request_bindings) >= state.TENDWIRE_TURN_JOB_LIMIT:
        return _recovery_error(
            "recovery_capacity_exceeded",
            "local recovery request binding capacity is full",
        )

    matches = [
        (key, entry)
        for key, entry in state.source_worker_entries(store).items()
        if entry.get("pending_plan_token") == failed_plan_token
    ]
    if len(matches) != 1:
        return _recovery_error(
            "recovery_plan_not_found",
            "failed plan is not the unique pending Herdres plan",
        )
    entry_key, entry = matches[0]
    if not state.worker_entry_is_uniquely_routable(store, entry_key, entry):
        return _recovery_error(
            "recovery_route_ambiguous",
            "failed plan no longer has one uniquely routable worker",
        )
    revision = entry.get("pending_content_revision")
    part_count = entry.get("pending_turn_part_count")
    job_count = entry.get("pending_turn_job_count")
    prior_generation = entry.get("pending_plan_generation", 1)
    predecessor_plan_token = entry.get("replaces_failed_plan_token")
    if (
        not isinstance(revision, str)
        or not revision.startswith("twrev1.")
        or isinstance(part_count, bool)
        or not isinstance(part_count, int)
        or part_count <= 0
        or isinstance(job_count, bool)
        or not isinstance(job_count, int)
        or job_count < part_count
        or isinstance(prior_generation, bool)
        or not isinstance(prior_generation, int)
        or prior_generation < 1
        or (
            prior_generation > 1
            and not _valid_recovery_plan_token(predecessor_plan_token)
        )
    ):
        return _recovery_error(
            "recovery_state_invalid",
            "pending plan coordinates are invalid",
        )

    inherited = _inherited_recovery_audit(
        store,
        failed_plan_token=failed_plan_token,
        expected_predecessor_plan_token=(
            str(predecessor_plan_token)
            if prior_generation > 1
            else None
        ),
        content_revision=str(revision),
        prior_generation=prior_generation,
        expected_job_count=job_count,
    )
    if inherited.get("ok") is not True:
        return inherited

    jobs = state.tendwire_turn_jobs(store)
    old_receipts: list[tuple[str, dict[str, Any]]] = []
    for job_key, receipt in jobs.items():
        if not isinstance(receipt, dict) or receipt.get("plan_token") != failed_plan_token:
            continue
        if (
            receipt.get("content_revision") != revision
            or not isinstance(receipt.get("sequence_index"), int)
            or isinstance(receipt.get("sequence_index"), bool)
        ):
            return _recovery_error(
                "recovery_state_invalid",
                "failed-plan receipt coordinates are invalid",
            )
        old_receipts.append((job_key, receipt))
    old_receipts.sort(key=lambda item: int(item[1]["sequence_index"]))

    prefix: list[tuple[str, dict[str, Any]]] = []
    expected_sequence = 0
    terminal_tail_seen = False
    for job_key, receipt in old_receipts:
        sequence = int(receipt["sequence_index"])
        substate = receipt.get("substate")
        if substate in {"telegram_applied", "old_slot_retired"}:
            return _recovery_error(
                "recovery_receipt_inflight",
                "failed plan has a provider outcome awaiting durable ACK",
            )
        if substate == "reserved":
            return _recovery_error(
                "recovery_receipt_uncertain",
                "failed plan has an unproven reserved provider operation",
            )
        if substate == "acknowledged":
            if terminal_tail_seen or sequence != expected_sequence:
                return _recovery_error(
                    "recovery_state_invalid",
                    "acknowledged receipts are not one contiguous prefix",
                )
            if not _valid_acknowledged_recovery_receipt(
                job_key,
                receipt,
                failed_plan_token=failed_plan_token,
                content_revision=str(revision),
            ):
                return _recovery_error(
                    "recovery_state_invalid",
                    "acknowledged prefix receipt is malformed",
                )
            prefix.append((job_key, receipt))
            expected_sequence += 1
            continue
        if substate == "failed":
            terminal_tail_seen = True
            continue
        return _recovery_error(
            "recovery_state_invalid",
            "failed plan contains an unknown receipt state",
        )

    bindings = state.message_bindings(store)
    prefix_keys = {job_key for job_key, _receipt in prefix}
    if any(
        isinstance(binding, dict)
        and binding.get("plan_token") == failed_plan_token
        and binding.get("tendwire_job_key") not in prefix_keys
        for binding in bindings.values()
    ):
        return _recovery_error(
            "recovery_receipt_uncertain",
            "failed plan has a binding without an acknowledged receipt",
        )
    if len(jobs) + job_count > state.TENDWIRE_TURN_JOB_LIMIT:
        return _recovery_error(
            "recovery_capacity_exceeded",
            "local receipt capacity cannot hold both immutable plan generations",
        )
    audits = _recovery_audits(store)
    if (
        len(audits) >= _TURN_FINAL_RECOVERY_AUDIT_LIMIT
        and _recovery_audit_eviction_key(
            store,
            released_plan_token=failed_plan_token,
        )
        is None
    ):
        return _recovery_error(
            "recovery_capacity_exceeded",
            "local recovery audit capacity is held by pending plans",
        )
    current_failed_tail_count = sum(
        receipt.get("substate") == "failed"
        for _job_key, receipt in old_receipts[len(prefix):]
    )
    retained_failed_job_count = (
        int(inherited["retained_failed_job_count"])
        + current_failed_tail_count
    )
    return {
        "ok": True,
        "replay": False,
        "entry_key": entry_key,
        "content_revision": revision,
        "part_count": part_count,
        "job_count": job_count,
        "prefix": prefix,
        "request_key": request_key,
        "prior_generation": prior_generation,
        "expected_predecessor_plan_token": (
            str(predecessor_plan_token)
            if prior_generation > 1
            else None
        ),
        "current_failed_tail_count": current_failed_tail_count,
        "inherited_retained_failed_job_count": int(
            inherited["retained_failed_job_count"]
        ),
        "failed_tail_count": current_failed_tail_count,
        "expected_retained_failed_job_count": retained_failed_job_count,
        "inherited_audit_identity": inherited["identity"],
        "fingerprint": (
            entry_key,
            revision,
            part_count,
            job_count,
            tuple(
                (
                    job_key,
                    receipt.get("substate"),
                    receipt.get("checkpoint_sequence"),
                )
                for job_key, receipt in old_receipts
            ),
            inherited["identity"],
            retained_failed_job_count,
            len(jobs),
        ),
    }


def _validate_recovery_response(
    response: dict[str, Any],
    *,
    failed_plan_token: str,
    content_revision: str,
    acknowledged_prefix_count: int,
    expected_job_count: int,
    expected_generation: int,
    retained_failed_job_count: int,
) -> dict[str, Any] | None:
    if set(response) != _TURN_FINAL_RECOVERY_RESPONSE_KEYS:
        return _recovery_error(
            "recovery_state_uncertain",
            "Tendwire recovery response shape is invalid",
        )
    plan_token = response.get("plan_token")
    integer_fields = (
        "generation",
        "acknowledged_prefix_count",
        "executable_job_count",
        "retained_failed_job_count",
        "prior_attempt_count",
    )
    if (
        response.get("schema_version") != 1
        or response.get("ok") is not True
        or response.get("status") != "recovered"
        or response.get("failed_plan_token") != failed_plan_token
        or not _valid_recovery_plan_token(plan_token)
        or plan_token == failed_plan_token
        or response.get("content_revision") != content_revision
        or response.get("state") != "active"
        or type(response.get("idempotent_replay")) is not bool
        or any(
            type(response.get(field)) is not int or int(response[field]) < 0
            for field in integer_fields
        )
        or int(response["generation"]) != expected_generation
        or int(response["acknowledged_prefix_count"])
        != acknowledged_prefix_count
        or int(response["acknowledged_prefix_count"])
        + int(response["executable_job_count"])
        != expected_job_count
        or int(response["retained_failed_job_count"])
        != retained_failed_job_count
    ):
        return _recovery_error(
            "recovery_state_uncertain",
            "Tendwire recovery response failed local revalidation",
        )
    return None


def _clone_recovery_prefix(
    store: dict[str, Any],
    *,
    failed_plan_token: str,
    plan_token: str,
    entry_key: str,
    prefix: list[tuple[str, dict[str, Any]]],
    executable_job_count: int,
    request_id: str,
    response: dict[str, Any],
) -> None:
    key_map: dict[str, str] = {}
    for old_key, receipt in prefix:
        sequence = int(receipt["sequence_index"])
        new_key = f"turn-final:{plan_token}:{sequence:06d}"
        cloned = state.reserve_tendwire_turn_job(
            store,
            new_key,
            plan_token=plan_token,
            content_revision=str(receipt["content_revision"]),
            operation=str(receipt["operation"]),
            sequence_index=sequence,
            part_ordinal=int(receipt["part_ordinal"]),
            part_count=int(receipt["part_count"]),
            telegram_message_id=str(receipt.get("telegram_message_id") or ""),
            prior_message_id=str(receipt.get("prior_message_id") or ""),
            bot_kind=str(receipt.get("bot_kind") or ""),
        )
        if cloned.get("operation") == "upsert":
            state.update_tendwire_turn_job(
                store,
                new_key,
                substate="telegram_applied",
                telegram_message_id=str(receipt["telegram_message_id"]),
                prior_message_id=(
                    str(receipt["prior_message_id"])
                    if receipt.get("prior_message_id")
                    else None
                ),
                bot_kind=(
                    str(receipt["bot_kind"])
                    if receipt.get("bot_kind")
                    else None
                ),
            )
            if receipt.get("prior_message_id"):
                state.update_tendwire_turn_job(
                    store,
                    new_key,
                    substate="old_slot_retired",
                )
        else:
            state.update_tendwire_turn_job(
                store,
                new_key,
                substate="telegram_applied",
                prior_message_id=str(receipt["prior_message_id"]),
                bot_kind=(
                    str(receipt["bot_kind"])
                    if receipt.get("bot_kind")
                    else None
                ),
            )
        state.update_tendwire_turn_job(
            store,
            new_key,
            substate="acknowledged",
        )
        key_map[old_key] = new_key

    for binding in state.message_bindings(store).values():
        if (
            isinstance(binding, dict)
            and binding.get("plan_token") == failed_plan_token
            and binding.get("tendwire_job_key") in key_map
        ):
            binding["plan_token"] = plan_token
            binding["tendwire_job_key"] = key_map[str(binding["tendwire_job_key"])]

    entry = state.source_worker_entries(store).get(entry_key)
    if entry is None or entry.get("pending_plan_token") != failed_plan_token:
        raise RuntimeError("recovery entry changed before copied-state cutover")
    entry["pending_plan_token"] = plan_token
    entry["pending_turn_job_count"] = len(prefix) + executable_job_count
    entry["pending_plan_generation"] = int(response["generation"])
    entry["pending_acknowledged_prefix_count"] = len(prefix)
    entry["replaces_failed_plan_token"] = failed_plan_token

    request_key = _recovery_request_key(request_id)
    request_bindings = _recovery_request_bindings(store)
    if not request_bindings:
        request_bindings = {}
        store["tendwire_turn_final_recovery_requests"] = request_bindings
    if (
        request_key in request_bindings
        or len(request_bindings) >= state.TENDWIRE_TURN_JOB_LIMIT
    ):
        raise RuntimeError("recovery request binding capacity conflict")
    request_bindings[request_key] = {
        "failed_plan_token": failed_plan_token,
        "plan_token": plan_token,
        "generation": int(response["generation"]),
    }

    audits = _recovery_audits(store)
    if not audits:
        audits = {}
        store["tendwire_turn_final_recoveries"] = audits
    if request_key in audits:
        raise RuntimeError("recovery audit already exists")
    if len(audits) >= _TURN_FINAL_RECOVERY_AUDIT_LIMIT:
        eviction_key = _recovery_audit_eviction_key(store)
        if eviction_key is None:
            raise RuntimeError("recovery audit capacity conflict")
        audits.pop(eviction_key, None)
    audits[request_key] = {
        key: copy.deepcopy(response[key])
        for key in _TURN_FINAL_RECOVERY_RESPONSE_KEYS
        if key
        not in {"schema_version", "ok", "status", "idempotent_replay"}
    }


def cmd_recover_turn_final(args: argparse.Namespace) -> int:
    config.load_env_file()
    config.require_source_mode()
    failed_plan_token = str(args.plan_token or "")
    request_id = str(args.request_id or "")
    if not _valid_recovery_plan_token(failed_plan_token) or not _valid_recovery_request_id(request_id):
        return _json(
            _recovery_error(
                "invalid_recovery_request",
                "plan token or request id is not a bounded public recovery coordinate",
            )
        )

    with state.state_lock():
        store = state.load_state()
        before = _turn_final_recovery_preflight(
            store,
            failed_plan_token,
            request_id,
        )
        if before.get("ok") is not True:
            return _json(before)
        client = TendwireClient()
        response = client.connector_prepare_recover(
            failed_plan_token=failed_plan_token,
            request_id=request_id,
        )
        if response.get("ok") is not True:
            return _json(response)

        if before.get("replay") is True:
            audit = before["audit"]
            validation = _validate_recovery_response(
                response,
                failed_plan_token=failed_plan_token,
                content_revision=str(audit["content_revision"]),
                acknowledged_prefix_count=int(audit["acknowledged_prefix_count"]),
                expected_job_count=int(audit["acknowledged_prefix_count"])
                + int(audit["executable_job_count"]),
                expected_generation=int(audit["generation"]),
                retained_failed_job_count=int(
                    audit["retained_failed_job_count"]
                ),
            )
            immutable_mismatch = any(
                response.get(key) != value
                for key, value in audit.items()
            )
            if (
                validation is not None
                or immutable_mismatch
                or response.get("idempotent_replay") is not True
            ):
                return _json(
                    validation
                    or _recovery_error(
                        "recovery_state_uncertain",
                        "idempotent Tendwire replay did not match the immutable audit",
                    )
                )
            return _json(response)

        current = _turn_final_recovery_preflight(
            store,
            failed_plan_token,
            request_id,
        )
        if (
            current.get("ok") is not True
            or current.get("replay") is True
            or current.get("fingerprint") != before.get("fingerprint")
        ):
            return _json(
                _recovery_error(
                    "recovery_state_uncertain",
                    "Herdres state changed during recovery RPC",
                )
            )
        validation = _validate_recovery_response(
            response,
            failed_plan_token=failed_plan_token,
            content_revision=str(current["content_revision"]),
            acknowledged_prefix_count=len(current["prefix"]),
            expected_job_count=int(current["job_count"]),
            expected_generation=int(current["prior_generation"]) + 1,
            retained_failed_job_count=int(
                current["expected_retained_failed_job_count"]
            ),
        )
        if validation is not None:
            return _json(validation)
        candidate = copy.deepcopy(store)
        _clone_recovery_prefix(
            candidate,
            failed_plan_token=failed_plan_token,
            plan_token=str(response["plan_token"]),
            entry_key=str(current["entry_key"]),
            prefix=current["prefix"],
            executable_job_count=int(response["executable_job_count"]),
            request_id=request_id,
            response=response,
        )
        state.save_state(candidate)
    return _json(response)


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
    recover = tendwire_sub.add_parser("recover-turn-final")
    recover.add_argument("--plan-token", required=True)
    recover.add_argument("--request-id", required=True)
    recover.set_defaults(func=cmd_recover_turn_final)
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
