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
import threading
import time
from typing import Any, Callable

from herdres_connector import config, decisions, doctor, speech, state
from herdres_connector import ingress_requests
from herdres_connector.ingress_identity import validate_request_id
from herdres_connector.managed_bots import (
    managed_bot_kind_for_entry,
    managed_bot_kind_for_username,
)
from herdres_connector.safe import compact_ws, public_prune, sanitize_text
from herdres_connector.source_sync import (
    SyncRuntime,
    deliver_submission_working_card,
    drain_outbound_once,
    sync_once,
)
from herdres_connector.telegram_delivery import TelegramClient
from herdres_connector.tendwire_client import (
    TendwireClient,
    command_process_ambiguous,
    command_process_not_started,
)

VERSION = "0.7.0rc4-tendwired-source-only"
SAFE_SEND_FAILURE_REPLY = "Could not send safely. Refresh status and choose the target again."
BUSY_SEND_REPLY = "Submitted to busy Tendwire worker."
TERMINAL_SUCCESS_REPLIES = {
    "Sent to Tendwire worker.",
    BUSY_SEND_REPLY,
}
REKEYED_TOPIC_QUARANTINE_REPLY = (
    "This pane was re-keyed after a Herdr restart; a fresh topic is being "
    "created. Please use the new pane topic when it appears."
)


def _json(data: dict[str, Any]) -> int:
    output = public_prune(data)
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
    source_identity = state.message_binding_source_identity(binding)
    has_source_fields = (
        "tendwire_stable_key" in binding
        or "tendwire_stable_key_version" in binding
    )
    pane_uuid = state.message_binding_pane_uuid(binding)
    identity = state.message_binding_stable_identity(binding)
    has_stable_fields = "stable_key" in binding or "stable_key_version" in binding
    if source_identity is not None:
        key, entry = state.find_worker_entry_by_stable_key(
            store, source_identity[0]
        )
    elif has_source_fields:
        return None, None
    elif pane_uuid:
        key, entry = state.find_worker_entry_by_pane_uuid(store, pane_uuid)
    elif "pane_uuid" in binding or "pane_uuid_version" in binding:
        return None, None
    elif identity is not None:
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
    binding_bot_kind = str(binding.get("bot_kind") or "").strip().lower()
    entry_bot_kind = managed_bot_kind_for_entry(entry)
    if (
        source_identity is None
        and binding_bot_kind
        and binding_bot_kind != "manager"
        and entry_bot_kind
        and binding_bot_kind != entry_bot_kind
    ):
        # A shared-topic pane reconciliation in older releases could rewrite
        # the local binding to the other pane.  The managed bot that authored
        # the Telegram message is an exact per-agent witness.  Recover only
        # when that kind names one unique worker in the same space; otherwise
        # the ordinary fail-closed checks below reject the reply.
        binding_space_id = str(binding.get("space_id") or "")
        kind_matches = [
            (candidate_key, candidate)
            for candidate_key, candidate in state.source_worker_entries(
                store
            ).items()
            if state.worker_entry_is_uniquely_routable(
                store, candidate_key, candidate
            )
            and str(
                candidate.get("tendwire_space_id")
                or candidate.get("space_id")
                or ""
            )
            == binding_space_id
            and managed_bot_kind_for_entry(candidate) == binding_bot_kind
        ]
        key, entry = kind_matches[0] if len(kind_matches) == 1 else (None, None)
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




def _stable_owner_for_entry(
    entry: dict[str, Any],
) -> tuple[str, int] | None:
    """Return durable owner evidence without changing the wire selector."""

    identity = state.entry_stable_identity(entry)
    if identity is None and str(entry.get("entry_type") or "") == "space":
        active_identity = (
            entry.get("active_worker_stable_key"),
            entry.get("active_worker_stable_key_version"),
        )
        if state.valid_stable_worker_key_pair(*active_identity):
            identity = active_identity
    return identity


def _target_for_entry(entry: dict[str, Any]) -> dict[str, str]:
    # The command wire accepts worker/fingerprint or space selectors. Stable
    # identity remains the separately persisted target_owner for v3 submission
    # correlation and never enters the external request.
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
# option): ACP decision events use these ids for AskUserQuestion's custom option and ExitPlanMode's
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
    request = {
        "schema_version": 1,
        "action": "send_instruction",
        "request_id": validate_request_id(payload.get("request_id")),
        "dry_run": False,
        "target": _target_for_entry(entry),
        "instruction": {"text": text},
    }
    if config.command_response_schema_version() == 3:
        request["response_schema_version"] = 3
    return request


def _success_reply(response: dict[str, Any]) -> str:
    if response.get("disposition") != "terminal_accepted":
        return ""
    result = response.get("result") if isinstance(response.get("result"), dict) else {}
    delivery = str(result.get("delivery_state") or "").strip().lower()
    if delivery == "duplicate_suppressed":
        return ""
    if delivery == "queued":
        return "Queued for Tendwire worker."
    if (
        str(result.get("transport_state") or "").strip().lower() == "submitted"
        and str(result.get("target_state_at_send") or "").strip().lower() == "working"
    ):
        return BUSY_SEND_REPLY
    return "Sent to Tendwire worker."


def _ingress_timing_log(
    hop: str,
    request_id: str,
    *,
    created_at: float | None = None,
    detail: str = "",
) -> None:
    """Write a private, parent-filtered ingress timing breadcrumb to stderr."""

    if not config.gateway_timing_logs_enabled():
        return
    now = time.time()
    fields = [
        f"hop={hop}",
        f"at={now:.6f}",
        f"request_id={request_id}",
    ]
    if created_at is not None:
        fields.append(f"receive_ms={max(0.0, now - float(created_at)) * 1000.0:.1f}")
    if detail:
        fields.append(f"detail={sanitize_text(detail, 120).replace(' ', '_')}")
    print("[herdres-timing] " + " ".join(fields), file=sys.stderr, flush=True)




def _submit_ingress_command_record(
    store: dict[str, Any],
    record: dict[str, Any],
    *,
    instant_ack_posted: bool = False,
    gateway_success_ack_enabled: bool = True,
) -> ingress_requests.IngressResult:
    """Replay durable bytes off-lock and reduce authoritative dispositions.

    ``command_reply`` enters with the connector state lock held.  The canonical
    request is already fsynced before this helper starts, so Tendwire may safely
    receive it while the global JSON flock is released.  Once the lock is
    reacquired, reload the store before applying the response so a concurrent
    sync/dispatcher write is never clobbered.
    """

    request_id = validate_request_id(record.get("request_id"))
    inline_in_progress_replayed = False

    while True:
        now = time.time()
        if now >= record["deadline_at"]:
            outcome = ingress_requests.quarantine_request(
                record, "request deadline reached", now=now
            )
            state.save_state(store)
            return outcome

        request_json = record.get("request_json")
        if not isinstance(request_json, str):
            outcome = ingress_requests.quarantine_request(
                record, "missing durable request JSON", now=now
            )
            state.save_state(store)
            return outcome

        # Construct the AF_UNIX client only after deadline/cache preflight.
        # command_json parses only the exact durably stored UTF-8 request.
        _ingress_timing_log(
            "tendwire_submit_sent",
            request_id,
            created_at=record.get("created_at"),
        )
        with state.released_lock():
            response = TendwireClient().command_json(request_json)
        transitioned_at = time.time()
        _ingress_timing_log(
            "tendwire_submit_returned",
            request_id,
            created_at=record.get("created_at"),
            detail=str(response.get("disposition") or response.get("status") or "unknown"),
        )

        latest = state.load_state()
        store.clear()
        store.update(latest)
        record, migrated = ingress_requests.ensure_request_shell(
            store,
            request_id,
            now=transitioned_at,
            retry_horizon=config.command_retry_horizon_seconds(),
            retention=config.command_request_retention_seconds(),
        )
        if migrated:
            state.save_state(store)
        if record["state"] in {"terminal", "quarantined"}:
            return ingress_requests.IngressResult.from_mapping(record["outcome"])
        if record.get("request_json") != request_json:
            outcome = ingress_requests.quarantine_request(
                record, "conflicting ingress request", now=transitioned_at
            )
            state.save_state(store)
            return outcome

        if command_process_ambiguous(response) or command_process_not_started(
            response
        ):
            if transitioned_at >= record["deadline_at"]:
                outcome = ingress_requests.quarantine_request(
                    record, "request deadline reached", now=transitioned_at
                )
                state.save_state(store)
                return outcome
            outcome = ingress_requests.mark_retryable(
                record, None, now=transitioned_at
            )
            state.save_state(store)
            return outcome

        disposition = response.get("disposition")
        if disposition == "terminal_accepted":
            result = (
                response.get("result")
                if isinstance(response.get("result"), dict)
                else {}
            )
            submission_id = result.get("submission_id")
            if isinstance(submission_id, str) and submission_id:
                try:
                    ingress_requests.attach_submission_receipt(
                        record,
                        submission_id,
                        str(result.get("observed_turn_state") or ""),
                        result.get("turn_id"),
                        now=transitioned_at,
                    )
                except ValueError:
                    outcome = ingress_requests.quarantine_request(
                        record,
                        "invalid Tendwire submission receipt",
                        now=transitioned_at,
                    )
                    state.save_state(store)
                    return outcome
            reply = _success_reply(response)
            if (
                not gateway_success_ack_enabled
                and reply in TERMINAL_SUCCESS_REPLIES
            ):
                reply = ""
            if instant_ack_posted and reply != BUSY_SEND_REPLY:
                reply = ""
            outcome = ingress_requests.mark_terminal(
                record,
                disposition,
                now=transitioned_at,
                # A legacy caller that already posted an instant success keeps
                # its empty terminal reply. The lane gateway leaves this reply
                # intact and queues it off the dispatch critical path.
                reply=reply,
            )
            state.save_state(store)
            _ingress_timing_log(
                "receipt_stored",
                request_id,
                created_at=record.get("created_at"),
                detail=disposition,
            )
            if isinstance(submission_id, str) and submission_id:
                try:
                    working = deliver_submission_working_card(
                        store,
                        request_id,
                        _runtime(
                            dry_run=False,
                            with_outbox=False,
                            checkpoint=lambda: state.save_state(store),
                        ),
                        chat_id=config.telegram_chat_id(store),
                        now=transitioned_at,
                    )
                    _ingress_timing_log(
                        "working_card_delivery",
                        request_id,
                        created_at=record.get("created_at"),
                        detail=str(working.get("status") or "unknown"),
                    )
                except Exception as exc:  # noqa: BLE001 - acceptance stays authoritative
                    _ingress_timing_log(
                        "working_card_delivery",
                        request_id,
                        created_at=record.get("created_at"),
                        detail=f"failed:{type(exc).__name__}",
                    )
            return outcome
        if disposition == "terminal_rejected":
            outcome = ingress_requests.mark_terminal(
                record,
                disposition,
                now=transitioned_at,
                reply=SAFE_SEND_FAILURE_REPLY,
            )
            state.save_state(store)
            _ingress_timing_log(
                "receipt_stored",
                request_id,
                created_at=record.get("created_at"),
                detail=disposition,
            )
            return outcome
        if disposition == "terminal_uncertain":
            outcome = ingress_requests.quarantine_request(
                record,
                "Tendwire reported terminal uncertainty",
                now=transitioned_at,
                disposition=disposition,
            )
            state.save_state(store)
            return outcome
        if disposition == "in_progress" and not inline_in_progress_replayed:
            # Tendwire v3 may return a short-lived pending receipt immediately
            # after accepting the transport write. One exact-byte replay is a
            # read of that idempotent request, not a second instruction. Keep
            # it inside this claim so the normal two-second lane backoff does
            # not turn a sub-second receipt into a serialized queue.
            inline_in_progress_replayed = True
            continue
        if disposition not in ingress_requests.RETRYABLE_DISPOSITIONS:
            outcome = ingress_requests.quarantine_request(
                record, "invalid Tendwire command result", now=transitioned_at
            )
            state.save_state(store)
            return outcome
        if transitioned_at >= record["deadline_at"]:
            outcome = ingress_requests.quarantine_request(
                record, "request deadline reached", now=transitioned_at
            )
            state.save_state(store)
            return outcome

        if (
            disposition == "no_receipt"
            and response.get("status") == "stale_target"
        ):
            # The only legal byte rewrite is a one-time removal of the stale
            # worker fingerprint. Recheck the immutable deadline before both
            # the durable rewrite and the second socket request.
            retry_at = time.time()
            if retry_at >= record["deadline_at"]:
                outcome = ingress_requests.quarantine_request(
                    record, "request deadline reached", now=retry_at
                )
                state.save_state(store)
                return outcome
            refreshed = ingress_requests.stale_target_refresh_json(
                record, now=retry_at
            )
            if refreshed is not None:
                state.save_state(store)
                continue

        outcome = ingress_requests.mark_retryable(
            record, disposition, now=transitioned_at
        )
        state.save_state(store)
        return outcome


def _local_ingress_outcome(
    store: dict[str, Any],
    record: dict[str, Any],
    *,
    reason: str,
    reply: str = "",
    handled: bool = True,
) -> dict[str, Any]:
    outcome = ingress_requests.quarantine_request(
        record,
        reason,
        now=time.time(),
        reply=reply,
        handled=handled,
    )
    state.save_state(store)
    return outcome




def command_reply(payload: dict[str, Any]) -> dict[str, Any]:
    with state.state_lock():
        store = state.load_state()
        now = time.time()
        durable_spool = payload.get("_durable_spool") is True
        first_seen_at = payload.get("_ingress_first_seen_at")
        durable_first_seen_at = (
            min(now, float(first_seen_at))
            if (
                durable_spool
                and isinstance(first_seen_at, (int, float))
                and not isinstance(first_seen_at, bool)
            )
            else None
        )
        changed = ingress_requests.prune_requests(store, now=now)
        record: dict[str, Any] | None = None
        try:
            ingress_request_id = validate_request_id(payload.get("request_id"))
        except ValueError:
            ingress_request_id = ""
        if ingress_request_id:
            shell_created = False
            if durable_first_seen_at is not None:
                # The lane row durably owns the original receive time. Restore
                # that time into the global request record before applying the
                # current deadline check, but keep all expiry/pruning decisions
                # on the current wall clock.
                _record, shell_created = ingress_requests.ensure_request_shell(
                    store,
                    ingress_request_id,
                    now=durable_first_seen_at,
                    retry_horizon=config.command_retry_horizon_seconds(),
                    retention=config.command_request_retention_seconds(),
                )
            record, cached, prepared = ingress_requests.preflight_request(
                store,
                ingress_request_id,
                now=now,
                retry_horizon=config.command_retry_horizon_seconds(),
                retention=config.command_request_retention_seconds(),
            )
            prepared = prepared or shell_created
            if changed or prepared:
                # First-seen lifecycle bounds are durable before routing,
                # speech preparation, or AF_UNIX request construction for legacy
                # direct callers. Lane mode already has those exact bytes and
                # bounds in the durable spool, so it folds this shell write into
                # the canonical request commit below.
                if not durable_spool or cached is not None:
                    state.save_state(store)
            if cached is not None:
                return cached
            if isinstance(record.get("request_json"), str):
                return _submit_ingress_command_record(
                    store,
                    record,
                    instant_ack_posted=payload.get("instant_ack_posted") is True,
                    gateway_success_ack_enabled=(
                        payload.get("_gateway_inbound_success_ack") is not False
                    ),
                )
        elif changed:
            state.save_state(store)
        # A write-in is deliberately two-step: only a prior inline-button tap
        # arms plain text. Slash commands (especially /send) bypass this branch
        # unchanged and retain their normal Tendwire instruction semantics.
        plain_text = str(payload.get("text") or "").strip()
        decision_record = decisions.active_decision(
            store, str(payload.get("topic_id") or "")
        )
        if (
            config.remote_decisions_enabled()
            and plain_text
            and not plain_text.startswith("/")
            and isinstance(decision_record, dict)
            and decision_record.get("await_freeform") is True
        ):
            if record is None or not ingress_request_id:
                return {
                    "handled": True,
                    "reply": SAFE_SEND_FAILURE_REPLY,
                    "status": "invalid_request",
                }
            decision_result = decisions.handle_freeform(
                store,
                topic_id=str(payload.get("topic_id") or ""),
                text=plain_text,
                request_id=ingress_request_id,
                telegram=TelegramClient(token=config.telegram_token()),
                tendwire=TendwireClient(),
                chat_id=config.telegram_chat_id(store),
            )
            if decision_result.get("handled"):
                return _local_ingress_outcome(
                    store,
                    record,
                    reason=f"decision write-in {decision_result.get('status') or 'handled'}",
                    reply=str(decision_result.get("reply") or ""),
                )
        _key, entry = state.find_entry_by_thread(store, str(payload.get("topic_id") or ""))
        if entry is None:
            if record is not None:
                return _local_ingress_outcome(
                    store,
                    record,
                    reason="message is not routed",
                    reply=REKEYED_TOPIC_QUARANTINE_REPLY,
                )
            return {"handled": False}
        voice_reply = _voice_mode_reply(store, entry, payload)
        if voice_reply is not None:
            if record is not None:
                return _local_ingress_outcome(
                    store,
                    record,
                    reason="voice mode handled locally",
                    reply=str(voice_reply.get("reply") or ""),
                    handled=voice_reply.get("handled") is not False,
                )
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
                    if record is not None:
                        return _local_ingress_outcome(
                            store,
                            record,
                            reason="unknown target alias",
                            reply=SAFE_SEND_FAILURE_REPLY,
                        )
                    return {"handled": True, "reply": SAFE_SEND_FAILURE_REPLY, "status": "unknown_target_alias"}
        else:
            _reply_key, reply_entry = _worker_entry_from_reply(store, payload)
            if reply_entry is not None:
                target_bot_kind = str(
                    payload.get("target_bot_kind") or ""
                ).strip().lower()
                reply_bot_kind = managed_bot_kind_for_entry(reply_entry)
                if target_bot_kind and reply_bot_kind != target_bot_kind:
                    _kind_key, kind_entry = _worker_entry_from_alias(
                        store, target_bot_kind, entry
                    )
                    if kind_entry is None:
                        if record is not None:
                            return _local_ingress_outcome(
                                store,
                                record,
                                reason="ambiguous reply author target",
                                reply=SAFE_SEND_FAILURE_REPLY,
                            )
                        return {
                            "handled": True,
                            "reply": SAFE_SEND_FAILURE_REPLY,
                            "status": "ambiguous_reply_author_target",
                        }
                    entry = kind_entry
                else:
                    entry = reply_entry
            elif payload.get("reply_to_message_id") and state.find_message_binding(
                store,
                payload.get("reply_to_message_id"),
                topic_id=payload.get("topic_id"),
            ):
                if record is not None:
                    return _local_ingress_outcome(
                        store,
                        record,
                        reason="ambiguous reply target",
                        reply=SAFE_SEND_FAILURE_REPLY,
                    )
                return {"handled": True, "reply": SAFE_SEND_FAILURE_REPLY, "status": "ambiguous_reply_target"}
            else:
                target_bot_kind = str(payload.get("target_bot_kind") or "").strip().lower()
                if target_bot_kind:
                    _kind_key, kind_entry = _worker_entry_from_alias(store, target_bot_kind, entry)
                    if kind_entry is not None:
                        entry = kind_entry
                    else:
                        if record is not None:
                            return _local_ingress_outcome(
                                store,
                                record,
                                reason="unknown target bot",
                                reply=SAFE_SEND_FAILURE_REPLY,
                            )
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
                reply = "🎙️ Got it — the next reply will be spoken."
                if record is not None:
                    return _local_ingress_outcome(
                        store,
                        record,
                        reason="voice reply armed locally",
                        reply=reply,
                    )
                state.save_state(store)
                return {"handled": True, "reply": reply}
        if not text:
            reply = (
                _voice_unavailable_reply(payload)
                if voice_payload
                else "Send a message in this topic or use /send <instruction>."
            )
            if record is not None:
                return _local_ingress_outcome(
                    store,
                    record,
                    reason="empty command handled locally",
                    reply=reply,
                )
            return {"handled": True, "reply": reply}
        # A bare number answering a live captured prompt: validate against the pending's choices and
        # fail closed on stale/out-of-range/custom, else send the digit (the picker's native input).
        number_reply = _pending_number_reply(entry, text)
        if number_reply is not None:
            mapped, error_reply = number_reply
            if error_reply:
                if record is not None:
                    return _local_ingress_outcome(
                        store,
                        record,
                        reason="pending answer rejected locally",
                        reply=error_reply,
                    )
                return {"handled": True, "reply": error_reply}
            text = mapped
        # Reply-to-voice auto-mode (#4): replying to one of this pane's voice notes speaks the next
        # reply back. One-shot flag consumed at delivery (_speak_reply in source_sync).
        if speech.speech_reply_on_voice_reply_enabled() and state.message_is_voice_reply(
            entry, payload.get("reply_to_message_id")
        ):
            entry["speak_next_reply"] = True
        if (
            record is not None
            and record.get("target_owner") is None
            and config.command_response_schema_version() == 3
        ):
            target_identity = _stable_owner_for_entry(entry)
            if target_identity is not None:
                ingress_requests.attach_target_owner(
                    record,
                    target_identity[0],
                    target_identity[1],
                    now=time.time(),
                )
                # Route ownership is durable before the command can mutate
                # Tendwire, including commands whose public target is a space.
                # The canonical request save below includes this owner in the
                # same fsync for durable-spool callers.
                if not durable_spool:
                    state.save_state(store)
            if record.get("target_owner") is None:
                return _local_ingress_outcome(
                    store,
                    record,
                    reason="missing durable target owner",
                    reply=SAFE_SEND_FAILURE_REPLY,
                )
        try:
            request = _command_request(entry, payload, text)
        except ValueError:
            if record is not None:
                return _local_ingress_outcome(
                    store,
                    record,
                    reason="invalid command request",
                    reply=SAFE_SEND_FAILURE_REPLY,
                )
            return {
                "handled": True,
                "reply": SAFE_SEND_FAILURE_REPLY,
                "status": "invalid_request",
            }
        if record is None:
            return {
                "handled": True,
                "reply": SAFE_SEND_FAILURE_REPLY,
                "status": "invalid_request",
            }
        request_json = ingress_requests.canonical_request_json(request)
        try:
            attached = ingress_requests.attach_request_json(
                record, request_json, now=time.time()
            )
        except ValueError:
            outcome = ingress_requests.quarantine_request(
                record, "conflicting ingress request", now=time.time()
            )
            state.save_state(store)
            return outcome
        if attached:
            # Canonical JSON is fsynced before Tendwire can observe the request,
            # and every retry reads only this stored string.
            state.save_state(store)
        _ingress_timing_log(
            "canonical_commit",
            ingress_request_id,
            created_at=record.get("created_at"),
            detail="new" if attached else "cached",
        )
        return _submit_ingress_command_record(
            store,
            record,
            instant_ack_posted=payload.get("instant_ack_posted") is True,
            gateway_success_ack_enabled=(
                payload.get("_gateway_inbound_success_ack") is not False
            ),
        )


def callback_reply(_payload: dict[str, Any]) -> dict[str, Any]:
    return {"handled": True, "reply": "This source-only Herdres branch does not use Telegram callbacks."}


def _sync_pass(*, with_outbox: bool = True) -> dict[str, Any]:
    with state.state_lock(phase="sync_pass.load"):
        with state.lock_phase("sync_pass.load"):
            store = state.load_state()

        def checkpoint() -> None:
            if not state.lock_held():
                raise RuntimeError("state checkpoint requires the held state lock")
            with state.lock_phase("sync.checkpoint"):
                state.save_state(store)

        with state.lock_phase("sync_once"):
            result = sync_once(
                store,
                _runtime(
                    dry_run=False,
                    with_outbox=with_outbox,
                    checkpoint=checkpoint,
                ),
            )
        if result.get("changed"):
            with state.lock_phase("sync_pass.final_save"):
                state.save_state(store)
    return result


def _outbound_pass() -> dict[str, Any]:
    """Drain connector work without snapshot, turn, pending, or pane scans."""

    with state.state_lock(phase="outbound_pass.load"):
        with state.lock_phase("outbound_pass.load"):
            store = state.load_state()

        def checkpoint() -> None:
            if not state.lock_held():
                raise RuntimeError("outbound checkpoint requires the held state lock")
            with state.lock_phase("outbound.checkpoint"):
                state.save_state(store)

        with state.lock_phase("outbound.drain"):
            result = drain_outbound_once(
                store,
                _runtime(
                    dry_run=False,
                    with_outbox=True,
                    checkpoint=checkpoint,
                ),
                chat_id=config.telegram_chat_id(store),
            )
        if result.get("changed"):
            with state.lock_phase("outbound_pass.final_save"):
                state.save_state(store)
    return result


def _connector_poll_loop(stop: threading.Event) -> None:
    cadence = config.tendwire_connector_poll_seconds()
    while not stop.is_set():
        started = time.monotonic()
        try:
            _outbound_pass()
        except Exception as exc:  # noqa: BLE001 - retain the bounded poll loop
            print(
                json.dumps(
                    {
                        "ok": False,
                        "status": "outbound_pass_failed",
                        "error": sanitize_text(str(exc), 300),
                    }
                ),
                flush=True,
            )
        remaining = cadence - (time.monotonic() - started)
        stop.wait(max(0.0, remaining))


def cmd_sync(args: argparse.Namespace) -> int:
    config.load_env_file()
    config.require_source_mode()
    interval = float(getattr(args, "loop", 0) or 0)
    if interval <= 0:
        return _json(_sync_pass())
    import time as _time

    stop = threading.Event()
    poller = threading.Thread(
        target=_connector_poll_loop,
        args=(stop,),
        name="herdres-connector-poll",
        daemon=True,
    )
    poller.start()
    try:
        while True:
            started = _time.monotonic()
            try:
                result = _sync_pass(with_outbox=False)
                if result.get("ok") is not True:
                    print(json.dumps(result), flush=True)
            except Exception as exc:  # noqa: BLE001 - survive transient failures
                print(
                    json.dumps(
                        {
                            "ok": False,
                            "status": "sync_pass_failed",
                            "error": sanitize_text(str(exc), 300),
                        }
                    ),
                    flush=True,
                )
            _time.sleep(max(0.5, interval - (_time.monotonic() - started)))
    finally:
        stop.set()
        poller.join(timeout=max(1.0, config.tendwire_connector_poll_seconds() + 0.5))


def cmd_command(_args: argparse.Namespace) -> int:
    config.load_env_file()
    try:
        payload = json.loads(sys.stdin.read() or "{}")
    except json.JSONDecodeError:
        payload = {}
    result = command_reply(payload if isinstance(payload, dict) else {})
    wire_result = (
        result.to_wire_dict()
        if isinstance(result, ingress_requests.IngressResult)
        else result
    )
    return _json(wire_result)


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


def cmd_ingress_status(_args: argparse.Namespace) -> int:
    """Print the operator-facing delivery truth without prompt contents."""

    config.load_env_file()
    store = state.load_state()
    rows = ingress_requests.operator_status_rows(store, now=time.time())
    return _json(
        {
            "ok": True,
            "schema_version": 1,
            "attention_required": sum(
                1 for row in rows if row["operator_attention_required"]
            ),
            "requests": rows,
        }
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="herdres")
    sub = parser.add_subparsers(dest="cmd", required=True)
    sync_parser = sub.add_parser("sync")
    sync_parser.add_argument("--loop", type=float, default=0.0, help="run continuously, one pass every N seconds")
    sync_parser.set_defaults(func=cmd_sync)
    sub.add_parser("command").set_defaults(func=cmd_command)
    sub.add_parser("callback").set_defaults(func=cmd_callback)
    sub.add_parser("doctor").set_defaults(func=cmd_doctor)
    sub.add_parser("ingress-status").set_defaults(func=cmd_ingress_status)
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
