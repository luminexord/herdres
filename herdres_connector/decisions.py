"""Remote Telegram controls for Tendwire's structured Claude decisions.

The connector only acts on Tendwire's neutral, single-question decision
contract.  Anything ambiguous stays visible through the ordinary pending
notice but deliberately gets no buttons: a wrong remote answer is worse than
requiring the owner to finish the prompt at the desk.
"""

from __future__ import annotations

import hashlib
from typing import Any

from . import config, state
from .ingress_identity import validate_request_id
from .safe import compact_ws, html_escape, sanitize_text, short_hash
from .telegram_delivery import TelegramClient
from .tendwire_client import TendwireClient


CALLBACK_PREFIX = "hdec:"
CUSTOM_TOKEN = "custom"
SUBMIT_TOKEN = "__submit__"
SUPPORTED_KINDS = frozenset({"single", "multi", "plan"})
RESERVED_OPTION_REFS = frozenset({CUSTOM_TOKEN, SUBMIT_TOKEN})
CALLBACK_DATA_LIMIT = 64
ANSWER_IN_PROGRESS_REPLY = (
    "That prompt is being answered right now — try again in a moment."
)


def _ref56(decision_id: str) -> str:
    """Return a deterministic 56-bit handle, keeping callback data private and short."""

    return hashlib.sha256(decision_id.encode("utf-8")).hexdigest()[:14]


def _callback_data(decision_id: str, option_ref: str) -> str | None:
    value = f"{CALLBACK_PREFIX}{_ref56(decision_id)}:{option_ref}"
    return value if len(value.encode("utf-8")) <= CALLBACK_DATA_LIMIT else None


def _decision_blob(item: dict[str, Any]) -> dict[str, Any] | None:
    meta = item.get("meta") if isinstance(item.get("meta"), dict) else {}
    decision = meta.get("decision") if isinstance(meta, dict) else None
    return decision if isinstance(decision, dict) else None


def _pending_items(payload: dict[str, Any]) -> list[dict[str, Any]]:
    rows = payload.get("pending_interactions", payload.get("pending", []))
    return [row for row in rows if isinstance(row, dict)] if isinstance(rows, list) else []


def _normalize_options(value: Any, decision_id: str) -> list[dict[str, str]] | None:
    if not isinstance(value, list) or not value:
        return None
    options: list[dict[str, str]] = []
    seen: set[str] = set()
    for raw in value:
        if not isinstance(raw, dict):
            return None
        option_ref = raw.get("ref")
        label = compact_ws(raw.get("label"), 80)
        if (
            not isinstance(option_ref, str)
            or not option_ref
            or sanitize_text(option_ref, 160) != option_ref
            or option_ref in seen
            or option_ref in RESERVED_OPTION_REFS
            or not label
            or _callback_data(decision_id, option_ref) is None
        ):
            return None
        seen.add(option_ref)
        options.append({"ref": option_ref, "label": label})
    return options


def resolve_decisions(
    store: dict[str, Any], pending_payload: dict[str, Any]
) -> list[dict[str, Any]]:
    """Join safe decision blobs to one uniquely routable worker topic.

    A shared topic can hold only one active keyboard record.  If Tendwire ever
    reports two decisions for the same topic, both are skipped rather than
    letting arrival order choose which worker Telegram controls.
    """

    candidates: list[dict[str, Any]] = []
    topic_counts: dict[str, int] = {}
    for item in _pending_items(pending_payload):
        decision = _decision_blob(item)
        if decision is None:
            continue
        kind = str(decision.get("kind") or "").strip().lower()
        question_count = decision.get("question_count", 1)
        if (
            kind not in SUPPORTED_KINDS
            or isinstance(question_count, bool)
            or not isinstance(question_count, int)
            or question_count > 1
            or question_count < 1
        ):
            continue
        worker_id = compact_ws(item.get("worker_id"), 160)
        entry_key = state.find_entry_key_by_worker(store, worker_id)
        if entry_key is None:
            continue
        entry = state.source_worker_entries(store).get(entry_key)
        topic_id = compact_ws((entry or {}).get("topic_id"), 80)
        if not topic_id:
            continue
        decision_id = decision.get("decision_ref")
        prompt = sanitize_text(decision.get("prompt"), 12000).strip()
        if (
            not isinstance(decision_id, str)
            or not decision_id
            or sanitize_text(decision_id, 4096) != decision_id
            or not prompt
        ):
            continue
        options = _normalize_options(decision.get("options"), decision_id)
        if options is None:
            continue
        content_hash = short_hash(
            {
                "kind": kind,
                "prompt": prompt,
                "options": options,
                "multi_select": decision.get("multi_select") is True,
                "question_count": question_count,
            },
            24,
        )
        candidates.append(
            {
                "decision_id": decision_id,
                "worker_id": worker_id,
                "entry_key": entry_key,
                "topic_id": topic_id,
                "kind": kind,
                "prompt": prompt,
                "options": options,
                "content_hash": content_hash,
            }
        )
        topic_counts[topic_id] = topic_counts.get(topic_id, 0) + 1
    return [row for row in candidates if topic_counts[row["topic_id"]] == 1]


def _active_records(
    store: dict[str, Any], *, create: bool
) -> dict[str, dict[str, Any]]:
    decisions = store.get("decisions")
    if not isinstance(decisions, dict):
        if not create:
            return {}
        decisions = {}
        store["decisions"] = decisions
    active = decisions.get("active")
    if not isinstance(active, dict):
        if not create:
            return {}
        active = {}
        decisions["active"] = active
    return active


def active_decision(store: dict[str, Any], topic_id: str | int) -> dict[str, Any] | None:
    record = _active_records(store, create=False).get(str(topic_id))
    return record if isinstance(record, dict) else None


def needs_sync(store: dict[str, Any], pending_payload: dict[str, Any]) -> bool:
    """Return whether a pass can post or retract a decision keyboard."""

    return bool(_active_records(store, create=False)) or any(
        _decision_blob(item) is not None for item in _pending_items(pending_payload)
    )


def render_decision(record: dict[str, Any]) -> str:
    labels = {
        "single": "Choose one answer",
        "multi": "Choose one or more answers",
        "plan": "Review the plan",
    }
    label = labels.get(str(record.get("kind") or ""), "Decision required")
    return (
        f"<b>{html_escape(label, 80)}</b>\n"
        f"{html_escape(record.get('prompt'), 12000)}"
    )


def inline_keyboard(record: dict[str, Any]) -> dict[str, list[list[dict[str, str]]]]:
    """Build Telegram's InlineKeyboardMarkup JSON object for one active record."""

    selected = {
        str(value)
        for value in record.get("selected", [])
        if isinstance(value, str)
    }
    kind = str(record.get("kind") or "")
    decision_id = str(record.get("decision_id") or "")
    rows: list[list[dict[str, str]]] = []
    for option in record.get("options", []):
        if not isinstance(option, dict):
            continue
        option_ref = str(option.get("ref") or "")
        callback_data = _callback_data(decision_id, option_ref)
        if callback_data is None:
            continue
        marker = ""
        if kind == "multi":
            marker = "✅ " if option_ref in selected else "▫️ "
        rows.append(
            [
                {
                    "text": marker + compact_ws(option.get("label"), 80),
                    "callback_data": callback_data,
                }
            ]
        )
    if kind == "single":
        rows.append(
            [
                {
                    "text": "✍️ Write a different answer",
                    "callback_data": str(_callback_data(decision_id, CUSTOM_TOKEN)),
                }
            ]
        )
    elif kind == "multi":
        rows.append(
            [
                {
                    "text": "✅ Submit",
                    "callback_data": str(_callback_data(decision_id, SUBMIT_TOKEN)),
                }
            ]
        )
    return {"inline_keyboard": rows}


def _retract(
    telegram: TelegramClient,
    chat_id: str,
    topic_id: str,
    record: dict[str, Any],
    note: str,
) -> None:
    message_id = str(record.get("message_id") or "")
    if not message_id:
        return
    telegram.edit_message_reply_markup(
        chat_id,
        message_id,
        {"inline_keyboard": []},
    )
    telegram.edit_message(
        chat_id,
        message_id,
        f"{render_decision(record)}\n\n{html_escape(note, 240)}",
    )


def sync_decisions(
    store: dict[str, Any],
    pending_payload: dict[str, Any],
    telegram: TelegramClient,
    *,
    chat_id: str,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Reconcile active inline keyboards with one already-fetched pending list."""

    if not config.remote_decisions_enabled():
        return {"enabled": False, "changed": False, "posted": 0, "retracted": 0}
    resolved = resolve_decisions(store, pending_payload)
    if dry_run:
        return {
            "enabled": True,
            "changed": False,
            "posted": 0,
            "retracted": 0,
            "resolved": len(resolved),
            "dry_run": True,
        }
    if not resolved and not _active_records(store, create=False):
        return {
            "enabled": True,
            "changed": False,
            "posted": 0,
            "retracted": 0,
            "resolved": 0,
        }
    active = _active_records(store, create=True)
    desired = {row["topic_id"]: row for row in resolved}
    raw_pending_ids = {
        blob.get("decision_ref")
        for item in _pending_items(pending_payload)
        if (blob := _decision_blob(item)) is not None
        and isinstance(blob.get("decision_ref"), str)
    }
    raw_pending_ids.discard("")
    posted = 0
    retracted = 0
    changed = False

    for topic_id, raw_record in list(active.items()):
        if not isinstance(raw_record, dict):
            active.pop(topic_id, None)
            changed = True
            continue
        wanted = desired.get(topic_id)
        if (
            wanted is not None
            and raw_record.get("decision_id") == wanted["decision_id"]
            and raw_record.get("content_hash") == wanted["content_hash"]
        ):
            desired.pop(topic_id, None)
            continue
        note = (
            "⚠️ This prompt must be answered at the desk."
            if str(raw_record.get("decision_id") or "") in raw_pending_ids
            else "✅ Answered."
        )
        _retract(telegram, chat_id, topic_id, raw_record, note)
        active.pop(topic_id, None)
        retracted += 1
        changed = True

    for topic_id, candidate in desired.items():
        record = {
            "decision_id": candidate["decision_id"],
            "worker_id": candidate["worker_id"],
            "entry_key": candidate["entry_key"],
            "kind": candidate["kind"],
            "prompt": candidate["prompt"],
            "options": candidate["options"],
            "message_id": "",
            "selected": [],
            "await_freeform": False,
            "content_hash": candidate["content_hash"],
        }
        sent = telegram.send_message(
            chat_id,
            render_decision(record),
            thread_id=topic_id,
            notify=True,
            reply_markup=inline_keyboard(record),
        )
        if not sent.get("ok"):
            continue
        record["message_id"] = str(
            sent.get("reply_markup_message_id") or sent.get("message_id") or ""
        )
        active[topic_id] = record
        posted += 1
        changed = True
    return {
        "enabled": True,
        "changed": changed,
        "posted": posted,
        "retracted": retracted,
        "resolved": len(resolved),
    }


def _parse_callback(value: Any) -> tuple[str, str] | None:
    data = str(value or "")
    if not data.startswith(CALLBACK_PREFIX):
        return None
    parts = data.split(":", 2)
    if len(parts) != 3 or not parts[1] or not parts[2]:
        return None
    return parts[1], parts[2]


def _failure_text(result: dict[str, Any]) -> str:
    status = compact_ws(result.get("status") or "answer_failed", 80)
    return f"⚠️ Could not answer that prompt ({status}). Try again or answer at the desk."


def _send_failure(
    telegram: TelegramClient,
    chat_id: str,
    topic_id: str,
    record: dict[str, Any],
    text: str,
) -> None:
    telegram.send_message(
        chat_id,
        html_escape(text, 300),
        thread_id=topic_id,
        reply_to_message_id=str(record.get("message_id") or "") or None,
        notify=True,
    )


def _answer_request(
    record: dict[str, Any], request_id: str, selection: dict[str, Any]
) -> dict[str, Any]:
    return {
        "schema_version": 1,
        "action": "answer_decision",
        "request_id": validate_request_id(request_id),
        # Explicit, like send_instruction: Tendwire's mutating actions default to
        # dry-run, so a live answer must say so rather than rely on a flipped default.
        "dry_run": False,
        "target": {"worker_id": str(record.get("worker_id") or "")},
        "params": {
            "decision_ref": str(record.get("decision_id") or ""),
            "selection": selection,
        },
    }


def _submit(
    store: dict[str, Any],
    topic_id: str,
    record: dict[str, Any],
    selection: dict[str, Any],
    *,
    request_id: str,
    telegram: TelegramClient,
    tendwire: TendwireClient,
    chat_id: str,
    callback: bool,
) -> dict[str, Any]:
    try:
        result = tendwire.command(_answer_request(record, request_id, selection))
    except Exception as exc:  # noqa: BLE001 - the keyboard must survive every submit failure
        result = {
            "ok": False,
            "status": "connector_error",
            "error": sanitize_text(str(exc), 160),
        }
    active = _active_records(store, create=True)
    if result.get("ok") is True and result.get("status") == "accepted":
        _retract(telegram, chat_id, topic_id, record, "✅ Answered.")
        active.pop(topic_id, None)
        return {
            "handled": True,
            "changed": True,
            "toast": "Answered.",
            "reply": "",
            "status": "accepted",
        }
    status = str(result.get("status") or "answer_failed")
    if status == "answer_in_progress":
        if callback:
            _send_failure(
                telegram,
                chat_id,
                topic_id,
                record,
                ANSWER_IN_PROGRESS_REPLY,
            )
        return {
            "handled": True,
            "changed": False,
            "toast": ANSWER_IN_PROGRESS_REPLY,
            "reply": ANSWER_IN_PROGRESS_REPLY,
            "status": status,
        }
    if status == "decision_not_pending":
        note = "⚠️ That prompt is no longer pending (answered at the desk?)"
        _retract(telegram, chat_id, topic_id, record, note)
        active.pop(topic_id, None)
        return {
            "handled": True,
            "changed": True,
            "toast": "Prompt is no longer pending.",
            "reply": "" if callback else note,
            "status": status,
        }
    error = _failure_text(result)
    if callback:
        _send_failure(telegram, chat_id, topic_id, record, error)
    return {
        "handled": True,
        "changed": False,
        "toast": "Could not answer; try again.",
        "reply": error,
        "status": status,
    }


def handle_callback(
    store: dict[str, Any],
    *,
    callback_data: str,
    topic_id: str,
    chat_id: str,
    request_id: str,
    telegram: TelegramClient,
    tendwire: TendwireClient,
) -> dict[str, Any]:
    """Select, toggle, submit, or arm write-in for one Telegram callback."""

    if not config.remote_decisions_enabled():
        return {
            "handled": False,
            "changed": False,
            "toast": "Remote decisions are disabled.",
            "reply": "",
            "status": "disabled",
        }
    parsed = _parse_callback(callback_data)
    if parsed is None:
        return {
            "handled": False,
            "changed": False,
            "toast": "Unknown action.",
            "reply": "",
            "status": "unknown_callback",
        }
    callback_ref, option_ref = parsed
    record = active_decision(store, topic_id)
    if record is None or callback_ref != _ref56(str(record.get("decision_id") or "")):
        return {
            "handled": True,
            "changed": False,
            "toast": "That button has expired.",
            "reply": "",
            "status": "expired",
        }
    option_refs = {
        str(option.get("ref") or "")
        for option in record.get("options", [])
        if isinstance(option, dict)
    }
    kind = str(record.get("kind") or "")
    if kind == "single" and option_ref == CUSTOM_TOKEN:
        record["await_freeform"] = True
        return {
            "handled": True,
            "changed": True,
            "toast": "Write your answer in this topic.",
            "reply": "",
            "status": "await_freeform",
        }
    if kind == "multi" and option_ref in option_refs:
        selected = [
            str(value)
            for value in record.get("selected", [])
            if isinstance(value, str) and value in option_refs
        ]
        if option_ref in selected:
            selected.remove(option_ref)
            toast = "Choice cleared."
        else:
            selected.append(option_ref)
            toast = "Choice selected."
        preview = dict(record)
        preview["selected"] = selected
        edited = telegram.edit_message_reply_markup(
            chat_id,
            str(record.get("message_id") or ""),
            inline_keyboard(preview),
        )
        if not edited.get("ok"):
            return {
                "handled": True,
                "changed": False,
                "toast": "Could not update choices.",
                "reply": "",
                "status": "telegram_edit_failed",
            }
        record["selected"] = selected
        return {
            "handled": True,
            "changed": True,
            "toast": toast,
            "reply": "",
            "status": "toggled",
        }
    if kind == "multi" and option_ref == SUBMIT_TOKEN:
        selection = {
            "option_refs": [
                str(value)
                for value in record.get("selected", [])
                if isinstance(value, str) and value in option_refs
            ]
        }
        return _submit(
            store,
            topic_id,
            record,
            selection,
            request_id=request_id,
            telegram=telegram,
            tendwire=tendwire,
            chat_id=chat_id,
            callback=True,
        )
    if kind in {"single", "plan"} and option_ref in option_refs:
        return _submit(
            store,
            topic_id,
            record,
            {"option_refs": [option_ref]},
            request_id=request_id,
            telegram=telegram,
            tendwire=tendwire,
            chat_id=chat_id,
            callback=True,
        )
    return {
        "handled": True,
        "changed": False,
        "toast": "That choice is no longer available.",
        "reply": "",
        "status": "invalid_selection",
    }


def handle_freeform(
    store: dict[str, Any],
    *,
    topic_id: str,
    text: str,
    request_id: str,
    telegram: TelegramClient,
    tendwire: TendwireClient,
    chat_id: str,
) -> dict[str, Any]:
    """Submit plain text only after the owner explicitly armed the write-in path."""

    if not config.remote_decisions_enabled():
        return {"handled": False, "changed": False, "reply": "", "status": "disabled"}
    record = active_decision(store, topic_id)
    if record is None or record.get("await_freeform") is not True:
        return {"handled": False, "changed": False, "reply": "", "status": "not_armed"}
    answer = sanitize_text(text, 12000).strip()
    if not answer:
        return {
            "handled": True,
            "changed": False,
            "reply": "Write a non-empty answer, or use the buttons.",
            "status": "invalid_selection",
        }
    return _submit(
        store,
        str(topic_id),
        record,
        {"text": answer},
        request_id=request_id,
        telegram=telegram,
        tendwire=tendwire,
        chat_id=chat_id,
        callback=False,
    )
