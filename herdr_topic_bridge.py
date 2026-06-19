"""Deterministic Herdr pane-topic command bridge for Telegram.

The outbound reconciler owns the state file. Hermes only reads that state and
delegates mapped topic commands to the external stdlib script. This avoids a
second Telegram getUpdates consumer while keeping routine pane control token-free.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

DEFAULT_STATE = Path.home() / ".local/share/herdres/state.json"
DEFAULT_SCRIPT = Path.home() / ".local/bin/herdres"
GENERAL_THREAD_ID = "1"
AMBIGUOUS_PANE_THREAD_REPLY = "Reply inside a pane thread so I know which Herdr pane to control."


def _state_path() -> Path:
    return Path(os.getenv("HERDR_TELEGRAM_TOPICS_STATE", str(DEFAULT_STATE))).expanduser()


def _script_path() -> Path:
    return Path(os.getenv("HERDR_TELEGRAM_TOPICS_SCRIPT", str(DEFAULT_SCRIPT))).expanduser()


def _stand_down() -> bool:
    # When the standalone herdres gateway owns inbound forwarding, Hermes should
    # stop forwarding mapped-pane-topic traffic (to avoid double-handling) while
    # still treating those topics as "handled" so it never chats inside them.
    return os.getenv("HERDRES_BRIDGE_DISABLED", "").strip().lower() in {"1", "true", "yes", "on"}


def _load_state() -> dict[str, Any] | None:
    path = _state_path()
    if not path.exists():
        return None
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        logger.warning("Herdr topic bridge state is unreadable", exc_info=True)
        return None
    if not isinstance(data, dict) or data.get("version") != 1 or not data.get("enabled", True):
        return None
    return data


def _message_thread_id(message: Any) -> str | None:
    thread_id = getattr(message, "message_thread_id", None)
    if thread_id is None:
        chat = getattr(message, "chat", None)
        if getattr(chat, "is_forum", False):
            return GENERAL_THREAD_ID
        return None
    return str(thread_id)


def _topic_space_entry(state: dict[str, Any], chat_id: str, thread_id: str | None) -> tuple[str, dict[str, Any]] | None:
    telegram = state.get("telegram") or {}
    if chat_id != str(telegram.get("chat_id") or ""):
        return None
    if not thread_id or thread_id == str(telegram.get("general_thread_id", GENERAL_THREAD_ID)):
        return None
    for key, space in (state.get("spaces") or {}).items():
        if isinstance(space, dict) and str(space.get("topic_id") or "") == str(thread_id):
            return str(key), space
    return None


def _live_entries_for_space(state: dict[str, Any], space: dict[str, Any]) -> list[tuple[str, dict[str, Any]]]:
    panes = state.get("panes") if isinstance(state.get("panes"), dict) else {}
    entries: list[tuple[str, dict[str, Any]]] = []
    for pane_key in space.get("pane_keys") or []:
        key = str(pane_key)
        entry = panes.get(key)
        if isinstance(entry, dict) and str(entry.get("last_known_status") or "").lower() != "closed":
            entries.append((key, entry))
    return entries


def _route_message_entry(
    state: dict[str, Any],
    chat_id: str,
    thread_id: str | None,
    message_id: str | int | None,
) -> tuple[str, dict[str, Any]] | None:
    if not thread_id:
        return None
    message_key = str(message_id or "").strip()
    if not message_key:
        return None
    mapped_space = _topic_space_entry(state, chat_id, thread_id)
    if not mapped_space:
        return None
    _space_key, space = mapped_space
    panes = state.get("panes") if isinstance(state.get("panes"), dict) else {}
    routes = space.get("message_routes") if isinstance(space.get("message_routes"), dict) else {}
    routed_key = str(routes.get(message_key) or "")
    routed_entry = panes.get(routed_key)
    if routed_key and isinstance(routed_entry, dict):
        return routed_key, routed_entry
    for pane_key, entry in _live_entries_for_space(state, space):
        if str(entry.get("pane_root_message_id") or "") == message_key:
            return pane_key, entry
    return None


def _resolve_mapped_entry(
    state: dict[str, Any],
    chat_id: str,
    thread_id: str | None,
    *,
    message_id: str | int | None = None,
    reply_to_message_id: str | int | None = None,
    prefer_message_id: bool = False,
) -> tuple[str, dict[str, Any]] | None:
    telegram = state.get("telegram") if isinstance(state.get("telegram"), dict) else {}
    configured_chat = str(telegram.get("chat_id") or "")
    if configured_chat and str(chat_id) != configured_chat:
        return None
    if prefer_message_id:
        routed = _route_message_entry(state, chat_id, thread_id, message_id)
        if routed:
            return routed
    routed = _route_message_entry(state, chat_id, thread_id, reply_to_message_id)
    if routed:
        return routed
    mapped_space = _topic_space_entry(state, chat_id, thread_id)
    if mapped_space:
        _space_key, space = mapped_space
        live_entries = _live_entries_for_space(state, space)
        if len(live_entries) == 1:
            return live_entries[0]
        return None
    for entry in (state.get("panes") or {}).values():
        if isinstance(entry, dict) and str(entry.get("topic_id") or "") == thread_id:
            return str(entry.get("pane_key") or ""), entry
    return None


def _mapped_topic_entry(state: dict[str, Any], chat_id: str, thread_id: str | None) -> dict[str, Any] | None:
    resolved = _resolve_mapped_entry(state, chat_id, thread_id)
    if not resolved:
        return None
    _pane_key, entry = resolved
    return entry


def _mapped_entry(state: dict[str, Any], message: Any) -> dict[str, Any] | None:
    chat = getattr(message, "chat", None)
    if chat is None:
        return None
    chat_id = str(getattr(chat, "id", ""))
    reply_to = getattr(message, "reply_to_message", None)
    resolved = _resolve_mapped_entry(
        state,
        chat_id,
        _message_thread_id(message),
        message_id=getattr(message, "message_id", ""),
        reply_to_message_id=getattr(reply_to, "message_id", "") if reply_to else "",
    )
    if not resolved:
        return None
    _pane_key, entry = resolved
    return entry


def _is_forwarded(message: Any) -> bool:
    return any(
        getattr(message, attr, None) is not None
        for attr in ("forward_origin", "forward_from", "forward_sender_name", "forward_date")
    )


async def _run_command_script(payload: dict[str, Any], mode: str = "command") -> dict[str, Any]:
    script = _script_path()
    if not script.exists():
        return {"handled": True, "reply": "Herdr topic command script is missing."}
    env = os.environ.copy()
    env["HERDR_TELEGRAM_TOPICS_STATE"] = str(_state_path())
    try:
        proc = await asyncio.create_subprocess_exec(
            str(script),
            mode,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            env=env,
        )
    except Exception:
        logger.warning("Failed to start Herdr topic command script", exc_info=True)
        return {"handled": True, "reply": "Herdr topic command could not start. Check host logs."}
    try:
        stdout, stderr = await asyncio.wait_for(
            proc.communicate(json.dumps(payload).encode("utf-8")),
            timeout=25,
        )
    except asyncio.TimeoutError:
        proc.kill()
        try:
            await proc.wait()
        except Exception:
            pass
        logger.warning("Herdr topic command timed out for topic %s", payload.get("topic_id"))
        return {"handled": True, "reply": "Herdr topic command timed out before completion."}
    if proc.returncode != 0:
        detail = (stderr or stdout).decode("utf-8", errors="replace").strip()
        logger.warning("Herdr topic command failed: %s", detail[:500])
        return {"handled": True, "reply": "Herdr topic command failed. Check host logs."}
    try:
        return json.loads(stdout.decode("utf-8"))
    except Exception:
        logger.warning("Herdr topic command returned invalid JSON")
        return {"handled": True, "reply": "Herdr topic command returned invalid output."}


def _attachment_payload(message: Any) -> dict[str, Any] | None:
    """Extract one attachment (document preferred, else largest photo) as a
    JSON-safe dict via getattr only. No Telegram calls, no token. Returns None
    when there is no usable attachment; never raises on a malformed object."""
    try:
        document = getattr(message, "document", None)
        if document is not None:
            file_id = str(getattr(document, "file_id", "") or "")
            if file_id:
                return {
                    "kind": "document",
                    "file_id": file_id,
                    "file_name": str(getattr(document, "file_name", "") or ""),
                    "mime_type": str(getattr(document, "mime_type", "") or ""),
                    "file_size": int(getattr(document, "file_size", 0) or 0),
                }
        photo = getattr(message, "photo", None)
        if photo:
            largest = photo[-1]
            file_id = str(getattr(largest, "file_id", "") or "")
            if file_id:
                return {
                    "kind": "photo",
                    "file_id": file_id,
                    "file_name": "",
                    "mime_type": "image/jpeg",
                    "file_size": int(getattr(largest, "file_size", 0) or 0),
                }
    except Exception:
        return None
    return None


async def maybe_handle_herdr_topic_message(adapter: Any, message: Any) -> bool:
    """Handle a Telegram message if it belongs to a mapped Herdr pane topic."""
    text = getattr(message, "text", None)
    attachment = _attachment_payload(message)
    # Gate on text or a supported attachment only: a caption rides with its
    # attachment, but a caption on unsupported media (video/voice/...) must fall
    # through so the host bot handles it normally instead of being swallowed.
    if not (text or attachment):
        return False
    state = _load_state()
    if not state:
        return False
    chat = getattr(message, "chat", None)
    chat_id = str(getattr(chat, "id", ""))
    thread_id = _message_thread_id(message)
    reply_to = getattr(message, "reply_to_message", None)
    user = getattr(message, "from_user", None)
    user_id = str(getattr(user, "id", "") if user else "")
    from_bot = bool(getattr(user, "is_bot", False)) if user else True
    resolved = _resolve_mapped_entry(
        state,
        chat_id,
        thread_id,
        message_id=getattr(message, "message_id", ""),
        reply_to_message_id=getattr(reply_to, "message_id", "") if reply_to else "",
    )
    if not resolved:
        if _topic_space_entry(state, chat_id, thread_id):
            if _stand_down():
                return True
            owners = {str(x) for x in (state.get("telegram") or {}).get("owner_user_ids", [])}
            if from_bot or (owners and user_id not in owners):
                return True
            metadata = {"thread_id": thread_id} if thread_id else None
            try:
                await adapter._send_with_retry(
                    chat_id=chat_id,
                    content=AMBIGUOUS_PANE_THREAD_REPLY,
                    reply_to=str(getattr(message, "message_id", "")),
                    metadata=metadata,
                )
            except Exception:
                logger.warning("Failed to send Herdr ambiguous-topic hint", exc_info=True)
            return True
        return False
    pane_key, entry = resolved
    if _stand_down():
        return True

    # Cheap owner pre-filter so non-owner / bot traffic in a mapped topic does
    # not spawn the herdres subprocess (it would just be dropped there anyway).
    # command_reply re-applies the authoritative gate. Only filter when owners
    # are configured in state; otherwise defer entirely to command_reply.
    owners = {str(x) for x in (state.get("telegram") or {}).get("owner_user_ids", [])}
    if from_bot or (owners and user_id not in owners):
        return True
    payload = {
        "chat_id": chat_id,
        "topic_id": thread_id,
        "pane_key": pane_key,
        "message_id": str(getattr(message, "message_id", "")),
        "reply_to_message_id": str(getattr(reply_to, "message_id", "") if reply_to else ""),
        "user_id": user_id,
        "from_bot": from_bot,
        "forwarded": _is_forwarded(message),
        "edited": bool(getattr(message, "edit_date", None)),
        "text": str(getattr(message, "text", "") or ""),
        "caption": str(getattr(message, "caption", "") or ""),
        "attachment": attachment,
    }
    try:
        result = await _run_command_script(payload)
    except Exception:
        logger.warning("Unhandled Herdr topic command bridge failure", exc_info=True)
        result = {"handled": True, "reply": "Herdr topic command failed before completion. Check host logs."}
    if not result.get("handled", True):
        logger.warning("Mapped Herdr topic command returned unhandled for topic %s", payload.get("topic_id"))
        return True
    reply = str(result.get("reply") or "").strip()
    if reply:
        metadata = {"thread_id": payload["topic_id"]} if payload.get("topic_id") else None
        try:
            await adapter._send_with_retry(
                chat_id=payload["chat_id"],
                content=reply,
                reply_to=payload["message_id"],
                metadata=metadata,
            )
        except Exception:
            logger.warning("Failed to send Herdr topic command reply", exc_info=True)
    return True


async def maybe_handle_herdr_topic_callback(adapter: Any, query: Any) -> bool:
    """Handle inline Herdr pane-topic callbacks without touching other callbacks."""
    data = str(getattr(query, "data", "") or "")
    if not data.startswith("herdr:"):
        return False
    state = _load_state()
    if not state:
        return False
    message = getattr(query, "message", None)
    chat = getattr(message, "chat", None)
    if not message or not chat:
        return False
    chat_id = str(getattr(chat, "id", getattr(message, "chat_id", "")))
    thread_id = _message_thread_id(message)
    resolved = _resolve_mapped_entry(
        state,
        chat_id,
        thread_id,
        message_id=getattr(message, "message_id", ""),
        prefer_message_id=True,
    )
    if not resolved:
        return False
    if _stand_down():
        return True
    pane_key, _entry = resolved

    user = getattr(query, "from_user", None)
    payload = {
        "chat_id": chat_id,
        "topic_id": thread_id,
        "pane_key": pane_key,
        "message_id": str(getattr(message, "message_id", "")),
        "user_id": str(getattr(user, "id", "") if user else ""),
        "data": data,
    }
    try:
        result = await _run_command_script(payload, mode="callback")
    except Exception:
        logger.warning("Unhandled Herdr topic callback bridge failure", exc_info=True)
        result = {"handled": True, "answer": "Herdr callback failed.", "show_alert": True}
    if not result.get("handled", True):
        logger.warning("Mapped Herdr topic callback returned unhandled for topic %s", payload.get("topic_id"))
        return True
    answer = str(result.get("answer") or "").strip()
    try:
        await query.answer(text=answer or None, show_alert=bool(result.get("show_alert")))
    except Exception:
        logger.warning("Failed to answer Herdr topic callback", exc_info=True)
    return True
