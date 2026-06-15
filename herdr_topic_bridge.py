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

DEFAULT_STATE = Path("/home/smith/.local/share/herdr-telegram-topics/state.json")
DEFAULT_SCRIPT = Path("/home/smith/.local/bin/herdr_telegram_topics.py")
GENERAL_THREAD_ID = "1"


def _state_path() -> Path:
    return Path(os.getenv("HERDR_TELEGRAM_TOPICS_STATE", str(DEFAULT_STATE))).expanduser()


def _script_path() -> Path:
    return Path(os.getenv("HERDR_TELEGRAM_TOPICS_SCRIPT", str(DEFAULT_SCRIPT))).expanduser()


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


def _mapped_topic_entry(state: dict[str, Any], chat_id: str, thread_id: str | None) -> dict[str, Any] | None:
    telegram = state.get("telegram") or {}
    if chat_id != str(telegram.get("chat_id") or ""):
        return None
    if not thread_id or thread_id == str(telegram.get("general_thread_id", GENERAL_THREAD_ID)):
        return None
    for entry in (state.get("panes") or {}).values():
        if str(entry.get("topic_id") or "") == thread_id:
            return entry
    return None


def _mapped_entry(state: dict[str, Any], message: Any) -> dict[str, Any] | None:
    chat = getattr(message, "chat", None)
    if chat is None:
        return None
    chat_id = str(getattr(chat, "id", ""))
    return _mapped_topic_entry(state, chat_id, _message_thread_id(message))


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


async def maybe_handle_herdr_topic_message(adapter: Any, message: Any) -> bool:
    """Handle a Telegram message if it belongs to a mapped Herdr pane topic."""
    if not getattr(message, "text", None):
        return False
    state = _load_state()
    if not state:
        return False
    entry = _mapped_entry(state, message)
    if not entry:
        return False

    user = getattr(message, "from_user", None)
    payload = {
        "chat_id": str(getattr(getattr(message, "chat", None), "id", "")),
        "topic_id": _message_thread_id(message),
        "message_id": str(getattr(message, "message_id", "")),
        "user_id": str(getattr(user, "id", "") if user else ""),
        "from_bot": bool(getattr(user, "is_bot", False)) if user else True,
        "forwarded": _is_forwarded(message),
        "edited": bool(getattr(message, "edit_date", None)),
        "text": str(getattr(message, "text", "") or ""),
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
    entry = _mapped_topic_entry(state, chat_id, thread_id)
    if not entry:
        return False

    user = getattr(query, "from_user", None)
    payload = {
        "chat_id": chat_id,
        "topic_id": thread_id,
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
