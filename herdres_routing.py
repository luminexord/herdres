"""Pure Telegram topic routing and payload helpers for Herdres inbound control."""

from __future__ import annotations

from typing import Any

GENERAL_THREAD_ID = "1"


def message_thread_id_obj(message: Any, general_thread_id: str = GENERAL_THREAD_ID) -> str | None:
    thread_id = getattr(message, "message_thread_id", None)
    if thread_id is None:
        chat = getattr(message, "chat", None)
        if getattr(chat, "is_forum", False):
            return general_thread_id
        return None
    return str(thread_id)


def message_thread_id_dict(message: dict[str, Any], general_thread_id: str = GENERAL_THREAD_ID) -> str | None:
    thread_id = message.get("message_thread_id")
    if thread_id is None:
        chat = message.get("chat") if isinstance(message.get("chat"), dict) else {}
        if chat.get("is_forum"):
            return general_thread_id
        return None
    return str(thread_id)


def mapped_topic_entry(
    state: dict[str, Any],
    chat_id: str,
    thread_id: str | None,
    general_thread_id: str = GENERAL_THREAD_ID,
) -> dict[str, Any] | None:
    telegram = state.get("telegram") if isinstance(state.get("telegram"), dict) else {}
    configured_chat = str(telegram.get("chat_id") or "")
    if not configured_chat or str(chat_id) != configured_chat:
        return None
    configured_general = str(telegram.get("general_thread_id", general_thread_id))
    if not thread_id or str(thread_id) == configured_general:
        return None
    panes = state.get("panes") if isinstance(state.get("panes"), dict) else {}
    for entry in panes.values():
        if isinstance(entry, dict) and str(entry.get("topic_id") or "") == str(thread_id):
            return entry
    return None


def mapped_entry_for_obj(state: dict[str, Any], message: Any) -> dict[str, Any] | None:
    chat = getattr(message, "chat", None)
    if chat is None:
        return None
    chat_id = str(getattr(chat, "id", ""))
    return mapped_topic_entry(state, chat_id, message_thread_id_obj(message))


def mapped_entry_for_dict(state: dict[str, Any], message: dict[str, Any]) -> dict[str, Any] | None:
    chat = message.get("chat") if isinstance(message.get("chat"), dict) else {}
    chat_id = str(chat.get("id") or "")
    return mapped_topic_entry(state, chat_id, message_thread_id_dict(message))


def is_forwarded_obj(message: Any) -> bool:
    return any(
        getattr(message, attr, None) is not None
        for attr in ("forward_origin", "forward_from", "forward_sender_name", "forward_date")
    )


def is_forwarded_dict(message: dict[str, Any]) -> bool:
    return any(
        message.get(attr) is not None
        for attr in ("forward_origin", "forward_from", "forward_sender_name", "forward_date")
    )


def attachment_payload_obj(message: Any) -> dict[str, Any] | None:
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


def attachment_payload_dict(message: dict[str, Any]) -> dict[str, Any] | None:
    try:
        document = message.get("document")
        if isinstance(document, dict):
            file_id = str(document.get("file_id") or "")
            if file_id:
                return {
                    "kind": "document",
                    "file_id": file_id,
                    "file_name": str(document.get("file_name") or ""),
                    "mime_type": str(document.get("mime_type") or ""),
                    "file_size": int(document.get("file_size") or 0),
                }
        photo = message.get("photo")
        if isinstance(photo, list) and photo:
            largest = photo[-1]
            if isinstance(largest, dict):
                file_id = str(largest.get("file_id") or "")
                if file_id:
                    return {
                        "kind": "photo",
                        "file_id": file_id,
                        "file_name": "",
                        "mime_type": "image/jpeg",
                        "file_size": int(largest.get("file_size") or 0),
                    }
    except Exception:
        return None
    return None


def owner_ids(state: dict[str, Any], env_value: str = "") -> set[str]:
    telegram = state.get("telegram") if isinstance(state.get("telegram"), dict) else {}
    owners = {str(x) for x in telegram.get("owner_user_ids", [])}
    if owners or not env_value:
        return owners
    return {part.strip() for part in env_value.split(",") if part.strip()}


def build_command_payload_obj(message: Any) -> dict[str, Any]:
    user = getattr(message, "from_user", None)
    reply_to = getattr(message, "reply_to_message", None)
    return {
        "chat_id": str(getattr(getattr(message, "chat", None), "id", "")),
        "topic_id": message_thread_id_obj(message),
        "message_id": str(getattr(message, "message_id", "")),
        "reply_to_message_id": str(getattr(reply_to, "message_id", "") if reply_to else ""),
        "user_id": str(getattr(user, "id", "") if user else ""),
        "from_bot": bool(getattr(user, "is_bot", False)) if user else True,
        "forwarded": is_forwarded_obj(message),
        "edited": bool(getattr(message, "edit_date", None)),
        "text": str(getattr(message, "text", "") or ""),
        "caption": str(getattr(message, "caption", "") or ""),
        "attachment": attachment_payload_obj(message),
    }


def build_command_payload_dict(message: dict[str, Any]) -> dict[str, Any]:
    user = message.get("from") if isinstance(message.get("from"), dict) else {}
    reply_to = message.get("reply_to_message") if isinstance(message.get("reply_to_message"), dict) else {}
    chat = message.get("chat") if isinstance(message.get("chat"), dict) else {}
    return {
        "chat_id": str(chat.get("id") or ""),
        "topic_id": message_thread_id_dict(message),
        "message_id": str(message.get("message_id") or ""),
        "reply_to_message_id": str(reply_to.get("message_id") or ""),
        "user_id": str(user.get("id") or ""),
        "from_bot": bool(user.get("is_bot")) if user else True,
        "forwarded": is_forwarded_dict(message),
        "edited": bool(message.get("edit_date")),
        "text": str(message.get("text") or ""),
        "caption": str(message.get("caption") or ""),
        "attachment": attachment_payload_dict(message),
    }


def build_callback_payload_obj(query: Any) -> dict[str, Any]:
    message = getattr(query, "message", None)
    user = getattr(query, "from_user", None)
    chat = getattr(message, "chat", None)
    return {
        "chat_id": str(getattr(chat, "id", getattr(message, "chat_id", ""))),
        "topic_id": message_thread_id_obj(message),
        "message_id": str(getattr(message, "message_id", "")),
        "user_id": str(getattr(user, "id", "") if user else ""),
        "data": str(getattr(query, "data", "") or ""),
    }


def build_callback_payload_dict(query: dict[str, Any]) -> dict[str, Any]:
    message = query.get("message") if isinstance(query.get("message"), dict) else {}
    user = query.get("from") if isinstance(query.get("from"), dict) else {}
    chat = message.get("chat") if isinstance(message.get("chat"), dict) else {}
    return {
        "chat_id": str(chat.get("id") or message.get("chat_id") or ""),
        "topic_id": message_thread_id_dict(message),
        "message_id": str(message.get("message_id") or ""),
        "user_id": str(user.get("id") or ""),
        "data": str(query.get("data") or ""),
    }
