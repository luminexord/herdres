"""Tendwire source-mode sync to Telegram."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from . import config, state
from .rendering import normalized_status, render_final_turn, render_pending, render_status_overview, render_working_update, status_emoji
from .safe import compact_ws, short_hash
from .telegram_delivery import TelegramClient, drain_outbox, topic_icon_id
from .tendwire_client import TendwireClient

RENDER_VERSION = "telegram-html-md-v2"


@dataclass
class SyncRuntime:
    tendwire: TendwireClient
    telegram: TelegramClient
    dry_run: bool = False
    with_outbox: bool = True
    max_sends: int = 8


def _workers(snapshot: dict[str, Any]) -> list[dict[str, Any]]:
    return [item for item in snapshot.get("workers", []) if isinstance(item, dict)]


def _turns(payload: dict[str, Any]) -> list[dict[str, Any]]:
    return [item for item in payload.get("turns", []) if isinstance(item, dict)]


def _pending(payload: dict[str, Any]) -> list[dict[str, Any]]:
    items = payload.get("pending_interactions", payload.get("pending", []))
    return [item for item in items if isinstance(item, dict)]


def _worker_is_open(worker: dict[str, Any]) -> bool:
    return normalized_status(worker.get("status")) not in {"closed", "failed"}


def _entry_for_turn(store: dict[str, Any], item: dict[str, Any]) -> tuple[str | None, dict[str, Any] | None]:
    worker_id = compact_ws(item.get("worker_id"), 160)
    key = state.find_entry_key_by_worker(store, worker_id)
    if key is None:
        return None, None
    entries = state.source_entries(store)
    return key, entries.get(key)


def _turn_id(item: dict[str, Any]) -> str:
    return compact_ws(item.get("id") or item.get("turn_id"), 200)


def _turn_content_hash(item: dict[str, Any], kind: str) -> str:
    return short_hash(
        {
            "kind": kind,
            "turn_id": _turn_id(item),
            "user": item.get("user_text"),
            "final": item.get("assistant_final_text"),
            "stream": item.get("assistant_stream_text"),
        },
        20,
    )


def _ensure_topic(
    store: dict[str, Any],
    worker: dict[str, Any],
    entry: dict[str, Any],
    runtime: SyncRuntime,
    *,
    chat_id: str,
) -> tuple[bool, bool]:
    if entry.get("topic_id"):
        return False, False
    if runtime.dry_run:
        return True, False
    created = runtime.telegram.create_topic(chat_id, entry.get("topic_name") or state.topic_name_for_worker(worker))
    if created.get("ok") and created.get("topic_id"):
        entry["topic_id"] = str(created["topic_id"])
        return True, True
    entry["last_topic_error"] = compact_ws(created.get("error"), 240)
    return False, False


def _sync_topic_icon(store: dict[str, Any], entry: dict[str, Any], runtime: SyncRuntime, *, chat_id: str) -> bool:
    thread_id = str(entry.get("topic_id") or "")
    if not thread_id:
        return False
    emoji = status_emoji(entry.get("status") or entry.get("tendwire_status_line"))
    emoji_id = topic_icon_id(store, emoji)
    if not emoji_id or entry.get("last_topic_icon") == emoji:
        return False
    if runtime.dry_run:
        entry["last_topic_icon"] = emoji
        return True
    result = runtime.telegram.edit_topic_icon(chat_id, thread_id, emoji_id)
    if result.get("ok"):
        entry["last_topic_icon"] = emoji
        return True
    return False


def _sync_workers(store: dict[str, Any], snapshot: dict[str, Any], runtime: SyncRuntime, *, chat_id: str) -> dict[str, int]:
    counts = {"created": 0, "panes": 0, "icon_updated": 0}
    for worker in _workers(snapshot):
        if not _worker_is_open(worker):
            continue
        _key, entry, created = state.upsert_worker_entry(store, worker)
        entry["status"] = normalized_status(worker.get("status"))
        topic_needed, topic_created = _ensure_topic(store, worker, entry, runtime, chat_id=chat_id)
        counts["created"] += int(created or topic_created or topic_needed)
        counts["icon_updated"] += int(_sync_topic_icon(store, entry, runtime, chat_id=chat_id))
        counts["panes"] += 1
    return counts


def _deliver_working(store: dict[str, Any], item: dict[str, Any], entry: dict[str, Any], runtime: SyncRuntime, *, chat_id: str) -> bool:
    thread_id = str(entry.get("topic_id") or "")
    if not thread_id:
        return False
    turn_id = _turn_id(item)
    content_hash = _turn_content_hash(item, "working")
    html = render_working_update(item, entry)
    if entry.get("last_stream_turn_id") == turn_id and entry.get("last_stream_hash") == content_hash:
        return False
    if runtime.dry_run:
        entry["last_stream_turn_id"] = turn_id
        entry["last_stream_hash"] = content_hash
        entry.setdefault("last_stream_message_id", "0")
        return True
    if entry.get("last_stream_message_id") and entry.get("last_stream_turn_id") == turn_id:
        sent = runtime.telegram.edit_message(chat_id, str(entry["last_stream_message_id"]), html)
    else:
        sent = runtime.telegram.send_message(chat_id, html, thread_id=thread_id, notify=False)
    if sent.get("ok"):
        entry["last_stream_turn_id"] = turn_id
        entry["last_stream_hash"] = content_hash
        entry["last_stream_message_id"] = str(sent.get("message_id") or entry.get("last_stream_message_id") or "")
        return True
    entry["last_delivery_error"] = compact_ws(sent.get("error"), 240)
    return False


def _deliver_final(store: dict[str, Any], item: dict[str, Any], entry: dict[str, Any], runtime: SyncRuntime, *, chat_id: str) -> bool:
    thread_id = str(entry.get("topic_id") or "")
    if not thread_id:
        return False
    turn_id = _turn_id(item)
    content_hash = _turn_content_hash(item, "final")
    identity = f"final:{turn_id}:{content_hash}"
    if identity in state.delivered_turns(store):
        if (
            entry.get("last_clean_message_id")
            and entry.get("last_render_version") != RENDER_VERSION
            and not runtime.dry_run
        ):
            edited = runtime.telegram.edit_message(chat_id, str(entry["last_clean_message_id"]), render_final_turn(item, entry))
            if edited.get("ok"):
                entry["last_render_version"] = RENDER_VERSION
                return True
        return False
    html = render_final_turn(item, entry)
    if runtime.dry_run:
        state.mark_delivered(store, identity, {"worker_id": entry.get("tendwire_worker_id"), "turn_id": turn_id})
        entry["last_turn_id"] = turn_id
        entry["last_clean_hash"] = content_hash
        entry["last_render_version"] = RENDER_VERSION
        entry.setdefault("last_clean_message_id", "0")
        return True
    sent = runtime.telegram.send_message(chat_id, html, thread_id=thread_id, notify=False)
    if sent.get("ok"):
        state.mark_delivered(store, identity, {"worker_id": entry.get("tendwire_worker_id"), "turn_id": turn_id})
        entry["last_turn_id"] = turn_id
        entry["last_clean_hash"] = content_hash
        entry["last_render_version"] = RENDER_VERSION
        entry["last_clean_message_id"] = str(sent.get("message_id") or "")
        return True
    entry["last_delivery_error"] = compact_ws(sent.get("error"), 240)
    return False


def _deliver_pending(store: dict[str, Any], item: dict[str, Any], runtime: SyncRuntime, *, chat_id: str) -> bool:
    key, entry = _entry_for_turn(store, item)
    if key is None or entry is None:
        return False
    thread_id = str(entry.get("topic_id") or "")
    if not thread_id:
        return False
    pending_id = compact_ws(item.get("id") or item.get("pending_id") or item.get("turn_id"), 200)
    content_hash = short_hash({"pending": pending_id, "text": item.get("prompt_text") or item.get("text")}, 20)
    identity = f"pending:{pending_id}:{content_hash}"
    if identity in state.delivered_turns(store):
        return False
    html = render_pending(item, entry)
    if runtime.dry_run:
        return state.mark_delivered(store, identity, {"worker_id": entry.get("tendwire_worker_id"), "pending_id": pending_id})
    sent = runtime.telegram.send_message(chat_id, html, thread_id=thread_id, notify=True)
    if sent.get("ok"):
        return state.mark_delivered(store, identity, {"worker_id": entry.get("tendwire_worker_id"), "pending_id": pending_id})
    return False


def _bootstrap_existing_turns(store: dict[str, Any], turns_payload: dict[str, Any], pending_payload: dict[str, Any]) -> int:
    """Record current Tendwire rows as seen on first deployment.

    The pre-slim source ledger used different identities. Without this bootstrap,
    the first source-only sync can repost historical rows. This migration is
    intentionally one-way and Telegram-silent.
    """
    if store.get("tendwired_bootstrap_complete"):
        return 0
    skipped = 0
    for item in _turns(turns_payload):
        _key, entry = _entry_for_turn(store, item)
        if entry is None:
            continue
        turn_id = _turn_id(item)
        if not turn_id:
            continue
        if bool(item.get("complete")) or item.get("assistant_final_text"):
            content_hash = _turn_content_hash(item, "final")
            identity = f"final:{turn_id}:{content_hash}"
            state.mark_delivered(store, identity, {"worker_id": entry.get("tendwire_worker_id"), "turn_id": turn_id})
            entry["last_turn_id"] = turn_id
            entry["last_clean_hash"] = content_hash
            entry.setdefault("last_clean_message_id", "0")
            skipped += 1
            continue
        if item.get("assistant_stream_text"):
            entry["last_stream_turn_id"] = turn_id
            entry["last_stream_hash"] = _turn_content_hash(item, "working")
            entry.setdefault("last_stream_message_id", "0")
            skipped += 1
    for item in _pending(pending_payload):
        _key, entry = _entry_for_turn(store, item)
        if entry is None:
            continue
        pending_id = compact_ws(item.get("id") or item.get("pending_id") or item.get("turn_id"), 200)
        if not pending_id:
            continue
        content_hash = short_hash({"pending": pending_id, "text": item.get("prompt_text") or item.get("text")}, 20)
        state.mark_delivered(
            store,
            f"pending:{pending_id}:{content_hash}",
            {"worker_id": entry.get("tendwire_worker_id"), "pending_id": pending_id},
        )
        skipped += 1
    store["tendwired_bootstrap_complete"] = True
    store["tendwired_bootstrap_seen"] = skipped
    return skipped


def _sync_turns(store: dict[str, Any], turns_payload: dict[str, Any], pending_payload: dict[str, Any], runtime: SyncRuntime, *, chat_id: str) -> dict[str, int]:
    counts = {"feed_sent": 0, "sent": 0}
    for item in _turns(turns_payload):
        _key, entry = _entry_for_turn(store, item)
        if entry is None:
            continue
        complete = bool(item.get("complete")) or bool(item.get("assistant_final_text"))
        if complete and (item.get("assistant_final_text") or item.get("assistant_stream_text")):
            delivered = _deliver_final(store, item, entry, runtime, chat_id=chat_id)
        elif item.get("assistant_stream_text"):
            delivered = _deliver_working(store, item, entry, runtime, chat_id=chat_id)
        else:
            delivered = False
        counts["feed_sent"] += int(delivered)
        counts["sent"] += int(delivered)
    for item in _pending(pending_payload):
        delivered = _deliver_pending(store, item, runtime, chat_id=chat_id)
        counts["feed_sent"] += int(delivered)
        counts["sent"] += int(delivered)
    return counts


def _sync_pinned(store: dict[str, Any], runtime: SyncRuntime, *, chat_id: str) -> bool:
    entries = list(state.source_entries(store).values())
    if not entries:
        return False
    html = render_status_overview(entries)
    telegram = store.setdefault("telegram", {})
    message_id = str(telegram.get("pinned_status_message_id") or "")
    content_hash = short_hash(html, 20)
    if telegram.get("pinned_status_hash") == content_hash:
        return False
    if runtime.dry_run:
        telegram["pinned_status_hash"] = content_hash
        telegram.setdefault("pinned_status_message_id", "0")
        return True
    if message_id:
        sent = runtime.telegram.edit_message(chat_id, message_id, html)
    else:
        sent = runtime.telegram.send_message(chat_id, html, thread_id=config.general_thread_id(store), notify=False)
        if sent.get("ok") and sent.get("message_id"):
            runtime.telegram.pin_message(chat_id, str(sent["message_id"]))
    if sent.get("ok"):
        telegram["pinned_status_hash"] = content_hash
        if sent.get("message_id"):
            telegram["pinned_status_message_id"] = str(sent["message_id"])
        return True
    telegram["pinned_status_last_error"] = compact_ws(sent.get("error"), 240)
    return False


def sync_once(store: dict[str, Any], runtime: SyncRuntime) -> dict[str, Any]:
    config.require_source_mode()
    chat_id = config.telegram_chat_id(store)
    snapshot = runtime.tendwire.snapshot()
    turns_payload = runtime.tendwire.turns()
    pending_payload = runtime.tendwire.pending()
    for name, payload in (("snapshot", snapshot), ("turns", turns_payload), ("pending", pending_payload)):
        if payload.get("ok") is False:
            return {
                "ok": False,
                "status": f"tendwire_{name}_failed",
                "changed": False,
                "created": 0,
                "panes": 0,
                "icon_updated": 0,
                "pinned_status_updated": 0,
                "feed_sent": 0,
                "sent": 0,
                "tendwire_outbox": {"enabled": runtime.with_outbox, "polled": 0, "delivered": 0, "acked": 0, "failed": 0, "deferred": 0, "changed": False},
            }
    changed = False
    worker_counts = _sync_workers(store, snapshot, runtime, chat_id=chat_id)
    bootstrapped = _bootstrap_existing_turns(store, turns_payload, pending_payload)
    turn_counts = {"feed_sent": 0, "sent": 0} if bootstrapped else _sync_turns(store, turns_payload, pending_payload, runtime, chat_id=chat_id)
    changed = changed or bool(worker_counts["created"] or worker_counts["icon_updated"] or turn_counts["sent"] or bootstrapped)
    pinned_changed = _sync_pinned(store, runtime, chat_id=chat_id)
    changed = changed or pinned_changed
    outbox_result = {"enabled": runtime.with_outbox, "polled": 0, "delivered": 0, "acked": 0, "failed": 0, "deferred": 0, "changed": False}
    if runtime.with_outbox:
        remaining = max(0, runtime.max_sends - int(turn_counts["sent"]))
        outbox_result = drain_outbox(store, runtime.telegram, runtime.tendwire, chat_id=chat_id, max_sends=remaining, dry_run=runtime.dry_run)
        changed = changed or bool(outbox_result.get("changed"))
    return {
        "ok": True,
        "changed": changed,
        "created": worker_counts["created"],
        "panes": worker_counts["panes"],
        "icon_updated": worker_counts["icon_updated"],
        "pinned_status_updated": int(pinned_changed),
        "feed_sent": turn_counts["feed_sent"],
        "sent": turn_counts["sent"],
        "bootstrap_seen": bootstrapped,
        "tendwire_outbox": outbox_result,
    }
