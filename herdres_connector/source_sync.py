"""Tendwire source-mode sync to Telegram."""

from __future__ import annotations

import sys
import time
from dataclasses import dataclass
from typing import Any

from . import config, speech, state
from .managed_bots import MANAGER_BOT_KIND, desired_message_bot_kind, managed_bot_kind_for_entry, managed_bot_token_for_entry
from .rendering import normalized_status, render_pending, render_status_overview, status_emoji
from .rich_delivery import edit_feed_item, feed_item_requires_send_split, render_feed_item_html, send_feed_item, split_legacy_message_ids, turn_item_from_source
from .safe import compact_ws, short_hash
from .telegram_delivery import MESSAGE_TEXT_LIMIT, TOPIC_ICON_COLORS, TelegramClient, drain_outbox, topic_icon_catalog, topic_icon_id
from .tendwire_client import TendwireClient

RENDER_VERSION = "telegram-rich-v26-clean"


@dataclass
class SyncRuntime:
    tendwire: TendwireClient
    telegram: TelegramClient
    dry_run: bool = False
    with_outbox: bool = True
    max_sends: int = 8


def _workers(snapshot: dict[str, Any]) -> list[dict[str, Any]]:
    return [item for item in snapshot.get("workers", []) if isinstance(item, dict)]


def _spaces(snapshot: dict[str, Any]) -> list[dict[str, Any]]:
    return [item for item in snapshot.get("spaces", []) if isinstance(item, dict)]


def _turns(payload: dict[str, Any]) -> list[dict[str, Any]]:
    return [item for item in payload.get("turns", []) if isinstance(item, dict)]


def _pending(payload: dict[str, Any]) -> list[dict[str, Any]]:
    items = payload.get("pending_interactions", payload.get("pending", []))
    return [item for item in items if isinstance(item, dict)]


def _normalize_voice_mode(value: Any) -> str:
    clean = str(value or "").strip().lower().replace("-", "_")
    if clean in {"per_agent", "peragent", "agent", "agents", "voice"}:
        return "per_agent"
    return "shared"


def _default_voice_mode() -> str:
    return "per_agent" if config.managed_bots_enabled() else "shared"


def _entry_voice_mode(entry: dict[str, Any] | None) -> str:
    if isinstance(entry, dict) and str(entry.get("voice_mode") or "").strip():
        return _normalize_voice_mode(entry.get("voice_mode"))
    return _default_voice_mode()


def _space_voice_mode(store: dict[str, Any], space_id: str | None) -> str:
    _space_key, space_entry = state.find_space_entry_by_id(store, str(space_id or ""))
    return _entry_voice_mode(space_entry)


def _stamp_managed_voice(entry: dict[str, Any], voice_mode: str) -> None:
    mode = _normalize_voice_mode(voice_mode)
    entry["voice_mode"] = mode
    entry["managed_voice_active"] = mode == "per_agent"


def _meta_raw_status(worker: dict[str, Any]) -> str:
    meta = worker.get("meta") if isinstance(worker.get("meta"), dict) else {}
    return compact_ws(meta.get("raw_status"), 80)


def _source_status(value: Any) -> str:
    raw = str(value or "").strip().lower().replace("-", "_")
    if raw == "active":
        return "idle"
    return normalized_status(value)


def _dominant_status(*values: Any) -> str:
    statuses = [normalized_status(value) for value in values if str(value or "").strip()]
    for wanted in ("failed", "attention", "working"):
        if wanted in statuses:
            return wanted
    return statuses[0] if statuses else ""


def _turn_activity_status(item: dict[str, Any]) -> str:
    if bool(item.get("complete")) or bool(item.get("assistant_final_text")):
        return "idle"
    if item.get("complete") is False or item.get("has_open_turn") is True or bool(item.get("assistant_stream_text")):
        return "working"
    return ""


def _turn_activity_statuses(payload: dict[str, Any], live_worker_ids: set[str] | None = None) -> tuple[dict[str, str], dict[str, str]]:
    by_worker: dict[str, str] = {}
    by_space: dict[str, str] = {}
    for item in _turns(payload):
        status = _turn_activity_status(item)
        if not status:
            continue
        worker_id = compact_ws(item.get("worker_id"), 160)
        space_id = compact_ws(item.get("space_id"), 160)
        if live_worker_ids is not None and worker_id and worker_id not in live_worker_ids:
            # Stale turn rows from retired worker ids must not pin a live
            # space/worker status (e.g. an abandoned open turn reading as
            # "working" forever).
            continue
        if worker_id and worker_id not in by_worker:
            by_worker[worker_id] = status
        if space_id and space_id not in by_space:
            by_space[space_id] = status
    return by_worker, by_space


def _effective_worker_status(worker: dict[str, Any], turn_status_by_worker: dict[str, str]) -> str:
    raw_status = normalized_status(worker.get("status"))
    if raw_status in {"closed", "failed", "attention"}:
        return raw_status
    public_raw_status = normalized_status(_meta_raw_status(worker))
    if public_raw_status in {"failed", "attention", "working"}:
        return public_raw_status
    worker_id = compact_ws(worker.get("id"), 160)
    if worker_id and turn_status_by_worker.get(worker_id):
        return turn_status_by_worker[worker_id]
    return _source_status(worker.get("status"))


def _worker_is_open(worker: dict[str, Any]) -> bool:
    return normalized_status(worker.get("status")) not in {"closed", "failed"}


def _worker_status_is_finished(value: Any) -> bool:
    status = str(value or "").strip().lower().replace("-", "_")
    return status in {"closed", "complete", "completed", "done", "failed", "failure"}


def _entry_status_is_finished(entry: dict[str, Any]) -> bool:
    return _worker_status_is_finished(entry.get("tendwire_raw_status") or entry.get("status"))


def _entry_is_council_topic(entry: dict[str, Any]) -> bool:
    """Ephemeral gitmoot delegation/council entries (gm-local-as workers, "gitmoot · local-as"
    delegation spaces, "Council · …" topics). The markers are deliberately PRECISE: a bare "gitmoot"
    substring would also match regular panes whose topic is named after the /root/gitmoot project
    dir (labels/cwd naming), and done-council cleanup would then delete a normal pane's topic every
    time it finished a task (live incident: "Gitmoot2"/"gitmoot 2" churned create/delete)."""
    material = " ".join(
        str(entry.get(key) or "").lower()
        for key in ("topic_name", "worker_name", "agent", "space_topic_name")
    )
    return any(marker in material for marker in ("council", "gm-local", "gm_", "gitmoot \u00b7"))


def _should_delete_done_council_topic(entry: dict[str, Any]) -> bool:
    return config.delete_done_council_topics() and _entry_is_council_topic(entry) and _entry_status_is_finished(entry)


def _topic_missing(error: Any) -> bool:
    text = str(error or "").lower()
    return "topic_id_invalid" in text or "message thread not found" in text


def _topic_not_modified(error: Any) -> bool:
    text = str(error or "").lower()
    return "topic_not_modified" in text or "not modified" in text


def _message_missing(error: Any) -> bool:
    text = str(error or "").lower()
    return "message to edit not found" in text or "message not found" in text


def _space_is_open(space: dict[str, Any]) -> bool:
    return normalized_status(space.get("status")) not in {"closed", "failed"}


def _select_space_worker(workers: list[dict[str, Any]], turn_status_by_worker: dict[str, str] | None = None) -> dict[str, Any]:
    turn_status_by_worker = turn_status_by_worker or {}
    for wanted in ("working", "attention", "idle"):
        matches = [worker for worker in workers if _effective_worker_status(worker, turn_status_by_worker) == wanted]
        if matches:
            return max(matches, key=lambda worker: str(worker.get("last_seen_at") or ""))
    return max(workers, key=lambda worker: str(worker.get("last_seen_at") or "")) if workers else {}


def _delivery_entry(space_entry: dict[str, Any], worker_entry: dict[str, Any] | None = None) -> dict[str, Any]:
    worker_entry = worker_entry or {}
    entry = worker_entry
    worker_name = compact_ws(worker_entry.get("worker_name") or worker_entry.get("agent"), 80)
    space_name = compact_ws(space_entry.get("topic_name"), 80)
    if worker_name and space_name:
        entry["topic_name"] = f"{space_name} · {worker_name}"
    elif space_name:
        entry["topic_name"] = space_name
    entry["topic_id"] = str(space_entry.get("topic_id") or "")
    entry["tendwire_space_id"] = space_entry.get("tendwire_space_id") or worker_entry.get("tendwire_space_id")
    entry["space_topic_name"] = space_name
    entry["tendwire_worker_id"] = worker_entry.get("tendwire_worker_id") or space_entry.get("active_worker_id")
    entry["tendwire_fingerprint"] = worker_entry.get("tendwire_fingerprint") or space_entry.get("active_worker_fingerprint")
    entry["agent"] = worker_entry.get("agent") or entry.get("agent")
    entry["managed_bot_kind"] = worker_entry.get("managed_bot_kind") or managed_bot_kind_for_entry(worker_entry)
    voice_mode = _entry_voice_mode(space_entry)
    entry["voice_mode"] = voice_mode
    entry["managed_voice_active"] = voice_mode == "per_agent"
    return entry


def _entry_worker_id(entry: dict[str, Any]) -> str:
    return compact_ws(entry.get("tendwire_worker_id") or entry.get("worker_id") or entry.get("active_worker_id"), 160)


def _entry_space_id(entry: dict[str, Any]) -> str:
    return compact_ws(entry.get("tendwire_space_id") or entry.get("space_id"), 160)


def _source_space_topic_ids(store: dict[str, Any]) -> dict[str, str]:
    topic_ids: dict[str, str] = {}
    for entry in state.source_space_entries(store).values():
        space_id = _entry_space_id(entry)
        topic_id = compact_ws(entry.get("topic_id"), 80)
        if space_id and topic_id:
            topic_ids[space_id] = topic_id
    return topic_ids


def _worker_entry_for_turn(store: dict[str, Any], worker_id: str, space_id: str) -> tuple[str | None, dict[str, Any] | None]:
    candidates = [
        (key, entry)
        for key, entry in state.source_worker_entries(store).items()
        if _entry_worker_id(entry) == worker_id
    ]
    if not candidates:
        return None, None
    if space_id:
        matches = [(key, entry) for key, entry in candidates if _entry_space_id(entry) == space_id]
        if matches:
            return matches[0]
        if any(_entry_space_id(entry) for _key, entry in candidates):
            return None, None
    return candidates[0]


def _telegram_state(store: dict[str, Any]) -> dict[str, Any]:
    telegram = store.get("telegram")
    if not isinstance(telegram, dict):
        telegram = {}
        store["telegram"] = telegram
    return telegram


def _delivery_bot(store: dict[str, Any], entry: dict[str, Any]) -> tuple[str | None, str]:
    telegram = _telegram_state(store)
    token = managed_bot_token_for_entry(telegram, entry)
    return token, desired_message_bot_kind(telegram, entry)


def _record_delivery_error(entry: dict[str, Any], result: dict[str, Any], bot_kind: str) -> None:
    error = compact_ws(result.get("error") or result.get("kind") or "Telegram delivery failed", 240)
    entry["last_delivery_error"] = error
    if bot_kind != MANAGER_BOT_KIND:
        entry["last_managed_bot_kind"] = bot_kind
        entry["last_managed_bot_error"] = error


def _record_delivery_success(entry: dict[str, Any], bot_kind: str) -> None:
    entry.pop("last_delivery_error", None)
    if bot_kind != MANAGER_BOT_KIND:
        entry["last_managed_bot_kind"] = bot_kind
        entry.pop("last_managed_bot_error", None)


def _entry_for_turn(store: dict[str, Any], item: dict[str, Any]) -> tuple[str | None, dict[str, Any] | None]:
    worker_id = compact_ws(item.get("worker_id"), 160)
    space_id = compact_ws(item.get("space_id"), 160)
    key, worker_entry = _worker_entry_for_turn(store, worker_id, space_id)
    if key is None:
        return None, None
    if worker_entry is None:
        return None, None
    if config.source_topic_mode() == "worker":
        return key, worker_entry
    _space_key, space_entry = state.find_space_entry_by_id(
        store,
        compact_ws(space_id or worker_entry.get("tendwire_space_id") or worker_entry.get("space_id"), 160),
    )
    if space_entry is None:
        return None, None
    return key, _delivery_entry(space_entry, worker_entry)


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


def _turn_user_hash(item: dict[str, Any]) -> str:
    text = compact_ws(item.get("user_text"), 2000)
    return short_hash({"user": text}, 16) if text else ""


# --- Delivery-state single writers ------------------------------------------
# These keys describe the last delivered final/stream message for an entry.
# Every write goes through the helpers below so the group stays consistent;
# never assign the keys directly.

_FINAL_DELIVERY_KEYS = (
    "last_turn_id",
    "last_clean_hash",
    "last_clean_user_hash",
    "last_clean_message_id",
    "last_clean_message_ids",
    "last_clean_bot_kind",
    "last_render_version",
)
_STREAM_DELIVERY_KEYS = (
    "last_stream_turn_id",
    "last_stream_hash",
    "last_stream_message_id",
    "last_stream_bot_kind",
    "last_stream_updated_at",
)


def _pop_keys(entry: dict[str, Any], keys: tuple[str, ...]) -> bool:
    changed = False
    for key in keys:
        if key in entry:
            entry.pop(key, None)
            changed = True
    return changed


def _clear_final_delivery_keys(entry: dict[str, Any]) -> bool:
    return _pop_keys(entry, _FINAL_DELIVERY_KEYS)


def _clear_stream_delivery_keys(entry: dict[str, Any]) -> bool:
    return _pop_keys(entry, _STREAM_DELIVERY_KEYS)


def _entry_put(entry: dict[str, Any], key: str, value: Any) -> bool:
    if entry.get(key) == value:
        return False
    entry[key] = value
    return True


def _entry_float(entry: dict[str, Any], key: str) -> float:
    try:
        return float(entry.get(key) or 0)
    except (TypeError, ValueError):
        return 0.0


def _same_turn_working_update_too_soon(entry: dict[str, Any], turn_id: str, *, now: float) -> bool:
    if not turn_id or entry.get("last_stream_turn_id") != turn_id:
        return False
    if not entry.get("last_stream_message_id"):
        return False
    min_seconds = config.working_update_min_seconds()
    if min_seconds <= 0:
        return False
    last_at = _entry_float(entry, "last_stream_updated_at")
    return bool(last_at and now - last_at < min_seconds)


def _set_final_delivery(
    entry: dict[str, Any],
    *,
    turn_id: str,
    content_hash: str,
    user_hash: str | None = None,
    message_ids: list[str] | None = None,
    bot_kind: str | None = None,
    render_version: int | None = None,
    placeholder: bool = False,
) -> bool:
    """Single writer for the final-delivery key group.

    ``user_hash``/``message_ids``/``bot_kind``/``render_version`` are left
    untouched when None. ``placeholder`` records the "0" sentinel used by
    dry-run and bootstrap paths without clobbering a real message id.
    """
    changed = _entry_put(entry, "last_turn_id", turn_id)
    changed = _entry_put(entry, "last_clean_hash", content_hash) or changed
    if user_hash is not None:
        if user_hash:
            changed = _entry_put(entry, "last_clean_user_hash", user_hash) or changed
        elif "last_clean_user_hash" in entry:
            entry.pop("last_clean_user_hash", None)
            changed = True
    if render_version is not None:
        changed = _entry_put(entry, "last_render_version", render_version) or changed
    if bot_kind:
        changed = _entry_put(entry, "last_clean_bot_kind", bot_kind) or changed
    if placeholder:
        if not entry.get("last_clean_message_id"):
            entry["last_clean_message_id"] = "0"
            changed = True
        changed = _entry_put(entry, "last_clean_message_ids", ["0"]) or changed
    elif message_ids is not None:
        kept = [message_id for message_id in message_ids if message_id]
        changed = _entry_put(entry, "last_clean_message_ids", kept) or changed
        changed = _entry_put(entry, "last_clean_message_id", kept[0] if kept else "") or changed
    return changed


def _set_stream_delivery(
    entry: dict[str, Any],
    *,
    turn_id: str,
    content_hash: str,
    message_id: str | None = None,
    bot_kind: str | None = None,
    placeholder: bool = False,
) -> bool:
    """Single writer for the stream-delivery key group."""
    changed = _entry_put(entry, "last_stream_turn_id", turn_id)
    changed = _entry_put(entry, "last_stream_hash", content_hash) or changed
    if placeholder:
        if not entry.get("last_stream_message_id"):
            entry["last_stream_message_id"] = "0"
            changed = True
    elif message_id is not None:
        changed = _entry_put(entry, "last_stream_message_id", message_id) or changed
    if bot_kind:
        changed = _entry_put(entry, "last_stream_bot_kind", bot_kind) or changed
    return changed


def _record_stream_update_time(entry: dict[str, Any], now: float | None = None) -> None:
    entry["last_stream_updated_at"] = f"{(time.time() if now is None else now):.3f}"


def _changed_final_should_send_new_message(item: dict[str, Any], entry: dict[str, Any]) -> bool:
    user_hash = _turn_user_hash(item)
    if not user_hash:
        return False
    if entry.get("last_turn_id") != _turn_id(item):
        return False
    previous = str(entry.get("last_clean_user_hash") or "")
    return bool(previous and previous != user_hash)


def _working_delivery_item(item: dict[str, Any]) -> dict[str, Any]:
    if item.get("assistant_stream_text") or item.get("assistant_final_text"):
        return item
    updated = dict(item)
    updated["assistant_stream_text"] = "Work is in progress."
    return updated


def _turn_is_working_placeholder(item: dict[str, Any], entry: dict[str, Any]) -> bool:
    if item.get("assistant_stream_text") or item.get("assistant_final_text"):
        return False
    if bool(item.get("complete")):
        return False
    if not _turn_id(item):
        return False
    return normalized_status(entry.get("status")) == "working"


def _final_delivery_bindings(store: dict[str, Any], turn_id: str) -> list[tuple[str, dict[str, Any]]]:
    return [
        (message_id, binding)
        for message_id, binding in state.message_bindings(store).items()
        if isinstance(binding, dict) and str(binding.get("kind") or "") == "final" and str(binding.get("turn_id") or "") == turn_id
    ]


def _final_turn_delivered(store: dict[str, Any], turn_id: str) -> bool:
    if not turn_id:
        return False
    prefix = f"final:{turn_id}:"
    for identity, record in state.delivered_turns(store).items():
        if str(identity).startswith(prefix):
            return True
        if isinstance(record, dict) and str(record.get("turn_id") or "") == turn_id:
            return True
    return False


def _clear_open_turn_final_delivery_state(store: dict[str, Any], entry: dict[str, Any], turn_id: str) -> bool:
    """Remove stale final-delivery markers for a turn Tendwire still reports open.

    Older source syncs could accidentally render stream-only progress as a final
    Response. If those markers remain, the real completed response for the same
    turn_id is suppressed later by the duplicate guard.
    """
    if not turn_id:
        return False
    changed = False
    delivered = state.delivered_turns(store)
    for identity, record in list(delivered.items()):
        same_turn_record = isinstance(record, dict) and str(record.get("turn_id") or "") == turn_id
        if str(identity).startswith(f"final:{turn_id}:") or same_turn_record:
            delivered.pop(identity, None)
            changed = True
    bindings = state.message_bindings(store)
    for message_id, binding in list(bindings.items()):
        if (
            isinstance(binding, dict)
            and str(binding.get("kind") or "") == "final"
            and str(binding.get("turn_id") or "") == turn_id
        ):
            bindings.pop(message_id, None)
            changed = True
    if entry.get("last_turn_id") == turn_id:
        changed = _clear_final_delivery_keys(entry) or changed
    return changed


def _repair_delivered_final_entry(store: dict[str, Any], item: dict[str, Any], entry: dict[str, Any], content_hash: str) -> bool:
    turn_id = _turn_id(item)
    final_bindings = _final_delivery_bindings(store, turn_id)
    message_ids = [message_id for message_id, _binding in final_bindings if message_id] if final_bindings else None
    bot_kind = str(final_bindings[-1][1].get("bot_kind") or "") if final_bindings else ""
    return _set_final_delivery(
        entry,
        turn_id=turn_id,
        content_hash=content_hash,
        user_hash=_turn_user_hash(item),
        message_ids=message_ids,
        bot_kind=bot_kind or None,
    )


def _clear_stream_delivery_state(entry: dict[str, Any], turn_id: str) -> None:
    if entry.get("last_stream_turn_id") != turn_id:
        return
    _clear_stream_delivery_keys(entry)


def _record_final_delivery_success(
    store: dict[str, Any],
    item: dict[str, Any],
    entry: dict[str, Any],
    *,
    thread_id: str,
    message_ids: list[str],
    content_hash: str,
    identity: str,
    bot_kind: str,
) -> None:
    turn_id = _turn_id(item)
    for message_id in message_ids:
        state.bind_message_to_worker(store, message_id, entry, topic_id=thread_id, kind="final", turn_id=turn_id, bot_kind=bot_kind)
    state.mark_delivered(store, identity, {"worker_id": entry.get("tendwire_worker_id"), "turn_id": turn_id})
    _set_final_delivery(
        entry,
        turn_id=turn_id,
        content_hash=content_hash,
        user_hash=_turn_user_hash(item),
        message_ids=message_ids,
        bot_kind=bot_kind,
        render_version=RENDER_VERSION,
    )
    _record_delivery_success(entry, bot_kind)
    _clear_stream_delivery_state(entry, turn_id)


def _promote_working_to_final(
    store: dict[str, Any],
    item: dict[str, Any],
    entry: dict[str, Any],
    runtime: SyncRuntime,
    *,
    chat_id: str,
    thread_id: str,
    content_hash: str,
    identity: str,
) -> bool:
    turn_id = _turn_id(item)
    stream_message_id = str(entry.get("last_stream_message_id") or "")
    if not stream_message_id or entry.get("last_stream_turn_id") != turn_id:
        return False
    telegram = _telegram_state(store)
    api_token, bot_kind = _delivery_bot(store, entry)
    stored_bot_kind = str(entry.get("last_stream_bot_kind") or MANAGER_BOT_KIND)
    if stored_bot_kind != bot_kind:
        return False
    feed_item = turn_item_from_source(item, entry)
    # Telegram legacy edits cannot split. If the final view is too large for a
    # single safe edit, use the send path instead so long responses are split.
    if len(render_feed_item_html(feed_item)) > MESSAGE_TEXT_LIMIT or feed_item_requires_send_split(feed_item):
        return False
    sent = edit_feed_item(
        runtime.telegram,
        chat_id,
        stream_message_id,
        feed_item,
        telegram=telegram,
        api_token=api_token,
    )
    if not sent.get("ok"):
        return False
    edited_message_id = str(sent.get("message_id") or "").strip()
    message_id = edited_message_id if edited_message_id and edited_message_id != "0" else stream_message_id
    _record_final_delivery_success(
        store,
        item,
        entry,
        thread_id=thread_id,
        message_ids=[message_id],
        content_hash=content_hash,
        identity=identity,
        bot_kind=bot_kind,
    )
    return True


def _replace_changed_final(
    store: dict[str, Any],
    item: dict[str, Any],
    entry: dict[str, Any],
    runtime: SyncRuntime,
    *,
    chat_id: str,
    thread_id: str,
    content_hash: str,
    identity: str,
) -> bool:
    bindings = _final_delivery_bindings(store, _turn_id(item))
    message_ids = [message_id for message_id, _binding in bindings if message_id]
    if len(message_ids) != 1:
        return False
    telegram = _telegram_state(store)
    api_token, bot_kind = _delivery_bot(store, entry)
    stored_bot_kind = str(bindings[-1][1].get("bot_kind") or entry.get("last_clean_bot_kind") or MANAGER_BOT_KIND)
    if stored_bot_kind != bot_kind:
        return False
    feed_item = turn_item_from_source(item, entry)
    if len(render_feed_item_html(feed_item)) > MESSAGE_TEXT_LIMIT or feed_item_requires_send_split(feed_item):
        return False
    sent = edit_feed_item(
        runtime.telegram,
        chat_id,
        message_ids[0],
        feed_item,
        telegram=telegram,
        api_token=api_token,
    )
    if not sent.get("ok"):
        return False
    edited_message_id = str(sent.get("message_id") or "").strip()
    message_id = edited_message_id if edited_message_id and edited_message_id != "0" else message_ids[0]
    _record_final_delivery_success(
        store,
        item,
        entry,
        thread_id=thread_id,
        message_ids=[message_id],
        content_hash=content_hash,
        identity=identity,
        bot_kind=bot_kind,
    )
    return True


def _suppress_historical_final(store: dict[str, Any], item: dict[str, Any], content_hash: str) -> bool:
    turn_id = _turn_id(item)
    if not turn_id or _final_turn_delivered(store, turn_id):
        return False
    return state.mark_delivered(
        store,
        f"final:{turn_id}:{content_hash}",
        {
            "worker_id": compact_ws(item.get("worker_id"), 160),
            "turn_id": turn_id,
            "suppressed": "historical_same_worker_turn",
        },
    )



_FOLD_ATTEMPT_CAP = 3
_FOLD_PASS_CAP = 3


def _fold_superseded_final(
    store: dict[str, Any],
    item: dict[str, Any],
    entry: dict[str, Any],
    runtime: SyncRuntime,
    *,
    chat_id: str,
    fold_state: dict[str, int] | None = None,
) -> bool:
    """Collapse the Response of a SUPERSEDED final (opt-in via
    HERDR_TELEGRAM_TOPICS_RESPONSE_COLLAPSE_PREVIOUS): re-render the previously delivered message with
    collapse_response=True so only the newest answer stays expanded. Runs in the historical-final
    branch of _sync_turns, which sees every non-latest completed final WITH its content each sync (the
    store retains a short per-worker turn history) — a self-healing sweep, no extra text persisted.
    Idempotent via binding["folded"]; bounded by _FOLD_ATTEMPT_CAP; single-message finals only (a
    split final has no single message to re-render). Never touches the latest delivery."""
    if runtime.dry_run or not config.response_collapse_previous_default():
        return False
    if fold_state is not None and fold_state.get("issued", 0) >= _FOLD_PASS_CAP:
        return False  # per-pass edit budget spent; the sweep is self-healing, rest folds next ticks
    if not str(item.get("assistant_final_text") or "").strip():
        return False
    bindings = _final_delivery_bindings(store, _turn_id(item))
    if len(bindings) != 1:
        return False
    message_id, binding = bindings[0]
    if not message_id or binding.get("folded") or int(binding.get("fold_attempts") or 0) >= _FOLD_ATTEMPT_CAP:
        return False
    if str(message_id) == str(entry.get("last_clean_message_id") or ""):
        return False  # belt-and-braces: never fold the latest delivered message
    telegram = _telegram_state(store)
    api_token, bot_kind = _delivery_bot(store, entry)
    # Only fold when the binding ITSELF records which bot sent the message. Guessing from
    # last_clean_bot_kind describes the LATEST delivery's bot, not this old message's — a wrong-bot
    # edit 404s and would falsely mark the fold done.
    stored_bot_kind = str(binding.get("bot_kind") or "")
    if not stored_bot_kind or stored_bot_kind != bot_kind:
        return False
    folded_item = dict(turn_item_from_source(item, entry))
    folded_item["collapse_response"] = True
    # Same oversize/split guards as _replace_changed_final: never let a cosmetic fold degrade a rich
    # message through the too-large -> legacy-plain fallback.
    if len(render_feed_item_html(folded_item)) > MESSAGE_TEXT_LIMIT or feed_item_requires_send_split(folded_item):
        return False
    if fold_state is not None:
        fold_state["issued"] = fold_state.get("issued", 0) + 1
    try:
        sent = edit_feed_item(
            runtime.telegram,
            chat_id,
            message_id,
            folded_item,
            telegram=telegram,
            api_token=api_token,
        )
    except Exception as exc:  # a rate-limit/transport blip must not abort the sync pass
        print(f"herdres fold edit failed: {exc}", file=sys.stderr)
        binding["fold_attempts"] = int(binding.get("fold_attempts") or 0) + 1
        return True
    error = str(sent.get("error") or "").lower()
    if sent.get("ok") or "not found" in error or _topic_missing(sent.get("error")):
        binding["folded"] = True  # done (or the message/topic is gone — nothing left to fold)
        return True
    binding["fold_attempts"] = int(binding.get("fold_attempts") or 0) + 1
    return True

def _ensure_topic(
    store: dict[str, Any],
    source: dict[str, Any],
    entry: dict[str, Any],
    runtime: SyncRuntime,
    *,
    chat_id: str,
    can_create: bool = True,
) -> tuple[bool, bool]:
    if entry.get("topic_id"):
        return False, False
    reused = state.find_legacy_topic_id_by_name(store, entry.get("topic_name") or "")
    if reused:
        entry["topic_id"] = reused
        return False, False
    if runtime.dry_run:
        return True, False
    if not can_create:
        return True, False   # real create deferred by the per-pass create cap; retry next tick
    topic_name = entry.get("topic_name") or state.topic_name_for_space(source)
    created = runtime.telegram.create_topic(chat_id, topic_name, icon_color=topic_color_for_name(topic_name))
    if created.get("ok") and created.get("topic_id"):
        entry["topic_id"] = str(created["topic_id"])
        return True, True
    entry["last_topic_error"] = compact_ws(created.get("error"), 240)
    return False, False


_ALERT_STATUSES = frozenset({"attention", "failed"})
_RESERVED_STATUS_EMOJIS = frozenset({"\u2753", "\u203c\ufe0f", "\u2705", "\u26a1\ufe0f", "\u2615\ufe0f"})


def _identity_topic_icon(store: dict[str, Any], entry: dict[str, Any], runtime: SyncRuntime) -> tuple[str, str]:
    """Deterministic per-topic identity icon from the allowed forum icon set."""
    catalog = topic_icon_catalog(store, runtime.telegram)
    choices = sorted(emoji for emoji in catalog if emoji not in _RESERVED_STATUS_EMOJIS)
    if not choices:
        return "", ""
    key = compact_ws(entry.get("topic_name") or entry.get("topic_id"), 80)
    emoji = choices[int(short_hash({"topic_icon": key}, 8), 16) % len(choices)]
    return emoji, catalog.get(emoji, "")


def topic_color_for_name(name: str) -> int:
    return TOPIC_ICON_COLORS[int(short_hash({"topic_color": compact_ws(name, 80)}, 8), 16) % len(TOPIC_ICON_COLORS)]


def _sync_topic_icon(store: dict[str, Any], entry: dict[str, Any], runtime: SyncRuntime, *, chat_id: str) -> bool:
    """Alert-only status icons: flip to attention/failed markers, restore the
    topic's stable identity icon on recovery, and never churn icons (which post
    unread-generating service messages) for routine working/idle transitions."""
    if not config.topic_status_icons_enabled():
        return False
    thread_id = str(entry.get("topic_id") or "")
    if not thread_id:
        return False
    status = normalized_status(entry.get("status") or entry.get("tendwire_status_line"))
    current = str(entry.get("last_topic_icon") or "")
    if status in _ALERT_STATUSES:
        emoji = status_emoji(status)
        emoji_id = topic_icon_id(store, emoji, runtime.telegram)
    else:
        if current and current not in _RESERVED_STATUS_EMOJIS and entry.get("last_topic_icon_id"):
            return False
        emoji, emoji_id = _identity_topic_icon(store, entry, runtime)
        if not emoji:
            return False
    if not emoji_id:
        entry["last_topic_icon_missing"] = emoji
        return False
    if entry.get("last_topic_icon") == emoji and entry.get("last_topic_icon_id") == emoji_id:
        return False
    if runtime.dry_run:
        entry["last_topic_icon"] = emoji
        entry["last_topic_icon_id"] = emoji_id
        entry.pop("last_topic_icon_missing", None)
        return True
    result = runtime.telegram.edit_topic_icon(chat_id, thread_id, emoji_id)
    if result.get("ok") or _topic_not_modified(result.get("error")):
        entry["last_topic_icon"] = emoji
        entry["last_topic_icon_id"] = emoji_id
        entry.pop("last_topic_icon_missing", None)
        entry.pop("last_topic_icon_error", None)
        return True
    entry["last_topic_icon_error"] = compact_ws(result.get("error"), 240)
    return False


def _legacy_pinned_message_id_for_topic(store: dict[str, Any], topic_id: str) -> str:
    if not topic_id:
        return ""
    spaces = store.get("spaces") if isinstance(store.get("spaces"), dict) else {}
    for entry in spaces.values():
        if not isinstance(entry, dict):
            continue
        if str(entry.get("topic_id") or "") != topic_id:
            continue
        message_id = str(entry.get("pinned_status_message_id") or "")
        if message_id:
            return message_id
    return ""


def _record_topic_pinned_status(entry: dict[str, Any], *, message_id: str, content_hash: str, pinned: bool = False) -> None:
    entry["pinned_status_message_id"] = str(message_id)
    entry["pinned_status_hash"] = content_hash
    if pinned:
        entry["pinned_status_pinned"] = True
    entry.pop("pinned_status_last_error", None)


def _entry_open_for_pin(entry: dict[str, Any]) -> bool:
    raw_status = str(entry.get("status") or entry.get("tendwire_raw_status") or entry.get("tendwire_status_line") or "").strip().lower().replace("-", "_")
    if raw_status in {"closed", "exited"}:
        return False
    status = normalized_status(raw_status)
    if status in {"closed", "failed"}:
        return False
    return not (entry.get("closed") or entry.get("exited") or entry.get("process_exited"))


def _status_entries_for_topic_pin(store: dict[str, Any], entry: dict[str, Any]) -> list[dict[str, Any]]:
    if str(entry.get("entry_type") or "") != "space":
        return [entry] if _entry_open_for_pin(entry) else []
    space_id = str(entry.get("tendwire_space_id") or entry.get("space_id") or "")
    worker_ids = entry.get("worker_ids")
    current_worker_ids = {str(worker_id) for worker_id in worker_ids if worker_id} if isinstance(worker_ids, list) else set()
    workers = [
        worker_entry
        for worker_entry in state.source_worker_entries(store).values()
        if _entry_open_for_pin(worker_entry)
        and str(worker_entry.get("tendwire_space_id") or worker_entry.get("space_id") or "") == space_id
        and (
            not current_worker_ids
            or str(worker_entry.get("tendwire_worker_id") or worker_entry.get("worker_id") or "") in current_worker_ids
        )
    ]
    return workers or ([entry] if _entry_open_for_pin(entry) else [])


def _sync_topic_pinned(store: dict[str, Any], entry: dict[str, Any], runtime: SyncRuntime, *, chat_id: str) -> bool:
    thread_id = str(entry.get("topic_id") or "")
    if not thread_id:
        return False
    html = render_status_overview(_status_entries_for_topic_pin(store, entry))
    content_hash = short_hash(html, 20)
    message_id = str(entry.get("pinned_status_message_id") or "") or _legacy_pinned_message_id_for_topic(store, thread_id)
    if message_id and entry.get("pinned_status_hash") == content_hash and entry.get("pinned_status_pinned"):
        return False
    if runtime.dry_run:
        _record_topic_pinned_status(entry, message_id=message_id or "0", content_hash=content_hash, pinned=True)
        return True
    sent: dict[str, Any]
    if message_id:
        sent = runtime.telegram.edit_message(chat_id, message_id, html)
        if sent.get("ok"):
            pass
        elif _message_missing(sent.get("error")):
            entry.pop("pinned_status_message_id", None)
            message_id = ""
        elif _topic_missing(sent.get("error")):
            entry["pinned_status_last_error"] = compact_ws(sent.get("error"), 240)
            return False
        else:
            entry["pinned_status_last_error"] = compact_ws(sent.get("error"), 240)
            return False
    if not message_id:
        sent = runtime.telegram.send_message(chat_id, html, thread_id=thread_id, notify=False)
        if not sent.get("ok"):
            entry["pinned_status_last_error"] = compact_ws(sent.get("error"), 240)
            return False
        message_id = str(sent.get("message_id") or "")
        if not message_id:
            entry["pinned_status_last_error"] = "Telegram returned no message id for topic pinned status"
            return False
    pin_result = runtime.telegram.pin_message(chat_id, message_id)
    pinned = bool(pin_result.get("ok"))
    _record_topic_pinned_status(entry, message_id=message_id, content_hash=content_hash, pinned=pinned)
    if not pinned:
        entry["pinned_status_pin_error"] = compact_ws(pin_result.get("error"), 240)
    else:
        entry.pop("pinned_status_pin_error", None)
    return True


_RENAME_ATTEMPT_CAP = 3


def _assign_worker_topic_names(store: dict[str, Any], workers: list[dict[str, Any]]) -> dict[str, str]:
    """Map each not-yet-topiced worker id -> a unique topic name (cwd basename, numbered on collision).
    Names already bound to a created topic are reserved and never renumbered, so numbers stay stable as
    panes come and go. Ordered by worker id for deterministic numbering."""
    # Reserved names are compared case-INSENSITIVELY to match _ensure_topic's reuse lookup
    # (find_legacy_topic_id_by_name uses .casefold()); otherwise "Foo"/"foo" would look distinct here
    # yet still collapse into one topic there.
    def _is_variant_of(current: str, base: str) -> bool:
        # "gitmoot" and "gitmoot 3" are both variants of base "gitmoot" — keep them (stable numbering).
        cur, b = current.casefold(), base.casefold()
        return cur == b or (cur.startswith(b + " ") and current[len(base) + 1 :].strip().isdigit())

    entries = state.source_worker_entries(store)
    # EVERY existing topic name starts reserved (absent/closed workers' topics included — a new pane
    # must never collide with them, or _ensure_topic's reuse-by-name would collapse into their topic).
    reserved: set[str] = set()
    for entry in entries.values():
        if entry.get("topic_id") and entry.get("topic_name"):
            reserved.add(compact_ws(entry.get("topic_name"), 120).casefold())
    keeps: dict[str, bool] = {}
    ordered = sorted(workers, key=lambda w: compact_ws(w.get("id"), 160))
    for worker in ordered:
        wid = compact_ws(worker.get("id"), 160)
        key = state.find_entry_key_by_worker(store, wid) if wid else None
        existing = entries.get(key) if key is not None else None
        if not existing or not existing.get("topic_id") or not existing.get("topic_name"):
            continue
        current = compact_ws(existing.get("topic_name"), 120)
        keep = _is_variant_of(current, state.topic_name_for_worker(worker))
        keeps[wid] = keep
        # NOTE: the old name stays RESERVED even when a rename is proposed — freeing it mid-pass
        # would let a new pane take it and collide into this topic via _ensure_topic's reuse-by-name
        # (two live panes sharing one topic). It frees naturally on the pass AFTER the rename lands.
    assigned: dict[str, str] = {}
    renames: dict[str, str] = {}
    for worker in ordered:
        wid = compact_ws(worker.get("id"), 160)
        if not wid:
            continue
        key = state.find_entry_key_by_worker(store, wid)
        existing = entries.get(key) if key is not None else None
        has_topic = bool(existing and existing.get("topic_id"))
        if has_topic and keeps.get(wid, True):
            continue  # topic name still matches its desired base; locked
        if has_topic and not _worker_is_open(worker):
            continue  # never rename a closed pane's topic (and never burn budget on it)
        if has_topic and int((existing or {}).get("rename_attempts") or 0) >= _RENAME_ATTEMPT_CAP:
            continue  # permanently-failing rename: stop proposing (no per-pass budget burn)
        base = state.topic_name_for_worker(worker)
        name, n = base, 2
        while name.casefold() in reserved:
            name = f"{base} {n}"
            n += 1
        reserved.add(name.casefold())
        if has_topic:
            renames[wid] = name   # desired name changed (e.g. the pane label appeared) -> rename in place
        else:
            assigned[wid] = name
    return assigned, renames


def _sync_sources(
    store: dict[str, Any],
    snapshot: dict[str, Any],
    turns_payload: dict[str, Any],
    runtime: SyncRuntime,
    *,
    chat_id: str,
) -> dict[str, int]:
    counts = {"created": 0, "updated": 0, "panes": 0, "spaces": 0, "icon_updated": 0}
    topic_mode = config.source_topic_mode()
    # Bound real topic-create calls per pass so a first source sync (a topic per open worker/space)
    # amortizes creation over ticks instead of one create burst under the state lock.
    create_cap = config.source_topic_create_cap()
    creates_issued = 0
    # One topic per pane, named by the pane label (else cwd basename); disambiguate same-name panes
    # ("gitmoot", "gitmoot 2"). Existing topics keep their name while it still matches the desired
    # base; when the desired name CHANGES (a pane label appeared/changed), the topic is renamed in
    # place (bounded per pass) so history is preserved.
    worker_topic_names: dict[str, str] = {}
    worker_topic_renames: dict[str, str] = {}
    if topic_mode == "worker":
        worker_topic_names, worker_topic_renames = _assign_worker_topic_names(store, _workers(snapshot))
    renames_issued = 0
    # Latest model per worker from the turn rows (recency-ordered: first non-empty wins). Stamped
    # cache-and-keep so an idle pane keeps showing its last-known model on the pinned board.
    model_by_worker: dict[str, str] = {}
    for row in _turns(turns_payload):
        row_wid = compact_ws(row.get("worker_id"), 160)
        row_model = compact_ws(row.get("model"), 80)
        if row_wid and row_model and row_wid not in model_by_worker:
            model_by_worker[row_wid] = row_model
    live_worker_ids = {
        compact_ws(worker.get("id"), 160)
        for worker in _workers(snapshot)
        if _worker_is_open(worker)
    }
    live_worker_ids.discard("")
    turn_status_by_worker, turn_status_by_space = _turn_activity_statuses(turns_payload, live_worker_ids)
    spaces = {compact_ws(item.get("id"), 160): item for item in _spaces(snapshot) if compact_ws(item.get("id"), 160)}
    workers_by_space: dict[str, list[dict[str, Any]]] = {}
    for worker in _workers(snapshot):
        space_id = compact_ws(worker.get("space_id"), 160)
        existing_key = state.find_entry_key_by_worker(store, compact_ws(worker.get("id"), 160))
        before = dict(state.source_worker_entries(store).get(existing_key) or {}) if existing_key is not None else {}
        _key, entry, created = state.upsert_worker_entry(store, worker)
        entry["status"] = _effective_worker_status(worker, turn_status_by_worker)
        _stamp_managed_voice(entry, _space_voice_mode(store, space_id))
        # Apply the cwd-based, disambiguated name before the topic is created (once it has a topic_id
        # the name is locked, so a later renumber can't rename an existing topic).
        wid = compact_ws(worker.get("id"), 160)
        if not entry.get("topic_id") and wid in worker_topic_names:
            entry["topic_name"] = worker_topic_names[wid]
        elif (
            entry.get("topic_id")
            and wid in worker_topic_renames
            and not runtime.dry_run
            and renames_issued < create_cap
        ):
            renamed = runtime.telegram.rename_topic(chat_id, str(entry["topic_id"]), worker_topic_renames[wid])
            renames_issued += 1
            if renamed.get("ok"):
                entry["topic_name"] = worker_topic_renames[wid]
                entry.pop("rename_attempts", None)
            elif _topic_missing(renamed.get("error")):
                # the topic is gone (hand-deleted): drop the mapping so _ensure_topic recreates it
                # under the new name instead of renaming a ghost forever.
                entry.pop("topic_id", None)
                entry["topic_name"] = worker_topic_renames[wid]
                entry.pop("rename_attempts", None)
            else:
                entry["rename_attempts"] = int(entry.get("rename_attempts") or 0) + 1
        model = model_by_worker.get(wid)
        if model:
            entry["model"] = model
        counts["created"] += int(created)
        counts["updated"] += int(not created and before != entry)
        if not _worker_is_open(worker):
            continue
        if space_id:
            workers_by_space.setdefault(space_id, []).append(worker)
        if topic_mode == "worker" and not _should_delete_done_council_topic(entry):
            topic_needed, topic_created = _ensure_topic(
                store, worker, entry, runtime, chat_id=chat_id, can_create=creates_issued < create_cap
            )
            creates_issued += int(topic_created)
            counts["created"] += int(topic_created or topic_needed)
            counts["icon_updated"] += int(_sync_topic_icon(store, entry, runtime, chat_id=chat_id))
        counts["panes"] += 1

    for space_id, workers in workers_by_space.items():
        if space_id not in spaces:
            spaces[space_id] = {"id": space_id, "name": space_id, "status": "unknown"}

    seen_space_keys: set[str] = set()
    if topic_mode == "worker":
        return counts

    for space_id, space in spaces.items():
        if not _space_is_open(space):
            continue
        selectable = [worker for worker in workers_by_space.get(space_id, []) if _worker_is_open(worker)]
        if not selectable:
            continue
        existing_key = state.find_entry_key_by_space(store, space_id)
        before = dict(state.source_space_entries(store).get(existing_key) or {}) if existing_key is not None else {}
        _key, entry, created = state.upsert_space_entry(store, space)
        if not entry.get("voice_mode"):
            entry["voice_mode"] = _default_voice_mode()
        _stamp_managed_voice(entry, _entry_voice_mode(entry))
        selected = _select_space_worker(selectable, turn_status_by_worker)
        seen_space_keys.add(_key)
        selected_status = _effective_worker_status(selected, turn_status_by_worker) if selected else ""
        space_turn_status = turn_status_by_space.get(space_id) or ""
        entry["status"] = _dominant_status(space_turn_status, selected_status, _source_status(space.get("status")))
        entry["worker_count"] = len(selectable)
        entry["worker_ids"] = [compact_ws(worker.get("id"), 160) for worker in selectable if compact_ws(worker.get("id"), 160)]
        if selected:
            entry["active_worker_id"] = compact_ws(selected.get("id"), 160)
            entry["active_worker_fingerprint"] = compact_ws(selected.get("fingerprint"), 160)
            entry["active_worker_name"] = compact_ws(selected.get("name"), 80)
            selected_model = model_by_worker.get(compact_ws(selected.get("id"), 160))
            if selected_model:
                entry["active_worker_model"] = selected_model
            entry["active_worker_status"] = _dominant_status(space_turn_status, selected_status)
        topic_needed, topic_created = _ensure_topic(
            store, space, entry, runtime, chat_id=chat_id, can_create=creates_issued < create_cap
        )
        creates_issued += int(topic_created)
        counts["created"] += int(created or topic_created or topic_needed)
        counts["updated"] += int(not created and before != entry)
        counts["icon_updated"] += int(_sync_topic_icon(store, entry, runtime, chat_id=chat_id))
        counts["spaces"] += 1
    for key in list(state.source_space_entries(store)):
        if key not in seen_space_keys:
            state.source_space_entries(store)[key]["stale_space_topic"] = True
    return counts


def _cleanup_topics(store: dict[str, Any], runtime: SyncRuntime, *, chat_id: str) -> dict[str, Any]:
    result = {"deleted": 0, "failed": 0, "pruned": 0, "changed": False}
    visible_space_topics = {
        str(entry.get("topic_id"))
        for entry in state.source_space_entries(store).values()
        if entry.get("topic_id")
    }
    panes = store.get("panes") if isinstance(store.get("panes"), dict) else {}
    audit = store.setdefault("telegram_deleted_topics", [])
    deleted_topic_ids: set[str] = set()
    # Bound real topic-delete calls per pass so a first source sync (which can reclassify many legacy
    # per-worker topics at once) amortizes the deletes over ticks instead of one burst under the lock.
    delete_cap = config.source_orphan_delete_cap()
    deletes_issued = 0

    def clear_worker_topic_refs(topic_id: str, reason: str) -> None:
        for worker_key, worker_entry in list(state.source_worker_entries(store).items()):
            if str(worker_entry.get("topic_id") or "") != topic_id:
                continue
            if _should_delete_done_council_topic(worker_entry):
                panes.pop(worker_key, None)
                continue
            worker_entry.pop("topic_id", None)
            worker_entry["deleted_topic_id"] = topic_id
            worker_entry["deleted_topic_reason"] = reason
    for key, entry in list(state.source_worker_entries(store).items()):
        topic_id = str(entry.get("topic_id") or "")
        if not topic_id:
            continue
        stale_worker_topic = config.source_topic_mode() == "space" and topic_id not in visible_space_topics
        done_council_topic = _should_delete_done_council_topic(entry) and (
            config.source_topic_mode() == "worker" or topic_id not in visible_space_topics
        )
        if not stale_worker_topic and not done_council_topic:
            continue
        reason = "done_council_topic" if done_council_topic else "stale_worker_topic"
        if runtime.dry_run:
            if topic_id not in deleted_topic_ids:
                result["deleted"] += 1
                deleted_topic_ids.add(topic_id)
            result["changed"] = True
            continue
        if deletes_issued >= delete_cap:
            continue  # per-pass delete budget spent; retry this topic next tick (record untouched)
        deletes_issued += 1
        deleted = runtime.telegram.delete_topic(chat_id, topic_id)
        if not deleted.get("ok"):
            if _topic_missing(deleted.get("error")):
                result["changed"] = True
                if done_council_topic:
                    panes.pop(key, None)
                else:
                    entry.pop("topic_id", None)
                    entry["deleted_topic_id"] = topic_id
                    entry["deleted_topic_reason"] = reason
                continue
            result["failed"] += 1
            entry["last_topic_delete_error"] = compact_ws(deleted.get("error"), 240)
            continue
        result["deleted"] += 1
        deleted_topic_ids.add(topic_id)
        result["changed"] = True
        audit.append({"topic_id": topic_id, "name": compact_ws(entry.get("topic_name"), 120), "reason": reason})
        if done_council_topic:
            panes.pop(key, None)
        else:
            entry.pop("topic_id", None)
            entry["deleted_topic_id"] = topic_id
            entry["deleted_topic_reason"] = reason
    spaces = store.get("spaces") if isinstance(store.get("spaces"), dict) else {}
    for key, entry in list(state.source_space_entries(store).items()):
        if not entry.get("stale_space_topic"):
            continue
        topic_id = str(entry.get("topic_id") or "")
        should_delete = config.delete_done_council_topics() and _entry_is_council_topic(entry) and bool(topic_id)
        if should_delete and not runtime.dry_run and topic_id not in deleted_topic_ids and deletes_issued >= delete_cap:
            continue  # budget spent; retry this space's delete+prune next tick (record untouched)
        if should_delete and topic_id not in deleted_topic_ids:
            if runtime.dry_run:
                result["deleted"] += 1
                deleted_topic_ids.add(topic_id)
                result["changed"] = True
                continue
            deletes_issued += 1
            deleted = runtime.telegram.delete_topic(chat_id, topic_id)
            if not deleted.get("ok"):
                if _topic_missing(deleted.get("error")):
                    clear_worker_topic_refs(topic_id, "done_council_space_topic")
                    spaces.pop(key, None)
                    result["pruned"] += 1
                    result["changed"] = True
                    continue
                result["failed"] += 1
                entry["last_topic_delete_error"] = compact_ws(deleted.get("error"), 240)
                continue
            result["deleted"] += 1
            deleted_topic_ids.add(topic_id)
            audit.append({"topic_id": topic_id, "name": compact_ws(entry.get("topic_name"), 120), "reason": "done_council_space_topic"})
        if not runtime.dry_run:
            if should_delete:
                clear_worker_topic_refs(topic_id, "done_council_space_topic")
            spaces.pop(key, None)
            result["pruned"] += 1
        result["changed"] = True
    store["telegram_deleted_topics"] = audit[-200:]
    return result


def _clear_entry_message_reference(entry: dict[str, Any], message_id: str, kind: str) -> bool:
    changed = False
    if kind == "working" and str(entry.get("last_stream_message_id") or "") == message_id:
        changed = _clear_stream_delivery_keys(entry)
    if kind == "final":
        message_ids = entry.get("last_clean_message_ids")
        if isinstance(message_ids, list) and message_id in {str(item) for item in message_ids}:
            kept = [str(item) for item in message_ids if str(item) != message_id]
            if kept:
                entry["last_clean_message_ids"] = kept
                entry["last_clean_message_id"] = kept[0]
            else:
                _clear_final_delivery_keys(entry)
            changed = True
        elif str(entry.get("last_clean_message_id") or "") == message_id:
            changed = _clear_final_delivery_keys(entry) or changed
    return changed


def _repair_space_mode_routing_state(store: dict[str, Any]) -> int:
    if config.source_topic_mode() != "space":
        return 0
    repaired = 0
    topic_by_space = _source_space_topic_ids(store)
    for entry in state.source_worker_entries(store).values():
        space_id = _entry_space_id(entry)
        expected_topic = topic_by_space.get(space_id)
        actual_topic = compact_ws(entry.get("topic_id"), 80)
        if expected_topic and actual_topic and actual_topic != expected_topic:
            entry.pop("topic_id", None)
            repaired += 1
    bindings = state.message_bindings(store)
    for message_id, binding in list(bindings.items()):
        if not isinstance(binding, dict):
            continue
        space_id = compact_ws(binding.get("space_id"), 160)
        expected_topic = topic_by_space.get(space_id)
        actual_topic = compact_ws(binding.get("topic_id"), 80)
        if not expected_topic or not actual_topic or actual_topic == expected_topic:
            continue
        worker_id = compact_ws(binding.get("worker_id"), 160)
        kind = str(binding.get("kind") or "")
        for entry in state.source_worker_entries(store).values():
            if _entry_worker_id(entry) == worker_id and _entry_space_id(entry) == space_id:
                repaired += int(_clear_entry_message_reference(entry, str(message_id), kind))
        bindings.pop(str(message_id), None)
        repaired += 1
    return repaired


def _backfill_message_bindings(store: dict[str, Any]) -> int:
    before = set(state.message_bindings(store))
    for entry in state.source_worker_entries(store).values():
        topic_id = str(entry.get("topic_id") or "")
        if not topic_id:
            _space_key, space_entry = state.find_space_entry_by_id(store, str(entry.get("tendwire_space_id") or entry.get("space_id") or ""))
            topic_id = str((space_entry or {}).get("topic_id") or "")
        if not topic_id:
            continue
        stream_id = str(entry.get("last_stream_message_id") or "")
        if stream_id:
            state.bind_message_to_worker(
                store,
                stream_id,
                entry,
                topic_id=topic_id,
                kind="working",
                turn_id=str(entry.get("last_stream_turn_id") or ""),
                bot_kind=str(entry.get("last_stream_bot_kind") or ""),
            )
        final_ids = entry.get("last_clean_message_ids")
        if not isinstance(final_ids, list) or not final_ids:
            final_ids = [entry.get("last_clean_message_id")]
        for message_id in final_ids:
            state.bind_message_to_worker(
                store,
                message_id,
                entry,
                topic_id=topic_id,
                kind="final",
                turn_id=str(entry.get("last_turn_id") or ""),
                bot_kind=str(entry.get("last_clean_bot_kind") or ""),
            )
    return len(set(state.message_bindings(store)) - before)


def _deliver_working(store: dict[str, Any], item: dict[str, Any], entry: dict[str, Any], runtime: SyncRuntime, *, chat_id: str) -> bool:
    thread_id = str(entry.get("topic_id") or "")
    if not thread_id:
        return False
    delivery_item = _working_delivery_item(item)
    turn_id = _turn_id(item)
    content_hash = _turn_content_hash(delivery_item, "working")
    feed_item = turn_item_from_source(delivery_item, entry)
    if entry.get("last_stream_turn_id") == turn_id and entry.get("last_stream_hash") == content_hash:
        return False
    now = time.time()
    if _same_turn_working_update_too_soon(entry, turn_id, now=now):
        return False
    if runtime.dry_run:
        _set_stream_delivery(entry, turn_id=turn_id, content_hash=content_hash, placeholder=True)
        _record_stream_update_time(entry, now)
        return True
    telegram = _telegram_state(store)
    api_token, bot_kind = _delivery_bot(store, entry)
    stored_bot_kind = str(entry.get("last_stream_bot_kind") or MANAGER_BOT_KIND)
    if entry.get("last_stream_message_id") and entry.get("last_stream_turn_id") == turn_id and stored_bot_kind == bot_kind:
        sent = edit_feed_item(
            runtime.telegram,
            chat_id,
            str(entry["last_stream_message_id"]),
            feed_item,
            telegram=telegram,
            live=True,
            api_token=api_token,
        )
    else:
        sent = send_feed_item(
            runtime.telegram,
            chat_id,
            feed_item,
            telegram=telegram,
            thread_id=thread_id,
            notify=False,
            live=True,
            api_token=api_token,
        )
    if sent.get("ok"):
        _set_stream_delivery(
            entry,
            turn_id=turn_id,
            content_hash=content_hash,
            message_id=str(sent.get("message_id") or entry.get("last_stream_message_id") or ""),
            bot_kind=bot_kind,
        )
        _record_stream_update_time(entry, now)
        _record_delivery_success(entry, bot_kind)
        state.bind_message_to_worker(store, entry.get("last_stream_message_id"), entry, topic_id=thread_id, kind="working", turn_id=turn_id, bot_kind=bot_kind)
        return True
    _record_delivery_error(entry, sent, bot_kind)
    return False


def _refind_entry(store: dict[str, Any], entry_key: str | None) -> dict[str, Any] | None:
    if not entry_key:
        return None
    for bucket in ("panes", "spaces"):
        candidate = (store.get(bucket) or {}).get(entry_key)
        if isinstance(candidate, dict):
            return candidate
    return None


def _speak_reply(
    store: dict[str, Any],
    item: dict[str, Any],
    entry: dict[str, Any],
    entry_key: str | None,
    runtime: SyncRuntime,
    *,
    chat_id: str,
    thread_id: str,
    reply_to: str | None,
) -> dict[str, Any]:
    """Strictly additive (issue #4): after a final text turn is delivered, optionally speak it back as
    one or more Telegram voice notes (long replies are chunked). Fires on the one-shot speak_next_reply
    (owner replied to a voice note), the trigger phrase, or force-all. Never breaks the delivered text
    turn. Returns the entry to keep using (re-derived when we reload off-lock).

    Phase 2: SYNTHESIS + SEND run OFF the state lock. We commit the delivered turn first, drop the lock
    for the ~1-3s synth (no `store` mutation in that window), then reload — so a competitor's write
    during synth survives — and record the sent voice-note ids on the freshly-reloaded entry."""
    if runtime.dry_run:
        return entry  # preview pass: don't consume the flag or synth; the real send speaks
    want = bool(entry.pop("speak_next_reply", None))
    if not (want or speech.speech_reply_triggered(item.get("user_text")) or speech.speech_replies_enabled()):
        return entry
    chunks = speech.speech_reply_chunks(item.get("assistant_final_text") or item.get("assistant_stream_text") or "")
    if not chunks:
        return entry
    api_token, _bot_kind = _delivery_bot(store, entry)
    client = runtime.telegram.with_token(api_token) if api_token else runtime.telegram

    def _synth_and_send() -> list[str]:
        # Runs OFF the lock: synth to OGG + upload. Touches no `store` state (so a competitor holding
        # the lock meanwhile can't be clobbered); the returned ids are recorded after we re-acquire.
        ids: list[str] = []
        for i, chunk in enumerate(chunks):
            try:
                dest = speech.outbound_speech_dir(prune=(i == 0)) / f"reply-{short_hash({'t': _turn_id(item), 'i': i, 'h': chunk}, 16)}.ogg"
                if not speech.speech_request("tts", {"text": chunk, "dest": str(dest)}).get("ok"):
                    continue
                sent = client.send_voice(
                    chat_id, dest, thread_id=thread_id,
                    reply_to_message_id=(reply_to if i == 0 else None), notify=False,
                )
                if sent.get("ok") and sent.get("message_id"):
                    ids.append(str(sent.get("message_id")))
            except Exception as exc:  # one chunk failing must not abort the rest or the text turn
                print(f"herdres speak-reply chunk failed: {exc}", file=sys.stderr)
        return ids

    if not state.lock_held():
        # No lock to release (tests / dry callers): synth+send inline and record on the given entry.
        for vid in _synth_and_send():
            state.record_voice_reply_message_id(entry, vid)
        return entry

    # Commit the delivered turn, synth+send OFF the lock, then reload and record on the fresh entry.
    state.save_state(store)
    with state.released_lock():
        voice_ids = _synth_and_send()
    fresh = state.load_state()
    store.clear()
    store.update(fresh)
    target = _refind_entry(store, entry_key)
    if target is None:
        # A competitor pruned this entry during the off-lock synth. The notes were sent, but recording
        # their ids on the detached pre-reload entry wouldn't persist (save_state writes `store`), so
        # skip it — leave a breadcrumb rather than silently drop the tracking.
        if voice_ids:
            print(f"herdres speak-reply: entry {entry_key} gone after off-lock synth; "
                  f"{len(voice_ids)} voice id(s) unrecorded", file=sys.stderr)
        return entry
    for vid in voice_ids:
        state.record_voice_reply_message_id(target, vid)
    if voice_ids:
        state.save_state(store)
    return target


def _deliver_final(store: dict[str, Any], item: dict[str, Any], entry: dict[str, Any], runtime: SyncRuntime, *, chat_id: str) -> bool:
    thread_id = str(entry.get("topic_id") or "")
    if not thread_id:
        return False
    turn_id = _turn_id(item)
    content_hash = _turn_content_hash(item, "final")
    identity = f"final:{turn_id}:{content_hash}"
    if identity in state.delivered_turns(store):
        _repair_delivered_final_entry(store, item, entry, content_hash)
        return False
    feed_item = turn_item_from_source(item, entry)
    if runtime.dry_run:
        state.mark_delivered(store, identity, {"worker_id": entry.get("tendwire_worker_id"), "turn_id": turn_id})
        _set_final_delivery(
            entry,
            turn_id=turn_id,
            content_hash=content_hash,
            user_hash=_turn_user_hash(item),
            render_version=RENDER_VERSION,
            placeholder=True,
        )
        return True
    send_changed_as_new = _changed_final_should_send_new_message(item, entry)
    if _final_turn_delivered(store, turn_id):
        if not send_changed_as_new and entry.get("last_clean_hash") == content_hash and _replace_changed_final(
            store,
            item,
            entry,
            runtime,
            chat_id=chat_id,
            thread_id=thread_id,
            content_hash=content_hash,
            identity=identity,
        ):
            return True
        if not send_changed_as_new:
            _repair_delivered_final_entry(store, item, entry, content_hash)
            return False
    if not send_changed_as_new and _replace_changed_final(
        store,
        item,
        entry,
        runtime,
        chat_id=chat_id,
        thread_id=thread_id,
        content_hash=content_hash,
        identity=identity,
    ):
        return True
    if _promote_working_to_final(
        store,
        item,
        entry,
        runtime,
        chat_id=chat_id,
        thread_id=thread_id,
        content_hash=content_hash,
        identity=identity,
    ):
        return True
    telegram = _telegram_state(store)
    api_token, bot_kind = _delivery_bot(store, entry)
    sent = send_feed_item(
        runtime.telegram,
        chat_id,
        feed_item,
        telegram=telegram,
        thread_id=thread_id,
        notify=False,
        api_token=api_token,
    )
    if not sent.get("ok"):
        _record_delivery_error(entry, sent, bot_kind)
        return False
    message_ids: list[str] = []
    for message_id in split_legacy_message_ids(sent):
        message_ids.append(message_id)
    _record_final_delivery_success(
        store,
        item,
        entry,
        thread_id=thread_id,
        message_ids=message_ids,
        content_hash=content_hash,
        identity=identity,
        bot_kind=bot_kind,
    )
    return True


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
    api_token, bot_kind = _delivery_bot(store, entry)
    client = runtime.telegram.with_token(api_token) if api_token else runtime.telegram
    sent = client.send_message(chat_id, html, thread_id=thread_id, notify=True)
    if sent.get("ok"):
        entry["last_prompt_bot_kind"] = bot_kind
        _record_delivery_success(entry, bot_kind)
        state.bind_message_to_worker(store, sent.get("message_id"), entry, topic_id=thread_id, kind="pending", turn_id=pending_id, bot_kind=bot_kind)
        return state.mark_delivered(store, identity, {"worker_id": entry.get("tendwire_worker_id"), "pending_id": pending_id})
    _record_delivery_error(entry, sent, bot_kind)
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
            _set_final_delivery(entry, turn_id=turn_id, content_hash=content_hash, placeholder=True)
            skipped += 1
            continue
        if item.get("assistant_stream_text"):
            _set_stream_delivery(entry, turn_id=turn_id, content_hash=_turn_content_hash(item, "working"), placeholder=True)
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


def _sync_turns(
    store: dict[str, Any],
    turns_payload: dict[str, Any],
    pending_payload: dict[str, Any],
    runtime: SyncRuntime,
    *,
    chat_id: str,
    live_worker_ids: set[str] | None = None,
    yield_barrier: Any | None = None,
) -> dict[str, int]:
    counts = {"feed_sent": 0, "sent": 0, "updated": 0}
    turns = _turns(turns_payload)
    if live_worker_ids is not None:
        # Retired-worker turns must not be delivered (same rule already applied
        # to status aggregation): a stale row can otherwise emit a duplicate
        # working card or a misattributed final.
        turns = [
            item
            for item in turns
            if compact_ws(item.get("worker_id"), 160) in live_worker_ids
        ]
    latest_content_turn_by_worker: dict[str, str] = {}
    # Pass 1: real content only (user prompt, stream, or a completed final).
    # Tendwire store output is already ordered by per-worker observed recency.
    # Payload updated_at can be absent on current worker-derived turns, so do
    # not let an older command row with updated_at suppress the live turn.
    for item in turns:
        _key, entry = _entry_for_turn(store, item)
        if entry is None:
            continue
        worker_key = str(entry.get("tendwire_worker_id") or item.get("worker_id") or "")
        if not worker_key:
            continue
        complete = bool(item.get("complete")) or bool(item.get("assistant_final_text"))
        has_real_content = (
            bool(item.get("assistant_stream_text"))
            or bool(item.get("user_text"))
            or (complete and bool(item.get("assistant_final_text")))
        )
        if not has_real_content:
            continue
        latest_content_turn_by_worker.setdefault(worker_key, _turn_id(item))
    # Pass 2: synthetic "Work is in progress." placeholders only fill workers
    # with no real turn at all — a placeholder must never outrank a real turn.
    for item in turns:
        _key, entry = _entry_for_turn(store, item)
        if entry is None:
            continue
        worker_key = str(entry.get("tendwire_worker_id") or item.get("worker_id") or "")
        if not worker_key or worker_key in latest_content_turn_by_worker:
            continue
        if _turn_is_working_placeholder(item, entry):
            latest_content_turn_by_worker.setdefault(worker_key, _turn_id(item))
    seen_final_workers: set[str] = set()
    seen_working_workers: set[str] = set()
    fold_state: dict[str, int] = {"issued": 0}
    turn_count = len(turns)
    for idx, item in enumerate(turns):
        entry_key, entry = _entry_for_turn(store, item)
        if entry is None:
            continue
        before = dict(entry)
        repaired_open_final = False
        worker_key = str(entry.get("tendwire_worker_id") or item.get("worker_id") or "")
        latest_turn_id = latest_content_turn_by_worker.get(worker_key)
        complete = bool(item.get("complete")) or bool(item.get("assistant_final_text"))
        if complete and (item.get("assistant_final_text") or item.get("assistant_stream_text")):
            if latest_turn_id and _turn_id(item) != latest_turn_id:
                delivered = False
                counts["updated"] += int(_suppress_historical_final(store, item, _turn_content_hash(item, "final")))
                counts["updated"] += int(_fold_superseded_final(store, item, entry, runtime, chat_id=chat_id, fold_state=fold_state))
                continue
            if worker_key in seen_final_workers:
                continue
            seen_final_workers.add(worker_key)
            delivered = _deliver_final(store, item, entry, runtime, chat_id=chat_id)
            if delivered:
                # Speak seam AFTER _deliver_final so it fires for every delivery branch (raw send,
                # promote-working-to-final, replace-changed-final), not just the raw path — the flag
                # is consumed here exactly once per delivered final. It may reload `store` off-lock
                # (Phase 2), so it returns the entry to keep using this iteration.
                entry = _speak_reply(
                    store, item, entry, entry_key, runtime,
                    chat_id=chat_id, thread_id=str(entry.get("topic_id") or ""),
                    reply_to=str(entry.get("last_clean_message_id") or "") or None,
                )
        elif item.get("assistant_stream_text") or _turn_is_working_placeholder(item, entry):
            if latest_turn_id and _turn_id(item) != latest_turn_id:
                continue
            if worker_key in seen_working_workers:
                continue
            seen_working_workers.add(worker_key)
            repaired_open_final = _clear_open_turn_final_delivery_state(store, entry, _turn_id(item))
            delivered = _deliver_working(store, item, entry, runtime, chat_id=chat_id)
        else:
            delivered = False
        counts["feed_sent"] += int(delivered)
        counts["sent"] += int(delivered)
        counts["updated"] += int((not delivered and before != entry) or (repaired_open_final and not delivered))
        # Only turns that changed the store did a Telegram send (the slow part). After such a turn,
        # yield the state lock so a queued inbound command can interleave instead of stalling behind
        # the rest of the loop. The barrier commits `store` under the lock, releases briefly, then
        # reloads in place — so a competitor's write survives and `entry` is re-derived fresh next
        # iteration (no detached reference). Skip after the last turn (nothing left to unblock for).
        if yield_barrier is not None and (delivered or before != entry) and idx + 1 < turn_count:
            yield_barrier()
    pending_items = _pending(pending_payload)
    pending_count = len(pending_items)
    for p_idx, item in enumerate(pending_items):
        delivered = _deliver_pending(store, item, runtime, chat_id=chat_id)
        counts["feed_sent"] += int(delivered)
        counts["sent"] += int(delivered)
        # Same yield between delivered pending prompts (each is a send under the lock).
        if yield_barrier is not None and delivered and p_idx + 1 < pending_count:
            yield_barrier()
    return counts


def _sync_pinned(store: dict[str, Any], runtime: SyncRuntime, *, chat_id: str) -> bool:
    current_worker_ids: set[str] = set()
    current_space_ids = {
        str(entry.get("tendwire_space_id") or entry.get("space_id") or "")
        for entry in state.source_space_entries(store).values()
        if _entry_open_for_pin(entry) and not entry.get("stale_space_topic")
    }
    for entry in state.source_space_entries(store).values():
        worker_ids = entry.get("worker_ids")
        if isinstance(worker_ids, list):
            current_worker_ids.update(str(worker_id) for worker_id in worker_ids if worker_id)
    entries = []
    for entry in state.source_worker_entries(store).values():
        if not _entry_open_for_pin(entry):
            continue
        worker_id = str(entry.get("tendwire_worker_id") or entry.get("worker_id") or "")
        space_id = str(entry.get("tendwire_space_id") or entry.get("space_id") or "")
        if current_worker_ids and worker_id not in current_worker_ids:
            continue
        if not current_worker_ids and current_space_ids and space_id not in current_space_ids:
            continue
        entries.append(entry)
    if not entries:
        entries = [entry for entry in state.source_entries(store).values() if _entry_open_for_pin(entry)]
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
        if not sent.get("ok") and _message_missing(sent.get("error")):
            sent = runtime.telegram.send_message(chat_id, html, thread_id=config.general_thread_id(store), notify=False)
            if not sent.get("ok") and _topic_missing(sent.get("error")):
                sent = runtime.telegram.send_message(chat_id, html, notify=False)
            if sent.get("ok") and sent.get("message_id"):
                runtime.telegram.pin_message(chat_id, str(sent["message_id"]))
    else:
        sent = runtime.telegram.send_message(chat_id, html, thread_id=config.general_thread_id(store), notify=False)
        if not sent.get("ok") and _topic_missing(sent.get("error")):
            sent = runtime.telegram.send_message(chat_id, html, notify=False)
        if sent.get("ok") and sent.get("message_id"):
            runtime.telegram.pin_message(chat_id, str(sent["message_id"]))
    if sent.get("ok"):
        telegram["pinned_status_hash"] = content_hash
        if sent.get("message_id"):
            telegram["pinned_status_message_id"] = str(sent["message_id"])
        telegram.pop("pinned_status_last_error", None)
        return True
    telegram["pinned_status_last_error"] = compact_ws(sent.get("error"), 240)
    return False


def _sync_topic_pinned_statuses(store: dict[str, Any], runtime: SyncRuntime, *, chat_id: str) -> int:
    updated = 0
    for entry in state.source_entries(store).values():
        updated += int(_sync_topic_pinned(store, entry, runtime, chat_id=chat_id))
    return updated


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
                "updated": 0,
                "panes": 0,
                "spaces": 0,
                "icon_updated": 0,
                "pinned_status_updated": 0,
                "feed_sent": 0,
                "sent": 0,
                "routing_repaired": 0,
                "message_bindings": 0,
                "turn_updates": 0,
                "topic_cleanup": {"deleted": 0, "failed": 0, "pruned": 0, "changed": False},
                "tendwire_outbox": {"enabled": runtime.with_outbox, "polled": 0, "delivered": 0, "acked": 0, "failed": 0, "deferred": 0, "changed": False},
            }
    changed = False
    source_counts = _sync_sources(store, snapshot, turns_payload, runtime, chat_id=chat_id)
    routing_repaired = _repair_space_mode_routing_state(store)
    message_bindings = _backfill_message_bindings(store)
    bootstrapped = _bootstrap_existing_turns(store, turns_payload, pending_payload)
    live_worker_ids = {
        compact_ws(worker.get("id"), 160)
        for worker in _workers(snapshot)
        if _worker_is_open(worker)
    }
    live_worker_ids.discard("")

    def _yield_between_turns() -> None:
        # Inert unless actually holding the state lock (the shim wraps sync_once in state_lock();
        # tests/dry-run call sync_once directly). Commit under the lock so a competitor's load-modify-
        # save lands on top of ours, release briefly, then reload IN PLACE (the shim owns the `store`
        # reference and saves it after us) so committed deliveries + the additive turn-delivery ledger
        # survive both sides.
        if not state.lock_held():
            return
        state.save_state(store)
        with state.released_lock():
            pass
        fresh = state.load_state()
        store.clear()
        store.update(fresh)

    yield_barrier = (
        _yield_between_turns
        if config.offlock_interpane_yield_enabled() and not runtime.dry_run
        else None
    )
    turn_counts = (
        {"feed_sent": 0, "sent": 0, "updated": 0}
        if bootstrapped
        else _sync_turns(
            store,
            turns_payload,
            pending_payload,
            runtime,
            chat_id=chat_id,
            live_worker_ids=live_worker_ids,
            yield_barrier=yield_barrier,
        )
    )
    routing_repaired += _repair_space_mode_routing_state(store)
    topic_cleanup = _cleanup_topics(store, runtime, chat_id=chat_id)
    changed = changed or bool(
        source_counts["created"]
        or source_counts["updated"]
        or source_counts["icon_updated"]
        or routing_repaired
        or turn_counts["sent"]
        or turn_counts["updated"]
        or bootstrapped
        or topic_cleanup.get("changed")
        or message_bindings
    )
    pinned_changed = _sync_pinned(store, runtime, chat_id=chat_id)
    topic_pinned_updated = _sync_topic_pinned_statuses(store, runtime, chat_id=chat_id)
    changed = changed or pinned_changed or bool(topic_pinned_updated)
    outbox_result = {"enabled": runtime.with_outbox, "polled": 0, "delivered": 0, "acked": 0, "failed": 0, "deferred": 0, "changed": False}
    if runtime.with_outbox:
        remaining = max(0, runtime.max_sends - int(turn_counts["sent"]))
        outbox_result = drain_outbox(store, runtime.telegram, runtime.tendwire, chat_id=chat_id, max_sends=remaining, dry_run=runtime.dry_run)
        changed = changed or bool(outbox_result.get("changed"))
    return {
        "ok": True,
        "changed": changed,
        "created": source_counts["created"],
        "updated": source_counts["updated"],
        "panes": source_counts["panes"],
        "spaces": source_counts["spaces"],
        "icon_updated": source_counts["icon_updated"],
        "pinned_status_updated": int(pinned_changed) + topic_pinned_updated,
        "feed_sent": turn_counts["feed_sent"],
        "sent": turn_counts["sent"],
        "routing_repaired": routing_repaired,
        "turn_updates": turn_counts["updated"],
        "bootstrap_seen": bootstrapped,
        "message_bindings": message_bindings,
        "topic_cleanup": topic_cleanup,
        "tendwire_outbox": outbox_result,
    }
