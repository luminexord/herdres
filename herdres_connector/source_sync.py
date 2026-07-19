"""Tendwire source-mode sync to Telegram."""

from __future__ import annotations

import sys
import time
from dataclasses import dataclass
from typing import Any, Callable, Mapping

from . import accounts, config, decisions, speech, state
from .managed_bots import MANAGER_BOT_KIND, desired_message_bot_kind, managed_bot_kind_for_entry, managed_bot_token, managed_bot_token_for_entry
from .rendering import normalized_status, render_pending, render_status_overview, status_emoji
from .rich_delivery import (
    edit_feed_item,
    edit_turn_delivery_part,
    feed_item_requires_send_split,
    prepare_turn_delivery_parts,
    render_feed_item_html,
    rich_message_send_enabled,
    send_feed_item,
    send_turn_delivery_part,
    split_legacy_message_ids,
    turn_item_from_source,
)
from .safe import compact_ws, html_escape, short_hash
from .telegram_delivery import (
    MESSAGE_TEXT_LIMIT,
    TOPIC_ICON_COLORS,
    RateLimited,
    TelegramClient,
    delete_turn_delivery_message,
    drain_outbox,
    topic_icon_catalog,
    topic_icon_id,
)
from .tendwire_client import TendwireClient

RENDER_VERSION = "telegram-rich-v27-multipart-margin"
PRESENTATION_VERSION = "turn-present-v29"
TURN_SCHEMA_VERSION = 2
TURN_CONTENT_SCHEMA_VERSION = 1


class _TurnContentError(RuntimeError):
    def __init__(self, status: str, message: str, *, conflict: bool = False) -> None:
        super().__init__(message)
        self.status = status
        self.conflict = conflict


@dataclass
class SyncRuntime:
    tendwire: TendwireClient
    telegram: TelegramClient
    dry_run: bool = False
    with_outbox: bool = True
    max_sends: int = 8
    checkpoint: Callable[[], None] | None = None
    after_provider_accept: Callable[[], None] | None = None


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


def _entry_is_reapable(entry: dict[str, Any]) -> bool:
    """Reap-eligibility for the worker-topic reaper: ONLY a genuinely closed/failed entry.

    This is the strict inverse of _worker_is_open on the persisted entry fields — deliberately NOT
    _entry_status_is_finished, which also counts 'done'/'complete' as finished (the done-council
    cleanup relies on that, so it is left untouched). Here 'done'/'idle'/'working' are all LIVE:
    normalized_status('done') == 'idle', an idle agent whose terminal is still open. herdr reports
    agent_status='done' for a pane that merely finished its last turn, so such a pane dropping out of
    a snapshot for a reconcile-lag blip must NEVER be reaped (it would take the whole scrollback).
    Only 'closed'/'failed' — a truly gone pane — is reapable."""
    return normalized_status(entry.get("tendwire_raw_status") or entry.get("status")) in {"closed", "failed"}


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
        and state.worker_entry_is_uniquely_routable(store, key, entry)
    ]
    if space_id:
        candidates = [(key, entry) for key, entry in candidates if _entry_space_id(entry) == space_id]
    return candidates[0] if len(candidates) == 1 else (None, None)


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


_TURN_CONTENT_OUTCOME_KEY = "_herdres_content_outcome"
_TURN_CONTENT_OUTCOME_LIMIT = 100
_TURN_CONTENT_MATERIALIZED_KEY = "_herdres_content_materialized"


def _strict_nonnegative_int(
    value: Any,
    field: str,
    *,
    status: str = "invalid_content_schema",
) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise _TurnContentError(status, f"invalid {field}")
    return value


def _validate_turn_field_descriptor(
    item: dict[str, Any],
    *,
    field: str,
    descriptor: dict[str, Any],
) -> None:
    availability = descriptor.get("availability")
    inline = descriptor.get("inline")
    char_length = _strict_nonnegative_int(
        descriptor.get("char_length"), f"{field}.char_length"
    )
    byte_length = _strict_nonnegative_int(
        descriptor.get("byte_length"), f"{field}.byte_length"
    )
    page_count = _strict_nonnegative_int(
        descriptor.get("page_count"), f"{field}.page_count"
    )
    first_cursor = descriptor.get("first_cursor")
    if type(inline) is not bool:
        raise _TurnContentError(
            "invalid_content_schema", f"invalid {field}.inline"
        )
    if availability == "absent":
        if inline or char_length or byte_length or page_count or first_cursor is not None:
            raise _TurnContentError(
                "invalid_content_schema", f"inconsistent absent {field}"
            )
        if field in item:
            raise _TurnContentError(
                "invalid_content_schema", f"unexpected absent {field}"
            )
        return
    if availability == "known_incomplete":
        if inline or page_count or first_cursor is not None or field in item:
            raise _TurnContentError(
                "invalid_content_schema",
                f"known-incomplete {field} must be non-inline and non-pageable",
            )
        return
    if availability != "complete":
        raise _TurnContentError(
            "invalid_content_schema", f"invalid {field}.availability"
        )
    if inline:
        value = item.get(field)
        if not isinstance(value, str):
            raise _TurnContentError(
                "invalid_content_schema", f"missing inline {field}"
            )
        if (
            len(value) != char_length
            or len(value.encode("utf-8")) != byte_length
            or page_count != 1
            or first_cursor is not None
        ):
            raise _TurnContentError(
                "invalid_content_schema", f"inline {field} metadata mismatch"
            )
        return
    if (
        field in item
        or page_count <= 0
        or not isinstance(first_cursor, str)
        or not first_cursor.startswith("twcur1.")
    ):
        raise _TurnContentError(
            "invalid_content_schema", f"non-inline {field} is not pageable"
        )


def _turn_local_outcome(
    item: dict[str, Any], status: str
) -> dict[str, str]:
    outcome = {
        "turn_id": compact_ws(
            item.get("id") or item.get("turn_id") or "unidentified", 200
        ),
        "status": status,
    }
    revision = _content_revision(item)
    if revision:
        outcome["content_revision"] = revision
    return outcome


def _validate_turn_row(raw: dict[str, Any]) -> dict[str, Any]:
    item = dict(raw)
    content = item.get("content")
    content_schema = (
        content.get("schema_version") if isinstance(content, dict) else None
    )
    if (
        type(content_schema) is not int
        or content_schema != TURN_CONTENT_SCHEMA_VERSION
    ):
        raise _TurnContentError(
            "unsupported_content_schema", "turn content schema v1 is required"
        )
    revision = content.get("content_revision")
    fields = content.get("fields")
    known_incomplete = content.get("known_incomplete")
    if (
        not isinstance(revision, str)
        or not revision.startswith("twrev1.")
        or not isinstance(fields, dict)
    ):
        raise _TurnContentError(
            "invalid_content_schema", "invalid content revision or fields"
        )
    if type(known_incomplete) is not bool:
        raise _TurnContentError(
            "invalid_content_schema", "known_incomplete must be boolean"
        )
    incomplete_field = False
    for field in ("user_text", "assistant_final_text"):
        descriptor = fields.get(field)
        if not isinstance(descriptor, dict):
            raise _TurnContentError(
                "invalid_content_schema", f"missing {field} descriptor"
            )
        _validate_turn_field_descriptor(item, field=field, descriptor=descriptor)
        incomplete_field = (
            incomplete_field
            or descriptor.get("availability") == "known_incomplete"
        )
    if known_incomplete != incomplete_field:
        raise _TurnContentError(
            "invalid_content_schema", "known-incomplete summary mismatch"
        )
    if known_incomplete:
        item[_TURN_CONTENT_OUTCOME_KEY] = _turn_local_outcome(
            item, "content_known_incomplete"
        )
    return item


def _validate_turns_payload(payload: dict[str, Any]) -> dict[str, Any]:
    schema = payload.get("schema_version")
    if type(schema) is int and schema == 1:
        # Direct-call Goal 01B compatibility. The production TendwireClient
        # negotiates schema v2 and refuses daemon/CLI v1 responses.
        return payload
    if type(schema) is not int or schema != TURN_SCHEMA_VERSION:
        raise _TurnContentError(
            "upgrade_required", "Tendwire turn schema v2 is required"
        )
    rows = payload.get("turns")
    if not isinstance(rows, list):
        raise _TurnContentError(
            "invalid_content_schema", "turns must be a list"
        )
    validated_rows: list[dict[str, Any]] = []
    for raw in rows:
        if not isinstance(raw, dict):
            # A row-level protocol defect is isolated just like a malformed
            # descriptor; it cannot safely participate in delivery.
            validated_rows.append(
                {
                    _TURN_CONTENT_OUTCOME_KEY: {
                        "turn_id": "unidentified",
                        "status": "invalid_content_schema",
                    }
                }
            )
            continue
        try:
            validated_rows.append(_validate_turn_row(raw))
        except _TurnContentError as exc:
            item = dict(raw)
            item[_TURN_CONTENT_OUTCOME_KEY] = _turn_local_outcome(
                item, exc.status
            )
            validated_rows.append(item)
    validated = dict(payload)
    validated["turns"] = validated_rows
    return validated


def _materialize_turn_field(
    runtime: SyncRuntime,
    item: dict[str, Any],
    *,
    content_revision: str,
    field: str,
    descriptor: dict[str, Any],
) -> tuple[str, int]:
    availability = descriptor["availability"]
    if availability == "absent":
        return "", 0
    if availability == "known_incomplete":
        raise _TurnContentError(
            "content_known_incomplete", f"{field} is known incomplete"
        )
    if descriptor["inline"]:
        return str(item[field]), 0

    char_length = int(descriptor["char_length"])
    byte_length = int(descriptor["byte_length"])
    page_count = int(descriptor["page_count"])
    turn_id = _turn_id(item)
    cursor: str | None = str(descriptor["first_cursor"])
    seen_cursors: set[str] = set()
    seen_segments: set[str] = set()
    chunks: list[str] = []
    for expected_index in range(page_count):
        if cursor is None or cursor in seen_cursors:
            raise _TurnContentError(
                "invalid_content_page", f"{field} cursor cycle or early end"
            )
        seen_cursors.add(cursor)
        page = runtime.tendwire.turn_content_get(
            turn_id, content_revision, field, cursor
        )
        if page.get("ok") is False:
            status = str(page.get("status") or "content_fetch_failed")
            raise _TurnContentError(
                status,
                str(page.get("error") or f"failed to fetch {field}"),
                conflict=status
                in {
                    "content_revision_not_found",
                    "revision_conflict",
                    "stale_revision",
                },
            )
        if (
            type(page.get("schema_version")) is not int
            or page.get("schema_version") != TURN_CONTENT_SCHEMA_VERSION
            or page.get("turn_id") != turn_id
            or page.get("content_revision") != content_revision
            or page.get("field") != field
            or page.get("availability") != "complete"
        ):
            conflict = page.get("content_revision") not in (
                None,
                content_revision,
            )
            raise _TurnContentError(
                "invalid_content_page",
                f"{field} page identity mismatch",
                conflict=conflict,
            )
        index = _strict_nonnegative_int(
            page.get("index"), "page.index", status="invalid_content_page"
        )
        count = _strict_nonnegative_int(
            page.get("count"), "page.count", status="invalid_content_page"
        )
        if index != expected_index or count != page_count:
            raise _TurnContentError(
                "invalid_content_page", f"{field} page order/count mismatch"
            )
        text = page.get("text")
        segment_id = page.get("segment_id")
        if (
            not isinstance(text, str)
            or not isinstance(segment_id, str)
            or not segment_id.startswith("twseg1.")
            or segment_id in seen_segments
        ):
            raise _TurnContentError(
                "invalid_content_page", f"{field} invalid or duplicate segment"
            )
        seen_segments.add(segment_id)
        if (
            _strict_nonnegative_int(
                page.get("segment_char_length"),
                "segment_char_length",
                status="invalid_content_page",
            )
            != len(text)
            or _strict_nonnegative_int(
                page.get("segment_byte_length"),
                "segment_byte_length",
                status="invalid_content_page",
            )
            != len(text.encode("utf-8"))
            or _strict_nonnegative_int(
                page.get("total_char_length"),
                "total_char_length",
                status="invalid_content_page",
            )
            != char_length
            or _strict_nonnegative_int(
                page.get("total_byte_length"),
                "total_byte_length",
                status="invalid_content_page",
            )
            != byte_length
        ):
            raise _TurnContentError(
                "invalid_content_page", f"{field} page length mismatch"
            )
        next_cursor = page.get("next_cursor")
        if expected_index + 1 < page_count:
            if (
                not isinstance(next_cursor, str)
                or not next_cursor.startswith("twcur1.")
                or next_cursor in seen_cursors
            ):
                raise _TurnContentError(
                    "invalid_content_page", f"{field} invalid next cursor"
                )
            cursor = next_cursor
        else:
            if next_cursor is not None:
                raise _TurnContentError(
                    "invalid_content_page",
                    f"{field} final cursor must be null",
                )
            cursor = None
        chunks.append(text)
    value = "".join(chunks)
    if (
        len(value) != char_length
        or len(value.encode("utf-8")) != byte_length
    ):
        raise _TurnContentError(
            "invalid_content_page", f"{field} reconstructed length mismatch"
        )
    return value, page_count


def _materialize_turn_item(
    item: dict[str, Any], runtime: SyncRuntime
) -> int:
    if item.get(_TURN_CONTENT_MATERIALIZED_KEY) is True:
        return 0
    if item.get(_TURN_CONTENT_OUTCOME_KEY):
        raise _TurnContentError(
            str(item[_TURN_CONTENT_OUTCOME_KEY].get("status")),
            "turn content is not eligible for materialization",
        )
    content = item.get("content")
    if not isinstance(content, dict):
        # Legacy direct-call fixtures carry their canonical inline values.
        return 0
    fields = content["fields"]
    revision = str(content["content_revision"])
    materialized = dict(item)
    page_calls = 0
    for field in ("user_text", "assistant_final_text"):
        descriptor = fields[field]
        value, fetched = _materialize_turn_field(
            runtime,
            item,
            content_revision=revision,
            field=field,
            descriptor=descriptor,
        )
        page_calls += fetched
        if descriptor["availability"] == "absent":
            materialized.pop(field, None)
        else:
            materialized[field] = value
    materialized[_TURN_CONTENT_MATERIALIZED_KEY] = True
    item.clear()
    item.update(materialized)
    return page_calls


def _turn_content_outcomes(
    payload: dict[str, Any],
) -> dict[str, Any]:
    outcomes = [
        dict(item[_TURN_CONTENT_OUTCOME_KEY])
        for item in _turns(payload)
        if isinstance(item.get(_TURN_CONTENT_OUTCOME_KEY), dict)
    ]
    return {
        "count": len(outcomes),
        "truncated": len(outcomes) > _TURN_CONTENT_OUTCOME_LIMIT,
        "items": outcomes[:_TURN_CONTENT_OUTCOME_LIMIT],
    }


def _turn_has_content_outcome(item: dict[str, Any]) -> bool:
    return isinstance(item.get(_TURN_CONTENT_OUTCOME_KEY), dict)


def _turn_has_complete_final(item: dict[str, Any]) -> bool:
    content = item.get("content")
    if not isinstance(content, dict):
        return bool(item.get("complete")) or isinstance(
            item.get("assistant_final_text"), str
        )
    fields = content.get("fields")
    descriptor = (
        fields.get("assistant_final_text")
        if isinstance(fields, dict)
        else None
    )
    return (
        not _turn_has_content_outcome(item)
        and isinstance(descriptor, dict)
        and descriptor.get("availability") == "complete"
    )


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
    content = item.get("content")
    fields = content.get("fields") if isinstance(content, dict) else None
    if isinstance(fields, dict) and any(
        isinstance(descriptor, dict)
        and descriptor.get("availability") == "complete"
        and descriptor.get("inline") is False
        for descriptor in fields.values()
    ):
        # A bounded delta can intentionally carry descriptors only. Treating
        # that as an empty turn would fabricate a Working card and bypass the
        # canonical Goal 05 content path.
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
    if (
        str(entry.get("entry_type") or "") == "worker"
        and not state.entry_is_routable(entry)
    ):
        return False, False
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
        # Topic creation has no provider idempotency key. Persist the returned
        # identity before any later turn validation can abort this sync pass,
        # otherwise the next pass can create a duplicate topic.
        if runtime.checkpoint is not None:
            runtime.checkpoint()
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
        return [
            entry
        ] if _entry_open_for_pin(entry) and state.entry_is_routable(entry) else []
    space_id = str(entry.get("tendwire_space_id") or entry.get("space_id") or "")
    worker_ids = entry.get("worker_ids")
    current_worker_ids = {str(worker_id) for worker_id in worker_ids if worker_id} if isinstance(worker_ids, list) else set()
    workers = [
        worker_entry
        for worker_key, worker_entry in state.source_worker_entries(store).items()
        if _entry_open_for_pin(worker_entry)
        and state.worker_entry_is_uniquely_routable(store, worker_key, worker_entry)
        and str(worker_entry.get("tendwire_space_id") or worker_entry.get("space_id") or "") == space_id
        and (
            not current_worker_ids
            or str(worker_entry.get("tendwire_worker_id") or worker_entry.get("worker_id") or "") in current_worker_ids
        )
    ]
    return workers or ([entry] if _entry_open_for_pin(entry) else [])


def _account_lines_html(entries: list[dict[str, Any]]) -> str:
    """The who-am-I/usage footer for a pinned board: one line per account kind present in
    `entries` ('' when disabled or nothing resolvable). Escaped, ready to append."""
    if not config.pinned_account_enabled():
        return ""
    kinds: list[str] = []
    for entry in entries:
        for field in ("agent", "worker_name", "tendwire_worker_id", "worker_id", "active_worker_id"):
            kind = accounts.agent_kind(entry.get(field))
            if kind:
                if kind not in kinds:
                    kinds.append(kind)
                break
    if not kinds:
        return ""
    snapshot = accounts.usage_snapshot()
    lines = [line for kind in sorted(kinds) for line in (accounts.account_line(kind, snapshot=snapshot),) if line]
    return "\n".join(html_escape(line, 200) for line in lines)


def _sync_topic_pinned(store: dict[str, Any], entry: dict[str, Any], runtime: SyncRuntime, *, chat_id: str) -> bool:
    thread_id = str(entry.get("topic_id") or "")
    if not thread_id:
        return False
    pin_entries = _status_entries_for_topic_pin(store, entry)
    html = render_status_overview(pin_entries)
    account_html = _account_lines_html(pin_entries or [entry])
    if account_html:
        html = f"{html}\n{account_html}"
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

# Consecutive sync passes a finished worker must be ABSENT from the tendwire snapshot before its
# stranded topic is reaped (see config.reap_closed_worker_topics). A small streak absorbs a one-tick
# partial snapshot without letting a genuinely-gone worker linger.
_REAP_ABSENCE_STREAK = 2


def _ordered_workers(workers: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return sorted(workers, key=state.canonical_worker_observation_key)


def _worker_topic_assignment_keys(workers: list[dict[str, Any]]) -> dict[int, str]:
    counts: dict[str, int] = {}
    for worker in workers:
        worker_id = compact_ws(worker.get("id"), 160)
        counts[worker_id] = counts.get(worker_id, 0) + 1
    result: dict[int, str] = {}
    for worker in workers:
        worker_id = compact_ws(worker.get("id"), 160)
        if counts.get(worker_id) == 1:
            result[id(worker)] = worker_id
            continue
        result[id(worker)] = "\x1f".join(
            state.canonical_worker_observation_key(worker)
        )
    return result


def _assign_worker_topic_names(
    store: dict[str, Any],
    workers: list[dict[str, Any]],
    *,
    blocked_stable_keys: set[str] | None = None,
    blocked_worker_ids: set[str] | None = None,
    worker_entry_reservations: Mapping[int, str | None] | None = None,
) -> tuple[dict[str, str], dict[str, str], dict[str, str]]:
    """Map each not-yet-topiced worker id -> a unique topic name (cwd basename, numbered on collision).
    Names already bound to a created topic are reserved and never renumbered, so numbers stay stable as
    panes come and go. Ordered by the shared canonical observation key for deterministic numbering."""
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
    # Names currently backing a real topic — a de-number target must be absent from this set (i.e. no
    # other topic already holds the bare base name).
    all_named = {
        compact_ws(e.get("topic_name"), 120).casefold()
        for e in entries.values()
        if e.get("topic_id") and e.get("topic_name")
    }
    keeps: dict[str, bool] = {}
    assignment_keys = _worker_topic_assignment_keys(workers)
    ordered = _ordered_workers(workers)
    for worker in ordered:
        wid = compact_ws(worker.get("id"), 160)
        assignment_key = assignment_keys[id(worker)]
        key = (
            worker_entry_reservations.get(id(worker))
            if worker_entry_reservations is not None
            else (
                None
                if wid in (blocked_worker_ids or set())
                else state.resolve_worker_entry_key(
                    store, worker, blocked_stable_keys=blocked_stable_keys
                )
                if wid
                else None
            )
        )
        existing = entries.get(key) if key is not None else None
        if not existing or not existing.get("topic_id") or not existing.get("topic_name"):
            continue
        current = compact_ws(existing.get("topic_name"), 120)
        keep = _is_variant_of(current, state.topic_name_for_worker(worker))
        keeps[assignment_key] = keep
        # NOTE: the old name stays RESERVED even when a rename is proposed — freeing it mid-pass
        # would let a new pane take it and collide into this topic via _ensure_topic's reuse-by-name
        # (two live panes sharing one topic). It frees naturally on the pass AFTER the rename lands.
    assigned: dict[str, str] = {}
    renames: dict[str, str] = {}
    # wid -> base for names the connector itself minted a " N" suffix onto this pass (name != base after
    # the while-reserved loop). _sync_sources stamps this as connector_numbered_base on the entry when it
    # applies the name — numbering-time provenance, so a later de-number acts only on connector-minted
    # numbers and can never collapse a user's own "Sonnet 4"-style label.
    numbered_bases: dict[str, str] = {}
    for worker in ordered:
        wid = compact_ws(worker.get("id"), 160)
        assignment_key = assignment_keys[id(worker)]
        if not wid:
            continue
        key = (
            worker_entry_reservations.get(id(worker))
            if worker_entry_reservations is not None
            else (
                None
                if wid in (blocked_worker_ids or set())
                else state.resolve_worker_entry_key(
                    store, worker, blocked_stable_keys=blocked_stable_keys
                )
            )
        )
        existing = entries.get(key) if key is not None else None
        has_topic = bool(existing and existing.get("topic_id"))
        # De-number a connector-minted suffix once its base name is free again. The marker
        # (connector_numbered_base) is stamped at NUMBERING time (when the connector mints the " N" —
        # see the while-reserved loop below, applied in _sync_sources), so it records true provenance
        # and this can never rename a user's genuinely-numbered label.
        marker = compact_ws((existing or {}).get("connector_numbered_base"), 120)
        if has_topic and marker and _worker_is_open(worker):
            current = compact_ws(existing.get("topic_name"), 120)
            numbered_variant = (
                current.casefold() != marker.casefold()
                and current.casefold().startswith(marker.casefold() + " ")
                and current[len(marker) + 1 :].strip().isdigit()
            )
            if numbered_variant and marker.casefold() not in all_named and marker.casefold() not in reserved:
                renames[assignment_key] = marker
                reserved.add(marker.casefold())
                continue
        if has_topic and keeps.get(assignment_key, True):
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
        if name.casefold() != base.casefold():
            numbered_bases[assignment_key] = base   # connector-minted number -> record its base as provenance
        if has_topic:
            renames[assignment_key] = name   # desired name changed (e.g. the pane label appeared) -> rename in place
        else:
            assigned[assignment_key] = name
    return assigned, renames, numbered_bases


def _stamp_numbered_base(entry: dict[str, Any], wid: str, numbered_bases: dict[str, str]) -> None:
    """Apply numbering-time provenance to an entry as its name is set: stamp connector_numbered_base
    when the connector minted a " N" suffix onto this pane's name, else clear any prior marker (a
    de-number or a bare rename removes the connector-minted number)."""
    base = numbered_bases.get(wid)
    if base:
        entry["connector_numbered_base"] = base
    else:
        entry.pop("connector_numbered_base", None)


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
    worker_numbered_bases: dict[str, str] = {}
    workers = _workers(snapshot)
    blocked_stable_keys = state.blocked_worker_stable_keys(store, workers)
    blocked_worker_ids = state.conflicting_snapshot_worker_ids(workers)
    counts["updated"] += state.quarantine_worker_stable_key_owners(
        store,
        blocked_stable_keys,
        reason="preflight_stable_key_conflict",
    )
    worker_assignment_keys = _worker_topic_assignment_keys(workers)
    worker_entry_reservations = state.precompute_worker_entry_reservations(
        store,
        workers,
        blocked_stable_keys=blocked_stable_keys,
        blocked_worker_ids=blocked_worker_ids,
    )
    reserved_entry_keys = frozenset(
        key for key in worker_entry_reservations.values() if key is not None
    )
    if topic_mode == "worker":
        worker_topic_names, worker_topic_renames, worker_numbered_bases = _assign_worker_topic_names(
            store,
            workers,
            blocked_stable_keys=blocked_stable_keys,
            blocked_worker_ids=blocked_worker_ids,
            worker_entry_reservations=worker_entry_reservations,
        )
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
    for worker in _ordered_workers(workers):
        space_id = compact_ws(worker.get("space_id"), 160)
        existing_key = worker_entry_reservations.get(id(worker))
        before = dict(state.source_worker_entries(store).get(existing_key) or {}) if existing_key is not None else {}
        _key, entry, created = state.upsert_worker_entry(
            store,
            worker,
            blocked_stable_keys=blocked_stable_keys,
            blocked_worker_ids=blocked_worker_ids,
            preplanned_key=existing_key,
            use_preplanned_key=True,
            reserved_entry_keys=reserved_entry_keys,
        )
        entry["status"] = _effective_worker_status(worker, turn_status_by_worker)
        _stamp_managed_voice(entry, _space_voice_mode(store, space_id))
        if not state.worker_entry_is_uniquely_routable(store, _key, entry):
            counts["created"] += int(created)
            counts["updated"] += int(not created and before != entry)
            continue
        # Apply the cwd-based, disambiguated name before the topic is created (once it has a topic_id
        # the name is locked, so a later renumber can't rename an existing topic).
        wid = compact_ws(worker.get("id"), 160)
        assignment_key = worker_assignment_keys[id(worker)]
        if not entry.get("topic_id") and assignment_key in worker_topic_names:
            entry["topic_name"] = worker_topic_names[assignment_key]
            _stamp_numbered_base(entry, assignment_key, worker_numbered_bases)
        elif (
            entry.get("topic_id")
            and assignment_key in worker_topic_renames
            and not runtime.dry_run
            and renames_issued < create_cap
        ):
            renamed = runtime.telegram.rename_topic(
                chat_id, str(entry["topic_id"]), worker_topic_renames[assignment_key]
            )
            renames_issued += 1
            if renamed.get("ok"):
                entry["topic_name"] = worker_topic_renames[assignment_key]
                entry.pop("rename_attempts", None)
                _stamp_numbered_base(entry, assignment_key, worker_numbered_bases)
            elif _topic_missing(renamed.get("error")):
                # the topic is gone (hand-deleted): drop the mapping so _ensure_topic recreates it
                # under the new name instead of renaming a ghost forever.
                entry.pop("topic_id", None)
                entry["topic_name"] = worker_topic_renames[assignment_key]
                entry.pop("rename_attempts", None)
                _stamp_numbered_base(entry, assignment_key, worker_numbered_bases)
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
        entry.pop("stale_space_topic", None)
        selected_status = _effective_worker_status(selected, turn_status_by_worker) if selected else ""
        space_turn_status = turn_status_by_space.get(space_id) or ""
        entry["status"] = _dominant_status(space_turn_status, selected_status, _source_status(space.get("status")))
        entry["worker_count"] = len(selectable)
        entry["worker_ids"] = [compact_ws(worker.get("id"), 160) for worker in selectable if compact_ws(worker.get("id"), 160)]
        state.clear_space_active_worker(entry)
        if selected:
            _selected_key, selected_entry = state.find_worker_entry_by_id(
                store, compact_ws(selected.get("id"), 160)
            )
            if selected_entry is not None and state.cache_space_active_worker(
                entry, selected_entry
            ):
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
            stale_entry = state.source_space_entries(store)[key]
            state.clear_space_active_worker(stale_entry)
            stale_entry["stale_space_topic"] = True
    return counts


def _cleanup_topics(
    store: dict[str, Any],
    runtime: SyncRuntime,
    *,
    chat_id: str,
    snapshot_worker_ids: set[str] | None = None,
) -> dict[str, Any]:
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

    # Worker-mode reaper (opt-in, DESTRUCTIVE): delete topics of workers that have durably CLOSED/FAILED
    # and left the tendwire snapshot. Positional worker-id churn across herdr restarts (claude-2 ->
    # claude-2-2 for a fresh terminal) otherwise strands the old pane's topic forever, and its squatted
    # name forces the live pane's topic to a " 2" suffix. Guards: opt-in flag, strict closed/failed
    # liveness (NOT 'done'/'idle'), absence across _REAP_ABSENCE_STREAK passes, a non-degraded snapshot,
    # and the shared per-pass delete cap.
    reap_enabled = (
        config.reap_closed_worker_topics()
        and config.source_topic_mode() == "worker"
        and snapshot_worker_ids is not None
    )
    if reap_enabled and not snapshot_worker_ids and state.source_worker_entries(store):
        reap_enabled = False  # a transient empty snapshot must never mass-reap live topics
    if reap_enabled:
        # Degraded/partial-snapshot guard: if NONE of the workers we still consider LIVE appear in this
        # snapshot, treat the whole pass as untrustworthy (a tendwire reconcile-lag / binding-expiry blip
        # that transiently dropped live panes) and skip reaping — otherwise the absent closed entries
        # keep marching toward a delete on a bad pass. A purely-closed store has no live anchor to check,
        # so it still reaps normally.
        live_known_ids = {
            compact_ws(entry.get("tendwire_worker_id") or entry.get("worker_id"), 160)
            for entry in state.source_worker_entries(store).values()
            if not _entry_is_reapable(entry)
        }
        live_known_ids.discard("")
        if live_known_ids and not (live_known_ids & snapshot_worker_ids):
            reap_enabled = False
    if reap_enabled:
        for key, entry in list(state.source_worker_entries(store).items()):
            wid = compact_ws(entry.get("tendwire_worker_id") or entry.get("worker_id"), 160)
            if wid and wid in snapshot_worker_ids:
                if entry.pop("reap_miss_count", None) is not None:
                    result["changed"] = True  # worker reappeared: reset its absence streak
                continue
            if not _entry_is_reapable(entry):
                # Only a genuinely closed/failed pane is reapable. 'done'/'idle'/'working' is a LIVE
                # idle/busy agent (normalized_status('done') == 'idle') whose terminal is still open — a
                # snapshot-absence blip must never delete its topic (and whole scrollback).
                continue
            topic_id = str(entry.get("topic_id") or "")
            if runtime.dry_run:
                # Preview every closed/failed+absent topic (no streak, no state mutation).
                if topic_id and topic_id not in deleted_topic_ids:
                    result["deleted"] += 1
                    deleted_topic_ids.add(topic_id)
                    result["changed"] = True
                continue
            misses = min(int(entry.get("reap_miss_count") or 0) + 1, _REAP_ABSENCE_STREAK)
            if misses < _REAP_ABSENCE_STREAK:
                entry["reap_miss_count"] = misses
                result["changed"] = True
                continue
            if not topic_id:
                panes.pop(key, None)  # finished, gone, no topic: dead cruft
                result["pruned"] += 1
                result["changed"] = True
                continue
            if deletes_issued >= delete_cap:
                continue  # per-pass delete budget spent; retry next tick (entry still eligible)
            deletes_issued += 1
            deleted = runtime.telegram.delete_topic(chat_id, topic_id)
            if not deleted.get("ok") and not _topic_missing(deleted.get("error")):
                result["failed"] += 1
                entry["last_topic_delete_error"] = compact_ws(deleted.get("error"), 240)
                continue
            if deleted.get("ok"):
                result["deleted"] += 1
                deleted_topic_ids.add(topic_id)
                audit.append({"topic_id": topic_id, "name": compact_ws(entry.get("topic_name"), 120), "reason": "reaped_closed_worker_topic"})
            # No de-number marker is stamped here: provenance is recorded at NUMBERING time (see
            # _assign_worker_topic_names / _stamp_numbered_base). Reaping merely frees the base name; the
            # live sibling that the connector minted "<base> N" already carries connector_numbered_base and
            # de-numbers on the next assign pass. Stamping by name-pattern at reap time could collapse a
            # user's own "<base> N" label, so it is deliberately gone.
            result["changed"] = True
            panes.pop(key, None)

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
        if kind == "final" and binding.get("plan_token"):
            # Keep the private delivery coordinate long enough for a replacement plan to
            # converge/delete it, but quarantine it from reply routing immediately.
            binding["routing_quarantined"] = True
            repaired += 1
            continue
        for entry in state.source_worker_entries(store).values():
            if _entry_worker_id(entry) == worker_id and _entry_space_id(entry) == space_id:
                repaired += int(_clear_entry_message_reference(entry, str(message_id), kind))
        bindings.pop(str(message_id), None)
        repaired += 1
    return repaired


def _backfill_message_bindings(store: dict[str, Any]) -> int:
    before = set(state.message_bindings(store))
    for entry in state.source_worker_entries(store).values():
        if not state.entry_is_routable(entry):
            continue
        topic_id = str(entry.get("topic_id") or "")
        if not topic_id:
            _space_key, space_entry = state.find_space_entry_by_id(store, str(entry.get("tendwire_space_id") or entry.get("space_id") or ""))
            topic_id = str((space_entry or {}).get("topic_id") or "")
        if not topic_id:
            continue
        stream_id = str(entry.get("last_stream_message_id") or "")
        if stream_id and state.find_message_binding(store, stream_id) is None:
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
            if state.find_message_binding(store, message_id) is not None:
                continue
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


def _deliver_working(
    store: dict[str, Any],
    item: dict[str, Any],
    entry: dict[str, Any],
    runtime: SyncRuntime,
    *,
    chat_id: str,
    reuse_previous_working: bool = False,
) -> bool:
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
    if (
        entry.get("last_stream_message_id")
        and stored_bot_kind == bot_kind
        and (
            entry.get("last_stream_turn_id") == turn_id
            or reuse_previous_working
        )
    ):
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


def _content_revision(item: dict[str, Any]) -> str:
    content = item.get("content")
    if not isinstance(content, dict):
        return ""
    revision = content.get("content_revision")
    return revision if isinstance(revision, str) and revision.startswith("twrev1.") else ""


def _stage_final_plan(
    store: dict[str, Any],
    item: dict[str, Any],
    entry: dict[str, Any],
    runtime: SyncRuntime,
    *,
    source_ref: str | None = None,
) -> tuple[bool, int]:
    revision = _content_revision(item)
    if not revision:
        return False, 0

    def _prepare_begin(part_count: int) -> dict[str, Any]:
        kwargs: dict[str, Any] = {
            "turn_id": _turn_id(item),
            "content_revision": revision,
            "presentation_version": PRESENTATION_VERSION,
            "part_count": part_count,
        }
        if source_ref is not None:
            kwargs["source_ref"] = source_ref
        return runtime.tendwire.connector_prepare_begin(**kwargs)

    def _prepare_commit(plan_token: str) -> dict[str, Any]:
        kwargs: dict[str, Any] = {"plan_token": plan_token}
        if source_ref is not None:
            kwargs["source_ref"] = source_ref
        return runtime.tendwire.connector_prepare_commit(**kwargs)

    if (
        source_ref is None
        and entry.get("last_turn_id") == _turn_id(item)
        and entry.get("last_clean_content_revision") == revision
    ):
        return False, 0
    if (
        entry.get("pending_turn_id") == _turn_id(item)
        and entry.get("pending_content_revision") == revision
        and isinstance(entry.get("pending_plan_token"), str)
        and str(entry.get("pending_plan_token")).startswith("twplan1.")
    ):
        pending_token = str(entry["pending_plan_token"])
        pending_count = entry.get("pending_turn_part_count")
        if (
            isinstance(pending_count, bool)
            or not isinstance(pending_count, int)
            or pending_count <= 0
        ):
            raise _TurnContentError(
                "invalid_pending_plan", "pending plan has invalid part count"
            )
        observed = _prepare_begin(pending_count)
        if observed.get("ok") is False or observed.get("plan_token") != pending_token:
            raise _TurnContentError(
                str(observed.get("status") or "prepare_failed"),
                str(observed.get("error") or "pending plan reconciliation failed"),
            )
        if source_ref is not None:
            observed = _prepare_commit(pending_token)
            if observed.get("ok") is False or observed.get("plan_token") != pending_token:
                raise _TurnContentError(
                    str(observed.get("status") or "prepare_failed"),
                    str(observed.get("error") or "pending plan handoff failed"),
                    conflict=str(observed.get("status") or "")
                    in {"revision_conflict", "stale_revision", "stale_ref"},
                )
        if observed.get("state") == "completed":
            receipts = [
                (job_key, receipt)
                for job_key, receipt in state.tendwire_turn_jobs(store).items()
                if isinstance(receipt, dict)
                and receipt.get("plan_token") == pending_token
            ]
            for job_key, receipt in receipts:
                if receipt.get("substate") in {
                    "telegram_applied",
                    "old_slot_retired",
                }:
                    state.update_tendwire_turn_job(
                        store, job_key, substate="acknowledged"
                    )
                elif receipt.get("substate") != "acknowledged":
                    raise _TurnContentError(
                        "invalid_pending_plan",
                        "completed plan lacks a durable Telegram outcome",
                    )
            if _maybe_complete_turn_plan(
                store,
                item,
                entry,
                plan_token=pending_token,
                revision=revision,
            ):
                _checkpoint_turn_job(runtime)
        return False, 0

    page_calls = _materialize_turn_item(item, runtime)
    feed_item = turn_item_from_source(item, entry)
    parts = prepare_turn_delivery_parts(
        feed_item,
        rich_transport=rich_message_send_enabled(_telegram_state(store)),
    )
    if not parts:
        raise _TurnContentError(
            "invalid_presentation_plan",
            "completed turn has no presentation parts",
        )
    if runtime.dry_run:
        entry["pending_turn_id"] = _turn_id(item)
        entry["pending_content_revision"] = revision
        entry["pending_plan_token"] = "dry-run"
        entry["pending_turn_part_count"] = len(parts)
        entry["pending_turn_job_count"] = len(parts)
        entry["pending_turn_user_hash"] = _turn_user_hash(item)
        entry["pending_plan_generation"] = 1
        return True, page_calls

    begin = _prepare_begin(len(parts))
    if begin.get("ok") is False:
        raise _TurnContentError(
            str(begin.get("status") or "prepare_failed"),
            str(begin.get("error") or "connector prepare begin failed"),
            conflict=str(begin.get("status") or "")
            in {"revision_conflict", "stale_revision", "stale_ref"},
        )
    plan_token = begin.get("plan_token")
    if not isinstance(plan_token, str) or not plan_token.startswith("twplan1."):
        raise _TurnContentError(
            "invalid_prepare_response",
            "prepare begin omitted a public plan token",
        )
    state_name = str(begin.get("state") or "")
    if state_name not in {
        "preparing",
        "active",
        "waiting_predecessor",
        "completed",
    }:
        raise _TurnContentError(
            "invalid_prepare_response", "prepare begin returned invalid state"
        )
    if state_name == "preparing":
        for ordinal, part in enumerate(parts):
            response = runtime.tendwire.connector_prepare_part(
                plan_token=plan_token,
                ordinal=ordinal,
                spans=part["spans"],
            )
            if (
                response.get("ok") is False
                or response.get("plan_token") != plan_token
                or response.get("ordinal") != ordinal
            ):
                raise _TurnContentError(
                    str(response.get("status") or "prepare_failed"),
                    str(response.get("error") or "connector prepare part failed"),
                )
        commit = _prepare_commit(plan_token)
    elif source_ref is not None:
        commit = _prepare_commit(plan_token)
    else:
        commit = begin
    if commit.get("ok") is False or commit.get("plan_token") != plan_token:
        raise _TurnContentError(
            str(commit.get("status") or "prepare_failed"),
            str(commit.get("error") or "connector prepare commit failed"),
            conflict=str(commit.get("status") or "")
            in {"revision_conflict", "stale_revision", "stale_ref"},
        )
    committed_state = str(commit.get("state") or state_name)
    if committed_state not in {"active", "waiting_predecessor", "completed"}:
        raise _TurnContentError(
            "invalid_prepare_response",
            "prepare commit did not activate the plan",
        )
    job_count = commit.get("job_count")
    if committed_state != "completed" and (
        isinstance(job_count, bool)
        or not isinstance(job_count, int)
        or job_count < len(parts)
    ):
        raise _TurnContentError(
            "invalid_prepare_response",
            "prepare commit returned invalid job count",
        )
    generation = commit.get("generation", begin.get("generation", 1))
    if (
        isinstance(generation, bool)
        or not isinstance(generation, int)
        or generation < 1
    ):
        raise _TurnContentError(
            "invalid_prepare_response",
            "prepare response returned an invalid plan generation",
        )
    entry["pending_turn_id"] = _turn_id(item)
    entry["pending_content_revision"] = revision
    entry["pending_plan_token"] = plan_token
    entry["pending_turn_part_count"] = len(parts)
    entry["pending_turn_job_count"] = int(job_count or 0)
    entry["pending_turn_user_hash"] = _turn_user_hash(item)
    entry["pending_plan_generation"] = generation
    final_identity = item.get(_TURN_FINAL_IDENTITY_KEY)
    if isinstance(final_identity, str) and final_identity:
        entry["pending_final_identity"] = final_identity
    return True, page_calls


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


def _bootstrap_existing_turns(
    store: dict[str, Any],
    turns_payload: dict[str, Any],
    pending_payload: dict[str, Any],
    *,
    skip_v2_finals: bool = False,
) -> int:
    """Record current Tendwire rows as seen on first deployment.

    The pre-slim source ledger used different identities. Without this bootstrap,
    the first source-only sync can repost historical rows. This migration is
    intentionally one-way and Telegram-silent.
    """
    if store.get("tendwired_bootstrap_complete"):
        return 0
    skipped = 0
    for item in _turns(turns_payload):
        if _turn_has_content_outcome(item):
            continue
        _key, entry = _entry_for_turn(store, item)
        if entry is None:
            continue
        turn_id = _turn_id(item)
        if not turn_id:
            continue
        if _turn_has_complete_final(item):
            if skip_v2_finals and _content_revision(item):
                continue
            revision = _content_revision(item)
            content_hash = revision or _turn_content_hash(item, "final")
            identity = f"final:{turn_id}:{content_hash}"
            record = {"worker_id": entry.get("tendwire_worker_id"), "turn_id": turn_id}
            if revision:
                record["content_revision"] = revision
                entry["last_clean_content_revision"] = revision
            state.mark_delivered(store, identity, record)
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
    relist_on_conflict: bool = True,
    chat_id: str,
    live_worker_ids: set[str] | None = None,
    yield_barrier: Any | None = None,
    list_finals_are_authoritative: bool = True,
) -> dict[str, int]:
    counts = {"feed_sent": 0, "sent": 0, "updated": 0, "content_pages": 0}
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
    placeholder_turn_ids_by_worker: dict[str, set[str]] = {}
    # Pass 1: real content only (user prompt, stream, or a completed final).
    # Tendwire store output is already ordered by per-worker observed recency.
    # Payload updated_at can be absent on current worker-derived turns, so do
    # not let an older command row with updated_at suppress the live turn.
    for item in turns:
        if _turn_has_content_outcome(item):
            continue
        _key, entry = _entry_for_turn(store, item)
        if entry is None:
            continue
        worker_key = str(entry.get("tendwire_worker_id") or item.get("worker_id") or "")
        if not worker_key:
            continue
        complete = _turn_has_complete_final(item)
        has_real_content = (
            bool(item.get("assistant_stream_text"))
            or bool(item.get("user_text"))
            or complete
        )
        if not has_real_content:
            continue
        latest_content_turn_by_worker.setdefault(worker_key, _turn_id(item))
    # Pass 2: synthetic "Work is in progress." placeholders only fill workers
    # with no real turn at all — a placeholder must never outrank a real turn.
    for item in turns:
        if _turn_has_content_outcome(item):
            continue
        _key, entry = _entry_for_turn(store, item)
        if entry is None:
            continue
        worker_key = str(entry.get("tendwire_worker_id") or item.get("worker_id") or "")
        if not worker_key:
            continue
        if _turn_is_working_placeholder(item, entry):
            placeholder_turn_ids_by_worker.setdefault(worker_key, set()).add(
                _turn_id(item)
            )
        if worker_key in latest_content_turn_by_worker:
            continue
        if _turn_is_working_placeholder(item, entry):
            latest_content_turn_by_worker.setdefault(worker_key, _turn_id(item))
    seen_final_workers: set[str] = set()
    seen_working_workers: set[str] = set()
    fold_state: dict[str, int] = {"issued": 0}
    turn_count = len(turns)
    for idx, item in enumerate(turns):
        if _turn_has_content_outcome(item):
            continue
        entry_key, entry = _entry_for_turn(store, item)
        if entry is None:
            continue
        before = dict(entry)
        repaired_open_final = False
        worker_key = str(entry.get("tendwire_worker_id") or item.get("worker_id") or "")
        latest_turn_id = latest_content_turn_by_worker.get(worker_key)
        complete = _turn_has_complete_final(item)
        if complete:
            if not list_finals_are_authoritative and _content_revision(item):
                continue
            if latest_turn_id and _turn_id(item) != latest_turn_id:
                delivered = False
                content_hash = _content_revision(item) or _turn_content_hash(item, "final")
                counts["updated"] += int(_suppress_historical_final(store, item, content_hash))
                counts["updated"] += int(_fold_superseded_final(store, item, entry, runtime, chat_id=chat_id, fold_state=fold_state))
                continue
            if worker_key in seen_final_workers:
                continue
            seen_final_workers.add(worker_key)
            if _content_revision(item):
                try:
                    _staged, page_calls = _stage_final_plan(
                        store, item, entry, runtime
                    )
                    counts["content_pages"] += page_calls
                except _TurnContentError as exc:
                    if exc.conflict and relist_on_conflict:
                        raise
                    item[_TURN_CONTENT_OUTCOME_KEY] = _turn_local_outcome(
                        item, exc.status
                    )
                    continue
                delivered = False
            else:
                delivered = _deliver_final(store, item, entry, runtime, chat_id=chat_id)
                if delivered:
                    # Legacy inline direct-call compatibility; v2 finals are spoken only after the
                    # complete ordered plan is acknowledged.
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
            previous_stream_turn_id = str(entry.get("last_stream_turn_id") or "")
            reuse_previous_working = bool(
                item.get("assistant_stream_text")
                and previous_stream_turn_id
                and previous_stream_turn_id != _turn_id(item)
                and previous_stream_turn_id
                in placeholder_turn_ids_by_worker.get(worker_key, set())
            )
            delivered = _deliver_working(
                store,
                item,
                entry,
                runtime,
                chat_id=chat_id,
                reuse_previous_working=reuse_previous_working,
            )
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


def _after_provider_accept(runtime: SyncRuntime) -> None:
    if runtime.after_provider_accept is not None:
        runtime.after_provider_accept()

def _checkpoint_turn_job(runtime: SyncRuntime) -> None:
    if runtime.checkpoint is not None:
        runtime.checkpoint()


_PUBLIC_OPAQUE_CHARS = frozenset(
    "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_"
)
_PUBLIC_LABEL_CHARS = _PUBLIC_OPAQUE_CHARS | frozenset(".")


def _strict_public_opaque(value: Any, prefix: str, field: str) -> str:
    if (
        not isinstance(value, str)
        or not value.startswith(prefix)
        or not value[len(prefix) :]
        or any(
            char not in _PUBLIC_OPAQUE_CHARS
            for char in value[len(prefix) :]
        )
        or len(value) > 200
    ):
        raise _TurnContentError(
            "invalid_turn_final_job", f"invalid public {field}"
        )
    return value


def _strict_public_label(
    value: Any, field: str, *, prefix: str | None = None
) -> str:
    if (
        not isinstance(value, str)
        or not value
        or len(value) > 200
        or (prefix is not None and not value.startswith(prefix))
        or any(char not in _PUBLIC_LABEL_CHARS for char in value)
    ):
        raise _TurnContentError(
            "invalid_turn_final_job", f"invalid public {field}"
        )
    return value


def _validate_ready_descriptor(
    value: Any,
    field: str,
    *,
    final_required: bool = False,
) -> dict[str, Any]:
    expected = {
        "availability",
        "inline",
        "char_length",
        "byte_length",
        "page_count",
        "first_cursor",
    }
    if not isinstance(value, dict) or set(value) != expected:
        raise _TurnContentError(
            "invalid_turn_final_job",
            f"invalid {field} descriptor shape",
        )
    availability = value.get("availability")
    inline = value.get("inline")
    char_length = value.get("char_length")
    byte_length = value.get("byte_length")
    page_count = value.get("page_count")
    cursor = value.get("first_cursor")
    if (
        type(inline) is not bool
        or inline
        or any(
            isinstance(number, bool)
            or not isinstance(number, int)
            or number < 0
            for number in (char_length, byte_length, page_count)
        )
        or byte_length < char_length
    ):
        raise _TurnContentError(
            "invalid_turn_final_job",
            f"invalid {field} descriptor values",
        )
    if availability == "absent":
        if (
            final_required
            or char_length
            or byte_length
            or page_count
            or cursor is not None
        ):
            raise _TurnContentError(
                "invalid_turn_final_job",
                f"inconsistent absent {field}",
            )
    elif availability == "complete":
        if char_length == 0:
            if byte_length or page_count or cursor is not None:
                raise _TurnContentError(
                    "invalid_turn_final_job",
                    f"inconsistent empty {field}",
                )
        elif (
            page_count <= 0
            or not isinstance(cursor, str)
            or not cursor.startswith("twcur1.")
            or not cursor[7:]
            or any(
                char not in _PUBLIC_OPAQUE_CHARS for char in cursor[7:]
            )
        ):
            raise _TurnContentError(
                "invalid_turn_final_job",
                f"unpageable complete {field}",
            )
    else:
        raise _TurnContentError(
            "invalid_turn_final_job",
            f"invalid {field} availability",
        )
    return value


def _validate_final_ready_payload(
    payload: Any,
    *,
    delivery_key: str | None = None,
) -> dict[str, Any]:
    base_fields = {
        "schema_version",
        "operation",
        "final_identity",
        "turn_id",
        "worker_id",
        "space_id",
        "content_revision",
        "content",
    }
    schema = (
        payload.get("schema_version")
        if isinstance(payload, dict)
        else None
    )
    expected = (
        base_fields | {"stable_key", "stable_key_version"}
        if type(schema) is int and schema == 2
        else base_fields
    )
    if (
        type(schema) is int
        and schema == 2
        and isinstance(payload, dict)
        and "working_predecessor_turn_id" in payload
    ):
        expected.add("working_predecessor_turn_id")
    if (
        not isinstance(payload, dict)
        or set(payload) != expected
        or type(schema) is not int
        or schema not in {1, 2}
        or payload.get("operation") != "materialize"
    ):
        raise _TurnContentError(
            "invalid_turn_final_job",
            "final-ready payload shape is invalid",
        )
    if schema == 2 and not state.valid_stable_worker_key_pair(
        payload.get("stable_key"),
        payload.get("stable_key_version"),
    ):
        raise _TurnContentError(
            "invalid_turn_final_job",
            "final-ready stable worker identity is invalid",
        )
    predecessor = payload.get("working_predecessor_turn_id")
    if predecessor is not None:
        _strict_public_label(
            predecessor,
            "working predecessor turn id",
            prefix="turn-",
        )
        if predecessor == payload.get("turn_id"):
            raise _TurnContentError(
                "invalid_turn_final_job",
                "working predecessor must differ from final turn",
            )
    final_identity = _strict_public_opaque(
        payload.get("final_identity"),
        "twfinal1.",
        "final identity",
    )
    if (
        delivery_key is not None
        and delivery_key != f"turn-final:revision:{final_identity}"
    ):
        raise _TurnContentError(
            "invalid_turn_final_job",
            "final-ready delivery key is inconsistent",
        )
    _strict_public_label(
        payload.get("turn_id"), "turn id", prefix="turn-"
    )
    _strict_public_label(payload.get("worker_id"), "worker id")
    if payload.get("space_id") is not None:
        _strict_public_label(payload.get("space_id"), "space id")
    _strict_public_opaque(
        payload.get("content_revision"),
        "twrev1.",
        "content revision",
    )
    content = payload.get("content")
    if (
        not isinstance(content, dict)
        or set(content)
        != {
            "schema_version",
            "content_revision",
            "known_incomplete",
            "fields",
        }
        or type(content.get("schema_version")) is not int
        or content.get("schema_version") != TURN_CONTENT_SCHEMA_VERSION
        or content.get("content_revision")
        != payload["content_revision"]
        or content.get("known_incomplete") is not False
        or not isinstance(content.get("fields"), dict)
        or set(content["fields"])
        != {"user_text", "assistant_final_text"}
    ):
        raise _TurnContentError(
            "invalid_turn_final_job",
            "final-ready content descriptor is invalid",
        )
    _validate_ready_descriptor(
        content["fields"]["user_text"], "user_text"
    )
    _validate_ready_descriptor(
        content["fields"]["assistant_final_text"],
        "assistant_final_text",
        final_required=True,
    )
    return payload


_TURN_FINAL_SOURCE_OWNERS_KEY = "tendwire_turn_final_source_owners"
_TURN_FINAL_IDENTITY_KEY = "_herdres_final_identity"
_TURN_FINAL_WORKING_PREDECESSOR_KEY = (
    "_herdres_working_predecessor_turn_id"
)


def _public_turn_stable_identity(
    item: dict[str, Any],
) -> tuple[str, int] | None:
    stable_key = item.get("stable_key")
    stable_key_version = item.get("stable_key_version")
    if stable_key is None and stable_key_version is None:
        meta = item.get("meta")
        if isinstance(meta, dict):
            stable_key = meta.get("stable_key")
            stable_key_version = meta.get("stable_key_version")
    if not state.valid_stable_worker_key_pair(
        stable_key, stable_key_version
    ):
        return None
    return str(stable_key), int(stable_key_version)


def _final_ready_row(payload: dict[str, Any]) -> dict[str, Any]:
    row = {
        "id": payload["turn_id"],
        "worker_id": payload["worker_id"],
        "space_id": payload["space_id"],
        "complete": True,
        _TURN_FINAL_IDENTITY_KEY: payload["final_identity"],
        "content": {
            **payload["content"],
            "content_revision": payload["content_revision"],
        },
    }
    identity = _public_turn_stable_identity(payload)
    if identity is not None:
        row["stable_key"] = identity[0]
        row["stable_key_version"] = identity[1]
    validated = _validate_turn_row(row)
    predecessor = payload.get("working_predecessor_turn_id")
    if isinstance(predecessor, str):
        validated[_TURN_FINAL_WORKING_PREDECESSOR_KEY] = predecessor
    return validated


def _turn_final_source_owners(
    store: dict[str, Any], *, create: bool = False
) -> dict[str, Any]:
    owners = store.get(_TURN_FINAL_SOURCE_OWNERS_KEY)
    if isinstance(owners, dict):
        return owners
    if not create:
        return {}
    owners = {}
    store[_TURN_FINAL_SOURCE_OWNERS_KEY] = owners
    return owners


def _canonical_final_source_owner(
    record: Mapping[str, Any],
) -> dict[str, Any] | None:
    if not isinstance(record, Mapping):
        return None
    turn_id = record.get("turn_id")
    content_revision = record.get("content_revision")
    stable_key = record.get("stable_key")
    stable_key_version = record.get("stable_key_version")
    if (
        not isinstance(turn_id, str)
        or not turn_id
        or not isinstance(content_revision, str)
        or not content_revision
        or not state.valid_stable_worker_key_pair(
            stable_key, stable_key_version
        )
    ):
        return None
    return {
        "turn_id": turn_id,
        "content_revision": content_revision,
        "stable_key": stable_key,
        "stable_key_version": stable_key_version,
    }


def _resolve_final_source_entry(
    store: dict[str, Any],
    item: dict[str, Any],
) -> tuple[str | None, dict[str, Any] | None]:
    identity = _public_turn_stable_identity(item)
    if identity is None:
        return None, None
    entry_key, worker_entry = state.find_worker_entry_by_stable_key(
        store, identity[0]
    )
    if (
        entry_key is None
        or worker_entry is None
        or state.entry_stable_identity(worker_entry) != identity
        or not state.worker_entry_is_uniquely_routable(
            store, entry_key, worker_entry
        )
    ):
        return None, None
    if config.source_topic_mode() == "worker":
        return entry_key, worker_entry
    _space_key, space_entry = state.find_space_entry_by_id(
        store, _entry_space_id(worker_entry)
    )
    if space_entry is None:
        return None, None
    return entry_key, _delivery_entry(space_entry, worker_entry)


def _bind_or_verify_final_source_owner(
    store: dict[str, Any],
    payload: dict[str, Any],
    entry_key: str | None,
    entry: dict[str, Any],
    *,
    allow_bind: bool,
) -> tuple[bool, bool]:
    identity = _public_turn_stable_identity(payload)
    if (
        identity is None
        or not entry_key
        or state.entry_stable_identity(entry) != identity
        or not state.worker_entry_is_uniquely_routable(
            store, entry_key, entry
        )
    ):
        return False, False
    record = {
        "turn_id": payload["turn_id"],
        "content_revision": payload["content_revision"],
        "stable_key": identity[0],
        "stable_key_version": identity[1],
    }
    final_identity = str(payload["final_identity"])
    owners = _turn_final_source_owners(store)
    if final_identity in owners:
        existing = _canonical_final_source_owner(
            owners[final_identity]
        )
        if existing is None or existing != record:
            return False, False
        if owners[final_identity] != record:
            owners[final_identity] = record
            return True, True
        return True, False
    if not allow_bind:
        return False, False
    owners = _turn_final_source_owners(store, create=True)
    owners[final_identity] = record
    return True, True


def _clear_final_source_owner(
    store: dict[str, Any], final_identity: Any
) -> None:
    if not isinstance(final_identity, str) or not final_identity:
        return
    owners = store.get(_TURN_FINAL_SOURCE_OWNERS_KEY)
    if not isinstance(owners, dict):
        return
    owners.pop(final_identity, None)
    if not owners:
        store.pop(_TURN_FINAL_SOURCE_OWNERS_KEY, None)


def _materialize_final_ready(
    payload: dict[str, Any],
    runtime: SyncRuntime,
) -> tuple[dict[str, Any], int]:
    row = _final_ready_row(payload)
    page_calls = _materialize_turn_item(row, runtime)
    return row, page_calls


def _materialize_legacy_plan_source(
    turns_payload: dict[str, Any],
    revision: str,
    runtime: SyncRuntime,
) -> tuple[dict[str, Any] | None, int]:
    matches = [
        item
        for item in _turns(turns_payload)
        if _content_revision(item) == revision
    ]
    if len(matches) != 1:
        return None, 0
    item = matches[0]
    page_calls = _materialize_turn_item(item, runtime)
    return item, page_calls


def _turn_item_by_revision(
    turns_payload: dict[str, Any], revision: str
) -> dict[str, Any] | None:
    matches = [item for item in _turns(turns_payload) if _content_revision(item) == revision]
    return matches[0] if len(matches) == 1 else None


def _slot_binding(
    store: dict[str, Any],
    *,
    turn_id: str,
    ordinal: int,
    plan_token: str = "",
) -> tuple[str, dict[str, Any]] | tuple[None, None]:
    candidates: list[tuple[str, dict[str, Any]]] = []
    for message_id, binding in _final_delivery_bindings(store, turn_id):
        binding_ordinal = binding.get("part_ordinal")
        if binding_ordinal is None:
            ids = [
                str(value)
                for value in (binding.get("message_ids") or [])
                if str(value or "")
            ]
            if ids:
                binding_ordinal = ids.index(message_id) if message_id in ids else None
        if binding_ordinal is None:
            entry_ids = [
                str(value)
                for entry in state.source_entries(store).values()
                if entry.get("last_turn_id") == turn_id
                for value in (entry.get("last_clean_message_ids") or [])
            ]
            binding_ordinal = entry_ids.index(message_id) if message_id in entry_ids else 0
        if binding_ordinal != ordinal:
            continue
        if plan_token and binding.get("plan_token") not in (None, "", plan_token):
            continue
        candidates.append((message_id, binding))
    return candidates[-1] if candidates else (None, None)


def _owning_bot_token(store: dict[str, Any], bot_kind: str) -> str | None:
    if not bot_kind or bot_kind == MANAGER_BOT_KIND:
        return None
    token = managed_bot_token(_telegram_state(store), bot_kind)
    if not token:
        raise _TurnContentError(
            "missing_message_owner_token",
            f"cannot retire a message owned by unavailable bot kind {bot_kind}",
        )
    return token


def _retire_local_message(
    store: dict[str, Any], entry: dict[str, Any], message_id: str
) -> None:
    state.message_bindings(store).pop(str(message_id), None)
    _clear_entry_message_reference(entry, str(message_id), "final")
    if entry.get("last_stream_message_id") == str(message_id):
        _clear_stream_delivery_keys(entry)


def _current_upsert_candidate(
    store: dict[str, Any],
    item: dict[str, Any],
    entry: dict[str, Any],
    *,
    ordinal: int,
    replaces_plan_token: str,
) -> tuple[str, str, str, str]:
    turn_id = _turn_id(item)
    if ordinal == 0:
        working_id = str(entry.get("last_stream_message_id") or "")
        predecessor_turn_id = str(
            item.get(_TURN_FINAL_WORKING_PREDECESSOR_KEY)
            or entry.get("pending_working_predecessor_turn_id")
            or ""
        )
        stream_turn_id = str(
            entry.get("last_stream_turn_id") or ""
        )
        if working_id and (
            stream_turn_id == turn_id
            or (
                predecessor_turn_id
                and stream_turn_id == predecessor_turn_id
            )
        ):
            binding = state.find_message_binding(store, working_id)
            return (
                working_id,
                str((binding or {}).get("bot_kind") or entry.get("last_stream_bot_kind") or MANAGER_BOT_KIND),
                str((binding or {}).get("topic_id") or entry.get("topic_id") or ""),
                "working",
            )
    message_id, binding = _slot_binding(
        store,
        turn_id=turn_id,
        ordinal=ordinal,
        plan_token=replaces_plan_token,
    )
    if message_id and binding:
        return (
            message_id,
            str(binding.get("bot_kind") or entry.get("last_clean_bot_kind") or MANAGER_BOT_KIND),
            str(binding.get("topic_id") or ""),
            "final",
        )
    return "", "", "", ""


def _validate_turn_final_item(item: dict[str, Any]) -> dict[str, Any]:
    key = item.get("key")
    ref = item.get("ref")
    if not isinstance(key, str):
        raise _TurnContentError(
            "invalid_turn_final_job",
            "turn-final delivery key is invalid",
        )
    _strict_public_opaque(ref, "twref1.", "lease ref")
    payload = item.get("payload")
    if key.startswith("turn-final:revision:"):
        return _validate_final_ready_payload(
            payload, delivery_key=key
        )
    if not key.startswith("turn-final:twplan1.") or not isinstance(
        payload, dict
    ):
        raise _TurnContentError(
            "invalid_turn_final_job",
            "turn-final lease identity is invalid",
        )
    required = {
        "schema_version",
        "plan_token",
        "content_revision",
        "presentation_version",
        "operation",
        "sequence_index",
        "part_ordinal",
        "part_count",
        "spans",
    }
    if (
        not required.issubset(payload)
        or type(payload.get("schema_version")) is not int
        or payload.get("schema_version") != 1
    ):
        raise _TurnContentError(
            "invalid_turn_final_job",
            "turn-final payload is incomplete",
        )
    plan_token = _strict_public_opaque(
        payload.get("plan_token"), "twplan1.", "plan token"
    )
    revision = _strict_public_opaque(
        payload.get("content_revision"),
        "twrev1.",
        "content revision",
    )
    sequence = payload.get("sequence_index")
    ordinal = payload.get("part_ordinal")
    part_count = payload.get("part_count")
    if (
        payload.get("presentation_version") != PRESENTATION_VERSION
        or payload.get("operation") not in {"upsert", "retire"}
        or any(
            isinstance(value, bool)
            or not isinstance(value, int)
            or value < 0
            for value in (sequence, ordinal)
        )
        or isinstance(part_count, bool)
        or not isinstance(part_count, int)
        or part_count <= 0
        or key != f"turn-final:{plan_token}:{sequence:06d}"
    ):
        raise _TurnContentError(
            "invalid_turn_final_job",
            "turn-final payload identity is inconsistent",
        )
    if "turn" in payload:
        source = _validate_final_ready_payload(payload.get("turn"))
        if source["content_revision"] != revision:
            raise _TurnContentError(
                "invalid_turn_final_job",
                "turn-final plan and source revisions differ",
            )
    replaces = payload.get("replaces_plan_token")
    if replaces is not None:
        _strict_public_opaque(
            replaces, "twplan1.", "replaced plan token"
        )
    predecessor_job_key = payload.get("predecessor_job_key")
    if predecessor_job_key is not None and (
        not isinstance(predecessor_job_key, str)
        or not predecessor_job_key.startswith("turn-final:twplan1.")
        or predecessor_job_key == key
    ):
        raise _TurnContentError(
            "invalid_turn_final_job",
            "turn-final predecessor receipt identity is invalid",
        )
    spans = payload.get("spans")
    operation = payload.get("operation")
    if (
        not isinstance(spans, list)
        or (
            operation == "upsert"
            and (ordinal >= part_count or not spans)
        )
        or (operation == "retire" and (ordinal < part_count or spans))
    ):
        raise _TurnContentError(
            "invalid_turn_final_job",
            "turn-final operation coordinates are invalid",
        )
    for span in spans:
        if (
            not isinstance(span, dict)
            or set(span) != {"field", "start_char", "end_char"}
            or span.get("field")
            not in {"user_text", "assistant_final_text"}
            or isinstance(span.get("start_char"), bool)
            or not isinstance(span.get("start_char"), int)
            or isinstance(span.get("end_char"), bool)
            or not isinstance(span.get("end_char"), int)
            or span["start_char"] < 0
            or span["end_char"] <= span["start_char"]
        ):
            raise _TurnContentError(
                "invalid_turn_final_job",
                "turn-final span is invalid",
            )
    return payload


def _maybe_complete_turn_plan(
    store: dict[str, Any],
    item: dict[str, Any],
    entry: dict[str, Any],
    *,
    plan_token: str,
    revision: str,
) -> bool:
    expected_jobs = entry.get("pending_turn_job_count")
    if (
        isinstance(expected_jobs, bool)
        or not isinstance(expected_jobs, int)
        or expected_jobs <= 0
    ):
        return False
    receipts = [
        receipt
        for receipt in state.tendwire_turn_jobs(store).values()
        if isinstance(receipt, dict)
        and receipt.get("plan_token") == plan_token
    ]
    if len(receipts) < expected_jobs or any(
        receipt.get("substate") != "acknowledged"
        for receipt in receipts
    ):
        return False
    part_count = entry.get("pending_turn_part_count")
    if (
        isinstance(part_count, bool)
        or not isinstance(part_count, int)
        or part_count <= 0
    ):
        return False
    selected_bindings: dict[int, tuple[str, dict[str, Any]]] = {}
    for message_id, binding in _final_delivery_bindings(
        store, _turn_id(item)
    ):
        binding_plan = str(binding.get("plan_token") or "")
        binding_revision = str(
            binding.get("content_revision") or ""
        )
        if binding_plan != plan_token and binding_revision != revision:
            continue
        ordinal = binding.get("part_ordinal")
        if not isinstance(ordinal, int) or not 0 <= ordinal < part_count:
            continue
        existing = selected_bindings.get(ordinal)
        if existing is None or binding_plan == plan_token:
            selected_bindings[ordinal] = (message_id, binding)
    bindings = [
        (ordinal, *selected_bindings[ordinal])
        for ordinal in sorted(selected_bindings)
    ]
    if [
        ordinal for ordinal, _message_id, _binding in bindings
    ] != list(range(part_count)):
        return False
    message_ids = [
        message_id for _ordinal, message_id, _binding in bindings
    ]
    bot_kind = str(
        bindings[0][2].get("bot_kind") or MANAGER_BOT_KIND
    )
    identity = f"final:{_turn_id(item)}:{revision}"
    state.mark_delivered(
        store,
        identity,
        {
            "worker_id": entry.get("tendwire_worker_id"),
            "turn_id": _turn_id(item),
            "content_revision": revision,
        },
    )
    _set_final_delivery(
        entry,
        turn_id=_turn_id(item),
        content_hash=revision,
        user_hash=str(entry.get("pending_turn_user_hash") or "")
        or _turn_user_hash(item),
        message_ids=message_ids,
        bot_kind=bot_kind,
        render_version=RENDER_VERSION,
    )
    entry["last_clean_content_revision"] = revision
    entry["last_clean_plan_token"] = plan_token
    final_identity = (
        entry.get("pending_final_identity")
        or item.get(_TURN_FINAL_IDENTITY_KEY)
    )
    _clear_final_source_owner(store, final_identity)
    for field in (
        "pending_turn_id",
        "pending_content_revision",
        "pending_plan_token",
        "pending_turn_part_count",
        "pending_turn_job_count",
        "pending_turn_user_hash",
        "pending_plan_generation",
        "pending_acknowledged_prefix_count",
        "replaces_failed_plan_token",
        "pending_final_identity",
        "pending_working_predecessor_turn_id",
    ):
        entry.pop(field, None)
    _clear_stream_delivery_state(entry, _turn_id(item))
    _record_delivery_success(entry, bot_kind)
    return True


def _reconcile_completed_turn_plans(
    store: dict[str, Any],
    runtime: SyncRuntime,
    *,
    pending_entry: dict[str, Any] | None = None,
) -> int:
    if not runtime.with_outbox or runtime.dry_run:
        return 0
    reconciled = 0
    entries = (
        (pending_entry,)
        if pending_entry is not None
        else state.source_worker_entries(store).values()
    )
    for entry in entries:
        plan_token = entry.get("pending_plan_token")
        revision = entry.get("pending_content_revision")
        turn_id = entry.get("pending_turn_id")
        part_count = entry.get("pending_turn_part_count")
        if (
            not isinstance(plan_token, str)
            or not plan_token.startswith("twplan1.")
            or not isinstance(revision, str)
            or not revision.startswith("twrev1.")
            or not isinstance(turn_id, str)
            or not turn_id
            or isinstance(part_count, bool)
            or not isinstance(part_count, int)
            or part_count <= 0
        ):
            continue
        observed = runtime.tendwire.connector_prepare_commit(
            plan_token=plan_token
        )
        observed_token = observed.get("plan_token")
        plan_not_found = (
            observed.get("ok") is False
            and observed.get("status") == "plan_not_found"
            and (observed_token is None or observed_token == plan_token)
        )
        superseded = (
            observed.get("ok") is True
            and observed_token == plan_token
            and observed.get("state") == "superseded"
        )
        if plan_not_found or superseded:
            _clear_final_source_owner(
                store, entry.get("pending_final_identity")
            )
            for field in (
                "pending_turn_id",
                "pending_content_revision",
                "pending_plan_token",
                "pending_turn_part_count",
                "pending_turn_job_count",
                "pending_turn_user_hash",
                "pending_plan_generation",
                "pending_acknowledged_prefix_count",
                "replaces_failed_plan_token",
                "pending_final_identity",
                "pending_working_predecessor_turn_id",
            ):
                entry.pop(field, None)
            reconciled += 1
            _checkpoint_turn_job(runtime)
            continue
        if (
            observed.get("ok") is not True
            or observed_token != plan_token
            or observed.get("state") != "completed"
        ):
            continue
        job_count = observed.get("job_count")
        if (
            isinstance(job_count, bool)
            or not isinstance(job_count, int)
            or job_count <= 0
        ):
            continue
        entry["pending_turn_job_count"] = job_count
        advanced = False
        for job_key, receipt in list(
            state.tendwire_turn_jobs(store).items()
        ):
            if (
                isinstance(receipt, dict)
                and receipt.get("plan_token") == plan_token
                and receipt.get("substate")
                in {"telegram_applied", "old_slot_retired"}
            ):
                state.update_tendwire_turn_job(
                    store, job_key, substate="acknowledged"
                )
                advanced = True
        item = {
            "id": turn_id,
            "worker_id": _entry_worker_id(entry),
            "space_id": _entry_space_id(entry),
        }
        if _maybe_complete_turn_plan(
            store,
            item,
            entry,
            plan_token=plan_token,
            revision=revision,
        ):
            reconciled += 1
            advanced = True
        if advanced:
            _checkpoint_turn_job(runtime)
    return reconciled


_TURN_FINAL_FAILURE_REASON_CODES = frozenset(
    {
        "content_fetch_failed",
        "content_known_incomplete",
        "content_revision_not_found",
        "delivery_rejected",
        "delivery_uncertain",
        "invalid_content_page",
        "invalid_content_schema",
        "invalid_pending_plan",
        "invalid_prepare_response",
        "invalid_presentation_plan",
        "invalid_recovery_predecessor_receipt",
        "invalid_turn_final_job",
        "missing_message_owner_token",
        "prepare_failed",
        "presentation_plan_mismatch",
        "receipt_reservation_failed",
        "revision_conflict",
        "stale_or_unavailable_content_revision",
        "stale_or_unroutable_turn_plan",
        "stale_ref",
        "stale_revision",
        "timeout",
        "unsupported_content_schema",
        "unroutable_final_ready",
        "upgrade_required",
    }
)
_TURN_FINAL_DEFER_REASON_CODES = frozenset(
    {
        "operation_budget_exhausted",
        "predecessor_pending",
        "rate_limited",
        "transient_delivery",
    }
)
_TURN_FINAL_DEFER_REASON_ALIASES = {
    "earlier presentation plan is still pending": "predecessor_pending",
    "edit target unavailable; retry as send": "transient_delivery",
    "physical operation budget exhausted": "operation_budget_exhausted",
}


def _turn_final_reason_code(
    reason: str,
    *,
    uncertain: bool = False,
    deferred: bool = False,
) -> str:
    if uncertain:
        return "delivery_uncertain"
    candidate = str(reason).split(":", 1)[0].strip()
    if deferred:
        if candidate in _TURN_FINAL_DEFER_REASON_CODES:
            return candidate
        return _TURN_FINAL_DEFER_REASON_ALIASES.get(
            str(reason), "transient_delivery"
        )
    if candidate in _TURN_FINAL_FAILURE_REASON_CODES:
        return candidate
    return "delivery_rejected"


def _fail_turn_final(
    runtime: SyncRuntime,
    ref: str,
    reason: str,
    result: dict[str, Any],
    *,
    uncertain: bool = False,
) -> None:
    reason_code = _turn_final_reason_code(
        reason, uncertain=uncertain
    )
    response = runtime.tendwire.turn_final_fail(ref, reason_code)
    result["failed"] += 1
    result["changed"] = True
    result["status"] = reason_code
    response_status = str(response.get("status") or "")
    if response_status == "attempts_exhausted":
        result["status"] = response_status
    if uncertain:
        result["uncertain"] += 1
        result["status"] = "delivery_uncertain"
    if response.get("ok") is False:
        result["status"] = str(
            response.get("status") or "turn_final_fail_failed"
        )


def _telegram_result_is_transient(result: dict[str, Any]) -> bool:
    if str(result.get("kind") or "") == "transient":
        return True
    error = str(result.get("error") or "").lower()
    return any(marker in error for marker in ("rate limit", "too many requests", "retry after"))


def _defer_turn_final(
    runtime: SyncRuntime,
    ref: str,
    reason: str,
    result: dict[str, Any],
    store: dict[str, Any] | None = None,
    job_key: str = "",
    *,
    delay_seconds: int,
) -> None:
    if store is not None and job_key:
        receipt = state.find_tendwire_turn_job(store, job_key)
        if (
            receipt is not None
            and receipt.get("substate") == "reserved"
        ):
            state.update_tendwire_turn_job(
                store,
                job_key,
                substate="retryable",
            )
            _checkpoint_turn_job(runtime)
    reason_code = _turn_final_reason_code(reason, deferred=True)
    response = runtime.tendwire.turn_final_defer(
        ref,
        reason_code,
        delay_seconds=max(1, int(delay_seconds)),
    )
    result["deferred"] += 1
    result["changed"] = True
    if response.get("ok") is False:
        result["status"] = str(
            response.get("status") or "turn_final_defer_failed"
        )


def _drain_turn_final(
    store: dict[str, Any],
    turns_payload: dict[str, Any],
    runtime: SyncRuntime,
    *,
    chat_id: str,
    max_operations: int,
    turn_projection: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    result = {
        "enabled": runtime.with_outbox,
        "polled": 0,
        "operations": 0,
        "delivered": 0,
        "acked": 0,
        "failed": 0,
        "deferred": 0,
        "uncertain": 0,
        "staged": 0,
        "content_pages": 0,
        "changed": False,
    }
    failed_job_key = ""
    if (
        not runtime.with_outbox
        or max_operations <= 0
        or runtime.dry_run
    ):
        return result
    materialized_sources: dict[
        str, tuple[dict[str, Any], dict[str, Any]]
    ] = {}
    lease_seconds = config.tendwire_turn_final_lease_seconds()
    for _iteration in range(max_operations + 100):
        if result["operations"] >= max_operations:
            break
        poll = runtime.tendwire.turn_final_poll(
            limit=1,
            lease_seconds=lease_seconds,
        )
        if poll.get("ok") is False:
            result["status"] = str(
                poll.get("status") or "turn_final_poll_failed"
            )
            result["changed"] = True
            break
        jobs = [
            job
            for job in poll.get("items", [])
            if isinstance(job, dict)
        ]
        if not jobs:
            break
        result["polled"] += 1
        lease = jobs[0]
        ref = str(lease.get("ref") or "")
        try:
            payload = _validate_turn_final_item(lease)
        except _TurnContentError as exc:
            _fail_turn_final(
                runtime, ref, f"{exc.status}: {exc}", result
            )
            break

        if payload["operation"] == "materialize":
            try:
                item = _final_ready_row(payload)
            except _TurnContentError as exc:
                _fail_turn_final(
                    runtime, ref, f"{exc.status}: {exc}", result
                )
                break
            _entry_key, entry = _resolve_final_source_entry(
                store, payload
            )
            if entry is None or not str(entry.get("topic_id") or ""):
                owner_matches = False
                owner_bound = False
            else:
                owner_matches, owner_bound = (
                    _bind_or_verify_final_source_owner(
                        store,
                        payload,
                        _entry_key,
                        entry,
                        allow_bind=True,
                    )
                )
            if not owner_matches:
                _defer_turn_final(
                    runtime,
                    ref,
                    "transient_delivery",
                    result,
                    delay_seconds=1,
                )
                break
            if owner_bound:
                _checkpoint_turn_job(runtime)
            pending_plan = str(
                entry.get("pending_plan_token") or ""
            )
            pending_revision = str(
                entry.get("pending_content_revision") or ""
            )
            if (
                pending_plan
                and pending_revision
                not in {"", payload["content_revision"]}
            ):
                _reconcile_completed_turn_plans(
                    store,
                    runtime,
                    pending_entry=entry,
                )
                pending_plan = str(
                    entry.get("pending_plan_token") or ""
                )
                pending_revision = str(
                    entry.get("pending_content_revision") or ""
                )
            if (
                pending_plan
                and pending_revision
                not in {"", payload["content_revision"]}
            ):
                _defer_turn_final(
                    runtime,
                    ref,
                    "predecessor_pending",
                    result,
                    delay_seconds=1,
                )
                break
            try:
                item, page_calls = _materialize_final_ready(
                    payload, runtime
                )
            except _TurnContentError as exc:
                _fail_turn_final(
                    runtime, ref, f"{exc.status}: {exc}", result
                )
                break
            except Exception:
                # No provider operation has started. Release the source root
                # instead of leaving a silent loop failure leased until expiry.
                _defer_turn_final(
                    runtime,
                    ref,
                    "transient_delivery",
                    result,
                    delay_seconds=1,
                )
                break
            result["content_pages"] += page_calls
            source_identity = str(payload["final_identity"])
            cached_source = materialized_sources.get(
                source_identity
            )
            if (
                cached_source is not None
                and cached_source[0] != payload
            ):
                _fail_turn_final(
                    runtime,
                    ref,
                    "invalid_turn_final_job",
                    result,
                )
                break
            try:
                staged, staged_pages = _stage_final_plan(
                    store,
                    item,
                    entry,
                    runtime,
                    source_ref=ref,
                )
            except _TurnContentError as exc:
                _fail_turn_final(
                    runtime, ref, f"{exc.status}: {exc}", result
                )
                break
            except Exception:
                # Plan preparation is idempotent and still precedes Telegram.
                # A transport/process failure is therefore safe to retry.
                _defer_turn_final(
                    runtime,
                    ref,
                    "transient_delivery",
                    result,
                    delay_seconds=1,
                )
                break
            result["content_pages"] += staged_pages
            predecessor_turn_id = str(
                payload.get("working_predecessor_turn_id") or ""
            )
            if predecessor_turn_id:
                entry["pending_working_predecessor_turn_id"] = (
                    predecessor_turn_id
                )
            materialized_sources[source_identity] = (
                payload,
                item,
            )
            result["staged"] += int(staged)
            result["changed"] = True
            _checkpoint_turn_job(runtime)
            continue

        revision = str(payload["content_revision"])
        plan_token = str(payload["plan_token"])
        job_key = str(lease["key"])
        failed_job_key = job_key
        operation = str(payload["operation"])
        sequence = int(payload["sequence_index"])
        ordinal = int(payload["part_ordinal"])
        part_count = int(payload["part_count"])
        replaces = str(payload.get("replaces_plan_token") or "")
        existing_receipt = state.find_tendwire_turn_job(
            store, job_key
        )
        durable_outcome = (
            str((existing_receipt or {}).get("substate") or "")
            in {
                "telegram_applied",
                "old_slot_retired",
                "acknowledged",
            }
        )

        source = payload.get("turn")
        source_identity = ""
        item: dict[str, Any] | None
        if isinstance(source, dict):
            source_identity = str(source["final_identity"])
            cached_source = materialized_sources.get(
                source_identity
            )
            if (
                cached_source is not None
                and cached_source[0] != source
            ):
                _fail_turn_final(
                    runtime,
                    ref,
                    "invalid_turn_final_job",
                    result,
                )
                break
            if cached_source is not None:
                item = cached_source[1]
            else:
                try:
                    item = _final_ready_row(source)
                except _TurnContentError as exc:
                    _fail_turn_final(
                        runtime,
                        ref,
                        f"{exc.status}: {exc}",
                        result,
                    )
                    break
                materialized_sources[source_identity] = (
                    source,
                    item,
                )
        else:
            item = _turn_item_by_revision(
                turns_payload, revision
            )
            if item is None and isinstance(turn_projection, Mapping):
                matches = [
                    candidate
                    for candidate in turn_projection.values()
                    if isinstance(candidate, dict)
                    and _content_revision(candidate) == revision
                ]
                item = matches[0] if len(matches) == 1 else None
            if item is None:
                _fail_turn_final(
                    runtime,
                    ref,
                    "stale_or_unavailable_content_revision",
                    result,
                )
                break

        _entry_key, entry = _resolve_final_source_entry(
            store, source if isinstance(source, dict) else item
        )
        pending_token = str(
            (entry or {}).get("pending_plan_token") or ""
        )
        owner_matches = (
            entry is not None
            and bool(str(entry.get("topic_id") or ""))
        )
        owner_bound = False
        if owner_matches and isinstance(source, dict):
            owner_matches, owner_bound = (
                _bind_or_verify_final_source_owner(
                    store,
                    source,
                    _entry_key,
                    entry,
                    allow_bind=pending_token
                    in {"", plan_token},
                )
            )
        if not owner_matches:
            _defer_turn_final(
                runtime,
                ref,
                "transient_delivery",
                result,
                delay_seconds=1,
            )
            break
        if owner_bound:
            _checkpoint_turn_job(runtime)
        if pending_token not in {"", plan_token}:
            _fail_turn_final(
                runtime,
                ref,
                "stale_or_unroutable_turn_plan",
                result,
            )
            break
        entry["pending_turn_id"] = _turn_id(item)
        entry["pending_content_revision"] = revision
        entry["pending_plan_token"] = plan_token
        entry["pending_turn_part_count"] = part_count
        if source_identity:
            entry["pending_final_identity"] = source_identity
        prior_expected = entry.get("pending_turn_job_count")
        entry["pending_turn_job_count"] = max(
            int(prior_expected)
            if isinstance(prior_expected, int)
            and not isinstance(prior_expected, bool)
            else 0,
            sequence + 1,
            part_count,
        )
        if not entry.get("pending_turn_user_hash"):
            entry["pending_turn_user_hash"] = _turn_user_hash(item)
        if "pending_plan_generation" not in entry:
            entry["pending_plan_generation"] = 1

        advanced_prior = False
        for prior_key, prior_receipt in list(
            state.tendwire_turn_jobs(store).items()
        ):
            if (
                isinstance(prior_receipt, dict)
                and prior_receipt.get("plan_token") == plan_token
                and isinstance(
                    prior_receipt.get("sequence_index"), int
                )
                and prior_receipt["sequence_index"] < sequence
                and prior_receipt.get("substate")
                in {"telegram_applied", "old_slot_retired"}
            ):
                state.update_tendwire_turn_job(
                    store,
                    prior_key,
                    substate="acknowledged",
                )
                advanced_prior = True
        if advanced_prior:
            _checkpoint_turn_job(runtime)

        predecessor_job_key = payload.get(
            "predecessor_job_key"
        )
        if predecessor_job_key is not None:
            try:
                predecessor_receipt = (
                    state.find_tendwire_turn_job(
                        store, predecessor_job_key
                    )
                )
            except ValueError:
                predecessor_receipt = None
            if (
                predecessor_receipt is None
                or predecessor_receipt.get("substate")
                != "acknowledged"
                or predecessor_receipt.get("content_revision")
                != revision
                or predecessor_receipt.get("sequence_index")
                != int(
                    entry.get(
                        "pending_acknowledged_prefix_count"
                    )
                    or 0
                )
                - 1
                or predecessor_receipt.get("plan_token")
                != entry.get("replaces_failed_plan_token")
            ):
                _fail_turn_final(
                    runtime,
                    ref,
                    "invalid_recovery_predecessor_receipt",
                    result,
                )
                break

        feed_item: dict[str, Any] | None = None
        plans: list[dict[str, Any]] = []
        if operation == "upsert" and not durable_outcome:
            try:
                page_calls = _materialize_turn_item(
                    item, runtime
                )
            except _TurnContentError as exc:
                _fail_turn_final(
                    runtime,
                    ref,
                    f"{exc.status}: {exc}",
                    result,
                )
                break
            result["content_pages"] += page_calls
            feed_item = turn_item_from_source(item, entry)
            plans = prepare_turn_delivery_parts(
                feed_item,
                rich_transport=rich_message_send_enabled(_telegram_state(store)),
            )
            if (
                ordinal >= len(plans)
                or part_count != len(plans)
                or payload.get("spans")
                != plans[ordinal].get("spans")
            ):
                _fail_turn_final(
                    runtime,
                    ref,
                    "presentation_plan_mismatch",
                    result,
                )
                break

        if operation == "upsert":
            (
                candidate_id,
                candidate_bot,
                candidate_topic,
                candidate_kind,
            ) = _current_upsert_candidate(
                store,
                item,
                entry,
                ordinal=ordinal,
                replaces_plan_token=replaces,
            )
        else:
            candidate_id, binding = _slot_binding(
                store,
                turn_id=_turn_id(item),
                ordinal=ordinal,
                plan_token=replaces,
            )
            candidate_bot = str(
                (binding or {}).get("bot_kind")
                or MANAGER_BOT_KIND
            )
            candidate_topic = str(
                (binding or {}).get("topic_id") or ""
            )
            candidate_kind = "final"
        desired_token, desired_bot = _delivery_bot(store, entry)
        compatible = bool(
            candidate_id
            and candidate_bot == desired_bot
            and candidate_topic
            == str(entry.get("topic_id") or "")
        )
        prior_for_reservation = (
            candidate_id if operation == "retire" else ""
        )
        try:
            receipt = state.reserve_tendwire_turn_job(
                store,
                job_key,
                plan_token=plan_token,
                content_revision=revision,
                operation=operation,
                sequence_index=sequence,
                part_ordinal=ordinal,
                part_count=part_count,
                prior_message_id=prior_for_reservation,
                bot_kind=desired_bot
                if operation == "upsert"
                else candidate_bot,
            )
            if existing_receipt is None:
                _checkpoint_turn_job(runtime)
        except (RuntimeError, ValueError) as exc:
            _fail_turn_final(
                runtime,
                ref,
                f"receipt_reservation_failed: {exc}",
                result,
            )
            break
        substate = str(receipt.get("substate") or "")
        if substate == "retryable":
            state.update_tendwire_turn_job(
                store, job_key, substate="reserved"
            )
            _checkpoint_turn_job(runtime)
            substate = "reserved"
            existing_receipt = None
        if substate == "reserved" and existing_receipt is not None:
            _fail_turn_final(
                runtime,
                ref,
                "delivery_uncertain",
                result,
                uncertain=True,
            )
            break
        if substate == "failed":
            _fail_turn_final(
                runtime,
                ref,
                "delivery_rejected",
                result,
            )
            break

        if substate in {
            "telegram_applied",
            "old_slot_retired",
            "acknowledged",
        }:
            pass
        elif operation == "retire":
            if not candidate_id:
                state.update_tendwire_turn_job(
                    store,
                    job_key,
                    substate="telegram_applied",
                    prior_message_id="already-missing",
                    bot_kind=candidate_bot
                    or MANAGER_BOT_KIND,
                )
                _checkpoint_turn_job(runtime)
                substate = "telegram_applied"
            else:
                try:
                    owner_token = _owning_bot_token(
                        store, candidate_bot
                    )
                    result["operations"] += 1
                    deleted = delete_turn_delivery_message(
                        runtime.telegram,
                        chat_id,
                        candidate_id,
                        api_token=owner_token,
                    )
                except RateLimited as exc:
                    _defer_turn_final(
                        runtime,
                        ref,
                        "rate_limited",
                        result,
                        store,
                        job_key,
                        delay_seconds=exc.retry_after,
                    )
                    break
                except _TurnContentError as exc:
                    _fail_turn_final(
                        runtime,
                        ref,
                        f"{exc.status}: {exc}",
                        result,
                    )
                    break
                except Exception:
                    _fail_turn_final(
                        runtime,
                        ref,
                        "delivery_uncertain",
                        result,
                        uncertain=True,
                    )
                    break
                if not deleted.get("ok"):
                    if _telegram_result_is_transient(deleted):
                        _defer_turn_final(
                            runtime,
                            ref,
                            str(
                                deleted.get("error")
                                or "transient delivery"
                            ),
                            result,
                            store,
                            job_key,
                            delay_seconds=1,
                        )
                    else:
                        _fail_turn_final(
                            runtime,
                            ref,
                            str(
                                deleted.get("error")
                                or "retire failed"
                            ),
                            result,
                        )
                    break
                _after_provider_accept(runtime)
                _retire_local_message(
                    store, entry, candidate_id
                )
                state.update_tendwire_turn_job(
                    store,
                    job_key,
                    substate="telegram_applied",
                    prior_message_id=candidate_id,
                    bot_kind=candidate_bot
                    or MANAGER_BOT_KIND,
                )
                _checkpoint_turn_job(runtime)
                substate = "telegram_applied"
        else:
            assert feed_item is not None and plans
            try:
                result["operations"] += 1
                if compatible:
                    applied = edit_turn_delivery_part(
                        runtime.telegram,
                        chat_id,
                        candidate_id,
                        feed_item,
                        plans[ordinal],
                        telegram=_telegram_state(store),
                        api_token=desired_token,
                    )
                else:
                    applied = send_turn_delivery_part(
                        runtime.telegram,
                        chat_id,
                        feed_item,
                        plans[ordinal],
                        telegram=_telegram_state(store),
                        thread_id=str(
                            entry.get("topic_id") or ""
                        ),
                        notify=False,
                        api_token=desired_token,
                    )
            except RateLimited as exc:
                _defer_turn_final(
                    runtime,
                    ref,
                    "rate_limited",
                    result,
                    store,
                    job_key,
                    delay_seconds=exc.retry_after,
                )
                break
            except Exception:
                _fail_turn_final(
                    runtime,
                    ref,
                    "delivery_uncertain",
                    result,
                    uncertain=True,
                )
                break
            if not applied.get("ok"):
                kind = str(applied.get("kind") or "")
                if _telegram_result_is_transient(applied):
                    _defer_turn_final(
                        runtime,
                        ref,
                        str(
                            applied.get("error")
                            or "transient delivery"
                        ),
                        result,
                        store,
                        job_key,
                        delay_seconds=1,
                    )
                    break
                retry_as_send = (
                    compatible
                    and kind in {"not_found", "topic_not_found"}
                )
                if retry_as_send and kind == "not_found":
                    _retire_local_message(
                        store, entry, candidate_id
                    )
                    candidate_id = ""
                    compatible = False
                    _checkpoint_turn_job(runtime)
                if (
                    retry_as_send
                    and result["operations"] >= max_operations
                ):
                    _defer_turn_final(
                        runtime,
                        ref,
                        "operation_budget_exhausted",
                        result,
                        store,
                        job_key,
                        delay_seconds=1,
                    )
                    break
                if retry_as_send:
                    try:
                        result["operations"] += 1
                        applied = send_turn_delivery_part(
                            runtime.telegram,
                            chat_id,
                            feed_item,
                            plans[ordinal],
                            telegram=_telegram_state(store),
                            thread_id=str(
                                entry.get("topic_id") or ""
                            ),
                            notify=False,
                            api_token=desired_token,
                        )
                        compatible = False
                    except RateLimited as exc:
                        _defer_turn_final(
                            runtime,
                            ref,
                            "rate_limited",
                            result,
                            store,
                            job_key,
                            delay_seconds=exc.retry_after,
                        )
                        break
                    except Exception:
                        _fail_turn_final(
                            runtime,
                            ref,
                            "delivery_uncertain",
                            result,
                            uncertain=True,
                        )
                        break
                if not applied.get("ok"):
                    if _telegram_result_is_transient(applied):
                        _defer_turn_final(
                            runtime,
                            ref,
                            str(
                                applied.get("error")
                                or "transient delivery"
                            ),
                            result,
                            store,
                            job_key,
                            delay_seconds=1,
                        )
                    else:
                        _fail_turn_final(
                            runtime,
                            ref,
                            str(
                                applied.get("error")
                                or "delivery rejected"
                            ),
                            result,
                        )
                    break
            message_id = str(
                applied.get("message_id")
                or candidate_id
                or ""
            )
            if not message_id or message_id == "0":
                _fail_turn_final(
                    runtime,
                    ref,
                    "delivery_uncertain",
                    result,
                    uncertain=True,
                )
                break
            _after_provider_accept(runtime)
            prior_id = (
                candidate_id
                if candidate_id
                and not compatible
                and candidate_id != message_id
                else None
            )
            state.bind_message_to_worker(
                store,
                message_id,
                entry,
                topic_id=str(entry.get("topic_id") or ""),
                kind="final",
                turn_id=_turn_id(item),
                bot_kind=desired_bot,
                content_revision=revision,
                plan_token=plan_token,
                part_ordinal=ordinal,
                part_count=part_count,
                tendwire_job_key=job_key,
            )
            state.update_tendwire_turn_job(
                store,
                job_key,
                substate="telegram_applied",
                telegram_message_id=message_id,
                prior_message_id=prior_id,
                bot_kind=desired_bot,
            )
            if (
                candidate_kind == "working"
                and message_id == candidate_id
            ):
                _clear_stream_delivery_state(
                    entry, _turn_id(item)
                )
            _checkpoint_turn_job(runtime)
            substate = "telegram_applied"

        receipt = state.find_tendwire_turn_job(store, job_key)
        if (
            operation == "upsert"
            and substate == "telegram_applied"
            and receipt is not None
        ):
            prior_id = str(
                receipt.get("prior_message_id") or ""
            )
            message_id = str(
                receipt.get("telegram_message_id") or ""
            )
            if prior_id and prior_id != message_id:
                if result["operations"] >= max_operations:
                    _defer_turn_final(
                        runtime,
                        ref,
                        "operation_budget_exhausted",
                        result,
                        store,
                        job_key,
                        delay_seconds=1,
                    )
                    break
                prior_binding = state.find_message_binding(
                    store, prior_id
                )
                prior_bot = str(
                    (prior_binding or {}).get("bot_kind")
                    or candidate_bot
                    or MANAGER_BOT_KIND
                )
                try:
                    owner_token = _owning_bot_token(
                        store, prior_bot
                    )
                    result["operations"] += 1
                    retired = delete_turn_delivery_message(
                        runtime.telegram,
                        chat_id,
                        prior_id,
                        api_token=owner_token,
                    )
                except RateLimited as exc:
                    _defer_turn_final(
                        runtime,
                        ref,
                        "rate_limited",
                        result,
                        store,
                        job_key,
                        delay_seconds=exc.retry_after,
                    )
                    break
                except _TurnContentError as exc:
                    _fail_turn_final(
                        runtime,
                        ref,
                        f"{exc.status}: {exc}",
                        result,
                    )
                    break
                except Exception:
                    _fail_turn_final(
                        runtime,
                        ref,
                        "delivery_uncertain",
                        result,
                        uncertain=True,
                    )
                    break
                if not retired.get("ok"):
                    if _telegram_result_is_transient(retired):
                        _defer_turn_final(
                            runtime,
                            ref,
                            str(
                                retired.get("error")
                                or "transient delivery"
                            ),
                            result,
                            store,
                            job_key,
                            delay_seconds=1,
                        )
                    else:
                        _fail_turn_final(
                            runtime,
                            ref,
                            str(
                                retired.get("error")
                                or "old slot retire failed"
                            ),
                            result,
                        )
                    break
                _after_provider_accept(runtime)
                _retire_local_message(store, entry, prior_id)
                state.update_tendwire_turn_job(
                    store,
                    job_key,
                    substate="old_slot_retired",
                )
                _checkpoint_turn_job(runtime)
                substate = "old_slot_retired"

        ack = runtime.tendwire.turn_final_ack(
            ref,
            {"outcome": "applied", "job_key": job_key},
        )
        if ack.get("ok") is False:
            observed = runtime.tendwire.connector_prepare_commit(
                plan_token=plan_token
            )
            advanced = False
            if (
                observed.get("ok") is True
                and observed.get("plan_token") == plan_token
                and observed.get("state") == "completed"
            ):
                observed_count = observed.get("job_count")
                if (
                    isinstance(observed_count, int)
                    and not isinstance(observed_count, bool)
                    and observed_count > 0
                ):
                    entry[
                        "pending_turn_job_count"
                    ] = observed_count
                for receipt_key, observed_receipt in list(
                    state.tendwire_turn_jobs(store).items()
                ):
                    if (
                        isinstance(observed_receipt, dict)
                        and observed_receipt.get("plan_token")
                        == plan_token
                        and observed_receipt.get("substate")
                        in {
                            "telegram_applied",
                            "old_slot_retired",
                        }
                    ):
                        state.update_tendwire_turn_job(
                            store,
                            receipt_key,
                            substate="acknowledged",
                        )
                        advanced = True
                if _maybe_complete_turn_plan(
                    store,
                    item,
                    entry,
                    plan_token=plan_token,
                    revision=revision,
                ):
                    advanced = True
            if advanced:
                _checkpoint_turn_job(runtime)
            result["status"] = str(
                ack.get("status") or "turn_final_ack_failed"
            )
            result["changed"] = True
            break
        if substate != "acknowledged":
            state.update_tendwire_turn_job(
                store, job_key, substate="acknowledged"
            )
            _checkpoint_turn_job(runtime)
        result["delivered"] += 1
        result["acked"] += 1
        result["changed"] = True
        if _maybe_complete_turn_plan(
            store,
            item,
            entry,
            plan_token=plan_token,
            revision=revision,
        ):
            _checkpoint_turn_job(runtime)

    if (
        result.get("status") == "attempts_exhausted"
        and failed_job_key
    ):
        failed_receipt = state.find_tendwire_turn_job(
            store, failed_job_key
        )
        if (
            failed_receipt is not None
            and failed_receipt.get("substate")
            in {
                "reserved",
                "retryable",
                "telegram_applied",
                "old_slot_retired",
            }
        ):
            state.update_tendwire_turn_job(
                store,
                failed_job_key,
                substate="failed",
            )
            _checkpoint_turn_job(runtime)
    return result


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
    account_html = _account_lines_html(entries)
    if account_html:
        html = f"{html}\n{account_html}"
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


def _deliver_decisions(
    store: dict[str, Any],
    pending_payload: dict[str, Any],
    runtime: SyncRuntime,
    *,
    chat_id: str,
    yield_barrier: Callable[[], None] | None = None,
) -> dict[str, Any]:
    """Reconcile decision keyboards without ever endangering the sync pass.

    Callback mutations and the normal source shim both hold ``state_lock``.
    The late barrier commits our preceding source work, briefly lets a queued
    callback save its decision bucket, and reloads the whole store before this
    step.  Therefore the shim's final save cannot clobber a callback that used
    the lock; only an out-of-contract writer that bypasses the shared lock can
    race the whole-file state save.
    """

    if not config.remote_decisions_enabled():
        return {"enabled": False, "changed": False, "posted": 0, "retracted": 0}
    if not decisions.needs_sync(store, pending_payload):
        return {"enabled": True, "changed": False, "posted": 0, "retracted": 0}
    try:
        if yield_barrier is not None:
            yield_barrier()
        return decisions.sync_decisions(
            store,
            pending_payload,
            runtime.telegram,
            chat_id=chat_id,
            dry_run=runtime.dry_run,
        )
    except Exception as exc:  # noqa: BLE001 - decisions are additive to the core sync loop
        return {
            "enabled": True,
            "changed": False,
            "posted": 0,
            "retracted": 0,
            "status": "failed",
            "error": compact_ws(exc, 240),
        }


def _tendwire_non_success(runtime: SyncRuntime, status: str) -> dict[str, Any]:
    return {
        "ok": False,
        "status": status,
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
        "content_pages": 0,
        "tendwire_turn_final": {
            "enabled": runtime.with_outbox,
            "polled": 0,
            "operations": 0,
            "delivered": 0,
            "acked": 0,
            "failed": 0,
            "deferred": 0,
            "uncertain": 0,
            "changed": False,
        },
        "tendwire_outbox": {
            "enabled": runtime.with_outbox,
            "polled": 0,
            "delivered": 0,
            "acked": 0,
            "failed": 0,
            "deferred": 0,
            "changed": False,
        },
    }


_TURN_SCHEMA_VERSION = 1


def _unsupported_turn_schema_version(
    runtime: SyncRuntime, received: Any
) -> dict[str, Any]:
    result = _tendwire_non_success(runtime, "unsupported_turn_schema_version")
    result["required_turn_schema_version"] = _TURN_SCHEMA_VERSION
    if isinstance(received, str):
        safe_received: Any = compact_ws(received, 80)
    elif received is None or isinstance(received, (bool, int, float)):
        safe_received = received
    else:
        safe_received = None
    result["received_turn_schema_version"] = safe_received
    return result


def _herdr_backend_explicitly_unhealthy(snapshot: dict[str, Any]) -> bool:
    backend_health = snapshot.get("backend_health")
    if not isinstance(backend_health, list):
        return False
    for item in backend_health:
        if not isinstance(item, dict):
            continue
        if str(item.get("name") or "").strip().lower() != "herdr":
            continue
        status = item.get("status")
        if isinstance(status, str) and status.strip() and status.strip().lower().replace("-", "_") != "healthy":
            return True
    return False


_DELTA_STATE_KEY = "tendwire_delta_sync"
_DELTA_SCHEMA_VERSION = 1
_DELTA_PROJECTION_SCHEMA_VERSION = 2
_DELTA_TRANSPORT_STATUS = "transport_ambiguous"
_DELTA_WATERMARK_RECOVERY = frozenset(
    {"invalid_watermark", "expired_watermark"}
)
_DELTA_TERMINAL_FALLBACK = frozenset(
    {
        "bootstrap_too_large",
        "cross_host_watermark",
        "incompatible_schema",
        "invalid_cursor",
        "expired_cursor",
    }
)


def _new_delta_state(*, reason: str, now: float) -> dict[str, Any]:
    return {
        "schema_version": _DELTA_SCHEMA_VERSION,
        "projection_schema_version": _DELTA_PROJECTION_SCHEMA_VERSION,
        "status": "bootstrapping",
        "watermark": None,
        "pending_cursor": None,
        "projection": {},
        "bootstrap_state": {
            "reason": reason,
            "attempt": 1,
            "pages_applied": 0,
            "started_at": now,
        },
        "failure_count": 0,
        "last_full_reconcile_at": now,
    }


def _delta_state(store: dict[str, Any], *, now: float) -> dict[str, Any]:
    current = store.get(_DELTA_STATE_KEY)
    if not isinstance(current, dict):
        current = _new_delta_state(reason="first_activation", now=now)
        store[_DELTA_STATE_KEY] = current
        return current
    if (
        current.get("schema_version") != _DELTA_SCHEMA_VERSION
        or current.get("projection_schema_version")
        != _DELTA_PROJECTION_SCHEMA_VERSION
        or not isinstance(current.get("projection"), dict)
    ):
        current = _new_delta_state(reason="invalid_local_state", now=now)
        store[_DELTA_STATE_KEY] = current
    return current


def _delta_health(delta: dict[str, Any] | None, *, now: float | None = None) -> dict[str, Any]:
    if not isinstance(delta, dict):
        return {"state": "fallback", "watermark_age_seconds": None, "last_batch": {}}
    clock = time.time() if now is None else now
    updated_at = delta.get("watermark_updated_at")
    age: int | None = None
    if isinstance(updated_at, (int, float)) and not isinstance(updated_at, bool):
        age = max(0, int(clock - float(updated_at)))
    raw_batch = delta.get("last_batch")
    batch: dict[str, Any] = {}
    if isinstance(raw_batch, dict):
        for key in (
            "mode",
            "changes_returned",
            "upserts",
            "removals",
            "journal_rows_scanned",
            "projection_rows_read",
            "duration_ms",
        ):
            value = raw_batch.get(key)
            if isinstance(value, str) and key == "mode":
                batch[key] = compact_ws(value, 24)
            elif isinstance(value, int) and not isinstance(value, bool) and value >= 0:
                batch[key] = value
    state_name = str(delta.get("status") or "bootstrapping")
    result: dict[str, Any] = {
        "state": state_name
        if state_name in {"active", "fallback", "bootstrapping"}
        else "bootstrapping",
        "watermark_age_seconds": age,
        "last_batch": batch,
    }
    health_flag = delta.get("health_flag")
    if isinstance(health_flag, str) and health_flag:
        result["health_flag"] = compact_ws(health_flag, 80)
    return result


def _delta_full_reconcile_due(delta: dict[str, Any], *, now: float) -> bool:
    if delta.get("status") != "active" or delta.get("pending_cursor"):
        return False
    if config.tendwire_force_full_reconcile():
        return True
    interval = config.tendwire_full_reconcile_seconds()
    if interval <= 0:
        return False
    try:
        last_at = float(delta.get("last_full_reconcile_at") or 0)
    except (TypeError, ValueError):
        last_at = 0
    return now - last_at >= interval


def _delta_error_code(payload: dict[str, Any]) -> str:
    status = str(payload.get("status") or "").strip().lower()
    error = payload.get("error")
    if not status and isinstance(error, dict):
        status = str(error.get("code") or "").strip().lower()
    return status


def _validate_delta_page(
    payload: dict[str, Any],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], dict[str, int]]:
    if (
        payload.get("schema_version") != _DELTA_SCHEMA_VERSION
        or payload.get("projection_schema_version")
        != _DELTA_PROJECTION_SCHEMA_VERSION
        or payload.get("mode") not in {"bootstrap", "changes"}
        or type(payload.get("has_more")) is not bool
        or not isinstance(payload.get("changes"), list)
        or not isinstance(payload.get("host_id"), str)
        or not payload.get("host_id")
    ):
        raise _TurnContentError(
            "delta_protocol_ambiguous",
            "Tendwire turn.delta returned a malformed envelope",
        )
    has_more = payload["has_more"]
    next_cursor = payload.get("next_cursor")
    checkpoint = payload.get("checkpoint")
    if has_more:
        if (
            not isinstance(next_cursor, str)
            or not next_cursor.startswith("twdeltac1.")
            or checkpoint is not None
        ):
            raise _TurnContentError(
                "delta_protocol_ambiguous",
                "Tendwire turn.delta returned invalid continuation state",
            )
    elif (
        next_cursor is not None
        or not isinstance(checkpoint, str)
        or not checkpoint.startswith("twdelta1.")
    ):
        raise _TurnContentError(
            "delta_protocol_ambiguous",
            "Tendwire turn.delta returned invalid checkpoint state",
        )
    upserts: list[dict[str, Any]] = []
    removals: list[dict[str, Any]] = []
    seen: set[str] = set()
    for raw_change in payload["changes"]:
        if not isinstance(raw_change, dict):
            raise _TurnContentError(
                "delta_protocol_ambiguous",
                "Tendwire turn.delta returned a malformed change",
            )
        op = raw_change.get("op")
        change_turn_id = raw_change.get("turn_id")
        if (
            not isinstance(change_turn_id, str)
            or not change_turn_id
            or change_turn_id in seen
        ):
            raise _TurnContentError(
                "delta_protocol_ambiguous",
                "Tendwire turn.delta returned an invalid turn identity",
            )
        seen.add(change_turn_id)
        if op == "upsert":
            raw_turn = raw_change.get("turn")
            if not isinstance(raw_turn, dict):
                raise _TurnContentError(
                    "delta_protocol_ambiguous",
                    "Tendwire turn.delta upsert omitted its projection",
                )
            turn = _validate_turn_row(raw_turn)
            if _turn_id(turn) != change_turn_id:
                raise _TurnContentError(
                    "delta_protocol_ambiguous",
                    "Tendwire turn.delta projection identity mismatched",
                )
            upserts.append(turn)
        elif op == "remove":
            if not isinstance(raw_change.get("removed_at"), str):
                raise _TurnContentError(
                    "delta_protocol_ambiguous",
                    "Tendwire turn.delta removal omitted its timestamp",
                )
            removals.append(raw_change)
        else:
            raise _TurnContentError(
                "delta_protocol_ambiguous",
                "Tendwire turn.delta returned an unsupported operation",
            )
    aggregate: dict[str, int] = {}
    raw_aggregate = payload.get("aggregate")
    if isinstance(raw_aggregate, dict):
        for key in (
            "journal_rows_scanned",
            "projection_rows_read",
            "changes_returned",
            "duration_ms",
        ):
            value = raw_aggregate.get(key)
            if isinstance(value, int) and not isinstance(value, bool) and value >= 0:
                aggregate[key] = value
    aggregate.setdefault("changes_returned", len(upserts) + len(removals))
    return upserts, removals, aggregate


def _clear_removed_turn_state(store: dict[str, Any], turn_id: str) -> bool:
    changed = False
    for bucket_name in ("panes", "spaces"):
        bucket = store.get(bucket_name)
        if not isinstance(bucket, dict):
            continue
        for entry in bucket.values():
            if not isinstance(entry, dict):
                continue
            if entry.get("last_stream_turn_id") == turn_id:
                changed = _clear_stream_delivery_keys(entry) or changed
            if entry.get("last_turn_id") == turn_id:
                changed = _clear_final_delivery_keys(entry) or changed
    bindings = state.message_bindings(store)
    for message_id, binding in list(bindings.items()):
        if isinstance(binding, dict) and str(binding.get("turn_id") or "") == turn_id:
            bindings.pop(message_id, None)
            changed = True
    return changed


def _clear_projection_stale_cards(store: dict[str, Any], projection: dict[str, Any]) -> int:
    cleared = 0
    known = set(projection)
    candidates: set[str] = set()
    for bucket_name in ("panes", "spaces"):
        bucket = store.get(bucket_name)
        if not isinstance(bucket, dict):
            continue
        for entry in bucket.values():
            if not isinstance(entry, dict):
                continue
            for key in ("last_stream_turn_id", "last_turn_id"):
                value = entry.get(key)
                if isinstance(value, str) and value and value not in known:
                    candidates.add(value)
    for turn_id in candidates:
        cleared += int(_clear_removed_turn_state(store, turn_id))
    return cleared


def _set_delta_fallback(delta: dict[str, Any], reason: str) -> None:
    delta["status"] = "fallback"
    delta["pending_cursor"] = None
    delta["bootstrap_state"] = None
    delta["health_flag"] = f"turn_delta_{compact_ws(reason, 48)}"


def _begin_delta_rebootstrap(delta: dict[str, Any], *, reason: str, now: float) -> None:
    previous = delta.get("bootstrap_state")
    attempt = (
        int(previous.get("attempt") or 0) + 1
        if isinstance(previous, dict)
        else 1
    )
    delta.update(
        {
            "status": "bootstrapping",
            "watermark": None,
            "pending_cursor": None,
            "projection": {},
            "bootstrap_state": {
                "reason": reason,
                "attempt": attempt,
                "pages_applied": 0,
                "started_at": now,
            },
            "failure_count": 0,
            "health_flag": f"turn_delta_{reason}",
        }
    )


def _observe_turn_delta(
    store: dict[str, Any],
    runtime: SyncRuntime,
    *,
    now: float,
) -> dict[str, Any]:
    def transition(result: dict[str, Any]) -> dict[str, Any]:
        if runtime.checkpoint is not None:
            runtime.checkpoint()
        return result

    delta = _delta_state(store, now=now)
    if delta.get("status") == "fallback":
        return {"kind": "full", "delta": delta, "reason": "fallback"}
    if _delta_full_reconcile_due(delta, now=now):
        return {"kind": "full", "delta": delta, "reason": "reconcile"}
    cursor = delta.get("pending_cursor")
    watermark = delta.get("watermark")
    if not isinstance(cursor, str) or not cursor:
        cursor = None
    if not isinstance(watermark, str) or not watermark:
        watermark = None
    page = runtime.tendwire.turn_delta(
        cursor=cursor,
        watermark=None if cursor is not None else watermark,
        limit=config.tendwire_delta_limit(),
    )
    if page.get("ok") is False:
        status = _delta_error_code(page) or "delta_failed"
        delta["last_error_at"] = now
        if status == "unsupported_method":
            _set_delta_fallback(delta, "unsupported")
            return transition(
                {"kind": "full", "delta": delta, "reason": "unsupported"}
            )
        if status == _DELTA_TRANSPORT_STATUS:
            delta["health_flag"] = "turn_delta_transport_ambiguous"
            return transition(
                {"kind": "empty", "delta": delta, "reason": status}
            )
        if status in _DELTA_WATERMARK_RECOVERY and cursor is None and watermark is not None:
            _begin_delta_rebootstrap(delta, reason=status, now=now)
            return transition(
                {"kind": "empty", "delta": delta, "reason": status}
            )
        if status in _DELTA_TERMINAL_FALLBACK or (
            status in _DELTA_WATERMARK_RECOVERY
            and delta.get("status") == "bootstrapping"
        ):
            _set_delta_fallback(delta, status)
            return transition(
                {"kind": "full", "delta": delta, "reason": status}
            )
        failure_count = int(delta.get("failure_count") or 0) + 1
        delta["failure_count"] = failure_count
        delta["health_flag"] = f"turn_delta_{compact_ws(status, 48)}"
        if failure_count >= 2:
            _set_delta_fallback(delta, f"repeated_{status}")
            return transition(
                {"kind": "full", "delta": delta, "reason": status}
            )
        return transition(
            {"kind": "empty", "delta": delta, "reason": status}
        )
    try:
        upserts, removals, aggregate = _validate_delta_page(page)
    except _TurnContentError:
        delta["last_error_at"] = now
        delta["health_flag"] = "turn_delta_transport_ambiguous"
        return transition(
            {
                "kind": "empty",
                "delta": delta,
                "reason": "delta_protocol_ambiguous",
            }
        )
    expected_mode = "bootstrap" if watermark is None and delta.get("status") == "bootstrapping" else "changes"
    if page.get("mode") != expected_mode:
        delta["last_error_at"] = now
        delta["health_flag"] = "turn_delta_transport_ambiguous"
        return transition(
            {
                "kind": "empty",
                "delta": delta,
                "reason": "delta_protocol_ambiguous",
            }
        )
    projection = delta["projection"]
    removed_ids: list[str] = []
    for row in upserts:
        projection[_turn_id(row)] = row
    for removal in removals:
        turn_id = str(removal["turn_id"])
        projection.pop(turn_id, None)
        removed_ids.append(turn_id)
    delta["failure_count"] = 0
    delta.pop("last_error_at", None)
    delta["last_batch"] = {
        "mode": str(page["mode"]),
        **aggregate,
        "upserts": len(upserts),
        "removals": len(removals),
    }
    return {
        "kind": "delta",
        "delta": delta,
        "page": page,
        "upserts": upserts,
        "removed_ids": removed_ids,
    }


def _finish_delta_page(
    store: dict[str, Any],
    observation: dict[str, Any],
    runtime: SyncRuntime,
    *,
    now: float,
) -> int:
    delta = observation["delta"]
    page = observation["page"]
    changed = 0
    for turn_id in observation["removed_ids"]:
        changed += int(_clear_removed_turn_state(store, turn_id))
    bootstrap = delta.get("bootstrap_state")
    if isinstance(bootstrap, dict):
        bootstrap["pages_applied"] = int(bootstrap.get("pages_applied") or 0) + 1
    if page["has_more"]:
        delta["pending_cursor"] = page["next_cursor"]
    else:
        delta["watermark"] = page["checkpoint"]
        delta["pending_cursor"] = None
        delta["status"] = "active"
        delta["bootstrap_state"] = None
        delta["watermark_updated_at"] = now
        delta["last_full_reconcile_at"] = (
            now if page["mode"] == "bootstrap" else delta.get("last_full_reconcile_at", now)
        )
        delta.pop("health_flag", None)
        if page["mode"] == "bootstrap":
            changed += _clear_projection_stale_cards(store, delta["projection"])
    if runtime.checkpoint is not None:
        runtime.checkpoint()
    return changed


def _apply_full_reconciliation(
    store: dict[str, Any],
    delta: dict[str, Any],
    turns_payload: dict[str, Any],
    runtime: SyncRuntime,
    *,
    now: float,
) -> int:
    projection = {_turn_id(row): row for row in _turns(turns_payload) if _turn_id(row)}
    delta["projection"] = projection
    delta["last_full_reconcile_at"] = now
    delta["last_batch"] = {
        "mode": "full_reconcile",
        "changes_returned": len(projection),
        "upserts": len(projection),
        "removals": 0,
    }
    changed = _clear_projection_stale_cards(store, projection)
    if runtime.checkpoint is not None:
        runtime.checkpoint()
    return changed


def sync_once(store: dict[str, Any], runtime: SyncRuntime) -> dict[str, Any]:
    config.require_source_mode()
    observed_at = time.time()
    delta_observation: dict[str, Any] | None = None
    delta: dict[str, Any] | None = None
    snapshot = runtime.tendwire.snapshot()
    if _herdr_backend_explicitly_unhealthy(snapshot):
        return _tendwire_non_success(runtime, "tendwire_herdr_unhealthy")
    if hasattr(runtime.tendwire, "turn_delta"):
        delta_observation = _observe_turn_delta(
            store,
            runtime,
            now=observed_at,
        )
        delta = delta_observation["delta"]
        if delta_observation["kind"] == "full":
            turns_payload = runtime.tendwire.turns()
        elif delta_observation["kind"] == "delta":
            turns_payload = {
                "schema_version": TURN_SCHEMA_VERSION,
                "turns": delta_observation["upserts"],
            }
        else:
            turns_payload = {
                "schema_version": TURN_SCHEMA_VERSION,
                "turns": [],
            }
    else:
        # Compatibility for embedders and old test doubles. The production
        # TendwireClient always exposes turn_delta; only its explicit
        # unsupported-method outcome activates the durable fallback lane.
        turns_payload = runtime.tendwire.turns()
    pending_payload = runtime.tendwire.pending()
    for name, payload in (("snapshot", snapshot), ("turns", turns_payload), ("pending", pending_payload)):
        if payload.get("ok") is False:
            return _tendwire_non_success(runtime, f"tendwire_{name}_failed")
    turn_schema = turns_payload.get("schema_version")
    if (
        type(turn_schema) is not int
        or turn_schema not in {_TURN_SCHEMA_VERSION, TURN_SCHEMA_VERSION}
    ):
        return _unsupported_turn_schema_version(runtime, turn_schema)
    list_finals_are_authoritative = turn_schema != TURN_SCHEMA_VERSION
    chat_id = config.telegram_chat_id(store)
    try:
        turns_payload = _validate_turns_payload(turns_payload)
    except _TurnContentError as exc:
        # The list envelope/schema is connector-wide. Descriptor defects are
        # converted to bounded row-local outcomes by _validate_turns_payload.
        return _tendwire_non_success(runtime, exc.status)
    changed = False
    source_counts = _sync_sources(store, snapshot, turns_payload, runtime, chat_id=chat_id)
    routing_repaired = _repair_space_mode_routing_state(store)
    message_bindings = _backfill_message_bindings(store)
    bootstrapped = _bootstrap_existing_turns(
        store,
        turns_payload,
        pending_payload,
        skip_v2_finals=not list_finals_are_authoritative,
    )
    live_worker_ids = {
        compact_ws(worker.get("id"), 160)
        for worker in _workers(snapshot)
        if _worker_is_open(worker)
    }
    live_worker_ids.discard("")
    reconciled_turn_plans = _reconcile_completed_turn_plans(
        store, runtime
    )

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
    try:
        feed_turns_payload = (
            {"schema_version": TURN_SCHEMA_VERSION, "turns": []}
            if delta_observation is not None
            and delta_observation.get("kind") == "delta"
            and delta_observation.get("page", {}).get("mode") == "bootstrap"
            else turns_payload
        )
        turn_counts = (
            {"feed_sent": 0, "sent": 0, "updated": 0, "content_pages": 0}
            if bootstrapped
            else _sync_turns(
                store,
                feed_turns_payload,
                pending_payload,
                runtime,
                chat_id=chat_id,
                live_worker_ids=live_worker_ids,
                yield_barrier=yield_barrier,
                list_finals_are_authoritative=list_finals_are_authoritative,
            )
        )
    except _TurnContentError as exc:
        if not exc.conflict:
            return _tendwire_non_success(runtime, exc.status)
        relisted = runtime.tendwire.turns()
        if relisted.get("ok") is False:
            return _tendwire_non_success(
                runtime, "tendwire_turns_relist_failed"
            )
        try:
            turns_payload = _validate_turns_payload(relisted)
            relisted_schema = turns_payload.get("schema_version")
            relisted_finals_are_authoritative = (
                relisted_schema != TURN_SCHEMA_VERSION
            )
            turn_counts = _sync_turns(
                store,
                turns_payload,
                pending_payload,
                runtime,
                chat_id=chat_id,
                live_worker_ids=live_worker_ids,
                relist_on_conflict=False,
                yield_barrier=yield_barrier,
                list_finals_are_authoritative=relisted_finals_are_authoritative,
            )
        except _TurnContentError as retry_exc:
            return _tendwire_non_success(runtime, retry_exc.status)
    decision_result = _deliver_decisions(
        store,
        pending_payload,
        runtime,
        chat_id=chat_id,
        yield_barrier=yield_barrier,
    )
    routing_repaired += _repair_space_mode_routing_state(store)
    snapshot_worker_ids = {compact_ws(worker.get("id"), 160) for worker in _workers(snapshot)}
    snapshot_worker_ids.discard("")
    topic_cleanup = _cleanup_topics(store, runtime, chat_id=chat_id, snapshot_worker_ids=snapshot_worker_ids)
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
        or reconciled_turn_plans
        or decision_result.get("changed")
    )
    if config.pinned_status_enabled():
        pinned_changed = _sync_pinned(store, runtime, chat_id=chat_id)
        topic_pinned_updated = _sync_topic_pinned_statuses(store, runtime, chat_id=chat_id)
    else:
        pinned_changed = False
        topic_pinned_updated = 0
    changed = changed or pinned_changed or bool(topic_pinned_updated)
    delta_card_updates = 0
    if delta_observation is not None:
        if delta_observation["kind"] == "delta":
            delta_card_updates = _finish_delta_page(
                store,
                delta_observation,
                runtime,
                now=observed_at,
            )
        elif delta_observation["kind"] == "full":
            delta_card_updates = _apply_full_reconciliation(
                store,
                delta_observation["delta"],
                turns_payload,
                runtime,
                now=observed_at,
            )
        elif runtime.checkpoint is not None:
            # Persist bootstrapping/fallback health transitions without ever
            # issuing a second turn observation in this pass.
            runtime.checkpoint()
        changed = True
    turn_final_result = {
        "enabled": runtime.with_outbox,
        "polled": 0,
        "operations": 0,
        "delivered": 0,
        "acked": 0,
        "failed": 0,
        "deferred": 0,
        "uncertain": 0,
        "staged": 0,
        "content_pages": 0,
        "changed": False,
    }
    outbox_result = {"enabled": runtime.with_outbox, "polled": 0, "delivered": 0, "acked": 0, "failed": 0, "deferred": 0, "changed": False}
    if runtime.with_outbox:
        remaining = max(0, runtime.max_sends - int(turn_counts["sent"]))
        turn_final_result = _drain_turn_final(
            store,
            turns_payload,
            runtime,
            chat_id=chat_id,
            max_operations=remaining,
            turn_projection=(
                delta.get("projection")
                if isinstance(delta, dict)
                and isinstance(delta.get("projection"), dict)
                else None
            ),
        )
        remaining = max(0, remaining - int(turn_final_result["operations"]))
        outbox_result = drain_outbox(
            store,
            runtime.telegram,
            runtime.tendwire,
            chat_id=chat_id,
            max_sends=remaining,
            dry_run=runtime.dry_run,
        )
        changed = changed or bool(turn_final_result.get("changed")) or bool(outbox_result.get("changed"))
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
        **(
            {"tendwire_delta_sync": _delta_health(delta, now=observed_at)}
            if delta is not None
            else {}
        ),
        "bootstrap_seen": bootstrapped,
        "message_bindings": message_bindings,
        "topic_cleanup": topic_cleanup,
        "content_pages": int(turn_counts["content_pages"])
        + int(turn_final_result.get("content_pages") or 0),
        "turn_content_outcomes": _turn_content_outcomes(turns_payload),
        "remote_decisions": decision_result,
        "tendwire_turn_final": turn_final_result,
        "tendwire_outbox": outbox_result,
    }
