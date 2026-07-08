"""Configuration loading for source-only Herdres."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any


HOME = Path.home()
DEFAULT_STATE_PATH = HOME / ".local/share/herdres/state.json"
DEFAULT_OFFSET_PATH = HOME / ".local/share/herdres/gateway.offset"
DEFAULT_PROCESSED_PATH = HOME / ".local/share/herdres/gateway_processed_messages.json"
DEFAULT_TENDWIRE_DB_PATH = HOME / ".local/share/tendwire/tendwire.db"
DEFAULT_HERDRES_ENV_PATH = HOME / ".config/herdres/herdres.env"
DEFAULT_GENERAL_THREAD_ID = "1"
SOURCE_SERVICES = ("tendwired.service", "herdres-gateway.service", "herdres.service")
LEGACY_TIMER = "herdr-telegram-topics.timer"
TOPIC_MODES = {"space", "worker"}
MANAGED_BOT_KINDS = {"codex", "claude", "glm", "kimi", "omp", "devin"}


def load_env_file(path: str | Path | None = None) -> None:
    env_path = Path(path or os.getenv("HERDRES_ENV_FILE", DEFAULT_HERDRES_ENV_PATH)).expanduser()
    if not env_path.exists():
        return
    for raw in env_path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        if not key or key in os.environ:
            continue
        os.environ[key] = value.strip().strip('"').strip("'")


def state_path(env: Any | None = None) -> Path:
    source = os.environ if env is None else env
    return Path(source.get("HERDR_TELEGRAM_TOPICS_STATE", DEFAULT_STATE_PATH)).expanduser()


def offset_path(env: Any | None = None) -> Path:
    source = os.environ if env is None else env
    return Path(source.get("HERDR_TELEGRAM_TOPICS_GATEWAY_OFFSET", DEFAULT_OFFSET_PATH)).expanduser()


def processed_path(env: Any | None = None) -> Path:
    source = os.environ if env is None else env
    return Path(source.get("HERDR_TELEGRAM_TOPICS_GATEWAY_PROCESSED", DEFAULT_PROCESSED_PATH)).expanduser()


def tendwire_db_path(env: Any | None = None) -> Path:
    source = os.environ if env is None else env
    return Path(source.get("HERDRES_TENDWIRE_DB_PATH", source.get("TENDWIRE_DB_PATH", DEFAULT_TENDWIRE_DB_PATH))).expanduser()


def mode(env: Any | None = None) -> str:
    source = os.environ if env is None else env
    return str(source.get("HERDRES_TENDWIRE_MODE", "source") or "source").strip().lower()


def require_source_mode(env: Any | None = None) -> None:
    current = mode(env)
    if current != "source":
        raise RuntimeError(f"Herdres tendwired branch supports only HERDRES_TENDWIRE_MODE=source, got {current!r}")


def source_topic_mode(env: Any | None = None) -> str:
    source = os.environ if env is None else env
    value = str(source.get("HERDRES_SOURCE_TOPIC_MODE", source.get("HERDRES_TOPIC_GRANULARITY", "space")) or "space").strip().lower()
    if value in {"pane", "panes", "worker", "workers"}:
        return "worker"
    return value if value in TOPIC_MODES else "space"


def delete_done_council_topics(env: Any | None = None) -> bool:
    source = os.environ if env is None else env
    value = str(source.get("HERDRES_DELETE_DONE_COUNCIL_TOPICS", "1") or "").strip().lower()
    return value not in {"0", "false", "no", "off"}


def response_collapse_previous_default(env: Any | None = None) -> bool:
    """Collapse the Response section of SUPERSEDED (non-latest) finals so the topic reads as a tidy
    history: only the newest answer stays expanded. Read at call time (runtime-flag idiom); default
    OFF to match the monolith — HERDR_TELEGRAM_TOPICS_RESPONSE_COLLAPSE_PREVIOUS=1 enables it."""
    source = os.environ if env is None else env
    value = str(source.get("HERDR_TELEGRAM_TOPICS_RESPONSE_COLLAPSE_PREVIOUS", "") or "").strip().lower()
    return value in {"1", "true", "yes", "on"}


def topic_status_icons_enabled(env: Any | None = None) -> bool:
    source = os.environ if env is None else env
    value = str(source.get("HERDR_TELEGRAM_TOPICS_STATUS_ICON", "1") or "").strip().lower()
    return value not in {"0", "false", "no", "off"}


def pinned_status_enabled(env: Any | None = None) -> bool:
    """Whether to post the pinned status board(s) — the global overview pinned in
    General and the per-topic status line — which show each agent's selected model.
    Default on; HERDRES_PINNED_STATUS=0 turns both off.

    Note: this only stops *updating* the boards. Any boards already pinned from a
    prior run stay pinned, frozen at their last content — set the flag off before
    first run, or unpin the existing boards manually, to avoid a stale board that
    reads as live status."""
    source = os.environ if env is None else env
    value = str(source.get("HERDRES_PINNED_STATUS", "1") or "").strip().lower()
    return value not in {"0", "false", "no", "off"}


def ack_on_send(env: Any | None = None) -> bool:
    """Whether to reply with a 'Sent to Tendwire worker' ack after a successful
    inbound send. Default on; HERDRES_ACK_ON_SEND=0 suppresses it, so you only see
    the agent's working + response messages. Send FAILURES are still reported."""
    source = os.environ if env is None else env
    value = str(source.get("HERDRES_ACK_ON_SEND", "1") or "").strip().lower()
    return value not in {"0", "false", "no", "off"}


def delete_topic_icon_service_messages(env: Any | None = None) -> bool:
    source = os.environ if env is None else env
    value = str(source.get("HERDR_TELEGRAM_TOPICS_DELETE_ICON_MESSAGES", "1") or "").strip().lower()
    return value not in {"0", "false", "no", "off"}


def topic_icon_cache_ttl_seconds(env: Any | None = None) -> int:
    source = os.environ if env is None else env
    try:
        return max(60, int(str(source.get("HERDR_TELEGRAM_TOPICS_STATUS_ICON_CACHE_TTL", "86400") or "86400")))
    except ValueError:
        return 86400


def offlock_interpane_yield_enabled(env: Any | None = None) -> bool:
    """Whether sync_once briefly releases the state lock between delivered turns so a queued inbound
    command can interleave instead of stalling behind the whole delivery loop's Telegram sends (the
    source-mode jam, #122). Read at call time, not import-time, so the plugin/subprocess paths (no
    systemd EnvironmentFile) still honour it."""
    source = os.environ if env is None else env
    value = str(source.get("HERDRES_OFFLOCK_INTERPANE_YIELD", "1") or "").strip().lower()
    return value not in {"0", "false", "no", "off"}


def source_orphan_delete_cap(env: Any | None = None) -> int:
    """Per-pass topic-delete cap for _cleanup_topics. Bounds the first source syncs (which prune many
    legacy per-worker topics) so the deletes amortize over several timer ticks instead of one long
    delete burst under the state lock. Read at call time. 0 pauses topic deletion entirely (a
    deliberate operator knob), so remaining stale topics are not reclaimed until it is raised."""
    source = os.environ if env is None else env
    try:
        return max(0, int(str(source.get("HERDR_TELEGRAM_TOPICS_SOURCE_DELETE_LIMIT", "3") or "3")))
    except (TypeError, ValueError):
        return 3


def source_topic_create_cap(env: Any | None = None) -> int:
    """Per-pass topic-create cap for _sync_sources. Bounds the first source syncs (which create a topic
    per open worker/space at once) so the creates amortize over several ticks instead of one create
    burst under the state lock. Read at call time. Raise HERDR_TELEGRAM_TOPICS_MAX_CREATES to backfill
    many topics in one pass; 0 pauses topic creation until it is raised."""
    source = os.environ if env is None else env
    try:
        return max(0, int(str(source.get("HERDR_TELEGRAM_TOPICS_MAX_CREATES", "3") or "3")))
    except (TypeError, ValueError):
        return 3


def managed_bots_enabled(env: Any | None = None) -> bool:
    source = os.environ if env is None else env
    value = str(source.get("HERDR_TELEGRAM_TOPICS_MANAGED_BOTS", "0") or "").strip().lower()
    return value in {"1", "true", "yes", "on"}


def managed_bot_token(kind: str, env: Any | None = None) -> str:
    source = os.environ if env is None else env
    normalized = "".join(char for char in str(kind or "").upper() if char.isalnum())
    if not normalized:
        return ""
    for key in (
        f"HERDRES_MANAGED_BOT_{normalized}_TOKEN",
        f"HERDR_TELEGRAM_TOPICS_MANAGED_BOT_{normalized}_TOKEN",
    ):
        value = str(source.get(key, "") or "").strip()
        if value:
            return value
    return ""


def rich_messages_enabled(env: Any | None = None) -> bool:
    source = os.environ if env is None else env
    value = str(source.get("HERDR_TELEGRAM_TOPICS_RICH_MESSAGES", "1") or "").strip().lower()
    return value not in {"0", "false", "no", "off"}


def telegram_token(env: Any | None = None) -> str:
    source = os.environ if env is None else env
    for key in ("HERDRES_OUTBOUND_BOT_TOKEN", "TELEGRAM_BOT_TOKEN", "BOT_TOKEN"):
        value = str(source.get(key, "") or "").strip()
        if value:
            return value
    return ""


def telegram_chat_id(state: dict[str, Any], env: Any | None = None) -> str:
    source = os.environ if env is None else env
    telegram = state.get("telegram") if isinstance(state.get("telegram"), dict) else {}
    return str(source.get("HERDRES_TELEGRAM_CHAT_ID", telegram.get("chat_id", "")) or "").strip()


def general_thread_id(state: dict[str, Any] | None = None, env: Any | None = None) -> str:
    source = os.environ if env is None else env
    telegram = state.get("telegram") if isinstance(state, dict) and isinstance(state.get("telegram"), dict) else {}
    return str(source.get("HERDRES_TELEGRAM_GENERAL_THREAD_ID", telegram.get("general_thread_id", DEFAULT_GENERAL_THREAD_ID)) or DEFAULT_GENERAL_THREAD_ID)
