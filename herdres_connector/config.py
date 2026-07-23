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
DEFAULT_REQUEST_ID_KEY_PATH = HOME / ".local/share/herdres/request-id.key"
DEFAULT_INBOUND_SPOOL_PATH = HOME / ".local/share/herdres/inbound_spool.db"
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


def tendwire_timeout_seconds(env: Any | None = None) -> float:
    source = os.environ if env is None else env
    try:
        value = float(str(source.get("HERDRES_TENDWIRE_TIMEOUT_SECONDS", "60") or "60"))
    except (TypeError, ValueError):
        return 60.0
    return min(300.0, max(1.0, value))


def tendwire_delta_limit(env: Any | None = None) -> int:
    source = os.environ if env is None else env
    try:
        value = int(str(source.get("HERDRES_TENDWIRE_DELTA_LIMIT", "500") or "500"))
    except (TypeError, ValueError):
        return 500
    return min(500, max(1, value))


def tendwire_full_reconcile_seconds(env: Any | None = None) -> int:
    source = os.environ if env is None else env
    try:
        value = int(
            str(
                source.get(
                    "HERDRES_TENDWIRE_FULL_RECONCILE_SECONDS",
                    "3600",
                )
                or "3600"
            )
        )
    except (TypeError, ValueError):
        return 3600
    # A disabled safety reconciliation leaves the retained delta projection
    # unbounded. Treat non-positive values as invalid and retain the hourly
    # safety net instead.
    if value <= 0:
        return 3600
    return min(604800, value)


def tendwire_force_full_reconcile(env: Any | None = None) -> bool:
    source = os.environ if env is None else env
    value = str(source.get("HERDRES_TENDWIRE_FORCE_FULL_RECONCILE", "") or "").strip().lower()
    return value in {"1", "true", "yes", "on"}


def request_id_key_path(env: Any | None = None) -> Path:
    source = os.environ if env is None else env
    configured = source.get("HERDRES_REQUEST_ID_KEY_PATH")
    key_path = Path(
        DEFAULT_REQUEST_ID_KEY_PATH
        if configured is None or configured == ""
        else configured
    ).expanduser()
    if not key_path.is_absolute():
        raise ValueError(
            "HERDRES_REQUEST_ID_KEY_PATH must expand to a nonempty absolute path"
        )
    return key_path


def mode(env: Any | None = None) -> str:
    source = os.environ if env is None else env
    return str(source.get("HERDRES_TENDWIRE_MODE", "source") or "source").strip().lower()


def tendwire_turn_final_lease_seconds(env: Any | None = None) -> int:
    source = os.environ if env is None else env
    try:
        value = int(
            str(
                source.get(
                    "HERDRES_TENDWIRE_TURN_FINAL_LEASE_SECONDS",
                    "60",
                )
                or "60"
            )
        )
    except (TypeError, ValueError):
        return 60
    return min(3600, max(60, value))


def command_retry_horizon_seconds(env: Any | None = None) -> int:
    source = os.environ if env is None else env
    try:
        value = int(
            str(
                source.get(
                    "HERDRES_COMMAND_RETRY_HORIZON_SECONDS",
                    "86400",
                )
                or "86400"
            )
        )
    except (TypeError, ValueError):
        return 86400
    return min(604800, max(60, value))


def command_request_retention_seconds(env: Any | None = None) -> int:
    return command_retry_horizon_seconds(env) + 86_400


def command_response_schema_version(env: Any | None = None) -> int:
    """Return the explicitly negotiated Tendwire command envelope version.

    Version 2 remains the default so an installed pre-v3 Tendwire keeps seeing
    byte-for-byte compatible command requests.  Operators can opt in to v3
    submission receipts without changing the request schema itself.
    """

    source = os.environ if env is None else env
    raw = source.get("HERDRES_TENDWIRE_COMMAND_RESPONSE_SCHEMA_VERSION", "2")
    try:
        value = int(str(raw or "2"))
    except (TypeError, ValueError):
        return 2
    return value if value in {2, 3} else 2


def inbound_lanes_enabled(env: Any | None = None) -> bool:
    """Enable the durable, independently dispatched Telegram ingress lanes."""

    source = os.environ if env is None else env
    value = str(source.get("HERDRES_INBOUND_LANES", "") or "").strip().lower()
    return value in {"1", "true", "yes", "on"}


def inbound_spool_path(env: Any | None = None) -> Path:
    source = os.environ if env is None else env
    return Path(
        source.get("HERDRES_INBOUND_SPOOL_PATH", DEFAULT_INBOUND_SPOOL_PATH)
    ).expanduser()


def inbound_dispatch_workers(env: Any | None = None) -> int:
    source = os.environ if env is None else env
    try:
        value = int(str(source.get("HERDRES_INBOUND_DISPATCH_WORKERS", "8") or "8"))
    except (TypeError, ValueError):
        return 8
    return min(64, max(1, value))


def inbound_lane_depth(env: Any | None = None) -> int:
    source = os.environ if env is None else env
    try:
        value = int(str(source.get("HERDRES_INBOUND_LANE_DEPTH", "32") or "32"))
    except (TypeError, ValueError):
        return 32
    return min(4096, max(1, value))


def inbound_lane_backoff_seconds(env: Any | None = None) -> float:
    source = os.environ if env is None else env
    try:
        value = float(
            str(source.get("HERDRES_INBOUND_LANE_BACKOFF_SECONDS", "2") or "2")
        )
    except (TypeError, ValueError):
        return 2.0
    return min(300.0, max(0.01, value))


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


def pinned_account_enabled(env: Any | None = None) -> bool:
    """Append a who-am-I/quota line to the pinned status boards: plan tier from the CLI
    credential files (named metadata fields only, never tokens) plus the remaining 5h and
    weekly rate-limit headroom (Claude: the OAuth usage endpoint behind in-app /usage;
    Codex: the rate_limits events in its local session logs). Default on; degrades to no
    line when the sources are absent. HERDRES_PINNED_ACCOUNT=0 turns it off."""
    source = os.environ if env is None else env
    value = str(source.get("HERDRES_PINNED_ACCOUNT", "1") or "").strip().lower()
    return value not in {"0", "false", "no", "off"}


def usage_refresh_seconds(env: Any | None = None) -> int:
    """Disk-cache TTL for the quota snapshot behind the pinned account line. Coarse on
    purpose: every refresh can change the line and re-edit every pinned board, so this TTL
    is the pin-edit rate limiter. HERDRES_USAGE_REFRESH_SECONDS, default 300."""
    source = os.environ if env is None else env
    raw = str(source.get("HERDRES_USAGE_REFRESH_SECONDS", "") or "").strip()
    try:
        value = int(raw)
    except ValueError:
        return 300
    return value if value > 0 else 300


def ack_on_send(env: Any | None = None) -> bool:
    """Whether to reply with a 'Sent to Tendwire worker' ack after a successful
    inbound send. Default on; HERDRES_ACK_ON_SEND=0 suppresses it, so you only see
    the agent's working + response messages. Send FAILURES are still reported."""
    source = os.environ if env is None else env
    value = str(source.get("HERDRES_ACK_ON_SEND", "1") or "").strip().lower()
    return value not in {"0", "false", "no", "off"}


def remote_decisions_enabled(env: Any | None = None) -> bool:
    """Whether structured Claude prompts get remote inline controls.

    Default on.  An empty value deliberately means the default rather than an
    accidental opt-out, matching the connector's other default-on flags.
    """
    source = os.environ if env is None else env
    value = str(source.get("HERDRES_REMOTE_DECISIONS", "1") or "").strip().lower()
    return value not in {"0", "false", "no", "off"}



def reap_closed_worker_topics(env: Any | None = None) -> bool:
    """Worker mode only: delete the Telegram topic of a worker that has durably FINISHED and left the
    tendwire snapshot. herdr/tendwire re-letters worker ids positionally across restarts (claude-2 ->
    claude-2-2 for a fresh terminal), so the connector mints a new topic for the re-registered pane and
    the old one strands forever (worker-mode cleanup otherwise only deletes done-council topics). This
    reaps those strays and frees their squatted names. DESTRUCTIVE (deletes finished topics + their
    scrollback), so default OFF; HERDRES_REAP_CLOSED_WORKER_TOPICS=1 opts in. Guarded by finished-status
    + N-pass absence + a non-empty-snapshot check + the per-pass delete cap."""
    source = os.environ if env is None else env
    value = str(source.get("HERDRES_REAP_CLOSED_WORKER_TOPICS", "") or "").strip().lower()
    return value in {"1", "true", "yes", "on"}


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


def working_update_min_seconds(env: Any | None = None) -> int:
    """Minimum seconds between same-turn Working card edits.

    The first Working card for a turn is still immediate. This only bounds
    repeat edits for a turn that is already visible in Telegram.
    """
    source = os.environ if env is None else env
    try:
        return max(0, int(str(source.get("HERDR_TELEGRAM_TOPICS_WORKING_UPDATE_MIN_SECONDS", "30") or "30")))
    except (TypeError, ValueError):
        return 30


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


def close_dormant_after_hours(env: Any | None = None) -> float:
    """Age at which closed pane and retired archive topics are auto-closed.

    Zero deliberately disables the lifecycle cleanup.  The legacy issue-draft
    name remains accepted so an operator who tested the pre-RC proposal does
    not silently lose their setting.
    """
    source = os.environ if env is None else env
    raw = source.get(
        "HERDRES_CLOSE_DORMANT_AFTER_HOURS",
        source.get("HERDR_TELEGRAM_TOPICS_CLOSE_DORMANT_AFTER_HOURS", "24"),
    )
    try:
        value = float(str(raw or "0"))
    except (TypeError, ValueError):
        return 24.0
    return min(24.0 * 365.0, max(0.0, value))


def cleanup_budget_seconds(env: Any | None = None) -> float:
    """Hard wall-clock budget for close/reopen Telegram calls in one pass."""
    source = os.environ if env is None else env
    try:
        value = float(
            str(source.get("HERDRES_CLEANUP_BUDGET_SECONDS", "5") or "0")
        )
    except (TypeError, ValueError):
        return 5.0
    return min(60.0, max(0.0, value))


def cleanup_max_ops(env: Any | None = None) -> int:
    """Maximum close/reopen calls in one pass, independent of time budget."""
    source = os.environ if env is None else env
    try:
        value = int(str(source.get("HERDRES_CLEANUP_MAX_OPS", "12") or "0"))
    except (TypeError, ValueError):
        return 12
    return min(100, max(0, value))


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
