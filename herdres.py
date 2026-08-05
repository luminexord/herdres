#!/usr/bin/env python3
"""Source-mode presenter and operator CLI.

Telegram ingress is owned exclusively by ``herdres-gateway.service``.  This
process keeps the pre-H7 presenter alive until the paired presenter cutover and
never opens the ingress queue for writing.
"""

from __future__ import annotations

import argparse
import copy
import json
import threading
import time
# H8-SLOC-BEGIN herdres-ingress-import-asdict
from dataclasses import asdict
# H8-SLOC-END herdres-ingress-import-asdict
from typing import Any, Callable

from herdres_connector import config, doctor, speech, state
# H8-SLOC-BEGIN herdres-ingress-import-queue
from herdres_connector.ingress_queue import IngressQueue
# H8-SLOC-END herdres-ingress-import-queue
from herdres_connector.safe import public_prune, sanitize_text
from herdres_connector.source_sync import (
    SyncRuntime,
    drain_outbound_once,
    sync_once,
)
from herdres_connector.telegram_delivery import TelegramClient
from herdres_connector.tendwire_client import TendwireClient


VERSION = "0.7.0rc4-tendwired-source-only"


def _json(data: dict[str, Any]) -> int:
    print(json.dumps(public_prune(data), ensure_ascii=False, sort_keys=True))
    return 0 if data.get("ok", True) else 1


def _runtime(
    *,
    dry_run: bool = False,
    with_outbox: bool = True,
    checkpoint: Callable[[], None] | None = None,
) -> SyncRuntime:
    return SyncRuntime(
        tendwire=TendwireClient(),
        telegram=TelegramClient(token=config.telegram_token(), dry_run=dry_run),
        dry_run=dry_run,
        with_outbox=with_outbox,
        checkpoint=checkpoint,
    )


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
        except Exception as exc:  # noqa: BLE001 - bounded service loop
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
        stop.wait(max(0.0, cadence - (time.monotonic() - started)))


def cmd_sync(args: argparse.Namespace) -> int:
    config.load_env_file()
    config.require_source_mode()
    interval = float(getattr(args, "loop", 0) or 0)
    if interval <= 0:
        return _json(_sync_pass())

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
            started = time.monotonic()
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
            time.sleep(max(0.5, interval - (time.monotonic() - started)))
    finally:
        stop.set()
        poller.join(
            timeout=max(1.0, config.tendwire_connector_poll_seconds() + 0.5)
        )


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
        ok, detail = speech.install_stt_model(
            force=bool(args.force), log=lambda message: logs.append(str(message))
        )
        result: dict[str, Any] = {
            "ok": bool(ok),
            "status": "ok" if ok else "failed",
            "stt_model": detail,
            "speech": speech.check(),
        }
        if logs:
            result["log"] = logs[-3:]
        if not speech.sherpa_available():
            result["next_step"] = (
                "Install the sherpa-onnx Python package, then enable "
                "HERDR_TELEGRAM_TOPICS_SPEECH_INPUT=1."
            )
        return _json(result)
    return _json(
        {"ok": False, "status": "failed", "error": f"unknown speech action: {action}"}
    )


def cmd_source_smoke(args: argparse.Namespace) -> int:
    config.load_env_file()
    config.require_source_mode()
    with state.state_lock():
        store = copy.deepcopy(state.load_state())
    result = sync_once(
        store, _runtime(dry_run=True, with_outbox=bool(args.with_outbox))
    )
    return _json(
        {
            "ok": bool(result.get("ok")),
            "status": "ok" if result.get("ok") else "failed",
            "mode": "source",
            "dry_run": True,
            "with_outbox": bool(args.with_outbox),
            "direct_herdr_calls": 0,
            "sync_result": result,
            "delivery_evidence": {
                "source_entry_count": len(state.source_entries(store)),
                "delivered_turn_count": len(
                    store.get("tendwire_source_delivered_turns") or {}
                ),
            },
        }
    )


# H8-SLOC-BEGIN herdres-ingress-status
def cmd_ingress_status(_args: argparse.Namespace) -> int:
    """Print aggregate ingress health without prompt or identity data."""

    config.load_env_file()
    now = time.time()
    with IngressQueue.observe(config.inbound_spool_path()) as observer:
        health = observer.health_snapshot(now)
        rows = observer.status_rows(now)
    attention = health.quarantine + health.claimed_notices + health.overdue_open
    return _json(
        {
            "ok": attention == 0,
            "schema_version": 1,
            "attention_required": attention,
            "health": asdict(health),
            "statuses": [asdict(row) for row in rows],
        }
    )
# H8-SLOC-END herdres-ingress-status


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="herdres")
    sub = parser.add_subparsers(dest="cmd", required=True)
    sync_parser = sub.add_parser("sync")
    sync_parser.add_argument(
        "--loop", type=float, default=0.0, help="run continuously, one pass every N seconds"
    )
    sync_parser.set_defaults(func=cmd_sync)
    sub.add_parser("doctor").set_defaults(func=cmd_doctor)
    # H8-SLOC-BEGIN herdres-ingress-parser
    sub.add_parser("ingress-status").set_defaults(func=cmd_ingress_status)
    # H8-SLOC-END herdres-ingress-parser
    sub.add_parser("version").set_defaults(
        func=lambda _args: (print(VERSION), 0)[1]
    )
    speech_parser = sub.add_parser("speech")
    speech_parser.add_argument(
        "action", nargs="?", default="check", choices=["check", "install"]
    )
    speech_parser.add_argument("--force", action="store_true")
    speech_parser.set_defaults(func=cmd_speech)
    tendwire = sub.add_parser("tendwire")
    tendwire_sub = tendwire.add_subparsers(dest="tendwire_cmd", required=True)
    smoke = tendwire_sub.add_parser("source-smoke")
    smoke.add_argument("--with-outbox", action="store_true")
    smoke.set_defaults(func=cmd_source_smoke)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        return int(args.func(args) or 0)
    except Exception as exc:  # noqa: BLE001 - CLI returns bounded public error
        return _json(
            {"ok": False, "status": "failed", "error": sanitize_text(str(exc), 300)}
        )


if __name__ == "__main__":
    raise SystemExit(main())
