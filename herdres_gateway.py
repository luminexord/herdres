#!/usr/bin/env python3
"""Executable composition root for durable Herdres Telegram ingress."""

from __future__ import annotations

import signal
import sys
import threading
from collections.abc import Callable
from types import FrameType

from herdres_connector import config, ingress, state
from herdres_connector.ingress_identity import load_request_id_key
from herdres_connector.ingress_queue import IngressQueue
from herdres_connector.telegram_delivery import TelegramClient
from herdres_connector.tendwire_client import TendwireClient


TELEGRAM_LONG_POLL_TRANSPORT_TIMEOUT_SECONDS = 65.0


def _log(message: str) -> None:
    print(f"[herdres-gateway] {message}", file=sys.stderr, flush=True)


def _install_stop_handlers(
    stop_event: threading.Event,
) -> dict[signal.Signals, signal.Handlers]:
    previous: dict[signal.Signals, signal.Handlers] = {}

    def request_stop(_signum: int, _frame: FrameType | None) -> None:
        stop_event.set()

    for signum in (signal.SIGINT, signal.SIGTERM):
        previous[signum] = signal.getsignal(signum)
        signal.signal(signum, request_stop)
    return previous


def _restore_stop_handlers(
    previous: dict[signal.Signals, signal.Handlers],
) -> None:
    for signum, handler in previous.items():
        signal.signal(signum, handler)


def _telegram_clients(
    receivers: tuple[state.IngressReceiver, ...],
) -> dict[str, TelegramClient]:
    return {
        receiver.receiver_id: TelegramClient(
            token=receiver.token.reveal_for_telegram_client(),
            timeout=TELEGRAM_LONG_POLL_TRANSPORT_TIMEOUT_SECONDS,
        )
        for receiver in receivers
    }


def main() -> int:
    config.load_env_file()
    config.require_source_mode()
    state_path = config.state_path()
    request_id_key = load_request_id_key()
    receivers = state.read_ingress_receivers(state_path)
    clients = _telegram_clients(receivers)
    stop_event = threading.Event()
    previous_handlers = _install_stop_handlers(stop_event)
    try:
        with IngressQueue.open_writer(config.inbound_spool_path()) as queue:
            ports = ingress.IngressPorts(
                state_path=state_path,
                request_id_key=request_id_key,
                queue=queue,
                receivers=receivers,
                telegram_clients=clients,
                tendwire=TendwireClient(),
                stop_event=stop_event,
                dispatch_workers=config.inbound_dispatch_workers(),
                log=_log,
            )
            return ingress.run_gateway(ports)
    finally:
        stop_event.set()
        _restore_stop_handlers(previous_handlers)


def _entrypoint(run: Callable[[], int] = main) -> int:
    try:
        return run()
    except Exception as exc:
        _log(f"startup failed: {type(exc).__name__}")
        return 1


if __name__ == "__main__":
    raise SystemExit(_entrypoint())
