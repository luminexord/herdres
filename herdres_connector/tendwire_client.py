"""Tendwire CLI client helpers for Herdres source mode."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

import herdres_tendwire


@dataclass(frozen=True)
class TendwireClient:
    runner: Callable[..., Any]
    sanitize: Callable[[str, int], str]
    default_herdr_bin: str

    def snapshot(self) -> dict[str, Any]:
        return herdres_tendwire.snapshot_payload(
            runner=self.runner,
            sanitize=self.sanitize,
            default_herdr_bin=self.default_herdr_bin,
        )

    def turns(self) -> dict[str, Any]:
        return herdres_tendwire.cached_turns_payload(
            runner=self.runner,
            sanitize=self.sanitize,
            default_herdr_bin=self.default_herdr_bin,
        )

    def command(self, request: dict[str, Any]) -> dict[str, Any]:
        return herdres_tendwire.command_submit(
            request,
            runner=self.runner,
            sanitize=self.sanitize,
            default_herdr_bin=self.default_herdr_bin,
        )

    def connector_call(self, action: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        return herdres_tendwire.connector_call(
            action,
            params,
            runner=self.runner,
            sanitize=self.sanitize,
            default_herdr_bin=self.default_herdr_bin,
        )

