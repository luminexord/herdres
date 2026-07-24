"""Suite-wide hermeticity guards.

The pinned account line (HERDRES_PINNED_ACCOUNT, default ON in prod) shells out to
ccusage and reads the real CLI credential files — neither may happen inside tests.
Default it OFF for the whole suite; tests that exercise it opt back in with
monkeypatch.setenv and stub the accounts module explicitly.
"""
from __future__ import annotations

import pytest


@pytest.fixture(autouse=True)
def _no_real_account_probes(monkeypatch):
    monkeypatch.setenv("HERDRES_PINNED_ACCOUNT", "0")
    # Most pre-lane unit tests call the legacy poll helper directly. Keep those
    # fixtures hermetic while dedicated lane tests exercise the production
    # default and explicitly opt into the durable spool.
    monkeypatch.setenv("HERDRES_INBOUND_LANES", "0")
