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
