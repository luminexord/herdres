"""Shared test infrastructure for Herdres tests.

Loads the standalone script modules (herdres.py, herdres_gateway.py,
herdres-gateway.py, herdr_turn_adapter.py, herdr_topic_bridge.py) into
``sys.modules`` once at collection time so individual test files can use a
plain ``import herdres`` instead of repeating the 5-line
``importlib.util.spec_from_file_location`` boilerplate.

Also provides shared builder helpers (``make_pane``, ``write_jsonl``) that
were previously duplicated across test files.
"""

from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]


def _load_script(filename: str, module_name: str) -> None:
    """Load a standalone script into sys.modules if not already loaded."""
    if module_name in sys.modules:
        return
    module_path = ROOT / filename
    spec = importlib.util.spec_from_file_location(module_name, module_path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)


# Load all script modules at collection time.
_load_script("herdres.py", "herdres")
_load_script("herdr_turn_adapter.py", "herdr_turn_adapter")
_load_script("herdres_decision_hook.py", "herdres_decision_hook")
_load_script("herdr_topic_bridge.py", "herdr_topic_bridge")
_load_script("herdres_gateway.py", "herdres_gateway_managed")
_load_script("herdres-gateway.py", "herdres_gateway_upstream")


# ---------------------------------------------------------------------------
# Shared builder helpers (previously duplicated in test files)
# ---------------------------------------------------------------------------

def make_pane(name: str, status: str, **extra) -> dict:
    """Build a pane dict with sensible defaults for tests.

    Extracted from test_pinned_status.py's ``pane()`` helper.
    """
    data = {
        "name": name,
        "label": name,
        "pane_id": name.lower(),
        "terminal_id": "term",
        "workspace_id": "work",
        "tab_id": "tab",
        "agent_status": status,
        "_goal_active": False,
    }
    data.update(extra)
    return data


def write_jsonl(path: Path, rows: list[dict]) -> None:
    """Write a JSONL file from a list of dicts.

    Extracted from test_turn_adapter.py's ``write_jsonl()`` helper.
    """
    path.write_text("".join(json.dumps(row) + "\n" for row in rows), encoding="utf-8")


@pytest.fixture(autouse=True)
def _clear_herdres_caches():
    """Clear per-sync caches before each test to prevent cross-test leakage."""
    herdres = sys.modules.get("herdres")
    if herdres and hasattr(herdres, "clear_sync_caches"):
        herdres.clear_sync_caches()
    yield
