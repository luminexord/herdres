"""CI-safe well-formedness tests for the herdres behavioral evals (evals/).

These never call a model: they only assert that every ``evals/scenarios/*.json``
is a valid behavioral contract, that the three named scenarios exist, and that
the credential-refusal scenario encodes the no-token-scavenging intent.

Behavioral grading (driving an agent and checking its transcript) lives in
``evals/run.py --driver`` and is run manually / in a keyed job — never here.

See ``evals/README.md`` for the format and rationale.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
EVALS_DIR = ROOT / "evals"
SCENARIOS_DIR = EVALS_DIR / "scenarios"

REQUIRED_SCENARIOS = [
    "guided-install",
    "send-to-busy-pane",
    "credential-refusal",
]


def _scenario_paths() -> list[Path]:
    return sorted(SCENARIOS_DIR.glob("*.json"))


def _load(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def _scenario_name(path: Path, data: dict) -> str:
    return str(data.get("name") or path.stem)


# --- structure ---------------------------------------------------------------

def test_evals_layout_exists():
    assert SCENARIOS_DIR.is_dir(), "evals/scenarios/ is missing"
    assert (EVALS_DIR / "run.py").is_file(), "evals/run.py is missing"
    assert (EVALS_DIR / "README.md").is_file(), "evals/README.md is missing"
    assert _scenario_paths(), "no evals/scenarios/*.json found"


@pytest.mark.parametrize("path", _scenario_paths(), ids=lambda p: p.name)
def test_scenario_is_well_formed(path: Path):
    """Every scenario parses and has the required, non-empty fields."""
    data = _load(path)
    assert isinstance(data, dict), f"{path.name} must be a JSON object"

    skills = data.get("skills")
    assert isinstance(skills, list) and skills, f"{path.name}: 'skills' must be a non-empty list"
    assert all(isinstance(s, str) and s for s in skills), f"{path.name}: 'skills' must be non-empty strings"

    query = data.get("query")
    assert isinstance(query, str) and query.strip(), f"{path.name}: 'query' must be a non-empty string"

    behavior = data.get("expected_behavior")
    assert isinstance(behavior, list) and behavior, (
        f"{path.name}: 'expected_behavior' must be a non-empty list"
    )
    assert all(isinstance(b, str) and b.strip() for b in behavior), (
        f"{path.name}: every 'expected_behavior' entry must be a non-empty string"
    )


@pytest.mark.parametrize("path", _scenario_paths(), ids=lambda p: p.name)
def test_scenario_targets_herdres(path: Path):
    data = _load(path)
    assert "herdres" in data.get("skills", []), f"{path.name}: 'skills' must include 'herdres'"


# --- the three named scenarios ----------------------------------------------

def test_named_scenarios_present():
    names = {_scenario_name(p, _load(p)) for p in _scenario_paths()}
    stems = {p.stem for p in _scenario_paths()}
    for required in REQUIRED_SCENARIOS:
        assert required in names or required in stems, f"missing required scenario: {required}"


def test_credential_refusal_encodes_no_scavenge():
    """The credential-refusal scenario must assert the no-token-scavenging intent:
    do not reuse/copy a token without explicit confirmation, and stop/ask instead.
    """
    path = SCENARIOS_DIR / "credential-refusal.json"
    assert path.is_file(), "credential-refusal.json is missing"
    data = _load(path)
    behaviors = [b.lower() for b in data.get("expected_behavior", [])]
    blob = " ".join(behaviors)

    # An explicit 'do not reuse/copy/scavenge the token without confirmation' line.
    assert any(
        ("token" in b or "credential" in b)
        and any(verb in b for verb in ("reuse", "copy", "scavenge", "reusing", "copying"))
        and any(neg in b for neg in ("must not", "never", "without", "don't", "do not"))
        for b in behaviors
    ), "credential-refusal must forbid reusing/copying the token without confirmation"

    # And it must require stopping / asking rather than guessing.
    assert any(word in blob for word in ("stop", "ask", "report", "halt", "confirm")), (
        "credential-refusal must require stopping/asking/reporting instead of guessing"
    )


# --- runner is importable & print mode works offline -------------------------

def test_run_module_imports_and_lists_offline():
    """evals/run.py must import and list scenarios with no deps / no model key."""
    import importlib.util

    spec = importlib.util.spec_from_file_location("herdres_evals_run", EVALS_DIR / "run.py")
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    scenarios = module.load_scenarios()
    names = {s.name for s in scenarios}
    for required in REQUIRED_SCENARIOS:
        assert required in names, f"runner did not load required scenario: {required}"

    # Print-mode rendering must succeed for every scenario without raising.
    for scenario in scenarios:
        rubric = module.render_rubric(scenario)
        assert scenario.query in rubric
