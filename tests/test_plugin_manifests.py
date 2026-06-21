"""Validation tests for the marketplace packaging manifests.

`herdres` ships as a plugin installable *by name* from a marketplace (issue #6),
not just by copying files. These tests guard the hand-authored manifests against
drift: they must parse as JSON, carry the required fields, agree on the skill
name, and resolve the bundled `skills/herdres/` skill. Static + offline.

See GOALS/issue-7/marketplace.md and the README "Install by name (marketplace)"
section.
"""

from __future__ import annotations

import json
import re
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]

CLAUDE_PLUGIN = ROOT / ".claude-plugin" / "plugin.json"
CODEX_PLUGIN = ROOT / ".codex-plugin" / "plugin.json"
MARKETPLACE = ROOT / ".claude-plugin" / "marketplace.json"
SKILL_MD = ROOT / "skills" / "herdres" / "SKILL.md"

# kebab-case (lowercase alphanumerics separated by single hyphens)
NAME_RE = re.compile(r"^[a-z0-9]+(-[a-z0-9]+)*$")
# semver MAJOR.MINOR.PATCH with optional -prerelease and +build metadata
SEMVER_RE = re.compile(
    r"^(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)"
    r"(?:-[0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*)?"
    r"(?:\+[0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*)?$"
)


def _load(path: Path) -> dict:
    assert path.is_file(), f"missing manifest: {path}"
    with path.open(encoding="utf-8") as fh:
        data = json.load(fh)
    assert isinstance(data, dict), f"{path} must be a JSON object"
    return data


@pytest.mark.parametrize("path", [CLAUDE_PLUGIN, CODEX_PLUGIN, MARKETPLACE])
def test_manifest_parses_as_json(path: Path) -> None:
    _load(path)


def test_claude_plugin_required_fields() -> None:
    data = _load(CLAUDE_PLUGIN)
    for key in ("name", "version", "description"):
        assert data.get(key), f"plugin.json missing non-empty {key!r}"
    assert data["name"] == "herdres"
    assert NAME_RE.match(data["name"]), "plugin name must be kebab-case"
    assert SEMVER_RE.match(data["version"]), f"version not semver: {data['version']!r}"


def test_plugin_name_matches_skill_name() -> None:
    # The plugin name must equal the bundled skill's frontmatter name.
    plugin_name = _load(CLAUDE_PLUGIN)["name"]
    text = SKILL_MD.read_text(encoding="utf-8")
    m = re.search(r"^name:\s*(\S+)\s*$", text, re.MULTILINE)
    assert m, "could not find `name:` in skills/herdres/SKILL.md frontmatter"
    assert plugin_name == m.group(1) == "herdres"


def test_codex_plugin_superset() -> None:
    data = _load(CODEX_PLUGIN)
    for key in ("name", "version", "description"):
        assert data.get(key), f"codex plugin.json missing non-empty {key!r}"
    assert data["name"] == "herdres"
    assert data.get("skills") == "./skills/"
    interface = data.get("interface")
    assert isinstance(interface, dict), "codex plugin.json needs an `interface` object"
    assert interface.get("displayName"), "interface.displayName must be non-empty"


def test_marketplace_manifest_shape() -> None:
    data = _load(MARKETPLACE)
    assert data.get("name"), "marketplace.json missing `name`"
    owner = data.get("owner")
    assert isinstance(owner, dict) and owner.get("name"), "marketplace.json needs an `owner`"
    plugins = data.get("plugins")
    assert isinstance(plugins, list) and plugins, "marketplace.json needs a non-empty `plugins`"
    first = plugins[0]
    assert isinstance(first, dict)
    for key in ("name", "description", "source"):
        assert first.get(key), f"plugins[0] missing non-empty {key!r}"
    assert first["name"] == "herdres"


def test_bundled_skill_resolves() -> None:
    # The plugin source is "." and bundles skills/herdres/; the entrypoint must exist.
    assert SKILL_MD.is_file(), "bundled skill skills/herdres/SKILL.md must exist"
