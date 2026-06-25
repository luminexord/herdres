"""Accuracy tests for the herdres operator Agent Skill (skills/herdres/).

These guard against the skill drifting from the code it documents: every CLI
subcommand, env var, and pane command the skill mentions must actually exist in
the herdres source. They are static + offline (no Telegram, no network), so they
run anywhere the repo does.

See SKILL.md for the skill itself; this is the "Level 1 (accuracy)" E2E.
"""

from __future__ import annotations

import re
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
SKILL_DIR = ROOT / "skills" / "herdres"
SKILL_MD = SKILL_DIR / "SKILL.md"
REF_DIR = SKILL_DIR / "references"
ROOT_SKILL_MD = ROOT / "SKILL.md"  # self-contained single-file entrypoint (install-anywhere)

EXPECTED_REFERENCES = [
    "SETUP.md",
    "COMMANDS.md",
    "TOPICS.md",
    "TURN_FEED.md",
    "MANAGED_BOTS.md",
    "COCKPIT.md",
    "SAFETY.md",
]

# Pane commands that herdres handles itself (not the forwarded agent commands
# like /goal or /clear, which are intentionally pass-through).
NATIVE_PANE_COMMANDS = [
    "send",
    "keys",
    "status",
    "report",
    "raw",
    "choices",
    "skills",
    "commands",
    "new",
    "debug",
    "help",
    "interrupt",
    "isend",
]


def _skill_files() -> list[Path]:
    return [SKILL_MD] + [REF_DIR / name for name in EXPECTED_REFERENCES]


def _all_skill_text() -> str:
    return "\n".join(p.read_text(encoding="utf-8") for p in _skill_files() if p.exists())


def _frontmatter_and_body(text: str) -> tuple[str, str]:
    m = re.match(r"^---\n(.*?)\n---\n", text, re.S)
    assert m, "SKILL.md must start with a YAML frontmatter block"
    return m.group(1), text[m.end():]


def _real_subcommands() -> set[str]:
    """The authoritative subcommand set, parsed from herdres' own --help."""
    out = subprocess.run(
        [sys.executable, str(ROOT / "herdres.py"), "-h"],
        capture_output=True, text=True, check=True,
    ).stdout
    m = re.search(r"\{([a-z0-9,\-]+)\}", out)
    assert m, f"could not find subcommand list in usage:\n{out}"
    return set(m.group(1).split(","))


def _source_corpus() -> str:
    """Everything a real env var / command could legitimately be defined in."""
    names = [
        ".env.example", "herdres.py", "herdres_gateway.py", "herdres-gateway.py",
        "herdr_topic_bridge.py", "herdr_turn_adapter.py", "README.md",
        "ssh/.env.example", "ssh/server/server.js",
    ]
    parts = []
    for name in names:
        p = ROOT / name
        if p.exists():
            parts.append(p.read_text(encoding="utf-8"))
    return "\n".join(parts)


# --- structure ---------------------------------------------------------------

def test_skill_files_exist():
    assert SKILL_MD.is_file(), "skills/herdres/SKILL.md is missing"
    for name in EXPECTED_REFERENCES:
        assert (REF_DIR / name).is_file(), f"missing reference: references/{name}"


def test_frontmatter_required_fields():
    fm, _ = _frontmatter_and_body(SKILL_MD.read_text(encoding="utf-8"))
    for key in ("name", "description", "license", "compatibility", "allowed-tools"):
        assert re.search(rf"^{key}\s*:", fm, re.M), f"frontmatter missing top-level '{key}'"
    assert re.search(r"^name\s*:\s*herdres\s*$", fm, re.M), "frontmatter name must be 'herdres'"
    # agentskills.io spec: there is NO top-level `version`; it lives under metadata
    assert not re.search(r"^version\s*:", fm, re.M), "version must live under metadata, not top-level"
    assert re.search(r"^\s+version\s*:", fm, re.M), "metadata.version is required"


def test_frontmatter_spec_constraints():
    """agentskills.io spec: name regex + matches dir; description 1-1024; no reserved words."""
    fm, _ = _frontmatter_and_body(SKILL_MD.read_text(encoding="utf-8"))
    name = re.search(r"^name\s*:\s*(\S+)", fm, re.M).group(1)
    assert re.fullmatch(r"[a-z0-9]+(-[a-z0-9]+)*", name), f"name '{name}' must be lowercase/hyphen (no leading/trailing/double hyphen)"
    assert name == SKILL_DIR.name, f"name '{name}' must match the skill directory '{SKILL_DIR.name}'"
    assert "anthropic" not in name and "claude" not in name, "name must not contain reserved words"
    desc = re.search(r'^description\s*:\s*"?(.*?)"?\s*$', fm, re.M).group(1)
    assert 0 < len(desc) <= 1024, f"description must be 1-1024 chars (got {len(desc)})"


def test_body_within_word_budget():
    _, body = _frontmatter_and_body(SKILL_MD.read_text(encoding="utf-8"))
    words = len(body.split())
    assert words < 2000, f"SKILL.md body is {words} words (budget < 2000)"


def test_every_reference_is_linked_and_resolves():
    skill = SKILL_MD.read_text(encoding="utf-8")
    linked = set(re.findall(r"references/([A-Z_]+\.md)", skill))
    # every link in SKILL.md must resolve on disk
    for name in linked:
        assert (REF_DIR / name).is_file(), f"SKILL.md links references/{name} which does not exist"
    # every shipped reference must be linked at least once
    for name in EXPECTED_REFERENCES:
        assert name in linked, f"references/{name} is never linked from SKILL.md"


# --- accuracy vs. the real code ---------------------------------------------

def test_no_invented_cli_subcommands():
    """Every `herdres <cmd>` in a code span must be a real subcommand."""
    real = _real_subcommands()
    text = _all_skill_text()
    mentioned = set(re.findall(r"`herdres\s+([a-z][a-z\-]*)", text))
    # 'plugin-enable'/'plugin-disable' etc. are real; bare words like 'sync' too.
    invented = {c for c in mentioned if c not in real}
    assert not invented, (
        f"skill mentions herdres subcommands that do not exist: {sorted(invented)}; "
        f"real set: {sorted(real)}"
    )
    # and the headline ones must be covered somewhere
    for must in ("sync", "probe", "cleanup-duplicates"):
        assert must in mentioned, f"skill never documents `herdres {must}`"


def test_no_invented_env_vars():
    corpus = _source_corpus()
    text = _all_skill_text()
    pattern = r"\b(HERDR_TELEGRAM_TOPICS_[A-Z0-9_]+|HERDRES_[A-Z0-9_]+|TELEGRAM_[A-Z0-9_]+|HERDR_BIN|HERDR_REAL_BIN)\b"
    mentioned = set(re.findall(pattern, text))
    assert mentioned, "expected the skill to mention env vars"
    invented = sorted(v for v in mentioned if v not in corpus)
    assert not invented, f"skill mentions env vars not found in any source file: {invented}"


def test_root_single_file_entrypoint_is_self_contained():
    """The repo-root SKILL.md installs as a lone file, so it must not depend on
    sibling files via hard relative links."""
    assert ROOT_SKILL_MD.is_file(), "repo-root SKILL.md (single-file entrypoint) is missing"
    text = ROOT_SKILL_MD.read_text(encoding="utf-8")
    fm, body = _frontmatter_and_body(text)
    assert re.search(r"^name\s*:\s*herdres\s*$", fm, re.M), "root SKILL.md name must be 'herdres'"
    assert re.search(r"^description\s*:", fm, re.M), "root SKILL.md missing description"
    # keep the root entrypoint spec-aligned & in sync with the packaged copy
    assert not re.search(r"^version\s*:", fm, re.M), "root SKILL.md: version must be under metadata"
    assert re.search(r"^allowed-tools\s*:", fm, re.M), "root SKILL.md missing allowed-tools"
    broken = re.findall(r"\]\((?:\./|references/|skills/)[^)]+\)", text)
    assert not broken, f"root SKILL.md has hard relative links (breaks single-file install): {broken}"


def test_native_pane_commands_are_real():
    """Each herdres-native /command must be handled in herdres.py."""
    src = (ROOT / "herdres.py").read_text(encoding="utf-8")
    skill = _all_skill_text()
    for cmd in NATIVE_PANE_COMMANDS:
        if f"/{cmd}" in skill or (cmd in ("interrupt", "isend") and "/send!" in skill):
            assert f'"{cmd}"' in src or f"'{cmd}'" in src, (
                f"skill documents /{cmd} but herdres.py never references the literal '{cmd}'"
            )
