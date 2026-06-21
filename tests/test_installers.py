"""Content assertions for the env-safe installers (issue #13, Phase 1+2).

These tests are CI-safe: they never run the installers or touch real config.
They read the shell scripts as text and assert two invariants from the
cross-task source-marker contract (see GOALS/issue-13/installers.md):

1. ``install-user.sh`` must NOT clobber an existing ``herdres.env`` — the
   ``.env.example`` copy has to be guarded behind a ``[ -f … ]`` check, mirroring
   ``install-macos.sh`` which already guards with ``if [ ! -f … ]``.
2. BOTH installers must record the local checkout path to the source marker
   ``~/.local/share/herdres/source`` so ``herdres update --edge`` can find it.

Static + offline; no installs, no systemctl/launchctl, no env writes.
"""

from __future__ import annotations

import shutil
import subprocess
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]

USER_INSTALLER = ROOT / "install-user.sh"
MACOS_INSTALLER = ROOT / "install-macos.sh"

# The source-marker path both installers must write (the cross-task contract).
SOURCE_MARKER = "share/herdres/source"


def _read(path: Path) -> str:
    assert path.is_file(), f"missing installer: {path}"
    return path.read_text(encoding="utf-8")


def _logical_lines(text: str) -> list[str]:
    """Split on newlines but fold shell backslash-continuations into one line.

    ``foo || \\<newline>    bar`` is a single logical statement; the guard and the
    guarded command can legitimately live on separate physical lines.
    """
    joined = text.replace("\\\n", " ")
    return joined.splitlines()


def test_user_installer_does_not_clobber_env() -> None:
    text = _read(USER_INSTALLER)

    # Every statement that installs .env.example onto herdres.env must be guarded
    # by a `[ -f ... ]` / `if [ ! -f ... ]` existence check — no unconditional copy.
    env_lines = [
        line
        for line in _logical_lines(text)
        if "install" in line and ".env.example" in line and "herdres.env" in line
    ]
    assert env_lines, "install-user.sh no longer copies .env.example to herdres.env?"
    for line in env_lines:
        assert "[ -f" in line or "[ ! -f" in line, (
            "install-user.sh writes herdres.env unconditionally (would clobber "
            f"live credentials): {line.strip()!r}"
        )

    # And the guard must reference the real config file, not just any path.
    assert ".config/herdres/herdres.env" in text


def test_macos_installer_guards_env() -> None:
    # Regression guard for the installer we mirror: macOS already protects the
    # config behind `if [ ! -f "$CFG/herdres.env" ]`.
    text = _read(MACOS_INSTALLER)
    assert "[ ! -f" in text and "herdres.env" in text


@pytest.mark.parametrize("path", [USER_INSTALLER, MACOS_INSTALLER])
def test_installer_writes_source_marker(path: Path) -> None:
    text = _read(path)
    assert SOURCE_MARKER in text, (
        f"{path.name} must write the source marker ~/.local/share/{SOURCE_MARKER}"
    )
    # The marker line must redirect *into* the source file (a write), not merely
    # mention the path in a comment.
    marker_writes = [
        line
        for line in _logical_lines(text)
        if '> "' in line and SOURCE_MARKER in line
    ]
    assert marker_writes, (
        f'{path.name} mentions {SOURCE_MARKER} but never writes to it (no `> "…/source"`)'
    )


@pytest.mark.parametrize(
    "path,shell",
    [(USER_INSTALLER, "sh"), (MACOS_INSTALLER, "bash")],
)
def test_installer_parses(path: Path, shell: str) -> None:
    # Optional robustness: confirm the scripts still parse after our edits.
    interp = shutil.which(shell)
    if interp is None:
        pytest.skip(f"{shell} not available")
    result = subprocess.run(
        [interp, "-n", str(path)],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, f"{path.name} failed `{shell} -n`: {result.stderr}"
