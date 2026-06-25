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


def _env_install(line: str) -> bool:
    """Does this line copy ``.env.example`` onto the live ``herdres.env``?"""
    return "install" in line and ".env.example" in line and "herdres.env" in line


def _env_install_logical_lines(text: str) -> list[str]:
    """Logical lines that copy ``.env.example`` onto the live ``herdres.env``."""
    return [line for line in _logical_lines(text) if _env_install(line)]


def _form_a_guarded(physical: list[str], i: int) -> bool:
    """Form A: ``[ -f … ] || install …``.

    The guard and the ``install`` may sit on separate physical lines joined by a
    trailing ``\\``; fold the env-copy line together with any continuation lines
    immediately above it, then look for the ``[ -f … ]`` / ``[ ! -f … ]`` test.
    """
    start = i
    while start > 0 and physical[start - 1].rstrip().endswith("\\"):
        start -= 1
    logical = " ".join(p.rstrip("\\") for p in physical[start : i + 1])
    return "[ -f" in logical or "[ ! -f" in logical


def _form_b_guarded(physical: list[str], i: int) -> bool:
    """Form B: the env copy sits inside an ``if [ ! -f … herdres.env … ]`` block.

    Scan upward for the nearest still-open ``if`` (matching intervening
    ``fi``s) and require it to be the file-existence guard for ``herdres.env``.
    """
    depth = 0
    for j in range(i - 1, -1, -1):
        stripped = physical[j].strip()
        if stripped == "fi" or stripped.startswith("fi "):
            depth += 1
        elif stripped.startswith("if "):
            if depth == 0:
                return (
                    ("[ ! -f" in physical[j] or "[ -f" in physical[j])
                    and "herdres.env" in physical[j]
                )
            depth -= 1
    return False


def _guards_env_install(text: str) -> bool:
    """Is *every* ``.env.example -> herdres.env`` copy behind an existence guard?

    Accepts BOTH guard shapes and fails (returns ``False``) if any copy is
    unguarded — i.e. removing the guard must make this return ``False``.
    """
    physical = text.splitlines()
    install_idx = [i for i, line in enumerate(physical) if _env_install(line)]
    if not install_idx:
        return False
    return all(
        _form_a_guarded(physical, i) or _form_b_guarded(physical, i)
        for i in install_idx
    )


def _checkout_dir_expr(text: str) -> str | None:
    """Return the shell expression a marker write uses for the checkout dir.

    Both installers must record the *same* expression (``$PWD``); the other
    installs are cwd-relative so the script is run from the checkout root.
    Returns the right-hand side of the ``printf … > "…/source"`` write.
    """
    for line in _logical_lines(text):
        if '> "' in line and SOURCE_MARKER in line and "printf" in line:
            return line
    return None


def test_user_installer_does_not_clobber_env() -> None:
    text = _read(USER_INSTALLER)

    # The .env.example -> herdres.env copy must exist and be guarded by an
    # existence check, in EITHER the `[ -f … ] || install …` short-circuit form
    # or an `if [ ! -f … ]; then … fi` block. Deleting the guard must flip this.
    assert _env_install_logical_lines(text), (
        "install-user.sh no longer copies .env.example to herdres.env?"
    )
    assert _guards_env_install(text), (
        "install-user.sh writes herdres.env unconditionally (would clobber live "
        "credentials): the copy is not behind a `[ -f … ]` / `if [ ! -f … ]` guard"
    )

    # And the guard must reference the real config file, not just any path.
    assert ".config/herdres/herdres.env" in text


def test_macos_installer_guards_env() -> None:
    # Regression guard for the installer we mirror: macOS protects the config
    # behind `if [ ! -f "$CFG/herdres.env" ]; then … install … fi`. Assert the
    # env-install line lives *inside* that guard block, so this fails outright
    # if the `if [ ! -f … ]` guard (or its `fi`) is deleted — not just two
    # independent substring checks that survive removing the guard.
    text = _read(MACOS_INSTALLER)

    # There must be an env copy at all (it lives on its own physical line).
    assert _env_install_logical_lines(text), (
        "install-macos.sh no longer copies .env.example to herdres.env?"
    )
    # And it must be enclosed by the `if [ ! -f … herdres.env … ]` guard block.
    assert _guards_env_install(text), (
        "install-macos.sh writes herdres.env unconditionally (would clobber live "
        "credentials): the copy is not inside an `if [ ! -f … ]` guard block"
    )


@pytest.mark.parametrize("path", [USER_INSTALLER, MACOS_INSTALLER])
def test_installer_writes_source_marker(path: Path) -> None:
    text = _read(path)
    assert SOURCE_MARKER in text, (
        f"{path.name} must write the source marker ~/.local/share/{SOURCE_MARKER}"
    )
    # The marker line must redirect *into* the source file (a write), not merely
    # mention the path in a comment.
    marker_line = _checkout_dir_expr(text)
    assert marker_line is not None, (
        f'{path.name} mentions {SOURCE_MARKER} but never writes to it (no `> "…/source"`)'
    )

    # Fix 3+4: the write must record the *checkout dir*, not an arbitrary string.
    # Both installers use the same consistent expression, $PWD (the cwd), because
    # their other installs are cwd-relative (`install -Dm755 herdres.py …`), so
    # the script is run from the checkout root and $PWD *is* the checkout.
    assert '"$PWD"' in marker_line, (
        f"{path.name} must write $PWD (the checkout dir) to the source marker, "
        f"not just any path: {marker_line.strip()!r}"
    )
    # Guard against the stale script-dir form so the two installers stay in sync.
    assert "dirname" not in marker_line, (
        f"{path.name} source-marker write should use $PWD, not a script-dir "
        f"`$(cd \"$(dirname \"$0\")\" && pwd)` expression: {marker_line.strip()!r}"
    )


@pytest.mark.parametrize("path", [USER_INSTALLER, MACOS_INSTALLER])
def test_installer_installs_runtime_commands(path: Path) -> None:
    """Issue #27: both installers must (re)install the /herdres slash commands by running
    ``herdres commands install`` — best-effort (``|| true``) and with an explicit ``--source`` so the
    copy is cwd-independent (the macOS installer's $PWD marker can't be trusted: its file installs
    are all $HERE-based)."""
    text = _read(path)
    lines = _logical_lines(text)
    cmd_idx = next((i for i, ln in enumerate(lines) if "commands install" in ln), None)
    assert cmd_idx is not None, (
        f"{path.name} must run `herdres commands install` to install the /herdres slash commands"
    )
    line = lines[cmd_idx]
    assert "|| true" in line, (
        f"{path.name}: `commands install` must be best-effort (`|| true`), not fail the install"
    )
    assert "--source" in line, (
        f"{path.name}: `commands install` must pass an explicit --source (cwd-independent copy)"
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
