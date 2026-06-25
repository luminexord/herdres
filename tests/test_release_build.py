"""Tests for scripts/build_release.sh (Issue #13 Phase 3, release assets).

These run the *real* build script against a fake herdres checkout on a tmp
filesystem and pin the cross-task release-asset contract that the
``herdres update --channel stable`` fetcher depends on:

- the assets are named ``herdres-<tag>.tar.gz`` and ``herdres-<tag>.tar.gz.sha256``;
- the tarball's members are the install set, rooted under a single
  ``herdres-<tag>/`` prefix, so an extraction is a directory the update engine's
  ``_apply_install_set(repo)`` can consume;
- ``herdres.env`` is never packaged (env-safe);
- the .sha256 file is in ``sha256sum -c`` format and actually matches the tarball.

The script is stdlib-only shell + coreutils (tar/sha256sum); nothing here touches
the network or GitHub.
"""

from __future__ import annotations

import hashlib
import subprocess
import tarfile
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
BUILD_SCRIPT = REPO_ROOT / "scripts" / "build_release.sh"

# The install set the engine replaces (mirrors herdres.py:_update_files_plan), plus
# the installer scripts + .env.example the packaged checkout carries. herdres.env is
# intentionally absent and must stay that way.
INSTALL_SET = [
    "herdres.py",
    "herdres_gateway.py",
    "herdres_routing.py",
    "herdres_decision_hook.py",
    "herdr_turn_adapter.py",
    "herdr_topic_bridge.py",
    "herdres-plugin/herdr-plugin.toml",
    "commands/herdres.md",
    "commands/herdres-setup.md",
    "commands/herdres-sync.md",
    "commands/herdres-status.md",
    "systemd/user/herdres.service",
    "systemd/user/herdres.timer",
    "systemd/user/herdres-gateway.service",
    "install-user.sh",
    "install-macos.sh",
    ".env.example",
]


def _make_checkout(root: Path) -> Path:
    """Materialize a minimal fake herdres checkout (install set + the build script)."""
    repo = root / "checkout"
    for rel in INSTALL_SET:
        path = repo / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        # Unique marker per file so we can prove the right bytes land in the tarball.
        path.write_text(f"# content of {rel}\n", encoding="utf-8")
    # The script resolves the repo root as its own parent dir, so it must live in
    # scripts/ inside the fake checkout.
    scripts_dir = repo / "scripts"
    scripts_dir.mkdir(parents=True, exist_ok=True)
    dest = scripts_dir / "build_release.sh"
    dest.write_bytes(BUILD_SCRIPT.read_bytes())
    dest.chmod(0o755)
    return repo


def _run_build(repo: Path, tag: str, out_dir: Path) -> subprocess.CompletedProcess:
    return subprocess.run(
        ["bash", str(repo / "scripts" / "build_release.sh"), tag, str(out_dir)],
        capture_output=True,
        text=True,
        check=False,
    )


def test_build_emits_named_assets(tmp_path: Path) -> None:
    repo = _make_checkout(tmp_path)
    out = tmp_path / "dist"
    tag = "v9.9.9"

    proc = _run_build(repo, tag, out)
    assert proc.returncode == 0, proc.stderr

    tarball = out / f"herdres-{tag}.tar.gz"
    checksum = out / f"herdres-{tag}.tar.gz.sha256"
    assert tarball.exists(), "tarball asset missing"
    assert checksum.exists(), "sha256 asset missing"


def test_tarball_contains_install_set_under_prefix(tmp_path: Path) -> None:
    repo = _make_checkout(tmp_path)
    out = tmp_path / "dist"
    tag = "v9.9.9"
    assert _run_build(repo, tag, out).returncode == 0

    tarball = out / f"herdres-{tag}.tar.gz"
    prefix = f"herdres-{tag}"
    with tarfile.open(tarball, "r:gz") as tar:
        members = tar.getnames()
        files = {m for m in members if tar.getmember(m).isfile()}

    expected = {f"{prefix}/{rel}" for rel in INSTALL_SET}
    assert expected <= files, f"missing from tarball: {expected - files}"
    # Every member is rooted under the single <name>/ prefix (extract == one dir).
    assert all(m == prefix or m.startswith(f"{prefix}/") for m in members), members


def test_env_config_never_packaged(tmp_path: Path) -> None:
    repo = _make_checkout(tmp_path)
    # Even if a stray herdres.env sits in the checkout, the build must never ship it.
    (repo / "herdres.env").write_text("TELEGRAM_BOT_TOKEN=secret\n", encoding="utf-8")
    out = tmp_path / "dist"
    tag = "v9.9.9"
    assert _run_build(repo, tag, out).returncode == 0

    with tarfile.open(out / f"herdres-{tag}.tar.gz", "r:gz") as tar:
        names = tar.getnames()
    assert not any(n.endswith("herdres.env") for n in names), names


def test_extracted_tree_carries_install_set_bytes(tmp_path: Path) -> None:
    """The extracted dir is a usable source checkout for _apply_install_set."""
    repo = _make_checkout(tmp_path)
    out = tmp_path / "dist"
    tag = "v9.9.9"
    assert _run_build(repo, tag, out).returncode == 0

    extract = tmp_path / "extract"
    with tarfile.open(out / f"herdres-{tag}.tar.gz", "r:gz") as tar:
        tar.extractall(extract, filter="data")
    root = extract / f"herdres-{tag}"
    for rel in INSTALL_SET:
        assert (root / rel).read_text(encoding="utf-8") == f"# content of {rel}\n"


def test_sha256_file_matches_tarball(tmp_path: Path) -> None:
    repo = _make_checkout(tmp_path)
    out = tmp_path / "dist"
    tag = "v9.9.9"
    assert _run_build(repo, tag, out).returncode == 0

    tarball = out / f"herdres-{tag}.tar.gz"
    checksum = out / f"herdres-{tag}.tar.gz.sha256"

    line = checksum.read_text(encoding="utf-8").strip()
    digest, _, fname = line.partition("  ")
    # sha256sum -c format: "<hexdigest>  <filename>" (two spaces), bare basename.
    assert fname == f"herdres-{tag}.tar.gz", repr(line)
    assert len(digest) == 64 and all(c in "0123456789abcdef" for c in digest), repr(digest)
    assert digest == hashlib.sha256(tarball.read_bytes()).hexdigest()


def test_sha256_verifies_with_sha256sum(tmp_path: Path) -> None:
    repo = _make_checkout(tmp_path)
    out = tmp_path / "dist"
    tag = "v9.9.9"
    assert _run_build(repo, tag, out).returncode == 0

    proc = subprocess.run(
        ["sha256sum", "-c", f"herdres-{tag}.tar.gz.sha256"],
        cwd=out,
        capture_output=True,
        text=True,
        check=False,
    )
    assert proc.returncode == 0, proc.stdout + proc.stderr


def test_missing_install_set_file_fails_closed(tmp_path: Path) -> None:
    repo = _make_checkout(tmp_path)
    (repo / "herdres_routing.py").unlink()
    out = tmp_path / "dist"

    proc = _run_build(repo, "v9.9.9", out)
    assert proc.returncode != 0
    assert "herdres_routing.py" in proc.stderr
    assert not (out / "herdres-v9.9.9.tar.gz").exists()


def test_requires_tag_argument(tmp_path: Path) -> None:
    repo = _make_checkout(tmp_path)
    proc = subprocess.run(
        ["bash", str(repo / "scripts" / "build_release.sh")],
        capture_output=True,
        text=True,
        check=False,
    )
    assert proc.returncode != 0
    assert "usage" in proc.stderr.lower()


def test_packaged_set_covers_live_update_plan(tmp_path: Path) -> None:
    """Guard against drift: every source the engine applies must ship in the tarball.

    Both INSTALL_SET (this test) and build_release.sh's ``files`` list mirror
    herdres.py:_update_files_plan() by hand. Import the *live* plan and prove the
    packaged set is a superset of it (+ that the bytes actually land), so adding a
    file to the plan without updating the build script fails here instead of silently
    shipping a stable release that ``_apply_install_set`` can't fully apply.
    """
    import sys

    sys.path.insert(0, str(REPO_ROOT))
    import herdres  # noqa: E402 - repo root injected above

    plan_srcs = {str(entry["src"]) for entry in herdres._update_files_plan()}
    # The build script (and this test's INSTALL_SET) must carry every engine source.
    assert plan_srcs <= set(INSTALL_SET), (
        "build_release.sh / INSTALL_SET out of sync with _update_files_plan(); "
        f"missing: {sorted(plan_srcs - set(INSTALL_SET))}"
    )

    repo = _make_checkout(tmp_path)
    out = tmp_path / "dist"
    tag = "v9.9.9"
    assert _run_build(repo, tag, out).returncode == 0
    with tarfile.open(out / f"herdres-{tag}.tar.gz", "r:gz") as tar:
        names = set(tar.getnames())
    for src in plan_srcs:
        assert f"herdres-{tag}/{src}" in names, f"engine source {src} missing from release tarball"
