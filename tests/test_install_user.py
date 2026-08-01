from __future__ import annotations

import os
import subprocess
from pathlib import Path


REPOSITORY = Path(__file__).resolve().parents[1]
HARDENING_DIRECTIVES = (
    "KillMode=control-group",
    "KillSignal=SIGTERM",
    "TimeoutStopSec=20",
    "FinalKillSignal=SIGKILL",
)


def _invoke_installer(home: Path) -> subprocess.CompletedProcess[str]:
    environment = dict(os.environ)
    environment["HOME"] = str(home)
    environment.pop("HERDRES_REQUEST_ID_KEY_PATH", None)
    return subprocess.run(
        ["sh", "install-user.sh"],
        cwd=REPOSITORY,
        env=environment,
        check=False,
        capture_output=True,
        text=True,
    )


def _run_installer(home: Path) -> subprocess.CompletedProcess[str]:
    result = _invoke_installer(home)
    assert result.returncode == 0, result.stderr
    return result


def _systemd_user_dir(home: Path) -> Path:
    return home / ".config/systemd/user"


def test_installer_rerun_preserves_existing_tendwired_dropins(
    tmp_path: Path,
) -> None:
    home = tmp_path / "home"
    _run_installer(home)
    dropins = _systemd_user_dir(home) / "tendwired.service.d"
    expected = {
        "kill-hardening.conf": "[Service]\nTimeoutStopSec=25\n",
        "maintenance.conf": "[Unit]\nOnFailure=notify.service\n",
        "socket-path.conf": "[Service]\nEnvironment=TENDWIRE_SOCKET=%t/tendwire.sock\n",
        "turn-model.conf": "[Service]\nEnvironment=TURN_MODEL=operator-choice\n",
    }
    dropins.mkdir()
    for name, content in expected.items():
        (dropins / name).write_text(content, encoding="utf-8")

    _run_installer(home)

    assert dropins.is_dir()
    assert {
        path.name: path.read_text(encoding="utf-8") for path in dropins.iterdir()
    } == expected


def test_installed_tendwired_unit_contains_stale_process_hardening(
    tmp_path: Path,
) -> None:
    home = tmp_path / "home"

    _run_installer(home)

    installed = (
        _systemd_user_dir(home) / "tendwired.service"
    ).read_text(encoding="utf-8")
    assert all(directive in installed for directive in HARDENING_DIRECTIVES)


def test_installer_backs_up_existing_tendwired_unit_before_refresh(
    tmp_path: Path,
) -> None:
    home = tmp_path / "home"
    unit_dir = _systemd_user_dir(home)
    unit_dir.mkdir(parents=True)
    unit = unit_dir / "tendwired.service"
    canonical_unit = (
        REPOSITORY / "systemd/user/tendwired.service.example"
    ).read_text(encoding="utf-8")
    unit.write_text(canonical_unit, encoding="utf-8")

    result = _run_installer(home)

    backups = list(unit_dir.glob("tendwired.service.bak-*"))
    assert len(backups) == 1
    assert backups[0].read_text(encoding="utf-8") == canonical_unit
    assert unit.read_text(encoding="utf-8") == canonical_unit
    assert (
        home / ".local/share/herdres/tendwired.service.managed"
    ).read_text(encoding="utf-8") == canonical_unit
    assert "Backed up existing tendwired.service" in result.stdout
    assert (
        "tendwired.service was installed/refreshed but was not enabled or started"
        in result.stdout
    )
    assert "systemctl --user cat tendwired.service" in result.stdout
    assert "enable --now tendwired.service" not in result.stdout


def test_installer_refuses_tendwired_unit_symlink_without_writing_target(
    tmp_path: Path,
) -> None:
    home = tmp_path / "home"
    unit_dir = _systemd_user_dir(home)
    unit_dir.mkdir(parents=True)
    external_target = tmp_path / "operator-managed-tendwired.service"
    external_content = "[Service]\nEnvironment=EXTERNAL_TARGET_MUST_SURVIVE=1\n"
    external_target.write_text(external_content, encoding="utf-8")
    unit = unit_dir / "tendwired.service"
    unit.symlink_to(external_target)

    result = _invoke_installer(home)

    assert result.returncode != 0
    assert unit.is_symlink()
    assert unit.resolve() == external_target
    assert external_target.read_text(encoding="utf-8") == external_content
    assert str(unit) in result.stderr
    assert str(external_target) in result.stderr
    assert "Refusing to refresh" in result.stderr


def test_installer_refuses_diverged_tendwired_unit_without_losing_pythonpath(
    tmp_path: Path,
) -> None:
    home = tmp_path / "home"
    unit_dir = _systemd_user_dir(home)
    unit_dir.mkdir(parents=True)
    unit = unit_dir / "tendwired.service"
    operator_unit = (
        "[Service]\n"
        "Environment=PYTHONPATH=/srv/operator/tendwire/src\n"
        "ExecStart=/usr/bin/python3 -m tendwire.cli daemon\n"
    )
    unit.write_text(operator_unit, encoding="utf-8")

    result = _invoke_installer(home)

    assert result.returncode != 0
    assert unit.read_text(encoding="utf-8") == operator_unit
    assert "PYTHONPATH=/srv/operator/tendwire/src" in unit.read_text(
        encoding="utf-8"
    )
    assert "PYTHONPATH=%h/tendwire/src" not in unit.read_text(encoding="utf-8")
    assert list(unit_dir.glob("tendwired.service.bak-*")) == []
    assert (
        "differs from both the current and last installer-managed"
        in result.stderr
    )
    assert "active unit was left unchanged" in result.stderr
    assert "operator drop-in" in result.stderr


def test_installer_refreshes_unchanged_previous_managed_tendwired_unit(
    tmp_path: Path,
) -> None:
    home = tmp_path / "home"
    unit_dir = _systemd_user_dir(home)
    unit_dir.mkdir(parents=True)
    managed_base = home / ".local/share/herdres/tendwired.service.managed"
    managed_base.parent.mkdir(parents=True)
    unit = unit_dir / "tendwired.service"
    previous_canonical = "[Service]\nKillMode=control-group\nVersion=previous\n"
    unit.write_text(previous_canonical, encoding="utf-8")
    managed_base.write_text(previous_canonical, encoding="utf-8")
    current_canonical = (
        REPOSITORY / "systemd/user/tendwired.service.example"
    ).read_text(encoding="utf-8")

    _run_installer(home)

    assert unit.read_text(encoding="utf-8") == current_canonical
    assert managed_base.read_text(encoding="utf-8") == current_canonical
    backups = list(unit_dir.glob("tendwired.service.bak-*"))
    assert len(backups) == 1
    assert backups[0].read_text(encoding="utf-8") == previous_canonical
