from __future__ import annotations

import base64
import os
import stat
import subprocess
import sys
from pathlib import Path

import pytest

from herdres_connector import config, ingress_identity
from herdres_connector.ingress_identity import (
    derive_telegram_request_id,
    load_request_id_key,
    validate_request_id,
)


KEY = bytes(range(32))
COORDINATES = {
    "receiver_id": "manager",
    "update_id": 2_147_483_647,
    "chat_id": -1_001_234_567_890,
    "message_id": 4_242,
}
EXPECTED_REQUEST_ID = "hri1_SIodGeqCeIvApzpEvIaEM-L07UzUMgUFyeltRQxPpqU"


def _write_key(path: Path, value: bytes = KEY, mode: int = 0o600) -> None:
    path.write_bytes(value)
    path.chmod(mode)


def _event_request_id(event: dict[str, object]) -> str:
    return derive_telegram_request_id(
        KEY,
        receiver_id=str(event["receiver_id"]),
        update_id=int(event["update_id"]),
        chat_id=int(event["chat_id"]),
        message_id=int(event["message_id"]),
    )


def _installer_environment(
    home: Path, configured_path: str | None
) -> dict[str, str]:
    environment = dict(os.environ)
    environment["HOME"] = str(home)
    if configured_path is None:
        environment.pop("HERDRES_REQUEST_ID_KEY_PATH", None)
    else:
        environment["HERDRES_REQUEST_ID_KEY_PATH"] = configured_path
    return environment


def _run_installer(
    repository: Path, environment: dict[str, str]
) -> subprocess.CompletedProcess[bytes]:
    return subprocess.run(
        ["sh", "install-user.sh"],
        cwd=repository,
        env=environment,
        check=False,
        capture_output=True,
    )


def _runtime_request_key_path(
    repository: Path, environment: dict[str, str]
) -> subprocess.CompletedProcess[bytes]:
    return subprocess.run(
        [
            sys.executable,
            "-c",
            "from herdres_connector import config; print(config.request_id_key_path())",
        ],
        cwd=repository,
        env=environment,
        check=False,
        capture_output=True,
    )


def _assert_success(result: subprocess.CompletedProcess[bytes]) -> None:
    assert result.returncode == 0, result.stderr.decode(errors="replace")


def _assert_key_not_disclosed(
    result: subprocess.CompletedProcess[bytes], key: bytes
) -> None:
    output = result.stdout + result.stderr
    representations = (
        key,
        key.hex().encode("ascii"),
        base64.b64encode(key),
        base64.urlsafe_b64encode(key),
        base64.urlsafe_b64encode(key).rstrip(b"="),
    )
    assert all(representation not in output for representation in representations)


def test_fixed_full_hmac_sha256_vector_and_request_id_validation() -> None:
    request_id = derive_telegram_request_id(KEY, **COORDINATES)

    assert request_id == EXPECTED_REQUEST_ID
    assert len(request_id) == 5 + 43
    assert request_id.endswith("=") is False
    assert validate_request_id(request_id) == request_id


@pytest.mark.parametrize(
    ("field", "different"),
    [
        ("receiver_id", "codex"),
        ("update_id", COORDINATES["update_id"] + 1),
        ("chat_id", COORDINATES["chat_id"] + 1),
        ("message_id", COORDINATES["message_id"] + 1),
    ],
)
def test_every_stable_telegram_coordinate_distinguishes_requests(
    field: str, different: object
) -> None:
    changed = dict(COORDINATES)
    changed[field] = different

    assert derive_telegram_request_id(KEY, **changed) != EXPECTED_REQUEST_ID
    assert derive_telegram_request_id(KEY, **COORDINATES) == EXPECTED_REQUEST_ID


def test_redelivery_is_stable_and_token_rotation_or_transient_fields_do_not_enter_id() -> None:
    original = {
        **COORDINATES,
        "token": "123456:old-private-token",
        "text": "deploy alpha",
        "topic_id": 91,
        "reply_to_message_id": 40,
        "target": "private-pane-a",
    }
    redelivery_after_rotation = {
        **COORDINATES,
        "token": "123456:rotated-private-token",
        "text": "different representation",
        "topic_id": 92,
        "reply_to_message_id": 41,
        "target": "private-pane-b",
    }

    assert _event_request_id(original) == _event_request_id(original)
    assert _event_request_id(redelivery_after_rotation) == _event_request_id(original)


def test_request_id_validator_rejects_bad_shape_and_noncanonical_base64() -> None:
    invalid = [
        None,
        "",
        EXPECTED_REQUEST_ID + "=",
        " hri1_" + EXPECTED_REQUEST_ID[5:],
        "xri1_" + EXPECTED_REQUEST_ID[5:],
        EXPECTED_REQUEST_ID[:-1],
        EXPECTED_REQUEST_ID[:-1] + "V",  # same decoded bytes, non-zero pad bits
    ]

    for value in invalid:
        with pytest.raises(ValueError, match="invalid Herdres request ID"):
            validate_request_id(value)


def test_derivation_rejects_malformed_key_receiver_and_coordinates() -> None:
    with pytest.raises(ValueError):
        derive_telegram_request_id(b"short", **COORDINATES)
    with pytest.raises(ValueError):
        derive_telegram_request_id(KEY, **{**COORDINATES, "receiver_id": ""})
    with pytest.raises(ValueError):
        derive_telegram_request_id(
            KEY, **{**COORDINATES, "receiver_id": "x" * 65}
        )
    with pytest.raises(ValueError):
        derive_telegram_request_id(KEY, **{**COORDINATES, "update_id": True})
    with pytest.raises(ValueError):
        derive_telegram_request_id(KEY, **{**COORDINATES, "message_id": 0})


@pytest.mark.parametrize(
    "environment",
    [{}, {"HERDRES_REQUEST_ID_KEY_PATH": ""}],
    ids=["unset", "empty"],
)
def test_request_identity_config_uses_absolute_default(
    environment: dict[str, str],
) -> None:
    resolved = config.request_id_key_path(environment)

    assert resolved == config.DEFAULT_REQUEST_ID_KEY_PATH.expanduser()
    assert resolved.is_absolute()


def test_request_identity_config_expands_custom_home_path(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("HOME", str(tmp_path))

    resolved = config.request_id_key_path(
        {"HERDRES_REQUEST_ID_KEY_PATH": "~/private/request-id.key"}
    )

    assert resolved == tmp_path / "private/request-id.key"
    assert resolved.is_absolute()


@pytest.mark.parametrize(
    "configured_path",
    ["request-id.key", "./private/request-id.key", "   "],
)
def test_request_identity_config_rejects_relative_custom_path(
    configured_path: str,
) -> None:
    with pytest.raises(
        ValueError,
        match="HERDRES_REQUEST_ID_KEY_PATH must expand to a nonempty absolute path",
    ):
        config.request_id_key_path(
            {"HERDRES_REQUEST_ID_KEY_PATH": configured_path}
        )


def test_command_retry_horizon_bounds() -> None:
    assert config.command_retry_horizon_seconds({}) == 86_400
    assert config.command_retry_horizon_seconds(
        {"HERDRES_COMMAND_RETRY_HORIZON_SECONDS": ""}
    ) == 86_400
    assert config.command_retry_horizon_seconds(
        {"HERDRES_COMMAND_RETRY_HORIZON_SECONDS": "invalid"}
    ) == 86_400
    assert config.command_retry_horizon_seconds(
        {"HERDRES_COMMAND_RETRY_HORIZON_SECONDS": "59"}
    ) == 60
    assert config.command_retry_horizon_seconds(
        {"HERDRES_COMMAND_RETRY_HORIZON_SECONDS": "604801"}
    ) == 604_800


@pytest.mark.parametrize(
    ("environment", "expected"),
    [
        ({}, 172_800),
        ({"HERDRES_COMMAND_RETRY_HORIZON_SECONDS": "59"}, 86_460),
        ({"HERDRES_COMMAND_RETRY_HORIZON_SECONDS": "604801"}, 691_200),
    ],
)
def test_command_request_retention_strictly_exceeds_retry_and_update_windows(
    environment: dict[str, str], expected: int
) -> None:
    retry_horizon = config.command_retry_horizon_seconds(environment)
    retention = config.command_request_retention_seconds(environment)

    assert retention == expected
    assert retention > retry_horizon
    assert retention > 86_400


def test_key_loader_accepts_only_exact_owner_private_regular_file(tmp_path: Path) -> None:
    key_path = tmp_path / "request-id.key"
    _write_key(key_path)
    assert load_request_id_key(key_path) == KEY

    key_path.chmod(0o640)
    with pytest.raises(RuntimeError, match="missing or unsafe"):
        load_request_id_key(key_path)

    key_path.unlink()
    key_path.mkdir(mode=0o700)
    with pytest.raises(RuntimeError, match="missing or unsafe"):
        load_request_id_key(key_path)


@pytest.mark.parametrize("value", [b"", b"a" * 31, b"a" * 33])
def test_key_loader_rejects_missing_or_malformed_initialized_key(
    tmp_path: Path, value: bytes
) -> None:
    key_path = tmp_path / "request-id.key"
    if value:
        _write_key(key_path, value)

    with pytest.raises(RuntimeError, match="missing or unsafe"):
        load_request_id_key(key_path)


def test_key_loader_rejects_symlink(tmp_path: Path) -> None:
    real_key = tmp_path / "real.key"
    linked_key = tmp_path / "request-id.key"
    _write_key(real_key)
    linked_key.symlink_to(real_key)

    with pytest.raises(RuntimeError, match="missing or unsafe"):
        load_request_id_key(linked_key)


def test_key_loader_detects_atomic_path_replacement_without_a_sleep(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    key_path = tmp_path / "request-id.key"
    replacement = tmp_path / "replacement.key"
    _write_key(key_path)
    _write_key(replacement, bytes(reversed(KEY)))
    real_open = ingress_identity.os.open
    replaced = False

    def replacing_open(path: object, flags: int) -> int:
        nonlocal replaced
        if not replaced and Path(path) == key_path:
            replaced = True
            os.replace(replacement, key_path)
        return real_open(path, flags)

    monkeypatch.setattr(ingress_identity.os, "open", replacing_open)

    with pytest.raises(RuntimeError, match="missing or unsafe"):
        load_request_id_key(key_path)
    assert replaced is True


@pytest.mark.parametrize(
    "configured_path",
    [None, ""],
    ids=["unset", "empty"],
)
def test_installer_and_runtime_use_same_default_and_preserve_private_key(
    tmp_path: Path,
    configured_path: str | None,
) -> None:
    repository = Path(__file__).resolve().parents[1]
    home = tmp_path / "home"
    environment = _installer_environment(home, configured_path)
    expected_path = home / ".local/share/herdres/request-id.key"

    runtime = _runtime_request_key_path(repository, environment)
    _assert_success(runtime)
    assert Path(runtime.stdout.decode().strip()) == expected_path

    first = _run_installer(repository, environment)
    _assert_success(first)

    original = expected_path.read_bytes()
    assert len(original) == 32
    assert stat.S_IMODE(expected_path.stat().st_mode) == 0o600
    assert stat.S_IMODE(expected_path.parent.stat().st_mode) == 0o700
    assert list(expected_path.parent.glob(".request-id.key.*")) == []
    _assert_key_not_disclosed(first, original)

    second = _run_installer(repository, environment)
    _assert_success(second)
    assert expected_path.read_bytes() == original
    assert list(expected_path.parent.glob(".request-id.key.*")) == []
    _assert_key_not_disclosed(second, original)


def test_installer_comments_legacy_lane_rollback_with_consent_marker(
    tmp_path: Path,
) -> None:
    repository = Path(__file__).resolve().parents[1]
    home = tmp_path / "home"
    env_path = home / ".config/herdres/herdres.env"
    env_path.parent.mkdir(parents=True)
    env_path.write_text(
        "TELEGRAM_BOT_TOKEN=private\nHERDRES_INBOUND_LANES=0\n",
        encoding="utf-8",
    )
    env_path.chmod(0o600)
    environment = _installer_environment(home, None)

    first = _run_installer(repository, environment)
    _assert_success(first)
    migrated = env_path.read_text(encoding="utf-8")
    assert (
        "approved by running install-user.sh" in migrated
    )
    assert "\n# HERDRES_INBOUND_LANES=0\n" in migrated
    assert "\nHERDRES_INBOUND_LANES=0\n" not in migrated
    assert stat.S_IMODE(env_path.stat().st_mode) == 0o600

    second = _run_installer(repository, environment)
    _assert_success(second)
    assert env_path.read_text(encoding="utf-8") == migrated


@pytest.mark.parametrize("use_home_expansion", [False, True])
def test_installer_and_runtime_expand_same_absolute_custom_path_without_rotation(
    tmp_path: Path,
    use_home_expansion: bool,
) -> None:
    repository = Path(__file__).resolve().parents[1]
    home = tmp_path / "home"
    expected_path = (
        home / "private/request-id.key"
        if use_home_expansion
        else tmp_path / "custom/request-id.key"
    )
    configured_path = (
        "~/private/request-id.key"
        if use_home_expansion
        else str(expected_path)
    )
    environment = _installer_environment(home, configured_path)

    runtime = _runtime_request_key_path(repository, environment)
    _assert_success(runtime)
    assert Path(runtime.stdout.decode().strip()) == expected_path

    first = _run_installer(repository, environment)
    _assert_success(first)
    original = expected_path.read_bytes()
    assert len(original) == 32
    assert stat.S_IMODE(expected_path.stat().st_mode) == 0o600
    assert stat.S_IMODE(expected_path.parent.stat().st_mode) == 0o700
    assert not (home / ".local/share/herdres/request-id.key").exists()
    assert list(expected_path.parent.glob(".request-id.key.*")) == []
    _assert_key_not_disclosed(first, original)

    second = _run_installer(repository, environment)
    _assert_success(second)
    assert expected_path.read_bytes() == original
    assert list(expected_path.parent.glob(".request-id.key.*")) == []
    _assert_key_not_disclosed(second, original)


def test_installer_and_runtime_both_reject_relative_custom_path_before_creation(
    tmp_path: Path,
) -> None:
    repository = Path(__file__).resolve().parents[1]
    home = tmp_path / "home"
    rejected_path = tmp_path / "must-not-exist/request-id.key"
    configured_path = os.path.relpath(rejected_path, repository)
    environment = _installer_environment(home, configured_path)
    expected_error = (
        b"HERDRES_REQUEST_ID_KEY_PATH must expand to a nonempty absolute path"
    )

    runtime = _runtime_request_key_path(repository, environment)
    assert runtime.returncode != 0
    assert expected_error in runtime.stderr

    installer = _run_installer(repository, environment)
    assert installer.returncode != 0
    assert expected_error in installer.stderr
    assert not rejected_path.exists()
    assert not (home / ".local/share/herdres/request-id.key").exists()
