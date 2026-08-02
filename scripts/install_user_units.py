#!/usr/bin/env python3
"""Safely install Herdres systemd user units without following symlinks."""

from __future__ import annotations

import argparse
import dataclasses
import datetime as dt
import os
import secrets
import stat
from collections.abc import Callable
from pathlib import Path


class InstallRefused(RuntimeError):
    pass


@dataclasses.dataclass(frozen=True)
class Snapshot:
    exists: bool
    data: bytes = b""
    device: int = 0
    inode: int = 0


@dataclasses.dataclass
class StagedWrite:
    directory_fd: int
    temporary_name: str
    destination_name: str


def _fail(message: str) -> None:
    raise InstallRefused(message)


def _open_directory_at(parent_fd: int, name: str, *, create: bool) -> int:
    flags = (
        os.O_RDONLY
        | getattr(os, "O_DIRECTORY", 0)
        | getattr(os, "O_CLOEXEC", 0)
        | getattr(os, "O_NOFOLLOW", 0)
    )
    try:
        return os.open(name, flags, dir_fd=parent_fd)
    except FileNotFoundError:
        if not create:
            raise
        try:
            os.mkdir(name, 0o755, dir_fd=parent_fd)
        except FileExistsError:
            pass
        try:
            return os.open(name, flags, dir_fd=parent_fd)
        except OSError as exc:
            _fail(f"refusing unsafe directory component {name!r}: {exc}")
    except OSError as exc:
        _fail(f"refusing unsafe directory component {name!r}: {exc}")


def _open_chain(root_fd: int, parts: tuple[str, ...], *, create: bool) -> list[int]:
    opened: list[int] = []
    parent_fd = root_fd
    try:
        for part in parts:
            descriptor = _open_directory_at(parent_fd, part, create=create)
            opened.append(descriptor)
            parent_fd = descriptor
    except Exception:
        for descriptor in reversed(opened):
            os.close(descriptor)
        raise
    return opened


def _open_home(home: Path, *, create: bool) -> int:
    flags = (
        os.O_RDONLY
        | getattr(os, "O_DIRECTORY", 0)
        | getattr(os, "O_CLOEXEC", 0)
        | getattr(os, "O_NOFOLLOW", 0)
    )
    try:
        return os.open(home, flags)
    except FileNotFoundError:
        if not create:
            raise
        home.mkdir(mode=0o700)
        return os.open(home, flags)
    except OSError as exc:
        _fail(f"refusing unsafe HOME {home}: {exc}")


def _snapshot_at(directory_fd: int, name: str, label: str) -> Snapshot:
    try:
        before = os.stat(name, dir_fd=directory_fd, follow_symlinks=False)
    except FileNotFoundError:
        return Snapshot(False)
    if stat.S_ISLNK(before.st_mode):
        try:
            target = os.readlink(name, dir_fd=directory_fd)
        except OSError:
            target = "<unreadable>"
        _fail(f"Refusing to refresh {label}: it is a symlink to {target}")
    if not stat.S_ISREG(before.st_mode):
        _fail(f"Refusing to refresh {label}: existing path is not a regular file")
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    descriptor = os.open(name, flags, dir_fd=directory_fd)
    try:
        opened = os.fstat(descriptor)
        chunks: list[bytes] = []
        while True:
            chunk = os.read(descriptor, 1024 * 1024)
            if not chunk:
                break
            chunks.append(chunk)
    finally:
        os.close(descriptor)
    after = os.stat(name, dir_fd=directory_fd, follow_symlinks=False)
    identity = (before.st_dev, before.st_ino)
    if (
        identity != (opened.st_dev, opened.st_ino)
        or identity != (after.st_dev, after.st_ino)
        or not stat.S_ISREG(after.st_mode)
    ):
        _fail(f"refusing {label}: path changed while it was inspected")
    return Snapshot(True, b"".join(chunks), before.st_dev, before.st_ino)


def _assert_snapshot(
    directory_fd: int, name: str, label: str, expected: Snapshot
) -> None:
    current = _snapshot_at(directory_fd, name, label)
    if current != expected:
        _fail(f"refusing {label}: path changed before commit")


def _stage_write(directory_fd: int, destination_name: str, data: bytes) -> StagedWrite:
    temporary_name = f".{destination_name}.tmp-{os.getpid()}-{secrets.token_hex(8)}"
    flags = (
        os.O_WRONLY
        | os.O_CREAT
        | os.O_EXCL
        | getattr(os, "O_CLOEXEC", 0)
        | getattr(os, "O_NOFOLLOW", 0)
    )
    descriptor = os.open(temporary_name, flags, 0o600, dir_fd=directory_fd)
    try:
        offset = 0
        while offset < len(data):
            offset += os.write(descriptor, data[offset:])
        os.fchmod(descriptor, 0o644)
        os.fsync(descriptor)
    except Exception:
        os.close(descriptor)
        os.unlink(temporary_name, dir_fd=directory_fd)
        raise
    else:
        os.close(descriptor)
    return StagedWrite(directory_fd, temporary_name, destination_name)


def _cleanup_staged(staged: list[StagedWrite]) -> None:
    for item in staged:
        try:
            os.unlink(item.temporary_name, dir_fd=item.directory_fd)
        except FileNotFoundError:
            pass


def _verify_chain(
    home: Path, home_fd: int, parts: tuple[str, ...], expected_fd: int
) -> None:
    home_stat = os.stat(home, follow_symlinks=False)
    held_home = os.fstat(home_fd)
    if not stat.S_ISDIR(home_stat.st_mode) or (
        home_stat.st_dev,
        home_stat.st_ino,
    ) != (held_home.st_dev, held_home.st_ino):
        _fail(f"refusing install: HOME changed before commit: {home}")
    opened = _open_chain(home_fd, parts, create=False)
    try:
        current = os.fstat(opened[-1])
        expected = os.fstat(expected_fd)
        if (current.st_dev, current.st_ino) != (expected.st_dev, expected.st_ino):
            _fail(
                f"refusing install: directory path changed before commit: {'/'.join(parts)}"
            )
    finally:
        for descriptor in reversed(opened):
            os.close(descriptor)


def _remove_tree_at(parent_fd: int, name: str) -> None:
    try:
        metadata = os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
    except FileNotFoundError:
        return
    if not stat.S_ISDIR(metadata.st_mode) or stat.S_ISLNK(metadata.st_mode):
        os.unlink(name, dir_fd=parent_fd)
        return
    descriptor = _open_directory_at(parent_fd, name, create=False)
    try:
        for child in os.listdir(descriptor):
            child_metadata = os.stat(child, dir_fd=descriptor, follow_symlinks=False)
            if stat.S_ISDIR(child_metadata.st_mode) and not stat.S_ISLNK(
                child_metadata.st_mode
            ):
                _remove_tree_at(descriptor, child)
            else:
                os.unlink(child, dir_fd=descriptor)
    finally:
        os.close(descriptor)
    os.rmdir(name, dir_fd=parent_fd)


def _read_required(path: Path) -> bytes:
    metadata = path.stat()
    if not stat.S_ISREG(metadata.st_mode):
        _fail(f"required installer source is not a regular file: {path}")
    return path.read_bytes()


def install_units(
    home: Path,
    source_root: Path,
    *,
    before_commit: Callable[[], None] | None = None,
) -> list[str]:
    home = home.absolute()
    source_root = source_root.absolute()
    templates = {
        "herdres.service": _read_required(source_root / "systemd/user/herdres.service"),
        "herdres-gateway.service": _read_required(
            source_root / "systemd/user/herdres-gateway.service"
        ),
        "tendwired.service": _read_required(
            source_root / "systemd/user/tendwired.service.example"
        ),
    }
    source_marker = f"{source_root}\n".encode()
    unit_labels = {
        name: str(home / ".config/systemd/user" / name) for name in templates
    }
    managed_label = str(home / ".local/share/herdres/tendwired.service.managed")
    source_label = str(home / ".local/share/herdres/source")

    # Phase one is read-only. Any refusal here occurs before the installer has
    # created or modified a byte under HOME.
    try:
        preflight_home_fd = _open_home(home, create=False)
    except FileNotFoundError:
        preflight_home_fd = None
    unit_snapshot = Snapshot(False)
    managed_snapshot = Snapshot(False)
    other_snapshots = {
        "herdres.service": Snapshot(False),
        "herdres-gateway.service": Snapshot(False),
    }
    source_snapshot = Snapshot(False)
    if preflight_home_fd is not None:
        try:
            try:
                unit_chain = _open_chain(
                    preflight_home_fd, (".config", "systemd", "user"), create=False
                )
            except FileNotFoundError:
                unit_chain = []
            if unit_chain:
                try:
                    unit_fd = unit_chain[-1]
                    unit_snapshot = _snapshot_at(
                        unit_fd,
                        "tendwired.service",
                        unit_labels["tendwired.service"],
                    )
                    for name in other_snapshots:
                        other_snapshots[name] = _snapshot_at(
                            unit_fd, name, unit_labels[name]
                        )
                finally:
                    for descriptor in reversed(unit_chain):
                        os.close(descriptor)
            try:
                managed_chain = _open_chain(
                    preflight_home_fd, (".local", "share", "herdres"), create=False
                )
            except FileNotFoundError:
                managed_chain = []
            if managed_chain:
                try:
                    managed_fd = managed_chain[-1]
                    managed_snapshot = _snapshot_at(
                        managed_fd,
                        "tendwired.service.managed",
                        managed_label,
                    )
                    source_snapshot = _snapshot_at(managed_fd, "source", source_label)
                finally:
                    for descriptor in reversed(managed_chain):
                        os.close(descriptor)
        finally:
            os.close(preflight_home_fd)

    tendwired_template = templates["tendwired.service"]
    if unit_snapshot.exists and unit_snapshot.data != tendwired_template:
        if not managed_snapshot.exists or managed_snapshot.data != unit_snapshot.data:
            _fail(
                "refusing tendwired.service: it differs from both the current and "
                "last installer-managed base unit; the active unit was left unchanged. "
                "Move host-specific Environment= values into an operator drop-in; an "
                "ExecStart= override must first clear ExecStart= in that drop-in."
            )

    home_fd = _open_home(home, create=True)
    unit_chain: list[int] = []
    managed_chain: list[int] = []
    staged: list[StagedWrite] = []
    messages: list[str] = []
    try:
        unit_chain = _open_chain(home_fd, (".config", "systemd", "user"), create=True)
        managed_chain = _open_chain(
            home_fd, (".local", "share", "herdres"), create=True
        )
        unit_fd = unit_chain[-1]
        managed_fd = managed_chain[-1]

        _assert_snapshot(
            unit_fd,
            "tendwired.service",
            unit_labels["tendwired.service"],
            unit_snapshot,
        )
        for name, snapshot in other_snapshots.items():
            _assert_snapshot(unit_fd, name, unit_labels[name], snapshot)
        _assert_snapshot(
            managed_fd,
            "tendwired.service.managed",
            managed_label,
            managed_snapshot,
        )
        _assert_snapshot(managed_fd, "source", source_label, source_snapshot)

        writes: list[tuple[int, str, bytes]] = []
        if unit_snapshot.data != tendwired_template:
            writes.append((unit_fd, "tendwired.service", tendwired_template))
        else:
            messages.append(
                "tendwired.service already matches the repository-managed base; left unchanged."
            )
        if managed_snapshot.data != tendwired_template:
            writes.append((managed_fd, "tendwired.service.managed", tendwired_template))
        for name in ("herdres.service", "herdres-gateway.service"):
            if other_snapshots[name].data != templates[name]:
                writes.append((unit_fd, name, templates[name]))
        if source_snapshot.data != source_marker:
            writes.append((managed_fd, "source", source_marker))

        backup_action: StagedWrite | None = None
        if unit_snapshot.exists and unit_snapshot.data != tendwired_template:
            timestamp = dt.datetime.now(dt.UTC).strftime("%Y%m%d%H%M%S")
            backup_name = f"tendwired.service.bak-{timestamp}"
            index = 0
            while True:
                try:
                    os.stat(backup_name, dir_fd=unit_fd, follow_symlinks=False)
                except FileNotFoundError:
                    break
                index += 1
                backup_name = f"tendwired.service.bak-{timestamp}-{index}"
            backup_action = _stage_write(unit_fd, backup_name, unit_snapshot.data)
            staged.append(backup_action)

        actions: list[StagedWrite] = []
        for directory_fd, destination_name, data in writes:
            action = _stage_write(directory_fd, destination_name, data)
            staged.append(action)
            actions.append(action)

        if before_commit is not None:
            before_commit()

        _verify_chain(home, home_fd, (".config", "systemd", "user"), unit_fd)
        _verify_chain(home, home_fd, (".local", "share", "herdres"), managed_fd)
        _assert_snapshot(
            unit_fd,
            "tendwired.service",
            unit_labels["tendwired.service"],
            unit_snapshot,
        )
        for name, snapshot in other_snapshots.items():
            _assert_snapshot(unit_fd, name, unit_labels[name], snapshot)
        _assert_snapshot(
            managed_fd,
            "tendwired.service.managed",
            managed_label,
            managed_snapshot,
        )
        _assert_snapshot(managed_fd, "source", source_label, source_snapshot)

        if backup_action is not None:
            os.rename(
                backup_action.temporary_name,
                backup_action.destination_name,
                src_dir_fd=unit_fd,
                dst_dir_fd=unit_fd,
            )
            staged.remove(backup_action)
            messages.append(
                f"Backed up existing tendwired.service to {backup_action.destination_name}."
            )
        for action in actions:
            os.rename(
                action.temporary_name,
                action.destination_name,
                src_dir_fd=action.directory_fd,
                dst_dir_fd=action.directory_fd,
            )
            staged.remove(action)

        for legacy_name in ("herdres.timer", "herdres-speech.service"):
            try:
                os.unlink(legacy_name, dir_fd=unit_fd)
            except FileNotFoundError:
                pass
        _remove_tree_at(unit_fd, "herdres-gateway.service.d")
        os.fsync(unit_fd)
        os.fsync(managed_fd)
    finally:
        _cleanup_staged(staged)
        for descriptor in reversed(managed_chain):
            os.close(descriptor)
        for descriptor in reversed(unit_chain):
            os.close(descriptor)
        os.close(home_fd)
    return messages


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--home", required=True, type=Path)
    parser.add_argument("--source-root", required=True, type=Path)
    args = parser.parse_args()
    try:
        messages = install_units(args.home, args.source_root)
    except (InstallRefused, OSError) as exc:
        parser.exit(1, f"{exc}\n")
    for message in messages:
        print(message)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
