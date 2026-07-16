#!/usr/bin/env python3
"""Herdr plugin actions for the source-only Herdres connector."""

from __future__ import annotations

import os
import shutil
import sys
from pathlib import Path


def _config_path() -> Path:
    configured = os.environ.get("HERDRES_ENV_FILE")
    if configured:
        return Path(configured).expanduser()
    plugin_config = os.environ.get("HERDR_PLUGIN_CONFIG_DIR")
    if not plugin_config:
        raise RuntimeError("HERDR_PLUGIN_CONFIG_DIR is unavailable")
    return Path(plugin_config) / "herdres.env"


def _init_config(project_root: Path) -> int:
    destination = _config_path()
    destination.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    destination.parent.chmod(0o700)
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_CLOEXEC", 0)
    try:
        descriptor = os.open(destination, flags, 0o600)
    except FileExistsError:
        print("Herdres plugin configuration already exists.")
        return 0
    source = project_root / ".env.example"
    try:
        with os.fdopen(descriptor, "wb") as destination_file, source.open(
            "rb"
        ) as source_file:
            shutil.copyfileobj(source_file, destination_file)
            destination_file.flush()
            os.fsync(destination_file.fileno())
    except BaseException:
        destination.unlink(missing_ok=True)
        raise
    print("Initialized private Herdres plugin configuration.")
    return 0


def main(argv: list[str] | None = None) -> int:
    args = list(sys.argv[1:] if argv is None else argv)
    if len(args) != 1 or args[0] not in {"init-config", "doctor"}:
        print("usage: herdr_plugin.py <init-config|doctor>", file=sys.stderr)
        return 2
    if sys.version_info < (3, 13):
        print("Herdres requires Python 3.13 or newer.", file=sys.stderr)
        return 1

    project_root = Path(__file__).resolve().parent.parent
    if args[0] == "init-config":
        return _init_config(project_root)

    os.environ.setdefault("HERDRES_ENV_FILE", str(_config_path()))
    sys.path.insert(0, str(project_root))
    from herdres import main as herdres_main

    return int(herdres_main(["doctor"]) or 0)


if __name__ == "__main__":
    raise SystemExit(main())
