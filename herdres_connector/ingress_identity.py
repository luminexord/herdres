"""Private, stable identities for mutating Telegram ingress requests."""

from __future__ import annotations

import base64
import binascii
import hashlib
import hmac
import json
import os
import re
import stat
from pathlib import Path
from typing import Any

from . import config


_KEY_BYTES = 32
_REQUEST_ID_PREFIX = "hri1_"
_REQUEST_ID_RE = re.compile(r"hri1_[A-Za-z0-9_-]{43}\Z", re.ASCII)
_RECEIVER_ID_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,63}\Z", re.ASCII)
_HMAC_DOMAIN = b"herdres.telegram.ingress-request.v1\0"
_SIGNED_64 = (-(1 << 63), (1 << 63) - 1)
_UNSAFE_KEY = "Herdres request identity key is missing or unsafe"

__all__ = ["derive_telegram_request_id", "load_request_id_key", "validate_request_id"]


def _safe_key(metadata: os.stat_result) -> bool:
    return all(
        (
            stat.S_ISREG(metadata.st_mode),
            metadata.st_uid == os.geteuid(),
            stat.S_IMODE(metadata.st_mode) == 0o600,
            metadata.st_nlink == 1,
            metadata.st_size == _KEY_BYTES,
        )
    )


def _identity(metadata: os.stat_result) -> tuple[int, int]:
    return metadata.st_dev, metadata.st_ino


def load_request_id_key(path: Path | None = None) -> bytes:
    """Read the installed key without following or racing a replacement."""

    key_path = Path(path) if path is not None else config.request_id_key_path()
    key_path = key_path.expanduser()
    try:
        before = os.lstat(key_path)
        if not _safe_key(before):
            raise RuntimeError(_UNSAFE_KEY)
        flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
        descriptor = os.open(key_path, flags)
        try:
            opened = os.fstat(descriptor)
            if not _safe_key(opened) or _identity(before) != _identity(opened):
                raise RuntimeError(_UNSAFE_KEY)
            key = os.read(descriptor, _KEY_BYTES + 1)
            after = os.lstat(key_path)
            if not _safe_key(after) or _identity(opened) != _identity(after):
                raise RuntimeError(_UNSAFE_KEY)
        finally:
            os.close(descriptor)
    except RuntimeError:
        raise
    except (OSError, ValueError) as exc:
        raise RuntimeError(_UNSAFE_KEY) from exc
    if len(key) != _KEY_BYTES:
        raise RuntimeError(_UNSAFE_KEY)
    return key


def _coordinate(value: int, minimum: int, maximum: int) -> int:
    if type(value) is not int or not minimum <= value <= maximum:
        raise ValueError("invalid Telegram ingress coordinate")
    return value


def derive_telegram_request_id(
    key: bytes, *, receiver_id: str, update_id: int, chat_id: int, message_id: int
) -> str:
    """Derive the opaque ID from the receiver and stable Telegram coordinates."""

    if type(key) is not bytes or len(key) != _KEY_BYTES:
        raise ValueError("request identity key must be exactly 32 bytes")
    if not isinstance(receiver_id, str) or not _RECEIVER_ID_RE.fullmatch(receiver_id):
        raise ValueError("invalid Telegram receiver identity")
    canonical = json.dumps(
        [
            receiver_id,
            _coordinate(update_id, 0, _SIGNED_64[1]),
            _coordinate(chat_id, *_SIGNED_64),
            _coordinate(message_id, 1, _SIGNED_64[1]),
        ],
        ensure_ascii=True,
        separators=(",", ":"),
    ).encode("ascii")
    digest = hmac.new(key, _HMAC_DOMAIN + canonical, hashlib.sha256).digest()
    return _REQUEST_ID_PREFIX + base64.urlsafe_b64encode(digest).rstrip(b"=").decode()


def validate_request_id(value: Any) -> str:
    """Return the canonical request ID or reject it without normalization."""

    if not isinstance(value, str) or not _REQUEST_ID_RE.fullmatch(value):
        raise ValueError("invalid Herdres request ID")
    encoded = value[len(_REQUEST_ID_PREFIX) :]
    try:
        digest = base64.b64decode(encoded + "=", altchars=b"-_", validate=True)
    except (binascii.Error, ValueError) as exc:
        raise ValueError("invalid Herdres request ID") from exc
    canonical = base64.urlsafe_b64encode(digest).rstrip(b"=").decode()
    if len(digest) != 32 or not hmac.compare_digest(encoded, canonical):
        raise ValueError("invalid Herdres request ID")
    return value
