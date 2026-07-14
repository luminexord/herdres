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
_MIN_SIGNED_64 = -(1 << 63)
_MAX_SIGNED_64 = (1 << 63) - 1
_UNSAFE_KEY_MESSAGE = "Herdres request identity key is missing or unsafe"

__all__ = [
    "derive_telegram_request_id",
    "load_request_id_key",
    "validate_request_id",
]


def _key_stat_is_safe(metadata: os.stat_result) -> bool:
    return (
        stat.S_ISREG(metadata.st_mode)
        and metadata.st_uid == os.geteuid()
        and stat.S_IMODE(metadata.st_mode) == 0o600
        and metadata.st_size == _KEY_BYTES
    )


def _same_file(left: os.stat_result, right: os.stat_result) -> bool:
    return left.st_dev == right.st_dev and left.st_ino == right.st_ino


def load_request_id_key(path: Path | None = None) -> bytes:
    """Load the installed request-ID key without following or racing a replacement.

    Key creation belongs to the installer. Runtime callers never generate or repair
    identity state: absence, malformed contents, unsafe ownership/mode, symlinks,
    and a pathname replacement during the read all fail closed.
    """

    key_path = Path(path) if path is not None else config.request_id_key_path()
    key_path = key_path.expanduser()
    try:
        before = os.lstat(key_path)
        if not _key_stat_is_safe(before):
            raise RuntimeError(_UNSAFE_KEY_MESSAGE)

        flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
        descriptor = os.open(key_path, flags)
        try:
            opened = os.fstat(descriptor)
            if not _key_stat_is_safe(opened) or not _same_file(before, opened):
                raise RuntimeError(_UNSAFE_KEY_MESSAGE)
            key = os.read(descriptor, _KEY_BYTES + 1)
            after = os.lstat(key_path)
            if not _same_file(opened, after) or not _key_stat_is_safe(after):
                raise RuntimeError(_UNSAFE_KEY_MESSAGE)
        finally:
            os.close(descriptor)
    except RuntimeError:
        raise
    except (OSError, ValueError) as exc:
        raise RuntimeError(_UNSAFE_KEY_MESSAGE) from exc

    if len(key) != _KEY_BYTES:
        raise RuntimeError(_UNSAFE_KEY_MESSAGE)
    return key


def _coordinate(value: int, *, minimum: int, maximum: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or not minimum <= value <= maximum:
        raise ValueError("invalid Telegram ingress coordinate")
    return value


def derive_telegram_request_id(
    key: bytes,
    *,
    receiver_id: str,
    update_id: int,
    chat_id: int,
    message_id: int,
) -> str:
    """Derive the opaque request ID for one received Telegram message.

    Only the stable receiving-bot identity and Telegram update coordinates enter
    the MAC. Tokens, text, topic/reply metadata, and resolved Tendwire targets are
    deliberately absent.
    """

    if not isinstance(key, bytes) or len(key) != _KEY_BYTES:
        raise ValueError("request identity key must be exactly 32 bytes")
    if not isinstance(receiver_id, str) or _RECEIVER_ID_RE.fullmatch(receiver_id) is None:
        raise ValueError("invalid Telegram receiver identity")

    canonical = json.dumps(
        [
            receiver_id,
            _coordinate(update_id, minimum=0, maximum=_MAX_SIGNED_64),
            _coordinate(chat_id, minimum=_MIN_SIGNED_64, maximum=_MAX_SIGNED_64),
            _coordinate(message_id, minimum=1, maximum=_MAX_SIGNED_64),
        ],
        ensure_ascii=True,
        separators=(",", ":"),
    ).encode("ascii")
    digest = hmac.new(key, _HMAC_DOMAIN + canonical, hashlib.sha256).digest()
    encoded = base64.urlsafe_b64encode(digest).rstrip(b"=").decode("ascii")
    return _REQUEST_ID_PREFIX + encoded


def validate_request_id(value: Any) -> str:
    """Return a canonical Herdres request ID or reject it without normalization."""

    if not isinstance(value, str) or _REQUEST_ID_RE.fullmatch(value) is None:
        raise ValueError("invalid Herdres request ID")
    encoded = value[len(_REQUEST_ID_PREFIX) :]
    try:
        digest = base64.b64decode(encoded + "=", altchars=b"-_", validate=True)
    except (binascii.Error, ValueError) as exc:
        raise ValueError("invalid Herdres request ID") from exc
    if len(digest) != hashlib.sha256().digest_size:
        raise ValueError("invalid Herdres request ID")
    canonical = base64.urlsafe_b64encode(digest).rstrip(b"=").decode("ascii")
    if not hmac.compare_digest(encoded, canonical):
        raise ValueError("invalid Herdres request ID")
    return value
