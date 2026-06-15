#!/usr/bin/env python3
"""Idempotently install the Herdr Telegram topic bridge into Hermes.

This script is intentionally small and text-anchor based. Hermes owns Telegram
polling, so the pane-topic bridge needs two call sites in the Telegram adapter
plus one bridge module importable from the gateway package. Running this before
Hermes starts makes the integration survive routine Hermes file updates.
"""

from __future__ import annotations

import argparse
import hashlib
import os
import shutil
import sys
from pathlib import Path


DEFAULT_HERMES_ROOT = Path("/home/smith/.hermes/hermes-agent")
DEFAULT_BRIDGE_SOURCE = Path("/home/smith/.local/share/herdr-telegram-topics/herdr_topic_bridge.py")

CALL_SNIPPET = "        if await self._maybe_handle_herdr_topic_message(msg):\n            return\n"
CALLBACK_CALL_SNIPPET = "        if await self._maybe_handle_herdr_topic_callback(query):\n            return\n"

METHOD_SNIPPET = '''    async def _maybe_handle_herdr_topic_message(self, message: Message) -> bool:
        """Route mapped Arasaka Herdr pane-topic commands outside the LLM path."""
        try:
            from gateway.herdr_topic_bridge import maybe_handle_herdr_topic_message
        except Exception:
            logger.debug("[%s] Herdr topic bridge unavailable", self.name, exc_info=True)
            return False
        try:
            return bool(await maybe_handle_herdr_topic_message(self, message))
        except Exception:
            logger.warning("[%s] Herdr topic bridge failed", self.name, exc_info=True)
            return False

'''

CALLBACK_METHOD_SNIPPET = '''    async def _maybe_handle_herdr_topic_callback(self, query: Any) -> bool:
        """Route mapped Arasaka Herdr pane-topic callbacks outside the LLM path."""
        try:
            from gateway.herdr_topic_bridge import maybe_handle_herdr_topic_callback
        except Exception:
            logger.debug("[%s] Herdr topic callback bridge unavailable", self.name, exc_info=True)
            return False
        try:
            return bool(await maybe_handle_herdr_topic_callback(self, query))
        except Exception:
            logger.warning("[%s] Herdr topic callback bridge failed", self.name, exc_info=True)
            return False

'''


class InstallError(RuntimeError):
    pass


def hermes_root() -> Path:
    return Path(os.getenv("HERDR_TELEGRAM_TOPICS_HERMES_ROOT", str(DEFAULT_HERMES_ROOT))).expanduser()


def bridge_source() -> Path:
    return Path(os.getenv("HERDR_TELEGRAM_TOPICS_BRIDGE_SOURCE", str(DEFAULT_BRIDGE_SOURCE))).expanduser()


def insert_after_in_function(text: str, function_name: str, anchor: str, snippet: str) -> tuple[str, bool]:
    func_marker = f"    async def {function_name}("
    start = text.find(func_marker)
    if start == -1:
        raise InstallError(f"could not find {function_name}")
    next_func = text.find("\n    async def ", start + len(func_marker))
    end = next_func if next_func != -1 else len(text)
    block = text[start:end]
    if snippet.strip() in block:
        return text, False
    anchor_at = block.find(anchor)
    if anchor_at == -1:
        raise InstallError(f"could not find insertion anchor in {function_name}")
    insert_at = start + anchor_at + len(anchor)
    return text[:insert_at] + snippet + text[insert_at:], True


def install_bridge_module(root: Path) -> bool:
    src = bridge_source()
    if not src.exists():
        raise InstallError(f"bridge source missing: {src}")
    dst = root / "gateway" / "herdr_topic_bridge.py"
    dst.parent.mkdir(parents=True, exist_ok=True)
    src_bytes = src.read_bytes()
    if dst.exists() and dst.read_bytes() == src_bytes:
        return False
    tmp = dst.with_suffix(dst.suffix + ".tmp")
    tmp.write_bytes(src_bytes)
    tmp.replace(dst)
    return True


def install_adapter_hook(root: Path) -> bool:
    telegram_py = root / "gateway" / "platforms" / "telegram.py"
    if not telegram_py.exists():
        raise InstallError(f"telegram adapter missing: {telegram_py}")
    text = telegram_py.read_text(encoding="utf-8")
    original = text
    text, _ = insert_after_in_function(
        text,
        "_handle_text_message",
        "        if not msg or not msg.text:\n            return\n",
        CALL_SNIPPET,
    )
    text, _ = insert_after_in_function(
        text,
        "_handle_command",
        "        if not msg or not msg.text:\n            return\n",
        CALL_SNIPPET,
    )
    text, _ = insert_after_in_function(
        text,
        "_handle_callback_query",
        "        data = query.data\n",
        CALLBACK_CALL_SNIPPET,
    )
    if "async def _maybe_handle_herdr_topic_message" not in text:
        anchor = "\n    async def _handle_location_message"
        anchor_at = text.find(anchor)
        if anchor_at == -1:
            raise InstallError("could not find _handle_location_message insertion anchor")
        text = text[: anchor_at + 1] + METHOD_SNIPPET + text[anchor_at + 1 :]
    if "async def _maybe_handle_herdr_topic_callback" not in text:
        anchor = "\n    async def _handle_location_message"
        anchor_at = text.find(anchor)
        if anchor_at == -1:
            raise InstallError("could not find _handle_location_message insertion anchor")
        text = text[: anchor_at + 1] + CALLBACK_METHOD_SNIPPET + text[anchor_at + 1 :]
    if text == original:
        return False
    try:
        compile(text, str(telegram_py), "exec")
    except SyntaxError as exc:
        raise InstallError(f"patched telegram adapter would not compile: {exc}") from exc
    digest = hashlib.sha256(original.encode("utf-8")).hexdigest()[:12]
    backup = telegram_py.with_name(f"{telegram_py.name}.herdr-topic.{digest}.bak")
    if not backup.exists():
        shutil.copy2(telegram_py, backup)
    tmp = telegram_py.with_suffix(telegram_py.suffix + ".tmp")
    tmp.write_text(text, encoding="utf-8")
    tmp.replace(telegram_py)
    return True


def install() -> dict[str, bool]:
    root = hermes_root()
    if not root.exists():
        raise InstallError(f"Hermes root missing: {root}")
    module_changed = install_bridge_module(root)
    adapter_changed = install_adapter_hook(root)
    return {"module_changed": module_changed, "adapter_changed": adapter_changed}


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--quiet", action="store_true")
    parser.add_argument("--strict", action="store_true", help="return nonzero if bridge install fails")
    args = parser.parse_args()
    try:
        result = install()
    except Exception as exc:
        print(f"herdr topic bridge install warning: {exc}", file=sys.stderr)
        return 1 if args.strict else 0
    if not args.quiet:
        print(result)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
