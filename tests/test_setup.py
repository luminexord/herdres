"""Tests for the `herdres setup` interactive credential wizard.

The wizard is the *enforcing* credential gate (a SKILL.md instruction can only
bias an agent). These tests pin its load-bearing guarantees: it refuses to run
unattended without flags, never silently adopts the Hermes token, verifies via
preflight *before* writing, and writes herdres.env at mode 0o600 with the three
required keys.

All Telegram I/O (``herdres.telegram_api``), the no-echo token prompt
(``getpass.getpass``), and plain prompts (``builtins.input``) are mocked, so the
tests are offline and deterministic.
"""

from __future__ import annotations

import os
import stat
import unittest
from types import SimpleNamespace
from unittest.mock import patch

import herdres

# The real load_dotenv, captured before setUp patches the module global with a
# no-op. One test (the load_dotenv precedence test) re-installs it on purpose.
_REAL_LOAD_DOTENV = herdres.load_dotenv

# A token that satisfies SETUP_TOKEN_RE (^\d+:[A-Za-z0-9_-]{30,}$).
GOOD_TOKEN = "123456:" + "A" * 35
HERMES_TOKEN = "999999:" + "H" * 35
GOOD_CHAT_ID = "-1001234567890"
GOOD_USERS = "123456789"


def _preflight_ok(method, payload, **kwargs):
    """A telegram_api stub that makes preflight() pass for a forum supergroup."""
    if method == "getChat":
        return {"ok": True, "result": {"type": "supergroup", "is_forum": True}}
    if method == "getMe":
        return {"ok": True, "result": {"id": 42, "username": "herdres_bot"}}
    if method == "getChatMember":
        return {"ok": True, "result": {"status": "administrator", "can_manage_topics": True}}
    return {"ok": True, "result": True}


def _args(**over):
    base = {
        "bot_token": "",
        "chat_id": "",
        "allowed_users": "",
        "reuse_hermes_token": False,
    }
    base.update(over)
    return SimpleNamespace(**base)


class SetupWizardTests(unittest.TestCase):
    def setUp(self):
        tmp = self.enterContext(_TempDir())
        self.env_path = tmp / "herdres.env"
        self.hermes_path = tmp / "hermes.env"
        # Redirect both dotenv targets into the tmp dir and neutralize the real
        # load_dotenv (its default path is bound to the real ~/.config at import).
        self.enterContext(patch.object(herdres, "DEFAULT_ENV", self.env_path))
        self.enterContext(patch.object(herdres, "DEFAULT_HERMES_ENV", self.hermes_path))
        self.enterContext(patch.object(herdres, "load_dotenv", lambda *a, **k: None))
        # Keep the real env clean of any token between tests.
        keep = {
            k: os.environ.get(k)
            for k in (
                "TELEGRAM_BOT_TOKEN",
                "HERDRES_OUTBOUND_BOT_TOKEN",
                "HERDR_TELEGRAM_TOPICS_CHAT_ID",
            )
        }
        self.addCleanup(self._restore_env, keep)
        for k in keep:
            os.environ.pop(k, None)

    @staticmethod
    def _restore_env(keep):
        for k, v in keep.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v

    def _write_hermes(self, token):
        self.hermes_path.write_text(f"TELEGRAM_BOT_TOKEN={token}\n", encoding="utf-8")

    # --- refuses unattended without flags ------------------------------------

    def test_non_interactive_missing_flags_refuses_and_writes_nothing(self):
        with patch.object(herdres.sys.stdin, "isatty", return_value=False), \
             patch.object(herdres, "telegram_api", side_effect=AssertionError("no API in this path")):
            with self.assertRaises(herdres.BridgeError) as ctx:
                herdres.setup_once(_args())
        self.assertIn("interactive terminal", str(ctx.exception))
        self.assertFalse(self.env_path.exists(), "must not write env when refusing")

    # --- no silent reuse of the Hermes token ---------------------------------

    def test_hermes_token_not_reused_without_flag(self):
        self._write_hermes(HERMES_TOKEN)
        with patch.object(herdres.sys.stdin, "isatty", return_value=False), \
             patch.object(herdres, "telegram_api", side_effect=AssertionError("must not preflight")):
            with self.assertRaises(herdres.BridgeError) as ctx:
                herdres.setup_once(_args(
                    bot_token=HERMES_TOKEN, chat_id=GOOD_CHAT_ID, allowed_users=GOOD_USERS,
                ))
        self.assertIn("reuse the Hermes bot token", str(ctx.exception))
        self.assertFalse(self.env_path.exists())

    def test_hermes_token_reused_with_explicit_flag(self):
        self._write_hermes(HERMES_TOKEN)
        with patch.object(herdres.sys.stdin, "isatty", return_value=False), \
             patch.object(herdres, "telegram_api", side_effect=_preflight_ok):
            out = herdres.setup_once(_args(
                bot_token=HERMES_TOKEN, chat_id=GOOD_CHAT_ID, allowed_users=GOOD_USERS,
                reuse_hermes_token=True,
            ))
        self.assertTrue(out["ok"])
        self.assertTrue(out["result"]["reused_hermes_token"])
        self.assertEqual(self.env_path.read_text().count(HERMES_TOKEN), 1)

    def test_blank_token_with_reuse_flag_adopts_hermes_token(self):
        self._write_hermes(HERMES_TOKEN)
        with patch.object(herdres.sys.stdin, "isatty", return_value=False), \
             patch.object(herdres, "telegram_api", side_effect=_preflight_ok):
            out = herdres.setup_once(_args(
                chat_id=GOOD_CHAT_ID, allowed_users=GOOD_USERS, reuse_hermes_token=True,
            ))
        self.assertTrue(out["result"]["reused_hermes_token"])
        self.assertIn(f"TELEGRAM_BOT_TOKEN={HERMES_TOKEN}", self.env_path.read_text())

    # --- happy path: preflight BEFORE write, 0o600, three keys ---------------

    def test_happy_path_preflights_before_write_and_writes_0600(self):
        order = []

        def api(method, payload, **kwargs):
            order.append(("api", method))
            return _preflight_ok(method, payload, **kwargs)

        real_replace = os.replace

        def tracking_replace(src, dst):
            order.append(("write", str(dst)))
            return real_replace(src, dst)

        with patch.object(herdres.sys.stdin, "isatty", return_value=False), \
             patch.object(herdres, "telegram_api", side_effect=api), \
             patch.object(herdres.os, "replace", side_effect=tracking_replace):
            out = herdres.setup_once(_args(
                bot_token=GOOD_TOKEN, chat_id=GOOD_CHAT_ID, allowed_users=GOOD_USERS,
            ))

        self.assertTrue(out["ok"])
        self.assertEqual(out["result"]["preflight"], "ok")
        self.assertEqual(out["result"]["chat_id"], GOOD_CHAT_ID)
        # preflight (getChat/getMe/getChatMember) ran before the file was replaced.
        first_write = next(i for i, e in enumerate(order) if e[0] == "write")
        self.assertTrue(any(e == ("api", "getChat") for e in order[:first_write]))
        self.assertTrue(any(e == ("api", "getMe") for e in order[:first_write]))
        self.assertTrue(any(e == ("api", "getChatMember") for e in order[:first_write]))

        self.assertTrue(self.env_path.exists())
        mode = stat.S_IMODE(self.env_path.stat().st_mode)
        self.assertEqual(mode, 0o600, f"expected 0o600, got {oct(mode)}")
        text = self.env_path.read_text()
        self.assertIn(f"TELEGRAM_BOT_TOKEN={GOOD_TOKEN}", text)
        self.assertIn(f"HERDR_TELEGRAM_TOPICS_CHAT_ID={GOOD_CHAT_ID}", text)
        self.assertIn(f"TELEGRAM_ALLOWED_USERS={GOOD_USERS}", text)
        # token is never echoed back in the structured result
        self.assertNotIn(GOOD_TOKEN, str(out["result"]))

    def test_preflight_failure_is_surfaced_and_nothing_written(self):
        def api(method, payload, **kwargs):
            if method == "getChat":
                return {"ok": True, "result": {"type": "group", "is_forum": False}}
            return _preflight_ok(method, payload, **kwargs)

        with patch.object(herdres.sys.stdin, "isatty", return_value=False), \
             patch.object(herdres, "telegram_api", side_effect=api):
            with self.assertRaises(herdres.BridgeError) as ctx:
                herdres.setup_once(_args(
                    bot_token=GOOD_TOKEN, chat_id=GOOD_CHAT_ID, allowed_users=GOOD_USERS,
                ))
        self.assertIn("forum-enabled supergroup", str(ctx.exception))
        self.assertFalse(self.env_path.exists())

    def test_preserves_existing_non_target_keys(self):
        self.env_path.write_text(
            "# header\n"
            "HERDR_TELEGRAM_TOPICS_PER_AGENT=1\n"
            "TELEGRAM_BOT_TOKEN=old:tokenoldoldoldoldoldoldoldoldoldold\n"
            "HERDR_BIN=herdr\n",
            encoding="utf-8",
        )
        with patch.object(herdres.sys.stdin, "isatty", return_value=False), \
             patch.object(herdres, "telegram_api", side_effect=_preflight_ok):
            herdres.setup_once(_args(
                bot_token=GOOD_TOKEN, chat_id=GOOD_CHAT_ID, allowed_users=GOOD_USERS,
            ))
        text = self.env_path.read_text()
        self.assertIn("HERDR_TELEGRAM_TOPICS_PER_AGENT=1", text)
        self.assertIn("HERDR_BIN=herdr", text)
        self.assertIn("# header", text)
        self.assertIn(f"TELEGRAM_BOT_TOKEN={GOOD_TOKEN}", text)
        self.assertNotIn("old:tokenold", text, "old token must be replaced, not duplicated")
        # exactly one TELEGRAM_BOT_TOKEN line
        self.assertEqual(
            sum(1 for ln in text.splitlines() if ln.startswith("TELEGRAM_BOT_TOKEN=")), 1
        )

    def test_duplicate_target_key_lines_are_all_dropped(self):
        # A file with TWO stale TELEGRAM_BOT_TOKEN lines: both must be replaced by
        # a single new line, with no stale copy surviving on disk.
        self.env_path.write_text(
            "TELEGRAM_BOT_TOKEN=old1:tokenoldoldoldoldoldoldoldoldoldold\n"
            "HERDR_BIN=herdr\n"
            "TELEGRAM_BOT_TOKEN=old2:tokenoldoldoldoldoldoldoldoldoldold\n",
            encoding="utf-8",
        )
        with patch.object(herdres.sys.stdin, "isatty", return_value=False), \
             patch.object(herdres, "telegram_api", side_effect=_preflight_ok):
            herdres.setup_once(_args(
                bot_token=GOOD_TOKEN, chat_id=GOOD_CHAT_ID, allowed_users=GOOD_USERS,
            ))
        text = self.env_path.read_text()
        self.assertNotIn("old1:token", text)
        self.assertNotIn("old2:token", text, "second stale token line must not survive")
        self.assertIn("HERDR_BIN=herdr", text)
        self.assertEqual(
            sum(1 for ln in text.splitlines() if ln.startswith("TELEGRAM_BOT_TOKEN=")),
            1,
            "exactly one TELEGRAM_BOT_TOKEN line must remain",
        )
        self.assertIn(f"TELEGRAM_BOT_TOKEN={GOOD_TOKEN}", text)

    def test_allowed_users_with_spaces_is_accepted_and_normalized(self):
        # "123, 456" (spaces after commas) is valid and stored without spaces.
        with patch.object(herdres.sys.stdin, "isatty", return_value=False), \
             patch.object(herdres, "telegram_api", side_effect=_preflight_ok):
            out = herdres.setup_once(_args(
                bot_token=GOOD_TOKEN, chat_id=GOOD_CHAT_ID, allowed_users="123, 456",
            ))
        self.assertEqual(out["result"]["allowed_users"], "123,456")
        self.assertIn("TELEGRAM_ALLOWED_USERS=123,456", self.env_path.read_text())

    # --- real load_dotenv precedence -----------------------------------------

    def test_injected_token_survives_load_dotenv_during_preflight(self):
        # Fix #3: with the REAL load_dotenv (not mocked), a token already in
        # os.environ before preflight must NOT be clobbered by a stale on-disk
        # herdres.env/.hermes value — _load_dotenv_file's "key not in os.environ"
        # guard must hold, so preflight verifies the NEW token.
        stale = "111111:" + "S" * 35
        # On-disk files carry the STALE token for BOTH keys telegram_token() reads
        # (HERDRES_OUTBOUND_BOT_TOKEN is preferred, then TELEGRAM_BOT_TOKEN); the
        # resolved NEW token is GOOD_TOKEN. If the load_dotenv guard failed for
        # either key, the stale disk value would surface here.
        self.env_path.write_text(
            f"HERDRES_OUTBOUND_BOT_TOKEN={stale}\nTELEGRAM_BOT_TOKEN={stale}\n",
            encoding="utf-8",
        )
        self._write_hermes(stale)

        seen_tokens: list[str] = []

        def api(method, payload, **kwargs):
            # telegram_api is mocked, so resolve the token the real code path would
            # use (which calls the real load_dotenv) and record it.
            seen_tokens.append(herdres.telegram_token())
            return _preflight_ok(method, payload, **kwargs)

        # Restore the REAL load_dotenv for this test only (setUp neutralized it).
        with patch.object(herdres, "load_dotenv", _REAL_LOAD_DOTENV), \
             patch.object(herdres.sys.stdin, "isatty", return_value=False), \
             patch.object(herdres, "telegram_api", side_effect=api):
            out = herdres.setup_once(_args(
                bot_token=GOOD_TOKEN, chat_id=GOOD_CHAT_ID, allowed_users=GOOD_USERS,
            ))

        self.assertTrue(out["ok"])
        self.assertTrue(seen_tokens, "preflight should have called the Telegram API")
        # Every preflight call used the injected NEW token, never the stale disk one.
        self.assertTrue(all(tok == GOOD_TOKEN for tok in seen_tokens), seen_tokens)
        self.assertNotIn(stale, seen_tokens)

    # --- invalid non-interactive values --------------------------------------

    def test_invalid_token_non_interactive_raises(self):
        with patch.object(herdres.sys.stdin, "isatty", return_value=False), \
             patch.object(herdres, "telegram_api", side_effect=AssertionError("no preflight on bad input")):
            with self.assertRaises(herdres.BridgeError) as ctx:
                herdres.setup_once(_args(
                    bot_token="not-a-token", chat_id=GOOD_CHAT_ID, allowed_users=GOOD_USERS,
                ))
        self.assertIn("invalid Telegram bot token", str(ctx.exception))
        self.assertFalse(self.env_path.exists())

    def test_invalid_chat_id_non_interactive_raises(self):
        with patch.object(herdres.sys.stdin, "isatty", return_value=False), \
             patch.object(herdres, "telegram_api", side_effect=AssertionError("no preflight on bad input")):
            with self.assertRaises(herdres.BridgeError) as ctx:
                herdres.setup_once(_args(
                    bot_token=GOOD_TOKEN, chat_id="42", allowed_users=GOOD_USERS,
                ))
        self.assertIn("invalid", str(ctx.exception).lower())
        self.assertFalse(self.env_path.exists())

    def test_invalid_allowed_users_non_interactive_raises(self):
        with patch.object(herdres.sys.stdin, "isatty", return_value=False), \
             patch.object(herdres, "telegram_api", side_effect=AssertionError("no preflight on bad input")):
            with self.assertRaises(herdres.BridgeError):
                herdres.setup_once(_args(
                    bot_token=GOOD_TOKEN, chat_id=GOOD_CHAT_ID, allowed_users="not,users,123",
                ))
        self.assertFalse(self.env_path.exists())

    # --- interactive prompting -----------------------------------------------

    def test_interactive_prompts_validate_and_write(self):
        prompts = iter([GOOD_CHAT_ID, GOOD_USERS])
        with patch.object(herdres.sys.stdin, "isatty", return_value=True), \
             patch.object(herdres, "telegram_api", side_effect=_preflight_ok), \
             patch("getpass.getpass", return_value=GOOD_TOKEN), \
             patch("builtins.input", lambda *a, **k: next(prompts)):
            out = herdres.setup_once(_args())
        self.assertTrue(out["ok"])
        self.assertIn(f"HERDR_TELEGRAM_TOPICS_CHAT_ID={GOOD_CHAT_ID}", self.env_path.read_text())

    def test_interactive_reprompts_on_invalid_then_accepts(self):
        # First chat-id is invalid, second is good; users is good.
        chat_inputs = iter(["nope", GOOD_CHAT_ID, GOOD_USERS])
        with patch.object(herdres.sys.stdin, "isatty", return_value=True), \
             patch.object(herdres, "telegram_api", side_effect=_preflight_ok), \
             patch("getpass.getpass", return_value=GOOD_TOKEN), \
             patch("builtins.input", lambda *a, **k: next(chat_inputs)):
            out = herdres.setup_once(_args())
        self.assertTrue(out["ok"])

    def test_interactive_reuse_requires_typed_confirmation(self):
        self._write_hermes(HERMES_TOKEN)
        # token prompt returns the Hermes token; confirmation typed is 'no'.
        with patch.object(herdres.sys.stdin, "isatty", return_value=True), \
             patch.object(herdres, "telegram_api", side_effect=AssertionError("must not preflight")), \
             patch("getpass.getpass", return_value=HERMES_TOKEN), \
             patch("builtins.input", side_effect=[GOOD_CHAT_ID, GOOD_USERS, "no"]):
            with self.assertRaises(herdres.BridgeError) as ctx:
                herdres.setup_once(_args())
        self.assertIn("reuse the Hermes bot token", str(ctx.exception))
        self.assertFalse(self.env_path.exists())


class _TempDir:
    """Minimal context-managed temp dir yielding a Path (for enterContext)."""

    def __enter__(self):
        import tempfile
        from pathlib import Path

        self._tmp = tempfile.TemporaryDirectory()
        return Path(self._tmp.name)

    def __exit__(self, *exc):
        self._tmp.cleanup()
        return False


if __name__ == "__main__":
    unittest.main()
