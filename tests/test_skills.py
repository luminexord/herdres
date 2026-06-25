"""Issue #27 (correct direction): surface a pane agent's Claude Code / Codex skills+commands as
tappable buttons inside its Telegram topic.

Covers:
  * enumerate_pane_skills: Claude user/project/plugin commands+skills, Codex prompts (reliable) vs
    Codex skills (best-effort), malformed/missing frontmatter fallback, dedup, unknown kind;
  * the /skills command branch: posts buttons, refuses while a decision prompt is active, empty +
    unsupported-kind notices, and stores an active_prompt that survives normalize_state;
  * a tapped skill button forwards its send_text to the pane via the existing callback path.
"""

from __future__ import annotations

import json
import tempfile
import unittest
import unittest.mock as mock
from pathlib import Path
from unittest.mock import Mock, patch

import herdres


def _write(p: Path, text: str = "x") -> None:
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(text, encoding="utf-8")


def _skill(dir_path: Path, dirname: str, *, name: str | None = None) -> None:
    body = ""
    if name is not None:
        body = f"---\nname: {name}\ndescription: d\n---\n\nbody\n"
    elif name == "":
        body = ""
    _write(dir_path / dirname / "SKILL.md", body or "no frontmatter here\n")


class EnumerateSkillsTests(unittest.TestCase):
    def _claude_home(self, home: Path) -> None:
        _write(home / ".claude" / "commands" / "foo.md")
        _write(home / ".claude" / "commands" / "bar.md")
        _skill(home / ".claude" / "skills", "myskill", name="my-skill")
        _skill(home / ".claude" / "skills", "nofm")            # no frontmatter -> dir name
        (home / ".claude" / "skills" / "notaskill").mkdir(parents=True)  # no SKILL.md -> skipped
        # plugins: one enabled, one disabled
        _write(home / ".claude" / "settings.json", json.dumps({
            "enabledPlugins": {"plug@mkt": True, "off@mkt": False},
        }))
        plug_path = home / "cache" / "plug"
        _write(home / ".claude" / "plugins" / "installed_plugins.json", json.dumps({
            "plugins": {"plug@mkt": [{"installPath": str(plug_path)}],
                        "off@mkt": [{"installPath": str(home / 'cache' / 'off')}]},
        }))
        _write(plug_path / "commands" / "pcmd.md")
        _skill(plug_path / "skills", "pskill", name="pskill")
        _write(home / "cache" / "off" / "commands" / "should_not_appear.md")

    def test_claude_user_and_plugin_no_cwd(self) -> None:
        with tempfile.TemporaryDirectory() as d:
            home = Path(d)
            self._claude_home(home)
            opts = herdres.enumerate_pane_skills("claude", home=home)
            by_send = {o["send_text"]: o for o in opts}
            self.assertIn("/foo", by_send)
            self.assertIn("/bar", by_send)
            self.assertIn("/my-skill", by_send)           # name from frontmatter
            self.assertIn("/nofm", by_send)               # fallback to dir name
            self.assertIn("/pcmd", by_send)               # enabled plugin command
            self.assertIn("/pskill", by_send)             # enabled plugin skill
            self.assertNotIn("/should_not_appear", by_send)  # disabled plugin excluded
            self.assertEqual(by_send["/pcmd"]["scope"], "plugin:plug")
            self.assertFalse(any(o["best_effort"] for o in opts))  # Claude is all reliable

    def test_claude_project_cwd_adds_project_skills(self) -> None:
        with tempfile.TemporaryDirectory() as d, tempfile.TemporaryDirectory() as proj:
            home = Path(d)
            self._claude_home(home)
            _write(Path(proj) / ".claude" / "commands" / "projcmd.md")
            _skill(Path(proj) / ".claude" / "skills", "projskill", name="proj-skill")
            opts = herdres.enumerate_pane_skills("claude", cwd=proj, home=home)
            by_send = {o["send_text"]: o for o in opts}
            self.assertEqual(by_send["/projcmd"]["scope"], "project")
            self.assertEqual(by_send["/proj-skill"]["scope"], "project")

    def test_codex_prompts_reliable_skills_best_effort(self) -> None:
        with tempfile.TemporaryDirectory() as d:
            home = Path(d)
            _write(home / ".codex" / "prompts" / "review.md")
            _skill(home / ".codex" / "skills", "writer", name="writer")
            opts = herdres.enumerate_pane_skills("codex", home=home)
            by_id = {o["id"]: o for o in opts}
            self.assertEqual(by_id["review"]["send_text"], "/review")      # prompt -> slash, reliable
            self.assertFalse(by_id["review"]["best_effort"])
            self.assertEqual(by_id["writer"]["send_text"], "Use the writer skill.")
            self.assertTrue(by_id["writer"]["best_effort"])

    def test_dedup_by_send_text(self) -> None:
        with tempfile.TemporaryDirectory() as d:
            home = Path(d)
            _write(home / ".claude" / "commands" / "dup.md")
            _skill(home / ".claude" / "skills", "dup", name="dup")  # also yields /dup
            opts = herdres.enumerate_pane_skills("claude", home=home)
            self.assertEqual(sum(1 for o in opts if o["send_text"] == "/dup"), 1)

    def test_unknown_kind_is_empty(self) -> None:
        with tempfile.TemporaryDirectory() as d:
            self.assertEqual(herdres.enumerate_pane_skills("kimi", home=Path(d)), [])

    def test_invalid_frontmatter_name_falls_back_to_dir(self) -> None:
        # `name: has space` is present but not a usable slug -> fall back to the dir name, don't drop.
        with tempfile.TemporaryDirectory() as d:
            home = Path(d)
            _write(home / ".claude" / "skills" / "good-dir" / "SKILL.md",
                   "---\nname: has space\n---\nbody\n")
            sends = [o["send_text"] for o in herdres.enumerate_pane_skills("claude", home=home)]
            self.assertIn("/good-dir", sends)

    def test_huge_skill_md_is_bounded_and_falls_back(self) -> None:
        # A 2 MB SKILL.md with no closing frontmatter must not drive the regex quadratic; only the
        # head is read, so the name is unparseable -> dir-name fallback, and it returns promptly.
        with tempfile.TemporaryDirectory() as d:
            home = Path(d)
            big = home / ".claude" / "skills" / "bigskill" / "SKILL.md"
            big.parent.mkdir(parents=True)
            big.write_text("---\n" + ("-" * 2_000_000), encoding="utf-8")
            sends = [o["send_text"] for o in herdres.enumerate_pane_skills("claude", home=home)]
            self.assertIn("/bigskill", sends)

    def test_missing_dirs_never_raise(self) -> None:
        with tempfile.TemporaryDirectory() as d:
            # No ~/.claude at all -> empty, no exception.
            self.assertEqual(herdres.enumerate_pane_skills("claude", home=Path(d) / "nope"), [])


# --- /skills command branch + tap ------------------------------------------------------------

def _state(agent: str = "claude") -> dict:
    return {
        "version": 1,
        "telegram": {"chat_id": "-1001", "general_thread_id": "1", "owner_user_ids": ["42"]},
        "spaces": {"sp": {"space_key": "sp", "topic_id": "77", "pane_keys": ["p1"], "message_routes": {}}},
        "panes": {"p1": {
            "pane_key": "p1", "pane_id": "p1", "agent": agent, "space_key": "sp",
            "topic_id": "77", "last_known_status": "working", "foreground_cwd": "/work",
        }},
    }


def _skills_payload(text: str = "/skills") -> dict:
    return {"chat_id": "-1001", "topic_id": "77", "user_id": "42", "text": text}


class SkillsCommandTests(unittest.TestCase):
    def _run(self, state, *, enum, feed=None):
        feed = feed or Mock(return_value={"ok": True, "message_id": "9001"})
        with patch.multiple(
            herdres,
            load_dotenv=Mock(),
            load_state=Mock(return_value=state),
            save_state=Mock(),
            send_feed_item=feed,
            send_notice=Mock(return_value={"ok": True, "message_id": "1"}),
            enumerate_pane_skills=Mock(return_value=enum),
        ):
            result = herdres.command_reply(_skills_payload())
        return result, feed

    def test_posts_buttons_and_stores_skills_prompt(self) -> None:
        state = _state("claude")
        enum = [
            {"id": "foo", "label": "/foo", "send_text": "/foo", "scope": "user", "kind": "command", "best_effort": False},
            {"id": "bar", "label": "/bar", "send_text": "/bar", "scope": "user", "kind": "skill", "best_effort": False},
        ]
        result, feed = self._run(state, enum=enum)
        self.assertEqual(result["reply"], "")
        feed.assert_called_once()
        markup = feed.call_args.kwargs["reply_markup"]
        # Two direct-choice ("c") skill buttons (+ the shared "Tell me differently" detail row).
        buttons = [b for row in markup["inline_keyboard"] for b in row]
        direct = [b for b in buttons if b["callback_data"].split(":")[1] == "c"]
        self.assertEqual(len(direct), 2)
        prompt = state["panes"]["p1"]["active_prompt"]
        self.assertEqual(prompt["source"], "skills")
        self.assertEqual(len(prompt["options"]), 2)
        self.assertEqual(prompt["options"][0]["send_text"], "/foo")
        # The stored prompt must survive normalize_state (not be treated as a disabled prompt).
        self.assertFalse(herdres.prompt_interaction_disabled(prompt))

    def test_empty_enumeration_notice(self) -> None:
        result, feed = self._run(_state("claude"), enum=[])
        self.assertIn("No skills", result["reply"])
        feed.assert_not_called()

    def test_unsupported_agent_kind(self) -> None:
        result, feed = self._run(_state("kimi"), enum=[{"id": "x", "label": "/x", "send_text": "/x",
                                                        "scope": "user", "kind": "command", "best_effort": False}])
        self.assertIn("Claude Code and Codex", result["reply"])
        feed.assert_not_called()

    def test_refuses_while_decision_prompt_active(self) -> None:
        state = _state("claude")
        state["panes"]["p1"]["active_prompt"] = {
            "id": "dec1", "text": "Approve?", "message_id": "5",
            "source": "pending_decision", "decision_id": "d1",
            "options": [{"number": "1", "callback_id": "1", "id": "1", "label": "Yes", "send_text": "yes"}],
        }
        result, feed = self._run(state, enum=[{"id": "foo", "label": "/foo", "send_text": "/foo",
                                               "scope": "user", "kind": "command", "best_effort": False}])
        self.assertIn("active question", result["reply"])
        feed.assert_not_called()

    def test_skills_prompt_survives_normalize_state(self) -> None:
        # End-to-end on the state object: a bound skills prompt must not be cleared by the
        # disabled-prompt sweep inside normalize_state.
        state = _state("claude")
        enum = [{"id": "foo", "label": "/foo", "send_text": "/foo", "scope": "user", "kind": "command", "best_effort": False}]
        self._run(state, enum=enum)
        normalized = herdres.normalize_state(state)
        self.assertIn("active_prompt", normalized["panes"]["p1"])
        self.assertEqual(normalized["panes"]["p1"]["active_prompt"]["source"], "skills")


class SkillsTapRoutingTests(unittest.TestCase):
    """End-to-end through the real /skills branch + callback path: a tap must run the *correct*
    command directly, even when the command name trips decision-prompt heuristics (refine/custom/…)
    or two long names share a 32-char prefix."""

    def _post_and_tap(self, enum, tap_index):
        state = _state("claude")
        feed = Mock(return_value={"ok": True, "message_id": "555"})
        with patch.multiple(
            herdres, load_dotenv=Mock(), load_state=Mock(return_value=state), save_state=Mock(),
            send_feed_item=feed, send_notice=Mock(return_value={"ok": True, "message_id": "1"}),
            enumerate_pane_skills=Mock(return_value=enum),
        ):
            herdres.command_reply(_skills_payload())
        markup = feed.call_args.kwargs["reply_markup"]
        cb = markup["inline_keyboard"][tap_index - 1][0]["callback_data"]
        self.assertEqual(cb.split(":")[1], "c")  # every skill button is a direct run
        send_to_pane = Mock(return_value=(True, ""))
        with patch.multiple(
            herdres, load_dotenv=Mock(), load_state=Mock(return_value=state), save_state=Mock(),
            telegram_api=Mock(return_value={"ok": True, "result": True}),
            send_notice=Mock(return_value={"ok": True, "message_id": "1"}), send_to_pane=send_to_pane,
        ):
            herdres.callback_reply({"chat_id": "-1001", "topic_id": "77", "user_id": "42",
                                    "message_id": "555", "data": cb})
        return send_to_pane

    def _cmd(self, name):
        return {"id": name, "label": f"/{name}", "send_text": f"/{name}",
                "scope": "user", "kind": "command", "best_effort": False}

    def test_keyword_named_skill_runs_directly(self) -> None:
        send_to_pane = self._post_and_tap([self._cmd("refine")], 1)
        send_to_pane.assert_called_once()
        self.assertEqual(send_to_pane.call_args.args[1], "/refine")

    def test_custom_named_skill_runs_directly(self) -> None:
        send_to_pane = self._post_and_tap([self._cmd("custom")], 1)
        send_to_pane.assert_called_once()
        self.assertEqual(send_to_pane.call_args.args[1], "/custom")

    def test_long_prefix_names_tap_runs_correct_one(self) -> None:
        a, b = "a" * 33 + "X", "a" * 33 + "Y"
        send_to_pane = self._post_and_tap([self._cmd(a), self._cmd(b)], 2)  # tap the SECOND
        send_to_pane.assert_called_once()
        self.assertEqual(send_to_pane.call_args.args[1], f"/{b}")  # not the first

    def test_tap_toast_shows_label_not_internal_token(self) -> None:
        # The success answer/notice must surface the command label, not the internal s{idx} token.
        state = _state("claude")
        feed = Mock(return_value={"ok": True, "message_id": "555"})
        with patch.multiple(
            herdres, load_dotenv=Mock(), load_state=Mock(return_value=state), save_state=Mock(),
            send_feed_item=feed, send_notice=Mock(return_value={"ok": True, "message_id": "1"}),
            enumerate_pane_skills=Mock(return_value=[self._cmd("deploy")]),
        ):
            herdres.command_reply(_skills_payload())
        cb = feed.call_args.kwargs["reply_markup"]["inline_keyboard"][0][0]["callback_data"]
        with patch.multiple(
            herdres, load_dotenv=Mock(), load_state=Mock(return_value=state), save_state=Mock(),
            telegram_api=Mock(return_value={"ok": True, "result": True}),
            send_notice=Mock(return_value={"ok": True, "message_id": "1"}),
            send_to_pane=Mock(return_value=(True, "")),
        ):
            result = herdres.callback_reply({"chat_id": "-1001", "topic_id": "77", "user_id": "42",
                                             "message_id": "555", "data": cb})
        self.assertIn("/deploy", result.get("answer", ""))
        self.assertNotIn("s1", result.get("answer", ""))


class SkillsChoicesRerenderTests(unittest.TestCase):
    def test_choices_rerenders_skills_with_dedicated_markup(self) -> None:
        # /choices on a stored skills picker must reuse the all-direct markup (no "Tell me
        # differently" custom row), not the decision-prompt markup.
        state = _state("claude")
        feed = Mock(return_value={"ok": True, "message_id": "600"})
        with patch.multiple(
            herdres, load_dotenv=Mock(), load_state=Mock(return_value=state), save_state=Mock(),
            send_feed_item=feed, send_notice=Mock(return_value={"ok": True, "message_id": "1"}),
            enumerate_pane_skills=Mock(return_value=[
                {"id": "foo", "label": "/foo", "send_text": "/foo", "scope": "user", "kind": "command", "best_effort": False},
            ]),
        ):
            herdres.command_reply(_skills_payload())          # post the skills picker
            feed.reset_mock()
            herdres.command_reply(_skills_payload("/choices"))  # re-surface it
        markup = feed.call_args.kwargs["reply_markup"]
        texts = [b["text"] for row in markup["inline_keyboard"] for b in row]
        self.assertEqual(texts, ["/foo"])                      # only the skill; no custom row
        self.assertNotIn("Tell me differently", texts)


class SkillsTapTests(unittest.TestCase):
    def test_tap_forwards_send_text_to_pane(self) -> None:
        # Build a skills active_prompt the way the /skills branch does, then tap a button: the
        # existing callback path must forward the option's send_text to the pane.
        state = _state("claude")
        callback_id = herdres._callback_id("foo", "1")
        prompt = {
            "id": "skp1", "text": "Tap a skill", "message_id": "555", "source": "skills",
            "created_at": herdres.utc_now(),  # else active_prompt_expired() rejects the tap
            "options": [{"number": callback_id, "callback_id": callback_id, "id": "foo",
                         "label": "/foo", "send_text": "/foo"}],
            "item": {"kind": "choices", "source": "skills"},
        }
        state["panes"]["p1"]["active_prompt"] = prompt
        send_to_pane = Mock(return_value=(True, ""))
        with patch.multiple(
            herdres,
            load_dotenv=Mock(),
            load_state=Mock(return_value=state),
            save_state=Mock(),
            telegram_api=Mock(return_value={"ok": True, "result": True}),
            send_notice=Mock(return_value={"ok": True, "message_id": "1"}),
            send_to_pane=send_to_pane,
        ):
            result = herdres.callback_reply({
                "chat_id": "-1001", "topic_id": "77", "user_id": "42", "message_id": "555",
                "data": f"herdr:c:skp1:{callback_id}",
            })
        send_to_pane.assert_called_once()
        self.assertEqual(send_to_pane.call_args.args[1], "/foo")
        self.assertIn("Selected", result.get("answer", ""))


if __name__ == "__main__":
    unittest.main()
