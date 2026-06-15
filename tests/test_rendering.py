import importlib.util
import unittest
from pathlib import Path
from unittest.mock import Mock, patch


MODULE_PATH = Path(__file__).resolve().parents[1] / "herdres.py"
SPEC = importlib.util.spec_from_file_location("herdres", MODULE_PATH)
herdres = importlib.util.module_from_spec(SPEC)
assert SPEC and SPEC.loader
SPEC.loader.exec_module(herdres)


class RenderingTests(unittest.TestCase):
    def test_preserves_report_structure_for_telegram_rich_html(self) -> None:
        sample = """\u2022 Fixed. The renderer was incorrectly turning every line
into a bullet list and only capturing the tail of the
report.

Changes made in .local/bin/herdr_telegram_topics.py:520:

- Preserves paragraphs as paragraphs.
- Converts only real markdown bullets into rich lists,
  stripping the -.
- Detects section labels like What changed: / Gitmoot
  review: as headings.

Verified with:

- python3 -m py_compile
- Renderer smoke test using text shaped like the bad
  Telegram output
- Dry-run sendRichMessage probe
"""

        lines = herdres.clean_feed_lines(sample)
        self.assertIn("", lines)
        self.assertTrue(any(line.startswith("- Preserves") for line in lines))

        item = herdres.make_feed_item("report", "Report", sample, notify=False)
        html = herdres.render_feed_item_html(item)

        self.assertIn("<h3>Report</h3>", html)
        self.assertIn("<h4>Changes made</h4>", html)
        self.assertIn("<code>.local/bin/herdr_telegram_topics.py:520</code>", html)
        self.assertIn("<h4>Verified with</h4>", html)
        self.assertIn("<ul>", html)
        self.assertGreaterEqual(html.count("<li>"), 5)
        self.assertIn("Converts only real markdown bullets into rich lists, stripping the -.", html)
        self.assertIn("Renderer smoke test using text shaped like the bad Telegram output", html)
        self.assertNotIn("Preserves paragraphs as paragraphs. Converts only", html)

    def test_report_html_escapes_hostile_content_and_renders_code(self) -> None:
        sample = """Report

Verified with:

python3 -m py_compile

- <b>bold</b>
- <script>alert(1)</script>
"""

        item = herdres.make_feed_item("report", "Report", sample, notify=False)
        html = herdres.render_feed_item_html(item)

        self.assertIn("<pre><code>python3 -m py_compile</code></pre>", html)
        self.assertIn("&lt;b&gt;bold&lt;/b&gt;", html)
        self.assertIn("&lt;script&gt;alert(1)&lt;/script&gt;", html)
        self.assertNotIn("<b>bold</b>", html)
        self.assertNotIn("<script>alert(1)</script>", html)

    def test_cleaner_drops_visible_tui_chrome_and_composer(self) -> None:
        sample = """Report

Fixed the parser leak.

• Bash(cd /workspace/project && git status)
  cd /workspace/project
└ started task-069 in the background
job: local-ask-example
state: running
repo: gaijinjoe/example
branch: main
Tip: Use /btw to ask a quick side question without interrupting Claude's current work
side question without interrupting Claude's current work
❯
"""

        lines = herdres.clean_feed_lines(sample)
        text = "\n".join(lines)

        self.assertIn("Fixed the parser leak.", text)
        self.assertNotIn("Bash(", text)
        self.assertNotIn("started task-069", text)
        self.assertNotIn("Tip: Use /btw", text)
        self.assertNotIn("side question without interrupting", text)
        self.assertNotIn("❯", text)

    def test_cleaner_drops_prompt_with_typed_user_input(self) -> None:
        sample = """What changed:

- Completed the visible work.

❯ this is still being typed by the owner
more typed input that should not post
"""

        lines = herdres.clean_feed_lines(sample)
        text = "\n".join(lines)

        self.assertIn("Completed the visible work.", text)
        self.assertNotIn("being typed by the owner", text)
        self.assertNotIn("more typed input", text)

    def test_btw_tip_does_not_create_question_card(self) -> None:
        raw = """• Bash(cd /workspace/project && git status)
  cd /workspace/project
Tip: Use /btw to ask a quick side question without interrupting Claude's current work
side question without interrupting Claude's current work
❯
"""

        item = herdres.extract_clean_feed_item({"agent_status": "working"}, {}, raw)

        self.assertIsNone(item)

    def test_non_action_question_is_suppressed(self) -> None:
        raw = """This is just transcript chatter.

Why did the logs look like that?
"""

        item = herdres.extract_clean_feed_item({"agent_status": "working"}, {}, raw)

        self.assertIsNone(item)

    def test_real_question_heading_still_creates_question_card(self) -> None:
        raw = """Question

Would you like me to deploy this now?
"""

        item = herdres.extract_clean_feed_item({"agent_status": "blocked"}, {}, raw)

        self.assertIsNotNone(item)
        assert item is not None
        self.assertEqual(item["kind"], "question")
        self.assertIn("Would you like me to deploy this now?", item["text"])

    def test_report_state_lines_are_not_global_noise(self) -> None:
        sample = """Report

State: passing
Branch: main
Repo: gaijinjoe/example
"""

        lines = herdres.clean_feed_lines(sample)
        text = "\n".join(lines)

        self.assertIn("State: passing", text)
        self.assertIn("Branch: main", text)
        self.assertIn("Repo: gaijinjoe/example", text)

    def test_long_report_keeps_beginning(self) -> None:
        body = "\n".join(f"Line {idx:02d}: fixed report detail." for idx in range(1, 56))
        raw = f"""What changed:

Fixed the long report cutoff.

{body}
"""

        item = herdres.extract_clean_feed_item({"agent_status": "done"}, {}, raw, allow_unbounded_reports=True)
        self.assertIsNotNone(item)
        assert item is not None

        html = herdres.render_feed_item_html(item)

        self.assertIn("Fixed the long report cutoff.", html)
        self.assertIn("Line 01: fixed report detail.", html)
        self.assertIn("Line 55: fixed report detail.", html)

    def test_bounded_report_allows_arbitrary_title(self) -> None:
        raw = """noise before

HERDRES_REPORT_START
Deployment

- Shipped the topic bridge.

Verification:

- tests pass
HERDRES_REPORT_END

noise after
"""

        item = herdres.extract_clean_feed_item({"agent_status": "done"}, {}, raw, allow_unbounded_reports=True)

        self.assertIsNotNone(item)
        assert item is not None
        text = herdres.item_plain_text(item)
        self.assertEqual(item["title"], "Deployment")
        self.assertIn("Shipped the topic bridge.", text)
        self.assertNotIn("noise before", text)
        self.assertNotIn("noise after", text)

    def test_bounded_report_rejects_bullet_as_title(self) -> None:
        raw = """HERDRES_REPORT_START
- first non-empty line becomes the title, so it can be Deployment,
Result, Flight Recorder, etc.
-
HERDRES_REPORT_END
"""

        item = herdres.extract_clean_feed_item({"agent_status": "done"}, {}, raw, allow_unbounded_reports=True)

        self.assertIsNone(item)

    def test_bounded_report_explicit_title_keeps_bullet_body(self) -> None:
        raw = """HERDRES_REPORT_START
HERDRES_REPORT_TITLE: Deployment
- First body bullet stays in the body.
- Second body bullet stays in the body.
Verification:
- tests pass
HERDRES_REPORT_END
"""

        item = herdres.extract_clean_feed_item({"agent_status": "done"}, {}, raw, allow_unbounded_reports=True)

        self.assertIsNotNone(item)
        assert item is not None
        text = herdres.item_plain_text(item)
        self.assertEqual(item["title"], "Deployment")
        self.assertIn("First body bullet", text)
        self.assertIn("Second body bullet", text)
        self.assertNotIn("HERDRES_REPORT_TITLE", text)

    def test_report_markers_must_be_standalone_lines(self) -> None:
        raw = "Example: HERDRES_REPORT_START this should not start a report HERDRES_REPORT_END"

        item = herdres.extract_clean_feed_item({"agent_status": "done"}, {}, raw, allow_unbounded_reports=True)

        self.assertIsNone(item)

    def test_lone_dash_does_not_render_as_code_block(self) -> None:
        html, _ = herdres._rich_structured_block(["-"], max_chars=1000, max_lines=10)

        self.assertNotIn("<pre>", html)
        self.assertNotIn("<code>", html)

    def test_done_report_with_numbered_list_stays_report(self) -> None:
        raw = """HERDRES_REPORT_START
HERDRES_REPORT_TITLE: Deployment
What changed:
1. Added cache.
2. Restarted timer.
Verification:
1. Tests pass.
2. Timer run succeeded.
HERDRES_REPORT_END
"""

        item = herdres.extract_clean_feed_item({"agent_status": "done"}, {}, raw, allow_unbounded_reports=True)

        self.assertIsNotNone(item)
        assert item is not None
        self.assertEqual(item["kind"], "report")
        self.assertEqual(item["title"], "Deployment")
        self.assertIn("Added cache", herdres.item_plain_text(item))

    def test_bounded_sprint_status_renders_table_checklist_details(self) -> None:
        raw = """HERDRES_REPORT_START
HERDRES_REPORT_TITLE: Sprint Status

SUMMARY:
Driver App release is done, Portal QA is in progress, Route Optimizer is blocked.

TABLE:
Task | Owner | Status
Driver App release | Alex | Done
Portal QA | Sam | In progress
Route optimizer | Luke | Blocked

CHECKLIST:
[x] Review PR
[ ] Run staging smoke test

DETAILS: Risks
- Route Optimizer dependency is blocking release.

FOOTER:
Sprint - Smith - 10:58
HERDRES_REPORT_END
"""

        item = herdres.extract_clean_feed_item({"agent_status": "done"}, {}, raw, allow_unbounded_reports=True)

        self.assertIsNotNone(item)
        assert item is not None
        html = herdres.render_feed_item_html(item)
        self.assertIn("<h3>Sprint Status</h3>", html)
        self.assertIn("<table>", html)
        self.assertIn("<th>Task</th>", html)
        self.assertIn("<td>Alex</td>", html)
        self.assertIn('<input type="checkbox" checked>', html)
        self.assertIn('<input type="checkbox">', html)
        self.assertIn("<details><summary>Risks</summary>", html)
        self.assertIn("<footer>Sprint - Smith - 10:58</footer>", html)

    def test_structured_sections_require_colon(self) -> None:
        raw = """HERDRES_REPORT_START
HERDRES_REPORT_TITLE: Deploy Notes
Summary of the deploy is below.
Tables are useful when they are intentional.
- Normal bullet.
HERDRES_REPORT_END
"""

        item = herdres.extract_clean_feed_item({"agent_status": "done"}, {}, raw)

        self.assertIsNotNone(item)
        assert item is not None
        html = herdres.render_feed_item_html(item)
        self.assertIn("Summary of the deploy is below.", html)
        self.assertIn("Tables are useful", html)
        self.assertNotIn("<table>", html)
        self.assertNotIn("<b>of the deploy is below.:</b>", html)

    def test_done_heading_report_with_numbered_list_stays_report(self) -> None:
        raw = """What changed:
1. Added cache.
2. Restarted timer.
Verification:
1. Tests pass.
2. Timer run succeeded.
"""

        item = herdres.extract_clean_feed_item({"agent_status": "done"}, {}, raw, allow_unbounded_reports=True)

        self.assertIsNotNone(item)
        assert item is not None
        self.assertEqual(item["kind"], "report")
        self.assertEqual(item["title"], "What changed")
        self.assertIn("Added cache", herdres.item_plain_text(item))

    def test_bounded_report_preserves_ascii_blockquote_lines(self) -> None:
        raw = """HERDRES_REPORT_START
Investigation

> quoted finding should stay

- Normal bullet should stay.
HERDRES_REPORT_END
"""

        item = herdres.extract_clean_feed_item({"agent_status": "done"}, {}, raw, allow_unbounded_reports=True)

        self.assertIsNotNone(item)
        assert item is not None
        text = herdres.item_plain_text(item)
        self.assertEqual(item["title"], "Investigation")
        self.assertIn("quoted finding should stay", text)
        self.assertIn("Normal bullet should stay.", text)

    def test_latest_bounded_report_wins(self) -> None:
        raw = """HERDRES_REPORT_START
Old Update
- Old item.
HERDRES_REPORT_END

HERDRES_REPORT_START
New Update
- New item.
HERDRES_REPORT_END
"""

        item = herdres.extract_clean_feed_item({"agent_status": "done"}, {}, raw, allow_unbounded_reports=True)

        self.assertIsNotNone(item)
        assert item is not None
        text = herdres.item_plain_text(item)
        self.assertEqual(item["title"], "New Update")
        self.assertIn("New item.", text)
        self.assertNotIn("Old item.", text)

    def test_report_uses_latest_what_changed(self) -> None:
        raw = """What changed:

- Old fix

Some later chatter

What changed:

- New fix

Verification:

- tests pass
"""

        item = herdres.extract_clean_feed_item({"agent_status": "done"}, {}, raw, allow_unbounded_reports=True)

        self.assertIsNotNone(item)
        assert item is not None
        text = herdres.item_plain_text(item)
        self.assertEqual(item["title"], "What changed")
        self.assertIn("New fix", text)
        self.assertNotIn("Old fix", text)

    def test_done_chatter_without_heading_does_not_auto_report(self) -> None:
        raw = """OK

I verified the thing and it is done.
I am pushing now.
"""

        item = herdres.extract_clean_feed_item({"agent_status": "done"}, {}, raw, allow_unbounded_reports=True)

        self.assertIsNone(item)

    def test_report_with_question_at_end_stays_report(self) -> None:
        raw = """What changed:

- Added clean extraction.

Verification:

- tests pass

Want me to deploy next?
"""

        item = herdres.extract_clean_feed_item({"agent_status": "done"}, {}, raw, allow_unbounded_reports=True)

        self.assertIsNotNone(item)
        assert item is not None
        self.assertEqual(item["kind"], "report")
        self.assertEqual(item["title"], "What changed")

    def test_empty_report_body_does_not_fall_back_to_full_tail(self) -> None:
        raw = """OK

I am preparing the final message.

What changed:
"""

        item = herdres.extract_clean_feed_item({"agent_status": "done"}, {}, raw, allow_unbounded_reports=True)

        self.assertIsNone(item)

    def test_report_starts_at_what_changed(self) -> None:
        raw = """OK

The public repo scan is clean, and tests still pass.
I'm committing and pushing the extraction fix now.

What changed:

- Added pane_feed_output().
- Strips bottom composer/input area.

Verification:

- tests pass
"""

        item = herdres.extract_clean_feed_item({"agent_status": "done"}, {}, raw, allow_unbounded_reports=True)

        self.assertIsNotNone(item)
        assert item is not None
        text = herdres.item_plain_text(item)
        self.assertEqual(item["title"], "What changed")
        self.assertNotIn("I'm committing", text)
        self.assertNotIn("The public repo scan", text)
        self.assertIn("Added pane_feed_output", text)
        self.assertIn("Verification", text)

    def test_inline_changes_made_heading_wins_over_later_verification(self) -> None:
        raw = """OK

I'm committing and pushing now.

Changes made in .local/bin/herdr_telegram_topics.py:520:

- Preserves paragraphs as paragraphs.
- Converts only real markdown bullets into rich lists.

Verified with:

- python3 -m py_compile
- unittest
"""

        item = herdres.extract_clean_feed_item({"agent_status": "done"}, {}, raw, allow_unbounded_reports=True)

        self.assertIsNotNone(item)
        assert item is not None
        text = herdres.item_plain_text(item)
        self.assertTrue(item["title"].startswith("Changes made in "))
        self.assertNotEqual(item["title"], "Verified with")
        self.assertNotIn("I'm committing", text)
        self.assertIn("Preserves paragraphs as paragraphs.", text)
        self.assertIn("python3 -m py_compile", text)

    def test_verification_heading_after_content_does_not_auto_report(self) -> None:
        raw = """Fixed the extraction issue.

- Added heading slicing.

Verified with:

- tests pass
"""

        item = herdres.extract_clean_feed_item({"agent_status": "done"}, {}, raw)

        self.assertIsNone(item)

    def test_summary_heading_does_not_auto_send_without_marker(self) -> None:
        raw = """Summary:
This is a transcript summary, not a final report.
"""

        item = herdres.extract_clean_feed_item({"agent_status": "done"}, {}, raw)

        self.assertIsNone(item)

    def test_what_changed_heading_does_not_auto_send_without_marker(self) -> None:
        raw = """What changed:
- This is visible transcript text without explicit report markers.
"""

        item = herdres.extract_clean_feed_item({"agent_status": "done"}, {}, raw)

        self.assertIsNone(item)

    def test_restart_transcript_does_not_send_update(self) -> None:
        raw = """Conversation interrupted and goal paused.

Summary:
Previous conversation state...
/venv/bin/python script.py
"""

        self.assertTrue(herdres.has_resume_control_noise(raw))
        item = herdres.extract_clean_feed_item({"agent_status": "idle"}, {}, raw)

        self.assertIsNone(item)

    def test_blank_lines_do_not_force_resend_migration(self) -> None:
        clean = """What changed

- Fixed extraction.

Verification

- tests pass
"""

        self.assertFalse(herdres.feed_text_has_ui_noise(clean))

    def test_stable_status_hash_ignores_label_changes(self) -> None:
        pane_a = {
            "pane_id": "1",
            "terminal_id": "t",
            "workspace_id": "w",
            "tab_id": "tab",
            "agent": "claude",
            "agent_status": "idle",
            "label": "Brewed for 1m",
        }
        pane_b = dict(pane_a)
        pane_b["label"] = "Brewed for 5m"

        self.assertEqual(
            herdres.status_hash(herdres.stable_status_object(pane_a)),
            herdres.status_hash(herdres.stable_status_object(pane_b)),
        )

    def test_short_sync_json_is_noise(self) -> None:
        raw = """Report

{"changed": false, "message": "another sync is running", "ok": true}

What changed:

- Fixed extraction.
"""

        lines = herdres.clean_feed_lines(raw)
        text = "\n".join(lines)

        self.assertNotIn('"another sync is running"', text)
        self.assertIn("Fixed extraction.", text)

    def test_failed_feed_send_does_not_update_clean_hash(self) -> None:
        pane = {
            "pane_id": "pane-1",
            "terminal_id": "term-1",
            "workspace_id": "workspace-1",
            "tab_id": "tab-1",
            "agent": "claude",
            "agent_status": "done",
        }
        key = herdres.pane_key(pane)
        entry = {"pane_key": key, "pane_id": "pane-1", "topic_id": "77"}
        state = {
            "version": 1,
            "enabled": True,
            "telegram": {"chat_id": "-1001", "general_thread_id": "1", "owner_user_ids": ["42"]},
            "panes": {key: entry},
        }

        with patch.multiple(
            herdres,
            load_dotenv=Mock(),
            load_state=Mock(return_value=state),
            save_state=Mock(),
            pane_list=Mock(return_value=[pane]),
            preflight_is_fresh=Mock(return_value=True),
            pane_feed_output=Mock(return_value="HERDRES_REPORT_START\nFix\n- Fixed extraction.\nHERDRES_REPORT_END"),
            send_feed_item=Mock(return_value={"ok": False, "format": "rich", "error": "temporary"}),
            LIVE_CARD_ENABLED=False,
        ):
            result = herdres.sync_once()

        self.assertTrue(result["ok"])
        self.assertNotIn("last_clean_hash", entry)
        self.assertIn("last_clean_attempt_hash", entry)
        self.assertIn("temporary", entry.get("last_clean_send_error", ""))

    def test_transient_send_is_attempt_throttled(self) -> None:
        pane = {
            "pane_id": "pane-1",
            "terminal_id": "term-1",
            "workspace_id": "workspace-1",
            "tab_id": "tab-1",
            "agent": "claude",
            "agent_status": "done",
        }
        key = herdres.pane_key(pane)
        entry = {"pane_key": key, "pane_id": "pane-1", "topic_id": "77"}
        state = {
            "version": 1,
            "enabled": True,
            "telegram": {"chat_id": "-1001", "general_thread_id": "1", "owner_user_ids": ["42"]},
            "panes": {key: entry},
        }
        send_feed_item = Mock(return_value={"ok": False, "transient": True, "error": "timeout"})
        common_patches = {
            "load_dotenv": Mock(),
            "load_state": Mock(return_value=state),
            "save_state": Mock(),
            "pane_list": Mock(return_value=[pane]),
            "preflight_is_fresh": Mock(return_value=True),
            "pane_feed_output": Mock(return_value="HERDRES_REPORT_START\nFix\n- Fixed extraction.\nHERDRES_REPORT_END"),
            "send_feed_item": send_feed_item,
            "LIVE_CARD_ENABLED": False,
        }

        with patch.multiple(herdres, **common_patches):
            first = herdres.sync_once()
            second = herdres.sync_once()

        self.assertTrue(first["changed"])
        self.assertFalse(second["changed"])
        send_feed_item.assert_called_once()

    def test_sync_suppresses_resume_transcript_until_bounded_report(self) -> None:
        pane = {
            "pane_id": "pane-1",
            "terminal_id": "term-1",
            "workspace_id": "workspace-1",
            "tab_id": "tab-1",
            "agent": "claude",
            "agent_status": "idle",
        }
        key = herdres.pane_key(pane)
        entry = {
            "pane_key": key,
            "pane_id": "pane-1",
            "topic_id": "77",
            "last_clean_hash": "old",
            "last_clean_text": "old update",
        }
        state = {
            "version": 1,
            "enabled": True,
            "telegram": {"chat_id": "-1001", "general_thread_id": "1", "owner_user_ids": ["42"]},
            "panes": {key: entry},
        }
        send_feed_item = Mock(return_value={"ok": True})

        with patch.multiple(
            herdres,
            load_dotenv=Mock(),
            load_state=Mock(return_value=state),
            save_state=Mock(),
            pane_list=Mock(return_value=[pane]),
            preflight_is_fresh=Mock(return_value=True),
            pane_feed_output=Mock(return_value="Conversation interrupted and goal paused.\n\nSummary:\nPrevious state."),
            send_feed_item=send_feed_item,
            LIVE_CARD_ENABLED=False,
        ):
            first = herdres.sync_once()
            second = herdres.sync_once()

        self.assertTrue(first["changed"])
        self.assertFalse(second["changed"])
        self.assertTrue(entry.get("suppress_auto_feed_until_bounded_report"))
        self.assertNotIn("last_clean_hash", entry)
        send_feed_item.assert_not_called()

    def test_sync_clears_resume_suppress_and_sends_later_question(self) -> None:
        pane = {
            "pane_id": "pane-1",
            "terminal_id": "term-1",
            "workspace_id": "workspace-1",
            "tab_id": "tab-1",
            "agent": "claude",
            "agent_status": "blocked",
        }
        key = herdres.pane_key(pane)
        entry = {
            "pane_key": key,
            "pane_id": "pane-1",
            "topic_id": "77",
            "suppress_auto_feed_until_bounded_report": True,
        }
        state = {
            "version": 1,
            "enabled": True,
            "telegram": {"chat_id": "-1001", "general_thread_id": "1", "owner_user_ids": ["42"]},
            "panes": {key: entry},
        }
        send_feed_item = Mock(return_value={"ok": True})

        with patch.multiple(
            herdres,
            load_dotenv=Mock(),
            load_state=Mock(return_value=state),
            save_state=Mock(),
            pane_list=Mock(return_value=[pane]),
            preflight_is_fresh=Mock(return_value=True),
            pane_feed_output=Mock(return_value="Question\nWould you like me to deploy now?"),
            send_feed_item=send_feed_item,
            LIVE_CARD_ENABLED=False,
        ):
            result = herdres.sync_once()

        self.assertTrue(result["changed"])
        self.assertNotIn("suppress_auto_feed_until_bounded_report", entry)
        send_feed_item.assert_called_once()

    def test_live_card_hash_ignores_label_only_changes(self) -> None:
        pane_a = {"agent_status": "working", "label": "Brewed for 1m"}
        pane_b = {"agent_status": "working", "label": "Brewed for 5m"}

        self.assertEqual(
            herdres.clean_feed_hash(herdres.live_status_item(pane_a)),
            herdres.clean_feed_hash(herdres.live_status_item(pane_b)),
        )

    def test_pane_feed_output_auto_uses_recent_unwrapped_only(self) -> None:
        calls: list[str] = []

        def fake_pane_output(pane_id: str, *, lines: int, max_chars: int, source: str) -> str:
            calls.append(source)
            return "clean transcript" if source == "transcript" else ""

        with patch.object(herdres, "pane_output", side_effect=fake_pane_output):
            text = herdres.pane_feed_output("pane-1")

        self.assertEqual(text, "")
        self.assertEqual(calls, ["recent-unwrapped"])

    def test_pane_feed_output_manual_can_fall_back_to_transcript(self) -> None:
        calls: list[str] = []

        def fake_pane_output(pane_id: str, *, lines: int, max_chars: int, source: str) -> str:
            calls.append(source)
            return "clean transcript" if source == "transcript" else ""

        with patch.object(herdres, "pane_output", side_effect=fake_pane_output):
            text = herdres.pane_feed_output("pane-1", manual=True)

        self.assertEqual(text, "clean transcript")
        self.assertEqual(calls, ["recent-unwrapped", "transcript"])

    def test_choices_buttons_include_labels_and_custom_reply(self) -> None:
        markup = herdres.choices_reply_markup(
            "abc123",
            [
                {"number": "1", "label": "Run sync now"},
                {"number": "2", "label": "Show planned changes"},
            ],
        )

        rows = markup["inline_keyboard"]
        self.assertEqual(rows[0][0]["text"], "1. Run sync now")
        self.assertEqual(rows[0][0]["callback_data"], "herdr:c:abc123:1")
        self.assertEqual(rows[-1][0]["text"], "Custom reply")
        self.assertEqual(rows[-1][0]["callback_data"], "herdr:d:abc123:custom")

    def test_callback_routes_only_authorized_matching_choice(self) -> None:
        state = callback_state()
        send_to_pane = Mock(return_value=(True, ""))

        with callback_patches(state, send_to_pane=send_to_pane):
            result = herdres.callback_reply(callback_payload(user_id="42", data="herdr:c:prompt1:1"))

        self.assertEqual(result["answer"], "Selected 1.")
        send_to_pane.assert_called_once_with("pane-1", "1")

    def test_callback_rejects_non_owner_stale_prompt_and_unknown_choice(self) -> None:
        for payload, expected in (
            (callback_payload(user_id="99", data="herdr:c:prompt1:1"), "Not authorized."),
            (callback_payload(user_id="42", data="herdr:c:oldprompt:1"), "Those choices are no longer active."),
            (callback_payload(user_id="42", data="herdr:c:prompt1:9"), "Choice not found."),
        ):
            state = callback_state()
            send_to_pane = Mock(return_value=(True, ""))
            with self.subTest(expected=expected), callback_patches(state, send_to_pane=send_to_pane):
                result = herdres.callback_reply(payload)
            self.assertEqual(result["answer"], expected)
            send_to_pane.assert_not_called()

    def test_callback_custom_reply_sets_force_reply_without_forwarding(self) -> None:
        state = callback_state()
        send_notice = Mock(return_value={"ok": True})
        send_to_pane = Mock(return_value=(True, ""))

        with callback_patches(state, send_notice=send_notice, send_to_pane=send_to_pane):
            result = herdres.callback_reply(callback_payload(user_id="42", data="herdr:d:prompt1:custom"))

        self.assertEqual(result["answer"], "Write the instruction in this topic.")
        self.assertEqual(state["panes"]["pane-1"]["awaiting_detail"]["choice"], "")
        send_to_pane.assert_not_called()
        send_notice.assert_called_once()
        notice_kwargs = send_notice.call_args.kwargs
        self.assertTrue(notice_kwargs["reply_markup"]["force_reply"])


def callback_state() -> dict:
    return {
        "version": 1,
        "telegram": {
            "chat_id": "-1001",
            "general_thread_id": "1",
            "owner_user_ids": ["42"],
        },
        "panes": {
            "pane-1": {
                "pane_id": "pane-1",
                "topic_id": "77",
                "last_known_status": "working",
                "active_prompt": {
                    "id": "prompt1",
                    "options": [
                        {"number": "1", "label": "Run sync now"},
                        {"number": "4", "label": "Other with details"},
                    ],
                },
            }
        },
    }


def callback_payload(*, user_id: str, data: str) -> dict:
    return {
        "chat_id": "-1001",
        "topic_id": "77",
        "user_id": user_id,
        "message_id": "555",
        "data": data,
    }


def callback_patches(
    state: dict,
    *,
    send_notice: Mock | None = None,
    send_to_pane: Mock | None = None,
):
    return patch.multiple(
        herdres,
        load_dotenv=Mock(),
        load_state=Mock(return_value=state),
        save_state=Mock(),
        send_notice=send_notice or Mock(return_value={"ok": True}),
        send_to_pane=send_to_pane or Mock(return_value=(True, "")),
    )


if __name__ == "__main__":
    unittest.main()
