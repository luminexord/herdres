import importlib.util
import re
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch


MODULE_PATH = Path(__file__).resolve().parents[1] / "herdres.py"
SPEC = importlib.util.spec_from_file_location("herdres", MODULE_PATH)
herdres = importlib.util.module_from_spec(SPEC)
assert SPEC and SPEC.loader
SPEC.loader.exec_module(herdres)


class RenderingTests(unittest.TestCase):
    def setUp(self) -> None:
        self._status_icon_patch = patch.object(herdres, "STATUS_ICON_ENABLED", False)
        self._status_icon_patch.start()

    def tearDown(self) -> None:
        self._status_icon_patch.stop()

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

    def test_one_space_wrapped_prose_is_not_code_block(self) -> None:
        html, _ = herdres._rich_structured_block(
            ["This is a paragraph,", " with wrapped continuation text."],
            max_chars=1000,
            max_lines=10,
        )

        self.assertNotIn("<pre>", html)
        self.assertIn("This is a paragraph, with wrapped continuation text.", html)

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

    def test_diagnostic_run_question_is_not_action_question(self) -> None:
        raw = "Why did the run fail?"

        item = herdres.extract_clean_feed_item({"agent_status": "working"}, {}, raw)

        self.assertIsNone(item)

    def test_owner_decision_question_posts(self) -> None:
        raw = """Question
Should I deploy this now?
"""

        item = herdres.extract_clean_feed_item({"agent_status": "blocked"}, {}, raw)

        self.assertIsNotNone(item)
        assert item is not None
        self.assertEqual(item["kind"], "question")

    def test_blocked_numbered_diagnostic_list_is_not_choices(self) -> None:
        raw = """Blocked
Findings:
1. Run failed because the cache is missing.
2. Deploy command was not executed.
"""

        item = herdres.extract_clean_feed_item({"agent_status": "blocked"}, {}, raw)

        self.assertIsNotNone(item)
        assert item is not None
        self.assertEqual(item["kind"], "blocked")

    def test_diagnostic_question_before_numbered_list_is_not_choices(self) -> None:
        raw = """Why did the deploy fail?
1. Network timeout.
2. Missing credentials.
"""

        item = herdres.extract_clean_feed_item({"agent_status": "blocked"}, {}, raw)

        self.assertIsNotNone(item)
        assert item is not None
        self.assertEqual(item["kind"], "blocked")

    def test_explicit_choices_block_posts_buttons_without_context(self) -> None:
        raw = """HERDRES_CHOICES_START
1. Run sync now
2. Show planned changes
HERDRES_CHOICES_END
"""

        item = herdres.extract_clean_feed_item({"agent_status": "blocked"}, {}, raw)

        self.assertIsNotNone(item)
        assert item is not None
        self.assertEqual(item["kind"], "choices")
        self.assertEqual(item["options"][0]["label"], "Run sync now")

    def test_marker_lines_are_noise_outside_explicit_report(self) -> None:
        raw = """HERDRES_REPORT_START
Deployment
Question
Should I deploy this now?
HERDRES_REPORT_END
"""

        lines = herdres.clean_feed_lines(raw)

        self.assertNotIn("HERDRES_REPORT_START", lines)
        self.assertNotIn("HERDRES_REPORT_END", lines)

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
        self.assertIn("<table bordered striped>", html)
        self.assertIn("<th>Task</th>", html)
        self.assertIn("<td>Alex</td>", html)
        self.assertIn('<input type="checkbox" checked>', html)
        self.assertIn('<input type="checkbox">', html)
        self.assertIn("<details><summary>Risks</summary>", html)
        self.assertIn("<footer>Sprint - Smith - 10:58</footer>", html)

    def test_bounded_report_preserves_process_lines_inside_details(self) -> None:
        raw = """HERDRES_REPORT_START
HERDRES_REPORT_TITLE: Deploy
SUMMARY:
Deploy completed.
DETAILS: Proof
{"changed": true, "sent": 5, "created": 0}
commit 123abc deployed
to https://github.com/gaijinjoe/herdres.git
enabled
HERDRES_REPORT_END
"""

        item = herdres.extract_clean_feed_item({"agent_status": "done"}, {}, raw)

        self.assertIsNotNone(item)
        assert item is not None
        text = herdres.item_plain_text(item)
        html = herdres.render_feed_item_html(item)
        self.assertIn('"changed": true', text)
        self.assertIn("commit 123abc deployed", text)
        self.assertIn("to https://github.com/gaijinjoe/herdres.git", text)
        self.assertIn("enabled", text)
        self.assertIn("<details><summary>Proof</summary>", html)

    def test_structured_report_requires_explicit_title_before_sections(self) -> None:
        raw = """HERDRES_REPORT_START
SUMMARY:
Deploy is done.
TABLE:
Task | Status
Deploy | Done
HERDRES_REPORT_END
"""

        item = herdres.extract_clean_feed_item({"agent_status": "done"}, {}, raw)

        self.assertIsNone(item)

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

    def test_structured_section_aliases_render_as_details_and_checklist(self) -> None:
        raw = """HERDRES_REPORT_START
HERDRES_REPORT_TITLE: Deployment
RISKS:
- Dependency is still blocked.
PROOF:
systemctl --user status herdres.timer
NEXT:
[ ] Watch next timer run
HERDRES_REPORT_END
"""

        item = herdres.extract_clean_feed_item({"agent_status": "done"}, {}, raw)

        self.assertIsNotNone(item)
        assert item is not None
        html = herdres.render_feed_item_html(item)
        self.assertIn("<details><summary>Risks</summary>", html)
        self.assertIn("<details><summary>Proof</summary><pre><code>", html)
        self.assertIn("<h4>Next</h4>", html)
        self.assertIn('<input type="checkbox">Watch next timer run', html)

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

    def test_reuses_closed_topic_mapping_after_public_pane_handle_change(self) -> None:
        pane = {
            "pane_id": "w123:p2",
            "terminal_id": "term-new",
            "workspace_id": "w123",
            "tab_id": "w123:t1",
            "agent": "codex",
            "agent_status": "idle",
            "label": "Topics Pane",
            "agent_session": {"value": "session-1"},
        }
        old_key = "w123-2:old"
        state = {
            "panes": {
                old_key: {
                    "pane_key": old_key,
                    "pane_id": "w123-2",
                    "agent_session_id": "session-1",
                    "workspace": "w123",
                    "tab": "w123:1",
                    "last_known_status": "closed",
                    "closed_at": "2026-06-16T00:00:00+00:00",
                    "topic_id": "13",
                    "topic_name": "Topics Pane",
                    "pane_label_topic_name": "Topics Pane",
                }
            }
        }

        key, entry, changed = herdres.ensure_pane_entry(state, pane)

        self.assertTrue(changed)
        self.assertNotIn(old_key, state["panes"])
        self.assertIn(key, state["panes"])
        self.assertEqual(entry["topic_id"], "13")
        self.assertEqual(entry["pane_id"], "w123:p2")
        self.assertNotIn("closed_at", entry)
        self.assertEqual(entry["reused_from_pane_key"], old_key)

    def test_duplicate_topic_records_match_closed_state_owned_topics_only(self) -> None:
        state = {
            "panes": {
                "old": {
                    "pane_id": "w123-2",
                    "agent_session_id": "session-1",
                    "last_known_status": "closed",
                    "topic_id": "13",
                    "topic_name": "Topics Pane",
                },
                "active": {
                    "pane_id": "w123:p2",
                    "agent_session_id": "session-1",
                    "last_known_status": "working",
                    "topic_id": "573",
                    "topic_name": "Topics Pane",
                },
                "unrelated": {
                    "pane_id": "w999-1",
                    "agent_session_id": "session-other",
                    "last_known_status": "closed",
                    "topic_id": "99",
                    "topic_name": "Other",
                },
            }
        }

        records = herdres.duplicate_topic_records(state)

        self.assertEqual(len(records), 1)
        self.assertEqual(records[0]["closed_key"], "old")
        self.assertEqual(records[0]["active_key"], "active")
        self.assertEqual(records[0]["topic_id"], "13")

    def test_status_marker_content_includes_workflow_counts(self) -> None:
        pane = {
            "agent_status": "working",
            "workflow_counts": {"done": 2, "total": 5, "active": 1},
        }

        title, body = herdres.status_marker_content(pane)

        self.assertEqual(title, "🟡 Working")
        self.assertEqual(body, "Working on 2/5 workflows; 1 active.")

    def test_sync_sends_status_marker_and_deletes_previous_marker(self) -> None:
        pane = {
            "pane_id": "pane-1",
            "terminal_id": "term-1",
            "workspace_id": "workspace-1",
            "tab_id": "tab-1",
            "agent": "codex",
            "agent_status": "working",
        }
        key = herdres.pane_key(pane)
        entry = {
            "pane_key": key,
            "pane_id": "pane-1",
            "topic_id": "77",
            "status_marker_message_id": "10",
            "status_marker_hash": "old",
        }
        state = {
            "version": 1,
            "enabled": True,
            "telegram": {"chat_id": "-1001", "general_thread_id": "1", "owner_user_ids": ["42"]},
            "panes": {key: entry},
        }
        send_notice = Mock(return_value={"ok": True, "message_id": "11"})
        delete_message = Mock(return_value=True)

        with patch.multiple(
            herdres,
            load_dotenv=Mock(),
            load_state=Mock(return_value=state),
            save_state=Mock(),
            pane_list=Mock(return_value=[pane]),
            preflight_is_fresh=Mock(return_value=True),
            pane_turn=Mock(return_value={"available": False, "reason": "no_structured_turn_source"}),
            send_notice=send_notice,
            delete_message=delete_message,
            STATUS_MARKER_ENABLED=True,
            LIVE_CARD_ENABLED=True,
            TURN_FEED_ENABLED=True,
        ):
            result = herdres.sync_once()

        self.assertTrue(result["changed"])
        self.assertEqual(result["sent"], 1)
        send_notice.assert_called_once()
        self.assertEqual(send_notice.call_args.args[1], "🟡 Working")
        delete_message.assert_called_once_with("-1001", "10")
        self.assertEqual(entry["status_marker_message_id"], "11")

    def test_sync_does_not_resend_unchanged_status_marker(self) -> None:
        pane = {
            "pane_id": "pane-1",
            "terminal_id": "term-1",
            "workspace_id": "workspace-1",
            "tab_id": "tab-1",
            "agent": "codex",
            "agent_status": "idle",
        }
        key = herdres.pane_key(pane)
        entry = {
            "pane_key": key,
            "pane_id": "pane-1",
            "topic_id": "77",
            "status_marker_message_id": "10",
            "status_marker_hash": herdres.status_marker_hash(pane),
            "last_turn_available": False,
            "last_turn_reason": "no_structured_turn_source",
            "last_topic_verified_at": herdres.utc_now(),
            "last_status_hash": herdres.status_hash(herdres.stable_status_object(pane)),
        }
        state = {
            "version": 1,
            "enabled": True,
            "telegram": {"chat_id": "-1001", "general_thread_id": "1", "owner_user_ids": ["42"]},
            "panes": {key: entry},
        }
        send_notice = Mock()

        with patch.multiple(
            herdres,
            load_dotenv=Mock(),
            load_state=Mock(return_value=state),
            save_state=Mock(),
            pane_list=Mock(return_value=[pane]),
            preflight_is_fresh=Mock(return_value=True),
            pane_turn=Mock(return_value={"available": False, "reason": "no_structured_turn_source"}),
            send_notice=send_notice,
            STATUS_MARKER_ENABLED=True,
            LIVE_CARD_ENABLED=True,
            TURN_FEED_ENABLED=True,
        ):
            result = herdres.sync_once()

        self.assertFalse(result["changed"])
        self.assertEqual(result["sent"], 0)
        send_notice.assert_not_called()

    def test_status_icon_update_suppresses_marker_message_when_available(self) -> None:
        pane = {
            "pane_id": "pane-1",
            "terminal_id": "term-1",
            "workspace_id": "workspace-1",
            "tab_id": "tab-1",
            "agent": "codex",
            "agent_status": "working",
        }
        key = herdres.pane_key(pane)
        entry = {
            "pane_key": key,
            "pane_id": "pane-1",
            "topic_id": "77",
            "status_marker_message_id": "10",
            "status_marker_hash": "old",
            "last_topic_verified_at": herdres.utc_now(),
        }
        state = {
            "version": 1,
            "enabled": True,
            "telegram": {"chat_id": "-1001", "general_thread_id": "1", "owner_user_ids": ["42"]},
            "panes": {key: entry},
        }
        calls = []

        def telegram_api(method, payload):
            calls.append((method, payload))
            if method == "getForumTopicIconStickers":
                return {"ok": True, "result": [{"emoji": "⚡️", "custom_emoji_id": "icon-working"}]}
            if method == "editForumTopic":
                return {"ok": True, "result": True}
            return {"ok": True, "result": True}

        send_notice = Mock(return_value={"ok": True, "message_id": "11"})
        delete_message = Mock(return_value=True)

        with patch.object(herdres, "STATUS_ICON_ENABLED", True), patch.multiple(
            herdres,
            load_dotenv=Mock(),
            load_state=Mock(return_value=state),
            save_state=Mock(),
            pane_list=Mock(return_value=[pane]),
            preflight_is_fresh=Mock(return_value=True),
            pane_turn=Mock(return_value={"available": False, "reason": "no_structured_turn_source"}),
            telegram_api=telegram_api,
            send_notice=send_notice,
            delete_message=delete_message,
            TURN_FEED_ENABLED=True,
            STATUS_MARKER_ENABLED=True,
            STATUS_MARKER_SUPPRESS_WHEN_ICON_OK=True,
            LIVE_CARD_ENABLED=False,
        ):
            result = herdres.sync_once()

        self.assertTrue(result["changed"])
        self.assertEqual(result["icon_updated"], 1)
        self.assertEqual(result["marker_sent"], 0)
        self.assertEqual(entry["topic_status_icon_custom_emoji_id"], "icon-working")
        self.assertEqual(entry["topic_status_icon_key"], "working")
        self.assertNotIn("status_marker_message_id", entry)
        self.assertEqual(calls[-1][0], "editForumTopic")
        self.assertEqual(calls[-1][1]["icon_custom_emoji_id"], "icon-working")
        send_notice.assert_not_called()
        delete_message.assert_called_once_with("-1001", "10")

    def test_send_to_pane_materializes_long_multiline_input(self) -> None:
        pane = {"pane_id": "pane-1", "agent": "codex"}
        long_text = "\n".join(f"line {idx}" for idx in range(20))
        commands = []

        def run_cmd(args, **kwargs):
            commands.append(args)
            proc = Mock()
            proc.returncode = 0
            proc.stdout = ""
            proc.stderr = ""
            return proc

        with tempfile.TemporaryDirectory() as tmpdir, patch.multiple(
            herdres,
            pane_by_id=Mock(return_value=pane),
            run_cmd=run_cmd,
            state_path=Mock(return_value=Path(tmpdir) / "state.json"),
            pane_input_looks_staged=Mock(return_value=False),
            PANE_INPUT_FILE_CHARS=1200,
            PANE_INPUT_FILE_LINES=6,
        ):
            ok, detail = herdres.send_to_pane("pane-1", long_text)
            self.assertTrue(ok, detail)
            self.assertEqual(commands[0][:3], [herdres.herdr_bin(), "pane", "run"])
            self.assertEqual(commands[0][3], "pane-1")
            outbound = commands[0][4]
            self.assertIn("Read that file", outbound)
            self.assertIn("20 lines", outbound)
            self.assertNotIn("line 19\n", outbound)
            match = re.search(r"saved at (.+?\.txt)", outbound)
            self.assertIsNotNone(match)
            saved = Path(match.group(1))
            self.assertEqual(saved.read_text(encoding="utf-8").strip(), long_text)

    def test_send_to_pane_keeps_short_input_inline(self) -> None:
        pane = {"pane_id": "pane-1", "agent": "codex"}
        commands = []

        def run_cmd(args, **kwargs):
            commands.append(args)
            proc = Mock()
            proc.returncode = 0
            proc.stdout = ""
            proc.stderr = ""
            return proc

        with patch.multiple(
            herdres,
            pane_by_id=Mock(return_value=pane),
            run_cmd=run_cmd,
            pane_input_looks_staged=Mock(return_value=False),
            PANE_INPUT_FILE_CHARS=1200,
            PANE_INPUT_FILE_LINES=6,
        ):
            ok, detail = herdres.send_to_pane("pane-1", "short instruction")

        self.assertTrue(ok, detail)
        self.assertEqual(commands[0][4], "short instruction")

    def test_send_to_pane_submits_staged_pasted_input(self) -> None:
        pane = {"pane_id": "pane-1", "agent": "claude"}
        commands = []

        def run_cmd(args, **kwargs):
            commands.append(args)
            proc = Mock()
            proc.returncode = 0
            proc.stdout = ""
            proc.stderr = ""
            return proc

        with patch.multiple(
            herdres,
            pane_by_id=Mock(return_value=pane),
            run_cmd=run_cmd,
            pane_input_looks_staged=Mock(return_value=True),
        ):
            ok, detail = herdres.send_to_pane("pane-1", "long pasted instruction", submit_staged=True)

        self.assertTrue(ok, detail)
        self.assertEqual(commands[0][:4], [herdres.herdr_bin(), "pane", "run", "pane-1"])
        self.assertEqual(commands[1], [herdres.herdr_bin(), "pane", "send-keys", "pane-1", "enter"])

    def test_visible_choice_selection_uses_numbers_by_default(self) -> None:
        with patch.object(herdres, "VISIBLE_CHOICE_SELECT_MODE", "number"), patch.object(
            herdres,
            "VISIBLE_CHOICE_NUMBER_ENTER",
            True,
        ):
            keys = herdres.visible_choice_selection_keys("4")

        self.assertEqual(keys, ["4", "enter"])

    def test_visible_choice_selection_can_omit_enter_when_digits_auto_activate(self) -> None:
        with patch.object(herdres, "VISIBLE_CHOICE_SELECT_MODE", "number"), patch.object(
            herdres,
            "VISIBLE_CHOICE_NUMBER_ENTER",
            False,
        ):
            keys = herdres.visible_choice_selection_keys("4")

        self.assertEqual(keys, ["4"])

    def test_visible_choice_selection_arrow_navigation_uses_displayed_number(self) -> None:
        with patch.object(herdres, "VISIBLE_CHOICE_SELECT_MODE", "arrows"):
            keys = herdres.visible_choice_selection_keys("4")

        self.assertEqual(keys, ["up"] * 24 + ["down"] * 3 + ["enter"])

    def test_visible_custom_detail_ready_allows_old_choices_above_prompt(self) -> None:
        raw = """Question
How should I proceed?

1) Build P1+P2
2) Instrument first
3) Discriminator
4) Type something

Write your custom answer now:
❯
"""

        self.assertTrue(herdres.visible_custom_detail_ready_text(raw))
        self.assertFalse(
            herdres.visible_custom_detail_ready_text(
                "Question\nHow should I proceed?\n\n1) Build P1+P2\n4) Type something\n"
            )
        )
        self.assertFalse(
            herdres.visible_custom_detail_ready_text(
                "Question\nHow should I proceed?\n\n1) Build P1+P2\n4) Type something\n\nEnter to select · Esc to cancel"
            )
        )

    def test_visible_choice_detail_fails_closed_without_custom_field(self) -> None:
        commands = []

        def run_cmd(args, **kwargs):
            commands.append(args)
            proc = Mock()
            proc.returncode = 0
            proc.stdout = ""
            proc.stderr = ""
            return proc

        send_to_pane = Mock(return_value=(True, ""))
        with patch.multiple(
            herdres,
            pane_by_id=Mock(return_value={"pane_id": "pane-1"}),
            run_cmd=run_cmd,
            wait_for_visible_custom_detail_field=Mock(return_value=False),
            send_to_pane=send_to_pane,
        ):
            ok, detail = herdres.send_visible_choice_detail_to_pane(
                "pane-1",
                "4",
                "custom text",
            )

        self.assertFalse(ok)
        self.assertIn("did not show a custom-answer field", detail)
        send_to_pane.assert_not_called()
        self.assertEqual(commands[0], [herdres.herdr_bin(), "pane", "send-keys", "pane-1", "4", "enter"])

    def test_visible_choice_detail_sends_after_custom_field_verification(self) -> None:
        commands = []

        def run_cmd(args, **kwargs):
            commands.append(args)
            proc = Mock()
            proc.returncode = 0
            proc.stdout = ""
            proc.stderr = ""
            return proc

        send_to_pane = Mock(return_value=(True, ""))
        with patch.multiple(
            herdres,
            pane_by_id=Mock(return_value={"pane_id": "pane-1"}),
            run_cmd=run_cmd,
            wait_for_visible_custom_detail_field=Mock(return_value=True),
            send_to_pane=send_to_pane,
        ):
            ok, detail = herdres.send_visible_choice_detail_to_pane(
                "pane-1",
                "4",
                "custom text",
            )

        self.assertTrue(ok, detail)
        send_to_pane.assert_called_once_with("pane-1", "custom text", timeout=8, submit_staged=True)

    def test_visible_prompt_matches_awaiting_requires_current_prompt_identity(self) -> None:
        entry = {"pane_id": "pane-1"}
        awaiting = {
            "prompt_id": "prompt1",
            "visible_choice": "4",
            "visible_options": [{"number": "4", "label": "Type something."}],
        }
        current = {
            "prompt_id": "prompt1",
            "options": [{"number": "4", "label": "Type something."}],
        }

        with patch.object(herdres, "current_visible_choice_item_for_entry", Mock(return_value=current)):
            self.assertTrue(herdres.visible_prompt_matches_awaiting(entry, awaiting))

        with patch.object(herdres, "current_visible_choice_item_for_entry", Mock(return_value=None)):
            self.assertFalse(herdres.visible_prompt_matches_awaiting(entry, awaiting))

        changed_prompt = dict(current, prompt_id="prompt2")
        with patch.object(herdres, "current_visible_choice_item_for_entry", Mock(return_value=changed_prompt)):
            self.assertFalse(herdres.visible_prompt_matches_awaiting(entry, awaiting))

        missing_choice = dict(current, options=[{"number": "5", "label": "Chat about this"}])
        with patch.object(herdres, "current_visible_choice_item_for_entry", Mock(return_value=missing_choice)):
            self.assertFalse(herdres.visible_prompt_matches_awaiting(entry, awaiting))

        changed_label = dict(current, options=[{"number": "4", "label": "Different option"}])
        with patch.object(herdres, "current_visible_choice_item_for_entry", Mock(return_value=changed_label)):
            self.assertFalse(herdres.visible_prompt_matches_awaiting(entry, awaiting))

    def test_cleanup_duplicates_delete_archives_closed_entry(self) -> None:
        state = {
            "version": 1,
            "enabled": True,
            "telegram": {"chat_id": "-1001"},
            "panes": {
                "old": {
                    "pane_key": "old",
                    "pane_id": "w123-2",
                    "agent_session_id": "session-1",
                    "last_known_status": "closed",
                    "topic_id": "13",
                    "topic_name": "Topics Pane",
                },
                "active": {
                    "pane_key": "active",
                    "pane_id": "w123:p2",
                    "agent_session_id": "session-1",
                    "last_known_status": "working",
                    "topic_id": "573",
                    "topic_name": "Topics Pane",
                },
            },
        }

        with patch.multiple(
            herdres,
            load_dotenv=Mock(),
            load_state=Mock(return_value=state),
            save_state=Mock(),
            preflight=Mock(),
            delete_topic=Mock(return_value=True),
        ):
            result = herdres.cleanup_duplicates_once(delete=True)

        self.assertTrue(result["ok"])
        self.assertEqual(result["deleted_count"], 1)
        self.assertNotIn("old", state["panes"])
        self.assertIn("active", state["panes"])
        self.assertEqual(state["deleted_duplicate_topics"][0]["deleted_duplicate_topic_id"], "13")

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
            TURN_FEED_ENABLED=False,
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
            "TURN_FEED_ENABLED": False,
            "LIVE_CARD_ENABLED": False,
        }

        with patch.multiple(herdres, **common_patches):
            first = herdres.sync_once()
            second = herdres.sync_once()

        self.assertTrue(first["changed"])
        self.assertFalse(second["changed"])
        send_feed_item.assert_called_once()

    def test_render_only_change_edits_previous_clean_message(self) -> None:
        raw = "HERDRES_REPORT_START\nFix\n- Fixed extraction.\nHERDRES_REPORT_END"
        pane = {
            "pane_id": "pane-1",
            "terminal_id": "term-1",
            "workspace_id": "workspace-1",
            "tab_id": "tab-1",
            "agent": "claude",
            "agent_status": "done",
        }
        item = herdres.extract_clean_feed_item(pane, {}, raw)
        assert item is not None
        key = herdres.pane_key(pane)
        entry = {
            "pane_key": key,
            "pane_id": "pane-1",
            "topic_id": "77",
            "last_clean_semantic_hash": herdres.clean_feed_hash(item, include_render_version=False),
            "last_clean_render_hash": "old-render",
            "last_clean_hash": "old-render",
            "last_clean_message_id": "999",
        }
        state = {
            "version": 1,
            "enabled": True,
            "telegram": {"chat_id": "-1001", "general_thread_id": "1", "owner_user_ids": ["42"]},
            "panes": {key: entry},
        }
        edit_feed_item = Mock(return_value={"ok": True, "kind": "edited"})
        send_feed_item = Mock(return_value={"ok": True})

        with patch.multiple(
            herdres,
            load_dotenv=Mock(),
            load_state=Mock(return_value=state),
            save_state=Mock(),
            pane_list=Mock(return_value=[pane]),
            preflight_is_fresh=Mock(return_value=True),
            pane_feed_output=Mock(return_value=raw),
            edit_feed_item=edit_feed_item,
            send_feed_item=send_feed_item,
            TURN_FEED_ENABLED=False,
            LIVE_CARD_ENABLED=False,
        ):
            result = herdres.sync_once()

        self.assertTrue(result["changed"])
        edit_feed_item.assert_called_once()
        send_feed_item.assert_not_called()
        self.assertEqual(entry["last_clean_message_id"], "999")
        self.assertEqual(entry["last_clean_render_hash"], herdres.clean_feed_hash(item))

    def test_same_turn_text_does_not_resend_when_hash_state_is_missing(self) -> None:
        pane = {
            "pane_id": "pane-1",
            "terminal_id": "term-1",
            "workspace_id": "workspace-1",
            "tab_id": "tab-1",
            "agent": "codex",
            "agent_status": "idle",
        }
        turn = {
            "available": True,
            "complete": True,
            "turn_id": "turn-1",
            "user_text": "Check the watcher.",
            "assistant_final_text": "Watcher is idle and healthy.",
        }
        item = herdres.make_turn_feed_item(turn)
        assert item is not None
        key = herdres.pane_key(pane)
        entry = {
            "pane_key": key,
            "pane_id": "pane-1",
            "topic_id": "77",
            "last_turn_id": "turn-1",
            "last_clean_text": herdres.item_plain_text(item),
            "last_clean_message_id": "999",
        }
        state = {
            "version": 1,
            "enabled": True,
            "telegram": {"chat_id": "-1001", "general_thread_id": "1", "owner_user_ids": ["42"]},
            "panes": {key: entry},
        }
        send_feed_item = Mock(return_value={"ok": True, "message_id": "1000"})

        with patch.multiple(
            herdres,
            load_dotenv=Mock(),
            load_state=Mock(return_value=state),
            save_state=Mock(),
            pane_list=Mock(return_value=[pane]),
            preflight_is_fresh=Mock(return_value=True),
            pane_turn=Mock(return_value=turn),
            send_feed_item=send_feed_item,
            TURN_FEED_ENABLED=True,
            LIVE_CARD_ENABLED=False,
            STATUS_MARKER_ENABLED=False,
        ):
            result = herdres.sync_once()

        self.assertTrue(result["changed"])
        send_feed_item.assert_not_called()
        self.assertEqual(entry["last_clean_message_id"], "999")

    def test_render_only_missing_old_message_does_not_repost_stale_turn(self) -> None:
        pane = {
            "pane_id": "pane-1",
            "terminal_id": "term-1",
            "workspace_id": "workspace-1",
            "tab_id": "tab-1",
            "agent": "codex",
            "agent_status": "idle",
        }
        turn = {
            "available": True,
            "complete": True,
            "turn_id": "turn-1",
            "user_text": "Check the watcher.",
            "assistant_final_text": "Watcher is idle and healthy.",
        }
        item = herdres.make_turn_feed_item(turn)
        assert item is not None
        key = herdres.pane_key(pane)
        entry = {
            "pane_key": key,
            "pane_id": "pane-1",
            "topic_id": "77",
            "last_turn_id": "turn-1",
            "last_clean_semantic_hash": herdres.clean_feed_hash(item, include_render_version=False),
            "last_clean_render_hash": "old-render",
            "last_clean_hash": "old-render",
            "last_clean_text": herdres.item_plain_text(item),
            "last_clean_message_id": "999",
        }
        state = {
            "version": 1,
            "enabled": True,
            "telegram": {"chat_id": "-1001", "general_thread_id": "1", "owner_user_ids": ["42"]},
            "panes": {key: entry},
        }
        edit_feed_item = Mock(return_value={"ok": False, "not_found": True, "kind": "not_found"})
        send_feed_item = Mock(return_value={"ok": True, "message_id": "1000"})

        with patch.multiple(
            herdres,
            load_dotenv=Mock(),
            load_state=Mock(return_value=state),
            save_state=Mock(),
            pane_list=Mock(return_value=[pane]),
            preflight_is_fresh=Mock(return_value=True),
            pane_turn=Mock(return_value=turn),
            edit_feed_item=edit_feed_item,
            send_feed_item=send_feed_item,
            TURN_FEED_ENABLED=True,
            LIVE_CARD_ENABLED=False,
            STATUS_MARKER_ENABLED=False,
        ):
            result = herdres.sync_once()

        self.assertTrue(result["changed"])
        edit_feed_item.assert_called_once()
        send_feed_item.assert_not_called()
        self.assertEqual(entry["last_clean_message_id"], "999")
        self.assertEqual(entry["last_clean_render_hash"], herdres.clean_feed_hash(item))
        self.assertIn("last_clean_message_missing_at", entry)
        self.assertNotIn("last_clean_send_error", entry)

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
            TURN_FEED_ENABLED=False,
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
            TURN_FEED_ENABLED=False,
            LIVE_CARD_ENABLED=False,
        ):
            result = herdres.sync_once()

        self.assertTrue(result["changed"])
        self.assertNotIn("suppress_auto_feed_until_bounded_report", entry)
        send_feed_item.assert_called_once()

    def test_transient_preflight_alert_does_not_blame_permissions(self) -> None:
        error = "Telegram getChat failed: <urlopen error [SSL: UNEXPECTED_EOF_WHILE_READING] EOF occurred>"

        text = herdres.preflight_alert_text(error)

        self.assertIn("network/TLS failure", text)
        self.assertNotIn("Grant the bot admin permission", text)
        self.assertTrue(herdres.is_transient_telegram_error(error))

    def test_sync_continues_on_transient_preflight_with_recent_success(self) -> None:
        pane = {
            "pane_id": "pane-1",
            "terminal_id": "term-1",
            "workspace_id": "workspace-1",
            "tab_id": "tab-1",
            "agent": "codex",
            "agent_status": "idle",
        }
        key = herdres.pane_key(pane)
        entry = {"pane_key": key, "pane_id": "pane-1", "topic_id": "77"}
        state = {
            "version": 1,
            "enabled": True,
            "telegram": {
                "chat_id": "-1001",
                "general_thread_id": "1",
                "owner_user_ids": ["42"],
                "last_preflight_ok_at": (
                    herdres._dt.datetime.now(tz=herdres._dt.timezone.utc)
                    - herdres._dt.timedelta(seconds=herdres.PREFLIGHT_TTL_SECONDS + 30)
                ).isoformat(),
            },
            "panes": {key: entry},
        }
        send_message = Mock(return_value={"ok": True})
        pane_turn = Mock(return_value={"available": False, "reason": "no_structured_turn_source"})

        with patch.multiple(
            herdres,
            load_dotenv=Mock(),
            load_state=Mock(return_value=state),
            save_state=Mock(),
            pane_list=Mock(return_value=[pane]),
            preflight=Mock(
                side_effect=herdres.BridgeError(
                    "Telegram getChat failed: <urlopen error [SSL: UNEXPECTED_EOF_WHILE_READING] EOF occurred>"
                )
            ),
            send_message=send_message,
            pane_turn=pane_turn,
            TURN_FEED_ENABLED=True,
            LIVE_CARD_ENABLED=False,
            STATUS_MARKER_ENABLED=False,
        ):
            result = herdres.sync_once()

        self.assertTrue(result["ok"])
        self.assertEqual(result["sent"], 0)
        self.assertIn("last_preflight_warning", state["telegram"])
        self.assertNotIn("last_preflight_error", state["telegram"])
        send_message.assert_not_called()
        pane_turn.assert_called_once_with("pane-1")

    def test_classifies_deleted_forum_topic_as_topic_missing(self) -> None:
        error = herdres.BridgeError("Telegram sendRichMessage failed: Bad Request: message thread not found")

        self.assertEqual(herdres.classify_telegram_error(error), "topic_not_found")
        self.assertTrue(herdres.result_topic_missing({"ok": False, "kind": "topic_not_found"}))

    def test_topic_not_modified_counts_as_verified_topic(self) -> None:
        entry = {"topic_id": "77", "topic_name": "Restored"}

        with patch.object(
            herdres,
            "edit_topic",
            side_effect=herdres.BridgeError("Telegram editForumTopic failed: Bad Request: TOPIC_NOT_MODIFIED"),
        ):
            result = herdres.verify_topic_mapping("-1001", entry)

        self.assertTrue(result["ok"])
        self.assertEqual(result["kind"], "not_modified")
        self.assertIn("last_topic_verified_at", entry)
        self.assertNotIn("last_topic_verify_error", entry)

    def test_sync_clears_stale_topic_mapping_when_verification_finds_deleted_topic(self) -> None:
        pane = {
            "pane_id": "pane-1",
            "terminal_id": "term-1",
            "workspace_id": "workspace-1",
            "tab_id": "tab-1",
            "agent": "codex",
            "agent_status": "idle",
        }
        key = herdres.pane_key(pane)
        entry = {
            "pane_key": key,
            "pane_id": "pane-1",
            "topic_id": "77",
            "topic_name": "Restored",
            "card_message_id": "555",
            "last_clean_hash": "old-clean",
            "last_clean_message_id": "999",
            "active_prompt": {"id": "old"},
        }
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
            edit_topic=Mock(
                side_effect=herdres.BridgeError(
                    "Telegram editForumTopic failed: Bad Request: message thread not found"
                )
            ),
            send_feed_item=Mock(return_value={"ok": True}),
            TURN_FEED_ENABLED=True,
            LIVE_CARD_ENABLED=False,
        ):
            result = herdres.sync_once()

        self.assertTrue(result["changed"])
        self.assertEqual(result["verified"], 1)
        self.assertNotIn("topic_id", entry)
        self.assertEqual(entry["topic_missing_id"], "77")
        self.assertNotIn("card_message_id", entry)
        self.assertNotIn("last_clean_hash", entry)
        self.assertNotIn("active_prompt", entry)

    def test_sync_recreates_topic_after_mapping_was_cleared(self) -> None:
        pane = {
            "pane_id": "pane-1",
            "terminal_id": "term-1",
            "workspace_id": "workspace-1",
            "tab_id": "tab-1",
            "agent": "codex",
            "agent_status": "idle",
        }
        key = herdres.pane_key(pane)
        entry = {
            "pane_key": key,
            "pane_id": "pane-1",
            "topic_name": "Restored",
            "topic_missing_id": "77",
            "topic_missing_at": "2026-06-15T00:00:00+00:00",
        }
        state = {
            "version": 1,
            "enabled": True,
            "telegram": {"chat_id": "-1001", "general_thread_id": "1", "owner_user_ids": ["42"]},
            "panes": {key: entry},
        }
        create_topic = Mock(return_value="88")

        with patch.multiple(
            herdres,
            load_dotenv=Mock(),
            load_state=Mock(return_value=state),
            save_state=Mock(),
            pane_list=Mock(return_value=[pane]),
            preflight_is_fresh=Mock(return_value=True),
            create_topic=create_topic,
            pane_turn=Mock(return_value={"available": False, "reason": "no_structured_turn_source"}),
            TURN_FEED_ENABLED=True,
            LIVE_CARD_ENABLED=False,
        ):
            result = herdres.sync_once()

        self.assertTrue(result["changed"])
        self.assertEqual(result["created"], 1)
        create_topic.assert_called_once_with("-1001", "Restored")
        self.assertEqual(entry["topic_id"], "88")
        self.assertIn("last_topic_verified_at", entry)
        self.assertNotIn("topic_missing_id", entry)
        self.assertNotIn("topic_missing_at", entry)

    def test_existing_pane_label_is_baselined_without_surprise_topic_rename(self) -> None:
        pane = {
            "pane_id": "pane-1",
            "terminal_id": "term-1",
            "workspace_id": "workspace-1",
            "tab_id": "tab-1",
            "agent": "codex",
            "agent_status": "idle",
            "label": "entmoot italy ping",
        }
        entry = {"pane_key": herdres.pane_key(pane), "topic_id": "77", "topic_name": "Italy Ping"}
        state = {"panes": {herdres.pane_key(pane): entry}}

        _, updated, created = herdres.ensure_pane_entry(state, pane)

        self.assertFalse(created)
        self.assertIs(updated, entry)
        self.assertEqual(entry["topic_name"], "Italy Ping")
        self.assertEqual(entry["pane_label_raw"], "entmoot italy ping")
        self.assertEqual(entry["pane_label_topic_name"], "Italy Ping")
        self.assertNotIn("topic_rename_pending_at", entry)

    def test_pane_label_preserves_two_word_topic_name(self) -> None:
        self.assertEqual(herdres.topic_name_from_pane_label("Topics Pane"), "Topics Pane")

    def test_baselined_pane_label_reconciles_stale_topic_name(self) -> None:
        pane = {
            "pane_id": "pane-1",
            "terminal_id": "term-1",
            "workspace_id": "workspace-1",
            "tab_id": "tab-1",
            "agent": "codex",
            "agent_status": "idle",
            "label": "Topics Pane",
        }
        entry = {
            "pane_key": herdres.pane_key(pane),
            "topic_id": "77",
            "topic_name": "Topic Names",
            "topic_title_source": "owner-correction/no-prefix",
            "pane_label_raw": "Topics Pane",
            "pane_label_topic_name": "Topics",
        }
        state = {"panes": {herdres.pane_key(pane): entry}}

        herdres.ensure_pane_entry(state, pane)

        self.assertEqual(entry["topic_name"], "Topics Pane")
        self.assertEqual(entry["topic_title_source"], "pane-label")
        self.assertEqual(entry["pane_label_raw"], "Topics Pane")
        self.assertEqual(entry["pane_label_topic_name"], "Topics Pane")
        self.assertEqual(entry["topic_rename_from"], "Topic Names")
        self.assertEqual(entry["topic_rename_to"], "Topics Pane")

    def test_pane_label_change_schedules_topic_rename(self) -> None:
        pane = {
            "pane_id": "pane-1",
            "terminal_id": "term-1",
            "workspace_id": "workspace-1",
            "tab_id": "tab-1",
            "agent": "codex",
            "agent_status": "idle",
            "label": "flight recorder",
        }
        entry = {
            "pane_key": herdres.pane_key(pane),
            "topic_id": "77",
            "topic_name": "Old Topic",
            "pane_label_raw": "old topic",
        }
        state = {"panes": {herdres.pane_key(pane): entry}}

        herdres.ensure_pane_entry(state, pane)

        self.assertEqual(entry["topic_name"], "Flight Recorder")
        self.assertEqual(entry["topic_title_source"], "pane-label")
        self.assertEqual(entry["topic_rename_from"], "Old Topic")
        self.assertEqual(entry["topic_rename_to"], "Flight Recorder")

    def test_new_labeled_pane_creates_topic_from_label(self) -> None:
        pane = {
            "pane_id": "pane-1",
            "terminal_id": "term-1",
            "workspace_id": "workspace-1",
            "tab_id": "tab-1",
            "agent": "codex",
            "agent_status": "idle",
            "label": "docker cache",
        }
        state = {"panes": {}}

        key, entry, created = herdres.ensure_pane_entry(state, pane)

        self.assertTrue(created)
        self.assertEqual(state["panes"][key], entry)
        self.assertEqual(entry["topic_name"], "Docker Cache")
        self.assertEqual(entry["topic_title_source"], "pane-label")

    def test_sync_renames_topic_when_pane_label_changes(self) -> None:
        pane = {
            "pane_id": "pane-1",
            "terminal_id": "term-1",
            "workspace_id": "workspace-1",
            "tab_id": "tab-1",
            "agent": "codex",
            "agent_status": "idle",
            "label": "flight recorder",
        }
        key = herdres.pane_key(pane)
        entry = {
            "pane_key": key,
            "pane_id": "pane-1",
            "topic_id": "77",
            "topic_name": "Old Topic",
            "pane_label_raw": "old topic",
            "last_topic_verified_at": herdres.utc_now(),
        }
        state = {
            "version": 1,
            "enabled": True,
            "telegram": {"chat_id": "-1001", "general_thread_id": "1", "owner_user_ids": ["42"]},
            "panes": {key: entry},
        }
        edit_topic = Mock(return_value=True)

        with patch.multiple(
            herdres,
            load_dotenv=Mock(),
            load_state=Mock(return_value=state),
            save_state=Mock(),
            pane_list=Mock(return_value=[pane]),
            preflight_is_fresh=Mock(return_value=True),
            edit_topic=edit_topic,
            pane_turn=Mock(return_value={"available": False, "reason": "no_structured_turn_source"}),
            TURN_FEED_ENABLED=True,
            LIVE_CARD_ENABLED=False,
        ):
            result = herdres.sync_once()

        self.assertTrue(result["changed"])
        self.assertEqual(result["renamed"], 1)
        edit_topic.assert_called_once_with("-1001", "77", "Flight Recorder")
        self.assertEqual(entry["topic_name"], "Flight Recorder")
        self.assertEqual(entry["pane_label_raw"], "flight recorder")
        self.assertNotIn("topic_rename_pending_at", entry)

    def test_sync_clears_topic_mapping_when_clean_send_reports_deleted_topic(self) -> None:
        pane = {
            "pane_id": "pane-1",
            "terminal_id": "term-1",
            "workspace_id": "workspace-1",
            "tab_id": "tab-1",
            "agent": "codex",
            "agent_status": "done",
        }
        key = herdres.pane_key(pane)
        entry = {
            "pane_key": key,
            "pane_id": "pane-1",
            "topic_id": "77",
            "topic_name": "Restored",
            "last_topic_verified_at": herdres.utc_now(),
        }
        state = {
            "version": 1,
            "enabled": True,
            "telegram": {"chat_id": "-1001", "general_thread_id": "1", "owner_user_ids": ["42"]},
            "panes": {key: entry},
        }
        pane_turn = Mock(
            return_value={
                "available": True,
                "complete": True,
                "turn_id": "turn-1",
                "user_text": "What happened?",
                "assistant_final_text": "Final answer only.",
            }
        )
        send_feed_item = Mock(
            return_value={
                "ok": False,
                "kind": "topic_not_found",
                "topic_missing": True,
                "error": "Bad Request: message thread not found",
            }
        )

        with patch.multiple(
            herdres,
            load_dotenv=Mock(),
            load_state=Mock(return_value=state),
            save_state=Mock(),
            pane_list=Mock(return_value=[pane]),
            preflight_is_fresh=Mock(return_value=True),
            pane_turn=pane_turn,
            send_feed_item=send_feed_item,
            TURN_FEED_ENABLED=True,
            LIVE_CARD_ENABLED=False,
        ):
            result = herdres.sync_once()

        self.assertTrue(result["changed"])
        send_feed_item.assert_called_once()
        self.assertNotIn("topic_id", entry)
        self.assertEqual(entry["topic_missing_id"], "77")
        self.assertNotIn("last_clean_attempt_hash", entry)

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

    def test_send_to_idle_agent_pane_uses_pane_run_to_submit(self) -> None:
        calls: list[list[str]] = []

        def fake_run_cmd(args: list[str], *, timeout: int = 8, input_text: str | None = None):
            calls.append(args)
            return Mock(returncode=0, stdout="", stderr="")

        with patch.multiple(
            herdres,
            pane_by_id=Mock(return_value={"pane_id": "pane-1", "agent": "codex", "agent_status": "idle"}),
            herdr_bin=Mock(return_value="herdr"),
            run_cmd=fake_run_cmd,
            pane_input_looks_staged=Mock(return_value=False),
        ):
            ok, detail = herdres.send_to_pane("pane-1", "Explain this codebase")

        self.assertTrue(ok)
        self.assertEqual(detail, "")
        self.assertEqual(calls, [["herdr", "pane", "run", "pane-1", "Explain this codebase"]])

    def test_send_to_working_agent_pane_uses_pane_run_to_submit(self) -> None:
        calls: list[list[str]] = []

        def fake_run_cmd(args: list[str], *, timeout: int = 8, input_text: str | None = None):
            calls.append(args)
            return Mock(returncode=0, stdout="", stderr="")

        with patch.multiple(
            herdres,
            pane_by_id=Mock(return_value={"pane_id": "pane-1", "agent": "codex", "agent_status": "working"}),
            herdr_bin=Mock(return_value="herdr"),
            run_cmd=fake_run_cmd,
            pane_input_looks_staged=Mock(return_value=False),
        ):
            ok, detail = herdres.send_to_pane("pane-1", "rn")

        self.assertTrue(ok)
        self.assertEqual(detail, "")
        self.assertEqual(calls, [["herdr", "pane", "run", "pane-1", "rn"]])

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
        self.assertEqual(rows[-1][0]["text"], "Tell me differently")
        self.assertEqual(rows[-1][0]["callback_data"], "herdr:d:abc123:custom")

    def test_extract_choices_skips_descriptions_between_options(self) -> None:
        raw = """Which is it? This picks the fix lever.

❯ 1. Mostly register/tone
     The teacher/analyst voice is the problem.
  2. Mostly length
     It's the word count making the account look AI.
  3. Both, equally
     Long AND explainer-register both signal AI.
  4. Type something.
─────────────────────────────────────────
  5. Chat about this

Enter to select · Tab/Arrow keys to navigate · Esc to cancel
"""

        item = herdres.extract_choices(herdres.clean_feed_lines(raw))

        self.assertIsNotNone(item)
        assert item is not None
        self.assertEqual([opt["label"] for opt in item["options"]], [
            "Mostly register/tone",
            "Mostly length",
            "Both, equally",
            "Type something.",
            "Chat about this",
        ])
        self.assertIn("teacher/analyst voice", item["options"][0]["description"])
        self.assertIn("word count", item["options"][1]["description"])
        self.assertIn("Which is it?", item["summary"])
        html = herdres.render_feed_item_html(item)
        self.assertIn("<small>", html)
        self.assertIn("teacher/analyst voice", html)
        self.assertIn("This picks the fix lever", html)

    def test_visible_choice_fallback_extracts_current_pane_prompt(self) -> None:
        pane = {"pane_id": "pane-1", "agent": "claude", "agent_status": "idle"}
        raw = """Both reviewers agree the value-ranker is overriding your voice contract.
It prefers a grounded explainer whenever it beats a shorter take by one point.

Codex thinks your real objection is register, not raw word count. Which is it?

❯ 1. Mostly register/tone
     A short explainer is still bad.
  2. Mostly length
     You want shorter by default.
  3. Both, equally
     Length and register both matter.
  4. Type something.
"""

        with patch.object(herdres, "pane_output", Mock(return_value=raw)):
            item = herdres.extract_visible_choice_feed_item(pane)

        self.assertIsNotNone(item)
        assert item is not None
        self.assertEqual(item["kind"], "choices")
        self.assertEqual(item["title"], "Decision needed")
        self.assertEqual(item["decision_id"], item["prompt_id"])
        self.assertEqual(item["turn_id"], f"visible-choice:{item['prompt_id']}")
        self.assertEqual(len(item["options"]), 4)
        self.assertIn("value-ranker", item["detail"])
        self.assertIn("Codex thinks", item["summary"])
        self.assertIn("short explainer", item["options"][0]["description"])
        html = herdres.render_feed_item_html(item)
        self.assertIn("value-ranker", html)
        self.assertIn("<h4>Question</h4>", html)
        self.assertIn("Codex thinks", html)

    def test_visible_choice_fallback_uses_recent_unwrapped_context(self) -> None:
        pane = {"pane_id": "pane-1", "agent": "claude", "agent_status": "idle"}
        visible = """days, then flip it on — that respects both "instrument first" and your "move faster."
yours to decide:
←  ☐ Length o…  ☐ Build pa…  ✔ Submit  →
Codex thinks your real objection is the
explainer REGISTER (the "let me explain the nuance" teacher tone), not raw word
count — your rejects say "AI-ish / over-analyzes," rarely "too long." Which
is it? This picks the fix lever.

❯ 1. Mostly register/tone
     The teacher/analyst voice is the problem.
  2. Mostly length
     It's the word count making the account look AI.
  3. Both, equally
     Long AND explainer-register both signal AI.
  4. Type something.
"""
        recent = """● Bash(cd /tmp && python3 review.py)
  ⎿  wrote review output

● Codex 5.5 xhigh concurs with the workflow — and sharpened it in three ways.

Both agree on the root cause

It's the value-ranker override, not generation and not the gate.

Both agree on what NOT to do

Don't ship MATERIAL_DELTA 1→2.

The converged plan (both reviewers)

- Phase 1 — instrument.
- Phase 2 — archive-derived marginal length-guard.
- Phase 3 — pairwise in-voice discriminator tie-break.

My recommendation: build Phase 1 + Phase 2 together, deploy with the guard off,
let the new telemetry confirm it fires on the right cases for a few days, then flip it on.
But two things are genuinely yours to decide:

Codex thinks your real objection is the explainer REGISTER (the "let me explain the nuance"
teacher tone), not raw word count — your rejects say "AI-ish / over-analyzes," rarely
"too long." Which is it? This picks the fix lever.

❯ 1. Mostly register/tone
     The teacher/analyst voice is the problem.
  2. Mostly length
     It's the word count making the account look AI.
  3. Both, equally
     Long AND explainer-register both signal AI.
  4. Type something.
"""

        def pane_output(_pane_id: str, **kwargs: object) -> str:
            return recent if kwargs.get("source") == "recent-unwrapped" else visible

        with patch.object(herdres, "pane_output", Mock(side_effect=pane_output)):
            item = herdres.extract_visible_choice_feed_item(pane)

        self.assertIsNotNone(item)
        assert item is not None
        self.assertIn("Both agree on the root cause", item["detail"])
        self.assertIn("The converged plan", item["detail"])
        self.assertNotIn("Bash(", item["detail"])
        self.assertIn("Codex thinks your real objection", item["summary"])
        self.assertIn("Which is it? This picks the fix lever", item["summary"])
        self.assertIn("teacher/analyst voice", item["options"][0]["description"])
        html = herdres.render_feed_item_html(item)
        self.assertIn("Both agree on the root cause", html)
        self.assertIn("<h4>Question</h4>", html)
        self.assertIn("This picks the fix lever", html)

    def test_self_contained_choice_prompt_suppresses_repeated_context_and_keeps_chat_option(self) -> None:
        raw = """Codex 5.5 xhigh concurs with the workflow.

The converged plan is long and already visible above.

How should I proceed on the build?

❯ 1. Build P1+P2, guard off, then flip ✔
     Recommended. Ship instrumentation and the guard together.
  2. Instrument first only
     Safest, slower.
  3. Go for the discriminator fix
     Most principled, heaviest.
  4. Type something.
─────────────────────────────────────────
  5. Chat about this

Enter to select · Tab/Arrow keys to navigate · Esc to cancel
"""

        item = herdres.extract_choices(herdres.clean_feed_lines(raw))

        self.assertIsNotNone(item)
        assert item is not None
        self.assertEqual([opt["label"] for opt in item["options"]], [
            "Build P1+P2, guard off, then flip ✔",
            "Instrument first only",
            "Go for the discriminator fix",
            "Type something.",
            "Chat about this",
        ])
        self.assertEqual(item["detail"], "")
        self.assertIn("How should I proceed", item["summary"])
        markup = herdres.choices_reply_markup(item["prompt_id"], item["options"])
        labels = [row[0]["text"] for row in markup["inline_keyboard"]]
        self.assertIn("5. Chat about this", labels)
        self.assertNotIn("Tell me differently", labels)

    def test_submitted_prompt_history_does_not_strip_current_choices(self) -> None:
        raw = """❯ ask me again the questions, i answered
  wrong

● No problem — here they are again.

Codex thinks your real objection is register, not length. Which is it?

❯ 1. Mostly register/tone
     Tone is the issue.
  2. Mostly length
     Length is the issue.
  3. Both, equally
     Both matter.
  4. Type something.
─────────────────────────────────────────
  5. Chat about this

Enter to select · Tab/Arrow keys to navigate · Esc to cancel
"""

        item = herdres.extract_choices(herdres.clean_feed_lines(raw))

        self.assertIsNotNone(item)
        assert item is not None
        self.assertEqual(len(item["options"]), 5)
        self.assertIn("Which is it?", item["summary"])

    def test_decision_buttons_can_send_explicit_text(self) -> None:
        item = herdres.make_turn_feed_item(
            {
                "available": True,
                "complete": False,
                "awaiting_input": True,
                "turn_id": "turn-1",
                "user_text": "Pick the next step.",
                "pending_decision": {
                    "decision_id": "turn-1:decision-1",
                    "prompt": "How should I proceed?",
                    "options": [
                        {"id": "watchdog", "label": "Build watchdog now", "send_text": "1"},
                        {"id": "timeout", "label": "Patch timeout only", "send_text": "2"},
                        {"id": "custom", "label": "Write custom instruction", "send_text": ""},
                    ],
                },
            }
        )

        self.assertIsNotNone(item)
        assert item is not None
        self.assertEqual(item["kind"], "decision")
        self.assertEqual(item["decision_id"], "turn-1:decision-1")
        html = herdres.render_feed_item_html(item)
        self.assertIn("<b>You asked</b>", html)
        self.assertIn("<h3>Decision needed</h3>", html)
        self.assertIn("How should I proceed?", html)
        markup, active_prompt, clear_prompt = herdres.prompt_delivery_state(item)
        assert markup is not None and active_prompt is not None
        rows = markup["inline_keyboard"]
        self.assertFalse(clear_prompt)
        self.assertEqual(rows[0][0]["text"], "1. Build watchdog now")
        self.assertEqual(rows[0][0]["callback_data"], f"herdr:c:{item['prompt_id']}:watchdog")
        self.assertEqual(rows[2][0]["callback_data"], f"herdr:d:{item['prompt_id']}:custom")
        self.assertEqual(active_prompt["decision_id"], "turn-1:decision-1")
        self.assertNotIn("###", html)
        self.assertNotIn("**", html)
        self.assertNotIn("`", html)

    def test_callback_data_stays_within_telegram_limit(self) -> None:
        options = [
            {
                "number": "this-is-a-very-long-internal-choice-identifier-that-will-be-trimmed",
                "label": "A long but readable option label",
            }
        ]
        markup = herdres.choices_reply_markup("this-prompt-id-is-also-too-long-for-telegram-callbacks", options)

        for row in markup["inline_keyboard"]:
            for button in row:
                self.assertLessEqual(len(button["callback_data"].encode("utf-8")), 64)

    def test_callback_routes_only_authorized_matching_choice(self) -> None:
        state = callback_state()
        send_to_pane = Mock(return_value=(True, ""))

        with callback_patches(state, send_to_pane=send_to_pane):
            result = herdres.callback_reply(callback_payload(user_id="42", data="herdr:c:prompt1:1"))

        self.assertEqual(result["answer"], "Selected 1.")
        send_to_pane.assert_called_once_with("pane-1", "1")

    def test_callback_routes_decision_send_text(self) -> None:
        state = callback_state()
        state["panes"]["pane-1"]["active_prompt"] = {
            "id": "decision1",
            "options": [
                {"number": "watchdog", "label": "Build watchdog now", "send_text": "1"},
                {"number": "timeout", "label": "Patch timeout only", "send_text": "2"},
            ],
        }
        send_to_pane = Mock(return_value=(True, ""))

        with callback_patches(state, send_to_pane=send_to_pane):
            result = herdres.callback_reply(callback_payload(user_id="42", data="herdr:c:decision1:timeout"))

        self.assertEqual(result["answer"], "Selected timeout.")
        send_to_pane.assert_called_once_with("pane-1", "2")
        self.assertNotIn("active_prompt", state["panes"]["pane-1"])

    def test_callback_custom_decision_option_sets_force_reply(self) -> None:
        state = callback_state()
        state["panes"]["pane-1"]["active_prompt"] = {
            "id": "decision1",
            "options": [
                {"number": "custom", "id": "custom", "label": "Write custom instruction", "send_text": "", "needs_detail": "1"},
            ],
        }
        send_notice = Mock(return_value={"ok": True, "message_id": "888"})
        send_to_pane = Mock(return_value=(True, ""))

        with callback_patches(state, send_notice=send_notice, send_to_pane=send_to_pane):
            result = herdres.callback_reply(callback_payload(user_id="42", data="herdr:d:decision1:custom"))

        self.assertEqual(result["answer"], "Write the instruction in this topic.")
        self.assertEqual(state["panes"]["pane-1"]["awaiting_detail"]["choice"], "")
        self.assertEqual(state["panes"]["pane-1"]["awaiting_detail"]["force_reply_message_id"], "888")
        send_to_pane.assert_not_called()
        send_notice.assert_called_once()
        self.assertTrue(send_notice.call_args.kwargs["reply_markup"]["force_reply"])

    def test_callback_detail_choice_with_send_text_waits_for_reply(self) -> None:
        state = callback_state()
        state["panes"]["pane-1"]["active_prompt"] = {
            "id": "decision1",
            "options": [
                {"number": "patch", "label": "Patch with extra detail", "send_text": "1", "needs_detail": "1"},
            ],
        }
        send_notice = Mock(return_value={"ok": True, "message_id": "777"})
        send_to_pane = Mock(return_value=(True, ""))

        with callback_patches(state, send_notice=send_notice, send_to_pane=send_to_pane):
            result = herdres.callback_reply(callback_payload(user_id="42", data="herdr:d:decision1:patch"))

        self.assertEqual(result["answer"], "Write the details in this topic.")
        self.assertEqual(state["panes"]["pane-1"]["awaiting_detail"]["choice"], "1")
        self.assertEqual(state["panes"]["pane-1"]["awaiting_detail"]["force_reply_message_id"], "777")
        send_to_pane.assert_not_called()

    def test_callback_visible_custom_choice_waits_for_in_question_detail(self) -> None:
        state = callback_state()
        state["panes"]["pane-1"]["active_prompt"] = {
            "id": "prompt1",
            "options": [
                {"number": "4", "label": "Type something."},
                {"number": "5", "label": "Chat about this"},
            ],
        }
        send_notice = Mock(return_value={"ok": True, "message_id": "777"})
        send_to_pane = Mock(return_value=(True, ""))

        with callback_patches(state, send_notice=send_notice, send_to_pane=send_to_pane):
            result = herdres.callback_reply(callback_payload(user_id="42", data="herdr:d:prompt1:4"))

        self.assertEqual(result["answer"], "Write the details in this topic.")
        self.assertEqual(state["panes"]["pane-1"]["awaiting_detail"]["choice"], "")
        self.assertEqual(state["panes"]["pane-1"]["awaiting_detail"]["visible_choice"], "4")
        self.assertEqual(state["panes"]["pane-1"]["awaiting_detail"]["visible_choice_index"], 1)
        send_to_pane.assert_not_called()

    def test_stale_visible_choice_callback_refreshes_current_prompt(self) -> None:
        state = callback_state()
        state["panes"]["pane-1"]["active_prompt"] = {
            "id": "oldprompt",
            "item": {"kind": "choices", "turn_id": "visible-choice:oldprompt"},
            "options": [{"number": "1", "label": "Old option"}],
        }
        current_item = {
            "kind": "choices",
            "title": "Decision needed",
            "summary": "Current question?",
            "detail": "",
            "text": "Question\nCurrent question?\n\n1) Current option",
            "options": [{"number": "1", "label": "Current option"}],
            "prompt_id": "newprompt",
            "turn_id": "visible-choice:newprompt",
            "notify": True,
        }
        send_feed_item = Mock(return_value={"ok": True, "message_id": "1000"})
        send_to_pane = Mock(return_value=(True, ""))

        with callback_patches(state, send_to_pane=send_to_pane), patch.multiple(
            herdres,
            current_visible_choice_item_for_entry=Mock(return_value=current_item),
            send_feed_item=send_feed_item,
        ):
            result = herdres.callback_reply(callback_payload(user_id="42", data="herdr:c:oldprompt:1"))

        self.assertEqual(result["answer"], "Those choices changed. I sent the current prompt.")
        self.assertTrue(result["show_alert"])
        send_to_pane.assert_not_called()
        send_feed_item.assert_called_once()
        self.assertEqual(state["panes"]["pane-1"]["active_prompt"]["id"], "newprompt")

    def test_force_reply_visible_choice_detail_selects_option_then_sends_text(self) -> None:
        state = callback_state()
        state["panes"]["pane-1"]["awaiting_detail"] = {
            "user_id": "42",
            "prompt_id": "prompt1",
            "choice": "",
            "visible_choice": "4",
            "visible_choice_index": 2,
            "visible_options": [{"number": "4", "label": "Type something."}],
            "option": "Type something.",
            "force_reply_message_id": "999",
            "created_at": herdres.utc_now(),
        }
        send_visible_choice_detail_to_pane = Mock(return_value=(True, ""))

        with callback_patches(state), patch.multiple(
            herdres,
            send_visible_choice_detail_to_pane=send_visible_choice_detail_to_pane,
            visible_prompt_matches_awaiting=Mock(return_value=True),
        ):
            result = herdres.command_reply(
                {
                    "chat_id": "-1001",
                    "topic_id": "77",
                    "user_id": "42",
                    "text": "ask gitmoot and report back",
                    "reply_to_message_id": "999",
                }
            )

        self.assertEqual(result["reply"], "Sent details.")
        send_visible_choice_detail_to_pane.assert_called_once_with(
            "pane-1",
            "4",
            "ask gitmoot and report back",
        )
        self.assertNotIn("awaiting_detail", state["panes"]["pane-1"])

    def test_force_reply_visible_choice_detail_fails_closed_when_prompt_changed(self) -> None:
        state = callback_state()
        state["panes"]["pane-1"]["awaiting_detail"] = {
            "user_id": "42",
            "prompt_id": "prompt1",
            "choice": "",
            "visible_choice": "4",
            "visible_options": [{"number": "4", "label": "Type something."}],
            "option": "Type something.",
            "force_reply_message_id": "999",
            "created_at": herdres.utc_now(),
        }
        send_visible_choice_detail_to_pane = Mock(return_value=(True, ""))

        with callback_patches(state), patch.multiple(
            herdres,
            send_visible_choice_detail_to_pane=send_visible_choice_detail_to_pane,
            visible_prompt_matches_awaiting=Mock(return_value=False),
        ):
            result = herdres.command_reply(
                {
                    "chat_id": "-1001",
                    "topic_id": "77",
                    "user_id": "42",
                    "text": "ask gitmoot and report back",
                    "reply_to_message_id": "999",
                }
            )

        self.assertIn("choices changed", result["reply"])
        send_visible_choice_detail_to_pane.assert_not_called()
        self.assertNotIn("awaiting_detail", state["panes"]["pane-1"])

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
        send_notice = Mock(return_value={"ok": True, "message_id": "999"})
        send_to_pane = Mock(return_value=(True, ""))

        with callback_patches(state, send_notice=send_notice, send_to_pane=send_to_pane):
            result = herdres.callback_reply(callback_payload(user_id="42", data="herdr:d:prompt1:custom"))

        self.assertEqual(result["answer"], "Write the instruction in this topic.")
        self.assertEqual(state["panes"]["pane-1"]["awaiting_detail"]["choice"], "")
        self.assertEqual(state["panes"]["pane-1"]["awaiting_detail"]["force_reply_message_id"], "999")
        send_to_pane.assert_not_called()
        send_notice.assert_called_once()
        notice_kwargs = send_notice.call_args.kwargs
        self.assertTrue(notice_kwargs["reply_markup"]["force_reply"])

    def test_force_reply_detail_requires_matching_reply_message(self) -> None:
        state = callback_state()
        state["panes"]["pane-1"]["awaiting_detail"] = {
            "user_id": "42",
            "prompt_id": "prompt1",
            "choice": "1",
            "option": "Patch with detail",
            "force_reply_message_id": "999",
            "created_at": herdres.utc_now(),
        }
        send_to_pane = Mock(return_value=(True, ""))

        with callback_patches(state, send_to_pane=send_to_pane):
            result = herdres.command_reply(
                {
                    "chat_id": "-1001",
                    "topic_id": "77",
                    "user_id": "42",
                    "text": "extra detail",
                    "reply_to_message_id": "123",
                }
            )

        self.assertEqual(result["reply"], "Reply directly to the detail prompt, or tap the button again.")
        send_to_pane.assert_not_called()

    def test_force_reply_detail_sends_choice_and_clears_prompt(self) -> None:
        state = callback_state()
        state["panes"]["pane-1"]["awaiting_detail"] = {
            "user_id": "42",
            "prompt_id": "prompt1",
            "choice": "1",
            "option": "Patch with detail",
            "force_reply_message_id": "999",
            "created_at": herdres.utc_now(),
        }
        send_to_pane = Mock(return_value=(True, ""))

        with callback_patches(state, send_to_pane=send_to_pane):
            result = herdres.command_reply(
                {
                    "chat_id": "-1001",
                    "topic_id": "77",
                    "user_id": "42",
                    "text": "extra detail",
                    "reply_to_message_id": "999",
                }
            )

        self.assertEqual(result["reply"], "Sent details.")
        send_to_pane.assert_called_once_with("pane-1", "1\nextra detail")
        self.assertNotIn("awaiting_detail", state["panes"]["pane-1"])
        self.assertNotIn("active_prompt", state["panes"]["pane-1"])

    def test_turn_feed_renders_user_prompt_and_final_reply_without_label(self) -> None:
        item = herdres.make_turn_feed_item(
            {
                "available": True,
                "complete": True,
                "turn_id": "turn-1",
                "user_text": "Why did the bot freeze?",
                "assistant_final_text": "Likely cause:\n\n- Browser navigation hung.\n- Service is restarted.",
            }
        )

        self.assertIsNotNone(item)
        assert item is not None
        self.assertEqual(item["kind"], "turn")
        html = herdres.render_feed_item_html(item)

        self.assertIn("<b>You asked</b>", html)
        self.assertIn("Why did the bot freeze?", html)
        self.assertIn("<h3>Likely cause</h3>", html)
        self.assertIn("<li>Browser navigation hung.</li>", html)
        self.assertNotIn("<h3>Question</h3>", html)
        self.assertNotIn("<h3>Report</h3>", html)
        self.assertNotIn("<h3>Update</h3>", html)

    def test_turn_feed_formats_screenshot_case_as_rich_html(self) -> None:
        item = {
            "kind": "turn",
            "user_text": (
                "also you see how each pane on herdr can have a name? i would like it so that "
                "if i change a name on a herdr pane manually, it automatically changes the name "
                "of the topic on telegram"
            ),
            "assistant_final_text": """Implemented.
Herdres now watches the Herdr pane `label` field and syncs Telegram topic names from it:
- Existing labeled panes are baselined first, so it does not surprise-rename current owner-corrected topics.
- If you manually change a Herdr pane name after this, the next sync updates the mapped Telegram topic name with `editForumTopic`.
- If Telegram says the topic is missing during rename, Herdres clears that pane's stale topic mapping and recreates it on the next sync.
- No Herdr core changes, no LLM calls, no extra messages posted.

Deployed live to `/home/smith/.local/bin/herdr_telegram_topics.py`.

Pushed
`cdee2ca Sync Telegram topic names from Herdr pane labels`

Verification

- `python3 -m py_compile herdres.py herdr_turn_adapter.py`
- `python3 -m unittest discover -s tests -p 'test*.py' -q` -> 78 tests OK
- Live sync ran successfully: `renamed=0`, `sent=0`, `panes=6`
- Existing `entmoot italy ping` label was baselined while keeping Telegram topic `Italy Ping`.
""",
        }

        html = herdres.render_turn_item_html(item)

        self.assertIn("<blockquote>", html)
        self.assertIn("<h3>Implemented</h3>", html)
        self.assertIn("<h4>Pushed</h4>", html)
        self.assertIn("<h4>Verification</h4>", html)
        self.assertIn("<code>label</code>", html)
        self.assertIn("<code>editForumTopic</code>", html)
        self.assertIn("<code>/home/smith/.local/bin/herdr_telegram_topics.py</code>", html)
        self.assertIn("<code>cdee2ca</code> Sync Telegram topic names from Herdr pane labels", html)
        self.assertIn("<code>renamed=0</code>", html)
        self.assertNotIn("`", html)

    def test_single_inline_command_does_not_become_pre_block(self) -> None:
        html = herdres.render_final_reply_html("- `python3 -m py_compile herdres.py`")

        self.assertNotIn("<pre>", html)
        self.assertIn("<code>python3 -m py_compile herdres.py</code>", html)

    def test_fenced_code_becomes_pre_code(self) -> None:
        html = herdres.render_final_reply_html("```bash\nsystemctl --user status herdres.timer\n```")

        self.assertIn('<pre><code class="language-bash">', html)
        self.assertIn("systemctl --user status herdres.timer", html)

    def test_unmatched_backtick_does_not_render_literal_entity_text(self) -> None:
        html = herdres.render_final_reply_html("This has one ` unmatched marker.")

        self.assertNotIn("`", html)
        self.assertNotIn("&amp;#96;", html)
        self.assertNotIn("&#96;", html)
        self.assertIn("unmatched marker", html)

    def test_report_table_cells_do_not_gain_turn_inline_code(self) -> None:
        html = herdres._rich_table_section(["File | Status", "herdres.py | OK"])

        self.assertIn("<td>herdres.py</td>", html)
        self.assertNotIn("<td><code>herdres.py</code></td>", html)

    def test_turn_table_cells_use_rich_inline_code(self) -> None:
        html = herdres.render_final_reply_html("Files\nFile | Status\nherdres.py | OK")

        self.assertIn("<td><code>herdres.py</code></td>", html)

    def test_short_standalone_lines_become_turn_headings(self) -> None:
        html = herdres.render_final_reply_html("Pushed\n`cdee2ca Sync topic names`\n\nVerification\n- tests OK")

        self.assertIn("<h3>Pushed</h3>", html)
        self.assertIn("<h4>Verification</h4>", html)
        self.assertIn("<code>cdee2ca</code> Sync topic names", html)

    def test_turn_renderer_breaks_claude_dense_progress_text_into_readable_blocks(self) -> None:
        text = (
            "Diagnosis workflow launched (`wcvm3g3qy`) — 3 parallel readers "
            "(automated path / manual path / button+storage) -> synthesis -> 2 adversarial verifiers "
            "(does the manual decision even *have* the failed candidate to persist? will the button actually "
            "*appear* after the fix?).\n\n"
            "My working hypothesis from the inline scout: `draft_status_link_reply` calls "
            "`finish_job(..., \"failed\", ...)` on every failure branch but **never** calls "
            "`record_failed_draft` / `save_failed_draft_attempts`, so there's nothing for the `faildraft:` "
            "button to read -> \"no failed draft recorded.\" The automated path *does* persist. "
            "The verifiers will confirm the two real risks. When it returns I'll design the exact fix."
        )

        html = herdres.render_final_reply_html(text)

        self.assertIn("<h3>Diagnosis workflow launched", html)
        self.assertIn("<code>wcvm3g3qy</code>", html)
        self.assertIn("<i>have</i>", html)
        self.assertIn("<i>appear</i>", html)
        self.assertIn("<i>does</i>", html)
        self.assertIn("<b>never</b>", html)
        self.assertGreaterEqual(html.count("<p>"), 3)
        self.assertNotIn("`", html)

    def test_oversized_turn_fallback_keeps_more_than_tiny_summary(self) -> None:
        text = "Implemented.\n" + "\n".join(
            f"- Item {idx}: `renamed={idx}` with enough text to inflate rich HTML output."
            for idx in range(1, 180)
        )
        item = {"kind": "turn", "user_text": "Long update?", "assistant_final_text": text}

        html = herdres.render_turn_item_html(item)

        self.assertLessEqual(len(html), herdres.MAX_RICH_HTML_CHARS + 300)
        self.assertIn("Item 1", html)
        self.assertIn("Item 30", html)

    def test_proof_section_collapses_in_turn_renderer(self) -> None:
        html = herdres.render_final_reply_html(
            "Verification\n- tests OK\n\nProof\n```bash\nsystemctl --user status herdres.timer\n```"
        )

        self.assertIn("<h3>Verification</h3>", html)
        self.assertIn("<details><summary>Proof</summary>", html)

    def test_turn_feed_ignores_incomplete_and_unavailable_turns(self) -> None:
        self.assertIsNone(
            herdres.make_turn_feed_item(
                {
                    "complete": True,
                    "user_text": "Prompt",
                    "assistant_final_text": "Final",
                }
            )
        )
        self.assertIsNone(
            herdres.make_turn_feed_item(
                {
                    "available": True,
                    "complete": True,
                    "user_text": "Prompt",
                    "assistant_final_text": "",
                }
            )
        )
        self.assertIsNone(
            herdres.make_turn_feed_item(
                {
                    "available": True,
                    "complete": False,
                    "user_text": "Still running",
                    "assistant_final_text": "Old final",
                }
            )
        )
        self.assertIsNone(
            herdres.make_turn_feed_item(
                {
                    "available": False,
                    "reason": "no_structured_turn_source",
                    "complete": True,
                    "user_text": "Prompt",
                    "assistant_final_text": "Final",
                }
            )
        )

    def test_sync_turn_feed_unavailable_does_not_parse_pane_output(self) -> None:
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
        pane_turn = Mock(return_value={"available": False, "reason": "no_structured_turn_source"})
        pane_feed_output = Mock(return_value="HERDRES_REPORT_START\nFallback\n- Do not parse this.\nHERDRES_REPORT_END")
        send_feed_item = Mock(return_value={"ok": True})

        with patch.multiple(
            herdres,
            load_dotenv=Mock(),
            load_state=Mock(return_value=state),
            save_state=Mock(),
            pane_list=Mock(return_value=[pane]),
            preflight_is_fresh=Mock(return_value=True),
            pane_turn=pane_turn,
            pane_feed_output=pane_feed_output,
            send_feed_item=send_feed_item,
            TURN_FEED_ENABLED=True,
            LIVE_CARD_ENABLED=False,
        ):
            result = herdres.sync_once()

        self.assertTrue(result["changed"])
        self.assertFalse(entry["last_turn_available"])
        self.assertEqual(entry["last_turn_reason"], "no_structured_turn_source")
        pane_turn.assert_called_once_with("pane-1")
        pane_feed_output.assert_not_called()
        send_feed_item.assert_not_called()
        self.assertNotIn("last_clean_hash", entry)

    def test_sync_turn_feed_sends_completed_turn_without_legacy_parser(self) -> None:
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
        pane_turn = Mock(
            return_value={
                "available": True,
                "complete": True,
                "turn_id": "turn-1",
                "user_text": "Why did the bot freeze?",
                "assistant_final_text": "Likely cause:\n\n- Browser navigation hung.",
            }
        )
        pane_feed_output = Mock(return_value="Question\nShould not be parsed")
        send_feed_item = Mock(return_value={"ok": True, "message_id": "999"})

        with patch.multiple(
            herdres,
            load_dotenv=Mock(),
            load_state=Mock(return_value=state),
            save_state=Mock(),
            pane_list=Mock(return_value=[pane]),
            preflight_is_fresh=Mock(return_value=True),
            pane_turn=pane_turn,
            pane_feed_output=pane_feed_output,
            send_feed_item=send_feed_item,
            TURN_FEED_ENABLED=True,
            LIVE_CARD_ENABLED=False,
            STATUS_MARKER_ENABLED=False,
        ):
            result = herdres.sync_once()

        self.assertTrue(result["changed"])
        pane_turn.assert_called_once_with("pane-1")
        pane_feed_output.assert_not_called()
        send_feed_item.assert_called_once()
        sent_item = send_feed_item.call_args.args[1]
        self.assertEqual(sent_item["kind"], "turn")
        self.assertEqual(entry["last_turn_id"], "turn-1")
        self.assertEqual(entry["last_clean_kind"], "turn")
        self.assertEqual(entry["last_clean_message_id"], "999")
        self.assertIn("You asked", entry["last_clean_text"])
        self.assertIn("Likely cause", entry["last_clean_text"])
        self.assertNotIn("Question\nShould not be parsed", entry["last_clean_text"])

    def test_status_marker_does_not_send_in_same_run_as_final_reply(self) -> None:
        pane = {
            "pane_id": "pane-1",
            "terminal_id": "term-1",
            "workspace_id": "workspace-1",
            "tab_id": "tab-1",
            "agent": "codex",
            "agent_status": "done",
        }
        key = herdres.pane_key(pane)
        entry = {
            "pane_key": key,
            "pane_id": "pane-1",
            "topic_id": "77",
            "status_marker_message_id": "10",
            "status_marker_hash": "old",
        }
        state = {
            "version": 1,
            "enabled": True,
            "telegram": {"chat_id": "-1001", "general_thread_id": "1", "owner_user_ids": ["42"]},
            "panes": {key: entry},
        }
        send_feed_item = Mock(return_value={"ok": True, "message_id": "999"})
        send_notice = Mock(return_value={"ok": True, "message_id": "11"})

        with patch.multiple(
            herdres,
            load_dotenv=Mock(),
            load_state=Mock(return_value=state),
            save_state=Mock(),
            pane_list=Mock(return_value=[pane]),
            preflight_is_fresh=Mock(return_value=True),
            pane_turn=Mock(
                return_value={
                    "available": True,
                    "complete": True,
                    "turn_id": "turn-1",
                    "user_text": "Do it.",
                    "assistant_final_text": "Done.",
                }
            ),
            send_feed_item=send_feed_item,
            send_notice=send_notice,
            TURN_FEED_ENABLED=True,
            STATUS_MARKER_ENABLED=True,
            LIVE_CARD_ENABLED=True,
        ):
            result = herdres.sync_once()

        self.assertEqual(result["feed_sent"], 1)
        self.assertEqual(result["marker_sent"], 0)
        send_feed_item.assert_called_once()
        send_notice.assert_not_called()
        self.assertEqual(entry["last_clean_message_id"], "999")
        self.assertEqual(entry["status_marker_message_id"], "10")

    def test_status_marker_budget_does_not_block_final_reply(self) -> None:
        pane = {
            "pane_id": "pane-1",
            "terminal_id": "term-1",
            "workspace_id": "workspace-1",
            "tab_id": "tab-1",
            "agent": "codex",
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
        counters = {"sends": 8, "feed_sends": 0, "marker_sends": 8, "creates": 0, "verifies": 0, "renames": 0}
        caps = {"max_sends": 8, "max_feed_sends": 8, "max_marker_sends": 8, "max_creates": 0, "max_verifies": 0}
        send_feed_item = Mock(return_value={"ok": True, "message_id": "999"})

        with patch.multiple(
            herdres,
            pane_turn=Mock(
                return_value={
                    "available": True,
                    "complete": True,
                    "turn_id": "turn-1",
                    "user_text": "Do it.",
                    "assistant_final_text": "Done.",
                }
            ),
            send_feed_item=send_feed_item,
            send_notice=Mock(),
            TURN_FEED_ENABLED=True,
            STATUS_MARKER_ENABLED=True,
            LIVE_CARD_ENABLED=False,
        ):
            changed = herdres.sync_pane_once(state, "-1001", state["telegram"], pane, counters, caps)

        self.assertTrue(changed)
        self.assertEqual(counters["feed_sends"], 1)
        self.assertEqual(counters["marker_sends"], 8)
        send_feed_item.assert_called_once()

    def test_sync_turn_feed_sends_pending_decision_with_buttons(self) -> None:
        pane = {
            "pane_id": "pane-1",
            "terminal_id": "term-1",
            "workspace_id": "workspace-1",
            "tab_id": "tab-1",
            "agent": "codex",
            "agent_status": "blocked",
        }
        key = herdres.pane_key(pane)
        entry = {"pane_key": key, "pane_id": "pane-1", "topic_id": "77"}
        state = {
            "version": 1,
            "enabled": True,
            "telegram": {"chat_id": "-1001", "general_thread_id": "1", "owner_user_ids": ["42"]},
            "panes": {key: entry},
        }
        pane_turn = Mock(
            return_value={
                "available": True,
                "complete": False,
                "awaiting_input": True,
                "turn_id": "turn-2",
                "user_text": "Choose an implementation path.",
                "pending_decision": {
                    "decision_id": "turn-2:decision-1",
                    "prompt": "Which path should I take?",
                    "options": [
                        {"id": "fast", "label": "Patch minimal path", "send_text": "1"},
                        {"id": "full", "label": "Build full path", "send_text": "2"},
                    ],
                },
            }
        )
        send_feed_item = Mock(return_value={"ok": True, "message_id": "1001"})

        with patch.multiple(
            herdres,
            load_dotenv=Mock(),
            load_state=Mock(return_value=state),
            save_state=Mock(),
            pane_list=Mock(return_value=[pane]),
            preflight_is_fresh=Mock(return_value=True),
            pane_turn=pane_turn,
            pane_feed_output=Mock(return_value="Question\nShould not be parsed"),
            send_feed_item=send_feed_item,
            TURN_FEED_ENABLED=True,
            LIVE_CARD_ENABLED=False,
        ):
            result = herdres.sync_once()

        self.assertTrue(result["changed"])
        send_feed_item.assert_called_once()
        sent_item = send_feed_item.call_args.args[1]
        self.assertEqual(sent_item["kind"], "decision")
        self.assertEqual(entry["last_clean_kind"], "decision")
        self.assertEqual(entry["active_prompt"]["decision_id"], "turn-2:decision-1")
        self.assertEqual(entry["active_prompt"]["options"][1]["send_text"], "2")
        reply_markup = send_feed_item.call_args.kwargs["reply_markup"]
        self.assertEqual(reply_markup["inline_keyboard"][1][0]["callback_data"], f"herdr:c:{sent_item['prompt_id']}:full")
        self.assertIn("Which path should I take?", entry["last_clean_text"])

    def test_sync_turn_feed_falls_back_to_visible_choice_prompt(self) -> None:
        pane = {
            "pane_id": "pane-1",
            "terminal_id": "term-1",
            "workspace_id": "workspace-1",
            "tab_id": "tab-1",
            "agent": "claude",
            "agent_status": "idle",
        }
        key = herdres.pane_key(pane)
        entry = {"pane_key": key, "pane_id": "pane-1", "topic_id": "77"}
        state = {
            "version": 1,
            "enabled": True,
            "telegram": {"chat_id": "-1001", "general_thread_id": "1", "owner_user_ids": ["42"]},
            "panes": {key: entry},
        }
        raw = """Codex thinks your real objection is register, not raw word count. Which is it?

❯ 1. Mostly register/tone
     A short explainer is still bad.
  2. Mostly length
     You want shorter by default.
  3. Both, equally
     Length and register both matter.
  4. Type something.
"""
        send_feed_item = Mock(return_value={"ok": True, "message_id": "1002"})

        with patch.multiple(
            herdres,
            load_dotenv=Mock(),
            load_state=Mock(return_value=state),
            save_state=Mock(),
            pane_list=Mock(return_value=[pane]),
            preflight_is_fresh=Mock(return_value=True),
            pane_turn=Mock(return_value={"available": False, "reason": "no_unique_claude_session_match"}),
            pane_output=Mock(return_value=raw),
            send_feed_item=send_feed_item,
            TURN_FEED_ENABLED=True,
            LIVE_CARD_ENABLED=False,
        ):
            result = herdres.sync_once()

        self.assertEqual(result["feed_sent"], 1)
        sent_item = send_feed_item.call_args.args[1]
        self.assertEqual(sent_item["kind"], "choices")
        self.assertEqual(sent_item["title"], "Decision needed")
        self.assertEqual(len(sent_item["options"]), 4)
        self.assertEqual(entry["last_clean_kind"], "choices")
        self.assertIn("active_prompt", entry)

    def test_report_command_turn_feed_uses_pane_turn_not_legacy_parser(self) -> None:
        pane = {
            "pane_id": "pane-1",
            "terminal_id": "term-1",
            "workspace_id": "workspace-1",
            "tab_id": "tab-1",
            "agent": "claude",
            "agent_status": "done",
        }
        entry = {"pane_id": "pane-1", "topic_id": "77"}
        state = {
            "version": 1,
            "telegram": {"chat_id": "-1001", "general_thread_id": "1", "owner_user_ids": ["42"]},
            "panes": {"pane-1": entry},
        }
        pane_turn = Mock(
            return_value={
                "available": True,
                "complete": True,
                "turn_id": "turn-1",
                "user_text": "What happened?",
                "assistant_final_text": "Final answer only.",
            }
        )
        pane_feed_output = Mock(return_value="Question\nShould not be parsed")
        send_feed_item = Mock(return_value={"ok": True, "message_id": "999"})

        with patch.multiple(
            herdres,
            load_dotenv=Mock(),
            load_state=Mock(return_value=state),
            save_state=Mock(),
            pane_by_id=Mock(return_value=pane),
            pane_turn=pane_turn,
            pane_feed_output=pane_feed_output,
            send_feed_item=send_feed_item,
            TURN_FEED_ENABLED=True,
        ):
            result = herdres.command_reply(
                {"chat_id": "-1001", "topic_id": "77", "user_id": "42", "text": "/report"}
            )

        self.assertTrue(result["handled"])
        self.assertEqual(result["reply"], "")
        pane_turn.assert_called_once_with("pane-1")
        pane_feed_output.assert_not_called()
        send_feed_item.assert_called_once()
        sent_item = send_feed_item.call_args.args[1]
        self.assertEqual(sent_item["kind"], "turn")
        self.assertEqual(entry["last_clean_kind"], "turn")
        self.assertIn("Final answer only.", entry["last_clean_text"])

    def test_report_command_turn_feed_unavailable_does_not_parse_pane_output(self) -> None:
        pane = {
            "pane_id": "pane-1",
            "terminal_id": "term-1",
            "workspace_id": "workspace-1",
            "tab_id": "tab-1",
            "agent": "claude",
            "agent_status": "done",
        }
        entry = {"pane_id": "pane-1", "topic_id": "77"}
        state = {
            "version": 1,
            "telegram": {"chat_id": "-1001", "general_thread_id": "1", "owner_user_ids": ["42"]},
            "panes": {"pane-1": entry},
        }
        pane_turn = Mock(return_value={"available": False, "reason": "no_structured_turn_source"})
        pane_feed_output = Mock(return_value="HERDRES_REPORT_START\nFallback\n- Do not parse this.\nHERDRES_REPORT_END")
        send_feed_item = Mock(return_value={"ok": True})

        with patch.multiple(
            herdres,
            load_dotenv=Mock(),
            load_state=Mock(return_value=state),
            save_state=Mock(),
            pane_by_id=Mock(return_value=pane),
            pane_turn=pane_turn,
            pane_feed_output=pane_feed_output,
            send_feed_item=send_feed_item,
            TURN_FEED_ENABLED=True,
        ):
            result = herdres.command_reply(
                {"chat_id": "-1001", "topic_id": "77", "user_id": "42", "text": "/status"}
            )

        self.assertTrue(result["handled"])
        self.assertEqual(result["reply"], "No structured turn is available yet.")
        self.assertFalse(entry["last_turn_available"])
        self.assertEqual(entry["last_turn_reason"], "no_structured_turn_source")
        pane_turn.assert_called_once_with("pane-1")
        pane_feed_output.assert_not_called()
        send_feed_item.assert_not_called()
        self.assertNotIn("last_clean_hash", entry)

    def test_turn_feed_hash_includes_turn_pair(self) -> None:
        first = herdres.make_turn_feed_item(
            {
                "available": True,
                "complete": True,
                "turn_id": "turn-1",
                "user_text": "Prompt A",
                "assistant_final_text": "Final",
            }
        )
        second = herdres.make_turn_feed_item(
            {
                "available": True,
                "complete": True,
                "turn_id": "turn-1",
                "user_text": "Prompt B",
                "assistant_final_text": "Final",
            }
        )

        assert first is not None and second is not None
        self.assertNotEqual(
            herdres.clean_feed_hash(first, include_render_version=False),
            herdres.clean_feed_hash(second, include_render_version=False),
        )

    def test_bridge_defaults_match_canonical_herdres_paths(self) -> None:
        module_path = Path(__file__).resolve().parents[1] / "herdr_topic_bridge.py"
        spec = importlib.util.spec_from_file_location("herdr_topic_bridge", module_path)
        bridge = importlib.util.module_from_spec(spec)
        assert spec and spec.loader
        spec.loader.exec_module(bridge)

        self.assertEqual(bridge.DEFAULT_STATE, Path.home() / ".local/share/herdres/state.json")
        self.assertEqual(bridge.DEFAULT_SCRIPT, Path.home() / ".local/bin/herdres")

    def test_plugin_manifest_hooks_herdres_event(self) -> None:
        import tomllib

        manifest = Path(__file__).resolve().parents[1] / "herdres-plugin" / "herdr-plugin.toml"
        data = tomllib.loads(manifest.read_text(encoding="utf-8"))

        self.assertEqual(data["id"], "gaijinjoe.herdres")
        self.assertEqual(data["min_herdr_version"], "0.7.0")
        self.assertIn({"on": "pane.agent_status_changed", "command": ["herdres", "event"]}, data["events"])
        commands = {action["id"]: action["command"] for action in data["actions"]}
        self.assertEqual(commands["enable"], ["herdres", "plugin-enable"])
        self.assertEqual(commands["disable"], ["herdres", "plugin-disable"])

    def test_plugin_enable_flag_is_separate_from_global_enabled(self) -> None:
        state = {"version": 1, "enabled": False, "plugin_event_enabled": True, "telegram": {}, "panes": {}}

        with patch.multiple(
            herdres,
            load_dotenv=Mock(),
            load_state=Mock(return_value=state),
            save_state=Mock(),
        ):
            result = herdres.plugin_enable_once(False)

        self.assertTrue(result["ok"])
        self.assertFalse(state["enabled"])
        self.assertFalse(state["plugin_event_enabled"])

    def test_event_noops_when_plugin_event_has_no_pane_id(self) -> None:
        state = {"version": 1, "enabled": True, "plugin_event_enabled": True, "telegram": {}, "panes": {}}
        pane_by_id = Mock()
        preflight_for_event = Mock()
        sync_pane_once = Mock()

        with patch.dict(herdres.os.environ, {"HERDR_PLUGIN_EVENT_JSON": "{}"}, clear=False), patch.multiple(
            herdres,
            load_dotenv=Mock(),
            load_state=Mock(return_value=state),
            save_state=Mock(),
            pane_by_id=pane_by_id,
            preflight_for_event=preflight_for_event,
            sync_pane_once=sync_pane_once,
        ):
            result = herdres.event_once()

        self.assertTrue(result["ok"])
        self.assertFalse(result["changed"])
        self.assertIn("no pane id", result["message"])
        pane_by_id.assert_not_called()
        preflight_for_event.assert_not_called()
        sync_pane_once.assert_not_called()

    def test_event_noops_for_unknown_pane(self) -> None:
        state = {
            "version": 1,
            "enabled": True,
            "plugin_event_enabled": True,
            "telegram": {"chat_id": "-1001"},
            "panes": {},
        }
        event_json = herdres.json.dumps({"pane_id": "pane-missing"})
        preflight_for_event = Mock()
        sync_pane_once = Mock()

        with patch.dict(herdres.os.environ, {"HERDR_PLUGIN_EVENT_JSON": event_json}, clear=False), patch.multiple(
            herdres,
            load_dotenv=Mock(),
            load_state=Mock(return_value=state),
            save_state=Mock(),
            pane_by_id=Mock(return_value=None),
            preflight_for_event=preflight_for_event,
            sync_pane_once=sync_pane_once,
        ):
            result = herdres.event_once()

        self.assertTrue(result["ok"])
        self.assertFalse(result["changed"])
        self.assertEqual(result["pane_id"], "pane-missing")
        preflight_for_event.assert_not_called()
        sync_pane_once.assert_not_called()

    def test_event_pane_id_does_not_use_generic_resource_id(self) -> None:
        event = {"resource": {"id": "not-a-pane"}, "payload": {"status": "done"}}

        self.assertEqual(herdres.event_pane_id({}, event), "")

    def test_event_reconciles_only_changed_pane_with_turn_only_mode(self) -> None:
        state = {
            "version": 1,
            "enabled": True,
            "plugin_event_enabled": True,
            "telegram": {"chat_id": "-1001", "general_thread_id": "1", "owner_user_ids": ["42"]},
            "panes": {},
        }
        pane = {
            "pane_id": "pane-1",
            "terminal_id": "term-1",
            "workspace_id": "workspace-1",
            "tab_id": "tab-1",
            "agent": "codex",
            "agent_status": "done",
        }
        event_json = herdres.json.dumps({"pane": {"pane_id": "pane-1"}})
        sync_pane_once = Mock(return_value=True)

        with patch.dict(herdres.os.environ, {"HERDR_PLUGIN_EVENT_JSON": event_json}, clear=False), patch.multiple(
            herdres,
            load_dotenv=Mock(),
            load_state=Mock(return_value=state),
            save_state=Mock(),
            pane_by_id=Mock(return_value=pane),
            preflight_for_event=Mock(return_value=(True, "")),
            sync_pane_once=sync_pane_once,
        ):
            result = herdres.event_once()

        self.assertTrue(result["ok"])
        self.assertTrue(result["changed"])
        self.assertEqual(result["pane_id"], "pane-1")
        args, kwargs = sync_pane_once.call_args
        self.assertIs(args[0], state)
        self.assertEqual(args[3], pane)
        self.assertTrue(kwargs["turn_only"])

    def test_event_retries_done_status_to_settle_turn_feed(self) -> None:
        state = {
            "version": 1,
            "enabled": True,
            "plugin_event_enabled": True,
            "telegram": {"chat_id": "-1001", "general_thread_id": "1", "owner_user_ids": ["42"]},
            "panes": {},
        }
        pane = {
            "pane_id": "pane-1",
            "terminal_id": "term-1",
            "workspace_id": "workspace-1",
            "tab_id": "tab-1",
            "agent": "codex",
            "agent_status": "done",
        }
        event_json = herdres.json.dumps({"pane": {"pane_id": "pane-1"}, "agent_status": "done"})
        sync_pane_once = Mock()

        def wrapped_sync(*args, **kwargs):
            if sync_pane_once.call_count == 1:
                return False
            args[4]["feed_sends"] = args[4].get("feed_sends", 0) + 1
            args[4]["sends"] = args[4].get("sends", 0) + 1
            return True

        with patch.dict(herdres.os.environ, {"HERDR_PLUGIN_EVENT_JSON": event_json}, clear=False), patch.multiple(
            herdres,
            load_dotenv=Mock(),
            load_state=Mock(return_value=state),
            save_state=Mock(),
            pane_by_id=Mock(return_value=pane),
            preflight_for_event=Mock(return_value=(True, "")),
            sync_pane_once=sync_pane_once,
            EVENT_SETTLE_SECONDS=0.05,
            EVENT_SETTLE_INTERVAL_SECONDS=0.01,
        ):
            sync_pane_once.side_effect = wrapped_sync
            result = herdres.event_once()

        self.assertTrue(result["ok"])
        self.assertTrue(result["changed"])
        self.assertEqual(result["feed_sent"], 1)
        self.assertEqual(result["attempts"], 2)
        self.assertEqual(sync_pane_once.call_count, 2)

    def test_event_keeps_settling_after_initial_turn_unavailable(self) -> None:
        pane = {
            "pane_id": "pane-1",
            "terminal_id": "term-1",
            "workspace_id": "workspace-1",
            "tab_id": "tab-1",
            "agent": "codex",
            "agent_status": "done",
        }
        key = herdres.pane_key(pane)
        state = {
            "version": 1,
            "enabled": True,
            "plugin_event_enabled": True,
            "telegram": {"chat_id": "-1001", "general_thread_id": "1", "owner_user_ids": ["42"]},
            "panes": {
                key: {
                    "pane_key": key,
                    "pane_id": "pane-1",
                    "topic_id": "77",
                    "last_topic_verified_at": herdres.utc_now(),
                }
            },
        }
        event_json = herdres.json.dumps({"pane": {"pane_id": "pane-1"}, "agent_status": "done"})
        sync_pane_once = Mock()

        def wrapped_sync(*args, **kwargs):
            entry = args[0]["panes"][key]
            if sync_pane_once.call_count == 1:
                entry["last_turn_available"] = False
                return True
            args[4]["feed_sends"] = args[4].get("feed_sends", 0) + 1
            args[4]["sends"] = args[4].get("sends", 0) + 1
            entry["last_turn_available"] = True
            return True

        with patch.dict(herdres.os.environ, {"HERDR_PLUGIN_EVENT_JSON": event_json}, clear=False), patch.multiple(
            herdres,
            load_dotenv=Mock(),
            load_state=Mock(return_value=state),
            save_state=Mock(),
            pane_by_id=Mock(return_value=pane),
            preflight_for_event=Mock(return_value=(True, "")),
            sync_pane_once=sync_pane_once,
            EVENT_SETTLE_SECONDS=0.05,
            EVENT_SETTLE_INTERVAL_SECONDS=0.01,
        ):
            sync_pane_once.side_effect = wrapped_sync
            result = herdres.event_once()

        self.assertTrue(result["ok"])
        self.assertEqual(result["feed_sent"], 1)
        self.assertEqual(result["attempts"], 2)
        self.assertEqual(sync_pane_once.call_count, 2)

    def test_event_turn_feed_does_not_read_pane_output(self) -> None:
        pane = {
            "pane_id": "pane-1",
            "terminal_id": "term-1",
            "workspace_id": "workspace-1",
            "tab_id": "tab-1",
            "agent": "codex",
            "agent_status": "done",
        }
        key = herdres.pane_key(pane)
        state = {
            "version": 1,
            "enabled": True,
            "plugin_event_enabled": True,
            "telegram": {"chat_id": "-1001", "general_thread_id": "1", "owner_user_ids": ["42"]},
            "panes": {
                key: {
                    "pane_key": key,
                    "pane_id": "pane-1",
                    "topic_id": "77",
                    "last_topic_verified_at": herdres.utc_now(),
                }
            },
        }
        event_json = herdres.json.dumps({"pane": {"pane_id": "pane-1"}})
        pane_feed_output = Mock(return_value="HERDRES_REPORT_START\nLeak\n- should not read\nHERDRES_REPORT_END")

        with patch.dict(herdres.os.environ, {"HERDR_PLUGIN_EVENT_JSON": event_json}, clear=False), patch.multiple(
            herdres,
            load_dotenv=Mock(),
            load_state=Mock(return_value=state),
            save_state=Mock(),
            pane_by_id=Mock(return_value=pane),
            preflight_for_event=Mock(return_value=(True, "")),
            pane_turn=Mock(return_value={"available": False, "reason": "no_structured_turn_source"}),
            pane_feed_output=pane_feed_output,
            TURN_FEED_ENABLED=True,
            LIVE_CARD_ENABLED=False,
        ):
            result = herdres.event_once()

        self.assertTrue(result["ok"])
        self.assertTrue(result["changed"])
        pane_feed_output.assert_not_called()

    def test_event_uses_blocking_lock_from_cli(self) -> None:
        lock = Mock(return_value={"ok": True, "changed": False})
        with patch.object(herdres.sys, "argv", ["herdres", "event"]), patch.object(herdres, "with_lock", lock):
            result = herdres.main()

        self.assertEqual(result, 0)
        self.assertTrue(lock.call_args.kwargs["blocking"])


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
