from __future__ import annotations

import subprocess
import unittest
from types import SimpleNamespace
from unittest.mock import Mock, patch

import herdres


class CouncilGitmootOutputTests(unittest.TestCase):
    def _council_pane(self, *, cwd: str = "/home/smith/.gitmoot/runs/delegations/root_1/d1/work") -> dict:
        return {
            "pane_id": "pane-1",
            "terminal_id": "term-1",
            "workspace_id": "gm-local-as-w1",
            "tab_id": "tab-1",
            "agent": "gm-local-as",
            "agent_status": "idle",
            "label": "council-codex · d1 · main",
            "foreground_cwd": cwd,
        }

    def _sync_caps(self) -> tuple[dict, dict]:
        counters = {"creates": 0, "sends": 0, "feed_sends": 0, "marker_sends": 0, "verifies": 0, "renames": 0}
        caps = {"max_creates": 0, "max_sends": 5, "max_feed_sends": 5, "max_marker_sends": 0, "max_verifies": 0}
        return counters, caps

    def test_valid_council_delegation_cwd_parses_root_and_delegation(self) -> None:
        pane = self._council_pane(cwd="/tmp/.gitmoot/workflows/delegations/root_42/d1/repo")

        self.assertEqual(herdres.gitmoot_delegation_ref_from_pane(pane), ("root_42", "d1"))

    def test_ambiguous_non_gitmoot_and_non_council_cwds_do_not_parse(self) -> None:
        cases = [
            self._council_pane(cwd="/tmp/.gitmoot/delegations/root_1/d1/delegations/root_2/d2"),
            self._council_pane(cwd="/tmp/project/delegations/root_1/d1"),
            {**self._council_pane(cwd="/tmp/.gitmoot/delegations/root_1/d1"), "label": "regular shell"},
        ]

        for pane in cases:
            with self.subTest(cwd=pane["foreground_cwd"], label=pane.get("label")):
                self.assertIsNone(herdres.gitmoot_delegation_ref_from_pane(pane))

    def test_workflow_event_filter_matches_lifecycle_events_not_model_prose(self) -> None:
        for event in (
            "advance_started",
            "advance_completed",
            "advance_failed",
            "delegation_worktree_created",
            "delegation_worktree_removed",
        ):
            with self.subTest(event=event):
                self.assertTrue(herdres.is_gitmoot_workflow_event(event))
                self.assertTrue(herdres.is_gitmoot_workflow_event({"event": event}))
                self.assertTrue(herdres.is_gitmoot_workflow_event(f'{{"event":"{event}"}}'))
                self.assertFalse(herdres.is_gitmoot_workflow_event(f"The model discussed {event} as prose."))

    def test_gitmoot_job_show_fails_closed(self) -> None:
        cases = [
            subprocess.TimeoutExpired(["gitmoot"], 3),
            SimpleNamespace(returncode=1, stdout='{"ok":true}'),
            SimpleNamespace(returncode=0, stdout="not-json"),
            FileNotFoundError("gitmoot"),
        ]

        for outcome in cases:
            with self.subTest(outcome=type(outcome).__name__):
                run_cmd = Mock(side_effect=outcome) if isinstance(outcome, Exception) else Mock(return_value=outcome)
                with patch.object(herdres, "run_cmd", run_cmd):
                    self.assertIsNone(herdres.gitmoot_job_show("root_1/delegation/d1", timeout=0.01))

    def test_gitmoot_job_model_content_prefers_and_filters_model_text(self) -> None:
        self.assertEqual(
            herdres.gitmoot_job_model_content({"job": {"artifact_body": "advance_started\nArtifact answer", "summary": "Summary answer"}}),
            "Artifact answer",
        )
        self.assertEqual(
            herdres.gitmoot_job_model_content({"result": {"job": {"summary": "advance_completed\nSummary answer"}}}),
            "Summary answer",
        )
        self.assertEqual(
            herdres.gitmoot_job_model_content({
                "job": {
                    "raw_outputs": [
                        {"event": "advance_failed", "message": "ignored workflow row"},
                        "delegation_worktree_created: /tmp/worktree",
                        "Raw answer",
                        {"summary": "delegation_worktree_removed\nDict answer"},
                    ]
                }
            }),
            "Raw answer\nDict answer",
        )

    def test_sync_posts_gitmoot_council_output_once_via_resolved_seat_bot(self) -> None:
        pane = self._council_pane(cwd="/tmp/.gitmoot/workflows/delegations/root_1/d1/repo")
        telegram = {"managed_bots": {"codex": {"token": "CODEX_TOKEN", "enabled": True}}}
        job = {"result": {"job": {"job_ref": "root_1/delegation/d1", "revision": "rev1", "artifact_body": "Council answer"}}}
        send_feed_item = Mock(return_value={"ok": True, "message_id": "2001"})
        gitmoot_job_show = Mock(return_value=job)

        with patch.object(herdres, "per_agent_topics_enabled", Mock(return_value=False)):
            space_key = herdres.space_key(pane)
            pane_key = herdres.pane_key(pane)
            state = {
                "version": 1,
                "telegram": telegram,
                "spaces": {
                    space_key: {
                        "space_key": space_key,
                        "topic_id": "77",
                        "topic_name": herdres.council_space_topic_name(pane, space_key),
                        "origin": "council",
                        "voice_mode": "per_agent",
                        "pane_keys": [pane_key, "sibling-pane"],
                        "message_routes": {},
                    }
                },
                "panes": {"sibling-pane": {"pane_key": "sibling-pane", "pane_thread_name": "council-claude", "last_council_job_ref": "sibling-marker"}},
            }
            counters, caps = self._sync_caps()
            with patch.multiple(
                herdres,
                gitmoot_job_show=gitmoot_job_show,
                extract_turn_feed_item=Mock(return_value=None),
                send_pending_prompt_message=Mock(return_value={"changed": False, "topic_missing": False, "pane_root_missing": False}),
                send_feed_item=send_feed_item,
                save_state=Mock(),
                apply_api_error_warning=Mock(return_value={"topic_missing": False, "changed": False}),
                fold_superseded_turns=Mock(return_value=False),
                flush_pending_plan_doc=Mock(return_value=False),
                flush_pending_speech_reply=Mock(return_value=False),
                pane_root_messages_enabled=Mock(return_value=False),
                CLEAN_FEED_ENABLED=True,
                TURN_FEED_ENABLED=True,
                LIVE_CARD_ENABLED=False,
                STATUS_MARKER_ENABLED=False,
                STATUS_ICON_ENABLED=False,
            ):
                first_changed = herdres.sync_pane_once(state, "-1001", telegram, pane, counters, caps)
                second_changed = herdres.sync_pane_once(state, "-1001", telegram, pane, counters, caps)

        self.assertTrue(first_changed)
        self.assertTrue(second_changed)
        send_feed_item.assert_called_once()
        sent_item = send_feed_item.call_args.args[1]
        self.assertEqual(sent_item["assistant_final_text"], "Council answer")
        self.assertEqual(send_feed_item.call_args.kwargs["thread_id"], "77")
        self.assertEqual(send_feed_item.call_args.kwargs["api_token"], "CODEX_TOKEN")
        self.assertEqual(gitmoot_job_show.call_args_list[0].args, ("root_1/delegation/d1",))
        entry = state["panes"][pane_key]
        self.assertTrue(entry["last_council_job_ref"].startswith("root_1/delegation/d1@rev1:"))
        self.assertEqual(state["panes"]["sibling-pane"]["last_council_job_ref"], "sibling-marker")
        self.assertEqual(counters["feed_sends"], 1)

    def test_sync_ignores_gitmoot_query_failure_without_posting(self) -> None:
        pane = self._council_pane(cwd="/tmp/.gitmoot/workflows/delegations/root_1/d1/repo")
        telegram = {"managed_bots": {"codex": {"token": "CODEX_TOKEN", "enabled": True}}}
        with patch.object(herdres, "per_agent_topics_enabled", Mock(return_value=False)):
            space_key = herdres.space_key(pane)
            pane_key = herdres.pane_key(pane)
            state = {
                "version": 1,
                "telegram": telegram,
                "spaces": {
                    space_key: {
                        "space_key": space_key,
                        "topic_id": "77",
                        "topic_name": herdres.council_space_topic_name(pane, space_key),
                        "origin": "council",
                        "voice_mode": "per_agent",
                        "pane_keys": [pane_key],
                        "message_routes": {},
                    }
                },
                "panes": {},
            }
            counters, caps = self._sync_caps()
            send_feed_item = Mock()
            with patch.multiple(
                herdres,
                gitmoot_job_show=Mock(return_value=None),
                extract_turn_feed_item=Mock(return_value=None),
                send_pending_prompt_message=Mock(return_value={"changed": False, "topic_missing": False, "pane_root_missing": False}),
                send_feed_item=send_feed_item,
                save_state=Mock(),
                apply_api_error_warning=Mock(return_value={"topic_missing": False, "changed": False}),
                fold_superseded_turns=Mock(return_value=False),
                flush_pending_plan_doc=Mock(return_value=False),
                flush_pending_speech_reply=Mock(return_value=False),
                pane_root_messages_enabled=Mock(return_value=False),
                CLEAN_FEED_ENABLED=True,
                TURN_FEED_ENABLED=True,
                LIVE_CARD_ENABLED=False,
                STATUS_MARKER_ENABLED=False,
                STATUS_ICON_ENABLED=False,
            ):
                changed = herdres.sync_pane_once(state, "-1001", telegram, pane, counters, caps)

        self.assertTrue(changed)
        send_feed_item.assert_not_called()
        self.assertNotIn("last_council_job_ref", state["panes"][pane_key])

    def test_clear_clean_feed_state_removes_council_job_ref(self) -> None:
        entry = {"last_clean_hash": "hash", "last_council_job_ref": "marker", "keep": "value"}

        herdres.clear_clean_feed_state(entry)

        self.assertNotIn("last_council_job_ref", entry)
        self.assertNotIn("last_clean_hash", entry)
        self.assertEqual(entry["keep"], "value")

    def test_gm_local_as_council_pane_uses_seat_label(self) -> None:
        pane = {"agent": "gm-local-as", "label": "council-codex · d1 · main", "pane_id": "pane-1", "terminal_id": "t", "workspace_id": "w", "tab_id": "tab"}
        state = {"panes": {herdres.pane_key(pane): {"pane_thread_name": "Council Codex"}}}

        self.assertEqual(herdres.council_display_label_for_entry_like(pane, pane), "Council Codex")
        self.assertEqual(herdres.pane_thread_name(pane), "Council Codex")
        self.assertEqual(herdres.pane_agent_status_label(pane), "Council Codex")
        self.assertEqual(herdres.pinned_status_pane_label(state, pane), "Council Codex")
        self.assertIn("Agent: Council Codex", herdres.format_status(pane))
        self.assertNotIn("Gm Local", herdres.format_status(pane))

    def test_council_per_agent_onboarding_suppressed_but_personal_space_sends(self) -> None:
        council_pane = self._council_pane()
        personal_pane = {
            "pane_id": "pane-2",
            "terminal_id": "term-2",
            "workspace_id": "personal-w1",
            "tab_id": "tab-2",
            "agent": "codex",
            "agent_status": "idle",
            "label": "Codex",
            "foreground_cwd": "/tmp/personal",
        }
        state = {"version": 1, "spaces": {}, "panes": {}}
        send_message = Mock(return_value="9001")

        with patch.object(herdres, "per_agent_topics_enabled", Mock(return_value=False)), patch.multiple(
            herdres,
            create_topic=Mock(side_effect=["77", "88"]),
            send_message=send_message,
            save_state=Mock(),
        ):
            council_space, _ = herdres.ensure_space_topic(state, "-1001", {}, council_pane, {"creates": 0}, 5)
            send_message.assert_not_called()
            personal_space, _ = herdres.ensure_space_topic(state, "-1001", {}, personal_pane, {"creates": 0}, 5)

        self.assertEqual(council_space["origin"], "council")
        self.assertEqual(council_space["voice_mode"], "per_agent")
        self.assertNotIn("onboarding_status", council_space)
        send_message.assert_called_once()
        self.assertEqual(personal_space["origin"], "personal")
        self.assertEqual(personal_space["onboarding_status"], "pending")
        self.assertEqual(send_message.call_args.kwargs["thread_id"], "88")


if __name__ == "__main__":
    unittest.main()
