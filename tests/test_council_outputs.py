from __future__ import annotations

import copy
import subprocess
import unittest
from types import SimpleNamespace
from unittest.mock import Mock, patch

import herdres


class GitmootCouncilHelperTests(unittest.TestCase):
    def test_delegation_ref_requires_one_council_gitmoot_foreground_cwd(self) -> None:
        pane = {
            "origin": "council",
            "council_seat": "codex",
            "foreground_cwd": "/home/smith/.gitmoot/worktrees/luminexord--herdres/delegations/root-123/delegation-456/src",
        }

        self.assertEqual(herdres.gitmoot_delegation_ref_from_pane(pane), ("root-123", "delegation-456"))
        self.assertIsNone(herdres.gitmoot_delegation_ref_from_pane({**pane, "foreground_cwd": "/home/smith/project"}))
        self.assertIsNone(
            herdres.gitmoot_delegation_ref_from_pane(
                {**pane, "foreground_cwd": "relative/.gitmoot/worktrees/a--b/delegations/root/delegation"}
            )
        )
        self.assertIsNone(
            herdres.gitmoot_delegation_ref_from_pane(
                {
                    **pane,
                    "foreground_cwd": (
                        "/x/.gitmoot/worktrees/a--b/delegations/root-a/delegation-a/"
                        "nested/.gitmoot/worktrees/c--d/delegations/root-b/delegation-b"
                    ),
                }
            )
        )
        self.assertIsNone(
            herdres.gitmoot_delegation_ref_from_pane(
                {"foreground_cwd": pane["foreground_cwd"], "agent": "codex", "label": "Plain Codex"}
            )
        )

    def test_job_show_returns_dict_and_swallows_gitmoot_failures(self) -> None:
        good = SimpleNamespace(returncode=0, stdout='{"ok": true, "id": "root"}', stderr="")
        with patch.object(herdres, "run_cmd", Mock(return_value=good)) as run_cmd:
            self.assertEqual(herdres.gitmoot_job_show("root/delegation/del", timeout=3), {"ok": True, "id": "root"})
        run_cmd.assert_called_once_with(["gitmoot", "job", "show", "root/delegation/del", "--json"], timeout=3)

        cases = [
            SimpleNamespace(returncode=1, stdout='{"ok": false}', stderr="boom"),
            SimpleNamespace(returncode=0, stdout="not json", stderr=""),
            SimpleNamespace(returncode=0, stdout="[]", stderr=""),
            subprocess.TimeoutExpired(["gitmoot"], 8),
            FileNotFoundError("gitmoot"),
        ]
        for case in cases:
            with self.subTest(case=type(case).__name__):
                side_effect = case if isinstance(case, BaseException) else None
                return_value = None if side_effect else case
                with patch.object(herdres, "run_cmd", Mock(return_value=return_value, side_effect=side_effect)):
                    self.assertIsNone(herdres.gitmoot_job_show("root/delegation/del"))

    def test_job_model_content_prefers_artifact_then_summary_then_raw_outputs(self) -> None:
        self.assertEqual(
            herdres.gitmoot_job_model_content(
                {
                    "artifact_body": "Artifact wins.",
                    "summary": "Summary loses.",
                    "raw_outputs": [{"text": "Raw loses."}],
                }
            ),
            "Artifact wins.",
        )
        self.assertEqual(
            herdres.gitmoot_job_model_content(
                {
                    "nested": {"summary": "Summary fallback."},
                    "raw_outputs": [{"text": "Raw loses."}],
                }
            ),
            "Summary fallback.",
        )
        self.assertEqual(
            herdres.gitmoot_job_model_content(
                {"raw_outputs": [{"text": "First raw."}, {"content": "Second raw."}]}
            ),
            "First raw.\n\nSecond raw.",
        )

    def test_job_model_content_ignores_workflow_events(self) -> None:
        self.assertEqual(
            herdres.gitmoot_job_model_content(
                {
                    "artifact_body": '{"event":"advance_completed"}',
                    "summary": "delegation_started",
                    "raw_outputs": [
                        {"event": "advance_started", "message": "noise"},
                        {"text": "Useful council output."},
                    ],
                }
            ),
            "Useful council output.",
        )


class CouncilLabelAndOnboardingTests(unittest.TestCase):
    def test_gm_local_council_labels_render_as_seat_names(self) -> None:
        codex_pane = {"origin": "council", "agent": "gm-local-as", "label": "Gm Local", "council_seat": "codex"}
        kimi_pane = {"origin": "council", "agent": "gm-local-as", "label": "Gm Local", "seat": "kimi"}

        self.assertEqual(herdres.council_display_label_for_entry_like(codex_pane), "Council Codex")
        self.assertEqual(herdres.pane_thread_name(codex_pane), "Council Codex")
        self.assertEqual(herdres.topic_name_for_pane(codex_pane), "Council Codex")
        self.assertEqual(herdres.agent_topic_name_for_pane(codex_pane), "Council Codex")
        with patch.object(herdres, "per_agent_topics_enabled", Mock(return_value=True)):
            self.assertEqual(herdres.space_name_for_pane(codex_pane), "Council Codex")
        self.assertEqual(herdres.managed_bot_kind_for_entry({}, codex_pane), "codex")
        self.assertNotEqual(herdres.pane_thread_name(codex_pane), "Gm Local")
        self.assertEqual(herdres.pane_thread_name(kimi_pane), "Council Kimi")

    def test_council_per_agent_space_suppresses_onboarding_card_only_for_council(self) -> None:
        council_state = {
            "spaces": {
                "workspace:work": {
                    "space_key": "workspace:work",
                    "pane_keys": [],
                    "voice_mode": "per_agent",
                    "origin": "council",
                }
            },
            "panes": {},
        }
        council_pane = {
            "pane_id": "p1",
            "terminal_id": "t1",
            "workspace_id": "work",
            "tab_id": "tab1",
            "agent": "gm-local-as",
            "label": "Gm Local",
            "origin": "council",
            "council_seat": "codex",
        }
        send_message = Mock(return_value="901")

        with patch.object(herdres, "create_topic", Mock(return_value="77")), patch.object(
            herdres, "send_message", send_message
        ), patch.object(herdres, "save_state", Mock()), patch.object(herdres, "per_agent_topics_enabled", Mock(return_value=False)):
            space, changed = herdres.ensure_space_topic(council_state, "-1001", {}, council_pane, {"creates": 0}, 5)

        self.assertTrue(changed)
        self.assertEqual(space["topic_id"], "77")
        self.assertNotIn("onboarding_status", space)
        self.assertNotIn("onboarding_message_id", space)
        send_message.assert_not_called()

        regular_state = {
            "spaces": {
                "workspace:work": {
                    "space_key": "workspace:work",
                    "pane_keys": [],
                    "voice_mode": "per_agent",
                }
            },
            "panes": {},
        }
        regular_pane = {
            "pane_id": "p2",
            "terminal_id": "t1",
            "workspace_id": "work",
            "tab_id": "tab1",
            "agent": "codex",
            "label": "Regular Codex",
        }
        send_message = Mock(return_value="902")

        with patch.object(herdres, "create_topic", Mock(return_value="78")), patch.object(
            herdres, "send_message", send_message
        ), patch.object(herdres, "save_state", Mock()), patch.object(herdres, "per_agent_topics_enabled", Mock(return_value=False)):
            space, changed = herdres.ensure_space_topic(regular_state, "-1001", {}, regular_pane, {"creates": 0}, 5)

        self.assertTrue(changed)
        self.assertEqual(space["onboarding_selected"], ["codex"])
        self.assertEqual(space["onboarding_status"], "pending")
        self.assertEqual(space["onboarding_message_id"], "902")
        send_message.assert_called_once()


class CouncilGitmootFallbackSyncTests(unittest.TestCase):
    def _pane(self, *, seat: str = "codex") -> dict:
        return {
            "pane_id": "pane-1",
            "terminal_id": "term-1",
            "workspace_id": "workspace-1",
            "tab_id": "tab-1",
            "agent": "gm-local-as",
            "agent_status": "done",
            "label": "Gm Local",
            "origin": "council",
            "council_seat": seat,
            "foreground_cwd": "/home/smith/.gitmoot/worktrees/luminexord--herdres/delegations/root-job/delegation-1/src",
        }

    def _state(self) -> tuple[dict, dict, str, dict, str, dict]:
        pane = self._pane()
        key = herdres.pane_key(pane)
        sibling_key = "sibling-pane-key"
        entry = {
            "pane_key": key,
            "pane_id": "pane-1",
            "space_key": "workspace:workspace-1",
            "topic_id": "77",
            "pane_root_message_id": "1001",
            "pane_thread_name": "Council Codex",
            "last_known_status": "done",
        }
        sibling = {
            "pane_key": sibling_key,
            "pane_id": "pane-2",
            "space_key": "workspace:workspace-1",
            "topic_id": "77",
            "pane_root_message_id": "1002",
            "pane_thread_name": "Council Kimi",
            "last_known_status": "done",
            "last_clean_hash": "sibling-clean-hash",
        }
        state = {
            "version": 1,
            "enabled": True,
            "telegram": {
                "chat_id": "-1001",
                "general_thread_id": "1",
                "owner_user_ids": ["42"],
                "managed_bots": {"codex": {"enabled": True, "token": "codex-token"}},
            },
            "spaces": {
                "workspace:workspace-1": {
                    "space_key": "workspace:workspace-1",
                    "topic_id": "77",
                    "topic_name": "Council Codex",
                    "pane_keys": [key, sibling_key],
                    "voice_mode": "per_agent",
                    "origin": "council",
                }
            },
            "panes": {key: entry, sibling_key: sibling},
        }
        return state, pane, key, entry, sibling_key, sibling

    def _caps(self) -> tuple[dict, dict]:
        counters = {"creates": 0, "sends": 0, "feed_sends": 0, "marker_sends": 0, "verifies": 0, "renames": 0, "icon_updates": 0}
        caps = {"max_creates": 0, "max_sends": 10, "max_feed_sends": 10, "max_marker_sends": 10, "max_verifies": 0}
        return counters, caps

    def _patch_sync(self, *, gitmoot_job_show: Mock, send_feed_item: Mock):
        return patch.multiple(
            herdres,
            gitmoot_job_show=gitmoot_job_show,
            extract_turn_feed_item=Mock(return_value=None),
            send_pending_prompt_message=Mock(return_value={"changed": False}),
            send_feed_item=send_feed_item,
            ensure_pane_root_message=Mock(return_value=(False, {"ok": True})),
            save_state=Mock(),
            MANAGED_BOTS_ENABLED=True,
            TURN_FEED_ENABLED=True,
            CLEAN_FEED_ENABLED=True,
            LIVE_CARD_ENABLED=False,
            STATUS_MARKER_ENABLED=False,
            STATUS_ICON_ENABLED=False,
            STREAMING_DRAFTS_ENABLED=False,
        )

    def test_sync_posts_gitmoot_fallback_once_through_council_seat_bot_and_dedupes(self) -> None:
        state, pane, _key, entry, _sibling_key, sibling = self._state()
        original_sibling = copy.deepcopy(sibling)
        first_job = {"revision": "rev-1", "hash": "job-hash-1", "artifact_body": "Council report complete."}
        changed_content_job = {"revision": "rev-1", "hash": "job-hash-1", "artifact_body": "Council report changed."}
        changed_revision_job = {"revision": "rev-2", "hash": "job-hash-2", "artifact_body": "Council report changed."}
        gitmoot_job_show = Mock(side_effect=[first_job, first_job, changed_content_job, changed_revision_job])
        send_feed_item = Mock(return_value={"ok": True, "message_id": "2001"})

        with self._patch_sync(gitmoot_job_show=gitmoot_job_show, send_feed_item=send_feed_item):
            counters, caps = self._caps()
            self.assertTrue(herdres.sync_pane_once(state, "-1001", state["telegram"], pane, counters, caps))
            first_marker = entry.get("last_gitmoot_job_marker")
            self.assertTrue(first_marker)
            self.assertEqual(entry.get("last_gitmoot_job_ref"), "root-job/delegation/delegation-1")
            self.assertEqual(send_feed_item.call_count, 1)

            counters, caps = self._caps()
            herdres.sync_pane_once(state, "-1001", state["telegram"], pane, counters, caps)
            self.assertEqual(entry.get("last_gitmoot_job_marker"), first_marker)
            self.assertEqual(send_feed_item.call_count, 1)

            counters, caps = self._caps()
            herdres.sync_pane_once(state, "-1001", state["telegram"], pane, counters, caps)
            changed_content_marker = entry.get("last_gitmoot_job_marker")
            self.assertNotEqual(changed_content_marker, first_marker)
            self.assertEqual(send_feed_item.call_count, 2)

            counters, caps = self._caps()
            herdres.sync_pane_once(state, "-1001", state["telegram"], pane, counters, caps)
            self.assertNotEqual(entry.get("last_gitmoot_job_marker"), changed_content_marker)
            self.assertEqual(send_feed_item.call_count, 3)

        self.assertEqual(
            [call.args[0] for call in gitmoot_job_show.call_args_list],
            [
                "root-job/delegation/delegation-1",
                "root-job/delegation/delegation-1",
                "root-job/delegation/delegation-1",
                "root-job/delegation/delegation-1",
            ],
        )
        self.assertEqual(send_feed_item.call_count, 3)
        first_item = send_feed_item.call_args_list[0].args[1]
        second_item = send_feed_item.call_args_list[1].args[1]
        third_item = send_feed_item.call_args_list[2].args[1]
        self.assertEqual(first_item.get("source"), "gitmoot_job")
        self.assertEqual(second_item.get("source"), "gitmoot_job")
        self.assertEqual(third_item.get("source"), "gitmoot_job")
        self.assertIn("Council report complete.", herdres.item_plain_text(first_item))
        self.assertIn("Council report changed.", herdres.item_plain_text(second_item))
        self.assertIn("Council report changed.", herdres.item_plain_text(third_item))
        self.assertEqual([call.kwargs["api_token"] for call in send_feed_item.call_args_list], ["codex-token"] * 3)
        self.assertEqual([call.kwargs["thread_id"] for call in send_feed_item.call_args_list], ["77"] * 3)
        self.assertEqual(entry["last_clean_bot_kind"], "codex")
        self.assertEqual(sibling, original_sibling)
        self.assertIn("last_gitmoot_job_marker", entry)
        self.assertIn("last_gitmoot_job_ref", entry)
        self.assertNotIn("last_gitmoot_job_marker", sibling)
        self.assertNotIn("last_gitmoot_job_ref", sibling)
        self.assertNotIn("last_gitmoot_job_marker", state["spaces"]["workspace:workspace-1"])
        self.assertNotIn("last_gitmoot_job_ref", state["spaces"]["workspace:workspace-1"])

    def test_sync_gitmoot_failure_posts_nothing_and_does_not_crash(self) -> None:
        state, pane, _key, entry, _sibling_key, sibling = self._state()
        original_entry = copy.deepcopy(entry)
        original_sibling = copy.deepcopy(sibling)
        gitmoot_job_show = Mock(return_value=None)
        send_feed_item = Mock(return_value={"ok": True, "message_id": "2001"})

        with self._patch_sync(gitmoot_job_show=gitmoot_job_show, send_feed_item=send_feed_item):
            counters, caps = self._caps()
            herdres.sync_pane_once(state, "-1001", state["telegram"], pane, counters, caps)

        self.assertEqual(gitmoot_job_show.call_args.args[0], "root-job/delegation/delegation-1")
        send_feed_item.assert_not_called()
        self.assertEqual(sibling, original_sibling)
        self.assertFalse([key for key in entry if "gitmoot" in key.lower()])
        self.assertEqual(entry.get("last_clean_hash"), original_entry.get("last_clean_hash"))
