from __future__ import annotations

import json
import os
import re
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

import herdres


# Keep the suite hermetic: the per-agent grouping flag is read from the
# environment at runtime, so a stray HERDR_TELEGRAM_TOPICS_PER_AGENT (exported,
# or leaked into os.environ by a load_dotenv() call reading this machine's
# herdres.env) would silently flip the default per-space assertions. Pop it for
# the duration of the module AND neutralize load_dotenv so no entry-point call
# under test can re-populate it mid-suite. (56+ tests already pass
# load_dotenv=Mock(); this just makes it impossible to forget.)
_SAVED_PER_AGENT_ENV: str | None = None
_LOAD_DOTENV_PATCH = patch.object(herdres, "load_dotenv", Mock())


def setUpModule() -> None:
    global _SAVED_PER_AGENT_ENV
    _SAVED_PER_AGENT_ENV = os.environ.pop("HERDR_TELEGRAM_TOPICS_PER_AGENT", None)
    _LOAD_DOTENV_PATCH.start()


def tearDownModule() -> None:
    _LOAD_DOTENV_PATCH.stop()
    if _SAVED_PER_AGENT_ENV is not None:
        os.environ["HERDR_TELEGRAM_TOPICS_PER_AGENT"] = _SAVED_PER_AGENT_ENV


class DocumentationRenderTests(unittest.TestCase):
    def test_readme_describes_space_topics_not_pane_topics(self) -> None:
        readme = (Path(__file__).resolve().parents[1] / "README.md").read_text(encoding="utf-8")

        self.assertIn("one Telegram forum topic per Herdr space", readme)
        self.assertIn("exactly one live pane", readme)
        self.assertIn("Reply inside a pane thread", readme)
        self.assertIn("sendMessageDraft", readme)
        self.assertIn("sendRichMessageDraft", readme)
        self.assertNotRegex(
            readme,
            r"Creates or maintains one Telegram forum topic per Herdr pane|"
            r"maps each live Herdr pane to a Telegram forum topic",
        )

    def test_env_example_documents_streaming_controls(self) -> None:
        env_example = (Path(__file__).resolve().parents[1] / ".env.example").read_text(encoding="utf-8")

        self.assertIn("HERDR_TELEGRAM_TOPICS_STREAMING=1", env_example)
        self.assertIn("HERDR_TELEGRAM_TOPICS_STREAM_MIN_INTERVAL=2", env_example)
        self.assertIn("HERDR_TELEGRAM_TOPICS_STREAM_MIN_CHARS=80", env_example)
        self.assertIn("HERDR_TELEGRAM_TOPICS_MAX_DRAFTS=8", env_example)

    def test_env_example_documents_managed_bot_controls(self) -> None:
        env_example = (Path(__file__).resolve().parents[1] / ".env.example").read_text(encoding="utf-8")

        self.assertIn("HERDR_TELEGRAM_TOPICS_MANAGED_BOTS=1", env_example)
        self.assertIn("HERDR_TELEGRAM_TOPICS_MANAGED_BOT_CODEX_PHOTO=", env_example)


class SpaceTopicStateTests(unittest.TestCase):
    def test_space_key_prefers_space_id_then_workspace_then_cwd_default(self) -> None:
        self.assertEqual(herdres.space_key({"space_id": "alpha"}), "space:alpha")
        self.assertEqual(herdres.space_key({"workspace_id": "workspace-1"}), "workspace:workspace-1")
        self.assertEqual(herdres.space_key({"foreground_cwd": "/tmp/herdres-demo"}), "cwd:herdres-demo")
        self.assertEqual(herdres.space_key({}), "default")

        self.assertEqual(herdres.space_name_for_pane({"space_name": "herdres"}), "herdres")
        self.assertEqual(
            herdres.space_name_for_pane({"workspace_id": "workspace-1"}),
            "Workspace 1",
        )

    def test_per_agent_mode_keys_per_pane_and_names_by_agent_and_folder(self) -> None:
        with patch.object(herdres, "per_agent_topics_enabled", lambda: True):
            # Grouping key is the stable pane id, regardless of workspace.
            self.assertEqual(
                herdres.space_key({"pane_id": "w6:p16", "workspace_id": "w6"}),
                "agent:w6:p16",
            )
            # Two agents in the same workspace get distinct grouping keys.
            self.assertNotEqual(
                herdres.space_key({"pane_id": "w6:p1", "workspace_id": "w6"}),
                herdres.space_key({"pane_id": "w6:p2", "workspace_id": "w6"}),
            )
            # Topic name is "<agent> · <folder>" when no manual label is set.
            self.assertEqual(
                herdres.space_name_for_pane({"agent": "claude", "foreground_cwd": "/root/herdres"}),
                "claude · herdres",
            )
            # A manual pane label wins over the agent/folder name.
            self.assertEqual(
                herdres.space_name_for_pane(
                    {"agent": "codex", "foreground_cwd": "/root/sawa", "label": "Deploy Bot"}
                ),
                "Deploy Bot",
            )
        # Flag off: per-space behavior is unchanged (patched off so this is
        # deterministic regardless of any HERDR_TELEGRAM_TOPICS_PER_AGENT in env).
        with patch.object(herdres, "per_agent_topics_enabled", lambda: False):
            self.assertEqual(herdres.space_key({"workspace_id": "workspace-1"}), "workspace:workspace-1")

    def test_reset_topic_grouping_clears_mappings_and_records_mode(self) -> None:
        state = {
            "version": 1,
            "spaces": {"workspace:workspace-1": {"space_key": "workspace:workspace-1", "topic_id": "77"}},
            "panes": {
                "pane-1": {
                    "pane_key": "pane-1",
                    "pane_id": "w6:p1",
                    "agent": "codex",
                    "topic_id": "77",
                    "space_key": "workspace:workspace-1",
                    "topic_name": "Mars",
                    "pane_root_message_id": "1001",
                    "topic_status_icon_custom_emoji_id": "emoji-1",
                    "last_prompt_message_id": "2002",
                },
            },
        }

        herdres.reset_topic_grouping(state, "agent", reason="switch")

        self.assertEqual(state["spaces"], {})
        self.assertEqual(state["topic_grouping_mode"], "agent")
        self.assertIn("topic_grouping_reset_at", state)
        entry = state["panes"]["pane-1"]
        # Topic linkage AND the caches that would suppress re-applying the icon /
        # re-posting the prompt on the fresh topic are all cleared.
        for dropped in (
            "topic_id",
            "space_key",
            "topic_name",
            "pane_root_message_id",
            "topic_status_icon_custom_emoji_id",
            "last_prompt_message_id",
        ):
            self.assertNotIn(dropped, entry)
        # Pane identity is preserved across the reset.
        self.assertEqual(entry["pane_id"], "w6:p1")
        self.assertEqual(entry["agent"], "codex")

    def test_per_agent_pane_entry_persists_cwd_and_names_space_by_folder(self) -> None:
        with patch.object(herdres, "per_agent_topics_enabled", lambda: True):
            pane = {
                "pane_id": "w6:p1",
                "terminal_id": "t1",
                "workspace_id": "w6",
                "tab_id": "w6:t1",
                "agent": "codex",
                "foreground_cwd": "/workspace/alpha",
            }
            state = {"version": 1, "spaces": {}, "panes": {}}
            _key, entry, _created = herdres.ensure_pane_entry(state, pane)
            # The fix: cwd is persisted on the entry so migrate_legacy_pane_topics()
            # can reconstruct a faithful pane_like instead of a folder-less one.
            self.assertEqual(entry["foreground_cwd"], "/workspace/alpha")
            # The per-agent space is keyed by pane id and named "<agent> · <folder>".
            space = state["spaces"].get("agent:w6:p1")
            self.assertIsNotNone(space)
            self.assertEqual(space.get("topic_name"), "codex · alpha")

    def test_pane_list_adds_herdr_workspace_label_as_space_name(self) -> None:
        pane_payload = {
            "result": {
                "panes": [
                    {
                        "pane_id": "w2:p1",
                        "terminal_id": "term-1",
                        "workspace_id": "w2",
                        "tab_id": "w2:t1",
                    }
                ]
            }
        }
        workspace_payload = {
            "result": {
                "workspaces": [
                    {"workspace_id": "w2", "label": "herdres"},
                ]
            }
        }

        with patch.object(herdres, "herdr_json", Mock(side_effect=[pane_payload, workspace_payload])):
            panes = herdres.pane_list()

        self.assertEqual(panes[0]["space_name"], "herdres")
        self.assertEqual(panes[0]["workspace_label"], "herdres")

    def test_pane_thread_name_uses_herdr_pane_label(self) -> None:
        labeled = {
            "pane_id": "wabc123:p2",
            "agent": "codex",
            "label": "  Build Runner  ",
        }
        alias_only = {"pane_id": "wabc123:p2", "agent": "codex", "label": ""}
        agent_and_id = {"pane_id": "pane-1234567890", "agent": "codex", "label": ""}

        self.assertEqual(herdres.pane_thread_name(labeled), "Build Runner")
        self.assertEqual(herdres.pane_thread_name(alias_only), "wabc123:2")
        self.assertEqual(herdres.pane_thread_name(agent_and_id), "Codex pane-1234567890")
        self.assertEqual(herdres.pane_thread_name({}), "Pane")

    def test_legacy_pane_topic_state_migrates_to_space_without_renaming(self) -> None:
        state = {
            "version": 1,
            "panes": {
                "pane-1": {
                    "pane_key": "pane-1",
                    "pane_id": "pane-1",
                    "workspace": "workspace-1",
                    "topic_id": "77",
                    "topic_name": "Build Runner",
                },
                "pane-2": {
                    "pane_key": "pane-2",
                    "pane_id": "pane-2",
                    "workspace": "workspace-1",
                    "topic_id": "88",
                    "topic_name": "Test Runner",
                },
            },
        }

        changed = herdres.migrate_legacy_pane_topics(state)

        self.assertTrue(changed)
        self.assertEqual(state["spaces"]["workspace:workspace-1"]["topic_id"], "77")
        self.assertEqual(state["spaces"]["workspace:workspace-1"]["pane_keys"], ["pane-1", "pane-2"])
        self.assertEqual(state["panes"]["pane-1"]["space_key"], "workspace:workspace-1")
        self.assertEqual(state["panes"]["pane-2"]["space_key"], "workspace:workspace-1")
        self.assertEqual(state["panes"]["pane-1"]["pane_thread_name"], "Build Runner")
        self.assertEqual(state["panes"]["pane-2"]["pane_thread_name"], "Test Runner")
        self.assertEqual(state["panes"]["pane-2"]["legacy_topic_id"], "88")
        self.assertEqual(state["panes"]["pane-2"]["topic_id"], "77")
        self.assertEqual(state["spaces"]["workspace:workspace-1"]["topic_name"], "Workspace 1")
        self.assertEqual(state["spaces"]["workspace:workspace-1"]["topic_rename_from"], "Build Runner")
        self.assertEqual(state["spaces"]["workspace:workspace-1"]["topic_rename_to"], "Workspace 1")
        self.assertEqual(state["panes"]["pane-1"]["topic_name"], "Workspace 1")
        self.assertEqual(state["panes"]["pane-1"]["legacy_topic_name"], "Build Runner")
        self.assertNotIn("topic_rename_pending_at", state["panes"]["pane-1"])


class SpaceTopicSyncTests(unittest.TestCase):
    def _pane(self, pane_id: str, workspace_id: str, label: str) -> dict:
        return {
            "pane_id": pane_id,
            "terminal_id": f"term-{pane_id}",
            "workspace_id": workspace_id,
            "tab_id": f"tab-{workspace_id}",
            "agent": "codex",
            "agent_status": "idle",
            "label": label,
            "cwd": "/workspace/herdres",
        }

    def _sync_caps(self) -> tuple[dict, dict]:
        counters = {
            "creates": 0,
            "sends": 0,
            "feed_sends": 0,
            "marker_sends": 0,
            "verifies": 0,
            "renames": 0,
            "icon_updates": 0,
        }
        caps = {
            "max_creates": 5,
            "max_sends": 10,
            "max_feed_sends": 0,
            "max_marker_sends": 0,
            "max_verifies": 0,
        }
        return counters, caps

    def test_new_idle_pane_does_not_send_visible_scaffolding_by_default(self) -> None:
        state = {"version": 1, "telegram": {"chat_id": "-1001"}, "spaces": {}, "panes": {}}
        telegram = state["telegram"]
        pane = self._pane("pane-1", "workspace-1", "Build Runner")
        counters, caps = self._sync_caps()
        caps["max_marker_sends"] = 10
        create_topic = Mock(return_value="77")
        send_rich = Mock(return_value={"ok": True, "format": "rich", "message_id": "1001"})
        update_status_marker = Mock(return_value={"ok": True, "attempted": True, "message_id": "1002"})
        update_live_card = Mock(return_value={"ok": True, "attempted": True, "message_id": "1003"})

        with patch.multiple(
            herdres,
            create_topic=create_topic,
            send_rich_message=send_rich,
            update_status_marker=update_status_marker,
            update_live_card=update_live_card,
            save_state=Mock(),
            apply_api_error_warning=Mock(return_value={"topic_missing": False, "changed": False}),
            STATUS_ICON_ENABLED=False,
            TURN_FEED_ENABLED=False,
        ):
            changed = herdres.sync_pane_once(state, "-1001", telegram, pane, counters, caps, turn_only=True)

        self.assertTrue(changed)
        create_topic.assert_called_once_with("-1001", "Workspace 1")
        send_rich.assert_not_called()
        update_status_marker.assert_not_called()
        update_live_card.assert_not_called()
        key = herdres.pane_key(pane)
        self.assertEqual(state["panes"][key]["topic_id"], "77")
        self.assertNotIn("pane_root_message_id", state["panes"][key])
        self.assertNotIn("status_marker_message_id", state["panes"][key])
        self.assertNotIn("card_message_id", state["panes"][key])

    def test_two_panes_same_space_create_one_forum_topic_and_two_roots(self) -> None:
        state = {"version": 1, "telegram": {"chat_id": "-1001"}, "spaces": {}, "panes": {}}
        telegram = state["telegram"]
        pane_a = self._pane("pane-1", "workspace-1", "Build Runner")
        pane_b = self._pane("pane-2", "workspace-1", "Test Runner")
        counters, caps = self._sync_caps()
        create_topic = Mock(return_value="77")
        send_root = Mock(
            side_effect=[
                {"ok": True, "format": "rich", "message_id": "1001"},
                {"ok": True, "format": "rich", "message_id": "1002"},
            ],
        )

        with patch.multiple(
            herdres,
            create_topic=create_topic,
            send_rich_message=send_root,
            save_state=Mock(),
            apply_api_error_warning=Mock(return_value={"topic_missing": False, "changed": False}),
            STATUS_ICON_ENABLED=False,
            LIVE_CARD_ENABLED=False,
            STATUS_MARKER_ENABLED=False,
            pane_root_messages_enabled=lambda: True,
            TURN_FEED_ENABLED=False,
        ):
            changed_a = herdres.sync_pane_once(state, "-1001", telegram, pane_a, counters, caps, turn_only=True)
            changed_b = herdres.sync_pane_once(state, "-1001", telegram, pane_b, counters, caps, turn_only=True)

        self.assertTrue(changed_a)
        self.assertTrue(changed_b)
        create_topic.assert_called_once_with("-1001", "Workspace 1")
        self.assertEqual(state["spaces"]["workspace:workspace-1"]["topic_id"], "77")
        self.assertEqual(counters["creates"], 1)
        self.assertEqual(send_root.call_count, 2)
        root_threads = [call.kwargs["thread_id"] for call in send_root.call_args_list]
        self.assertEqual(root_threads, ["77", "77"])
        key_a = herdres.pane_key(pane_a)
        key_b = herdres.pane_key(pane_b)
        self.assertEqual(state["panes"][key_a]["pane_root_message_id"], "1001")
        self.assertEqual(state["panes"][key_b]["pane_root_message_id"], "1002")
        self.assertEqual(state["panes"][key_a]["topic_id"], "77")
        self.assertEqual(state["panes"][key_b]["topic_id"], "77")
        self.assertEqual(state["panes"][key_a]["pane_thread_name"], "Build Runner")
        self.assertEqual(state["panes"][key_b]["pane_thread_name"], "Test Runner")

    def test_two_panes_different_spaces_create_two_topics(self) -> None:
        state = {"version": 1, "telegram": {"chat_id": "-1001"}, "spaces": {}, "panes": {}}
        telegram = state["telegram"]
        pane_a = self._pane("pane-1", "workspace-1", "Build Runner")
        pane_b = self._pane("pane-2", "workspace-2", "Test Runner")
        counters, caps = self._sync_caps()
        create_topic = Mock(side_effect=["77", "88"])
        send_root = Mock(
            side_effect=[
                {"ok": True, "format": "rich", "message_id": "1001"},
                {"ok": True, "format": "rich", "message_id": "1002"},
            ],
        )

        with patch.multiple(
            herdres,
            create_topic=create_topic,
            send_rich_message=send_root,
            save_state=Mock(),
            apply_api_error_warning=Mock(return_value={"topic_missing": False, "changed": False}),
            STATUS_ICON_ENABLED=False,
            LIVE_CARD_ENABLED=False,
            STATUS_MARKER_ENABLED=False,
            pane_root_messages_enabled=lambda: True,
            TURN_FEED_ENABLED=False,
        ):
            herdres.sync_pane_once(state, "-1001", telegram, pane_a, counters, caps, turn_only=True)
            herdres.sync_pane_once(state, "-1001", telegram, pane_b, counters, caps, turn_only=True)

        self.assertEqual(create_topic.call_args_list[0].args, ("-1001", "Workspace 1"))
        self.assertEqual(create_topic.call_args_list[1].args, ("-1001", "Workspace 2"))
        self.assertEqual(state["spaces"]["workspace:workspace-1"]["topic_id"], "77")
        self.assertEqual(state["spaces"]["workspace:workspace-2"]["topic_id"], "88")
        self.assertEqual([call.kwargs["thread_id"] for call in send_root.call_args_list], ["77", "88"])

    def test_per_agent_mode_two_agents_same_workspace_create_two_topics(self) -> None:
        state = {"version": 1, "telegram": {"chat_id": "-1001"}, "spaces": {}, "panes": {}}
        telegram = state["telegram"]
        pane_a = {
            "pane_id": "pane-1",
            "terminal_id": "term-pane-1",
            "workspace_id": "workspace-1",
            "tab_id": "tab-workspace-1",
            "agent": "codex",
            "agent_status": "idle",
            "cwd": "/workspace/alpha",
        }
        pane_b = {
            "pane_id": "pane-2",
            "terminal_id": "term-pane-2",
            "workspace_id": "workspace-1",
            "tab_id": "tab-workspace-1",
            "agent": "claude",
            "agent_status": "idle",
            "cwd": "/workspace/beta",
        }
        counters, caps = self._sync_caps()
        create_topic = Mock(side_effect=["77", "88"])
        send_root = Mock(
            side_effect=[
                {"ok": True, "format": "rich", "message_id": "1001"},
                {"ok": True, "format": "rich", "message_id": "1002"},
            ],
        )

        with patch.multiple(
            herdres,
            create_topic=create_topic,
            send_rich_message=send_root,
            save_state=Mock(),
            apply_api_error_warning=Mock(return_value={"topic_missing": False, "changed": False}),
            STATUS_ICON_ENABLED=False,
            LIVE_CARD_ENABLED=False,
            STATUS_MARKER_ENABLED=False,
            pane_root_messages_enabled=lambda: True,
            TURN_FEED_ENABLED=False,
            per_agent_topics_enabled=lambda: True,
        ):
            herdres.sync_pane_once(state, "-1001", telegram, pane_a, counters, caps, turn_only=True)
            herdres.sync_pane_once(state, "-1001", telegram, pane_b, counters, caps, turn_only=True)

        # Same workspace, but one topic per agent named "<agent> · <folder>".
        self.assertEqual(create_topic.call_args_list[0].args, ("-1001", "codex · alpha"))
        self.assertEqual(create_topic.call_args_list[1].args, ("-1001", "claude · beta"))
        self.assertEqual(state["spaces"]["agent:pane-1"]["topic_id"], "77")
        self.assertEqual(state["spaces"]["agent:pane-2"]["topic_id"], "88")
        self.assertEqual(counters["creates"], 2)
        self.assertEqual([call.kwargs["thread_id"] for call in send_root.call_args_list], ["77", "88"])

    def test_missing_pane_root_is_recreated_without_recreating_space_topic(self) -> None:
        pane = self._pane("pane-1", "workspace-1", "Build Runner")
        key = herdres.pane_key(pane)
        state = {
            "version": 1,
            "telegram": {"chat_id": "-1001"},
            "spaces": {
                "workspace:workspace-1": {
                    "space_key": "workspace:workspace-1",
                    "topic_id": "77",
                    "topic_name": "Workspace 1",
                    "pane_keys": [key],
                }
            },
            "panes": {
                key: {
                    "pane_key": key,
                    "pane_id": "pane-1",
                    "space_key": "workspace:workspace-1",
                    "topic_id": "77",
                    "pane_thread_name": "Build Runner",
                }
            },
        }
        counters, caps = self._sync_caps()
        create_topic = Mock(return_value="should-not-create")
        send_root = Mock(return_value={"ok": True, "format": "rich", "message_id": "1001"})

        with patch.multiple(
            herdres,
            create_topic=create_topic,
            send_rich_message=send_root,
            save_state=Mock(),
            apply_api_error_warning=Mock(return_value={"topic_missing": False, "changed": False}),
            STATUS_ICON_ENABLED=False,
            LIVE_CARD_ENABLED=False,
            STATUS_MARKER_ENABLED=False,
            pane_root_messages_enabled=lambda: True,
            TURN_FEED_ENABLED=False,
        ):
            changed = herdres.sync_pane_once(state, "-1001", state["telegram"], pane, counters, caps, turn_only=True)

        self.assertTrue(changed)
        create_topic.assert_not_called()
        send_root.assert_called_once()
        self.assertEqual(send_root.call_args.kwargs["thread_id"], "77")
        self.assertEqual(state["panes"][key]["pane_root_message_id"], "1001")

    def test_existing_space_topic_renames_to_herdr_workspace_label(self) -> None:
        pane = self._pane("pane-1", "w2", "Build Runner")
        pane["space_name"] = "herdres"
        key = herdres.pane_key(pane)
        state = {
            "version": 1,
            "telegram": {"chat_id": "-1001"},
            "spaces": {
                "workspace:w2": {
                    "space_key": "workspace:w2",
                    "space_id": "w2",
                    "topic_id": "20",
                    "topic_name": "W2",
                    "pane_keys": [key],
                    "last_topic_verified_at": herdres.utc_now(),
                }
            },
            "panes": {
                key: {
                    "pane_key": key,
                    "pane_id": "pane-1",
                    "space_key": "workspace:w2",
                    "topic_id": "20",
                    "pane_root_message_id": "1001",
                    "pane_thread_name": "Build Runner",
                    "last_topic_verified_at": herdres.utc_now(),
                }
            },
        }
        counters, caps = self._sync_caps()
        edit_topic = Mock(return_value=True)

        with patch.multiple(
            herdres,
            edit_topic=edit_topic,
            save_state=Mock(),
            apply_api_error_warning=Mock(return_value={"topic_missing": False, "changed": False}),
            STATUS_ICON_ENABLED=False,
            LIVE_CARD_ENABLED=False,
            STATUS_MARKER_ENABLED=False,
            TURN_FEED_ENABLED=False,
        ):
            changed = herdres.sync_pane_once(state, "-1001", state["telegram"], pane, counters, caps, turn_only=True)

        self.assertTrue(changed)
        edit_topic.assert_called_once_with("-1001", "20", "herdres")
        self.assertEqual(state["spaces"]["workspace:w2"]["topic_name"], "herdres")
        self.assertNotIn("topic_rename_pending_at", state["spaces"]["workspace:w2"])

    def test_space_topic_name_is_periodically_verified_without_pending_rename(self) -> None:
        pane = self._pane("pane-1", "workspace-1", "Build Runner")
        key = herdres.pane_key(pane)
        state = {
            "version": 1,
            "telegram": {"chat_id": "-1001"},
            "spaces": {
                "workspace:workspace-1": {
                    "space_key": "workspace:workspace-1",
                    "space_id": "workspace-1",
                    "topic_id": "77",
                    "topic_name": "Workspace 1",
                    "pane_keys": [key],
                }
            },
            "panes": {
                key: {
                    "pane_key": key,
                    "pane_id": "pane-1",
                    "space_key": "workspace:workspace-1",
                    "topic_id": "77",
                    "topic_name": "Workspace 1",
                    "pane_root_message_id": "1001",
                }
            },
        }
        counters, caps = self._sync_caps()
        caps["max_verifies"] = 1
        edit_topic = Mock(return_value=True)

        with patch.multiple(
            herdres,
            edit_topic=edit_topic,
            save_state=Mock(),
            apply_api_error_warning=Mock(return_value={"topic_missing": False, "changed": False}),
            STATUS_ICON_ENABLED=False,
            LIVE_CARD_ENABLED=False,
            STATUS_MARKER_ENABLED=False,
            TURN_FEED_ENABLED=False,
        ):
            changed = herdres.sync_pane_once(state, "-1001", state["telegram"], pane, counters, caps, turn_only=True)

        self.assertTrue(changed)
        edit_topic.assert_called_once_with("-1001", "77", "Workspace 1")
        self.assertEqual(counters["verifies"], 1)
        self.assertIn("last_topic_verified_at", state["spaces"]["workspace:workspace-1"])

    def test_closed_pane_in_shared_space_does_not_rename_space_topic(self) -> None:
        live_pane = self._pane("pane-1", "workspace-1", "Build Runner")
        closed_pane = self._pane("pane-2", "workspace-1", "Test Runner")
        live_key = herdres.pane_key(live_pane)
        closed_key = herdres.pane_key(closed_pane)
        state = {
            "version": 1,
            "enabled": True,
            "telegram": {
                "chat_id": "-1001",
                "general_thread_id": "1",
                "last_preflight_ok_at": herdres.utc_now(),
            },
            "spaces": {
                "workspace:workspace-1": {
                    "space_key": "workspace:workspace-1",
                    "topic_id": "77",
                    "topic_name": "Workspace 1",
                    "pane_keys": [live_key, closed_key],
                }
            },
            "panes": {
                live_key: {
                    "pane_key": live_key,
                    "pane_id": "pane-1",
                    "space_key": "workspace:workspace-1",
                    "topic_id": "77",
                    "topic_name": "Build Runner",
                    "pane_root_message_id": "1001",
                    "last_known_status": "working",
                },
                closed_key: {
                    "pane_key": closed_key,
                    "pane_id": "pane-2",
                    "space_key": "workspace:workspace-1",
                    "topic_id": "77",
                    "topic_name": "Test Runner",
                    "pane_root_message_id": "1002",
                    "last_known_status": "working",
                },
            },
        }
        edit_topic = Mock(return_value=True)
        send_notice = Mock(return_value={"ok": True, "message_id": "2002"})

        with patch.multiple(
            herdres,
            load_dotenv=Mock(),
            load_state=Mock(return_value=state),
            pane_list=Mock(return_value=[live_pane]),
            preflight_is_fresh=Mock(return_value=True),
            sync_pane_once=Mock(return_value=False),
            edit_topic=edit_topic,
            edit_topic_icon=Mock(return_value=True),
            send_notice=send_notice,
            save_state=Mock(),
        ):
            result = herdres.sync_once()

        self.assertTrue(result["ok"])
        edit_topic.assert_not_called()
        send_notice.assert_called_once()
        self.assertEqual(send_notice.call_args.kwargs["thread_id"], "77")
        self.assertIsNone(send_notice.call_args.kwargs["reply_to_message_id"])
        self.assertEqual(state["spaces"]["workspace:workspace-1"]["topic_name"], "Workspace 1")
        self.assertEqual(state["panes"][closed_key]["last_known_status"], "closed")


class PaneThreadRoutingTests(unittest.TestCase):
    def _pane_state(self) -> tuple[dict, dict, str, dict]:
        pane = {
            "pane_id": "pane-1",
            "terminal_id": "term-1",
            "workspace_id": "workspace-1",
            "tab_id": "tab-1",
            "agent": "claude",
            "agent_status": "done",
            "label": "Build Runner",
        }
        key = herdres.pane_key(pane)
        entry = {
            "pane_key": key,
            "pane_id": "pane-1",
            "space_key": "workspace:workspace-1",
            "topic_id": "77",
            "pane_thread_name": "Build Runner",
            "pane_root_message_id": "1001",
        }
        state = {
            "version": 1,
            "enabled": True,
            "telegram": {"chat_id": "-1001", "general_thread_id": "1", "owner_user_ids": ["42"]},
            "spaces": {
                "workspace:workspace-1": {
                    "space_key": "workspace:workspace-1",
                    "topic_id": "77",
                    "topic_name": "Workspace 1",
                    "pane_keys": [key],
                }
            },
            "panes": {key: entry},
        }
        return state, pane, key, entry

    def test_final_turn_posts_inside_space_topic_without_root_reply_by_default(self) -> None:
        state, pane, key, entry = self._pane_state()
        pane_turn = Mock(
            return_value={
                "available": True,
                "complete": True,
                "turn_id": "turn-1",
                "user_text": "What failed?",
                "assistant_final_text": "The build failed in pytest.",
            }
        )
        send_feed_item = Mock(return_value={"ok": True, "message_id": "2001"})
        counters = {
            "creates": 0,
            "sends": 0,
            "feed_sends": 0,
            "marker_sends": 0,
            "verifies": 0,
            "renames": 0,
            "icon_updates": 0,
        }
        caps = {
            "max_creates": 5,
            "max_sends": 10,
            "max_feed_sends": 10,
            "max_marker_sends": 0,
            "max_verifies": 0,
        }

        with patch.multiple(
            herdres,
            pane_turn=pane_turn,
            send_feed_item=send_feed_item,
            save_state=Mock(),
            apply_api_error_warning=Mock(return_value={"topic_missing": False, "changed": False}),
            TURN_FEED_ENABLED=True,
            LIVE_CARD_ENABLED=False,
            STATUS_MARKER_ENABLED=False,
            STATUS_ICON_ENABLED=False,
        ):
            changed = herdres.sync_pane_once(state, "-1001", state["telegram"], pane, counters, caps)

        self.assertTrue(changed)
        send_feed_item.assert_called_once()
        self.assertEqual(send_feed_item.call_args.kwargs["thread_id"], "77")
        self.assertIsNone(send_feed_item.call_args.kwargs["reply_to_message_id"])
        self.assertEqual(herdres.route_message_to_pane(state, "-1001", "77", "2001"), (key, entry))

    def test_pane_root_reply_target_requires_opt_in(self) -> None:
        entry = {"pane_root_message_id": "1001"}

        with patch.object(herdres, "pane_root_messages_enabled", lambda: False):
            self.assertIsNone(herdres.pane_root_reply_target(entry))
        with patch.object(herdres, "pane_root_messages_enabled", lambda: True):
            self.assertEqual(herdres.pane_root_reply_target(entry), "1001")

    def test_pane_root_messages_default_is_read_at_runtime(self) -> None:
        """Must be read at CALL time, not frozen at import. The Herdr plugin runs
        `herdres event` with no systemd EnvironmentFile, so the flag is set by
        load_dotenv() AFTER import; a frozen constant would be False and plugin-delivered
        turns would skip pane-root creation + reply-threading (split message thread)."""
        with patch.dict(os.environ, {"HERDR_TELEGRAM_TOPICS_PANE_ROOT_MESSAGES": "1"}, clear=False):
            self.assertTrue(herdres.pane_root_messages_enabled())
        with patch.dict(os.environ, {"HERDR_TELEGRAM_TOPICS_PANE_ROOT_MESSAGES": "0"}, clear=False):
            self.assertFalse(herdres.pane_root_messages_enabled())

    def test_message_route_index_records_final_prompt_and_notice_messages(self) -> None:
        state, _pane, key, entry = self._pane_state()

        herdres.record_pane_message_route(state, "workspace:workspace-1", key, "1001")
        herdres.record_pane_message_route(state, "workspace:workspace-1", key, "2001")
        herdres.record_pane_message_route(state, "workspace:workspace-1", key, "3001")

        self.assertEqual(herdres.route_message_to_pane(state, "-1001", "77", "1001"), (key, entry))
        self.assertEqual(herdres.route_message_to_pane(state, "-1001", "77", "2001"), (key, entry))
        self.assertEqual(herdres.route_message_to_pane(state, "-1001", "77", "3001"), (key, entry))
        self.assertEqual(state["spaces"]["workspace:workspace-1"]["message_routes"]["3001"], key)

    def test_legacy_send_message_uses_reply_parameters_when_root_id_exists(self) -> None:
        payloads = []

        def fake_api(method: str, payload: dict) -> dict:
            payloads.append((method, payload))
            return {"ok": True, "result": {"message_id": 42}}

        with patch.object(herdres, "telegram_api", fake_api):
            message_id = herdres.send_message("-1001", "hello", thread_id="77", reply_to_message_id="1001")

        self.assertEqual(message_id, "42")
        self.assertEqual(payloads[0][0], "sendMessage")
        self.assertIn("reply_parameters", payloads[0][1])
        self.assertNotIn("reply_to_message_id", payloads[0][1])
        self.assertEqual(payloads[0][1]["message_thread_id"], "77")


class SharedTopicCommandTests(unittest.TestCase):
    def _shared_state(self) -> dict:
        return {
            "version": 1,
            "telegram": {"chat_id": "-1001", "general_thread_id": "1", "owner_user_ids": ["42"]},
            "spaces": {
                "workspace:workspace-1": {
                    "space_key": "workspace:workspace-1",
                    "topic_id": "77",
                    "topic_name": "Workspace 1",
                    "pane_keys": ["pane-1", "pane-2"],
                    "message_routes": {"1002": "pane-2", "3002": "pane-2"},
                }
            },
            "panes": {
                "pane-1": {
                    "pane_key": "pane-1",
                    "pane_id": "pane-1",
                    "space_key": "workspace:workspace-1",
                    "topic_id": "77",
                    "pane_root_message_id": "1001",
                    "last_known_status": "working",
                    "active_prompt": active_prompt({
                        "id": "prompt1",
                        "text": "Question\nRun old pane?",
                        "choice_source": "explicit_block",
                        "options": [{"number": "1", "label": "Run old"}],
                    }, message_id="3001"),
                },
                "pane-2": {
                    "pane_key": "pane-2",
                    "pane_id": "pane-2",
                    "space_key": "workspace:workspace-1",
                    "topic_id": "77",
                    "pane_root_message_id": "1002",
                    "last_known_status": "working",
                    "active_prompt": active_prompt({
                        "id": "prompt2",
                        "text": "Question\nRun new pane?",
                        "choice_source": "explicit_block",
                        "options": [{"number": "1", "label": "Run new"}],
                    }, message_id="3002"),
                },
            },
        }

    def _devin_glm_space_state(self, space_overrides: dict | None = None) -> tuple[dict, list[dict]]:
        space = {
            "space_key": "workspace:workspace-1",
            "pane_keys": ["pane-1"],
        }
        if space_overrides:
            space.update(space_overrides)
        state = {
            "version": 1,
            "telegram": {},
            "spaces": {"workspace:workspace-1": space},
            "panes": {},
        }
        panes = [
            {
                "pane_id": "pane-1",
                "pane_key": "pane-1",
                "workspace_id": "workspace-1",
                "agent": "codex",
                "agent_status": "idle",
                "foreground_cwd": "/tmp/project",
            }
        ]
        return state, panes

    def _successful_devin_glm_seat_fields(self, *, created_at: str | None = None) -> dict:
        return {
            "devin_glm_seat_pane_id": "pane-devin",
            "devin_glm_seat_created_at": created_at or herdres.utc_now(),
            "devin_glm_seat_command": "devin --model glm-5.2 --permission-mode dangerous",
            "devin_glm_seat_model": "glm-5.2",
            "devin_glm_seat_permission_mode": "dangerous",
        }

    def _devin_glm_closed_marker(self, *, reason: str = "exited") -> dict:
        return {
            "devin_glm_seat_closed_at": herdres.utc_now(),
            "devin_glm_seat_closed_pane_id": "pane-devin",
            "devin_glm_seat_closed_reason": reason,
        }

    def _devin_glm_missing_marker(self, *, pane_id: str = "pane-devin") -> dict:
        return {
            "devin_glm_seat_missing_pane_id": pane_id,
            "devin_glm_seat_missing_at": herdres.utc_now(),
        }

    def _record_successful_devin_glm_run(self, commands: list[list[str]], *, pane_id: str = "pane-devin"):
        def run_cmd(args, **kwargs):
            commands.append(list(args))
            stdout = (
                herdres.json.dumps({"result": {"pane": {"pane_id": pane_id}}})
                if args[1:3] == ["pane", "split"]
                else ""
            )
            return SimpleNamespace(returncode=0, stdout=stdout, stderr="")

        return run_cmd

    def test_command_reply_uses_reply_route_not_first_topic_match(self) -> None:
        state = self._shared_state()
        send_to_pane = Mock(return_value=(True, ""))

        with callback_patches(state, send_to_pane=send_to_pane):
            result = herdres.command_reply(
                {
                    "chat_id": "-1001",
                    "topic_id": "77",
                    "message_id": "4000",
                    "reply_to_message_id": "1002",
                    "user_id": "42",
                    "text": "/send run tests",
                }
        )

        self.assertTrue(result["handled"])
        self.assertEqual(result["reply"], "")
        send_to_pane.assert_called_once_with("pane-2", "run tests")

    def test_command_reply_plain_reply_routes_without_implicit_send(self) -> None:
        state = self._shared_state()
        send_to_pane = Mock(return_value=(True, ""))

        with callback_patches(state, send_to_pane=send_to_pane):
            result = herdres.command_reply(
                {
                    "chat_id": "-1001",
                    "topic_id": "77",
                    "message_id": "4000",
                    "reply_to_message_id": "1002",
                    "user_id": "42",
                    "text": "run tests",
                }
            )

        self.assertTrue(result["handled"])
        self.assertEqual(result["reply"], "")
        send_to_pane.assert_called_once_with("pane-2", "run tests")

    def test_command_reply_plain_single_pane_space_routes_without_reply(self) -> None:
        state = self._shared_state()
        state["spaces"]["workspace:workspace-1"]["pane_keys"] = ["pane-1"]
        state["panes"].pop("pane-2")
        send_to_pane = Mock(return_value=(True, ""))

        with callback_patches(state, send_to_pane=send_to_pane):
            result = herdres.command_reply(
                {
                    "chat_id": "-1001",
                    "topic_id": "77",
                    "message_id": "4000",
                    "reply_to_message_id": "",
                    "user_id": "42",
                    "text": "run tests",
                }
            )

        self.assertTrue(result["handled"])
        self.assertEqual(result["reply"], "")
        send_to_pane.assert_called_once_with("pane-1", "run tests")

    def test_command_reply_new_agent_splits_space_and_runs_cli(self) -> None:
        state = self._shared_state()
        state["panes"]["pane-1"]["foreground_cwd"] = "/tmp/project"
        commands: list[list[str]] = []

        def run_cmd(args, **kwargs):
            commands.append(args)
            proc = Mock()
            proc.returncode = 0
            proc.stderr = ""
            proc.stdout = (
                herdres.json.dumps({"result": {"pane": {"pane_id": "pane-new"}}})
                if args[1:3] == ["pane", "split"]
                else ""
            )
            return proc

        with callback_patches(state), patch.object(herdres, "run_cmd", run_cmd):
            result = herdres.command_reply(
                {
                    "chat_id": "-1001",
                    "topic_id": "77",
                    "message_id": "4000",
                    "reply_to_message_id": "",
                    "user_id": "42",
                    "text": "/new claude",
                }
            )

        self.assertTrue(result["handled"])
        self.assertIn("Started Claude", result["reply"])
        self.assertEqual(
            commands[0],
            [
                herdres.herdr_bin(),
                "pane",
                "split",
                "pane-1",
                "--direction",
                "right",
                "--cwd",
                "/tmp/project",
                "--focus",
            ],
        )
        self.assertEqual(commands[1], [herdres.herdr_bin(), "pane", "run", "pane-new", "claude"])

    def test_devin_glm_seat_command_defaults_to_dangerous_glm(self) -> None:
        with patch.dict(
            herdres.os.environ,
            {
                "HERDR_TELEGRAM_TOPICS_DEVIN_GLM_COMMAND": "",
                "HERDR_TELEGRAM_TOPICS_DEVIN_GLM_EXTRA_ARGS": "",
                "HERDR_TELEGRAM_TOPICS_DEVIN_GLM_MODEL": "glm-5.2",
                "HERDR_TELEGRAM_TOPICS_DEVIN_GLM_PERMISSION_MODE": "dangerous",
            },
            clear=False,
        ):
            self.assertEqual(
                herdres.devin_glm_seat_command(),
                "devin --model glm-5.2 --permission-mode dangerous",
            )

    def test_ensure_devin_glm_space_seats_default_disabled_does_not_probe_or_start(self) -> None:
        state, panes = self._devin_glm_space_state()

        pane_list = Mock(return_value=panes)
        run_cmd = Mock(return_value=SimpleNamespace(returncode=0, stdout="", stderr=""))
        with patch.dict(herdres.os.environ, {}, clear=True), patch.object(
            herdres, "pane_list", pane_list
        ), patch.object(herdres, "run_cmd", run_cmd):
            result = herdres.ensure_devin_glm_space_seats(state, panes)

        self.assertEqual(result, {"changed": False, "started": 0, "skipped": True})
        pane_list.assert_not_called()
        run_cmd.assert_not_called()

    def test_ensure_devin_glm_space_seats_splits_and_runs_devin(self) -> None:
        state = {
            "version": 1,
            "telegram": {},
            "spaces": {
                "workspace:workspace-1": {
                    "space_key": "workspace:workspace-1",
                    "pane_keys": ["pane-1"],
                }
            },
            "panes": {},
        }
        panes = [
            {
                "pane_id": "pane-1",
                "pane_key": "pane-1",
                "workspace_id": "workspace-1",
                "agent": "codex",
                "agent_status": "idle",
                "foreground_cwd": "/tmp/project",
            }
        ]
        commands: list[list[str]] = []
        _seat_base = tempfile.mkdtemp()

        def run_cmd(args, **kwargs):
            commands.append(args)
            proc = Mock()
            proc.returncode = 0
            proc.stderr = ""
            proc.stdout = (
                herdres.json.dumps({"result": {"pane": {"pane_id": "pane-devin"}}})
                if args[1:3] == ["pane", "split"]
                else ""
            )
            return proc

        with patch.dict(
            herdres.os.environ,
            {
                "HERDR_TELEGRAM_TOPICS_DEVIN_GLM_SEAT": "1",
                "HERDR_TELEGRAM_TOPICS_DEVIN_GLM_SEAT_MAX_PER_RUN": "3",
                "HERDR_TELEGRAM_TOPICS_DEVIN_GLM_MODEL": "glm-5.2",
                "HERDR_TELEGRAM_TOPICS_DEVIN_GLM_PERMISSION_MODE": "dangerous",
                "HERDR_TELEGRAM_TOPICS_DEVIN_GLM_COMMAND": "",
                "HERDR_TELEGRAM_TOPICS_DEVIN_GLM_EXTRA_ARGS": "",
                "HERDR_TELEGRAM_TOPICS_DEVIN_GLM_SEAT_BASE": _seat_base,
            },
            clear=False,
        ), patch.object(herdres, "pane_list", Mock(return_value=panes)), patch.object(herdres, "run_cmd", run_cmd):
            result = herdres.ensure_devin_glm_space_seats(state, panes)

        self.assertTrue(result["changed"])
        self.assertEqual(result["started"], 1)
        space = state["spaces"]["workspace:workspace-1"]
        expected_seat_cwd = os.path.join(
            _seat_base,
            "workspace_workspace-1-" + herdres.hashlib.sha1(b"workspace:workspace-1").hexdigest()[:12],
        )
        self.assertEqual(space["devin_glm_seat_cwd"], expected_seat_cwd)
        self.assertEqual(space["devin_glm_seat_pane_id"], "pane-devin")
        self.assertEqual(space["devin_glm_seat_model"], "glm-5.2")
        self.assertEqual(space["devin_glm_seat_permission_mode"], "dangerous")
        self.assertEqual(
            commands[0],
            [
                herdres.herdr_bin(),
                "pane",
                "split",
                "pane-1",
                "--direction",
                "right",
                "--cwd",
                expected_seat_cwd,
                "--no-focus",
            ],
        )
        self.assertEqual(commands[1], [herdres.herdr_bin(), "pane", "rename", "pane-devin", "GLM Devin"])
        self.assertEqual(
            commands[2],
            [
                herdres.herdr_bin(),
                "pane",
                "run",
                "pane-devin",
                "devin --model glm-5.2 --permission-mode dangerous",
            ],
        )

    def test_ensure_devin_glm_space_seats_does_not_duplicate_pending_pane(self) -> None:
        state = {
            "version": 1,
            "telegram": {},
            "spaces": {
                "workspace:workspace-1": {
                    "space_key": "workspace:workspace-1",
                    "pane_keys": ["pane-1"],
                    "devin_glm_seat_pane_id": "pane-devin",
                    "devin_glm_seat_created_at": herdres.utc_now(),
                }
            },
            "panes": {},
        }
        panes = [
            {
                "pane_id": "pane-1",
                "pane_key": "pane-1",
                "workspace_id": "workspace-1",
                "agent": "codex",
                "agent_status": "idle",
            }
        ]
        all_panes = panes + [
            {
                "pane_id": "pane-devin",
                "pane_key": "pane-devin",
                "workspace_id": "workspace-1",
                "agent": "",
                "agent_status": "unknown",
                "label": "GLM Devin",
            }
        ]

        with patch.dict(
            herdres.os.environ,
            {
                "HERDR_TELEGRAM_TOPICS_DEVIN_GLM_SEAT": "1",
                "HERDR_TELEGRAM_TOPICS_DEVIN_GLM_SEAT_MAX_PER_RUN": "3",
            },
            clear=False,
        ), patch.object(herdres, "pane_list", Mock(return_value=all_panes)), patch.object(herdres, "run_cmd") as run_cmd:
            result = herdres.ensure_devin_glm_space_seats(state, panes)

        self.assertFalse(result["changed"])
        self.assertEqual(result["started"], 0)
        run_cmd.assert_not_called()

    def test_ensure_devin_glm_space_seats_backs_off_after_recent_error(self) -> None:
        state = {
            "version": 1,
            "telegram": {},
            "spaces": {
                "workspace:workspace-1": {
                    "space_key": "workspace:workspace-1",
                    "pane_keys": ["pane-1"],
                    "devin_glm_seat_error_at": herdres.utc_now(),
                }
            },
            "panes": {},
        }
        panes = [
            {
                "pane_id": "pane-1",
                "pane_key": "pane-1",
                "workspace_id": "workspace-1",
                "agent": "codex",
                "agent_status": "idle",
            }
        ]

        with patch.dict(
            herdres.os.environ,
            {
                "HERDR_TELEGRAM_TOPICS_DEVIN_GLM_SEAT": "1",
                "HERDR_TELEGRAM_TOPICS_DEVIN_GLM_SEAT_MAX_PER_RUN": "3",
            },
            clear=False,
        ), patch.object(herdres, "pane_list", Mock(return_value=panes)), patch.object(herdres, "run_cmd") as run_cmd:
            result = herdres.ensure_devin_glm_space_seats(state, panes)

        self.assertFalse(result["changed"])
        self.assertEqual(result["started"], 0)
        run_cmd.assert_not_called()

    def test_ensure_devin_glm_space_seats_latches_missing_successful_pane_after_ttl(self) -> None:
        old_created_at = (
            datetime.now(timezone.utc) - timedelta(seconds=herdres.DEVIN_GLM_SEAT_PENDING_TTL_SECONDS + 1)
        ).isoformat()
        state, panes = self._devin_glm_space_state(
            self._successful_devin_glm_seat_fields(created_at=old_created_at)
        )

        run_cmd = Mock(return_value=SimpleNamespace(returncode=0, stdout="", stderr=""))
        with patch.dict(
            herdres.os.environ,
            {
                "HERDR_TELEGRAM_TOPICS_DEVIN_GLM_SEAT": "1",
                "HERDR_TELEGRAM_TOPICS_DEVIN_GLM_SEAT_RECREATE": "0",
                "HERDR_TELEGRAM_TOPICS_DEVIN_GLM_SEAT_MAX_PER_RUN": "3",
            },
            clear=False,
        ), patch.object(herdres, "pane_list", Mock(return_value=panes)), patch.object(herdres, "run_cmd", run_cmd):
            first = herdres.ensure_devin_glm_space_seats(state, panes)

            space = state["spaces"]["workspace:workspace-1"]
            self.assertTrue(first["changed"])
            self.assertEqual(first["started"], 0)
            self.assertEqual(space.get("devin_glm_seat_missing_pane_id"), "pane-devin")
            self.assertTrue(space.get("devin_glm_seat_missing_at"))
            self.assertNotIn("devin_glm_seat_closed_at", space)
            self.assertNotIn("devin_glm_seat_closed_pane_id", space)
            self.assertNotIn("devin_glm_seat_closed_reason", space)
            run_cmd.assert_not_called()

            second = herdres.ensure_devin_glm_space_seats(state, panes)

            self.assertTrue(second["changed"])
            self.assertEqual(second["started"], 0)
            self.assertNotIn("devin_glm_seat_missing_pane_id", space)
            self.assertNotIn("devin_glm_seat_missing_at", space)
            self.assertEqual(space.get("devin_glm_seat_closed_pane_id"), "pane-devin")
            self.assertEqual(space.get("devin_glm_seat_closed_reason"), "missing_after_ttl")
            self.assertTrue(space.get("devin_glm_seat_closed_at"))
            sync_after_second = state.get("last_devin_glm_seat_sync_at")
            self.assertTrue(sync_after_second)
            closed_at = space.get("devin_glm_seat_closed_at")

            third = herdres.ensure_devin_glm_space_seats(state, panes)

        self.assertFalse(third["changed"])
        self.assertEqual(third["started"], 0)
        self.assertEqual(state.get("last_devin_glm_seat_sync_at"), sync_after_second)
        self.assertEqual(space.get("devin_glm_seat_closed_at"), closed_at)
        self.assertEqual(space.get("devin_glm_seat_closed_pane_id"), "pane-devin")
        self.assertEqual(space.get("devin_glm_seat_closed_reason"), "missing_after_ttl")
        self.assertNotIn("devin_glm_seat_missing_pane_id", space)
        self.assertNotIn("devin_glm_seat_missing_at", space)
        run_cmd.assert_not_called()

    def test_ensure_devin_glm_space_seats_latches_observed_closed_or_exited_and_honors_latch(self) -> None:
        for status in ("closed", "exited"):
            with self.subTest(status=status):
                space_fields = self._successful_devin_glm_seat_fields()
                space_fields.update(self._devin_glm_missing_marker())
                state, panes = self._devin_glm_space_state(space_fields)
                tracked_pane = {
                    "pane_id": "pane-devin",
                    "pane_key": "pane-devin",
                    "workspace_id": "workspace-1",
                    "agent": "devin",
                    "agent_status": status,
                    "label": "GLM Devin",
                }
                pane_list = Mock(side_effect=[panes + [tracked_pane], panes])
                run_cmd = Mock(return_value=SimpleNamespace(returncode=0, stdout="", stderr=""))

                with patch.dict(
                    herdres.os.environ,
                    {
                        "HERDR_TELEGRAM_TOPICS_DEVIN_GLM_SEAT": "1",
                        "HERDR_TELEGRAM_TOPICS_DEVIN_GLM_SEAT_RECREATE": "0",
                        "HERDR_TELEGRAM_TOPICS_DEVIN_GLM_SEAT_MAX_PER_RUN": "3",
                    },
                    clear=False,
                ), patch.object(herdres, "pane_list", pane_list), patch.object(herdres, "run_cmd", run_cmd):
                    first = herdres.ensure_devin_glm_space_seats(state, panes)
                    second = herdres.ensure_devin_glm_space_seats(state, panes)

                space = state["spaces"]["workspace:workspace-1"]
                self.assertTrue(first["changed"])
                self.assertEqual(first["started"], 0)
                self.assertIn("devin_glm_seat_closed_at", space)
                self.assertEqual(space.get("devin_glm_seat_closed_pane_id"), "pane-devin")
                self.assertEqual(space.get("devin_glm_seat_closed_reason"), status)
                self.assertTrue(space.get("devin_glm_seat_closed_at"))
                self.assertNotIn("devin_glm_seat_missing_pane_id", space)
                self.assertNotIn("devin_glm_seat_missing_at", space)
                self.assertFalse(second["changed"])
                self.assertEqual(second["started"], 0)
                run_cmd.assert_not_called()

    def test_ensure_devin_glm_space_seats_clears_missing_marker_when_pane_reappears(self) -> None:
        old_created_at = (
            datetime.now(timezone.utc) - timedelta(seconds=herdres.DEVIN_GLM_SEAT_PENDING_TTL_SECONDS + 1)
        ).isoformat()
        space_fields = self._successful_devin_glm_seat_fields(created_at=old_created_at)
        space_fields.update(self._devin_glm_missing_marker())
        state, panes = self._devin_glm_space_state(space_fields)
        tracked_pane = {
            "pane_id": "pane-devin",
            "pane_key": "pane-devin",
            "workspace_id": "workspace-1",
            "agent": "devin",
            "agent_status": "working",
            "label": "GLM Devin",
        }
        pane_list = Mock(side_effect=[panes + [tracked_pane], panes])
        run_cmd = Mock(return_value=SimpleNamespace(returncode=0, stdout="", stderr=""))

        with patch.dict(
            herdres.os.environ,
            {
                "HERDR_TELEGRAM_TOPICS_DEVIN_GLM_SEAT": "1",
                "HERDR_TELEGRAM_TOPICS_DEVIN_GLM_SEAT_RECREATE": "0",
                "HERDR_TELEGRAM_TOPICS_DEVIN_GLM_SEAT_MAX_PER_RUN": "3",
            },
            clear=False,
        ), patch.object(herdres, "pane_list", pane_list), patch.object(herdres, "run_cmd", run_cmd):
            reappeared = herdres.ensure_devin_glm_space_seats(state, panes)

            space = state["spaces"]["workspace:workspace-1"]
            self.assertTrue(reappeared["changed"])
            self.assertEqual(reappeared["started"], 0)
            self.assertNotIn("devin_glm_seat_missing_pane_id", space)
            self.assertNotIn("devin_glm_seat_missing_at", space)
            self.assertNotIn("devin_glm_seat_closed_at", space)
            self.assertNotIn("devin_glm_seat_closed_pane_id", space)
            self.assertNotIn("devin_glm_seat_closed_reason", space)

            fresh_missing = herdres.ensure_devin_glm_space_seats(state, panes)

        self.assertTrue(fresh_missing["changed"])
        self.assertEqual(fresh_missing["started"], 0)
        self.assertEqual(space.get("devin_glm_seat_missing_pane_id"), "pane-devin")
        self.assertTrue(space.get("devin_glm_seat_missing_at"))
        self.assertNotIn("devin_glm_seat_closed_at", space)
        self.assertNotIn("devin_glm_seat_closed_pane_id", space)
        self.assertNotIn("devin_glm_seat_closed_reason", space)
        run_cmd.assert_not_called()

    def test_ensure_devin_glm_space_seats_treats_different_missing_pane_as_fresh_first_miss(self) -> None:
        old_created_at = (
            datetime.now(timezone.utc) - timedelta(seconds=herdres.DEVIN_GLM_SEAT_PENDING_TTL_SECONDS + 1)
        ).isoformat()
        space_fields = self._successful_devin_glm_seat_fields(created_at=old_created_at)
        space_fields["devin_glm_seat_pane_id"] = "pane-devin-new"
        space_fields.update(self._devin_glm_missing_marker(pane_id="pane-devin"))
        state, panes = self._devin_glm_space_state(space_fields)
        run_cmd = Mock(return_value=SimpleNamespace(returncode=0, stdout="", stderr=""))

        with patch.dict(
            herdres.os.environ,
            {
                "HERDR_TELEGRAM_TOPICS_DEVIN_GLM_SEAT": "1",
                "HERDR_TELEGRAM_TOPICS_DEVIN_GLM_SEAT_RECREATE": "0",
                "HERDR_TELEGRAM_TOPICS_DEVIN_GLM_SEAT_MAX_PER_RUN": "3",
            },
            clear=False,
        ), patch.object(herdres, "pane_list", Mock(return_value=panes)), patch.object(herdres, "run_cmd", run_cmd):
            result = herdres.ensure_devin_glm_space_seats(state, panes)

        space = state["spaces"]["workspace:workspace-1"]
        self.assertTrue(result["changed"])
        self.assertEqual(result["started"], 0)
        self.assertEqual(space.get("devin_glm_seat_missing_pane_id"), "pane-devin-new")
        self.assertTrue(space.get("devin_glm_seat_missing_at"))
        self.assertNotIn("devin_glm_seat_closed_at", space)
        self.assertNotIn("devin_glm_seat_closed_pane_id", space)
        self.assertNotIn("devin_glm_seat_closed_reason", space)
        run_cmd.assert_not_called()

    def test_ensure_devin_glm_space_seats_keeps_recent_missing_pane_pending(self) -> None:
        state, panes = self._devin_glm_space_state(
            {
                "devin_glm_seat_pane_id": "pane-devin",
                "devin_glm_seat_created_at": herdres.utc_now(),
            }
        )

        run_cmd = Mock(return_value=SimpleNamespace(returncode=0, stdout="", stderr=""))
        with patch.dict(
            herdres.os.environ,
            {
                "HERDR_TELEGRAM_TOPICS_DEVIN_GLM_SEAT": "1",
                "HERDR_TELEGRAM_TOPICS_DEVIN_GLM_SEAT_RECREATE": "0",
                "HERDR_TELEGRAM_TOPICS_DEVIN_GLM_SEAT_MAX_PER_RUN": "3",
            },
            clear=False,
        ), patch.object(herdres, "pane_list", Mock(return_value=panes)), patch.object(herdres, "run_cmd", run_cmd):
            result = herdres.ensure_devin_glm_space_seats(state, panes)

        space = state["spaces"]["workspace:workspace-1"]
        self.assertFalse(result["changed"])
        self.assertEqual(result["started"], 0)
        self.assertNotIn("devin_glm_seat_closed_at", space)
        self.assertNotIn("devin_glm_seat_closed_pane_id", space)
        self.assertNotIn("devin_glm_seat_closed_reason", space)
        run_cmd.assert_not_called()

    def test_ensure_devin_glm_space_seats_recreate_bypasses_missing_debounce(self) -> None:
        old_created_at = (
            datetime.now(timezone.utc) - timedelta(seconds=herdres.DEVIN_GLM_SEAT_PENDING_TTL_SECONDS + 1)
        ).isoformat()
        space_fields = self._successful_devin_glm_seat_fields(created_at=old_created_at)
        space_fields.update(self._devin_glm_missing_marker())
        state, panes = self._devin_glm_space_state(space_fields)
        commands: list[list[str]] = []
        _seat_base = tempfile.mkdtemp()

        with patch.dict(
            herdres.os.environ,
            {
                "HERDR_TELEGRAM_TOPICS_DEVIN_GLM_SEAT": "1",
                "HERDR_TELEGRAM_TOPICS_DEVIN_GLM_SEAT_RECREATE": "1",
                "HERDR_TELEGRAM_TOPICS_DEVIN_GLM_SEAT_MAX_PER_RUN": "3",
                "HERDR_TELEGRAM_TOPICS_DEVIN_GLM_MODEL": "glm-5.2",
                "HERDR_TELEGRAM_TOPICS_DEVIN_GLM_PERMISSION_MODE": "dangerous",
                "HERDR_TELEGRAM_TOPICS_DEVIN_GLM_COMMAND": "",
                "HERDR_TELEGRAM_TOPICS_DEVIN_GLM_EXTRA_ARGS": "",
                "HERDR_TELEGRAM_TOPICS_DEVIN_GLM_SEAT_BASE": _seat_base,
            },
            clear=False,
        ), patch.object(herdres, "pane_list", Mock(return_value=panes)), patch.object(
            herdres,
            "run_cmd",
            self._record_successful_devin_glm_run(commands, pane_id="pane-devin-new"),
        ):
            result = herdres.ensure_devin_glm_space_seats(state, panes)

        space = state["spaces"]["workspace:workspace-1"]
        self.assertTrue(result["changed"])
        self.assertEqual(result["started"], 1)
        self.assertEqual(space["devin_glm_seat_pane_id"], "pane-devin-new")
        self.assertNotIn("devin_glm_seat_missing_pane_id", space)
        self.assertNotIn("devin_glm_seat_missing_at", space)
        self.assertNotIn("devin_glm_seat_closed_at", space)
        self.assertNotIn("devin_glm_seat_closed_pane_id", space)
        self.assertNotIn("devin_glm_seat_closed_reason", space)
        self.assertEqual(
            commands[-1],
            [
                herdres.herdr_bin(),
                "pane",
                "run",
                "pane-devin-new",
                "devin --model glm-5.2 --permission-mode dangerous",
            ],
        )

    def test_ensure_devin_glm_space_seats_recreate_success_clears_closed_and_missing_markers(self) -> None:
        old_created_at = (
            datetime.now(timezone.utc) - timedelta(seconds=herdres.DEVIN_GLM_SEAT_PENDING_TTL_SECONDS + 1)
        ).isoformat()
        space_fields = self._successful_devin_glm_seat_fields(created_at=old_created_at)
        space_fields.update(self._devin_glm_closed_marker(reason="exited"))
        space_fields.update(self._devin_glm_missing_marker())
        state, panes = self._devin_glm_space_state(space_fields)
        commands: list[list[str]] = []
        _seat_base = tempfile.mkdtemp()

        with patch.dict(
            herdres.os.environ,
            {
                "HERDR_TELEGRAM_TOPICS_DEVIN_GLM_SEAT": "1",
                "HERDR_TELEGRAM_TOPICS_DEVIN_GLM_SEAT_RECREATE": "1",
                "HERDR_TELEGRAM_TOPICS_DEVIN_GLM_SEAT_MAX_PER_RUN": "3",
                "HERDR_TELEGRAM_TOPICS_DEVIN_GLM_MODEL": "glm-5.2",
                "HERDR_TELEGRAM_TOPICS_DEVIN_GLM_PERMISSION_MODE": "dangerous",
                "HERDR_TELEGRAM_TOPICS_DEVIN_GLM_COMMAND": "",
                "HERDR_TELEGRAM_TOPICS_DEVIN_GLM_EXTRA_ARGS": "",
                "HERDR_TELEGRAM_TOPICS_DEVIN_GLM_SEAT_BASE": _seat_base,
            },
            clear=False,
        ), patch.object(herdres, "pane_list", Mock(return_value=panes)), patch.object(
            herdres,
            "run_cmd",
            self._record_successful_devin_glm_run(commands, pane_id="pane-devin-new"),
        ):
            result = herdres.ensure_devin_glm_space_seats(state, panes)

        space = state["spaces"]["workspace:workspace-1"]
        self.assertTrue(result["changed"])
        self.assertEqual(result["started"], 1)
        self.assertEqual(space["devin_glm_seat_pane_id"], "pane-devin-new")
        self.assertNotIn("devin_glm_seat_closed_at", space)
        self.assertNotIn("devin_glm_seat_closed_pane_id", space)
        self.assertNotIn("devin_glm_seat_closed_reason", space)
        self.assertNotIn("devin_glm_seat_missing_pane_id", space)
        self.assertNotIn("devin_glm_seat_missing_at", space)
        self.assertEqual(
            commands[-1],
            [
                herdres.herdr_bin(),
                "pane",
                "run",
                "pane-devin-new",
                "devin --model glm-5.2 --permission-mode dangerous",
            ],
        )

    def test_ensure_devin_glm_space_seats_keeps_closed_marker_when_recreate_run_fails(self) -> None:
        old_created_at = (
            datetime.now(timezone.utc) - timedelta(seconds=herdres.DEVIN_GLM_SEAT_PENDING_TTL_SECONDS + 1)
        ).isoformat()
        space_fields = self._successful_devin_glm_seat_fields(created_at=old_created_at)
        space_fields.update(self._devin_glm_closed_marker(reason="exited"))
        state, panes = self._devin_glm_space_state(space_fields)
        commands: list[list[str]] = []
        _seat_base = tempfile.mkdtemp()

        def run_cmd(args, **kwargs):
            commands.append(list(args))
            if args[1:3] == ["pane", "split"]:
                return SimpleNamespace(
                    returncode=0,
                    stdout=herdres.json.dumps({"result": {"pane": {"pane_id": "pane-devin-new"}}}),
                    stderr="",
                )
            if args[1:3] == ["pane", "run"]:
                return SimpleNamespace(returncode=1, stdout="", stderr="launch failed")
            return SimpleNamespace(returncode=0, stdout="", stderr="")

        with patch.dict(
            herdres.os.environ,
            {
                "HERDR_TELEGRAM_TOPICS_DEVIN_GLM_SEAT": "1",
                "HERDR_TELEGRAM_TOPICS_DEVIN_GLM_SEAT_RECREATE": "1",
                "HERDR_TELEGRAM_TOPICS_DEVIN_GLM_SEAT_MAX_PER_RUN": "3",
                "HERDR_TELEGRAM_TOPICS_DEVIN_GLM_MODEL": "glm-5.2",
                "HERDR_TELEGRAM_TOPICS_DEVIN_GLM_PERMISSION_MODE": "dangerous",
                "HERDR_TELEGRAM_TOPICS_DEVIN_GLM_COMMAND": "",
                "HERDR_TELEGRAM_TOPICS_DEVIN_GLM_EXTRA_ARGS": "",
                "HERDR_TELEGRAM_TOPICS_DEVIN_GLM_SEAT_BASE": _seat_base,
            },
            clear=False,
        ), patch.object(herdres, "pane_list", Mock(return_value=panes)), patch.object(herdres, "run_cmd", run_cmd):
            result = herdres.ensure_devin_glm_space_seats(state, panes)

        space = state["spaces"]["workspace:workspace-1"]
        self.assertTrue(result["changed"])
        self.assertEqual(result["started"], 0)
        self.assertIn("devin_glm_seat_closed_at", space)
        self.assertEqual(space["devin_glm_seat_closed_pane_id"], "pane-devin")
        self.assertEqual(space["devin_glm_seat_closed_reason"], "exited")
        self.assertEqual(commands[-1][1:3], ["pane", "run"])

    def test_command_reply_new_kimi_k27_launches_through_devin_with_model_label(self) -> None:
        state = self._shared_state()
        state["panes"]["pane-1"]["foreground_cwd"] = "/tmp/project"
        commands: list[list[str]] = []

        def run_cmd(args, **kwargs):
            commands.append(args)
            proc = Mock()
            proc.returncode = 0
            proc.stderr = ""
            proc.stdout = (
                herdres.json.dumps({"result": {"pane": {"pane_id": "pane-new"}}})
                if args[1:3] == ["pane", "split"]
                else ""
            )
            return proc

        with callback_patches(state), patch.dict(
            herdres.os.environ,
            {
                "HERDR_TELEGRAM_TOPICS_DEVIN_GLM_PERMISSION_MODE": "dangerous",
                "HERDR_TELEGRAM_TOPICS_DEVIN_KIMI_COMMAND": "",
                "HERDR_TELEGRAM_TOPICS_DEVIN_KIMI_EXTRA_ARGS": "",
            },
            clear=False,
        ), patch.object(herdres, "run_cmd", run_cmd):
            result = herdres.command_reply(
                {
                    "chat_id": "-1001",
                    "topic_id": "77",
                    "message_id": "4000",
                    "reply_to_message_id": "",
                    "user_id": "42",
                    "text": "/new kimi-k2.7",
                }
            )

        self.assertTrue(result["handled"])
        self.assertIn("Started Kimi Devin through Devin", result["reply"])
        self.assertEqual(commands[1], [herdres.herdr_bin(), "pane", "rename", "pane-new", "Kimi Devin"])
        self.assertEqual(
            commands[2],
            [
                herdres.herdr_bin(),
                "pane",
                "run",
                "pane-new",
                "devin --model kimi-k2.7 --permission-mode dangerous",
            ],
        )

    def test_command_reply_new_glm_devin_launches_without_mutating_auto_seat_markers(self) -> None:
        state = self._shared_state()
        state["panes"]["pane-1"]["foreground_cwd"] = "/tmp/project"
        auto_seat_markers = self._devin_glm_closed_marker(reason="exited")
        auto_seat_markers.update(self._devin_glm_missing_marker())
        state["spaces"]["workspace:workspace-1"].update(auto_seat_markers)
        commands: list[list[str]] = []

        with callback_patches(state), patch.dict(
            herdres.os.environ,
            {
                "HERDR_TELEGRAM_TOPICS_DEVIN_GLM_PERMISSION_MODE": "dangerous",
                "HERDR_TELEGRAM_TOPICS_DEVIN_GLM_COMMAND": "",
                "HERDR_TELEGRAM_TOPICS_DEVIN_GLM_EXTRA_ARGS": "",
            },
            clear=False,
        ), patch.object(
            herdres,
            "run_cmd",
            self._record_successful_devin_glm_run(commands, pane_id="pane-new"),
        ):
            result = herdres.command_reply(
                {
                    "chat_id": "-1001",
                    "topic_id": "77",
                    "message_id": "4000",
                    "reply_to_message_id": "",
                    "user_id": "42",
                    "text": "/new glm-5.2",
                }
            )

        self.assertTrue(result["handled"])
        self.assertIn("Started GLM Devin through Devin", result["reply"])
        space = state["spaces"]["workspace:workspace-1"]
        self.assertEqual(space["devin_glm_seat_missing_pane_id"], "pane-devin")
        self.assertEqual(space["devin_glm_seat_missing_at"], auto_seat_markers["devin_glm_seat_missing_at"])
        self.assertEqual(space["devin_glm_seat_closed_at"], auto_seat_markers["devin_glm_seat_closed_at"])
        self.assertEqual(space["devin_glm_seat_closed_pane_id"], "pane-devin")
        self.assertEqual(space["devin_glm_seat_closed_reason"], "exited")
        self.assertEqual(commands[1], [herdres.herdr_bin(), "pane", "rename", "pane-new", "GLM Devin"])
        self.assertEqual(
            commands[2],
            [
                herdres.herdr_bin(),
                "pane",
                "run",
                "pane-new",
                "devin --model glm-5.2 --permission-mode dangerous",
            ],
        )

    def test_command_reply_new_without_arg_sends_model_picker(self) -> None:
        state = self._shared_state()
        send_notice = Mock(return_value={"ok": True, "message_id": "555"})

        with callback_patches(state, send_notice=send_notice):
            result = herdres.command_reply(
                {
                    "chat_id": "-1001",
                    "topic_id": "77",
                    "message_id": "4000",
                    "reply_to_message_id": "",
                    "user_id": "42",
                    "text": "/new",
                }
            )

        self.assertTrue(result["handled"])
        self.assertEqual(result["reply"], "")
        _chat_id, title, body = send_notice.call_args.args[:3]
        self.assertEqual(title, "Open a model pane")
        self.assertIn("Labels ending in Devin run through the Devin CLI", body)
        markup = send_notice.call_args.kwargs["reply_markup"]
        button_texts = [button["text"] for row in markup["inline_keyboard"] for button in row]
        self.assertIn("GLM Devin", button_texts)
        self.assertIn("Kimi Devin", button_texts)
        self.assertIn("GPT Devin", button_texts)
        self.assertIn("DeepSeek Devin", button_texts)

    def test_devin_model_labels_use_family_names(self) -> None:
        expected = {
            "claude-opus-4.8": "Claude Devin",
            "gpt-5.5": "GPT Devin",
            "kimi-k2.7": "Kimi Devin",
            "glm-5.2": "GLM Devin",
            "gemini-3.1-pro": "Gemini Devin",
            "deepseek-v4-pro": "DeepSeek Devin",
            "swe-1.6-fast": "SWE Devin",
        }

        for model, label in expected.items():
            with self.subTest(model=model):
                self.assertEqual(herdres.new_pane_launch_spec(model)["label"], label)

    def test_new_pane_picker_callback_launches_glm_through_devin(self) -> None:
        state = self._shared_state()
        state["panes"]["pane-1"]["foreground_cwd"] = "/tmp/project"
        auto_seat_markers = self._devin_glm_closed_marker(reason="exited")
        auto_seat_markers.update(self._devin_glm_missing_marker())
        state["spaces"]["workspace:workspace-1"].update(auto_seat_markers)
        space_token = herdres._callback_id("workspace:workspace-1", "space")[:16]
        commands: list[list[str]] = []

        def run_cmd(args, **kwargs):
            commands.append(args)
            proc = Mock()
            proc.returncode = 0
            proc.stderr = ""
            proc.stdout = (
                herdres.json.dumps({"result": {"pane": {"pane_id": "pane-new"}}})
                if args[1:3] == ["pane", "split"]
                else ""
            )
            return proc

        telegram_api = Mock(return_value={"ok": True, "result": True})
        with callback_patches(state), patch.dict(
            herdres.os.environ,
            {
                "HERDR_TELEGRAM_TOPICS_DEVIN_GLM_PERMISSION_MODE": "dangerous",
                "HERDR_TELEGRAM_TOPICS_DEVIN_GLM_COMMAND": "",
                "HERDR_TELEGRAM_TOPICS_DEVIN_GLM_EXTRA_ARGS": "",
            },
            clear=False,
        ), patch.object(herdres, "run_cmd", run_cmd), patch.object(herdres, "telegram_api", telegram_api):
            result = herdres.callback_reply(
                {
                    "chat_id": "-1001",
                    "topic_id": "77",
                    "message_id": "555",
                    "user_id": "42",
                    "data": f"herdr:np:{space_token}:glm",
                }
            )

        self.assertEqual(result["answer"], "Started GLM Devin through Devin in pane pane-new.")
        self.assertEqual(commands[1], [herdres.herdr_bin(), "pane", "rename", "pane-new", "GLM Devin"])
        self.assertEqual(
            commands[2],
            [
                herdres.herdr_bin(),
                "pane",
                "run",
                "pane-new",
                "devin --model glm-5.2 --permission-mode dangerous",
            ],
        )
        telegram_api.assert_called_once()
        self.assertIn("Started GLM Devin through Devin", telegram_api.call_args.args[1]["text"])
        space = state["spaces"]["workspace:workspace-1"]
        self.assertEqual(space["devin_glm_seat_missing_pane_id"], "pane-devin")
        self.assertEqual(space["devin_glm_seat_missing_at"], auto_seat_markers["devin_glm_seat_missing_at"])
        self.assertEqual(space["devin_glm_seat_closed_at"], auto_seat_markers["devin_glm_seat_closed_at"])
        self.assertEqual(space["devin_glm_seat_closed_pane_id"], "pane-devin")
        self.assertEqual(space["devin_glm_seat_closed_reason"], "exited")

    def test_new_pane_picker_callback_launches_supported_devin_model(self) -> None:
        state = self._shared_state()
        state["panes"]["pane-1"]["foreground_cwd"] = "/tmp/project"
        space_token = herdres._callback_id("workspace:workspace-1", "space")[:16]
        commands: list[list[str]] = []

        def run_cmd(args, **kwargs):
            commands.append(list(args))
            if args[:3] == [herdres.herdr_bin(), "pane", "split"]:
                return SimpleNamespace(returncode=0, stdout=json.dumps({"result": {"pane": {"pane_id": "pane-gpt"}}}), stderr="")
            return SimpleNamespace(returncode=0, stdout="", stderr="")

        with callback_patches(state), patch.object(herdres, "run_cmd", run_cmd), patch.object(herdres, "telegram_api") as telegram_api:
            telegram_api.return_value = {"ok": True}
            result = herdres.callback_reply(
                {
                    "chat_id": "-1001",
                    "topic_id": "77",
                    "message_id": "555",
                    "user_id": "42",
                    "data": f"herdr:np:{space_token}:gpt-5.5",
                }
            )

        self.assertEqual(result["answer"], "Started GPT Devin through Devin in pane pane-gpt.")
        self.assertEqual(commands[1], [herdres.herdr_bin(), "pane", "rename", "pane-gpt", "GPT Devin"])
        self.assertEqual(
            commands[2],
            [
                herdres.herdr_bin(),
                "pane",
                "run",
                "pane-gpt",
                "devin --model gpt-5.5 --permission-mode dangerous",
            ],
        )

    def test_pinned_status_uses_devin_model_label(self) -> None:
        text = herdres.render_pinned_status(
            {},
            [
                {"agent": "devin", "agent_status": "idle", "label": "GLM Devin"},
                {"agent": "codex", "agent_status": "working", "label": "Codex"},
            ]
        )

        self.assertIn("GLM Devin 🟢", text)
        labels = [part.strip().rsplit(" ", 1)[0] for part in text.split("|")]
        self.assertNotIn("Devin", labels)

    def test_callback_reply_routes_by_callback_message_id(self) -> None:
        state = self._shared_state()
        send_to_pane = Mock(return_value=(True, ""))

        with callback_patches(state, send_to_pane=send_to_pane):
            result = herdres.callback_reply(
                {
                    "chat_id": "-1001",
                    "topic_id": "77",
                    "message_id": "3002",
                    "user_id": "42",
                    "data": "herdr:c:prompt2:1",
                }
            )

        self.assertEqual(result["answer"], "Selected 1.")
        send_to_pane.assert_called_once_with("pane-2", "1")

    def test_command_reply_ambiguous_top_level_shared_topic_does_not_send_to_pane(self) -> None:
        state = self._shared_state()
        send_to_pane = Mock(return_value=(True, ""))

        with callback_patches(state, send_to_pane=send_to_pane):
            result = herdres.command_reply(
                {
                    "chat_id": "-1001",
                    "topic_id": "77",
                    "message_id": "4000",
                    "reply_to_message_id": "",
                    "user_id": "42",
                    "text": "/send run tests",
                }
            )

        self.assertTrue(result["handled"])
        self.assertIn("Reply inside a pane thread", result["reply"])
        send_to_pane.assert_not_called()

    def test_command_reply_bot_mention_routes_to_matching_pane_in_shared_topic(self) -> None:
        state = self._shared_state()
        state["telegram"]["managed_bots"] = {
            "codex": {"username": "herdr_codex_bot", "token": "CODEX_TOKEN", "enabled": True},
            "claude": {"username": "herdr_claude_bot", "token": "CLAUDE_TOKEN", "enabled": True},
        }
        state["panes"]["pane-1"]["agent"] = "codex"
        state["panes"]["pane-2"]["agent"] = "claude"
        send_to_pane = Mock(return_value=(True, ""))

        with callback_patches(state, send_to_pane=send_to_pane):
            result = herdres.command_reply(
                {
                    "chat_id": "-1001",
                    "topic_id": "77",
                    "message_id": "4000",
                    "reply_to_message_id": "",
                    "user_id": "42",
                    "text": "@herdr_claude_bot run tests",
                }
            )

        self.assertTrue(result["handled"])
        self.assertEqual(result["reply"], "")
        send_to_pane.assert_called_once_with("pane-2", "run tests")

    def test_command_reply_bot_mention_overrides_mismatched_reply_route(self) -> None:
        state = self._shared_state()
        state["telegram"]["implicit_send_enabled"] = True
        state["telegram"]["managed_bots"] = {
            "codex": {"username": "herdr_codex_bot", "token": "CODEX_TOKEN", "enabled": True},
            "claude": {"username": "herdr_claude_bot", "token": "CLAUDE_TOKEN", "enabled": True},
        }
        state["panes"]["pane-1"]["agent"] = "codex"
        state["panes"]["pane-2"]["agent"] = "claude"
        send_to_pane = Mock(return_value=(True, ""))

        with callback_patches(state, send_to_pane=send_to_pane):
            result = herdres.command_reply(
                {
                    "chat_id": "-1001",
                    "topic_id": "77",
                    "message_id": "4000",
                    "reply_to_message_id": "1001",
                    "user_id": "42",
                    "text": "@herdr_claude_bot run tests",
                }
            )

        self.assertTrue(result["handled"])
        self.assertEqual(result["reply"], "")
        send_to_pane.assert_called_once_with("pane-2", "run tests")


class TelegramDraftTests(unittest.TestCase):
    def test_rich_enabled_recovers_from_non_capability_disabled_reason(self) -> None:
        telegram = {
            "rich_messages": {
                "supported": "no",
                "disabled_reason": "Telegram sendRichMessage failed: Bad Request: chat not found",
            }
        }

        self.assertTrue(herdres.rich_enabled(telegram))
        self.assertEqual(telegram["rich_messages"]["supported"], "unknown")
        self.assertNotIn("disabled_reason", telegram["rich_messages"])

    def test_rich_enabled_keeps_real_capability_disabled_reason(self) -> None:
        telegram = {
            "rich_messages": {
                "supported": "no",
                "disabled_reason": "Telegram sendRichMessage failed: Bad Request: method not found",
            }
        }

        self.assertFalse(herdres.rich_enabled(telegram))
        self.assertEqual(telegram["rich_messages"]["supported"], "no")

    def test_send_rich_message_draft_payload_uses_space_thread_and_root_reply(self) -> None:
        payloads = []
        telegram = {"rich_messages": {"supported": "yes"}}

        def fake_api(method: str, payload: dict) -> dict:
            payloads.append((method, payload))
            return {"ok": True, "result": True}

        with patch.object(herdres, "telegram_api", fake_api):
            result = herdres.send_rich_message_draft(
                "-1001",
                "<b>Partial</b>",
                telegram=telegram,
                fallback_text="Partial",
                draft_id=12345,
                thread_id="77",
                reply_to_message_id="1001",
            )

        self.assertTrue(result["ok"])
        self.assertEqual(payloads[0][0], "sendRichMessageDraft")
        payload = payloads[0][1]
        self.assertEqual(payload["chat_id"], "-1001")
        self.assertEqual(payload["draft_id"], "12345")
        self.assertEqual(payload["message_thread_id"], "77")
        self.assertIn("reply_parameters", payload)
        self.assertIn("rich_message", payload)

    def test_draft_capability_error_disables_streaming_not_final_messages(self) -> None:
        telegram = {"rich_messages": {"supported": "yes"}}
        entry = {
            "pane_key": "pane-1",
            "space_key": "workspace:workspace-1",
            "topic_id": "77",
            "pane_root_message_id": "1001",
        }

        with patch.object(
            herdres,
            "telegram_api",
            Mock(side_effect=herdres.BridgeError("Telegram sendRichMessageDraft failed: Bad Request: method not found")),
        ):
            result = herdres.send_stream_draft(
                "-1001",
                telegram,
                entry,
                turn_id="turn-1",
                text="Partial answer",
            )

        self.assertFalse(result["ok"])
        self.assertEqual(result["kind"], "capability")
        self.assertEqual(telegram["streaming_drafts"]["supported"], "no")
        self.assertTrue(herdres.rich_enabled(telegram))

    def test_dry_run_supports_draft_methods(self) -> None:
        with patch.dict(herdres.os.environ, {"HERDR_TELEGRAM_TOPICS_DRY_RUN": "1"}):
            self.assertTrue(herdres.telegram_api("sendMessageDraft", {"chat_id": "-1001", "draft_id": "1"})["ok"])
            self.assertTrue(herdres.telegram_api("sendRichMessageDraft", {"chat_id": "-1001", "draft_id": "2"})["ok"])

    def test_stream_draft_throttle_skips_unchanged_hash(self) -> None:
        telegram = {"rich_messages": {"supported": "yes"}}
        entry = {
            "pane_key": "pane-1",
            "space_key": "workspace:workspace-1",
            "topic_id": "77",
            "pane_root_message_id": "1001",
        }
        api = Mock(return_value={"ok": True, "result": True})

        with patch.object(herdres, "telegram_api", api):
            first = herdres.send_stream_draft(
                "-1001",
                telegram,
                entry,
                turn_id="turn-1",
                text="Partial answer",
            )
            second = herdres.send_stream_draft(
                "-1001",
                telegram,
                entry,
                turn_id="turn-1",
                text="Partial answer",
            )

        self.assertTrue(first["ok"])
        self.assertTrue(second["skipped"])
        self.assertEqual(api.call_count, 1)
        self.assertEqual(entry["last_stream_hash"], herdres.stream_text_hash("Partial answer"))

    def test_stream_draft_renders_partial_text_as_open_worklog(self) -> None:
        telegram = {"rich_messages": {"supported": "yes"}}
        entry = {
            "pane_key": "pane-1",
            "space_key": "workspace:workspace-1",
            "topic_id": "77",
            "pane_root_message_id": "1001",
        }
        calls: list[tuple[str, dict]] = []

        def fake_api(method: str, payload: dict) -> dict:
            calls.append((method, payload))
            return {"ok": True, "result": True}

        with patch.object(herdres, "telegram_api", fake_api), patch.multiple(
            herdres,
            STREAM_MIN_INTERVAL_SECONDS=0,
            STREAM_MIN_CHARS=0,
        ):
            result = herdres.send_stream_draft(
                "-1001",
                telegram,
                entry,
                turn_id="turn-1",
                text="Partial answer",
            )

        self.assertTrue(result["ok"])
        rich_message = json.loads(calls[0][1]["rich_message"])
        self.assertIn("<details open><summary><b>Working…</b></summary><blockquote>", rich_message["html"])
        self.assertNotIn("<b>Response</b>", rich_message["html"])

    def test_request_elapsed_label_formats_minutes_and_hours(self) -> None:
        self.assertEqual(
            herdres.request_elapsed_label(
                "2026-06-19T10:00:00+00:00",
                now=datetime(2026, 6, 19, 10, 2, 30, tzinfo=timezone.utc),
            ),
            "2m",
        )
        self.assertEqual(
            herdres.request_elapsed_label(
                "2026-06-19T10:00:00+00:00",
                now=datetime(2026, 6, 19, 11, 4, 0, tzinfo=timezone.utc),
            ),
            "1h 4m",
        )

    def test_stream_draft_renders_elapsed_worklog_label(self) -> None:
        telegram = {"rich_messages": {"supported": "yes"}}
        started_at = (datetime.now(timezone.utc) - timedelta(minutes=2, seconds=5)).replace(microsecond=0).isoformat()
        entry = {
            "pane_key": "pane-1",
            "space_key": "workspace:workspace-1",
            "topic_id": "77",
            "pane_root_message_id": "1001",
            "request_turn_id": "turn-1",
            "request_started_at": started_at,
        }
        calls: list[tuple[str, dict]] = []

        def fake_api(method: str, payload: dict) -> dict:
            calls.append((method, payload))
            return {"ok": True, "result": True}

        with patch.object(herdres, "telegram_api", fake_api), patch.multiple(
            herdres,
            STREAM_MIN_INTERVAL_SECONDS=0,
            STREAM_MIN_CHARS=0,
        ):
            result = herdres.send_stream_draft(
                "-1001",
                telegram,
                entry,
                turn_id="turn-1",
                text="Partial answer",
            )

        self.assertTrue(result["ok"])
        rich_message = json.loads(calls[0][1]["rich_message"])
        self.assertIn("<details open><summary><b>Working… (2m)</b></summary><blockquote>", rich_message["html"])

    def test_stream_draft_throttle_enforces_min_interval_between_updates(self) -> None:
        telegram = {"rich_messages": {"supported": "yes"}}
        entry = {
            "pane_key": "pane-1",
            "space_key": "workspace:workspace-1",
            "topic_id": "77",
            "pane_root_message_id": "1001",
        }
        api = Mock(return_value={"ok": True, "result": True})

        with patch.object(herdres, "telegram_api", api), patch.multiple(
            herdres,
            STREAM_MIN_INTERVAL_SECONDS=60,
            STREAM_MIN_CHARS=0,
            MAX_STREAM_DRAFTS=8,
        ):
            first = herdres.send_stream_draft(
                "-1001",
                telegram,
                entry,
                turn_id="turn-1",
                text="Partial answer",
            )
            second = herdres.send_stream_draft(
                "-1001",
                telegram,
                entry,
                turn_id="turn-1",
                text="Partial answer with a little more detail",
            )

        self.assertTrue(first["ok"])
        self.assertTrue(second["skipped"])
        self.assertEqual(second["reason"], "below_min_interval")
        self.assertEqual(api.call_count, 1)

    def test_stream_draft_throttle_enforces_max_updates_per_turn(self) -> None:
        telegram = {"rich_messages": {"supported": "yes"}}
        entry = {
            "pane_key": "pane-1",
            "space_key": "workspace:workspace-1",
            "topic_id": "77",
            "pane_root_message_id": "1001",
        }
        api = Mock(return_value={"ok": True, "result": True})

        with patch.object(herdres, "telegram_api", api), patch.multiple(
            herdres,
            STREAM_MIN_INTERVAL_SECONDS=0,
            STREAM_MIN_CHARS=0,
            MAX_STREAM_DRAFTS=1,
        ):
            first = herdres.send_stream_draft(
                "-1001",
                telegram,
                entry,
                turn_id="turn-1",
                text="Partial answer",
            )
            second = herdres.send_stream_draft(
                "-1001",
                telegram,
                entry,
                turn_id="turn-1",
                text="Partial answer with a little more detail",
            )

        self.assertTrue(first["ok"])
        self.assertTrue(second["skipped"])
        self.assertEqual(second["reason"], "max_stream_updates")
        self.assertEqual(api.call_count, 1)

    def test_stream_message_fallback_sends_then_edits_visible_reply(self) -> None:
        telegram = {"rich_messages": {"supported": "yes"}, "streaming_drafts": {"supported": "no"}}
        entry = {
            "pane_key": "pane-1",
            "space_key": "workspace:workspace-1",
            "topic_id": "77",
            "pane_root_message_id": "1001",
        }
        send_rich = Mock(return_value={"ok": True, "message_id": "5001", "format": "rich"})
        edit_rich = Mock(return_value={"ok": True, "message_id": "5001", "kind": "edited"})

        with patch.multiple(
            herdres,
            send_rich_message=send_rich,
            edit_rich_message=edit_rich,
            STREAM_MIN_INTERVAL_SECONDS=0,
        ):
            first = herdres.send_stream_message(
                "-1001",
                telegram,
                entry,
                turn_id="turn-1",
                text="Partial answer",
            )
            second = herdres.send_stream_message(
                "-1001",
                telegram,
                entry,
                turn_id="turn-1",
                text=(
                    "Partial answer with more detail that is long enough to pass the streaming "
                    "minimum character threshold and update the visible preview."
                ),
            )

        self.assertTrue(first["ok"])
        self.assertTrue(first["sent_message"])
        self.assertEqual(entry["last_stream_message_id"], "5001")
        send_rich.assert_called_once()
        self.assertIn("<details open><summary><b>Working…</b></summary><blockquote>", send_rich.call_args.args[1])
        self.assertNotIn("<b>Response</b>", send_rich.call_args.args[1])
        self.assertEqual(send_rich.call_args.kwargs["thread_id"], "77")
        self.assertIsNone(send_rich.call_args.kwargs["reply_to_message_id"])
        edit_rich.assert_called_once()
        self.assertIn("<details open><summary><b>Working…</b></summary><blockquote>", edit_rich.call_args.args[2])
        self.assertTrue(second["ok"])
        self.assertFalse(second.get("sent_message", False))

    def test_stream_message_edits_visible_reply_after_draft_cap(self) -> None:
        telegram = {"rich_messages": {"supported": "yes"}, "streaming_drafts": {"supported": "no"}}
        entry = {
            "pane_key": "pane-1",
            "space_key": "workspace:workspace-1",
            "topic_id": "77",
            "pane_root_message_id": "1001",
            "last_stream_turn_id": "turn-1",
            "last_stream_update_count": 8,
            "last_stream_hash": "old-hash",
            "last_stream_message_id": "5001",
        }
        send_rich = Mock(return_value={"ok": True, "message_id": "new-message", "format": "rich"})
        edit_rich = Mock(return_value={"ok": True, "message_id": "5001", "kind": "edited"})

        with patch.multiple(
            herdres,
            send_rich_message=send_rich,
            edit_rich_message=edit_rich,
            STREAM_MIN_INTERVAL_SECONDS=0,
            STREAM_MIN_CHARS=0,
            MAX_STREAM_DRAFTS=8,
        ):
            result = herdres.send_stream_message(
                "-1001",
                telegram,
                entry,
                turn_id="turn-1",
                text="Newer worklog that should still edit the visible message after the draft cap.",
            )

        self.assertTrue(result["ok"])
        self.assertEqual(result["format"], "message-edit")
        self.assertFalse(result.get("skipped", False))
        send_rich.assert_not_called()
        edit_rich.assert_called_once()
        self.assertEqual(entry["last_stream_update_count"], 9)
        self.assertEqual(
            entry["last_stream_text"],
            "Newer worklog that should still edit the visible message after the draft cap.",
        )


class ManagedBotTests(unittest.TestCase):
    def test_setup_markup_uses_open_pane_bot_kinds(self) -> None:
        panes = [
            {"agent": "codex", "agent_status": "working"},
            {"agent": "Claude Code", "agent_status": "idle"},
            {"agent": "codex", "agent_status": "blocked"},
            {"agent": "", "agent_status": "idle"},
        ]
        kinds = herdres.managed_bot_kinds_for_panes(panes)
        markup = herdres.managed_bot_setup_reply_markup("ManagerBot", kinds=kinds)
        buttons = [button for row in markup["inline_keyboard"] for button in row]

        self.assertEqual(kinds, ["codex", "claude"])
        self.assertEqual([button["text"] for button in buttons], ["Codex", "Claude"])
        self.assertIn("https://t.me/newbot/ManagerBot/herdr_codex_bot", buttons[0]["url"])
        self.assertIn("name=Herdr%20Codex", buttons[0]["url"])

    def test_private_request_keyboard_uses_managed_bot_buttons(self) -> None:
        keyboard = herdres.managed_bot_request_keyboard()
        first = keyboard["keyboard"][0][0]

        self.assertEqual(first["text"], "Create Codex bot")
        self.assertEqual(first["request_managed_bot"]["request_id"], 41001)
        self.assertEqual(first["request_managed_bot"]["suggested_name"], "Herdr Codex")
        self.assertEqual(first["request_managed_bot"]["suggested_username"], "herdr_codex_bot")

    def test_glm_devin_pane_uses_glm_managed_bot_kind(self) -> None:
        entry = {"agent": "devin", "pane_label_raw": "GLM Devin", "pane_thread_name": "GLM Devin"}

        self.assertEqual(herdres.managed_bot_kind_for_entry(entry), "glm")
        self.assertEqual(
            herdres.managed_bot_kinds_for_panes([
                {"agent": "devin", "label": "GLM Devin", "agent_status": "idle"}
            ]),
            ["glm"],
        )

    def test_kimi_devin_pane_uses_kimi_managed_bot_kind(self) -> None:
        entry = {"agent": "devin", "pane_label_raw": "Kimi Devin", "pane_thread_name": "Kimi Devin"}

        self.assertEqual(herdres.managed_bot_kind_for_entry(entry), "kimi")

    def test_guremi_managed_bot_payload_infers_glm(self) -> None:
        self.assertEqual(
            herdres.managed_bot_kind_for_payload({"username": "Guremi_bot", "first_name": "Guremi"}),
            "glm",
        )

    def test_env_managed_bot_token_assigns_guremi_to_glm(self) -> None:
        telegram = {"managed_bots": {}}

        with patch.dict(
            herdres.os.environ,
            {
                "HERDR_TELEGRAM_TOPICS_MANAGED_BOT_GLM_TOKEN": "GLM_TOKEN",
                "HERDR_TELEGRAM_TOPICS_MANAGED_BOT_GLM_USERNAME": "Guremi_bot",
                "HERDR_TELEGRAM_TOPICS_MANAGED_BOT_GLM_NAME": "Guremi",
            },
            clear=False,
        ):
            changed = herdres.sync_env_managed_bot_tokens(telegram)

        self.assertTrue(changed)
        self.assertEqual(telegram["managed_bots"]["glm"]["token"], "GLM_TOKEN")
        self.assertEqual(telegram["managed_bots"]["glm"]["username"], "Guremi_bot")
        self.assertEqual(telegram["managed_bots"]["glm"]["name"], "Guremi")
        self.assertEqual(telegram["managed_bots"]["glm"]["source"], "manual-env")

    def test_managed_bot_update_fetches_token_and_configures_profile(self) -> None:
        state = {"version": 1, "telegram": {"managed_bots": {}}}
        calls: list[tuple[str, dict, str | None]] = []

        def fake_api(method: str, payload: dict, *, token: str | None = None) -> dict:
            calls.append((method, payload, token))
            if method == "getManagedBotToken":
                return {"ok": True, "result": "CHILD_TOKEN"}
            return {"ok": True, "result": True}

        with patch.multiple(
            herdres,
            load_dotenv=Mock(),
            load_state=Mock(return_value=state),
            save_state=Mock(),
            telegram_api=fake_api,
            managed_bot_profile_photo_path=Mock(return_value=None),
        ):
            result = herdres.managed_bot_update(
                {
                    "managed_bot": {
                        "user": {"id": 42},
                        "bot": {"id": 111, "username": "herdr_codex_bot", "first_name": "Herdr Codex"},
                    }
                }
            )

        self.assertTrue(result["ok"])
        self.assertEqual(result["kind"], "codex")
        self.assertEqual(state["telegram"]["managed_bots"]["codex"]["token"], "CHILD_TOKEN")
        self.assertIn(("getManagedBotToken", {"user_id": "111"}, None), calls)
        self.assertIn(("setMyName", {"name": "Herdr Codex"}, "CHILD_TOKEN"), calls)
        self.assertIn(("setMyDescription", {"description": "Herdr Codex pane bot for Herdres."}, "CHILD_TOKEN"), calls)

    def test_profile_photo_upload_uses_child_bot_token(self) -> None:
        calls: list[tuple[str, dict, dict, str | None]] = []

        def fake_multipart(method: str, fields: dict, files: dict, *, token: str | None = None) -> dict:
            calls.append((method, fields, files, token))
            return {"ok": True, "result": True}

        with tempfile.NamedTemporaryFile(suffix=".jpg") as image:
            image.write(b"\xff\xd8\xff\xd9")
            image.flush()
            with patch.object(herdres, "telegram_api", Mock(return_value={"ok": True, "result": True})), patch.object(
                herdres,
                "telegram_api_multipart",
                fake_multipart,
            ):
                result = herdres.configure_managed_bot_profile("codex", "CHILD_TOKEN", photo_path=Path(image.name))

        self.assertTrue(result["ok"])
        self.assertEqual(calls[0][0], "setMyProfilePhoto")
        self.assertEqual(calls[0][3], "CHILD_TOKEN")
        self.assertIn("attach://profile_photo", calls[0][1]["photo"])
        self.assertIn("profile_photo", calls[0][2])

    def test_stream_update_uses_matching_managed_bot_token(self) -> None:
        telegram = {
            "rich_messages": {"supported": "yes"},
            "streaming_drafts": {"supported": "yes"},
            "managed_bots": {"claude": {"token": "CLAUDE_TOKEN", "enabled": True}},
        }
        entry = {
            "pane_key": "pane-1",
            "space_key": "workspace:workspace-1",
            "topic_id": "77",
            "pane_root_message_id": "1001",
            "agent": "claude",
            "managed_voice_active": True,
        }
        calls: list[tuple[str, dict, str | None]] = []

        def fake_api(method: str, payload: dict, *, token: str | None = None) -> dict:
            calls.append((method, payload, token))
            return {"ok": True, "result": True}

        with patch.object(herdres, "telegram_api", fake_api), patch.multiple(
            herdres,
            STREAM_MIN_INTERVAL_SECONDS=0,
            STREAM_MIN_CHARS=0,
        ):
            result = herdres.send_stream_update(
                "-1001",
                telegram,
                entry,
                turn_id="turn-1",
                text="Partial answer from Claude",
            )

        self.assertTrue(result["ok"])
        self.assertEqual(calls[0][2], "CLAUDE_TOKEN")

    def test_stream_update_renders_user_block_with_open_worklog(self) -> None:
        telegram = {"rich_messages": {"supported": "yes"}, "streaming_drafts": {"supported": "yes"}}
        entry = {
            "pane_key": "pane-1",
            "space_key": "workspace:workspace-1",
            "topic_id": "77",
            "pane_root_message_id": "1001",
        }
        calls: list[tuple[str, dict, str | None]] = []

        def fake_api(method: str, payload: dict, *, token: str | None = None) -> dict:
            calls.append((method, payload, token))
            return {"ok": True, "result": True}

        with patch.object(herdres, "telegram_api", fake_api), patch.multiple(
            herdres,
            STREAM_MIN_INTERVAL_SECONDS=0,
            STREAM_MIN_CHARS=0,
        ):
            result = herdres.send_stream_update(
                "-1001",
                telegram,
                entry,
                turn_id="turn-1",
                text="The CPU profile is still running.",
                user_text="Run the profile.",
            )

        self.assertTrue(result["ok"])
        rich_message = json.loads(calls[0][1]["rich_message"])
        self.assertIn("<details open><summary><b>User:</b></summary><blockquote>", rich_message["html"])
        self.assertIn("Run the profile.", rich_message["html"])
        self.assertIn("<details open><summary><b>Working…</b></summary><blockquote>", rich_message["html"])
        self.assertNotIn("<b>Response</b>", rich_message["html"])

    def test_stream_update_reissues_same_worklog_when_user_block_is_added(self) -> None:
        telegram = {"rich_messages": {"supported": "yes"}, "streaming_drafts": {"supported": "yes"}}
        entry = {
            "pane_key": "pane-1",
            "space_key": "workspace:workspace-1",
            "topic_id": "77",
            "pane_root_message_id": "1001",
            "last_stream_turn_id": "turn-1",
            "last_stream_hash": herdres.stream_text_hash("The CPU profile is still running."),
        }
        calls: list[tuple[str, dict, str | None]] = []

        def fake_api(method: str, payload: dict, *, token: str | None = None) -> dict:
            calls.append((method, payload, token))
            return {"ok": True, "result": True}

        with patch.object(herdres, "telegram_api", fake_api), patch.multiple(
            herdres,
            STREAM_MIN_INTERVAL_SECONDS=0,
            STREAM_MIN_CHARS=0,
        ):
            result = herdres.send_stream_update(
                "-1001",
                telegram,
                entry,
                turn_id="turn-1",
                text="The CPU profile is still running.",
                user_text="Run the profile.",
            )

        self.assertTrue(result["ok"])
        self.assertEqual(len(calls), 1)
        rich_message = json.loads(calls[0][1]["rich_message"])
        self.assertIn("<b>User:</b>", rich_message["html"])


class StreamingIntegrationTests(unittest.TestCase):
    def _state(self) -> tuple[dict, dict, str, dict]:
        pane = {
            "pane_id": "pane-1",
            "terminal_id": "term-1",
            "workspace_id": "workspace-1",
            "tab_id": "tab-1",
            "agent": "codex",
            "agent_status": "working",
            "label": "Build Runner",
        }
        key = herdres.pane_key(pane)
        entry = {
            "pane_key": key,
            "pane_id": "pane-1",
            "space_key": "workspace:workspace-1",
            "topic_id": "77",
            "pane_root_message_id": "1001",
            "pane_thread_name": "Build Runner",
            "last_known_status": "working",
        }
        state = {
            "version": 1,
            "enabled": True,
            "telegram": {"chat_id": "-1001", "general_thread_id": "1", "owner_user_ids": ["42"]},
            "spaces": {
                "workspace:workspace-1": {
                    "space_key": "workspace:workspace-1",
                    "topic_id": "77",
                    "topic_name": "Workspace 1",
                    "pane_keys": [key],
                }
            },
            "panes": {key: entry},
        }
        return state, pane, key, entry

    def _caps(self) -> tuple[dict, dict]:
        counters = {"creates": 0, "sends": 0, "feed_sends": 0, "marker_sends": 0, "verifies": 0, "renames": 0, "icon_updates": 0}
        caps = {"max_creates": 0, "max_sends": 10, "max_feed_sends": 10, "max_marker_sends": 10, "max_verifies": 0}
        return counters, caps

    def _fresh_direct_origin_marker(
        self,
        entry: dict,
        *,
        user_text: str = "Run direct task.",
        after_turn_id: str = "before-turn",
    ) -> None:
        entry.update({
            "direct_origin_at": herdres.utc_now(),
            "direct_origin_text_hash": herdres.stream_text_hash(user_text),
            "direct_origin_after_turn_id": after_turn_id,
        })

    def _assert_direct_origin_fields_cleared(self, entry: dict) -> None:
        for key in (
            "direct_origin_at",
            "direct_origin_text_hash",
            "direct_origin_after_turn_id",
            "direct_origin_turn_id",
            "direct_origin_bound_at",
            "direct_origin_consumed_turn_id",
            "direct_origin_consumed_hash",
            "direct_origin_consumed_at",
        ):
            self.assertNotIn(key, entry)

    def _direct_origin_turn(self, *, complete: bool = False) -> dict:
        if complete:
            return {
                "available": True,
                "complete": True,
                "turn_id": "turn-1",
                "user_text": "Run direct task.",
                "assistant_final_text": "Final answer.",
            }
        return {
            "available": True,
            "complete": False,
            "turn_id": "turn-1",
            "user_text": "Run direct task.",
            "assistant_final_text": "",
            "assistant_stream_text": "Partial answer.",
        }

    def _direct_origin_stream_patches(self, pane_turn: Mock, **overrides):
        patches = {
            "pane_turn": pane_turn,
            "ensure_pane_root_message": Mock(return_value=(False, {"ok": True})),
            "save_state": Mock(),
            "apply_api_error_warning": Mock(return_value={"topic_missing": False, "changed": False}),
            "TURN_FEED_ENABLED": True,
            "CLEAN_FEED_ENABLED": True,
            "STREAMING_DRAFTS_ENABLED": False,
            "LIVE_CARD_ENABLED": False,
            "STATUS_MARKER_ENABLED": False,
            "STATUS_ICON_ENABLED": False,
            "STREAM_MIN_INTERVAL_SECONDS": 0,
            "STREAM_MIN_CHARS": 0,
        }
        patches.update(overrides)
        return patch.multiple(herdres, **patches)

    def test_direct_origin_disabled_streaming_calls_message_fallback_with_allow_disabled(self) -> None:
        state, pane, _key, entry = self._state()
        pane["agent"] = "claude"
        entry["agent"] = "claude"
        self._fresh_direct_origin_marker(entry)
        counters, caps = self._caps()
        stream_hash = herdres.stream_render_hash("Partial answer.", "Run direct task.")
        send_stream_message = Mock(
            return_value={"ok": True, "sent_message": True, "message_id": "3001", "hash": stream_hash}
        )
        send_feed_item = Mock(return_value={"ok": True, "message_id": "2001"})

        with self._direct_origin_stream_patches(
            Mock(return_value=self._direct_origin_turn()),
            send_stream_message=send_stream_message,
            send_feed_item=send_feed_item,
        ):
            changed = herdres.sync_pane_once(state, "-1001", state["telegram"], pane, counters, caps)

        self.assertTrue(changed)
        send_stream_message.assert_called_once()
        self.assertEqual(send_stream_message.call_args.kwargs["turn_id"], "turn-1")
        self.assertEqual(send_stream_message.call_args.kwargs["text"], "Partial answer.")
        self.assertEqual(send_stream_message.call_args.kwargs["user_text"], "Run direct task.")
        self.assertIs(send_stream_message.call_args.kwargs["allow_disabled"], True)
        self.assertEqual(entry["direct_origin_turn_id"], "turn-1")
        self.assertIn("direct_origin_bound_at", entry)
        self.assertEqual(entry["direct_origin_consumed_turn_id"], "turn-1")
        self.assertEqual(entry["direct_origin_consumed_hash"], stream_hash)
        self.assertIn("direct_origin_consumed_at", entry)

    def test_direct_origin_disabled_streaming_one_shot_and_final_edits_stream_anchor(self) -> None:
        state, pane, _key, entry = self._state()
        pane["agent"] = "claude"
        entry["agent"] = "claude"
        self._fresh_direct_origin_marker(entry)
        pane_turn = Mock(side_effect=[
            self._direct_origin_turn(),
            self._direct_origin_turn(),
            self._direct_origin_turn(complete=True),
        ])
        send_rich_message = Mock(return_value={"ok": True, "message_id": "3001"})
        send_feed_item = Mock(return_value={"ok": True, "message_id": "2001"})
        edit_feed_item = Mock(return_value={"ok": True, "message_id": "3001"})

        with self._direct_origin_stream_patches(
            pane_turn,
            send_rich_message=send_rich_message,
            send_feed_item=send_feed_item,
            edit_feed_item=edit_feed_item,
        ):
            counters, caps = self._caps()
            self.assertTrue(herdres.sync_pane_once(state, "-1001", state["telegram"], pane, counters, caps))
            counters, caps = self._caps()
            self.assertFalse(herdres.sync_pane_once(state, "-1001", state["telegram"], pane, counters, caps))
            prompt_send_count = send_feed_item.call_count
            counters, caps = self._caps()
            self.assertTrue(herdres.sync_pane_once(state, "-1001", state["telegram"], pane, counters, caps))

        send_rich_message.assert_called_once()
        rich_html = send_rich_message.call_args.args[1]
        self.assertIn("Run direct task.", rich_html)
        self.assertIn("Partial answer.", rich_html)
        edit_feed_item.assert_called_once()
        self.assertEqual(edit_feed_item.call_args.args[1], "3001")
        final_item = edit_feed_item.call_args.args[2]
        self.assertEqual(final_item["turn_id"], "turn-1")
        self.assertEqual(final_item["assistant_final_text"], "Final answer.")
        self.assertEqual(final_item["worklog_text"], "Partial answer.")
        self.assertEqual(entry["last_clean_message_id"], "3001")
        self.assertEqual(send_feed_item.call_count, prompt_send_count)
        self._assert_direct_origin_fields_cleared(entry)

    def test_direct_origin_disabled_streaming_requires_fresh_unconsumed_marker(self) -> None:
        for case in ("no_marker", "stale_marker", "consumed_marker", "max_sends_exhausted"):
            with self.subTest(case=case):
                state, pane, _key, entry = self._state()
                pane["agent"] = "claude"
                entry["agent"] = "claude"
                if case != "no_marker":
                    self._fresh_direct_origin_marker(entry)
                if case == "stale_marker":
                    entry["direct_origin_at"] = (
                        datetime.now(timezone.utc)
                        - timedelta(seconds=herdres.DIRECT_ORIGIN_MARKER_TTL_SECONDS + 1)
                    ).isoformat()
                if case == "consumed_marker":
                    entry.update({
                        "direct_origin_turn_id": "turn-1",
                        "direct_origin_bound_at": herdres.utc_now(),
                        "direct_origin_consumed_turn_id": "turn-1",
                        "direct_origin_consumed_hash": herdres.stream_render_hash(
                            "Partial answer.",
                            "Run direct task.",
                        ),
                        "direct_origin_consumed_at": herdres.utc_now(),
                    })
                counters, caps = self._caps()
                if case == "max_sends_exhausted":
                    counters["sends"] = caps["max_sends"]
                send_stream_message = Mock(return_value={"ok": True, "sent_message": True, "message_id": "3001"})
                send_feed_item = Mock(return_value={"ok": True, "message_id": "2001"})

                with self._direct_origin_stream_patches(
                    Mock(return_value=self._direct_origin_turn()),
                    send_stream_message=send_stream_message,
                    send_feed_item=send_feed_item,
                ):
                    herdres.sync_pane_once(state, "-1001", state["telegram"], pane, counters, caps)

                send_stream_message.assert_not_called()
                if case == "stale_marker":
                    self._assert_direct_origin_fields_cleared(entry)

    def test_clean_feed_hash_ignores_direct_origin_progress_worklog_text(self) -> None:
        self.assertEqual(herdres.DIRECT_ORIGIN_MARKER_TTL_SECONDS, 1800)
        base = herdres.make_turn_feed_item({
            "available": True,
            "complete": True,
            "turn_id": "turn-1",
            "user_text": "Run direct task.",
            "assistant_final_text": "Final answer.",
        })
        assert base is not None
        with_worklog = dict(base, worklog_text="Partial answer.")
        self.assertEqual(herdres.clean_feed_hash(base), herdres.clean_feed_hash(with_worklog))
        self.assertEqual(
            herdres.clean_feed_hash(base, include_render_version=False),
            herdres.clean_feed_hash(with_worklog, include_render_version=False),
        )

    def test_sync_turn_lifecycle_renders_user_worklog_response_sequence(self) -> None:
        state, pane, _key, entry = self._state()
        state["telegram"]["rich_messages"] = {"supported": "yes"}
        state["telegram"]["streaming_drafts"] = {"supported": "no"}
        user_text = "Run the profile."
        worklog_text = "The CPU profile is still running."
        response_text = "Profile complete."
        pane_turn = Mock(
            side_effect=[
                {
                    "available": True,
                    "complete": False,
                    "turn_id": "turn-1",
                    "user_text": user_text,
                    "assistant_final_text": "",
                    "assistant_stream_text": "",
                },
                {
                    "available": True,
                    "complete": False,
                    "turn_id": "turn-1",
                    "user_text": user_text,
                    "assistant_final_text": "",
                    "assistant_stream_text": worklog_text,
                },
                {
                    "available": True,
                    "complete": True,
                    "turn_id": "turn-1",
                    "user_text": user_text,
                    "assistant_final_text": response_text,
                },
            ]
        )
        api_calls = []
        next_message_id = 2000

        def telegram_api(method, payload, *, token=None):
            nonlocal next_message_id
            api_calls.append((method, dict(payload)))
            if method == "sendRichMessage":
                next_message_id += 1
                return {"ok": True, "result": {"message_id": str(next_message_id)}}
            return {"ok": True, "result": True}

        patch_args = {
            "pane_turn": pane_turn,
            "telegram_api": telegram_api,
            "save_state": Mock(),
            "apply_api_error_warning": Mock(return_value={"topic_missing": False, "changed": False}),
            "TURN_FEED_ENABLED": True,
            "CLEAN_FEED_ENABLED": True,
            "LIVE_CARD_ENABLED": False,
            "STATUS_MARKER_ENABLED": False,
            "STATUS_ICON_ENABLED": False,
            "STREAMING_DRAFTS_ENABLED": True,
            "STREAM_MIN_INTERVAL_SECONDS": 0,
            "STREAM_MIN_CHARS": 0,
        }

        with patch.multiple(herdres, **patch_args):
            counters, caps = self._caps()
            self.assertTrue(herdres.sync_pane_once(state, "-1001", state["telegram"], pane, counters, caps))

            send_calls = [call for call in api_calls if call[0] == "sendRichMessage"]
            edit_calls = [call for call in api_calls if call[0] == "editMessageText"]
            self.assertEqual(len(send_calls), 1)
            self.assertEqual(len(edit_calls), 0)
            self.assertEqual(entry["last_prompt_message_id"], "2001")
            self.assertEqual(entry["last_pane_message_id"], "2001")
            self.assertEqual(entry["request_turn_id"], "turn-1")
            self.assertTrue(entry["request_started_at"])
            prompt_html = json.loads(send_calls[0][1]["rich_message"])["html"]
            self.assertIn("<details open><summary><b>User:</b></summary><blockquote>", prompt_html)
            self.assertIn(user_text, prompt_html)
            self.assertIn("<b>Working…</b>", prompt_html)  # issue #3: reasoning indicator on the bare prompt
            self.assertNotIn("Working… (", prompt_html)    # bare label, no ticking elapsed (no churn)
            self.assertNotIn("<b>Worklog</b>", prompt_html)
            self.assertNotIn("<b>Response</b>", prompt_html)

            counters, caps = self._caps()
            self.assertTrue(herdres.sync_pane_once(state, "-1001", state["telegram"], pane, counters, caps))

            send_calls = [call for call in api_calls if call[0] == "sendRichMessage"]
            edit_calls = [call for call in api_calls if call[0] == "editMessageText"]
            self.assertEqual(len(send_calls), 1)
            self.assertEqual(len(edit_calls), 1)
            self.assertEqual(edit_calls[0][1]["message_id"], "2001")
            working_html = json.loads(edit_calls[0][1]["rich_message"])["html"]
            self.assertIn("<details open><summary><b>User:</b></summary><blockquote>", working_html)
            self.assertIn(user_text, working_html)
            self.assertIn("<details open><summary><b>Working… (1m)</b></summary><blockquote>", working_html)
            self.assertIn(worklog_text, working_html)
            self.assertNotIn("<b>Response</b>", working_html)

            counters, caps = self._caps()
            self.assertTrue(herdres.sync_pane_once(state, "-1001", state["telegram"], pane, counters, caps))

        send_calls = [call for call in api_calls if call[0] == "sendRichMessage"]
        edit_calls = [call for call in api_calls if call[0] == "editMessageText"]
        self.assertEqual(len(send_calls), 1)
        self.assertEqual(len(edit_calls), 2)
        self.assertEqual(edit_calls[1][1]["message_id"], "2001")
        final_html = json.loads(edit_calls[1][1]["rich_message"])["html"]
        final_item = entry["last_clean_item"]
        self.assertEqual(final_item["kind"], "turn")
        self.assertEqual(final_item["user_text"], user_text)
        self.assertEqual(final_item["worklog_text"], worklog_text)
        self.assertEqual(final_item["assistant_final_text"], response_text)
        self.assertEqual(entry["last_clean_message_id"], "2001")
        self.assertIn("<details open><summary><b>User:</b></summary><blockquote>", final_html)
        self.assertIn("<details><summary><b>Worklog (1m)</b></summary><blockquote>", final_html)
        response_prefix = "<details open><summary><b>Response</b></summary>"
        response_body = "<p>Profile complete.</p>"
        self.assertIn(response_prefix + response_body, final_html)
        response_start = final_html.index(response_prefix)
        response_body_start = final_html.index(response_body, response_start)
        self.assertNotIn("<blockquote>", final_html[response_start:response_body_start])
        self.assertLess(final_html.index("<b>User:</b>"), final_html.index("<b>Worklog (1m)</b>"))
        self.assertLess(final_html.index("<b>Worklog (1m)</b>"), final_html.index("<b>Response</b>"))

    def _open_no_content_turn(self) -> dict:
        return {"available": True, "complete": False, "turn_id": "turn-1",
                "user_text": "Run it.", "assistant_final_text": "", "assistant_stream_text": ""}

    def _first_sync_prompt_html(self, state, pane, *, env=None):
        api_calls = []

        def telegram_api(method, payload, *, token=None):
            api_calls.append((method, dict(payload)))
            return {"ok": True, "result": {"message_id": "9001"} if method == "sendRichMessage" else True}

        state["telegram"]["rich_messages"] = {"supported": "yes"}
        ctx = patch.dict(os.environ, env or {})
        with ctx, patch.multiple(
            herdres,
            pane_turn=Mock(return_value=self._open_no_content_turn()),
            telegram_api=telegram_api,
            save_state=Mock(),
            apply_api_error_warning=Mock(return_value={"topic_missing": False, "changed": False}),
            TURN_FEED_ENABLED=True, CLEAN_FEED_ENABLED=True, LIVE_CARD_ENABLED=False,
            STATUS_MARKER_ENABLED=False, STATUS_ICON_ENABLED=False,
        ):
            counters, caps = self._caps()
            herdres.sync_pane_once(state, "-1001", state["telegram"], pane, counters, caps)
        return [json.loads(p["rich_message"])["html"] for m, p in api_calls if m == "sendRichMessage"]

    def test_working_badge_flag_off_omits_reasoning_indicator(self) -> None:
        # With HERDR_TELEGRAM_TOPICS_WORKING_BADGE=0 the open-turn prompt has NO "Working…" (the gate
        # is checked at staging time, not just the render layer).
        state, pane, _key, _entry = self._state()
        sends = self._first_sync_prompt_html(state, pane, env={"HERDR_TELEGRAM_TOPICS_WORKING_BADGE": "0"})
        self.assertTrue(sends)
        self.assertIn("Run it.", "\n".join(sends))
        self.assertNotIn("Working", "\n".join(sends))

    def test_blocked_pane_open_turn_omits_reasoning_indicator(self) -> None:
        # A pane parked on a blocked/awaiting-input prompt is NOT actively working — no "Working…".
        state, pane, _key, entry = self._state()
        pane["agent_status"] = "blocked"
        entry["last_known_status"] = "blocked"
        sends = self._first_sync_prompt_html(state, pane)
        for html in sends:
            self.assertNotIn("Working", html)

    def test_stream_message_edits_latest_prompt_anchor_when_stream_message_is_stale(self) -> None:
        _state, _pane, _key, entry = self._state()
        telegram = {
            "chat_id": "-1001",
            "rich_messages": {"supported": "yes"},
            "streaming_drafts": {"supported": "no"},
        }
        entry.update({
            "last_prompt_turn_id": "turn-2",
            "last_prompt_message_id": "2002",
            "last_pane_message_id": "2002",
            "last_stream_turn_id": "turn-2",
            "last_stream_message_id": "2001",
            "last_stream_hash": "old-hash",
        })
        edit_rich_message = Mock(return_value={"ok": True, "message_id": "2002"})
        send_rich_message = Mock(return_value={"ok": True, "message_id": "2003"})

        with patch.multiple(
            herdres,
            edit_rich_message=edit_rich_message,
            send_rich_message=send_rich_message,
            STREAM_MIN_INTERVAL_SECONDS=0,
            STREAM_MIN_CHARS=0,
        ):
            result = herdres.send_stream_message(
                "-1001",
                telegram,
                entry,
                turn_id="turn-2",
                text="Working on it.",
                user_text="Please check this.",
            )

        self.assertTrue(result["ok"])
        edit_rich_message.assert_called_once()
        self.assertEqual(edit_rich_message.call_args.args[1], "2002")
        send_rich_message.assert_not_called()
        self.assertEqual(entry["last_stream_message_id"], "2002")

    def test_pending_prompt_delivery_clears_stale_stream_message_state(self) -> None:
        state, _pane, _key, entry = self._state()
        entry.update({
            "pending_prompt_turn_id": "turn-2",
            "pending_prompt_text": "Fresh prompt",
            "pending_prompt_hash": herdres.stream_text_hash("Fresh prompt"),
            "last_stream_turn_id": "turn-1",
            "last_stream_message_id": "2001",
            "last_stream_hash": "old-hash",
            "last_stream_text": "Old partial answer.",
            "last_stream_sent_at": herdres.utc_now(),
        })
        send_feed_item = Mock(return_value={"ok": True, "message_id": "2002"})
        counters, caps = self._caps()

        with patch.object(herdres, "send_feed_item", send_feed_item):
            result = herdres.send_pending_prompt_message(
                state,
                "-1001",
                state["telegram"],
                entry,
                counters,
                caps["max_sends"],
                caps["max_feed_sends"],
            )

        self.assertTrue(result["changed"])
        send_feed_item.assert_called_once()
        self.assertEqual(entry["last_prompt_turn_id"], "turn-2")
        self.assertEqual(entry["last_prompt_message_id"], "2002")
        self.assertNotIn("last_stream_turn_id", entry)
        self.assertNotIn("last_stream_message_id", entry)
        self.assertNotIn("last_stream_hash", entry)

    def test_new_pane_baselines_existing_completed_turn_before_sending_future_turns(self) -> None:
        state, pane, key, _entry = self._state()
        state["panes"] = {}
        state["spaces"]["workspace:workspace-1"]["pane_keys"] = []
        pane["agent"] = "claude"
        pane["agent_status"] = "idle"
        old_turn = {
            "available": True,
            "complete": True,
            "turn_id": "old-turn",
            "user_text": "in zsh alias, change claude",
            "assistant_final_text": "Done. Added the alias.",
        }
        new_turn = {
            "available": True,
            "complete": True,
            "turn_id": "new-turn",
            "user_text": "fresh prompt",
            "assistant_final_text": "Fresh response.",
        }
        pane_turn = Mock(side_effect=[old_turn, old_turn, new_turn])
        send_feed_item = Mock(return_value={"ok": True, "message_id": "999"})

        with patch.multiple(
            herdres,
            pane_turn=pane_turn,
            send_feed_item=send_feed_item,
            ensure_pane_root_message=Mock(return_value=(False, {"ok": True})),
            TURN_FEED_ENABLED=True,
            CLEAN_FEED_ENABLED=True,
            LIVE_CARD_ENABLED=False,
            STATUS_MARKER_ENABLED=False,
            STATUS_ICON_ENABLED=False,
        ):
            counters, caps = self._caps()
            self.assertTrue(herdres.sync_pane_once(state, "-1001", state["telegram"], pane, counters, caps))
            entry = state["panes"][key]
            self.assertEqual(entry["last_clean_item"]["turn_id"], "old-turn")
            self.assertEqual(entry["last_clean_suppressed_reason"], "new_pane_initial_turn_baseline")
            send_feed_item.assert_not_called()

            counters, caps = self._caps()
            self.assertFalse(herdres.sync_pane_once(state, "-1001", state["telegram"], pane, counters, caps))
            send_feed_item.assert_not_called()

            counters, caps = self._caps()
            self.assertTrue(herdres.sync_pane_once(state, "-1001", state["telegram"], pane, counters, caps))

        send_feed_item.assert_called_once()
        self.assertEqual(send_feed_item.call_args.args[1]["turn_id"], "new-turn")
        self.assertEqual(state["panes"][key]["last_clean_message_id"], "999")

    def test_sync_completed_turn_resends_final_when_anchor_is_not_latest_message(self) -> None:
        state, pane, _key, entry = self._state()
        state["telegram"]["rich_messages"] = {"supported": "yes"}
        user_text = "Run the profile."
        response_text = "Profile complete."
        entry.update({
            "last_prompt_turn_id": "turn-1",
            "last_prompt_hash": herdres.stream_text_hash(user_text),
            "last_prompt_text": user_text,
            "last_prompt_message_id": "2001",
            "last_pane_message_id": "2002",
        })
        pane_turn = Mock(
            return_value={
                "available": True,
                "complete": True,
                "turn_id": "turn-1",
                "user_text": user_text,
                "assistant_final_text": response_text,
            }
        )
        api_calls = []

        def telegram_api(method, payload, *, token=None):
            api_calls.append((method, dict(payload)))
            if method == "sendRichMessage":
                return {"ok": True, "result": {"message_id": "2003"}}
            return {"ok": True, "result": True}

        with patch.multiple(
            herdres,
            pane_turn=pane_turn,
            telegram_api=telegram_api,
            save_state=Mock(),
            apply_api_error_warning=Mock(return_value={"topic_missing": False, "changed": False}),
            TURN_FEED_ENABLED=True,
            CLEAN_FEED_ENABLED=True,
            LIVE_CARD_ENABLED=False,
            STATUS_MARKER_ENABLED=False,
            STATUS_ICON_ENABLED=False,
        ):
            counters, caps = self._caps()
            self.assertTrue(herdres.sync_pane_once(state, "-1001", state["telegram"], pane, counters, caps))

            counters, caps = self._caps()
            self.assertFalse(herdres.sync_pane_once(state, "-1001", state["telegram"], pane, counters, caps))

        send_calls = [call for call in api_calls if call[0] == "sendRichMessage"]
        edit_calls = [call for call in api_calls if call[0] == "editMessageText"]
        self.assertEqual(len(send_calls), 1)
        self.assertEqual(len(edit_calls), 0)
        final_html = json.loads(send_calls[0][1]["rich_message"])["html"]
        self.assertIn("<details open><summary><b>User:</b></summary><blockquote>", final_html)
        response_prefix = "<details open><summary><b>Response</b></summary>"
        response_body = "<p>Profile complete.</p>"
        self.assertIn(response_prefix + response_body, final_html)
        response_start = final_html.index(response_prefix)
        response_body_start = final_html.index(response_body, response_start)
        self.assertNotIn("<blockquote>", final_html[response_start:response_body_start])
        self.assertEqual(entry["last_clean_message_id"], "2003")
        self.assertEqual(entry["last_pane_message_id"], "2003")

    def test_sync_sends_prompt_then_draft_for_incomplete_turn_stream_text(self) -> None:
        state, pane, _key, entry = self._state()
        counters, caps = self._caps()
        pane_turn = Mock(
            return_value={
                "available": True,
                "complete": False,
                "turn_id": "turn-1",
                "user_text": "Still running?",
                "assistant_final_text": "",
                "assistant_stream_text": "Partial answer.",
            }
        )
        send_stream = Mock(return_value={"ok": True, "draft_id": "123", "hash": "abc"})
        send_feed_item = Mock(return_value={"ok": True, "message_id": "2001"})

        with patch.multiple(
            herdres,
            pane_turn=pane_turn,
            send_stream_update=send_stream,
            send_feed_item=send_feed_item,
            save_state=Mock(),
            apply_api_error_warning=Mock(return_value={"topic_missing": False, "changed": False}),
            TURN_FEED_ENABLED=True,
            LIVE_CARD_ENABLED=False,
            STATUS_MARKER_ENABLED=False,
            STATUS_ICON_ENABLED=False,
        ):
            changed = herdres.sync_pane_once(state, "-1001", state["telegram"], pane, counters, caps)

        self.assertTrue(changed)
        send_stream.assert_called_once()
        self.assertEqual(send_stream.call_args.kwargs["turn_id"], "turn-1")
        self.assertEqual(send_stream.call_args.kwargs["text"], "Partial answer.")
        send_feed_item.assert_called_once()
        prompt_item = send_feed_item.call_args.args[1]
        self.assertEqual(prompt_item["kind"], "prompt")
        self.assertEqual(prompt_item["turn_id"], "turn-1")
        self.assertEqual(prompt_item["user_text"], "Still running?")
        self.assertEqual(send_feed_item.call_args.kwargs["thread_id"], "77")
        self.assertIsNone(send_feed_item.call_args.kwargs["reply_to_message_id"])
        self.assertEqual(entry["last_prompt_turn_id"], "turn-1")
        self.assertEqual(entry["last_prompt_message_id"], "2001")

    def test_sync_does_not_send_older_completed_turn_after_newer_prompt(self) -> None:
        state, pane, _key, entry = self._state()
        entry.update({
            "last_clean_item": {
                "kind": "turn",
                "turn_id": "turn-0",
                "user_text": "Previous prompt",
                "assistant_final_text": "Previous answer.",
            },
            "last_prompt_turn_id": "turn-2",
            "last_prompt_text": "Current prompt",
            "last_prompt_message_id": "2002",
        })
        counters, caps = self._caps()
        pane_turn = Mock(
            return_value={
                "available": True,
                "complete": False,
                "turn_id": "turn-2",
                "user_text": "Current prompt",
                "assistant_final_text": "",
                "assistant_stream_text": "Current worklog.",
                "recent_turns": [
                    {
                        "available": True,
                        "complete": True,
                        "turn_id": "turn-0",
                        "user_text": "Previous prompt",
                        "assistant_final_text": "Previous answer.",
                    },
                    {
                        "available": True,
                        "complete": True,
                        "turn_id": "turn-1",
                        "user_text": "Older prompt",
                        "assistant_final_text": "Older answer that should not replay.",
                    },
                    {
                        "available": True,
                        "complete": False,
                        "turn_id": "turn-2",
                        "user_text": "Current prompt",
                        "assistant_final_text": "",
                        "assistant_stream_text": "Current worklog.",
                    },
                ],
            }
        )
        send_stream = Mock(return_value={"ok": True, "draft_id": "123", "hash": "abc"})
        send_feed_item = Mock(return_value={"ok": True, "message_id": "3001"})

        with patch.multiple(
            herdres,
            pane_turn=pane_turn,
            send_stream_update=send_stream,
            send_feed_item=send_feed_item,
            save_state=Mock(),
            apply_api_error_warning=Mock(return_value={"topic_missing": False, "changed": False}),
            TURN_FEED_ENABLED=True,
            LIVE_CARD_ENABLED=False,
            STATUS_MARKER_ENABLED=False,
            STATUS_ICON_ENABLED=False,
        ):
            changed = herdres.sync_pane_once(state, "-1001", state["telegram"], pane, counters, caps)

        self.assertTrue(changed)
        send_feed_item.assert_not_called()
        send_stream.assert_called_once()
        self.assertEqual(send_stream.call_args.kwargs["turn_id"], "turn-2")

    def test_sync_does_not_send_completed_turn_when_newer_open_prompt_is_visible(self) -> None:
        state, pane, _key, entry = self._state()
        entry.update({
            "last_prompt_turn_id": "turn-fix",
            "last_prompt_text": "fix all of these yourself",
            "last_prompt_message_id": "2002",
        })
        counters, caps = self._caps()
        pane_turn = Mock(
            return_value={
                "available": True,
                "complete": True,
                "turn_id": "turn-review",
                "user_text": "review\nAll tasks complete.",
                "assistant_final_text": "Old review response that should not attach to the newer prompt.",
                "has_open_turn": True,
                "open_turn_id": "turn-fix",
                "open_user_text": "fix all of these yourself",
                "assistant_stream_text": "Current fix worklog.",
                "recent_turns": [
                    {
                        "available": True,
                        "complete": True,
                        "turn_id": "turn-review",
                        "user_text": "review\nAll tasks complete.",
                        "assistant_final_text": "Old review response that should not attach to the newer prompt.",
                    },
                ],
            }
        )
        send_stream = Mock(return_value={"ok": True, "draft_id": "123", "hash": "abc"})
        send_feed_item = Mock(return_value={"ok": True, "message_id": "3001"})

        with patch.multiple(
            herdres,
            pane_turn=pane_turn,
            send_stream_update=send_stream,
            send_feed_item=send_feed_item,
            save_state=Mock(),
            apply_api_error_warning=Mock(return_value={"topic_missing": False, "changed": False}),
            TURN_FEED_ENABLED=True,
            LIVE_CARD_ENABLED=False,
            STATUS_MARKER_ENABLED=False,
            STATUS_ICON_ENABLED=False,
        ):
            changed = herdres.sync_pane_once(state, "-1001", state["telegram"], pane, counters, caps)

        self.assertTrue(changed)
        send_feed_item.assert_not_called()
        send_stream.assert_called_once()
        self.assertEqual(send_stream.call_args.kwargs["turn_id"], "turn-fix")
        self.assertEqual(entry["last_clean_suppressed_reason"], "newer_prompt_already_visible")
        self.assertEqual(entry["last_clean_item"]["turn_id"], "turn-review")

    def test_sync_stream_does_not_inherit_stale_prompt_from_different_turn(self) -> None:
        state, pane, _key, entry = self._state()
        entry.update({
            "last_prompt_turn_id": "old-turn",
            "last_prompt_text": "--yolo",
            "last_prompt_message_id": "2001",
            "last_pane_message_id": "2001",
        })
        counters, caps = self._caps()
        pane_turn = Mock(
            return_value={
                "available": True,
                "complete": False,
                "turn_id": "current-visible",
                "user_text": "",
                "assistant_final_text": "",
                "assistant_stream_text": "Current visible Devin worklog.",
            }
        )
        send_stream = Mock(return_value={"ok": True, "message_id": "3001", "sent_message": True})

        with patch.multiple(
            herdres,
            pane_turn=pane_turn,
            send_stream_update=send_stream,
            save_state=Mock(),
            apply_api_error_warning=Mock(return_value={"topic_missing": False, "changed": False}),
            TURN_FEED_ENABLED=True,
            LIVE_CARD_ENABLED=False,
            STATUS_MARKER_ENABLED=False,
            STATUS_ICON_ENABLED=False,
        ):
            changed = herdres.sync_pane_once(state, "-1001", state["telegram"], pane, counters, caps)

        self.assertTrue(changed)
        send_stream.assert_called_once()
        self.assertEqual(send_stream.call_args.kwargs["turn_id"], "current-visible")
        self.assertEqual(send_stream.call_args.kwargs["user_text"], "")

    def test_sync_suppresses_duplicate_native_session_open_turn_in_same_space(self) -> None:
        state, pane_a, key_a, entry_a = self._state()
        pane_a.update({"agent": "devin", "agent_session": {"value": "oval-panda"}})
        entry_a.update({"agent": "devin", "agent_session_id": "oval-panda"})
        pane_b = {
            **pane_a,
            "pane_id": "pane-2",
            "terminal_id": "term-2",
            "label": "Duplicate Devin",
        }
        key_b = herdres.pane_key(pane_b)
        entry_b = {
            **entry_a,
            "pane_key": key_b,
            "pane_id": "pane-2",
            "pane_root_message_id": "1002",
            "pane_thread_name": "Duplicate Devin",
        }
        state["panes"][key_b] = entry_b
        state["spaces"]["workspace:workspace-1"]["pane_keys"] = [key_a, key_b]
        turn = {
            "available": True,
            "complete": False,
            "turn_id": "4807",
            "user_text": "OFFENSIVE - two under-probed surfaces",
            "assistant_final_text": "",
            "assistant_stream_text": "This is the gRPC client side. Let me find the server side.",
        }
        counters, caps = self._caps()
        pane_turn = Mock(return_value=turn)
        send_feed_item = Mock(return_value={"ok": True, "message_id": "6411"})
        send_stream = Mock(return_value={"ok": True, "message_id": "6412", "sent_message": True})

        with patch.multiple(
            herdres,
            pane_turn=pane_turn,
            send_feed_item=send_feed_item,
            send_stream_update=send_stream,
            save_state=Mock(),
            apply_api_error_warning=Mock(return_value={"topic_missing": False, "changed": False}),
            TURN_FEED_ENABLED=True,
            LIVE_CARD_ENABLED=False,
            STATUS_MARKER_ENABLED=False,
            STATUS_ICON_ENABLED=False,
        ):
            duplicate_changed = herdres.sync_pane_once(state, "-1001", state["telegram"], pane_b, counters, caps)
            owner_changed = herdres.sync_pane_once(state, "-1001", state["telegram"], pane_a, counters, caps)

        self.assertTrue(duplicate_changed)
        self.assertTrue(owner_changed)
        send_feed_item.assert_called_once()
        send_stream.assert_called_once()
        self.assertEqual(entry_b["last_prompt_turn_id"], "4807")
        self.assertEqual(entry_b["last_stream_turn_id"], "4807")
        self.assertEqual(entry_b["last_prompt_suppressed_reason"], f"duplicate_native_session:{key_a}")
        self.assertEqual(entry_b["last_stream_suppressed_reason"], f"duplicate_native_session:{key_a}")

    def test_sync_does_not_repost_render_only_change_without_message_id(self) -> None:
        state, pane, _key, entry = self._state()
        counters, caps = self._caps()
        turn = {
            "available": True,
            "complete": True,
            "turn_id": "turn-1",
            "user_text": "Run tests",
            "assistant_final_text": "Tests passed.",
        }
        item = herdres.make_turn_feed_item(turn)
        assert item is not None
        entry.update({
            "last_clean_item": item,
            "last_clean_text": herdres.item_plain_text(item),
            "last_clean_semantic_hash": herdres.clean_feed_hash(item, include_render_version=False),
            "last_clean_hash": "old-render-hash",
            "last_clean_render_hash": "old-render-hash",
        })
        pane_turn = Mock(return_value=turn)
        send_feed_item = Mock(return_value={"ok": True, "message_id": "3001"})

        with patch.multiple(
            herdres,
            pane_turn=pane_turn,
            send_feed_item=send_feed_item,
            save_state=Mock(),
            apply_api_error_warning=Mock(return_value={"topic_missing": False, "changed": False}),
            TURN_FEED_ENABLED=True,
            LIVE_CARD_ENABLED=False,
            STATUS_MARKER_ENABLED=False,
            STATUS_ICON_ENABLED=False,
        ):
            changed = herdres.sync_pane_once(state, "-1001", state["telegram"], pane, counters, caps)

        self.assertTrue(changed)
        send_feed_item.assert_not_called()
        self.assertEqual(entry["last_clean_hash"], herdres.clean_feed_hash(item))

    def test_worklog_on_delivered_turn_does_not_repost(self) -> None:
        """A worklog attached to an ALREADY-DELIVERED turn must be a no-op.

        worklog_text is excluded from clean_feed_hash, item_plain_text, and item["text"],
        so re-parsing a completed turn with a worklog neither re-sends nor edits it.
        Without this, the worklog flips the dedup hash and re-delivers the turn as a
        DUPLICATE (the issue #3 live-test regression).
        """
        state, pane, _key, entry = self._state()
        counters, caps = self._caps()
        delivered = {
            "available": True,
            "complete": True,
            "turn_id": "turn-1",
            "user_text": "Run tests",
            "assistant_final_text": "Tests passed.",
        }
        item = herdres.make_turn_feed_item(delivered)
        assert item is not None
        entry.update({
            "last_clean_item": item,
            "last_clean_text": herdres.item_plain_text(item),
            "last_clean_semantic_hash": herdres.clean_feed_hash(item, include_render_version=False),
            "last_clean_hash": herdres.clean_feed_hash(item),
            "last_clean_render_hash": herdres.clean_feed_hash(item),
            "last_clean_message_id": "3001",
            "last_turn_id": "turn-1",
        })
        with_worklog = dict(delivered, worklog_text="Bash go test ./...\nEdit engine.go")
        worklog_item = herdres.make_turn_feed_item(with_worklog)

        # The worklog must not change any dedup input...
        self.assertEqual(herdres.clean_feed_hash(item), herdres.clean_feed_hash(worklog_item))
        self.assertEqual(
            herdres.clean_feed_hash(item, include_render_version=False),
            herdres.clean_feed_hash(worklog_item, include_render_version=False),
        )
        self.assertEqual(herdres.item_plain_text(item), herdres.item_plain_text(worklog_item))
        # ...but is still carried on the item for rendering.
        self.assertIn("Bash go test", worklog_item["worklog_text"])

        send_feed_item = Mock(return_value={"ok": True, "message_id": "3002"})
        edit_feed_item = Mock(return_value={"ok": True})
        with patch.multiple(
            herdres,
            pane_turn=Mock(return_value=with_worklog),
            send_feed_item=send_feed_item,
            edit_feed_item=edit_feed_item,
            save_state=Mock(),
            apply_api_error_warning=Mock(return_value={"topic_missing": False, "changed": False}),
            TURN_FEED_ENABLED=True,
            LIVE_CARD_ENABLED=False,
            STATUS_MARKER_ENABLED=False,
            STATUS_ICON_ENABLED=False,
        ):
            herdres.sync_pane_once(state, "-1001", state["telegram"], pane, counters, caps)

        # The invariant: NO new message (no duplicate). The worklog may only ride along
        # as an in-place edit of the turn's OWN existing message (3001), never a fresh send.
        send_feed_item.assert_not_called()
        for call in edit_feed_item.call_args_list:
            self.assertEqual(call.args[1], "3001", "worklog must only edit the turn's own message")

    def test_worklog_label_elapsed_tick_does_not_change_hash(self) -> None:
        """worklog_label embeds a wall-clock elapsed time ("Worklog (1h 11m)"), so it must
        stay OUT of the render hash. Otherwise a long-idle pane whose turn still carries a
        worklog re-render-delivers (edits) its message every minute forever as the clock
        ticks — the live re-delivery loop this guards against (found via p19)."""
        base = {"available": True, "complete": True, "turn_id": "t1",
                "user_text": "q", "assistant_final_text": "done", "worklog_text": "Bash echo hi"}
        item = herdres.make_turn_feed_item(base)
        assert item is not None
        a = dict(item, worklog_label="Worklog (1h 9m)")
        b = dict(item, worklog_label="Worklog (1h 11m)")
        self.assertEqual(herdres.clean_feed_hash(a), herdres.clean_feed_hash(b))
        self.assertEqual(
            herdres.clean_feed_hash(a, include_render_version=False),
            herdres.clean_feed_hash(b, include_render_version=False),
        )

    def test_working_badge_label_is_hash_invariant(self) -> None:
        """Issue #3: the 'Working…' badge lives only in worklog_label (excluded from the render
        hash), so swapping 'Worklog' for 'Working…' must NOT change clean_feed_hash/item_plain_text
        — otherwise the badge would re-deliver/edit-loop the turn (the exact live regression #3 avoids)."""
        base = {"available": True, "complete": True, "turn_id": "t1",
                "user_text": "q", "assistant_final_text": "done", "worklog_text": "Bash echo hi"}
        item = herdres.make_turn_feed_item(base)
        assert item is not None
        worklog = dict(item, worklog_label="Worklog (1m)")
        working = dict(item, worklog_label="Working… (1m)")
        self.assertEqual(herdres.clean_feed_hash(worklog), herdres.clean_feed_hash(working))
        self.assertEqual(herdres.item_plain_text(worklog), herdres.item_plain_text(working))

    def test_prompt_item_renders_working_indicator(self) -> None:
        """Issue #3: a prompt item carrying working_label renders a bare 'Working…' indicator after
        the User block (the reasoning window); without it the base prompt render is unchanged."""
        item = herdres.make_prompt_feed_item("turn-1", "Profile the CPU.")
        base = herdres.render_feed_item_html(item)
        self.assertNotIn("Working", base)
        item["working_label"] = herdres.WORKING_LABEL
        html = herdres.render_feed_item_html(item)
        self.assertIn("<details open><summary><b>User:</b></summary><blockquote>", html)
        self.assertIn("Profile the CPU.", html)
        self.assertIn("<b>Working…</b>", html)
        self.assertNotIn("Working… (", html)  # bare label, no ticking elapsed

    def test_prompt_working_label_is_hash_invariant(self) -> None:
        """The prompt 'Working…' indicator lives in working_label only — excluded from clean_feed_hash
        and item_plain_text — so it can never churn/re-edit the prompt message."""
        item = herdres.make_prompt_feed_item("turn-1", "q")
        with_label = dict(item, working_label=herdres.WORKING_LABEL)
        self.assertEqual(herdres.clean_feed_hash(item), herdres.clean_feed_hash(with_label))
        self.assertEqual(herdres.item_plain_text(item), herdres.item_plain_text(with_label))

    def test_worklog_label_working_badge_toggle(self) -> None:
        """worklog_label_for_turn(working=True) shows 'Working…' when the badge flag is on (default)
        and reverts to 'Worklog' when working=False or HERDR_TELEGRAM_TOPICS_WORKING_BADGE=0."""
        entry: dict = {}  # no request_started_at -> bare label, no elapsed suffix
        self.assertEqual(herdres.worklog_label_for_turn(entry, "t1", working=True), "Working…")
        self.assertEqual(herdres.worklog_label_for_turn(entry, "t1", working=False), "Worklog")
        with patch.dict(os.environ, {"HERDR_TELEGRAM_TOPICS_WORKING_BADGE": "0"}):
            self.assertEqual(herdres.worklog_label_for_turn(entry, "t1", working=True), "Worklog")

    def test_collapse_response_flag_folds_only_the_response(self) -> None:
        base = {"available": True, "complete": True, "turn_id": "t1",
                "user_text": "q", "assistant_final_text": "## Heading\nthe answer body"}
        item = herdres.make_turn_feed_item(base)
        expanded = herdres.render_turn_item_html(item)
        collapsed = herdres.render_turn_item_html(dict(item, collapse_response=True))
        # latest turn: Response details is open
        self.assertIn("<details open><summary><b>Response</b>", expanded)
        # previous turn: Response details is folded (no ` open`), with a preview
        self.assertNotIn("<details open><summary><b>Response</b>", collapsed)
        self.assertIn("<details><summary><b>Response</b>", collapsed)
        # the response body is preserved either way (collapse is a fold, not a drop)
        self.assertIn("the answer body", collapsed)

    def test_new_turn_folds_previous_turn_response_when_enabled(self) -> None:
        state, pane, _key, entry = self._state()
        counters, caps = self._caps()
        space_key = str(entry.get("space_key") or "")
        state.setdefault("spaces", {}).setdefault(space_key, {})["collapse_previous_responses"] = True
        prior_item = herdres.make_turn_feed_item(
            {"available": True, "complete": True, "turn_id": "turn-1",
             "user_text": "q1", "assistant_final_text": "Answer one."})
        entry.update({
            "last_clean_item": prior_item,
            "last_clean_text": herdres.item_plain_text(prior_item),
            "last_clean_semantic_hash": herdres.clean_feed_hash(prior_item, include_render_version=False),
            "last_clean_hash": herdres.clean_feed_hash(prior_item),
            "last_clean_render_hash": herdres.clean_feed_hash(prior_item),
            "last_clean_message_id": "3001",
            "last_turn_id": "turn-1",
        })
        turn2 = {"available": True, "complete": True, "turn_id": "turn-2",
                 "user_text": "q2", "assistant_final_text": "Answer two."}
        send_feed_item = Mock(return_value={"ok": True, "message_id": "3002"})
        edit_feed_item = Mock(return_value={"ok": True})
        with patch.multiple(
            herdres,
            pane_turn=Mock(return_value=turn2),
            send_feed_item=send_feed_item,
            edit_feed_item=edit_feed_item,
            save_state=Mock(),
            apply_api_error_warning=Mock(return_value={"topic_missing": False, "changed": False}),
            TURN_FEED_ENABLED=True, LIVE_CARD_ENABLED=False,
            STATUS_MARKER_ENABLED=False, STATUS_ICON_ENABLED=False,
        ):
            herdres.sync_pane_once(state, "-1001", state["telegram"], pane, counters, caps)

        # the new turn was sent as its own fresh message...
        send_feed_item.assert_called()
        # ...and the PREVIOUS turn's message (3001) was folded in place
        folds = [c for c in edit_feed_item.call_args_list if c.args[1] == "3001"]
        self.assertTrue(folds, "previous turn's message should be folded")
        folded_item = folds[-1].args[2]
        self.assertTrue(folded_item.get("collapse_response"))
        self.assertEqual(str(folded_item.get("turn_id")), "turn-1")

    def test_collapse_default_is_read_at_runtime(self) -> None:
        """The collapse default must be read at CALL time, not frozen at import. The Herdr
        plugin runs `herdres event` with no systemd EnvironmentFile, so the flag is set by
        load_dotenv() AFTER import; a frozen constant would be False and silently skip the
        fold on every plugin-delivered turn (the bug behind 'previous responses not
        collapsing'). Mirrors the per_agent_topics_enabled runtime-read fix."""
        with patch.dict(os.environ, {"HERDR_TELEGRAM_TOPICS_RESPONSE_COLLAPSE_PREVIOUS": "1"}, clear=False):
            self.assertTrue(herdres.response_collapse_previous_default())
            self.assertTrue(herdres.space_collapse_previous_responses({"spaces": {}}, {"space_key": "s"}))
        with patch.dict(os.environ, {"HERDR_TELEGRAM_TOPICS_RESPONSE_COLLAPSE_PREVIOUS": ""}, clear=False):
            self.assertFalse(herdres.response_collapse_previous_default())
            self.assertFalse(herdres.space_collapse_previous_responses({"spaces": {}}, {"space_key": "s"}))

    def test_new_turn_does_not_fold_previous_when_disabled(self) -> None:
        state, pane, _key, entry = self._state()
        counters, caps = self._caps()
        # toggle left at default (off)
        prior_item = herdres.make_turn_feed_item(
            {"available": True, "complete": True, "turn_id": "turn-1",
             "user_text": "q1", "assistant_final_text": "Answer one."})
        entry.update({
            "last_clean_item": prior_item,
            "last_clean_text": herdres.item_plain_text(prior_item),
            "last_clean_semantic_hash": herdres.clean_feed_hash(prior_item, include_render_version=False),
            "last_clean_hash": herdres.clean_feed_hash(prior_item),
            "last_clean_render_hash": herdres.clean_feed_hash(prior_item),
            "last_clean_message_id": "3001",
            "last_turn_id": "turn-1",
        })
        turn2 = {"available": True, "complete": True, "turn_id": "turn-2",
                 "user_text": "q2", "assistant_final_text": "Answer two."}
        edit_feed_item = Mock(return_value={"ok": True})
        with patch.multiple(
            herdres,
            pane_turn=Mock(return_value=turn2),
            send_feed_item=Mock(return_value={"ok": True, "message_id": "3002"}),
            edit_feed_item=edit_feed_item,
            save_state=Mock(),
            apply_api_error_warning=Mock(return_value={"topic_missing": False, "changed": False}),
            TURN_FEED_ENABLED=True, LIVE_CARD_ENABLED=False,
            STATUS_MARKER_ENABLED=False, STATUS_ICON_ENABLED=False,
        ):
            herdres.sync_pane_once(state, "-1001", state["telegram"], pane, counters, caps)
        self.assertFalse([c for c in edit_feed_item.call_args_list if c.args[1] == "3001"],
                         "with the toggle off, the previous turn must not be folded")

    def test_sync_streams_open_turn_after_completed_turn_already_delivered(self) -> None:
        state, pane, _key, entry = self._state()
        completed_turn = {
            "available": True,
            "complete": True,
            "turn_id": "turn-1",
            "user_text": "Previous prompt",
            "assistant_final_text": "Previous final answer.",
        }
        completed_item = herdres.make_turn_feed_item(completed_turn)
        assert completed_item is not None
        entry.update({
            "last_turn_id": "turn-1",
            "last_clean_item": completed_item,
            "last_clean_text": herdres.item_plain_text(completed_item),
            "last_clean_hash": herdres.clean_feed_hash(completed_item),
            "last_clean_render_hash": herdres.clean_feed_hash(completed_item),
            "last_clean_semantic_hash": herdres.clean_feed_hash(completed_item, include_render_version=False),
            "last_clean_message_id": "2001",
            "last_clean_kind": "turn",
        })
        counters, caps = self._caps()
        pane_turn = Mock(
            return_value={
                **completed_turn,
                "has_open_turn": True,
                "open_turn_id": "turn-2",
                "open_user_text": "Next prompt",
                "assistant_stream_text": "Partial next answer.",
                "stream_revision": "stream-rev-2",
                "recent_turns": [completed_turn],
            }
        )
        send_stream = Mock(return_value={"ok": True, "draft_id": "123", "hash": "abc"})
        send_feed_item = Mock(return_value={"ok": True, "message_id": "2002"})

        with patch.multiple(
            herdres,
            pane_turn=pane_turn,
            send_stream_update=send_stream,
            send_feed_item=send_feed_item,
            save_state=Mock(),
            apply_api_error_warning=Mock(return_value={"topic_missing": False, "changed": False}),
            TURN_FEED_ENABLED=True,
            LIVE_CARD_ENABLED=False,
            STATUS_MARKER_ENABLED=False,
            STATUS_ICON_ENABLED=False,
        ):
            changed = herdres.sync_pane_once(state, "-1001", state["telegram"], pane, counters, caps)

        self.assertTrue(changed)
        send_stream.assert_called_once()
        self.assertEqual(send_stream.call_args.kwargs["turn_id"], "turn-2")
        self.assertEqual(send_stream.call_args.kwargs["text"], "Partial next answer.")
        send_feed_item.assert_called_once()
        prompt_item = send_feed_item.call_args.args[1]
        self.assertEqual(prompt_item["kind"], "prompt")
        self.assertEqual(prompt_item["turn_id"], "turn-2")
        self.assertEqual(prompt_item["user_text"], "Next prompt")
        self.assertEqual(entry["last_prompt_turn_id"], "turn-2")

    def test_completed_turn_after_stream_sends_final_and_clears_stream_state(self) -> None:
        state, pane, _key, entry = self._state()
        entry.update({
            "last_stream_hash": "old",
            "last_stream_turn_id": "turn-1",
            "last_stream_draft_id": "123",
            "last_stream_sent_at": herdres.utc_now(),
        })
        counters, caps = self._caps()
        pane_turn = Mock(
            return_value={
                "available": True,
                "complete": True,
                "turn_id": "turn-1",
                "user_text": "Still running?",
                "assistant_final_text": "Final answer.",
            }
        )
        send_feed_item = Mock(return_value={"ok": True, "message_id": "2001"})

        with patch.multiple(
            herdres,
            pane_turn=pane_turn,
            send_feed_item=send_feed_item,
            save_state=Mock(),
            apply_api_error_warning=Mock(return_value={"topic_missing": False, "changed": False}),
            TURN_FEED_ENABLED=True,
            LIVE_CARD_ENABLED=False,
            STATUS_MARKER_ENABLED=False,
            STATUS_ICON_ENABLED=False,
        ):
            changed = herdres.sync_pane_once(state, "-1001", state["telegram"], pane, counters, caps)

        self.assertTrue(changed)
        send_feed_item.assert_called_once()
        self.assertEqual(entry["last_clean_message_id"], "2001")
        self.assertNotIn("last_stream_hash", entry)
        self.assertNotIn("last_stream_draft_id", entry)

    def test_completed_turn_edits_visible_stream_message_to_final(self) -> None:
        state, pane, _key, entry = self._state()
        entry.update({
            "last_stream_hash": "old",
            "last_stream_turn_id": "turn-1",
            "last_stream_message_id": "3001",
            "last_stream_text": "Partial answer.",
            "last_stream_sent_at": herdres.utc_now(),
        })
        counters, caps = self._caps()
        pane_turn = Mock(
            return_value={
                "available": True,
                "complete": True,
                "turn_id": "turn-1",
                "user_text": "Still running?",
                "assistant_final_text": "Final answer.",
            }
        )
        edit_feed_item = Mock(return_value={"ok": True, "message_id": "3001"})
        send_feed_item = Mock(return_value={"ok": True, "message_id": "2001"})

        with patch.multiple(
            herdres,
            pane_turn=pane_turn,
            edit_feed_item=edit_feed_item,
            send_feed_item=send_feed_item,
            save_state=Mock(),
            apply_api_error_warning=Mock(return_value={"topic_missing": False, "changed": False}),
            TURN_FEED_ENABLED=True,
            LIVE_CARD_ENABLED=False,
            STATUS_MARKER_ENABLED=False,
            STATUS_ICON_ENABLED=False,
        ):
            changed = herdres.sync_pane_once(state, "-1001", state["telegram"], pane, counters, caps)

        self.assertTrue(changed)
        edit_feed_item.assert_called_once()
        self.assertEqual(edit_feed_item.call_args.args[1], "3001")
        edited_item = edit_feed_item.call_args.args[2]
        self.assertEqual(edited_item["worklog_text"], "Partial answer.")
        edited_html = herdres.render_turn_item_html(edited_item)
        self.assertIn("<details><summary><b>Worklog</b></summary><blockquote>", edited_html)
        response_prefix = "<details open><summary><b>Response</b></summary>"
        response_body = "<p>Final answer.</p>"
        self.assertIn(response_prefix + response_body, edited_html)
        response_start = edited_html.index(response_prefix)
        response_body_start = edited_html.index(response_body, response_start)
        self.assertNotIn("<blockquote>", edited_html[response_start:response_body_start])
        send_feed_item.assert_not_called()
        self.assertEqual(entry["last_clean_message_id"], "3001")
        self.assertNotIn("last_stream_message_id", entry)

    def test_streaming_config_disabled_keeps_status_marker_fallback(self) -> None:
        state, pane, _key, _entry = self._state()
        counters, caps = self._caps()
        pane_turn = Mock(
            return_value={
                "available": True,
                "complete": False,
                "turn_id": "turn-1",
                "user_text": "Still running?",
                "assistant_final_text": "",
                "assistant_stream_text": "Partial answer.",
            }
        )
        send_stream = Mock(return_value={"ok": True})
        marker = Mock(return_value={"ok": True, "attempted": True, "message_id": "3001"})
        send_feed_item = Mock(return_value={"ok": True, "message_id": "2001"})

        with patch.multiple(
            herdres,
            pane_turn=pane_turn,
            send_stream_update=send_stream,
            send_feed_item=send_feed_item,
            update_status_marker=marker,
            save_state=Mock(),
            apply_api_error_warning=Mock(return_value={"topic_missing": False, "changed": False}),
            TURN_FEED_ENABLED=True,
            STREAMING_DRAFTS_ENABLED=False,
            LIVE_CARD_ENABLED=False,
            STATUS_MARKER_ENABLED=True,
            STATUS_MARKER_SUPPRESS_WHEN_ICON_OK=False,
            STATUS_ICON_ENABLED=False,
        ):
            changed = herdres.sync_pane_once(state, "-1001", state["telegram"], pane, counters, caps)

        self.assertTrue(changed)
        send_stream.assert_not_called()
        send_feed_item.assert_called_once()
        marker.assert_called_once()

    def test_draft_unsupported_streams_visible_message_without_status_marker(self) -> None:
        state, pane, _key, _entry = self._state()
        state["telegram"]["streaming_drafts"] = {"supported": "no"}
        counters, caps = self._caps()
        pane_turn = Mock(
            return_value={
                "available": True,
                "complete": False,
                "turn_id": "turn-1",
                "user_text": "Still running?",
                "assistant_final_text": "",
                "assistant_stream_text": "Partial answer.",
            }
        )
        send_stream = Mock(return_value={"ok": True, "format": "message", "sent_message": True, "message_id": "3001"})
        marker = Mock(return_value={"ok": True, "attempted": True, "message_id": "4001"})
        send_feed_item = Mock(return_value={"ok": True, "message_id": "2001"})

        with patch.multiple(
            herdres,
            pane_turn=pane_turn,
            send_stream_update=send_stream,
            send_feed_item=send_feed_item,
            update_status_marker=marker,
            save_state=Mock(),
            apply_api_error_warning=Mock(return_value={"topic_missing": False, "changed": False}),
            TURN_FEED_ENABLED=True,
            STREAMING_DRAFTS_ENABLED=True,
            LIVE_CARD_ENABLED=False,
            STATUS_MARKER_ENABLED=True,
            STATUS_MARKER_SUPPRESS_WHEN_ICON_OK=False,
            STATUS_ICON_ENABLED=False,
        ):
            changed = herdres.sync_pane_once(state, "-1001", state["telegram"], pane, counters, caps)

        self.assertTrue(changed)
        send_feed_item.assert_called_once()
        send_stream.assert_called_once()
        self.assertEqual(send_stream.call_args.kwargs["turn_id"], "turn-1")
        marker.assert_not_called()
        self.assertEqual(counters["sends"], 2)

    def test_turn_only_event_path_streams_without_visible_scrape(self) -> None:
        state, pane, _key, _entry = self._state()
        counters, caps = self._caps()
        pane_turn = Mock(
            return_value={
                "available": True,
                "complete": False,
                "turn_id": "turn-1",
                "user_text": "Still running?",
                "assistant_final_text": "",
                "assistant_stream_text": "Partial answer.",
            }
        )
        pane_feed_output = Mock(return_value="visible scrape should not happen")
        send_stream = Mock(return_value={"ok": True, "draft_id": "123", "hash": "abc"})
        send_feed_item = Mock(return_value={"ok": True, "message_id": "2001"})

        with patch.multiple(
            herdres,
            pane_turn=pane_turn,
            pane_feed_output=pane_feed_output,
            send_stream_update=send_stream,
            send_feed_item=send_feed_item,
            save_state=Mock(),
            apply_api_error_warning=Mock(return_value={"topic_missing": False, "changed": False}),
            TURN_FEED_ENABLED=True,
            LIVE_CARD_ENABLED=False,
            STATUS_MARKER_ENABLED=False,
            STATUS_ICON_ENABLED=False,
        ):
            changed = herdres.sync_pane_once(state, "-1001", state["telegram"], pane, counters, caps, turn_only=True)

        self.assertTrue(changed)
        send_feed_item.assert_called_once()
        send_stream.assert_called_once()
        pane_feed_output.assert_not_called()

    def test_sync_does_not_resend_prompt_for_same_turn(self) -> None:
        state, pane, _key, entry = self._state()
        entry.update({
            "last_prompt_turn_id": "turn-1",
            "last_prompt_message_id": "2001",
        })
        counters, caps = self._caps()
        pane_turn = Mock(
            return_value={
                "available": True,
                "complete": False,
                "turn_id": "turn-1",
                "user_text": "Still running?",
                "assistant_final_text": "",
                "assistant_stream_text": "Partial answer.",
            }
        )
        send_stream = Mock(return_value={"ok": True, "draft_id": "123", "hash": "abc"})
        send_feed_item = Mock(return_value={"ok": True, "message_id": "2002"})

        with patch.multiple(
            herdres,
            pane_turn=pane_turn,
            send_stream_update=send_stream,
            send_feed_item=send_feed_item,
            save_state=Mock(),
            apply_api_error_warning=Mock(return_value={"topic_missing": False, "changed": False}),
            TURN_FEED_ENABLED=True,
            LIVE_CARD_ENABLED=False,
            STATUS_MARKER_ENABLED=False,
            STATUS_ICON_ENABLED=False,
        ):
            changed = herdres.sync_pane_once(state, "-1001", state["telegram"], pane, counters, caps)

        self.assertTrue(changed)
        send_feed_item.assert_not_called()
        send_stream.assert_called_once()
        self.assertNotIn("pending_prompt_turn_id", entry)

    def test_sync_resends_prompt_for_same_turn_when_prompt_hash_changed(self) -> None:
        state, pane, _key, entry = self._state()
        entry.update({
            "last_prompt_turn_id": "turn-1",
            "last_prompt_hash": "old-internal-prompt",
            "last_prompt_message_id": "2001",
        })
        counters, caps = self._caps()
        pane_turn = Mock(
            return_value={
                "available": True,
                "complete": False,
                "turn_id": "turn-1",
                "user_text": "Actual user prompt",
                "assistant_final_text": "",
                "assistant_stream_text": "Partial answer.",
            }
        )
        send_stream = Mock(return_value={"ok": True, "draft_id": "123", "hash": "abc"})
        send_feed_item = Mock(return_value={"ok": True, "message_id": "2002"})

        with patch.multiple(
            herdres,
            pane_turn=pane_turn,
            send_stream_update=send_stream,
            send_feed_item=send_feed_item,
            save_state=Mock(),
            apply_api_error_warning=Mock(return_value={"topic_missing": False, "changed": False}),
            TURN_FEED_ENABLED=True,
            LIVE_CARD_ENABLED=False,
            STATUS_MARKER_ENABLED=False,
            STATUS_ICON_ENABLED=False,
        ):
            changed = herdres.sync_pane_once(state, "-1001", state["telegram"], pane, counters, caps)

        self.assertTrue(changed)
        send_feed_item.assert_called_once()
        prompt_item = send_feed_item.call_args.args[1]
        self.assertEqual(prompt_item["kind"], "prompt")
        self.assertEqual(prompt_item["user_text"], "Actual user prompt")
        self.assertEqual(entry["last_prompt_turn_id"], "turn-1")
        self.assertEqual(entry["last_prompt_message_id"], "2002")
        self.assertEqual(entry["last_prompt_hash"], herdres.stream_text_hash("Actual user prompt"))
        send_stream.assert_called_once()

    def test_sync_clears_shared_space_mapping_when_stream_topic_is_missing(self) -> None:
        state, pane, _key, entry = self._state()
        entry["last_prompt_turn_id"] = "turn-1"
        counters, caps = self._caps()
        pane_turn = Mock(
            return_value={
                "available": True,
                "complete": False,
                "turn_id": "turn-1",
                "user_text": "Still running?",
                "assistant_final_text": "",
                "assistant_stream_text": "Partial answer.",
            }
        )
        send_stream = Mock(
            return_value={
                "ok": False,
                "kind": "topic_not_found",
                "topic_missing": True,
                "error": "forum topic not found",
            }
        )
        save_state = Mock()

        with patch.multiple(
            herdres,
            pane_turn=pane_turn,
            send_stream_update=send_stream,
            save_state=save_state,
            apply_api_error_warning=Mock(return_value={"topic_missing": False, "changed": False}),
            TURN_FEED_ENABLED=True,
            LIVE_CARD_ENABLED=False,
            STATUS_MARKER_ENABLED=False,
            STATUS_ICON_ENABLED=False,
        ):
            changed = herdres.sync_pane_once(state, "-1001", state["telegram"], pane, counters, caps)

        self.assertTrue(changed)
        self.assertNotIn("topic_id", state["spaces"]["workspace:workspace-1"])
        self.assertEqual(state["spaces"]["workspace:workspace-1"]["topic_missing_id"], "77")
        self.assertNotIn("topic_id", entry)
        save_state.assert_called_once_with(state)

    def test_sync_clears_pane_root_when_prompt_reply_target_is_missing(self) -> None:
        state, pane, _key, entry = self._state()
        counters, caps = self._caps()
        pane_turn = Mock(
            return_value={
                "available": True,
                "complete": False,
                "turn_id": "turn-1",
                "user_text": "Still running?",
                "assistant_final_text": "",
                "assistant_stream_text": "",
            }
        )
        send_feed_item = Mock(
            return_value={
                "ok": False,
                "kind": "not_found",
                "not_found": True,
                "error": "reply message not found",
            }
        )
        save_state = Mock()

        with patch.multiple(
            herdres,
            pane_turn=pane_turn,
            send_feed_item=send_feed_item,
            save_state=save_state,
            apply_api_error_warning=Mock(return_value={"topic_missing": False, "changed": False}),
            TURN_FEED_ENABLED=True,
            LIVE_CARD_ENABLED=False,
            STATUS_MARKER_ENABLED=False,
            STATUS_ICON_ENABLED=False,
        ):
            changed = herdres.sync_pane_once(state, "-1001", state["telegram"], pane, counters, caps)

        self.assertTrue(changed)
        self.assertEqual(state["spaces"]["workspace:workspace-1"]["topic_id"], "77")
        self.assertEqual(entry["topic_id"], "77")
        self.assertNotIn("pane_root_message_id", entry)
        self.assertEqual(entry["pane_root_message_missing_id"], "1001")
        save_state.assert_called_once_with(state)


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
        self.assertIn("<b>Changes made</b>", html)
        self.assertIn("<code>.local/bin/herdr_telegram_topics.py:520</code>", html)
        self.assertIn("<b>Verified with</b>", html)
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
        self.assertEqual(item["choice_source"], "explicit_block")
        self.assertEqual(item["options"][0]["label"], "Run sync now")
        markup, active_prompt, clear_prompt = herdres.prompt_delivery_state(item)
        self.assertIsNotNone(markup)
        assert active_prompt is not None
        self.assertFalse(clear_prompt)
        self.assertEqual(active_prompt["choice_source"], "explicit_block")

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
        self.assertIn("<b>Next</b>", html)
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

    def test_space_pinned_status_summary_orders_red_yellow_green(self) -> None:
        panes = [
            {"agent": "codex", "agent_status": "idle"},
            {"agent": "kimi", "agent_status": "blocked"},
            {"agent": "claude", "agent_status": "working"},
            {"agent": "omp", "agent_status": "closed"},
        ]

        self.assertEqual(herdres.render_pinned_status({}, panes), "Kimi 🔴 | Claude 🟡 | Codex 🟢")

    def test_sync_creates_and_pins_space_status_for_open_panes_only(self) -> None:
        pane_a = {
            "pane_id": "pane-1",
            "terminal_id": "term-1",
            "workspace_id": "workspace-1",
            "tab_id": "tab-1",
            "agent": "codex",
            "agent_status": "idle",
        }
        pane_b = {
            "pane_id": "pane-2",
            "terminal_id": "term-2",
            "workspace_id": "workspace-1",
            "tab_id": "tab-1",
            "agent": "kimi",
            "agent_status": "blocked",
        }
        pane_other = {
            "pane_id": "pane-3",
            "terminal_id": "term-3",
            "workspace_id": "workspace-2",
            "tab_id": "tab-2",
            "agent": "claude",
            "agent_status": "working",
        }
        state = {
            "version": 1,
            "enabled": True,
            "telegram": {"chat_id": "-1001", "general_thread_id": "1", "owner_user_ids": ["42"]},
            "spaces": {
                "workspace:workspace-1": {"space_key": "workspace:workspace-1", "topic_id": "77"},
                "workspace:workspace-2": {"space_key": "workspace:workspace-2", "topic_id": "88"},
            },
            "panes": {},
        }
        send_message = Mock(side_effect=["501", "502"])
        pin_chat_message = Mock(return_value={"ok": True})

        with patch.multiple(
            herdres,
            load_dotenv=Mock(),
            load_state=Mock(return_value=state),
            save_state=Mock(),
            pane_list=Mock(return_value=[pane_a, pane_b, pane_other]),
            preflight_is_fresh=Mock(return_value=True),
            sync_pane_once=Mock(return_value=False),
            ensure_managed_bot_setup_message=Mock(return_value=False),
            ensure_managed_bot_group_access_message=Mock(return_value=False),
            send_message=send_message,
            pin_chat_message=pin_chat_message,
            PINNED_STATUS_ENABLED=True,
        ):
            result = herdres.sync_once()

        self.assertTrue(result["changed"])
        self.assertEqual(result["pinned_status_updated"], 2)
        self.assertEqual(send_message.call_args_list[0].args[:2], ("-1001", "Kimi 🔴 | Codex 🟢"))
        self.assertEqual(send_message.call_args_list[0].kwargs["thread_id"], "77")
        self.assertEqual(send_message.call_args_list[1].args[:2], ("-1001", "Claude 🟡"))
        self.assertEqual(send_message.call_args_list[1].kwargs["thread_id"], "88")
        self.assertEqual(pin_chat_message.call_args_list[0].args, ("-1001", "501"))
        self.assertEqual(pin_chat_message.call_args_list[1].args, ("-1001", "502"))
        self.assertEqual(state["spaces"]["workspace:workspace-1"]["pinned_status_message_id"], "501")
        self.assertEqual(state["spaces"]["workspace:workspace-1"]["pinned_status_text"], "Kimi 🔴 | Codex 🟢")

    def test_sync_edits_existing_pinned_space_status_without_resending(self) -> None:
        pane = {
            "pane_id": "pane-1",
            "terminal_id": "term-1",
            "workspace_id": "workspace-1",
            "tab_id": "tab-1",
            "agent": "codex",
            "agent_status": "working",
        }
        state = {
            "version": 1,
            "enabled": True,
            "telegram": {"chat_id": "-1001", "general_thread_id": "1", "owner_user_ids": ["42"]},
            "spaces": {
                "workspace:workspace-1": {
                    "space_key": "workspace:workspace-1",
                    "topic_id": "77",
                    "pinned_status_message_id": "501",
                    "pinned_status_text": "Codex 🟢",
                    "pinned_status_hash": "old",
                    "pinned_status_pinned_at": herdres.utc_now(),
                }
            },
            "panes": {},
        }
        send_message = Mock()
        edit_message_text = Mock(return_value={"ok": True, "message_id": "501"})
        pin_chat_message = Mock(return_value={"ok": True})

        with patch.multiple(
            herdres,
            load_dotenv=Mock(),
            load_state=Mock(return_value=state),
            save_state=Mock(),
            pane_list=Mock(return_value=[pane]),
            preflight_is_fresh=Mock(return_value=True),
            sync_pane_once=Mock(return_value=False),
            ensure_managed_bot_setup_message=Mock(return_value=False),
            ensure_managed_bot_group_access_message=Mock(return_value=False),
            send_message=send_message,
            edit_message_text=edit_message_text,
            pin_chat_message=pin_chat_message,
            PINNED_STATUS_ENABLED=True,
        ):
            result = herdres.sync_once()

        self.assertTrue(result["changed"])
        self.assertEqual(result["pinned_status_updated"], 1)
        send_message.assert_not_called()
        edit_message_text.assert_called_once_with("-1001", "501", "Codex 🟡")
        pin_chat_message.assert_not_called()
        self.assertEqual(state["spaces"]["workspace:workspace-1"]["pinned_status_text"], "Codex 🟡")

    def test_sync_removes_closed_pane_from_space_status_membership(self) -> None:
        codex = {
            "pane_id": "pane-codex",
            "terminal_id": "term-codex",
            "workspace_id": "workspace-1",
            "tab_id": "tab-1",
            "agent": "codex",
            "agent_status": "idle",
        }
        codex_key = herdres.pane_key(codex)
        claude_key = "pane-claude:old"
        state = {
            "version": 1,
            "enabled": True,
            "telegram": {"chat_id": "-1001", "general_thread_id": "1", "owner_user_ids": ["42"]},
            "spaces": {
                "workspace:workspace-1": {
                    "space_key": "workspace:workspace-1",
                    "topic_id": "77",
                    "pane_keys": [codex_key, claude_key],
                    "pinned_status_message_id": "501",
                    "pinned_status_text": "Codex 🟢 | Claude 🟢",
                    "pinned_status_hash": "old",
                    "pinned_status_pinned_at": herdres.utc_now(),
                }
            },
            "panes": {
                codex_key: {"pane_key": codex_key, "pane_id": "pane-codex", "space_key": "workspace:workspace-1", "topic_id": "77"},
                claude_key: {
                    "pane_key": claude_key,
                    "pane_id": "pane-claude",
                    "space_key": "workspace:workspace-1",
                    "topic_id": "77",
                    "agent": "claude",
                    "last_known_status": "idle",
                },
            },
        }
        edit_message_text = Mock(return_value={"ok": True, "message_id": "501"})

        with patch.multiple(
            herdres,
            load_dotenv=Mock(),
            load_state=Mock(return_value=state),
            save_state=Mock(),
            pane_list=Mock(return_value=[codex]),
            preflight_is_fresh=Mock(return_value=True),
            sync_pane_once=Mock(return_value=False),
            ensure_managed_bot_setup_message=Mock(return_value=False),
            ensure_managed_bot_group_access_message=Mock(return_value=False),
            send_notice=Mock(return_value={"ok": True, "message_id": "900"}),
            edit_message_text=edit_message_text,
            PINNED_STATUS_ENABLED=True,
        ):
            result = herdres.sync_once()

        self.assertTrue(result["changed"])
        self.assertEqual(state["spaces"]["workspace:workspace-1"]["pane_keys"], [codex_key])
        edit_message_text.assert_called_once_with("-1001", "501", "Codex 🟢")
        self.assertEqual(state["spaces"]["workspace:workspace-1"]["pinned_status_text"], "Codex 🟢")

    def test_sync_updates_space_status_before_slow_pane_turn_sync(self) -> None:
        codex = {
            "pane_id": "pane-codex",
            "terminal_id": "term-codex",
            "workspace_id": "workspace-1",
            "tab_id": "tab-1",
            "agent": "codex",
            "agent_status": "idle",
        }
        codex_key = herdres.pane_key(codex)
        claude_key = "pane-claude:old"
        state = {
            "version": 1,
            "enabled": True,
            "telegram": {"chat_id": "-1001", "general_thread_id": "1", "owner_user_ids": ["42"]},
            "spaces": {
                "workspace:workspace-1": {
                    "space_key": "workspace:workspace-1",
                    "topic_id": "77",
                    "pane_keys": [codex_key, claude_key],
                    "pinned_status_message_id": "501",
                    "pinned_status_text": "Codex 🟢 | Claude 🟢",
                    "pinned_status_hash": "old",
                    "pinned_status_pinned_at": herdres.utc_now(),
                }
            },
            "panes": {
                codex_key: {"pane_key": codex_key, "pane_id": "pane-codex", "space_key": "workspace:workspace-1", "topic_id": "77"},
                claude_key: {
                    "pane_key": claude_key,
                    "pane_id": "pane-claude",
                    "space_key": "workspace:workspace-1",
                    "topic_id": "77",
                    "agent": "claude",
                    "last_known_status": "idle",
                },
            },
        }
        calls = []

        def edit_message_text(chat_id, message_id, text):
            calls.append(("pinned_status", text))
            return {"ok": True, "message_id": message_id}

        def sync_pane_once(*_args, **_kwargs):
            calls.append(("pane_sync", ""))
            return False

        with patch.multiple(
            herdres,
            load_dotenv=Mock(),
            load_state=Mock(return_value=state),
            save_state=Mock(),
            pane_list=Mock(return_value=[codex]),
            preflight_is_fresh=Mock(return_value=True),
            sync_pane_once=Mock(side_effect=sync_pane_once),
            ensure_managed_bot_setup_message=Mock(return_value=False),
            ensure_managed_bot_group_access_message=Mock(return_value=False),
            send_notice=Mock(return_value={"ok": True, "message_id": "900"}),
            edit_message_text=Mock(side_effect=edit_message_text),
            PINNED_STATUS_ENABLED=True,
        ):
            result = herdres.sync_once()

        self.assertTrue(result["changed"])
        self.assertEqual(result["pinned_status_updated"], 1)
        self.assertEqual(calls[0], ("pinned_status", "Codex 🟢"))
        self.assertEqual(calls[1], ("pane_sync", ""))

    def test_sync_edits_existing_space_status_to_no_active_panes_when_last_pane_deleted(self) -> None:
        claude_key = "pane-claude:old"
        state = {
            "version": 1,
            "enabled": True,
            "telegram": {"chat_id": "-1001", "general_thread_id": "1", "owner_user_ids": ["42"]},
            "spaces": {
                "workspace:workspace-1": {
                    "space_key": "workspace:workspace-1",
                    "topic_id": "77",
                    "pane_keys": [claude_key],
                    "pinned_status_message_id": "501",
                    "pinned_status_text": "Claude 🟢",
                    "pinned_status_hash": "old",
                    "pinned_status_pinned_at": herdres.utc_now(),
                }
            },
            "panes": {
                claude_key: {
                    "pane_key": claude_key,
                    "pane_id": "pane-claude",
                    "space_key": "workspace:workspace-1",
                    "topic_id": "77",
                    "agent": "claude",
                    "last_known_status": "idle",
                },
            },
        }
        edit_message_text = Mock(return_value={"ok": True, "message_id": "501"})

        with patch.multiple(
            herdres,
            load_dotenv=Mock(),
            load_state=Mock(return_value=state),
            save_state=Mock(),
            pane_list=Mock(return_value=[]),
            send_notice=Mock(return_value={"ok": True, "message_id": "900"}),
            edit_message_text=edit_message_text,
            PINNED_STATUS_ENABLED=True,
        ):
            result = herdres.sync_once()

        self.assertTrue(result["changed"])
        self.assertEqual(result["pinned_status_updated"], 1)
        self.assertEqual(state["spaces"]["workspace:workspace-1"]["pane_keys"], [])
        edit_message_text.assert_called_once_with("-1001", "501", "No active panes.")
        self.assertEqual(state["spaces"]["workspace:workspace-1"]["pinned_status_text"], "No active panes.")

    def test_sync_sends_one_compact_closed_notice_for_duplicate_closed_entries(self) -> None:
        state = {
            "version": 1,
            "enabled": True,
            "telegram": {"chat_id": "-1001", "general_thread_id": "1", "owner_user_ids": ["42"]},
            "spaces": {
                "workspace:workspace-1": {
                    "space_key": "workspace:workspace-1",
                    "topic_id": "77",
                    "pane_keys": ["pane-claude:a", "pane-claude:b"],
                }
            },
            "panes": {
                "pane-claude:a": {
                    "pane_key": "pane-claude:a",
                    "pane_id": "pane-claude",
                    "agent_session_id": "session-1",
                    "space_key": "workspace:workspace-1",
                    "topic_id": "77",
                    "last_known_status": "idle",
                },
                "pane-claude:b": {
                    "pane_key": "pane-claude:b",
                    "pane_id": "pane-claude",
                    "agent_session_id": "session-1",
                    "space_key": "workspace:workspace-1",
                    "topic_id": "77",
                    "last_known_status": "working",
                },
            },
        }
        send_notice = Mock(return_value={"ok": True, "message_id": "901"})

        with patch.multiple(
            herdres,
            load_dotenv=Mock(),
            load_state=Mock(return_value=state),
            save_state=Mock(),
            pane_list=Mock(return_value=[]),
            send_notice=send_notice,
            PINNED_STATUS_ENABLED=False,
        ):
            result = herdres.sync_once()

        self.assertTrue(result["changed"])
        self.assertEqual(result["sent"], 1)
        send_notice.assert_called_once()
        self.assertEqual(send_notice.call_args.args[1:3], ("Closed by User", ""))
        self.assertEqual(state["spaces"]["workspace:workspace-1"]["pane_keys"], [])

    def test_closed_notice_renders_title_only_without_empty_body(self) -> None:
        self.assertEqual(herdres.render_notice_html("Closed by User", ""), "<h3>Closed by User</h3>")

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
            PINNED_STATUS_ENABLED=False,
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
            "pane_root_message_id": "1001",
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
            PINNED_STATUS_ENABLED=False,
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
        skey = herdres.space_key(pane)
        entry = {
            "pane_key": key,
            "pane_id": "pane-1",
            "space_key": skey,
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
            "spaces": {skey: {"space_key": skey, "topic_id": "77", "topic_name": "Workspace 1", "pane_keys": [key]}},
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
            PINNED_STATUS_ENABLED=False,
        ):
            result = herdres.sync_once()

        self.assertTrue(result["changed"])
        self.assertEqual(result["icon_updated"], 1)
        self.assertEqual(result["marker_sent"], 0)
        space = state["spaces"][skey]
        self.assertEqual(space["topic_status_icon_custom_emoji_id"], "icon-working")
        self.assertEqual(space["topic_status_icon_key"], "working")
        for t in herdres._icon_fire_threads:
            t.join(timeout=2)
        icon_edits = [p for m, p in calls if m == "editForumTopic" and p.get("icon_custom_emoji_id")]
        self.assertTrue(icon_edits, "icon editForumTopic was not called")
        self.assertEqual(icon_edits[-1]["icon_custom_emoji_id"], "icon-working")
        self.assertEqual(icon_edits[-1]["name"], "Workspace 1")
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

    def test_send_to_pane_clears_existing_staged_input_before_run(self) -> None:
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
            pane_input_looks_staged=Mock(side_effect=[True, False, False, False, False]),
        ):
            ok, detail = herdres.send_to_pane("pane-1", "how are you")

        self.assertTrue(ok, detail)
        self.assertEqual(commands[0], [herdres.herdr_bin(), "pane", "send-keys", "pane-1", "ctrl+u"])
        self.assertEqual(commands[1][:4], [herdres.herdr_bin(), "pane", "run", "pane-1"])
        self.assertEqual(commands[1][4], "how are you")

    def test_send_to_pane_uses_fallback_clear_before_run(self) -> None:
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
            pane_input_looks_staged=Mock(side_effect=[True, True, True, True, True, False, False, False, False]),
        ):
            ok, detail = herdres.send_to_pane("pane-1", "@herdr_codex_bot sup")

        self.assertTrue(ok, detail)
        self.assertEqual(commands[0], [herdres.herdr_bin(), "pane", "send-keys", "pane-1", "ctrl+u"])
        self.assertEqual(commands[1], [herdres.herdr_bin(), "pane", "send-keys", "pane-1", "ctrl+a", "ctrl+k"])
        self.assertEqual(commands[2][:4], [herdres.herdr_bin(), "pane", "run", "pane-1"])
        self.assertEqual(commands[2][4], "@herdr_codex_bot sup")

    def test_send_to_pane_does_not_refuse_after_successful_forced_clear_attempts(self) -> None:
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
            pane_input_looks_staged=Mock(return_value=True),
        ), patch.object(herdres.time, "sleep", Mock()):
            ok, detail = herdres.send_to_pane("pane-1", "Fix it properly")

        self.assertTrue(ok, detail)
        self.assertNotIn("Could not clear existing staged pane input", detail)
        self.assertIn([herdres.herdr_bin(), "pane", "send-keys", "pane-1", "cmd+a", "backspace"], commands)
        self.assertEqual(commands[-2][:4], [herdres.herdr_bin(), "pane", "run", "pane-1"])
        self.assertEqual(commands[-2][4], "Fix it properly")

    def test_send_to_pane_falls_back_when_pane_run_reports_staged_input(self) -> None:
        pane = {"pane_id": "pane-1", "agent": "codex"}
        commands = []

        def run_cmd(args, **kwargs):
            commands.append(args)
            proc = Mock()
            if args[:4] == [herdres.herdr_bin(), "pane", "run", "pane-1"]:
                proc.returncode = 1
                proc.stdout = ""
                proc.stderr = "Could not clear existing staged pane input; refusing to append Telegram text."
                return proc
            proc.returncode = 0
            proc.stdout = ""
            proc.stderr = ""
            return proc

        with patch.multiple(
            herdres,
            pane_by_id=Mock(return_value=pane),
            run_cmd=run_cmd,
            pane_input_looks_staged=Mock(return_value=False),
        ), patch.object(herdres.time, "sleep", Mock()):
            ok, detail = herdres.send_to_pane("pane-1", "Fix it properly")

        self.assertTrue(ok, detail)
        self.assertNotIn("Could not clear existing staged pane input", detail)
        self.assertIn([herdres.herdr_bin(), "pane", "send-keys", "pane-1", "cmd+a", "backspace"], commands)
        self.assertIn([herdres.herdr_bin(), "pane", "send-text", "pane-1", "Fix it properly"], commands)
        self.assertEqual(commands[-1], [herdres.herdr_bin(), "pane", "send-keys", "pane-1", "enter"])

    def test_send_to_pane_ignores_codex_goal_usage_footer(self) -> None:
        pane = {"pane_id": "pane-1", "agent": "codex"}
        current_composer = """• Service tier set to default


› Explain this codebase

  gpt-5.5 xhigh · ~/Projects/herdres  Goal hit usage limits (/goal resume)"""
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
            pane_input_ansi=Mock(return_value=current_composer),
            pane_output=Mock(return_value=current_composer),
            run_cmd=run_cmd,
        ):
            ok, detail = herdres.send_to_pane("pane-1", "okay create PoCs")

        self.assertTrue(ok, detail)
        self.assertEqual(commands[0][:4], [herdres.herdr_bin(), "pane", "run", "pane-1"])
        self.assertEqual(commands[0][4], "okay create PoCs")

    def test_send_to_pane_ignores_truncated_codex_goal_usage_footer(self) -> None:
        pane = {"pane_id": "pane-1", "agent": "codex"}
        current_composer = """─ Worked for 2m 06s ──────


› Explain this codebase

  Goal hit usage limits (/"""
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
            pane_input_ansi=Mock(return_value=current_composer),
            pane_output=Mock(return_value=current_composer),
            run_cmd=run_cmd,
        ):
            ok, detail = herdres.send_to_pane("pane-1", "Complete the next 20 slices")

        self.assertTrue(ok, detail)
        self.assertEqual(commands[0][:4], [herdres.herdr_bin(), "pane", "run", "pane-1"])
        self.assertEqual(commands[0][4], "Complete the next 20 slices")

    def test_pane_input_stage_detection_ignores_old_visible_prompt_history(self) -> None:
        visible_history = """Previous answer.

❯ old text that was already submitted
  wrapped old text

⏺ Answer to the old text
...[truncated by herdr-topic bridge]"""
        current_composer = """⏺ Answer to the old text

─────────────────────────────────────────────────────────────
❯
─────────────────────────────────────────────────────────────
  ⏵⏵ bypass permissions on"""

        def pane_output(_pane_id: str, **kwargs: object) -> str:
            if kwargs.get("source") == "recent-unwrapped":
                return current_composer
            return visible_history

        with patch.object(herdres, "pane_output", side_effect=pane_output):
            self.assertFalse(herdres.pane_input_looks_staged("pane-1"))

    def test_pane_input_stage_detection_ignores_codex_placeholder_prompt(self) -> None:
        current_composer = """• Working (30s • esc to interrupt)


› Write tests for @filename

  gpt-5.5 xhigh fast · ~/Projects/herdres                       Goal achieved (17m)"""

        with patch.object(herdres, "pane_output", return_value=current_composer):
            self.assertFalse(herdres.pane_input_looks_staged("pane-1"))

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
            # detect: not-staged then staged; after Enter the box clears.
            pane_input_looks_staged=Mock(side_effect=[False, True, False]),
        ):
            ok, detail = herdres.send_to_pane("pane-1", "long pasted instruction", submit_staged=True)

        self.assertTrue(ok, detail)
        self.assertEqual(commands[0][:4], [herdres.herdr_bin(), "pane", "run", "pane-1"])
        self.assertEqual(commands[1], [herdres.herdr_bin(), "pane", "send-keys", "pane-1", "enter"])

    def test_send_to_pane_submits_staged_input_by_default(self) -> None:
        # Regression: an inbound Telegram message reached the pane input box but
        # was never submitted because the /send path used the old default
        # submit_staged=False. send_to_pane must press Enter on staged input even
        # without an explicit submit_staged kwarg.
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
            # detect: not-staged then staged; after Enter the box clears.
            pane_input_looks_staged=Mock(side_effect=[False, True, False]),
        ):
            ok, detail = herdres.send_to_pane("pane-1", "Everything pushed on origin right?")

        self.assertTrue(ok, detail)
        self.assertEqual(commands[0][:4], [herdres.herdr_bin(), "pane", "run", "pane-1"])
        self.assertEqual(commands[-1], [herdres.herdr_bin(), "pane", "send-keys", "pane-1", "enter"])

    def test_submit_staged_input_trusts_delivered_submit_when_idle(self) -> None:
        # IDLE agent with a still-staged box: Herdres does not refuse after a
        # delivered Enter — it trusts the submit (returns success, no queued note).
        with patch.object(herdres.time, "sleep", lambda *_: None), patch.multiple(
            herdres,
            run_cmd=Mock(return_value=Mock(returncode=0, stdout="", stderr="")),
            pane_input_looks_staged=Mock(return_value=True),  # box never clears
        ):
            ok, detail = herdres.submit_staged_pane_input_if_needed("pane-1", timeout=1, agent_status="idle")

        self.assertTrue(ok)
        self.assertEqual(detail, "")

    def test_submit_staged_input_queues_when_agent_working(self) -> None:
        # WORKING agent: the box stays staged because a busy agent queues typed
        # input until its turn ends — that is "queued", not a failure.
        with patch.object(herdres.time, "sleep", lambda *_: None), patch.multiple(
            herdres,
            run_cmd=Mock(return_value=Mock(returncode=0, stdout="", stderr="")),
            pane_input_looks_staged=Mock(return_value=True),  # box never clears (queued)
        ):
            ok, detail = herdres.submit_staged_pane_input_if_needed("pane-1", timeout=1, agent_status="working")

        self.assertTrue(ok)
        self.assertIn("Queued", detail)

    def test_interrupt_and_send_sends_escape_before_message_when_working(self) -> None:
        # /send! to a WORKING pane must halt the turn (Esc) BEFORE delivering, so
        # the message runs now instead of queueing behind the current turn.
        calls = []

        def run_cmd(args, **kwargs):
            calls.append(args)
            return Mock(returncode=0, stdout="", stderr="")

        working_pane = {"pane_id": "pane-1", "agent": "claude", "agent_status": "working"}
        with patch.object(herdres.time, "sleep", lambda *_: None), patch.multiple(
            herdres,
            run_cmd=run_cmd,
            pane_by_id=Mock(return_value=working_pane),
            clear_staged_pane_input_if_needed=Mock(return_value=(True, "")),
            pane_input_looks_staged=Mock(return_value=False),
        ):
            ok, detail = herdres.interrupt_and_send_to_pane("pane-1", "go now")

        self.assertTrue(ok)
        esc_idx = next(i for i, a in enumerate(calls) if "send-keys" in a and "escape" in a)
        run_idx = next(i for i, a in enumerate(calls) if "run" in a)
        self.assertLess(esc_idx, run_idx)  # Esc precedes the message

    def test_interrupt_and_send_skips_escape_when_idle(self) -> None:
        # No turn to interrupt on an idle pane: deliver without sending Esc
        # (Esc on idle Codex pops its recall preview — a needless side effect).
        calls = []

        def run_cmd(args, **kwargs):
            calls.append(args)
            return Mock(returncode=0, stdout="", stderr="")

        idle_pane = {"pane_id": "pane-1", "agent": "codex", "agent_status": "idle"}
        with patch.object(herdres.time, "sleep", lambda *_: None), patch.multiple(
            herdres,
            run_cmd=run_cmd,
            pane_by_id=Mock(return_value=idle_pane),
            clear_staged_pane_input_if_needed=Mock(return_value=(True, "")),
            pane_input_looks_staged=Mock(return_value=False),
        ):
            ok, detail = herdres.interrupt_and_send_to_pane("pane-1", "go now")

        self.assertTrue(ok)
        self.assertFalse(any("escape" in a for a in calls))  # no interrupt on idle
        self.assertTrue(any("run" in a for a in calls))  # still delivered

    def test_pane_input_dim_codex_placeholder_is_not_staged(self) -> None:
        # Codex shows a greyed (dim = \x1b[2m) example suggestion in an EMPTY box;
        # that must NOT be treated as staged input.
        dim_placeholder = "› \x1b[2mExplain this codebase\x1b[0m"
        with patch.object(herdres, "pane_input_ansi", Mock(return_value=dim_placeholder)):
            self.assertFalse(herdres.pane_input_looks_staged("pane-1"))

    def test_pane_input_real_typed_text_is_staged(self) -> None:
        # Real typed/queued input is not dim -> still detected as staged.
        real_text = "❯ deploy when ready"
        with patch.object(herdres, "pane_input_ansi", Mock(return_value=real_text)):
            self.assertTrue(herdres.pane_input_looks_staged("pane-1"))

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
        self.assertEqual(send_to_pane.call_count, 1)
        args, kwargs = send_to_pane.call_args
        self.assertEqual(args, ("pane-1", "custom text"))
        self.assertEqual(kwargs["timeout"], 8)
        self.assertTrue(kwargs["submit_staged"])
        self.assertIsInstance(kwargs["deadline"], float)

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
        entry = {"pane_key": key, "pane_id": "pane-1", "topic_id": "77", "pane_root_message_id": "1001"}
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

    def test_exhausted_transient_send_retries_next_sync_without_attempt_throttle(self) -> None:
        pane = {
            "pane_id": "pane-1",
            "terminal_id": "term-1",
            "workspace_id": "workspace-1",
            "tab_id": "tab-1",
            "agent": "claude",
            "agent_status": "done",
        }
        key = herdres.pane_key(pane)
        entry = {"pane_key": key, "pane_id": "pane-1", "topic_id": "77", "pane_root_message_id": "1001"}
        state = {
            "version": 1,
            "enabled": True,
            "telegram": {"chat_id": "-1001", "general_thread_id": "1", "owner_user_ids": ["42"]},
            "panes": {key: entry},
        }
        send_feed_item = Mock(return_value={"ok": False, "kind": "transient", "transient": True, "error": "timeout"})
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
            "STATUS_MARKER_ENABLED": False,
            "PINNED_STATUS_ENABLED": False,
        }

        with patch.object(herdres.time, "sleep", Mock()), patch.multiple(herdres, **common_patches):
            first = herdres.sync_once()
            first_attempts = send_feed_item.call_count
            second = herdres.sync_once()

        self.assertTrue(first["changed"])
        self.assertTrue(second["changed"])
        self.assertGreaterEqual(first_attempts, 2)
        self.assertGreaterEqual(send_feed_item.call_count - first_attempts, 2)
        self.assertNotIn("last_clean_hash", entry)
        self.assertNotIn("last_clean_message_id", entry)
        self.assertNotIn("last_clean_attempt_hash", entry)
        self.assertIn("timeout", entry.get("last_clean_send_error", ""))

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
            "pane_root_message_id": "1001",
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
            STATUS_MARKER_ENABLED=False,
            PINNED_STATUS_ENABLED=False,
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
            PINNED_STATUS_ENABLED=False,
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

    def test_classifies_well_known_permanent_telegram_errors(self) -> None:
        cases = [
            "Telegram sendMessage failed: Unauthorized",
            "Telegram sendMessage failed: Forbidden: bot was blocked by the user",
            "Telegram sendMessage failed: Bad Request: chat not found",
            "Telegram sendMessage failed: Forbidden: bot is not a member of the supergroup chat",
        ]
        for message in cases:
            with self.subTest(message=message):
                self.assertEqual(herdres.classify_telegram_error(herdres.BridgeError(message)), "permanent")
        self.assertEqual(
            herdres.classify_telegram_error(herdres.BridgeError("Telegram sendMessage failed: timed out")),
            "transient",
        )

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
        self.assertEqual(result["renamed"], 1)
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
        send_root = Mock(return_value={"ok": True, "format": "rich", "message_id": "1001"})

        with patch.multiple(
            herdres,
            load_dotenv=Mock(),
            load_state=Mock(return_value=state),
            save_state=Mock(),
            pane_list=Mock(return_value=[pane]),
            preflight_is_fresh=Mock(return_value=True),
            create_topic=create_topic,
            send_rich_message=send_root,
            pane_turn=Mock(return_value={"available": False, "reason": "no_structured_turn_source"}),
            TURN_FEED_ENABLED=True,
            LIVE_CARD_ENABLED=False,
            STATUS_MARKER_ENABLED=False,
        ):
            result = herdres.sync_once()

        self.assertTrue(result["changed"])
        self.assertEqual(result["created"], 1)
        create_topic.assert_called_once_with("-1001", "Workspace 1")
        self.assertEqual(entry["topic_id"], "88")
        self.assertNotIn("pane_root_message_id", entry)
        self.assertEqual(state["spaces"]["workspace:workspace-1"]["topic_id"], "88")
        send_root.assert_not_called()
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
        self.assertEqual(entry["topic_name"], "Workspace 1")
        self.assertEqual(entry["topic_title_source"], "space")
        self.assertEqual(entry["legacy_topic_name"], "Italy Ping")
        self.assertEqual(entry["pane_thread_name"], "entmoot italy ping")
        self.assertEqual(entry["pane_label_raw"], "entmoot italy ping")
        self.assertEqual(entry["pane_label_topic_name"], "Entmoot Italy")
        self.assertNotIn("topic_rename_pending_at", entry)

    def test_pane_label_preserves_two_word_topic_name(self) -> None:
        self.assertEqual(herdres.topic_name_from_pane_label("Topics Pane"), "Topics Pane")

    def test_pane_label_preserves_glm_acronym(self) -> None:
        self.assertEqual(herdres.topic_name_from_pane_label("GLM Devin"), "GLM Devin")

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

        self.assertEqual(entry["topic_name"], "Workspace 1")
        self.assertEqual(entry["topic_title_source"], "space")
        self.assertEqual(entry["legacy_topic_name"], "Topic Names")
        self.assertEqual(entry["pane_label_raw"], "Topics Pane")
        self.assertEqual(entry["pane_label_topic_name"], "Topics Pane")
        self.assertEqual(entry["pane_thread_name"], "Topics Pane")
        self.assertNotIn("topic_rename_pending_at", entry)

    def test_pane_label_change_updates_thread_name_without_topic_rename(self) -> None:
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

        self.assertEqual(entry["topic_name"], "Workspace 1")
        self.assertEqual(entry["topic_title_source"], "space")
        self.assertEqual(entry["pane_thread_name"], "flight recorder")
        self.assertEqual(entry["legacy_topic_name"], "Old Topic")
        self.assertNotIn("topic_rename_pending_at", entry)

    def test_new_labeled_pane_uses_label_as_thread_not_topic(self) -> None:
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
        self.assertEqual(entry["topic_name"], "Workspace 1")
        self.assertEqual(entry["topic_title_source"], "space")
        self.assertEqual(entry["pane_label_topic_name"], "Docker Cache")
        self.assertEqual(entry["pane_thread_name"], "docker cache")

    def test_sync_pane_label_change_does_not_rename_space_topic(self) -> None:
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
            "spaces": {
                "workspace:workspace-1": {
                    "space_key": "workspace:workspace-1",
                    "space_id": "workspace-1",
                    "topic_id": "77",
                    "topic_name": "Workspace 1",
                    "pane_keys": [key],
                    "last_topic_verified_at": herdres.utc_now(),
                }
            },
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
        self.assertEqual(result["renamed"], 0)
        edit_topic.assert_not_called()
        self.assertEqual(entry["topic_name"], "Workspace 1")
        self.assertEqual(entry["pane_label_raw"], "flight recorder")
        self.assertEqual(entry["pane_thread_name"], "flight recorder")
        self.assertEqual(entry["legacy_topic_name"], "Old Topic")
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

    def test_prompt_delivery_blocks_visible_scrape_choices_by_default(self) -> None:
        item = {
            "kind": "choices",
            "title": "Decision needed",
            "summary": "Choose one.",
            "text": "Choose one.\n1) A\n2) B",
            "options": [{"number": "1", "label": "A"}, {"number": "2", "label": "B"}],
            "prompt_id": "visible1",
            "turn_id": "visible-choice:visible1",
            "choice_source": "visible_scrape",
        }

        with patch.object(herdres, "VISIBLE_CHOICE_BUTTONS_ENABLED", False):
            markup, active_prompt, clear_prompt = herdres.prompt_delivery_state(item)

        self.assertIsNone(markup)
        self.assertIsNone(active_prompt)
        self.assertTrue(clear_prompt)

    def test_prompt_delivery_blocks_visible_readonly_choices(self) -> None:
        item = {
            "kind": "choices",
            "title": "Input needed",
            "summary": "Choose one.",
            "text": "Choose one.\n1) A\n2) B",
            "options": [{"number": "1", "label": "A"}, {"number": "2", "label": "B"}],
            "prompt_id": "visible1",
            "turn_id": "visible-readonly:visible1",
            "choice_source": "visible_readonly",
        }

        with patch.object(herdres, "VISIBLE_CHOICE_BUTTONS_ENABLED", True):
            markup, active_prompt, clear_prompt = herdres.prompt_delivery_state(item)

        self.assertIsNone(markup)
        self.assertIsNone(active_prompt)
        self.assertTrue(clear_prompt)

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
        self.assertEqual(item["choice_source"], "visible_scrape")
        self.assertEqual(item["turn_id"], f"visible-choice:{item['prompt_id']}")
        self.assertEqual(len(item["options"]), 4)
        self.assertIn("value-ranker", item["detail"])
        self.assertIn("Codex thinks", item["summary"])
        self.assertIn("short explainer", item["options"][0]["description"])
        html = herdres.render_feed_item_html(item)
        self.assertIn("value-ranker", html)
        self.assertIn("<b>Question</b>", html)
        self.assertIn("Codex thinks", html)

    def test_visible_readonly_choice_fallback_keeps_prompt_without_buttons(self) -> None:
        pane = {"pane_id": "pane-1", "agent": "claude", "agent_status": "idle"}
        raw = """Codex thinks your real objection is register, not raw word count. Which is it?

❯ 1. Mostly register/tone
     A short explainer is still bad.
  2. Mostly length
     You want shorter by default.
  3. Both, equally
     Length and register both matter.
  4. Type something.
"""

        with patch.object(herdres, "pane_output", Mock(return_value=raw)):
            item = herdres.extract_visible_readonly_feed_item(pane)

        self.assertIsNotNone(item)
        assert item is not None
        self.assertEqual(item["kind"], "choices")
        self.assertEqual(item["title"], "Input needed")
        self.assertEqual(item["choice_source"], "visible_readonly")
        self.assertNotIn("decision_id", item)
        self.assertTrue(str(item["turn_id"]).startswith("visible-readonly:"))
        self.assertIn("Which is it?", item["summary"])
        self.assertIn("Visible-screen prompt only", item["detail"])
        html = herdres.render_feed_item_html(item)
        self.assertIn("Codex thinks", html)
        self.assertIn("Mostly register/tone", html)
        self.assertIn("Visible-screen prompt only", html)

        markup, active_prompt, clear_prompt = herdres.prompt_delivery_state(item)
        self.assertIsNone(markup)
        self.assertIsNone(active_prompt)
        self.assertTrue(clear_prompt)

    def test_visible_readonly_choice_fallback_accepts_which_should_prompt(self) -> None:
        pane = {"pane_id": "pane-1", "agent": "claude", "agent_status": "blocked"}
        raw = """☐ Runner default

Now that the trade-off is clear (pool = faster per draft + clean
context but more idle RAM; claude -p = slower per draft but ~zero
idle RAM, simpler), which runner topology should the toggle
default to? All keep Codex as the real cross-provider backup.

❯ 1. Pool primary + Codex backup (Recommended)
     Keep claude-pool as primary.
  2. claude -p primary + Codex backup
     Flip the toggle default to print.
  3. claude -p → pool → Codex chain
     Try claude -p first, fall back to claude-pool, then Codex.
  4. Type something.
──────────────────────────────────────────────────────────────────
  5. Chat about this

Enter to select · ↑/↓ to navigate · Esc to cancel
"""

        with patch.object(herdres, "pane_output", Mock(return_value=raw)):
            item = herdres.extract_visible_readonly_feed_item(pane)

        self.assertIsNotNone(item)
        assert item is not None
        self.assertEqual(item["kind"], "choices")
        self.assertEqual(item["choice_source"], "visible_readonly")
        self.assertEqual(len(item["options"]), 5)
        self.assertIn("which runner topology should the toggle", item["summary"])

    def test_visible_readonly_choice_fallback_accepts_which_do_you_want_prompt(self) -> None:
        pane = {"pane_id": "pane-1", "agent": "claude", "agent_status": "blocked"}
        raw = """What I'd actually recommend

The clean toggle we just got Codex-approved supports either as
the default with a one-line flip — no architecture change. So
the real question is just which default, and I'd not build a
claude-p→pool fallback chain. Three options:
──────────────────────────────────────────────────────────────────
 ☐ Runner default

Given claude-pool is faster (warm) and claude -p is slower
(cold-start), and a pool-as-backup-for-claude-p adds little (same
account/CLI), which runner topology do you want? (All three keep
Codex as the real cross-provider backup.)

❯ 1. Pool primary + Codex backup (fastest) — Recommended
     Keep claude-pool as primary (the performant choice, ~2x
     faster per call), Codex as the fallback on Claude failures.
     This is the current approved plan (default OFF=pool). claude
     -p stays a clean toggle you can flip anytime.
  2. claude -p primary + Codex backup
     Flip the toggle's default to print (claude -p). Slower per
     call (~4s cold) but simpler/more predictable than the
     pool/TUI. NOT a performance win — choose this only if you
     value claude -p's simplicity/reliability. Codex still backs
     up Claude failures.
  3. claude -p primary → pool → Codex chain
     The literal ask: try claude -p, then claude-pool, then Codex.
     I advise against it: pool can't cover claude -p's quota/auth
     failures (same account), so the extra tier adds complexity
     for little resilience, and primary claude -p is slower.
  4. Type something.
──────────────────────────────────────────────────────────────────
  5. Chat about this

Enter to select · ↑/↓ to navigate · Esc to cancel
"""

        with patch.object(herdres, "pane_output", Mock(return_value=raw)):
            item = herdres.extract_visible_readonly_feed_item(pane)

        self.assertIsNotNone(item)
        assert item is not None
        self.assertEqual(item["kind"], "choices")
        self.assertEqual(item["title"], "Input needed")
        self.assertEqual(item["choice_source"], "visible_readonly")
        self.assertNotIn("decision_id", item)
        self.assertEqual(len(item["options"]), 5)
        self.assertIn("which runner topology do you want", item["summary"])
        self.assertIn("Three options", item["detail"])
        self.assertIn("performant choice", item["options"][0]["description"])
        self.assertIn("Visible-screen prompt only", item["detail"])

        markup, active_prompt, clear_prompt = herdres.prompt_delivery_state(item)
        self.assertIsNone(markup)
        self.assertIsNone(active_prompt)
        self.assertTrue(clear_prompt)

    def test_visible_readonly_choice_fallback_accepts_wrapped_which_do_you_want_prompt(self) -> None:
        pane = {"pane_id": "pane-1", "agent": "claude", "agent_status": "blocked"}
        raw = """Given claude-pool is faster (warm) and claude -p is slower
(cold-start), and a pool-as-backup-for-claude-p adds little,
which runner topology do you
want? (All three keep Codex as the real cross-provider backup.)

❯ 1. Pool primary + Codex backup (fastest) — Recommended
     Keep claude-pool as primary.
  2. claude -p primary + Codex backup
     Flip the toggle's default to print.
"""

        with patch.object(herdres, "pane_output", Mock(return_value=raw)):
            item = herdres.extract_visible_readonly_feed_item(pane)

        self.assertIsNotNone(item)
        assert item is not None
        self.assertEqual(item["kind"], "choices")
        self.assertEqual(item["choice_source"], "visible_readonly")
        self.assertEqual(len(item["options"]), 2)
        self.assertIn("which runner topology do you\nwant?", item["summary"])

    def test_wrapped_which_do_you_want_hint_stops_at_sentence_boundary(self) -> None:
        raw = """Blocked
Which file changed. Do you want the full diff? Files touched:
1. loader.py
2. parser.py
3. utils.py
"""

        item = herdres.extract_clean_feed_item({"agent_status": "blocked"}, {}, raw)

        self.assertIsNotNone(item)
        assert item is not None
        self.assertEqual(item["kind"], "blocked")

    def test_wrapped_which_should_hint_stops_at_sentence_boundary(self) -> None:
        raw = """Blocked
Which file changed. Should the report include the full diff? Files touched:
1. loader.py
2. parser.py
3. utils.py
"""

        item = herdres.extract_clean_feed_item({"agent_status": "blocked"}, {}, raw)

        self.assertIsNotNone(item)
        assert item is not None
        self.assertEqual(item["kind"], "blocked")

    def test_make_turn_feed_item_ignores_completed_turn_when_open_turn_exists_without_decision(self) -> None:
        item = herdres.make_turn_feed_item(
            {
                "available": True,
                "complete": True,
                "has_open_turn": True,
                "turn_id": "old-turn",
                "assistant_final_text": "Old completed final answer.",
            }
        )

        self.assertIsNone(item)

    def test_make_turn_feed_item_delivers_completed_turn_when_no_open_turn_exists(self) -> None:
        item = herdres.make_turn_feed_item(
            {
                "available": True,
                "complete": True,
                "turn_id": "current-turn",
                "assistant_final_text": "Current completed final answer.",
            }
        )

        self.assertIsNotNone(item)
        assert item is not None
        self.assertEqual(item["kind"], "turn")
        self.assertEqual(item["turn_id"], "current-turn")
        self.assertIn("Current completed final answer", item["assistant_final_text"])

    def test_make_turn_feed_item_allows_structured_decision_when_open_turn_exists(self) -> None:
        item = herdres.make_turn_feed_item(
            {
                "available": True,
                "complete": True,
                "has_open_turn": True,
                "awaiting_input": True,
                "turn_id": "turn-with-decision",
                "assistant_final_text": "Context for the decision.",
                "pending_decision": {
                    "decision_id": "decision-1",
                    "prompt": "How should I proceed?",
                    "options": [
                        {"id": "1", "label": "Continue", "send_text": "1"},
                        {"id": "2", "label": "Stop", "send_text": "2"},
                    ],
                },
            }
        )

        self.assertIsNotNone(item)
        assert item is not None
        self.assertEqual(item["kind"], "decision")
        self.assertEqual(item["decision_id"], "decision-1")

    def test_extract_turn_feed_item_delivers_completed_turn_after_auto_continue(self) -> None:
        # The agent finished a reply and immediately auto-pursued a new goal, so a
        # new turn is already open (has_open_turn) with no question awaiting the
        # user. make_turn_feed_item drops it and the visible-question fallbacks find
        # nothing, but the completed final message must still be delivered.
        pane = {"pane_id": "pane-1", "agent": "codex", "agent_status": "working"}
        turn = {
            "available": True,
            "complete": True,
            "has_open_turn": True,
            "turn_id": "finished-turn",
            "assistant_final_text": "Here is the final answer before auto-continuing.",
        }
        with (
            patch.object(herdres, "pane_turn", Mock(return_value=turn)),
            patch.object(herdres, "extract_visible_choice_feed_item", Mock(return_value=None)),
            patch.object(herdres, "extract_visible_readonly_feed_item", Mock(return_value=None)),
        ):
            item = herdres.extract_turn_feed_item(pane, {})

        self.assertIsNotNone(item)
        assert item is not None
        self.assertEqual(item["kind"], "turn")
        self.assertEqual(item["turn_id"], "finished-turn")
        self.assertIn("final answer before auto-continuing", item["assistant_final_text"])

    def test_select_turn_feed_item_catches_up_oldest_undelivered(self) -> None:
        # A burst of completions must be delivered in order (oldest undelivered
        # first), not collapsed to only the newest — the live X Issues case where
        # "PR #207 merged" (human-prompted) preceded an auto-pursued "Deployed".
        def mk(tid: str, user: str, asst: str) -> dict:
            return {"available": True, "complete": True, "turn_id": tid,
                    "user_text": user, "assistant_final_text": asst}

        a = mk("A", "p1", "answer A")
        b = mk("B", "you did it again", "PR #207 merged")
        c = mk("C", "", "Deployed and verified.")
        turn = {**c, "recent_turns": [a, b, c]}

        # delivered A -> next undelivered is B (with its real prompt)
        item = herdres.select_turn_feed_item(turn, {"last_clean_item": {"turn_id": "A"}})
        assert item is not None
        self.assertEqual(item["turn_id"], "B")
        self.assertIn("you did it again", item["text"])
        # delivered B -> next is C
        item = herdres.select_turn_feed_item(turn, {"last_clean_item": {"turn_id": "B"}})
        assert item is not None
        self.assertEqual(item["turn_id"], "C")
        # delivered C (already the latest) -> latest, dedup handles the no-op
        item = herdres.select_turn_feed_item(turn, {"last_clean_item": {"turn_id": "C"}})
        assert item is not None
        self.assertEqual(item["turn_id"], "C")
        # unknown cursor (window overflow / new pane) -> latest only, no history dump
        item = herdres.select_turn_feed_item(turn, {"last_turn_id": "ZZZ"})
        assert item is not None
        self.assertEqual(item["turn_id"], "C")
        # no recent_turns -> latest
        item = herdres.select_turn_feed_item(c, {})
        assert item is not None
        self.assertEqual(item["turn_id"], "C")

    def test_select_turn_feed_item_finalizes_streamed_turn_before_catchup(self) -> None:
        def mk(tid: str, user: str, asst: str) -> dict:
            return {
                "available": True,
                "complete": True,
                "turn_id": tid,
                "user_text": user,
                "assistant_final_text": asst,
            }

        streamed = mk("turn-1", "original prompt", "final streamed answer")
        auto_continued = mk("turn-2", "", "follow-up answer")
        turn = {**auto_continued, "recent_turns": [streamed, auto_continued]}

        item = herdres.select_turn_feed_item(
            turn,
            {"last_turn_id": "turn-1", "last_stream_turn_id": "turn-1"},
        )

        assert item is not None
        self.assertEqual(item["turn_id"], "turn-1")
        self.assertIn("original prompt", item["text"])

    def test_open_completed_turn_falls_back_to_visible_readonly_prompt(self) -> None:
        pane = {"pane_id": "pane-1", "agent": "claude", "agent_status": "blocked"}
        turn = {
            "available": True,
            "complete": True,
            "has_open_turn": True,
            "open_turn_id": "open-user-turn",
            "turn_id": "old-complete-turn",
            "assistant_final_text": "Old completed final answer.",
        }
        raw = """☐ Runner default

Now that the trade-off is clear, which runner topology should the
toggle default to?

❯ 1. Pool primary + Codex backup (Recommended)
     Keep claude-pool as primary.
  2. claude -p primary + Codex backup
     Flip the toggle default to print.
"""

        with (
            patch.object(herdres, "pane_turn", Mock(return_value=turn)),
            patch.object(herdres, "pane_output", Mock(return_value=raw)),
        ):
            item = herdres.extract_turn_feed_item(pane, {})

        self.assertIsNotNone(item)
        assert item is not None
        self.assertEqual(item["kind"], "choices")
        self.assertEqual(item["choice_source"], "visible_readonly")
        self.assertEqual(item["turn_id"][:17], "visible-readonly:")
        markup, active_prompt, clear_prompt = herdres.prompt_delivery_state(item)
        self.assertIsNone(markup)
        self.assertIsNone(active_prompt)
        self.assertTrue(clear_prompt)

    def test_visible_readonly_free_text_question_surfaces_without_buttons(self) -> None:
        pane = {"pane_id": "pane-1", "agent": "claude", "agent_status": "idle"}
        raw = """I found two possible fixes.

Should I patch the timeout guard now?
"""

        with patch.object(herdres, "pane_output", Mock(return_value=raw)):
            item = herdres.extract_visible_readonly_feed_item(pane)

        self.assertIsNotNone(item)
        assert item is not None
        self.assertEqual(item["kind"], "question")
        self.assertEqual(item["title"], "Input needed")
        self.assertEqual(item["choice_source"], "visible_readonly")
        self.assertTrue(str(item["turn_id"]).startswith("visible-readonly-question:"))
        self.assertIn("Should I patch", item["text"])
        self.assertIn("Visible-screen prompt only", item["text"])

    def test_visible_readonly_free_text_question_ignores_volatile_context(self) -> None:
        pane = {"pane_id": "pane-1", "agent": "claude", "agent_status": "idle"}
        first = """Status line A
I found two possible fixes.

Should I patch the timeout guard now?
"""
        second = """Different status line B
Additional visible context changed.

Should I patch the timeout guard now?
"""

        with patch.object(herdres, "pane_output", Mock(return_value=first)):
            first_item = herdres.extract_visible_readonly_feed_item(pane)
        with patch.object(herdres, "pane_output", Mock(return_value=second)):
            second_item = herdres.extract_visible_readonly_feed_item(pane)

        assert first_item is not None and second_item is not None
        self.assertEqual(first_item["turn_id"], second_item["turn_id"])
        self.assertEqual(first_item["text"], second_item["text"])
        self.assertNotIn("Status line", first_item["text"])
        self.assertNotIn("Different status", second_item["text"])

    def test_visible_readonly_skips_stale_scrollback_question_above_idle_prompt(self) -> None:
        # Regression: an action-question the agent already moved PAST (turn ended, then
        # compaction + tool activity, now idle at an empty prompt) must NOT be re-delivered
        # as "input needed". clean_feed_lines strips the idle chrome, which used to surface
        # the buried question; raw_visible_question_is_live() now guards it.
        pane = {"pane_id": "pane-1", "agent": "claude", "agent_status": "idle"}
        raw = """  Want me to kick off the login flow, or run the migrate for you?

✻ Worked for 5m 59s

❯ /compact
  ⎿  Compacted (ctrl+o to see full summary)
  ⎿  Read project/notes.md (31 lines)
  ⎿  Skills restored

──────────────────
❯
──────────────────
  ? for shortcuts · ← for agents
"""

        with patch.object(herdres, "pane_output", Mock(return_value=raw)):
            item = herdres.extract_visible_readonly_feed_item(pane)

        self.assertIsNone(item)

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
        self.assertIn("<b>Question</b>", html)
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
        self.assertEqual(item["choice_source"], "pending_decision")
        html = herdres.render_feed_item_html(item)
        self.assertIn("<details open><summary><b>User:</b></summary><blockquote>", html)
        self.assertIn("<b>User:</b>", html)
        self.assertNotIn("You asked", html)
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
        self.assertEqual(active_prompt["choice_source"], "pending_decision")
        self.assertNotIn("###", html)
        self.assertNotIn("**", html)
        self.assertNotIn("`", html)

    def test_structured_interactions_flag_disables_pending_decision_buttons(self) -> None:
        turn = {
            "available": True,
            "complete": False,
            "awaiting_input": True,
            "turn_id": "turn-1",
            "user_text": "Pick the next step.",
            "pending_decision": {
                "decision_id": "turn-1:decision-1",
                "prompt": "How should I proceed?",
                "options": [{"id": "watchdog", "label": "Build watchdog now", "send_text": "1"}],
            },
        }

        with patch.object(herdres, "STRUCTURED_INTERACTIONS_ENABLED", False):
            item = herdres.make_turn_feed_item(turn)

        self.assertIsNone(item)

    def test_pending_decision_rejects_multi_question_shape(self) -> None:
        turn = {
            "available": True,
            "complete": False,
            "awaiting_input": True,
            "turn_id": "turn-1",
            "user_text": "Answer the setup questions.",
            "pending_decision": {
                "decision_id": "turn-1:wizard",
                "kind": "multi_question_form",
                "prompt": "Answer these questions.",
                "questions": [
                    {
                        "question_id": "q1",
                        "title": "First",
                        "options": [{"id": "1", "label": "One", "send_text": "1"}],
                    },
                    {
                        "question_id": "q2",
                        "title": "Second",
                        "options": [{"id": "1", "label": "One", "send_text": "1"}],
                    },
                ],
                "options": [{"id": "1", "label": "Unsafe flat option", "send_text": "1"}],
            },
        }

        item = herdres.make_turn_feed_item(turn)

        self.assertIsNone(item)

    def test_valid_pending_interaction_wins_over_pending_decision(self) -> None:
        turn = {
            "available": True,
            "complete": False,
            "awaiting_input": True,
            "turn_id": "turn-1",
            "pending_interaction": {
                "interaction_id": "turn-1:interaction",
                "kind": "multi_question_form",
                "revision": 1,
                "questions": [
                    {
                        "question_id": "q1",
                        "title": "First question",
                        "options": [{"option_id": "1", "label": "One", "value": "1"}],
                    }
                ],
            },
            "pending_decision": {
                "decision_id": "turn-1:decision",
                "prompt": "Do not flatten this.",
                "options": [{"id": "1", "label": "One", "send_text": "1"}],
            },
        }

        item = herdres.make_turn_feed_item(turn)

        self.assertEqual(item["kind"], "interaction_readonly")
        self.assertEqual(item["interaction_id"], "turn-1:interaction")

    def test_pending_decision_rejects_when_pending_interaction_present(self) -> None:
        turn = {
            "available": True,
            "complete": False,
            "awaiting_input": True,
            "turn_id": "turn-1",
            "pending_interaction": {
                "interaction_id": "turn-1:interaction",
                "kind": "multi_question_form",
                "revision": 1,
                "questions": [
                    {
                        "question_id": "q1",
                        "title": "First question",
                        "options": [{"option_id": "1", "label": "One", "value": "1"}],
                    }
                ],
            },
            "pending_decision": {
                "decision_id": "turn-1:decision",
                "prompt": "This flat decision must not be used.",
                "options": [{"id": "1", "label": "Unsafe flat option", "send_text": "1"}],
            },
        }

        self.assertIsNone(herdres.normalize_pending_decision(turn))
        item = herdres.make_turn_feed_item(turn)

        self.assertIsNotNone(item)
        assert item is not None
        self.assertEqual(item["kind"], "interaction_readonly")
        self.assertEqual(item["interaction_id"], "turn-1:interaction")

    def test_pending_interaction_renders_readonly_questions_and_descriptions(self) -> None:
        item = herdres.make_turn_feed_item(
            {
                "available": True,
                "complete": False,
                "awaiting_input": True,
                "turn_id": "turn-1",
                "user_text": "Review the plan.",
                "pending_interaction": {
                    "interaction_id": "turn-1:interaction",
                    "revision": 2,
                    "kind": "multi_question_form",
                    "prompt": "Answer setup questions before implementation.",
                    "questions": [
                        {
                            "question_id": "q1",
                            "title": "Register/tone vs length",
                            "options": [
                                {
                                    "option_id": "1",
                                    "label": "Mostly register/tone",
                                    "description": "The objection is the teacher-like explainer voice.",
                                    "value": "1",
                                },
                                {
                                    "option_id": "2",
                                    "label": "Mostly length",
                                    "description": "The objection is that the reply is too long.",
                                    "value": "2",
                                },
                            ],
                        }
                    ],
                    "answers": {"q1": {"option_id": "1"}},
                },
            }
        )

        self.assertIsNotNone(item)
        assert item is not None
        self.assertEqual(item["kind"], "interaction_readonly")
        self.assertEqual(item["interaction_revision"], "2")
        self.assertEqual(item["questions"][0]["options"][0]["description"], "The objection is the teacher-like explainer voice.")
        html = herdres.render_feed_item_html(item)
        self.assertIn("<h3>Input needed</h3>", html)
        self.assertIn("Register/tone vs length", html)
        self.assertIn("Mostly register/tone", html)
        self.assertIn("teacher-like explainer voice", html)
        self.assertIn("Current answer", html)
        self.assertIn("⚠️ Manual action required", html)
        self.assertIn("Open Herdr and answer it there", html)
        self.assertEqual(html.count("Manual action required"), 1)
        plain = herdres.item_plain_text(item)
        self.assertIn("⚠️ Manual action required", plain)
        self.assertIn("Open Herdr and answer it there", plain)
        self.assertEqual(plain.count("Manual action required"), 1)
        self.assertNotIn("Read-only structured prompt", html)
        self.assertNotIn("Read-only structured prompt", plain)
        markup, active_prompt, clear_prompt = herdres.prompt_delivery_state(item)
        self.assertIsNone(markup)
        self.assertIsNone(active_prompt)
        self.assertTrue(clear_prompt)

    def test_malformed_pending_interaction_falls_through_to_valid_pending_decision(self) -> None:
        item = herdres.make_turn_feed_item(
            {
                "available": True,
                "complete": False,
                "awaiting_input": True,
                "turn_id": "turn-1",
                "pending_interaction": {
                    "interaction_id": "turn-1:interaction",
                    "kind": "multi_question_form",
                    "revision": 1,
                    "questions": [],
                },
                "pending_decision": {
                    "decision_id": "turn-1:decision",
                    "prompt": "Use the fallback decision?",
                    "options": [{"id": "1", "label": "Use decision", "send_text": "1"}],
                },
            }
        )

        self.assertIsNotNone(item)
        assert item is not None
        self.assertEqual(item["kind"], "decision")
        self.assertEqual(item["decision_id"], "turn-1:decision")

    def test_sync_turn_feed_sends_pending_interaction_readonly_without_buttons(self) -> None:
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
            "active_prompt": active_prompt({
                "id": "decision1",
                "choice_source": "pending_decision",
                "options": [{"number": "1", "label": "Old", "send_text": "1"}],
            }),
            "awaiting_detail": {
                "user_id": "42",
                "prompt_id": "decision1",
                "choice": "1",
                "created_at": herdres.utc_now(),
            },
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
                "complete": False,
                "awaiting_input": True,
                "turn_id": "turn-2",
                "pending_interaction": {
                    "interaction_id": "turn-2:interaction",
                    "revision": 1,
                    "kind": "multi_question_form",
                    "prompt": "Answer setup questions.",
                    "questions": [
                        {
                            "question_id": "q1",
                            "title": "Build plan",
                            "options": [
                                {"option_id": "1", "label": "Build now", "description": "Implement both phases."}
                            ],
                        }
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
            send_feed_item=send_feed_item,
            TURN_FEED_ENABLED=True,
            LIVE_CARD_ENABLED=False,
        ):
            result = herdres.sync_once()

        self.assertEqual(result["feed_sent"], 1)
        sent_item = send_feed_item.call_args.args[1]
        self.assertEqual(sent_item["kind"], "interaction_readonly")
        self.assertIsNone(send_feed_item.call_args.kwargs["reply_markup"])
        self.assertNotIn("active_prompt", entry)
        self.assertNotIn("awaiting_detail", entry)

    def test_prompt_delivery_blocks_pending_decision_when_structured_disabled(self) -> None:
        item = {
            "kind": "decision",
            "title": "Decision needed",
            "summary": "How should I proceed?",
            "text": "How should I proceed?\n1) Build",
            "options": [{"number": "1", "label": "Build", "send_text": "1"}],
            "prompt_id": "decision1",
            "decision_id": "turn-1:decision-1",
            "choice_source": "pending_decision",
        }

        with patch.object(herdres, "STRUCTURED_INTERACTIONS_ENABLED", False):
            markup, active_prompt, clear_prompt = herdres.prompt_delivery_state(item)

        self.assertIsNone(markup)
        self.assertIsNone(active_prompt)
        self.assertTrue(clear_prompt)

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

    def test_callback_rejects_old_message_with_same_prompt_id(self) -> None:
        state = callback_state()
        send_to_pane = Mock(return_value=(True, ""))

        with callback_patches(state, send_to_pane=send_to_pane):
            result = herdres.callback_reply(
                {
                    **callback_payload(user_id="42", data="herdr:c:prompt1:1"),
                    "message_id": "444",
                }
            )

        self.assertIn("older Telegram message", result["answer"])
        send_to_pane.assert_not_called()
        self.assertIn("active_prompt", state["panes"]["pane-1"])

    def test_callback_rejects_expired_unbound_prompt(self) -> None:
        state = callback_state()
        old = (
            herdres._dt.datetime.now(tz=herdres._dt.timezone.utc)
            - herdres._dt.timedelta(seconds=herdres.ACTIVE_PROMPT_TTL_SECONDS + 30)
        )
        prompt = state["panes"]["pane-1"]["active_prompt"]
        prompt.pop("message_id", None)
        prompt["created_at"] = old.isoformat()
        send_to_pane = Mock(return_value=(True, ""))

        with callback_patches(state, send_to_pane=send_to_pane):
            result = herdres.callback_reply(callback_payload(user_id="42", data="herdr:c:prompt1:1"))

        self.assertIn("no longer active", result["answer"])
        send_to_pane.assert_not_called()
        self.assertNotIn("active_prompt", state["panes"]["pane-1"])

    def test_callback_rejects_expired_bound_prompt(self) -> None:
        state = callback_state()
        old = (
            herdres._dt.datetime.now(tz=herdres._dt.timezone.utc)
            - herdres._dt.timedelta(seconds=herdres.ACTIVE_PROMPT_TTL_SECONDS + 30)
        )
        prompt = state["panes"]["pane-1"]["active_prompt"]
        prompt["message_id"] = "555"
        prompt["created_at"] = old.isoformat()
        send_to_pane = Mock(return_value=(True, ""))

        with callback_patches(state, send_to_pane=send_to_pane):
            result = herdres.callback_reply(callback_payload(user_id="42", data="herdr:c:prompt1:1"))

        self.assertIn("expired", result["answer"].lower())
        send_to_pane.assert_not_called()
        self.assertIn("active_prompt", state["panes"]["pane-1"])
        self.assertNotIn("awaiting_detail", state["panes"]["pane-1"])
        send_feed_item = Mock(return_value={"ok": True, "message_id": "999"})

        with patch.multiple(
            herdres,
            load_dotenv=Mock(),
            load_state=Mock(return_value=state),
            save_state=Mock(),
            send_feed_item=send_feed_item,
        ):
            refreshed = herdres.command_reply(
                {"chat_id": "-1001", "topic_id": "77", "user_id": "42", "text": "/choices"}
            )

        self.assertTrue(refreshed["handled"])
        self.assertEqual(refreshed["reply"], "")
        send_feed_item.assert_called_once()
        self.assertEqual(state["panes"]["pane-1"]["active_prompt"]["message_id"], "999")

    def test_callback_routes_decision_send_text(self) -> None:
        state = callback_state()
        state["panes"]["pane-1"]["active_prompt"] = active_prompt({
            "id": "decision1",
            "choice_source": "pending_decision",
            "options": [
                {"number": "watchdog", "label": "Build watchdog now", "send_text": "1"},
                {"number": "timeout", "label": "Patch timeout only", "send_text": "2"},
            ],
        })
        send_to_pane = Mock(return_value=(True, ""))

        with callback_patches(state, send_to_pane=send_to_pane):
            result = herdres.callback_reply(callback_payload(user_id="42", data="herdr:c:decision1:timeout"))

        self.assertEqual(result["answer"], "Selected timeout.")
        send_to_pane.assert_called_once_with("pane-1", "2")
        self.assertNotIn("active_prompt", state["panes"]["pane-1"])

    def test_callback_custom_decision_option_sets_force_reply(self) -> None:
        state = callback_state()
        state["panes"]["pane-1"]["active_prompt"] = active_prompt({
            "id": "decision1",
            "choice_source": "pending_decision",
            "options": [
                {"number": "custom", "id": "custom", "label": "Write custom instruction", "send_text": "", "needs_detail": "1"},
            ],
        })
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
        state["panes"]["pane-1"]["active_prompt"] = active_prompt({
            "id": "decision1",
            "choice_source": "pending_decision",
            "options": [
                {"number": "patch", "label": "Patch with extra detail", "send_text": "1", "needs_detail": "1"},
            ],
        })
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
        state["panes"]["pane-1"]["active_prompt"] = active_prompt({
            "id": "prompt1",
            "choice_source": "visible_scrape",
            "options": [
                {"number": "4", "label": "Type something."},
                {"number": "5", "label": "Chat about this"},
            ],
        })
        send_notice = Mock(return_value={"ok": True, "message_id": "777"})
        send_to_pane = Mock(return_value=(True, ""))

        with callback_patches(state, send_notice=send_notice, send_to_pane=send_to_pane), patch.object(
            herdres,
            "VISIBLE_CHOICE_BUTTONS_ENABLED",
            True,
        ):
            result = herdres.callback_reply(callback_payload(user_id="42", data="herdr:d:prompt1:4"))

        self.assertEqual(result["answer"], "Write the details in this topic.")
        self.assertEqual(state["panes"]["pane-1"]["awaiting_detail"]["choice"], "")
        self.assertEqual(state["panes"]["pane-1"]["awaiting_detail"]["visible_choice"], "4")
        self.assertEqual(state["panes"]["pane-1"]["awaiting_detail"]["visible_choice_index"], 1)
        send_to_pane.assert_not_called()

    def test_callback_rejects_leftover_visible_choice_buttons_when_disabled(self) -> None:
        state = callback_state()
        state["panes"]["pane-1"]["active_prompt"] = active_prompt({
            "id": "prompt1",
            "choice_source": "visible_scrape",
            "item": {"kind": "choices", "turn_id": "visible-choice:prompt1", "choice_source": "visible_scrape"},
            "options": [{"number": "4", "label": "Type something."}],
        })
        send_to_pane = Mock(return_value=(True, ""))
        send_visible_choice_detail_to_pane = Mock(return_value=(True, ""))

        with callback_patches(state, send_to_pane=send_to_pane), patch.multiple(
            herdres,
            VISIBLE_CHOICE_BUTTONS_ENABLED=False,
            send_visible_choice_detail_to_pane=send_visible_choice_detail_to_pane,
        ):
            result = herdres.callback_reply(callback_payload(user_id="42", data="herdr:d:prompt1:4"))

        self.assertTrue("no longer safe" in result["answer"] or "no longer active" in result["answer"])
        send_to_pane.assert_not_called()
        send_visible_choice_detail_to_pane.assert_not_called()
        self.assertNotIn("active_prompt", state["panes"]["pane-1"])
        self.assertNotIn("awaiting_detail", state["panes"]["pane-1"])

    def test_callback_rejects_legacy_clean_feed_choice_buttons_when_disabled(self) -> None:
        state = callback_state()
        state["panes"]["pane-1"]["active_prompt"] = active_prompt({
            "id": "prompt1",
            "choice_source": "legacy_clean_feed",
            "item": {"kind": "choices", "choice_source": "legacy_clean_feed"},
            "options": [{"number": "1", "label": "Build default path"}],
        })
        send_to_pane = Mock(return_value=(True, ""))

        with callback_patches(state, send_to_pane=send_to_pane), patch.object(
            herdres,
            "VISIBLE_CHOICE_BUTTONS_ENABLED",
            False,
        ):
            result = herdres.callback_reply(callback_payload(user_id="42", data="herdr:c:prompt1:1"))

        self.assertTrue("no longer safe" in result["answer"] or "no longer active" in result["answer"])
        send_to_pane.assert_not_called()
        self.assertNotIn("active_prompt", state["panes"]["pane-1"])

    def test_callback_rejects_legacy_choice_when_legacy_disabled_even_if_visible_enabled(self) -> None:
        state = callback_state()
        state["panes"]["pane-1"]["active_prompt"] = active_prompt({
            "id": "prompt1",
            "choice_source": "legacy_clean_feed",
            "item": {"kind": "choices", "choice_source": "legacy_clean_feed"},
            "options": [{"number": "1", "label": "Build default path"}],
        })
        send_to_pane = Mock(return_value=(True, ""))

        with callback_patches(state, send_to_pane=send_to_pane), patch.multiple(
            herdres,
            VISIBLE_CHOICE_BUTTONS_ENABLED=True,
            LEGACY_CHOICES_ENABLED=False,
        ):
            result = herdres.callback_reply(callback_payload(user_id="42", data="herdr:c:prompt1:1"))

        self.assertIn("no longer active", result["answer"])
        send_to_pane.assert_not_called()
        self.assertNotIn("active_prompt", state["panes"]["pane-1"])

    def test_disabled_visible_choice_cleanup_preserves_structured_prompts(self) -> None:
        state = {
            "panes": {
                "visible": {
                    "active_prompt": {
                        "id": "visible1",
                        "choice_source": "visible_scrape",
                        "item": {"turn_id": "visible-choice:visible1"},
                    },
                    "awaiting_detail": {"user_id": "42", "visible_choice": "4"},
                },
                "structured": {
                    "active_prompt": {
                        "id": "decision1",
                        "choice_source": "pending_decision",
                        "decision_id": "turn:decision1",
                        "message_id": "555",
                        "created_at": herdres.utc_now(),
                    },
                    "awaiting_detail": {
                        "user_id": "42",
                        "choice": "1",
                        "decision_id": "turn:decision1",
                    },
                },
            }
        }

        with patch.object(herdres, "VISIBLE_CHOICE_BUTTONS_ENABLED", False):
            changed = herdres.clear_disabled_visible_choice_state(state)

        self.assertTrue(changed)
        self.assertNotIn("active_prompt", state["panes"]["visible"])
        self.assertNotIn("awaiting_detail", state["panes"]["visible"])
        self.assertIn("active_prompt", state["panes"]["structured"])
        self.assertIn("awaiting_detail", state["panes"]["structured"])

    def test_disabled_prompt_cleanup_clears_structured_prompts_when_opted_out(self) -> None:
        state = {
            "panes": {
                "structured": {
                    "active_prompt": {
                        "id": "decision1",
                        "choice_source": "pending_decision",
                        "decision_id": "turn:decision1",
                    },
                    "awaiting_detail": {
                        "user_id": "42",
                        "choice": "1",
                        "decision_id": "turn:decision1",
                    },
                },
            }
        }

        with patch.object(herdres, "STRUCTURED_INTERACTIONS_ENABLED", False):
            changed = herdres.clear_disabled_visible_choice_state(state)

        self.assertTrue(changed)
        self.assertNotIn("active_prompt", state["panes"]["structured"])
        self.assertNotIn("awaiting_detail", state["panes"]["structured"])

    def test_unbound_active_prompt_cleanup_clears_awaiting_detail(self) -> None:
        entry = {
            "active_prompt": {
                "id": "decision1",
                "choice_source": "pending_decision",
                "created_at": herdres.utc_now(),
            },
            "awaiting_detail": {
                "user_id": "42",
                "prompt_id": "decision1",
                "choice": "custom",
                "created_at": herdres.utc_now(),
            },
        }
        state = {"version": 1, "telegram": {}, "panes": {"pane": entry}}

        changed = herdres.clear_disabled_visible_choice_state(state)

        self.assertTrue(changed)
        self.assertNotIn("active_prompt", entry)
        self.assertNotIn("awaiting_detail", entry)

    def test_sync_disabled_mode_saves_prompt_cleanup(self) -> None:
        entry = {
            "active_prompt": {
                "id": "visible1",
                "choice_source": "visible_scrape",
            },
            "awaiting_detail": {"user_id": "42", "visible_choice": "4"},
        }
        state = {"version": 1, "enabled": False, "telegram": {}, "panes": {"pane": entry}}
        save_state = Mock()

        with patch.multiple(
            herdres,
            load_dotenv=Mock(),
            load_state=Mock(return_value=state),
            save_state=save_state,
            VISIBLE_CHOICE_BUTTONS_ENABLED=False,
        ):
            result = herdres.sync_once()

        self.assertTrue(result["changed"])
        self.assertNotIn("active_prompt", entry)
        self.assertNotIn("awaiting_detail", entry)
        save_state.assert_called_once_with(state)

    def test_bound_active_prompt_with_detail_survives_cleanup_until_expired(self) -> None:
        entry = {
            "active_prompt": active_prompt({
                "id": "decision1",
                "choice_source": "pending_decision",
                "decision_id": "turn:decision1",
            }),
            "awaiting_detail": {
                "user_id": "42",
                "prompt_id": "decision1",
                "choice": "custom",
                "decision_id": "turn:decision1",
                "created_at": herdres.utc_now(),
            },
        }
        state = {"version": 1, "telegram": {}, "panes": {"pane": entry}}

        changed = herdres.clear_disabled_visible_choice_state(state)

        self.assertFalse(changed)
        self.assertIn("active_prompt", entry)
        self.assertIn("awaiting_detail", entry)

    def test_stale_visible_choice_callback_refreshes_current_prompt(self) -> None:
        state = callback_state()
        state["panes"]["pane-1"]["active_prompt"] = active_prompt({
            "id": "oldprompt",
            "item": {"kind": "choices", "turn_id": "visible-choice:oldprompt"},
            "options": [{"number": "1", "label": "Old option"}],
        })
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
            VISIBLE_CHOICE_BUTTONS_ENABLED=True,
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
            VISIBLE_CHOICE_BUTTONS_ENABLED=True,
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

    def test_force_reply_visible_choice_detail_disabled_clears_without_keydrive(self) -> None:
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
            VISIBLE_CHOICE_BUTTONS_ENABLED=False,
            send_visible_choice_detail_to_pane=send_visible_choice_detail_to_pane,
        ):
            result = herdres.command_reply(
                {
                    "chat_id": "-1001",
                    "topic_id": "77",
                    "user_id": "42",
                    "text": "custom q2 answer",
                    "reply_to_message_id": "999",
                }
            )

        self.assertTrue("no longer safe" in result["reply"] or "Use /send" in result["reply"])
        send_visible_choice_detail_to_pane.assert_not_called()
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
            VISIBLE_CHOICE_BUTTONS_ENABLED=True,
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

        self.assertEqual(
            result["reply"],
            "Reply to the detail prompt above (use Telegram's Reply), or tap the option button again to re-open it.",
        )
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

    # --- Issue #37: relax the reply gate for a BARE answer in an unambiguous topic ---
    # A plain typed message (no Telegram "Reply" gesture) is accepted as the answer to the one open
    # prompt when the topic unambiguously targets a single pane; an explicit reply to a DIFFERENT
    # message stays strict (it could target a superseded prompt / wrong pane).

    @staticmethod
    def _single_live_space(state: dict) -> None:
        """Map topic 77 to a space with exactly one live pane (mirrors prod per-agent topology),
        so is_single_live_space_pane(...) is True."""
        state["spaces"] = {
            "agent:pane-1": {"space_key": "agent:pane-1", "topic_id": "77", "pane_keys": ["pane-1"]}
        }

    def test_force_reply_detail_accepts_bare_message_in_single_pane_topic(self) -> None:
        # #37 happy path: no Telegram Reply gesture, no /send — a plain answer resolves the one prompt.
        state = callback_state()
        self._single_live_space(state)
        state["panes"]["pane-1"]["awaiting_detail"] = {
            "user_id": "42", "prompt_id": "prompt1", "choice": "1",
            "force_reply_message_id": "999", "created_at": herdres.utc_now(),
        }
        send_to_pane = Mock(return_value=(True, ""))
        with callback_patches(state, send_to_pane=send_to_pane):
            result = herdres.command_reply(
                {"chat_id": "-1001", "topic_id": "77", "user_id": "42", "text": "extra detail"}
            )  # NOTE: no reply_to_message_id
        self.assertEqual(result["reply"], "Sent details.")
        send_to_pane.assert_called_once_with("pane-1", "1\nextra detail")
        self.assertNotIn("awaiting_detail", state["panes"]["pane-1"])
        self.assertNotIn("active_prompt", state["panes"]["pane-1"])

    def test_force_reply_detail_wrong_reply_still_strict_in_single_pane_topic(self) -> None:
        # Relaxation is BARE-only: an explicit reply to a DIFFERENT/older message stays strict even in
        # a single-pane topic, so a reply to a superseded prompt cannot mis-attach to the current one.
        state = callback_state()
        self._single_live_space(state)
        state["panes"]["pane-1"]["awaiting_detail"] = {
            "user_id": "42", "prompt_id": "prompt1", "choice": "1",
            "force_reply_message_id": "999", "created_at": herdres.utc_now(),
        }
        send_to_pane = Mock(return_value=(True, ""))
        with callback_patches(state, send_to_pane=send_to_pane):
            result = herdres.command_reply(
                {"chat_id": "-1001", "topic_id": "77", "user_id": "42",
                 "text": "extra detail", "reply_to_message_id": "123"}  # explicit reply, != force_reply 999
            )
        self.assertEqual(
            result["reply"],
            "Reply to the detail prompt above (use Telegram's Reply), or tap the option button again to re-open it.",
        )
        send_to_pane.assert_not_called()
        self.assertIn("awaiting_detail", state["panes"]["pane-1"])  # preserved

    def test_force_reply_detail_still_strict_in_multi_pane_topic(self) -> None:
        # Two live panes share the topic: a mis-targeted reply stays ambiguous, so the gate holds.
        state = callback_state()
        state["spaces"] = {
            "workspace:w1": {
                "space_key": "workspace:w1", "topic_id": "77",
                "pane_keys": ["pane-1", "pane-2"], "message_routes": {"123": "pane-1"},
            }
        }
        state["panes"]["pane-2"] = {"pane_id": "pane-2", "topic_id": "77", "last_known_status": "working"}
        state["panes"]["pane-1"]["awaiting_detail"] = {
            "user_id": "42", "prompt_id": "prompt1", "choice": "1",
            "force_reply_message_id": "999", "created_at": herdres.utc_now(),
        }
        send_to_pane = Mock(return_value=(True, ""))
        with callback_patches(state, send_to_pane=send_to_pane):
            result = herdres.command_reply(
                {"chat_id": "-1001", "topic_id": "77", "user_id": "42",
                 "text": "extra detail", "reply_to_message_id": "123"}  # routes to pane-1 but != 999
            )
        self.assertEqual(
            result["reply"],
            "Reply to the detail prompt above (use Telegram's Reply), or tap the option button again to re-open it.",
        )
        send_to_pane.assert_not_called()
        self.assertIn("awaiting_detail", state["panes"]["pane-1"])  # preserved for a correct reply/re-tap

    def test_force_reply_detail_expired_clears_before_relaxed_gate_in_single_pane_topic(self) -> None:
        # The expiry guard runs BEFORE the relaxed gate: a stale prompt is cleared, never sent.
        state = callback_state()
        self._single_live_space(state)
        state["panes"]["pane-1"]["awaiting_detail"] = {
            "user_id": "42", "prompt_id": "prompt1", "choice": "1",
            "force_reply_message_id": "999", "created_at": "2020-01-01T00:00:00Z",
        }
        send_to_pane = Mock(return_value=(True, ""))
        with callback_patches(state, send_to_pane=send_to_pane):
            result = herdres.command_reply(
                {"chat_id": "-1001", "topic_id": "77", "user_id": "42", "text": "extra detail"}
            )
        self.assertEqual(result["reply"], "That detail request expired. Use /choices to resend the choices.")
        send_to_pane.assert_not_called()
        self.assertNotIn("awaiting_detail", state["panes"]["pane-1"])

    def test_force_reply_detail_other_owner_bare_message_does_not_answer_prompt(self) -> None:
        # Cross-user isolation: a different owner's plain text forwards as a fresh instruction and
        # must NOT consume someone else's open prompt (the choice is not prepended; slot preserved).
        state = callback_state()
        self._single_live_space(state)
        state["telegram"]["owner_user_ids"] = ["42", "99"]
        state["panes"]["pane-1"]["awaiting_detail"] = {
            "user_id": "42", "prompt_id": "prompt1", "choice": "1",
            "force_reply_message_id": "999", "created_at": herdres.utc_now(),
        }
        send_to_pane = Mock(return_value=(True, ""))
        with callback_patches(state, send_to_pane=send_to_pane):
            herdres.command_reply(
                {"chat_id": "-1001", "topic_id": "77", "user_id": "99", "text": "unrelated message"}
            )
        send_to_pane.assert_called_once_with("pane-1", "unrelated message")  # forwarded raw, not "1\n..."
        self.assertIn("awaiting_detail", state["panes"]["pane-1"])  # owner 42's prompt untouched

    def test_force_reply_detail_accepts_bare_message_when_agent_picked_in_multi_pane_topic(self) -> None:
        # Multi-pane topic, but the owner picked this agent via /agents (resolved_active_entry): a bare
        # answer is unambiguous and accepted, matching how a bare non-prompt message forwards there.
        state = callback_state()
        state["spaces"] = {
            "workspace:w1": {
                "space_key": "workspace:w1", "topic_id": "77",
                "pane_keys": ["pane-1", "pane-2"],
                "active_pane": {"42": {"pane_key": "pane-1", "set_at": herdres.utc_now()}},
            }
        }
        state["panes"]["pane-2"] = {"pane_id": "pane-2", "topic_id": "77", "last_known_status": "working"}
        state["panes"]["pane-1"]["awaiting_detail"] = {
            "user_id": "42", "prompt_id": "prompt1", "choice": "1",
            "force_reply_message_id": "999", "created_at": herdres.utc_now(),
        }
        send_to_pane = Mock(return_value=(True, ""))
        with callback_patches(state, send_to_pane=send_to_pane):
            result = herdres.command_reply(
                {"chat_id": "-1001", "topic_id": "77", "user_id": "42", "text": "extra detail"}
            )  # bare — resolves via the active pane, not a reply
        self.assertEqual(result["reply"], "Sent details.")
        send_to_pane.assert_called_once_with("pane-1", "1\nextra detail")
        self.assertNotIn("awaiting_detail", state["panes"]["pane-1"])

    def test_force_reply_detail_bare_send_failure_preserves_slot_in_single_pane_topic(self) -> None:
        # A transient send failure on the relaxed bare path must NOT consume the prompt (retryable).
        state = callback_state()
        self._single_live_space(state)
        state["panes"]["pane-1"]["awaiting_detail"] = {
            "user_id": "42", "prompt_id": "prompt1", "choice": "1",
            "force_reply_message_id": "999", "created_at": herdres.utc_now(),
        }
        send_to_pane = Mock(return_value=(False, "boom"))
        with callback_patches(state, send_to_pane=send_to_pane):
            result = herdres.command_reply(
                {"chat_id": "-1001", "topic_id": "77", "user_id": "42", "text": "extra detail"}
            )
        self.assertEqual(result["reply"], "Send failed: boom")
        self.assertIn("awaiting_detail", state["panes"]["pane-1"])  # preserved for retry
        self.assertIn("active_prompt", state["panes"]["pane-1"])

    def test_force_reply_detail_bare_select_choice_in_single_pane_topic(self) -> None:
        # The relaxed bare path also reaches the select_choice dispatch (keys + detail), not just plain.
        state = callback_state()
        self._single_live_space(state)
        state["panes"]["pane-1"]["awaiting_detail"] = {
            "user_id": "42", "prompt_id": "prompt1", "choice": "", "select_choice": "2",
            "force_reply_message_id": "999", "created_at": herdres.utc_now(),
        }
        send_choice = Mock(return_value=(True, ""))
        with callback_patches(state), patch.object(herdres, "send_choice_detail_to_pane", send_choice):
            result = herdres.command_reply(
                {"chat_id": "-1001", "topic_id": "77", "user_id": "42", "text": "with detail"}
            )  # bare
        self.assertEqual(result["reply"], "Sent details.")
        send_choice.assert_called_once_with("pane-1", "2", "with detail")
        self.assertNotIn("awaiting_detail", state["panes"]["pane-1"])

    def test_force_reply_visible_choice_bare_message_fails_closed_when_prompt_changed(self) -> None:
        # Visible-prompt re-validation runs AFTER the relaxed gate: a bare answer to a visible choice
        # whose on-screen options changed is still rejected (fail-closed) and never key-driven.
        state = callback_state()
        self._single_live_space(state)
        state["panes"]["pane-1"]["awaiting_detail"] = {
            "user_id": "42", "prompt_id": "prompt1", "choice": "",
            "visible_choice": "4", "visible_options": [{"number": "4", "label": "Type something."}],
            "force_reply_message_id": "999", "created_at": herdres.utc_now(),
        }
        send_visible = Mock(return_value=(True, ""))
        with callback_patches(state), patch.multiple(
            herdres,
            VISIBLE_CHOICE_BUTTONS_ENABLED=True,
            send_visible_choice_detail_to_pane=send_visible,
            visible_prompt_matches_awaiting=Mock(return_value=False),
        ):
            result = herdres.command_reply(
                {"chat_id": "-1001", "topic_id": "77", "user_id": "42", "text": "answer"}
            )  # bare, no reply_to
        self.assertIn("choices changed", result["reply"])
        send_visible.assert_not_called()
        self.assertNotIn("awaiting_detail", state["panes"]["pane-1"])

    def test_prompt_feed_renders_user_label_in_open_details_quote(self) -> None:
        item = herdres.make_prompt_feed_item("turn-1", "Why did the bot freeze?\nCheck logs.")

        html = herdres.render_feed_item_html(item)

        self.assertEqual(item["title"], "User:")
        self.assertEqual(item["text"], "User:\nWhy did the bot freeze?\nCheck logs.")
        self.assertIn("<details open><summary><b>User:</b></summary><blockquote>", html)
        self.assertIn("<b>User:</b>", html)
        self.assertIn("Why did the bot freeze?", html)
        self.assertNotIn("You asked", html)
        self.assertEqual(herdres.item_plain_text(item), "User:\nWhy did the bot freeze?\nCheck logs.")

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

        self.assertIn("<details open><summary><b>User:</b></summary><blockquote>", html)
        response_prefix = "<details open><summary><b>Response</b></summary>"
        response_body = "<h3>Likely cause</h3>"
        self.assertIn(response_prefix + response_body, html)
        response_start = html.index(response_prefix)
        response_body_start = html.index(response_body, response_start)
        self.assertNotIn("<blockquote>", html[response_start:response_body_start])
        self.assertIn("<b>User:</b>", html)
        self.assertNotIn("You asked", html)
        self.assertIn("Why did the bot freeze?", html)
        self.assertIn("<h3>Likely cause</h3>", html)
        self.assertIn("<li>Browser navigation hung.</li>", html)
        self.assertNotIn("<h3>Question</h3>", html)
        self.assertNotIn("<h3>Report</h3>", html)
        self.assertNotIn("<h3>Update</h3>", html)

    def test_turn_feed_renders_worklog_closed_when_response_exists(self) -> None:
        item = {
            "kind": "turn",
            "user_text": "Run the profile.",
            "worklog_text": "I started the CPU profile and am waiting on results.",
            "assistant_final_text": "Profile complete.\n\n- Hot path found.",
        }

        html = herdres.render_turn_item_html(item)

        self.assertIn("<details open><summary><b>User:</b></summary><blockquote>", html)
        self.assertIn("<details><summary><b>Worklog</b></summary><blockquote>", html)
        response_prefix = "<details open><summary><b>Response</b></summary>"
        response_body = "<p>Profile complete.</p>"
        self.assertIn(response_prefix + response_body, html)
        response_start = html.index(response_prefix)
        response_body_start = html.index(response_body, response_start)
        self.assertNotIn("<blockquote>", html[response_start:response_body_start])
        self.assertLess(html.index("<b>User:</b>"), html.index("<b>Worklog</b>"))
        self.assertLess(html.index("<b>Worklog</b>"), html.index("<b>Response</b>"))

    def test_turn_feed_renders_worklog_open_when_response_missing(self) -> None:
        item = {
            "kind": "turn",
            "user_text": "Run the profile.",
            "worklog_text": "The CPU profile is still running.",
            "assistant_final_text": "",
        }

        html = herdres.render_turn_item_html(item)

        self.assertIn("<details open><summary><b>User:</b></summary><blockquote>", html)
        self.assertIn("<details open><summary><b>Worklog</b></summary><blockquote>", html)
        self.assertNotIn("<b>Response</b>", html)

    def test_turn_feed_formats_screenshot_case_as_rich_html(self) -> None:
        item = {
            "kind": "turn",
            "user_text": (
                "also you see how each pane on herdr can have a name? i would like it so that "
                "if i change a name on a herdr pane manually, it automatically changes the name "
                "of the topic on telegram"
            ),
            "assistant_final_text": """Implemented.
Herdres now watches Herdr space names for Telegram topic names and Herdr pane `label` values for pane thread roots:
- Existing labeled panes keep their labels as thread names; they do not rename the shared space topic.
- If you manually change a Herdr pane name after this, the next sync updates the pane root with `sendMessage` routing metadata.
- If Telegram says the space topic is missing, Herdres clears that stale topic mapping and recreates it on the next sync.
- No Herdr core changes and no LLM calls.

Deployed live to `/home/smith/.local/bin/herdr_telegram_topics.py`.

Pushed
`cdee2ca Sync Telegram topics from Herdr spaces`

Verification

- `python3 -m py_compile herdres.py herdr_turn_adapter.py`
- `python3 -m unittest discover -s tests -p 'test*.py' -q` -> 78 tests OK
- Live sync ran successfully: `renamed=0`, `sent=0`, `panes=6`
- Existing `entmoot italy ping` label was kept as the pane thread name while the Telegram topic stayed the Herdr space name.
""",
        }

        html = herdres.render_turn_item_html(item)

        self.assertIn("<details open><summary><b>User:</b></summary><blockquote>", html)
        response_prefix = "<details open><summary><b>Response</b></summary>"
        response_body = "<h3>Implemented</h3>"
        self.assertIn(response_prefix + response_body, html)
        response_start = html.index(response_prefix)
        response_body_start = html.index(response_body, response_start)
        self.assertNotIn("<blockquote>", html[response_start:response_body_start])
        self.assertIn("<h3>Implemented</h3>", html)
        self.assertIn("<b>Pushed</b>", html)
        self.assertIn("<b>Verification</b>", html)
        self.assertIn("<code>label</code>", html)
        self.assertIn("<code>sendMessage</code>", html)
        self.assertIn("<code>/home/smith/.local/bin/herdr_telegram_topics.py</code>", html)
        self.assertIn("<code>cdee2ca</code> Sync Telegram topics from Herdr spaces", html)
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
        self.assertIn("<b>Verification</b>", html)
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

    def test_dense_codex_status_turn_splits_inline_sections(self) -> None:
        text = """Fixed — it's genuinely reviewing now (Codex worker at 11% CPU, model: gpt-5.5, sandbox: read-only, xhigh). The earlier Reading additional input from stdin... line is now harmless (it hit /dev/null -> immediate EOF -> proceeded).
What happened: my first Codex review invocation was malformed — codex exec was given the prompt as an argument and an open stdin (because I backgrounded it), and per its docs it then waits to append stdin as a <stdin> block. With nothing ever closing stdin, it blocked for 2h12m at 0% CPU and produced zero review.
No harm done (it was read-only and did nothing), but it wasted time.
Fix: the one-character-class change was < /dev/null so stdin closes immediately.
Now the real Codex 5.5 xhigh review of the plan is running. When it lands I'll report exactly where Codex and I agree/disagree and run another round if needed — then, since you've approved, proceed to implement.
"""

        html = herdres.render_final_reply_html(text)

        self.assertIn("<h3>Fixed</h3>", html)
        self.assertIn("<b>What happened</b>", html)
        self.assertIn("<b>Fix</b>", html)
        self.assertIn("genuinely reviewing now", html)
        self.assertIn("2h12m", html)
        self.assertIn("No harm done", html)
        self.assertNotIn("What happened: my first", html)
        self.assertNotIn("<b>model</b>", html)
        self.assertNotIn("<b>sandbox</b>", html)

    def test_dense_turn_inline_section_split_is_strict(self) -> None:
        html = herdres.render_final_reply_html(
            "Fixed the bug now\n"
            "Note: this should remain a paragraph.\n"
            "Example: this should also remain a paragraph.\n"
            "The change: keep it inline, not a heading.\n"
            "- Fix: this bullet should stay a bullet.\n"
            "What changed:\n"
            "- tests OK"
        )

        self.assertNotIn("<h3>Fixed</h3>", html)
        self.assertNotIn("<b>Note</b>", html)
        self.assertNotIn("<b>Example</b>", html)
        self.assertNotIn("<b>The change</b>", html)
        self.assertIn("<li>Fix: this bullet should stay a bullet.</li>", html)
        self.assertEqual(html.count("<b>What changed</b>"), 1)

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
        self.assertIn("User:", entry["last_clean_text"])
        self.assertNotIn("You asked", entry["last_clean_text"])
        self.assertIn("Likely cause", entry["last_clean_text"])
        self.assertNotIn("Question\nShould not be parsed", entry["last_clean_text"])

    def test_sync_turn_feed_failed_send_does_not_advance_turn_cursor(self) -> None:
        pane = {
            "pane_id": "pane-1",
            "terminal_id": "term-1",
            "workspace_id": "workspace-1",
            "tab_id": "tab-1",
            "agent": "claude",
            "agent_status": "done",
        }
        key = herdres.pane_key(pane)
        entry = {
            "pane_key": key,
            "pane_id": "pane-1",
            "topic_id": "77",
            "last_turn_id": "turn-a",
            "last_topic_verified_at": herdres.utc_now(),
        }
        state = {
            "version": 1,
            "enabled": True,
            "telegram": {"chat_id": "-1001", "general_thread_id": "1", "owner_user_ids": ["42"]},
            "panes": {key: entry},
        }

        def mk(tid: str, text: str) -> dict:
            return {"available": True, "complete": True, "turn_id": tid, "assistant_final_text": text}

        recent = [mk("turn-a", "Already sent."), mk("turn-b", "Retry me."), mk("turn-c", "Later.")]
        pane_turn = Mock(return_value={**recent[-1], "recent_turns": recent})
        send_feed_item = Mock(side_effect=[
            {"ok": False, "error": "temporary"},
            {"ok": True, "message_id": "999"},
        ])

        patches = {
            "load_dotenv": Mock(),
            "load_state": Mock(return_value=state),
            "save_state": Mock(),
            "pane_list": Mock(return_value=[pane]),
            "preflight_is_fresh": Mock(return_value=True),
            "pane_turn": pane_turn,
            "send_feed_item": send_feed_item,
            "TURN_FEED_ENABLED": True,
            "LIVE_CARD_ENABLED": False,
            "STATUS_MARKER_ENABLED": False,
        }
        with patch.multiple(herdres, **patches):
            first = herdres.sync_once()

        self.assertTrue(first["changed"])
        self.assertEqual(send_feed_item.call_args.args[1]["turn_id"], "turn-b")
        self.assertEqual(entry["last_turn_id"], "turn-a")
        self.assertNotIn("last_clean_item", entry)

        old_attempt = (
            herdres._dt.datetime.now(tz=herdres._dt.timezone.utc)
            - herdres._dt.timedelta(seconds=herdres.CLEAN_ATTEMPT_TTL_SECONDS + 1)
        )
        entry["last_clean_attempt_at"] = old_attempt.isoformat()

        with patch.multiple(herdres, **patches):
            second = herdres.sync_once()

        self.assertTrue(second["changed"])
        self.assertEqual(send_feed_item.call_args.args[1]["turn_id"], "turn-b")
        self.assertEqual(entry["last_turn_id"], "turn-b")
        self.assertEqual(entry["last_clean_item"]["turn_id"], "turn-b")

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
        self.assertEqual(sent_item["choice_source"], "pending_decision")
        self.assertEqual(entry["last_clean_kind"], "decision")
        self.assertEqual(entry["active_prompt"]["decision_id"], "turn-2:decision-1")
        self.assertEqual(entry["active_prompt"]["options"][1]["send_text"], "2")
        self.assertEqual(entry["active_prompt"]["message_id"], "1001")
        reply_markup = send_feed_item.call_args.kwargs["reply_markup"]
        self.assertEqual(reply_markup["inline_keyboard"][1][0]["callback_data"], f"herdr:c:{sent_item['prompt_id']}:full")
        self.assertIn("Which path should I take?", entry["last_clean_text"])

    def test_sync_button_send_without_message_id_does_not_activate_prompt(self) -> None:
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
                    "options": [{"id": "fast", "label": "Patch minimal path", "send_text": "1"}],
                },
            }
        )
        send_feed_item = Mock(return_value={"ok": True})

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

        self.assertEqual(result["feed_sent"], 1)
        self.assertNotIn("active_prompt", entry)
        self.assertIn("message_id", entry["last_prompt_bind_error"])
        self.assertIsNotNone(send_feed_item.call_args.kwargs["reply_markup"])

    def test_sync_turn_feed_sends_visible_choice_prompt_readonly_by_default(self) -> None:
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
            VISIBLE_CHOICE_BUTTONS_ENABLED=False,
            VISIBLE_READONLY_PROMPTS_ENABLED=True,
        ):
            result = herdres.sync_once()

        self.assertEqual(result["feed_sent"], 1)
        send_feed_item.assert_called_once()
        sent_item = send_feed_item.call_args.args[1]
        self.assertEqual(sent_item["kind"], "choices")
        self.assertEqual(sent_item["choice_source"], "visible_readonly")
        self.assertIn("Which is it?", sent_item["summary"])
        self.assertIn("Visible-screen prompt only", sent_item["detail"])
        self.assertIsNone(send_feed_item.call_args.kwargs["reply_markup"])
        self.assertEqual(entry["last_clean_kind"], "choices")
        self.assertNotIn("active_prompt", entry)

    def test_sync_turn_feed_does_not_fall_back_to_visible_choice_prompt_by_default(self) -> None:
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
            VISIBLE_CHOICE_BUTTONS_ENABLED=False,
            VISIBLE_READONLY_PROMPTS_ENABLED=True,
        ):
            result = herdres.sync_once()

        self.assertEqual(result["feed_sent"], 1)
        sent_item = send_feed_item.call_args.args[1]
        self.assertEqual(sent_item["kind"], "choices")
        self.assertEqual(sent_item["choice_source"], "visible_readonly")
        self.assertIsNone(send_feed_item.call_args.kwargs["reply_markup"])
        self.assertNotIn("active_prompt", entry)

        send_feed_item.reset_mock()
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
            VISIBLE_CHOICE_BUTTONS_ENABLED=False,
            VISIBLE_READONLY_PROMPTS_ENABLED=True,
        ):
            second_result = herdres.sync_once()

        self.assertEqual(second_result.get("feed_sent", 0), 0)
        send_feed_item.assert_not_called()
        self.assertNotIn("active_prompt", entry)

    def test_visible_readonly_prompts_flag_off_sends_nothing(self) -> None:
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
        raw = """Should I continue?

❯ 1. Continue
  2. Stop
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
            VISIBLE_CHOICE_BUTTONS_ENABLED=False,
            VISIBLE_READONLY_PROMPTS_ENABLED=False,
        ):
            result = herdres.sync_once()

        self.assertEqual(result.get("feed_sent", 0), 0)
        send_feed_item.assert_not_called()
        self.assertNotIn("active_prompt", entry)

    def test_choices_command_has_no_buttons_for_readonly_prompt(self) -> None:
        state = {
            "version": 1,
            "enabled": True,
            "telegram": {"chat_id": "-1001", "general_thread_id": "1", "owner_user_ids": ["42"]},
            "panes": {
                "pane-1": {
                    "pane_id": "pane-1",
                    "topic_id": "77",
                    "last_clean_item": {
                        "kind": "choices",
                        "choice_source": "visible_readonly",
                        "prompt_id": "prompt1",
                        "options": [{"number": "1", "label": "A"}, {"number": "2", "label": "B"}],
                    },
                }
            },
        }

        with patch.multiple(
            herdres,
            load_dotenv=Mock(),
            load_state=Mock(return_value=state),
            save_state=Mock(),
            send_feed_item=Mock(),
        ):
            result = herdres.command_reply(
                {"chat_id": "-1001", "topic_id": "77", "user_id": "42", "text": "/choices"}
            )

        self.assertTrue(result["handled"])
        self.assertEqual(result["reply"], "No active choices for this pane.")

    def test_choices_command_resends_pending_decision_when_enabled(self) -> None:
        entry = {
            "pane_id": "pane-1",
            "topic_id": "77",
            "active_prompt": active_prompt({
                "id": "decision1",
                "text": "How should I proceed?\n1) Build",
                "choice_source": "pending_decision",
                "decision_id": "turn-1:decision-1",
                "item": {
                    "kind": "decision",
                    "choice_source": "pending_decision",
                    "prompt_id": "decision1",
                    "decision_id": "turn-1:decision-1",
                    "summary": "How should I proceed?",
                    "options": [{"number": "1", "callback_id": "build", "label": "Build", "send_text": "1"}],
                },
                "options": [{"number": "1", "callback_id": "build", "label": "Build", "send_text": "1"}],
            }),
        }
        state = {
            "version": 1,
            "enabled": True,
            "telegram": {"chat_id": "-1001", "general_thread_id": "1", "owner_user_ids": ["42"]},
            "panes": {"pane-1": entry},
        }
        send_feed_item = Mock(return_value={"ok": True, "message_id": "1001"})
        pane_turn = Mock(
            return_value={
                "available": True,
                "complete": False,
                "awaiting_input": True,
                "turn_id": "turn-1",
                "pending_decision": {
                    "decision_id": "turn-1:decision-1",
                    "prompt": "How should I proceed now?",
                    "options": [{"id": "build", "label": "Build current path", "send_text": "2"}],
                },
            }
        )

        with patch.multiple(
            herdres,
            load_dotenv=Mock(),
            load_state=Mock(return_value=state),
            save_state=Mock(),
            send_feed_item=send_feed_item,
            pane_turn=pane_turn,
            STRUCTURED_INTERACTIONS_ENABLED=True,
        ):
            result = herdres.command_reply(
                {"chat_id": "-1001", "topic_id": "77", "user_id": "42", "text": "/choices"}
            )

        self.assertTrue(result["handled"])
        self.assertEqual(result["reply"], "")
        send_feed_item.assert_called_once()
        self.assertIsNotNone(send_feed_item.call_args.kwargs["reply_markup"])
        sent_item = send_feed_item.call_args.args[1]
        self.assertEqual(sent_item["summary"], "How should I proceed now?")
        self.assertEqual(sent_item["options"][0]["label"], "Build current path")
        self.assertEqual(entry["active_prompt"]["options"][0]["send_text"], "2")
        self.assertEqual(entry["active_prompt"]["message_id"], "1001")

    def test_choices_command_clears_confirmed_stale_pending_decision(self) -> None:
        entry = {
            "pane_id": "pane-1",
            "topic_id": "77",
            "active_prompt": active_prompt({
                "id": "decision1",
                "text": "How should I proceed?\n1) Build",
                "choice_source": "pending_decision",
                "decision_id": "turn-1:decision-1",
                "item": {
                    "kind": "decision",
                    "choice_source": "pending_decision",
                    "prompt_id": "decision1",
                    "decision_id": "turn-1:decision-1",
                    "summary": "How should I proceed?",
                    "options": [{"number": "1", "callback_id": "build", "label": "Build", "send_text": "1"}],
                },
                "options": [{"number": "1", "callback_id": "build", "label": "Build", "send_text": "1"}],
            }),
            "awaiting_detail": {
                "user_id": "42",
                "prompt_id": "decision1",
                "choice": "1",
                "created_at": herdres.utc_now(),
            },
        }
        state = {
            "version": 1,
            "enabled": True,
            "telegram": {"chat_id": "-1001", "general_thread_id": "1", "owner_user_ids": ["42"]},
            "panes": {"pane-1": entry},
        }
        send_feed_item = Mock(return_value={"ok": True, "message_id": "1001"})
        pane_turn = Mock(
            return_value={
                "available": True,
                "complete": True,
                "turn_id": "turn-2",
                "assistant_final_text": "Already moved on.",
            }
        )

        with patch.multiple(
            herdres,
            load_dotenv=Mock(),
            load_state=Mock(return_value=state),
            save_state=Mock(),
            send_feed_item=send_feed_item,
            pane_turn=pane_turn,
            STRUCTURED_INTERACTIONS_ENABLED=True,
        ):
            result = herdres.command_reply(
                {"chat_id": "-1001", "topic_id": "77", "user_id": "42", "text": "/choices"}
            )

        self.assertTrue(result["handled"])
        self.assertEqual(result["reply"], "No active choices for this pane.")
        send_feed_item.assert_not_called()
        self.assertNotIn("active_prompt", entry)
        self.assertNotIn("awaiting_detail", entry)

    def test_choices_command_keeps_prompt_when_turn_revalidation_unavailable(self) -> None:
        entry = {
            "pane_id": "pane-1",
            "topic_id": "77",
            "active_prompt": active_prompt({
                "id": "decision1",
                "text": "How should I proceed?\n1) Build",
                "choice_source": "pending_decision",
                "decision_id": "turn-1:decision-1",
                "item": {
                    "kind": "decision",
                    "choice_source": "pending_decision",
                    "prompt_id": "decision1",
                    "decision_id": "turn-1:decision-1",
                    "summary": "How should I proceed?",
                    "options": [{"number": "1", "callback_id": "build", "label": "Build", "send_text": "1"}],
                },
                "options": [{"number": "1", "callback_id": "build", "label": "Build", "send_text": "1"}],
            }),
        }
        state = {
            "version": 1,
            "enabled": True,
            "telegram": {"chat_id": "-1001", "general_thread_id": "1", "owner_user_ids": ["42"]},
            "panes": {"pane-1": entry},
        }
        send_feed_item = Mock(return_value={"ok": True, "message_id": "1001"})
        pane_turn = Mock(return_value={"available": False, "reason": "temporary_turn_read_failure"})

        with patch.multiple(
            herdres,
            load_dotenv=Mock(),
            load_state=Mock(return_value=state),
            save_state=Mock(),
            send_feed_item=send_feed_item,
            pane_turn=pane_turn,
            STRUCTURED_INTERACTIONS_ENABLED=True,
        ):
            result = herdres.command_reply(
                {"chat_id": "-1001", "topic_id": "77", "user_id": "42", "text": "/choices"}
            )

        self.assertTrue(result["handled"])
        self.assertEqual(result["reply"], "")
        send_feed_item.assert_called_once()
        sent_item = send_feed_item.call_args.args[1]
        self.assertEqual(sent_item["summary"], "How should I proceed?")
        self.assertIn("active_prompt", entry)
        self.assertEqual(entry["active_prompt"]["message_id"], "1001")

    def test_choices_command_without_message_id_clears_active_prompt(self) -> None:
        entry = {
            "pane_id": "pane-1",
            "topic_id": "77",
            "active_prompt": active_prompt({
                "id": "decision1",
                "text": "How should I proceed?\n1) Build",
                "choice_source": "pending_decision",
                "decision_id": "turn-1:decision-1",
                "item": {
                    "kind": "decision",
                    "choice_source": "pending_decision",
                    "prompt_id": "decision1",
                    "decision_id": "turn-1:decision-1",
                    "summary": "How should I proceed?",
                    "options": [{"number": "1", "callback_id": "build", "label": "Build", "send_text": "1"}],
                },
                "options": [{"number": "1", "callback_id": "build", "label": "Build", "send_text": "1"}],
            }),
        }
        state = {
            "version": 1,
            "enabled": True,
            "telegram": {"chat_id": "-1001", "general_thread_id": "1", "owner_user_ids": ["42"]},
            "panes": {"pane-1": entry},
        }
        send_feed_item = Mock(return_value={"ok": True})

        with patch.multiple(
            herdres,
            load_dotenv=Mock(),
            load_state=Mock(return_value=state),
            save_state=Mock(),
            send_feed_item=send_feed_item,
            STRUCTURED_INTERACTIONS_ENABLED=True,
        ):
            result = herdres.command_reply(
                {"chat_id": "-1001", "topic_id": "77", "user_id": "42", "text": "/choices"}
            )

        self.assertTrue(result["handled"])
        self.assertEqual(result["reply"], "")
        send_feed_item.assert_called_once()
        self.assertNotIn("active_prompt", entry)
        self.assertIn("message_id", entry["last_prompt_bind_error"])

    def test_choices_command_refuses_pending_decision_when_structured_disabled(self) -> None:
        entry = {
            "pane_id": "pane-1",
            "topic_id": "77",
            "active_prompt": {
                "id": "decision1",
                "text": "How should I proceed?\n1) Build",
                "choice_source": "pending_decision",
                "decision_id": "turn-1:decision-1",
                "options": [{"number": "1", "callback_id": "build", "label": "Build", "send_text": "1"}],
            },
        }
        state = {
            "version": 1,
            "enabled": True,
            "telegram": {"chat_id": "-1001", "general_thread_id": "1", "owner_user_ids": ["42"]},
            "panes": {"pane-1": entry},
        }
        send_feed_item = Mock()

        with patch.multiple(
            herdres,
            load_dotenv=Mock(),
            load_state=Mock(return_value=state),
            save_state=Mock(),
            send_feed_item=send_feed_item,
            STRUCTURED_INTERACTIONS_ENABLED=False,
        ):
            result = herdres.command_reply(
                {"chat_id": "-1001", "topic_id": "77", "user_id": "42", "text": "/choices"}
            )

        self.assertTrue(result["handled"])
        self.assertEqual(result["reply"], "No active choices for this pane.")
        send_feed_item.assert_not_called()
        self.assertNotIn("active_prompt", entry)

    def test_sync_turn_feed_visible_choice_prompt_is_opt_in(self) -> None:
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
            VISIBLE_CHOICE_BUTTONS_ENABLED=True,
        ):
            result = herdres.sync_once()

        self.assertEqual(result["feed_sent"], 1)
        sent_item = send_feed_item.call_args.args[1]
        self.assertEqual(sent_item["kind"], "choices")
        self.assertEqual(sent_item["choice_source"], "visible_scrape")
        self.assertEqual(entry["last_clean_kind"], "choices")
        self.assertEqual(entry["active_prompt"]["choice_source"], "visible_scrape")

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
            VISIBLE_CHOICE_BUTTONS_ENABLED=True,
            VISIBLE_READONLY_PROMPTS_ENABLED=True,
        ):
            result = herdres.sync_once()

        self.assertEqual(result["feed_sent"], 1)
        sent_item = send_feed_item.call_args.args[1]
        self.assertEqual(sent_item["kind"], "choices")
        self.assertEqual(sent_item["choice_source"], "visible_scrape")
        self.assertIsNotNone(send_feed_item.call_args.kwargs["reply_markup"])
        self.assertEqual(entry["active_prompt"]["choice_source"], "visible_scrape")

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
        self.assertEqual(entry["last_turn_id"], "turn-1")
        self.assertEqual(entry["last_clean_kind"], "turn")
        self.assertIn("Final answer only.", entry["last_clean_text"])

    def test_report_command_final_turn_clears_prompt_and_pending_detail(self) -> None:
        pane = {
            "pane_id": "pane-1",
            "terminal_id": "term-1",
            "workspace_id": "workspace-1",
            "tab_id": "tab-1",
            "agent": "claude",
            "agent_status": "done",
        }
        entry = {
            "pane_id": "pane-1",
            "topic_id": "77",
            "active_prompt": active_prompt({
                "id": "decision1",
                "choice_source": "pending_decision",
                "options": [{"number": "1", "label": "Build", "send_text": "1"}],
            }),
            "awaiting_detail": {
                "user_id": "42",
                "prompt_id": "decision1",
                "choice": "1",
                "created_at": herdres.utc_now(),
            },
        }
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
        send_feed_item = Mock(return_value={"ok": True, "message_id": "999"})

        with patch.multiple(
            herdres,
            load_dotenv=Mock(),
            load_state=Mock(return_value=state),
            save_state=Mock(),
            pane_by_id=Mock(return_value=pane),
            pane_turn=pane_turn,
            send_feed_item=send_feed_item,
            TURN_FEED_ENABLED=True,
        ):
            result = herdres.command_reply(
                {"chat_id": "-1001", "topic_id": "77", "user_id": "42", "text": "/report"}
            )

        self.assertTrue(result["handled"])
        self.assertEqual(result["reply"], "")
        self.assertNotIn("active_prompt", entry)
        self.assertNotIn("awaiting_detail", entry)

    def test_report_command_turn_feed_unavailable_is_read_only(self) -> None:
        pane = {
            "pane_id": "pane-1",
            "terminal_id": "term-1",
            "workspace_id": "workspace-1",
            "tab_id": "tab-1",
            "agent": "claude",
            "agent_status": "done",
        }
        entry = {
            "pane_id": "pane-1",
            "topic_id": "77",
            "last_turn_available": True,
            "last_turn_reason": "previous",
            "last_turn_id": "turn-old",
        }
        state = {
            "version": 1,
            "telegram": {"chat_id": "-1001", "general_thread_id": "1", "owner_user_ids": ["42"]},
            "panes": {"pane-1": entry},
        }
        pane_turn = Mock(return_value={"available": False, "reason": "no_structured_turn_source"})
        pane_feed_output = Mock(return_value="HERDRES_REPORT_START\nFallback\n- Do not parse this.\nHERDRES_REPORT_END")
        send_feed_item = Mock(return_value={"ok": True})
        save_state = Mock()

        with patch.multiple(
            herdres,
            load_dotenv=Mock(),
            load_state=Mock(return_value=state),
            save_state=save_state,
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
        self.assertTrue(entry["last_turn_available"])
        self.assertEqual(entry["last_turn_reason"], "previous")
        self.assertEqual(entry["last_turn_id"], "turn-old")
        pane_turn.assert_called_once_with("pane-1")
        pane_feed_output.assert_not_called()
        send_feed_item.assert_not_called()
        save_state.assert_not_called()
        self.assertNotIn("last_clean_hash", entry)

    def test_record_delivered_stores_supplied_pre_mutation_hashes(self) -> None:
        # prompt_delivery_state() mutates a choice item's options (adds callback_id)
        # before delivery, changing its hash. record_delivered_feed_item must store
        # the PRE-mutation hashes the caller computed, NOT recompute from the mutated
        # item — otherwise the next sync cycle (which hashes a fresh, unmutated item)
        # sees the prompt as "changed" and re-sends it every cycle.
        mutated_item = {
            "kind": "choices",
            "turn_id": "turn-1",
            "summary": "Choose:",
            "options": [{"number": 1, "label": "A", "callback_id": "post-mutation"}],
        }
        pre_render = "PRE_RENDER_HASH"
        pre_semantic = "PRE_SEMANTIC_HASH"
        entry: dict = {}
        herdres.record_delivered_feed_item(
            entry,
            mutated_item,
            {"ok": True, "message_id": "9"},
            pending_active_prompt=None,
            clear_active_prompt=False,
            item_render_hash=pre_render,
            item_semantic_hash=pre_semantic,
        )
        # stored the supplied (pre-mutation) hashes verbatim...
        self.assertEqual(entry["last_clean_render_hash"], pre_render)
        self.assertEqual(entry["last_clean_hash"], pre_render)
        self.assertEqual(entry["last_clean_semantic_hash"], pre_semantic)
        # ...and did NOT recompute from the mutated item (which would regress dedup)
        self.assertNotEqual(entry["last_clean_render_hash"], herdres.clean_feed_hash(mutated_item))

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

    def test_load_state_normalizes_backup_state(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "state.json"
            path.write_text("{broken", encoding="utf-8")
            path.with_suffix(path.suffix + ".bak").write_text(
                herdres.json.dumps({"version": 1}),
                encoding="utf-8",
            )

            with patch.dict(herdres.os.environ, {"HERDR_TELEGRAM_TOPICS_STATE": str(path)}, clear=False):
                state = herdres.load_state()

        self.assertTrue(state["enabled"])
        self.assertTrue(state["plugin_event_enabled"])
        self.assertEqual(state["telegram"], {})
        self.assertEqual(state["panes"], {})

    def test_telegram_api_invalid_success_json_raises_bridge_error(self) -> None:
        class FakeResponse:
            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

            def read(self):
                return b"{broken"

        with patch.dict(
            herdres.os.environ,
            {"TELEGRAM_BOT_TOKEN": "token", "HERDR_TELEGRAM_TOPICS_DRY_RUN": ""},
            clear=False,
        ), patch.object(herdres.urllib.request, "urlopen", Mock(return_value=FakeResponse())):
            with self.assertRaises(herdres.BridgeError) as ctx:
                herdres.telegram_api("sendMessage", {"chat_id": "1", "text": "hello"})

        self.assertIn("invalid JSON", str(ctx.exception))

    def test_bridge_defaults_match_canonical_herdres_paths(self) -> None:
        import herdr_topic_bridge as bridge

        self.assertEqual(bridge.DEFAULT_STATE, Path.home() / ".local/share/herdres/state.json")
        self.assertEqual(bridge.DEFAULT_SCRIPT, Path.home() / ".local/bin/herdres")

    def test_plugin_manifest_hooks_herdres_event(self) -> None:
        try:
            import tomllib
        except ModuleNotFoundError:
            import tomli as tomllib

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

    def test_event_for_deleted_pane_refreshes_space_status_without_full_turn_sync(self) -> None:
        claude_key = "pane-claude:old"
        state = {
            "version": 1,
            "enabled": True,
            "plugin_event_enabled": True,
            "telegram": {"chat_id": "-1001", "general_thread_id": "1", "owner_user_ids": ["42"]},
            "spaces": {
                "workspace:workspace-1": {
                    "space_key": "workspace:workspace-1",
                    "topic_id": "77",
                    "pane_keys": [claude_key],
                    "pinned_status_message_id": "501",
                    "pinned_status_text": "Claude 🟢",
                    "pinned_status_hash": "old",
                    "pinned_status_pinned_at": herdres.utc_now(),
                }
            },
            "panes": {
                claude_key: {
                    "pane_key": claude_key,
                    "pane_id": "pane-claude",
                    "space_key": "workspace:workspace-1",
                    "topic_id": "77",
                    "agent": "claude",
                    "last_known_status": "idle",
                },
            },
        }
        event_json = herdres.json.dumps({"pane_id": "pane-claude"})
        edit_message_text = Mock(return_value={"ok": True, "message_id": "501"})
        sync_pane_once = Mock(side_effect=AssertionError("deleted-pane event should not run full pane sync"))

        with patch.dict(herdres.os.environ, {"HERDR_PLUGIN_EVENT_JSON": event_json}, clear=False), patch.multiple(
            herdres,
            load_dotenv=Mock(),
            load_state=Mock(return_value=state),
            save_state=Mock(),
            pane_by_id=Mock(return_value=None),
            pane_list=Mock(return_value=[]),
            preflight_for_event=Mock(return_value=(True, "")),
            send_notice=Mock(return_value={"ok": True, "message_id": "900"}),
            edit_message_text=edit_message_text,
            sync_pane_once=sync_pane_once,
            PINNED_STATUS_ENABLED=True,
        ):
            result = herdres.event_once()

        self.assertTrue(result["ok"])
        self.assertTrue(result["changed"])
        self.assertEqual(result["pinned_status_updated"], 1)
        self.assertEqual(state["spaces"]["workspace:workspace-1"]["pane_keys"], [])
        edit_message_text.assert_called_once_with("-1001", "501", "No active panes.")
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

    def test_event_propagates_rate_limited(self) -> None:
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
        save_state = Mock()

        with patch.dict(herdres.os.environ, {"HERDR_PLUGIN_EVENT_JSON": event_json}, clear=False), patch.multiple(
            herdres,
            load_dotenv=Mock(),
            load_state=Mock(return_value=state),
            save_state=save_state,
            pane_by_id=Mock(return_value=pane),
            preflight_for_event=Mock(return_value=(True, "")),
            sync_pane_once=Mock(side_effect=herdres.RateLimited(3)),
        ):
            with self.assertRaises(herdres.RateLimited):
                herdres.event_once()

        save_state.assert_not_called()

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


def active_prompt(prompt: dict, *, message_id: str = "555") -> dict:
    prompt.setdefault("message_id", message_id)
    prompt.setdefault("created_at", herdres.utc_now())
    return prompt


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
                "active_prompt": active_prompt({
                    "id": "prompt1",
                    "text": "Question\nRun sync now?\n\n1) Run sync now\n4) Other with details",
                    "choice_source": "explicit_block",
                    "options": [
                        {"number": "1", "label": "Run sync now"},
                        {"number": "4", "label": "Other with details"},
                    ],
                }),
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


class CodeDetectionTests(unittest.TestCase):
    def test_statistical_notation_is_not_code(self) -> None:
        html = herdres.render_final_reply_html(
            "Results: N=16, OFF f=2 w=15 -> ON f=2 w=21, p=0.05."
        )
        for token in ("N=16", "f=2", "w=15", "w=21", "p=0.05"):
            self.assertNotIn(f"<code>{token}</code>", html)
            self.assertIn(token, html)  # still present, just plain text

    def test_env_style_assignment_still_code(self) -> None:
        html = herdres.render_final_reply_html("Set DEBUG=true to enable.")
        self.assertIn("<code>DEBUG=true</code>", html)

    def test_env_style_assignment_excludes_trailing_sentence_punctuation(self) -> None:
        html = herdres.render_final_reply_html("Set DEBUG=true.")
        self.assertIn("<code>DEBUG=true</code>.", html)
        self.assertNotIn("<code>DEBUG=true.</code>", html)

    def test_statistical_notation_with_trailing_punctuation_stays_plain(self) -> None:
        html = herdres.render_final_reply_html("Results stayed at N=16; f=2.")
        self.assertIn("N=16; f=2.", html)
        self.assertNotIn("<code>N=16</code>", html)
        self.assertNotIn("<code>f=2</code>", html)

    def test_env_assignment_excludes_trailing_closing_delimiter(self) -> None:
        html = herdres.render_final_reply_html("Set DEBUG=true) and continue.")
        self.assertIn("<code>DEBUG=true</code>)", html)
        self.assertNotIn("<code>DEBUG=true)</code>", html)


class HtmlToPlainTests(unittest.TestCase):
    def test_table_cells_are_separated_in_plaintext_fallback(self) -> None:
        text = herdres.html_to_plain("<table><tr><td>File</td><td>Status</td></tr></table>")
        self.assertEqual(text, "File | Status")
        self.assertNotIn("FileStatus", text)

    def test_table_header_cells_are_separated_in_plaintext_fallback(self) -> None:
        text = herdres.html_to_plain("<table><tr><th>Name</th><th>Result</th></tr></table>")
        self.assertEqual(text, "Name | Result")


class RichMessageSplitTests(unittest.TestCase):
    def _balanced(self, s: str) -> bool:
        import re as _re
        for tag in ("p", "table", "ul", "ol", "blockquote", "pre", "h3", "li", "tr", "td", "th", "b"):
            if len(_re.findall(rf"<{tag}\b", s)) != len(_re.findall(rf"</{tag}>", s)):
                return False
        return True

    def test_short_html_stays_one_chunk(self) -> None:
        html = "<p>hello</p><ul>\n<li>a</li>\n<li>b</li>\n</ul>"
        self.assertEqual(herdres.split_rich_html(html, 6000), [html])

    def test_long_html_splits_at_block_boundaries(self) -> None:
        html = "".join(f"<p>paragraph number {i} with some filler text here</p>" for i in range(200))
        chunks = herdres.split_rich_html(html, 1000)
        self.assertGreater(len(chunks), 1)
        self.assertTrue(all(len(c) <= 1000 for c in chunks))
        self.assertTrue(all(self._balanced(c) for c in chunks))      # no half-cut tags
        self.assertEqual("".join(chunks), html)                       # nothing lost

    def test_oversize_single_block_is_hard_split_and_rewrapped(self) -> None:
        rows = "".join(f"<tr><td>row {i}</td><td>value {i}</td></tr>" for i in range(300))
        html = "<table bordered striped>" + rows + "</table>"
        chunks = herdres.split_rich_html(html, 1500)
        self.assertGreater(len(chunks), 1)
        self.assertTrue(all(len(c) <= 1500 for c in chunks))
        # every piece is a self-contained <table>...</table>, not a half-open tag
        for c in chunks:
            self.assertTrue(c.startswith("<table"))
            self.assertTrue(c.endswith("</table>"))
            self.assertTrue(self._balanced(c))

    def test_hard_split_keeps_row_closing_delimiter_with_row(self) -> None:
        row = "<tr><td>" + ("x" * 186) + "</td></tr>"
        html = "<table>" + row + row + "</table>"
        chunks = herdres.split_rich_html(html, 100)
        self.assertGreater(len(chunks), 1)
        for c in chunks:
            self.assertTrue(self._balanced(c))

    def test_hard_split_newline_inside_row_stays_balanced(self) -> None:
        # A "\n" inside an open <tr> must NOT be treated as a split boundary for a
        # table (only </tr> is) — otherwise a chunk could end mid-row, unbalanced.
        row = "<tr><td>" + ("a" * 90) + "\n" + ("b" * 90) + "</td></tr>"
        html = "<table>" + row + row + "</table>"
        chunks = herdres.split_rich_html(html, 120)
        self.assertGreater(len(chunks), 1)
        for c in chunks:
            self.assertTrue(self._balanced(c))
        self.assertEqual("".join(chunks).count("<tr>"), "".join(chunks).count("</tr>"))

    def test_send_rich_message_splits_oversize_into_multiple_sends(self) -> None:
        html = "".join(f"<p>paragraph number {i} with filler</p>" for i in range(400))
        calls = []

        def fake_api(method, payload):
            calls.append((method, payload))
            return {"ok": True, "result": {"message_id": 100 + len(calls)}}

        tg = {"rich_messages": {"supported": "yes"}}
        with patch.object(herdres, "telegram_api", side_effect=fake_api):
            result = herdres.send_rich_message("-100", html, telegram=tg, thread_id="5",
                                               reply_markup={"inline_keyboard": [[{"text": "x", "callback_data": "y"}]]})
        rich_calls = [p for m, p in calls if m == "sendRichMessage"]
        self.assertGreater(len(rich_calls), 1)                        # split into >1 send
        for p in rich_calls:
            self.assertLessEqual(len(p["rich_message"]), herdres.RICH_SAFE_CHARS + 200)
        # buttons only on the final chunk
        self.assertNotIn("reply_markup", rich_calls[0])
        self.assertIn("reply_markup", rich_calls[-1])
        self.assertEqual(result["message_id"], "101")                 # anchor = first message

    def test_send_rich_message_single_when_small(self) -> None:
        calls = []
        with patch.object(herdres, "telegram_api", side_effect=lambda m, p: calls.append((m, p)) or {"ok": True, "result": {"message_id": 7}}):
            herdres.send_rich_message("-100", "<p>tiny</p>", telegram={"rich_messages": {"supported": "yes"}})
        self.assertEqual(len([m for m, _ in calls if m == "sendRichMessage"]), 1)

    def test_send_rich_message_uses_legacy_when_telegram_is_none(self) -> None:
        calls = []
        with patch.object(herdres, "telegram_api", side_effect=lambda m, p: calls.append((m, p)) or {"ok": True, "result": {"message_id": 7}}):
            result = herdres.send_rich_message("-100", "<p><b>tiny</b></p>", telegram=None)
        self.assertEqual([m for m, _ in calls], ["sendMessage"])
        self.assertEqual(result["format"], "legacy")
        self.assertEqual(calls[0][1]["text"], "tiny")

    def test_send_rich_message_uses_legacy_when_support_unknown(self) -> None:
        calls = []
        tg = {}
        with patch.object(herdres, "telegram_api", side_effect=lambda m, p: calls.append((m, p)) or {"ok": True, "result": {"message_id": 8}}):
            result = herdres.send_rich_message("-100", "<p>tiny</p>", telegram=tg)
        self.assertEqual([m for m, _ in calls], ["sendMessage"])
        self.assertEqual(result["format"], "legacy")
        self.assertEqual(tg["rich_messages"]["supported"], "unknown")

    def test_send_rich_message_uses_rich_when_supported(self) -> None:
        calls = []
        tg = {"rich_messages": {"supported": "yes"}}
        with patch.object(herdres, "telegram_api", side_effect=lambda m, p: calls.append((m, p)) or {"ok": True, "result": {"message_id": 9}}):
            result = herdres.send_rich_message("-100", "<p>tiny</p>", telegram=tg)
        self.assertEqual([m for m, _ in calls], ["sendRichMessage"])
        self.assertEqual(result["format"], "rich")

    def test_edit_message_text_returns_legacy_format(self) -> None:
        with patch.object(herdres, "telegram_api", Mock(return_value={"ok": True, "result": {"message_id": 10}})):
            result = herdres.edit_message_text("-100", "10", "updated")
        self.assertTrue(result["ok"])
        self.assertEqual(result["format"], "legacy")

    def test_update_live_card_uses_edit_result_format(self) -> None:
        item = herdres.make_feed_item("report", "Report", "Fixed the issue.", notify=False)
        entry = {"card_message_id": "10"}
        with patch.object(
            herdres,
            "edit_rich_message",
            Mock(return_value={"ok": True, "format": "legacy", "kind": "edited", "message_id": "10"}),
        ):
            result = herdres.update_live_card("-100", entry, item, telegram={"rich_messages": {"supported": "yes"}})
        self.assertTrue(result["ok"])
        self.assertEqual(result["format"], "legacy")
        self.assertEqual(entry["card_format"], "legacy")

    def test_send_and_edit_feed_item_pass_plain_fallback_text(self) -> None:
        item = herdres.make_feed_item("report", "Report", "First line\nSecond line", notify=False)
        send_rich_message = Mock(return_value={"ok": True, "format": "legacy", "message_id": "11"})
        edit_rich_message = Mock(return_value={"ok": True, "format": "legacy", "kind": "edited"})
        with patch.object(herdres, "send_rich_message", send_rich_message):
            herdres.send_feed_item("-100", item, telegram={}, thread_id="77")
        with patch.object(herdres, "edit_rich_message", edit_rich_message):
            herdres.edit_feed_item("-100", "11", item, telegram={})
        self.assertEqual(send_rich_message.call_args.kwargs["fallback_text"], herdres.item_plain_text(item))
        self.assertEqual(edit_rich_message.call_args.kwargs["fallback_text"], herdres.item_plain_text(item))

    def test_note_rich_bad_request_resets_corrupt_streak(self) -> None:
        telegram = {"rich_messages": {"supported": "yes", "bad_request_streak": "bad"}}
        herdres.note_rich_bad_request(telegram, "bad html")
        self.assertEqual(telegram["rich_messages"]["bad_request_streak"], 1)
        self.assertEqual(telegram["rich_messages"]["supported"], "yes")

    def test_send_rich_message_reports_later_chunk_failure(self) -> None:
        html = "".join(f"<p>paragraph number {i} with filler</p>" for i in range(400))
        calls = []

        def fake_chunk(chat_id, chunk, **kwargs):
            calls.append(chunk)
            if len(calls) == 1:
                return {"ok": True, "format": "rich", "message_id": "100"}
            return {"ok": False, "format": "rich", "kind": "transient", "transient": True, "error": "boom"}

        with patch.object(herdres, "_send_rich_chunk", side_effect=fake_chunk):
            result = herdres.send_rich_message("-100", html, telegram={"rich_messages": {"supported": "yes"}})
        self.assertFalse(result["ok"])
        self.assertTrue(result["partial_sent"])
        self.assertEqual(result["failed_chunk_index"], 1)
        self.assertEqual(result["message_id"], "100")
        self.assertEqual(len(calls), 2)

    def test_send_rich_message_stops_rich_after_capability_fallback(self) -> None:
        html = "".join(f"<p>paragraph number {i} with filler</p>" for i in range(400))
        calls = []
        tg = {"rich_messages": {"supported": "yes"}}

        def fake_api(method, payload):
            calls.append((method, payload))
            if method == "sendRichMessage":
                raise herdres.BridgeError("Telegram sendRichMessage failed: method not found")
            return {"ok": True, "result": {"message_id": len(calls)}}

        with patch.object(herdres, "telegram_api", side_effect=fake_api):
            result = herdres.send_rich_message("-100", html, telegram=tg)
        self.assertTrue(result["ok"])
        self.assertEqual(len([m for m, _ in calls if m == "sendRichMessage"]), 1)
        self.assertGreater(len([m for m, _ in calls if m == "sendMessage"]), 1)
        self.assertEqual(tg["rich_messages"]["supported"], "no")

    def test_edit_rich_message_uses_plain_edit_payload(self) -> None:
        calls = []
        tg = {"rich_messages": {"supported": "yes"}}
        with patch.object(herdres, "telegram_api", side_effect=lambda m, p: calls.append((m, p)) or {"ok": True, "result": {"message_id": 10}}):
            result = herdres.edit_rich_message("-100", "10", "<p><b>updated</b></p>", telegram=tg)
        self.assertTrue(result["ok"])
        self.assertEqual(result["format"], "legacy")
        self.assertEqual([m for m, _ in calls], ["editMessageText"])
        self.assertEqual(calls[0][1]["text"], "updated")
        self.assertNotIn("rich_message", calls[0][1])


class CodexFenceAndAcronymTests(unittest.TestCase):
    def test_text_fence_numbered_list_is_not_code_block(self) -> None:
        html = herdres.render_final_reply_html("Steps:\n\n```text\n1. Backup files.\n2. Restart service.\n3. Verify.\n```")
        self.assertNotIn("<pre>", html)
        self.assertIn("1. Backup files.", html)

    def test_text_fence_prose_is_not_code_block(self) -> None:
        html = herdres.render_final_reply_html("```text\nThis is an explanatory paragraph about the plan and what it does.\n```")
        self.assertNotIn("<pre>", html)

    def test_text_fence_aligned_table_stays_monospace(self) -> None:
        html = herdres.render_final_reply_html("```text\nAsia key:     6M0JL4hlOhIAY=\nRegistry key: Vhqghgj95WH0xHI=\n```")
        self.assertIn("<pre>", html)

    def test_text_fence_command_syntax_stays_monospace(self) -> None:
        html = herdres.render_final_reply_html("```text\nclaude --session-id <uuid> --settings <file>\n    --profile prod\n```")
        self.assertIn("<pre>", html)

    def test_real_language_fence_stays_code(self) -> None:
        html = herdres.render_final_reply_html("```bash\nsystemctl --user restart herdres\n```")
        self.assertIn('<pre><code class="language-bash">', html)

    def test_plain_acronyms_not_monospaced(self) -> None:
        html = herdres.render_final_reply_html("We use UDP and TURN over the VPS for the FAQ.")
        for a in ("UDP", "TURN", "VPS", "FAQ"):
            self.assertNotIn("<code>%s</code>" % a, html)

    def test_constant_with_underscore_or_digit_stays_code(self) -> None:
        html = herdres.render_final_reply_html("Set MAX_CHARS and check HTTP2 support.")
        self.assertIn("<code>MAX_CHARS</code>", html)
        self.assertIn("<code>HTTP2</code>", html)

    def test_blocks_separated_by_br(self) -> None:
        html = herdres.render_final_reply_html("First paragraph here.\n\nSecond paragraph here.")
        self.assertIn("<br>", html)


class InlineSpanAndSpacingTests(unittest.TestCase):
    def test_bold_spans_inline_code(self) -> None:
        html = herdres.render_final_reply_html("Use **bold `with code` inside** here.")
        self.assertIn("<b>bold <code>with code</code> inside</b>", html)
        self.assertNotIn("**", html)

    def test_italic_spans_inline_code(self) -> None:
        html = herdres.render_final_reply_html("This *is `code` italic* ok.")
        self.assertIn("<i>is <code>code</code> italic</i>", html)

    def test_text_fence_list_blank_line_before_next_block(self) -> None:
        html = herdres.render_final_reply_html("```text\n1. one\n2. two\n```\n\nNext paragraph here.")
        self.assertIn("<br><br>", html)

    def test_no_extra_break_after_code_block(self) -> None:
        html = herdres.render_final_reply_html("```python\nx = 1\n```\n\nAfter the code block.")
        self.assertIn("</pre>", html)
        self.assertNotIn("</pre><br>", html)

    def test_paragraphs_separated_by_one_break(self) -> None:
        html = herdres.render_final_reply_html(
            "This first paragraph has plenty of words to avoid heading promotion.\n\n"
            "And the second paragraph also has plenty of words right here."
        )
        self.assertIn("</p><br><p>", html)


class AttachmentTests(unittest.TestCase):
    def _attachment(self, **over):
        a = {"kind": "document", "file_id": "FILEID", "file_name": "report.pdf",
             "mime_type": "application/pdf", "file_size": 11}
        a.update(over)
        return a

    def _payload(self, **over):
        p = {"chat_id": "-1001", "topic_id": "77", "user_id": "42", "message_id": "9",
             "attachment": self._attachment(), "caption": "please review", "text": ""}
        p.update(over)
        return p

    # --- pure helpers --------------------------------------------------------

    def test_attachment_dest_path_sanitizes_traversal(self) -> None:
        with tempfile.TemporaryDirectory() as tmp, patch.object(
            herdres, "state_path", Mock(return_value=Path(tmp) / "state.json")
        ):
            safe_dir = herdres.attachment_dest_dir("pane-1")
            for evil in ["../../.ssh/authorized_keys", "/etc/cron.d/x", "..", "\x00evil", "a/b/c.txt"]:
                path = herdres.attachment_dest_path("pane-1", self._attachment(file_name=evil))
                self.assertEqual(path.parent, safe_dir)
                self.assertNotIn("/", path.name)
                self.assertNotIn("..", path.name)
                self.assertFalse(path.name.startswith("."))

    def test_attachment_dest_path_unique_per_call(self) -> None:
        # Same file_id twice must not collide (else a failed retry's cleanup could
        # unlink the earlier delivered file).
        with tempfile.TemporaryDirectory() as tmp, patch.object(
            herdres, "state_path", Mock(return_value=Path(tmp) / "state.json")
        ):
            att = self._attachment()
            p1 = herdres.attachment_dest_path("pane-1", att)
            p2 = herdres.attachment_dest_path("pane-1", att)
            self.assertNotEqual(p1, p2)

    def test_attachment_dest_dir_is_private(self) -> None:
        with tempfile.TemporaryDirectory() as tmp, patch.object(
            herdres, "state_path", Mock(return_value=Path(tmp) / "state.json")
        ):
            d = herdres.attachment_dest_dir("pane-1")
            self.assertTrue(d.is_dir())
            self.assertEqual(d.stat().st_mode & 0o777, 0o700)

    def test_download_dry_run_writes_placeholder_0600(self) -> None:
        with tempfile.TemporaryDirectory() as tmp, patch.dict(
            herdres.os.environ, {"HERDR_TELEGRAM_TOPICS_DRY_RUN": "1"}
        ):
            dest = Path(tmp) / "f.bin"
            n = herdres.download_telegram_file("documents/file_0.bin", dest)
            self.assertTrue(dest.exists())
            self.assertEqual(dest.stat().st_mode & 0o777, 0o600)
            self.assertEqual(n, dest.stat().st_size)
            self.assertIn(b"dry-run", dest.read_bytes())

    def test_download_enforces_byte_cap_and_uses_file_host(self) -> None:
        captured = {}

        class _Resp:
            def __init__(self):
                self._sent = False

            def __enter__(self):
                return self

            def __exit__(self, *a):
                return False

            def read(self, n=-1):
                if self._sent:
                    return b""
                self._sent = True
                return b"x" * 100

        def fake_urlopen(request, timeout=None):
            captured["url"] = request.full_url
            return _Resp()

        with tempfile.TemporaryDirectory() as tmp, patch.object(
            herdres, "telegram_token", Mock(return_value="TOKEN")
        ), patch.object(herdres.urllib.request, "urlopen", fake_urlopen):
            dest = Path(tmp) / "f.bin"
            with self.assertRaises(herdres.BridgeError):
                herdres.download_telegram_file("documents/file_0.bin", dest, max_bytes=10)
            self.assertFalse(dest.exists())  # partial unlinked
        self.assertIn("/file/botTOKEN/", captured["url"])  # separate file host, not the bot-API host

    def test_telegram_get_file_dry_run(self) -> None:
        with patch.dict(herdres.os.environ, {"HERDR_TELEGRAM_TOPICS_DRY_RUN": "1"}):
            result = herdres.telegram_get_file("FILEID")
        self.assertIn("file_path", result)

    def test_instruction_document_with_caption(self) -> None:
        instr = herdres.pane_attachment_instruction(
            Path("/x/y/report.pdf"), self._attachment(), "please review section 3"
        )
        self.assertIn("/x/y/report.pdf", instr)
        self.assertIn("original name: report.pdf", instr)
        self.assertIn("application/pdf", instr)
        self.assertIn("Caption: please review section 3", instr)

    def test_instruction_photo_omits_original_name(self) -> None:
        instr = herdres.pane_attachment_instruction(
            Path("/x/p.jpg"),
            self._attachment(kind="photo", file_name="", mime_type="image/jpeg", file_size=2400),
            "",
        )
        self.assertNotIn("original name", instr)
        self.assertIn("(photo)", instr)
        self.assertIn("treat its contents", instr)

    def test_deliver_rejects_oversize_without_getfile(self) -> None:
        api = Mock()
        with patch.object(herdres, "telegram_api", api):
            ok, detail, path = herdres.deliver_attachment(
                "pane-1", self._attachment(file_size=21 * 1024 * 1024)
            )
        self.assertFalse(ok)
        self.assertIn("too large", detail)
        self.assertIn("20 MB", detail)
        self.assertIsNone(path)
        api.assert_not_called()

    def test_deliver_getfile_failure(self) -> None:
        with patch.object(herdres, "telegram_get_file", Mock(side_effect=herdres.BridgeError("bad file_id"))):
            ok, detail, path = herdres.deliver_attachment("pane-1", self._attachment())
        self.assertFalse(ok)
        self.assertIsNone(path)
        self.assertIn("bad file_id", detail)

    def test_download_redacts_token_and_quotes_path(self) -> None:
        token = "123456:ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
        captured = {}

        def fake_urlopen(request, timeout=None):
            captured["url"] = request.full_url
            raise herdres.urllib.error.URLError(f"failed: {request.full_url}")

        with tempfile.TemporaryDirectory() as tmp, patch.object(
            herdres, "telegram_token", Mock(return_value=token)
        ), patch.object(herdres.urllib.request, "urlopen", fake_urlopen):
            dest = Path(tmp) / "f.bin"
            with self.assertRaises(herdres.BridgeError) as ctx:
                herdres.download_telegram_file("docs/a b.bin", dest)
            self.assertNotIn(token, str(ctx.exception))
            self.assertNotIn("ABCDEFGHIJKLMNOP", str(ctx.exception))
            self.assertFalse(dest.exists())
            self.assertFalse((Path(tmp) / "f.bin.part").exists())  # partial cleaned up
        self.assertIn(f"/file/bot{token}/", captured["url"])
        self.assertIn("docs/a%20b.bin", captured["url"])  # path is URL-quoted

    def test_download_429_raises_ratelimited(self) -> None:
        def fake_urlopen(request, timeout=None):
            raise herdres.urllib.error.HTTPError(
                request.full_url, 429, "Too Many Requests", {"Retry-After": "7"}, None
            )

        with tempfile.TemporaryDirectory() as tmp, patch.object(
            herdres, "telegram_token", Mock(return_value="T")
        ), patch.object(herdres.urllib.request, "urlopen", fake_urlopen):
            dest = Path(tmp) / "f.bin"
            with self.assertRaises(herdres.RateLimited) as ctx:
                herdres.download_telegram_file("docs/x.bin", dest)
            self.assertEqual(ctx.exception.retry_after, 7)
            self.assertFalse(dest.exists())

    def test_attachment_dest_dir_rejects_symlink_ancestor(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp) / "share"
            base.mkdir()
            target = Path(tmp) / "elsewhere"
            target.mkdir()
            (base / "attachments").symlink_to(target)
            with patch.object(herdres, "state_path", Mock(return_value=base / "state.json")):
                with self.assertRaises(herdres.BridgeError):
                    herdres.attachment_dest_dir("pane-1")

    def test_prune_keeps_recent_and_drops_parts(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            for i in range(5):
                p = root / f"f{i}.bin"
                p.write_bytes(b"x")
                herdres.os.utime(p, (i, i))
            (root / "stale.part").write_bytes(b"p")
            herdres.prune_attachment_dir(root, keep=2)
            self.assertEqual(sorted(p.name for p in root.iterdir()), ["f3.bin", "f4.bin"])

    def test_deliver_rejects_size_mismatch(self) -> None:
        # confirmed size says 5000 but the wire delivers fewer bytes -> reject.
        def fake_urlopen(request, timeout=None):
            class _R:
                def __init__(self_inner):
                    self_inner._sent = False

                def __enter__(self_inner):
                    return self_inner

                def __exit__(self_inner, *a):
                    return False

                def read(self_inner, n=-1):
                    if self_inner._sent:
                        return b""
                    self_inner._sent = True
                    return b"short"
            return _R()

        with tempfile.TemporaryDirectory() as tmp, patch.object(
            herdres, "state_path", Mock(return_value=Path(tmp) / "state.json")
        ), patch.object(
            herdres, "telegram_get_file", Mock(return_value={"file_path": "docs/x.bin", "file_size": 5000})
        ), patch.object(herdres, "telegram_token", Mock(return_value="T")), patch.object(
            herdres.urllib.request, "urlopen", fake_urlopen
        ):
            ok, detail, path = herdres.deliver_attachment("pane-1", self._attachment(file_size=0))
            leftover = list((Path(tmp) / "attachments" / "pane-1").glob("*"))
        self.assertFalse(ok)
        self.assertIn("incomplete", detail)
        self.assertIsNone(path)
        self.assertEqual(leftover, [])  # mismatched file unlinked

    # --- command_reply integration -------------------------------------------

    def test_command_reply_delivers_document(self) -> None:
        state = callback_state()
        state["panes"]["pane-1"].pop("active_prompt", None)
        sent = Mock(return_value=(True, ""))
        with tempfile.TemporaryDirectory() as tmp, patch.dict(
            herdres.os.environ, {"HERDR_TELEGRAM_TOPICS_DRY_RUN": "1"}
        ), patch.multiple(
            herdres,
            load_dotenv=Mock(),
            load_state=Mock(return_value=state),
            save_state=Mock(),
            state_path=Mock(return_value=Path(tmp) / "state.json"),
            send_to_pane=sent,
        ):
            result = herdres.command_reply(self._payload())
            # Glob inside the with-block: the temp dir is removed on exit.
            files = list((Path(tmp) / "attachments" / "pane-1").glob("*"))
            file_mode = files[0].stat().st_mode & 0o777 if files else None
        self.assertEqual(result["reply"], "Sent attachment to this pane.")
        sent.assert_called_once()
        instruction = sent.call_args.args[1]
        self.assertIn("saved at", instruction)
        self.assertIn("please review", instruction)
        self.assertEqual(len(files), 1)
        self.assertEqual(file_mode, 0o600)

    def test_command_reply_forwards_unknown_slash_command_to_pane(self) -> None:
        # /goal (and any non-herdres command) must reach the pane's agent, not be
        # rejected as "unknown".
        state = callback_state()
        state["panes"]["pane-1"].pop("active_prompt", None)
        sent = Mock(return_value=(True, ""))
        with patch.multiple(
            herdres,
            load_dotenv=Mock(),
            load_state=Mock(return_value=state),
            save_state=Mock(),
            send_to_pane=sent,
        ):
            result = herdres.command_reply(
                {"chat_id": "-1001", "topic_id": "77", "user_id": "42", "text": "/goal pursue the refactor"}
        )
        sent.assert_called_once_with("pane-1", "/goal pursue the refactor")
        self.assertEqual(result["reply"], "")

    def test_parse_command_splits_on_any_whitespace(self) -> None:
        self.assertEqual(herdres.parse_command("/send\nmulti\nline"), ("send", "multi\nline"))
        self.assertEqual(herdres.parse_command("/goal\ndo the thing"), ("goal", "do the thing"))
        self.assertEqual(herdres.parse_command("/raw 50"), ("raw", "50"))      # space case unchanged
        self.assertEqual(herdres.parse_command("/goal@Bot do it"), ("goal", "do it"))
        self.assertEqual(herdres.parse_command("hello there"), ("plain", "hello there"))

    def test_command_reply_long_command_keeps_command_and_files_argument(self) -> None:
        # A long /goal must stay a "/goal …" command (not become a file-read
        # instruction) and must not be truncated; the bulk goes to a file.
        state = callback_state()
        state["panes"]["pane-1"].pop("active_prompt", None)
        sent = Mock(return_value=(True, ""))
        long_arg = "pursue this detailed objective. " * 80  # > 1200 chars
        with tempfile.TemporaryDirectory() as tmp, patch.multiple(
            herdres,
            load_dotenv=Mock(),
            load_state=Mock(return_value=state),
            save_state=Mock(),
            state_path=Mock(return_value=Path(tmp) / "state.json"),
            send_to_pane=sent,
        ):
            herdres.command_reply(
                {"chat_id": "-1001", "topic_id": "77", "user_id": "42", "text": "/goal " + long_arg}
            )
            files = list((Path(tmp) / "inbound" / "pane-1").glob("*"))
            staged_text = files[0].read_text(encoding="utf-8") if files else ""
        sent.assert_called_once()
        forwarded = sent.call_args.args[1]
        self.assertTrue(forwarded.startswith("/goal "))           # command preserved
        self.assertIn("read that", forwarded.lower())
        self.assertNotIn("truncated", forwarded.lower())          # not the old cut-off instruction
        self.assertLess(len(forwarded), 600)                      # short line, not a paste blob
        self.assertEqual(len(files), 1)
        self.assertIn("pursue this detailed objective", staged_text)  # full goal staged to the file

    def test_command_reply_forwarded_command_strips_botname(self) -> None:
        state = callback_state()
        state["panes"]["pane-1"].pop("active_prompt", None)
        sent = Mock(return_value=(True, ""))
        with patch.multiple(
            herdres,
            load_dotenv=Mock(),
            load_state=Mock(return_value=state),
            save_state=Mock(),
            send_to_pane=sent,
        ):
            herdres.command_reply(
                {"chat_id": "-1001", "topic_id": "77", "user_id": "42", "text": "/goal@HermesBot do it"}
            )
        sent.assert_called_once_with("pane-1", "/goal do it")

    def test_command_reply_attachment_owner_gate(self) -> None:
        state = callback_state()
        deliver = Mock()
        with patch.multiple(
            herdres,
            load_dotenv=Mock(),
            load_state=Mock(return_value=state),
            save_state=Mock(),
            deliver_attachment=deliver,
        ):
            result = herdres.command_reply(self._payload(user_id="99"))
        self.assertEqual(result["reply"], "")
        deliver.assert_not_called()

    def test_command_reply_attachment_oversize_skips_getfile(self) -> None:
        state = callback_state()
        state["panes"]["pane-1"].pop("active_prompt", None)
        api = Mock()
        sent = Mock(return_value=(True, ""))
        with patch.multiple(
            herdres,
            load_dotenv=Mock(),
            load_state=Mock(return_value=state),
            save_state=Mock(),
            telegram_api=api,
            send_to_pane=sent,
        ):
            result = herdres.command_reply(
                self._payload(attachment=self._attachment(file_size=21 * 1024 * 1024))
            )
        self.assertIn("too large", result["reply"])
        api.assert_not_called()
        sent.assert_not_called()

    def test_command_reply_attachment_pane_closed(self) -> None:
        state = callback_state()
        state["panes"]["pane-1"]["last_known_status"] = "closed"
        deliver = Mock()
        with patch.multiple(
            herdres,
            load_dotenv=Mock(),
            load_state=Mock(return_value=state),
            save_state=Mock(),
            deliver_attachment=deliver,
        ):
            result = herdres.command_reply(self._payload())
        self.assertIn("closed or unavailable", result["reply"])
        deliver.assert_not_called()


class SpinnerAndWorkingPaneTests(unittest.TestCase):
    def test_is_noise_line_strips_claude_spinner_variants(self) -> None:
        for line in [
            "✻ Baked for 4m 47s",
            "✻ Brewed for 28s",
            "✻ Herding for 1m 3s",
            "✻ Pondering for 12s",
            "✻ Simmering for 2h 5m 1s",
            "✻ Brewing… (4s · esc to interrupt)",
        ]:
            self.assertTrue(herdres.is_noise_line(line), f"should be noise: {line!r}")
        # Glyph-anchored: legitimate prose/bullets are NOT swallowed.
        for line in [
            "We worked for 3 hours on the fix.",
            "Want me to do that?",
            "I baked a cake for the team.",
            "Waited for 3s",
            "- Compiled for 2m",
            "✦ Waited for 3 days before deploying.",  # decorative glyph + trailing prose
            "✻ Brewing… (4s · esc to interrupt) Should I continue?",  # spinner + REAL trailing question
        ]:
            self.assertFalse(herdres.is_noise_line(line), f"should NOT be noise: {line!r}")

    def test_closed_status_icon_default_emoji(self) -> None:
        self.assertEqual(herdres.status_icon_emoji("closed"), "📁")

    def test_status_icon_maps_match_legacy_explicit_values(self) -> None:
        self.assertEqual(
            herdres.STATUS_ICON_ENV_KEYS,
            {
                "working": "HERDR_TELEGRAM_TOPICS_STATUS_ICON_WORKING",
                "idle": "HERDR_TELEGRAM_TOPICS_STATUS_ICON_IDLE",
                "done": "HERDR_TELEGRAM_TOPICS_STATUS_ICON_DONE",
                "blocked": "HERDR_TELEGRAM_TOPICS_STATUS_ICON_BLOCKED",
                "error": "HERDR_TELEGRAM_TOPICS_STATUS_ICON_ERROR",
                "workflow": "HERDR_TELEGRAM_TOPICS_STATUS_ICON_WORKFLOW",
                "unknown": "HERDR_TELEGRAM_TOPICS_STATUS_ICON_UNKNOWN",
                "closed": "HERDR_TELEGRAM_TOPICS_STATUS_ICON_CLOSED",
                "goal": "HERDR_TELEGRAM_TOPICS_STATUS_ICON_GOAL",
            },
        )
        self.assertEqual(
            herdres.STATUS_ICON_EMOJI_ENV_KEYS,
            {
                "working": "HERDR_TELEGRAM_TOPICS_STATUS_ICON_WORKING_EMOJI",
                "idle": "HERDR_TELEGRAM_TOPICS_STATUS_ICON_IDLE_EMOJI",
                "done": "HERDR_TELEGRAM_TOPICS_STATUS_ICON_DONE_EMOJI",
                "blocked": "HERDR_TELEGRAM_TOPICS_STATUS_ICON_BLOCKED_EMOJI",
                "error": "HERDR_TELEGRAM_TOPICS_STATUS_ICON_ERROR_EMOJI",
                "workflow": "HERDR_TELEGRAM_TOPICS_STATUS_ICON_WORKFLOW_EMOJI",
                "unknown": "HERDR_TELEGRAM_TOPICS_STATUS_ICON_UNKNOWN_EMOJI",
                "closed": "HERDR_TELEGRAM_TOPICS_STATUS_ICON_CLOSED_EMOJI",
                "goal": "HERDR_TELEGRAM_TOPICS_STATUS_ICON_GOAL_EMOJI",
            },
        )
        self.assertEqual(
            herdres.STATUS_ICON_DEFAULT_EMOJI,
            {
                "working": "⚡️",
                "idle": "☕️",
                "done": "✅",
                "blocked": "❗️",
                "error": "‼️",
                "workflow": "📈",
                "unknown": "❓",
                "closed": "📁",
                "goal": "🧠",
            },
        )

    def test_closed_status_icon_resolves_to_custom_emoji_id(self) -> None:
        telegram = {"forum_topic_icons": {
            "by_emoji": {"📁": "id-folder", "❓": "id-unknown"},
            "fetched_at": herdres.utc_now(),
        }}
        cid, key, emoji = herdres.status_icon_id_for_keys(telegram, ["closed", "unknown"])
        self.assertEqual(emoji, "📁")
        self.assertEqual(cid, "id-folder")
        self.assertEqual(key, "closed")

    def test_api_error_recorded_on_entry_and_cleared_on_recovery(self) -> None:
        pane = {"pane_id": "pane-1", "agent": "claude", "agent_status": "idle"}
        entry: dict = {}
        turn_err = {
            "available": True, "complete": True, "turn_id": "a1", "assistant_final_text": "prev",
            "recent_turns": [{"available": True, "complete": True, "turn_id": "a1", "assistant_final_text": "prev"}],
            "api_error": {"id": "err1", "code": "server_error", "text": "API Error: overloaded"},
        }
        with patch.object(herdres, "pane_turn", Mock(return_value=turn_err)):
            herdres.extract_turn_feed_item(pane, entry)
        self.assertEqual((entry.get("pending_api_error") or {}).get("id"), "err1")
        # recovery: a clean turn clears the pending error
        turn_ok = {"available": True, "complete": True, "turn_id": "a2", "assistant_final_text": "ok",
                   "recent_turns": [{"available": True, "complete": True, "turn_id": "a2", "assistant_final_text": "ok"}]}
        with patch.object(herdres, "pane_turn", Mock(return_value=turn_ok)):
            herdres.extract_turn_feed_item(pane, entry)
        self.assertNotIn("pending_api_error", entry)

    def test_api_error_notice_body_includes_detail_and_action(self) -> None:
        body = herdres.api_error_notice_body({"id": "e", "code": "server_error", "text": "API Error: overloaded"})
        self.assertIn("overloaded", body)
        self.assertIn("continue", body.lower())

    def test_apply_api_error_warning_sends_once(self) -> None:
        entry = {"topic_id": "77", "pending_api_error": {"id": "err1", "code": "server_error", "text": "boom"}}
        counters = {"sends": 0}
        sent = []
        with patch.object(herdres, "send_notice",
                          Mock(side_effect=lambda *a, **k: sent.append(a) or {"ok": True, "message_id": "1"})):
            r1 = herdres.apply_api_error_warning("-1001", {}, entry, counters, 8)
            r2 = herdres.apply_api_error_warning("-1001", {}, entry, counters, 8)
        self.assertTrue(r1["changed"])
        self.assertEqual(entry["last_api_error_id"], "err1")
        self.assertEqual(len(sent), 1)        # same error -> sent exactly once
        self.assertFalse(r2["changed"])

    def test_apply_api_error_warning_not_marked_on_failed_send(self) -> None:
        entry = {"topic_id": "77", "pending_api_error": {"id": "err1", "text": "boom"}}
        with patch.object(herdres, "send_notice", Mock(return_value={"ok": False, "error": "network"})):
            herdres.apply_api_error_warning("-1001", {}, entry, {"sends": 0}, 8)
        self.assertIsNone(entry.get("last_api_error_id"))  # not marked -> retries next cycle

    def test_apply_api_error_warning_clears_only_on_reliable_recovery(self) -> None:
        # recovered: turn available again, no pending error -> clear
        recovered = {"topic_id": "77", "last_api_error_id": "err1", "last_turn_available": True}
        self.assertTrue(herdres.apply_api_error_warning("-1001", {}, recovered, {"sends": 0}, 8)["changed"])
        self.assertNotIn("last_api_error_id", recovered)
        # transient adapter miss (turn unavailable) -> do NOT clear (avoids re-warn thrash)
        miss = {"topic_id": "77", "last_api_error_id": "err1", "last_turn_available": False}
        herdres.apply_api_error_warning("-1001", {}, miss, {"sends": 0}, 8)
        self.assertEqual(miss.get("last_api_error_id"), "err1")

    def test_status_icon_goal_when_idle_and_goal_active(self) -> None:
        pane = {"pane_id": "p", "agent_status": "idle"}
        with patch.object(herdres, "pane_output", Mock(return_value="◎ /goal active (3h)")):
            self.assertEqual(herdres.status_icon_key(pane), "goal")
        self.assertEqual(herdres.status_icon_emoji("goal"), "🧠")

    def test_dry_run_status_icon_stickers_include_goal_and_closed(self) -> None:
        telegram: dict = {}
        with patch.object(herdres, "telegram_api", Mock(side_effect=herdres.dry_run_result)):
            cid, key, emoji = herdres.status_icon_id_for_keys(telegram, ["goal", "unknown"])
        self.assertEqual(cid, "dry-goal")
        self.assertEqual(key, "goal")
        self.assertEqual(emoji, "🧠")
        self.assertEqual(telegram["forum_topic_icons"]["by_emoji"]["📁"], "dry-closed")

    def test_status_icon_idle_when_goal_achieved_not_active(self) -> None:
        pane = {"pane_id": "p", "agent_status": "idle"}
        with patch.object(herdres, "pane_output", Mock(return_value="✔ Goal achieved (3h · 1 turn)")):
            self.assertEqual(herdres.status_icon_key(pane), "idle")  # achieved != active

    def test_status_icon_working_pane_skips_goal_read(self) -> None:
        pane = {"pane_id": "p", "agent_status": "working"}
        po = Mock(return_value="◎ /goal active")
        with patch.object(herdres, "pane_output", po):
            self.assertEqual(herdres.status_icon_key(pane), "working")
        po.assert_not_called()  # only idle panes pay the goal-marker read

    def test_status_icon_goal_marker_found_in_footer_of_tall_screen(self) -> None:
        # The real visible screen is ~75 lines and the "◎ /goal active" marker is
        # the very last (footer) line, well below the conversation. Prose higher up
        # mentioning "goal active" must NOT false-positive (only the /goal marker).
        screen = "\n".join(
            ["I will keep the goal active until it is done."]  # prose decoy near top
            + ["conversation line %d" % i for i in range(70)]
            + [
                "─" * 40 + " ultracode ─",
                "❯",
                "─" * 40,
                "  ⏵⏵ bypass permissions on · 4 shells · ↓ to manage",
                "                       ◎ /goal active (3h)",  # footer marker (last line)
            ]
        )
        pane = {"pane_id": "p", "agent_status": "idle"}
        with patch.object(herdres, "pane_output", Mock(return_value=screen)):
            self.assertEqual(herdres.status_icon_key(pane), "goal")

    def test_status_icon_idle_when_only_prose_mentions_goal(self) -> None:
        # "goal active" appearing only in conversation prose (no /goal footer marker)
        # must stay idle — the slash-marker is required.
        screen = "\n".join(
            ["Let me explain why the goal active flag matters here."]
            + ["body line %d" % i for i in range(40)]
            + ["❯", "  ⏵⏵ bypass permissions on · ↓ to manage"]
        )
        pane = {"pane_id": "p", "agent_status": "idle"}
        with patch.object(herdres, "pane_output", Mock(return_value=screen)):
            self.assertEqual(herdres.status_icon_key(pane), "idle")

    def test_event_path_never_scrapes_visible(self) -> None:
        # turn_only/event path passes allow_visible_fallback=False: even when the
        # turn is unavailable and status is a transient done/idle, never scrape.
        pane = {"pane_id": "pane-1", "agent": "claude", "agent_status": "done"}
        turn = {"available": False, "reason": "no_completed_turn"}
        vro = Mock(return_value={"kind": "choices", "turn_id": "x", "text": "scraped"})
        with patch.object(herdres, "pane_turn", Mock(return_value=turn)), \
             patch.object(herdres, "extract_visible_readonly_feed_item", vro):
            item = herdres.extract_turn_feed_item(pane, {}, allow_visible_fallback=False)
        vro.assert_not_called()
        self.assertIsNone(item)

    def test_reused_closed_topic_preserves_space_topic_name(self) -> None:
        state = {"panes": {"oldkey": {
            "pane_key": "oldkey", "topic_id": "77", "topic_name": "[OLD] Topics Pane",
            "pane_label_raw": "Topics Pane", "pane_label_topic_name": "Topics Pane",
            "agent_session_id": "sess-1",
            "last_known_status": "closed", "closed_at": "2026-01-01T00:00:00+00:00",
            "closed_topic_finalized": True,
            "status_icon_key": "closed",
            "topic_status_icon_key": "closed",
            "topic_status_icon_emoji": "📁",
            "topic_status_icon_custom_emoji_id": "closed-id",
            "topic_status_icon_updated_at": "2026-01-01T00:00:01+00:00",
        }}}
        pane = {"pane_id": "w1:p9", "terminal_id": "t", "workspace_id": "w1", "tab_id": "t1",
                "label": "Topics Pane", "agent": "codex", "agent_session": {"value": "sess-1"}}
        _key, entry, created = herdres.ensure_pane_entry(state, pane)
        self.assertTrue(created)
        self.assertEqual(entry["topic_name"], "W1")
        self.assertEqual(entry["legacy_topic_name"], "[OLD] Topics Pane")
        self.assertEqual(entry["pane_thread_name"], "Topics Pane")
        self.assertNotIn("topic_rename_pending_at", entry)

    def test_status_lag_race_prefers_completed_turn_over_scrape(self) -> None:
        # done->working lag: status reads non-working but a new turn is open and
        # the terminal may already show a spinner. We must deliver the completed
        # turn, NOT scrape the screen.
        for status in ("done", "idle", "running"):
            pane = {"pane_id": "pane-1", "agent": "claude", "agent_status": status}
            turn = {"available": True, "complete": True, "has_open_turn": True, "turn_id": "t1",
                    "assistant_final_text": "Real completed reply."}
            vro = Mock(return_value={"kind": "choices", "turn_id": "visible-readonly:x", "text": "scraped"})
            with patch.object(herdres, "pane_turn", Mock(return_value=turn)), \
                 patch.object(herdres, "extract_visible_readonly_feed_item", vro):
                item = herdres.extract_turn_feed_item(pane, {})
            vro.assert_not_called()
            assert item is not None
            self.assertEqual(item["turn_id"], "t1", f"status={status}")

    def test_clean_feed_lines_drops_spinner_so_question_is_stable(self) -> None:
        joined = " ".join(herdres.clean_feed_lines("Should I deploy the fix now?\n✻ Baked for 4m 47s\n"))
        self.assertNotIn("Baked for", joined)  # volatile spinner removed -> stable dedup
        self.assertIn("deploy the fix now", joined)

    def test_working_pane_does_not_surface_visible_readonly(self) -> None:
        pane = {"pane_id": "pane-1", "agent": "claude", "agent_status": "working"}
        turn = {"available": True, "complete": True, "has_open_turn": True, "turn_id": "t1",
                "user_text": "do x", "assistant_final_text": "Done. Want me to continue?"}
        vro = Mock(return_value={"kind": "choices", "turn_id": "visible-readonly:x", "text": "scraped"})
        with patch.object(herdres, "pane_turn", Mock(return_value=turn)), \
             patch.object(herdres, "extract_visible_readonly_feed_item", vro):
            item = herdres.extract_turn_feed_item(pane, {})
        vro.assert_not_called()  # working pane: never scrape the screen
        assert item is not None
        self.assertEqual(item["turn_id"], "t1")  # the completed turn is delivered instead

    def test_blocked_pane_still_surfaces_visible_readonly(self) -> None:
        pane = {"pane_id": "pane-1", "agent": "claude", "agent_status": "blocked"}
        turn = {"available": True, "complete": True, "has_open_turn": True, "turn_id": "t1",
                "assistant_final_text": "ctx"}
        ro_item = {"kind": "choices", "turn_id": "visible-readonly:x", "text": "Pick one"}
        vro = Mock(return_value=ro_item)
        with patch.object(herdres, "pane_turn", Mock(return_value=turn)), \
             patch.object(herdres, "extract_visible_readonly_feed_item", vro):
            item = herdres.extract_turn_feed_item(pane, {})
        vro.assert_called_once()  # genuine awaiting-input prompt still surfaces
        assert item is not None
        self.assertEqual(item["turn_id"], "visible-readonly:x")


class TopicLatestAnchorTests(unittest.TestCase):
    """A turn's edit-in-place anchor must only be reused while it is still the newest
    message in the shared topic. Otherwise a sibling pane (or an inbound owner message)
    posting afterward leaves an edited completion buried mid-feed where the owner never
    sees it — the real incident (a long codex turn finalized into a stale message that a
    newly-added Devin pane had since buried)."""

    def _state(self) -> tuple[dict, dict, str, dict]:
        pane = {
            "pane_id": "pane-1",
            "terminal_id": "term-1",
            "workspace_id": "workspace-1",
            "tab_id": "tab-1",
            "agent": "codex",
            "agent_status": "working",
            "label": "Build Runner",
        }
        key = herdres.pane_key(pane)
        entry = {
            "pane_key": key,
            "pane_id": "pane-1",
            "space_key": "workspace:workspace-1",
            "topic_id": "77",
            "pane_root_message_id": "1001",
            "pane_thread_name": "Build Runner",
            "last_known_status": "working",
        }
        state = {
            "version": 1,
            "enabled": True,
            "telegram": {"chat_id": "-1001", "general_thread_id": "1", "owner_user_ids": ["42"]},
            "spaces": {
                "workspace:workspace-1": {
                    "space_key": "workspace:workspace-1",
                    "topic_id": "77",
                    "topic_name": "Workspace 1",
                    "pane_keys": [key],
                }
            },
            "panes": {key: entry},
        }
        return state, pane, key, entry

    def _caps(self) -> tuple[dict, dict]:
        counters = {"creates": 0, "sends": 0, "feed_sends": 0, "marker_sends": 0, "verifies": 0, "renames": 0, "icon_updates": 0}
        caps = {"max_creates": 0, "max_sends": 10, "max_feed_sends": 10, "max_marker_sends": 10, "max_verifies": 0}
        return counters, caps

    def test_topic_message_is_latest_predicate(self) -> None:
        # No space context => degrade to True so callers fall back to the pane check.
        self.assertTrue(herdres.topic_message_is_latest(None, "2001"))
        # No high-water mark yet => anything is "latest".
        self.assertTrue(herdres.topic_message_is_latest({}, "2001"))
        space = {"last_topic_message_id": "2050"}
        self.assertFalse(herdres.topic_message_is_latest(space, "2001"))  # buried
        self.assertTrue(herdres.topic_message_is_latest(space, "2050"))   # is the latest
        self.assertTrue(herdres.topic_message_is_latest(space, "2099"))   # newer than mark
        self.assertFalse(herdres.topic_message_is_latest(space, ""))      # empty id

    def test_record_pane_message_route_advances_topic_high_water_mark(self) -> None:
        state, _pane, key, _entry = self._state()
        # A second pane sharing the same topic.
        other = dict(state["panes"][key])
        other["pane_key"] = "workspace:workspace-1:p2"
        other["pane_id"] = "pane-2"
        state["panes"]["workspace:workspace-1:p2"] = other
        sk = "workspace:workspace-1"

        herdres.record_pane_message_route(state, sk, key, "2001")
        self.assertEqual(state["spaces"][sk]["last_topic_message_id"], "2001")
        # A sibling pane posts a newer message -> topic mark advances (pane marks stay separate).
        herdres.record_pane_message_route(state, sk, "workspace:workspace-1:p2", "2005")
        self.assertEqual(state["spaces"][sk]["last_topic_message_id"], "2005")
        self.assertEqual(state["panes"][key]["last_pane_message_id"], "2001")
        self.assertEqual(other["last_pane_message_id"], "2005")
        # An older id never regresses the mark.
        herdres.record_pane_message_route(state, sk, key, "2002")
        self.assertEqual(state["spaces"][sk]["last_topic_message_id"], "2005")
        # pane p1's own anchor (2001) is no longer topic-latest -> would re-anchor.
        self.assertTrue(herdres.pane_message_is_latest(state["panes"][key], "2002"))
        self.assertFalse(herdres.topic_message_is_latest(state["spaces"][sk], "2001"))

    def _run_lifecycle_until_final(self, state, pane):
        user_text = "make sure you run glm with full permissions"
        worklog_text = "Wiring the seat provisioning."
        response_text = "Implemented and deployed."
        pane_turn = Mock(
            side_effect=[
                {"available": True, "complete": False, "turn_id": "turn-1", "user_text": user_text,
                 "assistant_final_text": "", "assistant_stream_text": ""},
                {"available": True, "complete": False, "turn_id": "turn-1", "user_text": user_text,
                 "assistant_final_text": "", "assistant_stream_text": worklog_text},
                {"available": True, "complete": True, "turn_id": "turn-1", "user_text": user_text,
                 "assistant_final_text": response_text},
            ]
        )
        api_calls = []
        next_message_id = 2000

        def telegram_api(method, payload, *, token=None):
            nonlocal next_message_id
            api_calls.append((method, dict(payload)))
            if method == "sendRichMessage":
                next_message_id += 1
                return {"ok": True, "result": {"message_id": str(next_message_id)}}
            return {"ok": True, "result": True}

        patch_args = {
            "pane_turn": pane_turn, "telegram_api": telegram_api, "save_state": Mock(),
            "apply_api_error_warning": Mock(return_value={"topic_missing": False, "changed": False}),
            "TURN_FEED_ENABLED": True, "CLEAN_FEED_ENABLED": True, "LIVE_CARD_ENABLED": False,
            "STATUS_MARKER_ENABLED": False, "STATUS_ICON_ENABLED": False,
            "STREAMING_DRAFTS_ENABLED": True, "STREAM_MIN_INTERVAL_SECONDS": 0, "STREAM_MIN_CHARS": 0,
        }
        return user_text, worklog_text, response_text, pane_turn, api_calls, patch_args

    def test_final_turn_edits_in_place_when_anchor_still_latest(self) -> None:
        # Positive control: no sibling activity -> the final turn edits its anchor (2001) as before.
        state, pane, _key, entry = self._state()
        state["telegram"]["rich_messages"] = {"supported": "yes"}
        state["telegram"]["streaming_drafts"] = {"supported": "no"}
        _u, _w, _r, _pt, api_calls, patch_args = self._run_lifecycle_until_final(state, pane)
        with patch.multiple(herdres, **patch_args):
            for _ in range(3):
                counters, caps = self._caps()
                herdres.sync_pane_once(state, "-1001", state["telegram"], pane, counters, caps)
        send_calls = [c for c in api_calls if c[0] == "sendRichMessage"]
        edit_calls = [c for c in api_calls if c[0] == "editMessageText"]
        self.assertEqual(len(send_calls), 1)           # only the original prompt send
        self.assertGreaterEqual(len(edit_calls), 1)
        self.assertEqual(edit_calls[-1][1]["message_id"], "2001")  # final edited in place
        self.assertEqual(entry["last_clean_message_id"], "2001")

    def test_final_turn_reanchors_when_sibling_buried_the_anchor(self) -> None:
        # The fix: a sibling pane posts to the shared topic mid-turn, burying the anchor.
        # The final completion must be SENT fresh at the bottom, not edited into the buried msg.
        state, pane, _key, entry = self._state()
        state["telegram"]["rich_messages"] = {"supported": "yes"}
        state["telegram"]["streaming_drafts"] = {"supported": "no"}
        _u, _w, _r, _pt, api_calls, patch_args = self._run_lifecycle_until_final(state, pane)
        with patch.multiple(herdres, **patch_args):
            # 1) prompt send (2001), 2) worklog edit (2001)
            for _ in range(2):
                counters, caps = self._caps()
                herdres.sync_pane_once(state, "-1001", state["telegram"], pane, counters, caps)
            self.assertEqual(entry["last_pane_message_id"], "2001")
            # A SIBLING pane posts a newer message into the same topic (the Devin "KIMI_OK" case).
            herdres.record_pane_message_route(state, "workspace:workspace-1", "workspace:workspace-1:sibling", "2043")
            self.assertEqual(state["spaces"]["workspace:workspace-1"]["last_topic_message_id"], "2043")
            # 3) final turn -> anchor 2001 is buried under 2043 -> must send a NEW message.
            counters, caps = self._caps()
            herdres.sync_pane_once(state, "-1001", state["telegram"], pane, counters, caps)

        send_calls = [c for c in api_calls if c[0] == "sendRichMessage"]
        edit_calls = [c for c in api_calls if c[0] == "editMessageText"]
        final_edit_to_2001 = [c for c in edit_calls if c[1].get("message_id") == "2001" and "Implemented and deployed" in c[1].get("rich_message", "")]
        self.assertEqual(len(final_edit_to_2001), 0, "final completion must NOT be edited into the buried anchor 2001")
        self.assertEqual(len(send_calls), 2, "final completion must be sent as a fresh bottom-of-topic message")
        # The new bottom message carries the completion and becomes the clean anchor, and is
        # itself route-recorded (so the topic high-water mark tracks it too). The mock assigns
        # 2002 sequentially — below the injected sibling 2043 — so the max mark stays 2043.
        self.assertEqual(entry["last_clean_message_id"], "2002")
        self.assertIn("Implemented and deployed", send_calls[1][1]["rich_message"])
        self.assertIn("2002", state["spaces"]["workspace:workspace-1"].get("message_routes", {}))


    def test_note_topic_high_water_mark_advances_only_forward(self) -> None:
        space = {}
        self.assertTrue(herdres.note_topic_high_water_mark(space, "2001"))
        self.assertEqual(space["last_topic_message_id"], "2001")
        self.assertFalse(herdres.note_topic_high_water_mark(space, "2001"))  # equal -> no advance
        self.assertFalse(herdres.note_topic_high_water_mark(space, "1999"))  # older -> no advance
        self.assertEqual(space["last_topic_message_id"], "2001")
        self.assertTrue(herdres.note_topic_high_water_mark(space, "2050"))   # newer -> advance
        self.assertEqual(space["last_topic_message_id"], "2050")
        self.assertFalse(herdres.note_topic_high_water_mark(None, "9999"))   # no space
        self.assertFalse(herdres.note_topic_high_water_mark(space, ""))      # empty id

    def test_command_reply_owner_message_advances_topic_high_water_mark(self) -> None:
        # The owner posting in the shared topic buries an in-place anchor just like a
        # sibling pane does. Owner inbound ids never reach record_pane_message_route, so
        # command_reply must advance the topic high-water mark itself (and persist it).
        state, _pane, _key, entry = self._state()
        entry["last_pane_message_id"] = "2001"
        state["spaces"]["workspace:workspace-1"]["last_topic_message_id"] = "2001"
        saved = []
        payload = {"chat_id": "-1001", "topic_id": "77", "user_id": "42",
                   "message_id": "2043", "text": "/help"}
        with patch.multiple(
            herdres,
            load_state=Mock(return_value=state),
            save_state=Mock(side_effect=lambda s: saved.append(True)),
            load_dotenv=Mock(),
        ):
            herdres.command_reply(payload)
        self.assertEqual(state["spaces"]["workspace:workspace-1"]["last_topic_message_id"], "2043")
        self.assertTrue(saved, "command_reply must persist the advanced topic high-water mark")
        # The pane's anchor 2001 is now buried under the owner's 2043 -> finalize will re-anchor.
        self.assertFalse(herdres.topic_message_is_latest(state["spaces"]["workspace:workspace-1"], "2001"))

    def test_command_reply_forwarded_owner_message_still_advances_mark(self) -> None:
        # GLM finding: a forwarded owner message creates a real message that physically
        # buries the anchor, even though command_reply rejects its CONTENT ("Ignored ...").
        # The high-water mark must still advance (the advance is before the forwarded gate).
        state, _pane, _key, entry = self._state()
        entry["last_pane_message_id"] = "2001"
        state["spaces"]["workspace:workspace-1"]["last_topic_message_id"] = "2001"
        saved = []
        payload = {"chat_id": "-1001", "topic_id": "77", "user_id": "42",
                   "message_id": "2044", "text": "fwd", "forwarded": True}
        with patch.multiple(
            herdres,
            load_state=Mock(return_value=state),
            save_state=Mock(side_effect=lambda s: saved.append(True)),
            load_dotenv=Mock(),
        ):
            result = herdres.command_reply(payload)
        self.assertEqual(state["spaces"]["workspace:workspace-1"]["last_topic_message_id"], "2044")
        self.assertTrue(saved, "forwarded owner message must still persist the advanced mark")
        # content is still rejected
        self.assertIn("Ignored", result.get("reply", ""))

    def test_command_reply_edited_owner_message_does_not_advance_mark(self) -> None:
        # An edited message does NOT create a new feed position, so it must NOT advance.
        state, _pane, _key, _entry = self._state()
        state["spaces"]["workspace:workspace-1"]["last_topic_message_id"] = "2001"
        payload = {"chat_id": "-1001", "topic_id": "77", "user_id": "42",
                   "message_id": "2044", "text": "x", "edited": True}
        with patch.multiple(
            herdres,
            load_state=Mock(return_value=state),
            save_state=Mock(),
            load_dotenv=Mock(),
        ):
            herdres.command_reply(payload)
        self.assertEqual(state["spaces"]["workspace:workspace-1"]["last_topic_message_id"], "2001")

    def test_command_reply_owner_message_in_general_does_not_create_mark(self) -> None:
        # A message in the General thread (not a mapped pane topic) must not fabricate a mark.
        state, _pane, _key, _entry = self._state()
        payload = {"chat_id": "-1001", "topic_id": "1", "user_id": "42",
                   "message_id": "2043", "text": "/help"}
        with patch.multiple(
            herdres,
            load_state=Mock(return_value=state),
            save_state=Mock(),
            load_dotenv=Mock(),
        ):
            herdres.command_reply(payload)
        self.assertNotIn("last_topic_message_id", state["spaces"]["workspace:workspace-1"])


class PromptAnchorFallbackTests(unittest.TestCase):
    """A completed turn must reuse the prompt message even when its turn_id differs from
    the open-prompt turn_id (the auto-continue case), so one input -> one message, not a
    duplicate. Reproduces the live 2818/2820 case (prompt 2818 turn '14275889' + finalized
    turn '18a4d8e5' posted as a separate 2820)."""

    def test_finalize_reuses_prompt_when_turn_id_differs(self) -> None:
        # Prompt 2818 was just delivered for the open turn; it's the pane's latest message.
        entry = {
            "last_prompt_message_id": "2818",
            "last_prompt_turn_id": "14275889",
            "last_pane_message_id": "2818",
        }
        # The completed turn finalizes under a DIFFERENT turn_id (auto-continue):
        self.assertEqual(herdres.turn_visible_anchor_message_id(entry, "18a4d8e5"), "2818")
        # ...and the strict same-turn_id case still works.
        self.assertEqual(herdres.turn_visible_anchor_message_id(entry, "14275889"), "2818")

    def test_fallback_skipped_when_prompt_not_latest(self) -> None:
        # Something newer was posted after the prompt -> don't hijack it; send fresh.
        entry = {
            "last_prompt_message_id": "2818",
            "last_prompt_turn_id": "14275889",
            "last_pane_message_id": "2820",  # a later message buried the prompt
        }
        self.assertEqual(herdres.turn_visible_anchor_message_id(entry, "18a4d8e5"), "")

    def test_fallback_skipped_when_stream_anchor_exists(self) -> None:
        # A live stream anchor is the proper anchor; the loose prompt fallback must not fire.
        entry = {
            "last_prompt_message_id": "2818",
            "last_prompt_turn_id": "14275889",
            "last_stream_message_id": "2819",
            "last_stream_turn_id": "other",
            "last_pane_message_id": "2818",
        }
        self.assertEqual(herdres.turn_visible_anchor_message_id(entry, "18a4d8e5"), "")


class StreamAnchorFallbackTests(unittest.TestCase):
    """A turn that streamed a live worklog message must finalize by EDITING that message,
    not by posting a fresh one. The duplicate (live 3100 -> 3105 case) happens when the
    completed turn finalizes under a turn_id that differs from the open_turn_id it streamed
    under: turn_visible_anchor_message_id strict-misses and, because a stream message
    exists, the #33 prompt fallback is gated off, so the completion orphans the stream
    message. turn_stream_anchor_fallback reuses it — guarded so an older catch-up turn (which
    streamed nothing) never hijacks the newer stream message."""

    def _streaming_entry(self) -> dict:
        # Stream message 3100 was just edited for the open turn; it's the pane's latest.
        return {
            "last_stream_message_id": "3100",
            "last_stream_turn_id": "1ee3de2e-open",
            "last_pane_message_id": "3100",
            "last_clean_message_id": "3072",
        }

    def test_reuses_stream_message_when_completed_turn_id_differs(self) -> None:
        # The completed turn finalizes under turn.turn_id (== the latest completed turn),
        # which differs from the open_turn_id the message streamed under.
        entry = self._streaming_entry()
        item = {"kind": "turn", "turn_id": "e74fdf39-done"}
        turn = {"turn_id": "e74fdf39-done", "open_turn_id": "1ee3de2e-open"}
        self.assertEqual(herdres.turn_stream_anchor_fallback(entry, item, turn), "3100")

    def test_reuses_stream_message_when_item_is_open_turn(self) -> None:
        entry = self._streaming_entry()
        item = {"kind": "turn", "turn_id": "1ee3de2e-open"}
        turn = {"turn_id": "e74fdf39-done", "open_turn_id": "1ee3de2e-open"}
        self.assertEqual(herdres.turn_stream_anchor_fallback(entry, item, turn), "3100")

    def test_skips_older_catch_up_turn(self) -> None:
        # Catch-up is delivering an OLDER completed turn while a newer turn streamed; its
        # id is neither the current completed nor the open turn -> must stay its own message.
        entry = self._streaming_entry()
        item = {"kind": "turn", "turn_id": "aaaa-older-interrupted"}
        turn = {"turn_id": "e74fdf39-done", "open_turn_id": "1ee3de2e-open"}
        self.assertEqual(herdres.turn_stream_anchor_fallback(entry, item, turn), "")

    def test_skips_when_stream_message_buried(self) -> None:
        # A later message buried the stream message -> editing it would strand the
        # completion mid-feed; send a fresh message instead.
        entry = self._streaming_entry()
        entry["last_pane_message_id"] = "3104"
        item = {"kind": "turn", "turn_id": "e74fdf39-done"}
        turn = {"turn_id": "e74fdf39-done", "open_turn_id": "1ee3de2e-open"}
        self.assertEqual(herdres.turn_stream_anchor_fallback(entry, item, turn), "")

    def test_skips_when_already_finalized_as_clean(self) -> None:
        entry = self._streaming_entry()
        entry["last_clean_message_id"] = "3100"  # already the finalized clean message
        item = {"kind": "turn", "turn_id": "e74fdf39-done"}
        turn = {"turn_id": "e74fdf39-done", "open_turn_id": "1ee3de2e-open"}
        self.assertEqual(herdres.turn_stream_anchor_fallback(entry, item, turn), "")

    def test_skips_when_no_stream_message(self) -> None:
        entry = {"last_pane_message_id": "3100"}
        item = {"kind": "turn", "turn_id": "e74fdf39-done"}
        turn = {"turn_id": "e74fdf39-done", "open_turn_id": "1ee3de2e-open"}
        self.assertEqual(herdres.turn_stream_anchor_fallback(entry, item, turn), "")

    def test_skips_when_turn_unavailable(self) -> None:
        # No authoritative current-turn ids -> cannot prove the item is the current turn.
        entry = self._streaming_entry()
        item = {"kind": "turn", "turn_id": "e74fdf39-done"}
        self.assertEqual(herdres.turn_stream_anchor_fallback(entry, item, {"available": False}), "")


class PruneOrphanedCollidingSpacesTests(unittest.TestCase):
    """A herdr pane renumber (p16 -> p1H) leaves the old space bound to the topic with a
    dangling pane_key; sorting first, it shadows the live space and inbound commands get
    'No live Herdr panes in this topic.' Prune the fully-orphaned space ONLY when a live
    sibling owns the same topic."""

    def test_prunes_orphan_when_live_sibling_owns_topic(self) -> None:
        state = {
            "panes": {"w:p1H:c9": {"last_known_status": "working", "topic_id": "198"}},
            "spaces": {
                "agent:w:p16": {"topic_id": "198", "pane_keys": ["w:p16:1f"]},   # dangling
                "agent:w:p1H": {"topic_id": "198", "pane_keys": ["w:p1H:c9"]},   # live
            },
        }
        self.assertTrue(herdres.prune_orphaned_colliding_spaces(state))
        self.assertEqual(list(state["spaces"]), ["agent:w:p1H"])

    def test_keeps_sole_orphan_when_no_live_sibling(self) -> None:
        # The only space for the topic is orphaned -> keep it (don't strand the topic).
        state = {
            "panes": {},
            "spaces": {"agent:w:p16": {"topic_id": "198", "pane_keys": ["w:p16:1f"]}},
        }
        self.assertFalse(herdres.prune_orphaned_colliding_spaces(state))
        self.assertIn("agent:w:p16", state["spaces"])

    def test_keeps_space_with_live_pane(self) -> None:
        state = {
            "panes": {"w:p1H:c9": {"last_known_status": "working"}},
            "spaces": {"agent:w:p1H": {"topic_id": "198", "pane_keys": ["w:p1H:c9"]}},
        }
        self.assertFalse(herdres.prune_orphaned_colliding_spaces(state))
        self.assertIn("agent:w:p1H", state["spaces"])

    def test_ignores_orphan_on_different_topic(self) -> None:
        # Orphan on topic 200; the live space owns 198 -> no collision, don't prune.
        state = {
            "panes": {"w:p1H:c9": {"last_known_status": "working"}},
            "spaces": {
                "agent:w:p16": {"topic_id": "200", "pane_keys": ["w:p16:1f"]},
                "agent:w:p1H": {"topic_id": "198", "pane_keys": ["w:p1H:c9"]},
            },
        }
        self.assertFalse(herdres.prune_orphaned_colliding_spaces(state))
        self.assertIn("agent:w:p16", state["spaces"])

    def test_does_not_prune_space_without_pane_keys(self) -> None:
        # A freshly-created space (no pane_keys yet) colliding with a live one is left alone.
        state = {
            "panes": {"w:p1H:c9": {"last_known_status": "working"}},
            "spaces": {
                "agent:w:new": {"topic_id": "198", "pane_keys": []},
                "agent:w:p1H": {"topic_id": "198", "pane_keys": ["w:p1H:c9"]},
            },
        }
        self.assertFalse(herdres.prune_orphaned_colliding_spaces(state))
        self.assertIn("agent:w:new", state["spaces"])

    def test_normalize_state_applies_the_prune(self) -> None:
        # End-to-end through normalize_state (the load path).
        state = {
            "version": 1,
            "telegram": {},
            "panes": {"w:p1H:c9": {"last_known_status": "working"}},
            "spaces": {
                "agent:w:p16": {"topic_id": "198", "pane_keys": ["w:p16:1f"]},
                "agent:w:p1H": {"topic_id": "198", "pane_keys": ["w:p1H:c9"]},
            },
        }
        herdres.normalize_state(state)
        self.assertNotIn("agent:w:p16", state["spaces"])
        self.assertIn("agent:w:p1H", state["spaces"])


class SelfHealingFoldTests(unittest.TestCase):
    """Self-healing fold: every previously-delivered turn (not the latest) collapses, and a
    fold missed on delivery heals on the next sync via the per-pane unfolded_turns buffer."""

    def _item(self, tid: str, resp: str = "Answer.") -> dict:
        return {"kind": "turn", "turn_id": tid, "user_text": "q", "worklog_text": "wl",
                "assistant_final_text": resp}

    def _state(self, enabled: bool = True) -> dict:
        return {"spaces": {"s": {"collapse_previous_responses": enabled}}}

    def _entry(self, **kw) -> dict:
        e = {"space_key": "s"}
        e.update(kw)
        return e

    # --- append_unfolded_turn ---
    def test_append_dedups_by_message_id_and_bounds(self) -> None:
        entry = self._entry()
        for i in range(herdres.UNFOLDED_TURNS_CAP + 3):
            herdres.append_unfolded_turn(entry, f"m{i}", self._item(f"t{i}"))
        buf = entry["unfolded_turns"]
        self.assertEqual(len(buf), herdres.UNFOLDED_TURNS_CAP)  # bounded
        self.assertEqual(buf[-1]["message_id"], f"m{herdres.UNFOLDED_TURNS_CAP + 2}")  # newest kept
        # re-append an existing message_id -> refreshed in place, not duplicated
        herdres.append_unfolded_turn(entry, buf[-1]["message_id"], self._item("tX"))
        ids = [e["message_id"] for e in entry["unfolded_turns"]]
        self.assertEqual(len(ids), len(set(ids)))

    def test_append_skips_turn_without_response(self) -> None:
        entry = self._entry()
        herdres.append_unfolded_turn(entry, "m1", self._item("t1", resp=""))
        self.assertNotIn("unfolded_turns", entry)

    def test_trimmed_item_keeps_render_fields(self) -> None:
        entry = self._entry()
        herdres.append_unfolded_turn(entry, "m1", {**self._item("t1"), "summary": "x", "lines": ["y"],
                                                   "prompt_collapse_chars": 50, "worklog_label": "Worklog (2m)"})
        stored = entry["unfolded_turns"][0]["item"]
        self.assertEqual(stored["assistant_final_text"], "Answer.")
        self.assertEqual(stored["worklog_label"], "Worklog (2m)")
        self.assertEqual(stored["prompt_collapse_chars"], 50)
        self.assertNotIn("summary", stored)  # dropped to keep state small

    def test_trimmed_item_does_not_truncate_long_response_body(self) -> None:
        # A response longer than MAX_REPLY_CHARS (3200) but within FINAL_REPLY_MAX_CHARS must be
        # preserved, so the folded body matches what was delivered (no content loss on expand).
        # Use varied text (sanitize_text collapses repeated chars, so "x"*N won't do).
        long_body = " ".join(f"detail-line-{i}" for i in range(700))  # ~9k varied chars
        self.assertGreater(len(long_body), herdres.MAX_REPLY_CHARS)
        entry = self._entry()
        herdres.append_unfolded_turn(entry, "m1", self._item("t1", resp=long_body))
        stored = entry["unfolded_turns"][0]["item"]
        # Bug would truncate to MAX_REPLY_CHARS (3200); fix keeps it at FINAL_REPLY_MAX_CHARS.
        self.assertGreater(len(stored["assistant_final_text"]), herdres.MAX_REPLY_CHARS)

    # --- fold_superseded_turns ---
    def test_folds_non_latest_and_keeps_latest(self) -> None:
        entry = self._entry(last_clean_message_id="m2", last_turn_id="t2", unfolded_turns=[
            {"message_id": "m1", "turn_id": "t1", "item": self._item("t1")},
            {"message_id": "m2", "turn_id": "t2", "item": self._item("t2")},
        ])
        edits = []
        ef = Mock(side_effect=lambda *a, **k: edits.append((a[1], a[2].get("collapse_response"))) or {"ok": True})
        with patch.multiple(herdres, edit_feed_item=ef, managed_bot_token_for_entry=Mock(return_value="tok")):
            mutated = herdres.fold_superseded_turns(self._state(), entry, {}, "-1001")
        self.assertTrue(mutated)
        self.assertEqual(edits, [("m1", True)])  # only the non-latest folded
        self.assertEqual([e["message_id"] for e in entry.get("unfolded_turns", [])], ["m2"])  # m1 dropped

    def test_self_heals_stuck_turn_without_a_new_turn(self) -> None:
        # A prior fold was missed: m1 sits unfolded while m2 is already the latest. A plain
        # sweep (no new delivery) folds it.
        entry = self._entry(last_clean_message_id="m2", last_turn_id="t2", unfolded_turns=[
            {"message_id": "m1", "turn_id": "t1", "item": self._item("t1")},
        ])
        ef = Mock(return_value={"ok": True})
        with patch.multiple(herdres, edit_feed_item=ef, managed_bot_token_for_entry=Mock(return_value="tok")):
            herdres.fold_superseded_turns(self._state(), entry, {}, "-1001")
        self.assertEqual(ef.call_args.args[1], "m1")
        self.assertTrue(ef.call_args.args[2].get("collapse_response"))

    def test_skips_latest_by_message_id_on_edit_in_place_reuse(self) -> None:
        # Edit-in-place reuse: a buffered entry shares the latest's message_id -> never fold it.
        entry = self._entry(last_clean_message_id="m1", last_turn_id="t2", unfolded_turns=[
            {"message_id": "m1", "turn_id": "t1", "item": self._item("t1")},
        ])
        ef = Mock(return_value={"ok": True})
        with patch.multiple(herdres, edit_feed_item=ef, managed_bot_token_for_entry=Mock(return_value="tok")):
            herdres.fold_superseded_turns(self._state(), entry, {}, "-1001")
        ef.assert_not_called()

    def test_drops_entry_on_not_found(self) -> None:
        entry = self._entry(last_clean_message_id="m2", last_turn_id="t2", unfolded_turns=[
            {"message_id": "m1", "turn_id": "t1", "item": self._item("t1")},
        ])
        with patch.multiple(herdres, edit_feed_item=Mock(return_value={"ok": False, "not_found": True}),
                            managed_bot_token_for_entry=Mock(return_value="tok")):
            herdres.fold_superseded_turns(self._state(), entry, {}, "-1001")
        self.assertNotIn("unfolded_turns", entry)  # dead message dropped

    def test_retries_then_drops_on_persistent_error(self) -> None:
        entry = self._entry(last_clean_message_id="m2", last_turn_id="t2", unfolded_turns=[
            {"message_id": "m1", "turn_id": "t1", "item": self._item("t1")},
        ])
        with patch.multiple(herdres, edit_feed_item=Mock(return_value={"ok": False}),
                            managed_bot_token_for_entry=Mock(return_value="tok")):
            for _ in range(herdres.FOLD_ATTEMPT_CAP):
                herdres.fold_superseded_turns(self._state(), entry, {}, "-1001")
        self.assertFalse(entry.get("unfolded_turns"))  # gave up after the attempt cap

    def test_no_fold_when_setting_disabled(self) -> None:
        entry = self._entry(last_clean_message_id="m2", last_turn_id="t2", unfolded_turns=[
            {"message_id": "m1", "turn_id": "t1", "item": self._item("t1")},
        ])
        ef = Mock(return_value={"ok": True})
        with patch.multiple(herdres, edit_feed_item=ef, managed_bot_token_for_entry=Mock(return_value="tok")):
            mutated = herdres.fold_superseded_turns(self._state(enabled=False), entry, {}, "-1001")
        self.assertFalse(mutated)
        ef.assert_not_called()

    # --- record_delivered_feed_item seeding ---
    def test_record_seeds_prior_then_appends_new_turn(self) -> None:
        entry = self._entry(last_clean_item=self._item("t1"), last_clean_message_id="m1", last_turn_id="t1")
        herdres.record_delivered_feed_item(
            entry, self._item("t2"), {"ok": True, "message_id": "m2"},
            pending_active_prompt=None, clear_active_prompt=False)
        self.assertEqual([e["message_id"] for e in entry["unfolded_turns"]], ["m1", "m2"])


class PlanAttachmentTests(unittest.TestCase):
    """Issue #26: a plan/oversized turn delivers a short summary turn + the full markdown as a
    .md document attachment, instead of truncating/flattening it inline."""

    def _turn(self, final_text: str) -> dict:
        return {"kind": "turn", "turn_id": "t1", "user_text": "make a plan",
                "worklog_text": "thinking", "assistant_final_text": final_text}

    # --- detection ---
    def test_detects_mermaid(self) -> None:
        self.assertTrue(herdres.turn_needs_plan_attachment(
            self._turn("# Plan\n```mermaid\nflowchart TD\nA-->B\n```\n")))

    def test_detects_large_structured(self) -> None:
        big = "# Plan\n\n" + "\n".join(f"## Section {i}\n- a\n- b" for i in range(400))
        self.assertGreater(len(big), herdres.PLAN_ATTACH_MIN_CHARS)
        self.assertTrue(herdres.turn_needs_plan_attachment(self._turn(big)))

    def test_detects_render_over_cap(self) -> None:
        # Unique paragraphs (no repeated-run collapse) so the rendered HTML exceeds the rich cap.
        prose = "\n\n".join(f"Paragraph {i} discusses a distinct idea about subject {i} in detail here."
                            for i in range(400))
        self.assertTrue(herdres.turn_needs_plan_attachment(self._turn(prose)))

    def test_small_turn_not_attached(self) -> None:
        self.assertFalse(herdres.turn_needs_plan_attachment(self._turn("# Plan\nshort and sweet.")))

    def test_non_turn_not_attached(self) -> None:
        self.assertFalse(herdres.turn_needs_plan_attachment(
            {"kind": "status", "assistant_final_text": "x" * 20000}))

    # --- summary ---
    def test_summary_item_shortens_and_keeps_user_worklog(self) -> None:
        big = "# Title\n" + "\n".join(f"line {i}" for i in range(200))
        item = self._turn(big)
        summ = herdres.plan_summary_item(item)
        self.assertEqual(summ["user_text"], item["user_text"])
        self.assertEqual(summ["worklog_text"], item["worklog_text"])
        self.assertLess(len(summ["assistant_final_text"]), len(big))
        self.assertIn("Full plan attached", summ["assistant_final_text"])

    def test_summary_skips_fenced_blocks(self) -> None:
        text = "# Title\n```mermaid\nflowchart TD\nA-->B\n```\nReal intro line."
        summ = herdres._plan_summary_text(text)
        self.assertNotIn("flowchart", summ)
        self.assertIn("Title", summ)

    # --- mermaid placeholder in the renderer ---
    def test_mermaid_fence_renders_placeholder_not_raw(self) -> None:
        html = herdres.render_final_reply_html("before\n```mermaid\nflowchart TD\nA-->B\n```\nafter")
        self.assertNotIn("language-mermaid", html)
        self.assertNotIn("flowchart TD", html)
        self.assertIn("mermaid diagram", html)

    # --- send_document ---
    def test_send_document_builds_sendDocument_multipart(self) -> None:
        with tempfile.TemporaryDirectory() as d:
            p = Path(d) / "plan.md"
            p.write_text("# Plan", encoding="utf-8")
            tam = Mock(return_value={"ok": True, "result": {"message_id": 999}})
            with patch.object(herdres, "telegram_api_multipart", tam):
                res = herdres.send_document("-1001", p, caption_text="📎 Plan",
                                            thread_id="77", reply_to_message_id="500", api_token="tok")
            self.assertTrue(res["ok"])
            method, fields, files = tam.call_args.args
            self.assertEqual(method, "sendDocument")
            self.assertEqual(fields["document"], "attach://file")
            self.assertEqual(fields["chat_id"], "-1001")
            self.assertEqual(fields["message_thread_id"], "77")
            self.assertIn("reply_parameters", fields)
            self.assertEqual(fields["caption"], "📎 Plan")
            self.assertEqual(files["file"][0], p)
            self.assertEqual(files["file"][1], "text/markdown")
            self.assertEqual(tam.call_args.kwargs["token"], "tok")

    # --- deliver (queue) / flush (send) dedup + retry ---
    def test_deliver_queues_and_flush_sends_once_per_turn(self) -> None:
        entry = {"topic_id": "77", "space_key": "s", "pane_key": "p"}
        state = {"spaces": {}, "panes": {}}
        sent = []
        with tempfile.TemporaryDirectory() as d:
            with patch.object(herdres, "state_path", Mock(return_value=Path(d) / "state.json")), \
                 patch.object(herdres, "record_pane_message_route", Mock()), \
                 patch.object(herdres, "send_document",
                              Mock(side_effect=lambda *a, **k: sent.append(k.get("reply_to_message_id"))
                                   or {"ok": True, "result": {"message_id": "9"}})):
                self.assertTrue(herdres.deliver_plan_document(entry, turn_id="t1",
                                plan_text="# Plan\nbody", reply_to_message_id="500"))
                self.assertIn("pending_plan_doc", entry)
                self.assertEqual(len(sent), 0)  # deliver only queues
                self.assertTrue(herdres.flush_pending_plan_doc(state, entry, {}, "-1001", api_token="tok"))
                self.assertEqual(len(sent), 1)
                self.assertEqual(entry.get("last_plan_doc_turn_id"), "t1")
                self.assertNotIn("pending_plan_doc", entry)  # sent + cleared
                # re-deliver the same turn -> no new queue
                self.assertFalse(herdres.deliver_plan_document(entry, turn_id="t1",
                                 plan_text="# Plan\nbody", reply_to_message_id="500"))
                herdres.flush_pending_plan_doc(state, entry, {}, "-1001", api_token="tok")
        self.assertEqual(len(sent), 1)

    def test_flush_retries_then_drops_on_persistent_failure(self) -> None:
        entry = {"topic_id": "77", "space_key": "s", "pane_key": "p"}
        state = {"spaces": {}, "panes": {}}
        with tempfile.TemporaryDirectory() as d:
            with patch.object(herdres, "state_path", Mock(return_value=Path(d) / "state.json")), \
                 patch.object(herdres, "send_document", Mock(return_value={"ok": False})):
                herdres.deliver_plan_document(entry, turn_id="t2", plan_text="# Plan", reply_to_message_id="1")
                self.assertIn("pending_plan_doc", entry)
                for _ in range(herdres.PLAN_DOC_ATTEMPT_CAP):
                    herdres.flush_pending_plan_doc(state, entry, {}, "-1001", api_token="tok")
        self.assertNotIn("pending_plan_doc", entry)  # gave up after the cap
        self.assertNotEqual(entry.get("last_plan_doc_turn_id"), "t2")

    def test_flush_rate_limited_keeps_queue_without_burning_attempt(self) -> None:
        entry = {"topic_id": "77", "space_key": "s", "pane_key": "p"}
        state = {"spaces": {}, "panes": {}}
        with tempfile.TemporaryDirectory() as d:
            with patch.object(herdres, "state_path", Mock(return_value=Path(d) / "state.json")), \
                 patch.object(herdres, "send_document", Mock(side_effect=herdres.RateLimited(30))):
                herdres.deliver_plan_document(entry, turn_id="t3", plan_text="# Plan", reply_to_message_id="1")
                changed = herdres.flush_pending_plan_doc(state, entry, {}, "-1001", api_token="tok")
        self.assertFalse(changed)  # rate-limit: no state change
        self.assertIn("pending_plan_doc", entry)  # kept for next sync
        self.assertEqual(int(entry["pending_plan_doc"].get("attempts") or 0), 0)  # attempt not burned

    def test_clear_clean_feed_state_clears_plan_doc(self) -> None:
        entry = {"last_plan_doc_turn_id": "t1", "pending_plan_doc": {"turn_id": "t1"}}
        herdres.clear_clean_feed_state(entry)
        self.assertNotIn("last_plan_doc_turn_id", entry)
        self.assertNotIn("pending_plan_doc", entry)


if __name__ == "__main__":
    unittest.main()
