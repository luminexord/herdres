from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

import herdres


FIXED_NOW = "2026-01-01T00:00:00Z"


def orphan_state(*, origin: str | None = None, workspace_id: str = "dead-workspace", topic_id: str = "77") -> dict:
    space_key = f"workspace:{workspace_id}"
    pane_key = f"pane:{workspace_id}"
    space = {
        "space_key": space_key,
        "space_id": workspace_id,
        "topic_id": topic_id,
        "topic_name": "Council · deadwork",
        "pane_keys": [pane_key],
    }
    if origin is not None:
        space["origin"] = origin
    return {
        "version": 1,
        "telegram": {"chat_id": "-1001", "general_thread_id": "1", "owner_user_ids": ["42"]},
        "spaces": {space_key: space},
        "panes": {
            pane_key: {
                "pane_key": pane_key,
                "pane_id": "pane-dead",
                "space_key": space_key,
                "workspace_id": workspace_id,
                "topic_id": topic_id,
                "pane_thread_name": "council-codex",
                "last_known_status": "closed",
            },
            "stale-extra": {
                "pane_key": "stale-extra",
                "pane_id": "pane-extra",
                "space_key": space_key,
                "last_known_status": "closed",
            },
        },
    }


def pane_for(*, workspace_id: str = "wabcdef123456", pane_id: str = "pane-1", terminal_id: str = "term-1", label: str = "council-codex") -> dict:
    return {
        "workspace_id": workspace_id,
        "tab_id": "tab-1",
        "pane_id": pane_id,
        "terminal_id": terminal_id,
        "agent": "codex",
        "label": label,
    }


class OrphanPruneLifecycleTests(unittest.TestCase):
    def test_prune_ended_space_deletes_topic_removes_state_and_audits(self) -> None:
        state = orphan_state()
        delete_topic = Mock(return_value=True)

        with patch.object(herdres, "delete_topic", delete_topic), patch.object(
            herdres, "utc_now", Mock(return_value=FIXED_NOW)
        ):
            pruned = herdres.prune_orphan_spaces(
                state,
                "-1001",
                Mock(name="telegram"),
                {"still-live"},
                [],
                delete_cap=5,
            )

        self.assertEqual(pruned, 1)
        delete_topic.assert_called_once_with("-1001", "77")
        self.assertEqual(state["spaces"], {})
        self.assertEqual(state["panes"], {})
        self.assertEqual(
            state["deleted_orphan_topics"],
            [
                {
                    "space_key": "workspace:dead-workspace",
                    "workspace_id": "dead-workspace",
                    "topic_id": "77",
                    "topic_name": "Council · deadwork",
                    "reason": "workspace_not_found",
                    "status": "deleted",
                    "pruned_at": FIXED_NOW,
                }
            ],
        )

    def test_mirror_backup_prevents_regeneration_when_state_json_is_corrupt(self) -> None:
        state = orphan_state(workspace_id="from-bak", topic_id="88")

        with tempfile.TemporaryDirectory() as tmp, patch.dict(
            "os.environ", {"HERDR_TELEGRAM_TOPICS_STATE": str(Path(tmp) / "state.json")}
        ):
            state_path = Path(tmp) / "state.json"
            herdres.save_state(state)
            with patch.object(herdres, "delete_topic", Mock(return_value=True)), patch.object(
                herdres, "utc_now", Mock(return_value=FIXED_NOW)
            ):
                pruned = herdres.prune_orphan_spaces(
                    state,
                    "-1001",
                    Mock(name="telegram"),
                    {"still-live"},
                    [],
                    delete_cap=5,
                )
            self.assertEqual(pruned, 1)
            herdres.save_state(state, mirror_bak=True)
            state_path.write_text("{ not json", encoding="utf-8")

            loaded = herdres.load_state()

        self.assertNotIn("workspace:from-bak", loaded["spaces"])
        self.assertEqual(loaded["deleted_orphan_topics"][0]["space_key"], "workspace:from-bak")

    def test_keeps_spaces_for_live_workspace_and_live_pane(self) -> None:
        live_workspace_state = orphan_state(workspace_id="live-workspace")
        live_pane = pane_for(workspace_id="pane-live-workspace", pane_id="live-pane")
        live_pane_key = herdres.pane_key(live_pane)
        live_pane_state = orphan_state(workspace_id="pane-live-workspace")
        live_space_key_pane = pane_for(workspace_id="space-key-live-workspace", pane_id="renumbered-pane")
        live_space_key_state = orphan_state(workspace_id="space-key-live-workspace")
        live_pane_state["spaces"]["workspace:pane-live-workspace"]["pane_keys"] = [live_pane_key]
        live_pane_state["panes"] = {
            live_pane_key: {
                "pane_key": live_pane_key,
                "pane_id": "live-pane",
                "space_key": "workspace:pane-live-workspace",
                "workspace_id": "pane-live-workspace",
                "last_known_status": "closed",
            }
        }

        with patch.object(herdres, "delete_topic", Mock(return_value=True)) as delete_topic, patch.object(
            herdres, "per_agent_topics_enabled", Mock(return_value=False)
        ):
            self.assertEqual(
                herdres.prune_orphan_spaces(
                    live_workspace_state,
                    "-1001",
                    Mock(name="telegram"),
                    {"live-workspace"},
                    [],
                    delete_cap=5,
                ),
                0,
            )
            self.assertEqual(
                herdres.prune_orphan_spaces(
                    live_pane_state,
                    "-1001",
                    Mock(name="telegram"),
                    {"other-live-workspace"},
                    [live_pane],
                    delete_cap=5,
                ),
                0,
            )
            self.assertEqual(
                herdres.prune_orphan_spaces(
                    live_space_key_state,
                    "-1001",
                    Mock(name="telegram"),
                    {"other-live-workspace"},
                    [live_space_key_pane],
                    delete_cap=5,
                ),
                0,
            )

        delete_topic.assert_not_called()
        self.assertIn("workspace:live-workspace", live_workspace_state["spaces"])
        self.assertIn("workspace:pane-live-workspace", live_pane_state["spaces"])
        self.assertIn("workspace:space-key-live-workspace", live_space_key_state["spaces"])

    def test_empty_live_workspace_ids_is_fail_safe(self) -> None:
        state = orphan_state()

        with patch.object(herdres, "delete_topic", Mock(return_value=True)) as delete_topic:
            pruned = herdres.prune_orphan_spaces(state, "-1001", Mock(name="telegram"), set(), [])

        self.assertEqual(pruned, 0)
        delete_topic.assert_not_called()
        self.assertIn("workspace:dead-workspace", state["spaces"])

    def test_personal_spaces_are_never_pruned(self) -> None:
        state = orphan_state(origin="personal")

        with patch.object(herdres, "delete_topic", Mock(return_value=True)) as delete_topic:
            pruned = herdres.prune_orphan_spaces(
                state,
                "-1001",
                Mock(name="telegram"),
                {"still-live"},
                [],
                delete_cap=5,
            )

        self.assertEqual(pruned, 0)
        delete_topic.assert_not_called()
        self.assertIn("workspace:dead-workspace", state["spaces"])


class CouncilLifecycleContractTests(unittest.TestCase):
    def test_council_topic_name_uses_workspace_root_and_non_council_label_is_unchanged(self) -> None:
        state = {"spaces": {}, "panes": {}}
        council = pane_for(workspace_id="wabcdef123456", label="council-codex")
        personal = {
            "workspace_id": "personal-workspace",
            "workspace_label": "Personal Project",
            "tab_id": "tab-1",
            "pane_id": "personal-pane",
            "terminal_id": "term-personal",
            "agent": "codex",
            "label": "Codex",
        }

        with patch.object(herdres, "per_agent_topics_enabled", Mock(return_value=False)):
            _council_key, council_entry, _ = herdres.ensure_space_entry(state, council)
            _personal_key, personal_entry, _ = herdres.ensure_space_entry(state, personal)

        self.assertEqual(council_entry["topic_name"], "Council · abcdef12")
        self.assertEqual(personal_entry["topic_name"], "Personal Project")

    def test_voice_mode_defaults_to_per_agent_for_council_and_shared_otherwise(self) -> None:
        state = {"spaces": {}, "panes": {}}
        council = pane_for(workspace_id="wabcdef123456", label="council-kimi")
        non_council = {
            "workspace_id": "shared-workspace",
            "workspace_label": "Shared Project",
            "tab_id": "tab-1",
            "pane_id": "shared-pane",
            "terminal_id": "term-shared",
            "agent": "codex",
            "label": "Codex",
        }
        personal = {
            "workspace_id": "personal-workspace",
            "workspace_label": "Personal Project",
            "tab_id": "tab-1",
            "pane_id": "personal-pane",
            "terminal_id": "term-personal",
            "agent": "codex",
            "label": "Personal Codex",
        }

        with patch.object(herdres, "per_agent_topics_enabled", Mock(return_value=False)):
            _council_key, council_entry, _ = herdres.ensure_space_entry(state, council)
            _non_key, non_council_entry, _ = herdres.ensure_space_entry(state, non_council)
            _personal_key, personal_entry, _ = herdres.ensure_space_entry(state, personal)

        self.assertEqual(council_entry["voice_mode"], "per_agent")
        self.assertEqual(non_council_entry["voice_mode"], "shared")
        self.assertEqual(personal_entry["voice_mode"], "shared")

    def test_managed_bot_kind_for_entry_resolves_council_labels(self) -> None:
        for label, expected in {
            "council-codex": "codex",
            "council-kimi": "kimi",
            "council-omp": "omp",
            "council-glm": "glm",
        }.items():
            with self.subTest(label=label):
                self.assertEqual(herdres.managed_bot_kind_for_entry({"label": label}), expected)

    def test_renumbered_same_council_seat_replaces_pane_key_instead_of_appending(self) -> None:
        state = {"spaces": {}, "panes": {}}
        old_pane = pane_for(pane_id="pane-1", terminal_id="term-1", label="council-codex")
        new_pane = pane_for(pane_id="pane-2", terminal_id="term-2", label="council-codex")

        with patch.object(herdres, "per_agent_topics_enabled", Mock(return_value=False)):
            old_key, _old_entry, _ = herdres.ensure_pane_entry(state, old_pane)
            new_key, _new_entry, _ = herdres.ensure_pane_entry(state, new_pane)

        space = state["spaces"]["workspace:wabcdef123456"]
        self.assertNotEqual(old_key, new_key)
        self.assertEqual(space["pane_keys"], [new_key])
        self.assertEqual(len(space["pane_keys"]), 1)


if __name__ == "__main__":
    unittest.main()
