from __future__ import annotations

import json
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
    def test_topic_cleanup_report_is_dry_run_and_uses_topic_refs(self) -> None:
        state = orphan_state(topic_id="77")
        state["panes"]["pseudo"] = {
            "pane_key": "pseudo",
            "source": "tendwire",
            "pane_id": "tendwire:worker-1",
            "topic_id": "88",
            "last_known_status": "working",
        }
        state["spaces"]["workspace:empty"] = {
            "space_key": "workspace:empty",
            "topic_id": "99",
            "pane_keys": [],
        }
        delete_topic = Mock(return_value=True)

        with patch.object(herdres, "load_dotenv"), \
                patch.object(herdres, "load_state", return_value=state), \
                patch.object(herdres, "delete_topic", delete_topic), \
                patch.object(herdres, "save_state") as save_state:
            report = herdres.topic_cleanup_report_once()

        self.assertTrue(report["ok"])
        self.assertTrue(report["dry_run"])
        self.assertFalse(report["changed"])
        self.assertEqual(report["counts"]["pseudo_panes"], 1)
        self.assertGreaterEqual(report["counts"]["orphan_spaces"], 1)
        encoded = json.dumps(report, sort_keys=True)
        self.assertNotIn('"77"', encoded)
        self.assertNotIn('"88"', encoded)
        self.assertNotIn('"99"', encoded)
        delete_topic.assert_not_called()
        save_state.assert_not_called()

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

    def test_topic_id_invalid_classifies_as_topic_not_found(self) -> None:
        # Telegram returns TOPIC_ID_INVALID when the forum topic is already deleted; it MUST be
        # treated as "topic gone" so the prune removes the orphan instead of retrying forever.
        exc = herdres.BridgeError("Telegram deleteForumTopic failed: Bad Request: TOPIC_ID_INVALID")
        self.assertEqual(herdres.classify_telegram_error(exc), "topic_not_found")

    def test_prune_removes_orphan_when_topic_already_deleted(self) -> None:
        # Regression for the prune looping on already-deleted topics (each deleteForumTopic was a slow
        # 20s TOPIC_ID_INVALID that never cleared the mapping, ballooning syncs to ~100s+).
        state = orphan_state()
        boom = Mock(side_effect=herdres.BridgeError(
            "Telegram deleteForumTopic failed: Bad Request: TOPIC_ID_INVALID"))
        with patch.object(herdres, "delete_topic", boom), patch.object(
            herdres, "utc_now", Mock(return_value=FIXED_NOW)
        ):
            pruned = herdres.prune_orphan_spaces(
                state, "-1001", Mock(name="telegram"), {"still-live"}, [], delete_cap=5,
            )
        self.assertEqual(pruned, 1)            # removed, not skipped-and-retried
        self.assertEqual(state["spaces"], {})  # orphan space gone from state
        self.assertEqual(state["panes"], {})
        self.assertEqual(state["deleted_orphan_topics"][0]["status"], "topic_not_found")

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


class CleanupHardeningTests(unittest.TestCase):
    """Issue #79: a non-'gone' delete failure must CONSUME the per-run cap and ABANDON the topic after
    N attempts, so a stuck/throttled topic can't be retried every sync (the #56 outage class)."""

    def _prune(self, state, **kw):
        return herdres.prune_orphan_spaces(state, "-1001", Mock(name="telegram"), {"still-live"}, [], **kw)

    def test_failed_delete_consumes_cap_and_abandons_after_limit(self):
        # The headline regression: today the old bare `continue` returned 0 (cap not consumed) and the
        # topic was retried forever; the fix consumes the cap each failure and gives up after the limit.
        boom = Mock(side_effect=herdres.RateLimited(20))  # the 429 class that caused the outage
        state = orphan_state()
        with patch.object(herdres, "delete_topic", boom), patch.object(herdres, "utc_now", Mock(return_value=FIXED_NOW)):
            for attempt in (1, 2):  # below the default limit (3)
                pruned = self._prune(state, delete_cap=5)
                self.assertEqual(pruned, 1, "a failed delete must consume the per-run cap")
                self.assertIn("workspace:dead-workspace", state["spaces"], "must NOT orphan a maybe-live topic")
                self.assertEqual(state["cleanup_topic_attempts"]["77"]["attempts"], attempt)
                self.assertFalse(state["cleanup_topic_attempts"]["77"].get("abandoned"))
            self._prune(state, delete_cap=5)  # attempt 3 == limit
            self.assertTrue(state["cleanup_topic_attempts"]["77"]["abandoned"])
            calls = boom.call_count
            self._prune(state, delete_cap=5)  # abandoned -> skipped, NO further Telegram call (loop-breaker)
            self.assertEqual(boom.call_count, calls)

    def test_cap_bounds_total_delete_calls_under_a_failing_backlog(self):
        # 30 dead orphans all rate-limited, cap=12 -> at most 12 deleteForumTopic calls in one sync
        # (failures consume the cap). This is the lock-starvation bound the outage lacked.
        state = orphan_state()
        spaces, panes = state["spaces"], state["panes"]
        for i in range(29):
            sk = f"workspace:dead-{i}"
            spaces[sk] = {"space_key": sk, "space_id": f"dead-{i}", "topic_id": str(1000 + i),
                          "topic_name": f"x{i}", "pane_keys": [f"pane:dead-{i}"]}
            panes[f"pane:dead-{i}"] = {"pane_key": f"pane:dead-{i}", "space_key": sk,
                                       "workspace_id": f"dead-{i}", "last_known_status": "closed"}
        boom = Mock(side_effect=herdres.RateLimited(20))
        with patch.object(herdres, "delete_topic", boom), patch.object(herdres, "utc_now", Mock(return_value=FIXED_NOW)):
            self._prune(state, delete_cap=12)
        self.assertLessEqual(boom.call_count, 12)

    def test_each_non_gone_error_bucket_consumes_cap(self):
        for exc, kind in [
            (herdres.RateLimited(5), "rate_limited"),
            (herdres.BridgeError("Telegram deleteForumTopic failed: Bad Request: CHAT_ADMIN_REQUIRED"), "bad_request"),
            (herdres.BridgeError("boom"), "transient"),
        ]:
            with self.subTest(kind=kind):
                state = orphan_state()
                with patch.object(herdres, "delete_topic", Mock(side_effect=exc)), \
                     patch.object(herdres, "utc_now", Mock(return_value=FIXED_NOW)):
                    pruned = self._prune(state, delete_cap=5)
                self.assertEqual(pruned, 1)
                self.assertEqual(state["cleanup_topic_attempts"]["77"]["last_error_kind"], kind)
                self.assertIn("workspace:dead-workspace", state["spaces"])

    def test_delete_returns_false_consumes_cap_without_removing(self):
        state = orphan_state()
        with patch.object(herdres, "delete_topic", Mock(return_value=False)), \
             patch.object(herdres, "utc_now", Mock(return_value=FIXED_NOW)):
            pruned = self._prune(state, delete_cap=5)
        self.assertEqual(pruned, 1)
        self.assertIn("workspace:dead-workspace", state["spaces"])
        self.assertEqual(state["cleanup_topic_attempts"]["77"]["attempts"], 1)

    def test_topic_not_found_removes_and_clears_attempts(self):
        state = orphan_state()
        state.setdefault("cleanup_topic_attempts", {})["77"] = {"topic_id": "77", "attempts": 2}
        boom = Mock(side_effect=herdres.BridgeError("Telegram deleteForumTopic failed: Bad Request: TOPIC_ID_INVALID"))
        with patch.object(herdres, "delete_topic", boom), patch.object(herdres, "utc_now", Mock(return_value=FIXED_NOW)):
            pruned = self._prune(state, delete_cap=5)
        self.assertEqual(pruned, 1)
        self.assertEqual(state["spaces"], {})            # removed (gone == success)
        self.assertNotIn("77", state.get("cleanup_topic_attempts", {}))  # prior attempts cleared

    def test_protected_topics_never_pruned(self):
        state = orphan_state(topic_id="1")               # General thread id
        state["telegram"]["general_thread_id"] = "1"
        delete = Mock(return_value=True)
        with patch.object(herdres, "delete_topic", delete):
            pruned = self._prune(state, delete_cap=5)
        self.assertEqual(pruned, 0)
        delete.assert_not_called()
        self.assertIn("workspace:dead-workspace", state["spaces"])

    def test_result_out_param_reports_counts(self):
        state = orphan_state()
        stats: dict = {}
        with patch.object(herdres, "delete_topic", Mock(side_effect=herdres.RateLimited(5))), \
             patch.object(herdres, "utc_now", Mock(return_value=FIXED_NOW)):
            self._prune(state, delete_cap=5, result=stats)
        self.assertEqual(stats["failed"], 1)
        self.assertEqual(stats["last_error_kind"], "rate_limited")

    def test_tier0_drops_already_gone_record_without_telegram(self):
        # A space whose topic is already audited-gone + no live pane is dropped with NO delete_topic call.
        state = orphan_state()
        state["deleted_orphan_topics"] = [{"topic_id": "77", "status": "deleted"}]
        delete = Mock(return_value=True)
        with patch.object(herdres, "delete_topic", delete):
            removed = herdres.reconcile_known_gone_spaces(state)
        self.assertEqual(removed, 1)
        self.assertEqual(state["spaces"], {})
        delete.assert_not_called()

    def test_tier0_skips_protected_and_unknown_topics(self):
        state = orphan_state()
        state["telegram"]["general_thread_id"] = "77"     # now protected
        state["deleted_orphan_topics"] = [{"topic_id": "77", "status": "deleted"}]
        removed = herdres.reconcile_known_gone_spaces(state)
        self.assertEqual(removed, 0)
        self.assertIn("workspace:dead-workspace", state["spaces"])

    def test_live_backref_pane_blocks_prune(self):
        # A pane that points at the orphan space by space_key but ISN'T closed (e.g. migrated/renumbered
        # and live elsewhere) must NOT be swept away — and the space must not be pruned.
        state = orphan_state()
        state["panes"]["migrated"] = {"pane_key": "migrated", "space_key": "workspace:dead-workspace",
                                      "workspace_id": "live-elsewhere", "last_known_status": "working"}
        delete = Mock(return_value=True)
        with patch.object(herdres, "delete_topic", delete):
            pruned = self._prune(state, delete_cap=5)
        self.assertEqual(pruned, 0)
        delete.assert_not_called()
        self.assertIn("migrated", state["panes"])

    def test_tier0_live_backref_pane_blocks_removal(self):
        state = orphan_state()
        state["deleted_orphan_topics"] = [{"topic_id": "77", "status": "deleted"}]
        state["panes"]["migrated"] = {"pane_key": "migrated", "space_key": "workspace:dead-workspace",
                                      "last_known_status": "idle"}
        removed = herdres.reconcile_known_gone_spaces(state)
        self.assertEqual(removed, 0)
        self.assertIn("workspace:dead-workspace", state["spaces"])
        self.assertIn("migrated", state["panes"])


if __name__ == "__main__":
    unittest.main()
