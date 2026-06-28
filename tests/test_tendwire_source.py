from __future__ import annotations

import json
import os
import subprocess
import unittest
from unittest.mock import Mock, patch

import herdres


def _pane(**extra) -> dict:
    pane = {
        "pane_id": "pane-1",
        "terminal_id": "term-1",
        "workspace_id": "workspace-1",
        "tab_id": "workspace-1:t1",
        "agent": "codex",
        "agent_status": "working",
        "label": "Codex",
        "name": "Codex",
        "foreground_cwd": "/home/smith/herdres",
        "space_name": "Workers",
        "workspace_label": "Workers",
    }
    pane.update(extra)
    return pane


def _snapshot(*workers: dict) -> dict:
    if not workers:
        workers = ({
            "id": "worker-1",
            "space_id": "workspace-1",
            "name": "codex",
            "status": "active",
            "status_line": "Working on tests",
            "last_seen_at": "2026-06-28T12:00:00+00:00",
            "fingerprint": "fp-1",
            "meta": {
                "agent": "codex",
                "tab_id": "workspace-1:t1",
                "cwd": "/home/smith/herdres",
                "foreground_cwd": "/home/smith/herdres",
            },
        },)
    return {
        "schema_version": 2,
        "host_id": "host-1",
        "spaces": [{"id": "workspace-1", "name": "Workers", "status": "active"}],
        "workers": list(workers),
    }


class TendwireHybridTests(unittest.TestCase):
    def test_tendwire_enriches_real_pane_without_replacing_id(self) -> None:
        panes = herdres.tendwire_enrich_panes([_pane()], _snapshot())

        self.assertEqual(len(panes), 1)
        self.assertEqual(panes[0]["pane_id"], "pane-1")
        self.assertFalse(str(panes[0]["pane_id"]).startswith("tendwire:"))
        self.assertTrue(panes[0]["_tendwire_enriched"])
        self.assertEqual(panes[0]["_tendwire_worker_id"], "worker-1")
        self.assertEqual(panes[0]["summary"], "Working on tests")

    def test_tendwire_ambiguous_match_is_ignored(self) -> None:
        worker = _snapshot()["workers"][0]
        duplicate = dict(worker, id="worker-2", fingerprint="fp-2")

        panes = herdres.tendwire_enrich_panes([_pane()], _snapshot(worker, duplicate))

        self.assertEqual(panes[0]["pane_id"], "pane-1")
        self.assertNotIn("_tendwire_enriched", panes[0])

    def test_observed_agent_panes_still_uses_herdr_pane_list_when_enabled(self) -> None:
        proc = subprocess.CompletedProcess(
            ["tendwire", "snapshot", "--json"],
            0,
            stdout=json.dumps(_snapshot()),
            stderr="",
        )
        pane_list = Mock(return_value=[_pane()])
        with patch.dict(os.environ, {"HERDRES_TENDWIRE_SNAPSHOT": "1"}, clear=False), \
                patch.object(herdres, "run_cmd", return_value=proc) as run_cmd, \
                patch.object(herdres, "pane_list", pane_list):
            panes = herdres.observed_agent_panes()

        pane_list.assert_called_once()
        run_cmd.assert_called_once()
        self.assertEqual([pane["pane_id"] for pane in panes], ["pane-1"])
        self.assertTrue(panes[0]["_tendwire_enriched"])

    def test_observed_agent_panes_falls_back_to_herdr_when_tendwire_fails(self) -> None:
        proc = subprocess.CompletedProcess(["tendwire", "snapshot", "--json"], 1, stdout="", stderr="boom")
        herdr_panes = [_pane()]
        with patch.dict(os.environ, {"HERDRES_TENDWIRE_SNAPSHOT": "1"}, clear=False), \
                patch.object(herdres, "run_cmd", return_value=proc), \
                patch.object(herdres, "pane_list", return_value=herdr_panes):
            panes = herdres.observed_agent_panes()

        self.assertEqual(panes, herdr_panes)

    def test_sync_pane_once_tendwire_enriched_pane_uses_clean_feed(self) -> None:
        pane = herdres.tendwire_enrich_panes([_pane()], _snapshot())[0]
        state: dict = {"panes": {}, "spaces": {}}
        counters = herdres.make_sync_counters()
        caps = herdres.make_sync_caps()
        clean_result = {
            "early_return": None,
            "changed": True,
            "feed_delivered": True,
            "stream_active": False,
        }
        with patch.object(herdres, "CLEAN_FEED_ENABLED", True), \
                patch.object(herdres, "LIVE_CARD_ENABLED", False), \
                patch.object(herdres, "STATUS_MARKER_ENABLED", False), \
                patch.object(herdres, "STATUS_ICON_ENABLED", False), \
                patch.object(herdres, "ensure_space_topic", return_value=({"topic_id": "77"}, False)), \
                patch.object(herdres, "ensure_pane_root_message", return_value=(False, {"ok": True})), \
                patch.object(herdres, "_sync_pane_clean_feed", return_value=clean_result) as clean_feed:
            changed = herdres.sync_pane_once(state, "-100", {}, pane, counters, caps)

        self.assertTrue(changed)
        clean_feed.assert_called_once()
        entry = state["panes"][herdres.pane_key(pane)]
        self.assertEqual(entry["source"], "herdr")
        self.assertEqual(entry["pane_id"], "pane-1")
        self.assertEqual(entry["tendwire_worker_id"], "worker-1")

    def test_sync_once_prefetches_real_pane_id_for_tendwire_enriched_pane(self) -> None:
        state: dict = {"enabled": True, "panes": {}, "spaces": {}, "telegram": {"chat_id": "-100"}}
        pane = herdres.tendwire_enrich_panes([_pane()], _snapshot())[0]
        with patch.object(herdres, "load_dotenv"), \
                patch.object(herdres, "load_state", return_value=state), \
                patch.object(herdres, "save_state"), \
                patch.object(herdres, "observed_agent_panes", return_value=[pane]), \
                patch.object(herdres, "sync_closed_pane_records", return_value={"changed": False, "sent": 0}), \
                patch.object(herdres, "workspace_label_map", return_value={"workspace-1": "Workers"}), \
                patch.object(herdres, "reconcile_known_gone_spaces", return_value=0), \
                patch.object(herdres, "prune_orphan_spaces", return_value=0), \
                patch.object(herdres, "preflight_is_fresh", return_value=True), \
                patch.object(herdres, "reconcile_pinned_status_views", return_value={"changed": False, "updated": 0}), \
                patch.object(herdres, "ensure_managed_bot_setup_message", return_value=False), \
                patch.object(herdres, "ensure_managed_bot_group_access_message", return_value=False), \
                patch.object(herdres, "ensure_multibot_offer_message", return_value=False), \
                patch.object(herdres, "prefetch_pane_turns") as prefetch, \
                patch.object(herdres, "update_topic_icons_for_spaces"), \
                patch.object(herdres, "sync_pane_once", return_value=False), \
                patch.object(herdres, "ensure_devin_glm_space_seats", return_value={"changed": False, "started": 0}), \
                patch.object(herdres, "TURN_FEED_ENABLED", True):
            result = herdres.sync_once()

        self.assertTrue(result["ok"])
        prefetch.assert_called_once_with(["pane-1"])

    def test_tendwire_enriched_entry_send_uses_real_pane_id(self) -> None:
        entry = {"source": "herdr", "tendwire_worker_id": "worker-1", "pane_id": "pane-1"}
        with patch.object(herdres, "send_to_pane", return_value=(True, "queued")) as send_to_pane, \
                patch.object(herdres, "save_state") as save_state:
            result = herdres.forward_text_to_pane_response(
                "pane-1",
                "continue",
                state={"panes": {}},
                entry=entry,
            )

        send_to_pane.assert_called_once_with("pane-1", "continue")
        save_state.assert_called_once()
        self.assertEqual(result["reply"], "queued")

    def test_stale_tendwire_only_entry_remains_read_only(self) -> None:
        entry = {"source": "tendwire", "pane_id": "tendwire:worker-1", "tendwire_worker_id": "worker-1"}
        with patch.object(herdres, "send_to_pane") as send_to_pane:
            result = herdres.forward_text_to_pane_response(
                "tendwire:worker-1",
                "continue",
                state={"panes": {}},
                entry=entry,
            )

        send_to_pane.assert_not_called()
        self.assertIn("Tendwire status entry", result["reply"])

    def test_drop_tendwire_source_pane_records_removes_only_pseudo_entries(self) -> None:
        state = {
            "panes": {
                "stale": {
                    "source": "tendwire",
                    "pane_id": "tendwire:worker-1",
                    "space_key": "workspace:w1",
                    "topic_id": "77",
                },
                "live": {
                    "source": "herdr",
                    "pane_id": "pane-1",
                    "tendwire_worker_id": "worker-1",
                    "space_key": "workspace:w1",
                },
            },
            "spaces": {
                "workspace:w1": {"pane_keys": ["stale", "live"]},
            },
        }

        removed = herdres.drop_tendwire_source_pane_records(state)

        self.assertEqual(removed, 1)
        self.assertNotIn("stale", state["panes"])
        self.assertIn("live", state["panes"])
        self.assertEqual(state["spaces"]["workspace:w1"]["pane_keys"], ["live"])
        self.assertEqual(state["deleted_tendwire_source_panes"][0]["pane_key"], "stale")

    def test_agent_picker_pending_send_uses_real_pane_id_for_enriched_entry(self) -> None:
        entry = {
            "source": "herdr",
            "tendwire_worker_id": "worker-1",
            "pane_id": "pane-1",
            "agent": "codex",
            "last_known_status": "working",
        }
        pane_key = "pane-key-1"
        space = {
            "pane_keys": [pane_key],
            "pending_pick": {
                "42": {"text": "continue", "set_at": herdres.utc_now()},
            },
        }
        state = {"panes": {pane_key: entry}, "spaces": {"space": space}}
        token = herdres.agent_picker_pane_tokens([(pane_key, entry)])[pane_key]

        with patch.object(herdres, "send_to_pane", return_value=(True, "")) as send_to_pane, \
                patch.object(herdres, "save_state") as save_state, \
                patch.object(herdres, "telegram_api") as telegram_api:
            result = herdres.handle_agent_pick_callback(
                state,
                {},
                "-100",
                "77",
                "123",
                "42",
                space,
                ["herdr", "pick", "space", token],
            )

        send_to_pane.assert_called_once_with("pane-1", "continue")
        save_state.assert_called_once_with(state)
        telegram_api.assert_called_once()
        self.assertTrue(result["answer"].startswith("Sent to"))
        self.assertNotIn("42", space["pending_pick"])


if __name__ == "__main__":
    unittest.main()
