from __future__ import annotations

import io
import json
import os
import subprocess
import sys
import tempfile
import unittest
from contextlib import ExitStack
from pathlib import Path
from unittest.mock import Mock, patch

import herdres
import herdres_tendwire


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


def _degraded_snapshot() -> dict:
    return {
        "schema_version": 2,
        "host_id": "host-1",
        "spaces": [{"id": "workspace-1", "name": "Workers", "status": "active"}],
        "workers": [],
        "backend_health": [
            {
                "name": "herdr",
                "status": "degraded",
                "outcome": "timeout",
                "message": "Herdr observation is degraded",
            }
        ],
    }


def _source_state() -> tuple[dict, str]:
    pane = herdres.tendwire_source_read_panes(_snapshot())[0]
    key = herdres.pane_key(pane)
    state = {
        "enabled": True,
        "telegram": {"chat_id": "-100", "general_thread_id": "1"},
        "spaces": {
            "agent:worker:worker-1": {
                "space_key": "agent:worker:worker-1",
                "pane_keys": [key],
                "topic_id": "77",
                "topic_name": "Workers",
            },
        },
        "panes": {
            key: {
                "pane_key": key,
                "source": "tendwire",
                "entry_type": "worker",
                "worker_id": "worker-1",
                "worker_fingerprint": "fp-1",
                "pane_id": "",
                "terminal_id": "",
                "agent": "codex",
                "workspace": "workspace-1",
                "tab": "workspace-1:t1",
                "space_key": "agent:worker:worker-1",
                "topic_id": "77",
                "topic_name": "Workers",
                "last_known_status": "working",
                "tendwire_worker_id": "worker-1",
                "tendwire_fingerprint": "fp-1",
                "tendwire_status_line": "Working on tests",
                "tendwire_last_seen_at": "2026-06-28T12:00:00+00:00",
            },
        },
    }
    return state, key


class TendwireModeTests(unittest.TestCase):
    def test_parse_tendwire_mode_defaults_to_off(self) -> None:
        self.assertEqual(herdres.parse_tendwire_mode({}), "off")

    def test_parse_tendwire_mode_accepts_public_modes_case_insensitively(self) -> None:
        cases = {
            " off ": "off",
            " ENRICH ": "enrich",
            " Commands ": "commands",
            " Source-Read ": "source-read",
            "SOURCE": "source",
        }
        for raw, expected in cases.items():
            with self.subTest(raw=raw):
                self.assertEqual(herdres.parse_tendwire_mode({"HERDRES_TENDWIRE_MODE": raw}), expected)

    def test_command_capable_modes_still_enable_enrichment(self) -> None:
        for mode in ("commands", "source-read", "source"):
            with self.subTest(mode=mode), patch.dict(os.environ, {"HERDRES_TENDWIRE_MODE": mode}, clear=True):
                self.assertEqual(herdres.tendwire_mode(), mode)
                self.assertTrue(herdres.tendwire_enrich_enabled())
                self.assertTrue(herdres.tendwire_snapshot_enabled())
                self.assertTrue(herdres.tendwire_commands_enabled())

    def test_source_mode_enables_connector_outbox_by_default(self) -> None:
        self.assertFalse(herdres.tendwire_connector_outbox_enabled({"HERDRES_TENDWIRE_MODE": "source-read"}))
        self.assertFalse(herdres.tendwire_connector_outbox_enabled({"HERDRES_TENDWIRE_MODE": "commands"}))
        self.assertTrue(herdres.tendwire_connector_outbox_enabled({"HERDRES_TENDWIRE_MODE": "source"}))
        self.assertFalse(
            herdres.tendwire_connector_outbox_enabled(
                {"HERDRES_TENDWIRE_MODE": "source", "HERDRES_TENDWIRE_CONNECTOR_OUTBOX": "0"}
            )
        )
        self.assertTrue(
            herdres.tendwire_connector_outbox_enabled(
                {"HERDRES_TENDWIRE_MODE": "source-read", "HERDRES_TENDWIRE_CONNECTOR_OUTBOX": "1"}
            )
        )

    def test_legacy_aliases_normalize_to_enrich_when_mode_unset(self) -> None:
        self.assertEqual(herdres.parse_tendwire_mode({"HERDRES_TENDWIRE_HYBRID": "1"}), "enrich")
        self.assertEqual(herdres.parse_tendwire_mode({"HERDRES_TENDWIRE_SNAPSHOT": "1"}), "enrich")
        self.assertEqual(
            herdres.parse_tendwire_mode({"HERDRES_TENDWIRE_HYBRID": "0", "HERDRES_TENDWIRE_SNAPSHOT": "0"}),
            "off",
        )

    def test_legacy_aliases_do_not_enable_command_routing(self) -> None:
        for key in ("HERDRES_TENDWIRE_HYBRID", "HERDRES_TENDWIRE_SNAPSHOT"):
            with self.subTest(key=key), patch.dict(os.environ, {key: "1"}, clear=True):
                self.assertEqual(herdres.tendwire_mode(), "enrich")
                self.assertTrue(herdres.tendwire_enrich_enabled())
                self.assertFalse(herdres.tendwire_commands_enabled())

    def test_explicit_valid_mode_wins_over_legacy_aliases(self) -> None:
        env = {
            "HERDRES_TENDWIRE_MODE": "off",
            "HERDRES_TENDWIRE_HYBRID": "1",
            "HERDRES_TENDWIRE_SNAPSHOT": "1",
        }
        self.assertEqual(herdres.parse_tendwire_mode(env), "off")
        env["HERDRES_TENDWIRE_MODE"] = "source-read"
        self.assertEqual(herdres.parse_tendwire_mode(env), "source-read")

    def test_invalid_modes_fall_back_to_off_with_diagnostic(self) -> None:
        stderr = io.StringIO()
        env = {"HERDRES_TENDWIRE_MODE": "hybrid"}
        with patch("sys.stderr", stderr):
            self.assertEqual(herdres.parse_tendwire_mode(env, diagnose_invalid=True), "off")
        text = stderr.getvalue()
        self.assertIn("invalid HERDRES_TENDWIRE_MODE 'hybrid'", text)
        self.assertIn("off, enrich, commands, source-read, source", text)
        for invalid in ("snapshot", "enabled", ""):
            with self.subTest(invalid=invalid):
                self.assertEqual(herdres.parse_tendwire_mode({"HERDRES_TENDWIRE_MODE": invalid}), "off")

    def test_off_and_invalid_modes_do_not_call_tendwire(self) -> None:
        for mode in ("off", "hybrid"):
            with self.subTest(mode=mode), \
                    patch.dict(os.environ, {"HERDRES_TENDWIRE_MODE": mode}, clear=True), \
                    patch.object(herdres, "run_cmd") as run_cmd, \
                    patch.object(herdres, "pane_list", return_value=[_pane()]):
                stderr = io.StringIO()
                with patch("sys.stderr", stderr):
                    panes = herdres.observed_agent_panes()

            run_cmd.assert_not_called()
            self.assertEqual([pane["pane_id"] for pane in panes], ["pane-1"])
            self.assertNotIn("_tendwire_enriched", panes[0])
            self.assertFalse(str(panes[0]["pane_id"]).startswith("tendwire:"))

    def test_command_capable_modes_call_tendwire_snapshot_for_enrichment(self) -> None:
        for mode in ("commands",):
            proc = subprocess.CompletedProcess(
                ["tendwire", "snapshot", "--json"],
                0,
                stdout=json.dumps(_snapshot()),
                stderr="",
            )
            with self.subTest(mode=mode), \
                    patch.dict(os.environ, {"HERDRES_TENDWIRE_MODE": mode}, clear=True), \
                    patch.object(herdres, "run_cmd", return_value=proc) as run_cmd, \
                    patch.object(herdres, "pane_list", return_value=[_pane()]):
                panes = herdres.observed_agent_panes()

            run_cmd.assert_called_once()
            self.assertEqual([pane["pane_id"] for pane in panes], ["pane-1"])
            self.assertTrue(panes[0]["_tendwire_enriched"])
            self.assertEqual(panes[0]["_tendwire_worker_id"], "worker-1")

    def test_source_read_observed_panes_use_tendwire_snapshot_without_herdr_pane_list(self) -> None:
        proc = subprocess.CompletedProcess(
            ["tendwire", "snapshot", "--json"],
            0,
            stdout=json.dumps(_snapshot()),
            stderr="",
        )
        with patch.dict(os.environ, {"HERDRES_TENDWIRE_MODE": "source-read"}, clear=True), \
                patch.object(herdres, "run_cmd", return_value=proc) as run_cmd, \
                patch.object(herdres, "pane_list") as pane_list:
            panes = herdres.observed_agent_panes()

        pane_list.assert_not_called()
        run_cmd.assert_called_once()
        self.assertEqual(len(panes), 1)
        pane = panes[0]
        self.assertEqual(pane["pane_id"], "")
        self.assertEqual(pane["entry_type"], "worker")
        self.assertEqual(pane["worker_id"], "worker-1")
        self.assertEqual(pane["worker_fingerprint"], "fp-1")
        self.assertFalse(str(pane["pane_id"]).startswith("tendwire:"))
        self.assertEqual(pane["source"], "tendwire")
        self.assertEqual(herdres.space_key(pane), "workspace:workspace-1")
        self.assertTrue(pane["_tendwire_source_read"])
        self.assertTrue(pane["_tendwire_enriched"])
        self.assertEqual(pane["_tendwire_worker_id"], "worker-1")
        self.assertEqual(pane["_tendwire_fingerprint"], "fp-1")
        self.assertEqual(pane["terminal_id"], "")
        self.assertEqual(pane["summary"], "Working on tests")

    def test_source_read_worker_pane_key_survives_fingerprint_change(self) -> None:
        first = herdres.tendwire_source_read_panes(
            _snapshot(
                {
                    "id": "worker-1",
                    "space_id": "workspace-1",
                    "name": "codex",
                    "status": "active",
                    "fingerprint": "fp-old",
                    "meta": {"agent": "codex", "tab_id": "workspace-1:t1"},
                }
            )
        )[0]
        second = herdres.tendwire_source_read_panes(
            _snapshot(
                {
                    "id": "worker-1",
                    "space_id": "workspace-1",
                    "name": "codex",
                    "status": "active",
                    "fingerprint": "fp-new",
                    "meta": {"agent": "codex", "tab_id": "workspace-1:t1"},
                }
            )
        )[0]

        self.assertEqual(herdres.pane_key(first), herdres.pane_key(second))
        self.assertEqual(first["worker_fingerprint"], "fp-old")
        self.assertEqual(second["worker_fingerprint"], "fp-new")

    def test_source_read_raw_herdr_space_id_does_not_become_topic_title(self) -> None:
        panes = herdres.tendwire_source_read_panes(
            {
                "spaces": [{"id": "w653e50b41be581", "name": "w653e50b41be581"}],
                "workers": [
                    {
                        "id": "worker-1",
                        "space_id": "w653e50b41be581",
                        "name": "codex",
                        "status": "active",
                        "fingerprint": "fp-1",
                        "meta": {"agent": "codex", "cwd": "/home/smith/tendwire"},
                    }
                ],
            }
        )

        self.assertEqual(len(panes), 1)
        self.assertEqual(panes[0]["space_name"], "")
        self.assertEqual(panes[0]["workspace_label"], "")
        self.assertEqual(herdres.space_name_for_pane(panes[0]), "Tendwire")

    def test_source_observed_panes_use_tendwire_snapshot_without_herdr_pane_list(self) -> None:
        proc = subprocess.CompletedProcess(
            ["tendwire", "snapshot", "--json"],
            0,
            stdout=json.dumps(_snapshot()),
            stderr="",
        )
        with patch.dict(os.environ, {"HERDRES_TENDWIRE_MODE": "source"}, clear=True), \
                patch.object(herdres, "run_cmd", return_value=proc) as run_cmd, \
                patch.object(herdres, "pane_list") as pane_list:
            panes = herdres.observed_agent_panes()

        pane_list.assert_not_called()
        run_cmd.assert_called_once()
        self.assertEqual(len(panes), 1)
        self.assertEqual(panes[0]["entry_type"], "worker")
        self.assertEqual(panes[0]["worker_id"], "worker-1")
        self.assertFalse(str(panes[0]["pane_id"]).startswith("tendwire:"))

    def test_source_read_panes_skip_closed_tendwire_workers(self) -> None:
        panes = herdres.tendwire_source_read_panes(
            _snapshot(
                {
                    "id": "worker-live",
                    "space_id": "workspace-1",
                    "name": "codex",
                    "status": "active",
                    "fingerprint": "fp-live",
                },
                {
                    "id": "worker-closed",
                    "space_id": "workspace-1",
                    "name": "codex",
                    "status": "closed",
                    "fingerprint": "fp-closed",
                },
            )
        )

        self.assertEqual([pane["worker_id"] for pane in panes], ["worker-live"])

    def test_source_read_degraded_snapshot_preserves_existing_source_entries(self) -> None:
        state, key = _source_state()
        proc = subprocess.CompletedProcess(
            ["tendwire", "snapshot", "--json"],
            0,
            stdout=json.dumps(_degraded_snapshot()),
            stderr="",
        )
        with patch.dict(os.environ, {"HERDRES_TENDWIRE_MODE": "source-read"}, clear=True), \
                patch.object(herdres, "run_cmd", return_value=proc) as run_cmd, \
                patch.object(herdres, "pane_list") as pane_list:
            panes = herdres.observed_agent_panes(state=state)

        pane_list.assert_not_called()
        run_cmd.assert_called_once()
        self.assertEqual([herdres.pane_key(pane) for pane in panes], [key])
        self.assertTrue(panes[0]["_tendwire_preserved_from_state"])
        self.assertEqual(state["tendwire_source_inventory_preserved"], 1)
        self.assertIn("tendwire_source_inventory_degraded_at", state)

        closed = herdres.sync_closed_pane_records(
            state,
            "-100",
            {},
            panes,
            sends=0,
            max_sends=0,
        )
        self.assertFalse(closed["changed"])
        self.assertEqual(state["panes"][key]["last_known_status"], "working")

    def test_source_read_removed_worker_closes_entry_without_closed_notice(self) -> None:
        state, key = _source_state()
        with patch.dict(os.environ, {"HERDRES_TENDWIRE_MODE": "source"}, clear=True), \
                patch.object(herdres, "send_notice") as send_notice:
            closed = herdres.sync_closed_pane_records(
                state,
                "-100",
                {},
                [],
                sends=0,
                max_sends=10,
            )

        self.assertTrue(closed["changed"])
        self.assertEqual(closed["sent"], 0)
        self.assertEqual(state["panes"][key]["last_known_status"], "closed")
        self.assertIn("closed_notice_suppressed_at", state["panes"][key])
        send_notice.assert_not_called()

    def test_prune_closed_tendwire_source_records_removes_space_membership(self) -> None:
        state, key = _source_state()
        active_key = "worker:active"
        state["panes"][key]["last_known_status"] = "closed"
        state["panes"][active_key] = {
            **state["panes"][key],
            "pane_key": active_key,
            "worker_id": "worker-active",
            "tendwire_worker_id": "worker-active",
            "last_known_status": "working",
        }
        state["spaces"]["agent:worker:worker-1"]["pane_keys"] = [key, active_key]

        removed = herdres.prune_closed_tendwire_source_records(state)

        self.assertEqual(removed, 1)
        self.assertNotIn(key, state["panes"])
        self.assertIn(active_key, state["panes"])
        self.assertEqual(state["spaces"]["agent:worker:worker-1"]["pane_keys"], [active_key])

    def test_prune_closed_tendwire_source_records_preserves_live_space_topic(self) -> None:
        state, key = _source_state()
        state["panes"][key]["last_known_status"] = "closed"
        state["spaces"]["agent:worker:worker-1"]["pane_keys"] = [key]

        removed = herdres.prune_closed_tendwire_source_records(
            state,
            active_space_keys={"agent:worker:worker-1"},
        )

        self.assertEqual(removed, 1)
        self.assertNotIn(key, state["panes"])
        self.assertIn("agent:worker:worker-1", state["spaces"])
        self.assertEqual(state["spaces"]["agent:worker:worker-1"]["topic_id"], "77")
        self.assertEqual(state["spaces"]["agent:worker:worker-1"]["pane_keys"], [])

    def test_source_read_clean_feed_delivers_tendwire_turn_without_direct_herdr(self) -> None:
        state, key = _source_state()
        pane = herdres.tendwire_source_read_panes(_snapshot())[0]
        entry = state["panes"][key]
        entry["prompt_collapse_chars"] = 0
        counters = {"sends": 0, "feed_sends": 0}
        sent_items: list[dict] = []
        turns_payload = {
            "schema_version": 1,
            "turns": [
                {
                    "id": "turn-public-1",
                    "worker_id": "worker-1",
                    "worker_fingerprint": "fp-1",
                    "user_text": "Why does Telegram show closed status?",
                    "assistant_final_text": "Because Herdres was skipping Tendwire turn text.",
                    "assistant_stream_text": "Checked source mode.",
                    "complete": True,
                    "has_open_turn": True,
                }
            ],
        }

        def fake_send_feed_item(chat_id: str, item: dict, **kwargs) -> dict:
            sent_items.append(item)
            return {"ok": True, "message_id": "501"}

        with patch.dict(os.environ, {"HERDRES_TENDWIRE_MODE": "source"}, clear=True), \
                patch.object(herdres, "TURN_FEED_ENABLED", True), \
                patch.object(herdres, "tendwire_turns", return_value=turns_payload), \
                patch.object(herdres, "extract_turn_feed_item") as extract_turn_feed_item, \
                patch.object(herdres, "cached_pane_turn") as cached_pane_turn, \
                patch.object(herdres, "pane_feed_output") as pane_feed_output, \
                patch.object(herdres, "send_pending_prompt_message", return_value={"changed": False}), \
                patch.object(herdres, "send_feed_item", side_effect=fake_send_feed_item), \
                patch.object(herdres, "fold_superseded_turns", return_value=False), \
                patch.object(herdres, "flush_pending_plan_doc", return_value=False), \
                patch.object(herdres, "flush_pending_speech_reply", return_value=False):
            result = herdres._sync_pane_clean_feed(
                state,
                "-100",
                {},
                pane,
                entry,
                counters,
                pane_api_token=None,
                turn_only=False,
                new_entry=False,
                max_sends=10,
                max_feed_sends=10,
                stable_obj_hash="status-hash",
                changed=False,
            )

        self.assertIsNone(result["early_return"])
        self.assertTrue(result["feed_delivered"])
        self.assertEqual(counters["sends"], 1)
        self.assertEqual(counters["feed_sends"], 1)
        self.assertEqual(sent_items[0]["turn_id"], "turn-public-1")
        self.assertIn("Herdres was skipping Tendwire turn text", sent_items[0]["assistant_final_text"])
        self.assertEqual(entry["last_clean_message_id"], "501")
        extract_turn_feed_item.assert_not_called()
        cached_pane_turn.assert_not_called()
        pane_feed_output.assert_not_called()

    def test_sync_once_source_delivers_completed_tendwire_turn_without_direct_herdr(self) -> None:
        state = {
            "enabled": True,
            "telegram": {"chat_id": "-100", "general_thread_id": "1"},
            "spaces": {
                "workspace:workspace-1": {
                    "space_key": "workspace:workspace-1",
                    "topic_id": "77",
                    "topic_name": "Workers",
                    "pane_keys": [],
                    "last_topic_verified_at": "9999-01-01T00:00:00+00:00",
                }
            },
            "panes": {},
        }
        turns_payload = {
            "schema_version": 1,
            "turns": [
                {
                    "id": "turn-public-1",
                    "worker_id": "worker-1",
                    "worker_fingerprint": "fp-1",
                    "user_text": "Summarize the source-mode delivery.",
                    "assistant_final_text": "Completed Tendwire turn text reached Telegram.",
                    "assistant_stream_text": "",
                    "complete": True,
                    "has_open_turn": False,
                }
            ],
        }
        sent_items: list[dict] = []

        def fake_send_feed_item(chat_id: str, item: dict, **kwargs) -> dict:
            sent_items.append(item)
            return {"ok": True, "message_id": "501"}

        with ExitStack() as stack:
            stack.enter_context(patch.dict(os.environ, {"HERDRES_TENDWIRE_MODE": "source"}, clear=True))
            stack.enter_context(patch.object(herdres, "load_dotenv"))
            stack.enter_context(patch.object(herdres, "load_state", return_value=state))
            save_state = stack.enter_context(patch.object(herdres, "save_state"))
            stack.enter_context(patch.object(herdres, "tendwire_snapshot", return_value=_snapshot()))
            stack.enter_context(patch.object(herdres, "tendwire_turns", return_value=turns_payload))
            outbox = stack.enter_context(
                patch.object(
                    herdres,
                    "drain_tendwire_connector_outbox",
                    return_value={"changed": False, "polled": 0},
                )
            )
            pane_list = stack.enter_context(patch.object(herdres, "pane_list"))
            pane_turn = stack.enter_context(patch.object(herdres, "pane_turn"))
            prefetch = stack.enter_context(patch.object(herdres, "prefetch_pane_turns"))
            send_to_pane = stack.enter_context(patch.object(herdres, "send_to_pane"))
            labels = stack.enter_context(patch.object(herdres, "workspace_label_map"))
            stack.enter_context(patch.object(herdres, "reconcile_known_gone_spaces", return_value=0))
            stack.enter_context(patch.object(herdres, "prune_orphan_spaces", return_value=0))
            stack.enter_context(patch.object(herdres, "preflight_is_fresh", return_value=True))
            stack.enter_context(
                patch.object(herdres, "reconcile_pinned_status_views", return_value={"changed": False, "updated": 0})
            )
            stack.enter_context(patch.object(herdres, "ensure_managed_bot_setup_message", return_value=False))
            stack.enter_context(patch.object(herdres, "ensure_managed_bot_group_access_message", return_value=False))
            stack.enter_context(patch.object(herdres, "ensure_multibot_offer_message", return_value=False))
            stack.enter_context(patch.object(herdres, "update_topic_icons_for_spaces"))
            stack.enter_context(patch.object(herdres, "ensure_pane_root_message", return_value=(False, {"ok": True})))
            stack.enter_context(patch.object(herdres, "send_pending_prompt_message", return_value={"changed": False}))
            stack.enter_context(patch.object(herdres, "send_feed_item", side_effect=fake_send_feed_item))
            stack.enter_context(patch.object(herdres, "fold_superseded_turns", return_value=False))
            stack.enter_context(patch.object(herdres, "flush_pending_plan_doc", return_value=False))
            stack.enter_context(patch.object(herdres, "flush_pending_speech_reply", return_value=False))
            create_topic = stack.enter_context(patch.object(herdres, "create_topic"))
            stack.enter_context(patch.object(herdres, "TURN_FEED_ENABLED", True))
            stack.enter_context(patch.object(herdres, "CLEAN_FEED_ENABLED", True))
            stack.enter_context(patch.object(herdres, "LIVE_CARD_ENABLED", False))
            stack.enter_context(patch.object(herdres, "STATUS_MARKER_ENABLED", False))
            stack.enter_context(patch.object(herdres, "STATUS_ICON_ENABLED", False))
            result = herdres.sync_once()

        self.assertTrue(result["ok"])
        self.assertEqual(result["feed_sent"], 1)
        self.assertEqual(result["sent"], 1)
        self.assertEqual(sent_items[0]["turn_id"], "turn-public-1")
        self.assertIn("Completed Tendwire turn text", sent_items[0]["assistant_final_text"])
        self.assertEqual(state["panes"][herdres.pane_key(herdres.tendwire_source_read_panes(_snapshot())[0])]["last_clean_message_id"], "501")
        outbox.assert_called_once()
        pane_list.assert_not_called()
        pane_turn.assert_not_called()
        prefetch.assert_not_called()
        send_to_pane.assert_not_called()
        labels.assert_not_called()
        create_topic.assert_not_called()
        save_state.assert_called()

    def test_source_read_snapshot_failure_preserves_existing_source_entries(self) -> None:
        state, key = _source_state()
        proc = subprocess.CompletedProcess(["tendwire", "snapshot", "--json"], 1, stdout="", stderr="socket down")
        with patch.dict(os.environ, {"HERDRES_TENDWIRE_MODE": "source-read"}, clear=True), \
                patch.object(herdres, "run_cmd", return_value=proc), \
                patch.object(herdres, "pane_list") as pane_list:
            panes = herdres.observed_agent_panes(state=state)

        pane_list.assert_not_called()
        self.assertEqual([herdres.pane_key(pane) for pane in panes], [key])
        self.assertTrue(panes[0]["_tendwire_preserved_from_state"])
        self.assertIn("socket down", state["tendwire_source_inventory_last_error"])
        self.assertIn("tendwire_source_inventory_preserved_at", state)

    def test_source_read_degraded_snapshot_preserves_legacy_pseudo_source_entries(self) -> None:
        state = {
            "panes": {
                "legacy": {
                    "pane_key": "legacy",
                    "source": "tendwire",
                    "pane_id": "tendwire:worker-legacy",
                    "agent": "codex",
                    "last_known_status": "working",
                    "topic_id": "77",
                },
            },
        }
        proc = subprocess.CompletedProcess(
            ["tendwire", "snapshot", "--json"],
            0,
            stdout=json.dumps(_degraded_snapshot()),
            stderr="",
        )
        with patch.dict(os.environ, {"HERDRES_TENDWIRE_MODE": "source-read"}, clear=True), \
                patch.object(herdres, "run_cmd", return_value=proc):
            panes = herdres.observed_agent_panes(state=state)

        self.assertEqual([herdres.pane_key(pane) for pane in panes], ["legacy"])
        self.assertEqual(panes[0]["worker_id"], "worker-legacy")
        self.assertEqual(panes[0]["pane_id"], "")

    def test_tendwire_helper_reconstructs_open_source_state_panes(self) -> None:
        state, key = _source_state()
        state["panes"]["closed"] = {
            **state["panes"][key],
            "pane_key": "closed",
            "worker_id": "worker-closed",
            "tendwire_worker_id": "worker-closed",
            "last_known_status": "closed",
        }
        state["panes"]["direct"] = {
            "pane_key": "direct",
            "pane_id": "pane-direct",
            "last_known_status": "working",
        }

        panes = herdres_tendwire.source_state_panes(
            state,
            is_source_entry=herdres.entry_is_tendwire_source,
        )

        self.assertEqual([pane["worker_id"] for pane in panes], ["worker-1"])
        self.assertTrue(panes[0]["_tendwire_preserved_from_state"])
        self.assertEqual(panes[0]["pane_id"], "")

    def test_tendwire_helper_merges_preserved_source_panes_without_duplicates(self) -> None:
        state, key = _source_state()
        state["panes"]["worker:preserved"] = {
            **state["panes"][key],
            "pane_key": "worker:preserved",
            "worker_id": "worker-preserved",
            "tendwire_worker_id": "worker-preserved",
            "worker_fingerprint": "fp-preserved",
            "tendwire_fingerprint": "fp-preserved",
        }
        live = herdres.tendwire_source_read_panes(_snapshot())
        preserved = herdres_tendwire.source_state_panes(
            state,
            is_source_entry=herdres.entry_is_tendwire_source,
        )

        merged, preserved_count = herdres_tendwire.merge_preserved_source_panes(
            live,
            preserved,
            pane_key=herdres.pane_key,
        )

        self.assertEqual([pane["worker_id"] for pane in merged], ["worker-1", "worker-preserved"])
        self.assertEqual(preserved_count, 1)


class TendwireConfigTests(unittest.TestCase):
    def test_child_env_preserves_parent_and_overrides_only_tendwire_keys(self) -> None:
        parent = {
            "PATH": "/bin:/usr/bin",
            "HOME": "/tmp/herdres-home",
            "SSH_AUTH_SOCK": "/tmp/ssh.sock",
            "DEPLOYMENT_FLAG": "kept",
            "HERDR_REAL_BIN": "/opt/herdr-real",
            "HERDR_BIN": "/opt/herdr",
            "HERDRES_TENDWIRE_HERDR_TIMEOUT_SECONDS": "2.5",
            "HERDRES_TENDWIRE_DATA_DIR": "~/tw-data",
            "HERDRES_TENDWIRE_DB_PATH": "$HOME/tw/db.sqlite",
            "HERDRES_TENDWIRE_HOST_ID": "host-a",
            "TENDWIRE_HERDR_BIN": "old-herdr",
            "TENDWIRE_HERDR_TIMEOUT_SECONDS": "9",
            "TENDWIRE_DATA_DIR": "old-data",
            "TENDWIRE_DB_PATH": "old-db",
            "TENDWIRE_HOST_ID": "old-host",
        }

        child = herdres.tendwire_child_env(parent)

        self.assertEqual(child["PATH"], parent["PATH"])
        self.assertEqual(child["HOME"], parent["HOME"])
        self.assertEqual(child["SSH_AUTH_SOCK"], parent["SSH_AUTH_SOCK"])
        self.assertEqual(child["DEPLOYMENT_FLAG"], parent["DEPLOYMENT_FLAG"])
        self.assertEqual(child["TENDWIRE_HERDR_BIN"], "/opt/herdr-real")
        self.assertEqual(child["TENDWIRE_HERDR_TIMEOUT_SECONDS"], "2.5")
        self.assertEqual(child["TENDWIRE_DATA_DIR"], "/tmp/herdres-home/tw-data")
        self.assertEqual(child["TENDWIRE_DB_PATH"], "/tmp/herdres-home/tw/db.sqlite")
        self.assertEqual(child["TENDWIRE_HOST_ID"], "host-a")
        changed = {key for key, value in child.items() if parent.get(key) != value}
        changed.update(key for key in parent if key not in child)
        self.assertLessEqual(
            changed,
            {
                "TENDWIRE_HERDR_BIN",
                "TENDWIRE_HERDR_TIMEOUT_SECONDS",
                "TENDWIRE_DATA_DIR",
                "TENDWIRE_DB_PATH",
                "TENDWIRE_HOST_ID",
            },
        )

    def test_herdr_bin_precedence_for_tendwire(self) -> None:
        self.assertEqual(
            herdres.tendwire_herdr_bin({"HERDR_REAL_BIN": "/real/herdr", "HERDR_BIN": "/configured/herdr"}),
            "/real/herdr",
        )
        self.assertEqual(herdres.tendwire_herdr_bin({"HERDR_BIN": "/configured/herdr"}), "/configured/herdr")
        self.assertEqual(herdres.tendwire_herdr_bin({}), "herdr")

    def test_invalid_inner_timeout_falls_back_to_default(self) -> None:
        for raw in ("nope", "0", "-2", "nan", "inf"):
            with self.subTest(raw=raw):
                env = {"HERDRES_TENDWIRE_HERDR_TIMEOUT_SECONDS": raw}
                self.assertEqual(herdres.tendwire_herdr_timeout_seconds(env), 1.0)
                self.assertEqual(herdres.tendwire_env_overrides(env)["TENDWIRE_HERDR_TIMEOUT_SECONDS"], "1.0")

    def test_optional_tendwire_values_are_passed_only_when_configured(self) -> None:
        parent = {
            "PATH": "/bin",
            "TENDWIRE_DATA_DIR": "old-data",
            "TENDWIRE_DB_PATH": "old-db",
            "TENDWIRE_HOST_ID": "old-host",
        }

        child = herdres.tendwire_child_env(parent)
        overrides = herdres.tendwire_env_overrides(parent)

        self.assertNotIn("TENDWIRE_DATA_DIR", child)
        self.assertNotIn("TENDWIRE_DB_PATH", child)
        self.assertNotIn("TENDWIRE_HOST_ID", child)
        self.assertNotIn("TENDWIRE_DATA_DIR", overrides)
        self.assertNotIn("TENDWIRE_DB_PATH", overrides)
        self.assertNotIn("TENDWIRE_HOST_ID", overrides)

    def test_tendwire_command_base_expands_path_like_executable_and_preserves_args(self) -> None:
        env = {
            "HOME": "/tmp/herdres-home",
            "HERDRES_TENDWIRE_BIN": "~/bin/tendwire --profile local --json-log",
        }

        self.assertEqual(
            herdres.tendwire_command_base(env),
            ["/tmp/herdres-home/bin/tendwire", "--profile", "local", "--json-log"],
        )

    def test_tendwire_snapshot_passes_explicit_child_env(self) -> None:
        proc = subprocess.CompletedProcess(["tendwire", "snapshot", "--json"], 0, stdout=json.dumps(_snapshot()), stderr="")
        env = {
            "HERDRES_TENDWIRE_MODE": "enrich",
            "HERDR_REAL_BIN": "/opt/herdr-real",
            "HERDR_BIN": "/opt/herdr",
            "HERDRES_TENDWIRE_HERDR_TIMEOUT_SECONDS": "1.5",
            "HERDRES_TENDWIRE_TIMEOUT_SECONDS": "4",
        }
        with patch.dict(os.environ, env, clear=True), patch.object(herdres, "run_cmd", return_value=proc) as run_cmd:
            data = herdres.tendwire_snapshot()

        self.assertEqual(data["host_id"], "host-1")
        run_cmd.assert_called_once()
        self.assertEqual(run_cmd.call_args.args[0], ["tendwire", "snapshot", "--json"])
        self.assertEqual(run_cmd.call_args.kwargs["timeout"], 4)
        child_env = run_cmd.call_args.kwargs["env"]
        self.assertEqual(child_env["TENDWIRE_HERDR_BIN"], "/opt/herdr-real")
        self.assertEqual(child_env["TENDWIRE_HERDR_TIMEOUT_SECONDS"], "1.5")

    def test_diagnostic_config_json_is_valid_and_sanitized(self) -> None:
        env = {
            "HOME": "/tmp/herdres-home",
            "HERDRES_TENDWIRE_MODE": "enrich",
            "HERDRES_TENDWIRE_BIN": "~/bin/tendwire --profile local",
            "HERDR_BIN": "/usr/local/bin/herdr",
            "HERDRES_TENDWIRE_TIMEOUT_SECONDS": "1",
            "HERDRES_TENDWIRE_HERDR_TIMEOUT_SECONDS": "2",
            "HERDRES_TENDWIRE_DATA_DIR": "$HOME/tendwire",
            "HERDRES_TENDWIRE_DB_PATH": "~/tendwire/db.sqlite",
            "HERDRES_TENDWIRE_HOST_ID": "host-a",
            "TELEGRAM_BOT_TOKEN": "123456:" + "A" * 35,
        }
        stdout = io.StringIO()
        with patch.dict(os.environ, env, clear=True), \
                patch.object(herdres, "load_dotenv"), \
                patch.object(sys, "argv", ["herdres", "tendwire", "config"]), \
                patch("sys.stdout", stdout):
            rc = herdres.main()

        self.assertEqual(rc, 0)
        payload = json.loads(stdout.getvalue())
        config = payload["config"]
        for key in (
            "tendwire_mode",
            "tendwire_bin",
            "tendwire_db_path",
            "tendwire_data_dir",
            "tendwire_herdr_bin",
            "tendwire_timeout_seconds",
            "tendwire_herdr_timeout_seconds",
        ):
            self.assertIn(key, config)
        self.assertEqual(config["tendwire_mode"], "enrich")
        self.assertEqual(config["tendwire_bin"], "/tmp/herdres-home/bin/tendwire --profile local")
        self.assertEqual(config["tendwire_data_dir"], "/tmp/herdres-home/tendwire")
        self.assertEqual(config["tendwire_db_path"], "/tmp/herdres-home/tendwire/db.sqlite")
        self.assertEqual(config["tendwire_host_id"], "host-a")
        self.assertEqual(config["tendwire_herdr_bin"], "/usr/local/bin/herdr")
        self.assertTrue(config["warnings"])
        text = stdout.getvalue()
        self.assertNotIn("TELEGRAM_BOT_TOKEN", text)
        self.assertNotIn(env["TELEGRAM_BOT_TOKEN"], text)


class TendwireHybridTests(unittest.TestCase):
    def test_source_turn_for_pane_prefers_matching_worker_fingerprint(self) -> None:
        pane = {
            "worker_id": "worker-1",
            "worker_fingerprint": "fp-current",
        }
        payload = {
            "turns": [
                {"id": "old", "worker_id": "worker-1", "worker_fingerprint": "fp-old"},
                {"id": "current", "worker_id": "worker-1", "worker_fingerprint": "fp-current"},
                {"id": "other", "worker_id": "worker-2", "worker_fingerprint": "fp-current"},
            ],
        }

        turn = herdres_tendwire.source_turn_for_pane(pane, payload)

        self.assertIsNotNone(turn)
        self.assertEqual(turn["id"], "current")

    def test_source_turn_for_pane_falls_back_to_worker_match_without_fingerprint(self) -> None:
        pane = {"_tendwire_worker_id": "worker-1", "_tendwire_fingerprint": "missing"}
        payload = {
            "turns": [
                {"id": "fallback", "worker_id": "worker-1", "worker_fingerprint": "fp-old"},
                {"id": "other", "worker_id": "worker-2", "worker_fingerprint": "missing"},
            ],
        }

        turn = herdres_tendwire.source_turn_for_pane(pane, payload)

        self.assertIsNotNone(turn)
        self.assertEqual(turn["id"], "fallback")

    def test_source_turn_feed_source_sanitizes_and_defaults_public_turn_fields(self) -> None:
        def sanitizer(value: str, limit: int) -> str:
            return value.replace("secret", "[redacted]")[:limit]

        source = herdres_tendwire.source_turn_feed_source(
            {
                "fingerprint": "turn-fp",
                "user_text": "user secret prompt",
                "assistant_final_text": "assistant secret final",
                "assistant_stream_text": "stream secret text",
            },
            sanitize=sanitizer,
            final_reply_max_chars=18,
            user_prompt_max_chars=16,
        )

        self.assertEqual(source["turn_id"], "turn-fp")
        self.assertEqual(source["user_text"], "user [redacted]")
        self.assertEqual(source["assistant_final_text"], "assistant [redacte")
        self.assertEqual(source["assistant_stream_text"], "stream [redacted]")
        self.assertTrue(source["complete"])
        self.assertFalse(source["has_open_turn"])

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

    def test_observed_agent_panes_still_uses_herdr_pane_list_when_mode_is_enrich(self) -> None:
        proc = subprocess.CompletedProcess(
            ["tendwire", "snapshot", "--json"],
            0,
            stdout=json.dumps(_snapshot()),
            stderr="",
        )
        pane_list = Mock(return_value=[_pane()])
        with patch.dict(os.environ, {"HERDRES_TENDWIRE_MODE": "enrich"}, clear=True), \
                patch.object(herdres, "run_cmd", return_value=proc) as run_cmd, \
                patch.object(herdres, "pane_list", pane_list):
            panes = herdres.observed_agent_panes()

        pane_list.assert_called_once()
        run_cmd.assert_called_once()
        self.assertEqual([pane["pane_id"] for pane in panes], ["pane-1"])
        self.assertFalse(any(str(pane["pane_id"]).startswith("tendwire:") for pane in panes))
        self.assertTrue(panes[0]["_tendwire_enriched"])

    def test_observed_agent_panes_legacy_aliases_still_enable_enrichment(self) -> None:
        for key in ("HERDRES_TENDWIRE_HYBRID", "HERDRES_TENDWIRE_SNAPSHOT"):
            proc = subprocess.CompletedProcess(
                ["tendwire", "snapshot", "--json"],
                0,
                stdout=json.dumps(_snapshot()),
                stderr="",
            )
            with self.subTest(key=key), \
                    patch.dict(os.environ, {key: "1"}, clear=True), \
                    patch.object(herdres, "run_cmd", return_value=proc) as run_cmd, \
                    patch.object(herdres, "pane_list", return_value=[_pane()]):
                panes = herdres.observed_agent_panes()

            run_cmd.assert_called_once()
            self.assertEqual([pane["pane_id"] for pane in panes], ["pane-1"])
            self.assertTrue(panes[0]["_tendwire_enriched"])

    def test_observed_agent_panes_falls_back_to_herdr_when_tendwire_fails(self) -> None:
        proc = subprocess.CompletedProcess(["tendwire", "snapshot", "--json"], 1, stdout="", stderr="boom")
        herdr_panes = [_pane()]
        with patch.dict(os.environ, {"HERDRES_TENDWIRE_MODE": "enrich"}, clear=True), \
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

    def test_sync_once_source_read_skips_herdr_inventory_helpers(self) -> None:
        state: dict = {"enabled": True, "panes": {}, "spaces": {}, "telegram": {"chat_id": "-100"}}
        pane = herdres.tendwire_source_read_panes(_snapshot())[0]
        with patch.dict(os.environ, {"HERDRES_TENDWIRE_MODE": "source-read"}, clear=True), \
                patch.object(herdres, "load_dotenv"), \
                patch.object(herdres, "load_state", return_value=state), \
                patch.object(herdres, "save_state"), \
                patch.object(herdres, "observed_agent_panes", return_value=[pane]), \
                patch.object(herdres, "sync_closed_pane_records", return_value={"changed": False, "sent": 0}), \
                patch.object(herdres, "drop_tendwire_source_pane_records", return_value=1) as drop_stale, \
                patch.object(herdres, "drain_tendwire_connector_outbox", return_value={"changed": False}), \
                patch.object(herdres, "workspace_label_map", return_value={"workspace-1": "Workers"}) as labels, \
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
                patch.object(herdres, "ensure_devin_glm_space_seats") as ensure_devin, \
                patch.object(herdres, "TURN_FEED_ENABLED", True):
            result = herdres.sync_once()

        self.assertTrue(result["ok"])
        self.assertEqual(result["panes"], 1)
        self.assertEqual(result["stale_tendwire_pruned"], 0)
        drop_stale.assert_not_called()
        labels.assert_not_called()
        prefetch.assert_not_called()
        ensure_devin.assert_not_called()

    def test_sync_once_source_read_degraded_snapshot_does_not_close_existing_source_entry(self) -> None:
        state, key = _source_state()
        proc = subprocess.CompletedProcess(
            ["tendwire", "snapshot", "--json"],
            0,
            stdout=json.dumps(_degraded_snapshot()),
            stderr="",
        )
        with patch.dict(os.environ, {"HERDRES_TENDWIRE_MODE": "source-read"}, clear=True), \
                patch.object(herdres, "load_dotenv"), \
                patch.object(herdres, "load_state", return_value=state), \
                patch.object(herdres, "save_state") as save_state, \
                patch.object(herdres, "run_cmd", return_value=proc), \
                patch.object(herdres, "drop_tendwire_source_pane_records") as drop_stale, \
                patch.object(herdres, "drain_tendwire_connector_outbox", return_value={"changed": False}), \
                patch.object(herdres, "workspace_label_map") as labels, \
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
                patch.object(herdres, "ensure_devin_glm_space_seats") as ensure_devin, \
                patch.object(herdres, "send_notice") as send_notice, \
                patch.object(herdres, "TURN_FEED_ENABLED", True):
            result = herdres.sync_once()

        self.assertTrue(result["ok"])
        self.assertEqual(result["panes"], 1)
        self.assertEqual(state["panes"][key]["last_known_status"], "working")
        self.assertNotIn("closed_at", state["panes"][key])
        self.assertEqual(state["tendwire_source_inventory_preserved"], 1)
        send_notice.assert_not_called()
        drop_stale.assert_not_called()
        labels.assert_not_called()
        prefetch.assert_not_called()
        ensure_devin.assert_not_called()
        save_state.assert_called()

    def test_source_modes_skip_direct_herdr_plugin_event_handling(self) -> None:
        for mode in ("source-read", "source"):
            with self.subTest(mode=mode):
                state: dict = {
                    "enabled": True,
                    "plugin_event_enabled": True,
                    "telegram": {"chat_id": "-100", "general_thread_id": "1"},
                }
                with patch.dict(
                    os.environ,
                    {
                        "HERDRES_TENDWIRE_MODE": mode,
                        "HERDR_PLUGIN_EVENT_JSON": '{"pane_id":"pane-secret"}',
                    },
                    clear=True,
                ), \
                        patch.object(herdres, "load_dotenv"), \
                        patch.object(herdres, "load_state", return_value=state), \
                        patch.object(
                            herdres,
                            "configure_telegram_state",
                            return_value=({"chat_id": "-100", "general_thread_id": "1"}, "-100"),
                        ), \
                        patch.object(herdres, "reconcile_topic_grouping", return_value=False) as grouping, \
                        patch.object(herdres, "parse_plugin_json_env") as parse_plugin, \
                        patch.object(herdres, "pane_by_id") as pane_by_id, \
                        patch.object(herdres, "sync_pane_once") as sync_pane_once, \
                        patch.object(herdres, "observed_agent_panes") as observed_agent_panes, \
                        patch.object(herdres, "save_state") as save_state:
                    result = herdres.event_once()

                self.assertTrue(result["ok"])
                self.assertFalse(result["changed"])
                self.assertEqual(result["message"], "plugin event skipped in Tendwire source mode")
                self.assertEqual(result["tendwire_mode"], mode)
                self.assertNotIn("pane-secret", json.dumps(result, sort_keys=True))
                self.assertIn("last_tendwire_source_plugin_event_skipped_at", state)
                self.assertEqual(state["last_tendwire_source_plugin_event_mode"], mode)
                grouping.assert_called_once()
                parse_plugin.assert_not_called()
                pane_by_id.assert_not_called()
                sync_pane_once.assert_not_called()
                observed_agent_panes.assert_not_called()
                save_state.assert_called_once_with(state)

    def test_tendwire_source_smoke_runs_source_dry_run_against_copied_state(self) -> None:
        captured: dict[str, str] = {}
        with tempfile.TemporaryDirectory() as tmp:
            state_path = Path(tmp) / "state.json"
            state_path.write_text(json.dumps({"enabled": True, "telegram": {"chat_id": "-100"}}), encoding="utf-8")

            def fake_run_cmd(args, *, timeout=10, input_text=None, env=None):
                del input_text
                assert env is not None
                captured.update(env)
                self.assertEqual(timeout, 12)
                self.assertEqual(args[-1], "sync")
                self.assertNotEqual(env["HERDR_TELEGRAM_TOPICS_STATE"], str(state_path))
                self.assertTrue(Path(env["HERDR_TELEGRAM_TOPICS_STATE"]).exists())
                return subprocess.CompletedProcess(
                    args,
                    0,
                    stdout='{"dry_run_method":"sendMessage","payload":{}}\n{"ok":true,"panes":1}\n',
                    stderr="",
                )

            with patch.dict(os.environ, {}, clear=True), \
                    patch.object(herdres, "load_dotenv"), \
                    patch.object(herdres, "state_path", return_value=state_path), \
                    patch.object(herdres, "run_cmd", side_effect=fake_run_cmd):
                result = herdres.tendwire_source_smoke_once(
                    type("Args", (), {"timeout": 12, "with_outbox": False})()
                )

        self.assertTrue(result["ok"])
        self.assertEqual(result["status"], "ok")
        self.assertEqual(result["direct_herdr_calls"], 0)
        self.assertEqual(result["json_lines"], 2)
        self.assertEqual(result["sync_result"]["panes"], 1)
        self.assertEqual(captured["HERDRES_TENDWIRE_MODE"], "source")
        self.assertEqual(captured["HERDR_TELEGRAM_TOPICS_DRY_RUN"], "1")
        self.assertEqual(captured["HERDRES_TENDWIRE_CONNECTOR_OUTBOX"], "0")
        self.assertEqual(captured["HERDR_REAL_BIN"], "herdr")
        self.assertNotEqual(captured["HERDR_BIN"], captured["HERDR_REAL_BIN"])

    def test_tendwire_source_smoke_fails_when_direct_herdr_is_called(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            state_path = Path(tmp) / "state.json"
            state_path.write_text(json.dumps({"enabled": True}), encoding="utf-8")

            def fake_run_cmd(args, *, timeout=10, input_text=None, env=None):
                del timeout, input_text
                assert env is not None
                Path(env["HERDRES_SOURCE_SMOKE_DIRECT_HERDR_LOG"]).write_text("pane list\n", encoding="utf-8")
                return subprocess.CompletedProcess(args, 0, stdout='{"ok":true}\n', stderr="")

            with patch.dict(os.environ, {}, clear=True), \
                    patch.object(herdres, "load_dotenv"), \
                    patch.object(herdres, "state_path", return_value=state_path), \
                    patch.object(herdres, "run_cmd", side_effect=fake_run_cmd):
                result = herdres.tendwire_source_smoke_once(
                    type("Args", (), {"timeout": 30, "with_outbox": False})()
                )

        self.assertFalse(result["ok"])
        self.assertEqual(result["status"], "direct_herdr_called")
        self.assertEqual(result["direct_herdr_calls"], 1)

    def test_enrich_mode_tendwire_enriched_entry_send_uses_real_pane_id(self) -> None:
        entry = {
            "source": "herdr",
            "tendwire_worker_id": "worker-1",
            "tendwire_fingerprint": "fp-1",
            "pane_id": "pane-1",
        }
        with patch.dict(os.environ, {"HERDRES_TENDWIRE_MODE": "enrich"}, clear=True), \
                patch.object(herdres, "send_to_pane", return_value=(True, "queued")) as send_to_pane, \
                patch.object(herdres, "tendwire_command") as tendwire_command, \
                patch.object(herdres, "save_state") as save_state:
            result = herdres.forward_text_to_pane_response(
                "pane-1",
                "continue",
                state={"panes": {}},
                entry=entry,
            )

        send_to_pane.assert_called_once_with("pane-1", "continue")
        tendwire_command.assert_not_called()
        save_state.assert_called_once()
        self.assertEqual(result["reply"], "queued")

    def test_stale_tendwire_only_entry_remains_read_only(self) -> None:
        entry = {"source": "tendwire", "entry_type": "worker", "pane_id": "", "tendwire_worker_id": "worker-1"}
        with patch.object(herdres, "send_to_pane") as send_to_pane:
            result = herdres.forward_text_to_pane_response(
                "",
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
                "worker": {
                    "source": "tendwire",
                    "entry_type": "worker",
                    "pane_id": "",
                    "worker_id": "worker-2",
                    "space_key": "workspace:w1",
                    "topic_id": "78",
                },
                "live": {
                    "source": "herdr",
                    "pane_id": "pane-1",
                    "tendwire_worker_id": "worker-1",
                    "space_key": "workspace:w1",
                },
            },
            "spaces": {
                "workspace:w1": {"pane_keys": ["stale", "worker", "live"]},
            },
        }

        removed = herdres.drop_tendwire_source_pane_records(state)

        self.assertEqual(removed, 2)
        self.assertNotIn("stale", state["panes"])
        self.assertNotIn("worker", state["panes"])
        self.assertIn("live", state["panes"])
        self.assertEqual(state["spaces"]["workspace:w1"]["pane_keys"], ["live"])
        self.assertEqual(state["deleted_tendwire_source_panes"][0]["pane_key"], "stale")
        self.assertEqual(state["deleted_tendwire_source_panes"][1]["worker_id"], "worker-2")

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
