from __future__ import annotations

import io
import json
import os
import subprocess
import sys
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
        self.assertTrue(pane["_tendwire_source_read"])
        self.assertTrue(pane["_tendwire_enriched"])
        self.assertEqual(pane["_tendwire_worker_id"], "worker-1")
        self.assertEqual(pane["_tendwire_fingerprint"], "fp-1")
        self.assertEqual(pane["terminal_id"], "")
        self.assertEqual(pane["summary"], "Working on tests")

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
