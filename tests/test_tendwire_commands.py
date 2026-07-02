from __future__ import annotations

import json
import os
import subprocess
import unittest
from contextlib import contextmanager
from unittest.mock import Mock, patch

import herdres
import herdres_tendwire


def _entry(**extra) -> dict:
    data = {
        "pane_key": "pane-1",
        "pane_id": "pane-1",
        "space_key": "workspace:workspace-1",
        "topic_id": "77",
        "pane_root_message_id": "1001",
        "last_known_status": "working",
        "agent": "codex",
        "source": "herdr",
        "last_turn_id": "turn-before",
        "tendwire_worker_id": "worker-1",
        "tendwire_fingerprint": "fp-1",
    }
    data.update(extra)
    return data


def _state(entry: dict | None = None) -> dict:
    pane_entry = entry if entry is not None else _entry()
    return {
        "version": 1,
        "enabled": True,
        "telegram": {
            "chat_id": "-1001",
            "general_thread_id": "1",
            "owner_user_ids": ["42"],
        },
        "spaces": {
            "workspace:workspace-1": {
                "space_key": "workspace:workspace-1",
                "topic_id": "77",
                "pane_keys": ["pane-1"],
                "message_routes": {"1001": "pane-1"},
            }
        },
        "panes": {"pane-1": pane_entry},
    }


def _payload(**extra) -> dict:
    payload = {
        "chat_id": "-1001",
        "topic_id": "77",
        "message_id": "5000",
        "reply_to_message_id": "1001",
        "user_id": "42",
        "from_bot": False,
        "forwarded": False,
        "edited": False,
        "text": "/send first line\nsecond line",
    }
    payload.update(extra)
    return payload


def _completed(stdout: dict | str, returncode: int = 0, stderr: str = "") -> subprocess.CompletedProcess[str]:
    text = json.dumps(stdout) if isinstance(stdout, dict) else stdout
    return subprocess.CompletedProcess(["tendwire"], returncode, stdout=text, stderr=stderr)


class TendwireCommandSubprocessTests(unittest.TestCase):
    def test_tendwire_command_writes_one_json_request_and_parses_one_object(self) -> None:
        request = {"schema_version": 1, "action": "send_instruction", "instruction": {"text": "hi"}}
        with patch.dict(os.environ, {"HERDRES_TENDWIRE_BIN": "tw --profile local", "HERDRES_TENDWIRE_TIMEOUT_SECONDS": "3"}, clear=True), \
                patch.object(herdres, "run_cmd", return_value=_completed({"status": "accepted"})) as run_cmd:
            response = herdres.tendwire_command(request)

        self.assertEqual(response["status"], "accepted")
        run_cmd.assert_called_once()
        self.assertEqual(run_cmd.call_args.args[0], ["tw", "--profile", "local", "command", "--json"])
        self.assertEqual(run_cmd.call_args.kwargs["timeout"], 3)
        input_text = run_cmd.call_args.kwargs["input_text"]
        self.assertTrue(input_text.endswith("\n"))
        self.assertEqual(json.loads(input_text), request)

    def test_tendwire_command_strict_stdout_failures(self) -> None:
        cases = (
            ("non_json", _completed("not json"), "non_json_stdout"),
            ("malformed", _completed("{"), "malformed_json"),
            ("non_object", _completed('["ok"]'), "non_object_json"),
            ("extra_json", _completed('{"ok":true}\n{"ok":true}'), "non_json_stdout"),
            ("nonzero", _completed({"status": "accepted"}, returncode=2, stderr="boom"), "nonzero_exit"),
        )
        for name, proc, status in cases:
            with self.subTest(name=name), patch.object(herdres, "run_cmd", return_value=proc):
                response = herdres.tendwire_command({"schema_version": 1})

            self.assertFalse(response["ok"])
            self.assertEqual(response["status"], status)

    def test_tendwire_command_timeout_and_subprocess_failure(self) -> None:
        with patch.object(herdres, "run_cmd", side_effect=subprocess.TimeoutExpired(["tendwire"], 5)):
            self.assertEqual(herdres.tendwire_command({"schema_version": 1})["status"], "timeout")
        with patch.object(herdres, "run_cmd", side_effect=OSError("no tendwire")):
            self.assertEqual(herdres.tendwire_command({"schema_version": 1})["status"], "subprocess_failed")


class TendwireRequestBuilderTests(unittest.TestCase):
    def test_request_shape_stable_id_and_exact_multiline_text(self) -> None:
        entry = _entry()
        text = "first line\n\nsecond line"
        request = herdres_tendwire.build_send_instruction_request(
            entry,
            text,
            origin="plain",
            chat_id="-1001",
            topic_id="77",
            message_id="5000",
            reply_to_message_id="1001",
        )
        again = herdres_tendwire.build_send_instruction_request(
            entry,
            text,
            origin="plain",
            chat_id="-1001",
            topic_id="77",
            message_id="5000",
            reply_to_message_id="1001",
        )
        changed = herdres_tendwire.build_send_instruction_request(
            entry,
            text + "!",
            origin="plain",
            chat_id="-1001",
            topic_id="77",
            message_id="5000",
            reply_to_message_id="1001",
        )

        self.assertEqual(request["schema_version"], 1)
        self.assertEqual(request["action"], "send_instruction")
        self.assertFalse(request["dry_run"])
        self.assertEqual(request["target"], {"worker_id": "worker-1", "worker_fingerprint": "fp-1"})
        self.assertEqual(request["instruction"]["text"], text)
        self.assertEqual(request["params"]["origin"], "telegram")
        self.assertEqual(request["params"]["telegram_origin"], "plain")
        self.assertEqual(request["request_id"], again["request_id"])
        self.assertNotEqual(request["request_id"], changed["request_id"])
        self.assertTrue(request["request_id"].startswith("herdres:worker-1:"))
        encoded = json.dumps(request, sort_keys=True)
        self.assertNotIn("-1001", encoded)
        self.assertNotIn('"77"', encoded)
        self.assertNotIn("5000", encoded)
        self.assertNotIn("1001", encoded)

    def test_submission_identity_is_stable_and_private_id_free(self) -> None:
        first = herdres_tendwire.instruction_submission_identity(
            chat_id="-1001",
            topic_id="77",
            message_id="5000",
            worker_id="worker-1",
            origin="plain",
            text="hello",
        )
        again = herdres_tendwire.instruction_submission_identity(
            chat_id="-1001",
            topic_id="77",
            message_id="5000",
            worker_id="worker-1",
            origin="plain",
            text="hello",
        )

        changed = herdres_tendwire.instruction_submission_identity(
            chat_id="-1001",
            topic_id="77",
            message_id="5000",
            worker_id="worker-1",
            origin="plain",
            text="hello!",
        )

        self.assertEqual(first, again)
        self.assertNotEqual(first, changed)
        self.assertNotIn("-1001", first)
        self.assertNotIn("5000", first)

    def test_submission_ledger_helpers_are_public_safe_and_bounded(self) -> None:
        entry = _entry(source="tendwire", entry_type="worker", pane_id="", worker_id="worker-1", worker_fingerprint="fp-1")
        state: dict = {}
        first_identity = herdres_tendwire.command_submission_identity_for_entry(
            entry,
            "hello",
            origin="plain",
            chat_id="-1001",
            topic_id="77",
            message_id="5000",
            reply_to_message_id="1001",
        )
        second_identity = herdres_tendwire.command_submission_identity_for_entry(
            entry,
            "again",
            origin="plain",
            chat_id="-1001",
            topic_id="77",
            message_id="5001",
        )

        self.assertTrue(first_identity)
        self.assertTrue(
            herdres_tendwire.note_command_submission(
                state,
                first_identity,
                request_id="request-1",
                worker_id="worker-1",
                origin="plain",
                text="hello",
                status="accepted",
                now="2026-07-02T00:00:00+00:00",
                limit=1,
            )
        )
        self.assertTrue(herdres_tendwire.command_submission_seen(state, first_identity))
        self.assertTrue(
            herdres_tendwire.note_command_submission(
                state,
                second_identity,
                request_id="request-2",
                worker_id="worker-1",
                origin="plain",
                text="again",
                status="accepted",
                now="2026-07-02T00:01:00+00:00",
                limit=1,
            )
        )

        ledger = state["tendwire_command_submissions"]
        self.assertEqual(set(ledger), {second_identity})
        record = ledger[second_identity]
        self.assertEqual(record["worker_id"], "worker-1")
        self.assertEqual(record["origin"], "plain")
        self.assertEqual(record["status"], "accepted")
        encoded = json.dumps(state, sort_keys=True)
        self.assertNotIn("-1001", encoded)
        self.assertNotIn('"77"', encoded)
        self.assertNotIn("5000", encoded)
        self.assertNotIn("5001", encoded)

    def test_duplicate_instruction_status_is_successful_noop(self) -> None:
        response = {
            "ok": True,
            "status": "duplicate_instruction",
            "result": {
                "delivery_state": "duplicate_suppressed",
                "deduplicated": True,
            },
        }

        self.assertTrue(herdres_tendwire.command_succeeded(response))
        self.assertEqual(herdres_tendwire.success_reply(response), "")

    def test_entry_metadata_classification_lives_in_tendwire_helper(self) -> None:
        legacy = _entry(source="herdr")
        source_entry = _entry(
            source="tendwire",
            entry_type="worker",
            pane_id="",
            worker_id="worker-1",
            worker_fingerprint="fp-1",
        )
        pseudo_source = _entry(source="tendwire", pane_id="tendwire:worker-1")
        self.assertFalse(herdres_tendwire.is_source_entry(legacy))
        self.assertTrue(herdres_tendwire.is_source_entry(source_entry))
        self.assertTrue(herdres_tendwire.is_source_entry(pseudo_source))
        self.assertFalse(
            herdres_tendwire.source_entry_commands_allowed(
                source_entry,
                source_read_enabled=True,
                commands_enabled=False,
            )
        )
        self.assertEqual(
            herdres_tendwire.entry_metadata_state(
                source_entry,
                source_read_enabled=False,
                commands_enabled=True,
            ),
            "none",
        )
        self.assertEqual(
            herdres_tendwire.entry_metadata_state(
                source_entry,
                source_read_enabled=True,
                commands_enabled=True,
            ),
            "valid",
        )
        partial = dict(legacy, tendwire_worker_id="worker-1", tendwire_fingerprint="")
        self.assertEqual(
            herdres_tendwire.entry_metadata_state(
                partial,
                source_read_enabled=False,
                commands_enabled=True,
            ),
            "partial",
        )

    def test_send_text_policy_keeps_source_mode_fail_closed(self) -> None:
        self.assertEqual(
            herdres_tendwire.send_text_policy(
                source_inventory_enabled=True,
                source_entry=False,
                source_entry_commands_allowed=False,
                commands_enabled=True,
                metadata_state="valid",
                direct_fallback_enabled=True,
            ),
            "legacy_source_block",
        )
        self.assertEqual(
            herdres_tendwire.send_text_policy(
                source_inventory_enabled=True,
                source_entry=True,
                source_entry_commands_allowed=True,
                commands_enabled=True,
                metadata_state="valid",
                direct_fallback_enabled=True,
            ),
            "tendwire",
        )
        self.assertEqual(
            herdres_tendwire.send_text_policy(
                source_inventory_enabled=True,
                source_entry=True,
                source_entry_commands_allowed=True,
                commands_enabled=True,
                metadata_state="partial",
                direct_fallback_enabled=True,
            ),
            "safe_failure",
        )
        self.assertEqual(
            herdres_tendwire.send_text_policy(
                source_inventory_enabled=False,
                source_entry=False,
                source_entry_commands_allowed=False,
                commands_enabled=True,
                metadata_state="partial",
                direct_fallback_enabled=True,
            ),
            "direct",
        )

    def test_entry_send_text_policy_reads_mode_and_metadata_without_telegram_state(self) -> None:
        source_entry = _entry(
            source="tendwire",
            entry_type="worker",
            pane_id="",
            tendwire_worker_id="worker-1",
            tendwire_fingerprint="fp-1",
        )
        partial_source_entry = dict(source_entry, tendwire_fingerprint="")
        legacy_entry = _entry(source="herdr", pane_id="pane-1")

        self.assertEqual(
            herdres_tendwire.entry_send_text_policy(
                source_entry,
                {"HERDRES_TENDWIRE_MODE": "source"},
            ),
            "tendwire",
        )
        self.assertEqual(
            herdres_tendwire.entry_send_text_policy(
                partial_source_entry,
                {"HERDRES_TENDWIRE_MODE": "source", "HERDRES_TENDWIRE_DIRECT_FALLBACK": "1"},
            ),
            "safe_failure",
        )
        self.assertEqual(
            herdres_tendwire.entry_send_text_policy(
                legacy_entry,
                {"HERDRES_TENDWIRE_MODE": "source"},
            ),
            "legacy_source_block",
        )
        self.assertEqual(
            herdres_tendwire.entry_send_text_policy(
                legacy_entry,
                {"HERDRES_TENDWIRE_MODE": "commands", "HERDRES_TENDWIRE_DIRECT_FALLBACK": "1"},
            ),
            "tendwire",
        )

    def test_entry_send_text_decision_maps_policy_without_telegram_state(self) -> None:
        source_entry = _entry(
            source="tendwire",
            entry_type="worker",
            pane_id="",
            tendwire_worker_id="worker-1",
            tendwire_fingerprint="fp-1",
        )
        partial_source_entry = dict(source_entry, tendwire_fingerprint="")
        legacy_entry = _entry(source="herdr", pane_id="pane-1")

        source_decision = herdres_tendwire.entry_send_text_decision(
            source_entry,
            {"HERDRES_TENDWIRE_MODE": "source"},
            safe_failure_reply=herdres.TENDWIRE_SAFE_SEND_FAILURE_REPLY,
        )
        self.assertEqual(source_decision["action"], "tendwire")

        partial_decision = herdres_tendwire.entry_send_text_decision(
            partial_source_entry,
            {"HERDRES_TENDWIRE_MODE": "source", "HERDRES_TENDWIRE_DIRECT_FALLBACK": "1"},
            safe_failure_reply=herdres.TENDWIRE_SAFE_SEND_FAILURE_REPLY,
        )
        self.assertEqual(partial_decision["action"], "reply")
        self.assertEqual(partial_decision["reply"], herdres.TENDWIRE_SAFE_SEND_FAILURE_REPLY)

        legacy_source_decision = herdres_tendwire.entry_send_text_decision(
            legacy_entry,
            {"HERDRES_TENDWIRE_MODE": "source"},
            safe_failure_reply=herdres.TENDWIRE_SAFE_SEND_FAILURE_REPLY,
        )
        self.assertEqual(legacy_source_decision["action"], "reply")
        self.assertIn("legacy Herdr mode", legacy_source_decision["reply"])

        off_decision = herdres_tendwire.entry_send_text_decision(
            legacy_entry,
            {"HERDRES_TENDWIRE_MODE": "off"},
            safe_failure_reply=herdres.TENDWIRE_SAFE_SEND_FAILURE_REPLY,
        )
        self.assertEqual(off_decision["action"], "direct")

    def test_callback_choice_preflight_policy_keeps_source_mode_fail_closed(self) -> None:
        self.assertEqual(
            herdres_tendwire.callback_choice_preflight_policy(
                source_inventory_enabled=True,
                source_entry=False,
                pane_id="pane-1",
                last_known_status="working",
                metadata_state="valid",
            ),
            "legacy_source_block",
        )
        self.assertEqual(
            herdres_tendwire.callback_choice_preflight_policy(
                source_inventory_enabled=False,
                source_entry=False,
                pane_id="",
                last_known_status="working",
                metadata_state="none",
            ),
            "pane_not_live",
        )
        self.assertEqual(
            herdres_tendwire.callback_choice_preflight_policy(
                source_inventory_enabled=True,
                source_entry=True,
                pane_id="",
                last_known_status="closed",
                metadata_state="valid",
            ),
            "source_not_live",
        )
        self.assertEqual(
            herdres_tendwire.callback_choice_preflight_policy(
                source_inventory_enabled=True,
                source_entry=True,
                pane_id="",
                last_known_status="working",
                metadata_state="partial",
            ),
            "safe_failure",
        )
        self.assertEqual(
            herdres_tendwire.callback_choice_preflight_policy(
                source_inventory_enabled=True,
                source_entry=True,
                pane_id="",
                last_known_status="working",
                metadata_state="valid",
            ),
            "ok",
        )

    def test_entry_metadata_state_for_env_owns_mode_parsing(self) -> None:
        source_entry = _entry(
            source="tendwire",
            entry_type="worker",
            pane_id="",
            tendwire_worker_id="worker-1",
            tendwire_fingerprint="fp-1",
        )
        partial_legacy = _entry(tendwire_worker_id="worker-1", tendwire_fingerprint="")

        self.assertEqual(
            herdres_tendwire.entry_metadata_state_for_env(
                source_entry,
                {"HERDRES_TENDWIRE_MODE": "source-read"},
            ),
            "valid",
        )
        self.assertEqual(
            herdres_tendwire.entry_metadata_state_for_env(
                source_entry,
                {"HERDRES_TENDWIRE_MODE": "commands"},
            ),
            "none",
        )
        self.assertEqual(
            herdres_tendwire.entry_metadata_state_for_env(
                partial_legacy,
                {"HERDRES_TENDWIRE_MODE": "commands"},
            ),
            "partial",
        )

    def test_callback_choice_preflight_for_entry_reads_mode_and_metadata(self) -> None:
        legacy_entry = _entry(source="herdr", pane_id="pane-1")
        source_entry = _entry(
            source="tendwire",
            entry_type="worker",
            pane_id="",
            tendwire_worker_id="worker-1",
            tendwire_fingerprint="fp-1",
        )
        partial_source = dict(source_entry, tendwire_fingerprint="")
        closed_source = dict(source_entry, last_known_status="closed")

        self.assertEqual(
            herdres_tendwire.callback_choice_preflight_for_entry(
                legacy_entry,
                {"HERDRES_TENDWIRE_MODE": "source"},
            ),
            "legacy_source_block",
        )
        self.assertEqual(
            herdres_tendwire.callback_choice_preflight_for_entry(
                source_entry,
                {"HERDRES_TENDWIRE_MODE": "source-read"},
            ),
            "ok",
        )
        self.assertEqual(
            herdres_tendwire.callback_choice_preflight_for_entry(
                partial_source,
                {"HERDRES_TENDWIRE_MODE": "source-read"},
            ),
            "safe_failure",
        )
        self.assertEqual(
            herdres_tendwire.callback_choice_preflight_for_entry(
                closed_source,
                {"HERDRES_TENDWIRE_MODE": "source-read"},
            ),
            "source_not_live",
        )

    def test_attachment_preflight_policy_keeps_source_mode_out_of_direct_herdr(self) -> None:
        self.assertEqual(
            herdres_tendwire.attachment_send_preflight_policy(
                source_inventory_enabled=True,
                source_entry=False,
                attachment_kind="document",
            ),
            "legacy_source_block",
        )
        self.assertEqual(
            herdres_tendwire.attachment_send_preflight_policy(
                source_inventory_enabled=True,
                source_entry=True,
                attachment_kind="voice",
            ),
            "source_attachment_unsupported",
        )
        self.assertEqual(
            herdres_tendwire.attachment_send_preflight_policy(
                source_inventory_enabled=False,
                source_entry=False,
                attachment_kind="photo",
            ),
            "ok",
        )

    def test_attachment_preflight_for_entry_reads_mode_and_source_state(self) -> None:
        legacy_entry = _entry(source="herdr", pane_id="pane-1")
        source_entry = _entry(
            source="tendwire",
            entry_type="worker",
            pane_id="",
            tendwire_worker_id="worker-1",
            tendwire_fingerprint="fp-1",
        )

        self.assertEqual(
            herdres_tendwire.attachment_send_preflight_for_entry(
                legacy_entry,
                "document",
                {"HERDRES_TENDWIRE_MODE": "source"},
            ),
            "legacy_source_block",
        )
        self.assertEqual(
            herdres_tendwire.attachment_send_preflight_for_entry(
                source_entry,
                "voice",
                {"HERDRES_TENDWIRE_MODE": "source-read"},
            ),
            "source_attachment_unsupported",
        )
        self.assertEqual(
            herdres_tendwire.attachment_send_preflight_for_entry(
                legacy_entry,
                "photo",
                {"HERDRES_TENDWIRE_MODE": "off"},
            ),
            "ok",
        )

    def test_raw_read_preflight_for_entry_blocks_direct_reads_in_source_mode(self) -> None:
        legacy_entry = _entry(source="herdr", pane_id="pane-1")
        source_entry = _entry(
            source="tendwire",
            entry_type="worker",
            pane_id="",
            tendwire_worker_id="worker-1",
            tendwire_fingerprint="fp-1",
        )

        self.assertEqual(
            herdres_tendwire.raw_read_preflight_for_entry(
                legacy_entry,
                {"HERDRES_TENDWIRE_MODE": "source"},
            ),
            "legacy_source_block",
        )
        self.assertEqual(
            herdres_tendwire.raw_read_preflight_for_entry(
                source_entry,
                {"HERDRES_TENDWIRE_MODE": "source-read"},
            ),
            "source_raw_unsupported",
        )
        self.assertEqual(
            herdres_tendwire.raw_read_preflight_for_entry(
                legacy_entry,
                {"HERDRES_TENDWIRE_MODE": "off"},
            ),
            "ok",
        )

    def test_new_and_raw_keys_preflight_helpers_block_source_mode_direct_paths(self) -> None:
        legacy_entry = _entry(source="herdr", pane_id="pane-1")
        source_entry = _entry(
            source="tendwire",
            entry_type="worker",
            pane_id="",
            tendwire_worker_id="worker-1",
            tendwire_fingerprint="fp-1",
        )

        self.assertEqual(
            herdres_tendwire.new_pane_preflight_for_env({"HERDRES_TENDWIRE_MODE": "source"}),
            "source_new_disabled",
        )
        self.assertEqual(
            herdres_tendwire.new_pane_preflight_for_env({"HERDRES_TENDWIRE_MODE": "off"}),
            "ok",
        )
        self.assertEqual(
            herdres_tendwire.raw_keys_preflight_for_entry(
                legacy_entry,
                {"HERDRES_TENDWIRE_MODE": "source"},
            ),
            "legacy_source_block",
        )
        self.assertEqual(
            herdres_tendwire.raw_keys_preflight_for_entry(
                source_entry,
                {"HERDRES_TENDWIRE_MODE": "source-read"},
            ),
            "source_keys_unsupported",
        )
        self.assertEqual(
            herdres_tendwire.raw_keys_preflight_for_entry(
                legacy_entry,
                {"HERDRES_TENDWIRE_MODE": "off"},
            ),
            "ok",
        )

    def test_interrupt_preflight_for_entry_blocks_source_and_command_mode_direct_paths(self) -> None:
        legacy_entry = _entry(source="herdr", pane_id="pane-1")
        source_entry = _entry(
            source="tendwire",
            entry_type="worker",
            pane_id="",
            tendwire_worker_id="worker-1",
            tendwire_fingerprint="fp-1",
        )
        enriched_entry = _entry(source="herdr", pane_id="pane-1")

        self.assertEqual(
            herdres_tendwire.interrupt_preflight_for_entry(
                legacy_entry,
                {"HERDRES_TENDWIRE_MODE": "source"},
            ),
            "source_interrupt_unsupported",
        )
        self.assertEqual(
            herdres_tendwire.interrupt_preflight_for_entry(
                source_entry,
                {"HERDRES_TENDWIRE_MODE": "commands"},
            ),
            "source_entry_interrupt_unsupported",
        )
        self.assertEqual(
            herdres_tendwire.interrupt_preflight_for_entry(
                enriched_entry,
                {"HERDRES_TENDWIRE_MODE": "commands"},
            ),
            "commands_interrupt_unsupported",
        )
        self.assertEqual(
            herdres_tendwire.interrupt_preflight_for_entry(
                legacy_entry,
                {"HERDRES_TENDWIRE_MODE": "off"},
            ),
            "ok",
        )


class TendwireCommandRoutingTests(unittest.TestCase):
    @contextmanager
    def _command_patches(self, state: dict, *, proc: subprocess.CompletedProcess[str] | None = None):
        mocks = {
            "load_dotenv": Mock(),
            "load_state": Mock(return_value=state),
            "save_state": Mock(),
            "run_cmd": Mock(return_value=proc or _completed({"status": "accepted"})),
            "send_to_pane": Mock(return_value=(True, "")),
            "fresh_cached_pane_turn": Mock(return_value={"turn_id": "turn-before"}),
        }
        with patch.multiple(herdres, **mocks):
            yield mocks

    def _shared_source_state(self) -> dict:
        claude = _entry(
            pane_key="worker:claude-1:current",
            pane_id="",
            source="tendwire",
            entry_type="worker",
            agent="claude",
            worker_id="claude-1",
            worker_fingerprint="claude-fp",
            tendwire_worker_id="claude-1",
            tendwire_fingerprint="claude-fp",
            pane_root_message_id="1001",
        )
        codex = _entry(
            pane_key="worker:codex-1:current",
            pane_id="",
            source="tendwire",
            entry_type="worker",
            agent="codex",
            worker_id="codex-1",
            worker_fingerprint="codex-fp",
            tendwire_worker_id="codex-1",
            tendwire_fingerprint="codex-fp",
            pane_root_message_id="1002",
        )
        return {
            "version": 1,
            "enabled": True,
            "telegram": {
                "chat_id": "-1001",
                "general_thread_id": "1",
                "owner_user_ids": ["42"],
            },
            "spaces": {
                "workspace:workspace-1": {
                    "space_key": "workspace:workspace-1",
                    "topic_id": "77",
                    "pane_keys": ["worker:claude-1:current", "worker:codex-1:current"],
                    "message_routes": {},
                }
            },
            "panes": {
                "worker:claude-1:current": claude,
                "worker:codex-1:current": codex,
            },
        }

    def test_commands_mode_send_uses_tendwire_command_not_send_to_pane(self) -> None:
        state = _state()
        with patch.dict(os.environ, {"HERDRES_TENDWIRE_MODE": "commands"}, clear=True), self._command_patches(state) as patched:
            result = herdres.command_reply(_payload())

        self.assertTrue(result["handled"])
        self.assertEqual(result["reply"], "")
        patched["send_to_pane"].assert_not_called()
        patched["run_cmd"].assert_called_once()
        self.assertEqual(patched["run_cmd"].call_args.args[0], ["tendwire", "command", "--json"])
        request = json.loads(patched["run_cmd"].call_args.kwargs["input_text"])
        self.assertEqual(request["action"], "send_instruction")
        self.assertEqual(request["target"]["worker_id"], "worker-1")
        self.assertEqual(request["target"]["worker_fingerprint"], "fp-1")
        self.assertEqual(request["instruction"]["text"], "first line\nsecond line")
        self.assertEqual(request["params"]["origin"], "telegram")
        self.assertEqual(request["params"]["telegram_origin"], "send")
        self.assertEqual(state["panes"]["pane-1"]["direct_origin_origin"], "send")

    def test_source_read_send_uses_tendwire_command_not_send_to_pane(self) -> None:
        entry = _entry(source="tendwire", entry_type="worker", pane_id="", worker_id="worker-1", worker_fingerprint="fp-1")
        state = _state(entry)
        with patch.dict(os.environ, {"HERDRES_TENDWIRE_MODE": "source-read"}, clear=True), \
                self._command_patches(state) as patched:
            result = herdres.command_reply(_payload())

        self.assertTrue(result["handled"])
        self.assertEqual(result["reply"], "")
        patched["send_to_pane"].assert_not_called()
        patched["fresh_cached_pane_turn"].assert_not_called()
        patched["run_cmd"].assert_called_once()
        request = json.loads(patched["run_cmd"].call_args.kwargs["input_text"])
        self.assertEqual(request["action"], "send_instruction")
        self.assertEqual(request["target"]["worker_id"], "worker-1")
        self.assertEqual(request["target"]["worker_fingerprint"], "fp-1")
        self.assertEqual(request["params"]["entry_type"], "worker")
        self.assertEqual(request["params"]["pane_key"], "pane-1")
        self.assertNotIn("pane_id", request["params"])
        self.assertNotIn("chat_id", request["params"])
        self.assertNotIn("topic_id", request["params"])
        self.assertNotIn("message_id", request["params"])
        self.assertEqual(request["instruction"]["text"], "first line\nsecond line")
        self.assertEqual(state["panes"]["pane-1"]["direct_origin_pane_id"], "")
        self.assertEqual(state["panes"]["pane-1"]["direct_origin_pane_key"], "pane-1")

    def test_source_read_same_telegram_message_is_submitted_once(self) -> None:
        entry = _entry(source="tendwire", entry_type="worker", pane_id="", worker_id="worker-1", worker_fingerprint="fp-1")
        state = _state(entry)
        with patch.dict(os.environ, {"HERDRES_TENDWIRE_MODE": "source-read"}, clear=True), \
                self._command_patches(state) as patched:
            first = herdres.command_reply(_payload())
            second = herdres.command_reply(_payload())
            third = herdres.command_reply(_payload(message_id="5001"))

        self.assertEqual(first["reply"], "")
        self.assertEqual(second["reply"], "")
        self.assertEqual(third["reply"], "")
        patched["send_to_pane"].assert_not_called()
        self.assertEqual(patched["run_cmd"].call_count, 2)
        ledger = state.get("tendwire_command_submissions")
        self.assertIsInstance(ledger, dict)
        self.assertEqual(len(ledger), 2)
        for record in ledger.values():
            self.assertNotIn("chat_id", record)
            self.assertNotIn("topic_id", record)
            self.assertNotIn("message_id", record)
            self.assertEqual(record["worker_id"], "worker-1")
            self.assertEqual(record["origin"], "send")
            self.assertEqual(record["status"], "accepted")

    def test_source_shared_topic_target_bot_kind_routes_to_current_worker(self) -> None:
        state = self._shared_source_state()
        with patch.dict(os.environ, {"HERDRES_TENDWIRE_MODE": "source"}, clear=True), \
                self._command_patches(state) as patched, \
                patch.object(herdres, "send_message") as send_message:
            result = herdres.command_reply(
                _payload(
                    text="hello claude",
                    reply_to_message_id="",
                    target_bot_kind="claude",
                )
            )

        self.assertEqual(result["reply"], "")
        send_message.assert_not_called()
        patched["send_to_pane"].assert_not_called()
        patched["run_cmd"].assert_called_once()
        request = json.loads(patched["run_cmd"].call_args.kwargs["input_text"])
        self.assertEqual(request["target"], {"worker_id": "claude-1", "worker_fingerprint": "claude-fp"})
        self.assertEqual(request["params"]["telegram_origin"], "plain")

    def test_source_shared_topic_target_bot_kind_prefers_single_active_same_agent_worker(self) -> None:
        state = self._shared_source_state()
        state["panes"]["worker:claude-1:current"]["last_known_status"] = "done"
        active = _entry(
            pane_key="worker:claude:active",
            pane_id="",
            source="tendwire",
            entry_type="worker",
            agent="claude",
            worker_id="claude",
            worker_fingerprint="active-fp",
            tendwire_worker_id="claude",
            tendwire_fingerprint="active-fp",
            last_known_status="working",
        )
        state["panes"]["worker:claude:active"] = active
        state["spaces"]["workspace:workspace-1"]["pane_keys"].append("worker:claude:active")
        with patch.dict(os.environ, {"HERDRES_TENDWIRE_MODE": "source"}, clear=True), \
                self._command_patches(state) as patched, \
                patch.object(herdres, "send_message") as send_message:
            result = herdres.command_reply(
                _payload(
                    text="hello active claude",
                    reply_to_message_id="",
                    target_bot_kind="claude",
                )
            )

        self.assertEqual(result["reply"], "")
        send_message.assert_not_called()
        patched["send_to_pane"].assert_not_called()
        request = json.loads(patched["run_cmd"].call_args.kwargs["input_text"])
        self.assertEqual(request["target"], {"worker_id": "claude", "worker_fingerprint": "active-fp"})

    def test_source_shared_topic_stale_message_route_does_not_shadow_picker(self) -> None:
        state = self._shared_source_state()
        state["panes"]["worker:claude-1:old"] = _entry(
            pane_key="worker:claude-1:old",
            pane_id="",
            source="tendwire",
            entry_type="worker",
            agent="claude",
            worker_id="claude-1",
            worker_fingerprint="old-fp",
            tendwire_worker_id="claude-1",
            tendwire_fingerprint="old-fp",
            last_known_status="closed",
        )
        state["spaces"]["workspace:workspace-1"]["message_routes"]["999"] = "worker:claude-1:old"
        with patch.dict(os.environ, {"HERDRES_TENDWIRE_MODE": "source"}, clear=True), \
                self._command_patches(state) as patched, \
                patch.object(herdres, "send_message") as send_message:
            result = herdres.command_reply(
                _payload(
                    text="this needs a picker",
                    reply_to_message_id="999",
                    target_bot_kind="",
                )
            )

        self.assertEqual(result["reply"], "")
        send_message.assert_called_once()
        patched["send_to_pane"].assert_not_called()
        patched["run_cmd"].assert_not_called()
        self.assertEqual(
            state["spaces"]["workspace:workspace-1"]["pending_pick"]["42"]["text"],
            "this needs a picker",
        )

    def test_commands_mode_plain_reply_uses_tendwire_command_not_send_to_pane(self) -> None:
        state = _state()
        with patch.dict(os.environ, {"HERDRES_TENDWIRE_MODE": "commands"}, clear=True), self._command_patches(state) as patched:
            result = herdres.command_reply(_payload(text="first line\nsecond line"))

        self.assertTrue(result["handled"])
        patched["send_to_pane"].assert_not_called()
        request = json.loads(patched["run_cmd"].call_args.kwargs["input_text"])
        self.assertEqual(request["instruction"]["text"], "first line\nsecond line")
        self.assertEqual(request["params"]["telegram_origin"], "plain")
        self.assertEqual(state["panes"]["pane-1"]["direct_origin_origin"], "plain")

    def test_queued_success_returns_reasonable_reply_and_sets_marker(self) -> None:
        state = _state()
        with patch.dict(os.environ, {"HERDRES_TENDWIRE_MODE": "commands"}, clear=True), \
                self._command_patches(state, proc=_completed({"status": "queued"})):
            result = herdres.command_reply(_payload())

        self.assertEqual(result["reply"], "Queued for Tendwire worker.")
        self.assertEqual(state["panes"]["pane-1"]["direct_origin_pane_id"], "pane-1")
        self.assertEqual(state["panes"]["pane-1"]["direct_origin_after_turn_id"], "turn-before")

    def test_safety_statuses_fail_without_direct_fallback(self) -> None:
        statuses = (
            "stale_target",
            "ambiguous_target",
            "ambiguous_backend_target",
            "backend_unavailable",
            "request_state_uncertain",
            "backend_unsupported",
            "backend_failed",
        )
        for status in statuses:
            entry = _entry()
            with self.subTest(status=status), \
                    patch.dict(os.environ, {"HERDRES_TENDWIRE_MODE": "commands"}, clear=True), \
                    patch.object(herdres, "tendwire_command", return_value={"status": status}), \
                    patch.object(herdres, "send_to_pane", return_value=(True, "")) as send_to_pane, \
                    patch.object(herdres, "save_state"):
                result = herdres.forward_text_to_pane_response("pane-1", "continue", state={"panes": {}}, entry=entry)

            self.assertEqual(result["reply"], herdres.TENDWIRE_SAFE_SEND_FAILURE_REPLY)
            send_to_pane.assert_not_called()
            self.assertNotIn("direct_origin_at", entry)

    def test_stale_target_same_worker_retries_with_current_fingerprint(self) -> None:
        entry = _entry(source="tendwire", entry_type="worker", pane_id="", worker_id="worker-1", tendwire_fingerprint="old-fp")
        state = _state(entry)
        stale = {
            "status": "stale_target",
            "result": {
                "candidates": [
                    {
                        "worker_id": "worker-1",
                        "worker_fingerprint": "new-fp",
                        "status": "idle",
                    }
                ]
            },
        }
        accepted = {"status": "accepted", "ok": True}
        with patch.dict(os.environ, {"HERDRES_TENDWIRE_MODE": "source-read"}, clear=True), \
                patch.object(herdres, "tendwire_command", side_effect=[stale, accepted]) as tendwire_command, \
                patch.object(herdres, "send_to_pane") as send_to_pane, \
                patch.object(herdres, "save_state") as save_state:
            result = herdres.send_to_tendwire_worker_response(
                entry,
                "continue",
                state=state,
                origin="plain",
                chat_id="-1001",
                topic_id="77",
                message_id="5000",
            )

        self.assertEqual(result["reply"], "")
        self.assertEqual(tendwire_command.call_count, 2)
        first = tendwire_command.call_args_list[0].args[0]
        second = tendwire_command.call_args_list[1].args[0]
        self.assertEqual(first["target"]["worker_fingerprint"], "old-fp")
        self.assertEqual(second["target"]["worker_fingerprint"], "new-fp")
        self.assertEqual(second["target"]["worker_id"], "worker-1")
        self.assertNotEqual(second["request_id"], first["request_id"])
        self.assertIn(":retry:", second["request_id"])
        self.assertEqual(entry["tendwire_fingerprint"], "new-fp")
        save_state.assert_called_once_with(state)
        send_to_pane.assert_not_called()

    def test_tendwire_helper_builds_same_worker_retry_request(self) -> None:
        entry = _entry(source="tendwire", entry_type="worker", pane_id="", worker_id="worker-1", tendwire_fingerprint="old-fp")
        stale = {
            "status": "stale_target",
            "result": {
                "candidates": [
                    {"worker_id": "worker-1", "worker_fingerprint": "new-fp"},
                    {"worker_id": "worker-2", "worker_fingerprint": "other-fp"},
                ]
            },
        }

        retry = herdres_tendwire.retry_send_instruction_request(
            entry,
            "continue",
            stale,
            chat_id="-1001",
            topic_id="77",
            message_id="5000",
            base_request_id="base-request",
        )

        self.assertIsNotNone(retry)
        retry_entry, retry_request = retry
        self.assertEqual(entry["tendwire_fingerprint"], "old-fp")
        self.assertEqual(retry_entry["tendwire_fingerprint"], "new-fp")
        self.assertEqual(retry_request["target"]["worker_id"], "worker-1")
        self.assertEqual(retry_request["target"]["worker_fingerprint"], "new-fp")
        self.assertTrue(retry_request["request_id"].startswith("base-request:retry:"))

        stale["result"]["candidates"] = [{"worker_id": "worker-2", "worker_fingerprint": "other-fp"}]
        self.assertIsNone(herdres_tendwire.retry_send_instruction_request(entry, "continue", stale))

    def test_tendwire_helper_submits_retry_and_updates_public_ledger(self) -> None:
        entry = _entry(source="tendwire", entry_type="worker", pane_id="", worker_id="worker-1", tendwire_fingerprint="old-fp")
        state = _state(entry)
        stale = {
            "status": "stale_target",
            "result": {"candidates": [{"worker_id": "worker-1", "worker_fingerprint": "new-fp"}]},
        }
        accepted = {"status": "accepted", "ok": True}
        calls: list[dict] = []

        def command_call(request: dict) -> dict:
            calls.append(request)
            return stale if len(calls) == 1 else accepted

        result = herdres_tendwire.submit_send_instruction_attempt(
            entry,
            "continue",
            state=state,
            command_call=command_call,
            now=lambda: "2026-01-01T00:00:00+00:00",
            origin="plain",
            chat_id="-1001",
            topic_id="77",
            message_id="5000",
        )

        self.assertFalse(result["duplicate"])
        self.assertTrue(result["ledger_changed"])
        self.assertEqual(result["response"], accepted)
        self.assertEqual(len(calls), 2)
        self.assertEqual(calls[0]["target"]["worker_fingerprint"], "old-fp")
        self.assertEqual(calls[1]["target"]["worker_fingerprint"], "new-fp")
        self.assertIn(":retry:", calls[1]["request_id"])
        self.assertEqual(entry["tendwire_fingerprint"], "new-fp")
        ledger = state["tendwire_command_submissions"]
        self.assertEqual(len(ledger), 1)
        record = next(iter(ledger.values()))
        self.assertEqual(record["status"], "accepted")
        self.assertEqual(record["worker_id"], "worker-1")
        self.assertNotIn("chat_id", record)
        self.assertNotIn("topic_id", record)
        self.assertNotIn("message_id", record)

    def test_tendwire_helper_success_reply(self) -> None:
        self.assertEqual(herdres_tendwire.success_reply({"status": "queued"}), "Queued for Tendwire worker.")
        self.assertEqual(
            herdres_tendwire.success_reply({"status": "accepted", "result": {"delivery_state": "queued"}}),
            "Queued for Tendwire worker.",
        )
        self.assertEqual(
            herdres_tendwire.success_reply(
                {"status": "accepted", "result": {"message": "Accepted for delivery"}},
                sanitize=lambda text, limit=300: text[:limit].lower(),
            ),
            "accepted for delivery",
        )

    def test_tendwire_helper_attempt_result_returns_public_send_reply(self) -> None:
        duplicate = herdres_tendwire.send_instruction_attempt_result(
            {"duplicate": True, "response": {"status": "accepted"}, "ledger_changed": True},
            safe_failure_reply=herdres.TENDWIRE_SAFE_SEND_FAILURE_REPLY,
        )
        self.assertTrue(duplicate["duplicate"])
        self.assertTrue(duplicate["succeeded"])
        self.assertEqual(duplicate["reply"], "")
        self.assertFalse(duplicate["ledger_changed"])

        queued = herdres_tendwire.send_instruction_attempt_result(
            {"response": {"status": "accepted", "result": {"delivery_state": "queued"}}, "ledger_changed": True},
            safe_failure_reply=herdres.TENDWIRE_SAFE_SEND_FAILURE_REPLY,
        )
        self.assertFalse(queued["duplicate"])
        self.assertTrue(queued["succeeded"])
        self.assertTrue(queued["ledger_changed"])
        self.assertEqual(queued["reply"], "Queued for Tendwire worker.")

        failed = herdres_tendwire.send_instruction_attempt_result(
            {"response": {"status": "backend_unavailable"}, "ledger_changed": True},
            safe_failure_reply=herdres.TENDWIRE_SAFE_SEND_FAILURE_REPLY,
        )
        self.assertFalse(failed["succeeded"])
        self.assertEqual(failed["status"], "backend_unavailable")
        self.assertEqual(failed["reply"], herdres.TENDWIRE_SAFE_SEND_FAILURE_REPLY)

    def test_duplicate_request_mismatched_payload_fails_without_direct_fallback(self) -> None:
        entry = _entry()
        with patch.dict(os.environ, {"HERDRES_TENDWIRE_MODE": "commands"}, clear=True), \
                patch.object(herdres, "tendwire_command", return_value={"status": "duplicate_request", "payload_mismatch": True}), \
                patch.object(herdres, "send_to_pane", return_value=(True, "")) as send_to_pane:
            result = herdres.forward_text_to_pane_response("pane-1", "continue", state={"panes": {}}, entry=entry)

        self.assertEqual(result["reply"], herdres.TENDWIRE_SAFE_SEND_FAILURE_REPLY)
        send_to_pane.assert_not_called()

    def test_command_process_failures_fail_without_direct_fallback(self) -> None:
        cases = (
            ("non_json", _completed("not json")),
            ("malformed", _completed("{")),
            ("non_object", _completed('"ok"')),
            ("nonzero", _completed("", returncode=1, stderr="boom")),
        )
        for name, proc in cases:
            entry = _entry()
            with self.subTest(name=name), \
                    patch.dict(os.environ, {"HERDRES_TENDWIRE_MODE": "commands"}, clear=True), \
                    patch.object(herdres, "run_cmd", return_value=proc), \
                    patch.object(herdres, "send_to_pane", return_value=(True, "")) as send_to_pane:
                result = herdres.forward_text_to_pane_response("pane-1", "continue", state={"panes": {}}, entry=entry)

            self.assertEqual(result["reply"], herdres.TENDWIRE_SAFE_SEND_FAILURE_REPLY)
            send_to_pane.assert_not_called()

    def test_partial_metadata_fails_closed_unless_direct_fallback_enabled(self) -> None:
        entry = _entry(tendwire_fingerprint="")
        with patch.dict(os.environ, {"HERDRES_TENDWIRE_MODE": "commands"}, clear=True), \
                patch.object(herdres, "send_to_pane", return_value=(True, "")) as send_to_pane:
            result = herdres.forward_text_to_pane_response("pane-1", "continue", state={"panes": {}}, entry=entry)

        self.assertEqual(result["reply"], herdres.TENDWIRE_SAFE_SEND_FAILURE_REPLY)
        send_to_pane.assert_not_called()

        with patch.dict(os.environ, {"HERDRES_TENDWIRE_MODE": "commands", "HERDRES_TENDWIRE_DIRECT_FALLBACK": "1"}, clear=True), \
                patch.object(herdres, "send_to_pane", return_value=(True, "queued")) as send_to_pane, \
                patch.object(herdres, "save_state"):
            result = herdres.forward_text_to_pane_response("pane-1", "continue", state={"panes": {}}, entry=entry)

        self.assertEqual(result["reply"], "queued")
        send_to_pane.assert_called_once_with("pane-1", "continue")

    def test_no_metadata_continues_to_use_legacy_direct_send(self) -> None:
        entry = _entry()
        for key in herdres_tendwire.ENTRY_METADATA_KEYS:
            entry.pop(key, None)
        with patch.dict(os.environ, {"HERDRES_TENDWIRE_MODE": "commands"}, clear=True), \
                patch.object(herdres, "send_to_pane", return_value=(True, "queued")) as send_to_pane, \
                patch.object(herdres, "tendwire_command") as tendwire_command, \
                patch.object(herdres, "save_state"):
            result = herdres.forward_text_to_pane_response("pane-1", "continue", state={"panes": {}}, entry=entry)

        self.assertEqual(result["reply"], "queued")
        send_to_pane.assert_called_once_with("pane-1", "continue")
        tendwire_command.assert_not_called()

    def test_stale_pseudo_tendwire_source_entry_stays_read_only(self) -> None:
        entry = _entry(source="tendwire", pane_id="tendwire:worker-1")
        with patch.dict(os.environ, {"HERDRES_TENDWIRE_MODE": "commands", "HERDRES_TENDWIRE_DIRECT_FALLBACK": "1"}, clear=True), \
                patch.object(herdres, "send_to_pane") as send_to_pane, \
                patch.object(herdres, "tendwire_command") as tendwire_command:
            result = herdres.forward_text_to_pane_response("tendwire:worker-1", "continue", state={"panes": {}}, entry=entry)

        self.assertIn("Tendwire status entry", result["reply"])
        send_to_pane.assert_not_called()
        tendwire_command.assert_not_called()

    def test_source_read_tendwire_failure_never_uses_direct_fallback(self) -> None:
        entry = _entry(source="tendwire", entry_type="worker", pane_id="", worker_id="worker-1", worker_fingerprint="fp-1")
        with patch.dict(
            os.environ,
            {"HERDRES_TENDWIRE_MODE": "source-read", "HERDRES_TENDWIRE_DIRECT_FALLBACK": "1"},
            clear=True,
        ), \
                patch.object(herdres, "tendwire_command", return_value={"status": "stale_target"}), \
                patch.object(herdres, "send_to_pane") as send_to_pane:
            result = herdres.forward_text_to_pane_response(
                "",
                "continue",
                state={"panes": {}},
                entry=entry,
            )

        self.assertEqual(result["reply"], herdres.TENDWIRE_SAFE_SEND_FAILURE_REPLY)
        send_to_pane.assert_not_called()

    def test_source_read_attachment_and_raw_do_not_call_herdr(self) -> None:
        entry = _entry(source="tendwire", entry_type="worker", pane_id="", worker_id="worker-1", worker_fingerprint="fp-1")
        state = _state(entry)
        with patch.dict(os.environ, {"HERDRES_TENDWIRE_MODE": "source-read"}, clear=True), \
                self._command_patches(state) as patched, \
                patch.object(herdres, "deliver_attachment") as deliver_attachment:
            result = herdres.command_reply(
                _payload(text="", attachment={"kind": "document", "file_id": "file-1"})
            )

        self.assertIn("Attachments and voice notes are not available", result["reply"])
        deliver_attachment.assert_not_called()
        patched["send_to_pane"].assert_not_called()
        patched["run_cmd"].assert_not_called()

        with patch.dict(os.environ, {"HERDRES_TENDWIRE_MODE": "source-read"}, clear=True), \
                self._command_patches(state) as patched, \
                patch.object(herdres, "recent_tail") as recent_tail:
            result = herdres.command_reply(_payload(text="/raw 20"))

        self.assertIn("Raw pane output is not available", result["reply"])
        recent_tail.assert_not_called()
        patched["send_to_pane"].assert_not_called()
        patched["run_cmd"].assert_not_called()

        legacy_entry = _entry(source="herdr", pane_id="pane-1")
        legacy_state = _state(legacy_entry)
        with patch.dict(os.environ, {"HERDRES_TENDWIRE_MODE": "source-read"}, clear=True), \
                self._command_patches(legacy_state) as patched, \
                patch.object(herdres, "recent_tail") as recent_tail:
            result = herdres.command_reply(_payload(text="/raw 20"))

        self.assertIn("legacy Herdr mode", result["reply"])
        recent_tail.assert_not_called()
        patched["send_to_pane"].assert_not_called()
        patched["run_cmd"].assert_not_called()

    def test_source_mode_legacy_entry_does_not_call_herdr_or_tendwire(self) -> None:
        entry = _entry(source="herdr", pane_id="pane-1")
        for key in herdres_tendwire.ENTRY_METADATA_KEYS:
            entry.pop(key, None)
        state = _state(entry)
        with patch.dict(os.environ, {"HERDRES_TENDWIRE_MODE": "source"}, clear=True), \
                self._command_patches(state) as patched:
            result = herdres.command_reply(_payload())

        self.assertIn("legacy Herdr mode", result["reply"])
        patched["send_to_pane"].assert_not_called()
        patched["run_cmd"].assert_not_called()

    def test_source_mode_legacy_entry_with_stale_metadata_fails_closed_in_send_helper(self) -> None:
        entry = _entry(source="herdr", pane_id="pane-1")
        with patch.dict(os.environ, {"HERDRES_TENDWIRE_MODE": "source"}, clear=True), \
                patch.object(herdres, "run_cmd") as run_cmd, \
                patch.object(herdres, "send_to_pane") as send_to_pane:
            result = herdres.forward_text_to_pane_response(
                "pane-1",
                "continue",
                state={"panes": {"pane-1": entry}},
                entry=entry,
            )

        self.assertIn("legacy Herdr mode", result["reply"])
        run_cmd.assert_not_called()
        send_to_pane.assert_not_called()

    def test_source_mode_new_and_keys_do_not_call_herdr(self) -> None:
        source_entry = _entry(source="tendwire", entry_type="worker", pane_id="", worker_id="worker-1", worker_fingerprint="fp-1")
        state = _state(source_entry)
        with patch.dict(os.environ, {"HERDRES_TENDWIRE_MODE": "source"}, clear=True), \
                self._command_patches(state) as patched, \
                patch.object(herdres, "new_agent_pane_response") as new_agent:
            new_result = herdres.command_reply(_payload(text="/new codex"))

        self.assertIn("disabled in Tendwire source mode", new_result["reply"])
        new_agent.assert_not_called()
        patched["send_to_pane"].assert_not_called()
        patched["run_cmd"].assert_not_called()

        with patch.dict(os.environ, {"HERDRES_TENDWIRE_MODE": "source"}, clear=True), \
                self._command_patches(state) as patched:
            keys_result = herdres.command_reply(_payload(text="/keys enter"))

        self.assertIn("Raw key delivery is not available", keys_result["reply"])
        patched["send_to_pane"].assert_not_called()
        patched["run_cmd"].assert_not_called()

        legacy_entry = _entry(source="herdr", pane_id="pane-1")
        legacy_state = _state(legacy_entry)
        with patch.dict(os.environ, {"HERDRES_TENDWIRE_MODE": "source"}, clear=True), \
                self._command_patches(legacy_state) as patched:
            legacy_keys_result = herdres.command_reply(_payload(text="/keys enter"))

        self.assertIn("legacy Herdr mode", legacy_keys_result["reply"])
        patched["send_to_pane"].assert_not_called()
        patched["run_cmd"].assert_not_called()

    def test_source_mode_voice_attachment_fails_before_direct_herdr_delivery(self) -> None:
        source_entry = _entry(
            source="tendwire",
            entry_type="worker",
            pane_id="",
            worker_id="worker-1",
            worker_fingerprint="fp-1",
        )
        state = _state(source_entry)
        payload = _payload(
            text="",
            attachment={"kind": "voice", "file_id": "voice-file"},
        )
        with patch.dict(os.environ, {"HERDRES_TENDWIRE_MODE": "source"}, clear=True), \
                self._command_patches(state) as patched, \
                patch.object(herdres, "deliver_attachment") as deliver_attachment:
            result = herdres.command_reply(payload)

        self.assertIn("not available in Tendwire source-read mode", result["reply"])
        deliver_attachment.assert_not_called()
        patched["send_to_pane"].assert_not_called()
        patched["run_cmd"].assert_not_called()

    def test_source_read_callback_choice_uses_tendwire_command_not_herdr(self) -> None:
        entry = _entry(
            source="tendwire",
            entry_type="worker",
            pane_id="",
            worker_id="worker-1",
            worker_fingerprint="fp-1",
            active_prompt={
                "id": "prompt-1",
                "message_id": "1001",
                "source": "skills",
                "text": "Choose",
                "created_at": "9999-01-01T00:00:00+00:00",
                "options": [{"number": "1", "label": "Yes", "send_text": "yes"}],
            },
        )
        state = _state(entry)
        payload = {
            "chat_id": "-1001",
            "topic_id": "77",
            "message_id": "1001",
            "user_id": "42",
            "data": "herdr:c:prompt-1:1",
        }
        with patch.dict(os.environ, {"HERDRES_TENDWIRE_MODE": "source-read"}, clear=True), \
                patch.object(herdres, "load_dotenv"), \
                patch.object(herdres, "load_state", return_value=state), \
                patch.object(herdres, "save_state") as save_state, \
                patch.object(herdres, "send_to_pane") as send_to_pane, \
                patch.object(herdres, "send_notice") as send_notice, \
                patch.object(herdres, "run_cmd", return_value=_completed({"status": "accepted"})) as run_cmd:
            result = herdres.callback_reply(payload)

        self.assertEqual(result["answer"], "Selected 1.")
        self.assertNotIn("active_prompt", entry)
        send_to_pane.assert_not_called()
        send_notice.assert_called_once()
        run_cmd.assert_called_once()
        request = json.loads(run_cmd.call_args.kwargs["input_text"])
        self.assertEqual(request["action"], "send_instruction")
        self.assertEqual(request["target"], {"worker_id": "worker-1", "worker_fingerprint": "fp-1"})
        self.assertEqual(request["instruction"]["text"], "yes")
        self.assertEqual(request["params"]["telegram_origin"], "choice")
        self.assertNotIn("-1001", json.dumps(request, sort_keys=True))
        self.assertNotIn('"77"', json.dumps(request, sort_keys=True))
        self.assertNotIn("1001", json.dumps(request, sort_keys=True))
        save_state.assert_called_once_with(state)

    def test_source_read_callback_choice_missing_metadata_fails_closed(self) -> None:
        entry = _entry(
            source="tendwire",
            entry_type="worker",
            pane_id="",
            worker_id="worker-1",
            worker_fingerprint="",
            tendwire_fingerprint="",
            active_prompt={
                "id": "prompt-1",
                "message_id": "1001",
                "source": "skills",
                "text": "Choose",
                "created_at": "9999-01-01T00:00:00+00:00",
                "options": [{"number": "1", "label": "Yes", "send_text": "yes"}],
            },
        )
        state = _state(entry)
        payload = {
            "chat_id": "-1001",
            "topic_id": "77",
            "message_id": "1001",
            "user_id": "42",
            "data": "herdr:c:prompt-1:1",
        }
        with patch.dict(os.environ, {"HERDRES_TENDWIRE_MODE": "source-read"}, clear=True), \
                patch.object(herdres, "load_dotenv"), \
                patch.object(herdres, "load_state", return_value=state), \
                patch.object(herdres, "save_state") as save_state, \
                patch.object(herdres, "send_to_pane") as send_to_pane, \
                patch.object(herdres, "run_cmd") as run_cmd:
            result = herdres.callback_reply(payload)

        self.assertEqual(result["answer"], herdres.TENDWIRE_SAFE_SEND_FAILURE_REPLY)
        self.assertTrue(result["show_alert"])
        self.assertNotIn("active_prompt", entry)
        send_to_pane.assert_not_called()
        run_cmd.assert_not_called()
        save_state.assert_called_once_with(state)

    def test_source_mode_legacy_callback_fails_closed_without_herdr(self) -> None:
        entry = _entry(
            source="herdr",
            pane_id="pane-1",
            active_prompt={
                "id": "prompt-1",
                "message_id": "1001",
                "text": "Choose",
                "created_at": "9999-01-01T00:00:00+00:00",
                "options": [{"number": "1", "label": "Yes", "send_text": "yes"}],
            },
        )
        state = _state(entry)
        payload = {
            "chat_id": "-1001",
            "topic_id": "77",
            "message_id": "1001",
            "user_id": "42",
            "data": "herdr:c:prompt-1:1",
        }
        with patch.dict(os.environ, {"HERDRES_TENDWIRE_MODE": "source"}, clear=True), \
                patch.object(herdres, "load_dotenv"), \
                patch.object(herdres, "load_state", return_value=state), \
                patch.object(herdres, "clear_disabled_visible_choice_state", return_value=False), \
                patch.object(herdres, "save_state") as save_state, \
                patch.object(herdres, "pane_by_id") as pane_by_id, \
                patch.object(herdres, "send_to_pane") as send_to_pane, \
                patch.object(herdres, "run_cmd") as run_cmd:
            result = herdres.callback_reply(payload)

        self.assertIn("legacy Herdr mode", result["answer"])
        self.assertTrue(result["show_alert"])
        pane_by_id.assert_not_called()
        send_to_pane.assert_not_called()
        run_cmd.assert_not_called()
        save_state.assert_not_called()

    def test_source_read_visible_choice_callback_does_not_refresh_from_herdr(self) -> None:
        entry = _entry(
            source="tendwire",
            entry_type="worker",
            pane_id="",
            worker_id="worker-1",
            worker_fingerprint="fp-1",
            active_prompt={
                "id": "oldprompt",
                "message_id": "1001",
                "text": "Choose",
                "created_at": "9999-01-01T00:00:00+00:00",
                "item": {"kind": "choices", "turn_id": "visible-choice:oldprompt"},
                "options": [{"number": "1", "label": "Yes", "send_text": "yes"}],
            },
        )
        state = _state(entry)
        payload = {
            "chat_id": "-1001",
            "topic_id": "77",
            "message_id": "1001",
            "user_id": "42",
            "data": "herdr:c:oldprompt:1",
        }
        with patch.dict(os.environ, {"HERDRES_TENDWIRE_MODE": "source-read"}, clear=True), \
                patch.object(herdres, "load_dotenv"), \
                patch.object(herdres, "load_state", return_value=state), \
                patch.object(herdres, "clear_disabled_visible_choice_state", return_value=False), \
                patch.object(herdres, "save_state") as save_state, \
                patch.object(herdres, "pane_by_id") as pane_by_id, \
                patch.object(herdres, "send_to_pane") as send_to_pane, \
                patch.object(herdres, "VISIBLE_CHOICE_BUTTONS_ENABLED", True), \
                patch.object(herdres, "run_cmd", return_value=_completed({"status": "accepted"})) as run_cmd:
            result = herdres.callback_reply(payload)

        self.assertEqual(result["answer"], "Selected 1.")
        pane_by_id.assert_not_called()
        send_to_pane.assert_not_called()
        run_cmd.assert_called_once()
        save_state.assert_called_once_with(state)

    def test_source_read_callback_detail_choice_uses_tendwire_followup(self) -> None:
        entry = _entry(
            source="tendwire",
            entry_type="worker",
            pane_id="",
            worker_id="worker-1",
            worker_fingerprint="fp-1",
            active_prompt={
                "id": "prompt-1",
                "message_id": "1001",
                "source": "skills",
                "text": "Choose",
                "created_at": "9999-01-01T00:00:00+00:00",
                "options": [{"number": "1", "label": "Yes", "send_text": "yes"}],
            },
        )
        state = _state(entry)
        payload = {
            "chat_id": "-1001",
            "topic_id": "77",
            "message_id": "1001",
            "user_id": "42",
            "data": "herdr:d:prompt-1:1",
        }
        with patch.dict(os.environ, {"HERDRES_TENDWIRE_MODE": "source-read"}, clear=True), \
                patch.object(herdres, "load_dotenv"), \
                patch.object(herdres, "load_state", return_value=state), \
                patch.object(herdres, "save_state") as save_state, \
                patch.object(herdres, "send_notice", return_value={"ok": True, "message_id": "2002"}) as send_notice, \
                patch.object(herdres, "send_to_pane") as send_to_pane, \
                patch.object(herdres, "run_cmd") as run_cmd:
            result = herdres.callback_reply(payload)

        self.assertEqual(result["answer"], "Write the details in this topic.")
        self.assertEqual(entry["awaiting_detail"]["choice"], "yes")
        self.assertEqual(entry["awaiting_detail"]["force_reply_message_id"], "2002")
        send_notice.assert_called_once()
        send_to_pane.assert_not_called()
        run_cmd.assert_not_called()
        save_state.assert_any_call(state)

        with patch.dict(os.environ, {"HERDRES_TENDWIRE_MODE": "source-read"}, clear=True), \
                patch.object(herdres, "load_dotenv"), \
                patch.object(herdres, "load_state", return_value=state), \
                patch.object(herdres, "save_state") as save_state, \
                patch.object(herdres, "send_to_pane") as send_to_pane, \
                patch.object(herdres, "run_cmd", return_value=_completed({"status": "accepted"})) as run_cmd:
            followup = herdres.command_reply(
                _payload(
                    text="because it is safer",
                    message_id="5001",
                    reply_to_message_id="2002",
                )
            )

        self.assertEqual(followup["reply"], "Sent details.")
        self.assertNotIn("awaiting_detail", entry)
        self.assertNotIn("active_prompt", entry)
        send_to_pane.assert_not_called()
        run_cmd.assert_called_once()
        request = json.loads(run_cmd.call_args.kwargs["input_text"])
        self.assertEqual(request["action"], "send_instruction")
        self.assertEqual(request["instruction"]["text"], "yes\nbecause it is safer")
        self.assertEqual(request["params"]["telegram_origin"], "choice_detail")
        encoded = json.dumps(request, sort_keys=True)
        self.assertNotIn("-1001", encoded)
        self.assertNotIn('"77"', encoded)
        self.assertNotIn("5001", encoded)
        self.assertNotIn("2002", encoded)
        save_state.assert_any_call(state)

    def test_source_read_callback_option_needing_detail_uses_tendwire_followup(self) -> None:
        entry = _entry(
            source="tendwire",
            entry_type="worker",
            pane_id="",
            worker_id="worker-1",
            worker_fingerprint="fp-1",
            active_prompt={
                "id": "prompt-1",
                "message_id": "1001",
                "source": "skills",
                "text": "Choose",
                "created_at": "9999-01-01T00:00:00+00:00",
                "options": [{"number": "1", "label": "Yes", "send_text": "yes", "needs_detail": True}],
            },
        )
        state = _state(entry)
        payload = {
            "chat_id": "-1001",
            "topic_id": "77",
            "message_id": "1001",
            "user_id": "42",
            "data": "herdr:c:prompt-1:1",
        }
        with patch.dict(os.environ, {"HERDRES_TENDWIRE_MODE": "source-read"}, clear=True), \
                patch.object(herdres, "load_dotenv"), \
                patch.object(herdres, "load_state", return_value=state), \
                patch.object(herdres, "save_state") as save_state, \
                patch.object(herdres, "send_notice", return_value={"ok": True, "message_id": "2003"}) as send_notice, \
                patch.object(herdres, "send_to_pane") as send_to_pane, \
                patch.object(herdres, "run_cmd") as run_cmd:
            result = herdres.callback_reply(payload)

        self.assertEqual(result["answer"], "Write the details in this topic.")
        self.assertEqual(entry["awaiting_detail"]["choice"], "yes")
        self.assertEqual(entry["awaiting_detail"]["force_reply_message_id"], "2003")
        send_notice.assert_called_once()
        send_to_pane.assert_not_called()
        run_cmd.assert_not_called()
        save_state.assert_called_once_with(state)

        with patch.dict(os.environ, {"HERDRES_TENDWIRE_MODE": "source-read"}, clear=True), \
                patch.object(herdres, "load_dotenv"), \
                patch.object(herdres, "load_state", return_value=state), \
                patch.object(herdres, "save_state") as save_state, \
                patch.object(herdres, "send_to_pane") as send_to_pane, \
                patch.object(herdres, "run_cmd", return_value=_completed({"status": "accepted"})) as run_cmd:
            followup = herdres.command_reply(
                _payload(
                    text="extra context",
                    message_id="5002",
                    reply_to_message_id="2003",
                )
            )

        self.assertEqual(followup["reply"], "Sent details.")
        self.assertNotIn("awaiting_detail", entry)
        self.assertNotIn("active_prompt", entry)
        send_to_pane.assert_not_called()
        run_cmd.assert_called_once()
        request = json.loads(run_cmd.call_args.kwargs["input_text"])
        self.assertEqual(request["instruction"]["text"], "yes\nextra context")
        self.assertEqual(request["params"]["telegram_origin"], "choice_detail")
        save_state.assert_any_call(state)

    def test_source_read_callback_custom_detail_uses_tendwire_followup(self) -> None:
        entry = _entry(
            source="tendwire",
            entry_type="worker",
            pane_id="",
            worker_id="worker-1",
            worker_fingerprint="fp-1",
            active_prompt={
                "id": "prompt-1",
                "message_id": "1001",
                "source": "skills",
                "text": "Choose",
                "created_at": "9999-01-01T00:00:00+00:00",
                "options": [{"number": "1", "label": "Yes", "send_text": "yes"}],
            },
        )
        state = _state(entry)
        payload = {
            "chat_id": "-1001",
            "topic_id": "77",
            "message_id": "1001",
            "user_id": "42",
            "data": "herdr:d:prompt-1:custom",
        }
        with patch.dict(os.environ, {"HERDRES_TENDWIRE_MODE": "source-read"}, clear=True), \
                patch.object(herdres, "load_dotenv"), \
                patch.object(herdres, "load_state", return_value=state), \
                patch.object(herdres, "save_state") as save_state, \
                patch.object(herdres, "send_notice", return_value={"ok": True, "message_id": "2004"}) as send_notice, \
                patch.object(herdres, "send_to_pane") as send_to_pane, \
                patch.object(herdres, "run_cmd") as run_cmd:
            result = herdres.callback_reply(payload)

        self.assertEqual(result["answer"], "Write the instruction in this topic.")
        self.assertEqual(entry["awaiting_detail"]["choice"], "")
        self.assertEqual(entry["awaiting_detail"]["force_reply_message_id"], "2004")
        send_notice.assert_called_once()
        send_to_pane.assert_not_called()
        run_cmd.assert_not_called()
        save_state.assert_called_once_with(state)

        with patch.dict(os.environ, {"HERDRES_TENDWIRE_MODE": "source-read"}, clear=True), \
                patch.object(herdres, "load_dotenv"), \
                patch.object(herdres, "load_state", return_value=state), \
                patch.object(herdres, "save_state") as save_state, \
                patch.object(herdres, "send_to_pane") as send_to_pane, \
                patch.object(herdres, "run_cmd", return_value=_completed({"status": "accepted"})) as run_cmd:
            followup = herdres.command_reply(
                _payload(
                    text="custom instruction",
                    message_id="5003",
                    reply_to_message_id="2004",
                )
            )

        self.assertEqual(followup["reply"], "Sent details.")
        send_to_pane.assert_not_called()
        request = json.loads(run_cmd.call_args.kwargs["input_text"])
        self.assertEqual(request["instruction"]["text"], "custom instruction")
        self.assertEqual(request["params"]["telegram_origin"], "choice_detail")
        save_state.assert_any_call(state)

    def test_send_force_fails_closed_for_command_mode_enriched_entry(self) -> None:
        entry = _entry()
        with patch.dict(os.environ, {"HERDRES_TENDWIRE_MODE": "commands"}, clear=True), \
                patch.object(herdres, "interrupt_and_send_to_pane") as interrupt_and_send:
            result = herdres.interrupt_and_send_response("pane-1", "stop now", state={"panes": {}}, entry=entry)

        self.assertIn("cannot safely interrupt", result["reply"])
        interrupt_and_send.assert_not_called()

    def test_send_force_fails_closed_for_legacy_entry_in_source_mode(self) -> None:
        entry = _entry(source="herdr", pane_id="pane-1")
        with patch.dict(os.environ, {"HERDRES_TENDWIRE_MODE": "source"}, clear=True), \
                patch.object(herdres, "interrupt_and_send_to_pane") as interrupt_and_send:
            result = herdres.interrupt_and_send_response("pane-1", "stop now", state={"panes": {}}, entry=entry)

        self.assertIn("cannot safely interrupt", result["reply"])
        interrupt_and_send.assert_not_called()

    def test_agent_picker_pending_text_uses_tendwire_command_in_commands_mode(self) -> None:
        entry = _entry(agent="codex")
        pane_key = "pane-1"
        space = {
            "space_key": "workspace:workspace-1",
            "pane_keys": [pane_key],
            "pending_pick": {
                "42": {
                    "text": "continue",
                    "set_at": herdres.utc_now(),
                    "request_context": {
                        "chat_id": "-1001",
                        "topic_id": "77",
                        "message_id": "5000",
                        "origin": "plain",
                    },
                },
            },
        }
        state = {"panes": {pane_key: entry}, "spaces": {"workspace:workspace-1": space}}
        token = herdres.agent_picker_pane_tokens([(pane_key, entry)])[pane_key]

        with patch.dict(os.environ, {"HERDRES_TENDWIRE_MODE": "commands"}, clear=True), \
                patch.object(herdres, "run_cmd", return_value=_completed({"status": "accepted"})) as run_cmd, \
                patch.object(herdres, "send_to_pane", return_value=(True, "")) as send_to_pane, \
                patch.object(herdres, "save_state") as save_state, \
                patch.object(herdres, "telegram_api") as telegram_api, \
                patch.object(herdres, "fresh_cached_pane_turn", return_value={"turn_id": "turn-before"}):
            result = herdres.handle_agent_pick_callback(
                state,
                {},
                "-1001",
                "77",
                "6000",
                "42",
                space,
                ["herdr", "pick", "workspace:workspace-1", token],
            )

        self.assertTrue(result["answer"].startswith("Sent to"))
        send_to_pane.assert_not_called()
        run_cmd.assert_called_once()
        request = json.loads(run_cmd.call_args.kwargs["input_text"])
        self.assertEqual(request["instruction"]["text"], "continue")
        self.assertNotIn("message_id", request["params"])
        self.assertNotIn("callback_message_id", request["params"])
        self.assertNotIn("5000", json.dumps(request, sort_keys=True))
        self.assertNotIn("6000", json.dumps(request, sort_keys=True))
        self.assertNotIn("42", space["pending_pick"])
        save_state.assert_called_once_with(state)
        telegram_api.assert_called_once()

    def test_agent_picker_tendwire_failure_consumes_pending_and_reports_not_sent(self) -> None:
        entry = _entry(agent="codex")
        pane_key = "pane-1"
        space = {
            "space_key": "workspace:workspace-1",
            "pane_keys": [pane_key],
            "pending_pick": {"42": {"text": "continue", "set_at": herdres.utc_now()}},
        }
        state = {"panes": {pane_key: entry}, "spaces": {"workspace:workspace-1": space}}
        token = herdres.agent_picker_pane_tokens([(pane_key, entry)])[pane_key]

        with patch.dict(os.environ, {"HERDRES_TENDWIRE_MODE": "commands"}, clear=True), \
                patch.object(herdres, "tendwire_command", return_value={"status": "stale_target"}), \
                patch.object(herdres, "send_to_pane", return_value=(True, "")) as send_to_pane, \
                patch.object(herdres, "save_state"), \
                patch.object(herdres, "telegram_api") as telegram_api:
            result = herdres.handle_agent_pick_callback(
                state,
                {},
                "-1001",
                "77",
                "6000",
                "42",
                space,
                ["herdr", "pick", "workspace:workspace-1", token],
            )

        self.assertEqual(result["answer"], "Not sent.")
        send_to_pane.assert_not_called()
        self.assertNotIn("42", space["pending_pick"])
        self.assertEqual(telegram_api.call_args.args[1]["text"], herdres.TENDWIRE_SAFE_SEND_FAILURE_REPLY)

    def test_source_read_agent_picker_missing_metadata_fails_closed(self) -> None:
        entry = _entry(source="tendwire", entry_type="worker", pane_id="", worker_id="worker-1", worker_fingerprint="fp-1", agent="codex")
        for key in herdres_tendwire.ENTRY_METADATA_KEYS:
            entry.pop(key, None)
        pane_key = "pane-1"
        space = {
            "space_key": "workspace:workspace-1",
            "pane_keys": [pane_key],
            "pending_pick": {"42": {"text": "continue", "set_at": herdres.utc_now()}},
        }
        state = {"panes": {pane_key: entry}, "spaces": {"workspace:workspace-1": space}}
        token = herdres.agent_picker_pane_tokens([(pane_key, entry)])[pane_key]

        with patch.dict(os.environ, {"HERDRES_TENDWIRE_MODE": "source-read"}, clear=True), \
                patch.object(herdres, "run_cmd") as run_cmd, \
                patch.object(herdres, "send_to_pane", return_value=(True, "")) as send_to_pane, \
                patch.object(herdres, "save_state") as save_state, \
                patch.object(herdres, "telegram_api") as telegram_api:
            result = herdres.handle_agent_pick_callback(
                state,
                {},
                "-1001",
                "77",
                "6000",
                "42",
                space,
                ["herdr", "pick", "workspace:workspace-1", token],
            )

        self.assertEqual(result["answer"], "Not sent.")
        run_cmd.assert_not_called()
        send_to_pane.assert_not_called()
        self.assertNotIn("42", space["pending_pick"])
        save_state.assert_called_once_with(state)
        self.assertEqual(telegram_api.call_args.args[1]["text"], herdres.TENDWIRE_SAFE_SEND_FAILURE_REPLY)

    def test_source_mode_agent_picker_legacy_entry_does_not_call_herdr_or_tendwire(self) -> None:
        entry = _entry(source="herdr", pane_id="pane-1", agent="codex")
        for key in herdres_tendwire.ENTRY_METADATA_KEYS:
            entry.pop(key, None)
        pane_key = "pane-1"
        space = {
            "space_key": "workspace:workspace-1",
            "pane_keys": [pane_key],
            "pending_pick": {"42": {"text": "continue", "set_at": herdres.utc_now()}},
        }
        state = {"panes": {pane_key: entry}, "spaces": {"workspace:workspace-1": space}}
        token = herdres.agent_picker_pane_tokens([(pane_key, entry)])[pane_key]

        with patch.dict(os.environ, {"HERDRES_TENDWIRE_MODE": "source"}, clear=True), \
                patch.object(herdres, "run_cmd") as run_cmd, \
                patch.object(herdres, "send_to_pane") as send_to_pane, \
                patch.object(herdres, "save_state") as save_state, \
                patch.object(herdres, "telegram_api") as telegram_api:
            result = herdres.handle_agent_pick_callback(
                state,
                {},
                "-1001",
                "77",
                "6000",
                "42",
                space,
                ["herdr", "pick", "workspace:workspace-1", token],
            )

        self.assertEqual(result["answer"], "Not sent.")
        run_cmd.assert_not_called()
        send_to_pane.assert_not_called()
        self.assertNotIn("42", space["pending_pick"])
        save_state.assert_called_once_with(state)
        self.assertIn("legacy Herdr mode", telegram_api.call_args.args[1]["text"])

    def test_source_mode_stale_new_pane_picker_does_not_call_herdr(self) -> None:
        space = {"space_key": "workspace:workspace-1"}
        state = {"panes": {}, "spaces": {"workspace:workspace-1": space}}

        with patch.dict(os.environ, {"HERDRES_TENDWIRE_MODE": "source"}, clear=True), \
                patch.object(herdres, "new_agent_pane_response") as new_agent:
            result = herdres.handle_new_pane_picker_callback(
                state,
                {},
                "-1001",
                "77",
                "6000",
                space,
                ["herdr", "new", "workspace:workspace-1", "codex"],
            )

        self.assertTrue(result["show_alert"])
        self.assertIn("disabled in Tendwire source mode", result["answer"])
        new_agent.assert_not_called()


if __name__ == "__main__":
    unittest.main()
