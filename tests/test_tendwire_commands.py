from __future__ import annotations

import json
import os
import subprocess
import unittest
from contextlib import contextmanager
from unittest.mock import Mock, patch

import herdres


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
        request = herdres.build_tendwire_send_instruction_request(
            entry,
            text,
            origin="plain",
            chat_id="-1001",
            topic_id="77",
            message_id="5000",
            reply_to_message_id="1001",
        )
        again = herdres.build_tendwire_send_instruction_request(
            entry,
            text,
            origin="plain",
            chat_id="-1001",
            topic_id="77",
            message_id="5000",
            reply_to_message_id="1001",
        )
        changed = herdres.build_tendwire_send_instruction_request(
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
        self.assertIn("telegram:-1001:77:5000:worker-1", request["request_id"])


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
        entry = _entry(source="tendwire", pane_id="tendwire:worker-1")
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
        self.assertEqual(request["params"]["pane_id"], "tendwire:worker-1")
        self.assertEqual(request["instruction"]["text"], "first line\nsecond line")
        self.assertEqual(state["panes"]["pane-1"]["direct_origin_pane_id"], "tendwire:worker-1")

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
        for key in herdres.TENDWIRE_ENTRY_METADATA_KEYS:
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
        entry = _entry(source="tendwire", pane_id="tendwire:worker-1")
        with patch.dict(
            os.environ,
            {"HERDRES_TENDWIRE_MODE": "source-read", "HERDRES_TENDWIRE_DIRECT_FALLBACK": "1"},
            clear=True,
        ), \
                patch.object(herdres, "tendwire_command", return_value={"status": "stale_target"}), \
                patch.object(herdres, "send_to_pane") as send_to_pane:
            result = herdres.forward_text_to_pane_response(
                "tendwire:worker-1",
                "continue",
                state={"panes": {}},
                entry=entry,
            )

        self.assertEqual(result["reply"], herdres.TENDWIRE_SAFE_SEND_FAILURE_REPLY)
        send_to_pane.assert_not_called()

    def test_source_read_attachment_and_raw_do_not_call_herdr(self) -> None:
        entry = _entry(source="tendwire", pane_id="tendwire:worker-1")
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

    def test_source_read_callback_choice_does_not_call_herdr(self) -> None:
        entry = _entry(
            source="tendwire",
            pane_id="tendwire:worker-1",
            active_prompt={
                "id": "prompt-1",
                "message_id": "1001",
                "source": "skills",
                "text": "Choose",
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
        with patch.object(herdres, "load_dotenv"), \
                patch.object(herdres, "load_state", return_value=state), \
                patch.object(herdres, "save_state") as save_state, \
                patch.object(herdres, "send_to_pane") as send_to_pane:
            result = herdres.callback_reply(payload)

        self.assertEqual(result["answer"], "Choices are not available for Tendwire source-read entries yet.")
        self.assertTrue(result["show_alert"])
        self.assertNotIn("active_prompt", entry)
        send_to_pane.assert_not_called()
        save_state.assert_called_once_with(state)

    def test_send_force_fails_closed_for_command_mode_enriched_entry(self) -> None:
        entry = _entry()
        with patch.dict(os.environ, {"HERDRES_TENDWIRE_MODE": "commands"}, clear=True), \
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
        self.assertEqual(request["params"]["message_id"], "5000")
        self.assertEqual(request["params"]["callback_message_id"], "6000")
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
        entry = _entry(source="tendwire", pane_id="tendwire:worker-1", agent="codex")
        for key in herdres.TENDWIRE_ENTRY_METADATA_KEYS:
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


if __name__ == "__main__":
    unittest.main()
