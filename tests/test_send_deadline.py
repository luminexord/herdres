from __future__ import annotations

import json
import subprocess
import unittest
from unittest.mock import Mock, patch

import herdres


class HerdrCommandTimeoutTests(unittest.TestCase):
    """A slow/unreachable pane's herdr command must degrade to a BridgeError (one pane
    unavailable), never an uncaught subprocess.TimeoutExpired that aborts the whole sync
    — which previously left already-delivered turns unsaved and re-delivered every cycle."""

    @staticmethod
    def _timeout_runner():
        def boom(args, **kw):
            raise subprocess.TimeoutExpired(cmd=args, timeout=kw.get("timeout", 8))
        return boom

    def test_herdr_json_converts_timeout_to_bridgeerror(self) -> None:
        with patch.object(herdres, "run_cmd", self._timeout_runner()):
            with self.assertRaises(herdres.BridgeError):
                herdres.herdr_json(["pane", "turn", "p1", "--last", "--format", "json"], timeout=8)

    def test_herdr_text_converts_timeout_to_bridgeerror(self) -> None:
        with patch.object(herdres, "run_cmd", self._timeout_runner()):
            with self.assertRaises(herdres.BridgeError):
                herdres.herdr_text(["pane", "read", "p1"], timeout=8)

    def test_pane_turn_degrades_to_unavailable_on_timeout(self) -> None:
        if hasattr(herdres, "_turn_cache"):
            herdres._turn_cache.clear()
        with patch.object(herdres, "run_cmd", self._timeout_runner()):
            turn = herdres.pane_turn("w:pTimeout")
        self.assertFalse(turn.get("available"))


class SendBudgetBoundTests(unittest.TestCase):
    """send_to_pane runs UNDER the global sync lock (command_reply is dispatched via
    with_lock), so its wall-time budget bounds how long a send to a BUSY pane can pin that
    lock — stalling the 30s timer sync and every other inbound command. Guard the budget
    against silently regressing to the old long value (which caused the ~40-60s 'ignored
    messages' stalls). The durable fix moves delivery off the lock entirely."""

    def test_budget_is_bounded_low_to_cap_lock_hold(self) -> None:
        # A busy-pane send must not pin the global lock for anywhere near a full 30s sync cycle.
        self.assertLessEqual(herdres.SEND_TO_PANE_BUDGET_SECONDS, 20)
        # ...but still leave room for at least ~two full-cap herdr calls to actually deliver.
        self.assertGreater(herdres.SEND_TO_PANE_BUDGET_SECONDS, herdres.SEND_TO_PANE_PER_CALL_CAP * 2)

    def test_budget_stays_under_gateway_command_timeout(self) -> None:
        # Documented invariant: BUDGET + PER_CALL_CAP <= COMMAND_TIMEOUT(60) - margin(15),
        # so send_to_pane self-bounds before the gateway SIGKILLs the subprocess.
        self.assertLessEqual(
            herdres.SEND_TO_PANE_BUDGET_SECONDS + herdres.SEND_TO_PANE_PER_CALL_CAP, 60 - 15
        )


class FakeClock:
    def __init__(self, start: float = 100.0) -> None:
        self.now = start

    def monotonic(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.now += max(0.0, float(seconds))


class HangingHerdr:
    def __init__(self, clock: FakeClock, deadline: float | None = None) -> None:
        self.clock = clock
        self.deadline = deadline
        self.records: list[dict[str, object]] = []

    def __call__(self, args: list[str], *, timeout: int = 10, input_text: str | None = None):
        start = self.clock.monotonic()
        self.records.append({
            "args": list(args),
            "timeout": timeout,
            "start": start,
            "remaining": None if self.deadline is None else self.deadline - start,
        })
        self.clock.sleep(timeout)
        proc = Mock()
        proc.returncode = 0
        proc.stderr = ""
        proc.stdout = ""
        if args[1:3] == ["pane", "list"]:
            proc.stdout = json.dumps({
                "result": {
                    "panes": [{
                        "pane_id": "pane-1",
                        "workspace_id": "workspace-1",
                        "agent": "codex",
                        "agent_status": "idle",
                    }]
                }
            })
        elif args[1:3] == ["workspace", "list"]:
            proc.stdout = json.dumps({"result": {"workspaces": []}})
        elif args[1:3] == ["pane", "read"]:
            proc.stdout = "❯ staged input"
        return proc


class SendDeadlineTests(unittest.TestCase):
    def test_send_to_pane_total_walltime_under_budget(self) -> None:
        clock = FakeClock()
        deadline = clock.monotonic() + herdres.SEND_TO_PANE_BUDGET_SECONDS
        runner = HangingHerdr(clock, deadline)

        with patch.object(herdres.time, "monotonic", clock.monotonic), patch.object(
            herdres.time, "sleep", clock.sleep
        ), patch.multiple(
            herdres,
            run_cmd=runner,
            pane_input_looks_staged=Mock(return_value=True),
        ):
            ok, detail = herdres.send_to_pane("pane-1", "deploy", deadline=deadline)

        self.assertFalse(ok)
        self.assertIn("Send timed out before delivery", detail)
        self.assertLess(clock.monotonic() - 100.0, herdres.SEND_TO_PANE_BUDGET_SECONDS + herdres.SEND_TO_PANE_PER_CALL_CAP)
        for record in runner.records:
            remaining = record["remaining"]
            self.assertIsNotNone(remaining)
            self.assertLessEqual(record["timeout"], remaining)

    def test_no_call_started_below_min_floor(self) -> None:
        clock = FakeClock()
        deadline = clock.monotonic() + 17.0
        runner = HangingHerdr(clock, deadline)

        with patch.object(herdres.time, "monotonic", clock.monotonic), patch.object(
            herdres.time, "sleep", clock.sleep
        ), patch.multiple(
            herdres,
            pane_by_id=Mock(return_value={"pane_id": "pane-1", "agent_status": "idle"}),
            run_cmd=runner,
            pane_input_looks_staged=Mock(return_value=True),
        ):
            ok, detail = herdres.send_to_pane("pane-1", "deploy", deadline=deadline)

        self.assertFalse(ok)
        self.assertIn("Send timed out before delivery", detail)
        for record in runner.records:
            self.assertGreater(record["remaining"], herdres.SEND_TO_PANE_MIN_CALL_SECONDS)

    def test_per_call_timeout_shrinks_near_deadline(self) -> None:
        clock = FakeClock()
        deadline = clock.monotonic() + 2.9
        runner = HangingHerdr(clock, deadline)

        with patch.object(herdres.time, "monotonic", clock.monotonic), patch.object(herdres, "run_cmd", runner):
            self.assertTrue(herdres.pane_input_ansi("pane-1", deadline=deadline))
            self.assertEqual(runner.records[-1]["timeout"], 2)
            ok, detail = herdres.send_to_pane("pane-1", "deploy", deadline=clock.monotonic() + 5.0)

        self.assertFalse(ok)
        self.assertIn("Send timed out before delivery", detail)

    def test_clear_loop_breaks_early_when_budget_exhausted(self) -> None:
        clock = FakeClock()
        deadline = clock.monotonic() + 12.0
        runner = HangingHerdr(clock, deadline)

        with patch.object(herdres.time, "monotonic", clock.monotonic), patch.object(
            herdres.time, "sleep", clock.sleep
        ), patch.multiple(
            herdres,
            run_cmd=runner,
            pane_input_looks_staged=Mock(return_value=True),
        ):
            ok, detail = herdres.clear_staged_pane_input("pane-1", deadline=deadline)

        self.assertTrue(ok)
        self.assertEqual(detail, "")
        clear_calls = [r for r in runner.records if r["args"][1:3] == ["pane", "send-keys"]]
        self.assertLess(len(clear_calls), len(herdres.CLEAR_STAGED_INPUT_KEY_SEQUENCES + herdres.CLEAR_STAGED_INPUT_FORCE_KEY_SEQUENCES))

    def test_run_budget_reserved_before_pane_run(self) -> None:
        clock = FakeClock()
        deadline = clock.monotonic() + 17.0
        runner = HangingHerdr(clock, deadline)

        with patch.object(herdres.time, "monotonic", clock.monotonic), patch.object(
            herdres.time, "sleep", clock.sleep
        ), patch.multiple(
            herdres,
            pane_by_id=Mock(return_value={"pane_id": "pane-1", "agent_status": "idle"}),
            run_cmd=runner,
            pane_input_looks_staged=Mock(return_value=True),
        ):
            ok, detail = herdres.send_to_pane("pane-1", "deploy", deadline=deadline)

        self.assertFalse(ok)
        self.assertIn("Send timed out before delivery", detail)
        self.assertFalse(any(r["args"][1:3] == ["pane", "run"] for r in runner.records))
        self.assertFalse(any(r["args"][1:3] == ["pane", "run"] and r["timeout"] < herdres.SEND_TO_PANE_MIN_CALL_SECONDS for r in runner.records))

    def test_busy_agent_still_reports_queued_with_deadline(self) -> None:
        clock = FakeClock()
        deadline = clock.monotonic() + herdres.SEND_TO_PANE_BUDGET_SECONDS

        with patch.object(herdres.time, "monotonic", clock.monotonic), patch.object(
            herdres.time, "sleep", lambda *_: None
        ), patch.multiple(
            herdres,
            run_cmd=Mock(return_value=Mock(returncode=0, stdout="", stderr="")),
            pane_input_looks_staged=Mock(return_value=True),
        ):
            ok, detail = herdres.submit_staged_pane_input_if_needed(
                "pane-1", agent_status="working", deadline=deadline
            )

        self.assertTrue(ok)
        self.assertIn("Queued", detail)
        self.assertNotIn("Send failed", detail)

    def test_non_working_budget_exhausted_reports_unconfirmed_not_failed(self) -> None:
        clock = FakeClock()
        deadline = clock.monotonic() + 6.0
        runner = HangingHerdr(clock, deadline)

        with patch.object(herdres.time, "monotonic", clock.monotonic), patch.object(
            herdres.time, "sleep", clock.sleep
        ), patch.multiple(
            herdres,
            run_cmd=runner,
            pane_input_looks_staged=Mock(return_value=True),
        ):
            ok, detail = herdres.submit_staged_pane_input_if_needed("pane-1", agent_status="idle", deadline=deadline)

        self.assertTrue(ok)
        self.assertIn("couldn't confirm it submitted within the time budget", detail)

    def test_detection_window_widens_for_multiline_input(self) -> None:
        staged = "\n".join(["❯ first line"] + [f"wrapped line {idx}" for idx in range(20)])

        with patch.object(herdres, "pane_input_ansi", Mock(return_value=staged)):
            self.assertFalse(herdres.pane_input_looks_staged("pane-1", window=16))
            self.assertTrue(herdres.pane_input_looks_staged("pane-1", window=24))

    def test_refusal_fallback_skipped_when_budget_exhausted(self) -> None:
        clock = FakeClock()
        deadline = clock.monotonic() + 11.0
        calls: list[list[str]] = []

        def run_cmd(args: list[str], *, timeout: int = 10, input_text: str | None = None):
            calls.append(list(args))
            clock.sleep(timeout)
            proc = Mock()
            proc.stdout = ""
            proc.stderr = ""
            if args[1:3] == ["pane", "run"]:
                proc.returncode = 1
                proc.stderr = "Could not clear existing staged pane input; refusing to append Telegram text."
            else:
                proc.returncode = 0
            return proc

        with patch.object(herdres.time, "monotonic", clock.monotonic), patch.object(
            herdres.time, "sleep", clock.sleep
        ), patch.multiple(
            herdres,
            pane_by_id=Mock(return_value={"pane_id": "pane-1", "agent_status": "idle"}),
            clear_staged_pane_input_if_needed=Mock(return_value=(True, "")),
            run_cmd=run_cmd,
            pane_input_looks_staged=Mock(return_value=False),
        ):
            ok, detail = herdres.send_to_pane("pane-1", "deploy", deadline=deadline)

        self.assertFalse(ok)
        self.assertIn("staged input could not be cleared", detail)
        self.assertFalse(any(call[1:3] == ["pane", "send-text"] for call in calls))
        self.assertFalse(any(call[1:4] == ["pane", "send-keys", "pane-1"] and call[-1] == "enter" for call in calls))

    def test_choice_and_visible_choice_share_one_budget(self) -> None:
        clock = FakeClock()
        choice_deadline = clock.monotonic() + herdres.SEND_TO_PANE_BUDGET_SECONDS
        captured: list[tuple[float, float]] = []

        def send_to_pane(_pane_id: str, _detail_text: str, **kwargs):
            deadline = kwargs["deadline"]
            captured.append((deadline, deadline - clock.monotonic()))
            return True, ""

        with patch.object(herdres.time, "monotonic", clock.monotonic), patch.object(
            herdres.time, "sleep", clock.sleep
        ), patch.multiple(
            herdres,
            pane_by_id=Mock(return_value={"pane_id": "pane-1"}),
            run_cmd=HangingHerdr(clock, choice_deadline),
            send_to_pane=send_to_pane,
        ):
            ok, detail = herdres.send_choice_detail_to_pane("pane-1", "2", "custom", deadline=choice_deadline)

        self.assertTrue(ok, detail)
        self.assertEqual(captured[-1][0], choice_deadline)
        self.assertLess(captured[-1][1], herdres.SEND_TO_PANE_BUDGET_SECONDS)

        visible_deadline = clock.monotonic() + herdres.SEND_TO_PANE_BUDGET_SECONDS
        visible_runner = HangingHerdr(clock, visible_deadline)
        with patch.object(herdres.time, "monotonic", clock.monotonic), patch.object(
            herdres.time, "sleep", clock.sleep
        ), patch.multiple(
            herdres,
            pane_by_id=Mock(return_value={"pane_id": "pane-1"}),
            run_cmd=visible_runner,
            wait_for_visible_custom_detail_field=Mock(return_value=True),
            send_to_pane=send_to_pane,
        ):
            ok, detail = herdres.send_visible_choice_detail_to_pane("pane-1", "4", "custom", deadline=visible_deadline)

        self.assertTrue(ok, detail)
        self.assertEqual(captured[-1][0], visible_deadline)
        self.assertLess(captured[-1][1], herdres.SEND_TO_PANE_BUDGET_SECONDS)

        failed = Mock(returncode=1, stdout="", stderr="selection failed")
        with patch.multiple(
            herdres,
            pane_by_id=Mock(return_value={"pane_id": "pane-1"}),
            run_cmd=Mock(return_value=failed),
        ):
            ok, detail = herdres.send_choice_detail_to_pane("pane-1", "2", "custom")
        self.assertFalse(ok)
        self.assertEqual(detail, "selection failed")

    def test_send_bang_unchanged(self) -> None:
        state = {
            "version": 1,
            "telegram": {"chat_id": "-1001", "owner_user_ids": ["42"]},
            "spaces": {
                "workspace:workspace-1": {
                    "space_key": "workspace:workspace-1",
                    "topic_id": "77",
                    "pane_keys": ["pane-1"],
                    "message_routes": {"1001": "pane-1"},
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
                }
            },
        }
        calls: list[list[str]] = []

        def run_cmd(args: list[str], **kwargs):
            calls.append(list(args))
            return Mock(returncode=0, stdout="", stderr="")

        with patch.object(herdres.time, "sleep", lambda *_: None), patch.multiple(
            herdres,
            load_dotenv=Mock(),
            load_state=Mock(return_value=state),
            save_state=Mock(),
            pane_by_id=Mock(return_value={"pane_id": "pane-1", "agent": "claude", "agent_status": "working"}),
            clear_staged_pane_input_if_needed=Mock(return_value=(True, "")),
            run_cmd=run_cmd,
            pane_input_looks_staged=Mock(return_value=False),
        ):
            result = herdres.command_reply({
                "chat_id": "-1001",
                "topic_id": "77",
                "message_id": "4000",
                "reply_to_message_id": "1001",
                "user_id": "42",
                "text": "/send! deploy now",
            })

        self.assertTrue(any("send-keys" in call and "escape" in call for call in calls), calls)
        self.assertTrue(any("run" in call for call in calls), calls)
        self.assertTrue(result["handled"])
        self.assertIn("Interrupted", result["reply"])
        self.assertNotIn("Queued", result["reply"])

    def test_off_path_callers_unchanged(self) -> None:
        calls: list[tuple[list[str], int]] = []

        def run_cmd(args: list[str], *, timeout: int = 10, input_text: str | None = None):
            calls.append((list(args), timeout))
            proc = Mock()
            proc.returncode = 0
            proc.stderr = ""
            if args[1:3] == ["pane", "list"]:
                proc.stdout = json.dumps({"result": {"panes": [{"pane_id": "pane-1", "workspace_id": "workspace-1"}]}})
            elif args[1:3] == ["workspace", "list"]:
                proc.stdout = json.dumps({"result": {"workspaces": []}})
            else:
                proc.stdout = "❯ staged"
            return proc

        with patch.object(herdres, "run_cmd", run_cmd):
            self.assertIsNotNone(herdres.pane_by_id("pane-1"))
            self.assertTrue(herdres.pane_list())
            self.assertTrue(herdres.pane_input_looks_staged("pane-1"))

        self.assertTrue(calls)
        self.assertTrue(all(timeout == 8 for _args, timeout in calls))
