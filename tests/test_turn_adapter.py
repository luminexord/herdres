import json
import os
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

import herdr_turn_adapter as adapter
from conftest import write_jsonl


class TurnAdapterTests(unittest.TestCase):
    def test_codex_extracts_completed_turn_from_task_complete(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "rollout-2026-06-15T00-00-00-session-1.jsonl"
            write_jsonl(
                path,
                [
                    {"type": "event_msg", "payload": {"type": "task_started", "turn_id": "turn-1", "started_at": 10}},
                    {
                        "type": "response_item",
                        "payload": {
                            "type": "message",
                            "role": "user",
                            "content": [{"type": "input_text", "text": "<environment_context>ignore</environment_context>"}],
                        },
                    },
                    {
                        "type": "response_item",
                        "payload": {
                            "type": "message",
                            "role": "user",
                            "content": [{"type": "input_text", "text": "<literal> What happened?"}],
                        },
                    },
                    {
                        "type": "response_item",
                        "payload": {
                            "type": "message",
                            "role": "assistant",
                            "content": [{"type": "output_text", "text": "progress update"}],
                        },
                    },
                    {
                        "type": "event_msg",
                        "payload": {
                            "type": "task_complete",
                            "turn_id": "turn-1",
                            "completed_at": 20,
                            "last_agent_message": "Final answer only.",
                        },
                    },
                ],
            )

            turn = adapter.extract_codex_turn(path, "pane-1", "session-1")

        self.assertTrue(turn["available"])
        self.assertTrue(turn["complete"])
        self.assertEqual(turn["turn_id"], "turn-1")
        self.assertEqual(turn["user_text"], "<literal> What happened?")
        self.assertEqual(turn["assistant_final_text"], "Final answer only.")

    def test_codex_worklog_captures_tool_calls(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "rollout-2026-06-15T00-00-00-session-1.jsonl"
            write_jsonl(
                path,
                [
                    {"type": "event_msg", "payload": {"type": "task_started", "turn_id": "t1", "started_at": 10}},
                    {"type": "response_item", "payload": {"type": "message", "role": "user",
                        "content": [{"type": "input_text", "text": "Build it."}]}},
                    {"type": "response_item", "payload": {"type": "function_call", "name": "exec_command",
                        "arguments": "{\"cmd\":\"go test ./...\\n(more)\"}"}},
                    {"type": "response_item", "payload": {"type": "custom_tool_call", "name": "apply_patch",
                        "input": "*** Begin Patch\n*** Update File: engine.go"}},
                    {"type": "response_item", "payload": {"type": "reasoning", "summary": [],
                        "content": None, "encrypted_content": "gAAAencryptedopaque"}},
                    {"type": "event_msg", "payload": {"type": "task_complete", "turn_id": "t1",
                        "completed_at": 20, "last_agent_message": "Built and tests pass."}},
                ],
            )
            turn = adapter.extract_codex_turn(path, "pane-1", "session-1")

        self.assertEqual(turn["assistant_final_text"], "Built and tests pass.")
        wl = turn.get("worklog_text") or ""
        self.assertIn("exec_command go test ./...", wl)  # arguments JSON parsed (cmd key)
        self.assertNotIn("(more)", wl)  # first line of the command only
        self.assertIn("apply_patch *** Begin Patch", wl)  # custom_tool_call input brief
        self.assertNotIn("Built and tests pass.", wl)  # final reply not duplicated into worklog
        self.assertNotIn("gAAAencryptedopaque", wl)  # encrypted reasoning never surfaced

    def test_devin_worklog_captures_tool_calls_and_interim_text(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "devin.json"
            path.write_text(json.dumps({"steps": [
                {"source": "user", "step_id": "s1", "timestamp": 1, "message": "Fix the bug."},
                {"source": "agent", "step_id": "s2", "timestamp": 2, "message": "Let me run the tests.",
                 "tool_calls": [{"type": "function", "function": {"name": "shell",
                                 "arguments": "{\"command\":\"go test ./...\"}"}}]},
                {"source": "agent", "step_id": "s3", "timestamp": 3, "message": "Fixed.", "tool_calls": []},
            ]}), encoding="utf-8")
            turn = adapter.extract_devin_turn(path, "pane-1", "session-1")

        self.assertEqual(turn["assistant_final_text"], "Fixed.")
        wl = turn.get("worklog_text") or ""
        self.assertIn("shell go test ./...", wl)  # OpenAI-style function.arguments parsed
        self.assertNotIn("Fixed.", wl)  # final reply not in worklog
        self.assertNotIn("Let me run the tests.", wl)  # step message excluded (it can become the final reply)

    def test_devin_worklog_no_dup_when_turn_closed_by_next_prompt(self) -> None:
        # A tool step's message becomes last_agent_text; if the turn is closed by the
        # NEXT prompt (not a text-only final step), that message is the final reply and
        # must NOT also appear in the worklog.
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "devin.json"
            path.write_text(json.dumps({"steps": [
                {"source": "user", "step_id": "s1", "timestamp": 1, "message": "Do step one."},
                {"source": "agent", "step_id": "s2", "timestamp": 2, "message": "Here is the summary of step one.",
                 "tool_calls": [{"type": "function", "function": {"name": "shell",
                                 "arguments": "{\"command\":\"ls\"}"}}]},
                {"source": "user", "step_id": "s3", "timestamp": 3, "message": "Now step two."},
            ]}), encoding="utf-8")
            turn = adapter.extract_devin_turn(path, "pane-1", "session-1")

        t1 = turn["recent_turns"][0]  # the turn closed by the next prompt
        self.assertEqual(t1["assistant_final_text"], "Here is the summary of step one.")
        wl = t1.get("worklog_text") or ""
        self.assertIn("shell ls", wl)
        self.assertNotIn("Here is the summary of step one.", wl)  # final reply not duplicated

    def test_codex_suppresses_new_internal_user_prefixes(self) -> None:
        self.assertTrue(adapter.is_internal_codex_user_text("<codex_internal_context foo>ignore"))
        self.assertTrue(adapter.is_internal_codex_user_text("  <codex_internal_context>ignore"))
        self.assertTrue(adapter.is_internal_codex_user_text("<subagent_notification>ignore</subagent_notification>"))
        self.assertFalse(adapter.is_internal_codex_user_text("<codex_user_text>keep"))
        self.assertFalse(adapter.is_internal_codex_user_text("normal prompt"))
        # must NOT over-match a real prompt that merely starts with the tag text
        self.assertFalse(adapter.is_internal_codex_user_text("<codex_internal_contextualize this>keep"))

    def test_devin_session_resolver_rejects_stale_cwd_match_for_new_pane(self) -> None:
        proc = Mock()
        proc.returncode = 0
        proc.stdout = json.dumps(
            [
                {
                    "id": "old-session",
                    "working_directory": "/tmp/project",
                    "last_activity_at": 1900,
                }
            ]
        )
        stat_result = Mock()
        stat_result.st_ctime = 2000

        with tempfile.TemporaryDirectory() as tmp, patch.object(
            adapter, "devin_bin", Mock(return_value="devin")
        ), patch.object(adapter.subprocess, "run", Mock(return_value=proc)), patch.object(
            adapter, "run_real_herdr_json",
            Mock(
                return_value={
                    "result": {
                        "process_info": {
                            "foreground_processes": [
                                {"name": "devin", "cmdline": "devin --model glm-5.2", "pid": 123}
                            ]
                        }
                    }
                }
            ),
        ), patch.object(adapter.os, "stat", Mock(return_value=stat_result)), patch.object(
            adapter, "devin_transcripts_dir", Mock(return_value=Path(tmp))
        ):
            (Path(tmp) / "old-session.json").write_text("{}", encoding="utf-8")
            self.assertEqual(
                adapter.devin_resolve_session_id({"pane_id": "pane-1", "cwd": "/tmp/project"}),
                "",
            )

    def test_devin_session_resolver_accepts_fresh_cwd_match(self) -> None:
        proc = Mock()
        proc.returncode = 0
        proc.stdout = json.dumps(
            [
                {
                    "id": "old-session",
                    "working_directory": "/tmp/project",
                    "last_activity_at": 1900,
                },
                {
                    "id": "fresh-session",
                    "working_directory": "/tmp/project",
                    "last_activity_at": 2010,
                },
            ]
        )
        stat_result = Mock()
        stat_result.st_ctime = 2000

        with tempfile.TemporaryDirectory() as tmp, patch.object(
            adapter, "devin_bin", Mock(return_value="devin")
        ), patch.object(adapter.subprocess, "run", Mock(return_value=proc)), patch.object(
            adapter, "run_real_herdr_json",
            Mock(
                return_value={
                    "result": {
                        "process_info": {
                            "foreground_processes": [
                                {"name": "devin", "cmdline": "devin --model glm-5.2", "pid": 123}
                            ]
                        }
                    }
                }
            ),
        ), patch.object(adapter.os, "stat", Mock(return_value=stat_result)), patch.object(
            adapter, "devin_transcripts_dir", Mock(return_value=Path(tmp))
        ):
            (Path(tmp) / "old-session.json").write_text("{}", encoding="utf-8")
            (Path(tmp) / "fresh-session.json").write_text("{}", encoding="utf-8")
            self.assertEqual(
                adapter.devin_resolve_session_id({"pane_id": "pane-1", "cwd": "/tmp/project"}),
                "fresh-session",
            )

    def test_devin_session_resolver_fails_closed_on_ambiguous_cwd(self) -> None:
        # The broadcast bug: multiple LIVE Devin sessions share one cwd (e.g. one GLM
        # seat per space, all rooted at /home/smith). Resolving the newest attributed
        # one pane's turn to every same-cwd pane -> broadcast to every topic. The
        # resolver must fail closed (return "") when the cwd is ambiguous.
        proc = Mock()
        proc.returncode = 0
        proc.stdout = json.dumps(
            [
                {"id": "session-a", "working_directory": "/tmp/project", "last_activity_at": 2010},
                {"id": "session-b", "working_directory": "/tmp/project", "last_activity_at": 2020},
            ]
        )
        stat_result = Mock()
        stat_result.st_ctime = 2000  # both sessions are fresher than the pane -> both valid

        with tempfile.TemporaryDirectory() as tmp, patch.object(
            adapter, "devin_bin", Mock(return_value="devin")
        ), patch.object(adapter.subprocess, "run", Mock(return_value=proc)), patch.object(
            adapter, "run_real_herdr_json",
            Mock(
                return_value={
                    "result": {
                        "process_info": {
                            "foreground_processes": [
                                {"name": "devin", "cmdline": "devin --model glm-5.2", "pid": 123}
                            ]
                        }
                    }
                }
            ),
        ), patch.object(adapter.os, "stat", Mock(return_value=stat_result)), patch.object(
            adapter, "devin_transcripts_dir", Mock(return_value=Path(tmp))
        ):
            (Path(tmp) / "session-a.json").write_text("{}", encoding="utf-8")
            (Path(tmp) / "session-b.json").write_text("{}", encoding="utf-8")
            self.assertEqual(
                adapter.devin_resolve_session_id({"pane_id": "pane-1", "cwd": "/tmp/project"}),
                "",
            )

    def test_codex_open_turn_is_incomplete(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "rollout-2026-06-15T00-00-00-session-1.jsonl"
            write_jsonl(
                path,
                [
                    {
                        "type": "event_msg",
                        "payload": {"type": "task_started", "turn_id": "turn-2", "started_at": 30},
                    },
                    {
                        "type": "response_item",
                        "payload": {
                            "type": "message",
                            "role": "user",
                            "content": [{"type": "input_text", "text": "Still running?"}],
                        },
                    },
                ],
            )

            turn = adapter.extract_codex_turn(path, "pane-1", "session-1")

        self.assertTrue(turn["available"])
        self.assertFalse(turn["complete"])
        self.assertEqual(turn["turn_id"], "turn-2")
        self.assertEqual(turn["user_text"], "Still running?")
        self.assertEqual(turn["assistant_final_text"], "")

    def test_codex_open_turn_exposes_stream_text_without_completing(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "rollout-2026-06-15T00-00-00-session-1.jsonl"
            write_jsonl(
                path,
                [
                    {"type": "event_msg", "payload": {"type": "task_started", "turn_id": "turn-2"}},
                    {
                        "type": "response_item",
                        "payload": {
                            "type": "message",
                            "role": "user",
                            "content": [{"type": "input_text", "text": "Still running?"}],
                        },
                    },
                    {
                        "type": "response_item",
                        "payload": {
                            "type": "message",
                            "role": "assistant",
                            "content": [{"type": "output_text", "text": "Partial answer in progress."}],
                        },
                    },
                ],
            )

            turn = adapter.extract_codex_turn(path, "pane-1", "session-1")

        self.assertFalse(turn["complete"])
        self.assertEqual(turn["assistant_final_text"], "")
        self.assertEqual(turn["assistant_stream_text"], "Partial answer in progress.")
        self.assertEqual(turn["stream_source"], "codex")
        self.assertEqual(turn["stream_revision"], adapter.stream_revision("Partial answer in progress."))

    def test_codex_open_turn_suppresses_subagent_notification_prompt(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "rollout-2026-06-15T00-00-00-session-1.jsonl"
            write_jsonl(
                path,
                [
                    {"type": "event_msg", "payload": {"type": "task_started", "turn_id": "turn-2"}},
                    {
                        "type": "response_item",
                        "payload": {
                            "type": "message",
                            "role": "user",
                            "content": [{"type": "input_text", "text": "<subagent_notification>{}</subagent_notification>"}],
                        },
                    },
                    {
                        "type": "response_item",
                        "payload": {
                            "type": "message",
                            "role": "assistant",
                            "content": [{"type": "output_text", "text": "Working on the repair."}],
                        },
                    },
                ],
            )

            turn = adapter.extract_codex_turn(path, "pane-1", "session-1")

        self.assertTrue(turn["available"])
        self.assertFalse(turn["complete"])
        self.assertEqual(turn["turn_id"], "turn-2")
        self.assertEqual(turn["user_text"], "")
        self.assertNotIn("open_user_text", turn)
        self.assertEqual(turn["assistant_stream_text"], "Working on the repair.")

    def test_codex_returns_latest_complete_when_newer_user_turn_is_open(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "rollout-2026-06-15T00-00-00-session-1.jsonl"
            write_jsonl(
                path,
                [
                    {"type": "event_msg", "payload": {"type": "task_started", "turn_id": "turn-1"}},
                    {
                        "type": "response_item",
                        "payload": {
                            "type": "message",
                            "role": "user",
                            "content": [{"type": "input_text", "text": "What finished?"}],
                        },
                    },
                    {
                        "type": "event_msg",
                        "payload": {
                            "type": "task_complete",
                            "turn_id": "turn-1",
                            "last_agent_message": "The completed result.",
                        },
                    },
                    {"type": "event_msg", "payload": {"type": "task_started", "turn_id": "turn-2"}},
                    {
                        "type": "response_item",
                        "payload": {
                            "type": "message",
                            "role": "user",
                            "content": [{"type": "input_text", "text": "This newer turn is still open."}],
                        },
                    },
                ],
            )

            turn = adapter.extract_codex_turn(path, "pane-1", "session-1")

        self.assertTrue(turn["available"])
        self.assertTrue(turn["complete"])
        self.assertEqual(turn["turn_id"], "turn-1")
        self.assertEqual(turn["user_text"], "What finished?")
        self.assertEqual(turn["assistant_final_text"], "The completed result.")
        self.assertTrue(turn["has_open_turn"])
        self.assertEqual(turn["open_turn_id"], "turn-2")
        self.assertEqual(turn["open_user_text"], "This newer turn is still open.")

    def test_claude_extracts_last_end_turn_assistant_message(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "session-1.jsonl"
            write_jsonl(
                path,
                [
                    {
                        "type": "user",
                        "uuid": "user-1",
                        "message": {"role": "user", "content": "Diagnose it."},
                    },
                    {
                        "type": "assistant",
                        "uuid": "tool-1",
                        "message": {
                            "role": "assistant",
                            "stop_reason": "tool_use",
                            "content": [{"type": "text", "text": "intermediate"}],
                        },
                    },
                    {
                        "type": "assistant",
                        "uuid": "assistant-1",
                        "timestamp": "2026-06-15T10:00:00Z",
                        "message": {
                            "role": "assistant",
                            "stop_reason": "end_turn",
                            "content": [{"type": "text", "text": "Final diagnosis."}],
                        },
                    },
                ],
            )

            turn = adapter.extract_claude_turn(path, "pane-1", "session-1")

        self.assertTrue(turn["available"])
        self.assertTrue(turn["complete"])
        self.assertEqual(turn["turn_id"], "assistant-1")
        self.assertEqual(turn["user_text"], "Diagnose it.")
        self.assertEqual(turn["assistant_final_text"], "Final diagnosis.")

    def _claude_turn_with_tools(self):
        return [
            {
                "type": "user",
                "uuid": "user-1",
                "message": {"role": "user", "content": "Fix the bug."},
            },
            {
                "type": "assistant",
                "uuid": "step-1",
                "message": {
                    "role": "assistant",
                    "stop_reason": "tool_use",
                    "content": [
                        {"type": "text", "text": "Let me run the tests."},
                        {"type": "tool_use", "name": "Bash", "input": {"command": "go test ./...\n(more)"}},
                    ],
                },
            },
            {  # tool_result comes back as an internal user event — must not break the turn
                "type": "user",
                "uuid": "tool-result-1",
                "message": {"role": "user", "content": [{"type": "tool_result", "content": "ok"}]},
            },
            {
                "type": "assistant",
                "uuid": "step-2",
                "message": {
                    "role": "assistant",
                    "stop_reason": "tool_use",
                    "content": [{"type": "tool_use", "name": "Edit", "input": {"file_path": "/root/gitmoot/engine.go"}}],
                },
            },
            {
                "type": "assistant",
                "uuid": "assistant-final",
                "timestamp": "2026-06-15T10:00:00Z",
                "message": {
                    "role": "assistant",
                    "stop_reason": "end_turn",
                    "content": [{"type": "text", "text": "Fixed and tests pass."}],
                },
            },
        ]

    def test_claude_worklog_captures_tool_use_and_interim_text(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "session-1.jsonl"
            write_jsonl(path, self._claude_turn_with_tools())
            turn = adapter.extract_claude_turn(path, "pane-1", "session-1")

        self.assertTrue(turn["complete"])
        self.assertEqual(turn["assistant_final_text"], "Fixed and tests pass.")
        worklog = turn.get("worklog_text") or ""
        # tool calls (with a short, single-line arg) + interim narration are preserved
        self.assertIn("Let me run the tests.", worklog)
        self.assertIn("Bash go test ./...", worklog)
        self.assertNotIn("(more)", worklog)  # multi-line tool args trimmed to first line
        self.assertIn("Edit engine.go", worklog)  # file paths shown as basename
        # the final response text is NOT duplicated into the worklog
        self.assertNotIn("Fixed and tests pass.", worklog)

    def test_claude_worklog_can_be_disabled(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "session-1.jsonl"
            write_jsonl(path, self._claude_turn_with_tools())
            with patch.object(adapter, "WORKLOG_ENABLED", False):
                turn = adapter.extract_claude_turn(path, "pane-1", "session-1")

        self.assertEqual(turn["assistant_final_text"], "Fixed and tests pass.")
        self.assertNotIn("worklog_text", turn)

    def test_claude_worklog_spans_coalesced_consecutive_end_turns(self) -> None:
        # Two end_turns under ONE prompt (no intervening user event) coalesce into a
        # single turn; the worklog must include the tool step that ran BETWEEN them.
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "session-1.jsonl"
            write_jsonl(
                path,
                [
                    {"type": "user", "uuid": "u1", "message": {"role": "user", "content": "Do it."}},
                    {"type": "assistant", "uuid": "s1", "message": {"role": "assistant", "stop_reason": "tool_use",
                        "content": [{"type": "tool_use", "name": "Bash", "input": {"command": "first-cmd"}}]}},
                    {"type": "assistant", "uuid": "e1", "message": {"role": "assistant", "stop_reason": "end_turn",
                        "content": [{"type": "text", "text": "Done one."}]}},
                    {"type": "assistant", "uuid": "s2", "message": {"role": "assistant", "stop_reason": "tool_use",
                        "content": [{"type": "tool_use", "name": "Bash", "input": {"command": "second-cmd"}}]}},
                    {"type": "assistant", "uuid": "e2", "message": {"role": "assistant", "stop_reason": "end_turn",
                        "content": [{"type": "text", "text": "Done one revised."}]}},
                ],
            )
            turn = adapter.extract_claude_turn(path, "pane-1", "session-1")

        self.assertEqual(turn["assistant_final_text"], "Done one revised.")
        self.assertEqual(len(turn["recent_turns"]), 1)  # coalesced into one turn
        worklog = turn.get("worklog_text") or ""
        self.assertIn("Bash first-cmd", worklog)
        self.assertIn("Bash second-cmd", worklog)  # the step between the two end_turns

    def test_claude_interrupt_finalizes_open_turn_worklog(self) -> None:
        # Issue #3: an interrupt ends the turn WITHOUT an end_turn. The accumulated worklog/stream
        # tail must be finalized (so it still lands on Telegram), and the "[Request interrupted…]"
        # marker must NOT become a visible prompt.
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "session-1.jsonl"
            write_jsonl(path, [
                {"type": "user", "uuid": "u1", "message": {"role": "user", "content": "Profile the CPU."}},
                {"type": "assistant", "uuid": "s1", "message": {"role": "assistant", "stop_reason": "tool_use",
                    "content": [{"type": "text", "text": "Running the profiler now."},
                                {"type": "tool_use", "name": "Bash", "input": {"command": "py-spy record"}}]}},
                {"type": "user", "uuid": "u2", "message": {"role": "user", "content": "[Request interrupted by user]"}},
            ])
            turn = adapter.extract_claude_turn(path, "pane-1", "session-1")

        self.assertTrue(turn["complete"])
        self.assertEqual(turn.get("complete_reason"), "interrupted")
        self.assertEqual(turn["user_text"], "Profile the CPU.")
        self.assertIn("Running the profiler now.", turn["assistant_final_text"])   # partial response preserved
        self.assertIn("Bash py-spy record", turn.get("worklog_text") or "")        # worklog tail preserved
        self.assertNotIn("Request interrupted", turn["user_text"])                 # marker is not a prompt
        self.assertNotIn("Request interrupted", turn.get("assistant_final_text") or "")

    def test_claude_interrupt_with_no_open_work_emits_no_turn(self) -> None:
        # A bare interrupt with no accumulated worklog/stream must not synthesize a spurious turn.
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "session-1.jsonl"
            write_jsonl(path, [
                {"type": "user", "uuid": "u1", "message": {"role": "user", "content": "Start."}},
                {"type": "user", "uuid": "u2", "message": {"role": "user", "content": "[Request interrupted by user]"}},
            ])
            turn = adapter.extract_claude_turn(path, "pane-1", "session-1")

        self.assertIsNot(turn.get("complete"), True)            # nothing finalized
        self.assertEqual(turn.get("reason"), "no_completed_turn")
        self.assertNotIn("Request interrupted", str(turn.get("user_text") or ""))

    def test_claude_worklog_redacts_secrets(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "session-1.jsonl"
            write_jsonl(
                path,
                [
                    {"type": "user", "uuid": "u1", "message": {"role": "user", "content": "Deploy."}},
                    {"type": "assistant", "uuid": "s1", "message": {"role": "assistant", "stop_reason": "tool_use",
                        "content": [
                            {"type": "tool_use", "name": "Bash", "input": {"command": "export TOKEN=ghp_supersecretvalue && go build"}},
                            {"type": "tool_use", "name": "WebFetch", "input": {"url": "https://user:pw@example.com/x"}},
                        ]}},
                    {"type": "assistant", "uuid": "e1", "message": {"role": "assistant", "stop_reason": "end_turn",
                        "content": [{"type": "text", "text": "Deployed."}]}},
                ],
            )
            turn = adapter.extract_claude_turn(path, "pane-1", "session-1")

        worklog = turn.get("worklog_text") or ""
        self.assertNotIn("ghp_supersecretvalue", worklog)  # secret value masked
        self.assertIn("***", worklog)
        self.assertNotIn("user:pw@", worklog)  # url credentials masked
        self.assertIn("go build", worklog)  # non-secret part of the command preserved

    def test_claude_worklog_strips_control_chars(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "session-1.jsonl"
            write_jsonl(
                path,
                [
                    {"type": "user", "uuid": "u1", "message": {"role": "user", "content": "Go."}},
                    {"type": "assistant", "uuid": "s1", "message": {"role": "assistant", "stop_reason": "tool_use",
                        "content": [{"type": "text", "text": "step‮RTL​zw done"}]}},
                    {"type": "assistant", "uuid": "e1", "message": {"role": "assistant", "stop_reason": "end_turn",
                        "content": [{"type": "text", "text": "ok"}]}},
                ],
            )
            turn = adapter.extract_claude_turn(path, "pane-1", "session-1")

        worklog = turn.get("worklog_text") or ""
        self.assertNotIn("‮", worklog)  # bidi override stripped
        self.assertNotIn("​", worklog)  # zero-width space stripped
        self.assertIn("step", worklog)

    def test_claude_suppresses_internal_task_notification_user_text(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "session-1.jsonl"
            write_jsonl(
                path,
                [
                    {
                        "type": "user",
                        "uuid": "user-1",
                        "message": {
                            "role": "user",
                            "content": "<task-notification><task-id>abc</task-id></task-notification>",
                        },
                    },
                    {
                        "type": "assistant",
                        "uuid": "assistant-1",
                        "timestamp": "2026-06-15T10:00:00Z",
                        "message": {
                            "role": "assistant",
                            "stop_reason": "end_turn",
                            "content": [{"type": "text", "text": "Final diagnosis."}],
                        },
                    },
                ],
            )

            turn = adapter.extract_claude_turn(path, "pane-1", "session-1")

        self.assertTrue(turn["complete"])
        self.assertEqual(turn["user_text"], "")
        self.assertEqual(turn["assistant_final_text"], "Final diagnosis.")

    def test_claude_returns_latest_complete_when_newer_user_turn_is_open(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "session-1.jsonl"
            write_jsonl(
                path,
                [
                    {
                        "type": "user",
                        "uuid": "user-1",
                        "message": {"role": "user", "content": "What finished?"},
                    },
                    {
                        "type": "assistant",
                        "uuid": "assistant-1",
                        "timestamp": "2026-06-15T10:00:00Z",
                        "message": {
                            "role": "assistant",
                            "stop_reason": "end_turn",
                            "content": [{"type": "text", "text": "The completed result."}],
                        },
                    },
                    {
                        "type": "user",
                        "uuid": "user-2",
                        "message": {"role": "user", "content": "This newer turn is still open."},
                    },
                ],
            )

            turn = adapter.extract_claude_turn(path, "pane-1", "session-1")

        self.assertTrue(turn["available"])
        self.assertTrue(turn["complete"])
        self.assertEqual(turn["turn_id"], "assistant-1")
        self.assertEqual(turn["user_text"], "What finished?")
        self.assertEqual(turn["assistant_final_text"], "The completed result.")
        self.assertTrue(turn["has_open_turn"])
        self.assertEqual(turn["open_turn_id"], "user-2")
        self.assertEqual(turn["open_user_text"], "This newer turn is still open.")

    def test_claude_open_turn_exposes_stream_text_without_completing(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "session-1.jsonl"
            write_jsonl(
                path,
                [
                    {"type": "user", "uuid": "user-1", "message": {"role": "user", "content": "Keep working?"}},
                    {
                        "type": "assistant",
                        "uuid": "assistant-1",
                        "timestamp": "2026-06-15T10:00:00Z",
                        "message": {
                            "role": "assistant",
                            "stop_reason": "tool_use",
                            "content": [{"type": "text", "text": "Partial Claude answer."}],
                        },
                    },
                ],
            )

            turn = adapter.extract_claude_turn(path, "pane-1", "session-1")

        self.assertFalse(turn["complete"])
        self.assertEqual(turn["assistant_final_text"], "")
        self.assertEqual(turn["assistant_stream_text"], "Partial Claude answer.")
        self.assertEqual(turn["stream_source"], "claude")
        self.assertEqual(turn["stream_revision"], adapter.stream_revision("Partial Claude answer."))

    def test_stream_text_ignores_api_error_and_internal_messages(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "session-1.jsonl"
            write_jsonl(
                path,
                [
                    {"type": "user", "uuid": "user-1", "message": {"role": "user", "content": "Recover?"}},
                    {
                        "type": "assistant",
                        "uuid": "api-error-1",
                        "isApiErrorMessage": True,
                        "error": "overloaded",
                        "message": {
                            "role": "assistant",
                            "content": [{"type": "text", "text": "API Error: overloaded"}],
                        },
                    },
                    {
                        "type": "user",
                        "uuid": "internal-1",
                        "message": {
                            "role": "user",
                            "content": "<task-notification><task-id>abc</task-id></task-notification>",
                        },
                    },
                ],
            )

            turn = adapter.extract_claude_turn(path, "pane-1", "session-1")

        self.assertFalse(turn["complete"])
        self.assertEqual(turn["assistant_final_text"], "")
        self.assertNotIn("assistant_stream_text", turn)
        self.assertIn("api_error", turn)
        self.assertNotIn("task-notification", turn["user_text"])

    def test_claude_no_session_id_can_match_unique_visible_session(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            project = root / "-tmp-project"
            project.mkdir()
            matched = project / "session-match.jsonl"
            other = project / "session-other.jsonl"
            final = (
                "Yes, done. The updater is working for Codex panes, and Claude panes were skipped "
                "because Herdr did not expose a session id. The fix is to match a unique visible session."
            )
            write_jsonl(
                matched,
                [
                    {"type": "user", "uuid": "u1", "message": {"content": "Check updater."}},
                    {
                        "type": "assistant",
                        "uuid": "a1",
                        "timestamp": "now",
                        "message": {"stop_reason": "end_turn", "content": [{"text": final}]},
                    },
                ],
            )
            write_jsonl(
                other,
                [
                    {"type": "user", "uuid": "u2", "message": {"content": "Other task."}},
                    {
                        "type": "assistant",
                        "uuid": "a2",
                        "timestamp": "now",
                        "message": {"stop_reason": "end_turn", "content": [{"text": "Different final answer."}]},
                    },
                ],
            )
            pane = {
                "pane_id": "pane-1",
                "agent": "claude",
                "cwd": "/tmp/project",
                "foreground_cwd": "/tmp/project",
                "agent_session": None,
            }
            with patch.object(adapter, "pane_from_list", Mock(return_value=pane)), patch.dict(
                adapter.os.environ,
                {"CLAUDE_PROJECTS_DIR": str(root)},
            ), patch.object(
                adapter,
                "pane_recent_text",
                Mock(return_value="prefix " + final + " suffix"),
            ), patch.object(adapter, "claude_sibling_count", Mock(return_value=2)), patch.object(
                adapter, "claude_pid_for_pane", Mock(return_value=None)
            ):
                result = adapter.pane_turn("pane-1")

        turn = result["result"]["turn"]
        self.assertTrue(turn["available"])
        self.assertTrue(turn["complete"])
        self.assertEqual(turn["agent_session_id"], "session-match")
        self.assertEqual(turn["session_match_source"], "pane_visible_match")
        self.assertEqual(turn["assistant_final_text"], final)

    def test_claude_visible_session_fallback_fails_closed_when_ambiguous(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            project = root / "-tmp-project"
            project.mkdir()
            final = (
                "The same visible final answer appears in two candidate sessions, so the adapter "
                "must not guess which one belongs to the pane."
            )
            for name in ("session-a.jsonl", "session-b.jsonl"):
                write_jsonl(
                    project / name,
                    [
                        {"type": "user", "uuid": "u", "message": {"content": "Check updater."}},
                        {
                            "type": "assistant",
                            "uuid": name,
                            "timestamp": "now",
                            "message": {"stop_reason": "end_turn", "content": [{"text": final}]},
                        },
                    ],
                )
            pane = {
                "pane_id": "pane-1",
                "agent": "claude",
                "cwd": "/tmp/project",
                "foreground_cwd": "/tmp/project",
                "agent_session": None,
            }
            with patch.object(adapter, "pane_from_list", Mock(return_value=pane)), patch.dict(
                adapter.os.environ,
                {"CLAUDE_PROJECTS_DIR": str(root)},
            ), patch.object(
                adapter,
                "pane_recent_text",
                Mock(return_value=final),
            ), patch.object(adapter, "claude_sibling_count", Mock(return_value=2)), patch.object(
                adapter, "claude_pid_for_pane", Mock(return_value=None)
            ):
                result = adapter.pane_turn("pane-1")

        turn = result["result"]["turn"]
        self.assertFalse(turn["available"])
        self.assertEqual(turn["reason"], "ambiguous_claude_session_match")

    def test_claude_exclusive_cwd_uses_newest_session_without_visible_match(self) -> None:
        # When this pane is the only claude session in its cwd, use the newest
        # session file directly even if the (tool-heavy) visible pane doesn't match.
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            project = root / "-tmp-project"
            project.mkdir()
            final = "Deployed and verified; all tests green."
            old = project / "session-old.jsonl"
            new = project / "session-new.jsonl"
            for path, txt, uu in ((old, "Old answer.", "ao"), (new, final, "an")):
                write_jsonl(path, [
                    {"type": "user", "uuid": "u" + uu, "message": {"content": "go"}},
                    {"type": "assistant", "uuid": uu, "timestamp": "now",
                     "message": {"stop_reason": "end_turn", "content": [{"text": txt}]}},
                ])
            adapter.os.utime(old, (1, 1))  # make 'new' clearly the most recently written
            pane = {"pane_id": "pane-1", "agent": "claude", "cwd": "/tmp/project",
                    "foreground_cwd": "/tmp/project", "agent_session": None}
            with patch.object(adapter, "pane_from_list", Mock(return_value=pane)), patch.dict(
                adapter.os.environ, {"CLAUDE_PROJECTS_DIR": str(root)},
            ), patch.object(adapter, "claude_sibling_count", Mock(return_value=1)), patch.object(
                adapter, "pane_recent_text", Mock(return_value="unrelated tool output noise"),
            ), patch.object(adapter, "claude_pid_for_pane", Mock(return_value=None)):
                result = adapter.pane_turn("pane-1")

        turn = result["result"]["turn"]
        self.assertTrue(turn["available"])
        self.assertEqual(turn["agent_session_id"], "session-new")
        self.assertEqual(turn["session_match_source"], "exclusive_cwd_mtime")
        self.assertEqual(turn["assistant_final_text"], final)

    # --- deterministic pane -> PID -> sessionId resolution -------------------

    def _claude_session_file(self, project: Path, sid: str, user: str, asst: str) -> None:
        write_jsonl(project / f"{sid}.jsonl", [
            {"type": "user", "uuid": "u-" + sid, "message": {"content": user}},
            {"type": "assistant", "uuid": "a-" + sid, "timestamp": "now",
             "message": {"stop_reason": "end_turn", "content": [{"text": asst}]}},
        ])

    def _pid_map_file(self, sessions: Path, pid: int, sid: str, cwd: str, proc_start: str) -> None:
        (sessions / f"{pid}.json").write_text(
            json.dumps({"pid": pid, "sessionId": sid, "cwd": cwd, "procStart": proc_start, "status": "idle"}),
            encoding="utf-8",
        )

    def test_claude_resolves_shared_cwd_via_pid_map(self) -> None:
        # Two claude panes in ONE cwd resolve to DISTINCT sessions via Claude's
        # own pid->sessionId map (the live X Issues / Telemetry X case, where the
        # visible-text matcher had guessed the OPPOSITE binding).
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            project = root / "-tmp-proj"
            project.mkdir()
            sessions = root / "sessions"
            sessions.mkdir()
            self._claude_session_file(project, "sidA", "prompt A", "answer A")
            self._claude_session_file(project, "sidB", "prompt B", "answer B")
            self._pid_map_file(sessions, 101, "sidA", "/tmp/proj", "111")
            self._pid_map_file(sessions, 102, "sidB", "/tmp/proj", "222")
            pids = {"paneA": 101, "paneB": 102}
            starts = {101: "111", 102: "222"}

            def pane_of(name: str) -> dict:
                return {"pane_id": name, "agent": "claude", "cwd": "/tmp/proj",
                        "foreground_cwd": "/tmp/proj", "agent_session": None}

            with patch.dict(adapter.os.environ, {"CLAUDE_PROJECTS_DIR": str(root), "CLAUDE_SESSIONS_DIR": str(sessions)}), \
                 patch.object(adapter, "claude_pid_for_pane", Mock(side_effect=lambda pane_id: pids[pane_id])), \
                 patch.object(adapter, "proc_starttime", Mock(side_effect=lambda pid: starts.get(pid))):
                for name, sid, asst in (("paneA", "sidA", "answer A"), ("paneB", "sidB", "answer B")):
                    with patch.object(adapter, "pane_from_list", Mock(return_value=pane_of(name))):
                        turn = adapter.pane_turn(name)["result"]["turn"]
                    self.assertEqual(turn["session_match_source"], "pid_session_map")
                    self.assertEqual(turn["agent_session_id"], sid)
                    self.assertEqual(turn["assistant_final_text"], asst)

    def test_claude_pid_reuse_guard_falls_through(self) -> None:
        # A recycled PID's stale map file (procStart mismatch) is rejected and we
        # fall through to the existing heuristics rather than serving a wrong session.
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            project = root / "-tmp-proj"
            project.mkdir()
            sessions = root / "sessions"
            sessions.mkdir()
            self._claude_session_file(project, "sidA", "prompt A", "answer A")
            self._pid_map_file(sessions, 101, "sidA", "/tmp/proj", "111")
            pane = {"pane_id": "p", "agent": "claude", "cwd": "/tmp/proj",
                    "foreground_cwd": "/tmp/proj", "agent_session": None}
            with patch.dict(adapter.os.environ, {"CLAUDE_PROJECTS_DIR": str(root), "CLAUDE_SESSIONS_DIR": str(sessions)}), \
                 patch.object(adapter, "pane_from_list", Mock(return_value=pane)), \
                 patch.object(adapter, "claude_pid_for_pane", Mock(return_value=101)), \
                 patch.object(adapter, "proc_starttime", Mock(return_value="999")), \
                 patch.object(adapter, "claude_sibling_count", Mock(return_value=1)), \
                 patch.object(adapter, "pane_recent_text", Mock(return_value="noise")):
                turn = adapter.pane_turn("p")["result"]["turn"]
            self.assertNotEqual(turn.get("session_match_source"), "pid_session_map")
            self.assertEqual(turn["session_match_source"], "exclusive_cwd_mtime")
            self.assertEqual(turn["assistant_final_text"], "answer A")

    def test_claude_missing_pid_map_falls_through(self) -> None:
        # process-info gives a PID but no <pid>.json exists -> fall through.
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            project = root / "-tmp-proj"
            project.mkdir()
            sessions = root / "sessions"
            sessions.mkdir()
            self._claude_session_file(project, "sidA", "prompt A", "answer A")
            pane = {"pane_id": "p", "agent": "claude", "cwd": "/tmp/proj",
                    "foreground_cwd": "/tmp/proj", "agent_session": None}
            with patch.dict(adapter.os.environ, {"CLAUDE_PROJECTS_DIR": str(root), "CLAUDE_SESSIONS_DIR": str(sessions)}), \
                 patch.object(adapter, "pane_from_list", Mock(return_value=pane)), \
                 patch.object(adapter, "claude_pid_for_pane", Mock(return_value=101)), \
                 patch.object(adapter, "claude_sibling_count", Mock(return_value=1)), \
                 patch.object(adapter, "pane_recent_text", Mock(return_value="noise")):
                turn = adapter.pane_turn("p")["result"]["turn"]
            self.assertEqual(turn["session_match_source"], "exclusive_cwd_mtime")

    def test_claude_stale_herdr_value_loses_to_pid_map(self) -> None:
        # THE X-topic regression: Herdr reports a NON-EMPTY but DEAD session id
        # (a /resume after a 529 left it stale); the live pid map points at the
        # active session. The pid map must win, and the stale id is surfaced.
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            project = root / "-tmp-proj"
            project.mkdir()
            sessions = root / "sessions"
            sessions.mkdir()
            self._claude_session_file(project, "sidStale", "old", "Dead: what would you like to work on?")
            self._claude_session_file(project, "sidActive", "do the work", "Done: the real final reply.")
            self._pid_map_file(sessions, 101, "sidActive", "/tmp/proj", "111")
            pane = {"pane_id": "p", "agent": "claude", "cwd": "/tmp/proj", "foreground_cwd": "/tmp/proj",
                    "agent_session": {"agent": "claude", "kind": "id", "source": "herdr:claude", "value": "sidStale"}}
            with patch.dict(adapter.os.environ, {"CLAUDE_PROJECTS_DIR": str(root), "CLAUDE_SESSIONS_DIR": str(sessions)}), \
                 patch.object(adapter, "pane_from_list", Mock(return_value=pane)), \
                 patch.object(adapter, "claude_pid_for_pane", Mock(return_value=101)), \
                 patch.object(adapter, "proc_starttime", Mock(return_value="111")):
                turn = adapter.pane_turn("p")["result"]["turn"]
            self.assertEqual(turn["session_match_source"], "pid_session_map")
            self.assertEqual(turn["agent_session_id"], "sidActive")
            self.assertEqual(turn["assistant_final_text"], "Done: the real final reply.")
            self.assertEqual(turn["herdr_session_id"], "sidStale")

    def test_claude_herdr_value_used_when_pid_map_missing(self) -> None:
        # PID hop broken (no foreground claude found) but Herdr's id is present
        # and its file exists -> use it (ranks below pid map, above heuristics).
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            project = root / "-tmp-proj"
            project.mkdir()
            sessions = root / "sessions"
            sessions.mkdir()
            self._claude_session_file(project, "sidH", "prompt", "herdr fallback answer")
            pane = {"pane_id": "p", "agent": "claude", "cwd": "/tmp/proj", "foreground_cwd": "/tmp/proj",
                    "agent_session": {"value": "sidH"}}
            with patch.dict(adapter.os.environ, {"CLAUDE_PROJECTS_DIR": str(root), "CLAUDE_SESSIONS_DIR": str(sessions)}), \
                 patch.object(adapter, "pane_from_list", Mock(return_value=pane)), \
                 patch.object(adapter, "claude_pid_for_pane", Mock(return_value=None)):
                turn = adapter.pane_turn("p")["result"]["turn"]
            self.assertEqual(turn["session_match_source"], "herdr_agent_session")
            self.assertEqual(turn["agent_session_id"], "sidH")
            self.assertEqual(turn["assistant_final_text"], "herdr fallback answer")

    def test_claude_pid_map_self_guard_rejects_mismatched_pid_field(self) -> None:
        # A map file whose own "pid" field disagrees with the queried PID is
        # corrupt/misnamed and must be rejected (falls through to heuristics).
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            project = root / "-tmp-proj"
            project.mkdir()
            sessions = root / "sessions"
            sessions.mkdir()
            self._claude_session_file(project, "sidA", "prompt A", "answer A")
            self._pid_map_file(sessions, 999, "sidA", "/tmp/proj", "111")  # file says pid 999
            (sessions / "101.json").write_text((sessions / "999.json").read_text(), encoding="utf-8")  # misnamed copy
            pane = {"pane_id": "p", "agent": "claude", "cwd": "/tmp/proj",
                    "foreground_cwd": "/tmp/proj", "agent_session": None}
            with patch.dict(adapter.os.environ, {"CLAUDE_PROJECTS_DIR": str(root), "CLAUDE_SESSIONS_DIR": str(sessions)}), \
                 patch.object(adapter, "pane_from_list", Mock(return_value=pane)), \
                 patch.object(adapter, "claude_pid_for_pane", Mock(return_value=101)), \
                 patch.object(adapter, "proc_starttime", Mock(return_value="111")), \
                 patch.object(adapter, "claude_sibling_count", Mock(return_value=1)), \
                 patch.object(adapter, "pane_recent_text", Mock(return_value="noise")):
                turn = adapter.pane_turn("p")["result"]["turn"]
            self.assertEqual(turn["session_match_source"], "exclusive_cwd_mtime")

    def test_claude_pid_for_pane_ancestor_walk_finds_claude_parent(self) -> None:
        # Foreground leaf is a Bash tool child; walk up to its claude parent.
        info = {"result": {"process_info": {
            "foreground_processes": [{"name": "bash", "pid": 200}],
            "foreground_process_group_id": 100}}}
        with patch.object(adapter, "run_real_herdr_json", Mock(return_value=info)), \
             patch.object(adapter, "proc_ppid", Mock(side_effect=lambda p: {200: 100, 100: 1}.get(p))), \
             patch.object(adapter, "proc_is_claude", Mock(side_effect=lambda p: p == 100)):
            self.assertEqual(adapter.claude_pid_for_pane("pane"), 100)

    def test_claude_pid_for_pane_skips_malformed_entry_without_aborting(self) -> None:
        # A foreground entry missing its pid must not abort resolution; the later
        # valid claude entry (and otherwise the pgid fallback) must still win.
        info = {"result": {"process_info": {
            "foreground_processes": [{"name": "bash"}, {"name": "claude", "pid": 101}],
            "foreground_process_group_id": 99}}}
        with patch.object(adapter, "run_real_herdr_json", Mock(return_value=info)):
            self.assertEqual(adapter.claude_pid_for_pane("pane"), 101)

    def test_claude_pid_for_pane_falls_back_to_pgid(self) -> None:
        # No claude-named foreground proc and no claude ancestor -> pgid leader.
        info = {"result": {"process_info": {
            "foreground_processes": [{"name": "bash", "pid": 200}],
            "foreground_process_group_id": 100}}}
        with patch.object(adapter, "run_real_herdr_json", Mock(return_value=info)), \
             patch.object(adapter, "proc_ppid", Mock(return_value=1)), \
             patch.object(adapter, "proc_is_claude", Mock(return_value=False)):
            self.assertEqual(adapter.claude_pid_for_pane("pane"), 100)

    def test_extract_claude_turn_preserves_real_prompt_through_trailing_internal_user(self) -> None:
        # Live X Issues case: a real prompt is answered, then the agent
        # auto-continues on an internal <task-notification>. The real prompt must
        # pair to ITS reply, and the internal-triggered reply must have NO prompt.
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "s.jsonl"
            write_jsonl(path, [
                {"type": "user", "uuid": "u-real", "message": {"content": "you did it again"}},
                {"type": "assistant", "uuid": "a1", "timestamp": "t1",
                 "message": {"stop_reason": "end_turn", "content": [{"text": "reply one"}]}},
                {"type": "user", "uuid": "u-int",
                 "message": {"content": "<task-notification><task-id>x</task-id></task-notification>"}},
                {"type": "user", "uuid": "u-empty", "message": {"content": ""}},
                {"type": "assistant", "uuid": "a2", "timestamp": "t2",
                 "message": {"stop_reason": "end_turn", "content": [{"text": "reply two"}]}},
            ])
            turn = adapter.extract_claude_turn(path, "pane-1", "session-1")
        self.assertEqual(turn["turn_id"], "a2")
        self.assertEqual(turn["user_text"], "")  # a2 triggered by internal notification
        self.assertEqual(turn["assistant_final_text"], "reply two")
        recent = {t["turn_id"]: t for t in turn["recent_turns"]}
        self.assertEqual(recent["a1"]["user_text"], "you did it again")  # NOT lost, NOT misattributed
        self.assertEqual(recent["a1"]["assistant_final_text"], "reply one")
        self.assertEqual(recent["a2"]["user_text"], "")

    def test_extract_claude_turn_coalesces_consecutive_end_turns(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "s.jsonl"
            write_jsonl(path, [
                {"type": "user", "uuid": "u1", "message": {"content": "go"}},
                {"type": "assistant", "uuid": "aEmpty", "timestamp": "t1",
                 "message": {"stop_reason": "end_turn", "content": [{"text": ""}]}},
                {"type": "assistant", "uuid": "aFinal", "timestamp": "t2",
                 "message": {"stop_reason": "end_turn", "content": [{"text": "final text"}]}},
            ])
            turn = adapter.extract_claude_turn(path, "pane-1", "session-1")
        self.assertEqual(turn["turn_id"], "aFinal")
        self.assertEqual(turn["user_text"], "go")
        self.assertEqual(turn["assistant_final_text"], "final text")
        self.assertEqual(len(turn["recent_turns"]), 1)

    def test_extract_claude_turn_exposes_recent_turns_for_catch_up(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "s.jsonl"
            rows = []
            for i in (1, 2, 3):
                rows.append({"type": "user", "uuid": f"u{i}", "message": {"content": f"prompt {i}"}})
                rows.append({"type": "assistant", "uuid": f"a{i}", "timestamp": f"t{i}",
                             "message": {"stop_reason": "end_turn", "content": [{"text": f"answer {i}"}]}})
            write_jsonl(path, rows)
            turn = adapter.extract_claude_turn(path, "pane-1", "session-1")
        ids = [t["turn_id"] for t in turn["recent_turns"]]
        self.assertEqual(ids, ["a1", "a2", "a3"])  # oldest -> newest
        self.assertEqual(turn["turn_id"], "a3")

    def test_claude_detects_unresolved_api_error(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "s.jsonl"
            write_jsonl(path, [
                {"type": "user", "uuid": "u1", "message": {"content": "do x"}},
                {"type": "assistant", "uuid": "a1", "timestamp": "t1",
                 "message": {"stop_reason": "end_turn", "content": [{"text": "first reply"}]}},
                {"type": "user", "uuid": "u2", "message": {"content": "do y"}},
                {"type": "assistant", "uuid": "err1", "isApiErrorMessage": True, "error": "server_error",
                 "timestamp": "t2", "message": {"role": "assistant", "content": [{"type": "text", "text": "API Error: overloaded"}]}},
            ])
            turn = adapter.extract_claude_turn(path, "pane-1", "sid")
        self.assertIn("api_error", turn)
        self.assertEqual(turn["api_error"]["id"], "err1")
        self.assertEqual(turn["api_error"]["code"], "server_error")
        self.assertIn("overloaded", turn["api_error"]["text"])
        self.assertEqual(turn["turn_id"], "a1")  # error is NOT treated as a completed turn

    def test_claude_api_error_cleared_on_recovery(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "s.jsonl"
            write_jsonl(path, [
                {"type": "user", "uuid": "u1", "message": {"content": "do x"}},
                {"type": "assistant", "uuid": "err1", "isApiErrorMessage": True, "error": "server_error",
                 "timestamp": "t1", "message": {"content": [{"type": "text", "text": "API Error"}]}},
                {"type": "assistant", "uuid": "a2", "timestamp": "t2",
                 "message": {"stop_reason": "end_turn", "content": [{"text": "recovered reply"}]}},
            ])
            turn = adapter.extract_claude_turn(path, "pane-1", "sid")
        self.assertNotIn("api_error", turn)  # a real completion after the error = recovered
        self.assertEqual(turn["turn_id"], "a2")

    def test_claude_api_error_cleared_when_user_reprompts(self) -> None:
        # owner already responded to the error (new real prompt, no completion yet)
        # -> don't keep reporting the stale error.
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "s.jsonl"
            write_jsonl(path, [
                {"type": "user", "uuid": "u1", "message": {"content": "do x"}},
                {"type": "assistant", "uuid": "err1", "isApiErrorMessage": True, "error": "server_error",
                 "timestamp": "t1", "message": {"content": [{"type": "text", "text": "API Error"}]}},
                {"type": "user", "uuid": "u2", "message": {"content": "please continue"}},
            ])
            turn = adapter.extract_claude_turn(path, "pane-1", "sid")
        self.assertNotIn("api_error", turn)  # superseded by the user's new prompt

    def test_codex_pane_turn_requires_agent_session_id(self) -> None:
        with patch.object(
            adapter,
            "pane_from_list",
            Mock(return_value={"pane_id": "pane-1", "agent": "codex", "agent_session": None}),
        ):
            result = adapter.pane_turn("pane-1")

        turn = result["result"]["turn"]
        self.assertFalse(turn["available"])
        self.assertEqual(turn["reason"], "no_agent_session_id")

    def test_pane_turn_reports_herdr_list_failure(self) -> None:
        with patch.object(adapter, "pane_from_list", Mock(return_value={"_adapter_error": "herdr_list_failed"})):
            result = adapter.pane_turn("pane-1")

        turn = result["result"]["turn"]
        self.assertFalse(turn["available"])
        self.assertEqual(turn["reason"], "herdr_list_failed")

    def test_non_turn_commands_delegate_to_real_herdr(self) -> None:
        exec_real = Mock()

        with patch.object(adapter.sys, "argv", ["herdr_turn_adapter.py", "pane", "list"]), patch.object(
            adapter,
            "exec_real_herdr",
            exec_real,
        ):
            result = adapter.main()

        exec_real.assert_called_once_with()
        self.assertEqual(result, 127)


def _codex_turns(n: int, prefix: str = "t", user_pad: str = "") -> list[dict]:
    rows: list[dict] = []
    for i in range(n):
        rows.append({"type": "event_msg", "payload": {"type": "task_started", "turn_id": f"{prefix}{i}", "started_at": i}})
        rows.append({"type": "response_item", "payload": {"type": "message", "role": "user",
                     "content": [{"type": "input_text", "text": f"prompt {prefix}{i} {user_pad}"}]}})
        rows.append({"type": "response_item", "payload": {"type": "function_call", "name": "exec_command",
                     "arguments": "{\"cmd\":\"echo hi\"}"}})
        rows.append({"type": "event_msg", "payload": {"type": "task_complete", "turn_id": f"{prefix}{i}",
                     "completed_at": i, "last_agent_message": f"answer {prefix}{i}"}})
    return rows


def _claude_turns(n: int, prefix: str = "u") -> list[dict]:
    rows: list[dict] = []
    for i in range(n):
        rows.append({"type": "user", "uuid": f"{prefix}{i}", "message": {"role": "user", "content": f"prompt {i}"}})
        rows.append({"type": "assistant", "uuid": f"a{prefix}{i}", "timestamp": f"ts{i}",
                     "message": {"role": "assistant", "stop_reason": "tool_use", "content": [{"type": "text", "text": "step"}]}})
        rows.append({"type": "assistant", "uuid": f"f{prefix}{i}", "timestamp": f"te{i}",
                     "message": {"role": "assistant", "stop_reason": "end_turn", "content": [{"type": "text", "text": f"answer {i}"}]}})
    return rows


class TailReadTests(unittest.TestCase):
    """Tail-reading a huge transcript must yield the SAME recent turns as a full read,
    while reading only a bounded tail (a 1+ GB rollout must not be fully parsed each sync)."""

    def test_codex_tail_read_matches_full_read(self) -> None:
        rows = _codex_turns(adapter.RECENT_TURNS + 5)
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "rollout-2026-06-15T00-00-00-s.jsonl"
            write_jsonl(path, rows)
            with patch.object(adapter, "TURN_TAIL_BYTES", 10**9):  # whole-file path
                full = adapter.extract_codex_turn(path, "p", "s")
            with patch.object(adapter, "TURN_TAIL_BYTES", 200):    # tail path
                tail = adapter.extract_codex_turn(path, "p", "s")
        self.assertEqual(tail["turn_id"], full["turn_id"])
        self.assertEqual(tail["user_text"], full["user_text"])
        self.assertEqual(tail["assistant_final_text"], full["assistant_final_text"])
        self.assertEqual([t["turn_id"] for t in tail["recent_turns"]],
                         [t["turn_id"] for t in full["recent_turns"]])
        self.assertEqual(len(full["recent_turns"]), adapter.RECENT_TURNS)

    def test_claude_tail_read_matches_full_read_with_internal_user(self) -> None:
        rows = _claude_turns(adapter.RECENT_TURNS + 5)
        # an internal user event mid-stream (turn boundary that must not change output)
        rows.insert(6, {"type": "user", "uuid": "internal-1",
                        "message": {"role": "user", "content": "<task-notification>ignore</task-notification>"}})
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "s.jsonl"
            write_jsonl(path, rows)
            with patch.object(adapter, "TURN_TAIL_BYTES", 10**9):
                full = adapter.extract_claude_turn(path, "p", "s")
            with patch.object(adapter, "TURN_TAIL_BYTES", 200):
                tail = adapter.extract_claude_turn(path, "p", "s")
        self.assertEqual(tail["turn_id"], full["turn_id"])
        self.assertEqual(tail["assistant_final_text"], full["assistant_final_text"])
        self.assertEqual([t["turn_id"] for t in tail["recent_turns"]],
                         [t["turn_id"] for t in full["recent_turns"]])
        self.assertEqual(len(full["recent_turns"]), adapter.RECENT_TURNS)

    def test_tail_read_is_bounded_excludes_old_turns(self) -> None:
        # One huge old turn (far bigger than the window) followed by RECENT_TURNS+1 small
        # turns. The tail reader must start at a recent boundary and NOT include the old turn.
        rows = _codex_turns(1, prefix="old", user_pad="X" * 200_000)
        rows += _codex_turns(adapter.RECENT_TURNS + 1, prefix="new")
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "rollout-2026-06-15T00-00-00-s.jsonl"
            write_jsonl(path, rows)
            size = path.stat().st_size
            lines = adapter._jsonl_tail_lines(
                path, adapter._codex_is_turn_start, adapter._codex_is_turn_end, 256, adapter.RECENT_TURNS)
        self.assertTrue(lines)
        joined = "\n".join(lines)
        self.assertNotIn("old0", joined)          # the huge old turn was not read
        self.assertIn('"new0"', joined)            # the recent turns were
        self.assertTrue(adapter._codex_is_turn_start(lines[0]))  # starts at a clean boundary
        self.assertLess(sum(len(l) for l in lines), size // 2)   # read far less than the whole file

    def test_codex_window_grows_past_trailing_aborted_turns(self) -> None:
        # Window sizing must count COMPLETED turns, not turn-starts: a tail full of aborted
        # turns (task_started but no task_complete) must still grow to the recent completions.
        rows = _codex_turns(adapter.RECENT_TURNS + 3, prefix="done")
        for i in range(20):
            rows.append({"type": "event_msg", "payload": {"type": "task_started", "turn_id": f"ab{i}", "started_at": i}})
            rows.append({"type": "response_item", "payload": {"type": "message", "role": "user",
                         "content": [{"type": "input_text", "text": f"aborted {i}"}]}})
            rows.append({"type": "event_msg", "payload": {"type": "turn_aborted"}})
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "rollout-2026-06-15T00-00-00-s.jsonl"
            write_jsonl(path, rows)
            with patch.object(adapter, "TURN_TAIL_BYTES", 10**9):
                full = adapter.extract_codex_turn(path, "p", "s")
            with patch.object(adapter, "TURN_TAIL_BYTES", 200):
                tail = adapter.extract_codex_turn(path, "p", "s")
        self.assertEqual(tail["assistant_final_text"], full["assistant_final_text"])
        self.assertEqual([t["turn_id"] for t in tail["recent_turns"]],
                         [t["turn_id"] for t in full["recent_turns"]])
        self.assertEqual(len(full["recent_turns"]), adapter.RECENT_TURNS)

    def test_claude_window_grows_past_trailing_internal_users(self) -> None:
        rows = _claude_turns(adapter.RECENT_TURNS + 3)
        for i in range(20):  # trailing internal-user markers (no end_turn completion)
            rows.append({"type": "user", "uuid": f"int{i}",
                         "message": {"role": "user", "content": "<task-notification>x</task-notification>"}})
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "s.jsonl"
            write_jsonl(path, rows)
            with patch.object(adapter, "TURN_TAIL_BYTES", 10**9):
                full = adapter.extract_claude_turn(path, "p", "s")
            with patch.object(adapter, "TURN_TAIL_BYTES", 200):
                tail = adapter.extract_claude_turn(path, "p", "s")
        self.assertEqual(tail["assistant_final_text"], full["assistant_final_text"])
        self.assertEqual([t["turn_id"] for t in tail["recent_turns"]],
                         [t["turn_id"] for t in full["recent_turns"]])
        self.assertEqual(len(full["recent_turns"]), adapter.RECENT_TURNS)

    def test_single_turn_over_ceiling_falls_back_to_full_read(self) -> None:
        # A single turn larger than the ceiling -> no boundary in the window -> full read.
        rows = _codex_turns(1, prefix="big", user_pad="Y" * 5000)
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "rollout-2026-06-15T00-00-00-s.jsonl"
            write_jsonl(path, rows)
            with patch.object(adapter, "TURN_TAIL_BYTES", 256), patch.object(adapter, "TURN_TAIL_MAX_BYTES", 512):
                turn = adapter.extract_codex_turn(path, "p", "s")
        self.assertTrue(turn["available"])
        self.assertEqual(turn["turn_id"], "big0")
        self.assertEqual(turn["assistant_final_text"], "answer big0")


class ModelExtractionTests(unittest.TestCase):
    """The adapter surfaces the model (for the pinned-status suffix). claude carries
    message.model on every assistant event; codex emits turn_context.model ~per turn,
    so it survives the tail read. Absent -> the field is omitted (sync won't clobber)."""

    def test_codex_turn_extracts_model_from_turn_context(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "rollout-2026-06-15T00-00-00-s.jsonl"
            write_jsonl(path, [
                {"type": "session_meta", "payload": {"cwd": "/root"}},
                {"type": "event_msg", "payload": {"type": "task_started", "turn_id": "t1", "started_at": 1}},
                {"type": "turn_context", "payload": {"model": "gpt-5.5", "effort": "high"}},
                {"type": "response_item", "payload": {"type": "message", "role": "user",
                    "content": [{"type": "input_text", "text": "Go."}]}},
                {"type": "event_msg", "payload": {"type": "task_complete", "turn_id": "t1",
                    "completed_at": 2, "last_agent_message": "Done."}},
            ])
            turn = adapter.extract_codex_turn(path, "p", "s")
        self.assertEqual(turn["model"], "gpt-5.5")

    def test_claude_turn_extracts_model_from_message(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "s.jsonl"
            write_jsonl(path, [
                {"type": "user", "uuid": "u1", "message": {"role": "user", "content": "Hi."}},
                {"type": "assistant", "uuid": "a1", "timestamp": "ts",
                 "message": {"role": "assistant", "model": "claude-opus-4-8",
                             "stop_reason": "end_turn", "content": [{"type": "text", "text": "Hello."}]}},
            ])
            turn = adapter.extract_claude_turn(path, "p", "s")
        self.assertEqual(turn["model"], "claude-opus-4-8")

    def test_claude_ignores_synthetic_model(self) -> None:
        # A trailing synthetic message (interrupt/error) must not overwrite the real model.
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "s.jsonl"
            write_jsonl(path, [
                {"type": "user", "uuid": "u1", "message": {"role": "user", "content": "Hi."}},
                {"type": "assistant", "uuid": "a1", "timestamp": "ts",
                 "message": {"role": "assistant", "model": "claude-opus-4-8",
                             "stop_reason": "end_turn", "content": [{"type": "text", "text": "Hello."}]}},
                {"type": "assistant", "uuid": "s1",
                 "message": {"role": "assistant", "model": "<synthetic>",
                             "stop_reason": "tool_use", "content": [{"type": "text", "text": "x"}]}},
            ])
            turn = adapter.extract_claude_turn(path, "p", "s")
        self.assertEqual(turn["model"], "claude-opus-4-8")

    def test_model_omitted_when_absent(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "rollout-2026-06-15T00-00-00-s.jsonl"
            write_jsonl(path, _codex_turns(2))  # no turn_context anywhere
            turn = adapter.extract_codex_turn(path, "p", "s")
        self.assertNotIn("model", turn)

    def test_codex_model_survives_tail_read(self) -> None:
        # turn_context recurs per turn, so even a tiny tail window still surfaces a model.
        rows: list[dict] = []
        for i in range(adapter.RECENT_TURNS + 5):
            rows.append({"type": "event_msg", "payload": {"type": "task_started", "turn_id": f"t{i}", "started_at": i}})
            rows.append({"type": "turn_context", "payload": {"model": "gpt-5.4"}})
            rows.append({"type": "response_item", "payload": {"type": "message", "role": "user",
                         "content": [{"type": "input_text", "text": f"prompt {i}"}]}})
            rows.append({"type": "event_msg", "payload": {"type": "task_complete", "turn_id": f"t{i}",
                         "completed_at": i, "last_agent_message": f"answer {i}"}})
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "rollout-2026-06-15T00-00-00-s.jsonl"
            write_jsonl(path, rows)
            with patch.object(adapter, "TURN_TAIL_BYTES", 200):  # force the tail path
                turn = adapter.extract_codex_turn(path, "p", "s")
        self.assertEqual(turn["model"], "gpt-5.4")


class ClaudeDecisionTests(unittest.TestCase):
    """Issue #36: the adapter surfaces a PENDING AskUserQuestion/ExitPlanMode — recorded by the
    herdres Claude hook to a per-session file (the transcript never contains it while pending) — as
    a structured pending_decision/pending_interaction turn so herdres renders buttons."""

    SESSION = "session-1"
    OPEN_PROMPT = [{"type": "user", "uuid": "user-1", "message": {"role": "user", "content": "Help me decide."}}]

    def _run(self, pending=None, events=None, session=None):
        """Write a transcript (default: just an open user prompt) + an optional hook-style pending
        file under a temp HERDRES_PENDING_DIR, then extract."""
        session = session or self.SESSION
        events = self.OPEN_PROMPT if events is None else events
        with tempfile.TemporaryDirectory() as tmp:
            pdir = Path(tmp) / "pending"
            if pending is not None:
                pdir.mkdir(parents=True, exist_ok=True)
                safe = "".join(c for c in session if c.isalnum() or c in "-_.")[:120] or "session"
                (pdir / f"{safe}.json").write_text(json.dumps(pending), encoding="utf-8")
            path = Path(tmp) / f"{session}.jsonl"
            write_jsonl(path, events)
            with patch.dict(os.environ, {"HERDRES_PENDING_DIR": str(pdir)}, clear=False):
                return adapter.extract_claude_turn(path, "pane-1", session)

    def _pending(self, name, tool_input, tool_use_id="hookdec-abc", ts=None):
        return {"tool_use_id": tool_use_id, "name": name, "input": tool_input,
                "session_id": self.SESSION, "ts": time.time() if ts is None else ts}

    def _auq(self, questions, **kw):
        return self._pending("AskUserQuestion", {"questions": questions}, **kw)

    def test_ask_user_question_pending_emits_decision(self) -> None:
        turn = self._run(self._auq([
            {"question": "Which DB?", "header": "Storage", "multiSelect": False,
             "options": [{"label": "Postgres", "description": "x"}, {"label": "SQLite"}]},
        ], tool_use_id="hookdec-q1"))
        self.assertTrue(turn.get("awaiting_input"))
        self.assertIs(turn.get("complete"), False)
        dec = turn.get("pending_decision")
        self.assertIsInstance(dec, dict)
        self.assertEqual(dec["decision_id"], "hookdec-q1")
        labels = [o["label"] for o in dec["options"]]
        self.assertEqual(labels[:2], ["Postgres", "SQLite"])
        self.assertEqual(dec["options"][0]["send_text"], "Postgres")  # send_text == label (verified round-trip)
        self.assertEqual(dec["options"][-1]["send_text"], "")  # trailing custom -> ForceReply
        self.assertIn("Storage", dec["prompt"])
        self.assertNotIn("pending_interaction", turn)

    def test_no_pending_file_no_decision(self) -> None:
        # The hook cleared the file on answer (PostToolUse) -> no decision; the open turn is normal.
        turn = self._run(pending=None)
        self.assertNotIn("pending_decision", turn)
        self.assertNotIn("pending_interaction", turn)

    def test_exit_plan_mode_pending_emits_two_option_decision(self) -> None:
        with patch.object(adapter, "PLAN_APPROVE_SEND_TEXT", "1"):
            turn = self._run(self._pending("ExitPlanMode", {"plan": "# Plan\n1. do thing\n2. more"},
                                           tool_use_id="hookdec-plan"))
        dec = turn.get("pending_decision")
        self.assertIsInstance(dec, dict)
        self.assertEqual(dec["decision_id"], "hookdec-plan")
        self.assertEqual([o["id"] for o in dec["options"]], ["approve", "revise"])
        self.assertEqual(dec["options"][0]["send_text"], "1")
        self.assertEqual(dec["options"][1]["send_text"], "")  # revise -> ForceReply
        self.assertIn("do thing", turn.get("assistant_final_text", ""))  # plan in the card body
        self.assertTrue(turn.get("awaiting_input"))

    def test_multi_question_is_readonly_interaction(self) -> None:
        turn = self._run(self._auq([
            {"question": "Q1?", "header": "A", "multiSelect": False, "options": [{"label": "a1"}, {"label": "a2"}]},
            {"question": "Q2?", "header": "B", "multiSelect": False, "options": [{"label": "b1"}, {"label": "b2"}]},
        ]))
        self.assertNotIn("pending_decision", turn)
        inter = turn.get("pending_interaction")
        self.assertIsInstance(inter, dict)
        self.assertEqual(inter["kind"], "multi_question_form")
        self.assertEqual(len(inter["questions"]), 2)

    def test_multi_select_single_question_is_readonly(self) -> None:
        turn = self._run(self._auq([
            {"question": "Pick several", "header": "Multi", "multiSelect": True,
             "options": [{"label": "x"}, {"label": "y"}]},
        ]))
        self.assertNotIn("pending_decision", turn)
        self.assertIsInstance(turn.get("pending_interaction"), dict)

    def test_decisions_disabled_by_flag(self) -> None:
        with patch.object(adapter, "DECISIONS_ENABLED", False):
            turn = self._run(self._auq([
                {"question": "Which DB?", "header": "S", "multiSelect": False, "options": [{"label": "Postgres"}]},
            ]))
        self.assertNotIn("pending_decision", turn)
        self.assertNotIn("pending_interaction", turn)

    def test_option_without_label_is_skipped_not_none(self) -> None:
        turn = self._run(self._auq([
            {"question": "Pick", "header": "H", "multiSelect": False,
             "options": [{"text": "Real A"}, {"label": "B"}, {"label": None}]},
        ]))
        labels = [o["label"] for o in turn["pending_decision"]["options"]]
        self.assertNotIn("None", labels)  # no literal "None" button from a missing label
        self.assertIn("Real A", labels)   # falls back to text
        self.assertIn("B", labels)

    def test_stale_pending_file_is_ignored(self) -> None:
        # An abandoned file (missed PostToolUse / crash) is bounded by the TTL.
        turn = self._run(self._auq([
            {"question": "Which DB?", "header": "S", "multiSelect": False, "options": [{"label": "Postgres"}]},
        ], ts=0))  # epoch -> far older than HERDRES_PENDING_TTL_SECONDS
        self.assertNotIn("pending_decision", turn)

    def test_foreign_tool_pending_file_is_ignored(self) -> None:
        # A stray file naming a non-decision tool must never produce buttons.
        turn = self._run(self._pending("Bash", {"command": "ls"}))
        self.assertNotIn("pending_decision", turn)
        self.assertNotIn("pending_interaction", turn)

    def test_decision_with_prior_completed_turns_keeps_recent(self) -> None:
        events = [
            {"type": "user", "uuid": "u0", "message": {"role": "user", "content": "First task."}},
            {"type": "assistant", "uuid": "a0", "timestamp": "2026-06-15T09:00:00Z",
             "message": {"role": "assistant", "stop_reason": "end_turn", "content": [{"type": "text", "text": "Done first."}]}},
            {"type": "user", "uuid": "user-1", "message": {"role": "user", "content": "Next?"}},
        ]
        turn = self._run(self._auq([
            {"question": "Next?", "header": "Step", "multiSelect": False, "options": [{"label": "Go"}, {"label": "Stop"}]},
        ]), events=events)
        self.assertIsInstance(turn.get("pending_decision"), dict)
        self.assertIn("recent_turns", turn)  # history preserved for catch-up

    def test_ask_user_question_decision_consumable_by_herdres(self) -> None:
        import herdres
        turn = self._run(self._auq([
            {"question": "Which DB?", "header": "Storage", "multiSelect": False,
             "options": [{"label": "Postgres"}, {"label": "SQLite"}]},
        ]))
        norm = herdres.normalize_pending_decision(turn)
        self.assertIsInstance(norm, dict)  # adapter output passes herdres' validation
        self.assertEqual([o["send_text"] for o in norm["options"]][:2], ["Postgres", "SQLite"])
        item = herdres.make_decision_feed_item(turn, norm)
        self.assertEqual(item["kind"], "decision")
        self.assertFalse(herdres.prompt_interaction_disabled(item))  # structured source -> buttons enabled

    def test_exit_plan_mode_decision_consumable_by_herdres(self) -> None:
        import herdres
        with patch.object(adapter, "PLAN_APPROVE_SEND_TEXT", "1"):
            turn = self._run(self._pending("ExitPlanMode", {"plan": "# Plan\nstep one"}))
        norm = herdres.normalize_pending_decision(turn)
        self.assertIsInstance(norm, dict)
        self.assertEqual([o["label"] for o in norm["options"]],
                         ["✅ Approve & proceed", "✍️ Keep planning / revise"])
        # approve carries the send_text knob; revise (empty) flags needs_detail -> ForceReply
        self.assertEqual(norm["options"][0]["send_text"], "1")
        self.assertEqual(norm["options"][1].get("needs_detail"), "1")


if __name__ == "__main__":
    unittest.main()
