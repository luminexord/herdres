import json
import tempfile
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


if __name__ == "__main__":
    unittest.main()
