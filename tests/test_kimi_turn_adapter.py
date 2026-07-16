from __future__ import annotations

import hashlib
import importlib.util
import json
from pathlib import Path


_SPEC = importlib.util.spec_from_file_location(
    "herdr_turn_adapter_kimi",
    Path(__file__).resolve().parent.parent / "herdr_turn_adapter.py",
)
adapter = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(adapter)


def _append(path: Path, value: dict) -> None:
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(value, separators=(",", ":")) + "\n")


def _fixture(tmp_path: Path) -> tuple[dict, Path, str]:
    cwd = tmp_path / "project"
    cwd.mkdir()
    home = tmp_path / ".kimi-code"
    suffix = hashlib.sha256(str(cwd.resolve()).encode()).hexdigest()[:12]
    session_id = "01b70f90-4bf0-4110-a5f6-2b1ba6ad69db"
    session = home / "sessions" / f"wd_project_{suffix}" / f"session_{session_id}"
    wire = session / "agents" / "main" / "wire.jsonl"
    wire.parent.mkdir(parents=True)
    (session / "state.json").write_text(
        json.dumps({"workDir": str(cwd.resolve())}),
        encoding="utf-8",
    )
    wire.write_text("", encoding="utf-8")
    pane = {
        "agent": "kimi",
        "pane_id": "pane-kimi",
        "cwd": str(cwd),
        "foreground_cwd": str(cwd),
    }
    return pane, wire, session_id


def _listing(*panes: dict) -> dict:
    return {"result": {"panes": list(panes)}}


def test_kimi_open_progress_and_final_keep_one_turn_identity(tmp_path, monkeypatch):
    pane, wire, session_id = _fixture(tmp_path)
    monkeypatch.setenv("KIMI_CODE_HOME", str(tmp_path / ".kimi-code"))
    monkeypatch.setattr(adapter, "pane_from_list", lambda pane_id: pane)
    monkeypatch.setattr(adapter, "cached_pane_list_json", lambda: _listing(pane))

    _append(
        wire,
        {
            "type": "turn.prompt",
            "time": 1_784_215_802_811,
            "origin": {"kind": "user"},
            "input": [{"type": "text", "text": "Review this change"}],
        },
    )
    _append(
        wire,
        {
            "type": "context.append_message",
            "time": 1_784_215_802_812,
            "message": {
                "role": "user",
                "origin": {"kind": "injection", "variant": "permission_mode"},
                "content": [{"type": "text", "text": "private injected context"}],
            },
        },
    )
    _append(
        wire,
        {
            "type": "context.append_loop_event",
            "time": 1_784_215_803_000,
            "event": {
                "type": "content.part",
                "turnId": "1",
                "step": 1,
                "part": {"type": "think", "think": "Checking the affected tests"},
            },
        },
    )

    opened = adapter.pane_turn("pane-kimi")["result"]["turn"]
    assert opened["agent"] == "kimi"
    assert opened["agent_session_id"] == session_id
    assert opened["user_text"] == "Review this change"
    assert opened["assistant_stream_text"] == "Checking the affected tests"
    assert opened["complete"] is False
    assert "private injected context" not in json.dumps(opened)
    open_id = opened["turn_id"]

    final_text = "  Final review\r\n\r\n- item one\n- item two  " + "x" * 20_000
    _append(
        wire,
        {
            "type": "context.append_loop_event",
            "time": 1_784_215_804_000,
            "event": {
                "type": "content.part",
                "turnId": "1",
                "step": 2,
                "part": {"type": "text", "text": final_text[:10_000]},
            },
        },
    )
    _append(
        wire,
        {
            "type": "context.append_loop_event",
            "time": 1_784_215_804_100,
            "event": {
                "type": "content.part",
                "turnId": "1",
                "step": 2,
                "part": {"type": "text", "text": final_text[10_000:]},
            },
        },
    )
    _append(
        wire,
        {
            "type": "context.append_loop_event",
            "time": 1_784_215_804_200,
            "event": {
                "type": "step.end",
                "turnId": "1",
                "step": 2,
                "finishReason": "end_turn",
            },
        },
    )

    final = adapter.pane_turn("pane-kimi")["result"]["turn"]
    assert final["turn_id"] == open_id
    assert final["user_text"] == "Review this change"
    assert final["assistant_final_text"] == final_text
    assert final["complete"] is True
    assert final["recent_turns"][0]["assistant_final_text"] == final_text
    assert "[truncated]" not in final["assistant_final_text"]


def test_kimi_new_prompt_exposes_open_fields_after_previous_final(tmp_path, monkeypatch):
    pane, wire, _ = _fixture(tmp_path)
    monkeypatch.setenv("KIMI_CODE_HOME", str(tmp_path / ".kimi-code"))
    monkeypatch.setattr(adapter, "cached_pane_list_json", lambda: _listing(pane))
    for value in (
        {"type": "turn.prompt", "time": 1000, "origin": {"kind": "user"}, "input": [{"type": "text", "text": "first"}]},
        {"type": "context.append_loop_event", "time": 1100, "event": {"type": "content.part", "turnId": "1", "step": 1, "part": {"type": "text", "text": "done"}}},
        {"type": "context.append_loop_event", "time": 1200, "event": {"type": "step.end", "turnId": "1", "step": 1, "finishReason": "end_turn"}},
        {"type": "turn.prompt", "time": 2000, "origin": {"kind": "user"}, "input": [{"type": "text", "text": "second"}]},
        {"type": "context.append_loop_event", "time": 2100, "event": {"type": "tool.call", "turnId": "2", "step": 1, "name": "search", "description": "Inspecting files"}},
    ):
        _append(wire, value)

    turn = adapter.infer_kimi_turn_from_workspace(pane, "pane-kimi")["result"]["turn"]
    assert turn["assistant_final_text"] == "done"
    assert turn["has_open_turn"] is True
    assert turn["open_user_text"] == "second"
    assert turn["assistant_stream_text"] == "Inspecting files"
    assert turn["open_turn_id"] != turn["turn_id"]


def test_kimi_same_workspace_multiple_live_panes_fail_closed(tmp_path, monkeypatch):
    pane, _, _ = _fixture(tmp_path)
    other = {**pane, "pane_id": "pane-kimi-2"}
    monkeypatch.setenv("KIMI_CODE_HOME", str(tmp_path / ".kimi-code"))
    monkeypatch.setattr(adapter, "cached_pane_list_json", lambda: _listing(pane, other))

    result = adapter.infer_kimi_turn_from_workspace(pane, "pane-kimi")

    assert result["result"]["turn"]["available"] is False
    assert result["result"]["turn"]["reason"] == "ambiguous_kimi_workspace"


def test_kimi_workspace_hash_and_state_must_both_match(tmp_path, monkeypatch):
    pane, _, _ = _fixture(tmp_path)
    home = tmp_path / ".kimi-code"
    suffix = adapter._kimi_workspace_suffix(pane["cwd"])
    decoy = home / "sessions" / f"wd_decoy_{suffix}" / "session_11111111-1111-4111-8111-111111111111"
    (decoy / "agents" / "main").mkdir(parents=True)
    (decoy / "state.json").write_text(json.dumps({"workDir": str(tmp_path)}))
    (decoy / "agents" / "main" / "wire.jsonl").write_text("")
    monkeypatch.setenv("KIMI_CODE_HOME", str(home))

    # Duplicate hash-shaped workspace directories are ambiguous even when one
    # state file points elsewhere.
    assert adapter._kimi_session_path_for_pane(pane) is None
