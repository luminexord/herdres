"""herdr_turn_adapter pane-list cache: tendwire captures turns for ~15 panes concurrently each cycle
and every capture used to run its own `herdr pane list` (twice) at seconds per call under load — a
storm that wedged the tendwire daemon (submits then failed with "Could not send safely"). The cache
lets all concurrent captures share one listing."""
from __future__ import annotations

import importlib.util
import json
from pathlib import Path

_SPEC = importlib.util.spec_from_file_location(
    "herdr_turn_adapter", Path(__file__).resolve().parent.parent / "herdr_turn_adapter.py"
)
adapter = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(adapter)


def _fake_listing(marker):
    return {"result": {"panes": [{"pane_id": "p1", "marker": marker}]}}


def test_cache_shares_one_listing(tmp_path, monkeypatch):
    calls = []
    monkeypatch.setattr(adapter, "PANE_LIST_CACHE", str(tmp_path / "pane_list.json"))
    monkeypatch.setenv("HERDR_ADAPTER_PANE_LIST_TTL", "60")

    def fake_run(args):
        calls.append(args)
        return _fake_listing("fresh")

    monkeypatch.setattr(adapter, "run_real_herdr_json", fake_run)
    first = adapter.cached_pane_list_json()
    second = adapter.cached_pane_list_json()   # within TTL -> served from the cache file
    assert first == second == _fake_listing("fresh")
    assert len(calls) == 1                     # only ONE real herdr call for both reads


def test_cache_expires_after_ttl(tmp_path, monkeypatch):
    cache = tmp_path / "pane_list.json"
    monkeypatch.setattr(adapter, "PANE_LIST_CACHE", str(cache))
    monkeypatch.setenv("HERDR_ADAPTER_PANE_LIST_TTL", "60")
    cache.write_text(json.dumps(_fake_listing("stale")))
    import os
    old = 10_000  # epoch seconds long past
    os.utime(cache, (old, old))
    monkeypatch.setattr(adapter, "run_real_herdr_json", lambda args: _fake_listing("fresh"))
    assert adapter.cached_pane_list_json() == _fake_listing("fresh")   # expired -> refetched
    assert json.loads(cache.read_text()) == _fake_listing("fresh")     # cache updated


def test_ttl_zero_disables_cache(tmp_path, monkeypatch):
    calls = []
    monkeypatch.setattr(adapter, "PANE_LIST_CACHE", str(tmp_path / "pane_list.json"))
    monkeypatch.setenv("HERDR_ADAPTER_PANE_LIST_TTL", "0")
    monkeypatch.setattr(adapter, "run_real_herdr_json", lambda args: calls.append(1) or _fake_listing("x"))
    adapter.cached_pane_list_json()
    adapter.cached_pane_list_json()
    assert len(calls) == 2                     # no caching, no cache file
    assert not (tmp_path / "pane_list.json").exists()


def test_corrupt_cache_falls_back_to_cli(tmp_path, monkeypatch):
    cache = tmp_path / "pane_list.json"
    monkeypatch.setattr(adapter, "PANE_LIST_CACHE", str(cache))
    monkeypatch.setenv("HERDR_ADAPTER_PANE_LIST_TTL", "60")
    cache.write_text("not json{{{")
    monkeypatch.setattr(adapter, "run_real_herdr_json", lambda args: _fake_listing("fresh"))
    assert adapter.cached_pane_list_json() == _fake_listing("fresh")   # fail-open


def test_pane_from_list_uses_cache(tmp_path, monkeypatch):
    monkeypatch.setattr(adapter, "PANE_LIST_CACHE", str(tmp_path / "pane_list.json"))
    monkeypatch.setenv("HERDR_ADAPTER_PANE_LIST_TTL", "60")
    monkeypatch.setattr(adapter, "run_real_herdr_json", lambda args: _fake_listing("fresh"))
    pane = adapter.pane_from_list("p1")
    assert pane == {"pane_id": "p1", "marker": "fresh"}


def _write_codex_turn(
    path: Path,
    prompt: str,
    final: str,
    *,
    internal_prompt: str | None = None,
) -> None:
    events = [
        {
            "type": "event_msg",
            "payload": {"type": "task_started", "turn_id": "turn-lossless"},
        },
    ]
    if internal_prompt is not None:
        events.append(
            {
                "type": "response_item",
                "payload": {
                    "type": "message",
                    "role": "user",
                    "content": [{"type": "input_text", "text": internal_prompt}],
                },
            }
        )
    events.extend(
        [
            {
                "type": "response_item",
                "payload": {
                    "type": "message",
                    "role": "user",
                    "content": [{"type": "input_text", "text": prompt}],
                },
            },
            {
                "type": "event_msg",
                "payload": {
                    "type": "task_complete",
                    "turn_id": "turn-lossless",
                    "last_agent_message": final,
                },
            },
        ]
    )
    path.write_text(
        "".join(json.dumps(event, separators=(",", ":")) + "\n" for event in events),
        encoding="utf-8",
    )


def test_canonical_prompt_and_final_preserve_twenty_thousand_sanitized_chars(tmp_path):
    prompt = "P" * 9_999 + "\x00" + "Q" * 10_001
    final = "R" * 10_001 + "\x00" + "S" * 9_999
    expected_prompt = prompt.replace("\x00", "")
    expected_final = final.replace("\x00", "")
    transcript = tmp_path / "codex.jsonl"
    _write_codex_turn(transcript, prompt, final)

    turn = adapter.extract_codex_turn(transcript, "pane-1", "session-1")

    assert len(expected_prompt) == len(expected_final) == 20_000
    assert turn["user_text"] == expected_prompt
    assert turn["assistant_final_text"] == expected_final
    assert turn["recent_turns"][0]["user_text"] == expected_prompt
    assert turn["recent_turns"][0]["assistant_final_text"] == expected_final
    assert "[truncated]" not in turn["user_text"]
    assert "[truncated]" not in turn["assistant_final_text"]


def test_adapter_serialization_preserves_prompt_and_final_larger_than_one_mib(
    tmp_path,
    monkeypatch,
    capsys,
):
    prompt = "prompt-" + "p" * (1024 * 1024 + 31) + "\x00"
    final = "final-" + "f" * (1024 * 1024 + 47) + "\x00"
    expected_prompt = prompt.replace("\x00", "")
    expected_final = final.replace("\x00", "")
    transcript = tmp_path / "codex-large.jsonl"
    _write_codex_turn(transcript, prompt, final)
    extracted = adapter.extract_codex_turn(transcript, "pane-1", "session-1")

    monkeypatch.setattr(
        adapter,
        "pane_turn",
        lambda pane_id: adapter.result_turn(extracted),
    )
    monkeypatch.setattr(adapter.sys, "argv", ["herdr_turn_adapter.py", "pane", "turn", "pane-1"])

    assert adapter.main() == 0
    serialized = json.loads(capsys.readouterr().out)
    turn = serialized["result"]["turn"]
    assert len(turn["user_text"].encode("utf-8")) > 1024 * 1024
    assert len(turn["assistant_final_text"].encode("utf-8")) > 1024 * 1024
    assert turn["user_text"] == expected_prompt
    assert turn["assistant_final_text"] == expected_final
    assert "[truncated]" not in turn["user_text"]
    assert "[truncated]" not in turn["assistant_final_text"]


def test_transient_stream_remains_bounded_while_short_text_is_unchanged():
    short = "short response"
    assert adapter.sanitize_canonical_text(short) == short
    assert adapter.sanitize_bounded_text(short) == short

    turn = adapter.add_stream_fields({}, "x" * (adapter.MAX_TEXT_CHARS + 500), "codex")

    assert len(turn["assistant_stream_text"]) <= adapter.MAX_TEXT_CHARS
    assert turn["assistant_stream_text"].endswith("\n[truncated]")


def test_internal_prompt_and_worklog_private_values_remain_safe(tmp_path):
    private_prompt = "<environment_context>pane_id=private-pane</environment_context>"
    transcript = tmp_path / "codex-private.jsonl"
    _write_codex_turn(
        transcript,
        "public prompt",
        "public final",
        internal_prompt=private_prompt,
    )

    turn = adapter.extract_codex_turn(transcript, "pane-1", "session-1")
    encoded = json.dumps(adapter.result_turn(turn))
    cleaned = adapter._clean_worklog_line(
        "Authorization: supersecret\u200b https://alice:hunter2@example.test/resource"
    )

    assert turn["user_text"] == "public prompt"
    assert "private-pane" not in encoded
    assert "supersecret" not in cleaned
    assert "hunter2" not in cleaned
    assert "\u200b" not in cleaned
    assert "***" in cleaned
