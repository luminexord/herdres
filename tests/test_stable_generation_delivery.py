"""Stable-key delivery across Tendwire worker-generation churn."""

from __future__ import annotations

from copy import deepcopy

import pytest

from herdres_connector import state
from herdres_connector.source_sync import SyncRuntime, sync_once

from test_source_only import FakeTelegram, _store
from test_stable_worker_key import KEY_A, _worker
from test_turn_delta_sync import _active_delta, _page, _upsert
from test_turn_final_delivery import ReadyQueueTendwire, _turn_row


@pytest.fixture(autouse=True)
def _worker_mode(monkeypatch):
    monkeypatch.setenv("HERDRES_TENDWIRE_MODE", "source")
    monkeypatch.setenv("HERDRES_SOURCE_TOPIC_MODE", "worker")
    monkeypatch.setenv("HERDRES_PINNED_STATUS", "0")
    monkeypatch.setenv("HERDRES_TENDWIRE_FULL_RECONCILE_SECONDS", "3600")
    monkeypatch.setenv("HERDR_TELEGRAM_TOPICS_STATUS_ICON", "0")
    monkeypatch.setenv("HERDR_TELEGRAM_TOPICS_WORKING_UPDATE_MIN_SECONDS", "0")


def _stable_row(
    worker_id: str,
    *,
    turn_id: str,
    revision: str,
    updated_at: str,
    final: str | None,
    stream: str | None = None,
) -> dict:
    row = _turn_row(turn_id, revision, final, user="Question")
    row.update(
        {
            "worker_id": worker_id,
            "worker_fingerprint": f"fp-{worker_id}",
            "stable_key": KEY_A,
            "stable_key_version": 1,
            "space_id": "w1",
            "updated_at": updated_at,
        }
    )
    if stream is not None:
        row["assistant_stream_text"] = stream
        row["status"] = "active"
    return row


class StableDeltaTurnFinalTendwire(ReadyQueueTendwire):
    """Production-shaped schema-v2 delta + turn-final fake."""

    def __init__(
        self,
        *,
        workers: list[dict],
        delta_rows: list[dict],
        ready_rows: list[dict] | None = None,
    ):
        ready = list(ready_rows or [])
        seed = (
            ready[0]
            if ready
            else _stable_row(
                "gen-A",
                turn_id="turn-unused",
                revision="twrev1.unused",
                updated_at="2000-01-01T00:00:00+00:00",
                final=None,
            )
        )
        super().__init__(ready or [seed])
        self.rows = ready
        self.emit_ready = bool(ready)
        self._snapshot_workers = deepcopy(workers)
        self._delta_page = _page(
            [_upsert(row) for row in delta_rows],
            mode="changes",
            checkpoint="twdelta1.current",
        )
        self.delta_calls: list[dict] = []
        self.turn_calls = 0

    def snapshot(self):
        return {
            "ok": True,
            "workers": deepcopy(self._snapshot_workers),
            "spaces": [
                {
                    "id": "w1",
                    "name": "Project",
                    "status": "active",
                    "fingerprint": "space-fp-1",
                }
            ],
        }

    def turn_delta(self, **kwargs):
        self.delta_calls.append(dict(kwargs))
        return deepcopy(self._delta_page)

    def turns(self):
        self.turn_calls += 1
        return {"ok": True, "schema_version": 2, "turns": []}

    def connector_prepare_begin(self, **kwargs):
        response = super().connector_prepare_begin(**kwargs)
        old_token = str(response.get("plan_token") or "")
        revision = str(kwargs["content_revision"])
        new_token = "twplan1." + revision.removeprefix("twrev1.")
        if (
            old_token
            and old_token != new_token
            and old_token in self._plans
            and new_token not in self._plans
        ):
            self._plans[new_token] = self._plans.pop(old_token)
            self._plan_by_revision[revision] = new_token
            response["plan_token"] = new_token
        return response

    def turn_final_poll(self, *, limit=1, lease_seconds=60):
        if not self.rows:
            self.poll_calls += 1
            return {"ok": True, "schema_version": 1, "items": []}
        return super().turn_final_poll(
            limit=limit, lease_seconds=lease_seconds
        )


def _persist_bound_entry(store: dict, worker_id: str = "gen-A"):
    entry_key, entry = state.upsert_worker_entry(
        store,
        _worker(worker_id, KEY_A),
        topic_id="26",
    )[:2]
    store["tendwire_delta_sync"] = _active_delta()
    return entry_key, entry


def _sync(
    store: dict,
    workers: list[dict],
    delta_rows: list[dict],
    *,
    ready_rows: list[dict] | None = None,
):
    telegram = FakeTelegram()
    tendwire = StableDeltaTurnFinalTendwire(
        workers=workers,
        delta_rows=delta_rows,
        ready_rows=ready_rows,
    )
    result = sync_once(
        store,
        SyncRuntime(
            tendwire,
            telegram,
            with_outbox=True,
            max_sends=50,
        ),
    )
    return result, telegram, tendwire


def test_same_stable_key_turn_on_new_generation_rebinds_and_delivers():
    store = _store()
    entry_key, _entry = _persist_bound_entry(store)
    latest = _stable_row(
        "gen-B",
        turn_id="turn-B",
        revision="twrev1.gen_b",
        updated_at="2030-01-01T00:00:02+00:00",
        final="final from generation B",
    )

    result, telegram, tendwire = _sync(
        store,
        [_worker("gen-A", KEY_A), _worker("gen-B", KEY_A)],
        [latest],
        ready_rows=[latest],
    )

    entry = state.source_worker_entries(store)[entry_key]
    assert tendwire.turn_calls == 0
    assert result["worker_rebinds"] == 1
    assert result["feed_sent"] == 0
    assert result["tendwire_turn_final"]["delivered"] == 1
    assert entry["tendwire_worker_id"] == "gen-B"
    assert entry["last_turn_id"] == "turn-B"
    assert [
        text
        for _chat, text, kwargs, _message_id in telegram.sent
        if kwargs.get("thread_id") == "26"
        and "final from generation B" in text
    ]
    assert store["tendwire_worker_rebind_audit"][-1] == {
        "stable_key": KEY_A,
        "from_worker_id": "gen-A",
        "to_worker_id": "gen-B",
        "reason": "freshest_turn_activity",
        "observed_at": store["tendwire_worker_rebind_audit"][-1][
            "observed_at"
        ],
    }


def test_three_generation_churn_converges_to_generation_with_latest_turn():
    store = _store()
    entry_key, _entry = _persist_bound_entry(store)
    turn_b = _stable_row(
        "gen-B",
        turn_id="turn-B",
        revision="twrev1.gen_b",
        updated_at="2030-01-01T00:00:02+00:00",
        final="B final",
    )

    first, _telegram, _tendwire = _sync(
        store,
        [_worker("gen-A", KEY_A), _worker("gen-B", KEY_A)],
        [turn_b],
        ready_rows=[turn_b],
    )
    turn_a = _stable_row(
        "gen-A",
        turn_id="turn-A",
        revision="twrev1.gen_a",
        updated_at="2030-01-01T00:00:01+00:00",
        final="A final",
    )
    turn_c = _stable_row(
        "gen-C",
        turn_id="turn-C",
        revision="twrev1.gen_c",
        updated_at="2030-01-01T00:00:03+00:00",
        final="C final",
    )
    second, telegram, _tendwire = _sync(
        store,
        [
            _worker("gen-A", KEY_A),
            _worker("gen-B", KEY_A),
            _worker("gen-C", KEY_A),
        ],
        [turn_b, turn_a, turn_c],
        ready_rows=[turn_c],
    )

    entry = state.source_worker_entries(store)[entry_key]
    assert first["worker_rebinds"] == second["worker_rebinds"] == 1
    assert entry["tendwire_worker_id"] == "gen-C"
    assert entry["last_turn_id"] == "turn-C"
    assert [
        (item["from_worker_id"], item["to_worker_id"])
        for item in store["tendwire_worker_rebind_audit"]
    ] == [("gen-A", "gen-B"), ("gen-B", "gen-C")]
    finals = [
        text
        for _chat, text, kwargs, _message_id in telegram.sent
        if kwargs.get("thread_id") == "26"
    ]
    assert sum("C final" in text for text in finals) == 1
    assert not any("A final" in text for text in finals)


def test_prod_lane_rebind_catchup_delivers_only_newest_completed_turn():
    store = _store()
    _entry_key, _entry = _persist_bound_entry(store)
    older = _stable_row(
        "gen-A",
        turn_id="turn-old",
        revision="twrev1.old",
        updated_at="2030-01-01T00:00:01+00:00",
        final="older completed answer",
    )
    newest = _stable_row(
        "gen-A",
        turn_id="turn-new",
        revision="twrev1.new",
        updated_at="2030-01-01T00:00:03+00:00",
        final="newest completed answer",
    )
    live = _stable_row(
        "gen-B",
        turn_id="turn-live",
        revision="twrev1.live",
        updated_at="2030-01-01T00:00:04+00:00",
        final=None,
        stream="generation B is live",
    )

    result, telegram, tendwire = _sync(
        store,
        [_worker("gen-A", KEY_A), _worker("gen-B", KEY_A)],
        [older, live, newest],
        # Tendwire orders stable-owner roots oldest first. The connector must
        # consume the historical root without Telegram before reaching newest.
        ready_rows=[older, newest],
    )

    assert tendwire.turn_calls == 0
    assert result["worker_rebinds"] == 1
    assert result["tendwire_turn_final"]["acked"] == 2
    delivered = "\n".join(
        text for _chat, text, _kwargs, _mid in telegram.sent
    )
    assert "newest completed answer" in delivered
    assert "older completed answer" not in delivered


def test_conflicting_live_generation_activity_keeps_binding_and_fails_closed():
    store = _store()
    entry_key, entry = _persist_bound_entry(store)
    before = deepcopy(entry)
    turns = [
        _stable_row(
            "gen-A",
            turn_id="turn-A",
            revision="twrev1.open_a",
            updated_at="2030-01-01T00:00:01+00:00",
            final=None,
            stream="A still working",
        ),
        _stable_row(
            "gen-B",
            turn_id="turn-B",
            revision="twrev1.open_b",
            updated_at="2030-01-01T00:00:02+00:00",
            final=None,
            stream="B also working",
        ),
    ]

    result, telegram, _tendwire = _sync(
        store,
        [_worker("gen-A", KEY_A), _worker("gen-B", KEY_A)],
        turns,
    )

    current = state.source_worker_entries(store)[entry_key]
    assert result["worker_rebinds"] == 0
    assert result["feed_sent"] == 0
    assert current["tendwire_worker_id"] == before["tendwire_worker_id"]
    assert state.entry_is_quarantined(current)
    assert (
        current["stable_key_quarantine_reason"]
        == "ambiguous_stable_key_generations"
    )
    assert current["tendwire_worker_generation_ambiguity"][
        "worker_ids"
    ] == ["gen-A", "gen-B"]
    assert "tendwire_worker_rebind_audit" not in store
    assert telegram.sent == []


def test_stable_worker_id_is_noop_without_rebind_audit():
    store = _store()
    entry_key, _entry = _persist_bound_entry(store)
    ordinary = _stable_row(
        "gen-A",
        turn_id="turn-A",
        revision="twrev1.ordinary",
        updated_at="2030-01-01T00:00:01+00:00",
        final="ordinary final",
    )

    result, telegram, _tendwire = _sync(
        store,
        [_worker("gen-A", KEY_A)],
        [ordinary],
        ready_rows=[ordinary],
    )

    entry = state.source_worker_entries(store)[entry_key]
    assert result["worker_rebinds"] == 0
    assert result["feed_sent"] == 0
    assert result["tendwire_turn_final"]["delivered"] == 1
    assert entry["tendwire_worker_id"] == "gen-A"
    assert "tendwire_worker_rebind_audit" not in store
    assert any(
        "ordinary final" in text
        for _chat, text, _kwargs, _mid in telegram.sent
    )


def test_observation_only_single_generation_refresh_does_not_arm_catchup():
    store = _store()
    entry_key, _entry = _persist_bound_entry(store)

    result, telegram, _tendwire = _sync(
        store,
        [_worker("gen-B", KEY_A)],
        [],
    )

    entry = state.source_worker_entries(store)[entry_key]
    assert result["worker_rebinds"] == 1
    assert entry["tendwire_worker_id"] == "gen-B"
    assert "tendwire_rebind_catchup_pending" not in entry
    assert "tendwire_rebind_catchup_bound" not in entry
    assert store["tendwire_worker_rebind_audit"][-1]["reason"] == (
        "stable_key_cache_refresh"
    )
    assert telegram.sent == []


def test_turnless_pass_with_two_live_generations_is_strict_state_file_noop(
    tmp_path,
):
    store = _store()
    entry_key, _entry = _persist_bound_entry(store)
    # Prime normal source-only bookkeeping before measuring the quiet pass.
    _sync(store, [_worker("gen-A", KEY_A)], [])
    state_file = tmp_path / "state.json"
    state.save_state(store, state_file)
    before_bytes = state_file.read_bytes()
    before_mtime = state_file.stat().st_mtime_ns

    restarted = state.load_state(state_file)
    result, telegram, _tendwire = _sync(
        restarted,
        [_worker("gen-A", KEY_A), _worker("gen-B", KEY_A)],
        [],
    )
    if result["changed"]:
        state.save_state(restarted, state_file)

    current = state.source_worker_entries(restarted)[entry_key]
    assert result["changed"] is False
    assert current["tendwire_worker_id"] == "gen-A"
    assert "tendwire_worker_generation_ambiguity" not in current
    assert "tendwire_worker_rebind_audit" not in restarted
    assert telegram.sent == []
    assert state_file.read_bytes() == before_bytes
    assert state_file.stat().st_mtime_ns == before_mtime


def test_stale_working_generation_never_steals_binding_across_four_passes():
    store = _store()
    entry_key, _entry = _persist_bound_entry(store)
    incumbent_turn = _stable_row(
        "gen-A",
        turn_id="turn-A",
        revision="twrev1.incumbent",
        updated_at="2030-01-01T00:00:01+00:00",
        final=None,
        stream="incumbent is live",
    )
    workers = [
        _worker("gen-A", KEY_A, status="idle"),
        _worker("gen-B", KEY_A, status="working"),
    ]

    results = []
    for turns in ([incumbent_turn], [], [incumbent_turn], []):
        result, _telegram, _tendwire = _sync(store, workers, turns)
        results.append(result)
        assert (
            state.source_worker_entries(store)[entry_key][
                "tendwire_worker_id"
            ]
            == "gen-A"
        )

    assert all(result["worker_rebinds"] == 0 for result in results)
    assert "tendwire_worker_rebind_audit" not in store
    assert "tendwire_rebind_catchup_pending" not in (
        state.source_worker_entries(store)[entry_key]
    )


def test_generation_quarantine_persists_across_simulated_restart():
    store = _store()
    entry_key, _entry = _persist_bound_entry(store)
    conflicting = [
        _stable_row(
            "gen-A",
            turn_id="turn-A",
            revision="twrev1.restart_a",
            updated_at="2030-01-01T00:00:01+00:00",
            final=None,
            stream="A active",
        ),
        _stable_row(
            "gen-B",
            turn_id="turn-B",
            revision="twrev1.restart_b",
            updated_at="2030-01-01T00:00:02+00:00",
            final=None,
            stream="B active",
        ),
    ]
    first, _telegram, _tendwire = _sync(
        store,
        [_worker("gen-A", KEY_A), _worker("gen-B", KEY_A)],
        conflicting,
    )
    assert first["changed"] is True
    restarted = deepcopy(store)

    quiet, _telegram, _tendwire = _sync(
        restarted,
        [_worker("gen-A", KEY_A), _worker("gen-B", KEY_A)],
        [],
    )

    entry = state.source_worker_entries(restarted)[entry_key]
    assert quiet["worker_rebinds"] == 0
    assert state.entry_is_quarantined(entry)
    assert (
        entry["stable_key_quarantine_reason"]
        == "ambiguous_stable_key_generations"
    )
    assert entry["tendwire_worker_generation_ambiguity"]["worker_ids"] == [
        "gen-A",
        "gen-B",
    ]
