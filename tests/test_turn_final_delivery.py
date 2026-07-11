from __future__ import annotations

from copy import deepcopy
import json
from contextlib import nullcontext
from types import SimpleNamespace

import herdres
import pytest
from herdres_connector import state
from herdres_connector.source_sync import PRESENTATION_VERSION, SyncRuntime, sync_once
from herdres_connector.telegram_delivery import RateLimited, TelegramClient, TelegramError
from test_source_only import FakeTelegram, _source_worker, _store




def _descriptor(value: str | None, *, inline: bool, page_count: int = 0, first_cursor: str | None = None):
    if value is None:
        return {
            "availability": "absent",
            "inline": False,
            "char_length": 0,
            "byte_length": 0,
            "page_count": 0,
            "first_cursor": None,
        }
    return {
        "availability": "complete",
        "inline": inline,
        "char_length": len(value),
        "byte_length": len(value.encode("utf-8")),
        "page_count": page_count if not inline else 1,
        "first_cursor": first_cursor if not inline else None,
    }


def _turn_row(turn_id: str, revision: str, final: str | None, *, user: str | None = None, inline: bool = True):
    row = {
        "id": turn_id,
        "worker_id": "worker-1",
        "complete": final is not None,
        "content": {
            "schema_version": 1,
            "content_revision": revision,
            "known_incomplete": False,
            "fields": {},
        },
    }
    row["content"]["fields"]["user_text"] = _descriptor(user, inline=inline)
    row["content"]["fields"]["assistant_final_text"] = _descriptor(final, inline=inline)
    if inline:
        if user is not None:
            row["user_text"] = user
        if final is not None:
            row["assistant_final_text"] = final
    return row


def _mark_known_incomplete(row, fragment):
    row["content"]["known_incomplete"] = True
    row["content"]["fields"]["assistant_final_text"] = {
        "availability": "known_incomplete",
        "inline": False,
        "char_length": len(fragment),
        "byte_length": len(fragment.encode("utf-8")),
        "page_count": 0,
        "first_cursor": None,
    }
    row.pop("assistant_final_text", None)
    return row


class TurnFinalTendwire:
    def __init__(self, row):
        self.row = row
        self.pages = {}
        self.page_calls = []
        self.prepare_calls = []
        self.poll_calls = 0
        self.ack_calls = []
        self.fail_calls = []
        self.defer_calls = []
        self._plans = {}
        self._plan_by_revision = {}
        self._jobs = []
        self._ref_counter = 0
        self._active_plan = ""
        self.ack_loss_once = False
        self.ack_committed_response_lost_once = False

    def snapshot(self):
        return {
            "ok": True,
            "workers": [_source_worker({
                "id": "worker-1",
                "name": "Alpha",
                "status": "idle" if self.row.get("complete") else "working",
                "space_id": "space-1",
                "fingerprint": "fp-1",
                "meta": {"agent": "codex"},
            })],
            "spaces": [{"id": "space-1", "name": "Project", "status": "active", "fingerprint": "space-fp-1"}],
        }

    def turns(self):
        return {"ok": True, "schema_version": 2, "turns": [deepcopy(self.row)]}

    def pending(self):
        return {"ok": True, "pending_interactions": []}

    def connector_poll(self, **_kwargs):
        return {"ok": True, "items": []}

    def turn_content_get(self, turn_id, revision, field, cursor=None):
        self.page_calls.append((turn_id, revision, field, cursor))
        return deepcopy(self.pages[(revision, field, cursor)])

    def install_pages(self, revision: str, field: str, value: str, cuts: tuple[int, ...]):
        starts = (0, *cuts)
        ends = (*cuts, len(value))
        count = len(ends)
        cursors = [f"twcur1.{revision.split('.')[-1]}_{field}_{index}" for index in range(count)]
        for index, (start, end) in enumerate(zip(starts, ends)):
            text = value[start:end]
            self.pages[(revision, field, cursors[index])] = {
                "ok": True,
                "schema_version": 1,
                "turn_id": self.row["id"],
                "content_revision": revision,
                "field": field,
                "availability": "complete",
                "segment_id": f"twseg1.{revision.split('.')[-1]}_{field}_{index}",
                "index": index,
                "count": count,
                "text": text,
                "segment_char_length": len(text),
                "segment_byte_length": len(text.encode("utf-8")),
                "total_char_length": len(value),
                "total_byte_length": len(value.encode("utf-8")),
                "next_cursor": cursors[index + 1] if index + 1 < count else None,
            }
        descriptor = self.row["content"]["fields"][field]
        descriptor.update({
            "inline": False,
            "page_count": count,
            "first_cursor": cursors[0],
            "char_length": len(value),
            "byte_length": len(value.encode("utf-8")),
        })
        self.row.pop(field, None)

    def connector_prepare_begin(self, *, turn_id, content_revision, presentation_version, part_count):
        assert presentation_version == PRESENTATION_VERSION
        assert "telegram" not in presentation_version and "herdres" not in presentation_version
        self.prepare_calls.append(("begin", content_revision, part_count))
        token = self._plan_by_revision.get(content_revision)
        if token:
            plan = self._plans[token]
            return {"ok": True, "plan_token": token, "state": plan["state"], "part_count": part_count, "accepted_parts": len(plan["parts"])}
        token = f"twplan1.plan{len(self._plans) + 1}"
        self._plan_by_revision[content_revision] = token
        self._plans[token] = {
            "state": "preparing",
            "turn_id": turn_id,
            "revision": content_revision,
            "part_count": part_count,
            "parts": {},
            "replaces": self._active_plan,
        }
        return {"ok": True, "plan_token": token, "state": "preparing", "part_count": part_count, "accepted_parts": 0}

    def connector_prepare_part(self, *, plan_token, ordinal, spans):
        self.prepare_calls.append(("part", plan_token, ordinal))
        self._plans[plan_token]["parts"][ordinal] = deepcopy(spans)
        return {"ok": True, "plan_token": plan_token, "ordinal": ordinal, "accepted_parts": len(self._plans[plan_token]["parts"])}

    def connector_prepare_commit(self, *, plan_token):
        self.prepare_calls.append(("commit", plan_token))
        plan = self._plans[plan_token]
        if plan["state"] != "preparing":
            count = len([job for job in self._jobs if job["payload"]["plan_token"] == plan_token])
            return {"ok": True, "plan_token": plan_token, "state": plan["state"], "job_count": count}
        sequence = 0
        jobs = []
        for ordinal in range(plan["part_count"]):
            jobs.append(self._job(plan_token, sequence, "upsert", ordinal, plan["part_count"], plan["parts"][ordinal], plan["replaces"]))
            sequence += 1
        if plan["replaces"]:
            old_count = self._plans[plan["replaces"]]["part_count"]
            for ordinal in range(old_count - 1, plan["part_count"] - 1, -1):
                jobs.append(self._job(plan_token, sequence, "retire", ordinal, plan["part_count"], [], plan["replaces"]))
                sequence += 1
        self._jobs.extend(jobs)
        plan["state"] = "active"
        self._active_plan = plan_token
        return {"ok": True, "plan_token": plan_token, "state": "active", "job_count": len(jobs)}

    def _job(self, token, sequence, operation, ordinal, part_count, spans, replaces):
        plan = self._plans[token]
        return {
            "status": "queued",
            "key": f"turn-final:{token}:{sequence:06d}",
            "payload": {
                "schema_version": 1,
                "plan_token": token,
                "content_revision": plan["revision"],
                "presentation_version": PRESENTATION_VERSION,
                "operation": operation,
                "sequence_index": sequence,
                "part_ordinal": ordinal,
                "part_count": part_count,
                "spans": deepcopy(spans),
                "replaces_plan_token": replaces or None,
            },
        }

    def turn_final_poll(self, *, limit=1, lease_seconds=60):
        assert limit == 1 and lease_seconds == 60
        self.poll_calls += 1
        for job in self._jobs:
            if job["status"] != "queued":
                continue
            token = job["payload"]["plan_token"]
            sequence = job["payload"]["sequence_index"]
            prior = [candidate for candidate in self._jobs if candidate["payload"]["plan_token"] == token and candidate["payload"]["sequence_index"] < sequence]
            if any(candidate["status"] != "delivered" for candidate in prior):
                continue
            self._ref_counter += 1
            job["status"] = "leased"
            job["ref"] = f"twref1.lease{self._ref_counter}"
            return {"ok": True, "schema_version": 1, "items": [{"ref": job["ref"], "key": job["key"], "attempt": self._ref_counter, "payload": deepcopy(job["payload"])}]}
        return {"ok": True, "schema_version": 1, "items": []}

    def _leased(self, ref):
        return next(job for job in self._jobs if job.get("ref") == ref and job["status"] == "leased")

    def turn_final_ack(self, ref, response=None):
        self.ack_calls.append((ref, deepcopy(response)))
        job = self._leased(ref)
        if self.ack_loss_once:
            self.ack_loss_once = False
            job["status"] = "queued"
            return {"ok": False, "schema_version": 1, "status": "timeout"}
        job["status"] = "delivered"
        token = job["payload"]["plan_token"]
        siblings = [candidate for candidate in self._jobs if candidate["payload"]["plan_token"] == token]
        if all(candidate["status"] == "delivered" for candidate in siblings):
            self._plans[token]["state"] = "completed"
        if self.ack_committed_response_lost_once:
            self.ack_committed_response_lost_once = False
            return {"ok": False, "schema_version": 1, "status": "timeout"}
        return {"ok": True, "schema_version": 1, "status": "acknowledged"}

    def turn_final_fail(self, ref, reason):
        self.fail_calls.append((ref, reason))
        self._leased(ref)["status"] = "queued"
        return {"ok": True, "schema_version": 1, "status": "retry_scheduled"}

    def turn_final_defer(self, ref, reason="", **_kwargs):
        self.defer_calls.append((ref, reason))
        self._leased(ref)["status"] = "queued"
        return {"ok": True, "schema_version": 1, "status": "deferred"}


class MultiTurnFinalTendwire(TurnFinalTendwire):
    def __init__(self, rows):
        known_rows = list(rows)
        super().__init__(known_rows[0])
        self.rows = known_rows
        self.known_rows = known_rows
        self.attention_acked = []
        self._attention_available = False

    def snapshot(self):
        workers = []
        seen = set()
        for row in self.known_rows:
            worker_id = row["worker_id"]
            if worker_id in seen:
                continue
            seen.add(worker_id)
            workers.append(
                _source_worker(
                    {
                        "id": worker_id,
                        "name": worker_id,
                        "status": "idle" if row.get("complete") else "working",
                        "space_id": "space-1",
                        "fingerprint": f"fp-{worker_id}",
                        "meta": {"agent": row.get("agent", "codex")},
                    }
                )
            )
        return {
            "ok": True,
            "workers": workers,
            "spaces": [
                {
                    "id": "space-1",
                    "name": "Project",
                    "status": "active",
                    "fingerprint": "space-fp-1",
                }
            ],
        }

    def turns(self):
        return {
            "ok": True,
            "schema_version": 2,
            "turns": deepcopy(self.rows),
        }

    def install_row_pages(self, row, field, value, cuts):
        previous = self.row
        self.row = row
        try:
            self.install_pages(row["content"]["content_revision"], field, value, cuts)
        finally:
            self.row = previous

    def enable_attention(self):
        self._attention_available = True

    def connector_poll(self, **_kwargs):
        if not self._attention_available:
            return {"ok": True, "items": []}
        return {
            "ok": True,
            "items": [
                {
                    "ref": "twref1.attention",
                    "key": "attention:goal05b",
                    "attempt": 1,
                    "payload": {
                        "event_type": "attention_created",
                        "attention": {
                            "severity": "warning",
                            "reason": "Needs input",
                        },
                    },
                }
            ],
        }

    def connector_ack(self, ref, response, **_kwargs):
        self.attention_acked.append((ref, deepcopy(response)))
        self._attention_available = False
        return {"ok": True}

    def connector_fail(self, _ref, _error, **_kwargs):
        return {"ok": True}


class DeletingTelegram(FakeTelegram):
    def __init__(self, token="fake", shared=None):
        super().__init__(token=token, shared=shared)
        self._shared.setdefault("deleted_messages", [])
        self.deleted_messages = self._shared["deleted_messages"]
        self.raise_after_accept = False

    def with_token(self, token):
        return DeletingTelegram(token=token, shared=self._shared)

    def api(self, method, payload):
        result = super().api(method, payload)
        if method == "sendRichMessage" and self.raise_after_accept:
            self.raise_after_accept = False
            raise RuntimeError("response lost after acceptance")
        return result

    def send_message(self, chat_id, html, **kwargs):
        result = super().send_message(chat_id, html, **kwargs)
        if self.raise_after_accept:
            self.raise_after_accept = False
            raise RuntimeError("response lost after acceptance")
        return result

    def delete_message(self, chat_id, message_id):
        self.deleted_messages.append((str(chat_id), str(message_id), self.token))
        return {"ok": True}


def _runtime(
    tendwire,
    telegram,
    *,
    max_sends=100,
    checkpoint=None,
    after_provider_accept=None,
):
    return SyncRuntime(
        tendwire,
        telegram,
        with_outbox=True,
        max_sends=max_sends,
        checkpoint=checkpoint,
        after_provider_accept=after_provider_accept,
    )


def test_short_inline_stages_and_delivers_without_page_fetch_then_two_syncs_noop(monkeypatch):
    monkeypatch.setenv("HERDRES_TENDWIRE_MODE", "source")
    row = _turn_row("turn-short", "twrev1.short", "short exact final", user="exact prompt")
    tendwire = TurnFinalTendwire(row)
    telegram = DeletingTelegram()
    store = _store()

    first = sync_once(store, _runtime(tendwire, telegram))
    prepare_count = len(tendwire.prepare_calls)
    send_count = len(telegram.sent)
    second = sync_once(store, _runtime(tendwire, telegram))
    third = sync_once(store, _runtime(tendwire, telegram))

    assert first["content_pages"] == 0
    assert first["tendwire_turn_final"]["acked"] == 1
    assert tendwire.page_calls == []
    assert second["tendwire_turn_final"]["polled"] == 0
    assert third["tendwire_turn_final"]["polled"] == 0
    assert len(tendwire.prepare_calls) == prepare_count
    assert len(telegram.sent) == send_count
    entry = next(iter(state.source_worker_entries(store).values()))
    assert entry["last_clean_content_revision"] == "twrev1.short"
    assert entry["last_clean_plan_token"] == "twplan1.plan1"


def test_paged_20k_final_edits_working_then_sends_ordered_bound_parts(monkeypatch):
    monkeypatch.setenv("HERDRES_TENDWIRE_MODE", "source")
    store = _store()
    telegram = DeletingTelegram()
    working = _turn_row("turn-long", "twrev1.working", None)
    working["assistant_stream_text"] = "Working exactly here"
    tendwire = TurnFinalTendwire(working)
    sync_once(store, _runtime(tendwire, telegram, max_sends=1))
    entry = next(iter(state.source_worker_entries(store).values()))
    working_id = entry["last_stream_message_id"]

    final = "## Exact\n\n" + ("- formatted αβ item\n" * 1100) + "TAIL_EXACT_20K"
    row = _turn_row("turn-long", "twrev1.long", final, user="prompt", inline=False)
    tendwire.row = row
    tendwire.install_pages("twrev1.long", "assistant_final_text", final, (7000, 15000))
    tendwire.install_pages("twrev1.long", "user_text", "prompt", ())
    result = sync_once(store, _runtime(tendwire, telegram, max_sends=100))

    assert len(final) > 20_000
    assert result["content_pages"] == 4
    assert result["tendwire_turn_final"]["operations"] == result["tendwire_turn_final"]["acked"]
    assert any(edit[1] == working_id and "Response 1/" in edit[2] for edit in telegram.edited)
    assert "TAIL_EXACT_20K" in "\n".join(sent[1] for sent in telegram.sent)
    entry = next(iter(state.source_worker_entries(store).values()))
    ids = entry["last_clean_message_ids"]
    assert len(ids) > 2
    assert ids[0] == working_id
    assert [state.find_message_binding(store, message_id)["part_ordinal"] for message_id in ids] == list(range(len(ids)))
    for message_id in ids:
        assert herdres._worker_entry_from_reply(store, {"reply_to_message_id": message_id, "topic_id": "77"})[1] is not None


def test_schema_incomplete_and_bad_page_refuse_before_any_telegram_activity(monkeypatch):
    monkeypatch.setenv("HERDRES_TENDWIRE_MODE", "source")
    monkeypatch.setenv("HERDRES_PINNED_STATUS", "0")
    telegram = DeletingTelegram()
    bad_schema = TurnFinalTendwire(_turn_row("turn-bad", "twrev1.bad", "answer"))
    bad_schema.turns = lambda: {"ok": True, "schema_version": 3, "turns": []}
    result = sync_once(_store(), _runtime(bad_schema, telegram))
    assert result["status"] == "unsupported_turn_schema_version"
    assert telegram.sent == [] and telegram.edited == [] and telegram.topics == []

    incomplete_row = _turn_row("turn-incomplete", "twrev1.incomplete", "fragment")
    incomplete_row["content"]["known_incomplete"] = True
    incomplete_row["content"]["fields"]["assistant_final_text"] = {
        "availability": "known_incomplete",
        "inline": False,
        "char_length": len("fragment"),
        "byte_length": len("fragment".encode("utf-8")),
        "page_count": 0,
        "first_cursor": None,
    }
    incomplete_row.pop("assistant_final_text")
    incomplete = TurnFinalTendwire(incomplete_row)
    result = sync_once(_store(), _runtime(incomplete, telegram))
    assert result["ok"] is True
    assert result["turn_content_outcomes"] == {
        "count": 1,
        "truncated": False,
        "items": [
            {
                "turn_id": "turn-incomplete",
                "status": "content_known_incomplete",
                "content_revision": "twrev1.incomplete",
            }
        ],
    }
    assert incomplete.page_calls == []
    assert incomplete.prepare_calls == []
    assert telegram.sent == [] and telegram.edited == []

    value = "α" * 13000
    paged = TurnFinalTendwire(_turn_row("turn-page", "twrev1.page", value, inline=False))
    paged.install_pages("twrev1.page", "assistant_final_text", value, (6000,))
    paged.row["content"]["fields"]["user_text"] = _descriptor(None, inline=False)
    first_cursor = paged.row["content"]["fields"]["assistant_final_text"]["first_cursor"]
    paged.pages[("twrev1.page", "assistant_final_text", first_cursor)]["segment_byte_length"] += 1
    result = sync_once(_store(), _runtime(paged, telegram))
    assert result["ok"] is True
    assert result["turn_content_outcomes"]["items"] == [
        {
            "turn_id": "turn-page",
            "status": "invalid_content_page",
            "content_revision": "twrev1.page",
        }
    ]
    assert paged.prepare_calls == []
    assert telegram.sent == [] and telegram.edited == []


def test_paged_checkpoint_before_ack_loss_resumes_without_fetch_or_resend(monkeypatch):
    monkeypatch.setenv("HERDRES_TENDWIRE_MODE", "source")
    row = _turn_row(
        "turn-ack", "twrev1.ack", "checkpointed answer", inline=False
    )
    tendwire = TurnFinalTendwire(row)
    tendwire.install_pages(
        "twrev1.ack",
        "assistant_final_text",
        "checkpointed answer",
        (),
    )
    tendwire.ack_loss_once = True
    telegram = DeletingTelegram()
    store = _store()
    checkpoints = []

    first = sync_once(store, _runtime(tendwire, telegram, max_sends=1, checkpoint=lambda: checkpoints.append(deepcopy(state.tendwire_turn_jobs(store)))))
    sent_after_first = len(telegram.sent)
    receipt = next(iter(state.tendwire_turn_jobs(store).values()))
    first_ref = tendwire.ack_calls[-1][0]
    page_calls_after_first = list(tendwire.page_calls)
    tendwire.turn_content_get = lambda *_args, **_kwargs: pytest.fail(
        "durable applied receipt retry must not fetch canonical pages"
    )
    second = sync_once(store, _runtime(tendwire, telegram, max_sends=1, checkpoint=lambda: checkpoints.append(deepcopy(state.tendwire_turn_jobs(store)))))

    assert first["tendwire_turn_final"]["operations"] == 1
    assert first["tendwire_turn_final"]["acked"] == 0
    assert receipt["substate"] == "acknowledged"
    receipt_key = next(iter(checkpoints[0]))
    assert checkpoints[0][receipt_key]["substate"] == "reserved"
    assert any(
        snapshot[receipt_key]["substate"] == "telegram_applied"
        for snapshot in checkpoints[1:]
        if receipt_key in snapshot
    )
    assert receipt_key == "turn-final:twplan1.plan1:000000"
    assert tendwire.ack_calls[-1][0] != first_ref
    assert second["tendwire_turn_final"]["operations"] == 0
    assert second["tendwire_turn_final"]["acked"] == 1
    assert tendwire.page_calls == page_calls_after_first
    assert len(telegram.sent) == sent_after_first


def test_paged_committed_ack_loss_finalizes_without_fetch_or_resend(monkeypatch):
    monkeypatch.setenv("HERDRES_TENDWIRE_MODE", "source")
    monkeypatch.setenv("HERDRES_PINNED_STATUS", "0")
    row = _turn_row(
        "turn-committed-ack",
        "twrev1.committedack",
        "durably applied",
        inline=False,
    )
    tendwire = TurnFinalTendwire(row)
    tendwire.install_pages(
        "twrev1.committedack",
        "assistant_final_text",
        "durably applied",
        (),
    )
    tendwire.ack_committed_response_lost_once = True
    telegram = DeletingTelegram()
    store = _store()
    checkpoints = []

    first = sync_once(
        store,
        _runtime(
            tendwire,
            telegram,
            max_sends=1,
            checkpoint=lambda: checkpoints.append(deepcopy(store)),
        ),
    )
    sends = len(telegram.sent)
    page_calls_after_first = list(tendwire.page_calls)
    tendwire.turn_content_get = lambda *_args, **_kwargs: pytest.fail(
        "completed pending plan must finalize without canonical pages"
    )
    second = sync_once(
        store,
        _runtime(
            tendwire,
            telegram,
            max_sends=1,
            checkpoint=lambda: checkpoints.append(deepcopy(store)),
        ),
    )

    assert first["tendwire_turn_final"]["status"] == "timeout"
    assert second["tendwire_turn_final"]["polled"] == 0
    assert len(telegram.sent) == sends
    assert tendwire.page_calls == page_calls_after_first
    entry = next(iter(state.source_worker_entries(store).values()))
    assert entry["last_clean_content_revision"] == "twrev1.committedack"
    assert "pending_plan_token" not in entry
    assert next(iter(state.tendwire_turn_jobs(store).values()))["substate"] == "acknowledged"


def test_revision_growth_shrink_and_wrong_owner_converge_without_surplus(monkeypatch):
    monkeypatch.setenv("HERDRES_TENDWIRE_MODE", "source")
    monkeypatch.setenv("HERDR_TELEGRAM_TOPICS_MANAGED_BOTS", "1")
    store = _store()
    store["telegram"]["managed_bots"] = {"claude": {"enabled": True, "token": "claude-token"}}
    telegram = DeletingTelegram()
    first_text = "A paragraph.\n\n" * 180
    tendwire = TurnFinalTendwire(_turn_row("turn-revise", "twrev1.r1", first_text))
    sync_once(store, _runtime(tendwire, telegram, max_sends=100))
    entry = next(iter(state.source_worker_entries(store).values()))
    first_count = len(entry["last_clean_message_ids"])
    assert first_count > 1

    growth = "B changed.\n\n" * 600
    tendwire.row = _turn_row("turn-revise", "twrev1.r2", growth)
    grow_result = sync_once(store, _runtime(tendwire, telegram, max_sends=100))
    grown_ids = list(entry["last_clean_message_ids"])
    assert len(grown_ids) > first_count
    assert grow_result["tendwire_turn_final"]["acked"] == len(grown_ids)

    old_zero = grown_ids[0]
    state.message_bindings(store)[old_zero]["bot_kind"] = "claude"
    state.message_bindings(store)[old_zero]["topic_id"] = "wrong-topic"
    shrink = "C final compact"
    tendwire.row = _turn_row("turn-revise", "twrev1.r3", shrink)
    shrink_result = sync_once(store, _runtime(tendwire, telegram, max_sends=100))
    current_ids = entry["last_clean_message_ids"]

    assert len(current_ids) == 1
    assert current_ids[0] != old_zero
    assert len(telegram.deleted_messages) >= len(grown_ids)
    assert shrink_result["tendwire_turn_final"]["acked"] == 1 + len(grown_ids) - 1
    assert all(state.find_message_binding(store, message_id) is None for message_id in grown_ids)
    assert state.find_message_binding(store, current_ids[0])["content_revision"] == "twrev1.r3"


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("index", 1),
        ("count", 3),
        ("segment_char_length", 1),
        ("content_revision", "twrev1.other"),
        ("next_cursor", "__cycle__"),
    ],
)
def test_page_identity_order_length_and_cursor_fail_closed(monkeypatch, field, value):
    monkeypatch.setenv("HERDRES_TENDWIRE_MODE", "source")
    monkeypatch.setenv("HERDRES_PINNED_STATUS", "0")
    text = "αβ" * 7000
    row = _turn_row("turn-invalid-page", "twrev1.invalidpage", text, inline=False)
    tendwire = TurnFinalTendwire(row)
    tendwire.install_pages("twrev1.invalidpage", "assistant_final_text", text, (6000,))
    row["content"]["fields"]["user_text"] = _descriptor(None, inline=False)
    cursor = row["content"]["fields"]["assistant_final_text"]["first_cursor"]
    tendwire.pages[("twrev1.invalidpage", "assistant_final_text", cursor)][field] = cursor if value == "__cycle__" else value
    telegram = DeletingTelegram()

    result = sync_once(_store(), _runtime(tendwire, telegram))

    assert result["ok"] is True
    assert result["turn_content_outcomes"]["items"] == [
        {
            "turn_id": "turn-invalid-page",
            "status": "invalid_content_page",
            "content_revision": "twrev1.invalidpage",
        }
    ]
    assert telegram.sent == [] and telegram.edited == []
    assert tendwire.prepare_calls == []


def test_revision_conflict_relists_and_never_mixes_page_generations(monkeypatch):
    monkeypatch.setenv("HERDRES_TENDWIRE_MODE", "source")
    monkeypatch.setenv("HERDRES_PINNED_STATUS", "0")
    old = "old " * 4000
    new = "new " * 4000 + "NEW_TAIL"
    tendwire = TurnFinalTendwire(_turn_row("turn-relist", "twrev1.old", old, inline=False))
    tendwire.install_pages("twrev1.old", "assistant_final_text", old, (8000,))
    tendwire.row["content"]["fields"]["user_text"] = _descriptor(None, inline=False)
    first_cursor = tendwire.row["content"]["fields"]["assistant_final_text"]["first_cursor"]
    tendwire.pages[("twrev1.old", "assistant_final_text", first_cursor)] = {
        "ok": False,
        "status": "revision_conflict",
        "error": "authoritative revision changed",
    }
    old_row = deepcopy(tendwire.row)
    new_row = _turn_row("turn-relist", "twrev1.new", new, inline=False)
    tendwire.row = new_row
    tendwire.install_pages("twrev1.new", "assistant_final_text", new, (7000, 13000))
    tendwire.row["content"]["fields"]["user_text"] = _descriptor(None, inline=False)
    listed = 0

    def turns():
        nonlocal listed
        listed += 1
        return {
            "ok": True,
            "schema_version": 2,
            "turns": [deepcopy(old_row if listed == 1 else tendwire.row)],
        }

    tendwire.turns = turns
    telegram = DeletingTelegram()
    result = sync_once(_store(), _runtime(tendwire, telegram, max_sends=100))

    assert listed == 2
    assert result["ok"] is True
    assert result["content_pages"] == 3
    assert "NEW_TAIL" in "\n".join(message[1] for message in telegram.sent)
    assert all("old old old" not in message[1] for message in telegram.sent)


class FailBeforeThirdTelegram(DeletingTelegram):
    def __init__(self):
        super().__init__()
        self.part_attempts = []
        self.failed = False

    def send_message(self, chat_id, html, **kwargs):
        self.part_attempts.append(html)
        if len(self.part_attempts) == 3 and not self.failed:
            self.failed = True
            return {"ok": False, "kind": "transient", "error": "failed before acceptance"}
        return FakeTelegram.send_message(self, chat_id, html, **kwargs)


def test_failure_before_part_n_retries_only_n_and_preserves_prefix(monkeypatch):
    monkeypatch.setenv("HERDRES_TENDWIRE_MODE", "source")
    monkeypatch.setenv("HERDRES_PINNED_STATUS", "0")
    monkeypatch.setenv("HERDR_TELEGRAM_TOPICS_RICH_MESSAGES", "0")
    text = "ordered response paragraph\n\n" * 450
    tendwire = TurnFinalTendwire(_turn_row("turn-retry", "twrev1.retry", text))
    telegram = FailBeforeThirdTelegram()
    store = _store()

    first = sync_once(store, _runtime(tendwire, telegram, max_sends=100))
    second = sync_once(store, _runtime(tendwire, telegram, max_sends=100))

    assert first["tendwire_turn_final"]["failed"] == 0
    assert first["tendwire_turn_final"]["deferred"] == 1
    assert first["tendwire_turn_final"]["uncertain"] == 0
    assert second["tendwire_turn_final"]["failed"] == 0
    assert telegram.part_attempts[0] != telegram.part_attempts[1]
    assert telegram.part_attempts[2] == telegram.part_attempts[3]
    assert telegram.part_attempts.count(telegram.part_attempts[0]) == 1
    assert telegram.part_attempts.count(telegram.part_attempts[1]) == 1
    entry = next(iter(state.source_worker_entries(store).values()))
    assert len(entry["last_clean_message_ids"]) == len(tendwire._plans["twplan1.plan1"]["parts"])


class LegacyErrorTelegram(TelegramClient):
    def api(self, _method, _payload):
        if self.token == "missing":
            raise TelegramError("Bad Request: message to edit not found")
        raise RateLimited(7, "Too Many Requests: retry after 7")


def test_legacy_telegram_primitives_preserve_not_found_and_rate_limit():
    missing = LegacyErrorTelegram(token="missing")
    result = missing.edit_message("-100", "501", "replacement")
    assert result["ok"] is False
    assert result["kind"] == "not_found"
    assert result["not_found"] is True

    limited = LegacyErrorTelegram(token="limited")
    with pytest.raises(RateLimited) as send_error:
        limited.send_message("-100", "one bounded message")
    assert send_error.value.retry_after == 7
    with pytest.raises(RateLimited):
        limited.edit_message("-100", "501", "replacement")


class MissingEditTelegram(DeletingTelegram):
    def __init__(self):
        super().__init__()
        self.edit_attempts = 0

    def edit_message(self, chat_id, message_id, html):
        self.edit_attempts += 1
        return LegacyErrorTelegram(token="missing").edit_message(
            chat_id,
            message_id,
            html,
        )


def test_not_found_edit_at_budget_boundary_retries_as_send_not_stale_edit(monkeypatch):
    monkeypatch.setenv("HERDRES_TENDWIRE_MODE", "source")
    monkeypatch.setenv("HERDRES_PINNED_STATUS", "0")
    monkeypatch.setenv("HERDR_TELEGRAM_TOPICS_RICH_MESSAGES", "0")
    tendwire = TurnFinalTendwire(
        _turn_row("turn-missing-edit", "twrev1.before", "before")
    )
    telegram = MissingEditTelegram()
    store = _store()
    sync_once(store, _runtime(tendwire, telegram, max_sends=1))
    sent_before = len(telegram.sent)
    tendwire.row = _turn_row(
        "turn-missing-edit",
        "twrev1.after",
        "after",
    )

    missing = sync_once(store, _runtime(tendwire, telegram, max_sends=1))
    resumed = sync_once(store, _runtime(tendwire, telegram, max_sends=1))

    assert missing["tendwire_turn_final"]["operations"] == 1
    assert missing["tendwire_turn_final"]["deferred"] == 1
    assert missing["tendwire_turn_final"]["failed"] == 0
    assert missing["tendwire_turn_final"]["uncertain"] == 0
    assert len(telegram.sent) == sent_before + 1
    assert telegram.edit_attempts == 1
    assert resumed["tendwire_turn_final"]["operations"] == 1
    assert resumed["tendwire_turn_final"]["acked"] == 1


class RateLimitedOnceTelegram(DeletingTelegram):
    def __init__(self):
        super().__init__()
        self.rate_limited = False

    def send_message(self, chat_id, html, **kwargs):
        if not self.rate_limited:
            self.rate_limited = True
            raise RateLimited(7, "retry later")
        return FakeTelegram.send_message(self, chat_id, html, **kwargs)


def test_rate_limit_defers_without_failure_or_uncertainty(monkeypatch):
    monkeypatch.setenv("HERDRES_TENDWIRE_MODE", "source")
    monkeypatch.setenv("HERDRES_PINNED_STATUS", "0")
    monkeypatch.setenv("HERDR_TELEGRAM_TOPICS_RICH_MESSAGES", "0")
    tendwire = TurnFinalTendwire(
        _turn_row("turn-rate-limit", "twrev1.ratelimit", "answer")
    )
    telegram = RateLimitedOnceTelegram()
    store = _store()

    limited = sync_once(store, _runtime(tendwire, telegram, max_sends=1))
    resumed = sync_once(store, _runtime(tendwire, telegram, max_sends=1))

    assert limited["tendwire_turn_final"]["operations"] == 1
    assert limited["tendwire_turn_final"]["deferred"] == 1
    assert limited["tendwire_turn_final"]["failed"] == 0
    assert limited["tendwire_turn_final"]["uncertain"] == 0
    assert tendwire.fail_calls == []
    assert resumed["tendwire_turn_final"]["acked"] == 1


def test_physical_budget_stops_before_next_lease_and_acceptance_loss_is_explicit(monkeypatch):
    monkeypatch.setenv("HERDRES_TENDWIRE_MODE", "source")
    monkeypatch.setenv("HERDRES_PINNED_STATUS", "0")
    long_text = "bounded part\n\n" * 500
    tendwire = TurnFinalTendwire(_turn_row("turn-budget", "twrev1.budget", long_text))
    telegram = DeletingTelegram()
    result = sync_once(_store(), _runtime(tendwire, telegram, max_sends=1))
    assert result["tendwire_turn_final"]["operations"] == 1
    assert result["tendwire_turn_final"]["polled"] == 1

    uncertain_wire = TurnFinalTendwire(_turn_row("turn-uncertain", "twrev1.uncertain", "one message"))
    uncertain_telegram = DeletingTelegram()
    uncertain_telegram.raise_after_accept = True
    uncertain = sync_once(_store(), _runtime(uncertain_wire, uncertain_telegram, max_sends=1))
    assert uncertain["tendwire_turn_final"]["status"] == "delivery_uncertain"
    assert uncertain["tendwire_turn_final"]["uncertain"] == 1
    assert len(uncertain_telegram.sent) == 1
    assert "delivery_uncertain" in uncertain_wire.fail_calls[-1][1]


def test_incomplete_row_isolated_while_working_final_pins_and_attention_continue(monkeypatch):
    monkeypatch.setenv("HERDRES_PINNED_STATUS", "1")
    monkeypatch.setenv("HERDRES_PINNED_ACCOUNT", "1")
    monkeypatch.setenv("HERDRES_TENDWIRE_MODE", "source")
    monkeypatch.setattr(
        "herdres_connector.source_sync.accounts.account_line",
        lambda kind, *, snapshot=None: f"{kind.title()} account: active",
    )
    monkeypatch.setattr(
        "herdres_connector.source_sync.accounts.usage_snapshot",
        lambda: {},
    )
    incomplete = _mark_known_incomplete(
        _turn_row("turn-incomplete", "twrev1.incomplete", "fragment"),
        "fragment",
    )
    working = _turn_row("turn-working", "twrev1.working", None)
    working["worker_id"] = "worker-2"
    working["assistant_stream_text"] = "Unrelated work continues"
    final_text = ("eligible final αβ\n\n" * 900) + "ELIGIBLE_TAIL"
    final = _turn_row(
        "turn-final",
        "twrev1.eligible",
        final_text,
        inline=False,
    )
    final["worker_id"] = "worker-3"
    tendwire = MultiTurnFinalTendwire([incomplete, working, final])
    tendwire.install_row_pages(final, "assistant_final_text", final_text, (7000,))
    tendwire.enable_attention()
    telegram = DeletingTelegram()

    result = sync_once(_store(), _runtime(tendwire, telegram, max_sends=100))

    assert result["ok"] is True
    assert result["content_pages"] == 2
    assert result["turn_content_outcomes"]["items"] == [
        {
            "turn_id": "turn-incomplete",
            "status": "content_known_incomplete",
            "content_revision": "twrev1.incomplete",
        }
    ]
    assert {call[1] for call in tendwire.page_calls} == {"twrev1.eligible"}
    assert [call for call in tendwire.prepare_calls if call[0] == "begin"] == [
        ("begin", "twrev1.eligible", len(tendwire._plans["twplan1.plan1"]["parts"]))
    ]
    assert tendwire.ack_calls
    assert tendwire.attention_acked == [
        ("twref1.attention", {"telegram": "delivered"})
    ]
    rendered = "\n".join(message[1] for message in telegram.sent)
    assert "Unrelated work continues" in rendered
    assert "ELIGIBLE_TAIL" in rendered
    assert "account: active" in rendered


def test_incomplete_revision_later_completes_once_then_forced_syncs_are_lazy(monkeypatch):
    monkeypatch.setenv("HERDRES_TENDWIRE_MODE", "source")
    monkeypatch.setenv("HERDRES_PINNED_STATUS", "0")
    incomplete = _mark_known_incomplete(
        _turn_row("turn-repair", "twrev1.incomplete", "fragment"),
        "fragment",
    )
    tendwire = MultiTurnFinalTendwire([incomplete])
    telegram = DeletingTelegram()
    store = _store()

    first = sync_once(store, _runtime(tendwire, telegram, max_sends=100))
    complete_text = ("authoritative repaired final\n\n" * 600) + "REPAIRED_TAIL"
    complete = _turn_row(
        "turn-repair",
        "twrev1.complete",
        complete_text,
        inline=False,
    )
    tendwire.rows = tendwire.known_rows = [complete]
    tendwire.install_row_pages(complete, "assistant_final_text", complete_text, (6000,))
    repaired = sync_once(store, _runtime(tendwire, telegram, max_sends=100))
    pages_after_repair = list(tendwire.page_calls)
    prepares_after_repair = list(tendwire.prepare_calls)
    sends_after_repair = len(telegram.sent)
    edits_after_repair = len(telegram.edited)
    second_noop = sync_once(store, _runtime(tendwire, telegram, max_sends=100))
    third_noop = sync_once(store, _runtime(tendwire, telegram, max_sends=100))

    assert first["turn_content_outcomes"]["count"] == 1
    assert repaired["content_pages"] == 2
    assert repaired["tendwire_turn_final"]["acked"] > 0
    assert {call[1] for call in pages_after_repair} == {"twrev1.complete"}
    assert tendwire.page_calls == pages_after_repair
    assert tendwire.prepare_calls == prepares_after_repair
    assert len(telegram.sent) == sends_after_repair
    assert len(telegram.edited) == edits_after_repair
    assert second_noop["content_pages"] == third_noop["content_pages"] == 0
    assert "REPAIRED_TAIL" in "\n".join(message[1] for message in telegram.sent)


def test_delivered_paged_revision_and_historical_rows_make_no_extra_page_calls(monkeypatch):
    monkeypatch.setenv("HERDRES_TENDWIRE_MODE", "source")
    monkeypatch.setenv("HERDRES_PINNED_STATUS", "0")
    current_text = ("current long final\n\n" * 700) + "CURRENT_TAIL"
    current = _turn_row(
        "turn-current",
        "twrev1.current",
        current_text,
        inline=False,
    )
    historical = [
        _turn_row(
            f"turn-history-{index}",
            f"twrev1.history{index}",
            ("historical long final\n\n" * 700) + str(index),
            inline=False,
        )
        for index in range(8)
    ]
    tendwire = MultiTurnFinalTendwire([current, *historical])
    tendwire.install_row_pages(current, "assistant_final_text", current_text, (6500,))
    for row in historical:
        value = ("historical long final\n\n" * 700) + row["id"].rsplit("-", 1)[-1]
        tendwire.install_row_pages(row, "assistant_final_text", value, (6500,))
    telegram = DeletingTelegram()
    store = _store()

    first = sync_once(store, _runtime(tendwire, telegram, max_sends=100))
    calls_after_first = list(tendwire.page_calls)
    prepare_count = len(tendwire.prepare_calls)
    sends = len(telegram.sent)
    edits = len(telegram.edited)
    second = sync_once(store, _runtime(tendwire, telegram, max_sends=100))
    third = sync_once(store, _runtime(tendwire, telegram, max_sends=100))

    assert first["content_pages"] == 2
    assert {call[1] for call in calls_after_first} == {"twrev1.current"}
    assert second["content_pages"] == third["content_pages"] == 0
    assert tendwire.page_calls == calls_after_first
    assert len(tendwire.prepare_calls) == prepare_count
    assert len(telegram.sent) == sends
    assert len(telegram.edited) == edits


def test_unroutable_and_quarantined_long_finals_are_never_paged(monkeypatch):
    monkeypatch.setenv("HERDRES_TENDWIRE_MODE", "source")
    monkeypatch.setenv("HERDRES_PINNED_STATUS", "0")
    eligible = _turn_row("turn-working", "twrev1.working", None)
    eligible["assistant_stream_text"] = "working"
    unroutable_text = "unroutable\n" * 2000
    unroutable = _turn_row(
        "turn-unroutable",
        "twrev1.unroutable",
        unroutable_text,
        inline=False,
    )
    unroutable["worker_id"] = "worker-missing"
    tendwire = MultiTurnFinalTendwire([eligible, unroutable])
    tendwire.known_rows = [eligible]
    tendwire.install_row_pages(
        unroutable,
        "assistant_final_text",
        unroutable_text,
        (7000,),
    )
    telegram = DeletingTelegram()
    store = _store()

    sync_once(store, _runtime(tendwire, telegram, max_sends=100))
    assert tendwire.page_calls == []
    assert tendwire.prepare_calls == []

    worker_entry = next(iter(state.source_worker_entries(store).values()))
    worker_entry["stable_key_quarantined"] = True
    worker_entry["stable_key_quarantine_reason"] = "test"
    quarantined_text = "quarantined\n" * 2000
    quarantined = _turn_row(
        "turn-quarantined",
        "twrev1.quarantined",
        quarantined_text,
        inline=False,
    )
    tendwire.rows = tendwire.known_rows = [quarantined]
    tendwire.install_row_pages(
        quarantined,
        "assistant_final_text",
        quarantined_text,
        (7000,),
    )
    sync_once(store, _runtime(tendwire, telegram, max_sends=100))

    assert tendwire.page_calls == []
    assert tendwire.prepare_calls == []


class ExhaustedRecoveryTendwire(TurnFinalTendwire):
    def __init__(self, row):
        super().__init__(row)
        self.recover_calls = []
        self._recoveries = {}

    def turn_final_fail(self, ref, reason):
        self.fail_calls.append((ref, reason))
        job = self._leased(ref)
        job["status"] = "dead_letter"
        self._plans[job["payload"]["plan_token"]]["state"] = "failed"
        return {
            "ok": True,
            "schema_version": 1,
            "status": "attempts_exhausted",
        }

    def connector_prepare_recover(self, *, failed_plan_token, request_id):
        self.recover_calls.append((failed_plan_token, request_id))
        prior = self._recoveries.get(request_id)
        if prior is not None:
            replay = deepcopy(prior)
            replay["idempotent_replay"] = True
            return replay
        failed = self._plans[failed_plan_token]
        failed_jobs = [
            job
            for job in self._jobs
            if job["payload"]["plan_token"] == failed_plan_token
        ]
        prefix_count = 0
        for job in sorted(
            failed_jobs,
            key=lambda item: item["payload"]["sequence_index"],
        ):
            if job["status"] != "delivered":
                break
            prefix_count += 1
        token = f"twplan1.plan{len(self._plans) + 1}"
        replacement = {
            "state": "active",
            "turn_id": failed["turn_id"],
            "revision": failed["revision"],
            "part_count": failed["part_count"],
            "parts": deepcopy(failed["parts"]),
            "replaces": failed_plan_token,
        }
        self._plans[token] = replacement
        self._plan_by_revision[failed["revision"]] = token
        for sequence in range(prefix_count, failed["part_count"]):
            recovered_job = self._job(
                token,
                sequence,
                "upsert",
                sequence,
                failed["part_count"],
                replacement["parts"][sequence],
                failed_plan_token,
            )
            if prefix_count:
                recovered_job["payload"]["predecessor_job_key"] = (
                    f"turn-final:{failed_plan_token}:{prefix_count - 1:06d}"
                )
            self._jobs.append(recovered_job)
        self._active_plan = token
        response = {
            "schema_version": 1,
            "ok": True,
            "status": "recovered",
            "failed_plan_token": failed_plan_token,
            "plan_token": token,
            "generation": 2,
            "content_revision": failed["revision"],
            "state": "active",
            "acknowledged_prefix_count": prefix_count,
            "executable_job_count": failed["part_count"] - prefix_count,
            "retained_failed_job_count": 1,
            "prior_attempt_count": 3,
            "idempotent_replay": False,
        }
        self._recoveries[request_id] = deepcopy(response)
        return response


class FailSecondPartOnceTelegram(DeletingTelegram):
    def __init__(self):
        super().__init__()
        self.final_attempts = []
        self.failed_once = False

    def send_message(self, chat_id, html, **kwargs):
        self.final_attempts.append(html)
        if len(self.final_attempts) == 2 and not self.failed_once:
            self.failed_once = True
            return {
                "ok": False,
                "kind": "permanent",
                "error": "provider rejected bounded part",
            }
        return FakeTelegram.send_message(self, chat_id, html, **kwargs)


def test_explicit_failed_plan_recovery_clones_prefix_and_never_replays_telegram(
    monkeypatch,
    capsys,
):
    monkeypatch.setenv("HERDRES_TENDWIRE_MODE", "source")
    monkeypatch.setenv("HERDRES_PINNED_STATUS", "0")
    monkeypatch.setenv("HERDR_TELEGRAM_TOPICS_RICH_MESSAGES", "0")
    text = ("immutable recovery response\n\n" * 500) + "RECOVERY_TAIL"
    tendwire = ExhaustedRecoveryTendwire(
        _turn_row("turn-recovery", "twrev1.recovery", text)
    )
    telegram = FailSecondPartOnceTelegram()
    store = _store()

    exhausted = sync_once(store, _runtime(tendwire, telegram, max_sends=100))
    attempts_after_failure = list(telegram.final_attempts)
    no_spin = sync_once(store, _runtime(tendwire, telegram, max_sends=100))
    old_jobs_before = deepcopy(state.tendwire_turn_jobs(store))
    failed_token = next(iter(tendwire._plans))

    assert exhausted["tendwire_turn_final"]["status"] == "attempts_exhausted"
    assert no_spin["tendwire_turn_final"]["polled"] == 0
    assert telegram.final_attempts == attempts_after_failure
    assert [
        receipt["substate"]
        for receipt in old_jobs_before.values()
    ] == ["acknowledged", "failed"]

    monkeypatch.setattr(herdres.config, "load_env_file", lambda: None)
    monkeypatch.setattr(herdres.config, "require_source_mode", lambda: None)
    monkeypatch.setattr(herdres.state, "state_lock", lambda: nullcontext())
    monkeypatch.setattr(herdres.state, "load_state", lambda: store)

    def save_candidate(candidate):
        saved = deepcopy(candidate)
        store.clear()
        store.update(saved)

    monkeypatch.setattr(herdres.state, "save_state", save_candidate)
    monkeypatch.setattr(herdres, "TendwireClient", lambda: tendwire)

    code = herdres.cmd_recover_turn_final(
        SimpleNamespace(
            plan_token=failed_token,
            request_id="operator-recovery-1",
        )
    )
    output = json.loads(capsys.readouterr().out)
    new_token = output["plan_token"]

    assert code == 0
    assert output == {
        "acknowledged_prefix_count": 1,
        "content_revision": "twrev1.recovery",
        "executable_job_count": len(tendwire._plans[new_token]["parts"]) - 1,
        "failed_plan_token": failed_token,
        "generation": 2,
        "idempotent_replay": False,
        "ok": True,
        "plan_token": new_token,
        "prior_attempt_count": 3,
        "retained_failed_job_count": 1,
        "schema_version": 1,
        "state": "active",
        "status": "recovered",
    }
    assert old_jobs_before == {
        key: state.tendwire_turn_jobs(store)[key]
        for key in old_jobs_before
    }
    new_prefix = state.tendwire_turn_jobs(store)[
        f"turn-final:{new_token}:000000"
    ]
    assert new_prefix["substate"] == "acknowledged"
    assert new_prefix["telegram_message_id"] == old_jobs_before[
        f"turn-final:{failed_token}:000000"
    ]["telegram_message_id"]
    prefix_binding = state.find_message_binding(
        store,
        new_prefix["telegram_message_id"],
    )
    assert prefix_binding["plan_token"] == new_token
    assert prefix_binding["tendwire_job_key"] == (
        f"turn-final:{new_token}:000000"
    )

    resumed = sync_once(store, _runtime(tendwire, telegram, max_sends=100))

    assert resumed["tendwire_turn_final"]["acked"] == (
        len(tendwire._plans[new_token]["parts"]) - 1
    )
    assert telegram.final_attempts.count(attempts_after_failure[0]) == 1
    assert "RECOVERY_TAIL" in "\n".join(telegram.final_attempts)
    entry = next(iter(state.source_worker_entries(store).values()))
    assert entry["last_clean_plan_token"] == new_token
    assert entry["last_clean_content_revision"] == "twrev1.recovery"

    replay_code = herdres.cmd_recover_turn_final(
        SimpleNamespace(
            plan_token=failed_token,
            request_id="operator-recovery-1",
        )
    )
    replay = json.loads(capsys.readouterr().out)
    assert replay_code == 0
    assert replay["plan_token"] == new_token
    assert replay["idempotent_replay"] is True
    assert tendwire.recover_calls == [
        (failed_token, "operator-recovery-1"),
        (failed_token, "operator-recovery-1"),
    ]


@pytest.mark.parametrize(
    ("receipt_substate", "expected_status"),
    [
        ("reserved", "recovery_receipt_uncertain"),
        ("telegram_applied", "recovery_receipt_inflight"),
        ("old_slot_retired", "recovery_receipt_inflight"),
    ],
)
def test_recovery_preflight_rejects_uncertain_or_inflight_without_rpc(
    monkeypatch,
    capsys,
    receipt_substate,
    expected_status,
):
    store = _store()
    worker = {
        "source": "tendwire",
        "entry_type": "worker",
        "status": "idle",
        "tendwire_worker_id": "worker-1",
        "tendwire_stable_key": "wsk1_" + ("a" * 64),
        "tendwire_stable_key_version": 1,
        "pending_plan_token": "twplan1.failed",
        "pending_content_revision": "twrev1.recovery",
        "pending_turn_part_count": 1,
        "pending_turn_job_count": 1,
    }
    store["panes"]["worker"] = worker
    receipt = state.reserve_tendwire_turn_job(
        store,
        "turn-final:twplan1.failed:000000",
        plan_token="twplan1.failed",
        content_revision="twrev1.recovery",
        operation="upsert",
        sequence_index=0,
        part_ordinal=0,
        part_count=1,
        telegram_message_id="501" if receipt_substate != "reserved" else "",
        prior_message_id="500" if receipt_substate == "old_slot_retired" else "",
        bot_kind="manager",
    )
    if receipt_substate != "reserved":
        state.update_tendwire_turn_job(
            store,
            "turn-final:twplan1.failed:000000",
            substate="telegram_applied",
            telegram_message_id="501",
        )
    if receipt_substate == "old_slot_retired":
        state.update_tendwire_turn_job(
            store,
            "turn-final:twplan1.failed:000000",
            substate="old_slot_retired",
        )
    calls = []

    class NeverCalled:
        def connector_prepare_recover(self, **kwargs):
            calls.append(kwargs)
            raise AssertionError("invalid local receipt must stop before RPC")

    monkeypatch.setattr(herdres.config, "load_env_file", lambda: None)
    monkeypatch.setattr(herdres.config, "require_source_mode", lambda: None)
    monkeypatch.setattr(herdres.state, "state_lock", lambda: nullcontext())
    monkeypatch.setattr(herdres.state, "load_state", lambda: store)
    monkeypatch.setattr(herdres, "TendwireClient", NeverCalled)

    code = herdres.cmd_recover_turn_final(
        SimpleNamespace(
            plan_token="twplan1.failed",
            request_id="operator-reject-1",
        )
    )
    output = json.loads(capsys.readouterr().out)

    assert code == 1
    assert output["status"] == expected_status
    assert calls == []
    assert receipt["substate"] == receipt_substate


def test_recovery_preflight_rejects_capacity_route_and_completed_plan_states(
    monkeypatch,
):
    worker = {
        "source": "tendwire",
        "entry_type": "worker",
        "status": "idle",
        "tendwire_worker_id": "worker-1",
        "tendwire_stable_key": "wsk1_" + ("b" * 64),
        "tendwire_stable_key_version": 1,
        "pending_plan_token": "twplan1.failed",
        "pending_content_revision": "twrev1.recovery",
        "pending_turn_part_count": 1,
        "pending_turn_job_count": 2,
    }
    store = _store()
    store["panes"]["worker"] = worker
    monkeypatch.setattr(state, "TENDWIRE_TURN_JOB_LIMIT", 1)

    capacity = herdres._turn_final_recovery_preflight(
        store,
        "twplan1.failed",
        "operator-capacity-1",
    )
    assert capacity["status"] == "recovery_capacity_exceeded"

    monkeypatch.setattr(state, "TENDWIRE_TURN_JOB_LIMIT", 20_001)
    worker["stable_key_quarantined"] = True
    quarantined = herdres._turn_final_recovery_preflight(
        store,
        "twplan1.failed",
        "operator-quarantine-1",
    )
    assert quarantined["status"] == "recovery_route_ambiguous"

    worker.pop("stable_key_quarantined")
    worker.pop("pending_plan_token")
    completed = herdres._turn_final_recovery_preflight(
        store,
        "twplan1.failed",
        "operator-complete-1",
    )
    assert completed["status"] == "recovery_plan_not_found"


def test_recovery_response_rejects_wrong_revision_and_cli_parser_is_one_shot():
    response = {
        "schema_version": 1,
        "ok": True,
        "status": "recovered",
        "failed_plan_token": "twplan1.failed",
        "plan_token": "twplan1.replacement",
        "generation": 2,
        "content_revision": "twrev1.wrong",
        "state": "active",
        "acknowledged_prefix_count": 1,
        "executable_job_count": 1,
        "retained_failed_job_count": 1,
        "prior_attempt_count": 3,
        "idempotent_replay": False,
    }

    invalid = herdres._validate_recovery_response(
        response,
        failed_plan_token="twplan1.failed",
        content_revision="twrev1.expected",
        acknowledged_prefix_count=1,
        expected_job_count=2,
        expected_generation=2,
        retained_failed_job_count=1,
    )
    args = herdres.build_parser().parse_args(
        [
            "tendwire",
            "recover-turn-final",
            "--plan-token",
            "twplan1.failed",
            "--request-id",
            "operator-1",
        ]
    )

    assert invalid["status"] == "recovery_state_uncertain"
    assert args.func is herdres.cmd_recover_turn_final
    assert args.plan_token == "twplan1.failed"
    assert args.request_id == "operator-1"


def test_provider_acceptance_crash_persists_reserved_and_restart_never_resends(
    monkeypatch,
):
    monkeypatch.setenv("HERDRES_TENDWIRE_MODE", "source")
    monkeypatch.setenv("HERDRES_PINNED_STATUS", "0")
    row = _turn_row(
        "turn-provider-crash",
        "twrev1.providercrash",
        "accepted before crash",
    )
    tendwire = TurnFinalTendwire(row)
    telegram = DeletingTelegram()
    store = _store()
    checkpoints = []

    def crash_after_accept():
        raise RuntimeError("deterministic crash after provider acceptance")

    with pytest.raises(
        RuntimeError,
        match="deterministic crash after provider acceptance",
    ):
        sync_once(
            store,
            _runtime(
                tendwire,
                telegram,
                max_sends=1,
                checkpoint=lambda: checkpoints.append(
                    deepcopy(state.tendwire_turn_jobs(store))
                ),
                after_provider_accept=crash_after_accept,
            ),
        )

    assert len(telegram.sent) == 1
    assert checkpoints
    job_key = "turn-final:twplan1.plan1:000000"
    assert checkpoints[-1][job_key]["substate"] == "reserved"
    assert state.tendwire_turn_jobs(store)[job_key]["substate"] == "reserved"

    # Simulate Tendwire lease expiry/requeue after the process crash.
    tendwire._jobs[0]["status"] = "queued"
    sends_after_crash = len(telegram.sent)
    restarted = sync_once(
        store,
        _runtime(tendwire, telegram, max_sends=1),
    )

    assert restarted["tendwire_turn_final"]["status"] == "delivery_uncertain"
    assert restarted["tendwire_turn_final"]["operations"] == 0
    assert restarted["tendwire_turn_final"]["uncertain"] == 1
    assert len(telegram.sent) == sends_after_crash
    assert state.tendwire_turn_jobs(store)[job_key]["substate"] == "reserved"
    preflight = herdres._turn_final_recovery_preflight(
        store,
        "twplan1.plan1",
        "operator-provider-crash-1",
    )
    assert preflight["status"] == "recovery_receipt_uncertain"


def _manual_recovery_worker(failed_plan_token="twplan1.failed"):
    return {
        "source": "tendwire",
        "entry_type": "worker",
        "status": "idle",
        "tendwire_worker_id": "worker-1",
        "tendwire_stable_key": "wsk1_" + ("c" * 64),
        "tendwire_stable_key_version": 1,
        "pending_plan_token": failed_plan_token,
        "pending_content_revision": "twrev1.recovery",
        "pending_turn_part_count": 1,
        "pending_turn_job_count": 1,
        "pending_plan_generation": 1,
    }


def _recovery_response(
    failed_plan_token="twplan1.failed",
    plan_token="twplan1.replacement",
    **updates,
):
    response = {
        "schema_version": 1,
        "ok": True,
        "status": "recovered",
        "failed_plan_token": failed_plan_token,
        "plan_token": plan_token,
        "generation": 2,
        "content_revision": "twrev1.recovery",
        "state": "active",
        "acknowledged_prefix_count": 0,
        "executable_job_count": 1,
        "retained_failed_job_count": 1,
        "prior_attempt_count": 3,
        "idempotent_replay": False,
    }
    response.update(updates)
    return response


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("operation", "unknown"),
        ("part_ordinal", 1),
        ("part_count", 0),
        ("telegram_message_id", ""),
    ],
)
def test_malformed_acknowledged_prefix_stops_before_recovery_rpc(
    monkeypatch,
    capsys,
    field,
    value,
):
    store = _store()
    worker = _manual_recovery_worker()
    store["panes"]["worker"] = worker
    key = "turn-final:twplan1.failed:000000"
    receipt = state.reserve_tendwire_turn_job(
        store,
        key,
        plan_token="twplan1.failed",
        content_revision="twrev1.recovery",
        operation="upsert",
        sequence_index=0,
        part_ordinal=0,
        part_count=1,
        telegram_message_id="501",
        bot_kind="manager",
    )
    state.update_tendwire_turn_job(
        store,
        key,
        substate="telegram_applied",
        telegram_message_id="501",
    )
    state.update_tendwire_turn_job(
        store,
        key,
        substate="acknowledged",
    )
    receipt[field] = value
    calls = []

    class NeverCalled:
        def connector_prepare_recover(self, **kwargs):
            calls.append(kwargs)
            raise AssertionError("malformed prefix must stop before RPC")

    monkeypatch.setattr(herdres.config, "load_env_file", lambda: None)
    monkeypatch.setattr(herdres.config, "require_source_mode", lambda: None)
    monkeypatch.setattr(herdres.state, "state_lock", lambda: nullcontext())
    monkeypatch.setattr(herdres.state, "load_state", lambda: store)
    monkeypatch.setattr(herdres, "TendwireClient", NeverCalled)

    code = herdres.cmd_recover_turn_final(
        SimpleNamespace(
            plan_token="twplan1.failed",
            request_id=f"malformed-prefix-{field}",
        )
    )
    output = json.loads(capsys.readouterr().out)

    assert code == 1
    assert output["status"] == "recovery_state_invalid"
    assert calls == []


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("generation", 3),
        ("retained_failed_job_count", 2),
    ],
)
def test_recovery_response_requires_exact_next_generation_and_failed_tail(
    field,
    value,
):
    response = _recovery_response(
        acknowledged_prefix_count=1,
        executable_job_count=1,
    )
    response[field] = value

    invalid = herdres._validate_recovery_response(
        response,
        failed_plan_token="twplan1.failed",
        content_revision="twrev1.recovery",
        acknowledged_prefix_count=1,
        expected_job_count=2,
        expected_generation=2,
        retained_failed_job_count=1,
    )

    assert invalid["status"] == "recovery_state_uncertain"


def test_recovery_request_binding_outlives_bounded_detail_audit():
    store = _store()
    worker = _manual_recovery_worker()
    store["panes"]["worker"] = worker

    for index in range(101):
        failed = f"twplan1.failed{index}"
        replacement = f"twplan1.replacement{index}"
        request_id = f"operator-audit-{index}"
        worker["pending_plan_token"] = failed
        response = _recovery_response(
            failed_plan_token=failed,
            plan_token=replacement,
        )
        herdres._clone_recovery_prefix(
            store,
            failed_plan_token=failed,
            plan_token=replacement,
            entry_key="worker",
            prefix=[],
            executable_job_count=1,
            request_id=request_id,
            response=response,
        )

    request_bindings = store["tendwire_turn_final_recovery_requests"]
    details = store["tendwire_turn_final_recoveries"]
    oldest_key = herdres._recovery_request_key("operator-audit-0")

    assert len(request_bindings) == 101
    assert len(details) == 100
    assert oldest_key in request_bindings
    assert oldest_key not in details
    assert "operator-audit-0" not in request_bindings
    conflict = herdres._turn_final_recovery_preflight(
        store,
        "twplan1.failed0",
        "operator-audit-0",
    )
    assert conflict["status"] == "recovery_request_conflict"


def test_idempotent_replay_requires_every_immutable_audit_field(monkeypatch, capsys):
    store = _store()
    worker = _manual_recovery_worker()
    store["panes"]["worker"] = worker
    original = _recovery_response()
    herdres._clone_recovery_prefix(
        store,
        failed_plan_token="twplan1.failed",
        plan_token="twplan1.replacement",
        entry_key="worker",
        prefix=[],
        executable_job_count=1,
        request_id="operator-replay-exact",
        response=original,
    )
    replay = deepcopy(original)
    replay["idempotent_replay"] = True
    replay["prior_attempt_count"] = 4
    saves = []

    class MismatchedReplay:
        def connector_prepare_recover(self, **_kwargs):
            return deepcopy(replay)

    monkeypatch.setattr(herdres.config, "load_env_file", lambda: None)
    monkeypatch.setattr(herdres.config, "require_source_mode", lambda: None)
    monkeypatch.setattr(herdres.state, "state_lock", lambda: nullcontext())
    monkeypatch.setattr(herdres.state, "load_state", lambda: store)
    monkeypatch.setattr(
        herdres.state,
        "save_state",
        lambda candidate: saves.append(candidate),
    )
    monkeypatch.setattr(herdres, "TendwireClient", MismatchedReplay)

    code = herdres.cmd_recover_turn_final(
        SimpleNamespace(
            plan_token="twplan1.failed",
            request_id="operator-replay-exact",
        )
    )
    output = json.loads(capsys.readouterr().out)

    assert code == 1
    assert output["status"] == "recovery_state_uncertain"
    assert saves == []
