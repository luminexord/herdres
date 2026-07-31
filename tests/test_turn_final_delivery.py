from __future__ import annotations

from copy import deepcopy
import hashlib
import json
import time
from contextlib import nullcontext
from types import SimpleNamespace

import herdres
import pytest
from herdres_connector import config, doctor, source_sync, state
from herdres_connector.rich_delivery import (
    TURN_DELIVERY_PLAIN_SOURCE_CHARS,
    split_legacy_message_ids,
)
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


def _stable_key(worker_id: str, fingerprint: str = "fp-1") -> str:
    material = f"{worker_id}\0{fingerprint}".encode()
    return "wsk1_" + hashlib.sha256(material).hexdigest()


def _turn_row(turn_id: str, revision: str, final: str | None, *, user: str | None = None, inline: bool = True):
    row = {
        "id": turn_id,
        "worker_id": "worker-1",
        "worker_fingerprint": "fp-1",
        "stable_key": _stable_key("worker-1"),
        "stable_key_version": 1,
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
    def __init__(
        self,
        row,
        *,
        emit_ready=False,
        turn_schema_version=1,
    ):
        self.row = row
        self.emit_ready = emit_ready
        self.turn_schema_version = turn_schema_version
        self.snapshot_worker_id = str(row["worker_id"])
        self.snapshot_space_id = str(
            row.get("space_id") or "space-1"
        )
        self.snapshot_worker_name = "Alpha"
        self.snapshot_agent = "codex"
        self.attach_plan_source = True
        self.snapshot_fingerprint = str(
            row.get("worker_fingerprint") or "fp-1"
        )
        self.snapshot_stable_key = str(row["stable_key"])
        self.snapshot_stable_key_version = int(
            row["stable_key_version"]
        )
        self.pages = {}
        self.page_calls = []
        self.prepare_calls = []
        self.poll_calls = 0
        self.poll_lease_seconds = []
        self.ack_calls = []
        self.fail_calls = []
        self.defer_calls = []
        self.defer_delay_seconds = []
        self.source_prepare_refs = []
        self._plans = {}
        self._plan_by_revision = {}
        self._jobs = []
        self._ref_counter = 0
        self._active_plan = ""
        self._ready_state = {}
        self._ready_ref = {}
        self.ack_loss_once = False
        self.ack_committed_response_lost_once = False
        self.commit_response_lost_once = False
        self.completed_observe_lost_once = False

    def _ready_payload(self):
        revision = self.row["content"]["content_revision"]
        fields = {}
        for field in ("user_text", "assistant_final_text"):
            descriptor = deepcopy(
                self.row["content"]["fields"][field]
            )
            if (
                descriptor["availability"] == "complete"
                and descriptor["inline"]
            ):
                value = self.row[field]
                cursor = (
                    f"twcur1.ready_{revision.split('.')[-1]}_{field}"
                )
                self.pages.setdefault(
                    (revision, field, cursor),
                    {
                        "ok": True,
                        "schema_version": 1,
                        "turn_id": self.row["id"],
                        "content_revision": revision,
                        "field": field,
                        "availability": "complete",
                        "segment_id": (
                            f"twseg1.ready_{revision.split('.')[-1]}_{field}"
                        ),
                        "index": 0,
                        "count": 1,
                        "text": value,
                        "segment_char_length": len(value),
                        "segment_byte_length": len(
                            value.encode("utf-8")
                        ),
                        "total_char_length": len(value),
                        "total_byte_length": len(
                            value.encode("utf-8")
                        ),
                        "next_cursor": None,
                    },
                )
                descriptor["inline"] = False
                descriptor["page_count"] = 1
                descriptor["first_cursor"] = cursor
            fields[field] = descriptor
        identity = (
            "twfinal1."
            + revision.removeprefix("twrev1.").replace(".", "_")
        )
        return {
            "schema_version": 2,
            "operation": "materialize",
            "final_identity": identity,
            "turn_id": self.row["id"],
            "worker_id": self.row["worker_id"],
            "stable_key": self.row["stable_key"],
            "stable_key_version": self.row["stable_key_version"],
            "space_id": self.row.get("space_id") or "space-1",
            "content_revision": revision,
            "content": {
                "schema_version": 1,
                "content_revision": revision,
                "known_incomplete": self.row["content"][
                    "known_incomplete"
                ],
                "fields": fields,
            },
        }

    def _ready_lease_for_ref(self, ref):
        revision = next(
            revision
            for revision, lease_ref in self._ready_ref.items()
            if lease_ref == ref
        )
        assert self._ready_state[revision] == "leased"
        return revision, self._ready_payload()

    def snapshot(self):
        return {
            "ok": True,
            "workers": [
                _source_worker(
                    {
                        "id": self.snapshot_worker_id,
                        "name": self.snapshot_worker_name,
                        "status": (
                            "idle"
                            if self.row.get("complete")
                            else "working"
                        ),
                        "space_id": self.snapshot_space_id,
                        "fingerprint": self.snapshot_fingerprint,
                        "meta": {
                            "agent": self.snapshot_agent,
                            "stable_key": self.snapshot_stable_key,
                            "stable_key_version": (
                                self.snapshot_stable_key_version
                            ),
                        },
                    }
                )
            ],
            "spaces": [
                {
                    "id": self.snapshot_space_id,
                    "name": "Project",
                    "status": "active",
                    "fingerprint": "space-fp-1",
                }
            ],
        }

    def turns(self):
        return {
            "ok": True,
            "schema_version": self.turn_schema_version,
            "turns": [deepcopy(self.row)],
        }

    def pending(self):
        return {"ok": True, "pending_interactions": []}

    def connector_poll(self, **_kwargs):
        return {"ok": True, "items": []}

    def turn_content_get(
        self, turn_id, revision, field, cursor=None
    ):
        self.page_calls.append((turn_id, revision, field, cursor))
        return deepcopy(self.pages[(revision, field, cursor)])

    def install_pages(
        self,
        revision: str,
        field: str,
        value: str,
        cuts: tuple[int, ...],
    ):
        starts = (0, *cuts)
        ends = (*cuts, len(value))
        count = len(ends)
        cursors = [
            f"twcur1.{revision.split('.')[-1]}_{field}_{index}"
            for index in range(count)
        ]
        for index, (start, end) in enumerate(zip(starts, ends)):
            text = value[start:end]
            self.pages[(revision, field, cursors[index])] = {
                "ok": True,
                "schema_version": 1,
                "turn_id": self.row["id"],
                "content_revision": revision,
                "field": field,
                "availability": "complete",
                "segment_id": (
                    f"twseg1.{revision.split('.')[-1]}_{field}_{index}"
                ),
                "index": index,
                "count": count,
                "text": text,
                "segment_char_length": len(text),
                "segment_byte_length": len(text.encode("utf-8")),
                "total_char_length": len(value),
                "total_byte_length": len(value.encode("utf-8")),
                "next_cursor": (
                    cursors[index + 1]
                    if index + 1 < count
                    else None
                ),
            }
        descriptor = self.row["content"]["fields"][field]
        descriptor.update(
            {
                "inline": False,
                "page_count": count,
                "first_cursor": cursors[0],
                "char_length": len(value),
                "byte_length": len(value.encode("utf-8")),
            }
        )
        self.row.pop(field, None)

    def connector_prepare_begin(
        self,
        *,
        turn_id,
        content_revision,
        presentation_version,
        part_count,
        source_ref=None,
    ):
        assert presentation_version == PRESENTATION_VERSION
        assert (
            "telegram" not in presentation_version
            and "herdres" not in presentation_version
        )
        self.prepare_calls.append(
            ("begin", content_revision, part_count)
        )
        if source_ref is not None:
            self.source_prepare_refs.append(("begin", source_ref))
        source = None
        if source_ref is not None:
            source_revision, source = self._ready_lease_for_ref(
                source_ref
            )
            assert source_revision == content_revision
            assert source["turn_id"] == turn_id
        token = self._plan_by_revision.get(content_revision)
        if token:
            plan = self._plans[token]
            if source is not None:
                plan["source"] = deepcopy(source)
                plan["source_ref"] = source_ref
            return {
                "ok": True,
                "plan_token": token,
                "state": plan["state"],
                "part_count": part_count,
                "accepted_parts": len(plan["parts"]),
            }
        token = f"twplan1.plan{len(self._plans) + 1}"
        self._plan_by_revision[content_revision] = token
        self._plans[token] = {
            "state": "preparing",
            "turn_id": turn_id,
            "revision": content_revision,
            "part_count": part_count,
            "parts": {},
            "replaces": (
                self._active_plan
                if self._active_plan
                and self._plans[self._active_plan]["turn_id"]
                == turn_id
                else ""
            ),
            "source": deepcopy(source),
            "source_ref": source_ref,
        }
        return {
            "ok": True,
            "plan_token": token,
            "state": "preparing",
            "part_count": part_count,
            "accepted_parts": 0,
        }

    def connector_prepare_part(
        self, *, plan_token, ordinal, spans
    ):
        self.prepare_calls.append(("part", plan_token, ordinal))
        self._plans[plan_token]["parts"][ordinal] = deepcopy(spans)
        return {
            "ok": True,
            "plan_token": plan_token,
            "ordinal": ordinal,
            "accepted_parts": len(
                self._plans[plan_token]["parts"]
            ),
        }

    def connector_prepare_commit(
        self, *, plan_token, source_ref=None
    ):
        self.prepare_calls.append(("commit", plan_token))
        if source_ref is not None:
            self.source_prepare_refs.append(("commit", source_ref))
        plan = self._plans[plan_token]
        if source_ref is not None:
            source_revision, source = self._ready_lease_for_ref(
                source_ref
            )
            assert source_ref == plan["source_ref"]
            assert source_revision == plan["revision"]
            assert source == plan["source"]
        if plan["state"] != "preparing":
            count = len(
                [
                    job
                    for job in self._jobs
                    if job["payload"]["plan_token"] == plan_token
                ]
            )
            if (
                source_ref is None
                and plan["state"] == "completed"
                and self.completed_observe_lost_once
            ):
                self.completed_observe_lost_once = False
                return {
                    "ok": False,
                    "schema_version": 1,
                    "status": "timeout",
                }
            return {
                "ok": True,
                "plan_token": plan_token,
                "state": plan["state"],
                "job_count": count,
            }
        sequence = 0
        jobs = []
        for ordinal in range(plan["part_count"]):
            jobs.append(
                self._job(
                    plan_token,
                    sequence,
                    "upsert",
                    ordinal,
                    plan["part_count"],
                    plan["parts"][ordinal],
                    plan["replaces"],
                )
            )
            sequence += 1
        if plan["replaces"]:
            old_count = self._plans[plan["replaces"]][
                "part_count"
            ]
            for ordinal in range(
                old_count - 1,
                plan["part_count"] - 1,
                -1,
            ):
                jobs.append(
                    self._job(
                        plan_token,
                        sequence,
                        "retire",
                        ordinal,
                        plan["part_count"],
                        [],
                        plan["replaces"],
                    )
                )
                sequence += 1
        self._jobs.extend(jobs)
        plan["state"] = "active"
        self._active_plan = plan_token
        if source_ref is not None:
            self._ready_state[plan["revision"]] = "awaiting_ack"
        result = {
            "ok": True,
            "plan_token": plan_token,
            "state": "active",
            "job_count": len(jobs),
            "generation": 1,
        }
        if source_ref is not None and self.commit_response_lost_once:
            self.commit_response_lost_once = False
            return {
                "ok": False,
                "schema_version": 1,
                "status": "timeout",
            }
        return result

    def _job(
        self,
        token,
        sequence,
        operation,
        ordinal,
        part_count,
        spans,
        replaces,
    ):
        plan = self._plans[token]
        payload = {
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
        }
        if self.attach_plan_source and plan.get("source") is not None:
            payload["turn"] = deepcopy(plan["source"])
        return {
            "status": "queued",
            "key": f"turn-final:{token}:{sequence:06d}",
            "payload": payload,
        }

    def turn_final_poll(self, *, limit=1, lease_seconds=60):
        assert limit == 1
        assert lease_seconds == config.tendwire_turn_final_lease_seconds()
        self.poll_calls += 1
        self.poll_lease_seconds.append(lease_seconds)
        for job in self._jobs:
            if job["status"] != "queued":
                continue
            token = job["payload"]["plan_token"]
            sequence = job["payload"]["sequence_index"]
            prior = [
                candidate
                for candidate in self._jobs
                if candidate["payload"]["plan_token"] == token
                and candidate["payload"]["sequence_index"]
                < sequence
            ]
            if any(
                candidate["status"] != "delivered"
                for candidate in prior
            ):
                continue
            self._ref_counter += 1
            job["status"] = "leased"
            job["ref"] = f"twref1.lease{self._ref_counter}"
            return {
                "ok": True,
                "schema_version": 1,
                "items": [
                    {
                        "ref": job["ref"],
                        "key": job["key"],
                        "attempt": self._ref_counter,
                        "payload": deepcopy(job["payload"]),
                    }
                ],
            }
        if (
            self.emit_ready
            and self.row.get("complete")
            and self.row["content"].get("known_incomplete") is False
        ):
            revision = self.row["content"]["content_revision"]
            status = self._ready_state.setdefault(
                revision, "queued"
            )
            if status == "queued":
                self._ref_counter += 1
                ref = f"twref1.ready{self._ref_counter}"
                self._ready_state[revision] = "leased"
                self._ready_ref[revision] = ref
                ready = self._ready_payload()
                return {
                    "ok": True,
                    "schema_version": 1,
                    "items": [
                        {
                            "ref": ref,
                            "key": (
                                "turn-final:revision:"
                                f"{ready['final_identity']}"
                            ),
                            "attempt": self._ref_counter,
                            "payload": deepcopy(ready),
                        }
                    ],
                }
        return {"ok": True, "schema_version": 1, "items": []}

    def _leased(self, ref):
        return next(
            job
            for job in self._jobs
            if job.get("ref") == ref
            and job["status"] == "leased"
        )

    def turn_final_ack(self, ref, response=None):
        self.ack_calls.append((ref, deepcopy(response)))
        job = self._leased(ref)
        if self.ack_loss_once:
            self.ack_loss_once = False
            job["status"] = "queued"
            return {
                "ok": False,
                "schema_version": 1,
                "status": "timeout",
            }
        job["status"] = "delivered"
        token = job["payload"]["plan_token"]
        siblings = [
            candidate
            for candidate in self._jobs
            if candidate["payload"]["plan_token"] == token
        ]
        if all(
            candidate["status"] == "delivered"
            for candidate in siblings
        ):
            self._plans[token]["state"] = "completed"
            revision = self._plans[token]["revision"]
            if (
                self._ready_state.get(revision)
                == "awaiting_ack"
            ):
                self._ready_state[revision] = "delivered"
        if self.ack_committed_response_lost_once:
            self.ack_committed_response_lost_once = False
            return {
                "ok": False,
                "schema_version": 1,
                "status": "timeout",
            }
        return {
            "ok": True,
            "schema_version": 1,
            "status": "acknowledged",
        }

    def _requeue_ref(self, ref):
        for revision, lease_ref in self._ready_ref.items():
            if (
                lease_ref == ref
                and self._ready_state.get(revision) == "leased"
            ):
                self._ready_state[revision] = "queued"
            if lease_ref == ref:
                return
        self._leased(ref)["status"] = "queued"

    def turn_final_fail(self, ref, reason):
        self.fail_calls.append((ref, reason))
        self._requeue_ref(ref)
        return {
            "ok": True,
            "schema_version": 1,
            "status": "retry_scheduled",
        }

    def turn_final_defer(self, ref, reason="", **kwargs):
        self.defer_calls.append((ref, reason))
        self.defer_delay_seconds.append(kwargs.get("delay_seconds"))
        self._requeue_ref(ref)
        return {
            "ok": True,
            "schema_version": 1,
            "status": "deferred",
        }


class MultiTurnFinalTendwire(TurnFinalTendwire):
    def __init__(self, rows):
        known_rows = list(rows)
        for row in known_rows:
            fingerprint = str(
                row.get("worker_fingerprint")
                or f"fp-{row['worker_id']}"
            )
            row["stable_key"] = _stable_key(
                str(row["worker_id"]), fingerprint
            )
            row["stable_key_version"] = 1
        super().__init__(
            known_rows[0],
            emit_ready=True,
            turn_schema_version=2,
        )
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
                        "fingerprint": str(
                            row.get("worker_fingerprint")
                            or f"fp-{worker_id}"
                        ),
                        "meta": {
                            "agent": row.get("agent", "codex"),
                            "stable_key": row["stable_key"],
                            "stable_key_version": (
                                row["stable_key_version"]
                            ),
                        },
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

    def turn_final_poll(self, *, limit=1, lease_seconds=60):
        known_worker_ids = {
            row["worker_id"] for row in self.known_rows
        }
        first_by_worker = {}
        for row in self.rows:
            worker_id = row["worker_id"]
            if worker_id in first_by_worker:
                continue
            first_by_worker[worker_id] = row
        candidate = next(
            (
                row
                for worker_id, row in first_by_worker.items()
                if worker_id in known_worker_ids
                and row.get("complete")
                and row["content"].get("known_incomplete") is False
                and self._ready_state.get(
                    row["content"]["content_revision"]
                )
                != "delivered"
            ),
            None,
        )
        self.emit_ready = candidate is not None
        if candidate is not None:
            self.row = candidate
        return super().turn_final_poll(
            limit=limit,
            lease_seconds=lease_seconds,
        )

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


def _ready_tendwire(row):
    return TurnFinalTendwire(
        row,
        emit_ready=True,
        turn_schema_version=2,
    )


class PlanRetentionTendwire(TurnFinalTendwire):
    def __init__(self, row):
        super().__init__(
            row,
            emit_ready=True,
            turn_schema_version=2,
        )
        self.missing_plans = set()
        self.plan_errors = set()
        self.supersede_on_ready = ""

    def connector_prepare_commit(
        self, *, plan_token, source_ref=None
    ):
        if source_ref is None and plan_token in self.plan_errors:
            return {
                "ok": False,
                "schema_version": 1,
                "status": "timeout",
            }
        if source_ref is None and plan_token in self.missing_plans:
            return {
                "ok": False,
                "schema_version": 1,
                "status": "plan_not_found",
            }
        return super().connector_prepare_commit(
            plan_token=plan_token,
            source_ref=source_ref,
        )

    def turn_final_poll(self, *, limit=1, lease_seconds=60):
        response = super().turn_final_poll(
            limit=limit,
            lease_seconds=lease_seconds,
        )
        if self.supersede_on_ready and any(
            isinstance(item, dict)
            and isinstance(item.get("payload"), dict)
            and item["payload"].get("operation")
            == "materialize"
            for item in response.get("items", [])
        ):
            self._plans[self.supersede_on_ready][
                "state"
            ] = "superseded"
            self.supersede_on_ready = ""
        return response


class SlowPageTendwire(TurnFinalTendwire):
    def __init__(self, row, *, page_seconds):
        super().__init__(
            row,
            emit_ready=True,
            turn_schema_version=2,
        )
        self.clock = 0
        self.page_seconds = page_seconds
        self.ready_deadline = 0
        self.ready_lease_seconds = []

    def turn_final_poll(self, *, limit=1, lease_seconds=60):
        response = super().turn_final_poll(
            limit=limit,
            lease_seconds=lease_seconds,
        )
        if any(
            isinstance(item, dict)
            and isinstance(item.get("payload"), dict)
            and item["payload"].get("operation")
            == "materialize"
            for item in response.get("items", [])
        ):
            self.ready_lease_seconds.append(lease_seconds)
            self.ready_deadline = self.clock + lease_seconds
        return response

    def turn_content_get(
        self, turn_id, revision, field, cursor=None
    ):
        self.clock += self.page_seconds
        return super().turn_content_get(
            turn_id, revision, field, cursor
        )

    def _ready_lease_for_ref(self, ref):
        assert self.clock < self.ready_deadline
        return super()._ready_lease_for_ref(ref)


class ReadyQueueTendwire(TurnFinalTendwire):
    def __init__(self, rows):
        self.rows = [deepcopy(row) for row in rows]
        super().__init__(
            self.rows[0],
            emit_ready=True,
            turn_schema_version=2,
        )

    def turns(self):
        return {
            "ok": True,
            "schema_version": 2,
            "turns": [],
        }

    def turn_final_poll(self, *, limit=1, lease_seconds=60):
        for row in self.rows:
            revision = row["content"]["content_revision"]
            if self._ready_state.get(revision) != "delivered":
                self.row = row
                break
        return super().turn_final_poll(
            limit=limit,
            lease_seconds=lease_seconds,
        )


class MutatingReadyTendwire(TurnFinalTendwire):
    def __init__(self, row, mutation):
        super().__init__(
            row,
            emit_ready=True,
            turn_schema_version=2,
        )
        self.mutation = mutation

    def _ready_payload(self):
        payload = super()._ready_payload()
        self.mutation(payload)
        return payload


class ConflictingAttachedSourceTendwire(TurnFinalTendwire):
    def __init__(self, row):
        super().__init__(
            row,
            emit_ready=True,
            turn_schema_version=2,
        )
        self.conflict_injected = False

    def turn_final_poll(self, *, limit=1, lease_seconds=60):
        response = super().turn_final_poll(
            limit=limit,
            lease_seconds=lease_seconds,
        )
        if not self.conflict_injected:
            for lease in response.get("items", []):
                payload = lease.get("payload")
                if (
                    isinstance(payload, dict)
                    and payload.get("operation") == "upsert"
                    and payload.get("sequence_index") == 1
                    and isinstance(payload.get("turn"), dict)
                ):
                    payload["turn"] = deepcopy(payload["turn"])
                    payload["turn"]["content"]["fields"][
                        "assistant_final_text"
                    ]["first_cursor"] = "twcur1.conflicting"
                    self.conflict_injected = True
                    break
        return response


class DeletingTelegram(FakeTelegram):
    def __init__(self, token="fake", shared=None):
        super().__init__(token=token, shared=shared)
        self._shared.setdefault("deleted_messages", [])
        self._shared.setdefault("recipient_messages", {})
        self.deleted_messages = self._shared["deleted_messages"]
        self.recipient_messages = self._shared["recipient_messages"]
        self.raise_after_accept = False

    def with_token(self, token):
        return DeletingTelegram(token=token, shared=self._shared)

    def api(self, method, payload):
        result = super().api(method, payload)
        if method == "sendRichMessage":
            message_id = str(
                (result.get("result") or {}).get("message_id") or ""
            )
            rich = json.loads(payload.get("rich_message") or "{}")
            self.recipient_messages[message_id] = {
                "format": "rich",
                "content": str(rich.get("html") or ""),
            }
        elif method == "editMessageText":
            message_id = str(payload.get("message_id") or "")
            rich = json.loads(payload.get("rich_message") or "{}")
            self.recipient_messages[message_id] = {
                "format": "rich" if rich else "html",
                "content": str(
                    rich.get("html") or payload.get("text") or ""
                ),
            }
        if method == "sendRichMessage" and self.raise_after_accept:
            self.raise_after_accept = False
            raise RuntimeError("response lost after acceptance")
        return result

    def send_message(self, chat_id, html, **kwargs):
        result = super().send_message(chat_id, html, **kwargs)
        for message_id in split_legacy_message_ids(result):
            self.recipient_messages[message_id] = {
                "format": str(result.get("format") or "html"),
                "content": str(html),
            }
        if self.raise_after_accept:
            self.raise_after_accept = False
            raise RuntimeError("response lost after acceptance")
        return result

    def edit_message(self, chat_id, message_id, html):
        result = super().edit_message(chat_id, message_id, html)
        self.recipient_messages[str(message_id)] = {
            "format": str(result.get("format") or "html"),
            "content": str(html),
        }
        return result

    def delete_message(self, chat_id, message_id):
        self.deleted_messages.append((str(chat_id), str(message_id), self.token))
        self.recipient_messages.pop(str(message_id), None)
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


def test_turn_final_drain_does_not_exceed_feed_write_budget(
    monkeypatch,
):
    monkeypatch.setenv("HERDRES_TENDWIRE_MODE", "source")
    monkeypatch.setenv("HERDRES_PINNED_STATUS", "0")
    working = _turn_row(
        "turn-budget-floor",
        "twrev1.budget_floor",
        None,
        user="consume the feed budget",
    )
    working["assistant_stream_text"] = "working update"
    tendwire = TurnFinalTendwire(working)

    result = sync_once(
        _store(),
        _runtime(tendwire, DeletingTelegram(), max_sends=1),
    )

    assert result["feed_sent"] == 1
    assert result["sent"] == 1
    assert tendwire.poll_calls == 0
    assert result["tendwire_turn_final"]["operations"] == 0


def test_stopped_mid_creation_plan_ages_to_visible_terminal_hold(
    monkeypatch,
):
    monkeypatch.setenv(
        "HERDRES_PARTIAL_FINAL_ESCALATION_SECONDS", "30"
    )
    store = _store()
    _key, entry, _created = state.upsert_worker_entry(
        store,
        _source_worker(
            {
                "id": "worker-mid-create",
                "status": "working",
                "space_id": "space-1",
                "fingerprint": "mid-create-fp",
            }
        ),
        topic_id="77",
    )
    source_sync._set_pending_turn_plan(
        entry,
        turn_id="turn-mid-create",
        revision="twrev1.mid_create",
        plan_token="twplan1.mid_create",
        part_count=11,
        job_count=7,
        now=time.time() - 31,
    )

    class StoppedPlan:
        def connector_prepare_commit(self, *, plan_token):
            return {
                "ok": True,
                "plan_token": plan_token,
                "state": "active",
                "job_count": 7,
            }

    visible_before_reconcile = doctor.outbound_partial_finals(store)
    checkpoints = []
    changed = source_sync._reconcile_completed_turn_plans(
        store,
        SyncRuntime(
            StoppedPlan(),
            DeletingTelegram(),
            with_outbox=True,
            checkpoint=lambda: checkpoints.append("checkpoint"),
        ),
        pending_entry=entry,
    )
    hold = state.find_partial_final_delivery(
        store, "turn-mid-create", "twrev1.mid_create"
    )

    assert visible_before_reconcile["ok"] is False
    assert (
        visible_before_reconcile["status"]
        == "pending_turn_plan_stalled"
    )
    stalled = visible_before_reconcile["first_stalled_plan"]
    assert stalled["turn_id"] == "turn-mid-create"
    assert stalled["plan_token"] == "twplan1.mid_create"
    assert stalled["worker_id"] == "worker-mid-create"
    assert stalled["topic_id"] == "77"
    assert stalled["age_seconds"] >= 30
    assert changed == 1
    assert checkpoints == ["checkpoint"]
    assert hold is not None
    assert hold["status"] == "held"
    assert hold["request_phase"] == "pending_plan_incomplete"
    assert hold["created_part_ordinals"] == list(range(7))
    assert hold["missing_part_ordinals"] == [7, 8, 9, 10]
    assert hold["operator_attention_required"] is True
    assert "pending_turn_id" not in entry
    assert doctor.outbound_partial_finals(store)["ok"] is False


def test_dead_lettered_child_immediately_holds_parent_plan(monkeypatch):
    monkeypatch.setenv(
        "HERDRES_PARTIAL_FINAL_ESCALATION_SECONDS", "300"
    )
    store = _store()
    _key, entry, _created = state.upsert_worker_entry(
        store,
        _source_worker(
            {
                "id": "worker-dead-child",
                "status": "working",
                "space_id": "space-1",
                "fingerprint": "dead-child-fp",
            }
        ),
        topic_id="78",
    )
    source_sync._set_pending_turn_plan(
        entry,
        turn_id="turn-dead-child",
        revision="twrev1.dead_child",
        plan_token="twplan1.dead_child",
        part_count=3,
        job_count=2,
    )
    job_key = "turn-final:twplan1.dead_child:000001"
    state.reserve_tendwire_turn_job(
        store,
        job_key,
        plan_token="twplan1.dead_child",
        content_revision="twrev1.dead_child",
        operation="upsert",
        sequence_index=1,
        part_ordinal=1,
        part_count=3,
        bot_kind="manager",
    )
    state.update_tendwire_turn_job(
        store, job_key, substate="failed"
    )

    class FailedPlan:
        def connector_prepare_commit(self, *, plan_token):
            return {
                "ok": True,
                "plan_token": plan_token,
                "state": "failed",
                "job_count": 2,
            }

    changed = source_sync._reconcile_completed_turn_plans(
        store,
        SyncRuntime(
            FailedPlan(), DeletingTelegram(), with_outbox=True
        ),
        pending_entry=entry,
    )
    hold = state.find_partial_final_delivery(
        store, "turn-dead-child", "twrev1.dead_child"
    )

    assert changed == 1
    assert hold is not None
    assert hold["request_phase"] == "pending_plan_incomplete"
    assert hold["missing_part_ordinals"] == [2]
    assert hold["failed_part_index"] == 1
    assert doctor.outbound_partial_finals(store)["ok"] is False


def test_oversize_final_is_explicit_terminal_and_delivers_no_prefix(
    monkeypatch,
):
    monkeypatch.setenv("HERDRES_FORCE_PLAIN_DELIVERY", "0")
    store = _store()
    row = _turn_row(
        "turn-oversize",
        "twrev1.oversize",
        "x" * (TURN_DELIVERY_PLAIN_SOURCE_CHARS * 8 + 1),
        user="legitimate large request",
    )
    _key, entry, _created = state.upsert_worker_entry(
        store,
        _source_worker(
            {
                "id": "worker-1",
                "status": "idle",
                "space_id": "space-1",
                "fingerprint": "fp-1",
            }
        ),
        topic_id="77",
    )
    telegram = DeletingTelegram()

    with pytest.raises(source_sync._TurnContentError) as raised:
        source_sync._stage_final_plan(
            store,
            row,
            entry,
            SyncRuntime(
                TurnFinalTendwire(row),
                telegram,
                dry_run=True,
                with_outbox=False,
            ),
        )

    hold = state.find_partial_final_delivery(
        store, "turn-oversize", "twrev1.oversize"
    )
    assert raised.value.status == "oversize_presentation"
    assert hold is not None
    assert hold["request_phase"] == "oversize_presentation"
    assert hold["terminal_outcome"] == "not_delivered"
    assert hold["recovery_action"] == "supersede-with-shorter-answer"
    assert telegram.recipient_messages == {}
    assert state.delivered_turns(store) == {}
    assert doctor.outbound_partial_finals(store)["ok"] is False


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


def test_final_edits_exact_command_predecessor_working_card(
    monkeypatch,
):
    monkeypatch.setenv("HERDRES_TENDWIRE_MODE", "source")
    monkeypatch.setenv("HERDRES_PINNED_STATUS", "0")

    class PredecessorTendwire(TurnFinalTendwire):
        predecessor_turn_id = ""

        def _ready_payload(self):
            payload = super()._ready_payload()
            if self.predecessor_turn_id:
                payload["working_predecessor_turn_id"] = (
                    self.predecessor_turn_id
                )
            return payload

    working_turn_id = "turn-command-working"
    working = _turn_row(
        working_turn_id,
        "twrev1.command_working",
        None,
    )
    working["assistant_stream_text"] = "Working exactly here"
    tendwire = PredecessorTendwire(
        working,
        emit_ready=True,
        turn_schema_version=2,
    )
    telegram = DeletingTelegram()
    store = _store()

    sync_once(store, _runtime(tendwire, telegram, max_sends=1))
    entry = next(iter(state.source_worker_entries(store).values()))
    working_id = entry["last_stream_message_id"]
    sends_before_final = len(telegram.sent)

    final = _turn_row(
        "turn-canonical-source",
        "twrev1.canonical_source",
        "exact canonical response",
        user="exact command prompt",
    )
    tendwire.row = final
    tendwire.predecessor_turn_id = working_turn_id

    result = sync_once(
        store,
        _runtime(tendwire, telegram, max_sends=10),
    )

    assert result["tendwire_turn_final"]["acked"] == 1
    assert len(telegram.sent) == sends_before_final
    assert any(edit[1] == working_id for edit in telegram.edited)
    binding = state.find_message_binding(
        store,
        working_id,
        topic_id="77",
    )
    assert binding is not None
    assert binding["kind"] == "final"
    assert binding["turn_id"] == "turn-canonical-source"


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
    incomplete.turn_schema_version = 2
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
    receipt_checkpoint = next(checkpoint for checkpoint in checkpoints if checkpoint)
    receipt_key = next(iter(receipt_checkpoint))
    assert receipt_checkpoint[receipt_key]["substate"] == "reserved"
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
    # Keep this lifecycle regression below the explicit eight-card oversize
    # terminal; oversize behavior has its own recipient-state regression.
    first_text = "A paragraph.\n\n" * 800
    tendwire = TurnFinalTendwire(_turn_row("turn-revise", "twrev1.r1", first_text))
    sync_once(store, _runtime(tendwire, telegram, max_sends=100))
    entry = next(iter(state.source_worker_entries(store).values()))
    first_ids = list(entry["last_clean_message_ids"])
    first_count = len(entry["last_clean_message_ids"])
    assert first_count > 1

    growth = "B changed.\n\n" * 1_500
    tendwire.row = _turn_row("turn-revise", "twrev1.r2", growth)
    grow_result = sync_once(store, _runtime(tendwire, telegram, max_sends=100))
    grown_ids = list(entry["last_clean_message_ids"])
    assert len(grown_ids) > first_count
    assert grow_result["tendwire_turn_final"]["acked"] == len(grown_ids)
    assert all(
        message_id in telegram.recipient_messages
        for message_id in grown_ids
    )
    assert all(
        message_id not in telegram.recipient_messages
        for message_id in first_ids
        if message_id not in grown_ids
    )
    assert all(
        "B changed." in telegram.recipient_messages[message_id]["content"]
        and "A paragraph." not in telegram.recipient_messages[message_id]["content"]
        for message_id in grown_ids
    )
    assert all(
        state.find_message_binding(store, message_id)["message_ids"]
        == grown_ids
        and state.find_message_binding(store, message_id)[
            "canonical_message_id"
        ]
        == grown_ids[0]
        for message_id in grown_ids
    )

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
    assert all(
        message_id not in telegram.recipient_messages
        for message_id in grown_ids
    )
    assert "C final compact" in telegram.recipient_messages[
        current_ids[0]
    ]["content"]


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
            "schema_version": 1,
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


def test_not_found_edit_resumes_as_send_in_next_budgeted_pass(monkeypatch):
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
    assert missing["tendwire_turn_final"]["acked"] == 0
    assert missing["tendwire_turn_final"]["failed"] == 0
    assert missing["tendwire_turn_final"]["uncertain"] == 0
    assert len(telegram.sent) == sent_before + 1
    assert telegram.edit_attempts == 1
    assert resumed["tendwire_turn_final"]["operations"] == 1
    assert resumed["tendwire_turn_final"]["acked"] == 1


def test_turn_final_resend_tombstones_requested_topic_after_concurrent_rebind(
    tmp_path, monkeypatch
):
    monkeypatch.setenv("HERDRES_TENDWIRE_MODE", "source")
    monkeypatch.setenv("HERDRES_SOURCE_TOPIC_MODE", "worker")
    monkeypatch.setenv("HERDRES_PINNED_STATUS", "0")
    monkeypatch.setenv(
        "HERDR_TELEGRAM_TOPICS_STATUS_ICON", "0"
    )
    monkeypatch.setenv(
        "HERDR_TELEGRAM_TOPICS_RICH_MESSAGES", "0"
    )
    state_path = tmp_path / "state.json"
    monkeypatch.setenv(
        "HERDR_TELEGRAM_TOPICS_STATE", str(state_path)
    )
    requested_topic_id = "15007"
    rebound_topic_id = "16000"
    tendwire = TurnFinalTendwire(
        _turn_row(
            "turn-concurrent-topic-rebind",
            "twrev1.before_rebind",
            "before rebind",
        )
    )
    store = _store()
    state.save_state(store, state_path)

    class ConcurrentRebindFinalTelegram(FakeTelegram):
        def __init__(self):
            super().__init__()
            self.fail_revision = False
            self.edit_attempts = 0
            self.resend_topic_ids = []

        def edit_message(self, chat_id, message_id, html):
            if self.fail_revision:
                self.edit_attempts += 1
                return {
                    "ok": False,
                    "kind": "topic_not_found",
                    "topic_missing": True,
                    "error": (
                        "Bad Request: message thread not found"
                    ),
                }
            return super().edit_message(
                chat_id, message_id, html
            )

        def send_message(self, chat_id, html, **kwargs):
            thread_id = str(kwargs.get("thread_id") or "")
            if (
                self.fail_revision
                and thread_id == requested_topic_id
            ):
                self.resend_topic_ids.append(thread_id)
                concurrent = state.load_state(state_path)
                _entry_key, rebound = (
                    state.find_worker_entry_by_stable_key(
                        concurrent, _stable_key("worker-1")
                    )
                )
                assert rebound is not None
                rebound["topic_id"] = rebound_topic_id
                concurrent["spaces"][
                    "space:concurrent-final-alias"
                ] = {
                    "source": "tendwire",
                    "entry_type": "space",
                    "tendwire_space_id": (
                        "concurrent-final-alias"
                    ),
                    "topic_id": requested_topic_id,
                    "topic_name": "Concurrent final alias",
                }
                state.save_state(concurrent, state_path)
                return {
                    "ok": False,
                    "kind": "topic_not_found",
                    "topic_missing": True,
                    "error": (
                        "Bad Request: message thread not found"
                    ),
                }
            return super().send_message(
                chat_id, html, **kwargs
            )

    telegram = ConcurrentRebindFinalTelegram()
    with state.state_lock(path=state_path):
        current = state.load_state(state_path)
        first = sync_once(
            current, _runtime(tendwire, telegram, max_sends=2)
        )

    assert first["tendwire_turn_final"]["acked"] == 1
    _first_key, first_entry = state.find_worker_entry_by_stable_key(
        current, _stable_key("worker-1")
    )
    assert first_entry is not None
    first_entry["topic_id"] = requested_topic_id
    for binding in state.message_bindings(current).values():
        if (
            isinstance(binding, dict)
            and binding.get("turn_id")
            == "turn-concurrent-topic-rebind"
        ):
            binding["topic_id"] = requested_topic_id
            binding.pop("routing_quarantined", None)
    ack_count = len(tendwire.ack_calls)
    tendwire.row = _turn_row(
        "turn-concurrent-topic-rebind",
        "twrev1.after_rebind",
        "after rebind",
    )
    telegram.fail_revision = True
    state.save_state(current, state_path)

    with state.state_lock(path=state_path):
        current = state.load_state(state_path)
        failed = sync_once(
            current, _runtime(tendwire, telegram, max_sends=2)
        )

    _entry_key, rebound = state.find_worker_entry_by_stable_key(
        current, _stable_key("worker-1")
    )
    old_topic_refs = [
        candidate
        for candidate in state.source_entries(current).values()
        if str(candidate.get("topic_id") or "")
        == requested_topic_id
    ]
    assert telegram.edit_attempts == 1
    assert telegram.resend_topic_ids == [requested_topic_id]
    assert failed["tendwire_turn_final"]["deferred"] == 1
    assert failed["tendwire_turn_final"]["acked"] == 0
    assert len(tendwire.ack_calls) == ack_count
    assert rebound is not None
    assert rebound["topic_id"] == rebound_topic_id
    assert state.topic_id_is_tombstoned(
        current, requested_topic_id
    )
    assert not state.topic_id_is_tombstoned(
        current, rebound_topic_id
    )
    assert old_topic_refs == []


def test_turn_final_edit_concurrent_clear_defers_without_root_send_then_recovers(
    tmp_path, monkeypatch
):
    monkeypatch.setenv("HERDRES_TENDWIRE_MODE", "source")
    monkeypatch.setenv("HERDRES_SOURCE_TOPIC_MODE", "worker")
    monkeypatch.setenv("HERDRES_PINNED_STATUS", "0")
    monkeypatch.setenv(
        "HERDR_TELEGRAM_TOPICS_STATUS_ICON", "0"
    )
    monkeypatch.setenv(
        "HERDR_TELEGRAM_TOPICS_RICH_MESSAGES", "0"
    )
    state_path = tmp_path / "state.json"
    monkeypatch.setenv(
        "HERDR_TELEGRAM_TOPICS_STATE", str(state_path)
    )
    dead_topic_id = "15007"
    tendwire = TurnFinalTendwire(
        _turn_row(
            "turn-concurrent-topic-clear",
            "twrev1.before_clear",
            "before clear",
        )
    )
    store = _store()
    state.save_state(store, state_path)

    class ConcurrentClearFinalTelegram(DeletingTelegram):
        def __init__(self):
            super().__init__()
            self.clear_during_edit = False
            self.edit_attempts = 0
            self.revision_send_topics = []

        def edit_message(self, chat_id, message_id, html):
            if self.clear_during_edit:
                self.edit_attempts += 1
                concurrent = state.load_state(state_path)
                state.tombstone_dead_topic(
                    concurrent, dead_topic_id
                )
                state.save_state(concurrent, state_path)
                return {
                    "ok": False,
                    "kind": "not_found",
                    "error": (
                        "Bad Request: message to edit not found"
                    ),
                }
            return super().edit_message(
                chat_id, message_id, html
            )

        def send_message(self, chat_id, html, **kwargs):
            if "after clear" in html:
                self.revision_send_topics.append(
                    str(kwargs.get("thread_id") or "")
                )
            return super().send_message(
                chat_id, html, **kwargs
            )

    telegram = ConcurrentClearFinalTelegram()
    with state.state_lock(path=state_path):
        current = state.load_state(state_path)
        first = sync_once(
            current, _runtime(tendwire, telegram, max_sends=4)
        )

    assert first["tendwire_turn_final"]["acked"] == 1
    _entry_key, entry = state.find_worker_entry_by_stable_key(
        current, _stable_key("worker-1")
    )
    assert entry is not None
    entry["topic_id"] = dead_topic_id
    for binding in state.message_bindings(current).values():
        if (
            isinstance(binding, dict)
            and binding.get("turn_id")
            == "turn-concurrent-topic-clear"
        ):
            binding["topic_id"] = dead_topic_id
            binding.pop("routing_quarantined", None)
    ack_count = len(tendwire.ack_calls)
    sent_count = len(telegram.sent)
    tendwire.row = _turn_row(
        "turn-concurrent-topic-clear",
        "twrev1.after_clear",
        "after clear",
    )
    telegram.clear_during_edit = True
    state.save_state(current, state_path)

    with state.state_lock(path=state_path):
        current = state.load_state(state_path)
        cleared = sync_once(
            current, _runtime(tendwire, telegram, max_sends=4)
        )

    _entry_key, entry = state.find_worker_entry_by_stable_key(
        current, _stable_key("worker-1")
    )
    assert telegram.edit_attempts == 1
    assert telegram.revision_send_topics == []
    assert len(telegram.sent) == sent_count
    assert cleared["tendwire_turn_final"]["operations"] == 1
    assert cleared["tendwire_turn_final"]["deferred"] == 1
    assert cleared["tendwire_turn_final"]["acked"] == 0
    assert len(tendwire.ack_calls) == ack_count
    assert entry is not None
    assert "topic_id" not in entry
    assert state.topic_id_is_tombstoned(
        current, dead_topic_id
    )

    telegram.clear_during_edit = False
    with state.state_lock(path=state_path):
        current = state.load_state(state_path)
        recovered = sync_once(
            current, _runtime(tendwire, telegram, max_sends=4)
        )
    with state.state_lock(path=state_path):
        current = state.load_state(state_path)
        stable = sync_once(
            current, _runtime(tendwire, telegram, max_sends=4)
        )

    assert recovered["tendwire_turn_final"]["acked"] == 1
    assert stable["tendwire_turn_final"]["polled"] == 0
    assert len(tendwire.ack_calls) == ack_count + 1
    assert len(telegram.revision_send_topics) == 1
    assert telegram.revision_send_topics[0]
    assert telegram.revision_send_topics[0] != dead_topic_id


def test_turn_final_successful_send_binds_attempted_topic_and_reconciles_rebind(
    tmp_path, monkeypatch
):
    monkeypatch.setenv("HERDRES_TENDWIRE_MODE", "source")
    monkeypatch.setenv("HERDRES_SOURCE_TOPIC_MODE", "worker")
    monkeypatch.setenv("HERDRES_PINNED_STATUS", "0")
    monkeypatch.setenv(
        "HERDR_TELEGRAM_TOPICS_STATUS_ICON", "0"
    )
    monkeypatch.setenv(
        "HERDR_TELEGRAM_TOPICS_RICH_MESSAGES", "0"
    )
    state_path = tmp_path / "state.json"
    monkeypatch.setenv(
        "HERDR_TELEGRAM_TOPICS_STATE", str(state_path)
    )
    attempted_topic_id = "15007"
    rebound_topic_id = "16000"
    turn_id = "turn-successful-send-rebind"
    revision = "twrev1.after_successful_rebind"
    tendwire = TurnFinalTendwire(
        _turn_row(
            turn_id,
            "twrev1.before_successful_rebind",
            "before successful rebind",
        )
    )
    store = _store()
    state.save_state(store, state_path)

    class SuccessfulRebindFinalTelegram(DeletingTelegram):
        def __init__(self):
            super().__init__()
            self.race_revision = False
            self.edit_attempts = 0
            self.revision_send_topics = []
            self.rebound = False

        def edit_message(self, chat_id, message_id, html):
            if self.race_revision:
                self.edit_attempts += 1
                return {
                    "ok": False,
                    "kind": "not_found",
                    "error": (
                        "Bad Request: message to edit not found"
                    ),
                }
            return super().edit_message(
                chat_id, message_id, html
            )

        def send_message(self, chat_id, html, **kwargs):
            result = super().send_message(
                chat_id, html, **kwargs
            )
            if "after successful rebind" not in html:
                return result
            thread_id = str(kwargs.get("thread_id") or "")
            self.revision_send_topics.append(thread_id)
            if (
                self.race_revision
                and not self.rebound
                and thread_id == attempted_topic_id
            ):
                concurrent = state.load_state(state_path)
                _entry_key, rebound = (
                    state.find_worker_entry_by_stable_key(
                        concurrent, _stable_key("worker-1")
                    )
                )
                assert rebound is not None
                rebound["topic_id"] = rebound_topic_id
                state.save_state(concurrent, state_path)
                self.rebound = True
            return result

    telegram = SuccessfulRebindFinalTelegram()
    with state.state_lock(path=state_path):
        current = state.load_state(state_path)
        first = sync_once(
            current, _runtime(tendwire, telegram, max_sends=4)
        )

    assert first["tendwire_turn_final"]["acked"] == 1
    _entry_key, entry = state.find_worker_entry_by_stable_key(
        current, _stable_key("worker-1")
    )
    assert entry is not None
    entry["topic_id"] = attempted_topic_id
    for binding in state.message_bindings(current).values():
        if (
            isinstance(binding, dict)
            and binding.get("turn_id") == turn_id
        ):
            binding["topic_id"] = attempted_topic_id
            binding.pop("routing_quarantined", None)
    ack_count = len(tendwire.ack_calls)
    tendwire.row = _turn_row(
        turn_id,
        revision,
        "after successful rebind",
    )
    telegram.race_revision = True
    state.save_state(current, state_path)

    with state.state_lock(path=state_path):
        current = state.load_state(state_path)
        raced = sync_once(
            current, _runtime(tendwire, telegram, max_sends=4)
        )

    revision_bindings = [
        (message_id, binding)
        for message_id, binding in state.message_bindings(
            current
        ).items()
        if (
            isinstance(binding, dict)
            and binding.get("content_revision") == revision
        )
    ]
    _entry_key, entry = state.find_worker_entry_by_stable_key(
        current, _stable_key("worker-1")
    )
    assert telegram.edit_attempts == 1
    assert telegram.revision_send_topics == [
        attempted_topic_id
    ]
    assert raced["tendwire_turn_final"]["deferred"] == 1
    assert raced["tendwire_turn_final"]["acked"] == 0
    assert len(tendwire.ack_calls) == ack_count
    assert entry is not None
    assert entry["topic_id"] == rebound_topic_id
    assert len(revision_bindings) == 1
    assert revision_bindings[0][1]["topic_id"] == (
        attempted_topic_id
    )

    telegram.race_revision = False
    with state.state_lock(path=state_path):
        current = state.load_state(state_path)
        reconciled = sync_once(
            current, _runtime(tendwire, telegram, max_sends=4)
        )
    with state.state_lock(path=state_path):
        current = state.load_state(state_path)
        stable = sync_once(
            current, _runtime(tendwire, telegram, max_sends=4)
        )

    current_revision_bindings = [
        binding
        for binding in state.message_bindings(current).values()
        if (
            isinstance(binding, dict)
            and binding.get("content_revision") == revision
        )
    ]
    assert reconciled["tendwire_turn_final"]["acked"] == 1
    assert stable["tendwire_turn_final"]["polled"] == 0
    assert telegram.revision_send_topics == [
        attempted_topic_id,
        rebound_topic_id,
    ]
    assert len(tendwire.ack_calls) == ack_count + 1
    assert len(current_revision_bindings) == 1
    assert current_revision_bindings[0]["topic_id"] == (
        rebound_topic_id
    )


def test_turn_final_rebind_during_ack_records_local_reconciliation(
    tmp_path, monkeypatch
):
    monkeypatch.setenv("HERDRES_TENDWIRE_MODE", "source")
    monkeypatch.setenv("HERDRES_SOURCE_TOPIC_MODE", "worker")
    monkeypatch.setenv("HERDRES_PINNED_STATUS", "0")
    monkeypatch.setenv("HERDR_TELEGRAM_TOPICS_STATUS_ICON", "0")
    monkeypatch.setenv(
        "HERDR_TELEGRAM_TOPICS_RICH_MESSAGES", "0"
    )
    state_path = tmp_path / "state.json"
    monkeypatch.setenv(
        "HERDR_TELEGRAM_TOPICS_STATE", str(state_path)
    )
    turn_id = "turn-rebind-during-ack"
    revision = "twrev1.rebind_during_ack"

    class RebindingAckTendwire(TurnFinalTendwire):
        def __init__(self, row):
            super().__init__(row)
            self.rebound = False

        def turn_final_ack(self, ref, response=None):
            result = super().turn_final_ack(ref, response)
            if not self.rebound:
                concurrent = state.load_state(state_path)
                _key, entry = state.find_worker_entry_by_stable_key(
                    concurrent, _stable_key("worker-1")
                )
                assert entry is not None
                entry["topic_id"] = "16000"
                state.save_state(concurrent, state_path)
                self.rebound = True
            return result

    tendwire = RebindingAckTendwire(
        _turn_row(turn_id, revision, "ack race answer")
    )
    telegram = DeletingTelegram()
    store = _store()
    state.save_state(store, state_path)

    with state.state_lock(path=state_path):
        current = state.load_state(state_path)
        raced = sync_once(
            current, _runtime(tendwire, telegram, max_sends=8)
        )

    receipt = next(iter(state.tendwire_turn_jobs(current).values()))
    assert raced["tendwire_turn_final"]["acked"] == 1
    assert raced["tendwire_turn_final"]["deferred"] == 0
    assert tendwire.defer_calls == []
    assert receipt["substate"] == "acknowledged"
    assert receipt["post_ack_reconcile"]["status"] == "reconcile"
    assert len(tendwire.ack_calls) == 1
    assert [sent[2]["thread_id"] for sent in telegram.sent] == ["77"]

    real_planner = source_sync.prepare_turn_delivery_parts
    monkeypatch.setattr(
        source_sync,
        "prepare_turn_delivery_parts",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            source_sync.PresentationContentError(
                "synthetic reconciliation presentation failure"
            )
        ),
    )
    with state.state_lock(path=state_path):
        current = state.load_state(state_path)
        blocked = sync_once(
            current, _runtime(tendwire, telegram, max_sends=8)
        )
        state.save_state(current, state_path)

    persisted = state.load_state(state_path)
    receipt = next(
        iter(state.tendwire_turn_jobs(persisted).values())
    )
    assert blocked["ok"] is False
    assert blocked["status"] == "outbound_delivery_stalled"
    assert (
        blocked["tendwire_turn_final"]["status"]
        == "invalid_presentation_plan"
    )
    assert receipt["post_ack_reconcile"]["planning_error"] == {
        "status": "invalid_presentation_plan",
        "error": "synthetic reconciliation presentation failure",
        "attempts": 1,
    }
    assert len(tendwire.ack_calls) == 1
    monkeypatch.setattr(
        source_sync,
        "prepare_turn_delivery_parts",
        real_planner,
    )

    with state.state_lock(path=state_path):
        current = state.load_state(state_path)
        healed = sync_once(
            current, _runtime(tendwire, telegram, max_sends=8)
        )

    receipt = next(iter(state.tendwire_turn_jobs(current).values()))
    bindings = [
        binding
        for binding in state.message_bindings(current).values()
        if (
            isinstance(binding, dict)
            and binding.get("content_revision") == revision
        )
    ]
    assert healed["tendwire_turn_final"]["polled"] == 0
    assert healed["tendwire_turn_final"]["post_ack_reconciled"] == 1
    assert receipt.get("post_ack_reconcile") is None
    assert len(tendwire.ack_calls) == 1
    assert tendwire.defer_calls == []
    assert [sent[2]["thread_id"] for sent in telegram.sent] == [
        "77",
        "16000",
    ]
    assert len(bindings) == 1
    assert bindings[0]["topic_id"] == "16000"


def test_suppressed_turn_final_rebind_during_ack_reconciles_locally(
    tmp_path, monkeypatch
):
    monkeypatch.setenv("HERDRES_TENDWIRE_MODE", "source")
    monkeypatch.setenv("HERDRES_SOURCE_TOPIC_MODE", "worker")
    monkeypatch.setenv("HERDRES_PINNED_STATUS", "0")
    monkeypatch.setenv("HERDR_TELEGRAM_TOPICS_STATUS_ICON", "0")
    monkeypatch.setenv(
        "HERDR_TELEGRAM_TOPICS_RICH_MESSAGES", "0"
    )
    state_path = tmp_path / "state.json"
    monkeypatch.setenv(
        "HERDR_TELEGRAM_TOPICS_STATE", str(state_path)
    )
    turn_id = "turn-suppressed-rebind-during-ack"
    revision = "twrev1.suppressed_rebind_during_ack"

    class RebindingSuppressedAckTendwire(TurnFinalTendwire):
        def __init__(self, row):
            super().__init__(row)
            self.rebound = False

        def turn_final_ack(self, ref, response=None):
            result = super().turn_final_ack(ref, response)
            if not self.rebound:
                concurrent = state.load_state(state_path)
                _key, entry = state.find_worker_entry_by_stable_key(
                    concurrent, _stable_key("worker-1")
                )
                assert entry is not None
                entry["topic_id"] = "16000"
                state.save_state(concurrent, state_path)
                self.rebound = True
            return result

    tendwire = RebindingSuppressedAckTendwire(
        _turn_row(turn_id, revision, "suppressed answer")
    )
    telegram = DeletingTelegram()
    state.save_state(_store(), state_path)

    # Stage the real durable plan without draining its job, then mark the
    # historical-plan suppression that the real catch-up path consumes.
    with state.state_lock(path=state_path):
        current = state.load_state(state_path)
        staged = sync_once(
            current,
            SyncRuntime(
                tendwire,
                telegram,
                with_outbox=False,
                max_sends=8,
            ),
        )
        _key, entry = state.find_worker_entry_by_stable_key(
            current, _stable_key("worker-1")
        )
        assert entry is not None
        plan_token = str(entry["pending_plan_token"])
        entry["pending_turn_suppressed"] = {
            "plan_token": plan_token,
            "turn_id": turn_id,
            "content_revision": revision,
            "reason": "test_historical_suppression",
        }
        state.save_state(current, state_path)

    assert staged["tendwire_turn_final"]["polled"] == 0

    with state.state_lock(path=state_path):
        current = state.load_state(state_path)
        raced = sync_once(
            current, _runtime(tendwire, telegram, max_sends=8)
        )

    receipt = next(iter(state.tendwire_turn_jobs(current).values()))
    assert raced["tendwire_turn_final"]["acked"] == 1
    assert raced["tendwire_turn_final"]["deferred"] == 0
    assert tendwire.defer_calls == []
    assert len(tendwire.ack_calls) == 1
    assert receipt["substate"] == "acknowledged"
    assert receipt["post_ack_reconcile"]["kind"] == "suppressed"
    assert receipt["post_ack_reconcile"]["status"] == "reconcile"
    assert telegram.sent == []

    with state.state_lock(path=state_path):
        current = state.load_state(state_path)
        healed = sync_once(
            current, _runtime(tendwire, telegram, max_sends=8)
        )

    receipt = next(iter(state.tendwire_turn_jobs(current).values()))
    _key, entry = state.find_worker_entry_by_stable_key(
        current, _stable_key("worker-1")
    )
    assert entry is not None
    assert healed["tendwire_turn_final"]["polled"] == 0
    assert len(tendwire.ack_calls) == 1
    assert tendwire.defer_calls == []
    assert receipt["substate"] == "acknowledged"
    assert receipt.get("post_ack_reconcile") is None
    assert entry["topic_id"] == "16000"
    assert "pending_turn_suppressed" not in entry
    assert "pending_plan_token" not in entry
    delivered = state.delivered_turns(current)
    assert delivered[f"final:{turn_id}:{revision}"]["suppressed"] is True

    with state.state_lock(path=state_path):
        current = state.load_state(state_path)
        stable = sync_once(
            current, _runtime(tendwire, telegram, max_sends=8)
        )
    assert stable["tendwire_turn_final"]["polled"] == 0
    assert stable["tendwire_turn_final"]["acked"] == 0
    assert len(tendwire.ack_calls) == 1


def test_stage_final_plan_returns_current_owner_for_suppression_marker(
    tmp_path, monkeypatch
):
    monkeypatch.setenv("HERDRES_TENDWIRE_MODE", "source")
    state_path = tmp_path / "state.json"
    monkeypatch.setenv("HERDR_TELEGRAM_TOPICS_STATE", str(state_path))
    turn_id = "turn-stage-suppressed-owner"
    revision = "twrev1.stage_suppressed_owner"
    item = _turn_row(turn_id, revision, "historical answer")
    tendwire = TurnFinalTendwire(item)
    initial = _store()
    _key, initial_entry, _created = state.upsert_worker_entry(
        initial,
        _source_worker(tendwire.snapshot()["workers"][0]),
    )
    initial_entry["topic_id"] = "77"
    state.save_state(initial, state_path)

    with state.state_lock(state_path):
        current = state.load_state(state_path)
        _key, old_entry = state.find_worker_entry_by_stable_key(
            current, _stable_key("worker-1")
        )
        assert old_entry is not None
        runtime = source_sync._offlock_runtime(
            current,
            _runtime(tendwire, DeletingTelegram(), max_sends=8),
        )
        staged, _pages, entry = source_sync._stage_final_plan(
            current, item, old_entry, runtime
        )
        assert staged is True
        assert entry is not old_entry
        entry["pending_turn_suppressed"] = {
            "plan_token": str(entry["pending_plan_token"]),
            "turn_id": turn_id,
            "content_revision": revision,
            "reason": "rebind_catchup_older_than_bound",
        }
        assert source_sync._suppressed_turn_plan(
            entry,
            str(entry["pending_plan_token"]),
            revision,
        )
        state.save_state(current, state_path)

    persisted = state.load_state(state_path)
    _key, persisted_entry = state.find_worker_entry_by_stable_key(
        persisted, _stable_key("worker-1")
    )
    assert persisted_entry is not None
    assert persisted_entry["pending_turn_suppressed"]["turn_id"] == turn_id


def test_turn_final_revalidates_after_old_copy_retirement(
    tmp_path, monkeypatch
):
    monkeypatch.setenv("HERDRES_TENDWIRE_MODE", "source")
    monkeypatch.setenv("HERDRES_SOURCE_TOPIC_MODE", "worker")
    monkeypatch.setenv("HERDRES_PINNED_STATUS", "0")
    monkeypatch.setenv("HERDR_TELEGRAM_TOPICS_STATUS_ICON", "0")
    monkeypatch.setenv(
        "HERDR_TELEGRAM_TOPICS_RICH_MESSAGES", "0"
    )
    state_path = tmp_path / "state.json"
    monkeypatch.setenv(
        "HERDR_TELEGRAM_TOPICS_STATE", str(state_path)
    )
    turn_id = "turn-retire-route-race"
    revision = "twrev1.retire_route_race"
    tendwire = TurnFinalTendwire(
        _turn_row(turn_id, "twrev1.retire_base", "base")
    )
    store = _store()
    state.save_state(store, state_path)

    class RebindDuringRetireTelegram(DeletingTelegram):
        def __init__(self):
            super().__init__()
            self.race_retire = False
            self.rebound = False
            self.revision_topics = []

        def send_message(self, chat_id, html, **kwargs):
            result = super().send_message(chat_id, html, **kwargs)
            if "replacement after retire race" in html:
                self.revision_topics.append(
                    str(kwargs.get("thread_id") or "")
                )
            return result

        def delete_message(self, chat_id, message_id):
            result = super().delete_message(chat_id, message_id)
            if self.race_retire and not self.rebound:
                concurrent = state.load_state(state_path)
                _key, rebound = state.find_worker_entry_by_stable_key(
                    concurrent, _stable_key("worker-1")
                )
                assert rebound is not None
                rebound["topic_id"] = "17000"
                state.save_state(concurrent, state_path)
                self.rebound = True
            return result

    telegram = RebindDuringRetireTelegram()
    with state.state_lock(path=state_path):
        current = state.load_state(state_path)
        initial = sync_once(
            current, _runtime(tendwire, telegram, max_sends=8)
        )
    assert initial["tendwire_turn_final"]["acked"] == 1

    _key, entry = state.find_worker_entry_by_stable_key(
        current, _stable_key("worker-1")
    )
    assert entry is not None
    entry["topic_id"] = "16000"
    for binding in state.message_bindings(current).values():
        if isinstance(binding, dict) and binding.get("turn_id") == turn_id:
            binding["topic_id"] = "15007"
    tendwire.row = _turn_row(
        turn_id, revision, "replacement after retire race"
    )
    telegram.race_retire = True
    state.save_state(current, state_path)
    ack_count = len(tendwire.ack_calls)

    with state.state_lock(path=state_path):
        current = state.load_state(state_path)
        raced = sync_once(
            current, _runtime(tendwire, telegram, max_sends=8)
        )

    receipt = next(
        receipt
        for receipt in state.tendwire_turn_jobs(current).values()
        if receipt.get("content_revision") == revision
    )
    stale = state.tendwire_turn_job_stale_copies(receipt)
    assert raced["tendwire_turn_final"]["acked"] == 0
    assert raced["tendwire_turn_final"]["deferred"] == 1
    assert len(tendwire.ack_calls) == ack_count
    assert [copy["topic_id"] for copy in stale] == ["16000"]

    telegram.race_retire = False
    with state.state_lock(path=state_path):
        current = state.load_state(state_path)
        healed = sync_once(
            current, _runtime(tendwire, telegram, max_sends=8)
        )

    receipt = next(
        receipt
        for receipt in state.tendwire_turn_jobs(current).values()
        if receipt.get("content_revision") == revision
    )
    assert healed["tendwire_turn_final"]["acked"] == 1
    assert len(tendwire.ack_calls) == ack_count + 1
    assert telegram.revision_topics == ["16000", "17000"]
    assert state.tendwire_turn_job_stale_copies(receipt) == []
    assert {
        binding["topic_id"]
        for binding in state.message_bindings(current).values()
        if (
            isinstance(binding, dict)
            and binding.get("content_revision") == revision
        )
    } == {"17000"}


def test_turn_final_tracks_two_consecutive_route_changes(
    tmp_path, monkeypatch
):
    monkeypatch.setenv("HERDRES_TENDWIRE_MODE", "source")
    monkeypatch.setenv("HERDRES_SOURCE_TOPIC_MODE", "worker")
    monkeypatch.setenv("HERDRES_PINNED_STATUS", "0")
    monkeypatch.setenv("HERDR_TELEGRAM_TOPICS_STATUS_ICON", "0")
    monkeypatch.setenv(
        "HERDR_TELEGRAM_TOPICS_RICH_MESSAGES", "0"
    )
    state_path = tmp_path / "state.json"
    monkeypatch.setenv(
        "HERDR_TELEGRAM_TOPICS_STATE", str(state_path)
    )
    turn_id = "turn-two-route-races"
    revision = "twrev1.two_route_races"
    tendwire = TurnFinalTendwire(
        _turn_row(turn_id, "twrev1.two_route_base", "base")
    )
    store = _store()
    state.save_state(store, state_path)

    class TwiceRebindingTelegram(DeletingTelegram):
        def __init__(self):
            super().__init__()
            self.revision = False
            self.edit_missing = False
            self.moves = ["16000", "17000"]
            self.revision_topics = []

        def edit_message(self, chat_id, message_id, html):
            if self.revision and not self.edit_missing:
                self.edit_missing = True
                return {
                    "ok": False,
                    "kind": "not_found",
                    "error": "Bad Request: message to edit not found",
                }
            return super().edit_message(chat_id, message_id, html)

        def send_message(self, chat_id, html, **kwargs):
            result = super().send_message(chat_id, html, **kwargs)
            if self.revision and "two route changes" in html:
                topic_id = str(kwargs.get("thread_id") or "")
                self.revision_topics.append(topic_id)
                if self.moves:
                    concurrent = state.load_state(state_path)
                    _key, rebound = (
                        state.find_worker_entry_by_stable_key(
                            concurrent, _stable_key("worker-1")
                        )
                    )
                    assert rebound is not None
                    rebound["topic_id"] = self.moves.pop(0)
                    state.save_state(concurrent, state_path)
            return result

    telegram = TwiceRebindingTelegram()
    with state.state_lock(path=state_path):
        current = state.load_state(state_path)
        initial = sync_once(
            current, _runtime(tendwire, telegram, max_sends=8)
        )
    assert initial["tendwire_turn_final"]["acked"] == 1
    _key, entry = state.find_worker_entry_by_stable_key(
        current, _stable_key("worker-1")
    )
    assert entry is not None
    entry["topic_id"] = "15007"
    for binding in state.message_bindings(current).values():
        if isinstance(binding, dict) and binding.get("turn_id") == turn_id:
            binding["topic_id"] = "15007"
    tendwire.row = _turn_row(
        turn_id, revision, "two route changes"
    )
    telegram.revision = True
    state.save_state(current, state_path)
    ack_count = len(tendwire.ack_calls)

    stale_counts = []
    for _pass in range(2):
        with state.state_lock(path=state_path):
            current = state.load_state(state_path)
            raced = sync_once(
                current, _runtime(tendwire, telegram, max_sends=8)
            )
        receipt = next(
            receipt
            for receipt in state.tendwire_turn_jobs(current).values()
            if receipt.get("content_revision") == revision
        )
        stale_counts.append(
            len(state.tendwire_turn_job_stale_copies(receipt))
        )
        assert raced["tendwire_turn_final"]["acked"] == 0
        assert raced["tendwire_turn_final"]["deferred"] == 1

    with state.state_lock(path=state_path):
        current = state.load_state(state_path)
        healed = sync_once(
            current, _runtime(tendwire, telegram, max_sends=8)
        )

    receipt = next(
        receipt
        for receipt in state.tendwire_turn_jobs(current).values()
        if receipt.get("content_revision") == revision
    )
    assert stale_counts == [1, 2]
    assert healed["tendwire_turn_final"]["acked"] == 1
    assert len(tendwire.ack_calls) == ack_count + 1
    assert telegram.revision_topics == ["15007", "16000", "17000"]
    assert state.tendwire_turn_job_stale_copies(receipt) == []
    assert {
        binding["topic_id"]
        for binding in state.message_bindings(current).values()
        if (
            isinstance(binding, dict)
            and binding.get("content_revision") == revision
        )
    } == {"17000"}


def test_turn_final_stale_copy_backpressure_bounds_sustained_churn(
    tmp_path, monkeypatch
):
    monkeypatch.setenv("HERDRES_TENDWIRE_MODE", "source")
    monkeypatch.setenv("HERDRES_SOURCE_TOPIC_MODE", "worker")
    monkeypatch.setenv("HERDRES_PINNED_STATUS", "0")
    monkeypatch.setenv("HERDR_TELEGRAM_TOPICS_STATUS_ICON", "0")
    monkeypatch.setenv(
        "HERDR_TELEGRAM_TOPICS_RICH_MESSAGES", "0"
    )
    state_path = tmp_path / "state.json"
    monkeypatch.setenv(
        "HERDR_TELEGRAM_TOPICS_STATE", str(state_path)
    )
    turn_id = "turn-sustained-route-churn"
    revision = "twrev1.sustained_route_churn"
    tendwire = TurnFinalTendwire(
        _turn_row(turn_id, "twrev1.churn_base", "base")
    )
    store = _store()
    state.save_state(store, state_path)

    class FlappingTelegram(DeletingTelegram):
        def __init__(self):
            super().__init__()
            self.revision = False
            self.edit_missing = False
            self.moves = [
                str(16000 + index)
                for index in range(
                    state.TENDWIRE_TURN_JOB_STALE_COPY_LIMIT + 3
                )
            ]
            self.revision_topics = []

        def edit_message(self, chat_id, message_id, html):
            if self.revision and not self.edit_missing:
                self.edit_missing = True
                return {
                    "ok": False,
                    "kind": "not_found",
                    "error": "Bad Request: message to edit not found",
                }
            return super().edit_message(chat_id, message_id, html)

        def send_message(self, chat_id, html, **kwargs):
            result = super().send_message(chat_id, html, **kwargs)
            if self.revision and "sustained churn" in html:
                self.revision_topics.append(
                    str(kwargs.get("thread_id") or "")
                )
                if self.moves:
                    concurrent = state.load_state(state_path)
                    _key, entry = state.find_worker_entry_by_stable_key(
                        concurrent, _stable_key("worker-1")
                    )
                    assert entry is not None
                    entry["topic_id"] = self.moves.pop(0)
                    state.save_state(concurrent, state_path)
            return result

    telegram = FlappingTelegram()
    with state.state_lock(path=state_path):
        current = state.load_state(state_path)
        initial = sync_once(
            current, _runtime(tendwire, telegram, max_sends=20)
        )
    assert initial["tendwire_turn_final"]["acked"] == 1
    _key, entry = state.find_worker_entry_by_stable_key(
        current, _stable_key("worker-1")
    )
    assert entry is not None
    entry["topic_id"] = "15007"
    for binding in state.message_bindings(current).values():
        if isinstance(binding, dict) and binding.get("turn_id") == turn_id:
            binding["topic_id"] = "15007"
    tendwire.row = _turn_row(
        turn_id, revision, "sustained churn"
    )
    telegram.revision = True
    state.save_state(current, state_path)
    ack_count = len(tendwire.ack_calls)

    observed_stale_counts = []
    healed = None
    for _pass in range(
        2 * state.TENDWIRE_TURN_JOB_STALE_COPY_LIMIT + 12
    ):
        with state.state_lock(path=state_path):
            current = state.load_state(state_path)
            result = sync_once(
                current, _runtime(tendwire, telegram, max_sends=20)
            )
        receipt = next(
            receipt
            for receipt in state.tendwire_turn_jobs(current).values()
            if receipt.get("content_revision") == revision
        )
        observed_stale_counts.append(
            len(state.tendwire_turn_job_stale_copies(receipt))
        )
        if result["tendwire_turn_final"]["acked"] == 1:
            healed = result
            break

    assert healed is not None
    assert max(observed_stale_counts) <= (
        state.TENDWIRE_TURN_JOB_STALE_COPY_LIMIT
    )
    assert len(tendwire.ack_calls) == ack_count + 1
    assert state.tendwire_turn_job_stale_copies(receipt) == []
    assert len(telegram.revision_topics) == (
        state.TENDWIRE_TURN_JOB_STALE_COPY_LIMIT + 4
    )
    assert len(
        [
            binding
            for binding in state.message_bindings(current).values()
            if (
                isinstance(binding, dict)
                and binding.get("content_revision") == revision
            )
        ]
    ) == 1


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


def test_physical_budget_is_exact_and_acceptance_loss_is_explicit(monkeypatch):
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


def test_table_delivery_operation_budget_matches_single_plain_write(monkeypatch):
    monkeypatch.setenv("HERDRES_TENDWIRE_MODE", "source")
    monkeypatch.setenv("HERDRES_PINNED_STATUS", "0")
    monkeypatch.setenv("HERDRES_FORCE_PLAIN_DELIVERY", "1")
    table = (
        "| Name | Status |\n"
        "| --- | --- |\n"
        "| Ada | Ready |"
    )
    tendwire = TurnFinalTendwire(
        _turn_row("turn-table-budget", "twrev1.tablebudget", table)
    )
    telegram = DeletingTelegram()

    result = sync_once(
        _store(), _runtime(tendwire, telegram, max_sends=1)
    )

    assert result["tendwire_turn_final"]["operations"] == 1
    assert result["tendwire_turn_final"]["acked"] == 1
    assert len(telegram.sent) == 1
    assert "Name | Status" in telegram.sent[0][1]
    assert "Ada | Ready" in telegram.sent[0][1]
    assert not any(
        method == "sendRichMessage"
        for method, _payload, _token in telegram.api_calls
    )


def _large_markdown_table(
    row_count: int, *, value_chars: int = 8
) -> tuple[str, list[str]]:
    rows = [
        (
            f"row-{index:03d} | "
            f"value-{index:03d}-{'x' * value_chars}"
        )
        for index in range(1, row_count + 1)
    ]
    source = "\n".join(
        [
            "| Name | Value |",
            "| --- | --- |",
            *[f"| {row} |" for row in rows],
        ]
    )
    return source, rows


def _header_only_table(source_chars: int) -> tuple[str, str]:
    wrapper = "|  |\n| --- |"
    assert source_chars > len(wrapper)
    header = "H" * (source_chars - len(wrapper))
    return f"| {header} |\n| --- |", header


def test_canonical_table_delivers_final_row_before_ack(monkeypatch):
    monkeypatch.setenv("HERDRES_TENDWIRE_MODE", "source")
    monkeypatch.setenv("HERDRES_PINNED_STATUS", "0")
    monkeypatch.setenv("HERDRES_FORCE_PLAIN_DELIVERY", "1")
    table, rows = _large_markdown_table(50)
    telegram = DeletingTelegram()

    class AckAfterFinalRowTendwire(TurnFinalTendwire):
        def turn_final_ack(self, ref, response=None):
            assert any(rows[-1] in message[1] for message in telegram.sent)
            return super().turn_final_ack(ref, response=response)

    tendwire = AckAfterFinalRowTendwire(
        _turn_row("turn-table-50", "twrev1.table50", table)
    )

    result = sync_once(
        _store(), _runtime(tendwire, telegram, max_sends=100)
    )

    assert result["tendwire_turn_final"]["operations"] == 1
    assert result["tendwire_turn_final"]["acked"] == 1
    assert len(telegram.sent) == 1
    assert rows[-1] in telegram.sent[0][1]


@pytest.mark.parametrize("source_chars", [3_400, 3_401])
def test_canonical_table_boundary_sizes_deliver_without_planning_escape(
    monkeypatch, source_chars
):
    monkeypatch.setenv("HERDRES_TENDWIRE_MODE", "source")
    monkeypatch.setenv("HERDRES_PINNED_STATUS", "0")
    table, header = _header_only_table(source_chars)
    tendwire = TurnFinalTendwire(
        _turn_row(
            f"turn-table-{source_chars}",
            f"twrev1.table{source_chars}",
            table,
        )
    )
    telegram = DeletingTelegram()

    result = sync_once(
        _store(), _runtime(tendwire, telegram, max_sends=100)
    )
    payload = "".join(message[1] for message in telegram.sent)

    assert result["ok"] is True
    assert result["tendwire_turn_final"]["failed"] == 0
    assert result["tendwire_turn_final"]["acked"] >= 1
    assert payload.count("H") == len(header)


@pytest.mark.parametrize(
    ("revision", "table", "marker", "expected_count"),
    [
        (
            "twrev1.oversized_data",
            "| Name | Value |\n| --- | --- |\n| row | "
            + ("D" * 5_000)
            + " |",
            "D",
            5_000,
        ),
        (
            "twrev1.oversized_header",
            _header_only_table(3_850)[0],
            "H",
            len(_header_only_table(3_850)[1]),
        ),
    ],
)
def test_oversized_table_row_or_header_delivers_losslessly(
    monkeypatch, revision, table, marker, expected_count
):
    monkeypatch.setenv("HERDRES_TENDWIRE_MODE", "source")
    monkeypatch.setenv("HERDRES_PINNED_STATUS", "0")
    tendwire = TurnFinalTendwire(
        _turn_row(
            f"turn-{revision.rsplit('.', 1)[-1]}",
            revision,
            table,
        )
    )
    telegram = DeletingTelegram()

    result = sync_once(
        _store(), _runtime(tendwire, telegram, max_sends=100)
    )
    payloads = [message[1] for message in telegram.sent]

    assert result["ok"] is True
    assert result["tendwire_turn_final"]["failed"] == 0
    assert result["tendwire_turn_final"]["acked"] == len(payloads)
    assert len(payloads) > 1
    assert sum(payload.count(marker) for payload in payloads) == expected_count


def test_fitting_large_header_repeats_on_every_continuation(
    monkeypatch,
):
    monkeypatch.setenv("HERDRES_TENDWIRE_MODE", "source")
    monkeypatch.setenv("HERDRES_PINNED_STATUS", "0")
    header = "H" * 3_300
    table = "\n".join(
        [
            f"| {header} |",
            "| --- |",
            "| short |",
            f"| {'D' * 250} |",
        ]
    )
    tendwire = TurnFinalTendwire(
        _turn_row(
            "turn-fitting-large-header",
            "twrev1.fitting_large_header",
            table,
        )
    )
    telegram = DeletingTelegram()

    result = sync_once(
        _store(), _runtime(tendwire, telegram, max_sends=100)
    )
    payloads = [message[1] for message in telegram.sent]

    assert result["tendwire_turn_final"]["failed"] == 0
    assert result["tendwire_turn_final"]["acked"] == 2
    assert len(payloads) == 2
    assert [payload.count("H") for payload in payloads] == [
        len(header),
        len(header),
    ]


def test_oversized_continued_header_is_not_repeated_past_effective_limit(
    monkeypatch,
):
    monkeypatch.setenv("HERDRES_TENDWIRE_MODE", "source")
    monkeypatch.setenv("HERDRES_PINNED_STATUS", "0")
    header = "H" * 3_850
    table = "\n".join(
        [
            f"| {header} |",
            "| --- |",
            f"| {'D' * 4_000} |",
        ]
    )
    tendwire = TurnFinalTendwire(
        _turn_row(
            "turn-oversized-continued-header",
            "twrev1.oversized_continued_header",
            table,
        )
    )
    telegram = DeletingTelegram()

    result = sync_once(
        _store(), _runtime(tendwire, telegram, max_sends=100)
    )
    payloads = [message[1] for message in telegram.sent]

    assert result["tendwire_turn_final"]["failed"] == 0
    assert result["tendwire_turn_final"]["acked"] == len(payloads)
    assert len(payloads) == 3
    assert sum(payload.count("H") for payload in payloads) == len(
        header
    )
    assert payloads[-1].count("H") == 0


def test_split_table_repeats_header_and_preserves_each_complete_row(
    monkeypatch,
):
    monkeypatch.setenv("HERDRES_TENDWIRE_MODE", "source")
    monkeypatch.setenv("HERDRES_PINNED_STATUS", "0")
    monkeypatch.setenv("HERDRES_FORCE_PLAIN_DELIVERY", "1")
    table, rows = _large_markdown_table(90, value_chars=80)
    tendwire = TurnFinalTendwire(
        _turn_row("turn-table-split", "twrev1.tablesplit", table)
    )
    telegram = DeletingTelegram()

    result = sync_once(
        _store(), _runtime(tendwire, telegram, max_sends=100)
    )
    payloads = [message[1] for message in telegram.sent]

    assert len(payloads) > 1
    assert result["tendwire_turn_final"]["operations"] == len(payloads)
    assert result["tendwire_turn_final"]["acked"] == len(payloads)
    assert all("Name | Value" in payload for payload in payloads)
    for row in rows:
        assert sum(row in payload for payload in payloads) == 1
    assert rows[-1] in payloads[-1]


def test_table_aware_split_keeps_near_limit_row_out_of_prior_part(
    monkeypatch,
):
    monkeypatch.setenv("HERDRES_TENDWIRE_MODE", "source")
    monkeypatch.setenv("HERDRES_PINNED_STATUS", "0")
    monkeypatch.setenv("HERDRES_FORCE_PLAIN_DELIVERY", "1")
    short_row = "short | first"
    crossing_row = "crossing | " + ("X" * 3_350)
    final_row = "final | last"
    table = "\n".join(
        [
            "| Name | Value |",
            "| --- | --- |",
            f"| {short_row} |",
            f"| {crossing_row} |",
            f"| {final_row} |",
        ]
    )
    tendwire = TurnFinalTendwire(
        _turn_row(
            "turn-table-row-boundary",
            "twrev1.table_row_boundary",
            table,
        )
    )
    telegram = DeletingTelegram()

    result = sync_once(
        _store(), _runtime(tendwire, telegram, max_sends=100)
    )
    payloads = [message[1] for message in telegram.sent]

    assert result["tendwire_turn_final"]["failed"] == 0
    assert len(payloads) == 2
    assert short_row in payloads[0]
    assert "crossing | " not in payloads[0]
    assert crossing_row in payloads[1]
    assert final_row in payloads[1]
    assert all("Name | Value" in payload for payload in payloads)


def test_content_presentation_failure_records_exact_failure_and_sync_continues(
    monkeypatch,
):
    monkeypatch.setenv("HERDRES_TENDWIRE_MODE", "source")
    monkeypatch.setenv("HERDRES_PINNED_STATUS", "0")
    row = _turn_row(
        "turn-planning-failure",
        "twrev1.planning_failure",
        "must not be reported delivered",
    )
    tendwire = MultiTurnFinalTendwire([row])
    tendwire.enable_attention()
    telegram = DeletingTelegram()
    monkeypatch.setattr(
        source_sync,
        "prepare_turn_delivery_parts",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            source_sync.PresentationContentError(
                "synthetic content presentation failure"
            )
        ),
    )

    result = sync_once(
        _store(), _runtime(tendwire, telegram, max_sends=100)
    )

    assert result["ok"] is True
    assert result["tendwire_turn_final"]["status"] == (
        "invalid_presentation_plan"
    )
    assert result["tendwire_turn_final"]["failed"] == 1
    assert result["tendwire_turn_final"]["delivered"] == 0
    assert result["tendwire_turn_final"]["acked"] == 0
    assert tendwire.fail_calls[-1][1] == "invalid_presentation_plan"
    assert tendwire.attention_acked == [
        ("twref1.attention", {"telegram": "delivered"})
    ]
    assert not any(
        "must not be reported delivered" in message[1]
        for message in telegram.sent
    )


@pytest.mark.parametrize(
    "error",
    [
        AttributeError("synthetic planner attribute defect"),
        TypeError("synthetic planner type defect"),
        ValueError("synthetic planner value defect"),
    ],
)
def test_programming_planning_defect_surfaces_as_systemic_failure(
    monkeypatch, error
):
    monkeypatch.setenv("HERDRES_TENDWIRE_MODE", "source")
    monkeypatch.setenv("HERDRES_PINNED_STATUS", "0")
    row = _turn_row(
        "turn-planning-defect",
        "twrev1.planning_defect",
        "must remain eligible after the loud failure",
    )
    tendwire = MultiTurnFinalTendwire([row])
    telegram = DeletingTelegram()
    monkeypatch.setattr(
        source_sync,
        "prepare_turn_delivery_parts",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(error),
    )

    with pytest.raises(type(error), match="synthetic planner"):
        sync_once(
            _store(), _runtime(tendwire, telegram, max_sends=100)
        )

    assert tendwire.fail_calls == []
    assert tendwire.ack_calls == []
    assert telegram.sent == []


def test_rich_state_uses_rich_primary_without_extra_physical_writes(
    monkeypatch,
):
    monkeypatch.setenv("HERDRES_TENDWIRE_MODE", "source")
    monkeypatch.setenv("HERDRES_PINNED_STATUS", "0")
    text = "\n\n".join("x" for _ in range(1800))
    assert len(text) == 5_398
    tendwire = TurnFinalTendwire(
        _turn_row("turn-plain-planning", "twrev1.plainplanning", text)
    )
    telegram = DeletingTelegram()
    store = _store()
    store["telegram"]["rich_messages"] = {"supported": "yes"}

    result = sync_once(
        store, _runtime(tendwire, telegram, max_sends=100)
    )

    assert result["tendwire_turn_final"]["operations"] == 2
    assert result["tendwire_turn_final"]["acked"] == 2
    assert len(telegram.sent) == 2
    assert all(
        method == "sendRichMessage"
        for method, _payload, _token in telegram.api_calls
        if method in {"sendRichMessage", "sendMessage"}
    )


def test_rich_rejection_plain_fallback_counts_both_physical_writes(
    monkeypatch,
):
    monkeypatch.setenv("HERDRES_TENDWIRE_MODE", "source")
    monkeypatch.setenv("HERDRES_PINNED_STATUS", "0")

    class RejectRichTelegram(DeletingTelegram):
        def api(self, method, payload):
            if method == "sendRichMessage":
                self.api_calls.append((method, dict(payload), self.token))
                raise TelegramError(
                    "Bad Request: rich message rejected"
                )
            return super().api(method, payload)

    tendwire = TurnFinalTendwire(
        _turn_row(
            "turn-rich-fallback",
            "twrev1.richfallback",
            "**Readable fallback**",
        )
    )
    telegram = RejectRichTelegram()

    result = sync_once(
        _store(), _runtime(tendwire, telegram, max_sends=3)
    )

    assert result["tendwire_turn_final"]["operations"] == 2
    assert result["tendwire_turn_final"]["acked"] == 1
    assert [
        method
        for method, _payload, _token in telegram.api_calls
        if method == "sendRichMessage"
    ] == ["sendRichMessage"]
    assert len(telegram.sent) == 1
    message_id = telegram.sent[0][3]
    assert telegram.recipient_messages[message_id]["format"] == "html"
    assert "Readable fallback" in telegram.recipient_messages[
        message_id
    ]["content"]


def test_turn_final_one_write_budget_stops_before_plain_variant_ladder(
    monkeypatch,
):
    monkeypatch.setenv("HERDRES_TENDWIRE_MODE", "source")
    monkeypatch.setenv("HERDRES_PINNED_STATUS", "0")
    monkeypatch.setenv("HERDRES_FORCE_PLAIN_DELIVERY", "0")
    tendwire = TurnFinalTendwire(
        _turn_row(
            "turn-one-write",
            "twrev1.one_write",
            "**Readable fallback**",
        ),
        emit_ready=True,
        turn_schema_version=2,
    )
    store = _store()
    staged = sync_once(
        store,
        SyncRuntime(
            tendwire,
            DeletingTelegram(),
            with_outbox=False,
            max_sends=100,
        ),
    )
    assert staged["tendwire_turn_final"]["enabled"] is False

    class RejectRichAndHtmlTelegram(TelegramClient):
        def __init__(self):
            super().__init__(token="test")
            object.__setattr__(self, "attempts", [])

        def api(self, method, payload):
            self.attempts.append(
                (method, str(payload.get("parse_mode") or ""))
            )
            if method == "sendRichMessage":
                raise TelegramError("Bad Request: rich rejected")
            if payload.get("parse_mode") == "HTML":
                raise TelegramError("can't parse entities")
            return {"ok": True, "result": {"message_id": 812}}

    telegram = RejectRichAndHtmlTelegram()
    result = source_sync.drain_outbound_once(
        store,
        SyncRuntime(
            tendwire,
            telegram,
            with_outbox=True,
            max_sends=100,
        ),
        chat_id="-100",
        max_operations=1,
    )

    assert result["tendwire_turn_final"]["operations"] == 1
    assert result["tendwire_turn_final"]["acked"] == 0
    assert telegram.attempts == [("sendRichMessage", "")]


class PartialLegacyFinalTelegram(DeletingTelegram):
    def __init__(self, *, ambiguous):
        super().__init__()
        self.ambiguous = ambiguous
        self.rich_attempts = 0
        self.before_partial_failure = None

    def api(self, method, payload):
        if method == "sendRichMessage":
            self.rich_attempts += 1
            if self.rich_attempts == 2:
                if self.before_partial_failure is not None:
                    self.before_partial_failure()
                self.api_calls.append((method, dict(payload), self.token))
                raise TelegramError(
                    "network timeout"
                    if self.ambiguous
                    else "Bad Request: rich part rejected"
                )
        return super().api(method, payload)

    def send_message(self, chat_id, html, **kwargs):
        if not self.ambiguous and self.rich_attempts >= 2:
            return {
                "ok": False,
                "kind": "permanent",
                "error": "plain fallback rejected",
                "physical_writes": 1,
            }
        return super().send_message(chat_id, html, **kwargs)


class RepeatedPartialLegacyFinalTelegram(DeletingTelegram):
    def __init__(self, failure_attempts):
        super().__init__()
        self.failure_attempts = set(failure_attempts)
        self.rich_attempts = 0

    def api(self, method, payload):
        if method == "sendRichMessage":
            self.rich_attempts += 1
            if self.rich_attempts in self.failure_attempts:
                self.api_calls.append((method, dict(payload), self.token))
                raise TelegramError("network timeout")
        return super().api(method, payload)


@pytest.mark.parametrize(
    ("ambiguous", "terminal_outcome"),
    [(True, "delivery_unknown"), (False, "not_delivered")],
)
def test_legacy_multipart_partial_final_is_bound_but_never_completed_or_replayed(
    monkeypatch,
    tmp_path,
    ambiguous,
    terminal_outcome,
):
    monkeypatch.setenv("HERDRES_TENDWIRE_MODE", "source")
    monkeypatch.setenv("HERDRES_PINNED_STATUS", "0")
    monkeypatch.setenv("HERDRES_FORCE_PLAIN_DELIVERY", "0")
    text = (
        "legacy multipart response paragraph\n\n" * 500
    ) + "MISSING_SUFFIX"
    row = _turn_row(
        "turn-legacy-partial",
        "twrev1.legacy_partial",
        text,
    )
    row.pop("content")
    tendwire = TurnFinalTendwire(
        row,
        turn_schema_version=1,
    )
    telegram = PartialLegacyFinalTelegram(ambiguous=ambiguous)
    store = _store()

    first = sync_once(
        store, _runtime(tendwire, telegram, max_sends=100)
    )
    attempts_after_first = telegram.rich_attempts
    entry = next(iter(state.source_worker_entries(store).values()))
    partial = entry["partial_final_delivery"]

    assert first["ok"] is False
    assert first["status"] == (
        f"partial_final_{terminal_outcome}"
    )
    assert first["feed_sent"] == 0
    assert state.delivered_turns(store) == {}
    assert tendwire.ack_calls == []
    assert partial["terminal_outcome"] == terminal_outcome
    assert partial["delivery_complete"] is False
    assert partial["message_ids"] == ["100"]
    assert partial["canonical_message_id"] == "100"
    assert partial["operator_attention_required"] is True
    assert partial["automatic_replay_authorized"] is False
    durable = state.find_partial_final_delivery(
        store,
        "turn-legacy-partial",
        source_sync._turn_content_hash(row, "final"),
    )
    assert durable is not None
    assert durable["status"] == "held"
    assert durable["recovery_action"] == (
        "accept-partial"
        if ambiguous
        else "retry-missing"
    )
    binding = state.find_message_binding(store, "100")
    assert binding is not None
    assert binding["partial_final_delivery"]["terminal_outcome"] == (
        terminal_outcome
    )
    assert list(telegram.recipient_messages) == ["100"]
    assert "MISSING_SUFFIX" not in telegram.recipient_messages["100"][
        "content"
    ]

    state_path = tmp_path / "partial-final-state.json"
    state.save_state(store, state_path)
    store = state.load_state(state_path)
    second = sync_once(
        store, _runtime(tendwire, telegram, max_sends=100)
    )
    entry = next(iter(state.source_worker_entries(store).values()))

    assert second["feed_sent"] == 0
    assert second["ok"] is False
    assert telegram.rich_attempts == attempts_after_first
    assert state.delivered_turns(store) == {}
    assert entry["last_delivery_error"]
    assert tendwire.ack_calls == []


def _legacy_partial_case(*, ambiguous):
    text = (
        "legacy multipart response paragraph\n\n" * 500
    ) + "MISSING_SUFFIX"
    row = _turn_row(
        "turn-route-independent-partial",
        "twrev1.route_independent_partial",
        text,
    )
    row.pop("content")
    tendwire = TurnFinalTendwire(row, turn_schema_version=1)
    telegram = PartialLegacyFinalTelegram(ambiguous=ambiguous)
    store = _store()
    first = sync_once(
        store, _runtime(tendwire, telegram, max_sends=100)
    )
    return store, row, tendwire, telegram, first


def _resolve_partial_with_command(
    monkeypatch,
    store,
    *,
    row,
    action,
    request_id,
    content_hash=None,
):
    monkeypatch.setattr(herdres.config, "load_env_file", lambda: None)
    monkeypatch.setattr(
        herdres.config, "require_source_mode", lambda: None
    )
    monkeypatch.setattr(
        herdres.state, "state_lock", lambda: nullcontext()
    )
    monkeypatch.setattr(herdres.state, "load_state", lambda: store)

    def save_candidate(candidate):
        saved = deepcopy(candidate)
        store.clear()
        store.update(saved)

    monkeypatch.setattr(herdres.state, "save_state", save_candidate)
    return herdres.cmd_resolve_partial_final(
        SimpleNamespace(
            turn_id=row["id"],
            content_hash=(
                content_hash
                or source_sync._turn_content_hash(row, "final")
            ),
            request_id=request_id,
            action=action,
        )
    )


def test_partial_final_hold_survives_owner_rebind_and_escalates_without_replay(
    monkeypatch,
):
    monkeypatch.setenv("HERDRES_TENDWIRE_MODE", "source")
    monkeypatch.setenv("HERDRES_PINNED_STATUS", "0")
    monkeypatch.setenv("HERDRES_FORCE_PLAIN_DELIVERY", "0")
    text = (
        "legacy multipart response paragraph\n\n" * 500
    ) + "MISSING_SUFFIX"
    row = _turn_row(
        "turn-route-independent-partial",
        "twrev1.route_independent_partial",
        text,
    )
    row.pop("content")
    tendwire = TurnFinalTendwire(row, turn_schema_version=1)
    telegram = PartialLegacyFinalTelegram(ambiguous=True)
    store = _store()

    def concurrent_rebind():
        entry = next(iter(state.source_worker_entries(store).values()))
        entry["tendwire_worker_id"] = "worker-rebound"
        entry["active_worker_id"] = "worker-rebound"
        entry["topic_id"] = "177"

    telegram.before_partial_failure = concurrent_rebind
    first = sync_once(
        store, _runtime(tendwire, telegram, max_sends=100)
    )
    assert first["ok"] is False
    attempts = telegram.rich_attempts
    prefix_ids = list(telegram.recipient_messages)
    entry = next(iter(state.source_worker_entries(store).values()))
    assert entry["tendwire_worker_id"] == "worker-rebound"
    entry.pop("partial_final_delivery", None)
    row["worker_id"] = "worker-rebound"
    tendwire.snapshot_worker_id = "worker-rebound"

    second = sync_once(
        store, _runtime(tendwire, telegram, max_sends=100)
    )

    assert second["ok"] is False
    assert telegram.rich_attempts == attempts
    assert list(telegram.recipient_messages) == prefix_ids
    assert state.delivered_turns(store) == {}
    hold = second["outbound_partial_finals"]
    assert hold["first_hold"]["turn_id"] == row["id"]
    assert hold["first_hold"]["original_topic_id"] == "77"
    assert hold["first_hold"]["current_worker_id"] == "worker-rebound"
    assert hold["first_hold"]["current_topic_id"]
    created_at = state.active_partial_final_deliveries(store)[0][
        "created_at"
    ]
    escalated = doctor.outbound_partial_finals(
        store,
        now=created_at
        + config.partial_final_escalation_seconds()
        + 1,
    )
    assert escalated["ok"] is False
    assert escalated["status"] == "partial_final_escalated"
    assert escalated["first_hold"]["turn_id"] == row["id"]
    assert state.active_partial_final_deliveries(store)


def test_partial_final_hold_blocks_a_revised_version_of_the_same_turn(
    monkeypatch,
    capsys,
):
    monkeypatch.setenv("HERDRES_TENDWIRE_MODE", "source")
    monkeypatch.setenv("HERDRES_PINNED_STATUS", "0")
    monkeypatch.setenv("HERDRES_FORCE_PLAIN_DELIVERY", "0")
    store, row, tendwire, telegram, first = _legacy_partial_case(
        ambiguous=True
    )
    prefix = deepcopy(telegram.recipient_messages)
    attempts = telegram.rich_attempts
    held_hash = source_sync._turn_content_hash(row, "final")
    revised_text = "REVISED COMPLETE FINAL MUST REMAIN HELD"
    row["assistant_final_text"] = revised_text
    revised_hash = source_sync._turn_content_hash(row, "final")

    revised = sync_once(
        store, _runtime(tendwire, telegram, max_sends=100)
    )

    assert first["ok"] is False
    assert held_hash != revised_hash
    assert revised["ok"] is False
    assert revised["feed_sent"] == 0
    assert telegram.rich_attempts == attempts
    assert telegram.recipient_messages == prefix
    assert all(
        revised_text not in message["content"]
        for message in telegram.recipient_messages.values()
    )
    hold = state.find_partial_final_delivery(
        store, row["id"], held_hash
    )
    assert hold is not None
    assert hold["content_hash"] == held_hash
    assert hold["blocked_revision_content_hash"] == revised_hash
    assert hold["status"] == "held"
    assert state.delivered_turns(store) == {}
    assert tendwire.ack_calls == []

    hold["created_at"] -= (
        config.partial_final_escalation_seconds() + 1
    )
    after_bound = sync_once(
        store, _runtime(tendwire, telegram, max_sends=100)
    )

    assert after_bound["ok"] is False
    assert after_bound["feed_sent"] == 1
    assert len(telegram.recipient_messages) == 1
    delivered_revision = telegram.recipient_messages["100"]["content"]
    assert revised_text in delivered_revision
    assert "Supersedes an incomplete earlier version" in delivered_revision
    assert "legacy multipart response paragraph" not in delivered_revision
    assert hold["status"] == "held"
    assert hold["superseded_by_content_hash"] == revised_hash
    assert hold["supersession"] == "newer_revision_delivered"
    assert hold["supersession_message_ids"] == ["100"]
    assert hold["operator_attention_required"] is True
    assert doctor.outbound_partial_finals(store)["ok"] is False

    attempts_after_revision = telegram.rich_attempts
    repeated = sync_once(
        store, _runtime(tendwire, telegram, max_sends=100)
    )
    assert repeated["ok"] is False
    assert telegram.rich_attempts == attempts_after_revision

    assert (
        _resolve_partial_with_command(
            monkeypatch,
            store,
            row=row,
            action="accept-partial",
            request_id="resolve-superseded-a",
            content_hash=held_hash,
        )
        == 0
    )
    capsys.readouterr()
    assert doctor.outbound_partial_finals(store)["ok"] is True


def test_partial_final_suffix_recovery_obeys_one_write_allowance_and_stays_held(
    monkeypatch,
    capsys,
):
    monkeypatch.setenv("HERDRES_TENDWIRE_MODE", "source")
    monkeypatch.setenv("HERDRES_PINNED_STATUS", "0")
    monkeypatch.setenv("HERDRES_FORCE_PLAIN_DELIVERY", "0")
    store, row, tendwire, telegram, first = _legacy_partial_case(
        ambiguous=False
    )
    assert first["ok"] is False
    assert (
        _resolve_partial_with_command(
            monkeypatch,
            store,
            row=row,
            action="retry-missing",
            request_id="bounded-recovery-1",
        )
        == 0
    )
    capsys.readouterr()
    attempts_before = telegram.rich_attempts
    recipient_ids_before = set(telegram.recipient_messages)

    limited = sync_once(
        store, _runtime(tendwire, telegram, max_sends=1)
    )

    assert limited["ok"] is False
    assert telegram.rich_attempts - attempts_before == 1
    assert limited["outbound_delivery"]["physical_writes"] == 1
    assert len(set(telegram.recipient_messages) - recipient_ids_before) == 1
    content_hash = source_sync._turn_content_hash(row, "final")
    hold = state.find_partial_final_delivery(
        store, row["id"], content_hash
    )
    assert hold is not None
    assert hold["status"] == "held"
    assert hold["terminal_outcome"] == "not_delivered"
    assert hold["failed_part_index"] == 2
    assert hold["message_ids"] == list(telegram.recipient_messages)
    assert state.delivered_turns(store) == {}
    assert tendwire.ack_calls == []

    attempts_after_limited = telegram.rich_attempts
    still_held = sync_once(
        store, _runtime(tendwire, telegram, max_sends=1)
    )
    assert still_held["ok"] is False
    assert telegram.rich_attempts == attempts_after_limited
    assert state.find_partial_final_delivery(
        store, row["id"], content_hash
    )["status"] == "held"

    assert (
        _resolve_partial_with_command(
            monkeypatch,
            store,
            row=row,
            action="retry-missing",
            request_id="bounded-recovery-2",
        )
        == 0
    )
    capsys.readouterr()
    completed = sync_once(
        store, _runtime(tendwire, telegram, max_sends=100)
    )
    assert completed["ok"] is True
    assert state.find_partial_final_delivery(
        store, row["id"], content_hash
    )["status"] == "resolved"
    assert state.delivered_turns(store)


def test_failed_recovery_write_consumes_budget_before_an_independent_final(
    monkeypatch,
    capsys,
):
    monkeypatch.setenv("HERDRES_TENDWIRE_MODE", "source")
    monkeypatch.setenv("HERDRES_PINNED_STATUS", "0")
    monkeypatch.setenv("HERDRES_FORCE_PLAIN_DELIVERY", "0")
    partial_text = (
        "budgeted recovery paragraph\n\n" * 500
    ) + "RECOVERY_SUFFIX"
    partial_row = _turn_row(
        "turn-budgeted-recovery",
        "twrev1.budgeted_recovery",
        partial_text,
    )
    independent_row = _turn_row(
        "turn-independent-after-recovery",
        "twrev1.independent_after_recovery",
        "INDEPENDENT FINAL MUST WAIT",
    )
    independent_row["worker_id"] = "worker-2"
    independent_row["worker_fingerprint"] = "fp-2"
    tendwire = MultiTurnFinalTendwire(
        [partial_row, independent_row]
    )
    partial_row.pop("content")
    independent_row.pop("content")
    tendwire.turn_schema_version = 1
    tendwire.rows = [partial_row]
    tendwire.turns = lambda: {
        "ok": True,
        "schema_version": 1,
        "turns": deepcopy(tendwire.rows),
    }
    tendwire.turn_final_poll = lambda **_kwargs: {
        "ok": True,
        "schema_version": 1,
        "items": [],
    }
    telegram = PartialLegacyFinalTelegram(ambiguous=False)
    store = _store()

    first = sync_once(
        store, _runtime(tendwire, telegram, max_sends=100)
    )
    assert first["ok"] is False
    assert (
        _resolve_partial_with_command(
            monkeypatch,
            store,
            row=partial_row,
            action="retry-missing",
            request_id="budget-cross-turn-1",
        )
        == 0
    )
    capsys.readouterr()
    attempts_before = telegram.rich_attempts
    tendwire.rows = [partial_row, independent_row]

    limited = sync_once(
        store, _runtime(tendwire, telegram, max_sends=1)
    )

    assert limited["ok"] is False
    assert telegram.rich_attempts - attempts_before == 1
    assert all(
        "INDEPENDENT FINAL MUST WAIT" not in message["content"]
        for message in telegram.recipient_messages.values()
    )
    independent_hash = source_sync._turn_content_hash(
        independent_row, "final"
    )
    assert (
        f"final:{independent_row['id']}:{independent_hash}"
        not in state.delivered_turns(store)
    )


def test_content_witnesses_prevent_a_b_a_partial_replay(
    monkeypatch,
):
    monkeypatch.setenv("HERDRES_TENDWIRE_MODE", "source")
    monkeypatch.setenv("HERDRES_PINNED_STATUS", "0")
    monkeypatch.setenv("HERDRES_FORCE_PLAIN_DELIVERY", "0")
    content_a = (
        "content A paragraph\n\n" * 500
    ) + "CONTENT_A_SUFFIX"
    content_b = (
        "content B paragraph\n\n" * 500
    ) + "CONTENT_B_SUFFIX"
    row = _turn_row(
        "turn-a-b-a-witness",
        "twrev1.a_b_a",
        content_a,
    )
    row.pop("content")
    tendwire = TurnFinalTendwire(row, turn_schema_version=1)
    telegram = RepeatedPartialLegacyFinalTelegram({2, 4})
    store = _store()

    first = sync_once(
        store, _runtime(tendwire, telegram, max_sends=100)
    )
    hash_a = source_sync._turn_content_hash(row, "final")
    assert first["ok"] is False
    state.resolve_partial_final_delivery(
        store,
        turn_id=row["id"],
        content_hash=hash_a,
        action="accept-partial",
        request_id="resolve-content-a",
        now=100.0,
    )

    row["assistant_final_text"] = content_b
    second = sync_once(
        store, _runtime(tendwire, telegram, max_sends=100)
    )
    hash_b = source_sync._turn_content_hash(row, "final")
    assert second["ok"] is False
    state.resolve_partial_final_delivery(
        store,
        turn_id=row["id"],
        content_hash=hash_b,
        action="accept-partial",
        request_id="resolve-content-b",
        now=200.0,
    )
    recipient_before_revert = deepcopy(telegram.recipient_messages)
    attempts_before_revert = telegram.rich_attempts

    row["assistant_final_text"] = content_a
    reverted = sync_once(
        store, _runtime(tendwire, telegram, max_sends=100)
    )

    assert reverted["ok"] is True
    assert telegram.rich_attempts == attempts_before_revert
    assert telegram.recipient_messages == recipient_before_revert
    assert state.find_partial_final_delivery(
        store, row["id"], hash_a
    )["status"] == "resolved"
    assert state.find_partial_final_delivery(
        store, row["id"], hash_b
    )["status"] == "resolved"
    assert doctor.outbound_partial_finals(store)["ok"] is True


def test_multiple_legacy_records_for_one_turn_remain_visible_and_resolvable():
    store = _store()
    turn_id = "turn-two-legacy-holds"
    records = {}
    for index, content_hash in enumerate(("hash-a", "hash-b"), start=1):
        records[f"legacy-slot-{index}"] = {
            "schema_version": 2,
            "turn_id": turn_id,
            "content_hash": content_hash,
            "status": "held",
            "terminal_outcome": "delivery_unknown",
            "delivery_complete": False,
            "message_ids": [str(700 + index)],
            "canonical_message_id": str(700 + index),
            "failed_part_index": 1,
            "operator_attention_required": True,
            "automatic_replay_authorized": False,
            "recovery_action": "accept-partial",
            "created_at": float(index),
            "updated_at": float(index),
            "error": f"legacy hold {index}",
        }
    store["telegram_partial_final_deliveries"] = records

    visible = doctor.outbound_partial_finals(store, now=10.0)
    adopted = state.partial_final_delivery_records_for_turn(
        store, turn_id
    )

    assert visible["ok"] is False
    assert visible["held_count"] == 2
    assert {record["content_hash"] for record in adopted} == {
        "hash-a",
        "hash-b",
    }
    for index, content_hash in enumerate(("hash-a", "hash-b"), start=1):
        state.resolve_partial_final_delivery(
            store,
            turn_id=turn_id,
            content_hash=content_hash,
            action="accept-partial",
            request_id=f"resolve-legacy-{index}",
            now=20.0 + index,
        )
        expected_remaining = 2 - index
        assert (
            doctor.outbound_partial_finals(store, now=30.0)[
                "held_count"
            ]
            == expected_remaining
        )

    assert doctor.outbound_partial_finals(store, now=30.0)["ok"] is True
    assert {
        record["status"]
        for record in state.partial_final_delivery_records_for_turn(
            store, turn_id
        )
    } == {"resolved"}


def test_every_legacy_delivery_shape_shares_one_physical_write_allowance(
    monkeypatch,
):
    monkeypatch.setenv("HERDRES_TENDWIRE_MODE", "source")
    monkeypatch.setenv("HERDRES_PINNED_STATUS", "0")
    monkeypatch.setenv("HERDRES_FORCE_PLAIN_DELIVERY", "0")

    class RejectRichTelegram(DeletingTelegram):
        def api(self, method, payload):
            if method == "sendRichMessage":
                self.api_calls.append(
                    (method, dict(payload), self.token)
                )
                raise TelegramError("Bad Request: reject rich")
            return super().api(method, payload)

    row = _turn_row(
        "turn-one-shared-write",
        "twrev1.one_shared_write",
        "**formatted fallback**",
    )
    row.pop("content")
    tendwire = TurnFinalTendwire(row, turn_schema_version=1)
    telegram = RejectRichTelegram()
    store = _store()

    result = sync_once(
        store, _runtime(tendwire, telegram, max_sends=1)
    )

    assert result["ok"] is False
    assert result["status"] == "outbound_delivery_stalled"
    assert result["outbound_delivery"]["physical_writes"] == 1
    assert [
        method
        for method, _payload, _token in telegram.api_calls
        if method == "sendRichMessage"
    ] == ["sendRichMessage"]
    assert telegram.sent == []
    assert state.delivered_turns(store) == {}


def test_escalated_supersession_and_working_then_final_each_obey_one_write(
    monkeypatch,
):
    monkeypatch.setenv("HERDRES_TENDWIRE_MODE", "source")
    monkeypatch.setenv("HERDRES_PINNED_STATUS", "0")
    monkeypatch.setenv("HERDRES_FORCE_PLAIN_DELIVERY", "0")

    store, row, tendwire, telegram, _first = _legacy_partial_case(
        ambiguous=True
    )
    hold_hash = source_sync._turn_content_hash(row, "final")
    hold = state.find_partial_final_delivery(
        store, row["id"], hold_hash
    )
    assert hold is not None
    hold["created_at"] -= (
        config.partial_final_escalation_seconds() + 1
    )
    row["assistant_final_text"] = (
        "long revised supersession\n\n" * 500
    ) + "SUPERSESSION_TAIL"
    attempts_before = telegram.rich_attempts

    superseded = sync_once(
        store, _runtime(tendwire, telegram, max_sends=1)
    )

    assert telegram.rich_attempts - attempts_before == 1
    assert superseded["outbound_delivery"]["physical_writes"] == 1
    assert superseded["ok"] is False

    working = _turn_row(
        "turn-working-budget",
        "twrev1.working_budget",
        None,
    )
    working["assistant_stream_text"] = "WORKING CARD"
    final = _turn_row(
        "turn-final-after-working",
        "twrev1.final_after_working",
        "FINAL MUST WAIT",
    )
    final["worker_id"] = "worker-2"
    final["worker_fingerprint"] = "fp-2"
    tendwire = MultiTurnFinalTendwire([working, final])
    working.pop("content")
    final.pop("content")
    tendwire.turn_schema_version = 1
    tendwire.turns = lambda: {
        "ok": True,
        "schema_version": 1,
        "turns": deepcopy(tendwire.rows),
    }
    tendwire.turn_final_poll = lambda **_kwargs: {
        "ok": True,
        "schema_version": 1,
        "items": [],
    }
    telegram = DeletingTelegram()

    combined = sync_once(
        _store(), _runtime(tendwire, telegram, max_sends=1)
    )

    assert combined["outbound_delivery"]["physical_writes"] == 1
    assert len(telegram.recipient_messages) == 1
    recipient_text = next(iter(telegram.recipient_messages.values()))[
        "content"
    ]
    assert "WORKING CARD" in recipient_text
    assert "FINAL MUST WAIT" not in recipient_text


def test_round_robin_moves_a_healthy_final_ahead_of_a_repeated_failure(
    monkeypatch,
):
    monkeypatch.setenv("HERDRES_TENDWIRE_MODE", "source")
    monkeypatch.setenv("HERDRES_PINNED_STATUS", "0")
    monkeypatch.setenv("HERDRES_FORCE_PLAIN_DELIVERY", "0")

    class FailOneTurnTelegram(DeletingTelegram):
        def api(self, method, payload):
            rich = json.loads(payload.get("rich_message") or "{}")
            if (
                method == "sendRichMessage"
                and "ALWAYS FAIL" in str(rich.get("html") or "")
            ):
                self.api_calls.append(
                    (method, dict(payload), self.token)
                )
                raise TelegramError("network timeout")
            return super().api(method, payload)

    failing = _turn_row(
        "turn-always-fails",
        "twrev1.always_fails",
        "ALWAYS FAIL",
    )
    healthy = _turn_row(
        "turn-healthy-behind-failure",
        "twrev1.healthy_behind_failure",
        "HEALTHY FINAL",
    )
    healthy["worker_id"] = "worker-2"
    healthy["worker_fingerprint"] = "fp-2"
    tendwire = MultiTurnFinalTendwire([failing, healthy])
    failing.pop("content")
    healthy.pop("content")
    tendwire.turn_schema_version = 1
    tendwire.turns = lambda: {
        "ok": True,
        "schema_version": 1,
        "turns": deepcopy(tendwire.rows),
    }
    tendwire.turn_final_poll = lambda **_kwargs: {
        "ok": True,
        "schema_version": 1,
        "items": [],
    }
    telegram = FailOneTurnTelegram()
    store = _store()

    first = sync_once(
        store, _runtime(tendwire, telegram, max_sends=1)
    )
    second = sync_once(
        store, _runtime(tendwire, telegram, max_sends=1)
    )

    assert first["ok"] is False
    assert first["status"] == "outbound_delivery_stalled"
    assert first["feed_sent"] == 0
    assert second["feed_sent"] == 1
    assert any(
        "HEALTHY FINAL" in message["content"]
        for message in telegram.recipient_messages.values()
    )
    failing_hash = source_sync._turn_content_hash(
        failing, "final"
    )
    assert (
        f"final:{failing['id']}:{failing_hash}"
        not in state.delivered_turns(store)
    )


class AttentionOnlyTendwire(TurnFinalTendwire):
    def __init__(self, *, ack_ok=True, item_available=True):
        super().__init__(
            _turn_row(
                "turn-attention-health",
                "twrev1.attention_health",
                None,
            ),
            emit_ready=False,
            turn_schema_version=2,
        )
        self.ack_ok = ack_ok
        self.item_available = item_available
        self.ack_calls = []
        self.fail_calls = []

    def snapshot(self):
        return {"ok": True, "workers": [], "spaces": []}

    def turns(self):
        return {"ok": True, "schema_version": 2, "turns": []}

    def turn_final_poll(self, **_kwargs):
        return {"ok": True, "schema_version": 1, "items": []}

    def connector_poll(self, **_kwargs):
        if not self.item_available:
            return {"ok": True, "items": []}
        return {
            "ok": True,
            "items": [
                {
                    "ref": "twref1.attention-health",
                    "key": "attention:health",
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
        self.ack_calls.append((ref, deepcopy(response)))
        return {"ok": self.ack_ok}

    def connector_fail(self, ref, error, **_kwargs):
        self.fail_calls.append((ref, str(error)))
        return {"ok": True}


def test_failed_attention_outbox_is_pending_and_stalls_zero_delivery(
    monkeypatch,
):
    monkeypatch.setenv("HERDRES_TENDWIRE_MODE", "source")
    monkeypatch.setenv("HERDRES_PINNED_STATUS", "0")

    class FailingAttentionTelegram(DeletingTelegram):
        def __init__(self):
            super().__init__()
            self.attempts = 0

        def send_message(self, _chat_id, _html, **_kwargs):
            self.attempts += 1
            return {"ok": False, "error": "synthetic attention failure"}

    tendwire = AttentionOnlyTendwire()
    telegram = FailingAttentionTelegram()

    result = sync_once(
        _store(), _runtime(tendwire, telegram, max_sends=1)
    )

    assert telegram.attempts == 1
    assert result["tendwire_outbox"]["failed"] == 1
    assert result["tendwire_outbox"]["delivered"] == 0
    assert result["outbound_delivery"] == {
        "ok": False,
        "status": "outbound_delivery_stalled",
        "pending_count": 1,
        "completed_count": 0,
        "physical_writes": 1,
    }
    assert result["ok"] is False
    assert result["status"] == "outbound_delivery_stalled"
    assert tendwire.fail_calls == [
        (
            "twref1.attention-health",
            "synthetic attention failure",
        )
    ]


def test_deferred_attention_outbox_stalls_when_no_delivery_completes(
    monkeypatch,
):
    monkeypatch.setenv("HERDRES_TENDWIRE_MODE", "source")
    monkeypatch.setenv("HERDRES_PINNED_STATUS", "0")
    tendwire = AttentionOnlyTendwire(ack_ok=False)
    telegram = DeletingTelegram()
    store = _store()

    accepted_but_unacked = sync_once(
        store, _runtime(tendwire, telegram, max_sends=1)
    )
    deferred_duplicate = sync_once(
        store, _runtime(tendwire, telegram, max_sends=1)
    )

    assert accepted_but_unacked["tendwire_outbox"]["delivered"] == 1
    assert accepted_but_unacked["tendwire_outbox"]["deferred"] == 1
    assert accepted_but_unacked["outbound_delivery"]["pending_count"] == 1
    assert accepted_but_unacked["outbound_delivery"]["completed_count"] == 1
    assert accepted_but_unacked["ok"] is True
    assert deferred_duplicate["tendwire_outbox"]["delivered"] == 0
    assert deferred_duplicate["tendwire_outbox"]["deferred"] == 1
    assert deferred_duplicate["outbound_delivery"]["pending_count"] == 1
    assert deferred_duplicate["outbound_delivery"]["completed_count"] == 0
    assert deferred_duplicate["outbound_delivery"]["status"] == (
        "outbound_delivery_stalled"
    )
    assert deferred_duplicate["ok"] is False
    assert len(telegram.sent) == 1


def test_quiet_outbound_pass_remains_healthy(monkeypatch):
    monkeypatch.setenv("HERDRES_TENDWIRE_MODE", "source")
    monkeypatch.setenv("HERDRES_PINNED_STATUS", "0")

    result = sync_once(
        _store(),
        _runtime(
            AttentionOnlyTendwire(item_available=False),
            DeletingTelegram(),
            max_sends=1,
        ),
    )

    assert result["outbound_delivery"] == {
        "ok": True,
        "status": "healthy",
        "pending_count": 0,
        "completed_count": 0,
        "physical_writes": 0,
    }
    assert result["ok"] is True


def test_partial_final_ledger_prunes_old_resolved_but_never_unresolved():
    store = _store()
    unresolved_hashes = {
        f"held-{index}" for index in range(7)
    }
    records = {}
    total = state.PARTIAL_FINAL_DELIVERY_LIMIT + 300
    for index in range(total):
        content_hash = (
            f"held-{index}"
            if index < len(unresolved_hashes)
            else f"resolved-{index}"
        )
        unresolved = content_hash in unresolved_hashes
        records[f"legacy-{index}"] = {
            "turn_id": f"turn-{index}",
            "content_hash": content_hash,
            "status": "held" if unresolved else "resolved",
            "operator_attention_required": unresolved,
            "delivery_complete": not unresolved,
            "created_at": float(index),
            "resolved_at": None if unresolved else float(index),
        }
    store["telegram_partial_final_deliveries"] = records

    normalized = state.partial_final_deliveries(store)

    assert len(normalized) == state.PARTIAL_FINAL_DELIVERY_LIMIT
    assert unresolved_hashes <= {
        record["content_hash"] for record in normalized.values()
    }
    retained_resolved = [
        record
        for record in normalized.values()
        if record["status"] == "resolved"
    ]
    assert min(record["resolved_at"] for record in retained_resolved) == (
        total - (state.PARTIAL_FINAL_DELIVERY_LIMIT - 7)
    )


def test_partial_final_recovery_actions_are_distinct_and_clear_doctor_only_when_safe(
    monkeypatch,
    capsys,
):
    monkeypatch.setenv("HERDRES_TENDWIRE_MODE", "source")
    monkeypatch.setenv("HERDRES_PINNED_STATUS", "0")
    monkeypatch.setenv("HERDRES_FORCE_PLAIN_DELIVERY", "0")

    unknown_store, unknown_row, unknown_tendwire, unknown_telegram, _ = (
        _legacy_partial_case(ambiguous=True)
    )
    unknown_attempts = unknown_telegram.rich_attempts
    monkeypatch.setattr(
        doctor, "source_services", lambda: {"ok": True}
    )
    monkeypatch.setattr(doctor, "legacy_timer", lambda: {"ok": True})
    monkeypatch.setattr(
        doctor, "sqlite_integrity", lambda: {"ok": True}
    )
    monkeypatch.setattr(
        doctor, "tendwire_backend", lambda _client=None: {"ok": True}
    )
    monkeypatch.setattr(
        doctor, "tendwire_delta_feed", lambda: {"ok": True}
    )
    monkeypatch.setattr(
        doctor, "inbound_lanes", lambda: {"ok": True, "status": "healthy"}
    )
    monkeypatch.setattr(
        doctor.state, "load_state", lambda: unknown_store
    )
    unhealthy = doctor.run_doctor()
    assert unhealthy["ok"] is False
    assert (
        unhealthy["checks"]["outbound_partial_finals"]["first_hold"][
            "turn_id"
        ]
        == unknown_row["id"]
    )
    assert (
        _resolve_partial_with_command(
            monkeypatch,
            unknown_store,
            row=unknown_row,
            action="accept-partial",
            request_id="accept-unknown-1",
        )
        == 0
    )
    capsys.readouterr()
    healthy = doctor.run_doctor()
    assert healthy["ok"] is True
    assert (
        healthy["checks"]["outbound_partial_finals"]["status"]
        == "healthy"
    )
    accepted = sync_once(
        unknown_store,
        _runtime(
            unknown_tendwire, unknown_telegram, max_sends=100
        ),
    )
    assert accepted["ok"] is True
    assert unknown_telegram.rich_attempts == unknown_attempts
    assert state.delivered_turns(unknown_store) == {}
    assert unknown_tendwire.ack_calls == []

    delivered_store, delivered_row, delivered_tendwire, delivered_telegram, _ = (
        _legacy_partial_case(ambiguous=False)
    )
    prefix_ids = list(delivered_telegram.recipient_messages)
    assert (
        _resolve_partial_with_command(
            monkeypatch,
            delivered_store,
            row=delivered_row,
            action="retry-missing",
            request_id="retry-known-missing-1",
        )
        == 0
    )
    capsys.readouterr()
    pending = doctor.outbound_partial_finals(delivered_store)
    assert pending["ok"] is False
    assert pending["status"] == "partial_final_recovery_pending"

    completed = sync_once(
        delivered_store,
        _runtime(
            delivered_tendwire,
            delivered_telegram,
            max_sends=100,
        ),
    )

    assert completed["ok"] is True
    assert prefix_ids == ["100"]
    assert "100" in delivered_telegram.recipient_messages
    assert len(delivered_telegram.recipient_messages) > 1
    assert (
        sum(
            "Response 1/" in message["content"]
            for message in delivered_telegram.recipient_messages.values()
        )
        == 1
    )
    assert sum(
        message_id == "100"
        for message_id in delivered_telegram.recipient_messages
    ) == 1
    assert any(
        "MISSING_SUFFIX" in message["content"]
        for message in delivered_telegram.recipient_messages.values()
    )
    assert state.delivered_turns(delivered_store)
    assert doctor.outbound_partial_finals(delivered_store)["ok"] is True


@pytest.mark.parametrize(
    ("inbound_ok", "outbound_ok", "expected_ok"),
    [
        (True, True, True),
        (False, True, False),
        (True, False, False),
        (False, False, False),
    ],
)
def test_doctor_composes_inbound_and_outbound_health_without_masking(
    monkeypatch,
    inbound_ok,
    outbound_ok,
    expected_ok,
):
    monkeypatch.setattr(
        doctor, "source_services", lambda: {"ok": True}
    )
    monkeypatch.setattr(doctor, "legacy_timer", lambda: {"ok": True})
    monkeypatch.setattr(
        doctor, "sqlite_integrity", lambda: {"ok": True}
    )
    monkeypatch.setattr(
        doctor, "tendwire_backend", lambda _client=None: {"ok": True}
    )
    monkeypatch.setattr(
        doctor, "tendwire_delta_feed", lambda: {"ok": True}
    )
    monkeypatch.setattr(
        doctor,
        "inbound_lanes",
        lambda: {
            "ok": inbound_ok,
            "status": "healthy" if inbound_ok else "stalled",
        },
    )
    monkeypatch.setattr(
        doctor,
        "outbound_partial_finals",
        lambda: {
            "ok": outbound_ok,
            "status": (
                "healthy"
                if outbound_ok
                else "partial_final_delivery_unknown"
            ),
        },
    )
    monkeypatch.setattr(
        doctor,
        "outbound_unbound_live_panes",
        lambda: {"ok": True, "status": "healthy"},
    )

    result = doctor.run_doctor()

    assert result["ok"] is expected_ok
    assert result["checks"]["inbound_lanes"]["ok"] is inbound_ok
    assert (
        result["checks"]["outbound_partial_finals"]["ok"]
        is outbound_ok
    )


@pytest.mark.parametrize(
    (
        "stalled",
        "unknown",
        "retry_obstructed",
        "expected_status",
    ),
    [
        (1, 1, 1, "stalled"),
        (0, 1, 1, "obstruction_unknown"),
        (0, 0, 1, "retry_obstructed"),
        (0, 0, 0, "healthy"),
    ],
)
def test_outbound_health_composition_preserves_inbound_precedence(
    monkeypatch,
    tmp_path,
    stalled,
    unknown,
    retry_obstructed,
    expected_status,
):
    db_path = tmp_path / "inbound.sqlite3"
    db_path.touch()
    snapshot = SimpleNamespace(
        stalled_lane_count=stalled,
        unknown_obstruction_lane_count=unknown,
        retry_obstructed_lane_count=retry_obstructed,
        pending_count=3,
        claimable_lane_count=0,
        blocked_count=3,
        first_unknown_obstruction_lane="lane-unknown",
        oldest_retry_obstructed_seconds=12.0,
        first_retry_obstructed_lane="lane-retry",
        oldest_stalled_seconds=20.0,
        first_stalled_lane="lane-stalled",
    )

    class FakeIngressLaneSpool:
        def __init__(self, _path):
            pass

        def dispatch_snapshot(self, **_kwargs):
            return snapshot

    monkeypatch.setattr(
        doctor.config, "inbound_lanes_enabled", lambda: True
    )
    monkeypatch.setattr(doctor, "IngressLaneSpool", FakeIngressLaneSpool)

    result = doctor.inbound_lanes(db_path)

    assert result["status"] == expected_status
    assert result["ok"] is (expected_status == "healthy")


def test_partial_final_recovery_rejects_cross_outcome_actions(
    monkeypatch,
    capsys,
):
    monkeypatch.setenv("HERDRES_TENDWIRE_MODE", "source")
    monkeypatch.setenv("HERDRES_PINNED_STATUS", "0")
    store, row, _tendwire, _telegram, _first = _legacy_partial_case(
        ambiguous=True
    )

    code = _resolve_partial_with_command(
        monkeypatch,
        store,
        row=row,
        action="retry-missing",
        request_id="unsafe-cross-outcome-1",
    )
    output = json.loads(capsys.readouterr().out)

    assert code == 1
    assert output["status"] == "partial_final_resolution_rejected"
    assert state.active_partial_final_deliveries(store)


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
    store = _store()

    result = sync_once(
        store,
        _runtime(tendwire, telegram, max_sends=100),
    )

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
    global_pin_id = str(
        store["telegram"]["pinned_status_message_id"]
    )
    global_pin_html = next(
        message[1]
        for message in telegram.sent
        if message[3] == global_pin_id
    )
    topic_entry = next(
        entry
        for entry in state.source_entries(store).values()
        if entry.get("pinned_status_message_id")
    )
    topic_pin_id = str(topic_entry["pinned_status_message_id"])
    topic_pin_html = next(
        message[1]
        for message in telegram.sent
        if message[3] == topic_pin_id
    )
    assert "account: active" in global_pin_html
    assert "account: active" in topic_pin_html


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
        inherited_prefix_count = int(
            failed.get("acknowledged_prefix_count", 0)
        )
        local_prefix_count = 0
        for job in sorted(
            failed_jobs,
            key=lambda item: item["payload"]["sequence_index"],
        ):
            if job["status"] != "delivered":
                break
            local_prefix_count += 1
        prefix_count = inherited_prefix_count + local_prefix_count
        current_failed_count = sum(
            job["status"] == "dead_letter"
            for job in failed_jobs[local_prefix_count:]
        )
        retained_failed_job_count = (
            int(failed.get("retained_failed_job_count", 0))
            + current_failed_count
        )
        generation = int(failed.get("generation", 1)) + 1
        prior_attempt_count = int(failed.get("prior_attempt_count", 0)) + 3
        token = f"twplan1.plan{len(self._plans) + 1}"
        replacement = {
            "state": "active",
            "turn_id": failed["turn_id"],
            "revision": failed["revision"],
            "part_count": failed["part_count"],
            "parts": deepcopy(failed["parts"]),
            "replaces": failed_plan_token,
            "generation": generation,
            "acknowledged_prefix_count": prefix_count,
            "retained_failed_job_count": retained_failed_job_count,
            "prior_attempt_count": prior_attempt_count,
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
            "generation": generation,
            "content_revision": failed["revision"],
            "state": "active",
            "acknowledged_prefix_count": prefix_count,
            "executable_job_count": failed["part_count"] - prefix_count,
            "retained_failed_job_count": retained_failed_job_count,
            "prior_attempt_count": prior_attempt_count,
            "idempotent_replay": False,
        }
        self._recoveries[request_id] = deepcopy(response)
        return response


class UnrelatedTerminalReadyAfterFirstPartTendwire(
    ExhaustedRecoveryTendwire
):
    def __init__(self, row, unrelated_row):
        super().__init__(row)
        self.emit_ready = True
        self.turn_schema_version = 2
        self.unrelated_row = unrelated_row
        self.unrelated_revision = unrelated_row["content"][
            "content_revision"
        ]
        self.unrelated_failure_emitted = False

    def snapshot(self):
        response = super().snapshot()
        response["workers"].append(
            _source_worker(
                {
                    "id": self.unrelated_row["worker_id"],
                    "name": "Unrelated",
                    "status": "idle",
                    "space_id": "space-1",
                    "fingerprint": self.unrelated_row[
                        "worker_fingerprint"
                    ],
                    "meta": {
                        "agent": "codex",
                        "stable_key": self.unrelated_row[
                            "stable_key"
                        ],
                        "stable_key_version": 1,
                    },
                }
            )
        )
        return response

    def turn_final_poll(self, *, limit=1, lease_seconds=60):
        first_part_delivered = any(
            job["status"] == "delivered"
            for job in self._jobs
        )
        if first_part_delivered and not self.unrelated_failure_emitted:
            self.unrelated_failure_emitted = True
            previous_row = self.row
            self.row = self.unrelated_row
            try:
                payload = self._ready_payload()
            finally:
                self.row = previous_row
            self._ref_counter += 1
            ref = f"twref1.ready{self._ref_counter}"
            return {
                "ok": True,
                "schema_version": 1,
                "items": [
                    {
                        "ref": ref,
                        "key": (
                            "turn-final:revision:"
                            f"{payload['final_identity']}"
                        ),
                        "attempt": self._ref_counter,
                        "payload": payload,
                    }
                ],
            }
        return super().turn_final_poll(
            limit=limit,
            lease_seconds=lease_seconds,
        )

    def turn_content_get(
        self, turn_id, revision, field, cursor=None
    ):
        response = super().turn_content_get(
            turn_id, revision, field, cursor
        )
        if revision == self.unrelated_revision:
            response["content_revision"] = "twrev1.wrong_revision"
        return response

    def turn_final_fail(self, ref, reason):
        if self.unrelated_failure_emitted and not any(
            job.get("ref") == ref for job in self._jobs
        ):
            self.fail_calls.append((ref, reason))
            return {
                "ok": True,
                "schema_version": 1,
                "status": "attempts_exhausted",
            }
        return super().turn_final_fail(ref, reason)


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
    failed_token = next(iter(tendwire._plans))
    failed_entry = next(
        iter(state.source_worker_entries(store).values())
    )

    assert exhausted["tendwire_turn_final"]["status"] == "attempts_exhausted"
    assert "pending_plan_token" not in failed_entry
    assert "pending_content_revision" not in failed_entry
    assert failed_entry["abandoned_plan_token"] == failed_token
    assert (
        failed_entry["abandoned_content_revision"]
        == "twrev1.recovery"
    )
    no_spin = sync_once(store, _runtime(tendwire, telegram, max_sends=100))
    old_jobs_before = deepcopy(state.tendwire_turn_jobs(store))
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


def test_unrelated_terminal_materialize_failure_preserves_live_plan(
    monkeypatch,
):
    monkeypatch.setenv("HERDRES_TENDWIRE_MODE", "source")
    monkeypatch.setenv("HERDRES_PINNED_STATUS", "0")
    monkeypatch.setenv(
        "HERDR_TELEGRAM_TOPICS_RICH_MESSAGES", "0"
    )
    live_revision = "twrev1.live_plan"
    live_row = _turn_row(
        "turn-live-plan",
        live_revision,
        ("live multipart response\n\n" * 500) + "LIVE_TAIL",
    )
    unrelated_row = _turn_row(
        "turn-unrelated-failure",
        "twrev1.unrelated_failure",
        "unrelated final",
    )
    unrelated_row["worker_id"] = "worker-2"
    unrelated_row["worker_fingerprint"] = "fp-2"
    unrelated_row["stable_key"] = _stable_key(
        "worker-2", "fp-2"
    )
    tendwire = UnrelatedTerminalReadyAfterFirstPartTendwire(
        live_row, unrelated_row
    )
    store = _store()

    result = sync_once(
        store,
        _runtime(tendwire, DeletingTelegram(), max_sends=100),
    )

    live_token = tendwire._plan_by_revision[live_revision]
    live_entry = next(
        iter(state.source_worker_entries(store).values())
    )
    live_jobs = [
        job
        for job in tendwire._jobs
        if job["payload"]["plan_token"] == live_token
    ]
    assert len(live_jobs) > 1
    assert live_jobs[0]["status"] == "delivered"
    assert any(job["status"] == "queued" for job in live_jobs[1:])
    assert result["tendwire_turn_final"]["status"] == (
        "attempts_exhausted"
    )
    assert live_entry["pending_plan_token"] == live_token
    assert live_entry["pending_content_revision"] == live_revision
    assert "abandoned_plan_token" not in live_entry
    assert "abandoned_content_revision" not in live_entry


class FailSecondPartTwiceTelegram(DeletingTelegram):
    def __init__(self):
        super().__init__()
        self.final_attempts = []

    def send_message(self, chat_id, html, **kwargs):
        self.final_attempts.append(html)
        if len(self.final_attempts) in {2, 3}:
            return {
                "ok": False,
                "kind": "permanent",
                "error": "provider rejected bounded part",
            }
        return FakeTelegram.send_message(self, chat_id, html, **kwargs)


def test_second_generation_recovery_inherits_failures_and_executes_only_suffix(
    monkeypatch,
    capsys,
):
    monkeypatch.setenv("HERDRES_TENDWIRE_MODE", "source")
    monkeypatch.setenv("HERDRES_PINNED_STATUS", "0")
    monkeypatch.setenv("HERDR_TELEGRAM_TOPICS_RICH_MESSAGES", "0")
    text = ("second generation recovery\n\n" * 500) + "GENERATION_THREE_TAIL"
    tendwire = ExhaustedRecoveryTendwire(
        _turn_row("turn-recovery-3", "twrev1.recovery3", text)
    )
    telegram = FailSecondPartTwiceTelegram()
    store = _store()

    first_failure = sync_once(
        store,
        _runtime(tendwire, telegram, max_sends=100),
    )
    first_token = next(iter(tendwire._plans))
    prefix_html = telegram.final_attempts[0]

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

    first_code = herdres.cmd_recover_turn_final(
        SimpleNamespace(
            plan_token=first_token,
            request_id="operator-recovery-generation-2",
        )
    )
    generation_two = json.loads(capsys.readouterr().out)
    second_token = generation_two["plan_token"]
    second_failure = sync_once(
        store,
        _runtime(tendwire, telegram, max_sends=100),
    )

    second_code = herdres.cmd_recover_turn_final(
        SimpleNamespace(
            plan_token=second_token,
            request_id="operator-recovery-generation-3",
        )
    )
    generation_three = json.loads(capsys.readouterr().out)
    third_token = generation_three["plan_token"]
    completed = sync_once(
        store,
        _runtime(tendwire, telegram, max_sends=100),
    )

    assert first_failure["tendwire_turn_final"]["status"] == "attempts_exhausted"
    assert first_code == 0
    assert generation_two["generation"] == 2
    assert generation_two["retained_failed_job_count"] == 1
    assert second_failure["tendwire_turn_final"]["status"] == "attempts_exhausted"
    assert second_code == 0
    assert generation_three == {
        "acknowledged_prefix_count": 1,
        "content_revision": "twrev1.recovery3",
        "executable_job_count": len(tendwire._plans[third_token]["parts"]) - 1,
        "failed_plan_token": second_token,
        "generation": 3,
        "idempotent_replay": False,
        "ok": True,
        "plan_token": third_token,
        "prior_attempt_count": 6,
        "retained_failed_job_count": 2,
        "schema_version": 1,
        "state": "active",
        "status": "recovered",
    }
    assert completed["tendwire_turn_final"]["acked"] == (
        len(tendwire._plans[third_token]["parts"]) - 1
    )
    assert telegram.final_attempts.count(prefix_html) == 1
    assert "GENERATION_THREE_TAIL" in telegram.final_attempts[-1]
    assert [
        job["payload"]["sequence_index"]
        for job in tendwire._jobs
        if job["payload"]["plan_token"] == third_token
    ] == list(range(1, len(tendwire._plans[third_token]["parts"])))
    second_generation_statuses = [
        job["status"]
        for job in tendwire._jobs
        if job["payload"]["plan_token"] == second_token
    ]
    assert second_generation_statuses[0] == "dead_letter"
    assert set(second_generation_statuses[1:]) == {"queued"}
    assert next(iter(state.source_worker_entries(store).values()))[
        "last_clean_plan_token"
    ] == third_token


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


def test_provider_acceptance_crash_checkpoints_message_and_restart_never_resends(
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
    assert checkpoints[-1][job_key]["substate"] == "telegram_applied"
    assert (
        state.tendwire_turn_jobs(store)[job_key]["substate"]
        == "telegram_applied"
    )
    accepted_message_id = state.tendwire_turn_jobs(store)[job_key][
        "telegram_message_id"
    ]
    assert state.find_message_binding(store, accepted_message_id) is not None

    # Simulate Tendwire lease expiry/requeue after the process crash.
    tendwire._jobs[0]["status"] = "queued"
    sends_after_crash = len(telegram.sent)
    restarted = sync_once(
        store,
        _runtime(tendwire, telegram, max_sends=1),
    )

    assert restarted["tendwire_turn_final"]["acked"] == 1
    assert restarted["tendwire_turn_final"]["operations"] == 0
    assert restarted["tendwire_turn_final"]["uncertain"] == 0
    assert len(telegram.sent) == sends_after_crash
    assert (
        state.tendwire_turn_jobs(store)[job_key]["substate"]
        == "acknowledged"
    )
    preflight = herdres._turn_final_recovery_preflight(
        store,
        "twplan1.plan1",
        "operator-provider-crash-1",
    )
    assert preflight["status"] == "recovery_plan_not_found"


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
def _second_generation_recovery_store():
    store = _store()
    worker = _manual_recovery_worker()
    store["panes"]["worker"] = worker
    response = _recovery_response()
    herdres._clone_recovery_prefix(
        store,
        failed_plan_token="twplan1.failed",
        plan_token="twplan1.replacement",
        entry_key="worker",
        prefix=[],
        executable_job_count=1,
        request_id="operator-generation-2",
        response=response,
    )
    replacement_key = "turn-final:twplan1.replacement:000000"
    state.reserve_tendwire_turn_job(
        store,
        replacement_key,
        plan_token="twplan1.replacement",
        content_revision="twrev1.recovery",
        operation="upsert",
        sequence_index=0,
        part_ordinal=0,
        part_count=1,
        bot_kind="manager",
    )
    state.update_tendwire_turn_job(
        store,
        replacement_key,
        substate="failed",
    )
    return store, worker


def test_generation_one_recovery_does_not_require_inherited_state():
    store = _store()
    worker = _manual_recovery_worker()
    store["panes"]["worker"] = worker
    failed_key = "turn-final:twplan1.failed:000000"
    state.reserve_tendwire_turn_job(
        store,
        failed_key,
        plan_token="twplan1.failed",
        content_revision="twrev1.recovery",
        operation="upsert",
        sequence_index=0,
        part_ordinal=0,
        part_count=1,
        bot_kind="manager",
    )
    state.update_tendwire_turn_job(store, failed_key, substate="failed")

    preflight = herdres._turn_final_recovery_preflight(
        store,
        "twplan1.failed",
        "operator-generation-1",
    )

    assert preflight["ok"] is True
    assert preflight["prior_generation"] == 1
    assert preflight["expected_predecessor_plan_token"] is None
    assert preflight["inherited_audit_identity"] is None


@pytest.mark.parametrize(
    "corruption",
    [
        "missing_entry_predecessor",
        "malformed_entry_predecessor",
        "audit_predecessor",
        "binding_predecessor",
        "audit_and_binding_predecessor",
    ],
)
def test_generation_three_predecessor_corruption_stops_before_rpc(
    monkeypatch,
    capsys,
    corruption,
):
    store, worker = _second_generation_recovery_store()
    request_key = herdres._recovery_request_key("operator-generation-2")
    audit = store["tendwire_turn_final_recoveries"][request_key]
    binding = store["tendwire_turn_final_recovery_requests"][request_key]
    if corruption == "missing_entry_predecessor":
        worker.pop("replaces_failed_plan_token")
    elif corruption == "malformed_entry_predecessor":
        worker["replaces_failed_plan_token"] = "not-a-plan-token"
    elif corruption == "audit_predecessor":
        audit["failed_plan_token"] = "twplan1.wrong_audit"
    elif corruption == "binding_predecessor":
        binding["failed_plan_token"] = "twplan1.wrong_binding"
    else:
        worker["replaces_failed_plan_token"] = "twplan1.real_generation1"
        audit["failed_plan_token"] = "twplan1.wrong_generation1"
        binding["failed_plan_token"] = "twplan1.wrong_generation1"
    calls = []

    class NeverCalled:
        def connector_prepare_recover(self, **kwargs):
            calls.append(kwargs)
            raise AssertionError("invalid predecessor must stop before RPC")

    monkeypatch.setattr(herdres.config, "load_env_file", lambda: None)
    monkeypatch.setattr(herdres.config, "require_source_mode", lambda: None)
    monkeypatch.setattr(herdres.state, "state_lock", lambda: nullcontext())
    monkeypatch.setattr(herdres.state, "load_state", lambda: store)
    monkeypatch.setattr(herdres, "TendwireClient", NeverCalled)

    code = herdres.cmd_recover_turn_final(
        SimpleNamespace(
            plan_token="twplan1.replacement",
            request_id=f"operator-predecessor-{corruption}",
        )
    )
    output = json.loads(capsys.readouterr().out)

    assert code == 1
    assert output["status"] == "recovery_state_invalid"
    assert calls == []


def test_predecessor_change_during_recovery_rpc_fails_revalidation(
    monkeypatch,
    capsys,
):
    store, worker = _second_generation_recovery_store()
    saves = []
    calls = []

    class MutatingClient:
        def connector_prepare_recover(self, **kwargs):
            calls.append(kwargs)
            worker["replaces_failed_plan_token"] = "twplan1.changed_generation1"
            return _recovery_response(
                failed_plan_token="twplan1.replacement",
                plan_token="twplan1.generation3",
                generation=3,
                retained_failed_job_count=2,
            )

    monkeypatch.setattr(herdres.config, "load_env_file", lambda: None)
    monkeypatch.setattr(herdres.config, "require_source_mode", lambda: None)
    monkeypatch.setattr(herdres.state, "state_lock", lambda: nullcontext())
    monkeypatch.setattr(herdres.state, "load_state", lambda: store)
    monkeypatch.setattr(
        herdres.state,
        "save_state",
        lambda candidate: saves.append(candidate),
    )
    monkeypatch.setattr(herdres, "TendwireClient", MutatingClient)

    code = herdres.cmd_recover_turn_final(
        SimpleNamespace(
            plan_token="twplan1.replacement",
            request_id="operator-predecessor-rpc-change",
        )
    )
    output = json.loads(capsys.readouterr().out)

    assert code == 1
    assert output["status"] == "recovery_state_uncertain"
    assert calls == [
        {
            "failed_plan_token": "twplan1.replacement",
            "request_id": "operator-predecessor-rpc-change",
        }
    ]
    assert saves == []




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


def test_inherited_recovery_audit_is_unique_and_revalidated_in_preflight():
    store = _store()
    worker = _manual_recovery_worker()
    store["panes"]["worker"] = worker
    response = _recovery_response()
    herdres._clone_recovery_prefix(
        store,
        failed_plan_token="twplan1.failed",
        plan_token="twplan1.replacement",
        entry_key="worker",
        prefix=[],
        executable_job_count=1,
        request_id="operator-generation-2",
        response=response,
    )
    replacement_key = "turn-final:twplan1.replacement:000000"
    state.reserve_tendwire_turn_job(
        store,
        replacement_key,
        plan_token="twplan1.replacement",
        content_revision="twrev1.recovery",
        operation="upsert",
        sequence_index=0,
        part_ordinal=0,
        part_count=1,
        bot_kind="manager",
    )
    state.update_tendwire_turn_job(
        store,
        replacement_key,
        substate="failed",
    )

    valid = herdres._turn_final_recovery_preflight(
        store,
        "twplan1.replacement",
        "operator-generation-3",
    )

    assert valid["ok"] is True
    assert valid["current_failed_tail_count"] == 1
    assert valid["inherited_retained_failed_job_count"] == 1
    assert valid["expected_retained_failed_job_count"] == 2
    assert valid["inherited_audit_identity"][1:] == (
        "twplan1.failed",
        "twplan1.failed",
        "twplan1.replacement",
        2,
        1,
    )

    duplicate = deepcopy(store)
    original_request_key = herdres._recovery_request_key(
        "operator-generation-2"
    )
    duplicate_request_key = herdres._recovery_request_key(
        "operator-generation-2-duplicate"
    )
    duplicate["tendwire_turn_final_recoveries"][duplicate_request_key] = (
        deepcopy(
            duplicate["tendwire_turn_final_recoveries"][
                original_request_key
            ]
        )
    )
    duplicate["tendwire_turn_final_recovery_requests"][
        duplicate_request_key
    ] = deepcopy(
        duplicate["tendwire_turn_final_recovery_requests"][
            original_request_key
        ]
    )
    duplicate_result = herdres._turn_final_recovery_preflight(
        duplicate,
        "twplan1.replacement",
        "operator-generation-3",
    )
    assert duplicate_result["status"] == "recovery_state_invalid"

    wrong_generation = deepcopy(store)
    wrong_generation["tendwire_turn_final_recoveries"][
        original_request_key
    ]["generation"] = 3
    wrong_generation["tendwire_turn_final_recovery_requests"][
        original_request_key
    ]["generation"] = 3
    wrong_generation_result = herdres._turn_final_recovery_preflight(
        wrong_generation,
        "twplan1.replacement",
        "operator-generation-3",
    )
    assert wrong_generation_result["status"] == "recovery_state_invalid"

    changed_count = deepcopy(store)
    changed_count["tendwire_turn_final_recoveries"][
        original_request_key
    ]["retained_failed_job_count"] = 2
    revalidated = herdres._turn_final_recovery_preflight(
        changed_count,
        "twplan1.replacement",
        "operator-generation-3",
    )
    assert revalidated["ok"] is True
    assert revalidated["expected_retained_failed_job_count"] == 3
    assert revalidated["fingerprint"] != valid["fingerprint"]


def test_pending_replacement_keeps_inherited_audit_at_capacity():
    store = _store()
    worker = _manual_recovery_worker()
    store["panes"]["worker"] = worker
    first_response = _recovery_response()
    herdres._clone_recovery_prefix(
        store,
        failed_plan_token="twplan1.failed",
        plan_token="twplan1.replacement",
        entry_key="worker",
        prefix=[],
        executable_job_count=1,
        request_id="operator-protected-generation-2",
        response=first_response,
    )
    replacement_key = "turn-final:twplan1.replacement:000000"
    state.reserve_tendwire_turn_job(
        store,
        replacement_key,
        plan_token="twplan1.replacement",
        content_revision="twrev1.recovery",
        operation="upsert",
        sequence_index=0,
        part_ordinal=0,
        part_count=1,
        bot_kind="manager",
    )
    state.update_tendwire_turn_job(
        store,
        replacement_key,
        substate="failed",
    )

    other = _manual_recovery_worker("twplan1.other0")
    other["tendwire_worker_id"] = "worker-other"
    other["tendwire_stable_key"] = "wsk1_" + ("d" * 64)
    store["panes"]["other"] = other
    for index in range(100):
        failed = f"twplan1.other{index}"
        replacement = f"twplan1.otherreplacement{index}"
        other["pending_plan_token"] = failed
        response = _recovery_response(
            failed_plan_token=failed,
            plan_token=replacement,
        )
        herdres._clone_recovery_prefix(
            store,
            failed_plan_token=failed,
            plan_token=replacement,
            entry_key="other",
            prefix=[],
            executable_job_count=1,
            request_id=f"operator-other-{index}",
            response=response,
        )

    protected_key = herdres._recovery_request_key(
        "operator-protected-generation-2"
    )
    assert len(store["tendwire_turn_final_recoveries"]) == 100
    assert protected_key in store["tendwire_turn_final_recoveries"]
    preflight = herdres._turn_final_recovery_preflight(
        store,
        "twplan1.replacement",
        "operator-protected-generation-3",
    )
    assert preflight["ok"] is True
    assert preflight["expected_retained_failed_job_count"] == 2


def test_recovery_stops_before_rpc_when_all_audits_protect_pending_plans():
    store = _store()
    worker = _manual_recovery_worker()
    store["panes"]["worker"] = worker
    failed_key = "turn-final:twplan1.failed:000000"
    state.reserve_tendwire_turn_job(
        store,
        failed_key,
        plan_token="twplan1.failed",
        content_revision="twrev1.recovery",
        operation="upsert",
        sequence_index=0,
        part_ordinal=0,
        part_count=1,
        bot_kind="manager",
    )
    state.update_tendwire_turn_job(store, failed_key, substate="failed")
    audits = {}
    request_bindings = {}
    for index in range(herdres._TURN_FINAL_RECOVERY_AUDIT_LIMIT):
        plan_token = f"twplan1.protected{index}"
        request_key = f"protected-{index}"
        protected = _manual_recovery_worker(plan_token)
        protected["tendwire_worker_id"] = f"worker-protected-{index}"
        protected["tendwire_stable_key"] = (
            "wsk1_" + f"{index + 1:064x}"
        )
        store["panes"][f"protected-{index}"] = protected
        audits[request_key] = {"plan_token": plan_token}
        request_bindings[request_key] = {"plan_token": plan_token}
    store["tendwire_turn_final_recoveries"] = audits
    store["tendwire_turn_final_recovery_requests"] = request_bindings

    preflight = herdres._turn_final_recovery_preflight(
        store,
        "twplan1.failed",
        "operator-capacity-protected",
    )

    assert preflight["status"] == "recovery_capacity_exceeded"


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


def test_twenty_same_worker_ready_anchors_drain_in_order_and_forced_syncs_noop(
    monkeypatch,
):
    monkeypatch.setenv("HERDRES_TENDWIRE_MODE", "source")
    monkeypatch.setenv("HERDRES_PINNED_STATUS", "0")
    rows = [
        _turn_row(
            f"turn-outage-{index:02d}",
            f"twrev1.outage_{index:02d}",
            f"outage final {index:02d}",
        )
        for index in range(20)
    ]
    tendwire = ReadyQueueTendwire(rows)
    telegram = DeletingTelegram()
    store = _store()

    recovered = sync_once(
        store,
        _runtime(tendwire, telegram, max_sends=100),
    )

    assert recovered["tendwire_turn_final"]["staged"] == 20
    assert recovered["tendwire_turn_final"]["acked"] == 20
    assert recovered["tendwire_turn_final"]["operations"] == 20
    assert len(telegram.sent) == 20
    rendered = [message[1] for message in telegram.sent]
    positions = [
        next(
            index
            for index, html in enumerate(rendered)
            if f"outage final {turn:02d}" in html
        )
        for turn in range(20)
    ]
    assert positions == list(range(20))
    assert len(tendwire.source_prepare_refs) == 40
    for index in range(0, 40, 2):
        begin, commit = tendwire.source_prepare_refs[
            index : index + 2
        ]
        assert begin[0] == "begin"
        assert commit[0] == "commit"
        assert begin[1] == commit[1]
    assert all(
        not ref.startswith("twref1.ready")
        for ref, _response in tendwire.ack_calls
    )
    encoded_ack_responses = "\n".join(
        str(response)
        for _ref, response in tendwire.ack_calls
    ).lower()
    assert all(
        forbidden not in encoded_ack_responses
        for forbidden in (
            "telegram",
            "chat_id",
            "topic_id",
            "message_id",
            "bot_token",
        )
    )

    assert "tendwire_turn_final_source_owners" not in store

    snapshot = {
        "store": deepcopy(store),
        "sent": deepcopy(telegram.sent),
        "edited": deepcopy(telegram.edited),
        "deleted": deepcopy(telegram.deleted_messages),
        "pages": deepcopy(tendwire.page_calls),
        "prepare": deepcopy(tendwire.prepare_calls),
        "source_refs": deepcopy(tendwire.source_prepare_refs),
        "plans": deepcopy(tendwire._plans),
        "jobs": deepcopy(tendwire._jobs),
        "ready_state": deepcopy(tendwire._ready_state),
        "ready_ref": deepcopy(tendwire._ready_ref),
        "acks": deepcopy(tendwire.ack_calls),
        "fails": deepcopy(tendwire.fail_calls),
        "defers": deepcopy(tendwire.defer_calls),
    }
    for _forced_index in range(2):
        forced = sync_once(
            store,
            _runtime(tendwire, telegram, max_sends=100),
        )
        final = forced["tendwire_turn_final"]
        assert final["polled"] == 0
        assert final["staged"] == 0
        assert final["operations"] == 0
        assert final["delivered"] == 0
        assert final["acked"] == 0
        assert final["failed"] == 0
        assert final["deferred"] == 0
        assert final["uncertain"] == 0
        assert final["content_pages"] == 0
        assert final["changed"] is False
        assert forced["content_pages"] == 0
        assert forced["sent"] == 0
        assert forced["turn_updates"] == 0
        assert store == snapshot["store"]
        assert telegram.sent == snapshot["sent"]
        assert telegram.edited == snapshot["edited"]
        assert telegram.deleted_messages == snapshot["deleted"]
        assert tendwire.page_calls == snapshot["pages"]
        assert tendwire.prepare_calls == snapshot["prepare"]
        assert tendwire.source_prepare_refs == snapshot["source_refs"]
        assert tendwire._plans == snapshot["plans"]
        assert tendwire._jobs == snapshot["jobs"]
        assert tendwire._ready_state == snapshot["ready_state"]
        assert tendwire._ready_ref == snapshot["ready_ref"]
        assert tendwire.ack_calls == snapshot["acks"]
        assert tendwire.fail_calls == snapshot["fails"]
        assert tendwire.defer_calls == snapshot["defers"]


@pytest.mark.parametrize(
    "mutation",
    [
        lambda payload: payload.pop("stable_key"),
        lambda payload: payload.__setitem__(
            "stable_key", "wsk1_not_hex"
        ),
        lambda payload: payload.__setitem__(
            "stable_key_version", True
        ),
        lambda payload: payload.__setitem__(
            "worker_id", "private worker id"
        ),
        lambda payload: payload["content"]["fields"][
            "assistant_final_text"
        ].__setitem__("inline", True),
        lambda payload: payload["content"].__setitem__(
            "assistant_final_text", "raw text"
        ),
        lambda payload: payload["content"]["fields"][
            "assistant_final_text"
        ].__setitem__("availability", "absent"),
        lambda payload: payload["content"].__setitem__(
            "content_revision", "twrev1.other"
        ),
    ],
)
def test_final_ready_public_identity_and_descriptors_fail_closed(
    monkeypatch, mutation
):
    monkeypatch.setenv("HERDRES_TENDWIRE_MODE", "source")
    monkeypatch.setenv("HERDRES_PINNED_STATUS", "0")
    tendwire = MutatingReadyTendwire(
        _turn_row(
            "turn-invalid-ready",
            "twrev1.invalid_ready",
            "must not send",
        ),
        mutation,
    )
    telegram = DeletingTelegram()

    result = sync_once(
        _store(), _runtime(tendwire, telegram)
    )

    assert (
        result["tendwire_turn_final"]["status"]
        == "invalid_turn_final_job"
    )
    assert result["tendwire_turn_final"]["failed"] == 1
    assert tendwire.page_calls == []
    assert tendwire.prepare_calls == []
    assert tendwire.ack_calls == []
    assert not any(
        "must not send" in message[1]
        for message in telegram.sent
    )


def test_legacy_v1_final_ready_defers_without_routing_or_attempt(
    monkeypatch,
):
    monkeypatch.setenv("HERDRES_TENDWIRE_MODE", "source")
    monkeypatch.setenv("HERDRES_PINNED_STATUS", "0")

    def legacy_v1(payload):
        payload["schema_version"] = 1
        payload.pop("stable_key")
        payload.pop("stable_key_version")

    tendwire = MutatingReadyTendwire(
        _turn_row(
            "turn-legacy-ready",
            "twrev1.legacy_ready",
            "legacy must not retarget",
        ),
        legacy_v1,
    )
    telegram = DeletingTelegram()
    result = sync_once(
        _store(), _runtime(tendwire, telegram, max_sends=1)
    )

    assert result["tendwire_turn_final"]["deferred"] == 1
    assert result["tendwire_turn_final"]["operations"] == 0
    assert result["tendwire_turn_final"]["failed"] == 0
    assert tendwire.defer_calls[-1][1] == "transient_delivery"
    assert tendwire.page_calls == []
    assert tendwire.prepare_calls == []
    assert tendwire.ack_calls == []
    assert tendwire.fail_calls == []
    assert telegram.sent == []
    assert telegram.edited == []


def test_temporarily_unroutable_root_defers_before_pages_or_plan(
    monkeypatch,
):
    monkeypatch.setenv("HERDRES_TENDWIRE_MODE", "source")
    monkeypatch.setenv("HERDRES_PINNED_STATUS", "0")
    tendwire = _ready_tendwire(
        _turn_row(
            "turn-unroutable-root",
            "twrev1.unroutable_root",
            "must stay durable",
        )
    )
    tendwire.snapshot = lambda: {
        "ok": True,
        "workers": [],
        "spaces": [],
    }
    telegram = DeletingTelegram()
    store = _store()

    result = sync_once(
        store, _runtime(tendwire, telegram, max_sends=1)
    )

    assert result["tendwire_turn_final"]["deferred"] == 1
    assert result["tendwire_turn_final"]["operations"] == 0
    assert result["tendwire_turn_final"]["failed"] == 0
    assert tendwire.defer_calls[-1][1] == "transient_delivery"
    assert tendwire.page_calls == []
    assert tendwire.prepare_calls == []
    assert tendwire.ack_calls == []
    assert tendwire.fail_calls == []
    assert state.tendwire_turn_jobs(store) == {}
    assert state.delivered_turns(store) == {}
    assert telegram.sent == []
    assert telegram.edited == []


def test_prepare_exception_defers_source_root_then_retries_once(
    monkeypatch,
):
    monkeypatch.setenv("HERDRES_TENDWIRE_MODE", "source")
    monkeypatch.setenv("HERDRES_PINNED_STATUS", "0")

    class PrepareFailsOnce(TurnFinalTendwire):
        def __init__(self, row):
            super().__init__(
                row,
                emit_ready=True,
                turn_schema_version=2,
            )
            self.prepare_failure_armed = True

        def connector_prepare_begin(self, **kwargs):
            if self.prepare_failure_armed:
                self.prepare_failure_armed = False
                raise source_sync.TendwireError(
                    "transient prepare transport failure"
                )
            return super().connector_prepare_begin(**kwargs)

    tendwire = PrepareFailsOnce(
        _turn_row(
            "turn-prepare-retry",
            "twrev1.prepare_retry",
            "deliver exactly once after retry",
        )
    )
    tendwire.turns = lambda: {
        "ok": True,
        "schema_version": 2,
        "turns": [],
    }
    telegram = DeletingTelegram()
    store = _store()

    first = sync_once(
        store, _runtime(tendwire, telegram, max_sends=1)
    )
    assert first["tendwire_turn_final"]["deferred"] == 1
    assert first["tendwire_turn_final"]["operations"] == 0
    assert tendwire.defer_calls[-1][1] == "transient_delivery"
    assert telegram.sent == []
    assert telegram.edited == []

    second = sync_once(
        store, _runtime(tendwire, telegram, max_sends=1)
    )
    assert second["tendwire_turn_final"]["delivered"] == 1
    assert second["tendwire_turn_final"]["acked"] == 1
    assert second["tendwire_turn_final"]["operations"] == 1
    assert len(telegram.sent) + len(telegram.edited) == 1


def test_conflicting_job_attached_source_fails_before_second_page_or_send(
    monkeypatch,
):
    monkeypatch.setenv("HERDRES_TENDWIRE_MODE", "source")
    monkeypatch.setenv("HERDRES_PINNED_STATUS", "0")
    monkeypatch.setenv("HERDR_TELEGRAM_TOPICS_RICH_MESSAGES", "0")
    text = "cached immutable response\n\n" * 900
    tendwire = ConflictingAttachedSourceTendwire(
        _turn_row(
            "turn-source-conflict",
            "twrev1.source_conflict",
            text,
        )
    )
    telegram = DeletingTelegram()
    store = _store()

    result = sync_once(
        store, _runtime(tendwire, telegram, max_sends=100)
    )

    assert tendwire.conflict_injected is True
    assert result["tendwire_turn_final"]["acked"] == 1
    assert result["tendwire_turn_final"]["failed"] == 1
    assert (
        result["tendwire_turn_final"]["status"]
        == "invalid_turn_final_job"
    )
    assert len(tendwire.page_calls) == 1
    assert len(telegram.sent) == 1
    assert tendwire.fail_calls[-1][1] == "invalid_turn_final_job"
    assert sum(
        receipt.get("substate") == "acknowledged"
        for receipt in state.tendwire_turn_jobs(store).values()
    ) == 1



@pytest.mark.parametrize("topic_mode", ["worker", "space"])
def test_committed_root_follows_same_stable_owner_through_worker_space_and_account_churn_then_two_syncs_noop(
    monkeypatch, topic_mode
):
    monkeypatch.setenv("HERDRES_TENDWIRE_MODE", "source")
    monkeypatch.setenv("HERDRES_PINNED_STATUS", "0")
    monkeypatch.setenv("HERDRES_SOURCE_TOPIC_MODE", topic_mode)
    monkeypatch.setenv("HERDR_TELEGRAM_TOPICS_MANAGED_BOTS", "1")
    stable_key = _stable_key("worker-A", "fp-A")
    row = _turn_row(
        "turn-committed-churn",
        "twrev1.committed_churn",
        "same owner final",
    )
    row.update(
        {
            "worker_id": "worker-A",
            "worker_fingerprint": "fp-A",
            "space_id": "space-A",
            "stable_key": stable_key,
        }
    )
    tendwire = _ready_tendwire(row)
    tendwire.snapshot_worker_id = "worker-A"
    tendwire.snapshot_fingerprint = "fp-A"
    tendwire.snapshot_space_id = "space-A"
    tendwire.snapshot_worker_name = "Alpha A"
    tendwire.commit_response_lost_once = True
    telegram = DeletingTelegram()
    store = _store()
    store["telegram"]["managed_bots"] = {
        "claude": {"enabled": True, "token": "claude-token"}
    }

    first = sync_once(
        store, _runtime(tendwire, telegram, max_sends=10)
    )

    assert first["tendwire_turn_final"]["status"] == "timeout"
    assert first["tendwire_turn_final"]["operations"] == 0
    assert telegram.sent == []
    original_entry_key, original_entry = (
        state.find_worker_entry_by_stable_key(store, stable_key)
    )
    assert original_entry_key is not None
    assert original_entry is not None
    original_topic_id = str(original_entry["topic_id"])
    final_identity = "twfinal1.committed_churn"
    canonical_owner = {
        "turn_id": row["id"],
        "content_revision": row["content"]["content_revision"],
        "stable_key": stable_key,
        "stable_key_version": 1,
    }
    owners = store["tendwire_turn_final_source_owners"]
    assert owners[final_identity] == canonical_owner
    owners[final_identity].update(
        {
            "worker_id": "worker-A",
            "space_id": "space-A",
            "account_kind": "manager",
            "diagnostic_source": "worker:worker-A",
        }
    )

    tendwire.snapshot_worker_id = "worker-B"
    tendwire.snapshot_fingerprint = "fp-B"
    tendwire.snapshot_space_id = "space-B"
    tendwire.snapshot_worker_name = "Claude B"
    tendwire.snapshot_agent = "claude"
    checkpoints = []
    second = sync_once(
        store,
        _runtime(
            tendwire,
            telegram,
            max_sends=10,
            checkpoint=lambda: checkpoints.append(
                {
                    "store": deepcopy(store),
                    "send_count": len(telegram.sent),
                }
            ),
        ),
    )

    assert second["tendwire_turn_final"]["acked"] == 1
    assert second["tendwire_turn_final"]["operations"] == 1
    assert second["tendwire_turn_final"]["deferred"] == 0
    assert second["tendwire_turn_final"]["failed"] == 0
    assert len(telegram.sent) == 1
    assert any(
        checkpoint["send_count"] == 0
        and checkpoint["store"].get(
            "tendwire_turn_final_source_owners", {}
        ).get(final_identity)
        == canonical_owner
        for checkpoint in checkpoints
    )
    current_entry_key, current_entry = (
        state.find_worker_entry_by_stable_key(store, stable_key)
    )
    assert current_entry_key == original_entry_key
    assert current_entry is original_entry
    assert current_entry["tendwire_worker_id"] == "worker-B"
    assert current_entry["tendwire_fingerprint"] == "fp-B"
    assert current_entry["tendwire_space_id"] == "space-B"
    assert state.entry_stable_identity(current_entry) == (
        stable_key,
        1,
    )
    current_topic_id = str(current_entry["topic_id"])
    if topic_mode == "worker":
        assert current_topic_id == original_topic_id
    else:
        _space_entry_key, space_entry = (
            state.find_space_entry_by_id(store, "space-B")
        )
        assert space_entry is not None
        assert current_topic_id == str(space_entry["topic_id"])
    sent = telegram.sent[0]
    assert sent[2]["thread_id"] == current_topic_id
    assert sent[2]["token"] == "claude-token"
    binding = state.find_message_binding(
        store, sent[3], topic_id=current_topic_id
    )
    assert binding is not None
    assert binding["worker_id"] == "worker-B"
    assert binding["worker_fingerprint"] == "fp-B"
    assert binding["space_id"] == "space-B"
    assert binding["bot_kind"] == "claude"
    assert "tendwire_turn_final_source_owners" not in store

    stable_snapshot = {
        "store": deepcopy(store),
        "sent": deepcopy(telegram.sent),
        "edited": deepcopy(telegram.edited),
        "deleted": deepcopy(telegram.deleted_messages),
        "api_calls": deepcopy(telegram.api_calls),
        "topics": deepcopy(telegram.topics),
        "pages": deepcopy(tendwire.page_calls),
        "prepare": deepcopy(tendwire.prepare_calls),
        "source_refs": deepcopy(tendwire.source_prepare_refs),
        "plans": deepcopy(tendwire._plans),
        "jobs": deepcopy(tendwire._jobs),
        "ready_state": deepcopy(tendwire._ready_state),
        "ready_ref": deepcopy(tendwire._ready_ref),
        "acks": deepcopy(tendwire.ack_calls),
        "fails": deepcopy(tendwire.fail_calls),
        "defers": deepcopy(tendwire.defer_calls),
    }
    for _forced_index in range(2):
        forced = sync_once(
            store, _runtime(tendwire, telegram, max_sends=10)
        )
        final = forced["tendwire_turn_final"]
        assert final["polled"] == 0
        assert final["staged"] == 0
        assert final["operations"] == 0
        assert final["delivered"] == 0
        assert final["acked"] == 0
        assert final["failed"] == 0
        assert final["deferred"] == 0
        assert final["uncertain"] == 0
        assert final["content_pages"] == 0
        assert final["changed"] is False
        assert forced["content_pages"] == 0
        assert forced["sent"] == 0
        assert forced["turn_updates"] == 0
        assert store == stable_snapshot["store"]
        assert telegram.sent == stable_snapshot["sent"]
        assert telegram.edited == stable_snapshot["edited"]
        assert telegram.deleted_messages == stable_snapshot["deleted"]
        assert telegram.api_calls == stable_snapshot["api_calls"]
        assert telegram.topics == stable_snapshot["topics"]
        assert tendwire.page_calls == stable_snapshot["pages"]
        assert tendwire.prepare_calls == stable_snapshot["prepare"]
        assert tendwire.source_prepare_refs == stable_snapshot[
            "source_refs"
        ]
        assert tendwire._plans == stable_snapshot["plans"]
        assert tendwire._jobs == stable_snapshot["jobs"]
        assert tendwire._ready_state == stable_snapshot["ready_state"]
        assert tendwire._ready_ref == stable_snapshot["ready_ref"]
        assert tendwire.ack_calls == stable_snapshot["acks"]
        assert tendwire.fail_calls == stable_snapshot["fails"]
        assert tendwire.defer_calls == stable_snapshot["defers"]


@pytest.mark.parametrize(
    ("mutation", "value"),
    [
        ("record", None),
        ("delete_content_revision", None),
        ("turn_id", "turn-other"),
        ("content_revision", "twrev1.other"),
        ("stable_key", "wsk1_" + ("e" * 64)),
        ("stable_key_version", True),
        ("stable_key_version", 2),
    ],
)
def test_existing_final_source_owner_rejects_malformed_or_different_immutable_record_before_side_effects(
    monkeypatch, mutation, value
):
    monkeypatch.setenv("HERDRES_TENDWIRE_MODE", "source")
    monkeypatch.setenv("HERDRES_PINNED_STATUS", "0")
    monkeypatch.setenv("HERDRES_SOURCE_TOPIC_MODE", "worker")
    row = _turn_row(
        "turn-owner-collision",
        "twrev1.owner_collision",
        "must not cross immutable owner",
    )
    stable_key = row["stable_key"]
    tendwire = _ready_tendwire(row)
    tendwire.commit_response_lost_once = True
    telegram = DeletingTelegram()
    store = _store()

    first = sync_once(
        store, _runtime(tendwire, telegram, max_sends=10)
    )

    assert first["tendwire_turn_final"]["status"] == "timeout"
    final_identity = "twfinal1.owner_collision"
    owners = store["tendwire_turn_final_source_owners"]
    assert final_identity in owners
    if mutation == "record":
        owners[final_identity] = value
    elif mutation == "delete_content_revision":
        owners[final_identity].pop("content_revision")
    else:
        owners[final_identity][mutation] = value
    invalid_record = deepcopy(owners[final_identity])
    pages_before = deepcopy(tendwire.page_calls)
    prepare_before = deepcopy(tendwire.prepare_calls)
    tendwire.snapshot_worker_id = "worker-B"
    tendwire.snapshot_fingerprint = "fp-B"
    tendwire.snapshot_space_id = "space-B"

    rejected = sync_once(
        store, _runtime(tendwire, telegram, max_sends=10)
    )

    assert rejected["tendwire_turn_final"]["deferred"] == 1
    assert rejected["tendwire_turn_final"]["operations"] == 0
    assert rejected["tendwire_turn_final"]["acked"] == 0
    assert rejected["tendwire_turn_final"]["failed"] == 0
    assert tendwire.page_calls == pages_before
    assert tendwire.prepare_calls == prepare_before
    assert telegram.sent == []
    assert owners[final_identity] == invalid_record
    entry_key, entry = state.find_worker_entry_by_stable_key(
        store, stable_key
    )
    assert entry_key is not None
    assert entry is not None
    assert entry["tendwire_worker_id"] == "worker-B"
    assert entry["tendwire_fingerprint"] == "fp-B"
    assert entry["tendwire_space_id"] == "space-B"


def test_recycled_worker_id_cannot_retarget_old_stable_root(
    monkeypatch,
):
    monkeypatch.setenv("HERDRES_TENDWIRE_MODE", "source")
    monkeypatch.setenv("HERDRES_PINNED_STATUS", "0")
    row = _turn_row(
        "turn-recycled-root",
        "twrev1.recycled_root",
        "old owner content",
    )
    stable_key_k1 = row["stable_key"]
    stable_key_k2 = "wsk1_" + ("a" * 64)
    tendwire = TurnFinalTendwire(
        row,
        emit_ready=False,
        turn_schema_version=2,
    )
    telegram = DeletingTelegram()
    store = _store()
    sync_once(store, _runtime(tendwire, telegram))
    original_entry_key, original_entry = (
        state.find_worker_entry_by_stable_key(
            store, stable_key_k1
        )
    )
    assert original_entry_key is not None
    assert original_entry is not None
    original_topic_id = str(original_entry["topic_id"])
    tendwire.emit_ready = True
    tendwire.snapshot_fingerprint = "fp-replacement"
    tendwire.snapshot_stable_key = stable_key_k2
    before = deepcopy(store)

    result = sync_once(
        store, _runtime(tendwire, telegram, max_sends=1)
    )

    assert result["tendwire_turn_final"]["deferred"] == 1
    assert result["tendwire_turn_final"]["operations"] == 0
    assert result["tendwire_turn_final"]["failed"] == 0
    assert tendwire.defer_calls[-1][1] == "transient_delivery"
    assert tendwire.page_calls == []
    assert tendwire.prepare_calls == []
    assert tendwire.ack_calls == []
    assert tendwire.fail_calls == []
    assert state.delivered_turns(store) == {}
    assert before.get("tendwire_turn_final_source_owners") is None
    assert store.get("tendwire_turn_final_source_owners") is None
    assert telegram.sent == []
    assert telegram.edited == []
    retained_k1 = state.source_worker_entries(store)[
        original_entry_key
    ]
    assert state.entry_stable_identity(retained_k1) == (
        stable_key_k1,
        1,
    )
    assert retained_k1["topic_id"] == original_topic_id
    k2_entries = [
        (entry_key, entry)
        for entry_key, entry in state.source_worker_entries(
            store
        ).items()
        if state.entry_stable_identity(entry)
        == (stable_key_k2, 1)
    ]
    assert len(k2_entries) == 1
    assert k2_entries[0][0] != original_entry_key
    assert (
        state.entry_stable_identity(k2_entries[0][1])
        != state.entry_stable_identity(retained_k1)
    )
    assert store.get("telegram_message_bindings") in (None, {})


def test_applied_blank_token_restart_rejects_recycled_worker_owner(
    monkeypatch,
):
    monkeypatch.setenv("HERDRES_TENDWIRE_MODE", "source")
    monkeypatch.setenv("HERDRES_PINNED_STATUS", "0")
    row = _turn_row(
        "turn-applied-recycled",
        "twrev1.applied_recycled",
        "send once to original owner",
    )
    tendwire = _ready_tendwire(row)
    tendwire.ack_loss_once = True
    telegram = DeletingTelegram()
    store = _store()
    first = sync_once(
        store, _runtime(tendwire, telegram, max_sends=1)
    )
    receipt = next(iter(state.tendwire_turn_jobs(store).values()))
    assert first["tendwire_turn_final"]["status"] == "timeout"
    assert receipt["substate"] == "telegram_applied"
    original_sends = deepcopy(telegram.sent)
    original_pages = deepcopy(tendwire.page_calls)
    original_acks = deepcopy(tendwire.ack_calls)
    entry = next(iter(state.source_worker_entries(store).values()))
    for field in (
        "pending_turn_id",
        "pending_content_revision",
        "pending_plan_token",
        "pending_turn_part_count",
        "pending_turn_job_count",
        "pending_turn_user_hash",
        "pending_plan_generation",
        "pending_acknowledged_prefix_count",
        "replaces_failed_plan_token",
        "pending_final_identity",
    ):
        entry.pop(field, None)
    tendwire.snapshot_fingerprint = "fp-replacement"
    tendwire.snapshot_stable_key = "wsk1_" + ("b" * 64)

    resumed = sync_once(
        store, _runtime(tendwire, telegram, max_sends=1)
    )

    assert resumed["tendwire_turn_final"]["deferred"] == 1
    assert resumed["tendwire_turn_final"]["operations"] == 0
    assert resumed["tendwire_turn_final"]["acked"] == 0
    assert resumed["tendwire_turn_final"]["failed"] == 0
    assert tendwire.defer_calls[-1][1] == "transient_delivery"
    assert telegram.sent == original_sends
    assert tendwire.page_calls == original_pages
    assert tendwire.ack_calls == original_acks
    assert receipt["substate"] == "telegram_applied"


def test_v2_turn_list_never_prepares_or_marks_final_without_outbox_ack(
    monkeypatch,
):
    monkeypatch.setenv("HERDRES_TENDWIRE_MODE", "source")
    monkeypatch.setenv("HERDRES_PINNED_STATUS", "0")
    tendwire = TurnFinalTendwire(
        _turn_row(
            "turn-list-only",
            "twrev1.list_only",
            "historical list final",
        ),
        turn_schema_version=2,
    )
    telegram = DeletingTelegram()
    store = _store()

    result = sync_once(
        store,
        SyncRuntime(tendwire, telegram, with_outbox=False),
    )

    assert result["feed_sent"] == 0
    assert tendwire.prepare_calls == []
    assert state.delivered_turns(store) == {}
    assert not any(
        "historical list final" in message[1]
        for message in telegram.sent
    )


def test_source_less_plan_recovers_exact_v2_root_by_stable_key_after_owner_churn(
    monkeypatch,
):
    monkeypatch.setenv("HERDRES_TENDWIRE_MODE", "source")
    monkeypatch.setenv("HERDRES_PINNED_STATUS", "0")
    monkeypatch.setenv("HERDR_TELEGRAM_TOPICS_MANAGED_BOTS", "1")
    final = "source-less staged final"
    stable_key = _stable_key("worker-A", "fp-A")
    row = _turn_row(
        "turn-source-less-plan",
        "twrev1.source_less_plan",
        final,
    )
    row.update(
        {
            "worker_id": "worker-A",
            "worker_fingerprint": "fp-A",
            "space_id": "space-A",
            "stable_key": stable_key,
        }
    )
    tendwire = TurnFinalTendwire(
        row,
        turn_schema_version=2,
    )
    tendwire.attach_plan_source = False
    tendwire.snapshot_worker_id = "worker-B"
    tendwire.snapshot_fingerprint = "fp-B"
    tendwire.snapshot_space_id = "space-B"
    tendwire.snapshot_worker_name = "Claude B"
    tendwire.snapshot_agent = "claude"
    begun = tendwire.connector_prepare_begin(
        turn_id=row["id"],
        content_revision=row["content"]["content_revision"],
        presentation_version=PRESENTATION_VERSION,
        part_count=1,
    )
    tendwire.connector_prepare_part(
        plan_token=begun["plan_token"],
        ordinal=0,
        spans=[
            {
                "field": "assistant_final_text",
                "start_char": 0,
                "end_char": len(final),
            }
        ],
    )
    tendwire.connector_prepare_commit(
        plan_token=begun["plan_token"]
    )
    assert tendwire._jobs
    assert all(
        "turn" not in job["payload"] for job in tendwire._jobs
    )
    telegram = DeletingTelegram()
    store = _store()
    store["telegram"]["managed_bots"] = {
        "claude": {"enabled": True, "token": "claude-token"}
    }

    result = sync_once(
        store, _runtime(tendwire, telegram)
    )

    assert result["tendwire_turn_final"]["acked"] == 1
    assert result["tendwire_turn_final"]["operations"] == 1
    assert result["tendwire_turn_final"]["deferred"] == 0
    assert result["tendwire_turn_final"]["failed"] == 0
    assert tendwire.defer_calls == []
    assert tendwire.fail_calls == []
    assert len(telegram.sent) == 1
    assert final in telegram.sent[0][1]
    entry_key, entry = state.find_worker_entry_by_stable_key(
        store, stable_key
    )
    assert entry_key is not None
    assert entry is not None
    assert entry["tendwire_worker_id"] == "worker-B"
    assert entry["tendwire_fingerprint"] == "fp-B"
    assert entry["tendwire_space_id"] == "space-B"
    assert state.entry_stable_identity(entry) == (stable_key, 1)
    assert telegram.sent[0][2]["thread_id"] == entry["topic_id"]
    assert telegram.sent[0][2]["token"] == "claude-token"
    binding = state.find_message_binding(
        store,
        telegram.sent[0][3],
        topic_id=entry["topic_id"],
    )
    assert binding is not None
    assert binding["worker_id"] == "worker-B"
    assert binding["space_id"] == "space-B"
    assert binding["bot_kind"] == "claude"
    assert "tendwire_turn_final_source_owners" not in store


def test_turn_final_lease_seconds_default_and_bounds():
    assert config.tendwire_turn_final_lease_seconds(env={}) == 60
    assert (
        config.tendwire_turn_final_lease_seconds(
            env={
                "HERDRES_TENDWIRE_TURN_FINAL_LEASE_SECONDS": "120"
            }
        )
        == 120
    )
    assert (
        config.tendwire_turn_final_lease_seconds(
            env={
                "HERDRES_TENDWIRE_TURN_FINAL_LEASE_SECONDS": "59"
            }
        )
        == 60
    )
    assert (
        config.tendwire_turn_final_lease_seconds(
            env={
                "HERDRES_TENDWIRE_TURN_FINAL_LEASE_SECONDS": "3601"
            }
        )
        == 3600
    )
    assert (
        config.tendwire_turn_final_lease_seconds(
            env={
                "HERDRES_TENDWIRE_TURN_FINAL_LEASE_SECONDS": "invalid"
            }
        )
        == 60
    )


def test_slow_final_materialization_stays_within_configured_root_lease(
    monkeypatch,
):
    monkeypatch.setenv("HERDRES_TENDWIRE_MODE", "source")
    monkeypatch.setenv("HERDRES_PINNED_STATUS", "0")
    monkeypatch.setenv(
        "HERDRES_TENDWIRE_TURN_FINAL_LEASE_SECONDS", "120"
    )
    tendwire = SlowPageTendwire(
        _turn_row(
            "turn-slow-pages",
            "twrev1.slow_pages",
            "slow canonical final",
            user="slow canonical prompt",
        ),
        page_seconds=40,
    )
    telegram = DeletingTelegram()
    store = _store()

    delivered = sync_once(
        store, _runtime(tendwire, telegram, max_sends=1)
    )
    page_calls = deepcopy(tendwire.page_calls)
    prepare_calls = deepcopy(tendwire.prepare_calls)
    ack_calls = deepcopy(tendwire.ack_calls)
    forced = sync_once(
        store, _runtime(tendwire, telegram, max_sends=1)
    )

    assert tendwire.clock == 80
    assert 60 < tendwire.clock < 120
    assert tendwire.ready_lease_seconds == [120]
    assert set(tendwire.poll_lease_seconds) == {120}
    assert [call[2] for call in page_calls] == [
        "user_text",
        "assistant_final_text",
    ]
    assert (
        delivered["tendwire_turn_final"]["content_pages"] == 2
    )
    assert delivered["tendwire_turn_final"]["staged"] == 1
    assert delivered["tendwire_turn_final"]["acked"] == 1
    assert len(ack_calls) == 1
    assert [
        call[0] for call in tendwire.source_prepare_refs
    ] == ["begin", "commit"]
    assert forced["tendwire_turn_final"]["polled"] == 0
    assert tendwire.page_calls == page_calls
    assert tendwire.prepare_calls == prepare_calls
    assert tendwire.ack_calls == ack_calls
    assert tendwire.fail_calls == []
    assert tendwire.defer_calls == []


def test_checkpoint_before_ack_loss_resumes_by_stable_key_without_resend(
    monkeypatch,
):
    monkeypatch.setenv("HERDRES_TENDWIRE_MODE", "source")
    monkeypatch.setenv("HERDRES_PINNED_STATUS", "0")
    monkeypatch.setenv("HERDRES_SOURCE_TOPIC_MODE", "worker")
    tendwire = _ready_tendwire(
        _turn_row(
            "turn-ack-root",
            "twrev1.ack_root",
            "checkpointed answer",
        )
    )
    stable_key = tendwire.row["stable_key"]
    tendwire.ack_loss_once = True
    telegram = DeletingTelegram()
    store = _store()
    checkpoints = []

    first = sync_once(
        store,
        _runtime(
            tendwire,
            telegram,
            max_sends=1,
            checkpoint=lambda: checkpoints.append(
                deepcopy(state.tendwire_turn_jobs(store))
            ),
        ),
    )
    sent_after_first = len(telegram.sent)
    pages_after_first = deepcopy(tendwire.page_calls)
    first_ref = tendwire.ack_calls[-1][0]
    receipt_key = next(iter(state.tendwire_turn_jobs(store)))
    assert any(
        snapshot.get(receipt_key, {}).get("substate")
        == "reserved"
        for snapshot in checkpoints
    )
    original_entry_key, entry = (
        state.find_worker_entry_by_stable_key(store, stable_key)
    )
    assert original_entry_key is not None
    assert entry is not None
    for field in (
        "pending_turn_id",
        "pending_content_revision",
        "pending_plan_token",
        "pending_turn_part_count",
        "pending_turn_job_count",
        "pending_turn_user_hash",
        "pending_plan_generation",
        "pending_acknowledged_prefix_count",
        "replaces_failed_plan_token",
    ):
        entry.pop(field, None)
    tendwire.snapshot_worker_id = "worker-B"
    tendwire.snapshot_fingerprint = "fp-B"
    tendwire.snapshot_space_id = "space-B"
    tendwire.snapshot_worker_name = "Claude B"
    tendwire.snapshot_agent = "claude"
    tendwire.turns = lambda: {
        "ok": True,
        "schema_version": 2,
        "turns": [],
    }

    second = sync_once(
        store,
        _runtime(tendwire, telegram, max_sends=1),
    )

    assert first["tendwire_turn_final"]["operations"] == 1
    assert first["tendwire_turn_final"]["acked"] == 0
    assert second["tendwire_turn_final"]["operations"] == 0
    assert second["tendwire_turn_final"]["acked"] == 1
    assert tendwire.ack_calls[-1][0] != first_ref
    assert len(telegram.sent) == sent_after_first
    assert tendwire.page_calls == pages_after_first
    assert (
        state.tendwire_turn_jobs(store)[receipt_key]["substate"]
        == "acknowledged"
    )
    current_entry_key, current_entry = (
        state.find_worker_entry_by_stable_key(store, stable_key)
    )
    assert current_entry_key == original_entry_key
    assert current_entry is entry
    assert current_entry["tendwire_worker_id"] == "worker-B"
    assert current_entry["tendwire_fingerprint"] == "fp-B"
    assert current_entry["tendwire_space_id"] == "space-B"
    assert state.entry_stable_identity(current_entry) == (
        stable_key,
        1,
    )
    assert second["tendwire_turn_final"]["deferred"] == 0
    assert second["tendwire_turn_final"]["failed"] == 0
    assert tendwire.defer_calls == []
    assert tendwire.fail_calls == []
    assert "tendwire_turn_final_source_owners" not in store


def test_commit_response_loss_resumes_from_job_attached_source_without_turn_list(
    monkeypatch,
):
    monkeypatch.setenv("HERDRES_TENDWIRE_MODE", "source")
    monkeypatch.setenv("HERDRES_PINNED_STATUS", "0")
    tendwire = _ready_tendwire(
        _turn_row(
            "turn-commit-loss",
            "twrev1.commit_loss",
            "exact source-backed answer",
            user="source-backed prompt",
        )
    )
    tendwire.commit_response_lost_once = True
    telegram = DeletingTelegram()
    store = _store()

    lost = sync_once(
        store, _runtime(tendwire, telegram, max_sends=10)
    )
    assert lost["tendwire_turn_final"]["status"] == "timeout"
    assert lost["tendwire_turn_final"]["operations"] == 0
    assert telegram.sent == []
    assert [
        call[0] for call in tendwire.source_prepare_refs[:2]
    ] == ["begin", "commit"]
    assert (
        tendwire.source_prepare_refs[0][1]
        == tendwire.source_prepare_refs[1][1]
    )
    tendwire.turns = lambda: {
        "ok": True,
        "schema_version": 2,
        "turns": [],
    }
    entry = next(
        iter(state.source_worker_entries(store).values())
    )
    for field in tuple(entry):
        if field.startswith("pending_"):
            entry.pop(field, None)

    resumed = sync_once(
        store, _runtime(tendwire, telegram, max_sends=10)
    )
    sends = len(telegram.sent)
    forced = sync_once(
        store, _runtime(tendwire, telegram, max_sends=10)
    )

    assert resumed["tendwire_turn_final"]["acked"] == 1
    assert forced["tendwire_turn_final"]["polled"] == 0
    assert len(telegram.sent) == sends == 1
    assert all(
        not ref.startswith("twref1.ready")
        for ref, _response in tendwire.ack_calls
    )


def test_restart_reconciles_committed_last_part_ack_without_turn_list_or_resend(
    monkeypatch,
):
    monkeypatch.setenv("HERDRES_TENDWIRE_MODE", "source")
    monkeypatch.setenv("HERDRES_PINNED_STATUS", "0")
    tendwire = _ready_tendwire(
        _turn_row(
            "turn-last-ack-crash",
            "twrev1.last_ack_crash",
            "provider accepted exactly once",
        )
    )
    tendwire.ack_committed_response_lost_once = True
    tendwire.completed_observe_lost_once = True
    telegram = DeletingTelegram()
    store = _store()

    interrupted = sync_once(
        store, _runtime(tendwire, telegram, max_sends=1)
    )
    sends = len(telegram.sent)
    receipt = next(
        iter(state.tendwire_turn_jobs(store).values())
    )
    assert interrupted["tendwire_turn_final"]["status"] == "timeout"
    assert receipt["substate"] == "telegram_applied"
    tendwire.turns = lambda: {
        "ok": True,
        "schema_version": 2,
        "turns": [],
    }

    resumed = sync_once(
        store, _runtime(tendwire, telegram, max_sends=1)
    )

    assert resumed["tendwire_turn_final"]["polled"] == 0
    assert len(telegram.sent) == sends == 1
    assert receipt["substate"] == "acknowledged"
    entry = next(
        iter(state.source_worker_entries(store).values())
    )
    assert (
        entry["last_clean_content_revision"]
        == "twrev1.last_ack_crash"
    )
    assert "pending_plan_token" not in entry


def test_committed_ack_response_loss_recovers_completed_plan_without_resend(
    monkeypatch,
):
    monkeypatch.setenv("HERDRES_TENDWIRE_MODE", "source")
    monkeypatch.setenv("HERDRES_PINNED_STATUS", "0")
    tendwire = _ready_tendwire(
        _turn_row(
            "turn-committed-ack-root",
            "twrev1.committed_ack_root",
            "durably applied",
        )
    )
    tendwire.ack_committed_response_lost_once = True
    telegram = DeletingTelegram()
    store = _store()

    first = sync_once(
        store, _runtime(tendwire, telegram, max_sends=1)
    )
    sends = len(telegram.sent)
    second = sync_once(
        store, _runtime(tendwire, telegram, max_sends=1)
    )

    assert first["tendwire_turn_final"]["status"] == "timeout"
    assert second["tendwire_turn_final"]["polled"] == 0
    assert len(telegram.sent) == sends == 1
    entry = next(
        iter(state.source_worker_entries(store).values())
    )
    assert (
        entry["last_clean_content_revision"]
        == "twrev1.committed_ack_root"
    )
    assert "pending_plan_token" not in entry
    assert (
        next(iter(state.tendwire_turn_jobs(store).values()))[
            "substate"
        ]
        == "acknowledged"
    )


@pytest.mark.parametrize(
    "obsolete_state", ["superseded", "plan_not_found"]
)
def test_restart_clears_obsolete_pending_plan_and_delivers_newer_root(
    monkeypatch, obsolete_state
):
    monkeypatch.setenv("HERDRES_TENDWIRE_MODE", "source")
    monkeypatch.setenv("HERDRES_PINNED_STATUS", "0")
    turn_id = "turn-obsolete-restart"
    tendwire = PlanRetentionTendwire(
        _turn_row(
            turn_id, "twrev1.r0", "last clean revision"
        )
    )
    telegram = DeletingTelegram()
    store = _store()

    initial = sync_once(
        store, _runtime(tendwire, telegram, max_sends=1)
    )
    assert initial["tendwire_turn_final"]["acked"] == 1
    entry = next(
        iter(state.source_worker_entries(store).values())
    )
    assert entry["last_clean_content_revision"] == "twrev1.r0"

    tendwire.row = _turn_row(
        turn_id, "twrev1.r1", "applied before restart"
    )
    tendwire.ack_committed_response_lost_once = True
    tendwire.completed_observe_lost_once = True
    interrupted = sync_once(
        store, _runtime(tendwire, telegram, max_sends=1)
    )
    assert interrupted["tendwire_turn_final"]["status"] == "timeout"
    pending_plan = entry["pending_plan_token"]
    assert entry["pending_content_revision"] == "twrev1.r1"
    assert entry["last_clean_content_revision"] == "twrev1.r0"
    pending_receipt_key, pending_receipt = next(
        (job_key, deepcopy(receipt))
        for job_key, receipt in state.tendwire_turn_jobs(
            store
        ).items()
        if receipt.get("plan_token") == pending_plan
    )
    assert pending_receipt["substate"] == "telegram_applied"
    pending_bindings = deepcopy(
        state.message_bindings(store)
    )

    tendwire.row = _turn_row(
        turn_id,
        "twrev1.r2",
        "new authoritative revision",
    )
    if obsolete_state == "superseded":
        tendwire.completed_observe_lost_once = True
        tendwire.supersede_on_ready = pending_plan
    else:
        tendwire.missing_plans.add(pending_plan)
    checkpoints = []
    recovered = sync_once(
        store,
        _runtime(
            tendwire,
            telegram,
            max_sends=1,
            checkpoint=lambda: checkpoints.append(
                deepcopy(store)
            ),
        ),
    )

    assert recovered["tendwire_turn_final"]["staged"] == 1
    assert recovered["tendwire_turn_final"]["acked"] == 1
    assert recovered["tendwire_turn_final"]["operations"] == 1
    cleared_store = next(
        snapshot
        for snapshot in checkpoints
        if "pending_plan_token"
        not in next(
            iter(
                state.source_worker_entries(
                    snapshot
                ).values()
            )
        )
    )
    cleared_entry = next(
        iter(state.source_worker_entries(cleared_store).values())
    )
    for field in (
        "pending_turn_id",
        "pending_content_revision",
        "pending_plan_token",
        "pending_turn_part_count",
        "pending_turn_job_count",
        "pending_turn_user_hash",
        "pending_plan_generation",
        "pending_acknowledged_prefix_count",
        "replaces_failed_plan_token",
    ):
        assert field not in cleared_entry
    assert (
        cleared_entry["last_clean_content_revision"]
        == "twrev1.r0"
    )
    assert (
        state.tendwire_turn_jobs(cleared_store)[
            pending_receipt_key
        ]
        == pending_receipt
    )
    assert (
        state.message_bindings(cleared_store)
        == pending_bindings
    )
    assert (
        state.tendwire_turn_jobs(store)[pending_receipt_key]
        == pending_receipt
    )
    assert entry["last_clean_content_revision"] == "twrev1.r2"
    assert all(
        not field.startswith("pending_turn_")
        for field in entry
    )


@pytest.mark.parametrize(
    "unresolved_state", ["unknown", "error"]
)
def test_newer_root_defers_while_pending_plan_is_not_strictly_obsolete(
    monkeypatch, unresolved_state
):
    monkeypatch.setenv("HERDRES_TENDWIRE_MODE", "source")
    monkeypatch.setenv("HERDRES_PINNED_STATUS", "0")
    turn_id = "turn-unresolved-restart"
    tendwire = PlanRetentionTendwire(
        _turn_row(
            turn_id,
            "twrev1.pending",
            "pending revision",
        )
    )
    tendwire.ack_committed_response_lost_once = True
    tendwire.completed_observe_lost_once = True
    telegram = DeletingTelegram()
    store = _store()
    interrupted = sync_once(
        store, _runtime(tendwire, telegram, max_sends=1)
    )
    assert interrupted["tendwire_turn_final"]["status"] == "timeout"
    entry = next(
        iter(state.source_worker_entries(store).values())
    )
    pending = {
        field: entry[field]
        for field in (
            "pending_turn_id",
            "pending_content_revision",
            "pending_plan_token",
            "pending_turn_part_count",
            "pending_turn_job_count",
            "pending_turn_user_hash",
            "pending_plan_generation",
        )
    }
    receipts = deepcopy(state.tendwire_turn_jobs(store))
    pending_plan = pending["pending_plan_token"]
    if unresolved_state == "error":
        tendwire.plan_errors.add(pending_plan)
    else:
        tendwire._plans[pending_plan][
            "state"
        ] = unresolved_state
    tendwire.row = _turn_row(
        turn_id,
        "twrev1.newer",
        "must remain queued",
    )
    sends = deepcopy(telegram.sent)
    edits = deepcopy(telegram.edited)
    pages = deepcopy(tendwire.page_calls)

    deferred = sync_once(
        store, _runtime(tendwire, telegram, max_sends=1)
    )

    assert deferred["tendwire_turn_final"]["staged"] == 0
    assert deferred["tendwire_turn_final"]["operations"] == 0
    assert deferred["tendwire_turn_final"]["deferred"] == 1
    assert tendwire.defer_calls[-1][1] == "predecessor_pending"
    assert {field: entry[field] for field in pending} == pending
    assert state.tendwire_turn_jobs(store) == receipts
    assert telegram.sent == sends
    assert telegram.edited == edits
    assert tendwire.page_calls == pages


def test_dead_letter_pending_plan_is_cleared_before_newer_final_delivers(
    monkeypatch,
):
    monkeypatch.setenv("HERDRES_TENDWIRE_MODE", "source")
    monkeypatch.setenv("HERDRES_PINNED_STATUS", "0")
    turn_id = "turn-dead-predecessor"
    tendwire = PlanRetentionTendwire(
        _turn_row(
            turn_id,
            "twrev1.dead_predecessor",
            "abandoned final",
        )
    )
    tendwire.ack_committed_response_lost_once = True
    tendwire.completed_observe_lost_once = True
    telegram = DeletingTelegram()
    store = _store()

    interrupted = sync_once(
        store, _runtime(tendwire, telegram, max_sends=1)
    )
    assert interrupted["tendwire_turn_final"]["status"] == "timeout"
    entry = next(iter(state.source_worker_entries(store).values()))
    dead_plan = entry["pending_plan_token"]
    dead_revision = entry["pending_content_revision"]
    dead_receipt_key, dead_receipt = next(
        (key, receipt)
        for key, receipt in state.tendwire_turn_jobs(store).items()
        if receipt.get("plan_token") == dead_plan
    )
    state.update_tendwire_turn_job(
        store, dead_receipt_key, substate="failed"
    )
    dead_job = next(
        job
        for job in tendwire._jobs
        if job["payload"]["plan_token"] == dead_plan
    )
    dead_job["status"] = "dead_letter"
    tendwire._plans[dead_plan]["state"] = "failed"
    tendwire.row = _turn_row(
        turn_id,
        "twrev1.after_dead_predecessor",
        "new final proceeds",
    )
    checkpoints = []

    delivered = sync_once(
        store,
        _runtime(
            tendwire,
            telegram,
            max_sends=1,
            checkpoint=lambda: checkpoints.append(deepcopy(store)),
        ),
    )

    assert delivered["tendwire_turn_final"]["deferred"] == 0
    assert delivered["tendwire_turn_final"]["acked"] == 1
    assert tendwire.defer_calls == []
    assert entry["last_clean_content_revision"] == (
        "twrev1.after_dead_predecessor"
    )
    assert "pending_plan_token" not in entry
    assert "pending_content_revision" not in entry
    cleared_entry = next(
        next(iter(state.source_worker_entries(snapshot).values()))
        for snapshot in checkpoints
        if "pending_plan_token"
        not in next(iter(state.source_worker_entries(snapshot).values()))
        and next(iter(state.source_worker_entries(snapshot).values())).get(
            "abandoned_plan_token"
        )
        == dead_plan
    )
    assert cleared_entry["abandoned_content_revision"] == dead_revision
    assert dead_receipt["substate"] == "failed"

class SensitiveProviderErrorTelegram(DeletingTelegram):
    def __init__(self, kind):
        super().__init__()
        self.kind = kind

    def send_message(self, chat_id, html, **kwargs):
        return {
            "ok": False,
            "kind": self.kind,
            "error": (
                "Telegram rejected message 12345 in topic "
                "-100987654; bot token secret-987"
            ),
        }


@pytest.mark.parametrize(
    ("kind", "expected_reason", "call_log"),
    [
        ("permanent", "delivery_rejected", "fail_calls"),
        ("transient", "transient_delivery", "defer_calls"),
    ],
)
def test_provider_errors_use_backend_neutral_turn_final_reason_codes(
    monkeypatch,
    kind,
    expected_reason,
    call_log,
):
    monkeypatch.setenv("HERDRES_TENDWIRE_MODE", "source")
    monkeypatch.setenv("HERDRES_PINNED_STATUS", "0")
    monkeypatch.setenv(
        "HERDR_TELEGRAM_TOPICS_RICH_MESSAGES", "0"
    )
    tendwire = _ready_tendwire(
        _turn_row(
            "turn-private-error",
            "twrev1.private_error",
            "answer",
        )
    )
    telegram = SensitiveProviderErrorTelegram(kind)

    result = sync_once(
        _store(),
        _runtime(tendwire, telegram, max_sends=1),
    )

    captured = getattr(tendwire, call_log)
    assert len(captured) == 1
    reason = captured[0][1]
    assert reason == expected_reason
    assert all(
        private not in reason.lower()
        for private in (
            "telegram",
            "message",
            "topic",
            "bot",
            "token",
            "12345",
            "-100987654",
            "secret-987",
        )
    )
    if kind == "permanent":
        assert (
            result["tendwire_turn_final"]["status"]
            == "delivery_rejected"
        )
    else:
        assert (
            result["tendwire_turn_final"]["deferred"] == 1
        )
