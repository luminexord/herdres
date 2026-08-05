from __future__ import annotations

import base64
from copy import deepcopy
import hashlib
import json
import time

import pytest
from herdres_connector import config, doctor, source_sync, state
from herdres_connector.ingress_queue import IngressQueue
from herdres_connector.rich_delivery import (
    RICH_SPLIT_CHUNK_CHARS,
    TURN_DELIVERY_PLAIN_SOURCE_CHARS,
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
        emit_ready=True,
        turn_schema_version=2,
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
        self.delta_calls = []

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

    def turn_delta(self, *, cursor=None, watermark=None, limit=500):
        self.delta_calls.append(
            {"cursor": cursor, "watermark": watermark, "limit": limit}
        )
        assert cursor is None
        assert limit > 0
        payload = self.turns()
        rows = deepcopy(payload.get("turns", []))
        revision = hashlib.sha256(
            json.dumps(rows, sort_keys=True, default=str).encode("utf-8")
        ).hexdigest()
        checkpoint = f"twdelta1.turn_final_{revision}"
        changes = []
        if watermark != checkpoint:
            changes.extend(
                {
                    "op": "upsert",
                    "turn_id": str(
                        row.get("id") or row.get("turn_id") or ""
                    ),
                    "changed_at": "2030-01-01T00:00:00Z",
                    "turn": row,
                }
                if isinstance(row, dict)
                else row
                for row in rows
            )
        return {
            "schema_version": 1,
            "projection_schema_version": payload.get("schema_version"),
            "host_id": "turn-final-fake-tendwire",
            "mode": "bootstrap" if watermark is None else "changes",
            "changes": changes,
            "has_more": False,
            "next_cursor": None,
            "checkpoint": checkpoint,
            "aggregate": {
                "journal_rows_scanned": len(changes),
                "projection_rows_read": len(rows),
                "changes_returned": len(changes),
                "duration_ms": 1,
            },
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
            raise TelegramError("response lost after acceptance")
        return result

    def send_message(self, chat_id, html, **kwargs):
        result = super().send_message(chat_id, html, **kwargs)
        message_id = str(result.get("message_id") or "").strip()
        if message_id:
            self.recipient_messages[message_id] = {
                "format": str(result.get("format") or "html"),
                "content": str(html),
            }
        if self.raise_after_accept:
            self.raise_after_accept = False
            raise TelegramError("response lost after acceptance")
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


def test_sparse_rebind_cannot_downgrade_plan_binding():
    store = _store()
    _key, entry, _created = state.upsert_worker_entry(
        store,
        _source_worker(
            {
                "id": "worker-binding-preserve",
                "status": "idle",
                "space_id": "space-1",
                "fingerprint": "binding-preserve-fp",
            }
        ),
        topic_id="77",
    )
    state.bind_message_to_worker(
        store,
        "501",
        entry,
        topic_id="77",
        kind="final",
        turn_id="turn-binding-preserve",
        bot_kind="manager",
        content_revision="twrev1.binding_preserve",
        plan_token="twplan1.binding_preserve",
        part_ordinal=0,
        part_count=1,
        tendwire_job_key=(
            "turn-final:twplan1.binding_preserve:000000"
        ),
        delivery_format="rich",
    )

    state.bind_message_to_worker(
        store,
        "501",
        entry,
        topic_id="77",
        kind="final",
        turn_id="turn-binding-preserve",
        bot_kind="manager",
    )

    binding = state.find_message_binding(store, "501")
    assert binding is not None
    assert binding["content_revision"] == "twrev1.binding_preserve"
    assert binding["plan_token"] == "twplan1.binding_preserve"
    assert binding["part_ordinal"] == 0
    assert binding["part_count"] == 1
    assert binding["delivery_format"] == "rich"










class AmbiguousAcceptedOversizeNoticeTelegram(TelegramClient):
    def __init__(self):
        super().__init__(token="test")
        object.__setattr__(self, "recipient_messages", [])
        object.__setattr__(self, "attempts", 0)

    def api(self, method, payload):
        assert method == "sendMessage"
        self.attempts += 1
        self.recipient_messages.append(str(payload.get("text") or ""))
        raise TelegramError(
            "network response lost after Telegram accepted the message"
        )


class RejectedOversizeNoticeTelegram(TelegramClient):
    def __init__(self):
        super().__init__(token="test")
        object.__setattr__(self, "attempts", [])

    def api(self, method, payload):
        assert method == "sendMessage"
        self.attempts.append(str(payload.get("parse_mode") or "plain"))
        raise TelegramError("Bad Request: oversize notice rejected")


def _oversize_notice_case(telegram):
    store = _store()
    row = _turn_row(
        "turn-oversize-notice-failure",
        "twrev1.oversize_notice_failure",
        "x" * (RICH_SPLIT_CHUNK_CHARS * 8 + 1),
        user="legitimate large request",
    )
    row.pop("content")
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
    runtime = SyncRuntime(
        TurnFinalTendwire(
            _turn_row(
                "unused",
                "twrev1.unused",
                "unused",
            )
        ),
        telegram,
        with_outbox=False,
        max_sends=8,
    )
    return store, row, entry, runtime




















def test_short_inline_ready_delivery_fetches_exact_fields_then_two_syncs_noop(monkeypatch):
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

    assert first["content_pages"] == 2
    assert first["tendwire_turn_final"]["acked"] == 1
    assert len(tendwire.page_calls) == 2
    assert second["tendwire_turn_final"]["polled"] == 0
    assert third["tendwire_turn_final"]["polled"] == 0
    assert len(tendwire.prepare_calls) == prepare_count
    assert len(telegram.sent) == send_count
    entry = next(iter(state.source_worker_entries(store).values()))
    assert entry["last_clean_content_revision"] == "twrev1.short"
    assert entry["last_clean_plan_token"] == "twplan1.plan1"


def test_paged_20k_final_edits_working_then_sends_ordered_bound_parts(
    monkeypatch, tmp_path
):
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
    state_path = tmp_path / "state.json"
    monkeypatch.setenv("HERDR_TELEGRAM_TOPICS_STATE", str(state_path))
    state.save_state(store, state_path)
    token = state.read_ingress_policy(state_path).state_token
    for ordinal, message_id in enumerate(ids):
        binding = state.find_message_binding(store, message_id)
        assert binding is not None
        assert binding["turn_id"] == "turn-long"
        assert binding["content_revision"] == "twrev1.long"
        assert binding["plan_token"] == "twplan1.plan1"
        assert binding["part_ordinal"] == ordinal
        assert binding["part_count"] == len(ids)
        assert binding["tendwire_job_key"].endswith(f":{ordinal:06d}")

        route = state.resolve_ingress_reply(
            state_path,
            state.IngressReplyQuery(
                chat_id="-100",
                topic_id="77",
                reply_message_id=message_id,
                observed_author_bot_kind="",
                explicit_alias="",
                explicit_bot_kind="",
                state_token=token,
            ),
        )
        assert route.status is state.RouteStatus.RESOLVED
        assert route.worker_id == "worker-1"
        assert route.owner is not None
        assert route.owner.stable_key == row["stable_key"]
        assert route.reply_binding_id == message_id
        assert route.binding_was_present is True


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
    monkeypatch.setenv("HERDRES_TENDWIRE_FORCE_FULL_RECONCILE", "1")
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
    assert result["ok"] is False
    assert result["status"] == "outbound_delivery_stalled"
    assert result["tendwire_turn_final"]["status"] == "invalid_content_page"
    assert paged.prepare_calls == []
    assert telegram.sent == [] and telegram.edited == []






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

    assert result["ok"] is False
    assert result["status"] == "outbound_delivery_stalled"
    assert result["tendwire_turn_final"]["status"] == "invalid_content_page"
    assert telegram.sent == [] and telegram.edited == []
    assert tendwire.prepare_calls == []




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

    assert raced["tendwire_turn_final"]["deferred"] == 1
    assert raced["tendwire_turn_final"]["acked"] == 0
    assert telegram.revision_send_topics == []
    with state.state_lock(path=state_path):
        current = state.load_state(state_path)
        rerouted = sync_once(
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
    assert rerouted["tendwire_turn_final"]["deferred"] == 1
    assert rerouted["tendwire_turn_final"]["acked"] == 0
    assert len(tendwire.ack_calls) == ack_count
    assert entry is not None
    assert entry["topic_id"] == rebound_topic_id
    assert revision_bindings == []

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








def test_final_plan_keeps_repeated_user_text_for_exact_coverage(
    tmp_path, monkeypatch
):
    monkeypatch.setenv("HERDRES_TENDWIRE_MODE", "source")
    state_path = tmp_path / "state.json"
    monkeypatch.setenv("HERDR_TELEGRAM_TOPICS_STATE", str(state_path))
    item = _turn_row(
        "turn-repeated-user-final",
        "twrev1.repeated_user_final",
        "deployment succeeded",
        user="continue",
    )
    tendwire = TurnFinalTendwire(item)
    initial = _store()
    _key, initial_entry, _created = state.upsert_worker_entry(
        initial,
        _source_worker(tendwire.snapshot()["workers"][0]),
    )
    initial_entry["topic_id"] = "77"
    initial_entry["last_turn_id"] = "turn-previous"
    initial_entry["last_clean_user_hash"] = source_sync._turn_user_hash(
        item
    )
    state.save_state(initial, state_path)

    with state.state_lock(state_path):
        current = state.load_state(state_path)
        _key, entry = state.find_worker_entry_by_stable_key(
            current, _stable_key("worker-1")
        )
        assert entry is not None
        runtime = source_sync._offlock_runtime(
            current,
            _runtime(tendwire, DeletingTelegram(), max_sends=8),
        )
        staged, _pages, entry = source_sync._stage_final_plan(
            current, item, entry, runtime
        )

    assert staged is True
    token = str(entry["pending_plan_token"])
    spans = [
        span
        for ordinal in sorted(tendwire._plans[token]["parts"])
        for span in tendwire._plans[token]["parts"][ordinal]
    ]
    assert spans == [
        {"field": "user_text", "start_char": 0, "end_char": 8},
        {
            "field": "assistant_final_text",
            "start_char": 0,
            "end_char": len("deployment succeeded"),
        },
    ]


def test_turn_final_reason_preserves_plan_incomplete():
    assert source_sync._turn_final_reason_code("plan_incomplete") == (
        "plan_incomplete"
    )








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


def test_physical_budget_is_exact_and_transport_loss_defers(monkeypatch):
    monkeypatch.setenv("HERDRES_TENDWIRE_MODE", "source")
    monkeypatch.setenv("HERDRES_PINNED_STATUS", "0")
    long_text = "bounded part\n\n" * 500
    tendwire = TurnFinalTendwire(_turn_row("turn-budget", "twrev1.budget", long_text))
    telegram = DeletingTelegram()
    result = sync_once(_store(), _runtime(tendwire, telegram, max_sends=1))
    assert result["tendwire_turn_final"]["operations"] == 1
    assert result["tendwire_turn_final"]["polled"] == 2

    uncertain_wire = TurnFinalTendwire(_turn_row("turn-uncertain", "twrev1.uncertain", "one message"))
    uncertain_telegram = DeletingTelegram()
    uncertain_telegram.raise_after_accept = True
    uncertain = sync_once(_store(), _runtime(uncertain_wire, uncertain_telegram, max_sends=1))
    assert uncertain["tendwire_turn_final"]["deferred"] == 1
    assert uncertain["tendwire_turn_final"]["uncertain"] == 0
    assert len(uncertain_telegram.sent) == 1
    assert uncertain_wire.defer_calls[-1][1] == "transient_delivery"


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
    monkeypatch.setenv("HERDRES_FORCE_PLAIN_DELIVERY", "1")
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
    monkeypatch.setenv("HERDRES_FORCE_PLAIN_DELIVERY", "1")
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
    monkeypatch.setenv("HERDRES_FORCE_PLAIN_DELIVERY", "1")
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

    assert result["tendwire_turn_final"]["operations"] == 8
    assert result["tendwire_turn_final"]["acked"] == 8
    assert len(telegram.sent) == 8
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








def test_inbound_queue_observer_health_and_status_compose_with_outbound_health(
    tmp_path,
):
    db_path = tmp_path / "inbound.sqlite3"

    def request_id(index: int) -> str:
        digest = hashlib.sha256(str(index).encode()).digest()
        return "hri1_" + base64.urlsafe_b64encode(digest).rstrip(b"=").decode()

    def accept(queue: IngressQueue, index: int) -> None:
        queue.accept_update(
            {
                "receiver_id": "manager",
                "update_id": index,
                "request_id": request_id(index),
                "ordering_key": "topic:77",
                "kind": "decision" if index % 2 else "message",
                "input": {"chat_id": "-100", "message_id": str(index)},
                "first_seen_at": 10.0,
                "deadline_at": 100.0,
                "retain_until": 200.0,
                "depth_limit": 1,
            }
        )

    with IngressQueue.open_writer(db_path) as queue:
        healthy = doctor.inbound_queue(db_path, now=20.0)
        assert healthy["ok"] is True
        assert healthy["status"] == "healthy"
        assert healthy["health"] == {
            "pending": 0,
            "processing": 0,
            "retry": 0,
            "terminal": 0,
            "quarantine": 0,
            "pending_notices": 0,
            "claimed_notices": 0,
            "expired_leases": 0,
            "overdue_open": 0,
        }
        assert healthy["statuses"] == []

        accept(queue, 1)
        accept(queue, 2)
        attention = doctor.inbound_queue(db_path, now=20.0)

    assert attention["ok"] is False
    assert attention["status"] == "attention_required"
    assert attention["signal"] == "inbound_queue_attention_required"
    assert attention["attention_required"] == 1
    assert attention["health"]["pending"] == 1
    assert attention["health"]["quarantine"] == 1
    assert attention["health"]["pending_notices"] == 1
    assert {
        (row["state"], row["kind"], row["count"])
        for row in attention["statuses"]
    } == {("pending", "decision", 1), ("quarantine", "message", 1)}




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


def test_prepare_commit_exception_reuses_checkpointed_presentation_version(
    monkeypatch,
):
    monkeypatch.setenv("HERDRES_TENDWIRE_MODE", "source")
    monkeypatch.setenv("HERDRES_PINNED_STATUS", "0")

    class CommitFailsOnce(TurnFinalTendwire):
        def __init__(self, row):
            super().__init__(
                row,
                emit_ready=True,
                turn_schema_version=2,
            )
            self.commit_failure_armed = True

        def connector_prepare_commit(self, **kwargs):
            if self.commit_failure_armed:
                self.commit_failure_armed = False
                raise source_sync.TendwireError(
                    "transient commit transport failure"
                )
            return super().connector_prepare_commit(**kwargs)

    tendwire = CommitFailsOnce(
        _turn_row(
            "turn-commit-retry",
            "twrev1.commit_retry",
            "deliver exactly once after interrupted commit",
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
    entry = next(iter(state.source_worker_entries(store).values()))
    pending_token = entry["pending_plan_token"]
    assert first["tendwire_turn_final"]["deferred"] == 1
    assert entry["pending_presentation_version"] == PRESENTATION_VERSION
    assert tendwire._plans[pending_token]["parts"]
    assert telegram.sent == []
    assert telegram.edited == []

    second = sync_once(
        store, _runtime(tendwire, telegram, max_sends=1)
    )
    assert second["tendwire_turn_final"]["delivered"] == 1
    assert second["tendwire_turn_final"]["acked"] == 1
    assert second["tendwire_turn_final"]["operations"] == 1
    assert len(tendwire._plans) == 1
    assert len(telegram.sent) + len(telegram.edited) == 1


def test_prepare_owner_rebind_defers_source_root_then_delivers_to_new_topic(
    tmp_path,
    monkeypatch,
):
    monkeypatch.setenv("HERDRES_TENDWIRE_MODE", "source")
    monkeypatch.setenv("HERDRES_SOURCE_TOPIC_MODE", "worker")
    monkeypatch.setenv("HERDRES_PINNED_STATUS", "0")
    monkeypatch.setenv(
        "HERDR_TELEGRAM_TOPICS_RICH_MESSAGES", "0"
    )
    state_path = tmp_path / "state.json"
    monkeypatch.setenv(
        "HERDR_TELEGRAM_TOPICS_STATE", str(state_path)
    )
    replacement_topic_id = "16000"
    store = _store()
    state.save_state(store, state_path)

    class RebindAfterPrepareTendwire(TurnFinalTendwire):
        def __init__(self, row):
            super().__init__(
                row,
                emit_ready=True,
                turn_schema_version=2,
            )
            self.rebind_armed = True

        def connector_prepare_begin(self, **kwargs):
            accepted = super().connector_prepare_begin(**kwargs)
            if self.rebind_armed:
                self.rebind_armed = False
                concurrent = state.load_state(state_path)
                _entry_key, entry = (
                    state.find_worker_entry_by_stable_key(
                        concurrent, _stable_key("worker-1")
                    )
                )
                assert entry is not None
                entry["topic_id"] = replacement_topic_id
                state.save_state(concurrent, state_path)
            return accepted

    tendwire = RebindAfterPrepareTendwire(
        _turn_row(
            "turn-prepare-owner-rebind",
            "twrev1.prepare_owner_rebind",
            "deliver once on the replacement topic",
        )
    )
    tendwire.turns = lambda: {
        "ok": True,
        "schema_version": 2,
        "turns": [],
    }
    telegram = DeletingTelegram()

    with state.state_lock(path=state_path):
        current = state.load_state(state_path)
        first = sync_once(
            current,
            _runtime(tendwire, telegram, max_sends=10),
        )

    assert first["tendwire_turn_final"]["deferred"] == 1
    assert first["tendwire_turn_final"]["failed"] == 0
    assert tendwire.defer_calls[-1][1] == "transient_delivery"
    assert tendwire.fail_calls == []
    assert telegram.sent == []
    assert telegram.edited == []

    with state.state_lock(path=state_path):
        current = state.load_state(state_path)
        second = sync_once(
            current,
            _runtime(tendwire, telegram, max_sends=10),
        )

    assert second["tendwire_turn_final"]["delivered"] == 1
    assert second["tendwire_turn_final"]["acked"] == 1
    assert second["tendwire_turn_final"]["failed"] == 0
    assert len(telegram.sent) + len(telegram.edited) == 1
    assert telegram.sent[0][2]["thread_id"] == replacement_topic_id
    assert [call[0] for call in tendwire.prepare_calls].count("begin") == 2





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
        emit_ready=False,
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


def test_exact_binding_survives_ack_loss_and_repoll_does_not_resend(
    monkeypatch,
):
    monkeypatch.setenv("HERDRES_TENDWIRE_MODE", "source")
    monkeypatch.setenv("HERDRES_PINNED_STATUS", "0")
    row = _turn_row(
        "turn-binding-ack-loss",
        "twrev1.binding_ack_loss",
        "accepted once",
    )
    tendwire = TurnFinalTendwire(row)
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
            checkpoint=lambda: checkpoints.append(deepcopy(store)),
        ),
    )

    bindings = [
        (message_id, binding)
        for message_id, binding in state.message_bindings(store).items()
        if binding.get("turn_id") == row["id"]
        and binding.get("kind") == "final"
    ]
    assert first["tendwire_turn_final"]["operations"] == 1
    assert first["tendwire_turn_final"]["acked"] == 0
    assert len(bindings) == 1
    message_id, binding = bindings[0]
    assert binding["content_revision"] == "twrev1.binding_ack_loss"
    assert binding["plan_token"] == "twplan1.plan1"
    assert binding["part_ordinal"] == 0
    assert binding["part_count"] == 1
    assert binding["tendwire_job_key"] == (
        "turn-final:twplan1.plan1:000000"
    )
    assert any(
        state.find_message_binding(snapshot, message_id) is not None
        for snapshot in checkpoints
    )
    provider_writes = (
        len(telegram.sent),
        len(telegram.edited),
        len(telegram.deleted_messages),
    )
    page_calls = len(tendwire.page_calls)

    second = sync_once(
        store,
        _runtime(tendwire, telegram, max_sends=1),
    )

    assert second["tendwire_turn_final"]["operations"] == 0
    assert second["tendwire_turn_final"]["acked"] == 1
    assert len(tendwire.page_calls) == page_calls
    assert (
        len(telegram.sent),
        len(telegram.edited),
        len(telegram.deleted_messages),
    ) == provider_writes
    entry = next(iter(state.source_worker_entries(store).values()))
    assert entry["last_clean_message_ids"] == [message_id]
    assert entry["last_clean_content_revision"] == (
        "twrev1.binding_ack_loss"
    )


def test_crash_after_binding_checkpoint_repolls_without_provider_mutation(
    monkeypatch,
):
    monkeypatch.setenv("HERDRES_TENDWIRE_MODE", "source")
    monkeypatch.setenv("HERDRES_PINNED_STATUS", "0")
    row = _turn_row(
        "turn-binding-crash",
        "twrev1.binding_crash",
        "checkpoint before acknowledgement",
    )
    tendwire = TurnFinalTendwire(row)
    telegram = DeletingTelegram()
    store = _store()
    checkpoints = []

    def crash_after_provider_accept():
        raise RuntimeError("crash after exact binding checkpoint")

    with pytest.raises(
        RuntimeError, match="crash after exact binding checkpoint"
    ):
        sync_once(
            store,
            _runtime(
                tendwire,
                telegram,
                max_sends=1,
                checkpoint=lambda: checkpoints.append(deepcopy(store)),
                after_provider_accept=crash_after_provider_accept,
            ),
        )

    persisted = checkpoints[-1]
    bindings = [
        (message_id, binding)
        for message_id, binding in state.message_bindings(persisted).items()
        if binding.get("turn_id") == row["id"]
        and binding.get("kind") == "final"
    ]
    assert len(bindings) == 1
    message_id, binding = bindings[0]
    assert binding["tendwire_job_key"] == (
        "turn-final:twplan1.plan1:000000"
    )
    leased_ref = next(
        job["ref"]
        for job in tendwire._jobs
        if job["status"] == "leased"
    )
    tendwire._requeue_ref(leased_ref)
    provider_writes = (
        len(telegram.sent),
        len(telegram.edited),
        len(telegram.deleted_messages),
    )
    page_calls = len(tendwire.page_calls)

    resumed = sync_once(
        persisted,
        _runtime(tendwire, telegram, max_sends=1),
    )

    assert resumed["tendwire_turn_final"]["operations"] == 0
    assert resumed["tendwire_turn_final"]["acked"] == 1
    assert len(tendwire.page_calls) == page_calls
    assert (
        len(telegram.sent),
        len(telegram.edited),
        len(telegram.deleted_messages),
    ) == provider_writes
    entry = next(iter(state.source_worker_entries(persisted).values()))
    assert entry["last_clean_message_ids"] == [message_id]


def test_committed_ack_response_loss_keeps_checkpointed_local_completion(
    monkeypatch,
):
    monkeypatch.setenv("HERDRES_TENDWIRE_MODE", "source")
    monkeypatch.setenv("HERDRES_PINNED_STATUS", "0")
    row = _turn_row(
        "turn-committed-ack-loss",
        "twrev1.committed_ack_loss",
        "committed before response loss",
    )
    tendwire = TurnFinalTendwire(row)
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

    assert first["tendwire_turn_final"]["operations"] == 1
    assert first["tendwire_turn_final"]["acked"] == 0
    assert first["tendwire_turn_final"]["status"] == "timeout"
    persisted = checkpoints[-1]
    entry = next(iter(state.source_worker_entries(persisted).values()))
    message_ids = list(entry["last_clean_message_ids"])
    assert len(message_ids) == 1
    assert entry["last_clean_content_revision"] == (
        "twrev1.committed_ack_loss"
    )
    assert "pending_plan_token" not in entry
    assert (
        "final:turn-committed-ack-loss:twrev1.committed_ack_loss"
        in state.delivered_turns(persisted)
    )
    provider_writes = (
        len(telegram.sent),
        len(telegram.edited),
        len(telegram.deleted_messages),
    )
    page_calls = len(tendwire.page_calls)

    resumed = sync_once(
        persisted,
        _runtime(tendwire, telegram, max_sends=1),
    )

    assert resumed["tendwire_turn_final"]["polled"] == 0
    assert resumed["tendwire_turn_final"]["operations"] == 0
    assert len(tendwire.page_calls) == page_calls
    assert (
        len(telegram.sent),
        len(telegram.edited),
        len(telegram.deleted_messages),
    ) == provider_writes
    entry = next(iter(state.source_worker_entries(persisted).values()))
    assert entry["last_clean_message_ids"] == message_ids
    assert "pending_plan_token" not in entry


def test_multipart_prefix_ack_loss_restart_replays_binding_without_resend(
    monkeypatch,
):
    monkeypatch.setenv("HERDRES_TENDWIRE_MODE", "source")
    monkeypatch.setenv("HERDRES_PINNED_STATUS", "0")
    text = ("ordered multipart answer\n\n" * 900) + "FINAL_TAIL"
    row = _turn_row(
        "turn-multipart-prefix",
        "twrev1.multipart_prefix",
        text,
    )
    tendwire = TurnFinalTendwire(row)
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
            checkpoint=lambda: checkpoints.append(deepcopy(store)),
        ),
    )

    plan = tendwire._plans["twplan1.plan1"]
    part_count = int(plan["part_count"])
    assert part_count > 1
    assert first["tendwire_turn_final"]["operations"] == 1
    assert first["tendwire_turn_final"]["acked"] == 0
    persisted = checkpoints[-1]
    prefix = [
        (message_id, binding)
        for message_id, binding in state.message_bindings(
            persisted
        ).items()
        if binding.get("turn_id") == row["id"]
        and binding.get("content_revision")
        == "twrev1.multipart_prefix"
    ]
    assert len(prefix) == 1
    prefix_message_id = prefix[0][0]
    writes_after_prefix = len(telegram.sent)
    resumed = sync_once(
        persisted,
        _runtime(tendwire, telegram, max_sends=100),
    )

    assert resumed["tendwire_turn_final"]["acked"] == part_count
    assert resumed["tendwire_turn_final"]["operations"] == part_count - 1
    assert len(telegram.sent) == writes_after_prefix + part_count - 1
    entry = next(iter(state.source_worker_entries(persisted).values()))
    message_ids = entry["last_clean_message_ids"]
    assert len(message_ids) == part_count
    assert message_ids[0] == prefix_message_id
    assert len(set(message_ids)) == part_count
    assert "pending_plan_token" not in entry
    assert (
        "final:turn-multipart-prefix:twrev1.multipart_prefix"
        in state.delivered_turns(persisted)
    )
