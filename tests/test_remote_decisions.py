"""Tests for the remote-answer feature (herdres_connector/decisions.py + the gateway wiring).

The connector consumes a decision from Tendwire's pending payload (meta.decision), renders a reply
keyboard, and answers by submitting a NEUTRAL ``answer_decision`` command (``decision_ref`` +
semantic ``selection``) back to Tendwire. No key sequence is ever encoded on this side — Tendwire owns
the pane, its calibration, and the "is the prompt still displayed" freshness check. These tests cover
the pure state machine (directives, not keystrokes) plus the gateway integration (durable/idempotent
submit, false-success protection, freshness gating, and /send fall-through).
"""
from __future__ import annotations

import json

import herdres
from herdres_connector import config, decisions, ingress_requests, state, tendwire_client
from herdres_connector.ingress_identity import derive_telegram_request_id


REQUEST_ID_KEY = bytes(range(32))


def _req_id(update_id=100, message_id=9001):
    return derive_telegram_request_id(
        REQUEST_ID_KEY, receiver_id="manager", update_id=update_id, chat_id=-100, message_id=message_id
    )


REQUEST_ID = _req_id()


# --- fixtures -----------------------------------------------------------------
def _store(topic_id="500", worker_id="claude-1"):
    return {
        "panes": {
            f"worker:{worker_id}:x": {
                "source": "tendwire", "entry_type": "worker", "status": "working",
                "topic_id": topic_id, "tendwire_worker_id": worker_id, "worker_id": worker_id,
                "agent": "claude",
                # a valid v1 stable identity so the entry is uniquely routable on the rc train
                "tendwire_stable_key": "wsk1_" + "a" * 64,
                "tendwire_stable_key_version": 1,
            }
        },
        "spaces": {},
        "telegram": {"chat_id": "-100", "general_thread_id": "1"},
    }


def _pending(worker_id="claude-1", *, kind="single", multi=False, prompt="Proceed?", options=None, ref="dref-1"):
    opts = options if options is not None else [{"ref": "1", "label": "Yes"}, {"ref": "2", "label": "No"}]
    return {"pending_interactions": [{
        "worker_id": worker_id, "question": prompt, "kind": kind,
        "meta": {"decision": {"decision_ref": ref, "kind": kind, "prompt": prompt,
                              "multi_select": multi, "options": opts}},
    }]}


def _resolved(store=None, **kwargs):
    store = store if store is not None else _store()
    return decisions.resolve_decisions(store, _pending(**kwargs))[0]


def _activate(store, decision):
    decisions.set_active(store, decision.topic_id, decisions.active_record_from(decision, "42"))


# --- resolve ------------------------------------------------------------------
def test_resolve_single_joins_worker_to_topic():
    resolved = decisions.resolve_decisions(_store(), _pending())
    assert len(resolved) == 1
    d = resolved[0]
    assert d.topic_id == "500" and d.worker_id == "claude-1" and d.kind == "single"
    assert d.decision_id == "dref-1"
    assert [o["label"] for o in d.options] == ["Yes", "No", decisions.FREEFORM_LABEL]
    # options carry only a neutral ref + label — no send_text / key material
    assert all(set(o) == {"id", "label"} for o in d.options)


def test_resolve_multi_normalizes_question():
    d = _resolved(multi=True, options=[{"ref": "1", "label": "Red"}, {"ref": "2", "label": "Blue"}])
    assert d.kind == "multi"
    q = d.questions[0]
    assert [o["label"] for o in q["options"]] == ["Red", "Blue"]
    assert [o["option_id"] for o in q["options"]] == ["1", "2"]


def test_resolve_plan_has_no_freeform_option():
    d = _resolved(kind="plan", options=[{"ref": "1", "label": "Approve"}, {"ref": "2", "label": "Revise"}])
    assert d.kind == "plan"
    assert [o["id"] for o in d.options] == ["1", "2"]  # no custom write-in on a plan gate


def test_resolve_skips_unknown_worker():
    assert decisions.resolve_decisions(_store(worker_id="other"), _pending(worker_id="claude-1")) == []


def test_resolve_skips_interaction_without_decision():
    payload = {"pending_interactions": [{"worker_id": "claude-1", "meta": {}}]}
    assert decisions.resolve_decisions(_store(), payload) == []


def test_multi_question_prompt_fails_closed():
    # A multi-question AskUserQuestion (more than one question group) is not provably calibratable as a
    # single keyboard -> it must degrade to the plain attention notice, not render a wizard we can't
    # drive (Jerry: multi-question must fail closed).
    payload = {"pending_interactions": [{
        "worker_id": "claude-1",
        "meta": {"decision": {
            "decision_ref": "dref-1", "kind": "single", "prompt": "Two things",
            "options": [{"ref": "1", "label": "A"}],
            "question_count": 2,
        }},
    }]}
    assert decisions.resolve_decisions(_store(), payload) == []


# --- render -------------------------------------------------------------------
def test_reply_keyboard_single_options_and_selective_false():
    d = _resolved()
    kb = decisions.reply_keyboard(d)
    texts = [b["text"] for row in kb["keyboard"] for b in row]
    assert texts == ["Yes", "No", decisions.FREEFORM_LABEL]
    # selective MUST be False — a selective keyboard is hidden on standalone forum-topic posts.
    assert kb["one_time_keyboard"] is True and kb["selective"] is False


def test_reply_keyboard_multi_has_submit_row_and_selective_false():
    d = _resolved(multi=True, options=[{"ref": "1", "label": "Red"}, {"ref": "2", "label": "Blue"}])
    kb = decisions.reply_keyboard(d)
    texts = [b["text"] for row in kb["keyboard"] for b in row]
    assert texts == ["Red", "Blue", decisions.SUBMIT_LABEL]
    assert kb["one_time_keyboard"] is False and kb["selective"] is False


def test_remove_keyboard_is_selective_false():
    assert decisions.remove_keyboard() == {"remove_keyboard": True, "selective": False}


def test_duplicate_labels_are_disambiguated_and_resolve_to_distinct_refs():
    # Two options share the label "Yes": each button must be unique so a reply-keyboard tap (which only
    # reports the label text) maps back to the right option ref instead of colliding (Jerry #7).
    d = _resolved(options=[{"ref": "1", "label": "Yes"}, {"ref": "2", "label": "Yes"}])
    kb_texts = [b["text"] for row in decisions.reply_keyboard(d)["keyboard"] for b in row]
    assert kb_texts == ["Yes (1)", "Yes (2)", decisions.FREEFORM_LABEL]
    assert decisions._resolve_button(d, "Yes (1)") == "1"
    assert decisions._resolve_button(d, "Yes (2)") == "2"


# --- answering: directives (no keystrokes, no submit here) --------------------
def test_answer_single_returns_options_submit_directive():
    store = _store()
    d = _resolved(store)
    _activate(store, d)
    directive = decisions.handle_decision_answer(store, "500", "No")
    assert directive["action"] == "submit"
    assert directive["worker_id"] == "claude-1" and directive["decision_ref"] == "dref-1"
    assert directive["selection"] == {"option_refs": ["2"]}
    assert "No" in directive["success_reply"]


def test_answer_custom_button_arms_then_submits_text_writein():
    store = _store()
    d = _resolved(store)
    _activate(store, d)
    armed = decisions.handle_decision_answer(store, "500", decisions.FREEFORM_LABEL)
    assert armed["action"] == "local"  # nothing submitted yet
    assert decisions.get_active(store, "500")["await_freeform"] is True
    submit = decisions.handle_decision_answer(store, "500", "my custom answer")
    assert submit["action"] == "submit"
    assert submit["selection"] == {"text": "my custom answer"}


def test_answer_single_free_text_is_a_writein():
    store = _store()
    d = _resolved(store)
    _activate(store, d)
    submit = decisions.handle_decision_answer(store, "500", "something else entirely")
    assert submit["action"] == "submit"
    assert submit["selection"] == {"text": "something else entirely"}


def test_answer_plan_free_text_nudges_instead_of_writein():
    store = _store()
    d = _resolved(store, kind="plan", options=[{"ref": "1", "label": "Approve"}, {"ref": "2", "label": "Revise"}])
    _activate(store, d)
    directive = decisions.handle_decision_answer(store, "500", "maybe later")
    assert directive["action"] == "local" and "Approve" in directive["reply"]


def test_answer_multi_toggle_accumulates_then_submits_refs():
    store = _store()
    d = _resolved(store, multi=True, options=[
        {"ref": "1", "label": "Red"}, {"ref": "2", "label": "Green"}, {"ref": "3", "label": "Blue"}])
    _activate(store, d)
    assert decisions.handle_decision_answer(store, "500", "Red")["action"] == "local"
    assert decisions.handle_decision_answer(store, "500", "Blue")["action"] == "local"
    assert decisions.get_active(store, "500")["selected"] == ["1", "3"]
    submit = decisions.handle_decision_answer(store, "500", decisions.SUBMIT_LABEL)
    assert submit["action"] == "submit"
    assert submit["selection"] == {"option_refs": ["1", "3"]}


def test_answer_multi_submit_with_no_selection_is_a_nudge():
    store = _store()
    d = _resolved(store, multi=True)
    _activate(store, d)
    directive = decisions.handle_decision_answer(store, "500", decisions.SUBMIT_LABEL)
    assert directive["action"] == "local" and "Submit" in directive["reply"]


def test_answer_returns_none_when_no_active_decision():
    assert decisions.handle_decision_answer(_store(), "500", "hi") is None


# --- freshness helper ---------------------------------------------------------
def test_decision_present_detects_answered():
    assert decisions.decision_present(_pending(), "claude-1", "dref-1") is True
    assert decisions.decision_present({"pending_interactions": []}, "claude-1", "dref-1") is False
    assert decisions.decision_present(_pending(), "claude-1", "other-ref") is False


# --- flag ---------------------------------------------------------------------
def test_empty_flag_keeps_default_on(monkeypatch):
    # HERDRES_REMOTE_DECISIONS= (empty, common in env files) must NOT silently disable (Jerry #10).
    monkeypatch.setenv("HERDRES_REMOTE_DECISIONS", "")
    assert config.remote_decisions_enabled() is True
    monkeypatch.setenv("HERDRES_REMOTE_DECISIONS", "0")
    assert config.remote_decisions_enabled() is False


# --- tendwire_client answer_decision command validation -----------------------
_UNSET = object()


def _answer_request(selection=_UNSET, **overrides):
    if selection is _UNSET:
        selection = {"option_refs": ["2"]}
    request = {
        "schema_version": 1, "action": "answer_decision", "request_id": REQUEST_ID, "dry_run": False,
        "target": {"worker_id": "claude-1"},
        "params": {"decision_ref": "dref-1", "selection": selection},
    }
    request.update(overrides)
    return request


def test_answer_decision_request_shape_is_accepted():
    # The connector carries the decision in params, exactly as Tendwire's answer_decision expects.
    assert tendwire_client._exact_public_command_request(_answer_request()) is not None
    assert tendwire_client._exact_public_command_request(
        _answer_request(selection={"text": "write-in"})
    ) is not None
    # both the tendwire_client gate and the durable ingress gate agree
    assert ingress_requests._valid_command_request(_answer_request()) is True


def test_answer_decision_request_rejects_bad_shapes():
    bad_selections = [
        {"option_refs": []},                     # empty refs
        {"option_refs": [""]},                   # blank ref
        {},                                      # empty selection
        {"text": ""},                            # empty text
        {"keys": ["enter"]},                     # a key sequence is NOT a valid neutral selection
        {"option_refs": ["1"], "text": "both"},  # exactly one form only
        {"refs": ["1"]},                         # wrong key name
    ]
    for selection in bad_selections:
        assert tendwire_client._exact_public_command_request(_answer_request(selection=selection)) is None
        assert ingress_requests._valid_command_request(_answer_request(selection=selection)) is False
    # a stray extra field, a top-level `decision`, or a bad request_id is rejected too
    assert tendwire_client._exact_public_command_request({**_answer_request(), "extra": 1}) is None
    assert tendwire_client._exact_public_command_request(_answer_request(request_id="not-hmac")) is None
    top_level = {**_answer_request()}
    top_level["decision"] = top_level.pop("params")
    assert tendwire_client._exact_public_command_request(top_level) is None


def test_answer_decision_rejection_statuses_are_recognized():
    # Tendwire's typed fail-closed statuses must validate as real rejections (not uncertain-retry).
    request = _answer_request()
    for status, disposition in [
        ("decision_not_pending", "no_receipt"),
        ("invalid_selection", "no_receipt"),
        ("unknown_worker", "terminal_rejected"),
        ("unsupported_decision", "terminal_rejected"),
    ]:
        response = {
            "schema_version": 2, "action": "answer_decision", "request_id": REQUEST_ID, "ok": False,
            "dry_run": False, "status": status, "disposition": disposition, "result": None,
            "error": {"code": status, "message": "fail closed"}, "warnings": [],
        }
        assert tendwire_client._validated_command_response(response, request) is not None


# --- gateway integration (command_reply) -------------------------------------
def _accepted_response(req):
    # Return the REAL shape Tendwire emits per action — answer_decision's accepted result echoes the
    # decision_ref and uses observed_pending_state (not the send_instruction target-turn shape).
    worker_id = str(req.get("target", {}).get("worker_id") or "claude-1")
    if req["action"] == "answer_decision":
        result = {
            "target": {"worker_id": worker_id},
            "decision": {"decision_ref": req["params"]["decision_ref"]},
            "delivery_state": "submitted", "transport_state": "submitted",
            "observed_pending_state": "pending_observation",
        }
    else:
        result = {
            "target": {"worker_id": worker_id}, "delivery_state": "submitted",
            "transport_state": "submitted", "target_state_at_send": "idle",
            "observed_turn_state": "pending_observation",
        }
    return {
        "schema_version": 2, "action": req["action"], "request_id": req["request_id"], "ok": True,
        "dry_run": False, "status": "accepted", "disposition": "terminal_accepted",
        "result": result, "error": None, "warnings": [],
    }


def _rejected_response(req, *, status="rejected", disposition="terminal_rejected"):
    return {
        "schema_version": 2, "action": req["action"], "request_id": req["request_id"], "ok": False,
        "dry_run": False, "status": status, "disposition": disposition, "result": None,
        "error": {"code": status, "message": "not accepted"}, "warnings": [],
    }


class FakeClient:
    def __init__(self, *, pending_payload=None, response="accept",
                 reject_status="rejected", reject_disposition="terminal_rejected"):
        self._pending = pending_payload if pending_payload is not None else _pending()
        self._response = response
        self._reject_status = reject_status
        self._reject_disposition = reject_disposition
        self.commands = []

    def pending(self):
        return self._pending

    def command_json(self, request_json):
        req = json.loads(request_json)
        self.commands.append(req)
        if self._response == "accept":
            return _accepted_response(req)
        return _rejected_response(req, status=self._reject_status, disposition=self._reject_disposition)


def _prepare(tmp_path, monkeypatch, *, fake, kind="single", multi=False, options=None):
    monkeypatch.setenv("HERDRES_TENDWIRE_MODE", "source")
    monkeypatch.setenv("HERDRES_SOURCE_TOPIC_MODE", "worker")
    monkeypatch.setenv("HERDR_TELEGRAM_TOPICS_STATE", str(tmp_path / "state.json"))
    store = _store()
    d = decisions.resolve_decisions(store, _pending(kind=kind, multi=multi, options=options))[0]
    _activate(store, d)
    state.save_state(store)
    monkeypatch.setattr(herdres, "TendwireClient", lambda: fake)
    return d


def _payload(text, message_id="9001", update_id=100):
    return {
        "chat_id": "-100", "topic_id": "500", "message_id": message_id,
        "request_id": _req_id(update_id=update_id, message_id=int(message_id)), "text": text,
    }


def test_gateway_answer_submits_answer_decision_and_confirms(tmp_path, monkeypatch):
    fake = FakeClient(response="accept")
    _prepare(tmp_path, monkeypatch, fake=fake)
    reply = herdres.command_reply(_payload("No"))
    assert reply["handled"] is True and reply["disposition"] == "terminal_accepted"
    assert "No" in reply["reply"]
    # exactly one neutral answer_decision command, carrying the semantic selection (no keystrokes)
    submits = [c for c in fake.commands if c["action"] == "answer_decision"]
    assert len(submits) == 1
    assert submits[0]["params"] == {"decision_ref": "dref-1", "selection": {"option_refs": ["2"]}}
    assert submits[0]["target"] == {"worker_id": "claude-1"}
    assert submits[0]["request_id"] == _payload("No")["request_id"]


def test_gateway_answer_failure_keeps_decision_active_and_reports_failure(tmp_path, monkeypatch):
    # Jerry #2/#11: a failed submit must NOT report success or destroy the prompt state.
    fake = FakeClient(response="reject")
    _prepare(tmp_path, monkeypatch, fake=fake)
    reply = herdres.command_reply(_payload("No"))
    assert reply["handled"] is True and reply["disposition"] == "terminal_rejected"
    assert "✅" not in reply["reply"] and ("didn't accept" in reply["reply"] or "tap again" in reply["reply"])
    # the decision record survives on disk, so the keyboard stays and the user can retry
    persisted = state.load_state()
    assert decisions.get_active(persisted, "500") is not None


def test_gateway_answer_is_stale_when_decision_left_pending(tmp_path, monkeypatch):
    # Jerry #3: if the prompt already left the pending payload (answered at the workstation / superseded),
    # refuse to key-drive a stale pane — no submit, and drop the active record.
    fake = FakeClient(pending_payload={"pending_interactions": []}, response="accept")
    _prepare(tmp_path, monkeypatch, fake=fake)
    reply = herdres.command_reply(_payload("No"))
    assert reply["handled"] is True
    assert "already answered" in reply["reply"].lower()
    assert [c for c in fake.commands if c["action"] == "answer_decision"] == []
    # the record must SURVIVE so the sync loop can retract the keyboard (the strict reply can't carry
    # a keyboard removal); clearing it here would orphan the keyboard.
    assert decisions.get_active(state.load_state(), "500") is not None


def test_gateway_decision_rejection_no_receipt_terminalizes(tmp_path, monkeypatch):
    # Tendwire's typed decision failures come back as disposition=no_receipt (a fail-safe default),
    # which is normally retryable. Replaying the identical answer_decision bytes would re-reject
    # forever and head-of-line-block the bot for the whole retry horizon, so the connector must
    # terminalize (advance the checkpoint) and surface an explicit failure instead of looping.
    fake = FakeClient(response="reject", reject_status="decision_not_pending", reject_disposition="no_receipt")
    _prepare(tmp_path, monkeypatch, fake=fake)
    reply = herdres.command_reply(_payload("No"))
    assert reply["handled"] is True
    assert reply["checkpoint"] == "advance"           # NOT a retry loop
    assert "✅" not in reply["reply"] and reply["reply"]  # an explicit, non-success failure reply
    assert decisions.get_active(state.load_state(), "500") is not None  # keyboard stays for a retry


def test_validated_response_accepts_real_decision_accepted_shape():
    # Guards the contract directly (the FakeClient gateway path bypasses _validated_command_response):
    # the connector must accept Tendwire's answer_decision accepted result and reject the
    # send_instruction-shaped one for an answer_decision request.
    request = _answer_request()
    accepted = {
        "schema_version": 2, "action": "answer_decision", "request_id": REQUEST_ID, "ok": True,
        "dry_run": False, "status": "accepted", "disposition": "terminal_accepted",
        "result": {"target": {"worker_id": "claude-1"}, "decision": {"decision_ref": "dref-1"},
                   "delivery_state": "submitted", "transport_state": "submitted",
                   "observed_pending_state": "pending_observation"},
        "error": None, "warnings": [],
    }
    assert tendwire_client._validated_command_response(accepted, request) is not None
    wrong_shape = {**accepted, "result": {"target": {"worker_id": "claude-1"}, "delivery_state": "submitted",
                   "transport_state": "submitted", "target_state_at_send": "idle",
                   "observed_turn_state": "pending_observation"}}
    assert tendwire_client._validated_command_response(wrong_shape, request) is None


def test_duplicate_label_collision_with_literal_suffix_stays_globally_unique():
    # A distinct option literally labelled "Yes (2)" must not collide with the "(2)" the connector
    # generates for a duplicate "Yes" — every button text must resolve to exactly one ref.
    d = _resolved(options=[{"ref": "1", "label": "Yes"}, {"ref": "2", "label": "Yes"},
                           {"ref": "3", "label": "Yes (2)"}])
    buttons = decisions._button_pairs(d)
    texts = [t for t, _ in buttons]
    assert len(texts) == len({t.casefold() for t in texts})  # all globally unique
    for text, ref in buttons:
        assert decisions._resolve_button(d, text) == ref


def test_gateway_explicit_send_is_not_hijacked_by_active_decision(tmp_path, monkeypatch):
    # Jerry #8: /send has documented queue-for-turn-boundary semantics; it must fall through to normal
    # instruction handling even while a decision is active, NOT be captured as a wizard write-in.
    fake = FakeClient(response="accept")
    _prepare(tmp_path, monkeypatch, fake=fake)
    herdres.command_reply(_payload("/send do the thing"))
    actions = [c["action"] for c in fake.commands]
    assert "answer_decision" not in actions
    assert "send_instruction" in actions
    # the /send text was routed as an instruction, not swallowed by the decision wizard
    send = next(c for c in fake.commands if c["action"] == "send_instruction")
    assert send["instruction"]["text"] == "do the thing"
    # and the decision is still open (not falsely answered)
    assert decisions.get_active(state.load_state(), "500") is not None


def test_gateway_alias_targeted_send_is_not_hijacked_by_active_decision(tmp_path, monkeypatch):
    # An alias-targeted message `@worker …` (including `@worker /send …`) addresses a specific worker
    # and must fall through — it must NOT be captured as a write-in to this topic's active decision
    # (Jerry #8, alias-targeted form, which "/send"-prefix matching alone misses).
    fake = FakeClient(response="accept")
    _prepare(tmp_path, monkeypatch, fake=fake)
    herdres.command_reply(_payload("@claude-1 /send do the thing"))
    assert "answer_decision" not in [c["action"] for c in fake.commands]
    assert decisions.get_active(state.load_state(), "500") is not None
