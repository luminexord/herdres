"""Remote answering of Claude Code follow-up prompts (AskUserQuestion / ExitPlanMode).

A blocked Claude pane's prompt is surfaced by Tendwire in its pending payload as a structured
``meta.decision`` (question + ordered options). This connector renders it into the worker's Telegram
topic as a native reply keyboard; a tapped/typed answer is turned into a **semantic** selection and
submitted to Tendwire as a neutral ``answer_decision`` command. Tendwire owns all Herdr access — it
resolves the worker to its pane, confirms the prompt is still displayed, and owns the per-Claude-Code
TUI calibration. The connector reads NO local files, spawns NO Herdr binary, and encodes NO key
sequence, so it stays within source-mode neutrality — it only ever talks to Tendwire.

Flow:
  1. Tendwire carries the decision in ``pending.get`` as
     ``pending_interactions[].meta.decision = {decision_ref, kind, prompt, options:[{ref,label}], multi_select}``.
  2. ``resolve_decisions`` joins each interaction's neutral ``worker_id`` -> the connector's worker
     entry -> its Telegram topic.
  3. ``reply_keyboard`` / ``render_decision_html`` render the keyboard.
  4. ``handle_decision_answer`` maps a tap/typed answer to a semantic ``selection`` (chosen option
     ref(s) or a write-in text) and returns a directive; the gateway submits it as an
     ``answer_decision`` command (target: worker_id, ``decision: {decision_ref, selection}``).

This module holds no Tendwire client and performs no submit itself: the gateway owns the durable,
idempotent submission (so a decision answer reuses the same request-id / retry discipline as any
inbound instruction), and Tendwire owns the pane. That split is what keeps calibration and the
"is the prompt still on screen" freshness check on the side that can actually observe the pane.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from . import config, state
from .safe import sanitize_text, short_hash

# The freeform "write a different answer" row is a synthetic single-select option: a tap arms a
# type-your-answer capture instead of selecting one of Claude's options.
CUSTOM_OPTION_ID = "custom"
FREEFORM_LABEL = "✍️ Write a different answer"
SUBMIT_LABEL = "✅ Submit answer"


def decisions_enabled() -> bool:
    """Master switch for the remote-answer feature (default ON; degrades to the plain attention
    notice when off or when a decision cannot be resolved). The flag lives in ``config`` with the
    standard runtime-flag idiom (empty value keeps the default)."""
    return config.remote_decisions_enabled()


# ---------------------------------------------------------------------------
# Resolve decisions from the Tendwire pending payload
# ---------------------------------------------------------------------------

@dataclass
class ResolvedDecision:
    worker_id: str
    topic_id: str
    entry_key: str
    decision_id: str
    kind: str  # "single" | "multi" | "plan"
    prompt: str
    options: list[dict[str, str]] = field(default_factory=list)   # single/plan: {id,label}
    questions: list[dict[str, Any]] = field(default_factory=list)  # multi: normalized single question

    def content_hash(self) -> str:
        return short_hash(
            {"d": self.decision_id, "k": self.kind, "p": self.prompt, "o": self.options, "q": self.questions},
            20,
        )


def _pending_interactions(pending_payload: dict[str, Any]) -> list[dict[str, Any]]:
    items = pending_payload.get("pending_interactions", pending_payload.get("pending", []))
    return [i for i in items if isinstance(i, dict)]


def _decision_from_meta(decision: Any) -> tuple[str, str, str, list, list] | None:
    """(kind, decision_id, prompt, options, questions) from a Tendwire ``meta.decision`` blob, or None.

    single/plan -> flat options (one button each; single also gets a freeform write-in option);
    multi -> a single normalized question with togglable options. Option ``ref`` (the 1-based ordinal
    Tendwire preserved) becomes the option id, so a tap maps back to that ref.

    Fails closed (returns None -> plain attention notice) for anything not provably answerable with a
    single-question keyboard: no options, or a multi-question AskUserQuestion (the pane's tab-group
    wizard needs per-Claude-Code calibration that only Tendwire can own)."""
    if not isinstance(decision, dict):
        return None
    try:
        question_count = int(decision.get("question_count") or 1)
    except (TypeError, ValueError):
        question_count = 1
    if question_count > 1:
        # Multi-question AskUserQuestion (up to 4 tab-separated groups): not representable as one
        # keyboard and not provably calibratable — Tendwire refuses it too (unsupported_decision), so
        # degrade to the read-only attention notice rather than render a wizard we can't drive.
        return None
    raw = [o for o in decision.get("options", []) if isinstance(o, dict)]
    if not raw:
        return None
    prompt = str(decision.get("prompt") or "Input needed.")
    decision_id = str(decision.get("decision_ref") or "")
    kind = str(decision.get("kind") or "single").strip().lower()
    if kind not in {"single", "multi", "plan"}:
        kind = "single"
    if bool(decision.get("multi_select")) and kind != "plan":
        kind = "multi"
    if kind == "multi":
        questions = [{
            "question_id": "q1",
            "title": prompt,
            "options": [
                {"option_id": str(o.get("ref") or i + 1), "label": str(o.get("label") or "")}
                for i, o in enumerate(raw)
            ],
        }]
        return kind, decision_id, prompt, [], questions
    options = [
        {"id": str(o.get("ref") or i + 1), "label": str(o.get("label") or "")}
        for i, o in enumerate(raw)
    ]
    if kind != "plan":
        options.append({"id": CUSTOM_OPTION_ID, "label": FREEFORM_LABEL})
    return kind, decision_id, prompt, options, []


def resolve_decisions(store: dict[str, Any], pending_payload: dict[str, Any]) -> list[ResolvedDecision]:
    """Join every Tendwire-carried pending decision to its Telegram topic by neutral ``worker_id``.

    Everything comes from the pending payload the sync loop already polled — the connector reads NO
    Herdr snapshot and NO local file. Returns [] for any interaction with no decision blob, an unknown
    worker, or an unmapped topic (those fall back to the plain attention notice)."""
    entries = state.source_worker_entries(store)
    resolved: list[ResolvedDecision] = []
    for item in _pending_interactions(pending_payload):
        meta = item.get("meta") if isinstance(item.get("meta"), dict) else {}
        parsed = _decision_from_meta(meta.get("decision"))
        if not parsed:
            continue
        worker_id = str(item.get("worker_id") or "")
        if not worker_id:
            continue
        entry_key = state.find_entry_key_by_worker(store, worker_id) or ""
        if not entry_key:
            continue
        topic_id = str((entries.get(entry_key) or {}).get("topic_id") or "")
        if not topic_id:
            continue
        kind, decision_id, prompt, options, questions = parsed
        if not decision_id:
            continue
        resolved.append(
            ResolvedDecision(
                worker_id=worker_id, topic_id=topic_id, entry_key=entry_key, decision_id=decision_id,
                kind=kind, prompt=prompt, options=options, questions=questions,
            )
        )
    return resolved


def decision_present(pending_payload: dict[str, Any], worker_id: str, decision_ref: str) -> bool:
    """True when ``decision_ref`` is still pending for ``worker_id`` in a freshly polled payload.

    The answer path calls this immediately before submitting: if the prompt was already answered at
    the workstation, superseded, or replaced by a modal, the decision leaves the pending payload and
    we refuse to key-drive a stale pane. Tendwire also fails closed on its side (it confirms the
    prompt is still displayed before replaying) — this is the connector-local belt-and-suspenders."""
    worker_id = str(worker_id or "")
    decision_ref = str(decision_ref or "")
    if not worker_id or not decision_ref:
        return False
    for item in _pending_interactions(pending_payload):
        if str(item.get("worker_id") or "") != worker_id:
            continue
        meta = item.get("meta") if isinstance(item.get("meta"), dict) else {}
        dec = meta.get("decision")
        if isinstance(dec, dict) and str(dec.get("decision_ref") or "") == decision_ref:
            return True
    return False


# ---------------------------------------------------------------------------
# Rendering: reply keyboard + message body
# ---------------------------------------------------------------------------

_MARKERS = ("✅ ", "▫️ ", "☑️ ", "✔️ ")


def _strip_marker(text: str) -> str:
    out = str(text or "").strip()
    for marker in _MARKERS:
        if out.startswith(marker):
            return out[len(marker):].strip()
    return out


def _labeled_refs(decision: ResolvedDecision) -> list[tuple[str, str]]:
    """(label, ref) pairs the keyboard offers, in order. Single/plan use the flat options (ref is the
    option id, incl. the synthetic ``custom`` freeform row); multi uses the single question's options
    (ref is the option_id)."""
    if decision.kind == "multi":
        question = decision.questions[0] if decision.questions else {"options": []}
        return [
            (str(o.get("label") or ""), str(o.get("option_id") or ""))
            for o in question.get("options", [])
            if isinstance(o, dict)
        ]
    return [
        (str(o.get("label") or ""), str(o.get("id") or ""))
        for o in decision.options
        if isinstance(o, dict)
    ]


def _button_pairs(decision: ResolvedDecision) -> list[tuple[str, str]]:
    """(button_text, ref) with duplicate labels disambiguated so every button resolves to exactly one
    ref. Two options that share a label (e.g. "Yes"/"Yes") would otherwise collide on a reply-keyboard
    tap (which only reports the label text); we suffix each occurrence with its 1-based position so the
    button text is unique and the tap maps back to the right option ref."""
    pairs = _labeled_refs(decision)
    folded = [_strip_marker(label).casefold() for label, _ in pairs]
    used: set[str] = set()
    seen: dict[str, int] = {}
    buttons: list[tuple[str, str]] = []
    for (label, ref), key in zip(pairs, folded):
        base = _strip_marker(label)
        if not base:
            continue
        text = base
        # Suffix self-duplicates AND keep bumping the suffix until the final text is unique against
        # EVERY other button: a distinct option literally labelled "Yes (2)" must not collide with a
        # generated "Yes (2)". A reply-keyboard tap only reports the text, so global uniqueness is what
        # makes each button resolve to exactly one ref.
        if folded.count(key) > 1 or text.casefold() in used:
            count = seen.get(key, 0)
            while True:
                count += 1
                text = f"{base} ({count})"
                if text.casefold() not in used:
                    break
            seen[key] = count
        used.add(text.casefold())
        buttons.append((text, ref))
    return buttons


def _resolve_button(decision: ResolvedDecision, text: str) -> str | None:
    """The option ref a tapped/typed button text selects, or None when nothing matches (free text)."""
    want = _strip_marker(text).casefold()
    if not want:
        return None
    for button_text, ref in _button_pairs(decision):
        if button_text.casefold() == want:
            return ref
    return None


def _keyboard_rows(labels: list[str]) -> list[list[dict[str, str]]]:
    return [[{"text": sanitize_text(label, 120)}] for label in labels if label]


def reply_keyboard(decision: ResolvedDecision) -> dict[str, Any]:
    """Native ReplyKeyboardMarkup for a decision. Single/plan: one button per option (one-time). Multi:
    one button per option plus a Submit row (persistent until submit).

    ``selective`` MUST stay False: a selective reply keyboard is shown only to users @mentioned in the
    text or the user being replied to. Decision messages are standalone forum-topic posts, so
    selective=True renders the text but hides the keyboard from everyone. The keyboard is static: a
    multi-select tap accumulates its choice in state and confirms in the reply text (the strict command
    reply channel carries no reply_markup, so there is no per-toggle keyboard re-render)."""
    buttons = [button_text for button_text, _ in _button_pairs(decision)]
    if decision.kind == "multi":
        keyboard = _keyboard_rows(buttons)
        keyboard.append([{"text": SUBMIT_LABEL}])
        return {
            "keyboard": keyboard, "resize_keyboard": True, "one_time_keyboard": False,
            "selective": False, "input_field_placeholder": "Tap options, then Submit",
        }
    return {
        "keyboard": _keyboard_rows(buttons), "resize_keyboard": True,
        "one_time_keyboard": True, "selective": False, "input_field_placeholder": "Tap an answer",
    }


def remove_keyboard() -> dict[str, Any]:
    return {"remove_keyboard": True, "selective": False}


def render_decision_html(decision: ResolvedDecision, selected: list[str] | None = None) -> str:
    from .safe import html_escape

    def esc(text: str) -> str:
        return html_escape(str(text), 1200)

    head = {"plan": "📋 <b>Plan review</b>", "multi": "🔀 <b>Choose one or more</b>",
            "single": "❓ <b>Claude is asking</b>"}.get(decision.kind, "❓ <b>Claude is asking</b>")
    lines = [head, esc(decision.prompt)]
    if decision.kind == "multi":
        selected = selected or []
        for q in decision.questions:
            title = esc(str(q.get("title") or ""))
            picks = [
                str(o.get("label")) for o in q.get("options", [])
                if isinstance(o, dict) and str(o.get("option_id")) in selected
            ]
            suffix = f" — <i>{esc(', '.join(picks))}</i>" if picks else ""
            lines.append(f"• {title}{suffix}")
        lines.append("<i>Tap to toggle, then Submit.</i>")
    else:
        lines.append("<i>Tap a button below to answer here.</i>")
    return "\n".join(part for part in lines if part)


# ---------------------------------------------------------------------------
# Active-decision state (shared via the connector store / state.json)
# ---------------------------------------------------------------------------

def _active_map(store: dict[str, Any]) -> dict[str, Any]:
    bucket = store.setdefault("decisions", {})
    if not isinstance(bucket, dict):
        bucket = {}
        store["decisions"] = bucket
    active = bucket.setdefault("active", {})
    if not isinstance(active, dict):
        active = {}
        bucket["active"] = active
    return active


def active_items(store: dict[str, Any]) -> list[tuple[str, Any]]:
    """Public snapshot of (topic_id, record) active decisions — callers iterate this instead of
    reaching into the private ``_active_map`` bucket."""
    return list(_active_map(store).items())


def reload_active_map(store: dict[str, Any]) -> None:
    """Re-read the persisted ``decisions`` bucket into ``store``.

    The sync delivery loop yields the state lock around Telegram sends, during which the gateway (a
    separate process) may ``clear_active`` an answered decision. Our in-memory ``store`` predates that,
    so a later ``save_state`` would resurrect the answered decision and re-drive the pane on the next
    message. Reloading just this bucket before we read/mutate it closes that round-trip race without
    disturbing other unsaved in-memory state."""
    fresh = state.load_state()
    bucket = fresh.get("decisions")
    store["decisions"] = bucket if isinstance(bucket, dict) else {}


def get_active(store: dict[str, Any], topic_id: str) -> dict[str, Any] | None:
    entry = _active_map(store).get(str(topic_id))
    return entry if isinstance(entry, dict) else None


def set_active(store: dict[str, Any], topic_id: str, record: dict[str, Any]) -> None:
    _active_map(store)[str(topic_id)] = record


def clear_active(store: dict[str, Any], topic_id: str) -> dict[str, Any] | None:
    return _active_map(store).pop(str(topic_id), None)


def active_record_from(decision: ResolvedDecision, message_id: str) -> dict[str, Any]:
    return {
        "decision_id": decision.decision_id,
        "worker_id": decision.worker_id,
        "entry_key": decision.entry_key,
        "kind": decision.kind,
        "prompt": decision.prompt,
        "options": decision.options,
        "questions": decision.questions,
        "message_id": str(message_id),
        "selected": [],
        "await_freeform": False,
        "content_hash": decision.content_hash(),
    }


def decision_from_record(record: dict[str, Any], topic_id: str = "") -> ResolvedDecision:
    """Rebuild a ResolvedDecision from a stored active record (for rendering / button resolution)."""
    return ResolvedDecision(
        worker_id=str(record.get("worker_id") or ""),
        topic_id=str(topic_id or ""),
        entry_key=str(record.get("entry_key") or ""),
        decision_id=str(record.get("decision_id") or ""),
        kind=str(record.get("kind") or "single"),
        prompt=str(record.get("prompt") or ""),
        options=[o for o in record.get("options", []) if isinstance(o, dict)],
        questions=[q for q in record.get("questions", []) if isinstance(q, dict)],
    )


# ---------------------------------------------------------------------------
# Answering: map a tap/typed answer to a semantic selection directive
# ---------------------------------------------------------------------------
#
# handle_decision_answer is a pure state machine over the topic's active record. It performs NO submit
# and holds NO Tendwire client; it returns a directive the gateway acts on:
#   * None                                  -> no active decision; fall through to normal handling.
#   * {"action": "local", "reply": ...}     -> answer the user here (toggle feedback / freeform arm /
#                                              reprompt); no command is submitted.
#   * {"action": "submit", "worker_id", "decision_ref", "selection", "success_reply"}
#                                           -> the gateway submits an answer_decision command (durable,
#                                              idempotent, with the message's request-id) and derives
#                                              its reply from the command disposition.
#
# selection is the neutral shape Tendwire's answer_decision expects: {"option_refs": [...]} (one ref
# for single/plan, several distinct refs for multi) or {"text": ...} (a single-select write-in). No
# key sequence is ever encoded here — Tendwire maps refs -> keys against the live pane.


def _submit_directive(worker_id: str, decision_ref: str, selection: dict[str, Any], success_reply: str) -> dict[str, Any]:
    return {
        "action": "submit",
        "worker_id": worker_id,
        "decision_ref": decision_ref,
        "selection": selection,
        "success_reply": success_reply,
    }


def handle_decision_answer(store: dict[str, Any], topic_id: str, text: str) -> dict[str, Any] | None:
    """Advance the topic's active decision by ``text`` and return a directive (see above), or None when
    there is no active decision so the gateway falls through to normal instruction handling.

    A pending decision BLOCKS the pane, so while one is active every plain message in the topic answers
    it: a matching button selects that option; any other text is a write-in (single) or a nudge (multi
    / plan)."""
    record = get_active(store, topic_id)
    if not record:
        return None
    text = str(text or "").strip()
    worker_id = str(record.get("worker_id") or "")
    decision_id = str(record.get("decision_id") or "")
    kind = str(record.get("kind") or "single")
    decision = decision_from_record(record, topic_id)

    # Freeform capture armed on a single-select: the next message is the write-in answer.
    if record.get("await_freeform"):
        if not text:
            return {"action": "local", "reply": "✍️ Type your answer as a normal message and I'll send it to Claude."}
        return _submit_directive(
            worker_id, decision_id, {"text": text},
            f"✍️ Sent your answer to Claude: “{sanitize_text(text, 200)}”.",
        )

    if kind == "multi":
        return _handle_multi_answer(store, topic_id, record, decision, text)

    ref = _resolve_button(decision, text)
    # Tapped (or typed) the "write a different answer" row -> collect the write-in next.
    if ref == CUSTOM_OPTION_ID:
        record["await_freeform"] = True
        set_active(store, topic_id, record)
        return {"action": "local", "reply": "✍️ Type your answer as a normal message and I'll send it to Claude."}
    if ref is not None:
        label = _label_for_ref(decision, ref)
        return _submit_directive(
            worker_id, decision_id, {"option_refs": [ref]},
            f"✅ Sent “{label}” to Claude.",
        )
    if kind == "plan":
        # No free-text answer for a plan gate — nudge back to the two buttons.
        return {"action": "local", "reply": "Tap Approve or Revise to answer the plan."}
    # Free text on a single-select -> submit it as a write-in.
    return _submit_directive(
        worker_id, decision_id, {"text": text},
        f"✍️ Sent your answer to Claude: “{sanitize_text(text, 200)}”.",
    )


def _handle_multi_answer(
    store: dict[str, Any], topic_id: str, record: dict[str, Any], decision: ResolvedDecision, text: str
) -> dict[str, Any]:
    worker_id = str(record.get("worker_id") or "")
    decision_id = str(record.get("decision_id") or "")
    selected: list[str] = [str(s) for s in record.get("selected", [])]

    if _strip_marker(text).casefold() == _strip_marker(SUBMIT_LABEL).casefold():
        if not selected:
            return {"action": "local", "reply": "Tap at least one option to select it, then tap Submit."}
        return _submit_directive(
            worker_id, decision_id, {"option_refs": selected},
            f"✅ Submitted your selection to Claude ({len(selected)} chosen).",
        )

    ref = _resolve_button(decision, text)
    if ref is None:
        return {"action": "local", "reply": "Tap an option to toggle it, then tap Submit."}
    if ref in selected:
        selected.remove(ref)
    else:
        selected.append(ref)
    record["selected"] = selected
    set_active(store, topic_id, record)
    chosen = [_label_for_ref(decision, r) for r in selected]
    picked = ", ".join(sanitize_text(c, 60) for c in chosen if c) or "none"
    return {"action": "local", "reply": f"Selected: {picked}. Tap more options or tap Submit."}


def _label_for_ref(decision: ResolvedDecision, ref: str) -> str:
    for label, candidate in _labeled_refs(decision):
        if candidate == ref:
            return _strip_marker(label)
    return ref
