# Structured Turn Feed

The structured turn feed is the cleanest way to run herdres. Instead of inferring reports, questions, and updates from scraped terminal text, herdres asks Herdr for a structured last-turn object per pane and renders only that. This page covers the turn feed contract, streaming drafts (live preview), clean report markers, and the local turn-adapter fallback for when Herdr does not expose the endpoint yet.

For installation and service wiring, see SETUP.md. For the full command list, see COMMANDS.md. For how topics map to spaces/panes, see TOPICS.md. For fail-closed behavior, see SAFETY.md.

## Why Turn Feed

`HERDR_TELEGRAM_TOPICS_TURN_FEED=1` is the default. With it enabled, herdres does **not** parse terminal transcripts to guess what happened. It calls a Herdr CLI endpoint that returns the submitted user instruction plus the final assistant reply as structured JSON, and renders only that into the mapped space topic.

Benefits:

- No TUI chrome, spinners, tool calls, or thinking blocks leak into Telegram.
- The owner sees the last instruction and the final answer, nothing else.
- Herdres consumes an **optional upstream contract** and never patches Herdr core, so Herdr stays upgrade-safe.

When turn feed is enabled and the endpoint is unavailable, empty, or returns an incomplete turn with no stream preview and no decision, **herdres sends nothing**. It does not fall back to scraping `pane read`.

## The `pane turn` Contract

Turn feed calls exactly:

```bash
herdr pane turn <pane_id> --last --format json
```

Herdres reads `result.turn` if the response wraps the turn under `result`, otherwise it uses the top-level object. A completed turn looks like:

```json
{
  "available": true,
  "pane_id": "pane-1",
  "agent_session_id": "session-1",
  "turn_id": "turn-1",
  "complete": true,
  "user_text": "Diagnose why the bot froze.",
  "assistant_final_text": "Likely cause...\n\nWhat I did..."
}
```

### Field semantics

| Field | Meaning |
|---|---|
| `available` | `true` if Herdr can produce a structured turn. `false` means send nothing for this pane. |
| `pane_id` | The pane the turn belongs to. |
| `agent_session_id` | Stable session identity. Used to re-anchor a pane after Herdr changes its public handle. |
| `turn_id` | Identity of this turn. Drives dedupe — a turn is only delivered once. |
| `complete` | `true` only when the assistant has finished. Only a complete turn with `assistant_final_text` is persisted as the final Telegram answer. |
| `user_text` | The **submitted** user instruction. Must never be the visible input composer text. |
| `assistant_final_text` | Final assistant output only — no thinking, tool calls, shell output, or TUI chrome. |

`user_text` is rendered as the prompt that opened the turn; `assistant_final_text` is rendered as the answer. Lengths are bounded by `HERDR_TELEGRAM_TOPICS_FINAL_REPLY_MAX_CHARS` / `..._FINAL_REPLY_MAX_LINES` for the answer and `HERDR_TELEGRAM_TOPICS_USER_PROMPT_MAX_CHARS` for the echoed prompt.

### Unavailable response

If Herdr cannot provide structured turn data it returns:

```json
{"available": false, "reason": "no_structured_turn_source"}
```

Herdres treats any of these as "send nothing": `available: false`, an empty turn, or an incomplete turn with no stream preview and no decision.

## Streaming Drafts (Live Preview)

While a turn is still running, the endpoint (or local adapter) can return partial assistant text without completing the turn:

```json
{
  "available": true,
  "pane_id": "pane-1",
  "turn_id": "turn-1",
  "complete": false,
  "user_text": "Diagnose why the bot froze.",
  "assistant_stream_text": "I am checking the process tree and logs...",
  "stream_revision": "a stable content revision"
}
```

Herdres treats `assistant_stream_text` as a **draft preview only**:

- It does **not** mark the turn complete.
- It still requires a later complete turn carrying `assistant_final_text` before the answer is persisted.
- `stream_revision` (when present) is the stable content revision used to decide whether the draft actually changed; otherwise herdres hashes the stream text.

On the Telegram side, drafts go out via `sendMessageDraft` / `sendRichMessageDraft` when the client supports draft streaming. Repeated drafts for the same turn reuse a stable `draft_id`, so Telegram animates the update in place instead of posting new messages. Drafts are sent into the space topic on the same thread; if pane-root messages are enabled they also carry `reply_parameters` to the pane root, otherwise the draft itself becomes the routed pane message.

Draft streaming is capability-probed at runtime. If Telegram rejects a draft method as unsupported, herdres records `telegram.streaming_drafts.supported=no` and stops sending draft previews. This **never** disables final rich messages.

On macOS the reconcile timer runs every ~5 seconds so short turns still get a sync window for at least one draft before the final answer lands.

### Streaming controls

| Env var | Default | Purpose |
|---|---|---|
| `HERDR_TELEGRAM_TOPICS_STREAMING` | `1` | Master switch for draft previews. |
| `HERDR_TELEGRAM_TOPICS_STREAM_MIN_INTERVAL` | `2` | Minimum seconds between draft updates for a pane (throttle). |
| `HERDR_TELEGRAM_TOPICS_STREAM_MIN_CHARS` | `80` | Suppress a new draft if the cleaned text is shorter than this and a prior draft already exists. |
| `HERDR_TELEGRAM_TOPICS_MAX_DRAFTS` | `8` | Cap on draft messages per turn. |

Drafts are recognized in dry-run mode, so include them in preflight QA:

```bash
HERDR_TELEGRAM_TOPICS_DRY_RUN=1 ~/.local/bin/herdres sync
```

## Pending Decisions (Structured Buttons)

If a pane is waiting for owner input, Herdr can return a structured decision instead of a completed turn:

```json
{
  "available": true,
  "pane_id": "pane-1",
  "turn_id": "turn-2",
  "complete": false,
  "awaiting_input": true,
  "user_text": "Choose an implementation path.",
  "pending_decision": {
    "decision_id": "turn-2:decision-1",
    "prompt": "Which path should I take?",
    "mode": "buttons",
    "options": [
      {"id": "fast", "label": "Patch minimal path", "send_text": "1"},
      {"id": "full", "label": "Build full path", "send_text": "2"},
      {"id": "custom", "label": "Write custom instruction", "send_text": ""}
    ]
  }
}
```

Herdres renders `pending_decision` as a rich decision card with inline buttons in the pane thread. Behavior:

- Button taps route **only** to that pane and are bound to the Telegram message that created the buttons, so stale buttons from older messages are rejected.
- `send_text` is the exact text sent to Herdr for a direct option.
- An empty `send_text` (`""`) opens a ForceReply-style custom-instruction prompt instead of sending a fixed string.
- Native Telegram polls are intentionally not part of the owner-control flow.

Structured `pending_decision` data is the supported automatic button path. The only other path that produces buttons is an explicit `HERDRES_CHOICES_START` block emitted by the pane (see Clean Report Markers below). Buttons inferred from visible TUI choice screens are **off by default** — see SAFETY.md.

The bundled Claude adapter (`herdr_turn_adapter.py`) emits `pending_decision` automatically for a pending **`AskUserQuestion`** (single, single-select → one option per choice + a "Write a different answer" custom option) and **`ExitPlanMode`** (→ "Approve & proceed" / "Keep planning / revise"), so these render as tappable buttons rather than a read-only visible-screen prompt. Multi-question or multi-select `AskUserQuestion` maps to a read-only `pending_interaction` (answer in the pane). Toggle with `HERDRES_TURN_ADAPTER_DECISIONS=0`. `ExitPlanMode` is a TUI approve/reject dialog, not a freeform-text prompt, so the approve answer is tunable via `HERDRES_PLAN_APPROVE_SEND_TEXT` (default `1`); "Keep planning / revise" uses an empty `send_text` → ForceReply for typed feedback.

**How the adapter sees a pending prompt (issue #36).** Claude Code does *not* write a pending `AskUserQuestion`/`ExitPlanMode` tool_use into the session transcript until it is answered — so the adapter cannot scrape it from the `.jsonl`. Instead a small Claude Code hook (`herdres-decision-hook`, installed via `herdres hooks install` — see SETUP.md) fires on `PreToolUse` for those two tools and records the prompt's structured `tool_input` to a per-session file at `~/.local/share/herdres/pending/<session_id>.json` (override `HERDRES_PENDING_DIR`). The adapter reads that file (keyed by the pane's `session_id`) and maps it through the same logic above. The hook clears the file when the prompt is answered (`PostToolUse`), when a brand-new prompt supersedes it (`UserPromptSubmit` — so a cancelled prompt can't re-surface stale buttons over a later, unrelated turn), and on `SessionEnd`; the adapter additionally ignores any file older than `HERDRES_PENDING_TTL_SECONDS` (default `3600`) as a final backstop against a missed clear. The decision is only surfaced while the turn is still awaiting owner input. **If the hook isn't installed, these never become buttons** (the rest of the turn feed is unaffected).

## Pending Interactions (Read-Only Forms)

For multi-question prompts (for example Claude wizards with a later "Review your answers" / submit step), the intended future contract is a normalized structured interaction, not visible-screen key driving:

```json
{
  "pending_interaction": {
    "interaction_id": "turn-2:interaction-1",
    "revision": 1,
    "kind": "multi_question_form",
    "questions": [
      {
        "question_id": "q1",
        "type": "single_choice",
        "title": "Register/tone vs length",
        "options": [
          {"option_id": "1", "label": "Mostly register/tone", "value": "1"}
        ]
      }
    ],
    "answers": {},
    "review": {"can_submit": false, "submit_label": "Submit answers"}
  }
}
```

Until Herdr (or the local adapter) exposes a semantic submit path for this shape, herdres **fails closed**: it shows the form **read-only** and does not send native TUI keys. This holds even when `kind` is `single_question` and options carry `value` or `send_text` — `pending_interaction` is read-only in this phase. Producers that want immediate Telegram buttons must emit `pending_decision` instead.

`HERDR_TELEGRAM_TOPICS_STRUCTURED_INTERACTIONS=1` (default) enables parsing of `pending_interaction` and `pending_decision`. When a structured interaction is present it takes precedence over `pending_decision` for the same turn.

When structured data is absent and `HERDR_TELEGRAM_TOPICS_VISIBLE_READONLY_PROMPTS=1` (default), herdres may still show a visible-screen prompt read-only so the owner can see the question — but read-only prompts never create buttons, ForceReply state, or key-driving callbacks. If a pane only exposes choices through visible TUI text, answer in the Herdr pane directly; use `/send <text>` only for simple text prompts until the pane exposes a structured contract.

## Clean Report Markers

By default, automatic sync posts only bounded reports, real choice prompts, actionable questions, and blocked/error items. It does **not** auto-post unbounded `Summary:`, `Final:`, `Verification:`, or `What changed:` transcript text unless `HERDR_TELEGRAM_TOPICS_UNBOUNDED_REPORTS=1` is set.

For the cleanest output, have the pane emit an explicit bounded report between markers:

```text
HERDRES_REPORT_START
HERDRES_REPORT_TITLE: Deployment
What changed:
- Added cached Docker stats collection.
- Reduced timer overhead.
Verification:
- Timer run succeeded.
- Cache path confirmed.
HERDRES_REPORT_END
```

`HERDRES_REPORT_TITLE:` is optional but recommended. Without it, the first report line must be a short title such as `Deployment` or `What changed:`. Malformed bounded reports are ignored rather than posted as noisy updates.

Bounded reports can use structured sections that render as Telegram rich headings, paragraphs, tables, checklists, collapsible details, and footers:

```text
HERDRES_REPORT_START
HERDRES_REPORT_TITLE: Sprint Status
SUMMARY:
Driver App release is done and Route Optimizer is blocked.
TABLE:
Task | Owner | Status
Driver App release | Alex | Done
Route optimizer | Luke | Blocked
CHECKLIST:
[x] Review PR
[ ] Run staging smoke test
DETAILS: Risks
- Route Optimizer dependency is blocking release.
FOOTER:
Sprint - Smith - 10:58
HERDRES_REPORT_END
```

Content inside bounded reports bypasses the global TUI-noise filters, but is still sanitized for secrets and control characters.

Section aliases are accepted: `SHORT SUMMARY:` → `SUMMARY:`; `STATUS:` or `METRICS:` → `TABLE:`; `NEXT:` → `CHECKLIST:`; `RISKS:`, `PROOF:`, `LOGS:`, `COMMANDS:`, `DIFF:` → collapsed details; `META:` → `FOOTER:`.

For explicit choice buttons without relying on nearby question text:

```text
HERDRES_CHOICES_START
Question:
Choose the next action.
1. Run sync now
2. Show planned changes
HERDRES_CHOICES_END
```

Explicit `HERDRES_CHOICES` blocks are treated as agent-authored safe controls and render buttons. Inferred legacy and visible-screen choice buttons are controlled separately and stay off unless you have verified the pane's native prompt can be safely driven from Telegram (see SAFETY.md).

## Local Turn-Adapter Fallback

If your Herdr build does not yet expose `pane turn`, herdres ships a wrapper so you can use turn feed today **without patching Herdr**. The wrapper is `herdr_turn_adapter.py`.

Install it:

```bash
install -Dm755 herdr_turn_adapter.py ~/.local/bin/herdr_turn_adapter.py
```

Point only the herdres service at it:

```bash
HERDR_BIN=/home/smith/.local/bin/herdr_turn_adapter.py
HERDR_REAL_BIN=/home/smith/.local/bin/herdr
HERDR_TELEGRAM_TOPICS_TURN_FEED=1
```

How it behaves:

- It implements **only** `pane turn <pane_id> --last --format json`. It synthesizes a turn from agent session logs and prints it as JSON.
- **Every other command** is delegated unchanged to `HERDR_REAL_BIN` (default `herdr`) via `exec`. The adapter is a pass-through wrapper, not a Herdr fork.
- Current local extraction supports Codex session IDs reported by Herdr, and Claude when Herdr reports a Claude `agent_session_id`.
- If a pane has no usable session id, the wrapper returns `available: false` (e.g. `unsupported_agent`) and herdres sends nothing for that pane.

Because `HERDR_BIN` only changes how the herdres service invokes Herdr, set it solely in the herdres service environment — do not export it globally where it could redirect your interactive `herdr` usage. Keep `HERDR_REAL_BIN` pointed at the genuine Herdr binary so delegated commands still work.

## Quick Verification

| Goal | Command |
|---|---|
| See the raw turn JSON herdres consumes | `herdr pane turn <pane_id> --last --format json` (or run the adapter directly with the same args) |
| Dry-run a full sync, including drafts | `HERDR_TELEGRAM_TOPICS_DRY_RUN=1 ~/.local/bin/herdres sync` |
| Confirm rich/draft delivery to a scratch topic | `~/.local/bin/herdres probe --thread-id <id>` |

If `pane turn` errors or returns `available: false` for every pane, turn feed will correctly post nothing. Confirm the endpoint (or the adapter) is reachable from the herdres service environment before assuming herdres is misconfigured.
