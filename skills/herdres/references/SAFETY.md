# herdres Operator Safety

Load-bearing rules that keep herdres from corrupting Telegram state, double-posting,
or driving the wrong answer into an agent pane. These are **fail-closed by design**:
when herdres cannot prove the safe action, it does nothing rather than guess. Honor
them when you operate herdres on a user's behalf.

For command syntax see COMMANDS.md. For install and service wiring see SETUP.md.

---

## 1. One `getUpdates` consumer per bot token (hard Telegram limit)

Telegram allows **only one active `getUpdates` long-poll consumer per bot token**.
Running two consumers makes them steal updates from each other, so inbound pane
commands and callback-button taps land randomly on one process or the other and
appear to be silently dropped.

**NEVER run two inbound consumers on the same token.** Concretely:

- Do **not** run a Hermes Telegram poller **and** `herdres-gateway` on the same
  `TELEGRAM_BOT_TOKEN`.
- Choose **one** inbound path:
  - the **Hermes bridge** (`herdr_topic_bridge.py` loaded into the Hermes gateway), or
  - the **standalone gateway** (`herdres-gateway`, `HERDRES_GATEWAY_RUNNER=embedded` by default).
- Outbound-only paths are safe to run alongside the chosen consumer:
  `herdres sync` and `herdres event` only **send**; they never call `getUpdates`.

Migration tip: give the gateway its **own** bot via `HERDRES_GATEWAY_BOT_TOKEN`.
It falls back to `TELEGRAM_BOT_TOKEN` only during a single-token migration — and
during that window you must stop the Hermes poller.

Managed child bots do **not** violate this rule: the gateway runs exactly **one**
long-poll worker per distinct bot token (manager + each child), never two on the
same token.

Before starting a second poller, confirm what already consumes the token:

```bash
systemctl --user status hermes-gateway.service herdres-gateway.service
```

**You MUST NOT scavenge or auto-reuse credentials.** The `TELEGRAM_BOT_TOKEN`,
`HERDR_TELEGRAM_TOPICS_CHAT_ID`, and `TELEGRAM_ALLOWED_USERS` are **user-supplied
secrets**. When setting herdres up you **MUST** obtain them **from the user** —
**NEVER** invent them, and **NEVER** copy a token out of another app's config
(e.g. an existing Hermes bot) on your own initiative. Reusing a token that Hermes
already long-polls is the most common way to violate the one-consumer rule above.
**Always** prefer a **dedicated** bot for herdres; if the user wants to share a
token, make it an explicit, informed choice and ensure only one consumer polls it.
Running non-interactively with a required secret missing? You **MUST** stop and
report what's needed — **do not guess.**

> A `SKILL.md` instruction can only *bias* a probabilistic agent; it cannot
> *guarantee* this. The enforcing fix is an interactive **setup wizard** — the
> binary, not the agent, does the asking (tracked in #7).

---

## 2. Owner-only gate — enforce `TELEGRAM_ALLOWED_USERS`

`TELEGRAM_ALLOWED_USERS` is the comma-separated allowlist of owner Telegram user
IDs. Every inbound pane command, plain-text forward, attachment, and `/new` pane
launch is gated on it. Messages from non-owners, edited messages, forwarded
messages, and bot-authored messages are **silently ignored** (they never reach a
pane).

- This must be set. Never leave it blank or point it at an untrusted user — a
  pane controls a live agent CLI, so anyone on the allowlist can drive that agent.
- Owner IDs are numeric Telegram user IDs, comma-separated:
  `TELEGRAM_ALLOWED_USERS=123456789,987654321`.
- Forwarded owner messages in a pane topic are rejected (`Ignored non-direct
  owner message in pane topic.`) so a forwarded screenshot/text cannot be
  replayed as a live instruction.

---

## 3. Multi-pane space topics FAIL CLOSED

In the default per-space topic model, one Telegram topic can map to several live
panes. herdres will not guess which pane an ambiguous message targets.

Resolution rules for an inbound message in a space topic:

| Situation | Behavior |
|---|---|
| Reply to a specific routed pane message / ForceReply / pane-root card | Forwarded to **that** pane |
| `/send <text>` or `/send! <text>` | Forwarded to the pane the command thread resolves to |
| Top-level plain text, topic has **exactly one** live pane | Forwarded to that single pane |
| Top-level plain text, topic has **multiple** live panes | **Fails closed** |

When it fails closed, herdres replies:

> `Reply inside a pane thread so I know which Herdr pane to control.`

If you (or the user) see that reply, do **not** retry the same way — reply
**inside the specific pane's thread**, or use `/send <text>` so the target pane
is unambiguous. The General topic stays normal Hermes chat and is never a pane.

Top-level plain-text → pane is additionally gated by
`telegram.implicit_send_enabled` (a state flag, default off) except in the
single-live-pane case above. If implicit send is off and a topic has multiple
panes, plain text is not forwarded; use `/send`.

---

## 4. NEVER key-drive Claude multi-question / review wizards from Telegram

Only **structured `pending_decision` buttons** are safe to drive a pane from
Telegram. Visible-TUI key driving can select the wrong question or the wrong
default answer in a multi-step wizard.

What is safe to render as actionable Telegram buttons:

- Structured `pending_decision` data from the turn feed (each option carries an
  exact `send_text`; an empty `send_text` opens a ForceReply custom-instruction prompt).
- Explicit agent-authored `HERDRES_CHOICES_START … HERDRES_CHOICES_END` blocks.

What is **READ-ONLY** and must never become a button, ForceReply, or key-driving
callback:

- `pending_interaction` (multi-question forms **and** single-question forms). This
  is read-only **even when** `kind` is `single_question` and options include
  `value`/`send_text`. herdres fails closed instead of sending native TUI keys.
  If a producer wants immediate buttons, it must emit `pending_decision` instead.
- Visible-screen prompts shown when `HERDR_TELEGRAM_TOPICS_VISIBLE_READONLY_PROMPTS=1`.
  These let the user *see* the question and option text in Telegram but create no
  buttons, no ForceReply state, and no callbacks.

Keep inferred visible-choice buttons **off**: `HERDR_TELEGRAM_TOPICS_VISIBLE_CHOICE_BUTTONS=0`
and `HERDR_TELEGRAM_TOPICS_LEGACY_CHOICES=0` (their defaults). Do not flip them on
unless you have confirmed the pane's native prompt can be safely driven from Telegram.

When a pane only exposes a choice through visible TUI text (no structured
contract), **answer in the Herdr pane directly**. Use `/send <text>` only for
simple free-text prompts, never to navigate a multi-question wizard.

**Stale-button protection:** decision-card button taps are bound to the exact
Telegram message that created them and route only to that pane. Taps from older
messages are rejected. If a choice prompt has gone unsafe by the time the user
replies, herdres answers:

> `That choice prompt is no longer safe from Telegram. Use /send or answer in Herdr.`

That is correct fail-closed behavior — re-fetch current choices with `/choices`
or answer in the pane; do not try to force the old answer through.

---

## 5. Transient/network send errors are NOT retried

To avoid **duplicate posts**, herdres does not resend a message that failed with a
transient or network error (timeouts, connection resets, HTTP 5xx, TLS/network
unreachable). A half-sent message that Telegram may already have accepted is not
blindly re-fired.

- Live status cards, pinned status, and topic icons **reconcile on the next timer
  tick** — they self-heal, so a transient blip resolves on its own.
- Do **not** "help" by re-running `herdres sync` in a tight loop or manually
  re-posting after a transient failure. Let the reconcile timer catch up.
- A transient **preflight** failure (when herdres recently succeeded) is recorded
  as a non-paging warning, not a loud alert; a genuine permission/auth failure
  does page. Treat repeated **non-transient** errors as a real config problem
  (token, chat ID, admin rights) — see SETUP.md.

The systemd timer (Linux) / launchd agent (macOS) is the slow repair path: it
detects closed panes, repairs stale topic mappings, and covers missed plugin
events. Keep it enabled even when plugin events are on.

---

## 6. State ownership — outbound writes vs inbound reads

Exactly one class of path may write **outbound delivery state** (message routes,
sent-message hashes, active-prompt bindings, pinned/marker IDs):

- **Owners of outbound state writes:** `herdres sync` (timer) and `herdres event`
  (plugin trigger). They send, then persist what they sent.
- **Inbound paths (`herdres command` / `herdres callback`) only read and delegate.**
  They resolve the target pane from the topic/reply, verify the owner, and hand
  the instruction to Herdr via `send_to_pane`. They do not author the outbound
  delivery record for that pane's turn — the next sync/event tick reconciles it.

Why it matters when you operate:

- Do not run an **old and a new** herdres version against the same
  `HERDR_TELEGRAM_TOPICS_STATE` file at once; both can write it and corrupt
  mappings. Rollback is **state-only**: stop herdres, restore the prior state
  JSON backup, restart the old version (see README "Migration And Rollback Notes").
- All state-mutating commands take a lock (`HERDR_TELEGRAM_TOPICS_LOCK`). Don't
  bypass it by editing `state.json` by hand while a service is running.

---

## 7. `cleanup-duplicates` deletes only proven-dead duplicates

`herdres cleanup-duplicates` removes legacy duplicate forum topics left over from
a migration (e.g. when a pane handle changed like `w...-2` → `w...:p2`). It is
deliberately conservative.

A topic is deleted **only when both** hold:

1. It is mapped in herdres state to a **CLOSED** pane, **and**
2. That closed pane matches a **live** pane by **strong identity** — the same
   agent session id (`agent_session_id`), or an equivalent old/new pane
   handle/alias. (Internally this is a match score ≥ 90, where an exact
   `agent_session_id` match alone scores 100.)

Guarantees:

- It **NEVER deletes a live space topic**, and never deletes a topic that is still
  mapped to a live pane.
- A closed pane that does **not** strongly match any live pane is left untouched.
- Deleted entries are archived under `deleted_duplicate_topics` in state for audit.

Safe operating procedure:

```bash
herdres cleanup-duplicates            # inspect only — lists what WOULD be deleted
herdres cleanup-duplicates --delete   # delete the listed duplicates
```

- Always run the **inspect** form first and review the list before `--delete`.
- Per-run deletions are capped by `HERDR_TELEGRAM_TOPICS_DUPLICATE_DELETE_LIMIT`
  (default 12). Re-run if there are more.
- Old Telegram topics from a **per-agent ⇄ per-space mode switch** are *not*
  touched by this command — that switch is a clean-slate reset that leaves old
  topics in place for **manual** deletion (see TOPICS.md). `cleanup-duplicates`
  only handles state-mapped closed duplicates, not stale topics from a mode flip.

---

## 8. Preflight before any destructive or first-time run

Validate non-destructively before doing anything that creates or deletes Telegram
state, especially on a new chat or after config changes:

```bash
HERDR_TELEGRAM_TOPICS_DRY_RUN=1 herdres sync
```

Dry-run recognizes rich and draft methods, so it doubles as preflight QA without
writing to Telegram. `herdres probe --thread-id <id>` sends one rich message to a
scratch topic and deletes it, to confirm rich delivery and bot permissions.

`cleanup-duplicates --delete` runs its own preflight first and aborts the run if
preflight fails, so it will not start deleting against a misconfigured chat.

---

## Quick checklist

- [ ] Exactly **one** `getUpdates` consumer per bot token (Hermes bridge **xor** gateway).
- [ ] `TELEGRAM_ALLOWED_USERS` set to trusted owner IDs only.
- [ ] Ambiguous multi-pane replies: reply **inside the pane thread** or use `/send`.
- [ ] Never key-drive `pending_interaction` / visible wizards — `pending_decision` buttons only.
- [ ] Never tight-loop retries after a transient send failure; let the timer reconcile.
- [ ] One herdres version per state file; rollback by restoring the state backup.
- [ ] `cleanup-duplicates` inspect → review → `--delete`; it never removes live topics.
- [ ] Dry-run / probe before first-time or destructive runs.
