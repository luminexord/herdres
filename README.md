<img width="2048" height="2048" alt="herdres" src="https://github.com/user-attachments/assets/d8324729-676a-49d8-9d24-800a8a411348" />

# Herdres

Rich Telegram forum-topic visibility and control for Herdr panes.

Herdres is a small stdlib-only Python bridge that maps each live Herdr pane to a Telegram forum topic. It can post explicit rich-message reports/questions/choices today, and it can switch to structured turn delivery when Herdr exposes a safe last-turn endpoint.

It does not patch Hermes or Herdr core files and routine sync uses no LLM calls.

## What It Does

- Creates or maintains one Telegram forum topic per Herdr pane.
- Keeps the General topic free for your normal Hermes chat.
- Sends pane updates with Telegram Bot API 10.1 `sendRichMessage`.
- Can use `editMessageText(rich_message=...)` for a quiet live status card when bottom status markers are disabled.
- Posts a compact bottom-of-topic status marker, such as `🟡 Working` or `🟢 Idle`, only when status changes.
- Shows clean reports, questions, blockers, numbered choices, and structured decision buttons.
- Optionally shows only the last submitted user instruction plus the final assistant reply when `herdr pane turn` is available.
- Keeps raw transcript and technical metadata behind explicit commands.
- Routes `/send`, `/keys`, and choice-button replies only to the mapped pane.

## Requirements

- Python 3.11+
- `herdr` available on `PATH`, or set `HERDR_BIN`
- Telegram bot token
- A Telegram supergroup with forum topics enabled
- Bot must be an admin with **Manage Topics**

## Quick Start

```bash
git clone https://github.com/gaijinjoe/herdres.git
cd herdres

install -Dm755 herdres.py ~/.local/bin/herdres
install -Dm644 .env.example ~/.config/herdres/herdres.env
$EDITOR ~/.config/herdres/herdres.env
```

Run a safe dry-run first:

```bash
HERDR_TELEGRAM_TOPICS_DRY_RUN=1 ~/.local/bin/herdres sync
```

Run once for real:

```bash
~/.local/bin/herdres sync
```

Install the user timer:

```bash
mkdir -p ~/.config/systemd/user
cp systemd/user/herdres.* ~/.config/systemd/user/
systemctl --user daemon-reload
systemctl --user enable --now herdres.timer
```

## Required Config

Edit `~/.config/herdres/herdres.env`:

```bash
TELEGRAM_BOT_TOKEN=
HERDR_TELEGRAM_TOPICS_CHAT_ID=-1001234567890
TELEGRAM_ALLOWED_USERS=123456789
```

Get your chat ID from Telegram bot updates or another admin tool. Owner user IDs are comma-separated.

## Pane Topic Commands

Inside a mapped pane topic:

- `/report` or `/status` - resend latest clean rich report
- `/choices` - resend active choices with buttons
- `/raw [lines]` - show sanitized raw visible output
- `/debug` - show technical mapping details
- `/send <text>` - send instruction to this pane
- `/keys <keys>` - send explicit keys to this pane

Plain text is not forwarded unless `telegram.implicit_send_enabled` is enabled in state. When enabled, plain text from an authorized owner in a mapped pane topic is sent only to that mapped pane. The General topic remains normal Hermes chat.

Long or multiline Telegram inputs are not pasted into the terminal directly. Herdres writes the exact owner message to `~/.local/share/herdres/inbound/<pane>/...txt` with owner-only permissions and submits a short instruction telling the pane to read that file. This avoids Herdr/TUI bracketed-paste collapse such as `[Pasted text #1 ...]` being sent as the actual instruction.

Controls:

```bash
HERDR_TELEGRAM_TOPICS_INPUT_FILE_CHARS=1200
HERDR_TELEGRAM_TOPICS_INPUT_FILE_LINES=6
HERDR_TELEGRAM_TOPICS_INPUT_FILE_MAX_CHARS=120000
```

Inbound pane-topic control is handled through the Hermes Telegram gateway, so Hermes must load the small bridge hook:

```bash
install -Dm644 herdr_topic_bridge.py ~/.local/share/herdres/herdr_topic_bridge.py
install -Dm755 herdr_telegram_topics_install_bridge.py ~/.local/bin/herdr_telegram_topics_install_bridge.py
mkdir -p ~/.config/systemd/user/hermes-gateway.service.d
cat > ~/.config/systemd/user/hermes-gateway.service.d/herdr-telegram-topics.conf <<'EOF'
[Service]
Environment=HERDR_TELEGRAM_TOPICS_STATE=%h/.local/share/herdres/state.json
Environment=HERDR_TELEGRAM_TOPICS_SCRIPT=%h/.local/bin/herdres
ExecStartPre=-%h/.local/bin/herdr_telegram_topics_install_bridge.py --quiet
EOF
systemctl --user daemon-reload
systemctl --user restart hermes-gateway.service
```

Standalone inbound gateway:

Herdres also includes `herdres-gateway.py`, a stdlib-only `getUpdates` daemon
that can replace the Hermes inbound bridge for pane-topic commands and
callbacks. Set `HERDRES_GATEWAY_BOT_TOKEN` in `~/.config/herdres/herdres.env`
for a gateway-owned bot, or let it fall back to `TELEGRAM_BOT_TOKEN` during a
single-token migration. For any one bot token, run either Hermes polling or
`herdres-gateway.service`, never both, because Telegram permits only one active
`getUpdates` consumer per bot token.

## Herdr Plugin Event Trigger

Herdr 0.7.0 adds local plugin events. Herdres can use those events as a low-latency trigger layer while keeping the Telegram forum-topic UX in Herdres.

The included plugin is intentionally thin:

```text
pane.agent_status_changed -> herdres event
```

It does not replace the Hermes bridge for inbound Telegram commands and callbacks, and it does not use the official plain Telegram notification example.

Install or link it after Herdr is upgraded:

```bash
herdr --version
herdr plugin list --json
herdr plugin link ~/.local/share/herdres/herdres-plugin
```

`install-user.sh` writes the installed plugin manifest with an absolute `~/.local/bin/herdres` command so it does not depend on Herdr's plugin `PATH`. If you link `herdres-plugin/` directly from a source checkout, make sure `herdres` is resolvable in the plugin runtime environment or edit the manifest command to an absolute path.

`herdres event` reads `HERDR_PLUGIN_CONTEXT_JSON` and `HERDR_PLUGIN_EVENT_JSON`, reconciles only the changed pane, ensures the topic exists, syncs the pane label to the Telegram topic name, and sends/edits the structured turn or pending decision if one is available. In plugin mode it does not parse terminal text. If structured turn data is unavailable, it sends nothing.

For completed/idle/blocked/error status events, `herdres event` waits briefly and rechecks the structured turn feed before giving up. This handles the normal race where Herdr fires `pane.agent_status_changed` just before the agent session file exposes the final completed turn. Tune this with `HERDR_TELEGRAM_TOPICS_EVENT_SETTLE_SECONDS` and `HERDR_TELEGRAM_TOPICS_EVENT_SETTLE_INTERVAL`.

Plugin events can be toggled independently of normal sync:

```bash
~/.local/bin/herdres plugin-disable
~/.local/bin/herdres plugin-enable
```

Keep the systemd timer enabled as a slower repair/reconcile path. It detects closed panes, repairs stale topic mappings, and covers missed plugin events.

## Duplicate Topic Cleanup

Herdres avoids duplicate topics by reusing a closed pane's existing topic mapping when Herdr changes a public pane handle, such as `w...-2` becoming `w...:p2`, as long as the state proves it is the same pane/session.

If duplicates already exist from a previous migration, inspect them first:

```bash
~/.local/bin/herdres cleanup-duplicates
```

Delete only Herdres-owned closed duplicates:

```bash
~/.local/bin/herdres cleanup-duplicates --delete
```

The cleanup only targets topics that are both:

- mapped in Herdres state to a closed pane, and
- matched to a live pane by strong identity, such as the same agent session or equivalent old/new pane handle.

It never deletes the live pane topic. Deleted state entries are archived under `deleted_duplicate_topics` for audit. Use `HERDR_TELEGRAM_TOPICS_DUPLICATE_DELETE_LIMIT` to cap deletions per run.

## Rich Message Behavior

Herdres tries rich delivery first:

- `sendRichMessage` for reports, choices, notices, and detail prompts
- `editMessageText` with `rich_message` for live cards
- `reply_parameters` for rich replies and ForceReply anchors
- `reply_markup` for inline choice buttons and ForceReply

Fallback policy:

- Missing rich endpoint: latch rich off and fall back to `sendMessage`
- Bad rich HTML: fall back once to `sendMessage`
- Transient/network error: do not resend, to avoid duplicate posts
- Live-card edits retry naturally on the next timer tick

## Topic Status Icons And Markers

Telegram bots cannot edit an existing forum topic's color directly, but they can edit the topic's `icon_custom_emoji_id`. Herdres can use that as the low-noise pane status surface:

```text
⚡️ working
☕️ idle
✅ done
❗️ blocked/waiting
‼️ error
📈 workflow activity
```

Herdres first checks explicit `HERDR_TELEGRAM_TOPICS_STATUS_ICON_*` custom emoji IDs. If those are unset, it calls `getForumTopicIconStickers`, caches the returned forum-icon stickers, and matches them by the configured `*_EMOJI` values. The defaults above are chosen from Telegram's allowed forum topic icon set. Icon edits happen only when the pane status icon changes, so routine sync does not spend messages or LLM tokens on status display.

Controls:

```bash
HERDR_TELEGRAM_TOPICS_STATUS_ICON=1
HERDR_TELEGRAM_TOPICS_STATUS_ICON_CACHE_TTL=86400
HERDR_TELEGRAM_TOPICS_STATUS_ICON_RETRY=300
HERDR_TELEGRAM_TOPICS_STATUS_MARKER_SUPPRESS_WHEN_ICON_OK=1

# Optional explicit Telegram custom emoji IDs for forum topic icons.
HERDR_TELEGRAM_TOPICS_STATUS_ICON_WORKING=
HERDR_TELEGRAM_TOPICS_STATUS_ICON_IDLE=
HERDR_TELEGRAM_TOPICS_STATUS_ICON_DONE=
HERDR_TELEGRAM_TOPICS_STATUS_ICON_BLOCKED=
HERDR_TELEGRAM_TOPICS_STATUS_ICON_ERROR=
HERDR_TELEGRAM_TOPICS_STATUS_ICON_WORKFLOW=
HERDR_TELEGRAM_TOPICS_STATUS_ICON_UNKNOWN=
```

If Telegram does not offer a matching forum icon sticker, Herdres leaves the topic icon unchanged and can still use compact marker messages.

Telegram does not move edited messages to the bottom of a topic. To keep each pane topic glanceable, Herdres can post a compact status marker as the latest message in the mapped topic:

```text
🟡 Working
Work is in progress.

🟢 Idle
No active work.
```

The marker is low-noise: it is sent only when the compact status changes. When possible, Herdres deletes the previous marker after posting the new one, so each topic keeps one current status marker near the bottom. Final replies and decision cards always take priority: if a final reply or decision card is delivered in a run, Herdres skips that pane's marker until the next status-only pass.

If Herdr exposes workflow metadata, Herdres includes it in the marker, for example:

```text
🟡 Working
Working on 2/5 workflows; 1 active.
```

Controls:

```bash
HERDR_TELEGRAM_TOPICS_STATUS_MARKER=1
HERDR_TELEGRAM_TOPICS_STATUS_MARKER_DELETE_OLD=1
HERDR_TELEGRAM_TOPICS_MAX_STATUS_MARKERS=8
```

## Structured Turn Feed

The cleanest mode is `HERDR_TELEGRAM_TOPICS_TURN_FEED=1`. In this mode Herdres does not infer reports, questions, or updates from terminal text. It calls:

```bash
herdr pane turn <pane_id> --last --format json
```

Expected Herdr response:

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

`user_text` must be a submitted user message, never the visible input composer. `assistant_final_text` must be final assistant output only, without thinking, tool calls, shell output, or TUI chrome.

If a pane is waiting for owner input, Herdr can return a structured decision instead of a completed turn:

```json
{
  "available": true,
  "pane_id": "pane-1",
  "agent_session_id": "session-1",
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

Herdres renders `pending_decision` as a rich decision card with inline buttons in the mapped Telegram topic. Button taps route only to that pane and are bound to the Telegram message that created the buttons, so stale buttons from older messages are rejected. `send_text` is the exact text sent to Herdr for direct options; an empty `send_text` opens a ForceReply-style custom instruction prompt. Native Telegram polls are intentionally not part of the default owner-control flow.

Buttons are rendered by default only for structured `pending_decision` data and explicit `HERDRES_CHOICES_START` blocks. Herdres no longer enables inferred buttons from visible terminal choice screens by default. When structured turn data is unavailable and `HERDR_TELEGRAM_TOPICS_VISIBLE_READONLY_PROMPTS=1`, it may still show visible-screen prompts read-only in Telegram so you can see the question and option descriptions; those read-only prompts never create Telegram buttons, ForceReply state, or key-driving callbacks. Claude can show multi-question wizards with a later "Review your answers" / submit screen; key-driving those visible screens from Telegram can select the wrong question or default answer. If a pane only exposes choices through visible TUI text, answer in the Herdr pane directly; use `/send <text>` only for simple text prompts until the pane exposes a structured interaction contract.

The intended future contract for multi-question prompts is a normalized structured interaction, not visible-screen key driving:

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

Until Herdr or the local adapter exposes a semantic submit path for this shape, Herdres fails closed rather than sending native TUI keys for multi-question forms.
In this phase, `pending_interaction` is read-only even when `kind` is `single_question` and options include `value` or `send_text`; producers that want immediate Telegram buttons should continue to emit `pending_decision`.

If Herdr cannot provide structured turn data, it should return:

```json
{"available": false, "reason": "no_structured_turn_source"}
```

When turn feed is enabled and the endpoint is unavailable, incomplete, or empty, Herdres sends nothing and does not fall back to `pane read`. This keeps Herdr upgrade-safe: Herdres consumes an optional upstream CLI contract and does not require local Herdr patches.

For local deployments that need this before Herdr exposes the endpoint upstream, Herdres includes `herdr_turn_adapter.py`. It is a wrapper, not a Herdr patch:

```bash
install -Dm755 herdr_turn_adapter.py ~/.local/bin/herdr_turn_adapter.py
```

Configure only the Herdres service to use it:

```bash
HERDR_BIN=/home/smith/.local/bin/herdr_turn_adapter.py
HERDR_REAL_BIN=/home/smith/.local/bin/herdr
HERDR_TELEGRAM_TOPICS_TURN_FEED=1
```

The wrapper implements only `herdr pane turn <pane_id> --last --format json`. Every other command is delegated to `HERDR_REAL_BIN`. Current local extraction supports Codex session IDs reported by Herdr, and Claude when Herdr reports a Claude `agent_session_id`. If a pane has no session id, the wrapper returns `available=false` and Herdres sends nothing.

## Clean Report Markers

By default, automatic sync posts only bounded reports, real choice prompts, actionable questions, and blocked/error items. It does not auto-post unbounded `Summary:`, `Final:`, `Verification:`, or `What changed:` transcript text unless `HERDR_TELEGRAM_TOPICS_UNBOUNDED_REPORTS=1` is set.

For the cleanest pane output, have the pane emit an explicit bounded report:

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

`HERDRES_REPORT_TITLE:` is optional, but recommended. Without it, the first report line must be a short title such as `Deployment`, `Flight Recorder`, or `What changed:`. Malformed bounded reports are ignored instead of being posted as noisy Telegram updates.

Bounded reports can also use structured sections:

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

These render as Telegram rich headings, paragraphs, bordered/striped tables, checklists, collapsible details, and footers when rich messages are available. Content inside bounded reports bypasses the global TUI-noise filters; it is still sanitized for secrets and control characters.

Section aliases are accepted: `SHORT SUMMARY:` for `SUMMARY:`, `STATUS:` or `METRICS:` for `TABLE:`, `NEXT:` for `CHECKLIST:`, `RISKS:`, `PROOF:`, `LOGS:`, `COMMANDS:`, and `DIFF:` for collapsed details, and `META:` for `FOOTER:`.

For explicit choice buttons without relying on nearby question text:

```text
HERDRES_CHOICES_START
Question:
Choose the next action.
1. Run sync now
2. Show planned changes
HERDRES_CHOICES_END
```

Explicit choices are treated as agent-authored safe controls. Structured `pending_decision` data remains the supported automatic button path. Inferred legacy and visible-screen choice buttons are controlled separately and should remain off unless you have verified the pane's native prompt can be safely driven from Telegram.

## Useful Environment Variables

```bash
HERDR_BIN=herdr
HERDR_REAL_BIN=/home/smith/.local/bin/herdr
HERDR_TELEGRAM_TOPICS_STATE=~/.local/share/herdres/state.json
HERDR_TELEGRAM_TOPICS_LOCK=~/.local/share/herdres/sync.lock
HERDR_TELEGRAM_TOPICS_SCRIPT=~/.local/bin/herdres
HERDR_TELEGRAM_TOPICS_GENERAL_THREAD_ID=1
HERDR_TELEGRAM_TOPICS_PLUGIN_EVENTS=1
HERDR_TELEGRAM_TOPICS_EVENT_SETTLE_SECONDS=4
HERDR_TELEGRAM_TOPICS_EVENT_SETTLE_INTERVAL=0.75
HERDR_TELEGRAM_TOPICS_DUPLICATE_DELETE_LIMIT=12
HERDR_TELEGRAM_TOPICS_MAX_CREATES=3
HERDR_TELEGRAM_TOPICS_MAX_SENDS=8
HERDR_TELEGRAM_TOPICS_MAX_STATUS_MARKERS=8
HERDR_TELEGRAM_TOPICS_FEED_READ_LINES=140
HERDR_TELEGRAM_TOPICS_FEED_MAX_CHARS=9000
HERDR_TELEGRAM_TOPICS_TURN_FEED=1
HERDR_TELEGRAM_TOPICS_VISIBLE_CHOICE_BUTTONS=0
HERDR_TELEGRAM_TOPICS_VISIBLE_READONLY_PROMPTS=1
HERDR_TELEGRAM_TOPICS_LEGACY_CHOICES=0
HERDR_TELEGRAM_TOPICS_STRUCTURED_INTERACTIONS=1
HERDR_TELEGRAM_TOPICS_ACTIVE_PROMPT_TTL=900
HERDR_TELEGRAM_TOPICS_FINAL_REPLY_MAX_CHARS=16000
HERDR_TELEGRAM_TOPICS_FINAL_REPLY_MAX_LINES=140
HERDR_TELEGRAM_TOPICS_USER_PROMPT_MAX_CHARS=1200
HERDR_TELEGRAM_TOPICS_RICH_MESSAGES=1
HERDR_TELEGRAM_TOPICS_RICH_MAX_CHARS=14000
HERDR_TELEGRAM_TOPICS_LIVE_CARD=1
HERDR_TELEGRAM_TOPICS_STATUS_ICON=1
HERDR_TELEGRAM_TOPICS_STATUS_ICON_CACHE_TTL=86400
HERDR_TELEGRAM_TOPICS_STATUS_ICON_RETRY=300
HERDR_TELEGRAM_TOPICS_STATUS_MARKER_SUPPRESS_WHEN_ICON_OK=1
HERDR_TELEGRAM_TOPICS_STATUS_MARKER=1
HERDR_TELEGRAM_TOPICS_STATUS_MARKER_DELETE_OLD=1
HERDR_TELEGRAM_TOPICS_UNBOUNDED_REPORTS=0
HERDR_TELEGRAM_TOPICS_DRY_RUN=0
```

## Probe

To verify rich delivery against a scratch topic:

```bash
~/.local/bin/herdres probe --thread-id 123
```

The probe sends a rich message and deletes it if possible.

## Security Notes

- Bot token is read from environment or `~/.config/herdres/herdres.env`.
- Secrets are redacted from raw output and errors before posting.
- State stores pane/topic mapping and hashes, not bot tokens.
- Raw pane output is only posted via explicit `/raw`.
