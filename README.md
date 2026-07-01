<img width="2048" height="2048" alt="herdres" src="https://github.com/user-attachments/assets/d8324729-676a-49d8-9d24-800a8a411348" />

# Herdres

Rich Telegram forum-topic visibility and control for Herdr spaces and panes.

Herdres is a small stdlib-only Python bridge that maps each Herdr space to a Telegram forum topic. It posts actual pane traffic into that space topic, records the Telegram messages it sends as pane routing anchors, and can optionally create pane-root cards when that older thread-style UI is explicitly enabled. It can post explicit rich-message reports/questions/choices today, stream partial assistant output with Telegram draft methods when available, and switch to structured turn delivery when Herdr exposes a safe last-turn endpoint.

It does not patch Hermes or Herdr core files and routine sync uses no LLM calls.

**Operate it from an AI agent:** the [`skills/herdres/`](skills/herdres/SKILL.md) Agent Skill (agentskills.io `SKILL.md`) teaches a skills-compatible agent (Claude Code, Codex, …) to install, configure, and drive this bridge — a guided, self-verifying setup plus the full operator command surface. That skill is for *operating* herdres; [`AGENTS.md`](AGENTS.md) is for *contributing* to it.

**Set it up by prompting an agent (simplest):** paste this to a coding agent (Claude Code, Codex, …) — it fetches the self-contained skill ([`SKILL.md`](SKILL.md)) and walks you through install:

```text
Read the herdres operator skill at https://raw.githubusercontent.com/luminexord/herdres/main/SKILL.md
then set up herdres on this machine so I can control my Herdr agents from Telegram. Follow the
skill's Quick install. When you need the Telegram bot token, the forum-group chat id, or my user
id, ask me — never invent or reuse another app's token. Verify with a dry-run sync.
```

The repo-root [`SKILL.md`](SKILL.md) is a **self-contained, single-file** copy of that skill — tell any agent to install it directly (no folder needed):

```text
Install the herdres operator skill from this single file (do NOT install any skill named "herdr" — different project):
  mkdir -p ~/.codex/skills/herdres        # or ~/.claude/skills/herdres for Claude Code
  curl -fsSL https://raw.githubusercontent.com/luminexord/herdres/main/SKILL.md \
    -o ~/.codex/skills/herdres/SKILL.md
Verify: the file's frontmatter says `name: herdres`.
```

The packaged `skills/herdres/` (this file + `references/`) is the canonical, progressive-disclosure version; the root `SKILL.md` is the install-anywhere entrypoint.

## Install By Name (Marketplace)

Beyond copying files, herdres ships as a plugin you can add **by name** from a marketplace (closes the [#6](https://github.com/luminexord/herdres/issues/6) gap). The manifests bundle the canonical `skills/herdres/`.

Claude Code:

```text
/plugin marketplace add luminexord/herdres
/plugin install herdres@herdres
```

Codex (or any skills-compatible agent that reads `.codex-plugin/`): add the same repo as a marketplace, then install the `herdres` plugin. Both manifests pin the bundled skill via `"source": "./"` and `"skills": "./skills/"`, so the operator skill resolves from the plugin root.

## What It Does

- Creates or maintains one Telegram forum topic per Herdr space.
- Routes replies to actual pane messages, ForceReply prompts, and optional pane-root cards.
- Keeps the General topic free for your normal Hermes chat.
- Sends pane updates with Telegram Bot API 10.1 `sendRichMessage`.
- Streams partial assistant output with `sendMessageDraft` or `sendRichMessageDraft` when Telegram supports it, then persists the final answer with a normal message.
- Can opt in to `editMessageText(rich_message=...)` live status cards.
- Can opt in to compact bottom-of-topic status markers, such as `🟡 Working` or `🟢 Idle`.
- Shows clean reports, questions, blockers, numbered choices, and structured decision buttons.
- Optionally shows only the last submitted user instruction plus the final assistant reply when `herdr pane turn` is available.
- Keeps raw transcript and technical metadata behind explicit commands.
- Routes `/send`, `/keys`, plain-text owner replies, and choice-button replies only to the pane thread they came from.

## Requirements

- Python 3.11+
- `herdr` available on `PATH`, or set `HERDR_BIN`
- Telegram bot token
- A Telegram supergroup with forum topics enabled
- Bot must be an admin with **Manage Topics** and pin-message rights for pinned space status

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

## Updating herdres

Once installed, update in place with one command instead of hand-copying files:

```bash
herdres update
```

This updates from your local checkout on the `edge` channel (the default for now: it tracks `main`). It runs `git pull --ff-only`, backs up the current install, then atomically replaces the installed code **preserving your `~/.config/herdres/herdres.env`** (that file is never touched). It then restarts the user timer and the gateway — disabling and re-enabling the gateway so it releases its single Telegram `getUpdates` lease — and **rolls back automatically if anything fails**.

Other commands:

```bash
herdres update --check       # git fetch, then report current vs available version (equal on a plain checkout until you pull); change nothing
herdres update --rollback    # restore the previous install from the latest backup
herdres update --dry-run     # print the plan (source, version, files, services); change nothing
herdres version              # print the installed version
```

`herdres update` finds your source checkout from the `~/.local/share/herdres/source` marker that the installer writes; override it with `HERDRES_SRC` or `--repo <path>`.

### Release channels

```bash
herdres update --stable             # install the latest published GitHub Release
herdres update --version v0.3.0     # pin to a specific release tag (implies --stable)
herdres update --edge               # track main from your local checkout
```

- **`--stable`** downloads the latest GitHub **Release** (asset `herdres-<tag>.tar.gz`), **verifies its SHA256** against the published `herdres-<tag>.tar.gz.sha256`, then applies it through the same backup → atomic env-preserving replace → restart → rollback engine. This is the recommended path once releases exist. The repo is public, so no token is needed; point at a fork with `HERDRES_REPO=<owner>/<repo>`.
- **`--version vX.Y.Z`** pins to a specific release tag (and implies `--stable`); without it, `--stable` takes the newest release and does nothing if you are already at or ahead of it.
- **`--edge`** tracks `main` via your local checkout (the source marker above). This is the **default today**; we'll flip the default to `stable` in a follow-up once the first release is published.

### Cutting a release (maintainers)

```bash
# bump HERDRES_VERSION in herdres.py to X.Y.Z, commit, then:
git tag vX.Y.Z && git push origin vX.Y.Z
# CI runs the tests, builds herdres-vX.Y.Z.tar.gz + herdres-vX.Y.Z.tar.gz.sha256, and publishes the GitHub Release.
```

The tag must exactly match `HERDRES_VERSION` (`v0.3.0` ⟺ `0.3.0`) or the release workflow fails, so cut releases from plain semver tags.

## Pane Thread Commands

In the mapped space topic, reply to a routed pane message or optional pane-root card:

- `/report` or `/status` - resend latest clean rich report
- `/choices` - resend active choices with buttons
- `/raw [lines]` - show sanitized raw visible output
- `/debug` - show technical mapping details
- `/send <text>` - send instruction to this pane
- `/keys <keys>` - send explicit keys to this pane
- `/new codex|claude|kimi|omp|devin|<devin-model-id>` - split a new pane to the right in this space and launch that agent/model. Devin model IDs such as `glm-5.2`, `kimi-k2.7`, `gpt-5.5`, and `claude-opus-4.8` are run through the local Devin CLI.

Plain text replies under a routed pane message, ForceReply prompt, or optional pane-root message are forwarded directly to that pane without `/send`. Top-level owner messages in a shared space topic are also forwarded when that topic has exactly one live pane. Topics with multiple possible panes still fail closed with: `Reply inside a pane thread so I know which Herdr pane to control.` The General topic remains normal Hermes chat.

Long or multiline Telegram inputs are not pasted into the terminal directly. Herdres writes the exact owner message to `~/.local/share/herdres/inbound/<pane>/...txt` with owner-only permissions and submits a short instruction telling the pane to read that file. This avoids Herdr/TUI bracketed-paste collapse such as `[Pasted text #1 ...]` being sent as the actual instruction.

Controls:

```bash
HERDR_TELEGRAM_TOPICS_INPUT_FILE_CHARS=1200
HERDR_TELEGRAM_TOPICS_INPUT_FILE_LINES=6
HERDR_TELEGRAM_TOPICS_INPUT_FILE_MAX_CHARS=120000
```

### Legacy Devin GLM Seats

The automatic Devin GLM seat provisioner is legacy and default-off. Prefer `/new glm-5.2` for user-driven GLM Devin panes; manual `/new` panes are independent of auto-seat tracking.

If you still want one automatically provisioned Devin-backed GLM pane per Herdr space, enable it only where the Devin CLI is installed and logged in. Herdres uses normal public commands to split, rename, and run:

```bash
devin --model glm-5.2 --permission-mode dangerous
```

```bash
HERDR_TELEGRAM_TOPICS_DEVIN_GLM_SEAT=1
HERDR_TELEGRAM_TOPICS_DEVIN_GLM_SEAT_RECREATE=0
HERDR_TELEGRAM_TOPICS_DEVIN_GLM_MODEL=glm-5.2
HERDR_TELEGRAM_TOPICS_DEVIN_GLM_PERMISSION_MODE=dangerous
HERDR_TELEGRAM_TOPICS_DEVIN_GLM_LABEL=GLM Devin
HERDR_TELEGRAM_TOPICS_DEVIN_GLM_SEAT_MAX_PER_RUN=1
```

Leave `HERDR_TELEGRAM_TOPICS_DEVIN_GLM_SEAT=0` (the default) for no auto-start. With the default `HERDR_TELEGRAM_TOPICS_DEVIN_GLM_SEAT_RECREATE=0`, when you close/remove a provisioned GLM Devin pane Herdres respects that choice and does not recreate it. Set `HERDR_TELEGRAM_TOPICS_DEVIN_GLM_SEAT_RECREATE=1` to restore the legacy continuous recreation behavior. `HERDR_TELEGRAM_TOPICS_DEVIN_GLM_SEAT_MAX_PER_RUN` defaults to `1` so a first rollout does not create many panes at once. Failed starts back off for `HERDR_TELEGRAM_TOPICS_DEVIN_GLM_SEAT_ERROR_RETRY` seconds, default `300`.

For user-driven seats, use manual model choices:

```bash
/new glm-5.2       # opens GLM Devin through Devin
/new kimi-k2.7     # opens Kimi Devin through Devin
/new gpt-5.5       # opens GPT Devin through Devin
```

Labels ending in `Devin` are hosted through the Devin CLI. Telegram topic names and pinned status rows use the model label, not the generic `Devin` label.

Inbound pane-thread control is handled through the Hermes Telegram gateway, so Hermes must load the small bridge hook:

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

Herdres also includes a stdlib-only `getUpdates` daemon installed as
`herdres-gateway` from `herdres_gateway.py`. It can replace the Hermes inbound
bridge for pane-topic commands and callbacks, and when managed child bots are
enabled it polls the manager token plus one worker per child bot token. Set
`TELEGRAM_BOT_TOKEN` in `~/.config/herdres/herdres.env`; if an older deployment
only set `HERDRES_GATEWAY_BOT_TOKEN`, add the `TELEGRAM_BOT_TOKEN` line before
switching. For any one bot token, run either Hermes polling or
`herdres-gateway.service`, never both, because Telegram permits only one active
`getUpdates` consumer per bot token. The Linux systemd unit pins
`HERDRES_GATEWAY_RUNNER=subprocess` to preserve the prior subprocess execution
model; embedded mode can be enabled later after operational validation.

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

`herdres event` reads `HERDR_PLUGIN_CONTEXT_JSON` and `HERDR_PLUGIN_EVENT_JSON`, reconciles only the changed pane, ensures the space topic exists, and sends/edits the structured turn, stream draft, or pending decision if one is available. If `HERDR_TELEGRAM_TOPICS_PANE_ROOT_MESSAGES=1`, it also ensures the optional pane-root card exists. In plugin mode it does not parse terminal text. If structured turn data is unavailable, it sends nothing.

For completed/idle/blocked/error status events, `herdres event` waits briefly and rechecks the structured turn feed before giving up. This handles the normal race where Herdr fires `pane.agent_status_changed` just before the agent session file exposes the final completed turn. Tune this with `HERDR_TELEGRAM_TOPICS_EVENT_SETTLE_SECONDS` and `HERDR_TELEGRAM_TOPICS_EVENT_SETTLE_INTERVAL`.

Plugin events can be toggled independently of normal sync:

```bash
~/.local/bin/herdres plugin-disable
~/.local/bin/herdres plugin-enable
```

Keep the systemd timer enabled as a slower repair/reconcile path. It detects closed panes, repairs stale topic mappings, and covers missed plugin events.

## Duplicate Topic Cleanup

Herdres avoids duplicate topics by reusing a closed pane's existing mapping when Herdr changes a public pane handle, such as `w...-2` becoming `w...:p2`, as long as the state proves it is the same pane/session. Current state stores the Telegram forum topic on the Herdr space entry and stores optional pane-root messages on pane entries when that feature is enabled.

If duplicates already exist from a previous migration, inspect them first:

```bash
~/.local/bin/herdres cleanup-duplicates
```

Delete only Herdres-owned closed duplicates:

```bash
~/.local/bin/herdres cleanup-duplicates --delete
```

The cleanup only targets legacy topics that are both:

- mapped in Herdres state to a closed pane, and
- matched to a live pane by strong identity, such as the same agent session or equivalent old/new pane handle.

It never deletes the live space topic. Deleted state entries are archived under `deleted_duplicate_topics` for audit. Use `HERDR_TELEGRAM_TOPICS_DUPLICATE_DELETE_LIMIT` to cap deletions per run.

## Migration And Rollback Notes

Existing state files are migrated in place. The first live pane in a space seeds the space topic and sibling panes in that space keep their old topic id under `legacy_topic_id`. Pane-root messages are no longer created by default; set `HERDR_TELEGRAM_TOPICS_PANE_ROOT_MESSAGES=1` only if you want the older per-pane root-card anchors. Old pane-owned mappings are not eagerly deleted; inspect and remove closed duplicates with `cleanup-duplicates`.

Rollback is state-only: stop Herdres, restore the previous `HERDR_TELEGRAM_TOPICS_STATE` JSON backup, and restart the old Herdres version. Avoid running old and new versions at the same time because both can write the same state file.

## Rich Message Behavior

Herdres tries rich delivery first:

- `sendRichMessage` for reports, choices, notices, and detail prompts
- `sendRichMessageDraft` for rich streamed drafts when Telegram supports draft streaming
- `editMessageText` with `rich_message` for live cards
- `reply_parameters` for rich replies and ForceReply anchors
- `reply_markup` for inline choice buttons and ForceReply

Fallback policy:

- Missing rich endpoint: latch rich off and fall back to `sendMessage`
- Missing draft endpoint: latch only streaming drafts off and keep final rich messages enabled
- Bad rich HTML: fall back once to `sendMessage`
- Transient/network error: do not resend, to avoid duplicate posts
- Live-card edits retry naturally on the next timer tick

Pinned space status is enabled by default. In each active space topic, Herdres keeps one manager-bot message pinned and edits it to show only panes currently open in that space:

```text
Codex 🟢 | Kimi 🔴 | Claude 🟡
```

Status dots are green for idle/done, yellow for working, and red for blocked/error. The pinned message is created once per space topic, then edited in place on later syncs. Telegram requires the bot to have permission to pin messages in the group. Disable it with `HERDR_TELEGRAM_TOPICS_PINNED_STATUS=0`.

Live cards and pane-root cards are disabled by default. Enable `HERDR_TELEGRAM_TOPICS_LIVE_CARD=1` or `HERDR_TELEGRAM_TOPICS_PANE_ROOT_MESSAGES=1` only if you want those extra Telegram messages.

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

If Telegram does not offer a matching forum icon sticker, Herdres leaves the topic icon unchanged. Compact marker messages are available as an opt-in fallback.

Telegram does not move edited messages to the bottom of a topic. If you want a bottom-of-topic status breadcrumb, Herdres can post a compact status marker as the latest message in the space topic when no final reply, decision card, or stream draft was delivered for that pane:

```text
🟡 Working
Work is in progress.

🟢 Idle
No active work.
```

The marker is disabled by default. When enabled, it is sent only when the compact status changes. When possible, Herdres deletes the previous marker after posting the new one, so each pane keeps one current status marker near the bottom of the space topic. Final replies, stream drafts, and decision cards always take priority: if one is delivered in a run, Herdres skips that pane's marker until the next status-only pass.

If Herdr exposes workflow metadata, Herdres includes it in the marker, for example:

```text
🟡 Working
Working on 2/5 workflows; 1 active.
```

Controls:

```bash
HERDR_TELEGRAM_TOPICS_STATUS_MARKER=0
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

While a turn is still running, the endpoint or local adapter can return partial assistant text without completing the turn:

```json
{
  "available": true,
  "pane_id": "pane-1",
  "agent_session_id": "session-1",
  "turn_id": "turn-1",
  "complete": false,
  "user_text": "Diagnose why the bot froze.",
  "assistant_stream_text": "I am checking the process tree and logs...",
  "stream_revision": "a stable content revision"
}
```

Herdres treats `assistant_stream_text` as a draft preview only. It does not mark the turn complete and it still requires a later complete turn with `assistant_final_text` before the answer is persisted in Telegram.

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

Herdres renders `pending_decision` as a rich decision card with inline buttons in the pane thread inside the mapped space topic. Button taps route only to that pane and are bound to the Telegram message that created the buttons, so stale buttons from older messages are rejected. `send_text` is the exact text sent to Herdr for direct options; an empty `send_text` opens a ForceReply-style custom instruction prompt. Native Telegram polls are intentionally not part of the default owner-control flow.

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

When turn feed is enabled and the endpoint is unavailable, empty, or incomplete without a stream preview or decision, Herdres sends nothing and does not fall back to `pane read`. This keeps Herdr upgrade-safe: Herdres consumes an optional upstream CLI contract and does not require local Herdr patches.

## Streaming Drafts

Telegram supports streamed draft previews through `sendMessageDraft` and `sendRichMessageDraft`. Drafts are temporary previews; Herdres always sends the final assistant answer with a normal `sendMessage` or `sendRichMessage` when the turn completes.

Draft streaming is capability-probed at runtime. If Telegram rejects a draft method as unsupported, Herdres records `telegram.streaming_drafts.supported=no` and stops sending draft previews. This does not disable final rich messages.

Drafts are sent into the space topic with the same `message_thread_id` as the space. If pane-root messages are enabled, drafts also include `reply_parameters` pointing at the pane root message; otherwise the draft itself becomes the routed pane message. Repeated drafts for the same turn use a stable `draft_id` so Telegram clients animate updates instead of creating new messages.

On macOS, the launchd reconcile timer runs every 5 seconds so short assistant turns still have a sync window for stream updates before the final answer lands.

Controls:

```bash
HERDR_TELEGRAM_TOPICS_STREAMING=1
HERDR_TELEGRAM_TOPICS_STREAM_MIN_INTERVAL=2
HERDR_TELEGRAM_TOPICS_STREAM_MIN_CHARS=80
HERDR_TELEGRAM_TOPICS_MAX_DRAFTS=8
```

Dry-run mode recognizes draft methods, so this should be part of preflight QA:

```bash
HERDR_TELEGRAM_TOPICS_DRY_RUN=1 ~/.local/bin/herdres sync
```

## Managed Pane Bots

When `HERDR_TELEGRAM_TOPICS_MANAGED_BOTS=1`, Herdres posts managed-bot setup links in General only for AI types that currently have open panes and do not already have a stored child-bot token. Supported pane bots are Codex, Claude, Kimi, OMP, Devin, and GLM Devin. After Telegram sends the manager bot a `managed_bot` update, Herdres calls `getManagedBotToken`, stores the child token under `telegram.managed_bots`, and updates the child bot profile.

Telegram still requires each child bot to have access to the forum group. If a child token is registered but Telegram rejects pane messages from it, Herdres posts add-to-group buttons in General and does not send that pane traffic as the manager bot.

Pane output is sent by the matching child bot when configured. Add each child bot to the Telegram forum group so replies to that child bot are delivered to the gateway; if a child bot is not yet allowed to post, Herdres falls back to the manager bot for that send.

The standalone gateway runs one long-poll worker for the manager bot and one worker for each registered child bot. Telegram returns a long poll immediately when a message arrives, and each child bot is isolated from other bot-token waits or reconnect backoff:

```bash
HERDRES_GATEWAY_LONG_POLL_SECONDS=50
HERDRES_GATEWAY_NETWORK_ERROR_BACKOFF=0.5
HERDRES_GATEWAY_DISPATCH_WORKERS=8
HERDRES_GATEWAY_DISPATCH_QUEUE_LIMIT=128
HERDRES_GATEWAY_RUNNER=embedded
```

Optional profile images must be JPG files:

```bash
HERDR_TELEGRAM_TOPICS_MANAGED_BOTS=1
HERDR_TELEGRAM_TOPICS_MANAGED_BOT_CODEX_PHOTO=~/.config/herdres/managed-bots/codex.jpg
HERDR_TELEGRAM_TOPICS_MANAGED_BOT_CLAUDE_PHOTO=~/.config/herdres/managed-bots/claude.jpg
HERDR_TELEGRAM_TOPICS_MANAGED_BOT_KIMI_PHOTO=~/.config/herdres/managed-bots/kimi.jpg
HERDR_TELEGRAM_TOPICS_MANAGED_BOT_OMP_PHOTO=~/.config/herdres/managed-bots/omp.jpg
HERDR_TELEGRAM_TOPICS_MANAGED_BOT_DEVIN_PHOTO=~/.config/herdres/managed-bots/devin.jpg
HERDR_TELEGRAM_TOPICS_MANAGED_BOT_GLM_PHOTO=~/.config/herdres/managed-bots/glm.jpg
```

If a child bot was created outside Telegram's managed-bot request flow, assign it manually by token. For example, to assign a bot named Guremi to GLM:

```bash
HERDR_TELEGRAM_TOPICS_MANAGED_BOT_GLM_TOKEN=123456:...
HERDR_TELEGRAM_TOPICS_MANAGED_BOT_GLM_USERNAME=Guremi_bot
HERDR_TELEGRAM_TOPICS_MANAGED_BOT_GLM_NAME=Guremi
```

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
HERDRES_TENDWIRE_MODE=off
HERDRES_TENDWIRE_HYBRID=0
HERDRES_TENDWIRE_SNAPSHOT=0
HERDRES_TENDWIRE_BIN=tendwire
HERDRES_TENDWIRE_TIMEOUT_SECONDS=5
HERDRES_TENDWIRE_HERDR_TIMEOUT_SECONDS=1.0
# HERDRES_TENDWIRE_DATA_DIR=~/.local/share/tendwire
# HERDRES_TENDWIRE_DB_PATH=~/.local/share/tendwire/tendwire.sqlite3
# HERDRES_TENDWIRE_HOST_ID=
HERDRES_TENDWIRE_FALLBACK_HERDR=1
HERDRES_TENDWIRE_DIRECT_FALLBACK=0
# Leave unset for the mode default: source enables it, earlier modes do not.
# HERDRES_TENDWIRE_CONNECTOR_OUTBOX=0
HERDRES_TENDWIRE_CONNECTOR_NAME=attention
HERDRES_TENDWIRE_CONNECTOR_LIMIT=3
HERDRES_TENDWIRE_CONNECTOR_LEASE_SECONDS=60
HERDRES_TENDWIRE_CONNECTOR_FAILURE_DELAY_SECONDS=60
HERDR_TELEGRAM_TOPICS_MAX_CREATES=3
HERDR_TELEGRAM_TOPICS_MAX_SENDS=8
HERDR_TELEGRAM_TOPICS_MAX_STATUS_MARKERS=8
HERDR_TELEGRAM_TOPICS_FEED_READ_LINES=140
HERDR_TELEGRAM_TOPICS_FEED_MAX_CHARS=9000
HERDR_TELEGRAM_TOPICS_STREAMING=1
HERDR_TELEGRAM_TOPICS_STREAM_MIN_INTERVAL=2
HERDR_TELEGRAM_TOPICS_STREAM_MIN_CHARS=80
HERDR_TELEGRAM_TOPICS_MAX_DRAFTS=8
HERDR_TELEGRAM_TOPICS_MANAGED_BOTS=1
HERDR_TELEGRAM_TOPICS_MANAGED_BOT_CODEX_PHOTO=
HERDR_TELEGRAM_TOPICS_MANAGED_BOT_CLAUDE_PHOTO=
HERDR_TELEGRAM_TOPICS_MANAGED_BOT_KIMI_PHOTO=
HERDR_TELEGRAM_TOPICS_MANAGED_BOT_OMP_PHOTO=
HERDR_TELEGRAM_TOPICS_MANAGED_BOT_DEVIN_PHOTO=
HERDR_TELEGRAM_TOPICS_MANAGED_BOT_GLM_PHOTO=
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
HERDR_TELEGRAM_TOPICS_PANE_ROOT_MESSAGES=0
HERDR_TELEGRAM_TOPICS_PINNED_STATUS=1
HERDR_TELEGRAM_TOPICS_NEW_PANE_CODEX_COMMAND=codex
HERDR_TELEGRAM_TOPICS_NEW_PANE_CLAUDE_COMMAND=claude
HERDR_TELEGRAM_TOPICS_NEW_PANE_KIMI_COMMAND=kimi
HERDR_TELEGRAM_TOPICS_NEW_PANE_OMP_COMMAND=omp
HERDR_TELEGRAM_TOPICS_NEW_PANE_DEVIN_COMMAND=devin
HERDR_TELEGRAM_TOPICS_DEVIN_GLM_SEAT=0
HERDR_TELEGRAM_TOPICS_DEVIN_GLM_SEAT_RECREATE=0
HERDR_TELEGRAM_TOPICS_DEVIN_GLM_MODEL=glm-5.2
HERDR_TELEGRAM_TOPICS_DEVIN_GLM_PERMISSION_MODE=dangerous
HERDR_TELEGRAM_TOPICS_DEVIN_GLM_LABEL=GLM Devin
HERDR_TELEGRAM_TOPICS_DEVIN_GLM_SEAT_MAX_PER_RUN=1
HERDR_TELEGRAM_TOPICS_DEVIN_GLM_SEAT_PENDING_TTL=1800
HERDR_TELEGRAM_TOPICS_DEVIN_GLM_SEAT_ERROR_RETRY=300
HERDR_TELEGRAM_TOPICS_DEVIN_KIMI_LABEL=Kimi Devin
HERDR_TELEGRAM_TOPICS_LIVE_CARD=0
HERDR_TELEGRAM_TOPICS_STATUS_ICON=1
HERDR_TELEGRAM_TOPICS_STATUS_ICON_CACHE_TTL=86400
HERDR_TELEGRAM_TOPICS_STATUS_ICON_RETRY=300
HERDR_TELEGRAM_TOPICS_STATUS_MARKER_SUPPRESS_WHEN_ICON_OK=1
HERDR_TELEGRAM_TOPICS_STATUS_MARKER=0
HERDR_TELEGRAM_TOPICS_STATUS_MARKER_DELETE_OLD=1
HERDR_TELEGRAM_TOPICS_UNBOUNDED_REPORTS=0
HERDR_TELEGRAM_TOPICS_DRY_RUN=0
```

Tendwire modes:

| `HERDRES_TENDWIRE_MODE` | Behavior |
| --- | --- |
| `off` | Default. Disables Tendwire calls and enrichment. |
| `enrich` | Enables the current safe enrichment path. Herdres still reads real Herdr panes directly with `pane_list()`, preserves real `pane_id` values, and Tendwire only adds metadata/status to unambiguous real-pane matches. |
| `commands` | Keeps real-pane enrichment, then routes normal Telegram text for Tendwire-enriched entries through `tendwire command --json` using the worker id and fingerprint. Entries without Tendwire metadata still use the legacy direct Herdr send path. |
| `source-read` | Uses the Tendwire public snapshot as the pane inventory instead of `pane_list()`, creates read-only worker entries (`entry_type=worker`, `worker_id`, `worker_fingerprint`) without inventing `tendwire:<worker>` pane ids, skips Herdr pane read/feed/turn inventory helpers for those entries, and routes text only through `tendwire command --json` when worker id/fingerprint metadata is present. Attachments, raw reads, picker callbacks, stale choices, `/new`, `/send!`, `/keys`, and direct Herdr fallback for source entries fail closed. |
| `source` | Full source mode. Uses the same Tendwire snapshot inventory and worker entries as `source-read`, routes normal Telegram text/buttons through Tendwire, drains the Tendwire connector outbox by default, and does not use direct Herdr calls for normal Telegram behavior. Legacy direct mode remains available only by switching `HERDRES_TENDWIRE_MODE=off`. |

Invalid mode values warn and fall back to `off`; they never enable Tendwire behavior. When `HERDRES_TENDWIRE_MODE` is unset, legacy `HERDRES_TENDWIRE_HYBRID=1` or `HERDRES_TENDWIRE_SNAPSHOT=1` aliases to `enrich`. Those legacy names remain compatibility aliases, not the public Tendwire mental model.

In `commands` mode and higher, if an enriched real pane has a worker id and fingerprint, Herdres does not direct-send the instruction to Herdr after selecting Tendwire command routing. Stale/ambiguous Tendwire command failures and malformed Tendwire CLI responses fail closed with a safe Telegram note. If an entry has partial Tendwire metadata, such as a worker id without a fingerprint, Herdres also fails closed. `HERDRES_TENDWIRE_DIRECT_FALLBACK=1` is an emergency operator override that allows direct Herdr fallback; it defaults off.

In `source-read` and `source` modes, the same emergency fallback does not apply to Tendwire worker entries. Source entries are not real Herdr pane ids; missing worker metadata, failed Tendwire sends, attachments, `/raw`, `/read`, stale choices, and picker callbacks that cannot be routed through Tendwire all fail closed instead of calling Herdr directly. Existing legacy `tendwire:<worker>` pseudo-pane records are pruned when source inventory mode is not active.

When Tendwire reports degraded or unavailable backend health, or when the
snapshot command fails while source entries already exist, Herdres preserves the
existing source worker topics from its local state for that sync instead of
marking them closed from incomplete inventory. Fresh healthy snapshots clear
that preservation state.

`/send!` is not routed through Tendwire in this phase. For command-mode enriched entries and source inventory modes it fails closed because Tendwire interrupt semantics are not represented here yet; use `/send` or interrupt directly in Herdr when running outside source mode. Non-enriched entries and `enrich` mode keep the existing `/send!` Herdr interrupt path.

Tendwire config:

- `HERDRES_TENDWIRE_BIN` is the Tendwire command base. Herdres parses it with shell-style quoting; when the executable token is path-like, `~` and environment variables are expanded while extra arguments are preserved.
- `HERDRES_TENDWIRE_TIMEOUT_SECONDS` is Herdres' outer wall-clock timeout for the Tendwire CLI subprocess.
- `HERDRES_TENDWIRE_HERDR_TIMEOUT_SECONDS` defaults to `1.0` and is passed to Tendwire as `TENDWIRE_HERDR_TIMEOUT_SECONDS`; it should stay below the outer timeout.
- `TENDWIRE_HERDR_BIN` is set for Tendwire from `HERDR_REAL_BIN` when present, otherwise `HERDR_BIN`, otherwise `herdr`.
- `HERDRES_TENDWIRE_DATA_DIR`, `HERDRES_TENDWIRE_DB_PATH`, and `HERDRES_TENDWIRE_HOST_ID` are optional Herdres-controlled overrides for `TENDWIRE_DATA_DIR`, `TENDWIRE_DB_PATH`, and `TENDWIRE_HOST_ID`. Path values expand `~` and environment variables. Leave them unset to let Tendwire use its own defaults.
- `HERDRES_TENDWIRE_DIRECT_FALLBACK=1` explicitly permits direct Herdr fallback after Tendwire command routing fails or metadata is partial. Leave it at `0` for normal fail-closed command ownership.

Tendwire connector outbox:

- `HERDRES_TENDWIRE_CONNECTOR_OUTBOX` controls the neutral connector drain during `herdres sync`. When unset, it defaults on only in `HERDRES_TENDWIRE_MODE=source` and remains off in earlier modes. Set `0` to disable it explicitly or `1` to enable it in `source-read` during staged testing.
- `HERDRES_TENDWIRE_CONNECTOR_NAME=attention` selects the Tendwire connector queue. The current consumer posts sanitized attention lifecycle notices to the configured General topic.
- `HERDRES_TENDWIRE_CONNECTOR_LIMIT`, `HERDRES_TENDWIRE_CONNECTOR_LEASE_SECONDS`, and `HERDRES_TENDWIRE_CONNECTOR_FAILURE_DELAY_SECONDS` bound per-sync leases and retries.

The connector drain uses `tendwire connector poll/ack/fail/defer` through the same `HERDRES_TENDWIRE_BIN` command base and optional Tendwire env overrides. It does not require or enable `source-read`/`source` mode, does not create pseudo panes, does not persist opaque refs in Herdres state, and sends only public-safe ack/fail data back to Tendwire. If Telegram is not configured, it does not lease Tendwire work.

Inspect the resolved Herdres-to-Tendwire config without touching Telegram state:

```bash
herdres tendwire config
herdres tendwire outbox --limit 1
```

## Tendwire Source Mode Rollout

Use this path when Tendwire should be the source/control plane and Herdres should
only handle Telegram mapping, formatting, replies, buttons, rate limits, and
delivery bookkeeping.

1. Run Herdr normally and start a Tendwire daemon with the socket backend and a
   persistent store:

   ```bash
   install -d ~/.local/share/tendwire ~/.config/systemd/user

   cat > ~/.config/systemd/user/tendwired.service <<'EOF'
   [Unit]
   Description=Tendwire daemon
   Wants=network-online.target
   After=network-online.target

   [Service]
   Type=simple
   Environment=TENDWIRE_HERDR_BACKEND=socket
   Environment=TENDWIRE_DB_PATH=%h/.local/share/tendwire/tendwire.sqlite3
   ExecStart=%h/.local/bin/tendwire daemon --db-path %h/.local/share/tendwire/tendwire.sqlite3
   Restart=always
   RestartSec=5s

   [Install]
   WantedBy=default.target
   EOF

   systemctl --user daemon-reload
   systemctl --user enable --now tendwired.service
   ```

   A non-systemd host can run the same command under launchd, supervisord, or a
   shell supervisor:

   ```bash
   TENDWIRE_HERDR_BACKEND=socket \
     tendwire daemon --db-path ~/.local/share/tendwire/tendwire.sqlite3
   ```

2. Verify Tendwire before moving Telegram traffic:

   ```bash
   tendwire doctor --json
   tendwire snapshot --json --store --db-path ~/.local/share/tendwire/tendwire.sqlite3
   tendwire store status --db-path ~/.local/share/tendwire/tendwire.sqlite3
   ```

   Healthy snapshots can be empty during startup, but degraded/unavailable backend
   health means Herdres source-read/source will preserve the previous Telegram
   worker inventory instead of closing topics from incomplete data.

3. Run the Herdres Telegram services. The timer performs outbound sync and
   connector draining; the gateway owns Telegram `getUpdates` for inbound replies
   and buttons:

   ```bash
   cp systemd/user/herdres.* ~/.config/systemd/user/
   systemctl --user daemon-reload
   systemctl --user enable --now herdres.timer herdres-gateway.service
   ```

   Do not run another long-poll consumer for the same bot token while
   `herdres-gateway.service` is active.

4. Roll out modes one step at a time in `~/.config/herdres/herdres.env`:

   ```bash
   HERDRES_TENDWIRE_DB_PATH=~/.local/share/tendwire/tendwire.sqlite3
   HERDRES_TENDWIRE_MODE=enrich
   ```

   - `enrich` keeps legacy direct Herdr sends and only adds Tendwire metadata.
   - `commands` routes enriched Telegram text/buttons through Tendwire
     `command.submit`; un-enriched legacy entries still use direct Herdr.
   - `source-read` renders inventory from Tendwire snapshots, uses Tendwire worker
     ids/fingerprints instead of pane ids, and keeps the connector outbox off
     unless `HERDRES_TENDWIRE_CONNECTOR_OUTBOX=1` is set for staging.
   - `source` is the normal final mode: direct Herdr calls are disabled for normal
     Telegram behavior, `/keys` is disabled by default, and the Tendwire connector
     outbox drains unless explicitly set to `0`.

   After changing modes:

   ```bash
   HERDR_TELEGRAM_TOPICS_DRY_RUN=1 herdres sync
   herdres tendwire config
   systemctl --user restart herdres.timer herdres-gateway.service
   ```

5. Roll back without losing Telegram state by changing only the mode and
   restarting Herdres:

   ```bash
   HERDRES_TENDWIRE_MODE=enrich   # keep Tendwire metadata, restore legacy sends
   # or:
   HERDRES_TENDWIRE_MODE=off      # pure legacy Herdres/Herdr behavior
   HERDRES_TENDWIRE_CONNECTOR_OUTBOX=0
   systemctl --user restart herdres.timer herdres-gateway.service
   ```

   `off` is the only supported legacy direct mode. Do not keep source/source-read
   enabled while expecting Herdres to call `pane_list`, `pane_turn`,
   `send_to_pane`, `herdr pane send-keys`, or `herdr pane read` for normal
   Telegram behavior.

## Probe

To verify rich delivery against a scratch topic:

```bash
~/.local/bin/herdres probe --thread-id 123
```

The probe sends a rich message and deletes it if possible.

## Security Notes

- Bot token is read from environment or `~/.config/herdres/herdres.env`.
- Secrets are redacted from raw output and errors before posting.
- State stores space topic ids, optional pane root ids, route indexes, and hashes, not bot tokens.
- Raw pane output is only posted via explicit `/raw`.

## macOS Setup

macOS has no user systemd, and the inbound design assumes Hermes already
long-polls the same bot. When Herdres owns its own Telegram bot (no other
`getUpdates` consumer), use the bundled macOS path instead of the systemd timer
and the Hermes bridge:

```bash
./install-macos.sh
$EDITOR ~/.config/herdres/herdres.env
herdr plugin link ~/.local/share/herdres/herdres-plugin
launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/com.gaijinjoe.herdres.plist
launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/com.gaijinjoe.herdres-gateway.plist
```

This provides:

- `com.gaijinjoe.herdres` — launchd agent running `herdres sync` every 5s, the
  equivalent of the systemd reconcile timer (closed-pane detection, stale-mapping
  repair, missed-event coverage).
- `com.gaijinjoe.herdres-gateway` — launchd agent running `herdres_gateway.py`, a
  stdlib-only long-poll loop that replaces the Hermes getUpdates bridge. It
  dispatches mapped space-topic pane-thread messages/callbacks to `herdres command` /
  `herdres callback` using the same JSON contract as `herdr_topic_bridge.py`.

Notes:

- The scripts are installed with their shebang pinned to a Python >= 3.11 found at
  install time, since the system `python3` on macOS may be older.
- Outbound `sync` / `event` only *send*; they never consume `getUpdates`, so they
  run safely alongside the gateway. Do not also run a Hermes poller on the same
  bot — Telegram allows only one `getUpdates` consumer per token.
- When managed child bots are configured, the gateway runs one long-poll worker
  per bot token so one token's wait or reconnect backoff does not delay another.
  Tune `HERDRES_GATEWAY_LONG_POLL_SECONDS` if you need a different long-poll
  timeout. Handler work is dispatched through a small worker pool so slow Herdr
  command processing does not stop the bot token from polling for the next
  update. Inbound commands run through an embedded Herdres module by default;
  set `HERDRES_GATEWAY_RUNNER=subprocess` only when you need to debug the older
  cold-process path.
- The gateway drains any backlog on first start so it never replays historical
  messages as live pane commands.
- Top-level inbound plain-text -> pane is still gated by
  `telegram.implicit_send_enabled` except in single-live-pane space topics. In
  multi-pane topics, reply to a pane message or use `/send <text>`.
