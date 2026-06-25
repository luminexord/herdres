# herdres Command Reference

Complete command reference for operating herdres: the `herdres` **CLI subcommands** (run on the host) and the **Telegram pane-thread commands** (typed inside a Herdr pane's forum topic).

For install, service enabling, and linking the Herdr plugin, see **SETUP.md**. For the per-space vs per-agent topic model, see **TOPICS.md**. For safety/fail-closed rules, see **SAFETY.md**.

---

## 1. CLI subcommands

Invoke as `herdres <subcommand>` (the actual binary lands wherever SETUP.md placed it; usually invoked through systemd/launchd timers, the Herdr plugin, or the Telegram gateway). Every subcommand prints a single JSON object to stdout and exits `0` on success, `1` on error, or `75` when rate-limited.

| Subcommand | Purpose |
|---|---|
| `setup` | Interactive credential wizard (run **once**, by a human in a terminal). Prompts for the bot token (no echo), forum chat ID, and allowed user IDs; validates each; runs preflight; then writes `~/.config/herdres/herdres.env` at mode `0600`, preserving any other keys. **Refuses** to run unattended unless you pass `--bot-token`/`--chat-id`/`--allowed-users`, and **never** silently reuses another app's (e.g. Hermes's) bot token — that needs `--reuse-hermes-token` or a typed `reuse` confirmation. This is the enforcing credential gate (see SAFETY.md). |
| `sync` | One reconciliation pass: create/update forum topics, post pending pane reports/cards. Non-blocking lock (skips if another run holds it). Driven by the timer. |
| `event` | Process a single Herdr plugin event (a pane changed). Blocking lock. This is the path the Herdr plugin calls on each agent turn. |
| `plugin-enable` | Flip the stored flag so `event` runs actually do work. Run once after linking the Herdr plugin. |
| `plugin-disable` | Set that flag off — `event` invocations become no-ops without unlinking the plugin. |
| `cleanup-duplicates` | **Report** duplicate forum topics (same pane mapped twice). Read-only by default. |
| `cleanup-duplicates --delete` | Actually delete the duplicate topics (rate/quantity limited). Run the read-only form first to preview. |
| `command` | Handle one inbound Telegram message. **Reads a JSON payload on stdin.** Called by the gateway; not for manual use. |
| `callback` | Handle one inbound Telegram callback (inline-button tap). **Reads a JSON payload on stdin.** Gateway-only. |
| `managed-bot` | Register/refresh a managed child bot token from a Telegram `managed_bot` update. **Reads a JSON payload on stdin.** Gateway-only. See MANAGED_BOTS.md. |
| `probe` | Send a throwaway "Rich Probe" message to verify rich-message delivery, then delete it. Useful for diagnosing chat/topic wiring. |
| `probe --thread-id <id>` | Same probe, but target a specific forum topic (thread) id instead of the General thread. |
| `speech check` | **Read-only** preflight for the local voice feature (issue #4): is `sherpa-onnx` importable, `ffmpeg` present, the STT model downloaded, the flags on. |
| `speech install` | Download + SHA256-verify the local STT model (parakeet) into `~/.local/share/herdres/speech-models`, and report what else is needed (`pip install --user sherpa-onnx`, `ffmpeg`). Opt-in; heavy deps are not installed by default. |

**stdin-JSON subcommands** — `command`, `callback`, and `managed-bot` each read a JSON object from stdin (`{}` if empty). These are wired to the Telegram gateway, which spawns a fresh `herdres` per update; you normally never run them by hand.

**Concurrency** — `sync` takes a non-blocking lock (a second concurrent `sync` simply reports `another sync is running` and exits cleanly). `event`, the two `plugin-*` commands, `cleanup-duplicates`, the stdin-JSON handlers, and `probe` take a blocking lock and wait their turn.

### Typical operator runs

```bash
herdres plugin-enable                 # once, after linking the Herdr plugin
herdres sync                          # force a reconciliation now
herdres cleanup-duplicates            # preview duplicate topics
herdres cleanup-duplicates --delete   # remove them
herdres probe --thread-id 42          # verify delivery into topic 42
```

For a one-shot backfill that creates more topics than the per-run cap, raise the cap for that single run:

```bash
HERDR_TELEGRAM_TOPICS_MAX_CREATES=12 herdres sync
```

---

## 2. Telegram pane-thread commands

These are typed **inside a Herdr pane's forum topic** (the thread herdres created for that agent/space). Only a configured owner (`TELEGRAM_ALLOWED_USERS`, or the stored owner ids) is obeyed; messages from non-owners, bots, forwards, or edits are ignored. The thread must map to a **live** pane — a closed pane replies `This topic is mapped to a closed or unavailable Herdr pane.`

An agent runs **one turn at a time**. So a command issued while the agent is `working` can only **queue** (wait for the turn to end) or **interrupt** (Esc the turn, then deliver) — there is no way to inject mid-turn. See the idle-vs-working column below.

### 2.1 Command table

| Command | Idle agent | Working (busy) agent |
|---|---|---|
| `/send <text>` | Delivered and submitted immediately | **Queued** — runs at the next turn boundary (reply: *"Queued — the agent is busy…"*) |
| `/send! <text>` (aliases `/interrupt`, `/isend`) | Delivered now (no Esc — nothing to interrupt) | **Interrupts** the turn (sends Esc, waits for idle), then delivers now |
| `/keys <keys>` | Sends raw key names to the pane | Same — sends keys regardless of status |
| `/status`, `/report` | Resend the latest clean report / question for this pane | Same |
| `/raw [lines]` | Sanitized raw visible pane output (default 80 lines, max 160) | Same |
| `/choices` | Resend the active decision prompt / inline buttons | Same |
| `/skills` (alias `/commands`) | List this pane agent's skills/slash-commands as tappable buttons; a tap runs one on the pane | Same (the chosen command queues if the agent is busy) |
| `/new <kind>` | Split a new pane in this space and launch an agent | Same (operates on the space, not the busy turn) |
| `/agents` | Inline picker to choose which agent this topic addresses (per-space mode only) | Same |
| `/voice shared\|per_agent` | Switch this space's Telegram voice (per-space, reversible) | Same |
| `/debug` | Technical mapping details (pane id, topic, route) | Same |
| `/help` (alias `/start`) | List these pane commands | Same |

### 2.2 Sending instructions

**`/send <text>`** — Forward `<text>` to this pane as the agent's next instruction. To an **idle** agent it is typed and submitted right away. To a **busy** agent it **queues** (it is *not* lost) and you get a `Queued — the agent is busy; your message will run when the current turn finishes.` note. Empty `/send` replies with the usage hint.

**`/send! <text>`** (aliases **`/interrupt`**, **`/isend`**) — Interrupt-and-send. If the agent is `working`, herdres sends `Esc` to halt the current turn, waits for it to go idle, then delivers `<text>` now. On an already-idle agent it just delivers (no Esc — Esc on an idle Codex pane would pop its "edit previous message" recall preview). On success: `⏹️ Interrupted the current turn and sent your message.` If the Esc didn't fully stop the turn, the message queues instead and the reply says so.

```
/send run the test suite and report failures
/send! stop — you're editing the wrong file
```

**`/keys <keys>`** — Send explicit key names to the pane terminal (parsed with shell-style quoting). Use for raw control input the agent CLI expects.

```
/keys enter
/keys ctrl-c
/keys escape enter
```

Empty `/keys` replies with `Usage: /keys <key> [key ...]`; a parse error reports it; a failure surfaces the Herdr error.

### 2.3 Reading pane state

**`/status`** and **`/report`** are equivalent: they resend the **latest clean report or pending question** for this pane (the same structured card the turn feed produces — see TURN_FEED.md), with any decision buttons re-attached.

**`/raw [lines]`** — Dump the **sanitized raw visible output** of the pane. `lines` defaults to **80** and is clamped to **1–160**. Use when the clean report isn't enough and you need to see exactly what's on screen.

```
/raw
/raw 40
```

**`/choices`** — Resend the pane's **active decision prompt** (question text plus its inline-button options). If the prompt is gone or no longer safe to answer from Telegram, you get `No active choices for this pane.` Tap a button, or reply to the prompt to send a free-text detail (see SAFETY.md for the fail-closed rules around prompts).

**`/skills`** (alias **`/commands`**) — List the slash-commands and skills the **underlying agent** can run, as tappable inline buttons, discovered from that pane's runtime on the herdres host: for **Claude Code** the user-level `~/.claude/commands` + `~/.claude/skills`, the pane's project `<cwd>/.claude/…`, and enabled plugins; for **Codex** the `~/.codex/prompts` (and skills). Tapping a button forwards the invocation to the pane — `/<name>` for Claude (and Codex prompts), or a best-effort `Use the <name> skill.` for Codex skills (marked “(skill)”; Codex has no slash-command parser). Reuses the same inline-button machinery as `/choices`, so a tap on a busy agent **queues** the command. Refuses while an agent decision prompt is already active in the topic (answer or dismiss it first). Shows the first 12; only the pane agent's *own* runtime is read (panes are local to herdres).

**`/debug`** — Show technical mapping details for troubleshooting: which pane id and topic this thread is bound to, and route metadata. Use this to confirm a topic is wired to the pane you expect.

### 2.4 Creating a new pane

**`/new <kind>`** — Split a new pane to the right of this space's anchor pane (inheriting its working directory) and launch an agent in it. Valid `kind` values:

| `kind` | Launches |
|---|---|
| `codex` | Codex |
| `claude` | Claude |
| `kimi` | Kimi |
| `omp` | OMP |
| `devin` | Devin |

Aliases are accepted (e.g. `gpt`/`openai` → codex, `anthropic` → claude, `moonshot` → kimi, `cognition` → devin). The launch command per kind is configurable via `HERDR_TELEGRAM_TOPICS_NEW_PANE_<KIND>_COMMAND` (see SETUP.md / `.env.example`). An unknown kind replies `Usage: /new codex|claude|kimi|omp|devin`. On success: `Started <Label> in pane <id>.`

```
/new claude
/new codex
```

### 2.5 Addressing an agent & space voice (`/agents`, `/voice`)

These exist because, by default, a **space speaks with one voice**: in per-space topic mode several agents share one topic and one manager bot. See MANAGED_BOTS.md and TOPICS.md.

**`/agents`** — Show an inline picker of the live agents in this topic. Tapping one sets a **per-user, per-space active pane** with a TTL (`HERDR_TELEGRAM_TOPICS_ACTIVE_PANE_TTL`, default **~600s / 10 min**); for that window, **all** subsequent commands you send in this topic route to that agent — no reply or `@` needed. Replying to a specific pane message and a topic with exactly one live pane still take priority over the active pane. Calling `/agents` again re-shows the picker; the active pane expires silently and does not persist across state resets. `/agents` exists **only in per-space topic mode**; on a single-agent (per-agent) topic it replies `Only one agent here (<label>) — your messages already route to it.` It is also suppressed when the topic has no resolvable space.

**`/voice shared|per_agent`** — Set this **space's** Telegram voice. `shared` (default) means one manager bot speaks for every agent in the space; `per_agent` means each agent **type** gets its own managed child bot (if available — see MANAGED_BOTS.md). `/voice` with no argument prints the current mode. The setting is per-space, persistent across resets, and reversible. It applies wherever the topic resolves to a space (including per-agent topic mode); it only no-ops if no space record can be resolved.

### 2.6 Forwarded agent CLI commands

Any `/command` that is **not** one of herdres' own meta-commands above is **forwarded to the pane as-is**, so the agent CLI's own slash-commands reach it intact:

```
/goal ship the release notes
/clear
/compact
/model opus
```

herdres only strips a trailing `@botname` (which Telegram adds to commands in groups) before forwarding — `/goal@herdr_codex_bot …` becomes `/goal …`. The agent then handles the command itself. herdres does not validate these against the agent; an unknown one is the agent's problem, not herdres'.

### 2.7 Plain text (no slash)

Plain text (no leading `/`) typed in a pane topic is handled by context:

- If the agent just asked a **detail question** and you are replying to that prompt, your text is sent as the detail answer.
- If **implicit send** is enabled (`implicit_send_enabled` in state), if you addressed a managed pane bot, if you **reply** to a pane message, if you have a live **active pane** from `/agents`, or if the topic maps to exactly one live pane, your text is **forwarded to the pane** (same idle-vs-working behavior as `/send`).
- Otherwise herdres replies: *"Not sure which agent this is for. Reply to a message in that agent's thread, send /agents to pick one (replies then route there), or use /send <text>."* (so a stray message isn't silently injected).

Plain text follows the same queue/interrupt rules as `/send` — to a busy agent it queues.

### 2.7a Voice notes → transcribed to the pane (issue #4, opt-in)

Send a **Telegram voice message** in a pane topic and herdres transcribes it **locally** (NVIDIA Parakeet via sherpa-onnx — no cloud) and forwards the text to the pane, same routing as plain text ("agents do better the more you tell them — too much to type"). It echoes **🎙️ Heard: …** back so you can see/correct what it heard.

**Opt-in, machine-agnostic, fail-open** (any speech failure degrades to text — it never breaks routing):
1. `herdres speech install` (downloads the model) + `pip install --user sherpa-onnx` + ensure `ffmpeg` is present — verify with `herdres speech check`.
2. Set `HERDR_TELEGRAM_TOPICS_SPEECH_INPUT=1`.

A later phase adds the reverse — the agent **speaks its reply back** as a voice message (`HERDR_TELEGRAM_TOPICS_SPEECH_REPLIES`). See `.env.example` for all `HERDR_TELEGRAM_TOPICS_SPEECH_*` knobs.

### 2.8 Long / multiline input → inbound file

Both forwarded text and forwarded slash-commands have a length guard. When the forwarded payload is **long or multiline** — at or above **~1200 chars** (`HERDR_TELEGRAM_TOPICS_INPUT_FILE_CHARS`) **or 6+ lines** (`HERDR_TELEGRAM_TOPICS_INPUT_FILE_LINES`) — herdres does **not** type the whole blob into the pane (a long paste would collapse into an opaque `[Pasted text]` block and a slash-command token would be lost). Instead it:

1. Writes the full content to an **inbound file** under the herdres state dir (`inbound/<pane>/<timestamp>-<hash>.txt`, mode `0600`).
2. Tells the pane to **read that file** and use its complete contents as the instruction.

For a **forwarded slash-command** (e.g. a very long `/goal …`), the short command token stays on its own line and only the bulk argument is staged to the file, so the agent still registers the slash-command:

> `/goal The full input for this command is saved at <path> — read that file and use its complete contents as the goal input, then proceed.`

For plain forwarded text, the instruction is a "Telegram topic message received… read that file… then respond to the owner." prompt with a short preview and the line/char counts. Content above `HERDR_TELEGRAM_TOPICS_INPUT_FILE_MAX_CHARS` (default 120000) is truncated locally with a marker. Pathologically long command references are refused rather than silently dropping the command.

This means: you can paste a long brief or a multi-paragraph goal into a pane topic and the agent receives the *full* text via the file, not a truncated preview.

---

## 3. Quick reference

| You want to… | Do this |
|---|---|
| Force a reconciliation now | `herdres sync` |
| Enable the plugin event path | `herdres plugin-enable` |
| Remove duplicate topics | `herdres cleanup-duplicates --delete` (preview first without `--delete`) |
| Verify delivery into a topic | `herdres probe --thread-id <id>` |
| Tell a pane what to do | `/send <text>` in its topic |
| Stop a runaway turn and redirect | `/send! <text>` |
| See what's on the pane's screen | `/raw` |
| Re-surface the last report/question | `/status` |
| Re-show decision buttons | `/choices` |
| See & run the agent's own skills/commands | `/skills` |
| Spin up another agent here | `/new <codex\|claude\|kimi\|omp\|devin>` |
| Pick which agent a shared topic addresses | `/agents` (per-space mode) |
| Switch this space's bot voice | `/voice shared\|per_agent` |
| Run an agent's own slash-command | Just type it (e.g. `/goal …`, `/clear`) |
| Send raw keys | `/keys <keys>` |
| Inspect the topic↔pane mapping | `/debug` |

Env var names above are verified against `.env.example`; their setup and defaults live in **SETUP.md**.
