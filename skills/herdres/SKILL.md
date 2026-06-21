---
name: herdres
description: "Use Herdres to operate the Herdr-to-Telegram bridge: install and configure it, set up per-agent forum topics, send/interrupt/queue messages to agent panes, read status and reports, manage child bots, drive the macOS cockpit, and run maintenance like cleanup-duplicates. Use when a user wants to control or set up Herdr agents over Telegram."
license: MIT
compatibility: "Requires the herdres CLI, Python 3.11+, a Telegram bot + forum supergroup, and a running Herdr 0.7.0+ multiplexer."
allowed-tools: Bash Read Edit
metadata:
  version: "0.1.0"
  author: luminexord
  source: luminexord/herdres
  tags: [telegram, herdr, bridge, operations, agents]
---

# herdres — operator skill

herdres mirrors each running Herdr agent into a Telegram forum supergroup: every pane (or space) gets its own topic, pane output streams in, and your replies route back to the right pane. this skill lets you install and configure that bridge, pick per-agent vs per-space topics, send/interrupt/queue instructions, read reports and decisions, manage per-agent child bots, drive the macOS cockpit, and run maintenance — all fail-closed.

before doing anything, confirm `~/.config/herdres/herdres.env` exists and `herdres` resolves on `PATH` (or `~/.local/bin/herdres`). if it is not installed yet, go to **Quick install**. if it is, go to **Before acting**.

## Quick install

the headline path. do not skip preflight, do not write Telegram state until verify passes, and **you MUST NOT invent or scavenge credentials — the bot token, chat ID, and allowed-user IDs come from the user.**

1. **preflight.** detect the OS: Linux uses **systemd** (`systemctl --user`), macOS uses **launchd** (no user systemd). check `python3 --version` is **>= 3.11**. confirm Herdr is running (`herdr --version`; `herdr` must be on `PATH` or set `HERDR_BIN`); **0.7.0+ is recommended** — older herdr still works in degraded mode (timer-only, no instant plugin trigger; see step 5). check whether `~/.config/herdres/herdres.env` already exists — if so, do not clobber it; just edit it and skip the installer's copy step.

2. **guided Telegram setup — you MUST pause and ask the user.** the bot token, chat ID, and allowed-user IDs are **user-supplied secrets**: you **MUST NOT** invent them, and you **MUST NOT** copy them from another app's config (e.g. an existing **Hermes** bot token) without the user's explicit approval — reusing a token another process already long-polls breaks the one-`getUpdates`-consumer rule (see **Safety rules**). **Always** prefer a **dedicated** bot for herdres. if you cannot ask the user (non-interactive), you **MUST** stop and report what is needed rather than guessing. walk the user through these steps and validate what they paste back:
   - **a.** message **@BotFather**, run `/newbot`, copy the **bot token** → `TELEGRAM_BOT_TOKEN`. Validate: shape is `<digits>:<base64-ish>`, e.g. `123456:ABC-...`. Then in @BotFather set the bot's **Group Privacy to OFF** (`/setprivacy` → pick the bot → **Disable**) — otherwise it only sees `/commands` and mentions, so plain-text topic replies won't reach panes and the chat ID is hard to fetch.
   - **b.** create a **group**, open Group Settings, and turn on **Topics** (this makes it a forum supergroup). herdres refuses any chat that is not a forum-enabled supergroup.
   - **c.** add the bot to the group and promote it to **Administrator with Manage Topics** — grant **both at once** (otherwise preflight fails closed twice: once for admin, once for `can_manage_topics`).
   - **d.** get the supergroup **chat ID** → `HERDR_TELEGRAM_TOPICS_CHAT_ID`. Validate: it is **negative** and starts with `-100…`. (Easiest: add @RawDataBot or @getidsbot briefly, then remove it.)
   - **e.** get **your own user ID** (a positive integer) → first entry of `TELEGRAM_ALLOWED_USERS`. Validate: numeric, positive.

3. **run the installer** from the repo checkout: Linux `./install-user.sh`; macOS `./install-macos.sh`. This installs the `herdres` CLI, the gateway, the env file (copied from `.env.example`, never clobbering an existing one), the Herdr plugin manifest, and the service units / launchd agents.
   - the CLI lands at `~/.local/bin/herdres` and the installer does **not** edit `PATH`. if `herdres` is "command not found" next, add `export PATH="$HOME/.local/bin:$PATH"` to your shell profile and re-open the shell, or call it as `~/.local/bin/herdres`. the service and plugin use absolute paths, so this only affects you typing `herdres`.

4. **write the three required env vars** in `~/.config/herdres/herdres.env`. **Fastest and safest: run `herdres setup`** — it prompts for the token (no echo), chat ID, and allowed users, validates each, runs preflight, and only then writes the file at mode `0600`. It **refuses** to run unattended without `--bot-token/--chat-id/--allowed-users`, and it **never** silently reuses the Hermes token (it demands `--reuse-hermes-token` or a typed `reuse` confirmation). If you write the file by hand instead:
   ```bash
   TELEGRAM_BOT_TOKEN=123456:ABC-yourBotToken
   HERDR_TELEGRAM_TOPICS_CHAT_ID=-1001234567890
   TELEGRAM_ALLOWED_USERS=123456789
   ```
   the first allowed-user id is treated as the owner. everything else has safe defaults. strongly recommend setting `HERDR_TELEGRAM_TOPICS_PER_AGENT=1` here too (one clean topic per pane — see **Before acting**).

5. **enable the service + link the plugin.**
   - Linux: `systemctl --user daemon-reload && systemctl --user enable --now herdres.timer`
   - macOS: `launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/com.gaijinjoe.herdres.plist` (plus the gateway and, if wanted, cockpit plists)
   - both: `herdr plugin link ~/.local/share/herdres/herdres-plugin` then `herdres plugin-enable` — **needs herdr 0.7.0+**. on older herdr, skip this (the timer still reconciles, just at its interval) and set `HERDR_BIN` to the bundled `herdr_turn_adapter.py` if `herdr pane turn` is missing; upgrading herdr is recommended.

6. **verify** (non-destructive first):
   ```bash
   HERDR_TELEGRAM_TOPICS_DRY_RUN=1 herdres sync   # preflight, no Telegram writes
   herdres sync                                    # real run; a topic should appear
   herdres probe --thread-id <id>                  # optional: confirm rich delivery into a topic
   ```
   a clean run prints a JSON result and, with Herdr panes up, topics appear in the supergroup. preflight surfaces config errors as fail-closed JSON (`...CHAT_ID is required`, `must be a forum-enabled supergroup`, `bot is not an administrator`, `bot lacks can_manage_topics`).

7. **handoff.** open a pane topic and try `/help`, `/status`, then `/send hello`. you are live.

Full install depth — service files, the standalone gateway, the turn-adapter fallback, state/lock locations — is in [references/SETUP.md](references/SETUP.md).

## Before acting

- **confirm config + service.** `~/.config/herdres/herdres.env` exists with the three required vars, and the timer/launchd agent is enabled (it is the slow repair path; keep it on even with plugin events). if anything is missing, go to **Quick install**.
- **identify the topic mode.** read `HERDR_TELEGRAM_TOPICS_PER_AGENT`: `1` = **one topic per pane** (recommended; unambiguous), `0` = **one topic per space** (multiple panes can share a thread). the flag is read at runtime by every entry point, so set it in `herdres.env` — the one file all contexts load. switching modes is a clean-slate reset of topic mappings. details in [references/TOPICS.md](references/TOPICS.md).
- **reply inside the pane thread.** to control a pane, reply **inside its topic** — text routes to that exact pane with no `/send` prefix. in per-space mode a top-level message only routes when the topic has **exactly one** live pane; otherwise herdres **fails closed** (`Reply inside a pane thread so I know which Herdr pane to control.`). never assume a top-level message in a multi-pane topic reached a pane.

## Core operations

these are the highest-frequency commands, typed **inside a pane's forum topic**. an agent runs one turn at a time, so a command to a busy agent can only **queue** or **interrupt** — never inject mid-turn.

- `/send <text>` — forward as the agent's next instruction; idle → submitted now, busy → **queued** for the turn boundary (never lost).
- `/send! <text>` (aliases `/interrupt`, `/isend`) — interrupt a running turn (Esc, wait for idle), then deliver; on an idle agent it just delivers.
- `/keys <keys>` — send raw key names to the pane (e.g. `/keys ctrl-c`, `/keys escape enter`).
- `/status`, `/report` — resend the latest clean report or pending question for this pane.
- `/raw [lines]` — sanitized raw visible pane output (default 80, max 160) when the clean report is not enough.
- `/choices` — resend the active decision prompt and its inline buttons.
- `/new <codex|claude|kimi|omp|devin>` — split a new pane in this space and launch that agent.
- `/debug` — show the topic↔pane mapping (pane id, topic, route) for troubleshooting.
- any **other** `/command` (e.g. `/goal`, `/clear`, `/model`) is forwarded to the agent CLI as-is. Long/multiline input is staged to an owner-only inbound file so the full text reaches the pane.

Full table, idle-vs-busy semantics, plain-text routing, and the inbound-file rule are in [references/COMMANDS.md](references/COMMANDS.md).

## Topics, turn feed, managed bots, cockpit

**Topics.** Topic granularity is one env var. Per-agent (`=1`) gives each pane its own clean thread, status icon, and unambiguous reply target; per-space (`=0`) collapses a workspace into one thread and forces the multi-pane fail-closed rule. Topic status shows as a per-topic icon (⚡️ working, ☕️ idle, ✅ done, ❗️ blocked, ‼️ error, 🧠 idle-but-still-on-a-`/goal`). Backfill many topics in one shot with `HERDR_TELEGRAM_TOPICS_MAX_CREATES=20 herdres sync`. See [references/TOPICS.md](references/TOPICS.md).

**Turn feed.** `HERDR_TELEGRAM_TOPICS_TURN_FEED=1` (default) renders only Herdr's structured last-turn (the submitted instruction + final answer) via `herdr pane turn <pane_id> --last --format json` — no TUI chrome, spinners, or thinking leak through. It supports live streaming drafts and structured `pending_decision` buttons, and ships a `herdr_turn_adapter.py` fallback for Herdr builds without `pane turn`. When the turn is unavailable, herdres sends **nothing** (it never scrapes the terminal). See [references/TURN_FEED.md](references/TURN_FEED.md).

**Managed bots.** `HERDR_TELEGRAM_TOPICS_MANAGED_BOTS=1` (default) gives each agent type its own Telegram identity (Herdr Codex, Herdr Claude, …) instead of one manager bot. herdres suggests child-bot creation in General for open pane types, captures tokens via Telegram's managed-bot handshake, and the gateway runs one long-poll worker per token. You must add each child bot to the forum group; on access rejection herdres falls back to the manager bot and re-prompts. See [references/MANAGED_BOTS.md](references/MANAGED_BOTS.md).

**Cockpit (macOS only).** A launchd + `tailscale serve` Mini App that mirrors the live `herdr` TUI (xterm.js, touch key bar, drove line) into Telegram — the hands-on companion to the ambient bot. Fail-closed on tailnet + Telegram `initData` for the owner id only. See [references/COCKPIT.md](references/COCKPIT.md).

## Maintenance

- `herdres sync` — force one reconciliation pass (create/update topics, post pending reports). Driven by the timer; safe to run by hand. Non-blocking lock.
- `herdres cleanup-duplicates` — **report** legacy duplicate topics (same pane mapped twice), read-only. Then `herdres cleanup-duplicates --delete` to remove them. Always inspect first; it never deletes a live topic, only a proven-dead closed-pane duplicate that strongly matches a live pane. Old topics left by a per-agent⇄per-space mode flip are **not** touched — delete those by hand.
- `herdres probe [--thread-id <id>]` — send a throwaway rich message and delete it, to confirm wiring and bot permissions into a specific topic.

## Safety rules

These are load-bearing and fail-closed. Honor them when operating on a user's behalf.

- **One `getUpdates` consumer per bot token** (hard Telegram limit). Never run a Hermes poller **and** `herdres-gateway` on the same `TELEGRAM_BOT_TOKEN`; pick one inbound path. Outbound-only `sync`/`event` are always safe alongside it. The gateway runs exactly one worker per token (manager + each child), so managed bots do not violate this.
- **Multi-pane space topics fail closed.** herdres routes only when the target pane is unambiguous. If it replies `Reply inside a pane thread…`, do not retry the same way — reply inside the specific pane thread or use `/send`.
- **Never key-drive Claude multi-question / review wizards from Telegram.** Only structured `pending_decision` buttons and explicit `HERDRES_CHOICES` blocks are safe; `pending_interaction` and visible-TUI prompts stay read-only. For an unstructured choice, answer in the Herdr pane directly.
- **No send retries.** Transient/network send failures are not re-fired (avoids duplicate posts). Do not tight-loop `herdres sync` or re-post — the timer reconciles status, icons, and pinned cards on the next tick.

Full rules — owner-only gate, state ownership, single-version-per-state-file, cleanup guarantees, preflight — are in [references/SAFETY.md](references/SAFETY.md).

## When unsure

- run `herdres` with no subcommand for usage, or `/help` inside a pane topic for the pane command list.
- run `herdres probe --thread-id <id>` to test wiring, or `HERDR_TELEGRAM_TOPICS_DRY_RUN=1 herdres sync` to preflight with no Telegram writes.
- reread the relevant reference: [SETUP.md](references/SETUP.md), [COMMANDS.md](references/COMMANDS.md), [TOPICS.md](references/TOPICS.md), [TURN_FEED.md](references/TURN_FEED.md), [MANAGED_BOTS.md](references/MANAGED_BOTS.md), [COCKPIT.md](references/COCKPIT.md), [SAFETY.md](references/SAFETY.md).
