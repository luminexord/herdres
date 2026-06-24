# Herdres Setup

End-to-end install, configuration, and service enablement for an operator standing up Herdres. Herdres maps each Herdr space (or pane) to a Telegram forum topic, posts pane traffic there, and routes operator replies back to the right pane.

This file covers **install + config + services + plugin linking only**. For the command surface see COMMANDS.md, for the forum-topic model see TOPICS.md, and for fail-closed rules see SAFETY.md.

---

## 1. Prerequisites

### On the host

| Requirement | Notes |
| --- | --- |
| Python **>= 3.11** | stdlib-only; no pip deps. macOS installer auto-picks `python3.11`..`python3.14`. |
| **Herdr >= 0.7.0** (recommended) | Provides local plugin events (`pane.agent_status_changed`) and the `herdr pane turn` structured-turn endpoint. `herdr` must be on `PATH`, or set `HERDR_BIN`. Older herdr (e.g. 0.6.x) still runs in **degraded mode** — timer-only reconcile, no instant plugin trigger; set `HERDR_BIN` to the bundled `herdr_turn_adapter.py` for the turn feed. |
| Linux: user systemd | For the reconcile timer (`systemctl --user`). macOS uses launchd instead (no user systemd). |

### In the Telegram app (the human must do this once)

1. **Create a bot.** Message **@BotFather**, run `/newbot`, and copy the **bot token** (looks like `123456:ABC-...`). This is `TELEGRAM_BOT_TOKEN`. Then **turn Group Privacy OFF** for the bot (`/setprivacy` in @BotFather → pick the bot → **Disable**) — required so the bot sees plain-text topic replies, not just `/commands`/mentions; with privacy *on*, the bot also won't surface group messages in `getUpdates`, which makes fetching the chat ID hard.
2. **Create a SUPERGROUP with forum Topics ENABLED.** Create a group, open Group Settings, and turn on **Topics** (this converts it to a forum supergroup). Herdres refuses to run unless the chat is a forum-enabled supergroup.
3. **Add the bot as an ADMIN with Manage Topics.** Add the bot to the group, promote it to **Administrator**, and grant **Manage Topics** (and pin-message rights if you plan to use pinned space status). Grant **both at the same time** — Herdres preflight checks them in order and fails closed first on admin, then on `can_manage_topics`, so granting one at a time means two separate failures.
4. **Get the numeric chat ID** of that supergroup (the negative `-100…` form). This is `HERDR_TELEGRAM_TOPICS_CHAT_ID`.
5. **Get your own Telegram user ID** (a positive integer). This is the first entry of `TELEGRAM_ALLOWED_USERS`.

#### How to obtain the chat ID and your user ID

- Easiest: add a helper bot such as **@RawDataBot** or **@getidsbot** to the group briefly — it reports the chat ID (`-100…`) and, when you message it directly, your user ID. Remove the helper bot afterward.
- Alternatively, after the bot is in the group and config is filled in, run `herdres sync` once and read the chat/user IDs out of the resulting Telegram bot updates.
- The supergroup chat ID is always **negative** and starts with `-100`. Owner user IDs are **positive**.

---

## 2. The three REQUIRED env vars

All config lives in **`~/.config/herdres/herdres.env`** (the installers copy `.env.example` there). Only three values are mandatory:

```bash
TELEGRAM_BOT_TOKEN=123456:ABC-yourBotToken
HERDR_TELEGRAM_TOPICS_CHAT_ID=-1001234567890
TELEGRAM_ALLOWED_USERS=123456789
```

| Var | Meaning |
| --- | --- |
| `TELEGRAM_BOT_TOKEN` | The @BotFather token for the manager bot. |
| `HERDR_TELEGRAM_TOPICS_CHAT_ID` | The forum-supergroup chat ID, negative `-100…` form. |
| `TELEGRAM_ALLOWED_USERS` | Comma-separated owner user IDs allowed to control panes. The **first** entry is treated as the owner (e.g. it seeds `HERDRES_OWNER_ID` for the macOS cockpit). |

Everything else in `.env.example` is optional with safe defaults. A few worth knowing at setup time (see TOPICS.md / TURN_FEED.md / MANAGED_BOTS.md for the rest):

| Var | Default | Use |
| --- | --- | --- |
| `HERDR_BIN` | `herdr` | Path to the `herdr` CLI if not on `PATH`, or the turn adapter (below). |
| `HERDR_TELEGRAM_TOPICS_PER_AGENT` | `0` | `0` = one topic per space; `1` = one topic per pane (`<agent> · <folder>`). Toggling triggers a one-time clean-slate reset of topic mappings. |
| `HERDR_TELEGRAM_TOPICS_TURN_FEED` | `1` | Use `herdr pane turn` structured turns instead of parsing the terminal. Requires Herdr to expose `pane turn`. |
| `HERDR_TELEGRAM_TOPICS_DRY_RUN` | `0` | Set `1` to test with no Telegram writes. |

**If your `herdr` lacks `pane turn`**, point Herdres at the bundled adapter:

```bash
HERDR_BIN=~/.local/bin/herdr_turn_adapter.py
HERDR_REAL_BIN=$(command -v herdr)
```

---

## 3. Linux install

From the repo checkout:

```bash
./install-user.sh
```

This installs to the user prefix:

- `~/.local/bin/herdres` — the CLI
- `~/.local/bin/herdres-gateway` (+ `herdres_routing.py`) — optional standalone inbound gateway
- `~/.config/herdres/herdres.env` — config (copied from `.env.example`)
- `~/.local/share/herdres/herdres-plugin/herdr-plugin.toml` — plugin manifest with an absolute `herdres` path baked in
- `~/.config/systemd/user/herdres.{service,timer}` and `herdres-gateway.service`

> **PATH gotcha:** the installer puts `herdres` at `~/.local/bin/herdres` but does **not** edit your `PATH`. If `herdres` is later "command not found", `~/.local/bin` is not on `PATH` — add `export PATH="$HOME/.local/bin:$PATH"` to your shell profile (`~/.bashrc`/`~/.zshrc`) and re-open the shell, or invoke it by full path. The systemd/launchd units and the plugin manifest bake in absolute paths, so the **service** runs regardless; this only affects typing `herdres` yourself.

Then edit the config and enable the reconcile timer:

```bash
$EDITOR ~/.config/herdres/herdres.env     # set the three required vars
systemctl --user daemon-reload
systemctl --user enable --now herdres.timer
```

The timer drives periodic `herdres sync` — the reconcile/repair path that detects closed panes, repairs stale topic mappings, and covers any missed plugin events.

### Optional: standalone inbound gateway (Linux)

`herdres-gateway.service` is a stdlib-only `getUpdates` daemon that handles inbound pane-thread commands and callback buttons **without** the Hermes Telegram bridge.

```bash
systemctl --user enable --now herdres-gateway.service
```

> **One-consumer caveat (load-bearing):** Telegram allows only **one** active `getUpdates` consumer per bot token. Run the gateway **only** when Herdres owns its bot token and nothing else (e.g. a Hermes poller) polls `getUpdates` for it. **Never run the gateway alongside Hermes polling on the same token.** If you instead route inbound through Hermes, leave this service disabled and install the Hermes bridge hook (see README "Pane Thread Commands").

---

## 4. macOS install

macOS has no user systemd, so the bundled installer uses **launchd** plus a Tailscale cockpit. Use this path only when Herdres owns its own bot token (no other `getUpdates` consumer).

```bash
./install-macos.sh
```

This finds a Python >= 3.11, installs `herdres` / `herdr_turn_adapter.py` / `herdres-gateway` with their shebangs pinned to that interpreter, writes `~/.config/herdres/herdres.env` (mode 600, never clobbering an existing one), and installs three launchd agents into `~/Library/LaunchAgents/`.

Finish per the installer's printed steps:

```bash
$EDITOR ~/.config/herdres/herdres.env
herdr plugin link ~/.local/share/herdres/herdres-plugin
launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/com.gaijinjoe.herdres.plist
launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/com.gaijinjoe.herdres-gateway.plist
launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/com.gaijinjoe.herdres-cockpit.plist
tailscale serve --bg https / http://127.0.0.1:8787
```

The three launchd agents:

| Label | Role |
| --- | --- |
| `com.gaijinjoe.herdres` | Runs `herdres sync` **every 5 seconds** (the reconcile timer; short turns still get a sync window for stream updates before the final answer lands). |
| `com.gaijinjoe.herdres-gateway` | Runs `herdres_gateway.py`, the stdlib long-poll loop that replaces the Hermes `getUpdates` bridge — dispatches space-topic messages/callbacks to `herdres command` / `herdres callback`. |
| `com.gaijinjoe.herdres-cockpit` | The Tailscale-served terminal cockpit Mini App. See COCKPIT.md (macOS only). |

To reload an agent after editing config:

```bash
launchctl bootout   gui/$(id -u)/com.gaijinjoe.herdres-gateway
launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/com.gaijinjoe.herdres-gateway.plist
```

> Same one-consumer caveat applies: the gateway is the single `getUpdates` consumer. Outbound `sync` / `event` only **send** and never consume `getUpdates`, so they run safely alongside the gateway. Do not also run a Hermes poller on the same bot token.

---

## 5. One-tap questions & plan approvals (Claude Code hook)

When a Claude Code pane asks a question (`AskUserQuestion`) or requests plan approval
(`ExitPlanMode`), Herdres can surface it on Telegram as **tappable buttons** instead of a
read-only screen the owner has to answer in the terminal (issue #36). Claude Code never writes a
*pending* prompt to its transcript, so detection rides a small `PreToolUse`/`PostToolUse` hook
(`herdres-decision-hook`) that records the prompt to a per-session file the turn adapter reads.

Both installers copy the hook script to `~/.local/bin/herdres-decision-hook` and register it in
`~/.claude/settings.json` automatically. The registration is **idempotent and coexists with other
hooks** (orca/herdr) — it only adds Herdres' own entries and never touches others. To (re-)assert it
by hand — e.g. on a host where Claude Code was installed after Herdres, or during a canary deploy:

```bash
herdres hooks install        # adds PreToolUse/PostToolUse (AskUserQuestion|ExitPlanMode) + SessionEnd
```

It is a no-op if `~/.claude/` does not exist (Claude Code not installed). Relevant knobs:

| Env | Default | Meaning |
| --- | --- | --- |
| `HERDRES_TURN_ADAPTER_DECISIONS` | `1` | Master toggle for adapter-emitted decision buttons. |
| `HERDRES_PLAN_APPROVE_SEND_TEXT` | `1` | Text sent to the pane when the owner taps **Approve & proceed** on a plan. |
| `HERDRES_PENDING_DIR` | `~/.local/share/herdres/pending` | Where the hook writes per-session pending files. |
| `HERDRES_PENDING_TTL_SECONDS` | `3600` | Adapter ignores (and the hook's misses are bounded by) pending files older than this. |

See TURN_FEED.md → "How the adapter sees a pending prompt" for the full round-trip.

---

## 6. Link the Herdr plugin

Herdr 0.7.0 plugin events give Herdres a low-latency trigger layer. The included plugin is thin: `pane.agent_status_changed -> herdres event`. It does **not** replace the inbound bridge/gateway for commands and callbacks.

```bash
herdr --version                                   # confirm >= 0.7.0
herdr plugin link ~/.local/share/herdres/herdres-plugin
~/.local/bin/herdres plugin-enable
```

`install-user.sh` / `install-macos.sh` write the installed manifest with an absolute `herdres` command, so it does not depend on Herdr's plugin `PATH`. If you link `herdres-plugin/` directly from a source checkout, ensure `herdres` resolves in the plugin runtime, or edit the manifest command to an absolute path.

Toggle events independently of normal sync (keep the timer enabled either way as the slower repair path):

```bash
~/.local/bin/herdres plugin-disable
~/.local/bin/herdres plugin-enable
```

---

## 7. Verify

Run a safe dry-run first (no Telegram writes):

```bash
HERDR_TELEGRAM_TOPICS_DRY_RUN=1 ~/.local/bin/herdres sync
```

Then a real sync:

```bash
~/.local/bin/herdres sync
```

A successful run prints a JSON result object. Herdres preflight will surface, as a fail-closed JSON error, any of:

- `HERDR_TELEGRAM_TOPICS_CHAT_ID is required` — the chat ID is unset.
- `Telegram chat must be a forum-enabled supergroup` — Topics is off, or the chat is not a supergroup.
- `bot is not an administrator in the Telegram forum group` — promote the bot to admin.
- `bot lacks can_manage_topics in the Telegram forum group` — grant Manage Topics.
- **Bot not visible in `getUpdates` / can't fetch the chat ID** — the bot's **Group Privacy** is still on. Disable it in @BotFather (`/setprivacy` → **Disable**), then send one message in the group.

If the run is clean, open the supergroup: with Herdr panes running you should see space (or per-agent) topics appear and pane traffic posted into them. To control a pane, reply inside its topic (see COMMANDS.md).

---

## 8. State, lock, inbound, and offset locations

All Herdres runtime data lives under **`~/.local/share/herdres/`**:

| Path | Purpose | Env override |
| --- | --- | --- |
| `state.json` | Topic mappings, route indexes, hashes — **no bot tokens**. | `HERDR_TELEGRAM_TOPICS_STATE` |
| `sync.lock` | Single-run lock so reconcile passes don't overlap. | `HERDR_TELEGRAM_TOPICS_LOCK` |
| `inbound/<pane>/…txt` | Long/multiline owner messages staged to disk (owner-only perms) so the pane reads the file instead of pasting raw text into the TUI. | — |
| `gateway_offset` | Standalone gateway's `getUpdates` offset cursor (per bot token). | `HERDR_TELEGRAM_TOPICS_GATEWAY_OFFSET` |
| `herdres-plugin/herdr-plugin.toml` | Linked Herdr plugin manifest. | — |

State stores topic IDs and routing metadata only; it never stores bot tokens. Managed child-bot tokens (when enabled) are stored separately — see MANAGED_BOTS.md.

---

## Cross-references

- **COMMANDS.md** — full `herdres` CLI subcommands and in-topic pane-thread commands.
- **TOPICS.md** — topic granularity and the forum-topic model.
- **TURN_FEED.md** — structured turn feed, streaming drafts, adapter fallback.
- **MANAGED_BOTS.md** — one managed Telegram bot per agent type.
- **COCKPIT.md** — macOS Tailscale cockpit Mini App.
- **SAFETY.md** — fail-closed rules and operator safety.
