# Cockpit (macOS only) — live herdr terminal as a Telegram Mini App

> **Platform: macOS only.** The cockpit ships as a launchd agent and is fronted
> by `tailscale serve`. It does not run on Linux herdres deployments. If the
> operator is not on a Mac with Tailscale, skip this entirely.

The cockpit is a mobile, in-Telegram **mirror of the live `herdr` multiplexer**.
It runs the real `herdr` TUI in a pseudo-terminal on the Mac and streams it to
an **xterm.js** terminal inside a Telegram Mini App — every pane, real colors,
real keys, plus a touch key bar and a live "drove line" of agent statuses.

It is the hands-on companion to the Herdres bot. The bot (forum topics, turn
feed, managed bots) is the ambient awareness layer; the cockpit is what the
operator opens when they want to **drive** the terminal directly.

For core bridge install and configuration, see **SETUP.md**. For the CLI and
Telegram pane-thread commands, see **COMMANDS.md**. This file is the cockpit
operator reference only.

---

## Architecture (one host, no broker)

The Mac and the phone are on the same tailnet, so the phone reaches the Mac
directly. **No broker, no VPS, no inbound port** on the Mac.

```
 Phone (on tailnet) · Telegram Mini App ──wss──►  tailscale serve (HTTPS, ts.net cert)
                                                        │  proxies to 127.0.0.1:8787
                                                        ▼
                                          cockpit server on the Mac
                                          serves web/ · initData auth · local herdr PTY
```

| Piece | Runs where | Does |
| --- | --- | --- |
| `ssh/server/` | the Mac | Node.js + `node-pty` + `ws`. Serves the Mini App, validates each viewer's Telegram `initData`, bridges WebSocket ⇄ a local `herdr` PTY, and polls `herdr pane list` / `herdr workspace list` for the status ribbon. |
| `ssh/web/` | served by `server/` | The Mini App UI: xterm.js terminal, touch key bar, drove line, text-input row. |

The PTY is spawned **lazily on the first connected viewer** and torn down after
the last viewer leaves (idle grace ~8s). When no one is attached the launchd
agent idles cheaply.

---

## Access control (two layers, fail-closed)

1. **Tailnet.** Only the operator's own tailnet devices can reach the server at
   all. The server binds `127.0.0.1:8787` only and is never directly exposed.
2. **Telegram `initData`.** On every WebSocket connect the phone sends its
   Telegram `initData`; the server recomputes the HMAC-SHA256 signature with the
   bot token, applies an `auth_date` freshness check, and rejects any
   `user.id` that isn't the owner. No SSH keys, no room ids, no public exposure.

The server **refuses to start** unless both a bot token and an owner id are
available — it exits with `[cockpit] missing TELEGRAM_BOT_TOKEN or
HERDRES_OWNER_ID`. See **SAFETY.md** for the broader fail-closed posture.

---

## Environment (verified — `ssh/.env.example` + `server.js`)

The cockpit reads its own env, falling back to `~/.config/herdres/herdres.env`.
Only these variables are real:

| Variable | Required | Default | Meaning |
| --- | --- | --- | --- |
| `HERDRES_OWNER_ID` | yes* | — | Numeric Telegram user id; the **only** allowed viewer. |
| `TELEGRAM_BOT_TOKEN` | yes** | — | Used to validate `initData`. If unset, read from `~/.config/herdres/herdres.env`. |
| `TELEGRAM_ALLOWED_USERS` | — | — | Not read by the server, but the launchd **wrapper** uses its first id as `HERDRES_OWNER_ID` when that is unset (see below). |
| `PORT` | — | `8787` | Local listen port; `tailscale serve` proxies HTTPS to it. |
| `HERDR_SHARE_CMD` | — | `herdr` | Command mirrored in the PTY (the herdr TUI). |
| `HERDR_BIN` | — | `herdr` | Binary used for `pane list` / `workspace list` status polling and focus. |
| `HERDR_COCKPIT_SHARE_CMD` | — | — | Cockpit-only override for the mirrored command (wrapper). |
| `HERDR_COCKPIT_HERDR_BIN` | — | — | Cockpit-only override for the status/focus binary (wrapper). |
| `HERDRES_IDLE_GRACE_MS` | — | `8000` | Keep the PTY this long after the last viewer leaves. |
| `HERDRES_STATUS_INTERVAL_MS` | — | `2500` | How often the drove-line status refreshes. |
| `HERDRES_INITDATA_MAX_AGE` | — | `86400` | Reject `initData` older than N seconds. |
| `HERDRES_WEB_DIR` | — | `../web` | Static Mini App dir the server serves. |

\* Required either directly, or indirectly via the wrapper deriving it from
`TELEGRAM_ALLOWED_USERS`.
\** Required as a value, but it can be inherited from `herdres.env` rather than
set in the cockpit env.

### Wrapper behavior (`~/.local/share/herdres/herdres-cockpit.sh`)

The launchd agent runs a wrapper, not `node` directly. The wrapper:

- Sources `~/.config/herdres/herdres.env`.
- If `HERDRES_OWNER_ID` is unset but `TELEGRAM_ALLOWED_USERS` is set, uses the
  **first** id from the comma list as `HERDRES_OWNER_ID`.
- Exits with an error if neither owner id nor allowed users is available.
- Forces the cockpit to use the **real** `herdr` binary even when the Telegram
  bot bridge points `HERDR_BIN` at `herdr_turn_adapter.py` — if `HERDR_BIN`'s
  basename is `herdr_turn_adapter.py`, it is replaced with
  `${HERDR_REAL_BIN:-$HOME/.local/bin/herdr}`. (See TURN_FEED.md for the adapter.)

So the practical setup is: set `HERDRES_OWNER_ID` (or rely on
`TELEGRAM_ALLOWED_USERS`) and `TELEGRAM_BOT_TOKEN` in `herdres.env`, and the
cockpit works without extra secrets.

---

## Setup and enable

`install-macos.sh` installs the cockpit server, Node dependencies, the wrapper,
and the launchd plist alongside the regular Herdres agents (see SETUP.md for the
full install). The cockpit-specific finishing steps are:

```bash
# 1. Set owner + token (or rely on TELEGRAM_ALLOWED_USERS) in herdres.env
$EDITOR ~/.config/herdres/herdres.env

# 2. Start the cockpit launchd agent (binds 127.0.0.1:8787)
launchctl bootstrap gui/$(id -u) \
  ~/Library/LaunchAgents/com.gaijinjoe.herdres-cockpit.plist

# 3. Front it with Tailscale HTTPS (one-time; persists in tailscaled)
tailscale serve --bg https / http://127.0.0.1:8787
tailscale serve status        # prints https://<mac>.<tailnet>.ts.net
```

Then register the Mini App in **`@BotFather`** (`/newapp`, or **Bot Settings →
Menu Button**) with the URL `https://<mac>.<tailnet>.ts.net`, and make sure the
**phone is on the tailnet with MagicDNS on** so it can resolve and reach that
name. Tapping the Mini App button in the bot opens the cockpit.

`tailscale serve` config persists in tailscaled, so step 3 is one-time.

### The launchd agent

| Field | Value |
| --- | --- |
| Label | `com.gaijinjoe.herdres-cockpit` |
| Program | `~/.local/share/herdres/herdres-cockpit.sh` |
| Binds | `127.0.0.1:8787` (env `PORT=8787` in the plist) |
| `RunAtLoad` / `KeepAlive` | both true — restarts on crash, idles when no viewer |
| stdout log | `~/.local/share/herdres/cockpit.out.log` |
| stderr log | `~/.local/share/herdres/cockpit.err.log` |

### Reload after editing env or code

```bash
launchctl bootout   gui/$(id -u)/com.gaijinjoe.herdres-cockpit
launchctl bootstrap gui/$(id -u) \
  ~/Library/LaunchAgents/com.gaijinjoe.herdres-cockpit.plist
```

### Quick health check

```bash
tail -f ~/.local/share/herdres/cockpit.err.log   # look for the listen line
# [cockpit] listening on 127.0.0.1:8787 · owner <id> · serving <web dir>
tailscale serve status                            # confirm the ts.net URL is live
```

---

## What the operator sees and does

- **Terminal.** A real xterm.js mirror of the herdr TUI — all panes, colors, and
  keys. Shared sizing: like `tmux attach`, the phone and any desktop herdr
  client negotiate one shared size, so the phone may shrink the session.
- **Touch key bar.** `esc / tab / ctrl / arrows / ⏎` so the operator rarely has
  to fight the iOS soft keyboard for control characters.
- **Drove line.** A live ribbon of spaces/panes with agent statuses, refreshed
  every `HERDRES_STATUS_INTERVAL_MS`. Tapping a pane marker focuses it
  (best-effort `herdr agent focus`; a space marker uses `herdr workspace focus`).
  Focus degrades silently for non-agent panes.
- **Image paste/upload.** An image sent through the cockpit is written to a temp
  file on the Mac and its **path** is typed into the PTY — useful for handing a
  screenshot path to an agent.

### Input path (relevant to voice work — issue #4)

Typed text and keystrokes follow a single path. Keep this in mind for any
voice-input feature: the cockpit has **no separate input API** — everything is
terminal bytes.

```
text-input row / key bar / xterm onData
        │  sendInput(text)                 (web/app.js)
        ▼
  ws.send(<binary frame>)                  TextEncoder bytes over /ws
        ▼
  term.write(data.toString('utf8'))        (server/server.js — node-pty)
        ▼
  herdr PTY                                keystrokes reach the live TUI
```

The text-input row appends `\r\n` (submit), the key bar sends control sequences,
and raw xterm keystrokes pass straight through. A voice feature would convert
speech to text and feed it through `sendInput` exactly like typed text — no new
server endpoint is required. Keep deeper design notes out of this operator file.

---

## Known edges (operator-facing)

- **Phone must be on the tailnet.** If the cockpit must work while the phone is
  **off** the VPN, expose it with `tailscale funnel` instead of `serve` — but
  then `initData` validation is the only gate, so keep `HERDRES_INITDATA_MAX_AGE`
  tight.
- **Shared sizing** can shrink a shared session; mirror a dedicated
  workspace/tab if exact desktop sizing matters.
- **Mobile keyboard** quirks are real on iOS; use the key bar for control
  characters rather than the OSK.
- **Pane focus** from the drove line is best-effort and silent for non-agent
  panes.
- **`node-pty` is native.** If it failed to build at install time, the server
  logs `node-pty unavailable` and the Mini App shows a host-exit; reinstall deps
  with `npm ci --omit=dev && npm rebuild node-pty` in `ssh/server`.

---

## Cross-links

- **SETUP.md** — full install, configuration, enabling services, linking the
  Herdr plugin.
- **COMMANDS.md** — herdres CLI subcommands and Telegram pane-thread commands.
- **SAFETY.md** — fail-closed rules and access-control posture.
- **TURN_FEED.md** — the turn adapter the cockpit wrapper deliberately bypasses.
