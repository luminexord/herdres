# Herdres cockpit — a live herdr terminal as a Telegram Mini App (Tailscale)

A mobile, in-Telegram mirror of your running **herdr** multiplexer. It runs the
real `herdr` TUI in a pseudo-terminal on your Mac and streams it to **xterm.js**
inside a Telegram Mini App — every pane, real colors, real keys — with a
touch-friendly control surface and a live "drove line" of agent statuses.

It is the hands-on companion to the Herdres bot: the bot is your ambient
awareness layer; this is the cockpit you open when you want to drive.

## Why this is simple under Tailscale

With the Mac and the phone on the same tailnet, the phone reaches the Mac
directly — so there is **no broker, no VPS, and no inbound port** on your Mac.
The server runs on the Mac, and **`tailscale serve`** fronts it with a valid
`ts.net` HTTPS certificate, which is exactly the public-TLS origin Telegram's
webview requires.

```
 Phone (on tailnet) · Telegram Mini App ──wss──►  tailscale serve (HTTPS, ts.net cert)
                                                        │  proxies to 127.0.0.1:PORT
                                                        ▼
                                          cockpit server on the Mac
                                          serves web/ · initData auth · local herdr PTY
```

Two layers of access control: only your tailnet devices can connect at all, and
on top of that the server validates each viewer's Telegram `initData` and rejects
any `user.id` that isn't yours.

## Pieces

| Dir | Runs where | Does |
| --- | --- | --- |
| `server/` | your Mac | Serves the Mini App, validates `initData`, and bridges WebSocket ⇄ a local `herdr` PTY. Polls `herdr pane list` for the status ribbon. |
| `web/` | served by `server/` | The Mini App: xterm.js, the night-field UI, the touch key bar, the drove line. |

## Setup

### 1. Install and run the server on your Mac

```bash
cd ssh/server
npm install              # builds node-pty (native)
HERDRES_OWNER_ID=<your-telegram-id> npm start
# TELEGRAM_BOT_TOKEN is reused from ~/.config/herdres/herdres.env if unset
```

It listens on `127.0.0.1:8787` only — it is never directly exposed.

### 2. Put HTTPS in front of it with Tailscale

```bash
tailscale serve --bg https / http://127.0.0.1:8787
tailscale serve status        # shows your https://<mac>.<tailnet>.ts.net URL
```

`tailscale serve` provisions a real Let's Encrypt cert for your MagicDNS name and
proxies HTTPS (including WebSocket upgrades) to the local port. The URL it prints
is your Mini App origin.

### 3. Register the Mini App with BotFather

In `@BotFather`: `/newapp` (or **Bot Settings → Menu Button**) and set the URL to
`https://<mac>.<tailnet>.ts.net`. Telegram now shows a button that opens the
cockpit inside the app.

### 4. Make sure the phone is on the tailnet

The phone needs the Tailscale app running with **MagicDNS** on so it can resolve
and reach `<mac>.<tailnet>.ts.net`. Then tap the Mini App button in your bot.

### Keep it running (launchd)

`install-macos.sh` installs the cockpit server, Node dependencies, wrapper, and
launchd plist together with the regular Herdres agents:

```bash
./install-macos.sh
launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/com.gaijinjoe.herdres-cockpit.plist
```

The wrapper reads `~/.config/herdres/herdres.env`, uses the first
`TELEGRAM_ALLOWED_USERS` id as `HERDRES_OWNER_ID` if needed, and uses the real
`herdr` binary for the cockpit even when the Telegram bot bridge points
`HERDR_BIN` at `herdr_turn_adapter.py`.

Manual install is still possible:

```bash
mkdir -p ~/.local/share/herdres/ssh
cp -R server web ~/.local/share/herdres/ssh/        # or symlink the repo
sed "s#__HOME__#$HOME#g" com.gaijinjoe.herdres-cockpit.plist > ~/Library/LaunchAgents/com.gaijinjoe.herdres-cockpit.plist
$EDITOR ~/Library/LaunchAgents/com.gaijinjoe.herdres-cockpit.plist   # set HERDRES_OWNER_ID
launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/com.gaijinjoe.herdres-cockpit.plist
```

`tailscale serve` config persists in tailscaled, so step 2 is a one-time setup.

## Auth model

- The **phone** proves identity with Telegram `initData`; the server recomputes
  the HMAC-SHA256 signature with the bot token and rejects anything whose
  `user.id` isn't `HERDRES_OWNER_ID`, plus an `auth_date` freshness check.
- The **tailnet** ensures only your own devices can reach the server at all.
- No SSH keys, no room ids, no public exposure.

## Known edges

- **The phone must be on the tailnet.** If you want the cockpit to work when the
  phone is *off* the VPN, expose it publicly with `tailscale funnel` instead of
  `serve` — but then `initData` validation is your only gate, so keep
  `HERDRES_INITDATA_MAX_AGE` tight.
- **Shared sizing.** Like `tmux attach`, the phone and any desktop herdr client
  negotiate a shared size; the phone view may shrink the session. Mirror a
  dedicated workspace/tab if that matters.
- **Mobile keyboard.** iOS soft-keyboard/viewport quirks are real; the key bar
  (esc / tab / ctrl / arrows / ⏎) exists so you rarely fight the OSK for control
  characters. Tap a pane marker in the drove line to focus it.
- **Pane focus** from the drove line is best-effort via `herdr agent focus` and
  degrades silently for non-agent panes.

## Config

See [`.env.example`](./.env.example) for every variable the server reads.
