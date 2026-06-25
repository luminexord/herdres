#!/usr/bin/env bash
# Herdres macOS installer.
#
# macOS has no systemd, so instead of the Linux timer + Hermes-gateway-bridge
# this installs:
#   - herdres, herdr_turn_adapter.py, herdres_gateway.py  (pinned to python>=3.11)
#   - a launchd agent that runs `herdres sync` every 5s    (the reconcile timer)
#   - a launchd agent that runs herdres_gateway.py          (inbound long-poll)
#
# The standalone gateway replaces the Hermes getUpdates bridge for deployments
# where Herdres owns its own Telegram bot (no other getUpdates consumer).
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
BIN="$HOME/.local/bin"
CFG="$HOME/.config/herdres"
SHARE="$HOME/.local/share/herdres"
LA="$HOME/Library/LaunchAgents"

# 1. Find a Python >= 3.11
PY=""
for c in python3.11 python3.12 python3.13 python3.14 python3; do
    p="$(command -v "$c" 2>/dev/null || true)"
    [ -n "$p" ] || continue
    if "$p" -c 'import sys; raise SystemExit(0 if sys.version_info[:2] >= (3, 11) else 1)' 2>/dev/null; then
        PY="$p"; break
    fi
done
[ -n "$PY" ] || { echo "error: need Python >= 3.11 on PATH (try: brew install python@3.11)"; exit 1; }
echo "Using interpreter: $PY ($("$PY" --version 2>&1))"

mkdir -p "$BIN" "$CFG" "$CFG/managed-bots" "$SHARE/herdres-plugin" "$SHARE/inbound" "$SHARE/ssh/server" "$SHARE/ssh/web" "$LA"

# Record this checkout's absolute path so `herdres update --edge` can git pull it.
# Use $PWD (the cwd) for parity with install-user.sh, which records the same
# expression; this installer is run from the checkout root.
printf '%s\n' "$PWD" > "$HOME/.local/share/herdres/source"

# 2. Install scripts, pinning the shebang to the chosen interpreter
install_pinned() { install -m 755 "$HERE/$1" "$BIN/$2"; /usr/bin/sed -i '' "1s|.*|#!$PY|" "$BIN/$2"; }
install_pinned herdres.py            herdres
install_pinned herdr_turn_adapter.py herdr_turn_adapter.py
install_pinned herdres_gateway.py    herdres-gateway
# herdres_gateway.py imports routing helpers from herdres_routing.py, so it
# must sit next to the installed gateway binary on the import path.
install -m 644 "$HERE/herdres_routing.py" "$BIN/herdres_routing.py"
# Claude Code hook (issue #36): mirrors a pending AskUserQuestion/ExitPlanMode to Telegram as
# tappable buttons; register it in ~/.claude/settings.json (idempotent, no-op without Claude Code).
install_pinned herdres_decision_hook.py herdres-decision-hook
"$BIN/herdres" hooks install >/dev/null 2>&1 || true

if [ -d "$HERE/assets/managed-bots" ]; then
    for photo in codex claude kimi omp devin; do
        if [ -f "$HERE/assets/managed-bots/$photo.jpg" ] && [ ! -f "$CFG/managed-bots/$photo.jpg" ]; then
            install -m 644 "$HERE/assets/managed-bots/$photo.jpg" "$CFG/managed-bots/$photo.jpg"
        fi
    done
fi

# 3. Config (never clobber an existing one)
if [ ! -f "$CFG/herdres.env" ]; then
    install -m 600 "$HERE/.env.example" "$CFG/herdres.env"
    echo "Wrote $CFG/herdres.env — set TELEGRAM_BOT_TOKEN, HERDR_TELEGRAM_TOPICS_CHAT_ID, TELEGRAM_ALLOWED_USERS"
    echo "If herdr lacks 'pane turn', also set HERDR_BIN=$BIN/herdr_turn_adapter.py and HERDR_REAL_BIN=\$(command -v herdr)"
fi

# 4. Plugin manifest with an absolute herdres path
sed "s#\\[\"herdres\", #\\[\"$BIN/herdres\", #g" \
    "$HERE/herdres-plugin/herdr-plugin.toml" > "$SHARE/herdres-plugin/herdr-plugin.toml"

if command -v rsync >/dev/null 2>&1; then
    rsync -a --delete --exclude node_modules "$HERE/ssh/server/" "$SHARE/ssh/server/"
    rsync -a --delete "$HERE/ssh/web/" "$SHARE/ssh/web/"
else
    cp -R "$HERE/ssh/server/." "$SHARE/ssh/server/"
    rm -rf "$SHARE/ssh/server/node_modules"
    cp -R "$HERE/ssh/web/." "$SHARE/ssh/web/"
fi

if command -v npm >/dev/null 2>&1; then
    (cd "$SHARE/ssh/server" && npm ci --omit=dev && npm_config_build_from_source=true npm rebuild node-pty --foreground-scripts)
else
    echo "warning: npm not found; cockpit dependencies were not installed"
fi

cat > "$SHARE/herdres-sync.sh" <<'EOF'
#!/bin/sh
set -a; . "$HOME/.config/herdres/herdres.env"; set +a
exec "$HOME/.local/bin/herdres" sync
EOF
cat > "$SHARE/herdres-gateway.sh" <<'EOF'
#!/bin/sh
set -a; . "$HOME/.config/herdres/herdres.env"; set +a
exec "$HOME/.local/bin/herdres-gateway"
EOF
cat > "$SHARE/herdres-cockpit.sh" <<'EOF'
#!/bin/sh
if [ -f "$HOME/.config/herdres/herdres.env" ]; then
    set -a; . "$HOME/.config/herdres/herdres.env"; set +a
fi

if [ -z "${HERDRES_OWNER_ID:-}" ] && [ -n "${TELEGRAM_ALLOWED_USERS:-}" ]; then
    HERDRES_OWNER_ID="$(printf '%s' "$TELEGRAM_ALLOWED_USERS" | cut -d, -f1 | tr -d '[:space:]')"
    export HERDRES_OWNER_ID
fi

if [ -z "${HERDRES_OWNER_ID:-}" ]; then
    echo "error: set HERDRES_OWNER_ID or TELEGRAM_ALLOWED_USERS in $HOME/.config/herdres/herdres.env" >&2
    exit 1
fi

if [ -n "${HERDR_COCKPIT_SHARE_CMD:-}" ]; then
    HERDR_SHARE_CMD="$HERDR_COCKPIT_SHARE_CMD"
elif [ -z "${HERDR_SHARE_CMD:-}" ]; then
    HERDR_SHARE_CMD="${HERDR_REAL_BIN:-$HOME/.local/bin/herdr}"
fi
export HERDR_SHARE_CMD

if [ -n "${HERDR_COCKPIT_HERDR_BIN:-}" ]; then
    HERDR_BIN="$HERDR_COCKPIT_HERDR_BIN"
elif [ -z "${HERDR_BIN:-}" ] || [ "$(basename "$HERDR_BIN")" = "herdr_turn_adapter.py" ]; then
    HERDR_BIN="${HERDR_REAL_BIN:-$HOME/.local/bin/herdr}"
fi
export HERDR_BIN

exec node "$HOME/.local/share/herdres/ssh/server/server.js"
EOF
chmod +x "$SHARE/herdres-sync.sh" "$SHARE/herdres-gateway.sh" "$SHARE/herdres-cockpit.sh"

for label in herdres herdres-gateway; do
    sed "s#__HOME__#$HOME#g" "$HERE/launchd/com.gaijinjoe.$label.plist" > "$LA/com.gaijinjoe.$label.plist"
done
sed "s#__HOME__#$HOME#g" "$HERE/ssh/com.gaijinjoe.herdres-cockpit.plist" > "$LA/com.gaijinjoe.herdres-cockpit.plist"

cat <<EOF

Installed. Finish with:
  1) edit $CFG/herdres.env
  2) herdr plugin link $SHARE/herdres-plugin
  3) launchctl bootstrap gui/\$(id -u) $LA/com.gaijinjoe.herdres.plist
  4) launchctl bootstrap gui/\$(id -u) $LA/com.gaijinjoe.herdres-gateway.plist
  5) launchctl bootstrap gui/\$(id -u) $LA/com.gaijinjoe.herdres-cockpit.plist
  6) tailscale serve --bg https / http://127.0.0.1:8787

Reload an agent after edits:
  launchctl bootout   gui/\$(id -u)/com.gaijinjoe.herdres-gateway
  launchctl bootstrap gui/\$(id -u) $LA/com.gaijinjoe.herdres-gateway.plist
EOF
