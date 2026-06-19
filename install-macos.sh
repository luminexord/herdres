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

mkdir -p "$BIN" "$CFG" "$CFG/managed-bots" "$SHARE/herdres-plugin" "$SHARE/inbound" "$LA"

# 2. Install scripts, pinning the shebang to the chosen interpreter
install_pinned() { install -m 755 "$HERE/$1" "$BIN/$2"; /usr/bin/sed -i '' "1s|.*|#!$PY|" "$BIN/$2"; }
install_pinned herdres.py            herdres
install_pinned herdr_turn_adapter.py herdr_turn_adapter.py
install_pinned herdres_gateway.py    herdres-gateway

if [ -d "$HERE/assets/managed-bots" ]; then
    for photo in codex claude kimi omp; do
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

# 5. launchd wrappers (mirror systemd EnvironmentFile)
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
chmod +x "$SHARE/herdres-sync.sh" "$SHARE/herdres-gateway.sh"

# 6. launchd agents (materialize templates)
for label in herdres herdres-gateway; do
    sed "s#__HOME__#$HOME#g" "$HERE/launchd/com.gaijinjoe.$label.plist" > "$LA/com.gaijinjoe.$label.plist"
done

cat <<EOF

Installed. Finish with:
  1) edit $CFG/herdres.env
  2) herdr plugin link $SHARE/herdres-plugin
  3) launchctl bootstrap gui/\$(id -u) $LA/com.gaijinjoe.herdres.plist
  4) launchctl bootstrap gui/\$(id -u) $LA/com.gaijinjoe.herdres-gateway.plist

Reload an agent after edits:
  launchctl bootout   gui/\$(id -u)/com.gaijinjoe.herdres-gateway
  launchctl bootstrap gui/\$(id -u) $LA/com.gaijinjoe.herdres-gateway.plist
EOF
