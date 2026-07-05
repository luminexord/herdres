#!/usr/bin/env sh
set -eu

install -Dm755 herdres.py "$HOME/.local/bin/herdres"
install -Dm755 herdres_gateway.py "$HOME/.local/bin/herdres-gateway"
find herdres_connector -type f -name '*.py' | while IFS= read -r f; do
    install -Dm644 "$f" "$HOME/.local/bin/$f"
done
rm -f \
    "$HOME/.local/bin/herdres_tendwire.py" \
    "$HOME/.local/bin/herdres_routing.py" \
    "$HOME/.local/bin/herdres_gateway.py" \
    "$HOME/.local/bin/herdres-decision-hook" \
    "$HOME/.local/bin/herdres-speech" \
    "$HOME/.local/bin/herdr_telegram_topics_install_bridge.py" \
    "$HOME/.local/bin/herdres_connector/formatter.py" \
    "$HOME/.local/bin/herdres_connector/source_state.py"

# The monolith wrote herdres-decision-hook entries into the REAL ~/.claude/settings.json. We just
# removed the script (above); a dangling hook -> missing script blocks ALL Claude Code prompts, so
# strip those entries too. Guarded + backed up + never fails the install (best-effort).
CLAUDE_SETTINGS="$HOME/.claude/settings.json"
if [ -f "$CLAUDE_SETTINGS" ] && grep -q "herdres-decision-hook" "$CLAUDE_SETTINGS" 2>/dev/null && command -v python3 >/dev/null 2>&1; then
    cp -a "$CLAUDE_SETTINGS" "$CLAUDE_SETTINGS.bak-herdres-uninstall" 2>/dev/null || true
    python3 - "$CLAUDE_SETTINGS" <<'PY' || true
import json, sys
p = sys.argv[1]
try:
    d = json.load(open(p))
except Exception:
    sys.exit(0)
hooks = d.get("hooks")
if not isinstance(hooks, dict):
    sys.exit(0)
for event, groups in list(hooks.items()):
    if not isinstance(groups, list):
        continue
    new_groups = []
    for g in groups:
        inner = g.get("hooks", []) if isinstance(g, dict) else []
        kept = [h for h in inner if isinstance(h, dict) and "herdres-decision-hook" not in str(h.get("command", ""))]
        if kept:
            g["hooks"] = kept
            new_groups.append(g)
    if new_groups:
        hooks[event] = new_groups
    else:
        del hooks[event]
json.dump(d, open(p, "w"), indent=2)
PY
    printf '%s\n' "Removed stale herdres-decision-hook entries from ~/.claude/settings.json."
fi

[ -f "$HOME/.config/herdres/herdres.env" ] || \
    install -Dm600 .env.example "$HOME/.config/herdres/herdres.env"

mkdir -p "$HOME/.config/systemd/user" "$HOME/.local/share/herdres"
cp systemd/user/herdres.service systemd/user/herdres-gateway.service "$HOME/.config/systemd/user/"
rm -f "$HOME/.config/systemd/user/herdres.timer"
rm -f "$HOME/.config/systemd/user/herdres-speech.service"
rm -rf "$HOME/.config/systemd/user/herdres-gateway.service.d"
printf '%s\n' "$PWD" > "$HOME/.local/share/herdres/source"

printf '%s\n' "Installed source-only Herdres."
printf '%s\n' "Edit $HOME/.config/herdres/herdres.env, then run:"
printf '%s\n' "  systemctl --user daemon-reload"
printf '%s\n' "  systemctl --user enable --now herdres.service herdres-gateway.service"
