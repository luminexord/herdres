#!/usr/bin/env sh
set -eu

install -Dm755 herdres.py "$HOME/.local/bin/herdres"
install -Dm755 herdres_gateway.py "$HOME/.local/bin/herdres-gateway"
# Turn adapter: the tendwire daemon captures turn content by running `herdr pane turn`, which some
# herdr builds lack. tendwired.service must set TENDWIRE_HERDR_BIN to this adapter (and
# HERDR_REAL_BIN to the real herdr) or turn finals are never captured — the topic then shows
# "Work is in progress" forever. See systemd/user/tendwired.service.example.
install -Dm755 herdr_turn_adapter.py "$HOME/.local/bin/herdr_turn_adapter.py"
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
