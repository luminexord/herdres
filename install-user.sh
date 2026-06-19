#!/usr/bin/env sh
set -eu

install -Dm755 herdres.py "$HOME/.local/bin/herdres"
install -Dm755 herdr_telegram_topics_install_bridge.py "$HOME/.local/bin/herdr_telegram_topics_install_bridge.py"
install -Dm644 .env.example "$HOME/.config/herdres/herdres.env"
install -Dm644 herdr_topic_bridge.py "$HOME/.local/share/herdres/herdr_topic_bridge.py"
# Optional standalone inbound gateway (alternative to routing inbound through the
# Hermes bridge). herdres_routing.py must sit next to it on the import path.
install -Dm755 herdres-gateway.py "$HOME/.local/bin/herdres-gateway"
install -Dm644 herdres_routing.py "$HOME/.local/bin/herdres_routing.py"
install -d "$HOME/.local/share/herdres/herdres-plugin"
sed "s#\\[\"herdres\", #\\[\"$HOME/.local/bin/herdres\", #g" \
    herdres-plugin/herdr-plugin.toml > "$HOME/.local/share/herdres/herdres-plugin/herdr-plugin.toml"
mkdir -p "$HOME/.config/systemd/user"
cp systemd/user/herdres.service systemd/user/herdres.timer systemd/user/herdres-gateway.service "$HOME/.config/systemd/user/"

printf '%s\n' "Installed herdres."
printf '%s\n' "Edit $HOME/.config/herdres/herdres.env, then run:"
printf '%s\n' "  systemctl --user daemon-reload"
printf '%s\n' "  systemctl --user enable --now herdres.timer"
printf '%s\n' "Optional standalone inbound gateway (only if herdres owns its bot token and"
printf '%s\n' "nothing else polls getUpdates for it — never run alongside Hermes polling):"
printf '%s\n' "  systemctl --user enable --now herdres-gateway.service"
