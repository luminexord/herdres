#!/usr/bin/env sh
set -eu

install -Dm755 herdres.py "$HOME/.local/bin/herdres"
install -Dm755 herdr_telegram_topics_install_bridge.py "$HOME/.local/bin/herdr_telegram_topics_install_bridge.py"
# Never clobber an existing config: re-install becomes a safe update.
[ -f "$HOME/.config/herdres/herdres.env" ] || \
    install -Dm644 .env.example "$HOME/.config/herdres/herdres.env"
install -Dm644 herdr_topic_bridge.py "$HOME/.local/share/herdres/herdr_topic_bridge.py"
# Multi-token standalone inbound gateway (manager + per-agent child bots).
# herdres_routing.py must sit next to it on the import path.
install -Dm755 herdres_gateway.py "$HOME/.local/bin/herdres-gateway"
install -Dm644 herdres_routing.py "$HOME/.local/bin/herdres_routing.py"
install -d "$HOME/.local/share/herdres/herdres-plugin"
sed "s#\\[\"herdres\", #\\[\"$HOME/.local/bin/herdres\", #g" \
    herdres-plugin/herdr-plugin.toml > "$HOME/.local/share/herdres/herdres-plugin/herdr-plugin.toml"
# Record this checkout's absolute path so `herdres update --edge` can git pull it.
install -d "$HOME/.local/share/herdres"
printf '%s\n' "$(cd "$(dirname "$0")" && pwd)" > "$HOME/.local/share/herdres/source"
mkdir -p "$HOME/.config/systemd/user"
cp systemd/user/herdres.service systemd/user/herdres.timer systemd/user/herdres-gateway.service "$HOME/.config/systemd/user/"

printf '%s\n' "Installed herdres."
printf '%s\n' "Edit $HOME/.config/herdres/herdres.env, then run:"
printf '%s\n' "  systemctl --user daemon-reload"
printf '%s\n' "  systemctl --user enable --now herdres.timer"
printf '%s\n' "Optional multi-token inbound gateway (manager + per-agent child bots)."
printf '%s\n' "Run either this gateway OR Hermes on the manager bot token, never both."
printf '%s\n' "Before re-installing/restarting the gateway, release the old getUpdates lease:"
printf '%s\n' "  systemctl --user disable --now herdres-gateway.service"
printf '%s\n' "Then reload and start it:"
printf '%s\n' "  systemctl --user daemon-reload && systemctl --user enable --now herdres-gateway.service"
printf '%s\n' "If your env only has HERDRES_GATEWAY_BOT_TOKEN, also set TELEGRAM_BOT_TOKEN."
