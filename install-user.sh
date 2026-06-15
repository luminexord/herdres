#!/usr/bin/env sh
set -eu

install -Dm755 herdres.py "$HOME/.local/bin/herdres"
install -Dm755 herdr_telegram_topics_install_bridge.py "$HOME/.local/bin/herdr_telegram_topics_install_bridge.py"
install -Dm644 .env.example "$HOME/.config/herdres/herdres.env"
install -Dm644 herdr_topic_bridge.py "$HOME/.local/share/herdr-telegram-topics/herdr_topic_bridge.py"
mkdir -p "$HOME/.config/systemd/user"
cp systemd/user/herdres.service systemd/user/herdres.timer "$HOME/.config/systemd/user/"

printf '%s\n' "Installed herdres."
printf '%s\n' "Edit $HOME/.config/herdres/herdres.env, then run:"
printf '%s\n' "  systemctl --user daemon-reload"
printf '%s\n' "  systemctl --user enable --now herdres.timer"
