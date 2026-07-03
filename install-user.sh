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
install -Dm644 herdres_tendwire.py "$HOME/.local/bin/herdres_tendwire.py"
find herdres_connector -type f -name '*.py' | while IFS= read -r f; do
    install -Dm644 "$f" "$HOME/.local/bin/$f"
done
# Optional local speech engine (issue #4): herdres imports it best-effort, so it must sit next to the
# CLI on the import path. Heavy deps (sherpa-onnx + models) are opt-in via `herdres speech install`.
install -Dm644 herdres_speech.py "$HOME/.local/bin/herdres_speech.py"
# Warm speech sidecar (issue #4 v2) — installed but NOT enabled by default (opt-in; needs the models).
install -Dm755 herdres-speech "$HOME/.local/bin/herdres-speech"
# Claude Code hook (issue #36): mirrors a pending AskUserQuestion/ExitPlanMode to Telegram as
# tappable buttons. Install the script, then register it in ~/.claude/settings.json (idempotent,
# coexists with other hooks; no-op if Claude Code isn't installed).
install -Dm755 herdres_decision_hook.py "$HOME/.local/bin/herdres-decision-hook"
"$HOME/.local/bin/herdres" hooks install >/dev/null 2>&1 || true
install -d "$HOME/.local/share/herdres/herdres-plugin"
sed "s#\\[\"herdres\", #\\[\"$HOME/.local/bin/herdres\", #g" \
    herdres-plugin/herdr-plugin.toml > "$HOME/.local/share/herdres/herdres-plugin/herdr-plugin.toml"
# Record this checkout's absolute path so `herdres update --edge` can git pull it.
# Use $PWD: the installs above are cwd-relative (e.g. `install -Dm755 herdres.py`),
# so this script must be run from the checkout root and $PWD *is* that checkout.
install -d "$HOME/.local/share/herdres"
printf '%s\n' "$PWD" > "$HOME/.local/share/herdres/source"
mkdir -p "$HOME/.config/systemd/user"
cp systemd/user/herdres.service systemd/user/herdres.timer systemd/user/herdres-gateway.service systemd/user/herdres-speech.service "$HOME/.config/systemd/user/"

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
