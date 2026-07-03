# Herdres Tendwired

This branch is a tiny source-mode-only Telegram connector for Tendwire.

Herdres does not observe or control Herdr directly here. Tendwire owns Herdr
observation, worker bindings, turns, pending interactions, command routing,
receipts, backend health, and connector outbox. Herdres owns Telegram polling,
topics, message send/edit, compact working updates, final response display, and
Telegram delivery dedup.

## Runtime

```text
Telegram topic input
  -> herdres-gateway
  -> herdres command
  -> tendwire command --json

herdres sync
  -> tendwire snapshot/turns/pending/connector
  -> Telegram topics/messages/pinned status
```

Only `HERDRES_TENDWIRE_MODE=source` is supported.

## Install

```sh
./install-user.sh
$EDITOR ~/.config/herdres/herdres.env
systemctl --user daemon-reload
systemctl --user enable --now herdres.timer herdres-gateway.service
```

Do not run the legacy `herdr-telegram-topics.timer` with this branch.

## Checks

```sh
HERDRES_TENDWIRE_MODE=source ./herdres.py doctor
HERDRES_TENDWIRE_MODE=source ./herdres.py tendwire source-smoke --with-outbox
```

`source-smoke` must report `direct_herdr_calls=0`.
