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

By default Herdres creates one Telegram topic per Tendwire space:

```sh
HERDRES_SOURCE_TOPIC_MODE=space
```

Use worker/pane topics only when explicitly wanted:

```sh
HERDRES_SOURCE_TOPIC_MODE=worker
```

Finished council/gitmoot/gm worker topics are deleted automatically when
`HERDRES_DELETE_DONE_COUNCIL_TOPICS=1`.

Rich Telegram messages are enabled by default. Final responses render as open
rich content; working updates render as compact editable updates.

Optional per-agent bot identities are configured with generic private tokens:

```sh
HERDR_TELEGRAM_TOPICS_MANAGED_BOTS=1
HERDRES_MANAGED_BOT_CODEX_TOKEN=...
HERDRES_MANAGED_BOT_CLAUDE_TOKEN=...
```

When enabled, Herdres polls configured child bots as well as the manager bot.
Use `/voice per_agent` or `/voice shared` inside a space topic to switch that
space. Unconfigured agent kinds fall back to the manager bot. Do not commit real
bot tokens or local bot names.

Telegram voice notes are separate from bot identity. Inbound audio transcription
is local and opt-in:

```sh
pip install sherpa-onnx
herdres speech install
HERDR_TELEGRAM_TOPICS_SPEECH_INPUT=1
```

The gateway downloads a voice note with the bot token that received it, deletes
the temporary audio after transcription, and sends only the transcript through
Tendwire.

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
