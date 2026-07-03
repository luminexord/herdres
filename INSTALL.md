# Install

Herdres is a local Telegram connector. For the release-candidate source-mode
setup, run Tendwire as the Herdr source/control plane and Herdres as the Telegram
connector.

## Files

Install the Herdres scripts from this checkout into a local bin directory, or use
the existing installer:

```bash
./install-user.sh
```

Create a local environment file from `.env.example`:

```bash
install -d ~/.config/herdres ~/.local/share/herdres
cp .env.example ~/.config/herdres/herdres.env
chmod 600 ~/.config/herdres/herdres.env
```

Fill in `TELEGRAM_BOT_TOKEN`, `HERDR_TELEGRAM_TOPICS_CHAT_ID`, and
`TELEGRAM_ALLOWED_USERS`. Keep `HERDRES_TENDWIRE_MODE=source` for normal daily
source-mode use and keep `HERDRES_TENDWIRE_DB_PATH` aligned with the Tendwire
daemon store. `HERDRES_TENDWIRE_SOURCE_COMPACT_RESPONSES=1` keeps source-mode
turn updates short in Telegram while preserving the full expandable response.

## Services

Install and enable the Herdres timer and gateway:

```bash
cp systemd/user/herdres.* ~/.config/systemd/user/
systemctl --user daemon-reload
systemctl --user enable --now herdres.timer herdres-gateway.service
systemctl --user disable --now herdr-telegram-topics.timer 2>/dev/null || true
```

The required source-mode service shape is:

- `tendwired.service` active and enabled
- `herdres.timer` active and enabled
- `herdres-gateway.service` active and enabled
- `herdr-telegram-topics.timer` inactive

## Verification

```bash
herdres doctor
herdres tendwire source-smoke --with-outbox
herdres topic-cleanup-report
HERDRES_TENDWIRE_MODE=source herdres sync
HERDRES_TENDWIRE_MODE=source herdres sync
```

The source smoke must report `direct_herdr_calls=0`. On a quiet system the second
sync should not repost completed-turn feed text.

Run `herdres topic-cleanup-report` before deleting or pruning old topics. It is
read-only and reports candidate stale source, pseudo-pane, duplicate-topic, and
orphan-space records using stable topic refs rather than raw Telegram topic IDs.

## Rollback

To roll back without deleting Telegram state, change only:

```bash
HERDRES_TENDWIRE_MODE=enrich
# or:
HERDRES_TENDWIRE_MODE=off
HERDRES_TENDWIRE_CONNECTOR_OUTBOX=0
```

Then restart `herdres.timer` and `herdres-gateway.service`. Re-enable the legacy
`herdr-telegram-topics.timer` only for an intentional `off`-mode rollback where
no other Herdres source-mode process is writing the same Telegram state.
