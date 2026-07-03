# Install

This branch installs only:

- `herdres`
- `herdres-gateway`
- `herdres_connector/*.py`
- `herdres.service`
- `herdres.timer`
- `herdres-gateway.service`

```sh
./install-user.sh
```

Required env:

```sh
HERDRES_TENDWIRE_MODE=source
TELEGRAM_BOT_TOKEN=...
HERDRES_TELEGRAM_CHAT_ID=...
TENDWIRE_DB_PATH=~/.local/share/tendwire/tendwire.db
```

Start only the source connector services:

```sh
systemctl --user daemon-reload
systemctl --user enable --now herdres.timer herdres-gateway.service
```

`herdr-server.service` is not managed by Herdres.
