# Install

This branch installs only:

- `herdres`
- `herdres-gateway`
- `herdres_connector/*.py`
- `herdres.service` (source sync loop; replaces the old `herdres.timer`)
- `herdres-gateway.service`

```sh
./install-user.sh
```

Required env:

```sh
HERDRES_TENDWIRE_MODE=source
HERDRES_SOURCE_TOPIC_MODE=space
HERDRES_DELETE_DONE_COUNCIL_TOPICS=1
TELEGRAM_BOT_TOKEN=...
HERDRES_TELEGRAM_CHAT_ID=...
TENDWIRE_DB_PATH=~/.local/share/tendwire/tendwire.db
```

Start only the source connector services:

```sh
systemctl --user daemon-reload
systemctl --user enable --now herdres.service herdres-gateway.service
```

`herdr-server.service` is not managed by Herdres. There is no `herdres.timer`
on this branch; `herdres.service` runs the sync loop directly.

## Rollback

This branch is source-only and does not support disabling Tendwire via
environment. To roll back, switch to a legacy (non-tendwired) Herdres branch or
tag and reinstall — it is a code switch, not `HERDRES_TENDWIRE_MODE=off`:

```sh
systemctl --user disable --now herdres.service herdres-gateway.service
git checkout <legacy-herdres-tag>
./install-user.sh
```

Optional inbound voice-note transcription is disabled by default. To enable it:

```sh
python3 -m venv ~/.local/share/herdres/speech-venv
uv pip install --python ~/.local/share/herdres/speech-venv/bin/python sherpa-onnx numpy
~/.local/share/herdres/speech-venv/bin/python ~/.local/bin/herdres speech install
HERDR_TELEGRAM_TOPICS_SPEECH_INPUT=1
systemctl --user restart herdres-gateway.service
```
