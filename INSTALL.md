# Install

## Prerequisite: Tendwire

Herdres does not function on its own. It has no observation, worker identity,
turn/pending, command routing, or backend-health logic of its own — every one of
those comes from Tendwire over the `tendwire` CLI/daemon. Install Tendwire first:

```sh
git clone https://github.com/plotarmordev/tendwire.git ~/tendwire
cd ~/tendwire
python3 -m venv .venv && . .venv/bin/activate
python -m pip install -e .
tendwire doctor --json
```

See [Tendwire's own INSTALL.md](https://github.com/plotarmordev/tendwire/blob/main/INSTALL.md)
for the `tendwired.service` daemon setup. Herdres finds Tendwire via the
`tendwire` binary on `PATH`, or falls back to `TENDWIRE_SOURCE_DIR`
(default `~/tendwire/src`) and runs it as `python -m tendwire.cli`.

## Continuity data and upgrades

Tendwire owns a 32-byte installation key at
`data_dir/installation.key` (normally
`~/.local/share/tendwire/installation.key`), a nonsecret digest marker at
`data_dir/installation.key.sha256`, and the one-byte nonsecret ASCII `1`
initialization sentinel at `data_dir/installation.key.initialized`. The Tendwire
data directory must be mode `0700`; all three files must be mode `0600`. Do not
copy a generated key value into configuration, examples, logs, or tickets.

Treat these five items as one operational backup and restore unit:

1. the Herdres state selected by `HERDR_TELEGRAM_TOPICS_STATE` (default
   `~/.local/share/herdres/state.json`), which contains topic/message bindings
   and delivery ledgers;
2. the Tendwire database selected by `TENDWIRE_DB_PATH`;
3. Tendwire's `installation.key`;
4. Tendwire's `installation.key.sha256` marker; and
5. Tendwire's `installation.key.initialized` sentinel.

Take a consistent snapshot while writers are quiesced and restore the entire
paired set together. Restoring only Herdres state, only the Tendwire database,
or an incomplete key/marker/sentinel triplet breaks the continuity contract.
Once the sentinel records initialization, ordinary Tendwire key loading never
rotates the identity or silently replaces missing key material; incomplete,
mismatched, malformed, or unsafe state fails closed.

An ordinary upgrade must retain this state set. Persisted Herdres workers with
absent identity or a legacy 24-character lowercase hexadecimal identity are not
independently routable. They are eligible only for one-time migration when a
compatible current observation supplies an exact valid-v1 identity for the same
unambiguous, live worker. The migration retains the existing Telegram topic,
message bindings, and delivery ledgers, does not replay delivered turns, and
does not change `HERDRES_SOURCE_TOPIC_MODE` or any Telegram topic deletion
policy.

Herdres independently routes only persisted, live, nonquarantined entries with
the exact v1 identity pair. Before topic creation or selection, and before turn
or reply routing, it quarantines missing, malformed, partial, or unknown
identity and fresh-snapshot or persisted collisions. Quarantined claimants do
not receive or select topics, and repeated faulty snapshots do not create
duplicate state entries or topics. A reply binding additionally resolves only
when the resolved worker directly owns its topic or owns it through the
worker's matching Tendwire source-space topic.

Tendwire preserves a handle for moves within the same workspace/tab and
intentionally changes it across workspaces. If continuity state is lost,
replaced, malformed, or mismatched, stop writers and restore the complete paired
backup; do not edit Herdres state, copy a handle, delete individual key files,
or use rotation as recovery.

A deliberate rotation is a separate destructive operation. With Tendwire,
Herdres, and all identity users offline, the supported reset is
`tendwire.worker_identity.reset_installation_key(data_dir,
acknowledge_continuity_break=True)`. The reset fails without that explicit
acknowledgement; after it succeeds, the next ordinary load may bootstrap a new
key, marker, and sentinel. Rotation changes every handle. Keep Herdres offline
until the resulting old-binding quarantine and new identities have been
reviewed and explicitly migrated or retired.

Herdres sees only Tendwire's public handle and version. It validates their
exact v1 shape but does not possess the HMAC key, cannot cryptographically
authenticate an exact-format spoof, never reads raw pane identity, and never
queries Herdr.

## Herdres itself

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
