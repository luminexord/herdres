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

This is a paired command protocol, not a best-effort JSON integration. Herdres
sends schema-v1 requests and requires an exact, correlated schema-v2 response.
For a non-dry-run command, Tendwire CLI exit `0` must carry `ok: true`; exit
`1` must carry `ok: false`. Exit `2`, malformed output, or any exit/body
mismatch is private unproven process ambiguity and is not converted into a
forged disposition.

## Continuity data and upgrades

### Herdres ingress request-ID key

`./install-user.sh` initializes a dedicated raw 32-byte HMAC key once at
`HERDRES_REQUEST_ID_KEY_PATH`. Unset or empty selects
`~/.local/share/herdres/request-id.key`. A nondefault value is expanded for
`~` and must then be a nonempty absolute file path; set that identical path in
the installer's environment and `~/.config/herdres/herdres.env` before
starting the gateway. The value is a path, never key material. Do not paste,
encode, or otherwise copy the raw key bytes into that environment file.

The installer creates or tightens the owner-owned parent directory to mode
`0700` and atomically installs an owner-owned regular 32-byte file with mode
`0600`. Re-running the installer validates and preserves the existing key. It
does not rotate, repair, overwrite, or follow a symlink. At runtime the gateway
also refuses a missing, malformed, symlinked, incorrectly owned or permissioned,
or concurrently replaced key; runtime never creates one.

Herdres persists private state by flushing and fsyncing a temporary file,
atomically replacing the state path, and then fsyncing its parent directory.
This file-plus-directory barrier protects the ingress record needed for replay;
do not replace the state file with an editor write while services are running.

The key makes Telegram ingress request IDs stable across gateway restart,
Telegram redelivery, and managed-bot token rotation. Manager and managed-bot
polling offsets are keyed by stable receiving-bot kind rather than bot token;
the current legacy token-keyed managed-bot offset is migrated to that stable
path, so rotation does not reset its polling position. Back up the key with
Herdres state and restore the same file with mode `0600` before restarting the
gateway. Deleting, regenerating, or changing the key path without restoring
the original key changes every derived request ID and can make an already-seen
Telegram update appear to be a new mutation.

When `HERDRES_INBOUND_LANES=1`, the authoritative receiving-bot cursors and
pending updates live in the separate SQLite WAL selected by
`HERDRES_INBOUND_SPOOL_PATH` (default
`~/.local/share/herdres/inbound_spool.db`). The legacy offset files are only
atomic, fsynced rollback mirrors. Stop the gateway before backing up or
restoring the spool so SQLite can checkpoint its WAL consistently.

### Tendwire worker continuity key

Tendwire owns a 32-byte installation key at
`data_dir/installation.key` (normally
`~/.local/share/tendwire/installation.key`), a nonsecret digest marker at
`data_dir/installation.key.sha256`, and the one-byte nonsecret ASCII `1`
initialization sentinel at `data_dir/installation.key.initialized`. The Tendwire
data directory must be mode `0700`; all three files must be mode `0600`. Do not
copy a generated key value into configuration, examples, logs, or tickets.

Treat these six items as one operational backup and restore unit:

1. the Herdres state selected by `HERDR_TELEGRAM_TOPICS_STATE` (default
   `~/.local/share/herdres/state.json`), which contains private Telegram
   topic/message IDs, bot credentials and routing/ownership, ingress command
   request records, final bindings, and stable-job delivery
   checkpoints/receipts;
2. the Herdres request-ID key selected by `HERDRES_REQUEST_ID_KEY_PATH`;
3. the Tendwire database selected by `TENDWIRE_DB_PATH`;
4. Tendwire's `installation.key`;
5. Tendwire's `installation.key.sha256` marker; and
6. Tendwire's `installation.key.initialized` sentinel.

Take a consistent snapshot while writers are quiesced and restore the entire
six-item set together. Restoring only Herdres state, only either key, only the
Tendwire database, or an incomplete key/marker/sentinel triplet breaks the
continuity contract.
Once the sentinel records initialization, ordinary Tendwire key loading never
rotates the identity or silently replaces missing key material; incomplete,
mismatched, malformed, or unsafe state fails closed.

An ordinary service restart must retain the private Herdres state file,
Herdres request-ID key, stable polling-offset files, and Tendwire database
unchanged. Before private route reconstruction, Herdres returns any cached
terminal or quarantined child outcome for a redelivered request ID. A retained
retryable record replays only its stored exact UTF-8 request bytes; token,
routing, transcription, or worker-state churn cannot rebuild or replace them.
The sole exception is the one persisted removal of `worker_fingerprint` after
`stale_target` with disposition `no_receipt`. Terminal and quarantine cache
survives restart and route loss, bypasses Tendwire client construction, sends
the same sanitized reply, and advances the polling offset.

Herdres also resumes stable-job checkpoints under fresh transient lease refs,
ACKs already-applied work without repeating the Telegram operation, and
reconciles a completed pending plan when the final ACK response or
completed-plan observation was lost. A pending plan confirmed as `superseded`
or `plan_not_found` is cleared before its newer durable root is handled; every
other unresolved state continues to block the newer root. Do not clear or edit
either state store, polling offsets, or replace either key as part of a restart.

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

Tendwire store schema v14 final-ready materialization roots use exact payload
`schema_version: 2` and repeat that same public opaque `stable_key` plus exact
integer `stable_key_version: 1` to bind retained work to worker continuity.
Herdres never treats these public coordinates as private checkpoint data, and a
schema-v1 root cannot be routed by reusable worker or space IDs alone. No
Telegram routing, credentials, message state, or private checkpoint belongs in
the root.

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

Core environment (the shown state path and final-root lease are defaults):

```sh
HERDRES_TENDWIRE_MODE=source
HERDRES_SOURCE_TOPIC_MODE=space
HERDRES_DELETE_DONE_COUNCIL_TOPICS=1
TELEGRAM_BOT_TOKEN=...
HERDRES_TELEGRAM_CHAT_ID=...
HERDR_TELEGRAM_TOPICS_STATE=~/.local/share/herdres/state.json
HERDRES_REQUEST_ID_KEY_PATH=~/.local/share/herdres/request-id.key
HERDRES_COMMAND_RETRY_HORIZON_SECONDS=86400
HERDRES_INBOUND_LANES=0
HERDRES_INBOUND_DISPATCH_WORKERS=8
HERDRES_INBOUND_LANE_DEPTH=32
HERDRES_INBOUND_LANE_BACKOFF_SECONDS=2
TENDWIRE_DB_PATH=~/.local/share/tendwire/tendwire.db
HERDRES_TENDWIRE_TURN_FINAL_LEASE_SECONDS=60
```

`HERDRES_COMMAND_RETRY_HORIZON_SECONDS` fixes the retry deadline from the
record's first-seen time; it does not configure a sliding idle timeout. Unset,
empty, or invalid values use `86400` seconds, and configured values clamp to
`60` through `604800`. `updated_at`, retry, redelivery, terminalization, and a
later configuration change do not move the stored deadline. Equality expires
the request before another Tendwire client is created.

The local `retain_until` is also immutable: first-seen time plus the effective
horizon and a `86400`-second margin (`172800` by default, `86460` minimum,
`691200` maximum). A valid record remains through equality and is pruned only
after that instant, regardless of its update or terminal timestamps. Configure
the paired Tendwire service with
`TENDWIRE_COMMAND_RETRY_HORIZON_SECONDS >= HERDRES_COMMAND_RETRY_HORIZON_SECONDS`
and
`TENDWIRE_COMMAND_RECEIPT_RETENTION_SECONDS >= HERDRES_COMMAND_RETRY_HORIZON_SECONDS + 86400`.
Tendwire's `604800` retry, `2592000` receipt-age, and `4096` newest inactive
receipt defaults satisfy this; its receipt age has a `691200`-second floor and
must be strictly greater than its own retry horizon. The gateway reads the
Herdres variable from `~/.config/herdres/herdres.env`.

During the horizon, `no_receipt` and `in_progress` dispositions retain the
Telegram update for same-ID retry. `terminal_accepted` and
`terminal_rejected` advance. `terminal_uncertain`, deadline equality,
corrupt/conflicting durable evidence, or an unrecoverable local command
decision is quarantined (locally dead-lettered), caches the fixed sanitized
failure `Could not send safely. Refresh status and choose the target again.`,
and advances so later updates are not blocked. The cached outcome is reused
after redelivery or restart without another Tendwire call. A
`backend_unavailable` status follows `no_receipt` or `terminal_rejected`;
status text alone is never retry authority.

The final-root lease covers canonical paging, range-only presentation-plan
begin/part/commit staging, and ACK. It uses 900 seconds when unset, empty, or
invalid and clamps configured values to 60 through 3600 seconds. Keep it long
enough for the largest expected completed response. Tendwire owns durable
final-ready roots, jobs, leases, ACK/dead-letter state, and retention; Herdres
owns private Telegram provider state and stable-job checkpoints.

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
