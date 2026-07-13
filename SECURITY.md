# Security

The tendwired branch keeps direct Herdr access out of Herdres.

Herdres private state may contain Telegram chat/topic/message IDs, bot tokens
and routing/ownership, final message bindings, and stable-job checkpoints and
receipts. Retain that state across restart and never publish it. Public JSON
from Herdres commands is pruned so it does not expose tokens, socket paths, raw
backend targets, command stdout/stderr, Telegram IDs, or private checkpoints.

The connector boundary remains neutral. Herdres sends Tendwire only bounded
canonical character ranges, Tendwire-issued stable public identities/tokens
and job keys, leased `source_ref` values, and neutral outcome or reason codes.
It never sends Telegram chat/topic/message IDs, bot tokens or routing, or
provider error prose to Tendwire. Tendwire owns canonical content, durable
final-ready roots, range validation, jobs, leases, ACK/dead-letter state, and
retention; Herdres owns Telegram formatting and private provider state.

Dead-letter inspection is bounded and public-safe, and retry selects one exact
public `final_identity`; neither surface exposes Herdres's private checkpoint
or Telegram routing. Provider acceptance without a recorded receipt remains
ambiguous, so an explicit retry may duplicate a Telegram operation and must
not be represented as provider-perfect exactly-once.

The `final_ready` materialization-root payload uses exact integer
`schema_version: 2` and carries an exact public opaque `stable_key` plus integer
`stable_key_version: 1`. That pair binds retained work to the accepted worker
continuity identity; it is protocol metadata, not a private checkpoint or
secret. A schema-v1 root cannot authorize routing through reusable `worker_id`
or `space_id` values alone. Canonical descriptors and the public identity pair
may cross this boundary; Telegram routing, credentials, message state, and
private checkpoints never do.

## Stable worker handle boundary

Herdres treats a persisted worker entry as independently routable only when its
Tendwire public identity pair is exact:

- `meta.stable_key` is a string matching `wsk1_[0-9a-f]{64}` in full, with no
  whitespace, suffix, embedded metadata, uppercase hexadecimal, or other
  decoration.
- `meta.stable_key_version` is the integer `1` exactly. The string `"1"`,
  booleans, missing values, and other versions are invalid.

Both fields are required. A current worker with a missing, malformed, partial,
or unknown pair is quarantined before topic creation or selection and before
turn or reply routing. Persisted entries with absent identity or a legacy
24-character lowercase hexadecimal identity are not independently routable;
they are migration-only for a compatible current observation carrying an exact
valid-v1 pair.

This is protocol validation, **not cryptographic authentication**: an attacker
who can alter Tendwire's public output can supply an exact-format spoof.
Tendwire alone owns the 32-byte installation key used to derive handles.
Herdres never reads or stores that key, never receives raw pane or terminal
identity, never queries Herdr, and has no way to recompute or verify a handle.
Protect the local Tendwire CLI/daemon boundary and its output from untrusted
writers.

Tendwire keeps its continuity triplet at `data_dir/installation.key`, the
nonsecret digest marker `data_dir/installation.key.sha256`, and the one-byte
nonsecret ASCII `1` sentinel `data_dir/installation.key.initialized`. Its data
directory is mode `0700` and all three files are mode `0600`. Back up and
restore the Herdres state file, Tendwire database, key, marker, and sentinel as
one consistent set. Once initialized, ordinary key loading never rotates the
identity or replaces missing key material.

Deliberate rotation requires Tendwire and every identity consumer to be offline
and an explicit call to
`tendwire.worker_identity.reset_installation_key(data_dir,
acknowledge_continuity_break=True)`. A reset without that acknowledgement fails.
Rotation changes every handle and is not a recovery substitute for restoring
the paired backup.

Herdres preflights both fresh snapshot claims and persisted identities. Fresh or
persisted collisions are quarantined before topic creation or selection and
before turn or reply routing; they do not remain routable merely because stable
identity adoption was blocked. Repeated faulty snapshots update the same
quarantined claimant rather than creating duplicate state entries or topics. A
correctly shaped value never overrides a collision or a quarantined claimant.

Reply binding resolution also fails closed unless the resolved worker owns the
binding topic directly or through its matching Tendwire source-space topic.

A one-time migration can annotate only an unambiguous, live absent-identity or
legacy-24 entry for the same compatible current valid-v1 worker. It preserves
the existing topic, message bindings, and delivery ledgers and does not replay
already delivered turns. Ambiguity or a failed sanity check is quarantined
instead of being rebound.

Normal verification:

```sh
HERDRES_TENDWIRE_MODE=source ./herdres.py doctor
HERDRES_TENDWIRE_MODE=source ./herdres.py tendwire source-smoke --with-outbox
```

`direct_herdr_calls` must remain `0`.
