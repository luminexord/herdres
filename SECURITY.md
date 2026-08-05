# Security

The tendwired branch keeps direct Herdr access out of Herdres.

Herdres private state may contain Telegram chat/topic/message IDs, bot tokens
and routing/ownership, ingress command request records, exact Telegram message
bindings, delivered identities used for deduplication, and compact
accepted-topic receipts. Retain that state across restart and never publish it.
Public JSON from Herdres commands is pruned so it does not
expose tokens, socket paths, raw backend targets, command stdout/stderr,
Telegram IDs, exact message bindings, delivered identities, or ingress state.

The connector boundary remains neutral. Herdres sends Tendwire only bounded
canonical character ranges, Tendwire-issued stable public identities/tokens
and job keys, leased `source_ref` values, and neutral outcome or reason codes.
It never sends Telegram chat/topic/message IDs, bot tokens or routing, or
provider error prose to Tendwire. Tendwire exclusively owns canonical content,
durable final-ready roots, range validation, connector jobs, leases, attempts,
recovery, ACK/dead-letter state, and retention. Herdres owns Telegram
formatting and private provider state.

Herdres never invokes a Tendwire CLI or child process. Its bounded client talks
only to the configured owner-private AF_UNIX daemon endpoint, so no inherited
Telegram credentials, private connector paths, child environment, or process
output crosses the boundary.

## Inbound queue, identity, and provider serialization

Independent H8 has one durable ingress store:
`~/.local/share/herdres/inbound_spool.db`. The writer pins an absolute
owner-private parent, uses no-follow directory-relative opens, verifies stable
device/inode identity, and requires the database, writer lock, WAL, and SHM to
be EUID-owned regular single-link mode-`0600` files. The parent and physical
provider-lock directory are mode `0700`. Wrong schema, ownership, permissions,
type, link count, symlink, replacement, partial sidecars, or failed integrity
checks fail closed; runtime never chmod-repairs or silently recreates an unsafe
existing store.

The queue is schema 1 with one exclusive writer, WAL, `synchronous=FULL`,
`trusted_schema=OFF`, bounded busy time, fixed depth/lease/deadline/retention
limits, and atomic receiver-cursor acceptance. A separate observer is
query-only and exposes aggregate health/status only. There is no lane spool,
JSON ingress ledger in state, second database, child process, CLI fallback, or
receipt-derived presentation shortcut.

`install-user.sh` creates one private 32-byte HMAC key at
`HERDRES_REQUEST_ID_KEY_PATH` (default
`~/.local/share/herdres/request-id.key`). Runtime accepts only an EUID-owned
regular single-link mode-`0600` file of exactly 32 bytes under a private parent,
pins its inode during reading, and never creates, repairs, rotates, serializes,
or logs it. The canonical public ID is `hri1_` plus the 43-character unpadded
base64url HMAC-SHA256 digest over receiver identity and Telegram
update/chat/message coordinates. Tokens, text, sender, reply metadata, and
resolved route are excluded.

Ingress reads legacy schema-2 state only through frozen typed operations.
Callers receive opaque `StateToken` values, bounded public route/decision data,
and slotted `SecretStr` receiver credentials that reject display, pickling, and
dataclass serialization. Each secret is revealed only directly into one
`TelegramClient`; it never enters the queue, another dataclass, an exception,
status, or log.

Decision edits and deletes are serialized per exact `PhysicalOwner` by the
shared `provider_mutation_guard`. Its sibling lock filename is `pg1.<43>`, the
unpadded base64url HMAC-SHA256 of the three owner fields under the request-ID
key and a separate domain. No chat/topic/account coordinate appears in the
namespace. Waiting has a monotonic deadline and never holds the state flock or
a queue transaction. Typed mutations use composite
`(request_id, DecisionMutationKind)` idempotency plus a canonical mutation
digest, so selection and provider-markup checkpoints replay independently and
conflicting bytes fail closed.

Before AF_UNIX mutation the queue durably checkpoints the exact canonical
command or local action. `TendwireClient.command_json()` is the only command
transport; no subprocess or database watcher exists. Definitely-not-started
work may retry the same bytes. Started ambiguity cannot be converted into a new
send or live-route substitute; it follows the bounded retry/quarantine rule for
that exact operation. Only public allowlisted command fields cross to Tendwire.
Raw Telegram coordinates, credentials, provider bindings, queue rows, private
routes, and process output do not.

Back up and restore the queue DB/WAL/SHM, request-ID key, private Herdres state,
and Tendwire database/continuity family together while all writers are stopped.
Key regeneration is not recovery: it changes request and provider-lock
identities. The old presenter remains until paired H7/H6 and shares the same
physical-owner guard, but never opens the H8 queue.
## Final delivery ambiguity

Dead-letter inspection is bounded and public-safe, and retry selects one exact
public `final_identity`; neither surface exposes Herdres's exact Telegram
bindings, delivered identities, or provider routing. Provider acceptance
without an exact message binding remains ambiguous, so an explicit retry may
duplicate a Telegram operation and must not be represented as provider-perfect
exactly-once.

The `final_ready` materialization-root payload uses exact integer
`schema_version: 2` and carries an exact public opaque `stable_key` plus integer
`stable_key_version: 1`. That pair binds retained work to the accepted worker
continuity identity; it is protocol metadata, not a private Telegram binding,
delivered identity, or secret. A schema-v1 root cannot authorize routing through
reusable `worker_id` or `space_id` values alone. Canonical descriptors and the
public identity pair may cross this boundary; Telegram routing, credentials,
message state, exact bindings, and delivered identities never do.

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
Protect the local Tendwire daemon boundary and its output from untrusted
writers.

Tendwire keeps its continuity triplet at `data_dir/installation.key`, the
nonsecret digest marker `data_dir/installation.key.sha256`, and the one-byte
nonsecret ASCII `1` sentinel `data_dir/installation.key.initialized`. Its data
directory is mode `0700` and all three files are mode `0600`. Back up and
restore the Herdres state file, Herdres request-ID key, Tendwire database,
Tendwire key, marker, and sentinel as one consistent set. Once initialized,
ordinary Tendwire key loading never rotates the worker identity or replaces
missing key material.

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
the existing topic, exact message bindings, and delivered identities used for
deduplication and does not replay already delivered turns. Ambiguity or a failed
sanity check is quarantined instead of being rebound.

Normal verification:

```sh
HERDRES_TENDWIRE_MODE=source ./herdres.py doctor
HERDRES_TENDWIRE_MODE=source ./herdres.py tendwire source-smoke --with-outbox
```

`direct_herdr_calls` must remain `0`.
