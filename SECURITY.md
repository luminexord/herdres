# Security

The tendwired branch keeps direct Herdr access out of Herdres.

Herdres private state may contain Telegram chat/topic/message IDs, bot tokens
and routing/ownership, ingress command request records, final message bindings,
and stable-job checkpoints and receipts. Retain that state across restart and
never publish it. Public JSON from Herdres commands is pruned so it does not
expose tokens, socket paths, raw backend targets, command stdout/stderr,
Telegram IDs, or private checkpoints.

The connector boundary remains neutral. Herdres sends Tendwire only bounded
canonical character ranges, Tendwire-issued stable public identities/tokens
and job keys, leased `source_ref` values, and neutral outcome or reason codes.
It never sends Telegram chat/topic/message IDs, bot tokens or routing, or
provider error prose to Tendwire. Tendwire owns canonical content, durable
final-ready roots, range validation, jobs, leases, ACK/dead-letter state, and
retention; Herdres owns Telegram formatting and private provider state.

When Herdres invokes the Tendwire CLI, it builds a child environment that keeps
public Tendwire overrides but removes Telegram variables and private ingress,
gateway, managed-bot, state, request-key, and binary-selector variables. This
prevents inherited bot credentials and private connector paths from crossing
the process boundary even when an explicit Tendwire command is configured.

## Inbound command request identity

`install-user.sh` creates a dedicated private 32-byte raw HMAC key at
`HERDRES_REQUEST_ID_KEY_PATH`. Unset or empty selects
`~/.local/share/herdres/request-id.key`; every configured value is expanded for
`~` and must then be a nonempty absolute path. The owner-owned parent directory
is mode `0700`; the owner-owned regular key file is mode `0600`. Existing valid
material is preserved. Symlinks, unsafe ownership or permissions, malformed
length, missing material, and replacement during validation fail closed.
Runtime loads the key but never creates, repairs, or rotates it. Never put raw
key bytes in environment variables, examples, logs, tickets, or source.

The canonical public form is `hri1_` followed by the 43-character unpadded
URL-safe base64 encoding of an HMAC-SHA256 digest. Its versioned MAC scope is
exactly the stable receiving-bot identity and Telegram `update_id`, `chat_id`,
and `message_id`. It excludes bot tokens, text, topic/reply metadata, Telegram
user identity, and resolved Tendwire target. Managed-bot polling offsets use
the same stable bot kind rather than a token-derived runtime key, with the
current legacy token-keyed offset migrated to the stable path. Token rotation
therefore preserves both polling position and request identity without
disclosing raw coordinates to Tendwire. The ID is an idempotency coordinate,
not an authentication credential; Tendwire does not receive the key and cannot
recompute it.

Every distinct Telegram update receives a distinct ID, even if two messages
have identical text. Herdres performs no content-based command suppression.
The gateway derives the ID and durably creates fixed first-seen lifecycle
bounds before private routing, transcription, or child creation. It checks a
retained terminal/quarantine cache before those operations. Before the first
Tendwire call it stores canonical schema-v1 request JSON and fsyncs the
temporary file, atomic replacement, and parent directory; retries pass the
stored UTF-8 bytes verbatim.

The only authorized byte rewrite is one `stale_target` response carrying
disposition `no_receipt`: Herdres may remove only `worker_fingerprint`, must
persist the resulting exact bytes, and reuses the same request ID. No other
status, route change, transcription, or worker observation may rebuild the
request.

Herdres treats the Tendwire schema-v2 disposition and the CLI exit/body pair as
one authority. It requires the exact response field set and request
correlation, validates public pruning and the complete
disposition/`ok`/`status`/result/error tuple, and accepts only exit `0` with
`ok: true` or exit `1` with `ok: false`. Status alone is not authority:
`backend_unavailable` plus `no_receipt` is retryable, whereas
`backend_unavailable` plus `terminal_rejected` is terminal. Exit `2`,
timeout, malformed or non-UTF-8 output, a wrong field/correlation/tuple, or an
exit/body mismatch supplies no proven schema-v2 envelope. Herdres keeps that
process evidence private, exposes no marker/stdout/stderr, forges no
disposition, and retries only the already durable request within its fixed
deadline.

`HERDRES_COMMAND_RETRY_HORIZON_SECONDS` defaults/falls back to `86400` seconds
and clamps to `60` through `604800`. The stored deadline is first-seen plus
that value; `updated_at`, redelivery, retry, and configuration changes never
slide it, and equality expires. Local retention is also first-seen based:
horizon plus a `86400`-second margin (`172800` default, `86460` minimum,
`691200` maximum). A valid record is pruned only when `now > retain_until`.
Pair Tendwire retry age at or above the Herdres horizon and Tendwire receipt
age at or above the Herdres retention bound; Tendwire's defaults are `604800`
and `2592000`, with a newest-`4096` inactive-receipt floor.

An authoritative `terminal_uncertain`, deadline expiry, or corrupt/conflicting
receipt evidence is quarantined (locally dead-lettered) rather than retried
forever. Terminal rejection and quarantine cache the fixed sanitized reply
`Could not send safely. Refresh status and choose the target again.` in an
`advance` child outcome. The gateway sends the reply, advances the stable
polling offset, and permits later updates; restart/redelivery reuses the cache
without a Tendwire call. This local ingress quarantine is distinct from
Tendwire's final-delivery dead-letter queue.

Only schema/action, opaque request ID, `dry_run`, a public target, and
instruction text can cross to Tendwire. Tendwire's public request-ID grammar is
`[A-Za-z0-9._-]{1,128}` while Herdres generates its narrower canonical
`hri1_…` value. Raw Telegram receiver, update, chat, topic, message, reply, or
user IDs, bot tokens, and private routes/backend targets cannot cross.

Back up and restore the request-ID key together with private Herdres state and
the Tendwire database/continuity key set while writers are quiesced. Preserve
mode `0600` on restore. Replacing this key changes every request ID and can
bypass continuity for a redelivered mutation; key regeneration is not a
recovery operation.

## Final delivery ambiguity

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
the existing topic, message bindings, and delivery ledgers and does not replay
already delivered turns. Ambiguity or a failed sanity check is quarantined
instead of being rebound.

Normal verification:

```sh
HERDRES_TENDWIRE_MODE=source ./herdres.py doctor
HERDRES_TENDWIRE_MODE=source ./herdres.py tendwire source-smoke --with-outbox
```

`direct_herdr_calls` must remain `0`.
