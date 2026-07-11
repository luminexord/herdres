# Release checklist (Herdres Goal 05B / tendwired source-only RC)

Build release artifacts from a **clean git checkout only**. Never zip the working
directory directly — it can contain `__pycache__/`, `*.pyc`, `.pytest_cache/`,
and local state/tokens that must not ship. `.gitignore` already excludes these,
so building from git is what guarantees a clean artifact.

## 1. Preconditions

```sh
git status --porcelain            # must be empty
python -m py_compile $(find . -name '*.py' -not -path './.git/*')
HOME="$HOME" PYTHONPATH=. python -m pytest -q     # all green
```

Before verifying an existing installation's state, copy it first and point the
dry source check at the private copy, never the live file:

```sh
state_path="${HERDR_TELEGRAM_TOPICS_STATE:-$HOME/.local/share/herdres/state.json}"
backup_path="${state_path}.pre-source-v1"
cp -p -- "$state_path" "$backup_path"
HERDR_TELEGRAM_TOPICS_STATE="$backup_path" \
  HERDRES_TENDWIRE_MODE=source \
  ./herdres.py tendwire source-smoke --with-outbox
```

The check must succeed against a compatible Tendwire store-schema-v7 producer
using turn-list schema `2`, content-schema-v1 descriptors/pages, and the
turn-final prepare/lease/ACK/recovery protocol, with
`direct_herdr_calls=0`. It must not save the copied Herdres state or send/edit
Telegram messages. Keep both files private, and do not proceed to a live
reconciliation if the dry result is non-success.

## 2. Paired continuity, delivery, and recovery verification

Verify the Tendwire producer/recovery contract and the Herdres
consumer/reconciliation contract together as one compatible release pair:

```sh
# From the Tendwire checkout:
python -m pytest -q \
  tests/test_worker_stable_key.py \
  tests/test_herdr_turns.py \
  tests/test_turns.py \
  tests/test_connector_outbox.py \
  tests/test_store.py

# From the Herdres checkout:
python -m pytest -q \
  tests/test_source_only.py \
  tests/test_stable_worker_key.py \
  tests/test_tendwire_client.py \
  tests/test_turn_final_delivery.py \
  tests/test_offlock_delivery.py
HERDRES_TENDWIRE_MODE=source ./herdres.py tendwire source-smoke --with-outbox
```

The paired gate must establish all of the following:

- Tendwire retains a 32-byte installation key, matching digest marker, and
  one-byte nonsecret ASCII `1` initialization sentinel with the required
  `0700` data-directory and `0600` file modes. Once initialized, ordinary load
  never rotates or silently replaces missing key material.
- Reset and rotation require all identity users to be offline and the explicit
  `acknowledge_continuity_break=True` acknowledgement. Rotation changes every
  handle and is not an ordinary upgrade or recovery path.
- Only a persisted, live, nonquarantined identity containing a full
  `wsk1_[0-9a-f]{64}` string plus exact integer version `1` is independently
  routable and authoritative.
- Tendwire's SQLite store is schema version `7` and provides the turn-list-v2,
  immutable content-page, ordered turn-final plan, lease/ACK, and explicit
  replacement-generation records expected by this Herdres consumer. Do not
  qualify either side in isolation.
- Production Herdres requests and accepts only exact integer `2` in the
  top-level Tendwire turn-list response. A v1 producer returns
  `upgrade_required`; a missing or unsupported content schema returns
  `unsupported_content_schema`. Unsupported outer envelopes fail the whole
  connector before source, Telegram, cleanup, or outbox mutation.
- Before paging any row, Herdres validates content-schema-v1 descriptors for
  both canonical text fields, including availability/inline consistency,
  content revision, character and UTF-8 byte lengths, page count/cursor, and
  the `known_incomplete` summary. Malformed descriptors are turn-local
  `invalid_content_schema` outcomes; explicitly incomplete content is
  `content_known_incomplete`. Neither is paged, planned, or sent, while
  unrelated working/final, attention, status, and enabled account-pin work
  continues.
- Paging is lazy: unchanged delivered revisions, historical rows, unroutable
  turns, quarantined owners, and inline content cause zero page fetches. Eligible
  non-inline fields use immutable linear pages of at most 49,152 UTF-8 bytes;
  exact identities, order, unique cursors/segments, character/byte lengths, and
  null termination are verified. A defective page is the turn-local
  `invalid_content_page` outcome before prepare or Telegram mutation.
- Herdres materializes exact content before deriving ordered multipart ranges.
  Prepare begin/part/commit sends only neutral field/start/end spans, never turn
  text. Every leased span must match the local plan. Stable
  plan-token/sequence receipts progress strictly through reservation, Telegram
  apply, optional old-slot retirement, Tendwire ACK, and `acknowledged`, with a
  durable checkpoint after each provider-side transition and after ACK.
- Public absent, legacy-24, malformed, partial, and explicitly invalid
  identities are not private adoption candidates. The only private exception
  is one persisted exact-shaped v1 handle whose version field is absent.
- Private adoption is planned in stable order and revalidated before mutation.
  It requires one exact-v1 current claimant, one compatible live
  nonquarantined persisted candidate, sole live topic ownership, no exact-v1
  owner, and no conflicting binding ownership. It never falls back to worker
  ID.
- A safe adoption preserves topic/message/private state and delivery ledgers
  and retargets only compatible owned bindings. No current claimant waits
  without mutation; ambiguity or incompatibility blocks adoption and
  quarantines affected claimants and related unsafe bindings while leaving
  unrelated bindings unchanged. Reordered and repeated passes are identical
  no-ops after convergence.
- Same-workspace/tab continuity retains the existing worker topic while a
  cross-workspace move intentionally changes identity.
- Missing, malformed, partial, or unknown public identity and fresh-snapshot or
  persisted collisions are quarantined before topic creation or selection and
  before turn or reply routing. Blocked claimants do not remain routable, and
  repeated faulty snapshots create no duplicate state entries or topics.
- Reply binding resolution additionally requires the resolved worker to own the
  binding topic directly or through its matching Tendwire source-space topic.
- Goal 01B recovery still flows through Tendwire's existing `turn.list`
  refresh and durable public source projection; Herdres never refreshes Herdr
  itself.
- For a matching editable Working card, the authoritative completed revision
  uses the same ordered plan and produces one Working-to-final edit; repeated
  identical source syncs make paging, prepare, edit/send, and ledger no-ops.
- An exception after possible Telegram acceptance or an omitted message receipt
  is `delivery_uncertain`. It is failed closed, not automatically replayed, and
  these checks do not establish perfect provider exactly-once behavior.
- Herdres performs syntactic validation only. It never receives the HMAC
  secret or raw pane identity, never imports or invokes a direct Herdr client,
  and never opens a direct Herdr process/socket path. The smoke result reports
  `direct_herdr_calls=0`.

### Failed-plan operator evidence

An `attempts_exhausted` plan must remain idle on ordinary sync until an operator
issues an explicit command for that failed generation:

```sh
herdres tendwire recover-turn-final \
  --plan-token twplan1.<failed-plan> \
  --request-id operator-2026.07.11:1
```

The request ID is 1–128 ASCII `[A-Za-z0-9._:-]` characters and is both the
idempotency key and audit key. Local preflight must stop before RPC with:

- `invalid_recovery_request` for a malformed/bounded-coordinate failure;
- `recovery_request_conflict` when the request ID is bound elsewhere;
- `recovery_plan_not_found` when the token is not the unique pending plan,
  including a plan that is no longer pending or is already complete;
- `recovery_route_ambiguous` for a quarantined or nonunique route;
- `recovery_state_invalid` for invalid coordinates, unknown substates, or a
  noncontiguous acknowledged prefix;
- `recovery_receipt_uncertain` for a reserved/unproven operation or binding
  without an acknowledged receipt;
- `recovery_receipt_inflight` for `telegram_applied` or `old_slot_retired`
  provider outcomes still awaiting durable ACK; and
- `recovery_capacity_exceeded` when both immutable generations will not fit or
  every bounded audit slot is protected by a pending replacement plan.

Typed Tendwire failures pass through. A malformed/mismatched response or any
state change across the RPC is `recovery_state_uncertain`. Success must return a
different token and exact next generation for the same revision, with exact
prefix and executable counts. A failed replacement must have one uniquely
matching inherited recovery audit and request binding for the preceding
generation. Herdres adds that audit's retained-failure count to the current
failed tail and binds the inherited identity and cumulative count into
preflight revalidation. Audits needed by pending replacements are protected
from bounded-detail eviction; an all-protected audit table fails before RPC
rather than stranding a later generation. The old receipts remain byte-for-byte
unchanged. Herdres clones the contiguous acknowledged prefix, retargets only
its bindings,
records the request-keyed audit, validates the suffix's predecessor against the
old ACK-prefix barrier, and executes only the suffix. Output records both
tokens, generation, prefix/executable/retained/prior-attempt counts, state, and
`idempotent_replay`. Repeating the same request returns the same audited token
with `idempotent_replay=true`; each generation requires one explicit request
and there is no automatic recovery loop.

Do not treat exact identity format as cryptographic proof: a correctly shaped
spoof in altered public input is outside Herdres's ability to authenticate. Do
not claim eager refresh, immediate delivery, perfect provider exactly-once
behavior, or deployment completion from these checks. Also do not change
Telegram policy as part of this gate: `space` remains the default topic mode,
`worker` remains opt-in, the existing completed-council topic deletion setting
remains unchanged, and enabled account lines remain in the pinned status.

## 3. Build a clean source artifact

```sh
git archive --format=zip -o dist/herdres-$(git describe --always).zip HEAD
```

`git archive` ships only tracked files: `herdres.py`, `herdres_gateway.py`,
`herdres_connector/*.py`, `systemd/user/*.service`, `install-user.sh`, docs, and
`.env.example`. It never includes caches, real `.env`, `state.json`, offsets, or
`*.session` credentials.

## 4. Verify the artifact is clean

The following must print nothing:

```sh
git archive --format=tar HEAD | tar -t | grep -E '__pycache__|\.pyc$|\.pytest_cache|\.env$|state\.json|\.session'
```

## 5. Local hygiene (optional, before building from a dirty tree)

```sh
find . -type d -name __pycache__ -not -path './.git/*' -exec rm -rf {} +
rm -rf .pytest_cache
find . -name '*.pyc' -not -path './.git/*' -delete
```

## Service model shipped

- `herdres.service` — source sync loop (`herdres sync --loop`). Replaces the
  old `herdres.timer`; there is no timer unit on this branch.
- `herdres-gateway.service` — inbound Telegram polling.
- `tendwired.service` — the Tendwire daemon (ships from the Tendwire repo).

`config.SOURCE_SERVICES`, the shipped `systemd/user/*.service` files, and the
`enable --now` lines in README/INSTALL are kept in agreement by
`tests/test_release_readiness.py`.

## Rollback

Preserve the pre-verification Herdres state copy. If dry verification fails,
leave the live state untouched and investigate the non-success result; do not
edit state, copy handles, delete individual key files, or rotate identity as a
repair.

If live continuity state must be recovered, stop every writer and restore the
complete paired Herdres state, Tendwire database, installation key, digest, and
initialization sentinel backup described in INSTALL.md. A standalone Herdres
state copy is not sufficient when Tendwire continuity material changed. Run the
dry source check against a fresh copy of the restored state before resuming
writers.

Source-only: `HERDRES_TENDWIRE_MODE` must be `source` (there is no `off`). Roll
back code by switching the checkout to a legacy (non-tendwired) Herdres branch
or tag and reinstalling — a code switch, not an environment toggle. State
recovery remains a separate paired restore.
