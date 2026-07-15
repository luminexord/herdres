# Release checklist (Herdres 0.7.0rc3 / Tendwire 0.1.0rc1)

## 0. RC pairing and low-minute gate

This candidate supports Python 3.13 and pairs with Tendwire `0.1.0rc1` or a
reviewed descendant preserving its public contract. Routine automatic CI runs once in
Tendwire. Herdres deliberately has no duplicate automatic workflow consuming
another repository's GitHub Actions minutes.

Before tagging or deployment, run the complete local pair from clean checkouts:

```sh
# Tendwire checkout
python3 scripts/release_artifacts.py source
python3 -m compileall -q src tests scripts
python3 -m pytest -q
python3 scripts/herdr_smoke.py --fixture-dir tests/fixtures/herdr/live_smoke/ok
python3 -m build
python3 scripts/release_artifacts.py artifacts dist

# Herdres checkout; bind pairing explicitly to avoid a skipped test
HERDRES_PAIRED_TENDWIRE_SOURCE_DIR=/absolute/tendwire/src \
  python3 -m pytest -q
python3 -m compileall -q herdres.py herdres_gateway.py herdres_connector tests
```

The paired run must execute rather than skip `tests/test_tendwire_cli_pairing.py`
and must retain `direct_herdr_calls=0`, exact turn/pending/command schemas,
stable-owner migration, neutral outbox behavior, and the two forced no-op sync
proof. Record both commits and the Tendwire wheel/sdist digests.

Deployment remains separately owner-authorized. Back up Tendwire's complete
database/identity family and Herdres state/request-ID key while all writers are
stopped. Install Tendwire first, then Herdres and gateway. Never restart Herdr.
If migration, source smoke, or delivery validation fails, stop writers and
restore the complete paired backup and prior installed artifacts before retry.

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

The dry `source-smoke --with-outbox` check is only a snapshot, turn-list,
pending, and public-safety preflight against the compatible store-schema-v14
producer. It must report `direct_herdr_calls=0`, must not save the copied
Herdres state or send/edit Telegram messages, and does not poll or page a
`final_ready` root, prepare a plan, ACK connector work, or exercise recovery.
Keep both files private, and do not proceed to a live reconciliation if the dry
result is non-success. Never treat this smoke result as the Goal 10 delivery
protocol gate.

## 2. Paired continuity, delivery, and recovery verification

Verify the Tendwire producer/recovery contract and the Herdres
consumer/reconciliation contract together as one compatible release pair:

```sh
# From the Tendwire checkout:
python -m pytest -q \
  tests/test_worker_stable_key.py \
  tests/test_commands.py \
  tests/test_cli_command.py \
  tests/test_command_submission.py \
  tests/test_herdr_turns.py \
  tests/test_turns.py \
  tests/test_connector_outbox.py \
  tests/test_store.py

# From the Herdres checkout:
python -m pytest -q \
  tests/test_source_only.py \
  tests/test_command_ingress_idempotency.py \
  tests/test_stable_worker_key.py \
  tests/test_tendwire_client.py \
  tests/test_turn_final_delivery.py \
  tests/test_offlock_delivery.py
HERDRES_TENDWIRE_MODE=source ./herdres.py tendwire source-smoke --with-outbox
```

The exact Herdres `tests/test_command_ingress_idempotency.py`,
`tests/test_tendwire_client.py`, and `tests/test_turn_final_delivery.py` suites
in this block are the hermetic Goal 11 ingress/client and Goal 10 final-delivery
gates. The listed Tendwire command, connector, and store tests are the paired
producer gate. Every listed test must pass before any stateful sync. The
repeated `source-smoke --with-outbox` remains only the shallow preflight
described above.

The paired gate must establish all of the following:

- Unset or empty `HERDRES_REQUEST_ID_KEY_PATH` selects
  `~/.local/share/herdres/request-id.key`; a configured value expands `~` and
  must then be a nonempty absolute path. `install-user.sh` initializes exactly
  one persistent private raw 32-byte key there, with an owner-owned `0700`
  parent directory and owner-owned regular `0600` file. Reinstall preserves
  valid material; runtime refuses missing, malformed, symlinked, unsafe, or
  replaced material and never creates or repairs it.
- `hri1_` IDs are canonical unpadded URL-safe HMAC-SHA256 digests scoped only
  to stable receiving-bot identity plus Telegram update/chat/message
  coordinates. Tokens, text, topic/reply/user identity, and resolved targets
  are excluded. Same-update redelivery and managed-bot token rotation retain
  the same ID; every distinct update has a different ID even for identical
  text. Identical content does not merge distinct commands.
- Manager and managed-bot polling offsets are keyed to stable receiving-bot
  kinds, not token-derived runtime keys; a current legacy managed-bot offset
  migrates to the stable path, so token rotation preserves polling position.
- On first sight of an update, before routing or child creation, Herdres
  persists immutable `created_at`, `deadline_at`, and `retain_until` bounds.
  It then persists canonical schema-v1 request JSON before command start.
  Every retry sends those exact UTF-8 bytes. The sole rewrite is one
  `stale_target` + `no_receipt` removal of `worker_fingerprint`, durably stored
  before the same-ID retry.
- The paired CLI returns the exact ten-field schema-v2 response. Herdres checks
  action/request/dry-run correlation, public pruning, and each complete
  disposition tuple: accepted/`terminal_accepted`, pending/`in_progress`,
  request-state-uncertain/`terminal_uncertain`, and the allowed rejection
  statuses paired with either `terminal_rejected` or `no_receipt`.
- CLI exit `0` pairs only with `ok: true`, and exit `1` only with `ok: false`.
  Exit `2`, malformed/non-UTF-8 output, timeout, wrong schema/shape/correlation,
  or any exit/body mismatch remains private process ambiguity. No schema-v2
  disposition or private process/stdout/stderr detail is forged from it.
- Disposition, never status alone, controls lifecycle. In particular,
  `backend_unavailable` + `no_receipt` retains the checkpoint for retry, while
  `backend_unavailable` + `terminal_rejected` caches failure and advances.
- The retry deadline is first-seen plus the effective `60..604800` horizon
  (`86400` default/fallback), does not slide with `updated_at`, retry,
  redelivery, or configuration changes, and expires at equality. Before client
  creation and after a retryable response, deadline expiry quarantines instead
  of starting another child.
- `retain_until` is first-seen plus the horizon and `86400`: `172800` default,
  `86460` minimum, `691200` maximum. It is immutable and pruning is strictly
  after it. The paired Tendwire retry horizon is at least the Herdres horizon,
  and Tendwire receipt age is at least the Herdres retention bound. Tendwire's
  `604800`/`2592000`/`4096` defaults satisfy the pair.
- `terminal_accepted` and `terminal_rejected` cache an exact sanitized child
  outcome. `terminal_uncertain`, deadline expiry, and unsafe/corrupt evidence
  are quarantined (locally dead-lettered) with the fixed reply `Could not send
  safely. Refresh status and choose the target again.` and checkpoint
  `advance`. The gateway replies, saves the advanced offset, and processes
  later updates. Restart/redelivery returns terminal or quarantine cache before
  route resolution and never constructs another Tendwire client.
- Only the exact allowlisted public command object reaches Tendwire; its
  request ID satisfies `[A-Za-z0-9._-]{1,128}` and Herdres uses the narrower
  `hri1_…` form. No raw Telegram receiver/update/chat/topic/message/reply/user
  ID, bot token, or private/backend route crosses. The Tendwire child
  environment retains public overrides while stripping Telegram and private
  ingress, gateway, managed-bot, state, request-key, and binary-selector
  variables.
- The Herdres state, Herdres request-ID key, Tendwire database, and Tendwire
  installation key/marker/sentinel are backed up quiescently and restored as
  one set. A replaced Herdres key changes every derived ID and is not recovery.

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
- Tendwire's SQLite store is schema version `14` and provides turn-list-v2
  observational projections, immutable content pages, durable retained
  `final_ready` roots, range-only turn-final plans, restart-stable ordered jobs,
  leases/ACK/dead-letter state, and explicit replacement-generation records
  expected by this Herdres consumer. Do not qualify either side in isolation.
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
- A complete authoritative final creates a durable connector-neutral
  `final_ready` materialization root as part of Tendwire persistence; it does
  not wait for Herdres availability. The root payload has exact integer
  `schema_version: 2` and carries the exact public opaque
  `stable_key`/integer-`1` `stable_key_version` pair, binding retained work to
  the accepted worker continuity identity. A schema-v1 root never routes by
  reusable worker or space IDs alone, and no private checkpoint or Telegram
  state crosses in the root. A schema-v2 turn-list final remains observational
  only and never by itself marks the final delivered.
- Herdres leases the root and materializes exact content before deriving
  ordered multipart ranges. Prepare begin/part/commit sends only neutral
  field/start/end spans, never turn text. Begin and commit bind the leased
  `source_ref`; part requests carry only the plan token, ordinal, and ranges.
  Every leased span must match the local plan.
- Rich plans retain Telegram's 32,768-character and 500-block limits for
  complete single-card messages. Multipart plans default to 24,000-character
  source chunks and a 28 KiB rendered UTF-8 ceiling, avoiding fragile
  boundary-sized cards without returning to the obsolete small-span behavior.
  Plain-message fallback keeps an independent 4,096-safe plan. Successful topic
  creation is checkpointed immediately so a later sync failure cannot create a
  duplicate.
- `HERDRES_TENDWIRE_TURN_FINAL_LEASE_SECONDS` defaults or falls back to 900
  when unset, empty, or invalid, and clamps configured values to 60 through
  3600 seconds. One root lease covers canonical paging, plan staging, and ACK.
- Stable job-key receipts progress strictly through reservation, Telegram
  apply, optional old-slot retirement, Tendwire ACK, and `acknowledged`, with a
  durable private checkpoint after every provider-side transition and after
  ACK. The stable job key, not a transient lease ref, is restart identity.
- An ordinary restart resumes proven work under a fresh ref without repeating
  Telegram work and reconciles a committed pending plan after lost final-ACK
  response or completed-plan observation, even without a turn-list row. Only a
  Tendwire-confirmed `superseded` or `plan_not_found` pending plan clears before
  a newer root; other unresolved states continue to block it.
- Tendwire owns final-ready/dead-letter retention and the bounded, public-safe
  connector inspect plus identity-specific retry surfaces. Herdres owns
  Telegram formatting, private provider state, and local checkpoints. Retry
  can return `not_retryable` or `stale_revision` and does not remove provider
  acceptance ambiguity.
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
- For Tendwire worker handles, Herdres performs syntactic validation only. It
  never receives Tendwire's worker-identity HMAC secret or raw pane identity,
  never imports or invokes a direct Herdr client, and never opens a direct
  Herdr process/socket path. The separate Herdres ingress request-ID key stays
  private to Herdres. The smoke result reports `direct_herdr_calls=0`.

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
`.env.example`. It never includes caches, real `.env`, `state.json`, gateway
offsets, `request-id.key`, or `*.session` credentials.

## 4. Verify the artifact is clean

The following must print nothing:

```sh
git archive --format=tar HEAD | tar -t | grep -E '__pycache__|\.pyc$|\.pytest_cache|\.env$|state\.json|request-id\.key|\.session'
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
complete Herdres state, Herdres request-ID key, Tendwire database, installation
key, digest, and initialization-sentinel backup described in INSTALL.md. A
standalone Herdres state copy is not sufficient when either key or Tendwire
continuity material changed. Run the dry source check against a fresh copy of
the restored state before resuming writers.

Source-only: `HERDRES_TENDWIRE_MODE` must be `source` (there is no `off`). Roll
back code by switching the checkout to a legacy (non-tendwired) Herdres branch
or tag and reinstalling — a code switch, not an environment toggle. State
recovery remains a separate paired restore.
