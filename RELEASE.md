# Release checklist (Herdres 0.7.0rc4 / Tendwire 0.1.0rc5)

## 0. RC pairing and release gate

This candidate supports Python 3.13 and pairs with Tendwire `0.1.0rc5` or a
reviewed descendant preserving its public contract. Run the complete paired
gate from clean checkouts before tagging or deployment.

Independent H8 is an implementation/review state, not evidence that a cutover
or deployment has occurred and not authorization to perform one. It retains the
old presenter in `herdres.service`; the later state/presenter replacement must
ship only as its separately approved paired H7/H6 release.

Before tagging or deployment, run the complete local pair from clean checkouts:

```sh
# Tendwire checkout
python3 scripts/release_artifacts.py source
python3 -m compileall -q src tests scripts
python3 -m pytest -q
python3 scripts/herdr_smoke.py --fixture-dir tests/fixtures/herdr/live_smoke/ok
python3 -m build
python3 scripts/release_artifacts.py artifacts dist

# Herdres checkout
python3 -m pytest -q
python3 -m compileall -q herdres.py herdres_gateway.py herdres_connector tests
```

The recorded paired socket probe must use a temporary Tendwire daemon and
retain `direct_herdr_calls=0`, exact turn/pending/command schemas,
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
TENDWIRE_CHECKOUT="$(pwd -P)"
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
TENDWIRE_SOURCE="$(cd -- "${TENDWIRE_CHECKOUT:?set TENDWIRE_CHECKOUT to the clean Tendwire checkout}" && pwd -P)/src"
test -f "$TENDWIRE_SOURCE/tendwire/daemon_api.py"
HERDRES_PAIRED_TENDWIRE_SOURCE_DIR="$TENDWIRE_SOURCE" python -m pytest -q \
  tests/test_ingress.py \
  tests/test_source_only.py \
  tests/test_command_ingress_idempotency.py \
  tests/test_stable_worker_key.py \
  tests/test_tendwire_client.py \
  tests/test_tendwire_socket_pairing.py \
  tests/test_turn_final_delivery.py \
  tests/test_outbound_latency.py \
  tests/test_offlock_delivery.py \
  tests/test_release_readiness.py
HERDRES_TENDWIRE_MODE=source ./herdres.py tendwire source-smoke --with-outbox
```

The Herdres ingress/client/final-delivery suites in this block are hermetic
contract gates. `tests/test_tendwire_socket_pairing.py` is the executable real
server/API/SQLite pair: it proves poll, provider binding, ACK, empty repoll, and
ACK-response loss followed by authoritative empty repoll. The listed Tendwire
command, connector, and store tests are the producer gate. Every listed test
must pass before any stateful sync. The repeated `source-smoke --with-outbox`
remains only the shallow preflight described above.

The paired gate must establish all of the following:

- `herdres_gateway.py` remains executable and is 70--100 physical lines. It
  loads environment/source mode, the safe request-ID key, typed receivers, one
  `IngressQueue.open_writer`, fixed ingress defaults plus configured dispatcher
  count, bounded signal stop handling, and `run_gateway` only. It contains no
  business logic, state-root access, SQL, subprocess, speech, or presenter
  import.
- Exactly one schema-1 ingress queue exists at
  `HERDRES_INBOUND_SPOOL_PATH`. DB/WAL/SHM and writer lock satisfy the pinned
  parent, EUID ownership, regular/single-link, `0600`, inode, integrity, WAL,
  and `synchronous=FULL` checks. The old lane/request modules and JSON ingress
  state key are absent; doctor uses the read-only aggregate observer.
- Queue acceptance atomically stores each stable receiver cursor and request.
  Ordering-key FIFO, fixed depth, claim/renew/expiry, exact operation bytes,
  notice claims, quarantine, pruning, and restart convergence pass
  `tests/test_ingress.py`. Only `HERDRES_INBOUND_DISPATCH_WORKERS` configures
  concurrency; removed lane/hold/stall/response-version flags do not return.
- `hri1_` identities use the installed private 32-byte key and exact receiver
  plus Telegram update/chat/message coordinates. The key and every database
  namespace fail closed on unsafe type, owner, mode, link, symlink, or inode
  replacement. Receiver secrets remain redacted typed values and are revealed
  only into one Telegram client.
- Ingress calls only the frozen typed state operations; it never obtains the
  state root or uses generic load/save/lock helpers. Local decisions use
  composite request/kind idempotency and exact mutation digests. All local and
  retained-presenter decision markup/edit/delete calls share the request-key
  HMAC-derived `pg1.<43>` physical-owner guard without waiting under state or
  queue locks.
- Commands use `TendwireClient.command_json()` directly over the owner-private
  AF_UNIX socket. No child, CLI/database fallback, second queue, or
  receipt-derived working-card shortcut exists. Accepted sends use the pinned
  v3 success contract; decisions and bounded failure matrices retain their
  specified compatibility.
- The old observational presenter remains in `herdres.service` until paired
  H7/H6. It never opens the H8 queue. Passing this independent H8 gate records
  review evidence only and does not assert deployment/cutover completion or
  authorize deployment.

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
  outbox recovery, and leases/ACK/dead-letter state expected by this Herdres
  consumer. Do not qualify either side in isolation.
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
  reusable worker or space IDs alone, and no exact Telegram binding, delivered
  identity, or provider state crosses in the root. A schema-v2 turn-list final
  remains observational only and never by itself marks the final delivered.
- Herdres leases the root, materializes exact content, and submits neutral
  ordered spans. Tendwire owns durable roots, plans, ordered jobs, leases,
  retries, and dead-letter state.
- Before Telegram mutation, Herdres checks for one exact message binding by job
  key, turn, revision, plan, ordinal, and part count. A match is ACK replay
  evidence and causes zero Telegram writes.
- After a successful Telegram mutation, Herdres fsyncs that exact message
  binding. When every part is bound, it fsyncs the ordered message ids and
  delivered identity and clears pending presentation fields before issuing the
  Tendwire ACK. Tendwire alone owns job/outbox recovery; restart re-polls it and
  the exact Telegram binding prevents a duplicate provider mutation.
- Working-to-final edits, multipart ordering, supersession, retirement,
  managed-bot ownership, folds, and reply routing remain binding-driven.
  Missing retire targets are idempotent; route changes quarantine and retire
  accepted stale-route messages before retry.
- Rich plans retain Telegram's 32,768-character and 500-block limits for
  complete single-card messages. Multipart source chunks use a 28 KiB rendered
  UTF-8 ceiling, and formatted-plain fallback keeps an independent 4,096-safe
  plan.
- `HERDRES_TENDWIRE_TURN_FINAL_LEASE_SECONDS` defaults or falls back to 60
  seconds and clamps to 60 through 3600. Lease refs are transient; replay
  identity is the Tendwire job key plus the exact Herdres message binding.
- Topic creation checkpoints a compact accepted-create receipt immediately,
  then absorbs it into the resolved topic binding. Restart adopts or retires
  that accepted topic instead of creating a duplicate.
- Tendwire owns final-ready/dead-letter inspection and identity-specific retry.
  Herdres owns Telegram formatting, private message bindings, delivered
  identities, and topic lifecycle state.

- Public absent, legacy-24, malformed, partial, and explicitly invalid
  identities are not private adoption candidates. The only private exception
  is one persisted exact-shaped v1 handle whose version field is absent.
- Private adoption is planned in stable order and revalidated before mutation.
  It requires one exact-v1 current claimant, one compatible live
  nonquarantined persisted candidate, sole live topic ownership, no exact-v1
  owner, and no conflicting binding ownership. It never falls back to worker
  ID.
- A safe adoption preserves topic/message/private state and delivered identities
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
- Recovery flows through Tendwire's durable public projection and outbox;
  Herdres never maintains a local job ledger or refreshes Herdr itself.
- For a matching editable Working card, the authoritative completed revision
  uses the same ordered plan and produces one Working-to-final edit; repeated
  identical source syncs make paging, prepare, edit/send, and binding no-ops.
- An exception after possible Telegram acceptance or an omitted message receipt
  is `delivery_uncertain`. It is failed closed, not automatically replayed, and
  these checks do not establish perfect provider exactly-once behavior.
- For Tendwire worker handles, Herdres performs syntactic validation only. It
  never receives Tendwire's worker-identity HMAC secret or raw pane identity,
  never imports or invokes a direct Herdr client, and never opens a direct
  Herdr process/socket path. The separate Herdres ingress request-ID key stays
  private to Herdres. The smoke result reports `direct_herdr_calls=0`.

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
