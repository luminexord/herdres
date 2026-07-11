# Release checklist (Herdres tendwired / source-only RC)

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

The check must succeed with turn schema version `1` and
`direct_herdr_calls=0`. It must not save the copied Herdres state or send/edit
Telegram messages. Keep both files private, and do not proceed to a live
reconciliation if the dry result is non-success.

## 2. Paired continuity verification

Verify the Tendwire producer/recovery contract and the Herdres
consumer/reconciliation contract together before release:

```sh
# From the Tendwire checkout:
python -m pytest -q tests/test_worker_stable_key.py tests/test_herdr_turns.py

# From the Herdres checkout:
python -m pytest -q tests/test_source_only.py tests/test_stable_worker_key.py
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
- Every Herdres source sync accepts only an exact integer `1` in the top-level
  `schema_version` of Tendwire's turns response. Missing, boolean, string,
  float, container, and other-version values return
  `unsupported_turn_schema_version` before source state, Telegram, cleanup, or
  connector-outbox mutation. There is no coercion or lossy fallback.
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
- Recovered structured content flows through Tendwire's existing
  `turn.list` refresh and durable public source projection. For a matching
  editable Working card, the later authoritative completed turn produces one
  Working-to-final edit, updates the binding and delivery ledger, and makes
  repeated identical source syncs edit/send/ledger no-ops.
- Herdres performs syntactic validation only. It never receives the HMAC
  secret or raw pane identity, never imports or invokes a direct Herdr client,
  and never opens a direct Herdr process/socket path. The smoke result reports
  `direct_herdr_calls=0`.

Do not treat exact format as cryptographic proof: a correctly shaped spoof in
altered public input is outside Herdres's ability to authenticate. Do not claim
eager refresh, immediate delivery, or deployment completion from these checks.
Also do not change Telegram policy as part of this gate: `space` remains the
default topic mode, `worker` remains opt-in, and the existing completed-council
topic deletion setting remains unchanged.

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
