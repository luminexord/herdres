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

## 2. Paired continuity verification

Verify the Tendwire producer contract and the Herdres consumer/reconciliation
contract together before release:

```sh
# From the Tendwire checkout:
python -m pytest -q tests/test_worker_stable_key.py

# From the Herdres checkout:
python -m pytest -q tests/test_stable_worker_key.py
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
  routable.
- Persisted absent-identity and legacy-24 entries are migration-only for a
  compatible current observation carrying an exact valid-v1 identity.
- Herdres performs syntactic validation only. It never receives the HMAC
  secret or raw pane identity, never queries Herdr, and the smoke result reports
  `direct_herdr_calls=0`.
- Same-workspace/tab continuity retains the existing worker topic while a
  cross-workspace move intentionally changes identity.
- Missing, malformed, partial, or unknown identity and fresh-snapshot or
  persisted collisions are quarantined before topic creation or selection and
  before turn or reply routing. Blocked claimants do not remain routable, and
  repeated faulty snapshots create no duplicate state entries or topics.
- Reply binding resolution additionally requires the resolved worker to own the
  binding topic directly or through its matching Tendwire source-space topic.
- The one-time unambiguous, compatible live migration preserves topic/message
  bindings and delivery ledgers, creates no duplicate topic, and replays no
  delivered turn.

Do not treat exact format as cryptographic proof: a correctly shaped spoof in
altered public input is outside Herdres's ability to authenticate. Also do not
change Telegram policy as part of this gate: `space` remains the default topic
mode, `worker` remains opt-in, and the existing completed-council topic deletion
setting remains unchanged.


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

Source-only: `HERDRES_TENDWIRE_MODE` must be `source` (there is no `off`). Roll
back by switching the checkout to a legacy (non-tendwired) Herdres branch or tag
and reinstalling — a code switch, not an env toggle. See INSTALL.md.
