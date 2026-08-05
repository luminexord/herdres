<img width="2048" height="2048" alt="herdres" src="https://github.com/user-attachments/assets/d8324729-676a-49d8-9d24-800a8a411348" />

# Herdres

The current release-candidate pairing is Herdres `0.7.0rc4` with Tendwire `0.1.0rc5`
on Python 3.13. Paired and live proofs are explicit local release-owner
operations.

This branch is a tiny source-mode-only Telegram connector for Tendwire.

Herdres does not observe or control Herdr directly here. Tendwire owns Herdr
observation, worker bindings, canonical turn content and revisions, durable
final-ready roots and retention, pending interactions, command routing,
backend health, range-only presentation staging, and ordered connector jobs,
leases, ACK state, and dead-letter state. Herdres owns Telegram polling,
topics, presentation planning and formatting, message send/edit, compact
working updates, continuation messages, private provider state, exact Telegram
bindings, delivered identities, and the compact accepted-topic receipt.

**Requires [Tendwire](https://github.com/plotarmordev/tendwire)** — Herdres has
no functionality without it. See [INSTALL.md](INSTALL.md) for setup order.

## Herdr plugin

Herdres is listed as a community Herdr plugin. Install its source checkout and
local actions with:

```sh
herdr plugin install luminexord/herdres
herdr plugin action invoke luminexord.herdres.init-config
herdr plugin action invoke luminexord.herdres.doctor
```

The first action creates a private `herdres.env` template in Herdr's managed
plugin configuration directory; it never overwrites an existing file. The
plugin does not send Telegram messages or start services during installation.
After configuring credentials, follow [INSTALL.md](INSTALL.md) to install and
start the persistent Tendwire, Herdres, and gateway user services.

## Runtime and lossless turn delivery

```text
Telegram topic input
  -> herdres-gateway
  -> one durable in-process ingress queue
  -> Tendwire command JSON over the protected AF_UNIX socket

herdres sync
  -> tendwire snapshot/turn.delta/pending/connector
  -> Telegram topics/messages/pinned status
```

Only `HERDRES_TENDWIRE_MODE=source` is supported. Herdres obtains observations,
turns, pending interactions, and connector work only through Tendwire's public
source/command interfaces; it makes no direct Herdr API, process, or socket
calls.

### Low-latency turn synchronization

After one bounded, cursor-resumable bootstrap, Herdres persists Tendwire's
`twdelta1.*` watermark and local schema-v2 turn projection under the single
`tendwire_delta_sync` state key. Bootstrap pages use the 500-row protocol
maximum by default and a one-hour stateless Tendwire cursor. This keeps the
full supported 100,000-row bootstrap inside the production five-second loop's
cursor window while preserving the existing apply-and-fsync guard on every
page. Normal syncs read one bounded `turn.delta` page. An unchanged page does
not traverse retained history, fetch canonical content, mutate local state, or
fsync the state file. The watermark advances only after the final page of the
frozen batch is applied and fsynced through the existing atomic state writer.

Timeouts, EOF, and malformed frames never trigger a second turn observation.
An invalid or expired watermark starts one fresh bounded bootstrap, resuming
the same cursor after ambiguity. Only Tendwire's explicit unsupported-method
outcome activates legacy full polling. Oversized or repeatedly failing
bootstraps also degrade to full polling and expose the reason in sync/doctor
health instead of crashing the loop.

The change feed is observation-only. Working rows may update local cards, but
completed rows still require the existing `turn_final` outbox before Telegram
delivery. Descriptor-only rows never synthesize content; Goal 05 paging remains
the canonical rendering path. A bounded full reconciliation runs hourly by
default as a safety net. Configure the page/repair behavior with:

```text
HERDRES_TENDWIRE_TIMEOUT_SECONDS=60
HERDRES_TENDWIRE_CONNECTOR_POLL_SECONDS=1
HERDRES_TENDWIRE_DELTA_LIMIT=500
HERDRES_TENDWIRE_FULL_RECONCILE_SECONDS=3600
HERDRES_TENDWIRE_FORCE_FULL_RECONCILE=0
```

Zero, negative, or invalid reconciliation intervals use the hourly default so
the retained projection stays bounded. Set the force flag for an explicit
reconciliation pass.

Outbound presentation is not paced by the full source reconciliation.
`herdres sync --loop` retains its connector-only presenter poll until the paired
H7/H6 replacement. Independent H8 ingress never calls that presenter path and
does not synthesize a working card from a command receipt. Neither path inspects
Tendwire's database or watches its SQLite/WAL files.

### Durable inbound queue

Independent H8 ingress runs entirely inside `herdres-gateway.service`. The
96-line `herdres_gateway.py` wrapper loads typed receiver credentials, opens one
`IngressQueue` writer, and starts in-process poller and dispatcher threads. It
does not invoke `herdres.py`, a command child, a subprocess, a CLI fallback, or
the retained presenter.

Accepted updates and stable receiver cursors commit atomically to the sole
private queue at `~/.local/share/herdres/inbound_spool.db` (override with
`HERDRES_INBOUND_SPOOL_PATH`). The schema-1 SQLite database uses WAL,
`synchronous=FULL`, one exclusive writer, fixed depth/lease/deadline/retention
bounds, strict FIFO per ordering key, and up to
`HERDRES_INBOUND_DISPATCH_WORKERS` concurrent dispatchers (default `8`). There
is no lane database, JSON request ledger in `state.json`, second queue, or
receipt-derived working-card shortcut.

The queue checkpoints one canonical command or local-decision action before
external mutation. Commands use `TendwireClient.command_json()` directly over
the pinned owner-private AF_UNIX socket. A definite not-started result may retry
the same stored bytes; post-start ambiguity, corrupt evidence, owner drift, or
deadline expiry fails closed or follows the bounded convergence rule for the
known target. Terminal and quarantine outcomes are durable, and notices are
claimed separately so a slow Telegram send does not hold the ordering head.

Ingress never receives the schema-2 state root. It uses the typed policy,
receiver, route, reply, and decision operations in `state.py`, comparing opaque
`StateToken` values only for equality. Receiver tokens remain inside slotted
`SecretStr` values and are revealed only into their one `TelegramClient`.
Decision message edit/delete operations share the request-key-derived
`pg1.<43>` physical-owner guard with the retained presenter; guard files expose
no provider coordinates and waiting never holds the state flock or queue
transaction.

The old source presenter remains in `herdres.service` until the paired H7/H6
replacement. It continues ordinary observational working/final presentation,
but it does not open the inbound queue and no longer consumes ingress JSON or
submission receipts to synthesize a working card. Voice/media ingress is not
part of H8. This documents the independent implementation state only; it is not
a claim that deployment, cutover, or the later H7/H6 release is complete.

### Inbound command identity and redelivery

`install-user.sh` initializes one private 32-byte request-ID key at
`HERDRES_REQUEST_ID_KEY_PATH` (default
`~/.local/share/herdres/request-id.key`). The parent is owner-owned mode `0700`
and the regular single-link key is mode `0600`; unsafe, missing, malformed, or
replaced material fails startup closed. Runtime never creates, repairs, rotates,
logs, or serializes the key.

Each Telegram update receives a canonical `hri1_` plus 43-character unpadded
base64url HMAC-SHA256 ID over receiver identity and update/chat/message
coordinates. Tokens, text, user identity, reply metadata, and resolved routes
are excluded. The queue stores canonical request bytes and the composite local
decision identity before mutation. Duplicate bytes converge; a conflicting
digest quarantines. The sole fenced command rewrite removes
`worker_fingerprint` once only for a pre-route-generation `stale_target` with
`no_receipt` while stable owner and worker ID remain identical.

Only the allowlisted public command object crosses to Tendwire. Accepted sends
must satisfy the pinned schema-3 success contract; bounded compatible failures
retain their exact allowed schema-2/3 disposition matrix. Decision answers use
their exact schema-2 contract. Raw Telegram/provider coordinates, credentials,
private routes, queue rows, and process output never cross the AF_UNIX boundary.
Back up the queue DB/WAL/SHM, request-ID key, private Herdres state, and Tendwire
continuity set together while all writers are stopped.
### Turn content and paging contract

Production source sync negotiates Tendwire's top-level `turn.list` schema as the
exact integer `2`; a v1 response returns `upgrade_required`, while a missing or
unsupported per-row content schema returns `unsupported_content_schema`.
Schema-v2 rows carry content-schema-v1 descriptors for both `user_text` and
`assistant_final_text`. Herdres validates every descriptor before any row can
page: availability, inline placement, character and UTF-8 byte lengths, page
count, first cursor, content revision, and the `known_incomplete` summary must
agree. There is no coercion or lossy fallback.
Completed finals in the schema-v2 list are observational source projections;
their presence or absence never creates delivery work or proves delivery.
Delivery begins only from Tendwire's durable connector work described below.

An invalid list envelope is a connector-wide failure (`tendwire_turns_failed`
through source sync; a directly observed unsupported outer version is
`unsupported_turn_schema_version`). A malformed descriptor is instead isolated
to that turn as `invalid_content_schema`, and an explicitly incomplete field is
isolated as `content_known_incomplete`. Neither row is paged, planned, or sent;
unrelated working/final delivery, attention, status, and enabled account-pin
updates continue.

Paging is eligibility-only. Herdres first excludes an unchanged delivered
revision, a historical row, a turn without a uniquely routable live owner, and
a quarantined owner. Those rows perform zero content fetches, as does complete
inline content. For an eligible non-inline field, Tendwire exposes immutable,
linear content-schema-v1 pages of at most 49,152 UTF-8 bytes. Herdres follows
the cursor chain once and verifies turn, revision, field, availability, page
index/count, unique segment and cursor identities, exact per-segment and total
character/byte lengths, and a null final cursor. A defective page becomes the
turn-local `invalid_content_page` outcome before prepare or Telegram activity.

With Tendwire store schema v14, committing a complete authoritative final also
creates a durable, connector-neutral `final_ready` materialization root. Its
payload has exact integer `schema_version: 2` and carries the public opaque
`stable_key` (`wsk1_` plus 64 lowercase hexadecimal characters) with exact
integer `stable_key_version: 1`, binding retained work to the accepted worker
continuity identity. A schema-v1 root never routes by reusable `worker_id` or
`space_id` alone. Root creation and retention do not depend on Herdres being
installed, running, or available. The root contains canonical content
descriptors and the public identity pair, never an exact Telegram binding,
delivered identity, provider routing, credentials, or message state.

After leasing a `final_ready` root, Herdres materializes the exact canonical
content and asks Tendwire to commit neutral ordered presentation spans. Tendwire
owns the durable root, presentation plan, ordered delivery jobs, leases,
retries, and dead-letter state. Herdres owns Telegram rendering and the private
mapping from an exact Tendwire job to its accepted Telegram message.

For every Tendwire-leased upsert, Herdres first checks for one exact Telegram
binding matching job key, turn, revision, plan, ordinal, and part count. An
existing exact binding is replay evidence: Herdres performs no send or edit and
proceeds to the Tendwire ACK. Otherwise it performs one Telegram mutation,
persists the returned message id with those exact coordinates, and fsyncs that
binding before ACK. Once every ordinal is represented exactly once, Herdres
fsyncs the ordered message ids and delivered identity and clears the pending
presentation fields before ACK. Tendwire alone owns the durable job, outbox,
and recovery state; a restart re-polls it and uses Herdres's exact binding to
avoid duplicate Telegram work.

Working-card replacement, multipart ordering, revision supersession, old-slot
retirement, managed-bot ownership, response folding, and reply routing all use
the same message bindings. Missing retire targets are idempotent. Telegram 429
and transient failures defer with bounded retry timing; definite terminal
failures are reported to Tendwire. If a route changes during an accepted send,
the exact accepted message is checkpointed as quarantined and retired before
the job is retried on the current route.

Rich cards remain the primary transport, with formatted-plain fallback using
the same stable spans. `max_sends` is the exact pass-wide physical-write
allowance across rich/plain attempts, multipart work, working cards, folds,
pending cards, and additive voice notes.

`HERDRES_TENDWIRE_TURN_FINAL_LEASE_SECONDS` defaults to 60 seconds and is
clamped to 60 through 3600 seconds. Lease refs are transient; durable replay
identity is the Tendwire job key plus the exact Herdres message binding.

Telegram topic creation has no idempotency key. Herdres therefore checkpoints a
compact accepted-create receipt in its ordinary state immediately after
acceptance, then replaces that receipt with the resolved topic binding. A crash
between those barriers adopts or retires the already-created topic on restart
instead of creating another one.

Telegram itself cannot provide perfect caller-side exactly-once semantics when
a transport failure hides whether an unkeyed operation was accepted. Herdres
never claims otherwise: known exact bindings prevent proven duplicates,
transient errors defer, and missing accepted-message identity fails closed.

### Inspecting and retrying dead-letter finals

Inspect Tendwire's connector-neutral dead-letter state with an explicit bounded
limit:

```sh
tendwire connector inspect --name turn-final --status dead_letter --limit 100 --db-path /path/to/tendwire.db
```

Retry one exact unresolved final by the public `final_identity` returned by
inspection:

```sh
tendwire connector retry --name turn-final --final-identity 'twfinal1.<opaque>' --db-path /path/to/tendwire.db
```

Tendwire owns this retention and retry surface. Retry is identity-specific and
can return `not_retryable` or `stale_revision`; never edit either database or
Herdres private state to force replay.

## Worker identity continuity

Tendwire may publish a v1 worker handle in public worker metadata. Herdres
treats a persisted worker entry as independently routable only when
`meta.stable_key` is a string consisting of exactly `wsk1_` followed by 64
lowercase hexadecimal characters and `meta.stable_key_version` is the exact
integer `1` (not a string or boolean). Both fields must be present and valid.
Malformed, partial, differently versioned, or decorated values are not stable
identity.

This check is deliberately syntactic. Tendwire derives the handle from its
private 32-byte installation key, but Herdres never receives or reads that key,
never sees raw pane identity, and cannot cryptographically distinguish a
correctly shaped spoof from a Tendwire-generated handle. Authenticity therefore
depends on the local Tendwire boundary and access controls around its public
output, not on Herdres's format check. Herdres does not query Herdr to confirm an
identity.

With the same Tendwire installation key, moves within the same workspace/tab
retain the authoritative handle and an existing worker topic. A cross-workspace
move intentionally receives another handle. This reconciliation does not change
the Telegram topic policy below: space topics remain the default and worker/pane
topics remain opt-in.

The v1 handle is not derived from an agent session. Tendwire derives it from the
host plus Herdr's workspace/public-pane identity, so an agent `/clear` that only
rotates the Claude/Codex session does not intentionally change the handle. A
pane close/reopen or replacement can receive a new public-pane identity and
therefore a new handle without restarting Herdr. Herdres treats that as a
continuity event when exactly one historical topic and one live pane agree on
the operator-visible label and agent; matching cwd/space evidence strengthens
the match, while an explicit cwd disagreement is always a veto. The handoff is
planned before stable-key consolidation, topic naming, or `createForumTopic`, so
the existing topic id and history survive and no numbered replacement is minted.

Closed-history healing may deliberately move an old row's key into the paired
`retired_tendwire_stable_key` fields so it cannot remain routable. Those fields
remain eligible only for this one-to-one physical topic handoff; they never make
the historical row independently routable again.

Public observations always pass through the exact-v1 gate. A narrow private
state migration exists only for a persisted exact-shaped `wsk1_` handle whose
persisted version field is absent. An absent handle, a legacy 24-character
handle, an explicit null, a malformed or explicit version, and a worker-id-only
match are never adoption candidates.

Herdres plans that private migration deterministically before mutation and
revalidates the complete plan before applying it. Adoption requires exactly one
current exact-v1 claimant and exactly one compatible, live, nonquarantined
Tendwire worker entry that solely owns its live topic, with no existing exact-v1
owner or conflicting reply binding. With no current claimant, the candidate is
left unchanged to wait for a later observation. A safe adoption adds version
`1`, refreshes the public observation fields, retargets only compatible owned
bindings, and preserves the topic, message history, private state, and delivered
identities. Repeating it is a no-op.

Multiple current claimants, multiple persisted candidates, incompatible state,
ambiguous live topic ownership, an existing exact-v1 owner, or conflicting
binding ownership blocks adoption. Herdres quarantines the affected claimants
and related unsafe bindings rather than guessing; unrelated bindings are left
unchanged. Ordering of current observations, persisted entries, and bindings
does not change the decision.

Before topic creation or selection, and before turn or reply routing, Herdres
also preflights current observations and persisted state. A missing, malformed,
partial, or unknown public identity, a fresh-snapshot collision, or a persisted
collision is quarantined. A quarantined claimant is not routable and cannot
receive or select a topic; repeated faulty snapshots update the same claimant
rather than creating duplicate state entries or topics. A reply binding resolves
only when its worker owns the binding topic directly or through that worker's
matching Tendwire source-space topic.

Tendwire's continuity set includes `installation.key`,
`installation.key.sha256`, and the one-byte nonsecret
`installation.key.initialized` sentinel. Once initialized, ordinary key loading
never rotates the installation identity. A deliberate offline rotation requires
an explicit acknowledged reset, changes every handle, and requires operator
review of quarantined old bindings. See [INSTALL.md](INSTALL.md) for the paired
backup, restore, and reset requirements.

By default Herdres creates one Telegram topic per Tendwire space:

```sh
HERDRES_SOURCE_TOPIC_MODE=space
```

Use worker/pane topics only when explicitly wanted:

```sh
HERDRES_SOURCE_TOPIC_MODE=worker
```

After Telegram accepts a topic creation, Herdres checkpoints the returned
topic identity immediately, before turn parsing, paging, or delivery. A later
turn-local failure therefore cannot make the next sync create the same topic
again.

Finished council/gitmoot/gm worker topics are deleted automatically when
`HERDRES_DELETE_DONE_COUNCIL_TOPICS=1`.

In worker mode, a pane topic whose entry remains closed for 24 hours is
reversibly closed by default; retired duplicate/archive topics use the same
TTL. A UUID-owned pane that becomes live again is reopened before turn
delivery. Set `HERDRES_TOPIC_CLEANUP_ACTION=delete` to delete eligible topics
instead; a revived pane then keeps its durable identity and mints a fresh topic
through the normal creation path. General, shared space topics, pinned and
dashboard/setup topics, and live routable pane topics remain protected in both
modes. The selected `close` or `delete` action is recorded in the lifecycle
audit. Disable new cleanup with:

```sh
HERDRES_CLOSE_DORMANT_AFTER_HOURS=0
```

Telegram close/delete/reopen work is off-lock and bounded per sync pass by
`HERDRES_CLEANUP_BUDGET_SECONDS` (default `5`) and
`HERDRES_CLEANUP_MAX_OPS` (default `12`). Repeated target failures are
permanently abandoned after three attempts; Telegram 429 responses persist
their requested backoff. Changing from `close` to `delete` also deletes
already-auto-closed dormant or retired topics once they are TTL-eligible.

Rich Telegram messages are enabled by default and are attempted first. Final
responses render as open rich content; working updates render as compact
editable updates. A definite rich rejection falls back to the existing
formatted `sendMessage` ladder. Transient or ambiguous rich failures do not
blindly resend because Telegram has no receiver-side idempotency key.

Set `HERDRES_FORCE_PLAIN_DELIVERY=1` to bypass rich delivery immediately for a
client that cannot render cards. `HERDR_TELEGRAM_TOPICS_RICH_MESSAGES=0` remains
the capability-level disable switch.

Bot API acceptance cannot prove recipient rendering. The operational read-back
probe uses an owner-authorized Telethon session to fetch the delivered message
and inspect both `message.rich_message.blocks` and `message.message` (or
`raw_text`). Rich cards normally have blocks while the ordinary text field is
empty; that empty field alone does not mean the card is blank. Confirm the card
visually in the owner's client, and use the force-plain switch if the client
does not render those blocks. Never put the owner's session credentials in
Herdres state or repository configuration.

Optional per-agent bot identities are configured with generic private tokens:

```sh
HERDR_TELEGRAM_TOPICS_MANAGED_BOTS=1
HERDRES_MANAGED_BOT_CODEX_TOKEN=...
HERDRES_MANAGED_BOT_CLAUDE_TOKEN=...
```

When enabled, H8 polls configured child bots as well as the manager bot.
Unconfigured agent kinds fall back to the manager bot. Do not commit real bot
tokens or local bot names. Voice/audio/media-only ingress and the old `/voice`
gateway command are not supported by H8. The outbound speech module remains
only because the old presenter still imports it; paired H7/H6 owns its removal.

## Install

```sh
./install-user.sh
$EDITOR ~/.config/herdres/herdres.env
systemctl --user daemon-reload
systemctl --user enable --now herdres.service herdres-gateway.service
```

Do not run the legacy `herdr-telegram-topics.timer` with this branch.

### Services

This branch runs two user services (plus the Tendwire daemon):

- `herdres.service` — the source sync loop (`herdres sync --loop`). It reads
  Tendwire snapshots/turns/pending and drives Telegram topics, messages, and
  pinned status. Its bounded connector poll loop drains working/final cards
  independently of long reconciliation passes. This replaces the old
  `herdres.timer`; there is no timer unit on this branch.
- `herdres-gateway.service` — inbound Telegram polling, durable queue dispatch,
  and direct Tendwire AF_UNIX command submission in one process.
- `tendwired.service` — the Tendwire daemon (installed from the Tendwire repo);
  Herdres depends on it but does not manage it.

## Send transport

Herdres submits every outbound instruction through Tendwire's public
`command.submit` method over the protected local daemon socket. Herdres never
sees or handles `pane_id`, `terminal_id`, or `send_keys` — those never appear in
public or source-mode state. Tendwire owns the private send target and may, for
delivery reliability, drive a private Herdr pane transport internally; that is a
Tendwire implementation detail behind the public command contract. Planned
follow-up: switch Tendwire's internal send to the semantic `agent.send` API once
it is stable, with no change to the public command path Herdres depends on.

## Rollback

Before the first live source reconciliation of existing state, copy the private
Herdres state and run the dry source check against that copy:

```sh
state_path="${HERDR_TELEGRAM_TOPICS_STATE:-$HOME/.local/share/herdres/state.json}"
backup_path="${state_path}.pre-source-v1"
cp -p -- "$state_path" "$backup_path"
HERDR_TELEGRAM_TOPICS_STATE="$backup_path" \
  HERDRES_TENDWIRE_MODE=source \
  ./herdres.py tendwire source-smoke --with-outbox
```

Keep the copy private. A compatible pair uses Tendwire store schema `14`,
top-level turn-list schema `2`, content-schema-v1 descriptors/pages, and the
Tendwire-owned turn-final outbox prepare/lease/ACK protocol. The dry check must
succeed with `direct_herdr_calls=0` before a live sync; it does not save the
copied Herdres state or send/edit Telegram messages. If verification fails, leave the live
state untouched. Do not repair continuity by editing state, copying public
handles, deleting individual key files, or rotating identity.

For continuity recovery, stop all writers and restore the complete paired
Herdres/Tendwire backup described in [INSTALL.md](INSTALL.md), then repeat the
dry check against a copy before resuming writers. A Herdres state copy alone is
not a substitute when either continuity key or the Tendwire database changed.

This branch is source-only: `HERDRES_TENDWIRE_MODE` must be `source`
(`require_source_mode` rejects any other value — there is no
`HERDRES_TENDWIRE_MODE=off`). To roll back code, switch the checkout to a legacy
(non-tendwired) Herdres branch or release tag and reinstall from there:

```sh
systemctl --user disable --now herdres.service herdres-gateway.service
git checkout <legacy-herdres-tag>
./install-user.sh   # or the legacy branch's installer
```

Rolling back code is a branch/release switch, not an environment-variable
toggle, and does not replace paired state recovery.

## Checks

```sh
python -m pytest -q \
  tests/test_source_only.py \
  tests/test_command_ingress_idempotency.py \
  tests/test_stable_worker_key.py \
  tests/test_tendwire_client.py \
  tests/test_turn_final_delivery.py \
  tests/test_offlock_delivery.py
HERDRES_TENDWIRE_MODE=source ./herdres.py doctor
HERDRES_TENDWIRE_MODE=source ./herdres.py tendwire source-smoke --with-outbox
```

The focused tests cover continuity and quarantine, stable ingress identity and
exact-request redelivery, schema-v2 descriptor isolation, lazy exact paging,
neutral multipart plans, Tendwire-outbox recovery through exact Telegram
bindings, and explicit uncertainty.
`source-smoke` must run against a copied state file and report
`direct_herdr_calls=0`.
