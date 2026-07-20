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
working updates, continuation messages, private provider state, local
stable-job checkpoints, and Telegram delivery dedup.

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
  -> herdres command
  -> tendwire command --json

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
`tendwire_delta_sync` state key. Normal syncs read one bounded `turn.delta`
page, so an unchanged pass does not traverse retained turn history or fetch
canonical content. Each continuation cursor is saved only after its page has
been applied; the watermark advances only after the final page of the frozen
batch is applied and fsynced through the existing atomic state writer.

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
HERDRES_TENDWIRE_DELTA_LIMIT=100
HERDRES_TENDWIRE_FULL_RECONCILE_SECONDS=3600
HERDRES_TENDWIRE_FORCE_FULL_RECONCILE=0
```

Zero, negative, or invalid reconciliation intervals use the hourly default so
the retained projection stays bounded. Set the force flag for an explicit
reconciliation pass.

### Durable inbound lanes

`HERDRES_INBOUND_LANES=1` enables the opt-in per-topic ingress scheduler. It is
off by default, so an upgrade keeps the existing synchronous gateway behavior
until explicitly enabled. Lane mode stores accepted updates in
`~/.local/share/herdres/inbound_spool.db` (override with
`HERDRES_INBOUND_SPOOL_PATH`) using SQLite WAL and `synchronous=FULL`. The lane
item and stable receiving-bot cursor commit in one transaction; the old offset
file remains a best-effort, atomic rollback mirror.

Message lanes are keyed by receiving-bot kind and Telegram topic. A topic uses
that key immediately even before its state entry resolves, so resolution cannot
reorder its first messages. General-topic chatter, owner slash commands, and
callback queries use three separate receiver-wide lanes; owner commands retain
their own FIFO without queueing behind conversation. Each lane is strict FIFO
with at most one leased head, while up to
`HERDRES_INBOUND_DISPATCH_WORKERS` lanes run concurrently (default `8`). A
retry delays only its lane using exponential backoff based on
`HERDRES_INBOUND_LANE_BACKOFF_SECONDS` (default `2`, capped at five minutes).
The dispatcher renews a claimed lease across the full pipeline, including
unbounded voice pretranscription, command execution, and acknowledgement.
Expired leases are reclaimed after a crash and replay the same request ID;
terminal ingress records complete without a second Tendwire submission.

`HERDRES_INBOUND_LANE_DEPTH` bounds each lane at `32` open items by default.
When a lane is full, the gateway does not spool that update: it advances the
durable cursor and posts one throttled owner-visible notice in the affected
topic. Wrong-chat, non-owner, bot-authored, empty, and other-token updates are
also dropped and advanced before spooling. Dispatch releases the global
`state.json` flock around the idempotent Tendwire request and reloads state after
reacquiring it, so unrelated lane dispatchers do not serialize on the backend
call.

### Inbound command identity and redelivery

`install-user.sh` initializes one private 32-byte request-ID key at the path
selected by `HERDRES_REQUEST_ID_KEY_PATH`. Unset or empty selects
`~/.local/share/herdres/request-id.key`; a configured value is expanded for
`~` and must then be a nonempty absolute path. The installer creates or
tightens the owner-owned parent directory to mode `0700`, creates the
owner-owned regular key file atomically with mode `0600`, and preserves an
existing valid key instead of rotating it. The gateway only loads this
installed file; a missing, malformed, symlinked, incorrectly owned or
permissioned, or concurrently replaced key fails startup closed. Never put
the raw key in an environment file, command line, log, ticket, or repository.

For each received Telegram message, Herdres emits a canonical `hri1_` request
ID containing an unpadded URL-safe base64 HMAC-SHA256 digest. The MAC is scoped
only to the stable receiving-bot identity plus Telegram `update_id`, `chat_id`,
and `message_id`, under a versioned Herdres domain. Bot tokens, message text,
topic/reply metadata, and the resolved Tendwire target are not inputs. Manager
and managed-bot polling offsets are likewise keyed by the stable receiving-bot
kind, not token-derived runtime keys; a current legacy token-keyed managed-bot
offset is migrated to that stable path. Token rotation therefore preserves both
the polling position and the opaque request ID. Every distinct update receives
a different ID even when its content is identical, and Herdres does not
suppress commands by comparing their content.

The gateway derives the opaque ID before private route resolution. In legacy
mode it creates the durable schema-v2 lifecycle shell before routing,
transcription, or child creation; lane mode first commits the update and cursor
to the separate spool and creates the same unchanged state record during lane
dispatch. In either mode `created_at`, `deadline_at`, and `retain_until` are
fixed from the Telegram first-seen instant. Before Tendwire can observe a command, Herdres
canonicalizes the exact schema-v1 public request, stores that JSON string, and
fsyncs the temporary state file, atomic replacement, and parent directory. The
Tendwire child receives those stored UTF-8 bytes verbatim; an ID-only
redelivery probe consults the record before current topic, route,
transcription, or worker-state lookup. The only permitted byte change is one
`stale_target` response with disposition `no_receipt`: when the recorded target
contains `worker_fingerprint`, Herdres removes only that field, durably stores
the second exact byte string, and retries once under the same request ID.

`HERDRES_COMMAND_RETRY_HORIZON_SECONDS` fixes `deadline_at` to first-seen time
plus the effective horizon. Unset, empty, or invalid values use `86400`
seconds; configured values clamp to `60` through `604800`. Neither redelivery,
retry, `updated_at`, a terminal transition, nor a later configuration change
moves that deadline. Equality is expired: before creating another client, and
again after every retryable result, `now >= deadline_at` quarantines
(dead-letters) the local ingress record.

`retain_until` is independently fixed at first-seen time plus the effective
horizon and a `86400`-second safety margin: `172800` by default, `86460` at
the minimum, and `691200` at the maximum. Valid records are pruned only when
`now > retain_until`; equality remains cached. `updated_at`, `terminal_at`,
and `quarantined_at` describe transitions but never slide retention. Pair the
services so `TENDWIRE_COMMAND_RETRY_HORIZON_SECONDS` is at least the Herdres
horizon and `TENDWIRE_COMMAND_RECEIPT_RETENTION_SECONDS` is at least the
Herdres first-seen retention bound. Tendwire's defaults (`604800` retry,
`2592000` receipt age, and newest `4096` bounded inactive receipts per host)
cover Herdres's full range; Tendwire also enforces a `691200`-second receipt-age
floor and an age strictly greater than its own retry horizon.

Terminal accepted/rejected results and every quarantine store one exact,
sanitized child outcome. Tendwire rejection, terminal uncertainty, deadline
expiry, and unsafe/corrupt command evidence use the fixed public reply
`Could not send safely. Refresh status and choose the target again.` and the
child `checkpoint: "advance"`. The gateway sends that reply, advances and
saves the stable polling offset, and continues with later Telegram updates.
Redelivery or restart returns the cached outcome before private routing and
without constructing a Tendwire client. This bounded terminalization is
separate from Tendwire's connector-final dead-letter queue.

Herdres sends an exact schema-v1 `send_instruction` object containing only
`schema_version`, `action`, `request_id`, `dry_run`, `target`, and
`instruction`; `dry_run` is false, `instruction` contains only nonempty
`text`, and `target` is exactly one of `{worker_id}`,
`{worker_id, worker_fingerprint}`, `{space_id}`, `{name}`, or
`{name, space_id}`, with nonempty string values. Tendwire's general request-ID
grammar is `[A-Za-z0-9._-]{1,128}`; Herdres emits and locally requires its
narrower canonical 48-character `hri1_…` form. Raw Telegram receiver, update,
chat, topic, message, reply, and user IDs, bot tokens, private routes, and
backend targets never cross. The child environment preserves public Tendwire
overrides but strips Telegram variables and private ingress, gateway,
managed-bot, state, request-key, and binary-selector settings.

For a non-dry-run ingress send, Herdres accepts only an exact schema-v2 Tendwire
response with the ten fields `schema_version`, `action`, `request_id`, `ok`,
`dry_run`, `status`, `disposition`, `result`, `error`, and `warnings`; action,
request ID, and `dry_run: false` must correlate, warnings must be strings, and
the complete value must survive public pruning unchanged. It then validates the
whole disposition tuple:

- `terminal_accepted` requires `ok: true`, status `accepted`, a null error, and
  a result containing `target`, `delivery_state`, `transport_state`,
  `target_state_at_send`, and `observed_turn_state`, plus an optional nonempty
  public `turn_id`. The
  target contains only the correlated `worker_id`, delivery and transport are
  `submitted`, target state is nonempty, and observed turn state is
  `pending_observation`, `observed`, or `complete`.
- Every nonaccepted tuple requires `ok: false` and an error whose code equals
  status and whose message is nonempty.
- `in_progress` permits only status `pending`.
- `terminal_uncertain` permits only status `request_state_uncertain`.
- `terminal_rejected` permits `rejected`, `not_found`, `ambiguous_target`,
  `stale_target`, `backend_unavailable`, `backend_unsupported`,
  `ambiguous_backend_target`, `backend_failed`, `duplicate_request`, or
  `invalid_request`.
- `no_receipt` permits the same rejection-status set except
  `duplicate_request`.

The process exit is part of the envelope: exit `0` must pair with `ok: true`
and exit `1` with `ok: false`. Exit `2`, a mismatched pair, timeout,
invalid UTF-8/JSON, wrong shape/correlation, or other unproven post-start loss
becomes private process ambiguity. Herdres does not forge a schema-v2
disposition from it or expose process markers, stdout, or stderr; it keeps the
durable record retryable with no reply and retains the gateway checkpoint until
the fixed deadline. A definite spawn failure follows the same bounded retry
path. At the deadline, or for an authoritative `terminal_uncertain`,
quarantine caches the fixed safe outcome and advances the queue.

A `SIGKILL` during `command_json` is intentionally recovered by replay, not by
a local `submitting` marker: Herdres cannot prove whether the remote mutation
happened. The retry uses the same request ID and the exact fsynced request bytes,
and Tendwire's durable request receipt deduplicates those bytes before applying
the mutation. This Tendwire receipt is the double-submit protection for the
mid-RPC crash window.

`status` alone never decides retry or finality. In particular,
`backend_unavailable` with `no_receipt` retries with no reply, while the same
status with `terminal_rejected` caches the fixed failure and advances.
`stale_target` refreshes a fingerprint only when paired with `no_receipt`;
otherwise its disposition is authoritative. Back up and restore the request-ID
key with private Herdres state and the Tendwire continuity set; replacing the
key changes every derived ID and can turn a redelivery into a different
mutation.

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
descriptors and the public identity pair, never a private checkpoint, Telegram
routing, credentials, or message state.

After leasing a `final_ready` root and materializing its exact canonical
content, Herdres derives ordered multipart presentation ranges. Prepare
begin/part/commit sends only neutral field/start/end spans, never turn text;
the leased `source_ref` is bound on begin and commit, while part requests carry
only the plan token, ordinal, and ranges. Tendwire validates complete,
nonoverlapping coverage and commits stable ordered jobs. Leased upserts are
checked against the same local ranges and applied in part order, followed by
any ordered old-slot retirement. Each stable job-key receipt is reserved before
a provider operation, checkpointed after Telegram apply and again after
old-slot retirement, then ACKed to Tendwire and checkpointed as
`acknowledged`. The stable job key, not a transient lease ref, is restart
identity.

Rich-message plans retain Telegram's current 32,768-character text ceiling and
500-block ceiling for complete single-card messages. Once multipart delivery is
required, source chunks default to 24,000 characters and each rendered card is
also held below a 28 KiB UTF-8 operational ceiling. This margin avoids
provider-accepted boundary messages that some Telegram clients fail to display,
without returning to small 4K-era chunks. Ordinary `sendMessage` fallback plans
retain their separate 4,096-safe bound; a rich plan is never silently truncated
into that smaller transport. The presentation version binds the selected
transport and ranges so an older boundary-sized plan cannot be replayed as the
current layout.

`HERDRES_TENDWIRE_TURN_FINAL_LEASE_SECONDS` bounds recovery after a connector
poll response is lost. It defaults to 60 seconds; unset, empty, or invalid
values use the same 60-second fallback, and configured values are clamped to 60
through 3600 seconds. Durable plan and job checkpoints make expiry and restart
recovery idempotent without repeating a proven Telegram operation.

An ordinary restart retains the private Herdres state and Tendwire database.
Herdres resumes proven provider work under a fresh transient ref without
resending it and reconciles a committed pending plan after a lost final ACK
response or completed-plan observation, even if the turn list is empty. A
pending plan is cleared before a newer root only when Tendwire confirms
`superseded` or `plan_not_found`; every other unresolved state continues to
block that newer root.

Exactly-once applies only to acknowledged outcomes. Telegram Bot API sends
have no caller idempotency key: if Telegram may have accepted an operation but
the response, process, or message receipt is lost before Herdres durably
records it, the outcome is inherently ambiguous. Herdres reports
`delivery_uncertain` and fails closed instead of claiming provider-perfect
exactly-once; an explicit retry may duplicate the provider operation.

Goal 01B recovery continues through this same source boundary. Tendwire's
existing `turn.list` path refreshes structured content from a recovered
persisted binding before returning the durable public projection; Herdres does
not refresh Herdr itself. When a later source sync receives the authoritative
schema-v2 completed revision for a matching editable Working card, Herdres
edits that card once into the final response and records the final binding and
delivery ledger. Repeating the same sync performs no additional page fetch,
prepare, edit, send, or ledger update.

### Inspecting and retrying dead-letter finals

Inspect Tendwire's retained, connector-neutral dead-letter state with an
explicit bounded limit from 1 through 100:

```sh
tendwire connector inspect --name turn-final --status dead_letter --limit 100 --db-path /path/to/tendwire.db
```

Retry one exact unresolved final by the public `final_identity` returned by
inspection:

```sh
tendwire connector retry --name turn-final --final-identity 'twfinal1.<opaque>' --db-path /path/to/tendwire.db
```

Inspection is read-only and public-safe. Retry is identity-specific rather than
a bulk replay and can return `not_retryable` or `stale_revision`. It does not
erase provider ambiguity: retrying after an unrecorded Telegram acceptance may
duplicate a message. Tendwire, not Herdres, owns final-ready/dead-letter
retention. Never edit either database or the private Herdres state to force
replay.

### Explicit failed-plan recovery

An `attempts_exhausted` turn-final plan does not spin on later ordinary syncs.
After investigating the provider outcome, an operator may request an explicit
replacement generation:

```sh
herdres tendwire recover-turn-final \
  --plan-token twplan1.<failed-plan> \
  --request-id operator-2026.07.11:1
```

The plan token must be a bounded public `twplan1.` coordinate. The request ID
must contain 1–128 ASCII characters from `[A-Za-z0-9._:-]`; it is the durable
idempotency and audit key. Before the Tendwire RPC, Herdres requires exactly one
pending failed plan on one uniquely routable, nonquarantined worker, valid
revision/part/job coordinates, enough receipt capacity for both generations,
and old receipts consisting only of one contiguous `acknowledged` prefix
followed by failed tail receipts. A `reserved` receipt or a binding without an
acknowledged receipt is `recovery_receipt_uncertain`; a
`telegram_applied`/`old_slot_retired` receipt awaiting durable ACK is
`recovery_receipt_inflight`.

Other local preflight outcomes are `invalid_recovery_request`,
`recovery_request_conflict`, `recovery_plan_not_found`,
`recovery_route_ambiguous`, `recovery_state_invalid`, and
`recovery_capacity_exceeded`. Typed Tendwire failures pass through unchanged.
Any malformed, mismatched, or state-changing success response becomes
`recovery_state_uncertain`; no copied-state cutover occurs.

A successful response must name the same content revision, a different
`twplan1.` token in the exact next generation, the exact acknowledged-prefix
count, and an active executable suffix. When a replacement generation also
fails, Herdres requires exactly one matching prior recovery audit and request
binding for that failed token and generation. The expected retained-failure
count is the inherited audit count plus the current generation's failed tail;
that audit identity and count are included in the pre-RPC state fingerprint.
Audits referenced by pending replacement plans are never evicted; if all audit
slots are protected, preflight returns `recovery_capacity_exceeded` before RPC.
Herdres leaves every old receipt immutable, clones the acknowledged prefix
under the new token, retargets only that prefix's bindings, records the
request-keyed recovery audit, and executes only the suffix. The JSON output
includes both plan tokens, generation, prefix and executable counts, retained
failed-job and prior-attempt counts, state, and `idempotent_replay`. Repeating
the same request ID for the same failed plan returns the audited token with
`idempotent_replay=true`; reusing it for another plan conflicts. Each
replacement is one-shot, uses a new request ID, and never installs an automatic
recovery loop.

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
bindings, and preserves the topic, message history, private state, and delivery
ledger. Repeating it is a no-op.

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

Rich Telegram messages are enabled by default. Final responses render as open
rich content; working updates render as compact editable updates.

Optional per-agent bot identities are configured with generic private tokens:

```sh
HERDR_TELEGRAM_TOPICS_MANAGED_BOTS=1
HERDRES_MANAGED_BOT_CODEX_TOKEN=...
HERDRES_MANAGED_BOT_CLAUDE_TOKEN=...
```

When enabled, Herdres polls configured child bots as well as the manager bot.
Use `/voice per_agent` or `/voice shared` inside a space topic to switch that
space. Unconfigured agent kinds fall back to the manager bot. Do not commit real
bot tokens or local bot names.

Telegram voice notes are separate from bot identity. Inbound audio transcription
is local and opt-in:

```sh
python3 -m venv ~/.local/share/herdres/speech-venv
uv pip install --python ~/.local/share/herdres/speech-venv/bin/python sherpa-onnx numpy
~/.local/share/herdres/speech-venv/bin/python ~/.local/bin/herdres speech install
HERDR_TELEGRAM_TOPICS_SPEECH_INPUT=1
```

The gateway downloads a voice note with the bot token that received it, deletes
the temporary audio after transcription, and sends only the transcript through
Tendwire.

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
  pinned status. This replaces the old `herdres.timer`; there is no timer unit
  on this branch.
- `herdres-gateway.service` — inbound Telegram polling; forwards topic input to
  `herdres command` → `tendwire command`.
- `tendwired.service` — the Tendwire daemon (installed from the Tendwire repo);
  Herdres depends on it but does not manage it.

## Send transport

Herdres submits every outbound instruction through Tendwire's public command
path (`command.submit`, invoked as `tendwire command --json`). Herdres never
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
turn-final prepare/lease/ACK/recovery protocol. The dry check must succeed with
`direct_herdr_calls=0` before a live sync; it does not save the copied Herdres
state or send/edit Telegram messages. If verification fails, leave the live
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

The focused tests cover Goal 01B continuity/quarantine and recovered-final
single-edit behavior, Goal 11 stable ingress identity and exact-request
redelivery, schema-v2 descriptor isolation, lazy exact paging, neutral
multipart plans, durable checkpoint/ACK resumption, explicit uncertainty, and
one-shot failed-plan recovery. `source-smoke` must run against a copied state
file and report `direct_herdr_calls=0`.
