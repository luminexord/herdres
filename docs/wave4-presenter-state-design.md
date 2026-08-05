# Wave 4 Herdres ingress, state, and presenter design

Status: design candidate. No H7 or H6 production implementation may start
until this note, the frozen H8 ingress seam, the paired Tendwire T6 store
design, and the authoritative connector contract are approved together.

Baselines are Tendwire `9ab5597b55bb918ac8e62c86651ccac7c03d18fb`
and Herdres `ec0e36a25b16469979402abfc8bfa1525db6f68a`.

Pinned status boards and voice/STT/TTS are deleted under decision-gate
defaults. This design adds no connector name, socket method, provider search,
raw-state editor, recovery CLI, or second presentation source.

## Release units and compatibility matrix

H8, H7/H6, and Tendwire T6 have different merge boundaries:

1. H8 removes ingress lanes, child request workers, and every ingress join or
   shortcut through presentation receipts. It owns a separate durable ingress
   queue and freezes the typed H7 seam below. H8 is independently reviewable,
   mergeable, deployable, and reversible against the current Tendwire/Herdres
   contract.
2. H7 replaces `state.py` with the fresh typed lifecycle/provider-fact store.
   H6 deletes `source_sync.py` and installs one five-kind presenter. H7 and H6
   are one private stack and are not independently mergeable.
3. The combined H7/H6 stack and Tendwire T6 are one paired contract-v3 release.
   Neither side may be deployed against the old peer.

| Deployed state | Ingress | Working presentation | Final/decision presentation | Route token |
| --- | --- | --- | --- | --- |
| current baseline | legacy ingress | ordinary observation-driven working | current two-kind final path and local shortcuts | absent |
| H8 only, before T6 | one H8 queue; no receipt shortcut joins | ordinary non-durable observations may continue in the old presenter | current contract only | `None` is required and valid |
| H7/H6 + T6 | same H8 queue through the frozen typed seam | only `working` v1 from `turn-final` | only the five-kind contract-v3 poll/presenter | exact `twroute1.*` required |

The H8-only row is compatibility, not a dual presentation design. The old
presenter may continue to observe ordinary working state only after H8 has
removed ingress-to-receipt joins. H8 never fabricates an outer key or route
generation for those observations. At the paired cutover, the old observation
source, current final consumer, and every local presentation shortcut are
stopped before the contract-v3 presenter starts. The final stack has no turn
list/delta, pending list, transcript, snapshot-content, or local state source
for presentation.

## Frozen transport v2 and paired payload contract v3

The wire transport remains the existing `turn-final` connector. The generic
socket methods remain exactly:

```text
connector.prepare
connector.poll
connector.ack
connector.fail
connector.defer
connector.renew
connector.release
connector.reclaim
connector.retry
connector.inspect
```

Final materialization also retains the existing generic
`turn.content.get` method; it is not a connector-specific method. Its request
has exactly `schema_version: 1`, `turn_id`, `content_revision`, `field`, and an
optional `cursor`. `field` is `user_text` or `assistant_final_text`. A successful
page has exactly `schema_version`, `ok`, `turn_id`, `content_revision`, `field`,
`availability`, `index`, `count`, `text`, `segment_id`,
`segment_char_length`, `segment_byte_length`, `total_char_length`,
`total_byte_length`, and nullable `next_cursor`. H6 requires `ok: true`,
`availability: "complete"`, exact identity correlation, zero-based contiguous
indexes, a constant positive count equal to the descriptor page count, exact
Unicode/UTF-8 lengths, unique `twseg1.*` segment IDs, an acyclic `twcur1.*`
cursor chain, and null final cursor. The concatenated page text must equal the
descriptor's exact total lengths. Unknown fields, partial pages, stale revision,
cursor cycles, or length mismatch stop preparation without provider mutation.

There is no `telegram-present`, connector alias, connector-specific method,
second queue, or inner job key. The polled outer `key` is the sole durable
replay and provider-fact identity. `ref` is only the newest attempt-scoped lease
capability. Attempts, refs, plan tokens, content revisions, state tokens, and
provider coordinates never become alternate deduplication keys.

T6 changes the payload contract, not the transport. The paired contract-v3
consumer accepts exactly these five kinds and versions under one poll loop:

```text
working      schema_version 1
final_ready  schema_version 3
final_part   schema_version 2
retire       schema_version 1
decision     schema_version 1
```

Every payload has exactly `schema_version`, `kind`, `created_at`, `worker`, and
`route`, plus the declared kind-specific object. `worker` has exactly
`worker_id`, `stable_key`, `stable_key_version`, and `route_generation`.
`route` has exactly `partition_key` and positive `partition_sequence`.

`route_generation` has exact grammar:

```text
twroute1.<43 unpadded URL-safe base64 characters>
```

It is opaque and compared byte-for-byte. It is neither decoded nor derived
from labels, local revision, Telegram coordinates, H8 state tokens, or
installation paths. All five payloads must carry the same stable-key/version
and route-generation correlations as their outer queue partition.

Outer keys have these exact grammars:

```text
final_ready:        turn-final:revision:twfinal1.<43 base64url characters>
final_part/plan retire:
                    turn-final:twplan1.<1-256 base64url characters>:<six decimal digits>
working:            turn-final:working:twwork1.<43 base64url characters>
decision:           turn-final:decision:twdecision1.<43 base64url characters>
standalone retire:  turn-final:retire:twretire1.<43 base64url characters>
```

No payload contains `key`, `job_key`, `delivery_key`, `ref`, or `attempt`.

## Exact five-kind payload validation

The socket client validates the daemon envelope, item, and complete payload
before state lookup, paging, rendering, or mutation:

- one bounded newline-terminated UTF-8 JSON object each way, exact request ID,
  exact schema-1 daemon envelope, consistent success/error branch, and both
  outer and inner `ok` true for success;
- exact `name: "turn-final"`, documented status, unique outer keys, exact
  `twref1.*` ref, positive non-Boolean attempt, canonical timestamps, and no
  unknown item fields;
- exact kind/version/common fields, exact opaque-token grammar, bounded
  depth/items/strings, no duplicate keys/ordinals, and no Boolean-as-integer;
- recursive rejection of private identities, provider coordinates,
  credentials, socket/path/process/session data, command/argv/environment,
  ACP option IDs, stdout/stderr, exception prose, or forbidden values hidden in
  otherwise allowed strings.

Kind-specific validation is exact:

- `working` adds only `turn`: `turn_id`, `content_revision`, nullable
  `replaces_key`, and `text`. `text` has only `assistant_stream_text`,
  `char_length`, and `byte_length`; both lengths must match exactly.
- `final_ready` adds only `turn`: `turn_id`, `final_identity`,
  `content_revision`, nullable `replaces_key`, and exact content-schema-v1
  descriptors. The descriptor has only `schema_version`, `content_revision`,
  `known_incomplete`, and `fields`; fields are exactly `user_text` and
  `assistant_final_text`, each with availability, inline value, exact lengths,
  page count, and first cursor. Final identity, revision, key, cursor, and
  descriptor correlations must agree.
- `final_part` adds exactly `turn`, `plan`, and `lineage`. `turn` has turn ID,
  final identity, and content revision. `plan` has plan token, positive
  generation, presentation version, ordinal, part count, and ordered,
  nonoverlapping spans over the retained exact revision. `lineage` has nullable
  recovered plan token, predecessor key, and replaces key, all present even
  when null.
- `retire` adds exactly `turn` and `retire`. `retire` has immutable
  `target_key`, target kind, nullable target ordinal, nullable predecessor key,
  nullable plan token/generation, and the exact enumerated reason. It contains
  no provider coordinate or content. Target kind is exactly `working`,
  `final_part`, or `decision`; reason is exactly `working_replaced`,
  `final_replaced`, `excess_part`, or `decision_resolved`.
- `decision` adds only `decision`: decision ref, revision digest, mode, title,
  body, and bounded choices. Each choice has only public ordinal, public
  one-based `option_ref`, and label. Private ACP choice IDs never leave
  Tendwire. Mode is exactly `single`, `multi`, or `plan`; ordinals and option
  refs are unique and choices are in canonical ordinal order.

The exact outer-key grammars and cross-field correlations are those in the
paired contract-v3 document. Unknown kinds, versions, fields, enum values,
nullable omissions, cross-revision spans, malformed text lengths, route/key
conflicts, and invalid target keys fail or defer without a Telegram mutation.
The exact-text allowlist includes `assistant_stream_text`, decision title/body/
labels, and materialized final slices; sanitization must not silently alter
validated text or its lengths.

## Frozen H8/H7 typed seam

H8 owns ingress identities, its queue, claims, receipts, and notification
cadence. H7 owns current lifecycle receivers and the private provider route.
H7 preserves H8's exact five state operations, the shared guard operation, and
their dataclass meanings:

```text
read_ingress_policy(state_path: Path) -> IngressPolicy
resolve_ingress_route(state_path: Path, query: IngressRouteQuery) -> IngressRouteResult
resolve_ingress_reply(state_path: Path, query: IngressReplyQuery) -> IngressRouteResult
read_decision_ingress(state_path: Path, query: DecisionIngressQuery) -> DecisionIngressResult
apply_decision_ingress(state_path: Path, mutation: DecisionMutation) -> DecisionMutationResult
provider_mutation_guard(
    state_path: Path,
    owner: PhysicalOwner,
    deadline_monotonic: float,
) -> ContextManager[ProviderMutationGuard]
```

Receiver credentials use the separate ephemeral call:

```text
read_ingress_receivers(state_path: Path) -> tuple[IngressReceiver, ...]
```

The exact common types are:

```text
StateToken:
    value: str                    # opaque, nonempty, max 128 bytes

PhysicalOwner:
    bot_identity: str             # stable private provider-account identity
    chat_id: str
    topic_id: str

ProviderMutationGuard:
    owner: PhysicalOwner
    lock_identity: str            # opaque pg1.<43 base64url>, never logged

IngressReceiver:
    receiver_id: str              # stable non-secret identity
    bot_kind: str                 # manager or supported managed kind
    username: str
    token: SecretStr              # ephemeral; repr/log/serialization forbidden

IngressPolicy:
    chat_id: str
    general_topic_id: str
    owner_user_ids: frozenset[str]
    managed_usernames: tuple[(username: str, bot_kind: str), ...]
    state_token: StateToken

RouteStatus enum:
    RESOLVED, MISSING, AMBIGUOUS, QUARANTINED, RETIRED, STALE,
    BINDING_AMBIGUOUS, AUTHOR_AMBIGUOUS

StableOwner:
    stable_key: str
    stable_key_version: int       # exact 1
    route_generation: str | None  # null pre-T6; twroute1.* after paired cutover

IngressRouteQuery:
    chat_id: str
    topic_id: str
    receiver_bot_kind: str
    explicit_alias: str
    explicit_bot_kind: str
    state_token: StateToken

IngressReplyQuery:
    chat_id: str
    topic_id: str
    reply_message_id: str
    observed_author_bot_kind: str
    explicit_alias: str
    explicit_bot_kind: str
    state_token: StateToken

IngressRouteResult:
    status: RouteStatus
    state_token: StateToken
    chat_id: str
    topic_id: str
    worker_id: str
    worker_fingerprint: str | None
    owner: StableOwner | None
    space_id: str
    bot_kind: str
    reply_binding_id: str | None
    binding_was_present: bool
    reason: str                  # bounded enum-like public reason
```

The exact decision types are:

```text
DecisionIngressQuery:
    chat_id: str
    topic_id: str
    callback_ref: str | None      # nonempty exact callback; null active-by-topic
    state_token: StateToken

DecisionStatus enum:
    ACTIVE, EXPIRED, MISSING, AMBIGUOUS, QUARANTINED, STALE

DecisionOption:
    option_ref: str
    label: str                   # bounded public Telegram label

DecisionIngressResult:
    status: DecisionStatus
    state_token: StateToken
    decision_ref: str
    revision_digest: str
    mode: str                    # single, multi, plan
    worker_id: str
    owner: StableOwner | None
    message_binding_id: str
    chat_id: str
    topic_id: str
    message_id: str
    option_refs: tuple[str, ...]
    options: tuple[DecisionOption, ...]  # same refs/order as option_refs
    selected_refs: tuple[str, ...]
    await_freeform: bool
    render_fingerprint: str
    physical_owner: PhysicalOwner

DecisionMutationKind enum:
    ARM_FREEFORM, TOGGLE_OPTION, RECORD_LOCAL_MARKUP

DecisionMutation:
    request_id: str              # first half of composite idempotency identity
    kind: DecisionMutationKind
    decision_ref: str
    revision_digest: str
    option_ref: str | None
    desired_selected_refs: tuple[str, ...]
    desired_markup_fingerprint: str
    expected_state_token: StateToken

DecisionMutationStatus enum:
    APPLIED, ALREADY_APPLIED, STALE, CONFLICT, EXPIRED

DecisionMutationResult:
    status: DecisionMutationStatus
    state_token: StateToken
    decision_ref: str
    selected_refs: tuple[str, ...]
    await_freeform: bool
    message_binding_id: str
    message_id: str
    desired_markup_fingerprint: str
```

`decisions.render_ingress_markup(snapshot, desired_selected_refs)` is the one
public pure H8 renderer. It accepts only a typed active `DecisionIngressResult`,
returns the exact bounded Telegram markup object and fingerprint, and performs
no state/provider I/O. H8 checkpoints those exact bytes before mutation.

For `DecisionIngressQuery`, null `callback_ref` resolves exactly one active
decision in the exact chat/topic and is used only by an armed free-form message.
A nonempty value performs exact callback lookup. Empty strings and multiple
active topic decisions fail closed; neither form falls through to raw state.

`state_token` is an opaque H7 snapshot/CAS token returned only by
the typed reads/mutations. H8 persists and compares it only as an opaque value;
it never parses it, substitutes its queue revision, or uses it as a replay
identity. A stale token returns the exact `STALE` result so H8 re-reads and
revalidates before any socket or Telegram call. Pre-cutover `StableOwner`
equality permits `route_generation=None`; post-cutover newly resolved routes
require an exact matching `twroute1.*` token. H8 never mints or derives one.

Each mutator takes the state flock, reloads, validates token and semantic
decision identity, applies at most one idempotent bounded control mutation, and
returns a new token. No Telegram or Tendwire call occurs under the flock.
Receiver secrets exist only in memory and never enter policy, route, queue,
state results, repr, or logs. H7 stores no receiver credential, so paired
cutover requires all receiver tokens/usernames in environment configuration.

`provider_mutation_guard` is part of the frozen H8/H7 seam, but is not the state
flock. Both the H8-only old presenter and the paired H7 presenter use this exact
guard for every decision message markup/edit/delete; H8 uses it for local
markup. `PhysicalOwner` equality is byte-for-byte equality of all three fields.
The lock namespace is the sibling owner-private directory
`.provider-mutation-locks`; its file name is
`pg1.` plus the unpadded URL-safe HMAC-SHA256 of domain
`herdres-provider-owner-v1`, NUL, and canonical UTF-8 JSON for the three owner
fields, using the retained request-ID HMAC key. No provider coordinate appears
in a file name or log. The directory is EUID-owned mode 0700. Lock files are
EUID-owned regular single-link mode 0600, opened relative to a pinned directory
FD with `O_NOFOLLOW|O_CLOEXEC`, and held with exclusive `flock` until context
exit. The helper may create the absent directory with one `mkdirat(..., 0700)`
under the already pinned owner-private state parent; an existing directory is
validated, never chmod-repaired. Unsafe namespace, file, key, or deadline fails
closed.

Acquisition retries only until `deadline_monotonic`; it never holds the state
flock or a queue transaction while waiting. The global order is: acquire the
provider guard; perform at most one short H8 lease-renew/CAS transaction; call
typed state reads/mutations, each of which takes and releases the state flock;
perform at most one known-target Telegram mutation; checkpoint state and queue;
release the provider guard. A queue transaction and state flock are never held
together. Before Telegram, H8 must renew enough queue lease for the provider
timeout plus state-fsync and settlement margins, re-read the exact decision,
and byte-compare decision ref/revision, message binding/ID, physical owner,
selected refs, and markup fingerprint. The retained old presenter repeats its
decision target lookup under this same guard before a retire or markup edit.
Consequently a local markup edit cannot race a presenter decision retire.

H8 may import only these frozen operations and their types. It may not import
presenter, topic mutation, provider-job, current-slot, or persistence
primitives. The private route/result coordinates never enter Tendwire payloads.

## Fresh H7 state: provider facts, not a delivery ledger

H7 creates schema 1 fresh and refuses every other schema. It does not migrate
the old JSON. The canonical file is at most 64 MiB and has exactly these
top-level keys:

```text
schema_version
revision
workers
spaces
topics
topic_create_claims
topic_tombstones
provider_messages
provider_jobs
provider_aliases
current_slots
decision_controls
```

`schema_version` is exact integer 1. `revision` is a nonnegative integer that
increments once per committed logical mutation. Map limits are exact:

```text
workers                 512 records
spaces                  128 records
topics                1,024 records
topic_create_claims     128 records
topic_tombstones      4,096 records
provider_messages     8,192 records
provider_jobs        16,384 records
provider_aliases     16,384 records
current_slots         8,192 records
decision_controls       512 records
```

Every map key and identity/fingerprint/token string is nonempty canonical UTF-8
and at most 256 bytes, except exact outer keys and opaque tokens which may use
their contract bound up to 512 bytes. Labels are at most 256 bytes, bounded
public reason enums at most 64 bytes, timestamps at most 40 ASCII bytes, and a
canonical serialized record at most 8 KiB. Integers reject Booleans. Unknown
fields, enum values, noncanonical timestamps, duplicate tuple values, excess
UTF-8 bytes, and non-finite numeric values refuse the whole file.

The record schemas are exact:

```text
WorkerRecord:
    worker_id, worker_fingerprint, stable_key, stable_key_version,
    route_generation, space_id, bot_kind, lifecycle_status,
    observed_at, expires_at
    lifecycle_status = live | quarantined | retired

SpaceRecord:
    space_id, bot_kind, lifecycle_status, observed_at, expires_at
    lifecycle_status = live | quarantined | retired

TopicRecord:
    topic_binding_id, lifecycle_owner_key, route_generation,
    physical_owner, status, created_at, updated_at
    status = active | quarantined | retired | gone

TopicCreateClaim:
    claim_id, physical_owner, lifecycle_owner_key, status,
    provider_topic_id, created_at, updated_at
    status = reserved | in_flight | accepted_candidate |
             accepted_orphan | active | ambiguous

TopicTombstone:
    physical_owner, lifecycle_owner_key, outcome, retired_at
    outcome = retired | already_gone | accepted_orphan

ProviderMessage:
    provider_message_binding_id, physical_owner, message_id,
    created_kind, captured_route_generation, current_owner_key,
    status, created_at, updated_at
    created_kind = working | final_part | decision
    status = active | quarantined | deleted | gone

ProviderJob:
    outer_key, kind, payload_fingerprint, captured_route_generation,
    physical_owner, provider_message_binding_id, target_key,
    render_fingerprint, outcome, accepted_at
    kind = working | final_part | retire | decision
    outcome = sent | edited | not_modified | accepted_quarantined |
              deleted | already_gone | protected_reuse_noop

ProviderAlias:
    old_outer_key, provider_message_binding_id, current_owner_key, updated_at

CurrentSlot:
    slot_id, logical_identity, slot_kind, ordinal,
    provider_message_binding_id, current_owner_key, route_generation,
    status, updated_at
    slot_kind = working | final_part | decision
    status = current | quarantined | retired

DecisionControl:
    decision_ref, callback_ref_hash, revision_digest, mode, option_refs, selected_refs,
    await_freeform, render_fingerprint, desired_markup_fingerprint,
    applied_markup_fingerprint, route_generation, physical_owner,
    provider_message_binding_id, message_id, current_owner_key, status,
    applied_ingress_identities, created_at, updated_at
    mode = single | multi | plan
    status = active | resolved | retired | quarantined
```

Nullable fields are present as null. A provider job's `target_key` is non-null
only for retire; its provider-message reference is non-null for an accepted
send/edit/not-modified outcome and null for an accepted target-less no-op. A
decision has at most 64 unique canonical `option_refs` and 64 selected refs,
with selected refs a subset in option order. It retains no title, body, label,
or private ACP option ID. `applied_ingress_identities` has at most 256 unique
entries `(request_id, DecisionMutationKind, mutation_digest)`. The first two
fields are the composite idempotency identity; `mutation_digest` is the
unpadded base64url SHA-256 of the domain-separated canonical mutation bytes.
Request IDs and mutation-kind values retain their already validated bounded
canonical forms. Thus `TOGGLE_OPTION` and `RECORD_LOCAL_MARKUP` for one queue
request have distinct identities while each remains replay-stable and can
return `ALREADY_APPLIED` independently; replay of one composite identity with
a different digest fails closed as corruption.

`provider_jobs` is the immutable outer-key authority. It is inserted once only
after a known provider acceptance/outcome and can never be updated, rebound, or
deleted inside retention. Current ownership, aliases, slots, controls, and
message tombstones are separate mutable indexes. Raw provider responses are
never stored.

Aliases are one hop. Every alias old key must name an immutable provider job,
the current owner must name a non-alias provider job for the same provider
message, and old/current keys must differ. On a second reuse, the same atomic
transaction flattens every retained alias for that provider message directly to
the new owner. No alias may target another alias, cross provider messages, or
form a self-edge/cycle. The old provider job is never changed.

Decision mutation atomically validates decision/ref/revision/route/message and
the composite `(request_id, DecisionMutationKind)` plus mutation digest, then
either applies that exact entry or returns `ALREADY_APPLIED`. The same composite
identity with different canonical mutation bytes/digest is corruption.
Selection/free-form and markup-applied fingerprints are distinct mutations; a
crash between them resumes the missing mutation without toggling twice.

There is no local plan, pending-plan, accepted-ordinal, recovery, completion,
source-root, retry, lease, attempt, delivered-turn, notification, queue, or
presentation-content ledger. H7 does not persist canonical text, spans, poll
payloads, refs, provider errors/responses, private ACP option IDs, credentials,
paths, process IDs, or exception prose. Tendwire alone owns plans, DAG/FIFO,
recovery lineage, completion, attempts, retry, and source-root state.

Bounds refuse new work; they never evict a live route, current slot, targetable
immutable job, alias, unretired provider message, active decision control, or
unresolved topic claim. The paired T6 release fixes dead-letter/retry inspection
at 30 days and route/content/retire-target retention at 45 days. H7 retains every
provider job and every associated message, alias, tombstone, slot history, and
terminal decision control for at least 60 days after the last reference becomes
terminal. Active/current/unresolved facts have no age-based pruning. The fixed
15-day margin is validated at startup; H7 refuses configuration claiming a T6
horizon above 45 days rather than silently pruning earlier. Terminal pruning is
one bounded reference-checked mutation and never rewrites surviving immutable
jobs.

## Frozen H7 APIs and lock ownership

All APIs accept `state_path: Path`, frozen typed inputs, and return frozen
dataclasses/enums. Mutators take the state flock, reload and validate schema 1,
perform one logical transition, increment revision, and atomically fsync the
new state. No caller receives a root dictionary, transaction, or generic
load/save primitive.

Lifecycle/topic APIs, used only by orchestration and `topics.py`, are:

```text
reconcile_lifecycle(state_path, observation) -> ReconcileResult
resolve_presentation_route(state_path, route_identity) -> RouteResult
list_topic_actions(state_path, now) -> tuple[TopicAction, ...]
record_topic_action(state_path, action, provider_result) -> TopicActionResult
reserve_topic_create(state_path, request) -> TopicCreateClaim
mark_topic_create_in_flight(state_path, claim) -> TopicCreateClaim
record_topic_create_acceptance(state_path, claim, topic_id) -> TopicCreateResult
read_topic_create_blocker(state_path, owner) -> TopicCreateBlocker | None
```

Presenter APIs, used only by `presenter.py`, are:

```text
read_provider_job(state_path, key) -> ProviderJob | None
select_provider_mutation(state_path, request) -> ProviderMutation
record_provider_mutation(state_path, acceptance) -> ProviderJob
record_retire_outcome(state_path, outcome) -> ProviderJob
read_current_slots(state_path, logical_identity) -> CurrentSlots
```

The frozen `provider_mutation_guard` spans the final key/decision recheck,
route/message revalidation, lease renewal where applicable, at most one
Telegram mutation, and immediate provider-fact or markup-phase checkpoint. It
never spans polling, paging, preparation, or rendering. No Telegram call occurs
under the state flock or H8 queue transaction.

Deleted APIs include generic load/save/reload/root/lock exports, mutable
message dictionaries, delivered-turn tracking, source entries, rekey plans,
recovery registries, response-fold health, orphan journals, and raw finders.

## One poll loop and binding-before-route rule

One presenter polls `turn-final`; there is no `source_sync` companion loop.
After full validation it dispatches by kind.

`final_ready` is prepare-only and never binds a provider job. It may resolve no
Telegram route and performs zero Telegram mutations. The presenter materializes
the exact retained revision, derives a deterministic bounded part layout, and
uses `connector.prepare`. Commit moves the source root to Tendwire-owned
awaiting-ACK state; later `final_part` and `retire` rows arrive through the same
poll loop. H7 stores no preparation state.

The provider kinds are `working`, `final_part`, `retire`, and `decision`. Every
provider-kind lease follows this order:

1. Validate the response, item, payload, outer-key grammar, route generation,
   partition coordinates, and kind correlations.
2. Call `read_provider_job(key)` before any current route or slot lookup.
3. If the immutable fact matches, make no provider call and ACK the newest ref,
   even if the current worker/route is now retired or quarantined.
4. If the same key has another fingerprint, kind, target, route generation, or
   captured coordinates, fail closed as protocol/local-state corruption.
5. Render/materialize at most one exact provider request outside all locks.
6. Renew if the remaining lease lacks the guard, provider-call, fsync, and ACK
   safety budget.
7. For working/final-part/decision, resolve the exact payload stable owner and
   route generation and acquire its physical-owner guard. For retire, resolve
   `target_key` to its immutable provider message/captured physical owner before
   acquiring that owner's guard; never retarget through the current live route.
   Under the guard repeat key lookup, applicable route/target resolution,
   target selection, and lease renewal.
8. Perform at most the kind's authorized mutation budget.
9. Immediately record the accepted provider fact and all ownership/slot effects
   in one state transaction, even if current lifecycle revision changed during
   the provider call.
10. Release the guard and settle the newest ref exactly once.

The accepted checkpoint for a replacement that edits/reuses an old provider
message is atomic: write the new immutable outer-key owner fact, add an alias
reference from the unchanged old key to the same message/new owner, and update
the current slot. A crash exposes either the complete old ownership or the
complete new ownership; it cannot expose a rewritten old job or a slot without
its new owner fact.

## Exact mutation budget and settlement

| Kind | Telegram content mutation budget | Durable successful outcomes |
| --- | ---: | --- |
| `final_ready` | 0 | prepare committed/rediscovered in Tendwire; root is not ACKed directly |
| `working` | at most 1 edit or send | accepted current, accepted superseded/quarantined, not-modified |
| `final_part` | at most 1 edit or send | accepted current, accepted alias/reuse, accepted quarantined, not-modified |
| `retire` | at most 1 delete | deleted, already gone, protected-reuse no-op |
| `decision` | at most 1 edit or send | accepted current, accepted superseded/quarantined, not-modified |

There is no edit-then-send, rich-then-plain fallback, upsert-plus-delete, or
multi-part provider call in one row. Message-not-found on an edit durably marks
the selected target gone and defers; a later attempt may select one send.
`message is not modified` is accepted only for the exact immutable target and
render fingerprint.

Missing topic, route churn before the call, guard-budget exhaustion, 429, and a
transport failure proven to occur before Telegram could start the request
defer. Permanent payload/content/protocol failure fails with an enumerated
public reason. Ambiguous renew causes no provider call. Every branch uses
exactly one of ACK, fail, defer, or release for the newest live ref.

Provider uncertainty is operation-exact:

- definitely not started: no provider fact exists and ordinary bounded retry/
  defer is safe;
- known acceptance/result: fsync the immutable job and ownership effects before
  ACK, even if the lease expired during the call;
- started or outcome-ambiguous new send: create no false provider fact and fail
  with exact reason `provider_uncertain`; T6 moves that row/root immediately to
  inspectable dead letter without another automatic attempt;
- started or outcome-ambiguous edit of a known immutable message: a later
  attempt may repeat only the same target and render fingerprint; exact success
  or `message is not modified` converges, while target drift fails closed;
- started or outcome-ambiguous delete of a known immutable target: a later
  attempt repeats only after target/alias/current-owner revalidation under the
  guard; exact success, not-found, or protected reuse converges.

Explicit retry/recovery of a dead-letter ambiguous send is an operator choice
that can create a duplicate or orphan and must say so in inspect/release notes.
Herdres never converts ambiguity into acceptance or claims exactly-once.

## Route, guard, lease, and crash boundary

The route fence for a provider upsert is the exact payload stable key/version
and route generation. Before a call, missing/mismatched/retired/quarantined
route evidence defers or fails closed according to its exact enum. Drift while
waiting for the guard is caught by the repeated resolution. Drift during the
provider call cannot erase acceptance: H7 checkpoints the immutable key,
captured route generation, captured physical owner, and provider result, then
either installs it as current or leaves it quarantined. It never attaches that
fact to the newer route.

T6 inserts `route_generation` into public worker metadata before computing and
publishing that observation's `worker_fingerprint`; it atomically stores the
same enriched fingerprint on the matching private worker binding. H7 accepts a
post-cutover route only when stable owner, exact route generation, worker ID,
and enriched fingerprint all correlate. H8 checkpoints that fingerprint before
`command.submit`. The pre-T6 one-time fingerprint-removal compatibility is
legal only when the stored route generation is null. Once a request has a
non-null `twroute1.*`, H8 may neither strip nor refresh its fingerprint; a stale
fingerprint quarantines that immutable request rather than sending it to a new
route. Tendwire command submission CASes the supplied enriched fingerprint, so
route rotation between local resolution and send cannot retarget the command.

The shared provider guard is keyed by private physical
`(bot identity, chat_id, topic_id)`, not
stable worker key or route generation. Different Tendwire partitions may run
concurrently, but generations sharing one Telegram topic serialize. H8 ingress
takes the same guard around its bounded Telegram reply/markup mutation and never
while holding its queue transaction.

The client derives a monotonic local lease deadline from the validated server
timestamp. Paging/rendering occurs outside the guard. Waiting for the guard is
bounded. Renewal is required whenever the remaining interval does not cover
the provider timeout, immediate state fsync, guard release, and ACK margin.
Provider timeout is strictly below that remaining mutation budget. A stale,
expired, malformed, unsuccessful, or transport-ambiguous renew authorizes no
provider call.

The honest duplicate/unknown-object window is irreducible:

```text
Telegram may accept a new send -> response is lost or Herdres dies
-> provider fact was not fsynced
```

Telegram provides neither a connector idempotency key nor an authoritative
message lookup that closes this window. Automatic retry is forbidden, but an
explicit operator retry can repeat the new send.
After the immutable outer-key fact is fsynced, ACK loss, lease expiry, new refs/
attempts, process restart, and route drift cannot repeat that key. Known-target
edits commonly converge through not-modified; the design never calls either
case exactly-once across the pre-fsync window.

## Final-ready prepare and response loss

The request transport remains prepare schema 1 with only `begin`, `part`,
`commit`, and `recover`; H6 does not add fields or methods. Contract v3 removes
source-less prepare. Every new plan is rooted in one polled `final_ready` v3
outer key. Begin identity is exactly source outer key, final identity, content
revision, route generation, presentation version, part count, and generation
one. Begin and first commit require the newest live source ref. Part uses the
plan token and Tendwire internally fences it to that same source root/attempt.
Omitting `source_ref` from begin or first commit is `invalid_ref`; H6 never calls
source-less begin and has no local source from which it could do so.

Successful prepare results are exact inner objects. No listed field is optional
and no extra field is accepted:

```text
begin:
    schema_version=1, ok=true, status="ok", host_id, name="turn-final",
    plan_token, state, generation, part_count, accepted_ordinals

part:
    schema_version=1, ok=true, status="ok", host_id, name="turn-final",
    plan_token, state, generation, part_count, ordinal, accepted_ordinals

commit:
    schema_version=1, ok=true, status="ok", host_id, name="turn-final",
    plan_token, state, generation, part_count, job_count, accepted_ordinals

recover:
    schema_version=1, ok=true, status="recovered", host_id,
    name="turn-final", failed_plan_token, plan_token, generation,
    content_revision, state="active", acknowledged_prefix_count,
    executable_job_count, retained_failed_job_count, prior_attempt_count,
    idempotent_replay
```

`accepted_ordinals` is a sorted unique array of non-Boolean integers in
`[0, part_count)`, bounded by the 10,000-part contract ceiling. `ordinal` is the
exact part request ordinal. `job_count` is the complete committed executable
and retire-node count. Generation and all counts are nonnegative integers with
generation/part count positive. Begin/part replay may report state
`preparing`, `active`, `waiting_predecessor`, `completed`, `failed`, or
`superseded`; commit reports only `active`, `waiting_predecessor`, `completed`,
`failed`, or `superseded`. Plan tokens and every correlation are byte-identical
on replay. Error results use the authoritative contract's exact closed status
and correlation fields, `ok: false`, and no exception/private prose.

H6 renews the source root after validation, after each bounded page batch,
before each part upload, and immediately before first commit. Failed or
ambiguous renew stops preparation.

Response-loss behavior is exact:

- Lost begin: repeat identical begin; rediscover the same token, state, count,
  and accepted ordinals. After lease expiry only a new attempt of the same
  immutable root may rebind that staged plan.
- Lost part: repeat the identical ordinal/spans; a mismatch is conflict.
- Crash before commit: repoll the same outer root and resume missing ordinals;
  no H7 state is required.
- Lost commit: repeat commit. A committed replay returns the exact persisted
  result even though the old ref is no longer live, but only after full
  plan/source/key/identity correlation. An unrelated stale ref learns nothing.
- Recover: repeat the bounded request ID to rediscover the same new suffix.

Uncommitted stale refs remain stale. H6 never infers commit from local state.
After successful commit it does not ACK the final-ready ref: Tendwire owns the
awaiting-ACK root and completes it only when the effective provider lineage is
delivered.

## Recovery lineage and target-key retirement

Tendwire may retain an acknowledged prefix and emit a new suffix. H6 validates
predecessor links through immutable provider jobs: bounded, acyclic, same final
identity/revision/presentation version, compatible part count, and contiguous
ordinals. Old keys and facts remain unchanged. The first executable suffix has
no executable predecessor when the acknowledged prefix is empty; the source
root is separate and cannot deadlock FIFO eligibility.

Missing acknowledged predecessor facts are local provider-state loss. H6 fails
closed; it does not reconstruct them from Telegram, resend the prefix, or add a
local recovery registry.

A `retire` addresses only immutable `target_key`. H6 first reads that target's
provider job/message before current route lookup:

- If the target still owns the message and no current slot/new owner protects
  it, perform one delete and atomically write the retire key's immutable job
  fact, tombstone the provider message, and clear/retire current indexes. The
  target key's immutable provider job is never changed.
- If a replacement owns the same message and the target is an immutable alias
  reference, record `protected_reuse_noop`; never delete it.
- If a durable tombstone says deleted/gone, record `already_gone`.
- Provider not-found becomes `already_gone`; uncertain provider outcome never
  becomes deleted.

Replacement-driven working/final retires are pollable only after their explicit
replacement predecessor is delivered. Decision-resolution retire is the one
exception: T6 may poll it after the target decision has no live attempt and is
terminal as delivered, superseded, or dead-letter, because any prior lease may
have reached Telegram without ACK. H6 accepts that row, resolves only immutable
`target_key`, and applies the same ownership/alias/tombstone proof. If the
target has no durable provider fact or tombstone, H6 fails the mandatory retire
closed; it never invents `already_gone`. A decision superseded before its first
lease has no possible provider object and T6 does not poll a retire for it.

Herdres never decides that mandatory retire work is unnecessary or silently
drops it. Protected reuse is itself a durable ACKed outcome. Contract v3 has no
standalone route-retirement payload reason or implicit route-retirement
producer; known topic/provider cleanup remains the bounded Herdres lifecycle
defined below.

## Decision delivery and decision ingress

Decision presentation and decision answers are separate flows:

- Delivery: Tendwire enqueues immutable `decision` v1 rows. The one presenter
  validates public choices and performs at most one send/edit. It atomically
  records the outer-key provider fact, current decision slot, and bounded
  `decision_controls` metadata before ACK.
- Ingress: a callback is accepted only when its provider message, decision ref,
  revision digest, and public option ref match `read_decision_ingress`. H8
  durably queues the answer with its receiver and opaque state-token checkpoint.
  `ARM_FREEFORM`, `TOGGLE_OPTION`, and `RECORD_LOCAL_MARKUP` use only the frozen
  idempotent `apply_decision_ingress` CAS and their distinct composite phase
  IDs. Any Telegram markup call occurs under the shared physical-owner guard
  after that state flock is released and after decision/message/route
  revalidation. H8 resolves the command route through the frozen route/reply
  seam and submits the public option ref to Tendwire. It never calls presenter,
  provider-job, or current-slot APIs.
- Resolution: Tendwire atomically supersedes outstanding decision delivery and
  emits an ordered `retire` targeting the decision outer key. The retire removes
  or logically retires controls through the normal provider-kind flow.

Duplicate callbacks converge through H8/Tendwire command receipts, not a H7
delivery ledger. Stale revision digest, absent control, wrong message, resolved
decision, or route-token mismatch produces an exact neutral/rejected result
without leaking private ACP identity.

## Topic lifecycle and create ambiguity

Topic creation is lifecycle work, never a content-row side effect. Missing
topic defers the content lease before mutation. The bounded state machine is:

```text
absent -> reserved -> in_flight -> accepted_candidate
                                 -> active | accepted_orphan
in_flight + lost outcome -> ambiguous
```

Reserve is fsynced before a call; in-flight is fsynced immediately before it;
a returned ID is fsynced against captured coordinates before classification.
Owner drift creates a known accepted orphan eligible for bounded known-ID
retirement. A lost in-flight result is ambiguous and blocks that physical owner;
automatic retry and provider discovery are forbidden. Known name/icon/close/
retire actions authorize one provider mutation and checkpoint the exact fact.

## Filesystem, socket, and privacy boundary

H7 JSON state and the H8 SQLite queue use separate files/locks but the same
filesystem policy: absolute leaf, component-by-component dirfd traversal with
`O_DIRECTORY|O_NOFOLLOW|O_CLOEXEC`, EUID-owned non-group/world-writable private
parents, regular EUID-owned one-link leaf, exact 0600 files, and pinned/rechecked
device/inode identity. Unsafe existing files or SQLite WAL/SHM/journal sidecars
are refused, not chmod-repaired. H8 connects through an anchored pinned leaf,
uses no arbitrary SQLite URI, validates bounded JSON on reads and writes, and
holds no transaction across socket/provider I/O.

H7 atomic JSON writes create a unique 0600 regular temporary file in the pinned
parent, write bounded canonical JSON, fsync it, rename within the same dirfd,
and fsync the parent. Corruption, wrong version, unsafe topology, or over-bound
state causes explicit refusal and is never silently recreated.

The AF_UNIX client uses an absolute anchored socket path, no-follow parent walk,
EUID-owned socket with no mode bits beyond 0600, socket-type and device/inode
checks before and after connect, `AF_UNIX/SOCK_STREAM`, same-EUID `SO_PEERCRED`,
a 1 MiB frame ceiling, exact UTF-8/JSON, monotonic deadline, and deterministic
descriptor cleanup.

Tendwire sees no Telegram coordinate or provider fact. Herdres accepts only
the five enumerated public payloads and neutral public error/reason enums. Logs,
health, and cutover records contain aggregate counts/schema versions, never
payloads, keys, refs, content, credentials, or provider coordinates.

## Exact pin and voice deletions

Pinned boards delete `accounts.py`, pin configuration/environment options,
account caches, pin state/counters, pin renderers, sync/call sites, CLI/docs,
`test_accounts.py`, `test_model_in_pins.py`, and embedded pin cases.

Voice/STT/TTS delete `speech.py`, speech/voice configuration, parsers, commands,
gateway attachment/download/transcription, reply-by-voice logic, delivery
batching, state/models/temp directories, CLI/docs, `test_speech.py`,
`test_speak_back.py`, and embedded voice cases. Grep/AST gates reject any
production references or relocated equivalents.

## Precise production file scope

The final paired Herdres stack has this exact production disposition:

```text
edit    herdres.py
retain  herdres_gateway.py                 small H8 compatibility wrapper
edit    herdres_connector/__init__.py
delete  herdres_connector/accounts.py
edit    herdres_connector/config.py
edit    herdres_connector/decisions.py
edit    herdres_connector/doctor.py
retain  herdres_connector/ingress_identity.py
delete  herdres_connector/ingress_lanes.py
delete  herdres_connector/ingress_requests.py
add     herdres_connector/ingress_queue.py
edit    herdres_connector/ingress.py
edit    herdres_connector/managed_bots.py
edit    herdres_connector/rendering.py
edit    herdres_connector/rich_delivery.py
edit    herdres_connector/safe.py
delete  herdres_connector/source_sync.py
add     herdres_connector/presenter.py
add     herdres_connector/presentation.py
add     herdres_connector/topics.py
delete  herdres_connector/speech.py
replace herdres_connector/state.py
edit    herdres_connector/telegram_delivery.py
edit    herdres_connector/tendwire_client.py
edit    README.md
edit    RELEASE.md
edit    SECURITY.md
edit    docs/connector-rpc-contract.md
edit    docs/remote-decisions.md
edit    docs/wave4-ingress-dependency-design.md
edit    docs/wave4-presenter-state-design.md
edit/delete only the tests enumerated below
```

No production compatibility implementation for the deleted root state,
source-sync loop, ingress lanes, pins, or voice may remain.

## Whole-production SLOC budget

The measured baseline at `ec0e36a` is 24,685 canonical production SLOC. The
complete after-state target is about 8,920 and must stay below 11,000. The H8
module rows use targets inside the exact ranges in the H8 design; its small
`herdres.py` integration is already included in the full `herdres.py` row:

| Module | Responsibility | Target SLOC |
| --- | --- | ---: |
| `herdres.py` | orchestration and small CLI | 260 |
| `herdres_gateway.py` | small H8 wrapper | 90 |
| `__init__.py`, `accounts.py` | empty/deleted | 0 |
| `config.py` | supported configuration | 220 |
| `decisions.py` | callback validation and H8 enqueue | 480 |
| `doctor.py` | bounded supported health | 80 |
| `ingress_identity.py` | frozen identities | 90 |
| `ingress_lanes.py`, `ingress_requests.py` | deleted | 0 |
| `ingress_queue.py` | durable H8 queue | 400 |
| `ingress.py` | claim/resolve/submit loop | 750 |
| `managed_bots.py` | minimal bot ownership | 90 |
| `rendering.py` | supported message rendering | 380 |
| `rich_delivery.py` | one-request materialization | 520 |
| `safe.py` | privacy validators | 180 |
| `source_sync.py` | deleted | 0 |
| `presenter.py` | one poll/prepare/mutate/settle loop | 1,450 |
| `presentation.py` | five-kind validation/render/lineage | 850 |
| `topics.py` | bounded topic lifecycle | 400 |
| `speech.py` | deleted | 0 |
| `state.py` | lifecycle, provider facts, and shared guard | 1,500 |
| `telegram_delivery.py` | bounded provider operations | 550 |
| `tendwire_client.py` | strict generic RPC transport | 630 |
| **Total** |  | **8,920** |

No function exceeds 120 lines and most target fewer than 60. A row over target
by more than 15 percent or total over 11,000 is a design send-back. Moving
deleted logic into helpers does not count as reduction.

## Required tests and static gates

Every baseline Herdres test file has an exact disposition:

| Baseline test file | Final disposition |
| --- | --- |
| `conftest.py` | retain; edit only bounded shared fixtures, never protocol or production logic |
| `test_accounts.py` | delete with pinned boards |
| `test_collapse_previous.py` | rewrite for immutable target keys, flattened one-hop aliases, and reuse-safe retire |
| `test_command_ingress_idempotency.py` | rewrite/retain H8 HMAC request identity, key security, and replay vectors |
| `test_gateway_cleanup.py` | delete old gateway cleanup; move known-ID topic cases to `test_topics.py` |
| `test_ingress_lanes.py` | delete with lanes; surviving FIFO/crash cases move to `test_ingress.py` |
| `test_ingress_requests.py` | delete with JSON request workers; surviving receipt/quarantine cases move to `test_ingress.py` |
| `test_lossless_turn_rendering.py` | rewrite for contract-v3 deterministic exact spans and no stored content |
| `test_model_in_pins.py` | delete with pinned boards |
| `test_offlock_delivery.py` | rewrite for shared guard, lock order, route revalidation, and uncertainty classes |
| `test_outbound_latency.py` | rewrite for one poll/presenter and bounded lease/guard budgets |
| `test_pane_topic_binding_integrity.py` | rewrite for typed lifecycle, current slots, aliases, and immutable provider facts |
| `test_pending_inputs.py` | delete pending-list/bare-number local presentation path |
| `test_release_readiness.py` | rewrite for exact scope, static gates, 8,920 SLOC target, security, and paired cutover |
| `test_remote_decisions.py` | rewrite for decision controls, composite phases, shared guard, and resolution retire |
| `test_restart_rekey_continuity.py` | delete old rekey machinery; route continuity moves to state/presenter tests |
| `test_rich_delivery.py` | rewrite for deterministic one-request materialization and no fallback mutation |
| `test_source_only.py` | rewrite surviving connector receipt/crash cases; delete local-source/source-less cases |
| `test_source_status_placeholders.py` | delete local source/status placeholder presentation |
| `test_speak_back.py` | delete with voice/TTS |
| `test_speech.py` | delete with voice/STT/TTS |
| `test_stable_generation_delivery.py` | rewrite for exact route token, enriched fingerprint, and stale-route fence |
| `test_stable_worker_key.py` | retain and extend stable identity, route reuse/rotation, and collision cases |
| `test_table_rendering.py` | retain supported renderer; remove pin/voice coupling if present |
| `test_telegram_backpressure.py` | rewrite for one guarded mutation, exact 429 defer, and no tight loop |
| `test_tendwire_client.py` | rewrite for five kinds, exact prepare/content responses, renew/release, and socket validation |
| `test_tendwire_socket_pairing.py` | rewrite for real paired five-kind/ingress/receipt/restart integration |
| `test_topic_lifecycle_cleanup.py` | rewrite as bounded known-ID cases in `test_topics.py` |
| `test_topic_names.py` | retain/rewrite for minimal supported topic naming only |
| `test_turn_delta_sync.py` | delete; final presentation has no turn-delta source |
| `test_turn_final_delivery.py` | replace with source-bound root, provider kinds, recovery, retry, and retire integration |
| `test_worker_topic_dedup.py` | rewrite for physical-owner serialization across route generations |

The final stack adds exactly `test_ingress.py`, `test_state.py`,
`test_presenter.py`, `test_presentation.py`, and `test_topics.py`. No unlisted
test file may be added, deleted, or used as a compatibility dump. Rewritten
files may share fixtures through `tests/conftest.py`; production logic and
copied protocol validators are forbidden there.

H8 must be green independently before H7/H6 begins:

- durable queue claim/receipt/restart, both legacy durability sources drained
  or explicitly discarded, and no presentation-receipt shortcut joins;
- all frozen ingress operations and exact dataclasses, `PhysicalOwner`, shared
  provider guard, ephemeral receiver secrets, opaque state token, pre-cutover
  null route token, stale-token revalidation, and post-cutover exact route token;
- decision callback through the same queue, bounded receipt/control fields,
  ingress progress during blocked provider presentation, and at-most-once
  best-effort operator notification semantics;
- owner/mode/symlink/hard-link/sidecar/corruption/JSON-bound tests and no generic
  H7 state imports.

H7 tests cover fresh schema refusal, every bound, filesystem hardening, frozen
typed exports, immutable outer-key conflict, route-generation capture,
ownership/alias/tombstone/current-slot invariants, atomic new-owner plus alias
plus slot update, one-hop alias flattening/cycle refusal, exact decision control
phases, 60-day retention against 30/45-day T6 horizons, terminal pruning,
cross-process guard serialization, topic-create crash points, and absence of
every forbidden local ledger.

H6 tests cover exact generic RPC names/envelopes, all five versions and field
sets, key/route/partition correlation, recursive privacy scans, strict socket
failures, final-ready zero-provider source-bound prepare, source-less refusal,
lost begin/part/commit, lease expiry and rediscovery without local plan state,
binding-before-route for all provider kinds, ACK loss/restart dedup, route drift,
enriched-fingerprint/no-strip fencing, definitely-not-started versus ambiguous
provider outcomes, immediate ambiguous-send dead letter, one-mutation budgets,
shared-owner serialization, working-to-final replacement,
multipart prefix/suffix recovery, empty-prefix eligibility, immutable target-key
retire, protected alias reuse, decision delivery/ingress/resolution, 429 defer,
topic ambiguity, and the honest send-before-fsync duplicate window.

Static gates prove `source_sync`, generic state roots, plan/recovery/completion
ledgers, turn-list/delta presentation, pins, voice, old ingress lanes/workers,
raw provider search, source-less prepare, a standalone route-retirement payload,
post-cutover
fingerprint stripping, private presentation-only guard namespaces, and invented
connector methods are absent. Integration uses
a real Tendwire socket and recording Telegram stub to exercise all five kinds,
prepare response loss, lost provider ACK, recovery suffix, alias-safe retire,
decision markup racing presentation/retire under the shared guard, decision
answer/resolution, source completion, and two logical partitions that share one
physical provider owner.

## Cutover and rollback

H8-only cutover stops every old ingress writer, quiesces and inventories both
the old SQLite lanes and `state.json["tendwire_ingress_command_requests"]`, then
drains or explicitly discards both. Discard acknowledges that commands arriving
during downtime are not replayed when the fresh cursor begins at the newest
boundary. Only after checkpointing are old DB/WAL/SHM and state snapshots
archived together with the active H8 request-ID/HMAC key. Start one H8 writer
and verify one real request/receipt. Rollback stops it before restoring the
matched old ingress/state/key snapshot; old and new writers never overlap.

The paired T6+H7/H6 cutover stops all Tendwire writers, the old Herdres
presenter/source observer, lifecycle writer, H8 gateway/submitter, and Telegram
mutators. It drains or explicitly quarantines old leases, awaiting roots,
prepare plans, delivery work, ambiguous known provider operations, and every H8
row lacking a durable route generation. It then archives as one matched set:

```text
Tendwire database including WAL/SHM
Tendwire installation key, marker, and sentinel family
old Herdres state and provider bindings
H8 ingress database including WAL/SHM
active H8 request-ID/HMAC key
release/config identifiers and aggregate active counts
```

Fresh H7 schema is created securely. T6 reconciles and durably publishes route
generations and enriched worker fingerprints. While all external writers remain
stopped, H7 consumes that exact lifecycle snapshot, reconciles workers/spaces/
topics, and proves every sendable stable owner has the same route generation and
fingerprint. This verified H7 reconciliation barrier must complete before H8 or
the presenter is resumed. Operators then verify exact peer/contract versions,
route-token correlation, topic ownership, one ingress receipt, and one complete
five-kind demo lineage before enabling normal cadence.

Rollback is automatically safe only before the first new Telegram mutation.
After any new send/edit/delete/topic action, restoring old snapshots can lose
new key-to-provider facts and create duplicates or orphans. Rollback then
requires all known new provider facts to be drained/retired and reconciled
under the new stack before restoring the matched old set; if that proof is
impossible, operators keep the new state and perform a forward fix while
recording explicit orphan/duplicate risk. Unknown pre-fsync sends and ambiguous
topic creates remain unknowable and are never described as repaired by snapshot
restoration.

No implementation, merge, deployment, service restart, or Kimi review is
authorized by this design.
