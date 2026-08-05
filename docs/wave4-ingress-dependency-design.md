# Wave 4 H8-first ingress dependency design

Status: **corrected design candidate. No production implementation may begin
until this note passes ordinary and adversarial review.**

This note is based on Herdres `ec0e36a` and the current pre-T6 Tendwire baseline
`9ab5597b55bb918ac8e62c86651ccac7c03d18fb`. It preserves Herdres's
reply-author routing fix and does not assume any T6 production field exists at
the independent H8 merge point. It authorizes no code, commit, deployment,
service restart, migration, or cleanup.

## Outcome and dependency proof

H8 can be implemented, tested, merged, and deployed against the current
pre-T6 Tendwire daemon while the old presenter remains, but only with the
dependency cuts in this note. In particular, H8 must remove the old
presenter's joins to the JSON ingress ledger; deleting
`ingress_requests.py` without those edits would leave live imports and a hidden
receipt-derived working-card path.

H8 does not make H7 independently mergeable. After H8, generic JSON users
remain only in old presenter/lifecycle code. H7 and H6 therefore remain one
private stack and one integration/deployment unit. The integration branch gets
one H8 ingress runtime first; it later gets H7+H6 together. It never receives
dual ingress or dual presenter runtimes.

The later T6 contract changes presentation, not command ingress. H8 continues
to use `command.submit` across both contracts. Route-generation information is
optional before T6 and required only after the paired T6+H7/H6 cutover.

### Honest compatibility table

| Concern | H8 merged against current Tendwire and old presenter | Paired T6 + H7/H6 after-state |
|---|---|---|
| Command mutation | Direct `command.submit` through `TendwireClient.command_json()` | Same exact socket method and command schemas |
| Ingress durability | Fresh `inbound_spool.db` schema 1; sole ingress ledger | Same H8 queue and lifecycle |
| Route fence | Stable key/version plus current worker/fingerprint; `route_generation` may be null | Valid `twroute1.*` generation required for newly resolved routes |
| Working cards | Old presenter's ordinary live turn observation temporarily remains | Only T6 `turn-final` `working` v1 rows; no local observation path |
| Receipt-derived working | Deleted in H8 | Absent |
| Finals | Old presenter's current `turn-final` contract | T6 `final_ready` v3 and `final_part` v2 |
| Decision cards | Old presenter reconciles cards; H8 owns callback ingress | T6 `decision` v1 presents cards; H8 still owns callback/answer ingress |
| Retire work | Old presenter behavior until paired cutover | T6 `retire` v1 only |
| State access from ingress | Temporary frozen typed adapter over schema-2 state | Same typed signatures implemented by H7 schema 1 |
| Deployability | H8 may deploy independently after its explicit cutover | T6 may deploy only together with H7/H6; never with old presenter |

## One runtime and exact module ownership

`herdres-gateway.service` is the only ingress process. It performs Telegram
polling, durable acceptance, queue dispatch, direct daemon submission, decision
local actions, and bounded terminal notices. `herdres.service` remains the old
presenter/lifecycle process until H7/H6 but never reads or writes the ingress
queue.

### Add

`herdres_connector/ingress_queue.py` is the only SQLite owner. Its public
surface is:

```text
IngressQueue.open_writer(path) -> IngressQueue
cursor(receiver_id) -> int | None
initialize_cursor(receiver_id, next_update_id) -> int
accept_update(acceptance) -> AcceptResult
claim(lease_owner, now, lease_seconds) -> QueueItem | None
renew(seq, lease_owner, now, lease_seconds) -> bool
store_command(seq, lease_owner, checkpoint) -> StoreResult
store_local_action(seq, lease_owner, checkpoint) -> StoreResult
advance_local_phase(seq, lease_owner, transition) -> StoreResult
settle_receipt(seq, lease_owner, settlement) -> SettleResult
schedule_retry(seq, lease_owner, retry) -> SettleResult
quarantine(seq, lease_owner, quarantine) -> SettleResult
claim_notice(seq, now) -> NoticeClaim | None
mark_notice_sent(seq, notice_claim_id, message_id, now) -> bool
prune(now) -> int
IngressQueue.observe(path) -> ContextManager[IngressQueueObserver]
IngressQueueObserver.health_snapshot(now) -> QueueHealth
IngressQueueObserver.status_rows(now) -> tuple[QueueStatus, ...]
```

`herdres_connector/ingress.py` owns all non-SQLite ingress orchestration:

```text
preview_update(update, policy, receiver) -> Preview
canonical_input(preview) -> str
ordering_key(preview) -> str
build_send_instruction(item, route) -> CanonicalCommand
build_answer_decision(item, decision) -> CanonicalCommand
dispatch_one(queue, item, ports) -> DispatchResult
dispatch_decision(queue, item, ports) -> DispatchResult
reduce_daemon_receipt(command, response) -> ReceiptReduction
apply_local_decision(queue, item, action, ports) -> DispatchResult
send_terminal_notice(queue, claim, telegram) -> NoticeResult
poll_receiver_once(receiver, queue, ports) -> PollResult
run_gateway(ports) -> int
```

No function exceeds 120 lines; most target fewer than 60.

### Replace

`herdres_gateway.py` remains the installed 70--100-line executable wrapper for
`herdres-gateway.service`. It loads private receiver configuration, opens one
queue, constructs ports, calls `run_gateway()`, and performs bounded shutdown.
It contains no business logic, SQL, subprocess, or state-root access.

### Delete

- `herdres_connector/ingress_lanes.py`;
- `herdres_connector/ingress_requests.py`;
- `herdres_gateway.run_herdres_command()`;
- `_private_retry_child_result()`;
- `_validated_child_response()`;
- `_checkpoint_for_command_result()`;
- `_preflight_ingress_request()`;
- `handle_message()` and `handle_update()`;
- `_InboundLaneDispatcher` and its notification/child workers;
- child schema/checkpoint/stdin/stdout constants;
- `herdres.command_reply()`;
- `_submit_ingress_command_record()`;
- `_local_ingress_outcome()`;
- `callback_reply()`;
- `cmd_command()` and `cmd_callback()`; and
- the command/callback CLI parser entries.

### Required H8 edits to the old presenter

H8 edits `herdres_connector/source_sync.py` only to sever the JSON ingress
ledger and receipt-derived presentation. It removes:

- the `ingress_requests` import;
- `_complete_submission_receipt()`;
- `_submission_owner_entry()`;
- `_associate_submission_working()`;
- `_submission_instruction()`;
- `_apply_submission_links()`;
- `_sync_submission_working_cards()`;
- `_deliver_submission_working_record()`;
- `deliver_submission_working_card()`;
- `ingress_requests.RECORDS_KEY` handling in
  `_clear_projection_stale_cards()`; and
- every caller, counter, checkpoint, and test that exists only for those
  functions.

H8 also removes `deliver_submission_working_card` from `herdres.py` imports and
the accepted-command shortcut that calls it. It does not replace this with a
queue reader or another local working-card path. The old presenter's ordinary
live turn-observation working behavior remains temporarily so H8 can merge
before T6. H7/H6 deletes that last observational path and consumes only T6
`working` rows.

### Other narrow edits

- `state.py`: implement the temporary typed ingress protocol and shared
  physical-provider mutation guard below.
- `decisions.py`: expose typed decision snapshots/mutations and rendering for
  H8 local decision actions, and put every retained old-presenter decision
  markup/delete mutation under the shared physical-provider guard; do not
  retain a second submission state machine.
- `doctor.py`: enter `IngressQueue.observe(...)` and call the observer's
  `health_snapshot()` for live inbound health; stop reading JSON ingress records.
- `config.py`: retain the existing inbound queue path and fixed bounds, remove
  the command response-version knob because H8 pins v3 send, and add no second
  queue/path or new runtime knob.
- `managed_bots.py`: retain only canonical manager/child kind normalization and
  username-to-kind mapping; replace ingress direct-root/token lookup with typed
  receiver/policy inputs and add no I/O or persistence.
- `ingress_identity.py`: retain the current HMAC request identity and private
  key loader as a dedicated 80--100-line module.
- `tendwire_client.py`: retain direct AF_UNIX `command_json()` only.
- `telegram_delivery.py`: H8 uses bounded Telegram methods only, never its
  presenter-state helpers.
- `herdres.py`: retain only small ingress status integration in addition to its
  pre-H7/H6 presenter CLI.

## Frozen temporary state protocol shared with H7

The following five state operations, one shared physical-provider guard, and
their dataclasses are canonical. H8 and the H7/H6 design must use these exact
names and meanings:

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

Receiver credentials use one additional ephemeral operation because tokens
must never enter policy, route, queue, state result, or logs:

```text
read_ingress_receivers(state_path: Path) -> tuple[IngressReceiver, ...]
```

Before H7 this adapter may read schema-2 JSON internally. H8 callers never
receive a root dictionary or call `load_state`, `save_state`, `state_lock`, or
`released_lock`. H7 preserves these signatures while replacing their internals
with its fresh schema.

### Exact common types

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

`state_token` is deliberately opaque. The schema-2 adapter computes it under
the state flock from a canonical state digest plus pinned file identity; it
does not pretend the old file has a revision counter. H7 encodes its real
monotonic revision into the same opaque type. Callers compare tokens only for
equality.

`SecretStr` is a small local slotted wrapper in `state.py`, not a new dependency.
It accepts one nonempty bounded token string, returns redacted text from
`repr`/`str`, rejects pickling and JSON/dataclass serialization, and exposes the
raw value only through an explicit `reveal_for_telegram_client()` call in the
gateway port-construction path. The revealed string is never placed in another
dataclass, queue row, exception, status, or log and is discarded with the
receiver client.

### Exact decision types

```text
DecisionIngressQuery:
    chat_id: str
    topic_id: str
    callback_ref: str
    state_token: StateToken

DecisionStatus enum:
    ACTIVE, EXPIRED, MISSING, AMBIGUOUS, QUARANTINED, STALE

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

Every mutator acquires its own state flock, reloads, validates the opaque token
and semantic decision identity, applies at most one idempotent mutation keyed by
the exact composite `(request_id, DecisionMutationKind)`,
persists, and returns a new token. No Telegram or Tendwire call occurs under
the state flock. Schema-2 compatibility may internally read `telegram`,
`panes`, `spaces`, `telegram_message_bindings`, and `decisions`; it never reads
or writes `tendwire_ingress_command_requests`.

`read_ingress_receivers()` may temporarily read legacy state-configured managed
bot tokens so H8 preserves current operation. It returns secrets only in
memory. H7 stores no credentials, so the paired cutover requires all receiver
tokens/usernames in environment configuration.

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

## Exact routing contract from `ec0e36a`

Preview selects the responsible receiver in this order:

1. Telegram `reply_to_message.from.username` mapped to managed bot kind;
2. exact provider message binding `bot_kind`; and
3. explicit managed-bot mention.

The manager receiver defers child-owned input to the matching child. A child
receiver defers input owned by another kind. The observed result is persisted
in canonical input before the cursor advances.

Dispatch selects the worker in this order:

1. explicit `@alias` in command text;
2. immutable reply-message binding;
3. observed reply-author bot-kind correction;
4. explicit managed-bot kind; and
5. topic route only when exactly one live worker is eligible.

If a binding exists but cannot identify exactly one current owner, the row is
quarantined as `ambiguous_reply_target`. If author kind disagrees with a
corrupt binding, resolution may select exactly one worker of the author kind
in the same space; zero or multiple candidates quarantine as
`ambiguous_reply_author_target`. `binding_was_present` prevents binding failure
from silently falling through to topic routing. Stable owner and current route
evidence are checkpointed before the socket call and never recomputed for a
stored command.

## Exact command contracts

H8 has no response-version flag or dual command builder.

### Send instruction

Canonical request has exactly:

```json
{
  "schema_version": 1,
  "action": "send_instruction",
  "request_id": "<queue request_id>",
  "dry_run": false,
  "target": {"worker_id": "...", "worker_fingerprint": "..."},
  "instruction": {"text": "..."},
  "response_schema_version": 3
}
```

`target` is exactly worker ID, worker ID plus fingerprint, or space ID. Empty
instruction text is rejected. The embedded request ID must equal the queue row
request ID. A stored command is canonical UTF-8 JSON and immutable except the
one fenced fingerprint removal described below.

Successful response is exact command schema 3; H8 rejects the schema-2 success
that the baseline compatibility validator can still accept. The response has
the existing exact shell fields:

```text
schema_version, action, request_id, ok, dry_run, status,
disposition, result, error, warnings
```

It requires `ok=true`, `action=send_instruction`, correlated request ID,
`dry_run=false`, `status=accepted`, `disposition=terminal_accepted`, null error,
and result with:

```text
target, delivery_state, transport_state, target_state_at_send,
observed_turn_state, optional submission_id/submission_verdict/turn_id
```

The result has no other keys. `target` is exactly a nonempty `worker_id`; it
equals the requested worker when the request selected a worker, while a space
request may resolve to any nonempty worker ID. Delivery and transport are both
exactly `submitted`; target state is nonempty; observed turn state is exactly
`pending_observation`, `observed`, `complete`, or `linked`. If present,
`submission_id` is nonempty, `submission_verdict` is `submitted` or
`written_to_pty`, and `turn_id` is null or nonempty. Warnings are a list of
public strings.

For `ok=false`, the live compatibility validator permits response schema 2 or
3; H8 preserves that failure-envelope compatibility because the negotiated
baseline daemon normally leaves non-accepted responses at schema 2. The exact
send matrix is:

| Disposition | Permitted status |
|---|---|
| `in_progress` | `pending` |
| `terminal_uncertain` | `request_state_uncertain` |
| `terminal_rejected` | `rejected`, `stale_target`, `backend_unavailable`, `backend_unsupported`, `ambiguous_backend_target`, `backend_failed`, `duplicate_request` |
| `no_receipt` | every `terminal_rejected` status above, plus `invalid_request`, `not_found`, `ambiguous_target` |

Every failure has null result and a public-pruned error object with a nonempty
message; an error code is absent/null or equals status. No other
status/disposition pair is accepted. A client transport result marked definitely
not-started or started-ambiguous is not a daemon response and is reduced by its
separate origin marker.

### Answer decision

Canonical request has exactly:

```json
{
  "schema_version": 1,
  "action": "answer_decision",
  "request_id": "<queue request_id>",
  "dry_run": false,
  "target": {"worker_id": "..."},
  "params": {
    "decision_ref": "...",
    "selection": {"option_refs": ["..."]}
  }
}
```

`selection` is exactly one of nonempty text or unique option refs. Successful
response is exact command schema 2 and the exact common shell above with
`ok=true`, `status=accepted`, `disposition=terminal_accepted`, and null error.
Its result has exactly `target`, `decision`, `delivery_state`,
`transport_state`, and `observed_pending_state`: target equals the requested
worker object; decision is exactly the requested decision ref; delivery and
transport are both `submitted`; observed pending state is
`pending_observation`.

The exact `ok=false` decision matrix is:

| Status | Permitted disposition |
|---|---|
| `answer_in_progress` | `no_receipt`, `in_progress` |
| `decision_not_pending` | `no_receipt`, `terminal_rejected` |
| `invalid_selection` | `no_receipt`, `terminal_rejected` |
| `unsupported_decision` | `no_receipt`, `terminal_rejected` |
| `unknown_worker` | `no_receipt`, `terminal_rejected` |

Failure result/error rules are the same as send. No daemon decision failure
uses `terminal_uncertain`; only the separate client transport ambiguity marker
represents a lost result after request start.

`TendwireClient.command_json()` remains the sole validator and AF_UNIX caller.
No subprocess, CLI, database watcher, raw Tendwire SQLite access, or fallback
is permitted.

## Sole durable queue and exact DDL

H8 reuses `HERDRES_INBOUND_SPOOL_PATH` and `inbound_spool.db`. The schema is
fresh version 1 and refuses every other `user_version`; there is no runtime
migration or compatibility table.

```sql
CREATE TABLE receiver_cursors (
    receiver_id TEXT PRIMARY KEY,
    next_update_id INTEGER NOT NULL CHECK(next_update_id >= 0),
    updated_at REAL NOT NULL
);

CREATE TABLE requests (
    seq INTEGER PRIMARY KEY AUTOINCREMENT,
    request_id TEXT NOT NULL UNIQUE,
    receiver_id TEXT NOT NULL,
    update_id INTEGER NOT NULL CHECK(update_id >= 0),
    ordering_key TEXT NOT NULL,
    kind TEXT NOT NULL CHECK(kind IN ('message','decision')),
    input_json TEXT NOT NULL,
    command_json TEXT,
    command_digest TEXT,
    local_action_json TEXT,
    local_action_digest TEXT,
    local_phase TEXT CHECK(local_phase IN
        ('checkpointed','state_applied','provider_ready',
         'provider_applied','markup_recorded') OR local_phase IS NULL),
    local_expected_state_token TEXT,
    local_applied_state_token TEXT,
    local_provider_state_token TEXT,
    local_markup_state_token TEXT,
    local_provider_outcome TEXT CHECK(local_provider_outcome IN
        ('accepted','not_modified') OR local_provider_outcome IS NULL),
    local_provider_at REAL,
    target_stable_key TEXT,
    target_stable_key_version INTEGER,
    target_route_generation TEXT,
    target_worker_id TEXT,
    target_space_id TEXT,
    target_bot_kind TEXT,
    route_refresh_count INTEGER NOT NULL DEFAULT 0
        CHECK(route_refresh_count IN (0,1)),
    state TEXT NOT NULL CHECK(state IN
        ('pending','processing','retry','terminal','quarantine')),
    attempts INTEGER NOT NULL DEFAULT 0 CHECK(attempts >= 0),
    first_seen_at REAL NOT NULL,
    deadline_at REAL NOT NULL,
    retain_until REAL NOT NULL,
    next_attempt_at REAL NOT NULL,
    lease_owner TEXT,
    lease_until REAL,
    disposition TEXT,
    receipt_kind TEXT CHECK(receipt_kind IN ('daemon','local') OR receipt_kind IS NULL),
    receipt_json TEXT,
    terminal_reply TEXT,
    quarantine_reason TEXT,
    notify_state TEXT NOT NULL CHECK(notify_state IN
        ('none','pending','claimed','sent')),
    notice_claim_id TEXT,
    notice_claimed_at REAL,
    notice_message_id TEXT,
    created_at REAL NOT NULL,
    updated_at REAL NOT NULL,
    UNIQUE(receiver_id, update_id),
    CHECK(first_seen_at < deadline_at AND deadline_at < retain_until),
    CHECK(
      (state = 'processing' AND lease_owner IS NOT NULL AND lease_until IS NOT NULL)
      OR
      (state != 'processing' AND lease_owner IS NULL AND lease_until IS NULL)
    ),
    CHECK((command_json IS NULL) = (command_digest IS NULL)),
    CHECK((local_action_json IS NULL) = (local_action_digest IS NULL)),
    CHECK(command_json IS NULL OR local_action_json IS NULL),
    CHECK(
      (local_action_json IS NULL
       AND local_phase IS NULL
       AND local_expected_state_token IS NULL
       AND local_applied_state_token IS NULL
       AND local_provider_state_token IS NULL
       AND local_markup_state_token IS NULL
       AND local_provider_outcome IS NULL
       AND local_provider_at IS NULL)
      OR
      (local_action_json IS NOT NULL
       AND local_phase IS NOT NULL
       AND local_expected_state_token IS NOT NULL)
    ),
    CHECK(
      local_phase IS NULL OR local_phase != 'checkpointed'
      OR (local_applied_state_token IS NULL
          AND local_provider_state_token IS NULL
          AND local_markup_state_token IS NULL
          AND local_provider_outcome IS NULL
          AND local_provider_at IS NULL)
    ),
    CHECK(
      local_phase IS NULL OR local_phase NOT IN
        ('state_applied','provider_ready','provider_applied','markup_recorded')
      OR local_applied_state_token IS NOT NULL
    ),
    CHECK(
      local_phase IS NULL OR local_phase NOT IN
        ('provider_ready','provider_applied','markup_recorded')
      OR local_provider_state_token IS NOT NULL
    ),
    CHECK(
      local_phase IS NULL OR local_phase NOT IN
        ('provider_applied','markup_recorded')
      OR (local_provider_outcome IS NOT NULL AND local_provider_at IS NOT NULL)
    ),
    CHECK(
      local_phase IS NULL OR local_phase != 'markup_recorded'
      OR local_markup_state_token IS NOT NULL
    ),
    CHECK(
      target_stable_key_version IS NULL
      OR target_stable_key_version = 1
    ),
    CHECK(
      (receipt_kind IS NULL AND receipt_json IS NULL)
      OR
      (receipt_kind IS NOT NULL AND receipt_json IS NOT NULL)
    ),
    CHECK(
      notify_state IN ('none','pending')
      OR (notice_claim_id IS NOT NULL AND notice_claimed_at IS NOT NULL)
    ),
    CHECK(
      notify_state != 'sent' OR notice_message_id IS NOT NULL
    )
);

CREATE INDEX requests_open_head
    ON requests(ordering_key, seq)
    WHERE state IN ('pending','processing','retry');
CREATE INDEX requests_retry
    ON requests(next_attempt_at, seq)
    WHERE state IN ('pending','retry');
CREATE INDEX requests_lease
    ON requests(lease_until)
    WHERE state = 'processing';
CREATE INDEX requests_retention ON requests(retain_until);
```

Application validation additionally enforces exact state-specific field sets,
canonical JSON, request correlation, opaque-token grammar, timestamp finiteness,
and fixed bounds: 64 KiB input, 64 KiB command/local action, 64 KiB receipt,
160 characters terminal reply, and 240 characters reason. Terminal daemon rows
require a validated terminal receipt; terminal local rows require an exact
local receipt. Quarantine may occur before route/command and therefore may have
no receipt. Pending/retry rows cannot carry terminal receipt or notice state.
Every enum is parsed from an explicit closed value set, every integer rejects
Boolean values, every JSON read revalidates the exact schema and canonical byte
encoding, and no row is trusted merely because it was written by an earlier
process in the same release.

`local_expected_state_token`, `local_applied_state_token`,
`local_provider_state_token`, and `local_markup_state_token` are opaque strings
of 1--128 UTF-8 bytes. They are never logged or copied into a daemon request.
Application validation additionally requires that arm actions use only
`checkpointed -> state_applied -> terminal`, while toggle actions use only
`checkpointed -> state_applied -> provider_ready -> provider_applied ->
markup_recorded -> terminal`; no phase or populated token column may move
backward or be cleared.

`attempts` counts claims: it increments atomically on every successful claim.
It never increments on a denied claim or lease renewal.

Canonical `input_json` contains only bounded Telegram coordinates, sender,
text/caption, reply coordinates/author kind, mention kind, callback ID/data,
and receiver identity. It never contains tokens, raw media, attachment URL,
full update, raw state, socket/path, or provider body. Voice/audio/media-only
updates are rejected before acceptance.

## Complete lifecycle and CAS matrix

Every queue mutation is one `BEGIN IMMEDIATE` transaction. No Telegram, state,
or Tendwire call occurs inside it.

### Cursor and acceptance

`accept_update()` checks both request ID and `(receiver_id, update_id)`. Exact
duplicates return the existing row. Any cross-match mismatch is a fatal
identity collision. For an authorized supported update, insert and cursor
advance to at least `update_id + 1` commit atomically. Authorized queue overflow
inserts a visible quarantine row before advancing. Unauthorized, wrong-chat,
wrong-receiver, unsupported, or explicitly historical input may advance the
cursor without a row. A failed commit retains the prior cursor.

Message and decision updates in one chat/topic share one ordering key so a
custom-arm callback precedes following free-form text. Owner commands use a
receiver-scoped owner-command key. General topic and ordinary topics have
stable collision-free keys. Different keys may progress concurrently.

### Claim and renewal

Before selection, expired processing rows become retry if before deadline or
quarantine at/after deadline. Claimable means pending/retry, due, before
deadline, and no lower-sequence open row for the same ordering key.

Claim CAS:

```text
seq + old state + due + no earlier open row
  -> processing + attempts+1 + lease owner + lease deadline
```

Renew CAS requires the same seq, `processing`, exact lease owner, and unexpired
current lease. It only extends the deadline. A stale or expired renewal changes
nothing.

### Route and operation checkpoint

Before any external mutation, one of these is stored:

- exact command JSON/digest plus captured target fields; or
- exact local-decision action JSON/digest plus captured decision/message
  identity.

The CAS requires processing plus lease owner and null operation fields. A
replay with existing fields succeeds only on exact digest and coordinate
equality; conflict quarantines. All later attempts reuse stored operation bytes
and captured owner.

Canonical local-action schema 1 has exactly one of these shapes. Tokens are
columns, not part of the immutable action JSON, so a semantically identical
snapshot can refresh an opaque stale token without changing action identity:

```text
ARM_FREEFORM:
  schema_version, action="ARM_FREEFORM", request_id, callback_ref,
  decision_ref, revision_digest

TOGGLE_OPTION:
  schema_version, action="TOGGLE_OPTION", request_id, callback_ref,
  decision_ref, revision_digest, option_ref, desired_selected_refs,
  desired_markup_fingerprint, physical_owner, message_binding_id,
  message_id, reply_markup
```

`physical_owner` has exactly the three `PhysicalOwner` fields. Reply markup is
the exact bounded canonical Telegram JSON object to replay, not a renderer
instruction; selected refs are unique and in decision option order. The action
request ID equals the queue request ID, the captured decision/message/owner
equals the typed read that supplied `local_expected_state_token`, and the
action/digest never changes. `RECORD_LOCAL_MARKUP` is a later typed state phase,
not another queue action. Its idempotency identity is
`(request_id, RECORD_LOCAL_MARKUP)` and is therefore distinct from
`(request_id, TOGGLE_OPTION)`.

Pre-T6 route generation may be null. A non-null value must match
`twroute1.<43 base64url>`. The only command rewrite is one removal of
`worker_fingerprint`, allowed only for a pre-T6 checkpoint whose stored
`target_route_generation IS NULL`, when stable owner/version and worker ID
remain identical and `route_refresh_count=0`. Once a row has any non-null route
generation, fingerprint removal is forbidden; stale fingerprint/route evidence
quarantines rather than weakening the post-T6 fence. Anything else quarantines
rather than retargeting.

### Daemon convergence

The socket call happens outside the transaction. Settlement CAS requires seq,
processing, exact lease owner, exact command digest, and still-current stored
coordinates.

- terminal accepted -> terminal daemon receipt;
- terminal rejected -> terminal daemon receipt;
- definitely not started, `no_receipt`, or `in_progress` before deadline ->
  retry with bounded exponential backoff;
- started transport ambiguity or `terminal_uncertain` -> quarantine;
- malformed response/correlation -> quarantine;
- deadline equality or expiry -> quarantine.

Retry clears lease, increments no additional attempt, and bounds
`next_attempt_at <= deadline_at`. Terminal/quarantine clears the lease and is
absorbing. If the queue lease expires during the socket call, settlement loses
its CAS; the next claim reuses the identical Tendwire request ID/bytes and
converges through Tendwire command receipts.

### Local decision convergence

`advance_local_phase()` CASes seq, `processing`, exact lease owner, exact local
digest, expected old phase, and expected populated token/outcome fields. A lost
CAS performs no external action; the next claim validates and resumes the
durable phase. An opaque-token `STALE` result never immediately quarantines.
H8 re-reads policy and the decision under their short state flocks and advances
`local_expected_state_token` or `local_provider_state_token` only when decision
ref/revision, message binding/ID, physical owner, option set, and desired
semantics remain identical. Semantic drift, missing/expired decision, or a
non-stale conflict quarantines.

The exact phase/crash matrix is:

| Durable phase | ARM_FREEFORM next action | TOGGLE_OPTION next action | Crash/lost-result convergence |
|---|---|---|---|
| `checkpointed` | Call `ARM_FREEFORM` with `(request_id, ARM_FREEFORM)` and `local_expected_state_token` | Call desired-state `TOGGLE_OPTION` with `(request_id, TOGGLE_OPTION)` and `local_expected_state_token` | Crash before call repeats it; crash after state fsync returns `ALREADY_APPLIED`; `STALE` re-reads and CAS-refreshes the expected token only after full semantic equality |
| `state_applied` | Settle terminal local receipt using `local_applied_state_token`; no provider call | Acquire the exact physical guard, renew the queue lease, re-read/revalidate, then CAS `provider_ready` with the returned snapshot token | Lost state-phase CAS repeats the same composite mutation and converges as `ALREADY_APPLIED` |
| `provider_ready` | invalid for arm | Re-renew under guard, revalidate lease/digest and decision snapshot, then edit exactly the stored message with exactly stored `reply_markup` | Crash before call repeats; accepted result lost/transport ambiguity retries the same known-target idempotent edit until deadline; it does not quarantine merely for ambiguity |
| `provider_applied` | invalid for arm | Under the still-held/reacquired same guard, call `RECORD_LOCAL_MARKUP` with `(request_id, RECORD_LOCAL_MARKUP)` and the freshest semantically equal state token | Provider acceptance is already durable, so Telegram is skipped; crash after state fsync but before phase CAS repeats only `RECORD_LOCAL_MARKUP`, which returns `ALREADY_APPLIED` |
| `markup_recorded` | invalid for arm | Settle terminal local receipt using all three token checkpoints and provider outcome | Lost settlement repeats only the queue settlement CAS; neither state mutation nor Telegram is repeated |

`APPLIED` or `ALREADY_APPLIED` stores the returned token before advancing.
`ARM_FREEFORM` therefore uses `checkpointed -> state_applied -> terminal` and no
Telegram call. Toggle uses every phase in order. Telegram success and exact
`message is not modified` for the stored message/fingerprint CAS
`provider_applied` with outcome `accepted` or `not_modified` and timestamp.
Known message-not-found, semantic owner/message drift, malformed provider
response, or deadline expiry quarantines. A transport-ambiguous known-target
markup is retryable because replaying the identical edit is convergent; it is
never converted into a send-new or live-route-substitute operation.

For `RECORD_LOCAL_MARKUP`, a stale token after provider acceptance causes a
fresh read under the same physical guard. If the exact control still owns the
same message, the provider-applied desired refs/fingerprint are recorded using
the refreshed token; if it has been retired or rebound, H8 records a bounded
local `provider_applied_control_drift` quarantine without another provider
mutation. Single/plan selection, multi submit, and armed free-form build
`answer_decision` and use daemon convergence.

H7 decision presentation later stores one exact bounded `decision_controls`
record keyed by decision outer key. It contains exactly: decision ref, callback
ref hash, revision digest, mode, ordered public option refs, selected refs,
free-form arm Boolean, message binding ID, `PhysicalOwner`, message ID, current
owner outer key, render fingerprint, desired markup fingerprint, applied markup
fingerprint, route generation, terminal/tombstone state, timestamps, and
applied ingress entries `(request_id, DecisionMutationKind, mutation_digest)`.
The first two entry fields are the composite idempotency identity; the digest is
the canonical mutation-byte conflict fence. Prompt/body/labels, reply markup,
credentials, raw callback data, and private ACP option IDs are not persisted
there. H7 must retain enough entries to return `ALREADY_APPLIED` independently
for `TOGGLE_OPTION` and `RECORD_LOCAL_MARKUP` and to fail closed if one
composite identity is replayed with a different digest.

### Notice and callback ambiguity

Callback spinner acknowledgement is best effort immediately after durable
acceptance. It is not retried and is never treated as command settlement.
Exact duplicates use the cached row to choose a bounded queued/expired/final
toast when Telegram still accepts the callback ID.

Terminal notices are policy-exact:

- accepted success: none by default, except the existing busy-worker reply;
- terminal rejection: pending safe failure notice;
- quarantine: pending safe uncertainty/failure notice;
- local toggle/arm: no separate message;
- authorized overflow: pending throttled overflow notice.

Overflow throttling is durable and has no third table. In the same
`accept_update()` transaction that inserts an authorized overflow quarantine,
the new row gets `notify_state='pending'` only if this exact query finds no row:

```sql
SELECT 1 FROM requests
 WHERE ordering_key = :ordering_key
   AND disposition = 'queue_overflow'
   AND created_at > :now - 60.0
   AND notify_state IN ('pending','claimed','sent')
 LIMIT 1;
```

Otherwise the overflow row is still inserted and cursor-committed but gets
`notify_state='none'`. The key is the already canonical collision-free ordering
key, the window is exactly 60 seconds, and equality at exactly 60 seconds allows
a new notice, preserving the baseline throttle. `now` is the same validated
finite wall-clock timestamp used for the insertion. Retention cannot remove an
overflow row inside this window.

`claim_notice()` CASes pending to claimed, creates a random claim ID, and stores
claim time before Telegram. `mark_notice_sent()` requires exact claim ID and
stores the returned message ID. Provider failure or crash after claim leaves an
absorbing claimed ambiguity: it is never resent, remains visible in status and
health, and cannot change the command result. This is explicitly at-most-once
notification, not exactly-once notification: an owner-visible notice may be
lost in the claimed-before-send or accepted-before-checkpoint window.

An unsent `claimed` notice is retained until the later of the row's ordinary
`retain_until` and `notice_claimed_at + 604800` seconds. This seven-day evidence
cutoff is fixed, not configurable. `prune()` uses strict `now > cutoff`; sent or
never-claimed notices use ordinary retention after all other row constraints
allow deletion.

### Replay, FIFO, and retention

Terminal rows never call Tendwire/state mutation again; only an unclaimed
notice can proceed. Quarantine is absorbing and never auto-replays. There is no
operator recovery/replay CLI. Expired leases recover only nonterminal work.
Terminal and quarantine rows unblock their ordering key. `prune()` removes
only terminal/quarantine rows strictly after retention and never drops claimed
notice ambiguity before its longer fixed evidence cutoff.

## WAL and filesystem security

The queue uses exactly SQLite WAL mode with `synchronous=FULL`,
`foreign_keys=ON`, `trusted_schema=OFF`, and a fixed bounded busy timeout.

`IngressQueue.open_writer()`:

1. requires an absolute leaf beneath an euid-owned private directory;
2. walks/pins parents with directory FDs, `O_DIRECTORY|O_NOFOLLOW|O_CLOEXEC`;
3. rejects group/world-writable or wrong-owner parents;
4. opens and exclusively flocks a same-parent `.inbound-spool.sqlite.writer.lock`
   regular single-link EUID-owned mode-0600 file through the pinned dirfd;
5. creates a new DB with `O_CREAT|O_EXCL|O_NOFOLLOW`, mode 0600, or opens an
   existing leaf with `O_NOFOLLOW`;
6. rejects existing wrong owner/mode/type, symlink, or link count other than
   one rather than chmod-repairing it;
7. connects only through the anchored
   `/proc/self/fd/<pinned-parent-fd>/<leaf>` path, with no URI or path fallback,
   and verifies pre/post-connect device/inode identity;
8. while the writer lock remains held, lets SQLite create `-wal`/`-shm` under
   restrictive umask, then opens them no-follow relative to the same pinned
   parent and validates regular type, same owner, single link, mode 0600, and
   stable device/inode; application code never pre-creates a fake SQLite
   sidecar header;
9. rejects corrupt/unsafe/partial sidecars and non-version-1 databases; and
10. retains the pinned parent and writer-lock FDs for the `IngressQueue`
    lifetime, revalidating DB/sidecars before transactions, checkpoint, and
    close.

`IngressQueue.observe()` is a separate read-only path for live doctor/status.
It repeats steps 1--3 and the existing-leaf/sidecar owner, type, link, mode, and
device/inode validations, never creates any file, and does not acquire the
writer-ownership lock. It connects only to a percent-encoded anchored
`file:/proc/self/fd/<pinned-parent-fd>/<leaf>?mode=ro` URI with `uri=True`,
`query_only=ON`, `trusted_schema=OFF`, and the same bounded busy timeout so WAL
commits are visible while the sole writer remains live. It exposes only
aggregate `health_snapshot` and `status_rows`, performs no DDL, checkpoint,
prune, migration, repair, or mutation, and retains/revalidates its pinned
parent and database/sidecar identities through close. URI use is forbidden on
the writer path and any observer URI/query deviation fails closed.

New files use a restrictive process umask and explicit `fchmod(0600)`. Running
services never migrate, recreate, repair permissions, or silently discard a
wrong-version/corrupt queue. WAL checkpoint/truncation is bounded maintenance,
not a filesystem watcher. Logs contain only request/update/sequence IDs,
bounded enum reasons, and timing; never text, callbacks, receipts, routes, or
tokens.

That last log allowance applies only to owner-private runtime diagnostics, and
all three IDs are opaque and bounded. Health/status responses and every cutover,
archive, release, or operator report contain aggregate counts and schema
versions only: no request ID, update ID, sequence, receiver ID, key, ref,
provider coordinate, or payload. Thus runtime correlation does not weaken the
aggregate-only cutover privacy rule.

## Voice and presentation boundaries

Voice/STT/TTS is outside the preserved set. H8 removes gateway speech imports,
attachment download/pretranscription, `/voice`, reply-by-voice ingress, and
all ingress writes such as `speak_next_reply`. Voice/audio/media-only updates
are rejected. The old outbound speech module may exist only until H7/H6 because
the old presenter still imports it; H7/H6 performs complete deletion.

Pinned boards are not touched or redesigned by H8. H7/H6 owns their complete
default deletion.

H8 never imports `source_sync`, calls a presenter delivery helper, polls
Tendwire presentation state, or writes H7 presentation bindings. Conversely,
the old/new presenter never opens `inbound_spool.db`.

## Exact SLOC budget

The whole H8 ingress change is counted once, including the compatibility seam
and physical guard added to modules that H7 later replaces. It must total
1,680--2,075 canonical SLOC:

| Module/portion | Budget |
|---|---:|
| `herdres_connector/ingress.py` | 700--800 |
| `herdres_connector/ingress_queue.py` | 350--425 |
| `herdres_gateway.py` | 70--100 |
| `herdres_connector/ingress_identity.py` | 80--100 |
| `herdres.py` ingress status/integration | 20--30 |
| `herdres_connector/state.py` H8 typed seam and shared guard portion | 260--340 |
| `herdres_connector/decisions.py` H8 typed decision/guard portion | 180--250 |
| `herdres_connector/managed_bots.py` H8 typing portion | 20--30 |
| **Total** | **1,680--2,075** |

At paired H7/H6 replacement, the final whole-module budgets supersede the three
H8 portion rows; they are not added a second time. For the independently
deployable H8 result, however, these lines are real production SLOC and must be
measured. Exceeding 2,075 or hiding queue/submission logic in compatibility
modules is a design failure.

## Exhaustive production file disposition

| File | H8 disposition |
|---|---|
| `herdres_gateway.py` | replace with retained 70--100-line wrapper |
| `herdres.py` | delete child command/callback and working shortcut; retain small status integration |
| `herdres_connector/ingress.py` | add |
| `herdres_connector/ingress_queue.py` | add |
| `herdres_connector/ingress_identity.py` | retain and trim to 80--100 |
| `herdres_connector/ingress_lanes.py` | delete |
| `herdres_connector/ingress_requests.py` | delete |
| `herdres_connector/source_sync.py` | remove exact JSON-ingress/submission-working functions and callers listed above; otherwise old presenter remains |
| `herdres_connector/state.py` | edit only to add the frozen compatibility protocol, composite mutation identities, and shared physical guard implementation |
| `herdres_connector/decisions.py` | edit only for typed decision query/mutation/render support and to wrap every retained old-presenter decision markup/delete in the shared guard; submission goes through H8 |
| `herdres_connector/doctor.py` | edit: replace inbound lane probe with queue health |
| `herdres_connector/config.py` | edit: retain fixed queue/lease/deadline bounds and receiver env, delete response-version selection, add no knob |
| `herdres_connector/managed_bots.py` | edit only to retain kind normalization/username mapping and replace ingress root/token lookup with typed receiver/policy inputs |
| `herdres_connector/tendwire_client.py` | edit only to pin v3 accepted-send validation and the exact command matrices over the retained direct socket |
| `herdres_connector/telegram_delivery.py` | retain unchanged; H8 calls only its existing bounded known-target methods and never presenter-state helpers |
| `herdres_connector/speech.py` | retain for old presenter until H7/H6, but remove every H8 import/call |
| `install-user.sh`, `herdres-gateway.service` | retain executable topology and wrapper path unchanged |

No other production file is in H8 scope.

H8 documentation scope is exact: edit `README.md`, `RELEASE.md`, and
`SECURITY.md` only to replace deleted lane/child/JSON-ingress and gateway-voice
claims with the one-queue/direct-socket behavior, and update this design with
measured review evidence. No other document is edited by H8.

## Exhaustive test disposition

| Test file | Disposition |
|---|---|
| `tests/test_ingress_lanes.py` | delete; rewrite surviving queue/FIFO/crash behavior in `test_ingress.py` |
| `tests/test_ingress_requests.py` | delete; rewrite surviving exact-byte/receipt/quarantine behavior in `test_ingress.py` |
| `tests/test_ingress.py` | add as the one queue/orchestration behavior suite |
| `tests/test_command_ingress_idempotency.py` | retain HMAC vectors, identity coordinates, key path/permissions/inode-swap, installer path tests; remove only old retry-record/child assumptions |
| `tests/test_gateway_cleanup.py` | delete exactly the topic-icon/service-message cases whose only subject is the removed gateway side effect; retain every presenter/lifecycle cleanup case |
| `tests/test_remote_decisions.py` | retain render/keyboard/Tendwire-schema and old-presenter cases; rewrite only callback, toggle, arm/freeform, shared-guard, and ingress-concurrency cases against H8 typed queue path; H8 deletes no H7/H6 generic-provider case |
| `tests/test_pending_inputs.py` | remove only bare-number/pending-input cases that invoke the deleted command child; retain old-presenter pending-list cases until H7/H6 |
| `tests/test_source_only.py` | remove command child, JSON ingress, submission-link, and receipt-derived working tests; retain ordinary observational working and `ec0e36a` routing until H7/H6 |
| `tests/test_offlock_delivery.py` | remove ingress/global-state-lock coupling in H8; retain presenter-only cases until H7/H6 |
| `tests/test_tendwire_client.py` | retain exact send/decision request/result and socket ambiguity tests; add pinned v3 send validation |
| `tests/test_tendwire_socket_pairing.py` | extend with real queue -> daemon command -> durable receipt -> restart duplicate proof |
| `tests/test_release_readiness.py` | update static gates for retained gateway wrapper, no child/subprocess/fallback, one queue, and installed service topology |
| `tests/test_speech.py` | remove only gateway download/pretranscription and voice-ingress cases in H8; retain old-presenter outbound speech cases until H7/H6 deletion |
| `tests/test_speak_back.py` | remove only gateway reply-by-voice/`speak_next_reply` ingress cases in H8; retain old-presenter cases until H7/H6 deletion |

`tests/test_ingress.py` must cover:

- exact DDL/user-version and every structural invariant;
- DB/WAL/SHM owner/mode/type/link/symlink/inode-swap/corruption refusal;
- atomic cursor+enqueue, duplicate/collision, commit failure, overflow quarantine;
- explicit initial-cursor behavior and cutover seeding;
- same-topic message/decision FIFO and unrelated-key concurrency;
- claim attempt count, renew, expiry, wrong-owner/digest CAS, deadline equality;
- canonical command/local-action checkpoint, exact local phase/token columns,
  and composite mutation identity before external mutation;
- every exact v3 send and v2 decision success/failure matrix pair and result
  constraint;
- not-started/no-receipt/in-progress retry versus started ambiguity quarantine;
- lease loss during socket call and Tendwire receipt convergence;
- nullable pre-T6 generation, valid T6 generation, invalid grammar, one
  null-generation stable-owner fingerprint refresh, and absolute refusal to
  remove a fingerprint once route generation is non-null;
- terminal/quarantine replay, pruning, and aggregate status/health through a
  read-only observer while the exclusive writer lock is held by another process;
- exact 60-second durable overflow throttle, callback toast versus terminal
  notice, seven-day claimed evidence, notice claim loss, and no duplicate send;
- arm-freeform composite idempotence, stale-token refresh, and
  callback-before-text ordering;
- every multi-toggle phase/crash boundary, independently idempotent toggle and
  markup-record phases, stale-token refresh, not-modified and ambiguous-edit
  convergence, missing target, owner drift, and no send-new fallback;
- shared physical-owner guard namespace/permissions/deadline/lock ordering,
  lease renewal under contention, and a real old-presenter-retire versus H8
  markup race proving at most one provider mutation at a time;
- single, plan, multi-submit, and free-form decision submission;
- exact manager/child receiver precedence;
- alias, reply binding, author correction, mention, ambiguous binding, and the
  two `ec0e36a` corrupt-binding regressions;
- voice/media rejection and privacy/log scans; and
- concurrent H8 progress while old presenter blocks in Telegram.

Static gates fail on any production occurrence of the old JSON ingress key,
old ingress modules, command child, ingress subprocess, presenter import from
H8, H8 generic state persistence call, direct CLI/database fallback, or second
ingress queue. They also fail if any retained old-presenter decision
markup/delete or H8 local markup bypasses `provider_mutation_guard`, or if a
non-null-route command removes its worker fingerprint.

## Private build, merge, and deployment order

1. H8-A privately adds the typed compatibility protocol and tests.
2. H8-B privately adds the queue/runtime, removes both old ingress ledgers,
   removes the exact `source_sync.py` ingress joins, and restores the full
   pre-T6 suite.
3. H8-A+B merge as one reviewed change. Neither partial change lands.
4. H8 may be deployed against current Tendwire after the cutover below.
5. T6 is developed separately. H7 then H6 form a private Herdres stack based
   on H8 and the exact T6 five-kind contract.
6. T6 and H7/H6 pass their paired real-daemon contract suite before deployment.
7. T6 deploys only with H7/H6. T6 must never run with the old presenter.

## H8 cutover

The old ingress has two durable evidence stores. Before switching, stop the old
gateway and report aggregate counts from both:

- old `inbound_spool.db`: pending, processing, blocked, done, and each receiver
  cursor;
- old `state.json["tendwire_ingress_command_requests"]`: resolving, retryable,
  terminal, quarantined, and uncertain/operator-attention rows.

Operators must drain all open work or explicitly record its discard. Merely
draining SQLite is insufficient. Archive the old DB, WAL, SHM, a read-only copy
of the request-ID key, and a state snapshot as one rollback set only after
writers are quiescent and a successful SQLite WAL checkpoint has established
the archived DB/WAL/SHM boundary. The installed active request-ID HMAC key is
retained byte-for-byte at the same configured path for H8; it is copied into the
archive, never moved, regenerated, or replaced. Record the checkpoint result
and aggregate evidence counts before copying or moving any database/state file.
H8 does not migrate the old JSON records; production contains no reader for
that key after the switch.

For receiver cursors choose exactly one release procedure:

1. **Seed:** record each old next-update cursor and initialize the fresh
   schema-1 `receiver_cursors` rows before any H8 poll; or
2. **Blackout/discard:** explicitly acknowledge that updates arriving during
   downtime will be discarded, then initialize each receiver at Telegram's
   newest boundary.

This is an operator-controlled one-shot fresh cutover, not a runtime migration
chain. Start one H8 gateway, verify DB/WAL/SHM identities, receiver cursors,
queue health, and one real daemon-socket terminal receipt before normal use.

## Rollback limitation

Rollback is straightforward only before H8 accepts new work. After H8 accepts
any update, the old snapshot has stale cursors and lacks H8 terminal/quarantine
evidence. Such rollback is not declared lossless.

Post-acceptance rollback requires stopping H8, draining it where possible,
recording its per-receiver cursors and accepted/quarantined request IDs, and an
explicit operator decision to seed/advance old cursors or discard the affected
window. Tendwire's stable request IDs may suppress an exact replay, but rollback
must not assume that canonical bytes from old and new routing are identical.
Restore the matching old release, old SQLite/WAL/SHM, request key, and state
snapshot only after that decision. Old and new gateways never overlap or open
each other's queue format.

The next safe major deployment boundary after independent H8 is the paired T6
+ H7/H6 release. Its cutover separately drains/discards Tendwire presentation
work and replaces Herdres state. No design may deploy T6 five-kind production
with the old final-only presenter.

The paired cutover likewise includes a read-only archive copy of the same active
H8 request-ID key and retains the active file byte-for-byte. Its matched set is
Tendwire DB/WAL/SHM and installation-key family, H8 DB/WAL/SHM and request-ID
key copy, old Herdres state/provider facts, and aggregate release/config
identifiers. Replacing either installation or request key is an explicit
continuity break, never rollback or routine key handling.

Production work remains blocked until adversarial review confirms the typed
protocol matches H7 verbatim, queue SQL/CAS is executable, the live
`source_sync.py` dependency is fully removed, `ec0e36a` routing tests survive,
and the measured complete H8 result is within 1,680--2,075 canonical SLOC.
