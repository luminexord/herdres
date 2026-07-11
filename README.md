<img width="2048" height="2048" alt="herdres" src="https://github.com/user-attachments/assets/d8324729-676a-49d8-9d24-800a8a411348" />

# Herdres

This branch is a tiny source-mode-only Telegram connector for Tendwire.

Herdres does not observe or control Herdr directly here. Tendwire owns Herdr
observation, worker bindings, turns, pending interactions, command routing,
receipts, backend health, and connector outbox. Herdres owns Telegram polling,
topics, message send/edit, compact working updates, final response display, and
Telegram delivery dedup.

**Requires [Tendwire](https://github.com/plotarmordev/tendwire)** — Herdres has
no functionality without it. See [INSTALL.md](INSTALL.md) for setup order.

## Runtime

```text
Telegram topic input
  -> herdres-gateway
  -> herdres command
  -> tendwire command --json

herdres sync
  -> tendwire snapshot/turns/pending/connector
  -> Telegram topics/messages/pinned status
```

Only `HERDRES_TENDWIRE_MODE=source` is supported. Herdres obtains observations,
turns, pending interactions, and connector work only through Tendwire's public
source/command interfaces; it makes no direct Herdr API, process, or socket
calls.

Every source sync requires the top-level `schema_version` in Tendwire's
`turns --json` response to be the exact integer `1`. A missing value, a boolean,
a string such as `"1"`, a float, a container, or any other integer fails closed
with `status=unsupported_turn_schema_version` and
`required_turn_schema_version=1`. The check occurs before source-state,
Telegram, cleanup, or connector-outbox mutation. Herdres neither coerces an
incompatible value nor falls back to a lossy turn shape.

Recovery uses this same source path. Tendwire's existing `turn.list` path
refreshes structured turn content from a recovered persisted binding before it
returns the durable public projection; Herdres does not refresh Herdr itself.
When a later source sync receives the authoritative schema-v1 completed turn
for a matching editable Working card, Herdres edits that card once into the
final response and records the final binding and delivery ledger. Repeating the
same sync performs no additional edit, send, or ledger update.

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

Keep the copy private. The dry check must succeed with schema version `1` and
`direct_herdr_calls=0` before a live sync; it does not save the copied Herdres
state or send/edit Telegram messages. If verification fails, leave the live
state untouched. Do not repair continuity by editing state, copying public
handles, deleting individual key files, or rotating identity.

For continuity recovery, stop all writers and restore the complete paired
Herdres/Tendwire backup described in [INSTALL.md](INSTALL.md), then repeat the
dry check against a copy before resuming writers. A Herdres state copy alone is
not a substitute when Tendwire database or installation-key material changed.

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
python -m pytest -q tests/test_source_only.py tests/test_stable_worker_key.py
HERDRES_TENDWIRE_MODE=source ./herdres.py doctor
HERDRES_TENDWIRE_MODE=source ./herdres.py tendwire source-smoke --with-outbox
```

The focused tests cover the schema-v1 preflight, deterministic private
migration/quarantine, and recovered-final single-edit/no-replay behavior.
`source-smoke` must report `direct_herdr_calls=0`.
