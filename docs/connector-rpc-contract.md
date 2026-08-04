# Current Herdres connector RPC contract

This Wave 0 document freezes the connector seam that exists before the Wave 3
socket-client rewrite. It describes the wire-level Tendwire daemon RPC at
Tendwire commit `91891bfb4aa44a03bea37571911b5c83e0ec977d` and the operations
the Herdres client actually issues at Herdres commit
`ff000d6c19ba8c24cf21aff301cbba883cc39f07`.

The primary evidence is:

- `/home/smith/tendwire/src/tendwire/daemon_api.py`: required method names at
  lines 143-165, request validation at 346-385, connector dispatch at 637-655,
  response envelopes at 245-290, and newline-delimited socket framing at
  1724-1833.
- `herdres_connector/tendwire_client.py`: connector constants at lines 41-45,
  daemon-socket selection at 644-649, attention helpers at 990-1040, prepare
  helpers at 1057-1199, and turn-final helpers at 1201-1280.
- `/home/smith/tendwire/src/tendwire/connectors/outbox.py`: the connector
  implementation delegated to by `daemon_api.py`, including strict prepare
  shapes at lines 235-403, poll results at 406-467, mutation fields at
  480-581, and verb dispatch at 660-680.
- `/home/smith/tendwire/src/tendwire/cli.py`: the temporary subprocess
  adapter's flag-to-RPC normalization at lines 1510-1563.

Herdres currently invokes a Tendwire CLI subprocess, with
`TENDWIRE_SOCKET_PATH` forced so the CLI attempts the long-running daemon.
Waves 3-4 may remove that subprocess hop, but must preserve the contract below.

## Wire envelope

The transport is one UTF-8 JSON object followed by `\n` over an `AF_UNIX`
stream. A request is at most 1 MiB and has no top-level fields other than the
optional `id`, `method`, and `params`:

```json
{"method":"connector.poll","params":{"name":"turn-final","limit":1,"lease_seconds":60}}
```

`params` is an object; omitted or `null` params normalize to `{}`. The standard
successful dispatch envelope is:

```json
{
  "schema_version": 1,
  "ok": true,
  "status": "ok",
  "result": {"schema_version": 1, "ok": true, "status": "ok"},
  "error": null
}
```

The optional safe request `id` is echoed as `id`. A dispatch/protocol failure is:

```json
{
  "schema_version": 1,
  "ok": false,
  "status": "error",
  "result": null,
  "error": {"code": "invalid_params", "message": "...", "details": {}}
}
```

Connector business failures are different: dispatch succeeded, so outer `ok`
remains `true`, while `result.ok` is `false` and `result.status`/`result.error`
describe the connector failure. The existing CLI unwraps `result` before
Herdres sees it. A direct socket client must therefore validate the outer
envelope, unwrap exactly once, and then preserve the inner business result.
The daemon sanitizes the connector result and explicitly restores valid opaque
plan tokens after sanitization.

## Methods Herdres uses

Only five daemon verbs are in the current Herdres connector path:

| Herdres helper | Daemon RPC | Queue |
|---|---|---|
| `connector_prepare_begin/part/commit/recover` | `connector.prepare` | `turn-final` |
| `connector_poll` | `connector.poll` | `attention` by default |
| `connector_ack` | `connector.ack` | `attention` by default |
| `connector_fail` | `connector.fail` | `attention` by default |
| `turn_final_poll` | `connector.poll` | `turn-final` |
| `turn_final_ack` | `connector.ack` | `turn-final` |
| `turn_final_fail` | `connector.fail` | `turn-final` |
| `turn_final_defer` | `connector.defer` | `turn-final` |

`turn_final_poll`, `turn_final_ack`, `turn_final_fail`, and
`turn_final_defer` are Herdres helper names, **not RPC verbs**. The daemon does
not expose `turn_final_*` or `turn-final.*` methods; sending one produces
`unknown_method`.

The daemon also advertises `connector.renew`, `connector.release`,
`connector.reclaim`, `connector.retry`, and `connector.inspect`. The current
Herdres client has no corresponding helper or call site, so those verbs are
outside this current-use freeze. Their presence in Tendwire must not be mistaken
for existing Herdres dependence.

## Request shapes

All opaque refs and tokens below are strings. JSON examples omit the outer
`{"method": ..., "params": ...}` wrapper for readability.

### `connector.prepare`

Every prepare action uses schema 1 and `name: "turn-final"`. Herdres bounds the
encoded request to 64 KiB.

Begin:

```json
{
  "schema_version": 1,
  "action": "begin",
  "name": "turn-final",
  "turn_id": "turn-...",
  "content_revision": "twrev1....",
  "presentation_version": "...",
  "part_count": 2,
  "source_ref": "twref1...."
}
```

`source_ref` is optional. Part:

```json
{
  "schema_version": 1,
  "action": "part",
  "name": "turn-final",
  "plan_token": "twplan1....",
  "ordinal": 0,
  "spans": [
    {"field":"assistant_final_text","start_char":0,"end_char":3900}
  ]
}
```

Each span has exactly `field`, `start_char`, and `end_char`; `field` is
`user_text` or `assistant_final_text`; indexes are integers with
`0 <= start_char < end_char`. Herdres' current client-side bound is 1-256
spans, but Tendwire's delegated connector implementation accepts at most 64.
The effective current contract is therefore **1-64 spans**. This mismatch is a
baseline finding; `daemon_api.py` treats `params` as opaque and cannot reconcile
the two bounds itself.

Commit:

```json
{
  "schema_version": 1,
  "action": "commit",
  "name": "turn-final",
  "plan_token": "twplan1....",
  "source_ref": "twref1...."
}
```

`source_ref` is optional. Recover:

```json
{
  "schema_version": 1,
  "action": "recover",
  "name": "turn-final",
  "failed_plan_token": "twplan1....",
  "request_id": "public-safe-id"
}
```

The Herdres client requires a `twplan1.` token with a non-empty ASCII
alphanumeric/`_`/`-` body and a 1-128 character public-safe request id using
ASCII alphanumerics plus `.`, `_`, `:`, or `-`.

### `connector.poll`

```json
{"name":"attention","limit":3,"lease_seconds":60}
```

or, through `turn_final_poll`:

```json
{"name":"turn-final","limit":1,"lease_seconds":60}
```

`limit` and `lease_seconds` are integers. The shown values are current client
defaults; callers may supply different values.

### `connector.ack`

```json
{"name":"attention","ref":"twref1....","response":{"duplicate":true}}
```

or:

```json
{"name":"turn-final","ref":"twref1....","response":{"outcome":"applied"}}
```

`response` is optional and must be an object. Herdres sanitizes it before
crossing the boundary; Telegram/provider identifiers are not contract fields.

### `connector.fail`

```json
{"name":"attention","ref":"twref1....","reason":"..."}
```

or:

```json
{"name":"turn-final","ref":"twref1....","reason":"..."}
```

The public Herdres attention helper calls its local argument `error`, while the
turn-final helper calls it `reason`; the daemon RPC field is `reason`. Herdres
sanitizes the text to at most 240 characters before invocation.

At this freeze point the attention helper serializes that value as the CLI flag
`--error`, but Tendwire's subprocess adapter accepts `--reason`. Consequently,
an attention failure does not currently reach `connector.fail` through this
subprocess path. This is a baseline transport mismatch, not an alternate RPC
shape: a direct socket client must send `params.reason` while retaining the
Herdres helper's public `error` argument if callers still need it.

### `connector.defer`

```json
{
  "name":"turn-final",
  "ref":"twref1....",
  "reason":"rate limited",
  "available_at":"2026-08-04T12:00:00+00:00",
  "delay_seconds":30
}
```

`reason` may be empty. `available_at` and `delay_seconds` are independently
optional in the current client shape; most callers provide at most one.

## Inner result shapes consumed by Herdres

All turn-final helpers reject a nominally successful result unless
`schema_version` is the integer `1`. Beyond the fields below, the current client
permits additive public fields.

Prepare results have the common prefix:

```json
{"schema_version":1,"ok":true,"status":"ok","host_id":"...","name":"turn-final"}
```

Successful action-specific fields currently consumed or preserved are:

| Action | Fields after the common prefix |
|---|---|
| `begin` | `plan_token`, `state`, `part_count`, `accepted_parts`, optionally `generation` |
| `part` | `plan_token`, `ordinal`, `accepted_parts` |
| `commit` | `plan_token`, `state`, `job_count`, optionally `generation` |
| `recover` | `failed_plan_token`, `plan_token`, `generation`, `content_revision`, `state`, `acknowledged_prefix_count`, `executable_job_count`, `retained_failed_job_count`, `prior_attempt_count`, `idempotent_replay` |

A poll result is:

```json
{
  "schema_version": 1,
  "ok": true,
  "status": "ok",
  "host_id": "...",
  "name": "turn-final",
  "items": [
    {
      "ref": "twref1....",
      "key": "...",
      "attempt": 1,
      "leased_until": "...",
      "available_at": "...",
      "created_at": "...",
      "payload": {}
    }
  ]
}
```

`created_at` is turn-final-specific and may be absent. `payload` is a sanitized
connector payload; Herdres depends on valid `twplan1.` values surviving in
`plan_token`, `replaces_plan_token`, and `failed_plan_token`. An empty queue is
represented by `items: []`, not by a missing result.

Successful `ack`, `fail`, and `defer` results use schema 1 and carry `ok`,
`status`, `host_id`, `name`, `ref`, `key`, `attempt`, and `available_at` as
applicable. Herdres makes control-flow decisions from `ok` and `status`; refs
and tokens remain opaque. A connector rejection uses the same inner shape with
`ok: false` and an `error` object containing at least `code` and `message`.

## Compatibility rule for Waves 3-4

A replacement direct socket client is compatible only if it:

1. sends the five `connector.*` verbs and queue names above;
2. preserves the exact prepare action objects and lease mutation fields;
3. distinguishes outer RPC failure from inner connector failure;
4. requires connector schema 1 for the turn-final path;
5. keeps opaque refs, revision tokens, and plan tokens byte-for-byte; and
6. does not add a subprocess fallback, SQLite/WAL access, or invented
   `turn_final_*` socket methods.
