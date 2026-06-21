# AGENTS.md

Guidance for AI agents (Devin, Codex, Claude, etc.) working in this repository.

## What Herdres is

Herdres is a **stdlib-only Python 3.11+ bridge** that maps each [Herdr](https://github.com/gaijinjoe/herdr) space to a Telegram forum topic. It posts pane traffic into the mapped space topic, records the Telegram messages it sends as pane routing anchors, and lets an owner drive panes from Telegram (`/send`, `/keys`, plain-text replies, inline choice buttons, `/new <agent>` pane launches).

It does **not** patch Hermes or Herdr core. Routine sync uses no LLM calls. The cleanest mode (`HERDR_TELEGRAM_TOPICS_TURN_FEED=1`) consumes a structured `herdr pane turn <id> --last --format json` contract instead of parsing terminal text.

> **Operating vs contributing:** this file guides agents *working on* herdres. To *operate* a deployment (install it, drive panes from Telegram), use the [`skills/herdres/`](skills/herdres/SKILL.md) Agent Skill instead — it is the portable operator counterpart to this contributor guide.

## Repository layout

| Path | Role |
| --- | --- |
| `herdres.py` | The main CLI. ~10k lines, single file, stdlib-only. Subcommands: `sync`, `event`, `plugin-enable`, `plugin-disable`, `cleanup-duplicates`, `command`, `callback`, `managed-bot`, `probe`. Owns the state file and the file lock. |
| `herdres_routing.py` | Pure routing/payload helpers (topic lookup, attachment extraction, command/callback payload builders). Shared by both gateways and the Hermes bridge. **No I/O, no token.** |
| `herdres_gateway.py` | Standalone inbound gateway for the **managed-bots** macOS path. Long-polls `getUpdates` per bot token (manager + each child bot), dispatches through a worker pool, embeds the Herdres command runner by default. |
| `herdres-gateway.py` | Simpler **upstream/single-token** gateway (Linux/no-managed-bots path). Imports `herdres_routing`. |
| `herdr_topic_bridge.py` | Async Hermes hook. Used when Hermes already long-polls the same bot token. Honors `HERDRES_BRIDGE_DISABLED=1` to stand down when the standalone gateway owns inbound. |
| `herdr_turn_adapter.py` | Wrapper that adds `herdr pane turn` from agent session logs (Codex/Claude) when Herdr doesn't expose it yet. Delegates every other command to `HERDR_REAL_BIN`. |
| `herdres-plugin/herdr-plugin.toml` | Herdr 0.7.0+ plugin manifest: `pane.agent_status_changed -> herdres event`. |
| `install-user.sh` | Linux/systemd installer. |
| `install-macos.sh` | macOS installer: pins shebangs to a Python >= 3.11, installs launchd agents + the cockpit. |
| `launchd/`, `systemd/user/` | Service/timer/plist templates (`__HOME__` replaced by `install-macos.sh`). |
| `ssh/` | Tailscale cockpit: `ssh/server/` (Node.js, `node-pty` + `ws`, validates Telegram `initData`) and `ssh/web/` (xterm.js Mini App). See `ssh/README.md`. |
| `tests/` | pytest suite (456 tests). `conftest.py` loads the standalone scripts into `sys.modules` so tests can `import herdres`. |
| `.env.example` | All environment knobs with defaults and comments. The source of truth for config. |
| `assets/managed-bots/` | Default JPG profile photos for managed child bots. |

## Runtime data locations (defaults)

- Config: `~/.config/herdres/herdres.env`
- State: `~/.local/share/herdres/state.json` (schema `version: 1`, `enabled: true`)
- Sync lock: `~/.local/share/herdres/sync.lock`
- Gateway offset: `~/.local/share/herdres/gateway.offset` (or `gateway_offset`)
- Inbound files for long/multiline owner input: `~/.local/share/herdres/inbound/<pane>/...txt` (owner-only perms)
- Gateway trace: `~/.local/share/herdres/gateway.trace.log`

## Commands

### Run the test suite

```bash
python3 -m pytest -q
```

No pytest config file exists; collection works from the repo root because `tests/conftest.py` loads the script modules. Tests are `unittest.TestCase`-based and use `unittest.mock`. There is no separate lint/typecheck config — keep code stdlib-only and Py3.11-compatible.

### Run a single test file / test

```bash
python3 -m pytest tests/test_gateway.py -q
python3 -m pytest tests/test_pinned_status.py::PinnedStatusTests::test_status_dot_mapping -q
```

### Cockpit (Node) tests

```bash
cd ssh/server && npm test        # node --test
```

### Dry-run sync (no Telegram writes)

```bash
HERDR_TELEGRAM_TOPICS_DRY_RUN=1 ~/.local/bin/herdres sync
```

### Probe rich delivery against a scratch topic

```bash
~/.local/bin/herdres probe --thread-id 123
```

### Version and self-update

`herdres version` prints `HERDRES_VERSION`; `herdres update` does an env-safe self-update (`--channel edge` default, plus `--check`/`--rollback`/`--dry-run`/`--repo`), never overwriting `herdres.env`.

## Architecture invariants — respect these when editing

1. **Stdlib-only Python.** No third-party imports in any `.py` at the repo root or in `herdres_routing.py` / gateways / bridge / turn adapter. The only non-stdlib code is the Node cockpit under `ssh/`.
2. **One `getUpdates` consumer per bot token.** Never run Hermes polling and `herdres-gateway`/`herdres_gateway` on the same token. Outbound `sync`/`event` only *send* and are safe to run alongside either.
3. **The state file is owned by the outbound reconciler** (`herdres sync`/`event`). Inbound paths (bridge, gateways) only *read* state and delegate to `herdres command`/`callback`/`managed-bot` via a JSON-on-stdin contract. Don't add state writes to the inbound side.
4. **Fail closed on ambiguity.** Multi-pane space topics reply with `Reply inside a pane thread so I know which Herdr pane to control.` rather than guessing. Single-live-pane topics are the only implicit-send exception.
5. **Never key-drive multi-question TUI wizards from Telegram.** Structured `pending_decision` and explicit `HERDRES_CHOICES_START` blocks render buttons; inferred visible-screen buttons stay off by default. `pending_interaction` is read-only in this phase.
6. **No secrets in state.** State stores topic ids, route indexes, hashes, optional pane-root ids, and managed-bot tokens under `telegram.managed_bots`. It never stores the manager bot token. Raw pane output is only posted via explicit `/raw`; secrets are redacted before posting.
7. **Upgrade-safe Herdr integration.** `herdr pane turn` is an *optional* upstream contract. When unavailable/empty/incomplete without a stream preview or decision, Herdres sends nothing and does not fall back to `pane read`. Don't add hard Herdr version requirements.
8. **Capability-probe, then latch.** Rich endpoints (`sendRichMessage`, `sendMessageDraft`, `editMessageText(rich_message=...)`) are probed at runtime; on rejection, latch that feature off and fall back to `sendMessage`. Transient/network errors are not retried (avoid duplicate posts); live-card edits retry naturally on the next tick.
9. **DRY helpers live in `herdres_routing.py`.** Both gateways and the bridge share topic-routing and payload-building helpers there (dict and obj variants). When adding routing logic, extend `herdres_routing.py` instead of duplicating it in three places. Env-flag parsing and Telegram response handling are also already deduplicated — reuse them.

## Conventions

- **Single-file CLI.** `herdres.py` is intentionally one large module; do not split it without reason. New functions go in the appropriate existing section (managed bots, routing, rendering, turn feed, etc.).
- **`from __future__ import annotations`** at the top of every module; type hints use PEP 604 unions (`str | None`).
- **No comments added/removed unless asked.** Existing comments are load-bearing (they document invariants and fail-closed reasoning). Preserve them in edits.
- **Compact code.** Collapse duplicate branches, avoid unnecessary nesting, share abstractions. Match the surrounding style.
- **Error handling at boundaries.** Don't sprinkle try/except everywhere. The CLI `main()` wraps subcommands and emits `{"ok": false, "error": ...}` (sanitized) or a rate-limit result (exit 75). Inbound handlers log and drop a single update, never crash the poll loop.
- **Atomic state/offset writes.** Use the existing temp-file + `os.replace` + fsync pattern (see `write_offset_atomic` / `save_state`).
- **Tests use `unittest.TestCase` + `unittest.mock`**, loaded via `tests/conftest.py`. Add new tests as `tests/test_*.py`. Use the shared `make_pane` / `write_jsonl` helpers from `conftest.py` instead of redefining them.
- **Commit messages** are short, lowercase-prefixed by area (`gateway:`, `topics:`, `bridge:`, `adapter:`, `install-macos:`) — match `git log --oneline`.

## JSON contract for inbound handlers

`herdres command` (stdin) and `herdres callback` (stdin) consume the payload built by `herdres_routing.build_command_payload_dict` / `build_callback_payload_dict`. Both gateways and the Hermes bridge produce this same shape. When changing the payload, update `herdres_routing.py` and the corresponding `*_obj` variant together, and update tests in `tests/test_gateway.py` / `tests/test_topic_bridge.py`.

Reply contract (stdout JSON):

- `command`: `{"handled": bool, "reply": str?}` — `reply` is sent back to the user; `handled: false` means "drop silently".
- `callback`: `{"handled": bool, "answer": str?, "show_alert": bool?}` — passed to `answerCallbackQuery`. **Always answer callbacks** (even on auth failure) or the Telegram button spinner spins forever.
- `managed-bot`: produced by `managed_bot_update` for `managed_bot` updates.

## When adding config knobs

- Add to `.env.example` with a comment explaining the default and trade-off.
- Read via the existing env-flag parsing helpers (booleans through `parse_bool_env`).
- Document the new knob in `README.md` under "Useful Environment Variables" if user-facing.
- Add a test that asserts the default behavior and the override.

## macOS vs Linux split

- **Linux/systemd**: `install-user.sh` + `systemd/user/*`. Reconcile timer (`herdres.timer`, 60s) + optional `herdres-gateway.service`. Inbound traditionally via the Hermes bridge (`herdr_topic_bridge.py`).
- **macOS/launchd**: `install-macos.sh` + `launchd/*`. Reconcile timer (`com.gaijinjoe.herdres`, 5s) + `com.gaijinjoe.herdres-gateway` (the managed-bots gateway) + optional `com.gaijinjoe.herdres-cockpit` (Tailscale Mini App). The standalone gateway replaces the Hermes bridge.

When editing install scripts or service templates, keep both platforms in sync where the behavior is equivalent.
