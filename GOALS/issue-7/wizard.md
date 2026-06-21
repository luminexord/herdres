# Task 1 — `herdres setup` interactive credential wizard

**Branch:** `feat/issue7-wizard` (off `skill-herdres-operator`)
**Why:** a `SKILL.md` instruction only *biases* an agent; the binary asking for secrets is the real guarantee that nobody scavenges another app's token.

## Implement in `herdres.py`

1. **Argparse** (near `main()`'s subparser block, ~line 10715):
   ```python
   setup = sub.add_parser("setup")
   setup.add_argument("--bot-token", default="")
   setup.add_argument("--chat-id", default="")
   setup.add_argument("--allowed-users", default="")
   setup.add_argument("--reuse-hermes-token", action="store_true")
   ```
   Dispatch (~10742): `elif args.cmd == "setup": result = setup_once(args)` — return the standard `{"ok": True, "result": {...}}` dict so `main()` JSON-serializes it.

2. **`setup_once(args)`** — new function. Reuse existing helpers:
   - `load_dotenv()` (390) so an existing `~/.config/herdres/herdres.env` and `~/.hermes/.env` are visible.
   - **Interactivity gate:** `interactive = sys.stdin.isatty()`. Resolve each value from its flag first, else prompt if interactive. If **not interactive** and any required value is still missing → `raise BridgeError("herdres setup needs an interactive terminal, or pass --bot-token/--chat-id/--allowed-users")`.
   - **Prompts:** token via `getpass.getpass("Telegram bot token: ", stream=sys.stderr)` (no echo); chat-id and allowed-users via `input()`. Re-prompt on invalid input (interactive only).
   - **Validation:** token `^\d+:[A-Za-z0-9_-]{30,}$`; chat-id `^-100\d+$`; allowed-users `^\d+(,\d+)*$`. On invalid non-interactive value → `BridgeError`.
   - **No scavenging:** read `~/.hermes/.env` (`DEFAULT_HERMES_ENV`, line 35). If its `TELEGRAM_BOT_TOKEN` equals the resolved token (or the user left the token blank intending to reuse), require `--reuse-hermes-token` **or** an interactive `reuse`-typed confirmation; print a warning about the one-`getUpdates`-consumer rule. Never adopt the Hermes token silently.
   - **Verify before write:** set the env in-process and call `preflight(chat_id)` (8254 — `getChat`/`getMe`/`getChatMember`). Surface its `BridgeError` as-is.
   - **Write** `~/.config/herdres/herdres.env` (`DEFAULT_ENV`) atomically at **`0o600`**, reusing the pattern at line 5773 (`os.open(path, os.O_WRONLY|os.O_CREAT|os.O_TRUNC, 0o600)` → write → `fsync` → `os.replace` from a temp in the same dir). **Preserve** any existing non-target keys in the file (parse, update the 3 keys, rewrite).
   - Print next steps (enable the timer / link the plugin) and return the result dict (never echo the token back).

3. **stdlib only:** `getpass`, `os`, `sys`, `re`, `tempfile` (all already imported or stdlib).

## Tests — `tests/test_setup.py`

Mock `herdres.telegram_api`, `getpass.getpass`, `builtins.input`. Cover:
- non-interactive + missing flags → `BridgeError` (refuses), writes nothing;
- the Hermes token would be reused without `--reuse-hermes-token` → refuses/asks (no silent reuse);
- happy path (flags) → calls `preflight` **before** writing, writes `herdres.env` at mode `0o600`, content has the 3 keys;
- invalid token/chat-id/users (non-interactive) → `BridgeError`.

## Docs

- Add a real `herdres setup` step to **both** `SKILL.md` (Quick install — "fastest: run `herdres setup`") and `references/COMMANDS.md` (CLI table). Update `references/SAFETY.md` to name `herdres setup` as the enforcing mechanism. Add a one-line pointer to `.env.example` header.
- These make `setup` a real subcommand, so `tests/test_skill_herdres.py::test_no_invented_cli_subcommands` stays green.

## Acceptance

- [ ] `herdres setup` works in a TTY (prompt → validate → preflight → write `0o600`).
- [ ] Non-interactive without flags **refuses**; reusing the Hermes token requires explicit confirmation.
- [ ] `tests/test_setup.py` + full `pytest tests/` green.
- [ ] `/code-review` clean.
