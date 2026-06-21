# Task 1 — `herdres update` core

**Branch:** `feat/issue13-update-core` (off `main`)
**Files:** `herdres.py`, `tests/test_update.py` only.

## Implement in `herdres.py`

1. **Version:** add `HERDRES_VERSION = "0.2.0"` near the top constants. Add a `version` subcommand (argparse near `main()` ~line 10715, dispatch ~10742) whose handler returns the standard dict `{"ok": True, "version": HERDRES_VERSION}` so `main()` JSON-prints it.

2. **`update` subcommand** with flags:
   - `--channel` (`edge` default; `stable` accepted but errors "stable channel needs releases (#13 Phase 3)")
   - `--check` (report only), `--rollback`, `--dry-run`, `--repo <path>`.

3. **`update_once(args)`** — return the standard `{"ok": ...}` dict.
   - **Resolve source repo:** `args.repo` > `os.environ.get("HERDRES_SRC")` > read `~/.local/share/herdres/source` (the marker Task 2 writes) > raise `BridgeError("herdres update: source checkout not found; pass --repo or set HERDRES_SRC")`.
   - **`--check`:** `git -C <repo> pull --ff-only --dry-run` is unreliable; instead `git -C <repo> fetch` then compare `HERDRES_VERSION` parsed from `<repo>/herdres.py` (and/or `git rev-parse`) against the running `HERDRES_VERSION`. Print current vs available, **apply nothing**.
   - **edge apply:**
     1. `git -C <repo> pull --ff-only` (fail closed on a dirty/diverged repo with a clear message).
     2. **Back up** the current install set into `~/.local/share/herdres/backups/<UTC-timestamp>/` (copy the installed files listed below). Keep the last N (e.g. 5) backups; prune older.
     3. **Atomic, env-preserving replace** of the code files — the `install-user.sh` set: `herdres.py`→`~/.local/bin/herdres` (0755), `herdres_gateway.py`→`~/.local/bin/herdres-gateway` (0755), `herdres_routing.py`→`~/.local/bin/herdres_routing.py` (0644), `herdr_topic_bridge.py`→`~/.local/share/herdres/herdr_topic_bridge.py` (0644), the plugin manifest (sed the absolute `herdres` path) → `~/.local/share/herdres/herdres-plugin/herdr-plugin.toml`, and `systemd/user/herdres*.{service,timer}` → `~/.config/systemd/user/`. Use a temp file in the dest dir + `os.replace` per file (reuse the pattern at `herdres.py:5773`). **NEVER touch `~/.config/herdres/herdres.env`.**
     4. **Restart services** — detect platform: **Linux/systemd:** `systemctl --user daemon-reload`; `systemctl --user restart herdres.timer`; gateway **`systemctl --user disable --now herdres-gateway.service`** then **`enable --now`** (releases the single getUpdates lease). **macOS/launchd:** `launchctl bootout gui/$(id -u)/<label>` then `bootstrap` for the herdres + gateway plists. Only restart a unit that is currently enabled/loaded.
     5. **Verify:** run `~/.local/bin/herdres version` (parse the new version) and `HERDR_TELEGRAM_TOPICS_DRY_RUN=1 ~/.local/bin/herdres sync` exits ok.
     6. **Rollback on ANY failure** in steps 3–5: restore files from the backup dir, restart services, raise a `BridgeError` describing what failed.
   - **`--rollback`:** restore the most recent `backups/<ts>/` set + restart services.
   - **`--dry-run`:** print the plan (source, current→target version, files, service actions) without changing anything.
   - Reuse `BridgeError` (341), `DEFAULT_ENV`/`DEFAULT_HERMES_ENV` (35), `subprocess` (stdlib). **Self-contained** — do NOT shell out to `install-user.sh`.

## Tests — `tests/test_update.py`

Mock `subprocess.run` (git / systemctl / launchctl) and use a tmp filesystem (monkeypatch the install dest roots). Cover:
- `version` returns `HERDRES_VERSION`.
- source resolution order (`--repo` / `HERDRES_SRC` / marker / not-found error).
- `--check` applies nothing (no file writes, no service calls).
- edge apply: backup created, code files replaced, **`herdres.env` untouched**, gateway restart issued as **disable→enable in that order**.
- a failure during replace → **rollback restores** the backup.
- `--rollback` restores the latest backup.

## Acceptance
- [ ] `herdres version` + `herdres update --check/--edge/--rollback/--dry-run` work.
- [ ] `herdres.env` never overwritten; gateway lease released on restart.
- [ ] `tests/test_update.py` + full `pytest tests/` green; `/code-review` clean.
