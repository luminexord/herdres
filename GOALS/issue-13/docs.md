# Task 3 — docs

**Branch:** `feat/issue13-docs` (off `main`)
**Files:** `README.md`, `AGENTS.md` only.

## `README.md` — add an "Updating herdres" section

Place it near the install sections (after "Quick Start" / "Install By Name", or after "Required Config"). Cover:

- **`herdres update`** — updates from the local checkout (`--edge`, the default for now): `git pull`, backs up the current install, atomically replaces the code **preserving your `herdres.env`**, restarts the timer + gateway (releasing the gateway's single `getUpdates` lease), and **rolls back on failure**.
- **`herdres update --check`** — show current vs available version, change nothing.
- **`herdres update --rollback`** — restore the previous install.
- **`herdres version`** — print the installed version.
- A short note: it reads the source checkout from the `~/.local/share/herdres/source` marker the installer writes (or `HERDRES_SRC` / `--repo`).
- One line: **versioned `stable` releases are coming** (Phase 3 of [#13](https://github.com/luminexord/herdres/issues/13)); today's path is `--edge` (track `main`).

Keep it concise and copy-pasteable, matching the existing README tone. Use real flag names only (from `GOALS/issue-13/update-core.md`).

## `AGENTS.md` — one-line note

In the repo-layout / commands area, note the new subcommands: `version` (prints `HERDRES_VERSION`) and `update` (env-safe self-update; `--edge`/`--check`/`--rollback`).

## Acceptance
- [ ] README has a clear "Updating herdres" section with the real commands/flags.
- [ ] AGENTS.md notes the `version`/`update` subcommands.
- [ ] No code touched; full `pytest tests/` still green; `/code-review` clean.
