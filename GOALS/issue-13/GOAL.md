# GOAL — Issue #13 `herdres update` (Phase 1+2)

Master goal for [luminexord/herdres#13](https://github.com/luminexord/herdres/issues/13). Implements **Phase 1+2** only — `version` + env-safe installers + `herdres update --edge`. Phase 3 (release CI + stable channel) is a tracked follow-up, **out of scope here**.

## Objective

A one-command, **env-safe** self-update so operators stop hand-copying files. `herdres update --edge` pulls `main`, backs up, atomically replaces the code (preserving `herdres.env`), restarts the services correctly (incl. the gateway getUpdates-lease release), and rolls back on failure.

| Task | Branch | Plan | Files |
|---|---|---|---|
| 1. update core | `feat/issue13-update-core` | [update-core.md](update-core.md) | `herdres.py`, `tests/test_update.py` |
| 2. env-safe installers | `feat/issue13-installers` | [installers.md](installers.md) | `install-user.sh`, `install-macos.sh`, `tests/test_installers.py` |
| 3. docs | `feat/issue13-docs` | [docs.md](docs.md) | `README.md`, `AGENTS.md` |

## Cross-task contract (so the tasks stay independent)

**Source marker** — where the local checkout lives, so `update --edge` knows where to `git pull`:
- **Task 2 (installers)** writes the absolute checkout path to the file `~/.local/share/herdres/source`.
- **Task 1 (update)** resolves the source as: `--repo <path>` > `HERDRES_SRC` env > the `~/.local/share/herdres/source` marker > a clear error.

Both tasks must use **exactly** `~/.local/share/herdres/source` and the env name `HERDRES_SRC`.

## Ground rules (all tasks)

- **stdlib-only** Python; match existing herdres style (`from __future__ import annotations`, PEP 604 unions, atomic temp+`os.replace` writes).
- Each task on its own branch + worktree off `main`; **no cross-task file edits**.
- **Never overwrite `herdres.env`.**
- Each task: implement → its targeted tests green → **`/code-review` clean** → its own PR to `main`.

## Status
- [ ] Task 1 — update core
- [ ] Task 2 — installers
- [ ] Task 3 — docs
