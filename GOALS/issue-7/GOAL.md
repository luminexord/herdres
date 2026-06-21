# GOAL — Issue #7 follow-ups

Master goal file for [luminexord/herdres#7](https://github.com/luminexord/herdres/issues/7). The pre-merge trio (spec frontmatter, CI validation, MUST credential gate) already shipped in PR #5. This goal covers the **three remaining follow-ups**, each built independently and opened as its own PR.

## Objective

Make herdres setup **safe-by-construction** and the skill **distributable**, by implementing:

| Task | Branch | Plan | PR target |
|---|---|---|---|
| 1. `herdres setup` credential wizard | `feat/issue7-wizard` | [wizard.md](wizard.md) | `skill-herdres-operator` |
| 2. Behavioral evals | `feat/issue7-evals` | [evals.md](evals.md) | `skill-herdres-operator` |
| 3. Marketplace packaging | `feat/issue7-marketplace` | [marketplace.md](marketplace.md) | `skill-herdres-operator` |

## Ground rules (all tasks)

- **stdlib-only** Python (no new deps); match the existing herdres style.
- Each task on its **own branch + git worktree** off `skill-herdres-operator`; **no cross-task file edits**.
- **Never scavenge credentials** — the wizard is the enforcement; the others must not weaken it.
- Each task: implement → its targeted tests green → **`/code-review` clean** → rebase on base → **full `pytest tests/` green** → its own PR.
- Branches stack on PR #5; after #5 merges to `main`, rebase + retarget PRs to `main`.

## Status

- [ ] Task 1 — wizard
- [ ] Task 2 — evals
- [ ] Task 3 — marketplace
