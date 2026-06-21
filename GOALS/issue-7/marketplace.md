# Task 3 — Marketplace packaging

**Branch:** `feat/issue7-marketplace` (off `skill-herdres-operator`)
**Why:** make `herdres` installable **by name** from a marketplace (closes the #6 gap), not just by copying files. Schema mirrors gitmoot's ponytail manifests + the [official docs](https://code.claude.com/docs/en/plugin-marketplaces).

## Files (all new)

### `.claude-plugin/plugin.json`
```json
{
  "name": "herdres",
  "version": "0.1.0",
  "description": "Operate the Herdr-to-Telegram bridge: per-agent topics, send/interrupt/queue, status, managed bots, cockpit, maintenance.",
  "author": { "name": "jerryfane", "url": "https://github.com/jerryfane" }
}
```
Required: `name` (kebab-case, == skill name `herdres`), `version` (semver), `description`. `author` optional but include.

### `.codex-plugin/plugin.json`
Superset of the above plus:
```json
  "homepage": "https://github.com/luminexord/herdres",
  "repository": "https://github.com/luminexord/herdres",
  "license": "MIT",
  "keywords": ["telegram", "herdr", "bridge", "agents", "operations"],
  "skills": "./skills/",
  "interface": {
    "displayName": "Herdres",
    "shortDescription": "Operate Herdr agents over Telegram",
    "longDescription": "Install and operate the Herdr-to-Telegram bridge: set up per-agent forum topics, send/interrupt/queue messages to agent panes, read status and reports, manage child bots, drive the macOS cockpit, and run maintenance.",
    "developerName": "jerryfane",
    "category": "Productivity",
    "capabilities": ["Instructions"],
    "websiteURL": "https://github.com/luminexord/herdres",
    "defaultPrompt": ["Set up herdres so I can control my Herdr agents from Telegram."]
  }
```

### `.claude-plugin/marketplace.json`
```json
{
  "$schema": "https://anthropic.com/claude-code/marketplace.schema.json",
  "name": "herdres",
  "description": "Operate the Herdr-to-Telegram bridge from a skills-compatible agent.",
  "owner": { "name": "jerryfane", "url": "https://github.com/jerryfane" },
  "plugins": [
    { "name": "herdres", "description": "Herdres operator skill.", "source": ".", "category": "productivity" }
  ]
}
```
The plugin bundles the existing `skills/herdres/` (already present on the base branch). Hand-author the manifests — **no** build command.

## Tests — `tests/test_plugin_manifests.py`

- `.claude-plugin/plugin.json`, `.codex-plugin/plugin.json`, `.claude-plugin/marketplace.json` all parse as JSON.
- `plugin.json` has `name`/`version`/`description`; `name == "herdres"` and matches `^[a-z0-9]+(-[a-z0-9]+)*$`; `version` is semver.
- `.codex-plugin/plugin.json` has `skills == "./skills/"` and a non-empty `interface.displayName`.
- `marketplace.json` has `name`, `owner`, and a `plugins[0]` with `name`/`description`/`source`.
- The bundled skill resolves: `skills/herdres/SKILL.md` exists.

## Docs

- Add a README "Install by name (marketplace)" section: add the marketplace then install `herdres` (Claude Code + Codex), referencing #6. Keep it short.

## Acceptance

- [ ] Manifests valid + `tests/test_plugin_manifests.py` green; full `pytest tests/` green.
- [ ] `skills/herdres/` resolves from the plugin root.
- [ ] `/code-review` clean.
