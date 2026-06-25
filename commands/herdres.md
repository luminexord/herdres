---
name: herdres
description: herdres status + help — is it installed, configured, and are its services healthy?
argument-hint: "[setup | sync | status]"
allowed-tools: Bash(command -v herdres:*), Bash(herdres status:*), Bash(*/herdres status:*)
---

You are the `/herdres` status-and-help command for **herdres** — the Telegram ⇄ Herdr pane bridge.

Routing (this command only runs the read-only `status` itself; the other two have their own dedicated commands so their safety rules and tool grants apply):
- `$ARGUMENTS` is `setup` → tell the user to run **`/herdres-setup`** (the credential wizard) and stop.
- `$ARGUMENTS` is `sync` → tell the user to run **`/herdres-sync`** (it mutates state / may post to Telegram) and stop.
- `$ARGUMENTS` is `status` or empty → run the status below.

Resolve the binary and run the read-only `status` (no Telegram traffic, no state writes):

```bash
HERDRES="$(command -v herdres || echo "$HOME/.local/bin/herdres")"
"$HERDRES" status
```

Parse the single JSON object it prints and report a one-screen summary:

- **Install** — `version`, `installed.env_present`, `installed.state_present`.
- **Config** — chat configured? (`config.chat_id_set`), `config.allowed_users_count`, `config.topics_per_agent`, `config.plugin_event_enabled`.
- **Services** — reconcile timer + inbound gateway running? (`services.timer_active`, `services.gateway_active`, `services.platform`).
- **State** — `counts` (panes / spaces / open_panes) and `health.last_preflight_ok_at`.

Routing & safety:
- If the binary is missing **or** `installed.env_present` is false, do **not** guess — tell the user herdres isn't installed/configured and point them at **`/herdres-setup`** (or the `herdres` skill for the full guided install).
- For any other `$ARGUMENTS` that isn't `setup`/`sync`/`status`, defer to the `herdres` skill rather than inventing CLI flags.
- Never print secrets; the JSON already exposes only booleans/counts.
