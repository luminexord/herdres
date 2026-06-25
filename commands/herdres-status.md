---
name: herdres-status
description: Show the current herdres panes/topics roster and install/service health (read-only).
allowed-tools: Bash(command -v herdres:*), Bash(herdres status:*), Bash(*/herdres status:*)
---

You are the `/herdres-status` command — show the **herdres** roster and health (read-only; no Telegram traffic, no state writes).

```bash
HERDRES="$(command -v herdres || echo "$HOME/.local/bin/herdres")"
"$HERDRES" status
```

Parse the JSON and render:
- **Roster** — `panes[]` (pane_id, agent, status, topic_id) and `spaces[]` (space_key, topic_id, topic_name, pane_count). Group or sort sensibly; highlight non-`closed` panes.
- **Counts** — `counts.panes`, `counts.spaces`, `counts.open_panes`.
- **Health** — `installed.env_present`/`state_present`, `services.timer_active`/`gateway_active`, `health.last_preflight_ok_at`.

Distinguish the cases:
- `installed.state_present` is false → **never synced** (fresh install) — suggest `/herdres-sync`.
- state present but `counts.open_panes` is 0 → synced but no live panes right now.
- binary missing or `installed.env_present` false → not installed/configured — point at **`/herdres-setup`**.

Never print secrets; the JSON exposes only booleans/counts.
