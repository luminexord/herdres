---
name: herdres-sync
description: Force one herdres reconcile pass (sync panes/turns to Telegram) and report what changed.
allowed-tools: Bash(command -v herdres:*), Bash(herdres sync:*), Bash(*/herdres sync:*)
---

You are the `/herdres-sync` command — run one **herdres** reconciliation pass.

```bash
HERDRES="$(command -v herdres || echo "$HOME/.local/bin/herdres")"
"$HERDRES" sync
```

Parse the JSON counts and report what changed: `panes`, `created`, `verified`, `renamed`, `sent`, `icon_updated`, `pinned_status_updated`. If it early-outs (`disabled`, or no agent panes), say so plainly.

Notes & safety:
- This **mutates** state and may post to Telegram. Run it **once** — do **not** tight-loop sync on a transient failure.
- For a non-destructive preflight, you can run `HERDR_TELEGRAM_TOPICS_DRY_RUN=1 "$HERDRES" sync` (no Telegram writes).
- `sync` returns counts only, not the current roster — for the panes/topics list, point the user at **`/herdres-status`**.
- If the binary is missing or herdres isn't configured, stop and point at **`/herdres-setup`** rather than guessing.
