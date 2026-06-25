---
name: herdres-setup
description: Run the herdres credential setup wizard (bot token, chat id, allowed users).
allowed-tools: Bash(command -v herdres:*), Bash(herdres setup:*), Bash(*/herdres setup:*)
---

You are the `/herdres-setup` command — launch the **herdres** credential wizard.

```bash
HERDRES="$(command -v herdres || echo "$HOME/.local/bin/herdres")"
"$HERDRES" setup
```

`herdres setup` prompts on a TTY for the bot token (no echo), chat id, and allowed users, validates them, runs a preflight, and writes `~/.config/herdres/herdres.env` at mode 0600.

Hard rules:
- **Never invent, guess, or scavenge credentials.** Do not reuse a Hermes token unless the user explicitly asks (`--reuse-hermes-token`). If you don't have a value, stop and ask the user — do not pass flags blindly.
- If the environment is **non-interactive** (no TTY), do not try to fake input — report exactly what the wizard needs (token / chat id / allowed users) and how to obtain it.
- On success, surface `result.env_path`, `result.chat_id`, `result.allowed_users`, and the preflight outcome from the JSON. **Never echo the bot token.**
- For the full guided walkthrough (BotFather, creating the forum supergroup, adding the bot as admin, finding the chat/user ids), defer to the `herdres` skill — this command just launches the wizard and reports.
