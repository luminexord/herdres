# Managed Pane Bots & "One Space, One Voice"

By **default a space speaks with one voice**: a single **manager bot** posts for every agent in that space, and you address a specific agent by **replying to its message** or with the **`/agents`** picker (per-space topic mode only). Giving each agent type its own Telegram bot identity — so traffic shows up as "Herdr Codex", "Herdr Claude", and so on — is an **opt-in, per-space, reversible upgrade**. This reference covers the voice model, enabling the upgrade, the setup handshake, token storage, group access, gateway workers, and optional profile photos.

For general install and service wiring see SETUP.md. For the command list see COMMANDS.md. For the topic model see TOPICS.md.

## One Space, One Voice (the default)

A **space** (a Herdr workspace, mapped to one forum topic in per-space mode) can hold multiple agents. By default they all share the **manager bot** voice.

- **`voice_mode` is per-space**, stored on the space entry (`space["voice_mode"]`) and **persists across state resets** (including a per-agent⇄per-space topic-mode flip, via `_preserved_voice_mode`).
  - `"shared"` (**default**) — one manager bot speaks for the whole space.
  - `"per_agent"` (opt-in) — each agent **type** gets its own managed child bot, when one is available.
- **`/voice shared|per_agent`** is the reversible per-space setter (`/voice` with no arg prints the current mode). It works wherever the topic resolves to a space (including per-agent topic mode); it only no-ops when no space record can be resolved.
- **`/agents`** (per-space mode only) shows an inline picker; tapping an agent sets a TTL'd per-user **active pane** (~600s, `HERDR_TELEGRAM_TOPICS_ACTIVE_PANE_TTL`) so subsequent commands in that topic route to it without a reply/@. Reply-to-message and a single live pane still take priority. In per-agent mode `/agents` replies `Only one agent here…`.
- **`managed_voice_active`** on each pane entry is the **single source of truth** for whether that pane sends via a child bot. `refresh_entry_managed_voice` (run during sync and command handling) reconciles it from `space["voice_mode"]`, **overriding** the env flag. `managed_bot_token_for_entry` reads `managed_voice_active` if present, else falls back to `MANAGED_BOTS_ENABLED`.
- **Auto-migration:** existing deployments with a live child-bot token registered for a pane are migrated to `voice_mode="per_agent"` on first load (`migrate_space_voice_mode`), preserving multibot behavior so the `1→0` default flip never silently downgrades them. The migration is idempotent and only touches spaces lacking a `voice_mode` that have a live child bot.

## The per-agent upgrade offer (increment 2)

When managed bots are enabled (`HERDR_TELEGRAM_TOPICS_MANAGED_BOTS=1`), Herdres posts a **lazy, dismissible** "Give each agent its own bot (optional)" card **once per space** in that space's topic. It is emitted only when **all** hold:

| Condition | Required value |
|---|---|
| Managed bots enabled | `HERDR_TELEGRAM_TOPICS_MANAGED_BOTS=1` |
| Manager bot can manage bots | `can_manage_bots = true` |
| Topic mode | per-space (`HERDR_TELEGRAM_TOPICS_PER_AGENT=0`) |
| Space voice mode | not already `per_agent` |
| Not dismissed | `multibot_offer_dismissed` unset |
| Multi-kind space | **≥ 2** distinct agent kinds with open panes |
| No recent send error | older than `HERDR_TELEGRAM_TOPICS_MULTIBOT_OFFER_RETRY` (default 3600s) |

- **Dismiss** sets `multibot_offer_dismissed=true` and clears the signal — the card never returns for that space.
- **Upgrade** (`herdr:mb:<space>:up` callback) sets `voice_mode="per_agent"`, refreshes every entry in the space, and shows BotFather links **scoped to that space's agent kinds**. From there it is the same child-bot setup handshake below.
- You can always switch back later with `/voice shared`.

`MULTIBOT_OFFER_RETRY_SECONDS` is a cooldown before re-attempting a failed offer send, so an undeliverable space (deleted topic, kicked bot) does not burn a send slot every sync.

## What managed bots are

- The **manager bot** is the single `TELEGRAM_BOT_TOKEN` that runs Herdres. It always exists and is the only voice in `shared` mode.
- A **managed (child) bot** is a separate Telegram bot, one per agent type, created through Telegram's managed-bot API and owned by your manager bot account. It is used only when the space is in `per_agent` voice mode.
- Supported agent types: **codex, claude, kimi, omp, devin**. Each maps to a fixed identity:

  | Type | Display name | Suggested username |
  |---|---|---|
  | codex | Herdr Codex | `herdr_codex_bot` |
  | claude | Herdr Claude | `herdr_claude_bot` |
  | kimi | Herdr Kimi | `herdr_kimi_bot` |
  | omp | Herdr OMP | `herdr_omp_bot` |
  | devin | Herdr Devin | `herdr_devin_bot` |

  Herdres matches a pane's agent to a type by alias (for example `gpt`/`openai` map to codex, `anthropic` maps to claude, `moonshot` maps to kimi, `cognition` maps to devin), so the right child bot is chosen even when Herdr labels the agent differently.
- When a child bot is configured and allowed to post in the forum group, pane output for that agent type is sent by the matching child bot. Otherwise Herdres falls back to the manager bot for that send.

## Enable

Managed bots are **opt-in**. Set in `~/.config/herdres/herdres.env`:

```bash
HERDR_TELEGRAM_TOPICS_MANAGED_BOTS=1
```

The **code default when the variable is unset is `0`** (off). **But the shipped `.env.example` sets `HERDR_TELEGRAM_TOPICS_MANAGED_BOTS=1`** and the installers copy it verbatim, so a **standard install has the feature ON** — the per-agent "give each agent its own bot" offer card can appear once a space has ≥2 agent kinds. Set it to `0` in your `herdres.env` for the pure shared-voice model with no offers. The flag only gates the *feature*: which spaces actually use child bots is decided per-space by `voice_mode` / each entry's `managed_voice_active`, so you can leave `=1` and still keep most spaces on the shared voice. Existing deployments with live child bots are auto-migrated to `per_agent` on first load regardless of the flag.

## How Herdres suggests child bots

Herdres only suggests creating child bots for agent types that **currently have an open pane** and **do not already have a stored child-bot token**.

On a normal `sync`, when both conditions hold for one or more agent types, Herdres posts a single "Managed pane bots" notice **in the General topic** with one inline button per missing type. Each button is a `https://t.me/newbot/...` deep link pre-filled with the suggested name and username for that type.

Conditions and behavior:

| Condition | Behavior |
|---|---|
| `HERDR_TELEGRAM_TOPICS_MANAGED_BOTS=0` | Feature off — no offer card, no setup message, no child-bot sends. (Code default when *unset*; the shipped `.env.example` sets `=1`.) |
| Manager bot lacks `can_manage_bots` | No setup message (Herdres checks `getMe`; managed-bot creation is unavailable on that bot). |
| No open panes for any supported type | No setup message. |
| A type already has a stored token | That type is omitted from the suggestion. |
| Setup message already posted for the same set of missing types | Not re-posted. |

The notice is posted to General using `HERDR_TELEGRAM_TOPICS_GENERAL_THREAD_ID` (default `1`). Only the missing types are listed; you do not get prompted for agent types you are not running.

## Setup handshake (token capture)

1. Tap a "Create … bot" button in the General notice. Telegram walks you through creating the child bot under your account.
2. After creation, Telegram sends the **manager bot** a `managed_bot` update.
3. Herdres handles that update: it infers the agent type from the new bot, calls Telegram's `getManagedBotToken` for the new bot's user id, and retrieves the child token.
4. Herdres stores the child token and configures the child bot's profile (name, descriptions, optional photo).

If `getManagedBotToken` returns no token, Herdres records the error and does not store a partial record. The handshake is idempotent per type: the stored record is overwritten with fresh data on each `managed_bot` update for that type.

## Token storage

Child tokens are stored in Herdres state under `telegram.managed_bots`, keyed by agent type. Each record holds the type, bot id, username, display name, owning user id, the **token**, an `enabled` flag, the profile-configuration result, and an updated timestamp.

Important distinctions:

- This is the **one** place tokens are persisted. The state file (`HERDR_TELEGRAM_TOPICS_STATE`, default `~/.local/share/herdres/state.json`) therefore contains live bot tokens once child bots are registered. Treat it as a secret. Lock down its permissions and back it up carefully.
- The manager bot token is **not** stored here; it comes from the environment / `herdres.env`.
- To revoke a child bot, you can revoke it in Telegram and/or remove its record from `telegram.managed_bots`; Herdres skips records whose `enabled` is `false` or whose token is empty.

## Child bots must be in the forum group

Creating a child bot is not enough. Telegram only delivers messages from a bot that is a member of the forum group. After a child bot is created you must **add it to the same forum supergroup**.

If a child token is registered but Telegram rejects a pane send from it (a `bot_access` error), Herdres:

1. **Falls back to the manager bot** for that send, so the pane message is still delivered (just from the manager identity). The send is marked as a managed-bot fallback so Herdres knows the child still lacks access.
2. Posts an **"Add pane bots to this group"** notice in General, listing the affected types with add-to-group buttons (`https://t.me/<bot_username>?startgroup=...`).

Herdres retries the child bot periodically rather than every sync. The reissue/retry backoff defaults to 300 seconds (`HERDR_TELEGRAM_TOPICS_MANAGED_BOT_REISSUE_RETRY_SECONDS`). Once you add the bot to the group, the next eligible send routes from the child bot.

You do not need to do anything beyond adding the bot to the group. There is no separate "register access" command — Herdres detects access from successful sends.

## The standalone gateway: one worker per bot token

Inbound replies and button taps reach Herdres through the gateway (`herdres_gateway.py`). Telegram allows only **one** active `getUpdates` consumer per bot token, and each child bot is a distinct token. The gateway therefore runs **one long-poll worker per bot token**: one for the manager bot plus one for each registered child bot.

This isolation matters: each child bot's long poll, network error backoff, and reconnect are independent, so a wait or reconnect on one token never delays delivery on another. Replies a user sends to a specific child bot are picked up by that bot's own worker and dispatched to the correct pane.

The gateway discovers child tokens by re-reading `telegram.managed_bots` from state on a reconcile loop. When you finish the setup handshake for a new child bot, the gateway adds a worker for it automatically — no restart required. When a child record is removed or disabled, its worker is stopped.

Relevant gateway env vars (set in `herdres.env`):

| Var | Default | Meaning |
|---|---|---|
| `HERDRES_GATEWAY_LONG_POLL_SECONDS` | `50` | Long-poll timeout for the manager worker (and child workers when child timeout is 0). |
| `HERDRES_GATEWAY_CHILD_POLL_SECONDS` | `0` | Per-child long-poll timeout. `0` means use the manager value. |
| `HERDRES_GATEWAY_NETWORK_ERROR_BACKOFF` | `0.5` | Backoff after a network error before re-polling. |
| `HERDRES_GATEWAY_DISPATCH_WORKERS` | `8` | Worker-pool size that runs Herdr command handling, so slow processing does not block polling. |
| `HERDRES_GATEWAY_DISPATCH_QUEUE_LIMIT` | `128` | Max queued inbound updates. |
| `HERDRES_GATEWAY_RUNNER` | `embedded` | Inbound command runner. `embedded` avoids a Python cold start per update; set `subprocess` only to debug the older cold-process path. |

Run exactly one `getUpdates` consumer per token. Do not run both a Hermes poller and the standalone gateway on the same bot token. See SETUP.md for which inbound path to enable.

## Optional profile photos

Each child bot can get a custom profile photo. Telegram requires a **freshly uploaded static JPG file** for a bot profile photo — point at a real `.jpg` on disk, not a URL or a Telegram `file_id`.

Per-type env vars:

```bash
HERDR_TELEGRAM_TOPICS_MANAGED_BOT_CODEX_PHOTO=~/.config/herdres/managed-bots/codex.jpg
HERDR_TELEGRAM_TOPICS_MANAGED_BOT_CLAUDE_PHOTO=~/.config/herdres/managed-bots/claude.jpg
HERDR_TELEGRAM_TOPICS_MANAGED_BOT_KIMI_PHOTO=~/.config/herdres/managed-bots/kimi.jpg
HERDR_TELEGRAM_TOPICS_MANAGED_BOT_OMP_PHOTO=~/.config/herdres/managed-bots/omp.jpg
HERDR_TELEGRAM_TOPICS_MANAGED_BOT_DEVIN_PHOTO=~/.config/herdres/managed-bots/devin.jpg
```

Resolution and fallback:

- If the env var for a type is set, that path is used (`~` is expanded).
- If unset, Herdres looks for a default at `~/.config/herdres/managed-bots/<type>.jpg`. If that file exists it is used; otherwise no photo is set.
- The photo is uploaded as part of the setup handshake when the child token is captured. If the configured path does not exist, the profile is still configured (name and descriptions) and the photo step is recorded as missing/failed — it does not block child-bot registration.

Photos are cosmetic. Names and short/long descriptions are always set from the fixed per-type identity regardless of whether a photo is provided.

## Quick checklist

1. Ensure `HERDR_TELEGRAM_TOPICS_MANAGED_BOTS=1` (the shipped `.env.example` already sets this; the code default when *unset* is `0`) and (optionally) the `*_PHOTO` paths.
2. Make sure the manager bot can manage bots (Herdres skips setup links otherwise).
3. Open ≥ 2 agent kinds in a space, then run `sync` — the "Give each agent its own bot (optional)" card appears in that space's topic. Tap **Upgrade** (or run `/voice per_agent`) to flip the space to per-agent voice.
4. Tap the "Create … bot" buttons and complete Telegram's flow.
5. Add each new child bot to the forum group.
6. Confirm the gateway is running (it adds a worker per child token automatically).
7. Future pane output for that type is sent by its child bot; on any access rejection Herdres falls back to the manager bot and re-prompts you to add the bot to the group. Revert anytime with `/voice shared`.

## Gaps / verify against your deployment

- Adding a child bot to the group is a manual Telegram action; Herdres can only prompt and detect, not perform it.
- The state file holds live child tokens once registered — confirm its file permissions and backup handling meet your security needs (see SAFETY.md).
