# Topic Granularity & the Forum-Topic Model

How herdres maps Herdr panes onto Telegram forum topics, when to use one topic per agent versus one per space, how to switch safely, the multi-pane reply rule, and what each topic status icon means.

For commands, see COMMANDS.md. For install/enablement, see SETUP.md.

---

## The forum-topic model in one paragraph

Herdres mirrors Herdr activity into a Telegram **supergroup with forum topics enabled**. Each tracked unit (a space or an individual agent pane, depending on the mode) gets its own **forum topic** — a separate thread inside the group. Pane output lands in that topic; you reply **inside** a topic to control the pane it represents. The group's **General** topic is left alone for normal chat. The supergroup must have topics enabled and the bot must be an admin with **Manage Topics** rights so Herdres can create topics and edit their status icons.

---

## Two grouping modes

Topic granularity is controlled by one env var, `HERDR_TELEGRAM_TOPICS_PER_AGENT`.

| Mode | Flag | What you get | Topic title |
|---|---|---|---|
| **Per-space** (default) | `HERDR_TELEGRAM_TOPICS_PER_AGENT=0` | One topic per Herdr **space/workspace**; every agent in that space shares the thread | The space/workspace name (or cwd folder fallback) |
| **Per-agent** (recommended) | `HERDR_TELEGRAM_TOPICS_PER_AGENT=1` | One topic per Herdr **agent (pane)** | `<agent> · <folder>`, e.g. `codex · sawa` — or a manual pane label if one is set |

Per-agent topic titles are derived as `<agent> · <folder>` where `<agent>` is the agent type lowercased (e.g. `codex`, `claude`) and `<folder>` is the pane's working-directory basename. A **manual pane label** (set in Herdr) is treated as explicit operator intent and overrides the generated name.

### Lead with per-agent

**Recommend per-agent (`=1`) by default.** One topic per pane gives each agent its own clean thread, its own status icon, and its own unambiguous reply target — so a plain-text reply or `/send` always reaches exactly one pane with no disambiguation step. Per-space collapses multiple agents into one thread, which forces the fail-closed disambiguation rule below whenever a space has more than one live pane.

Use per-space only when an operator specifically wants every agent in a workspace consolidated into a single thread.

---

## The flag is read at runtime

`HERDR_TELEGRAM_TOPICS_PER_AGENT` is read **at call time**, not frozen at import. This matters because herdres runs in three contexts and only some inject env before Python starts:

| Context | How env arrives |
|---|---|
| systemd timer (`herdres sync`) | env preloaded via `EnvironmentFile` |
| Herdr plugin (`herdres event`) | **no** `EnvironmentFile` — reads `herdres.env` via `load_dotenv()` |
| bare CLI | inherited shell env |

Every entry point calls `load_dotenv()` before touching grouping logic, then checks the flag. If the value were captured as an import-time constant, the Herdr plugin path would read `0` and silently collapse all agents back onto one topic. Because it is read at runtime, all three contexts agree on the mode. **Practical consequence:** set the flag in `~/.config/herdres/herdres.env` (the file all three contexts load), not just in a shell or a single service unit.

---

## Switching modes is a clean slate

Flipping `HERDR_TELEGRAM_TOPICS_PER_AGENT` triggers a one-time **clean-slate reset** on the next topic-creating run:

- Herdres **forgets** every old space→topic and pane→topic mapping (and the per-space pinned-status / route state tied to them).
- It then **creates fresh topics** for the new grouping.
- **Old Telegram topics are left in place** — Herdres does not delete them. Delete the stale ones manually in Telegram if you want a tidy group.

The reset is reconciled on **every** path that can create or adopt topics (both `herdres sync` and the plugin's `herdres event`), so a mode flip is honored no matter which path observes it first — it can't half-apply or keep cross-wiring topics until the next sync.

```text
PER_AGENT=0 -> 1   reset + recreate (fresh per-agent topics)
PER_AGENT=1 -> 0   reset + recreate (fresh per-space topics)
no change          existing topics reused
```

**Operator flow to switch modes:**

1. Set `HERDR_TELEGRAM_TOPICS_PER_AGENT=1` (or `0`) in `~/.config/herdres/herdres.env`.
2. Run `herdres sync` (or let the timer / a plugin event fire). The reset and recreation happen automatically.
3. Manually delete the now-orphaned old topics in Telegram if desired.

---

## Backfilling many topics at once

Topic creation is rate-limited so a single run never spams the group. `HERDR_TELEGRAM_TOPICS_MAX_CREATES` caps how many **new** topics are created per run (**default `3`**). After a clean-slate reset with many panes, only a few topics appear per sync until the backlog clears.

To create all topics in one shot, raise the cap for that single run:

```bash
HERDR_TELEGRAM_TOPICS_MAX_CREATES=20 herdres sync
```

Set it high enough to cover the number of panes you expect topics for. Leave the env-file default low (`3`) for steady-state runs so routine sync stays quiet.

---

## The multi-pane fail-closed rule

Herdres routes a reply to a pane **only when the target pane is unambiguous**. This is a load-bearing safety rule (see SAFETY.md).

- **Reply inside a pane's topic/thread** (or to a routed pane message / pane-root card) and the text goes to that exact pane — no `/send` prefix needed.
- In **per-agent mode**, every topic maps to exactly one pane, so any reply in that topic is unambiguous.
- In **per-space mode**, a top-level message in a shared space topic is forwarded **only** when that topic has **exactly one live pane**. If the topic could match **more than one** pane, Herdres **fails closed** and replies:

  > `Reply inside a pane thread so I know which Herdr pane to control.`

**To control a specific pane in a multi-pane space topic:** reply directly under that pane's routed message, or use `/send <text>` as a reply to it. Never assume a top-level message in a multi-pane topic reached a pane. This per-pane ambiguity is the main reason to lead with per-agent mode.

---

## Topic status icons

Telegram bots cannot recolor a forum topic, but they **can** edit the topic's icon (`icon_custom_emoji_id`). Herdres uses that as a low-noise, per-topic status surface. Icon edits happen only when a pane's status **changes**, so routine sync spends no extra messages on status display.

| State | Default icon | Meaning |
|---|---|---|
| working | ⚡️ | agent is running a turn |
| idle | ☕️ | idle, no active work |
| done | ✅ | turn completed |
| blocked | ❗️ | blocked / waiting on input |
| error | ‼️ | error state |
| workflow | 📈 | working with active workflow metadata |
| idle + active goal | 🧠 | idle **but still pursuing a `/goal`** |
| unknown | ❓ | status not determined |

### The idle-with-active-goal brain icon (🧠)

An idle pane that is **still pursuing a goal** reads as "on a goal" (🧠), not as a coffee break (☕️). Herdres detects this by scanning the pane's footer for an active-goal marker (the literal `/goal active`, e.g. `◎ /goal active (3h)`). The check runs **only for idle panes** and is read once per pane per sync. "Goal achieved" / done deliberately does **not** match — only a still-active goal flips an idle topic to 🧠. The same 🧠 also appears in the optional pinned all-panes status overview.

### Configuring icons

Icons are enabled by default (`HERDR_TELEGRAM_TOPICS_STATUS_ICON=1`). Herdres resolves each icon in two steps:

1. If an explicit custom-emoji ID env var is set, use it.
2. Otherwise call `getForumTopicIconStickers`, cache the result, and match by the configured `*_EMOJI` value.

If Telegram offers no matching forum icon sticker, the topic icon is left unchanged.

| Env var | Default | Purpose |
|---|---|---|
| `HERDR_TELEGRAM_TOPICS_STATUS_ICON` | `1` | Master switch for icon updates |
| `HERDR_TELEGRAM_TOPICS_STATUS_ICON_CACHE_TTL` | `86400` | Seconds to cache the fetched icon-sticker set |
| `HERDR_TELEGRAM_TOPICS_STATUS_ICON_RETRY` | `300` | Retry backoff after a failed icon lookup |
| `HERDR_TELEGRAM_TOPICS_STATUS_ICON_<STATE>` | _(unset)_ | Explicit custom-emoji ID for a state (`WORKING`, `IDLE`, `DONE`, `BLOCKED`, `ERROR`, `WORKFLOW`, `UNKNOWN`) |
| `HERDR_TELEGRAM_TOPICS_STATUS_ICON_<STATE>_EMOJI` | see table above | Emoji matched against Telegram's allowed forum-icon set |

The emoji defaults are: `WORKING=⚡️`, `IDLE=☕️`, `DONE=✅`, `BLOCKED=❗️`, `ERROR=‼️`, `WORKFLOW=📈`, `UNKNOWN=❓`.

### Status markers (opt-in fallback)

Telegram never moves an edited message to the bottom of a topic, so the icon edit is invisible if you're scrolled away. As a fallback, Herdres can post a compact **status marker** message at the bottom of the topic (e.g. `🟡 Working` / `🟢 Idle`). It is **off by default** and is meant only as a breadcrumb when the icon surface isn't enough:

| Env var | Default | Purpose |
|---|---|---|
| `HERDR_TELEGRAM_TOPICS_STATUS_MARKER` | `0` | Post a compact bottom-of-topic status marker |
| `HERDR_TELEGRAM_TOPICS_STATUS_MARKER_DELETE_OLD` | `1` | Delete the previous marker after posting the new one |
| `HERDR_TELEGRAM_TOPICS_STATUS_MARKER_SUPPRESS_WHEN_ICON_OK` | `1` | Skip the marker when the icon already conveys status |

Final replies, stream drafts, and decision cards always take priority over markers. A marker is sent only when the compact status changes and nothing richer was delivered for that pane in the run.

---

## Related per-topic options

| Env var | Default | Purpose |
|---|---|---|
| `HERDR_TELEGRAM_TOPICS_PANE_ROOT_MESSAGES` | `0` | Maintain a stable per-pane **root card** you can always reply to (older thread-style UI) |
| `HERDR_TELEGRAM_TOPICS_INCLUDE_SHELLS` | `0` | Also map panes that have **no** agent (plain shells) |
| `HERDR_TELEGRAM_TOPICS_GENERAL_THREAD_ID` | `1` | Thread id treated as General (left for normal chat, never a pane target) |

`HERDR_TELEGRAM_TOPICS_PANE_ROOT_MESSAGES=1` is useful in **per-space** mode: it gives each pane a stable anchor message to reply to even when several panes share one space topic, reducing reliance on replying to a fresh routed message. In per-agent mode it is rarely needed, since the topic itself is the pane.

---

## Quick recipe

```bash
# Recommended: one topic per agent, set in the env file all contexts load
# (in ~/.config/herdres/herdres.env)
HERDR_TELEGRAM_TOPICS_PER_AGENT=1

# Apply: clean-slate reset + create fresh per-agent topics, backfilling many at once
HERDR_TELEGRAM_TOPICS_MAX_CREATES=20 herdres sync

# Then delete the orphaned old topics in Telegram by hand.
```
