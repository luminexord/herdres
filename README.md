# Herdres

Rich Telegram forum-topic visibility and control for Herdr panes.

Herdres is a small stdlib-only Python bridge that maps each live Herdr pane to a Telegram forum topic. It can post explicit rich-message reports/questions/choices today, and it can switch to structured turn delivery when Herdr exposes a safe last-turn endpoint.

It does not patch Hermes or Herdr core files and routine sync uses no LLM calls.

## What It Does

- Creates or maintains one Telegram forum topic per Herdr pane.
- Keeps the General topic free for your normal Hermes chat.
- Sends pane updates with Telegram Bot API 10.1 `sendRichMessage`.
- Uses `editMessageText(rich_message=...)` for a quiet live status card.
- Shows clean reports, questions, blockers, and numbered choices.
- Optionally shows only the last submitted user instruction plus the final assistant reply when `herdr pane turn` is available.
- Keeps raw transcript and technical metadata behind explicit commands.
- Routes `/send`, `/keys`, and choice-button replies only to the mapped pane.

## Requirements

- Python 3.11+
- `herdr` available on `PATH`, or set `HERDR_BIN`
- Telegram bot token
- A Telegram supergroup with forum topics enabled
- Bot must be an admin with **Manage Topics**

## Quick Start

```bash
git clone https://github.com/gaijinjoe/herdres.git
cd herdres

install -Dm755 herdres.py ~/.local/bin/herdres
install -Dm644 .env.example ~/.config/herdres/herdres.env
$EDITOR ~/.config/herdres/herdres.env
```

Run a safe dry-run first:

```bash
HERDR_TELEGRAM_TOPICS_DRY_RUN=1 ~/.local/bin/herdres sync
```

Run once for real:

```bash
~/.local/bin/herdres sync
```

Install the user timer:

```bash
mkdir -p ~/.config/systemd/user
cp systemd/user/herdres.* ~/.config/systemd/user/
systemctl --user daemon-reload
systemctl --user enable --now herdres.timer
```

## Required Config

Edit `~/.config/herdres/herdres.env`:

```bash
TELEGRAM_BOT_TOKEN=
HERDR_TELEGRAM_TOPICS_CHAT_ID=-1001234567890
TELEGRAM_ALLOWED_USERS=123456789
```

Get your chat ID from Telegram bot updates or another admin tool. Owner user IDs are comma-separated.

## Pane Topic Commands

Inside a mapped pane topic:

- `/report` or `/status` - resend latest clean rich report
- `/choices` - resend active choices with buttons
- `/raw [lines]` - show sanitized raw visible output
- `/debug` - show technical mapping details
- `/send <text>` - send instruction to this pane
- `/keys <keys>` - send explicit keys to this pane

Plain text is not forwarded unless `telegram.implicit_send_enabled` is enabled in state. When enabled, plain text from an authorized owner in a mapped pane topic is sent only to that mapped pane. The General topic remains normal Hermes chat.

Inbound pane-topic control is handled through the Hermes Telegram gateway, so Hermes must load the small bridge hook:

```bash
install -Dm644 herdr_topic_bridge.py ~/.local/share/herdr-telegram-topics/herdr_topic_bridge.py
install -Dm755 herdr_telegram_topics_install_bridge.py ~/.local/bin/herdr_telegram_topics_install_bridge.py
mkdir -p ~/.config/systemd/user/hermes-gateway.service.d
cat > ~/.config/systemd/user/hermes-gateway.service.d/herdr-telegram-topics.conf <<'EOF'
[Service]
ExecStartPre=-%h/.local/bin/herdr_telegram_topics_install_bridge.py --quiet
EOF
systemctl --user daemon-reload
systemctl --user restart hermes-gateway.service
```

## Rich Message Behavior

Herdres tries rich delivery first:

- `sendRichMessage` for reports, choices, notices, and detail prompts
- `editMessageText` with `rich_message` for live cards
- `reply_parameters` for rich replies and ForceReply anchors
- `reply_markup` for inline choice buttons and ForceReply

Fallback policy:

- Missing rich endpoint: latch rich off and fall back to `sendMessage`
- Bad rich HTML: fall back once to `sendMessage`
- Transient/network error: do not resend, to avoid duplicate posts
- Live-card edits retry naturally on the next timer tick

## Structured Turn Feed

The cleanest mode is `HERDR_TELEGRAM_TOPICS_TURN_FEED=1`. In this mode Herdres does not infer reports, questions, or updates from terminal text. It calls:

```bash
herdr pane turn <pane_id> --last --format json
```

Expected Herdr response:

```json
{
  "available": true,
  "pane_id": "pane-1",
  "agent_session_id": "session-1",
  "turn_id": "turn-1",
  "complete": true,
  "user_text": "Diagnose why the bot froze.",
  "assistant_final_text": "Likely cause...\n\nWhat I did..."
}
```

`user_text` must be a submitted user message, never the visible input composer. `assistant_final_text` must be final assistant output only, without thinking, tool calls, shell output, or TUI chrome.

If Herdr cannot provide structured turn data, it should return:

```json
{"available": false, "reason": "no_structured_turn_source"}
```

When turn feed is enabled and the endpoint is unavailable, incomplete, or empty, Herdres sends nothing and does not fall back to `pane read`. This keeps Herdr upgrade-safe: Herdres consumes an optional upstream CLI contract and does not require local Herdr patches.

For local deployments that need this before Herdr exposes the endpoint upstream, Herdres includes `herdr_turn_adapter.py`. It is a wrapper, not a Herdr patch:

```bash
install -Dm755 herdr_turn_adapter.py ~/.local/bin/herdr_turn_adapter.py
```

Configure only the Herdres service to use it:

```bash
HERDR_BIN=/home/smith/.local/bin/herdr_turn_adapter.py
HERDR_REAL_BIN=/home/smith/.local/bin/herdr
HERDR_TELEGRAM_TOPICS_TURN_FEED=1
```

The wrapper implements only `herdr pane turn <pane_id> --last --format json`. Every other command is delegated to `HERDR_REAL_BIN`. Current local extraction supports Codex session IDs reported by Herdr, and Claude when Herdr reports a Claude `agent_session_id`. If a pane has no session id, the wrapper returns `available=false` and Herdres sends nothing.

## Clean Report Markers

By default, automatic sync posts only bounded reports, real choice prompts, actionable questions, and blocked/error items. It does not auto-post unbounded `Summary:`, `Final:`, `Verification:`, or `What changed:` transcript text unless `HERDR_TELEGRAM_TOPICS_UNBOUNDED_REPORTS=1` is set.

For the cleanest pane output, have the pane emit an explicit bounded report:

```text
HERDRES_REPORT_START
HERDRES_REPORT_TITLE: Deployment
What changed:
- Added cached Docker stats collection.
- Reduced timer overhead.
Verification:
- Timer run succeeded.
- Cache path confirmed.
HERDRES_REPORT_END
```

`HERDRES_REPORT_TITLE:` is optional, but recommended. Without it, the first report line must be a short title such as `Deployment`, `Flight Recorder`, or `What changed:`. Malformed bounded reports are ignored instead of being posted as noisy Telegram updates.

Bounded reports can also use structured sections:

```text
HERDRES_REPORT_START
HERDRES_REPORT_TITLE: Sprint Status
SUMMARY:
Driver App release is done and Route Optimizer is blocked.
TABLE:
Task | Owner | Status
Driver App release | Alex | Done
Route optimizer | Luke | Blocked
CHECKLIST:
[x] Review PR
[ ] Run staging smoke test
DETAILS: Risks
- Route Optimizer dependency is blocking release.
FOOTER:
Sprint - Smith - 10:58
HERDRES_REPORT_END
```

These render as Telegram rich headings, paragraphs, bordered/striped tables, checklists, collapsible details, and footers when rich messages are available. Content inside bounded reports bypasses the global TUI-noise filters; it is still sanitized for secrets and control characters.

Section aliases are accepted: `SHORT SUMMARY:` for `SUMMARY:`, `STATUS:` or `METRICS:` for `TABLE:`, `NEXT:` for `CHECKLIST:`, `RISKS:`, `PROOF:`, `LOGS:`, `COMMANDS:`, and `DIFF:` for collapsed details, and `META:` for `FOOTER:`.

For explicit choice buttons without relying on nearby question text:

```text
HERDRES_CHOICES_START
Question:
Choose the next action.
1. Run sync now
2. Show planned changes
HERDRES_CHOICES_END
```

## Useful Environment Variables

```bash
HERDR_BIN=herdr
HERDR_REAL_BIN=/home/smith/.local/bin/herdr
HERDR_TELEGRAM_TOPICS_STATE=~/.local/share/herdres/state.json
HERDR_TELEGRAM_TOPICS_LOCK=~/.local/share/herdres/sync.lock
HERDR_TELEGRAM_TOPICS_GENERAL_THREAD_ID=1
HERDR_TELEGRAM_TOPICS_MAX_CREATES=3
HERDR_TELEGRAM_TOPICS_MAX_SENDS=8
HERDR_TELEGRAM_TOPICS_FEED_READ_LINES=140
HERDR_TELEGRAM_TOPICS_FEED_MAX_CHARS=9000
HERDR_TELEGRAM_TOPICS_TURN_FEED=0
HERDR_TELEGRAM_TOPICS_FINAL_REPLY_MAX_CHARS=9000
HERDR_TELEGRAM_TOPICS_FINAL_REPLY_MAX_LINES=140
HERDR_TELEGRAM_TOPICS_USER_PROMPT_MAX_CHARS=1200
HERDR_TELEGRAM_TOPICS_RICH_MESSAGES=1
HERDR_TELEGRAM_TOPICS_LIVE_CARD=1
HERDR_TELEGRAM_TOPICS_UNBOUNDED_REPORTS=0
HERDR_TELEGRAM_TOPICS_DRY_RUN=0
```

## Probe

To verify rich delivery against a scratch topic:

```bash
~/.local/bin/herdres probe --thread-id 123
```

The probe sends a rich message and deletes it if possible.

## Security Notes

- Bot token is read from environment or `~/.config/herdres/herdres.env`.
- Secrets are redacted from raw output and errors before posting.
- State stores pane/topic mapping and hashes, not bot tokens.
- Raw pane output is only posted via explicit `/raw`.
