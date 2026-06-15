# Herdres

Rich Telegram forum-topic visibility and control for Herdr panes.

Herdres is a small stdlib-only Python bridge that maps each live Herdr pane to a Telegram forum topic. It posts clean rich-message reports/questions/choices, keeps a quiet per-pane live status card, and routes owner replies back to the matching pane.

It does not patch Hermes core files and routine sync uses no LLM calls.

## What It Does

- Creates or maintains one Telegram forum topic per Herdr pane.
- Keeps the General topic free for your normal Hermes chat.
- Sends pane updates with Telegram Bot API 10.1 `sendRichMessage`.
- Uses `editMessageText(rich_message=...)` for a quiet live status card.
- Shows clean reports, questions, blockers, and numbered choices.
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

Plain text is not forwarded unless `implicit_send_enabled` is enabled in state.

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

These render as Telegram rich headings, paragraphs, tables, checklists, collapsible details, and footers when rich messages are available.

## Useful Environment Variables

```bash
HERDR_BIN=herdr
HERDR_TELEGRAM_TOPICS_STATE=~/.local/share/herdres/state.json
HERDR_TELEGRAM_TOPICS_LOCK=~/.local/share/herdres/sync.lock
HERDR_TELEGRAM_TOPICS_GENERAL_THREAD_ID=1
HERDR_TELEGRAM_TOPICS_MAX_CREATES=3
HERDR_TELEGRAM_TOPICS_MAX_SENDS=8
HERDR_TELEGRAM_TOPICS_FEED_READ_LINES=140
HERDR_TELEGRAM_TOPICS_FEED_MAX_CHARS=9000
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
