# AGENTS.md

## Gitmoot work discipline

All multi-step work in this repo goes through gitmoot under a NAMED workflow, so progress
survives session compaction and shows on the operator dashboard
(https://gitmoot.themartian.app).

1. Starting an initiative: gitmoot workflow describe herdres/<topic> "one line on the goal"
2. Every dispatch carries the label: gitmoot agent run ... --repo luminexord/herdres --workflow herdres/<topic>
3. At milestones, handoffs, blockers, and root causes: gitmoot workflow note herdres/<topic> "what happened"

Before starting a new workflow, check gitmoot workflow list and continue an existing one when
the work belongs to it. Never leave multi-step work unlabeled: unlabeled jobs are invisible to
the operator and to every other session.

## Tables in messages the owner reads

What you write in a turn is delivered to Telegram as a rich card by default, but
not always: rich delivery is *attempted* first and falls back to a formatted
`sendMessage` when it is disabled (`HERDRES_FORCE_PLAIN_DELIVERY=1`), when the
content is oversized, or when the provider definitely rejects it.

Tables (`<table>`), headings (`<h1>`-`<h6>`) and `<details>` blocks exist only on
the rich path. **The fallback does not translate them — it unwraps them.** A
`<details>` block arrives as its summary line followed by its body, with no
collapsing at all: `<blockquote expandable>` is honoured on the `sendMessage`
path only when it is already present, and the rich fallback never emits it. So a
turn that leans on rich structure loses that structure entirely; write so the
plain form still reads.

The oversized case is harsher still. Beyond roughly 3900 characters the content
is split into plain chunks sent **without `parse_mode`**, so it is not merely
unstructured but unformatted — no bold, no code, no links.

One thing is easy to get wrong on the rich path: **a markdown table only renders
as a table if it has the separator row.**

```
| column | column |
|---|---|
| value  | value  |
```

The `|---|---|` line is what converts it. Without that line it is not a markdown
table — it arrives as plain text with the pipe characters visible. It looks
correct while you are writing it, which is exactly why it kept shipping broken.

Do not draw tables with ASCII box characters (`+---+---+`); those never convert.

**A table must also start its own block.** Leave a blank line between any prose
and the table's header row. Without it the paragraph collector swallows the
header and the table is never recognised — measured 2026-08-01, four tables in
the owner's own topic arrived as raw pipes for exactly this reason.

Within an eligible table block three things are forgiving: outer pipes are
optional, alignment colons are fine, and extra spaces in the separator are fine.
That list is exhaustive, not an example — a table inside a code fence stays
literal because the fence is handled before table parsing, and an all-empty
table is rejected outright.
