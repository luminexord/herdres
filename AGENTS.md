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

What you write in a turn is delivered to Telegram as a rich card by default. Two
things can change that, and a third can leave you with nothing:

- **rich delivery disabled** (`HERDRES_FORCE_PLAIN_DELIVERY=1`) — falls back to
  `sendMessage`;
- **a definite provider rejection** — falls back to `sendMessage`, *unless* the
  physical-write budget is already spent, in which case there is no fallback at
  all and the turn ends `operation_budget_exhausted`.

Length does **not** push you onto the plain path: an oversized turn is split into
several rich cards (`format=rich-split`), not downgraded.

Tables (`<table>`), headings (`<h1>`-`<h6>`) and `<details>` blocks exist only on
the rich path. **The fallback does not translate them — it unwraps them.** A
`<details>` block arrives as its summary line followed by its body, with no
collapsing at all: `<blockquote expandable>` is honoured on the `sendMessage`
path only when it is already present, and the rich fallback never emits it. So a
turn that leans on rich structure loses that structure entirely; write so the
plain form still reads.

Once you are already on the plain path, length hurts again: beyond roughly 3900
characters the content is split into chunks sent **without `parse_mode`**, so it
arrives unformatted as well as unstructured — no bold, no code, no links.

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

Inside an eligible block the parser is permissive and I will not try to bound it
again — a single hyphen per cell (`|-|-|`), deep indentation, alignment colons,
outer pipes, and stray whitespace all still parse. Two things reliably defeat a
table regardless: a code fence around it (the fence is handled before table
parsing, so it stays literal), and a table with no content at all, which is
rejected outright.

Treat the two rules above — separator row, own block — as the requirements, and
everything else as unspecified rather than guaranteed.
