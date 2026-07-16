# Remote decisions

Herdres can answer a bounded set of Claude prompts from an inline Telegram
keyboard when `HERDRES_REMOTE_DECISIONS=1`.

The flow is deliberately end to end: the Claude hook records a pending
`AskUserQuestion` or `ExitPlanMode`; `herdr_turn_adapter.py` emits a
`pending_decision`; Tendwire publishes the neutral form as
`meta.decision`; Herdres joins it to one unambiguous worker topic and renders
the inline keyboard; and a tap submits Tendwire's schema-v1
`answer_decision` command. Tendwire owns the backend interaction and returns a
schema-v2 command envelope.

## Supported prompts

- A single-choice `AskUserQuestion` with at most 11 source options becomes a
  `single` decision. It includes a write-in row that arms a separate text
  reply.
- A single multi-select `AskUserQuestion` with at most 11 source options
  becomes a `multi` decision. Options use 1-based ordinal IDs, and Telegram
  adds a Submit row. Multi-select decisions never offer a custom/write-in row.
- `ExitPlanMode` becomes a `plan` decision with approve and revise choices.

Multiple questions remain a read-only pending interaction. A single question
with more than 11 source options also remains read-only; Herdres never truncates
a choice set that it might drive remotely.

## Fail-closed behavior

No keyboard is emitted for malformed or empty options, unsupported decision
kinds, multiple decisions competing for one topic, ambiguous or missing worker
routing, or a decision with more than one question. Invalid callback data and
stale option references are not forwarded.

Herdres accepts only the exact schema-v2 `answer_decision` response envelope,
including the correlated request ID and decision reference. A missing field,
extra field, wrong result shape, mismatched CLI exit code, or other malformed
response becomes `request_state_uncertain` rather than being treated as an
answer.

When Tendwire reports `answer_in_progress`, another request currently owns the
answer claim. Herdres keeps both the decision record and keyboard, performs no
automatic retry, and tells the owner to tap again in a moment.

## Paired Tendwire requirement

Remote decisions require the paired Tendwire version that implements this same
`answer_decision` contract: exact schema-v2 accepted and typed-failure
envelopes, plus `answer_in_progress` with `no_receipt` or `in_progress`
disposition. Upgrade Herdres and Tendwire as a tested pair; leave remote
decisions disabled when either side predates that contract.
