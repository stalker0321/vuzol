# Read-only chat recovery and memory wiring

The project text-chat path remains Jev intake → hidden read-only Sol workflow →
durable Telegram delivery. Explicit work still requires its existing Start and
approval boundary. This change adds no extra classifier/model call or planner,
and no database migration.

## Recovery

New failures have a plain error message with `Ещё раз`, not a task card. Callback
wire action `v1:chat_retry:<hidden-task-uuid>` is bound to the confirmed bot error
message, chat/topic/user, active session/project and latest canonical user turn.
The persisted inbox and session row lock serialize repeated/racing clicks.

- Failed model/prepare attempt: create a new hidden RO workflow for the SAME
  source turn, rebuild current context, preserve full original intake text and
  do not classify again or append another user turn. All ordinary admission,
  reservations, concurrency and timeout limits apply. Old tasks/usage/evidence
  remain immutable; an event links the explicit retry to the new task.
- Valid generated response but local persistence/delivery-step failure: enqueue
  the recorded answer idempotently, no new model request/task.
- Known unsuccessful Telegram delivery: requeue only dead-lettered unconfirmed
  parts, audit the prior attempts/error and grant the normal bounded delivery
  retry allowance. Delivered prefix is untouched. Rate limits/active delivery
  already use the existing automatic retry/defer path.
- Ambiguous send or non-exited/unknown supervised process: refuse blind replay.
  Unknown-send notices have no retry button. No method resets terminal tasks or
  relaxes manual work authority. A newer user turn invalidates an older button.

This does not retroactively attach buttons to historical T095 notices. A rollout
may enqueue a new recovery notice for a specifically owner-approved latest
failed chat task; never replay the task merely to obtain a button.

## Memory

Opt-in `VUZOL_CHAT__MEMORY_ENABLED=true` connects the existing discussion reader,
D5 scoped recall and summary storage to both Jev and Sol. Current raw input is
authoritative and included exactly once in the Sol prompt. Recalled units retain
IDs/types/status and are contextual data, not operational truth or authority.

Recall uses project-scoped FTS matches followed by recent active units (existing
repository policy excludes hypothesis/superseded/retracted/tombstoned status).
Whole derived units fit a 6000-character recall budget; individual text is never
prefix-cut. The latest summary retains a six-ordinal verbatim overlap; raw turns
still use existing bounded-history packing. The newest single oversized turn is
kept whole in the opt-in reader rather than prefix-cut; overall 40k provider
payload and token/cost guards still apply. Packing omissions are exposed to the
model; this is not unlimited context or a semantic-quality guarantee.

The same Sol response may include `memory_summary`. Existing `{"reply": "..."}`
responses remain valid. Summaries are derived/model-labelled, at most 2000
characters; unsafe/oversized/missing candidates do not discard a valid answer.
They preserve goals/negations/unresolved questions and proposals-vs-decisions,
and cannot create AcceptedDecision records or grant execution permission.
Publishing happens only in the fenced transaction confirming the last reply
part (or full Rich reply). Incomplete unsummarized input cannot advance coverage;
delayed candidates cannot overwrite a newer revision. Coverage keeps the existing
exclusive-boundary storage semantics. A pending/partial/ambiguous assistant
response is excluded from opt-in conversation history.

T103 supersedes the last20/24k raw packing for opt-in chat: all uncovered turns
are kept whole, file texts have their own50k partition, and only summary-covered
overlap may be removed. The ordinary compiled-prompt partition remains40k;
true overflow is explicit. Legacy readers keep their previous bounds. See
`docs/CHAT_INTENT_CONTEXT.md` for precise admission/compatibility rules.

The existing standalone `vuzol-memory-writer` still exclusively consumes
`memory_extract`: explicit accepted decisions/package outcomes → derived templates.
It is NOT a chat summarizer; merely starting it does not connect recall. The chat
summary producer described above fills that separate gap without another call.
Acceptance/decision producers retain their existing explicit-user authority.

## Rollout

Chat memory is live: `deploy/production/compose.yaml` runs the standalone `memory-writer`, and the
executor unit sets `VUZOL_CHAT__MEMORY_ENABLED=true` through `/etc/vuzol/executor.env`. The
one-off rollout overlay and executor drop-in used for the first activation were removed.

Rollback must retain compatible T094 chunk handling for pending or ambiguous
chains. Turn memory off consistently to stop new summaries/recall, stop ONLY the
new standalone writer if reverting activation, and retain canonical rows/usage.
Older optional summary fields do not require destructive data cleanup.
