# Engineering backlog

This file records agreed follow-up work that is important but intentionally
deferred. Completed work belongs in `CHANGELOG.md`.

## Product consolidation — 2026-10-21

Direction: `ASTRA_PRODUCT_CONSOLIDATION_HANDOFF.md` (owner-provided, separate
from implementation). Safety repairs from the pilot precede new interfaces.
This inventory is not authorization to delete stored projects, results or grants.

| Mechanism | Supported scenario | Replacement | Dependencies / retained data | Decision |
| --- | --- | --- | --- | --- |
| Project lead + command receipts | Goal, bounded packages, independent acceptance | Existing leadership application | Goals, delegations, turns, evidence, receipts | Keep; fix pilot contracts first |
| Work-package sequencer | Ordering tasks inside the lead's chosen package | None needed | Plans, materialization links, package approvals | Keep; it is not a second lead |
| Workflow workers, applier, supervisor | Execute/review/repair, stop, reconcile effects | None needed | Runs, leases, effects, validation and provider receipts | Keep; do not infer completion from a green plan |
| Model role matrix | Explicit interchangeable model/account/reasoning choices | Current `/model` | Revision-pinned runs and turns | Keep; new choices apply to new invocations |
| Legacy executor preferences and family fallbacks | Older project selections and already-pinned runs | Role matrix | Must audit preference rows AND persisted run/step payloads first | Remove after explicit data conversion; not safe to delete as dead code today |
| Direct `/lead <goal>` intake | Existing explicit execution command | Discuss-by-default `/lead`, intent-to-lead admission | Existing Telegram routing, durable intake and owner identity | Replace only with tested conversational admission; never make discussion execute accidentally |
| Horizon planner and per-task planner routes | Historical/optional planning | Lead chooses packages; sequencer runs them | Contract-pinned packages, template and recovery tests | Needs scenario/data inventory before removal; no new parallel planner |
| Snapshot-only lead/goal-review calls | Bounded decisions and captured-commit checks in an empty workspace | None for this boundary | Durable turn receipts, transport restrictions | Keep; this is NOT the policy for all read-only research |
| Telegram rich formatting and delivery | Safe formatted answers, durable retries | Reuse existing renderer and delivery | Outbox, part identity, escaped fallback | Keep; replace dense goal cards, not the delivery engine |
| Memory and context assembly | Project memory and bounded invocation context | Reuse existing assemblers/artifact provenance | Chat turns, memory units, native CLI history | Measure before changing ceilings; do not conflate context with spending |
| ChatGPT/MCP entry | Explicit durable submission and status lookup | Thin adapter to the same application | Principal/project authorization, stable request identity | Add later; no public endpoint or independent scheduler in the pilot fix |

Compact Telegram cards are implemented locally with authorized full-request/details
reads and offline rendered working/ready/paused/question/failure examples. See
`TELEGRAM.md`; deployment and live-client visual acceptance are separate.

Remaining implementation slices after the pilot repair:

1. Persist `/lead` and `/think` per topic; Jev routes intent, not work assignments.
   Explicit changes go to the same lead in either mode. Read-only research may
   use inspection tools and bounded delegation, with inherited write restrictions.
2. Minimal authenticated MCP submission/status/result adapter. Confirm hosting,
   authentication and the actual ChatGPT client's support before exposing it.
   Per handoff §8.1, MCP, a future personal assistant and the project lead reuse
   the same application operations with server-enforced principal/project scope.
   MCP does not route through Telegram or need a second model to reclassify an
   already explicit request. Equivalent requests must pass identical admission,
   authorization and idempotency checks; similar separate requests stay separate.
   The MCP slice does not depend on first building the general inbox.
3. Measure assembled input and native CLI history; support the owner's preferred
   250–500k ceiling where the selected model permits, with summaries and original
   artifact links. No newly imposed 16–32k product cap and no silent truncation.
4. Remove confirmed obsolete mechanisms together with their configuration/docs;
   migrate dependent data explicitly. No DB reset or edited applied migration.

Do not present these slices as shipped. Mode switches, research delegation and
MCP admission require separate end-to-end acceptance; the current pilot remains
paused and unaccepted until a supported verification/recovery flow proves readiness.

## Next after Telegram dogfood testing

### TODO — Discussion intake lifecycle is write-only

`IntakeStatus` never advances past `awaiting_interpretation`: no code path
writes any later state, so the interpreter backlog (73 rows as of 2026-08-24)
is indistinguishable from real pending work at a glance. Related findings from
the same review: three `discussion_classify` jobs are dead-lettered
(`provider_unavailable` ×2, `discussion_context_mismatch` ×1), meaning real
user messages were silently dropped after retry exhaustion. Needed: either
record actual terminal intake states or stop pretending the field tracks
progress, plus minimal operator tooling to inspect and re-drive dead-lettered
classify jobs instead of querying `transactional_outbox` by hand.

### URGENT — Task failures are silent in-place message edits

When a task fails or blocks, the existing status card is edited in place far
up the topic history. Nothing new appears, nothing is pinned, nothing
notifies — the operator has no signal that work stopped and waits for them.
Design a proper failure notification surface (fresh terminal card in the
topic, a mention/ping, or a digest in the system topic) without turning every
transient retry into noise. Decide the trigger policy: which terminal states
notify, and whether repeated identical outcomes collapse into one update.

### URGENT — Work-item discussion never mutates the plan

**Priority:** immediate fix; this blocks the entire dogfood loop for
iterating on a running work item.

Observed live on task `4190012` (three-body-problem, 2026-08-24): the user
requested a scope simplification inside the work-item discussion («сделай
панель просто…», «го», «да, делай»). The discussion interpreter classified
every message as non-mutating (`should_mutate_plan=false`) or refused control
(`control_requires_button`), while the assistant replied conversationally as
if the change were accepted ("Ок — упрощаем панель"). Result: no edit session,
no plan revision, no confirmation card — the user waits for an approval that
can never arrive, and retry keeps reviewing the stale scope.

Required behavior:

1. An explicit scope change in a work-item discussion must produce an
   authoritative artifact — an edit session and a revised plan (or revised
   item) with an approval card carrying buttons. Textual agreement by the
   assistant without a following card is a contract violation.
2. The interpretation prompt/policy must stop classifying concrete scope
   changes as ordinary discussion turns.
3. The assistant must never claim acceptance ("ок, делаем") for something the
   policy will refuse two turns later.

Open design question (from the operator): the «Изменить» button lives under
the *task* status card, not under the plan, yet today the only mutation path
is a whole-plan revision. Maybe the confirmation should come back as an
updated *task/item-level* card (revised goal, scope, completion criteria)
instead of re-approving the entire plan. Decide granularity before
implementing: per-item revision cards vs full-plan revision cards.

### TODO — Revisit planner architecture

The current coding planner runs before repository context is prepared, so it
can only generate generic “inspect → fix → test” plans that add little value
over the executor’s own reasoning. Keep the planner disabled for now; later
either move bounded repository recon/context before planning so it can produce
project-aware plans, or restrict planner usage to genuinely multi-stage/orchestration-heavy
tasks where a separate planning step adds value.

### TODO — Selectively adopt Benjamin-Plus ideas

Do not integrate Benjamin-Plus as a whole injected skill; instead later port
only the behaviors that fit VUZOL mechanically into the harness/scheduler:
batched reconnaissance, bounded/keyhole reads, one-shot dependency preflight,
stop immediately once task checks are green, and suppression of polling,
repeated, or failed-approach loops. Add lightweight instrumentation for
duplicate tool work, retries, polling, tokens/cost, and wall time so the
changes can be A/B tested rather than assumed useful.

### Build an interpreter/planner evaluation corpus

Create an agreed corpus of roughly 20 realistic requests spanning trivial,
ambiguous, multi-step, risky, and cross-domain work. Before running models,
review with the user a human reference decomposition into the smallest useful
work items. Then execute the real interpreter and planner contracts for every
request/work item and retain model/profile, prompt revision, structured output,
tokens, latency, repairs, and validation failures.

Evaluation must keep interpreter and planner scores separate and compare model
outputs against explicit observable criteria (intent and constraints retained,
clarification quality, task boundaries, dependencies, completion criteria,
scope/risk, schema validity, unnecessary steps, and token/latency cost). The
reference is a rubric, not a requirement for identical wording or an identical
plan. Runs must be reproducible so small/free models can be compared against the
current production baseline instead of being judged as a black box.

### Replace fixed diff-size failure with chunked independent review

**Priority:** first engineering task after the current end-to-end Telegram
dogfood pass is complete.

The current independent reviewer accepts a complete diff up to 120,000
characters and fails closed above that boundary. The bound protects provider
context and spend, but a large valid work item must not require human recovery
solely because its diff crossed an arbitrary character count.

Implement a bounded review pipeline that:

1. runs trusted mechanical gates over the complete retained result;
2. separates generated files, lockfiles, and other review noise without hiding
   their presence or bypassing mechanical checks;
3. partitions the remaining diff on file or coherent logical boundaries;
4. independently reviews every partition;
5. aggregates all findings into one result-level verdict, retaining file and
   line provenance;
6. limits automatic review batches or repair cycles rather than treating diff
   size itself as a task failure;
7. asks the worker to split an exceptionally large change before involving the
   user, and escalates only after the bounded automatic recovery is exhausted.

Acceptance requires tests proving that a diff above the former 60,000-character
boundary completes review automatically, findings from separate chunks are not
lost, a blocking finding blocks the aggregate verdict, generated/noisy files do
not consume the whole model context, and the batch cap terminates deterministically.

### Make repair loops progress-aware

Replace token pressure as the normal repair-loop control with explicit progress
semantics:

- fingerprint normalized validation failures and reviewer findings;
- do not send the worker through an unchanged failure a second time unless new
  evidence, code, configuration, or instructions can change the outcome;
- keep a high aggregate token ceiling (order of magnitude: one million tokens)
  only as an emergency spend guard, not a target that ordinary tasks routinely
  approach;
- bound automatic repair cycles explicitly and let a user-requested Retry open
  one new bounded round;
- show a compact attempt history on a blocked task card (attempt, stage, model,
  normalized outcome), collapsing consecutive identical outcomes instead of
  dumping raw logs or prompts.

The exact aggregate ceiling and automatic-cycle count must be chosen from
dogfood telemetry. Acceptance requires deterministic same-failure detection,
proof that a changed failure remains eligible for repair, and bounded Telegram
rendering for long histories.
