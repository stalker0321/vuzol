# Engineering backlog

This file records agreed follow-up work that is important but intentionally
deferred. Completed work belongs in `CHANGELOG.md`.

## Next after Telegram dogfood testing

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
