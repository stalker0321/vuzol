# ADR-0012 — D1 identity/revisions: WorkAttempt, spec versions, history, fences

Status: accepted (D1 writer, base 7f651d9 D0-PASS). Implements DELTA §D1.

## 1. WorkAttempt (additive lineage, no backfill)

- New table `work_attempts` per frozen `attempt.schema.json`/A01.3 (`attempt_id`
  PK, task/run/step FKs, nullable plan/item/horizon refs, `attempt_no` unique
  within `step_id`, self-FK `parent_attempt_id`, kind/purpose, executor/lease
  snapshot, outcome, usage/cost refs). `Run`/`Step` stay substrate, provider
  attempts stay accounting; `ProviderBudgetReservation.id` is never reused;
  `provider_budget_reservations`/`usage_records` gain no columns.
- `attempt_no` ≠ `lease_generation` ≠ `provider_attempt` (A01.4); a `retry`
  never increments the fence and the fence never creates business effect.
- `stable_item_id`: `WorkItemDraft.id` for materialized package items (read
  via `MaterializationLink`, never rewritten), else the task's own id.
  `tasks.id` and `MaterializationLink` SQL links are untouched.
- `takeover` is a reserved `attempt_kind` with no production writer yet
  (lead Q7; `AttemptKind.TAKEOVER` already in the enum, `RECOVERY_POLICY.md`
  documents the gap).
- Legacy executions have no rows: NULL/`unknown`, never fabricated (A01.7).

## 2. Versioned TaskSpec + source refs

- The spec lives in `task_spec_revisions` (content-hash `spec_revision`,
  unique per task), versioned separately from the mutating `task_draft`;
  `Task.spec_revision` is the current pointer. Creation + all four mutation
  points (`interpretation/service.py` ×2, `projects/naming.py`,
  `experiments/service.py`) snapshot; history rows are never rewritten.
- `Task.source_turn_id` (typed FK, session-validated fail-closed like
  `accept_decision`) promotes the discussion-agent `task_draft` string; the
  original user turn is referenced, never replaced by generated text.
  Materialized package tasks keep NULL until the originating turn is threaded
  (follow-up, not fabricated here).
- `WorkPackage.goal_revision` stays write-only (lead Q12); the intent fence
  uses a separate additive pointer below.

## 3. Review/outcome history + acceptance key

- History table `review_outcome_history`, separate from mutable `Step.result`.
  Acceptance key (lead consent on T048 REDO, dossier L3 candidate): the
  verdict content hash, unique within `(run_id, step_id)`
  (`uq_review_outcome_run_step_key`) — never a global unique, so a second
  BLOCKED verdict with different content is always recorded, never swallowed;
  an identical re-commit returns the existing row (safe retries).
- Q11-as-decided (`Approval.action_envelope_hash`) is explicitly NOT
  implemented, with lead consent: a BLOCKED review precedes approval creation
  in the workflow (`definitions.py:93–127` — `approve_result` has
  predecessors `publish_preview ← build_static ← review`), so no approval
  exists for the verdict's step at record time and an approval-hash branch
  would be dead in production. The dead branch was removed, not left as
  "done Q11". If a future flow records outcomes where an approval exists,
  D2 may revisit the key — as a new decision, not a silent change.
- Minimal fix in the BLOCKED branch of `commit_step_outcome` only; the
  success path is untouched.
- History is evidence retention: it is never read as proof of a past review
  (a changed hash still requires a fresh review).

## 4. Unified item contract + pinned guard + intent fence

- One field set + one hash (`ITEM_CONTRACT_FIELDS`, `item_contract_hash`)
  for carry-forward (`_same_plan_item`) and the history guard
  (`_require_future_only_revision`): scope/dependencies/approval rewrites of
  a passed item are `revision_conflict`, never an unchanged prefix.
  `PlanRevision.content_hash` (plan-level) is unchanged.
- `revise_draft` guard and the `record_acceptance` gate read
  `pinned_horizon_enabled(package)` (lead pinned-guard decision, T045 r2
  follow-up); pre-D0 NULL rows fall back to the passed flag.
- `WorkPackage.intent_revision` (additive pointer, content hash of the head
  revision): `revise_draft(..., expected_intent_revision=...)` rejects stale
  intent with `stale_intent_revision`. Callers that omit it keep the legacy
  generation-CAS path (documented opt-in).

## 5. Receipts + pause fence (C6)

- Task commands record `payload.outcome` on the `TelegramControlAction`
  (package-controls parity); duplicate telegram delivery returns the prior
  outcome in `IngressResult.outcome` instead of "unknown".
- `expected_task_version` stays explicitly opt-in on all task-control paths
  (`controls.py`, `dispatch.py`, `cli/task.py` default None) with a test
  pinning the `_noop`-without-receipt behavior; making it mandatory needs a
  transport-migration decision (follow-up, not silently changed here).
- Soft pause + intent fence (lead Q9): pause keeps leases, but
  `commit_step_outcome` rejects commits while `run.status == PAUSED`
  (`LeaseLost`), so correction fencing holds during pause.

## 6. REDO as a new attempt + transitions dead path

- REDO cancels the run/step as before and additionally records a
  `WorkAttempt` (`retry`, `cancelled`) of the same logical item with
  `parent_attempt_id` and refs to the superseded candidate
  (`prior_candidate_hash`) and review (`prior_review_summary`); nothing is
  erased. Legacy `MaterializationLink` chains read unchanged.
- `storage/transitions.py` is a documented dead path (narrower duplicate of
  `workflows.transitions`, sole consumer `test_transactions.py`); kept,
  not deleted/merged in D1 (lead Q8).
