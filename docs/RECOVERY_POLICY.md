# Recovery policy: fingerprints and the decision table (WP04)

Status: implemented additively in `workflows/service.py`, using only persisted
`Step.payload` and existing `Run`/`Step`/`Worktree` columns. No migration is
required. Existing safety behavior (unknown effects fail closed, approvals,
budgets) is unchanged.

## Normalized failure fingerprint

`vuzol.workflows.recovery_policy` computes a versioned fingerprint
(`failure-fingerprint.v1`) over the orthogonal persisted facts of a failure:

| Component | Source |
|---|---|
| `step_type` | failed step |
| `category` | normalized `outcome.category` (`strip`/casefold) |
| `evidence_hash` | `envelope_hash(outcome.result)` (validation/review evidence) |
| `environment_hash` | current project environment contract (`project_environment.environment_hash`) |
| `result_hash` | worktree `diff_hash or result_commit` |
| `strategy_hash` | executor profile + run policy/prompt revisions |

`failure_fingerprint(components)` is a sha256 over the canonical component map.
Changing any component (new evidence, new diff, changed environment/strategy)
produces a different fingerprint. The fingerprint and its components/history are
stored in the failed step payload:

- `failure_fingerprint`, `failure_fingerprint_schema`, `failure_fingerprint_components`
- `failure_fingerprint_history` — bounded (≤8) list of fingerprints for which a
  repair was already scheduled.
- `last_recovery_summary` — the decision record.

Backfill: no migration/backfill; a step without the keys is treated as an empty
history. Adding future component fields is a new `failure-fingerprint.vN`.

## Decision table

`decide_recovery(state, policy) -> RecoveryAction` is pure and total. Rows are
evaluated in order; the default is fail-closed `attention`:

| Condition | Action |
|---|---|
| `unknown_effects` | `attention` (never retry) |
| recovery deadline exceeded (`run.started_at` + policy) | `attention` |
| backpressure category (`disk_pressure`, `rate_limited`, `quota_exhausted`, `provider_unavailable`) and waits remain | `wait` (requeue later, refund attempt) |
| backpressure category and wait cap reached | `attention` |
| repairable `validate`/`review` failure whose fingerprint is already in history | `attention` (identical / A→B→A) |
| repairable and per-step repair cap reached | `attention` |
| repairable and task-wide repair cap reached | `attention` |
| otherwise repairable | `repair` |
| transient outcome and step is safely retryable | `retry` |
| anything else | `attention` |

`takeover` is a modelled action with no producer yet (`docs/contracts/ADR-A01.md` keeps the label);
`wait` is produced for backpressure. There is no Jev and no new scheduler.

## Bounds are policy (invariant 48)

`WorkflowSettings` now carries `max_step_repairs` (3), `max_task_repairs` (6),
`max_backpressure_waits` (5) and `recovery_deadline_seconds` (3600).
`WorkflowWorker` builds the `RecoveryPolicy` from settings and passes it to
`commit_step_outcome`. Tests may pass a `RecoveryPolicy` directly.

Cap semantics:

- **Per step**: cumulative `repair_count` in the failed step payload. It is no
  longer keyed to `task.budget_epoch`, so a manual retry (which bumps the epoch
  for token caps) does **not** silently re-arm repairs.
- **Per task**: the count of `repair_code` steps in the run, covering `validate`
  and `review` together. This is the shared cap required by WP04.
- Any re-arming of repairs is therefore an explicit, operator-visible policy
  change; there is no hidden grant. Manual retry still grants exactly one
  bounded attempt on the retried step (unchanged).

## Backpressure vs attempt burning

`wait` requeues with a delay and refunds the claim attempt (`attempt_count -= 1`)
for all backpressure categories, generalizing the existing `disk_pressure`
precedent. It does not touch reservation/ledger semantics. The wait count is
persisted in `step.payload["backpressure_count"]` and bounded by
`max_backpressure_waits`; when exceeded the step goes to `attention`.

## Operator-visible attempt summary

Every decision emits a `workflow.recovery_decision` run event carrying
`recovery_attempt_summary`: `decision`, `fingerprint`, `fingerprint_schema`,
`seen_fingerprints`, `repair_count`, `task_repair_count`,
`backpressure_count`, `category`, `step_type`. `workflow.repair_scheduled`
additionally carries `fingerprint` and `task_repair_count`. Repair evidence
(`repair_context`) includes `failure_fingerprint`, and the worker ContextItem
reference becomes `repair:<step>:<fingerprint[:12]>`.

## Durability and partial results

The decision and the repair enqueue run in the same transaction as the outcome
commit, so a restart/replay cannot double-schedule (a second commit for the same
lease raises `LeaseLost`). Partial results are never discarded by the recovery
path: `step.result`, `Artifact` rows and worktree commits are untouched when the
decision is `attention`.
