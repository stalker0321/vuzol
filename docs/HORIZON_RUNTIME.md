# Horizon v1 runtime over WorkPackage (WP08, `docs/contracts/ADR-A01.md` §A01.5 horizon)

Horizon is an opt-in contract layered on `WorkPackage`. A package without a
goal keeps the legacy lifecycle byte-for-byte. Behaviour is gated by
`Settings.horizon.enabled` (`src/vuzol/config/settings.py:479`), default off.

## 1. Schema

Additive nullable columns on `work_packages` (`src/vuzol/storage/models.py:910-921`,
migration `c4e8f1a92b70`, parent `a1c5e7b93d20`):

| Column | Type | Meaning |
|---|---|---|
| `goal` | Text | Product goal; presence makes the package a horizon (`is_horizon`) |
| `goal_revision` | Integer | Goal version; `1` at creation, bumped on adopted goal |
| `exit_criteria` | JSONB | Acceptance criteria list; empty/missing is **not** success |
| `lifetime_budget` | JSONB | `{max_cost, max_attempts, ...}` over the whole horizon |
| `deadline` | timestamptz | Horizon deadline |
| `permission_envelope_hash` | String(64) | References the WP05 effect intent mechanism, not a new one |
| `owner` | String(100) | Owner; backfill `'legacy'` for pre-horizon rows |
| `horizon_phase` | String(30) | Transient phase marker, see lifecycle |
| `acceptance_artifact_id` | FK artifacts | Retained acceptance evidence |
| `accepted_at` | timestamptz | Acceptance timestamp |

Downgrade drops the FK and all 10 columns (full reversibility). No table was
renamed; no existing column changed. Migration owner: writer WP08.

## 2. Lifecycle

Standard `WorkPackage` states apply. Horizon adds transient phases recorded in
`horizon_phase` while status stays `RUNNING`:

- **evaluating** — queue exhausted behind the flag: not success until
  `record_acceptance` accepts (`sequencer.py`, `PACKAGE_EVALUATING` event).
  Legacy path still completes (`COMPLETED`).
- **waiting_approval** — item with `needs_approval=True` is not materialized
  until approved; repeat observations are idempotent (`PACKAGE_WAITING_APPROVAL`).
- **item_approved:{ordinal}** — single-use marker written by
  `approve_waiting_item` (`APPROVE_ITEM`, `RUNNING`→`RUNNING`); lets exactly
  this ordinal through, cleared as the item materializes (`PACKAGE_ITEM_APPROVED`).
- **accept / reject** — `record_acceptance`: accept writes `accepted_at` +
  `acceptance_artifact_id`, closes to `COMPLETED` (`PACKAGE_ACCEPTED`);
  reject stays `RUNNING`/`evaluating` for bounded corrective work through the
  existing retry/skip controls (`PACKAGE_ACCEPTANCE_REJECTED`).
- **restart** — `restart_plan` behind the flag continues the approved horizon:
  `STOPPED`→`APPROVED` on the same approved revision, no new DRAFT, no
  re-approve; the ingress skips `approve` and resumes via `sequencer.start`.
  Non-horizon packages keep the legacy clone + re-approve path.
- **revise** — goal change on a horizon raises `goal_change_requires_choice`
  (choice is the explicit replan/approve path); rolling revisions may only
  change future items (past ordinals below the cursor keep identity + content,
  else `revision_conflict`).
- **limits** — order is queue-end → limits → resume/materialize: an
  exhausted queue always reaches `evaluating` first (acceptance stays callable
  even if the budget is spent or the deadline passed — limits gate new spend,
  not acceptance of finished work). For pending items, lifetime spend (links ∪
  retried-away/cancelled tasks, cost summed with no epoch filter) is checked
  via `budget_state`; exhausted budget or passed deadline pauses (`PAUSED` +
  `ITEM_BLOCKED`, payload reason), including on resume with a stale link.
  Retry epochs never reset lifetime. Resume from a limit pause is via replan.

`result_approval.py` is untouched: final approval stays under current policy.

## 3. Frozen status mapping

`HORIZON_STATUS_MAPPING` (`src/vuzol/discussion/horizon.py:19`) is the frozen
`WorkPackageStatus` (+phase, +accepted) ↔ `docs/contracts/ADR-A01.md` §A01.5 `horizon.status` contract.
Changing it is a contract change:

- `DRAFT` → `draft`; `APPROVED` → `ready`; `RUNNING` → `running`
- `RUNNING` + phase `evaluating`/`waiting_resource`/`waiting_approval`/`waiting_input` → that phase
- `COMPLETED` + accepted → `succeeded`; `COMPLETED` + not accepted → `running`
  (an unaccepted completion is not success)
- `PAUSED` → `paused`; `STOPPED` → `failed`; anything else → `cancelled`

## 4. Opt-in flag / pinned admission (D0)

- Enable: `VUZOL_HORIZON__ENABLED=true` (or config). Default off: the legacy
  lifecycle is the only executable path for new admissions; all new parameters
  default `False` through `PackageControlIngress`, `WorkPackageSequencer`, and
  the telegram composition boundary (read via `discussion/horizon.py:horizon_enabled`).
- Pinned contract (D0): `WorkPackage.execution_contract_version` +
  `Run.execution_contract_version` (nullable/additive, migration `d0c0n7r4c7v1`).
  `sequencer.start` pins `horizon-v1:enabled|disabled` on first start only
  (NULL → pin from admission flag); restart/resume never re-pins. Active
  packages read the pinned value (`pinned_horizon_enabled`): `approve_waiting_item`,
  RESTART `horizon_resume` + `restart_plan` horizon branch, `_materialize_current` /
  `materialize_running` / `observe_terminal`. A pinned-enabled package therefore
  passes APPROVE_ITEM and keeps its `waiting_approval`/`evaluating` gates with
  the live flag off. Pre-D0 rows (NULL) fall back to the passed flag for
  compatibility. Old serialized drafts/workflows/approvals remain readable.
- Rollback: set the flag off for new admissions. Already-running horizon
  packages keep their persisted state and evidence; they continue/pause
  explicitly via the existing stop/replan controls, never silent
  legacy-COMPLETED. `Run.workflow_version` is recorded at materialization and
  documented as written-not-branched (not used for contract selection in D0).
- Rollback never deletes evidence: approvals, artifacts, attempts, and event
  rows are append-only; the migration downgrade is schema-only and is not
  part of the flag rollback.

## 5. Compatibility matrix

| Area | Base behaviour | Horizon (flag on) |
|---|---|---|
| Statuses | 7-value enum, `COMPLETED` without acceptance | Same enum; `COMPLETED` requires acceptance, else `running` |
| Restart | New DRAFT + re-approve | Continues approved revision, no re-approve |
| Empty queue | `COMPLETED` | `evaluating`, not success |
| `needs_approval` | Stored only | Runtime gate (`waiting_approval` → approve → materialize) |
| Budget | Per `Task.budget_epoch` (WP01 unchanged) | Lifetime composes over epochs, no reset |
| Deadline | Run-level recovery deadline only | Horizon-level enforcement |
| Revisions | Silent replan | Goal change → choice; past rewrite → conflict |
| Approvals | `result_approval.py` final (unchanged) | Unchanged; intermediate via item approval |
| Old packages | — | `owner='legacy'` backfill; flows keep working (survival test) |
| Permissions/routing/fencing/prompts | — | Untouched |
