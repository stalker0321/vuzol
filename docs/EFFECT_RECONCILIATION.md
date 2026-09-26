# Effect intent and reconciliation (WP05)

Status: implemented for the single existing effect adapter — the trusted local
Git CAS apply (`ResultApplyHandler` / `LocalGit.apply_result`). The contract is
the frozen `tasks/T001/schemas/effect.schema.json` (`effect.v1`). No deployment or
rollback engine, no new scheduler, and `ProxyStartupReconciler` semantics are
untouched.

## Effect contract

`effects` (new, additive table) is the durable intent/receipt for one mutating
operation. Owner = applier. Mapping to `effect.v1`:

| effect.v1 | column |
|---|---|
| `schema` | `schema_version` (`effect.v1`) |
| `effect_id` | `id` |
| `operation_key` | `operation_key` (stable, unique) |
| `step_id` / `horizon_id` / `attempt_id` | `step_id` / `horizon_id` / `attempt_id` |
| `effect_class` / `target` / `idempotency` | `effect_class` / `target_kind`+`target_reference` / `idempotency` |
| `authorization` | `permission_envelope_hash`, `approval_id`, `approval_envelope_hash` |
| `intent` | `payload_hash`, `lease_generation`, `created_at` |
| `launch` | `launch_started_at`, `launch_dispatch_token`, `launch_generation` |
| `receipt` | `receipt_status`, `receipt_observed_at`, `receipt_external_ref`, `receipt_output_hash` |
| `reconcile` | `reconcile_status`, `reconcile_reconciled_at`, `reconcile_method` |
| `status` | `status` |

For the Git adapter the authorization envelope is the existing result-approval
envelope, so `permission_envelope_hash` = `approval_envelope_hash` =
`envelope_hash(approval.action_envelope)`. Adapter-specific, non-secret
observation data lives in `context` (`project_id`, `worktree_id`, `target_branch`,
`expected_head`, `result_commit`). No credential values are stored.

Operation key: `apply:<approval_id>:<result_commit>:<target_branch>` — created
before launch and reused on every retry.

## Order of operations (ADR-A01)

1. `_load` validates the approval/envelope/worktree binding and the lease; the
   policy gates and `_assert_current_lease` run.
2. `_record_intent` writes the effect in a short fenced transaction and marks it
   `dispatched` **before** the side effect. If an existing effect for the same
   `operation_key` is `uncertain`, it raises and the handler returns BLOCKED —
   an uncertain effect is never launched blindly.
3. `git.apply_result` performs the CAS **outside** any DB transaction.
4. `_record_applied` settles the effect (`settled` / receipt `applied` /
   reconcile `confirmed`, method `read_git_ref`) in the same fenced transaction
   as the Worktree/Approval/package update.

## Cancel/crash window — reconciled to applied, never a false "not applied"

If the process is killed or cancelled between the CAS and `_record_applied`, the
ref has moved but the business record was fenced out. The intent row survives as
`dispatched`. `EffectReconciler` (started at applier startup) observes the real
ref:

- ref == `result_commit` → `settled`/`applied`/`confirmed` **and** the same
  business state as a successful apply (Worktree `applied`, Approval `consumed`,
  package `integration_head_commit`) — without moving the ref again.
- ref == `expected_head` → `failed`/`denied`; business state is untouched
  (honest "not applied").
- anything else or unreadable → `uncertain`; if the step is non-terminal its
  `unknown_effects` is set so recovery never blind-retries it.

Cancellation is not a rollback (invariant 7): a cancelled-but-applied effect is
settled as applied.

## Observation taxonomy

`classify_effect_observation(observed_ref, result_commit, expected_head)` is pure
and fail-closed (`applied` / `not_applied` / `uncertain`). `read_ref` never
raises for a missing branch (returns `None` → `uncertain`).

## Adapter conformance checklist

A future effect adapter is accepted only if it declares and demonstrates:

1. **Idempotency class** — `idempotent` / `reconcilable` (an observable external
   state a reconciler can read) / `non_idempotent` / `unknown`. `unknown` must
   block, never blind-retry.
2. **Stable operation key** created before launch and reused across retries.
3. **Observable target** the reconciler can read without side effects
   (`read_git_ref` is the current value of `reconcile.method`).
4. **Fence behavior** — settlement writes are fenced by lease generation; the
   physical guard is the external CAS/expectation, not DB fencing alone.
5. **Unknown ⇒ block** — an unprovable observation yields `uncertain` and a
   blocked/attention outcome, never an automatic re-run.

## Recovery runbook

1. Applier startup runs `EffectReconciler.reconcile_startup()` under
   `EFFECT_RECONCILIATION_LOCK_KEY` (`pg_try_advisory_lock`, timeout → skip).
2. Inspect `effects` for `status in (intent_recorded, dispatched, uncertain)`:
   - `settled`/`applied` — done.
   - `failed`/`denied` — the effect did not reach the target; the step's normal
     recovery/manual-retry path applies.
   - `uncertain` — do not relaunch; investigate the target, then settle manually
     or escalate.
3. Events `execution.effect_reconciliation` carry `operation_key`, classification
   and `step_id` for tracing.

## Migration note

Revision `e7d2c4a91b06` (parent `b3f7a2c91e04`), single linear head. It only
creates `effects` (+ indexes); there is no data backfill because the table is
new and no historical effect may be fabricated — existing runs are read through
their Worktree/Approval state. `downgrade()` drops the table. `Step.unknown_effects`
is intentionally kept until WP08.

## Out of scope

`DeliveryStatus.AMBIGUOUS` / `mark_outbox_ambiguous` are untouched (no resolver
is added). Push/patch delivery, static publish and proxy/egress effects are out
of scope; they must pass the conformance checklist when added.
