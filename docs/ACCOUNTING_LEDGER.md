# Accounting ledger (WP01)

Status: implemented additively on the existing `usage_records` and
`provider_budget_reservations` tables. No table was renamed and no existing
column was changed. Contracts: `docs/contracts/ADR-A01.md`, `docs/contracts/ADR-A02.md`.

## Records

- **Attempt** = one `provider_budget_reservations` row (`step_id`,
  `provider_attempt`). Append-only; status transitions
  `reserved → reconciled | conservative | released`.
- **Invocation** = one `usage_records` row per settled attempt (unique
  `reservation_id`). Intake/review calls that have no workflow step write a row
  with `reservation_id = NULL` and `step_id = NULL`.

### New columns

`usage_records`: `purpose`, `attempt_kind`, `horizon_id`, `pricing_revision`,
`currency`, `cost_known`, `late_receipt`.

`provider_budget_reservations`: `purpose`, `attempt_kind`, `horizon_id`,
`pricing_revision`, `currency`.

All are nullable/additive. `horizon_id` is a stable nullable scope reference;
binding it to a real horizon is WP08.

## Semantics (`docs/contracts/ADR-A02.md`)

- `purpose` ∈ `intake | planning | coding | review | research | setup`.
- `attempt_kind` ∈ `initial | repair | retry | takeover`.
- They are **orthogonal**: a row has exactly one of each. The purpose breakdown
  is a projection of all rows; the retry subtotal is another projection of the
  same rows (`attempt_kind != 'initial'`). Never add the retry subtotal to the
  purpose total.
- `pricing_revision` is the content revision of the provider profile config
  (`content_revision(profile)`) that priced the call. `currency` is `USD`.
  A full pricing registry (TOML vs SQL) is deferred (`docs/contracts/ADR-A02.md` open question);
  tariff values still come from the current config.
- `cost_known = false` means the amount is a conservative floor
  (`reserved_cost_units` / `minimum_unknown_usage_cost`), **not** a measured
  cost and **not zero**. Unknown stays visible in its own bucket.
- `cached_tokens` are recorded for provenance but never priced or added on top
  of `input_tokens`, so cached usage cannot be double-charged.
- Lifetime budget: usage rows are never reset by a new `budget_epoch`. Epochs
  only reset the task/step **caps**; the ledger total is cumulative.

## Money vs business state

`reconcile_usage()` is lease-fenced and coupled to business state. When the
originating lease is lost, `reconcile_usage` raises `LeaseLost`; callers then
use `record_late_receipt()`, which:

- records the receipt in its own transaction with no lease fence;
- never mutates `Step`/`Run`/`Task` (business state);
- sets `late_receipt = true`;
- is idempotent by `reservation_id`.

A late receipt is only meaningful when a provider request was actually sent; the
un-sent case (e.g. authentication rejection) uses `release_reservation_unfenced`
so the orphan sweep cannot charge it.

## Orphan RESERVED closure

No known path leaves a reservation `RESERVED` forever:

- **lease expiry** (`workflows/recovery.py`): a step that never started
  (`LEASED` only) is released; a step that may have reached the provider is
  charged conservatively (`close_step_reservations`).
- **cancellation/shutdown** (`workflows/worker.py`): when the handler's
  accounting transaction rolled back, `close_step_reservations` settles the
  leaked reservation conservatively.
- **bounded orphan sweep** (`release_orphan_reservations`, called at the end of
  every `recover_expired_steps`): closes reservations older than
  `ORPHAN_RESERVATION_MIN_AGE_SECONDS = 900` that are not attached to an active
  lease. Bounded by age and `batch_size`; `FOR UPDATE SKIP LOCKED`; never runs
  inside a request path. Releases reservations for steps that never started,
  conservatively settles the rest.

## Migration / backfill

Revision `c1a9f0b7d234` (parent `b7e2d9c41a05`), single linear head.

- Adds the columns above plus `ix_usage_records_horizon_id`,
  `ix_provider_budget_reservations_status`,
  `ix_provider_budget_reservations_horizon_id`.
- Backfill: pre-existing rows get `pricing_revision = 'legacy'` and
  `cost_known = false` (server default). `purpose`/`attempt_kind`/`currency`
  stay `NULL` — legacy provenance is not fabricated.
- `downgrade()` drops the columns and indexes (rolling back loses only the new
  provenance; existing `cost_units`/`reconciled_*` values are untouched).

Deploy note: `storage/migration_preflight.py` fails closed when the DB head is
not the code head, so `make db-migrate` must run with the deploy.

## SQL breakdown

Purpose breakdown (total, no double counting):

```sql
SELECT purpose,
       count(*)                       AS invocations,
       coalesce(sum(cost_units), 0)   AS cost_units,
       count(*) FILTER (WHERE NOT cost_known) AS unknown_rows
FROM usage_records
GROUP BY purpose
ORDER BY purpose;
```

Retry slice — a separate projection of the **same** rows:

```sql
SELECT attempt_kind,
       count(*)                     AS invocations,
       coalesce(sum(cost_units), 0) AS cost_units
FROM usage_records
WHERE attempt_kind IS NOT NULL AND attempt_kind <> 'initial'
GROUP BY attempt_kind;
```

Late receipts and unknown bucket:

```sql
SELECT late_receipt, cost_known, count(*), coalesce(sum(cost_units), 0)
FROM usage_records
GROUP BY late_receipt, cost_known;
```

Python helpers mirror these for CLI/tests: `usage_totals_by_purpose()` and
`usage_retry_subtotal()` in `vuzol.providers.budgets`.
