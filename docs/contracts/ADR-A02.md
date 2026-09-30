# ADR-A02 — Accounting, pricing, attribution and evidence

> Status: frozen, base 9be9054. Immutable frozen copy (WP00 baseline) — do not edit semantics.

- **Status:** Proposed (WP00 contracts baseline). Not yet accepted.
- **Scope:** cost/usage ledger semantics, pricing revisions, unknown markers, attribution dimensions, review/evidence binding.
- **Base commit:** `79d7326e3e490647cb8a0e50c2c25ae23daa6409`.
- **Relation to existing decisions:** complements ADR-0001, ADR-0007, ADR-0008; preserves invariants 1, 4, 42, 48 and the report's stable boundary 5 ("missing usage ≠ 0"). Does not weaken any accepted ADR.

## Context

The current ledger (`UsageRecord`, `ProviderBudgetReservation`, `NormalizedUsage`) already records `input_tokens`, `output_tokens`, `cached_tokens`, `cost_units`, `quota_units`, `duration_ms`, `reserved_*`/`reconciled_*` and `budget_epoch`. Gaps recorded by the audit (`E07`, `E08`, report §21): not all LLM paths are covered (intake/repairs/setup/research), missing usage is not consistently modelled as unknown, `cost_units` is conditional and can be confused with a currency amount, and retry/review cost can be double-counted. Report §27 D07 and §21 define the required semantics.

## Decision

### A02.1 Currency and pricing revision

Every monetary amount carries a **currency** and a **`pricing_revision`**. Conditional `cost_units` must never be summed with a currency amount without applying the versioned pricing interpretation in force at the time of the call.

- `PricingRevision`: `{pricing_revision, currency, input_rate, cached_input_rate, output_rate, reasoning_billing_semantics, unknown_floor, effective_from}` — immutable, append-only, one active revision per (provider profile, effective window).
- Usage rows store the `pricing_revision` used; switching a revision does not rewrite history.
- Costs from different currencies/revisions are reported per revision and converted only through an explicit, versioned FX/interpretation rule.
- Subscription/quota consumption is a **separate** dimension from cash cost; zero marginal cash is not zero quota consumption (report §7).

### A02.2 Unknown is not zero

- Missing usage or missing rate is **unknown**, never `0`.
- `null` nullable fields (`input_tokens`, `output_tokens`, `cached_tokens`, `cost_units`, `quota_units`) mean *unknown*, not *zero*.
- An unknown priced call gets a **conservative reserve** (`unknown_floor`) and is reported in a distinct "unknown" bucket.
- Unknown remains visible after the run closes; it is not zeroed out and not silently dropped from denominators.
- `attempt.cost_known = false` propagates this to the Attempt contract; `unknown` and `0` are different states throughout.

### A02.3 Orthogonal attribution dimensions

Each usage row has exactly one value for each dimension; the dimensions are not additive with each other:

| Dimension | Values |
|---|---|
| **purpose** | `intake`, `planning`, `coding`, `review`, `research`, `setup` |
| **attempt_kind** | `initial`, `repair`, `retry`, `takeover` |
| **resource** | `model`, `tool`, `compute`, `storage` |

- The **total** is the sum over all rows.
- A **purpose breakdown** is a projection of those same rows.
- A **retry subtotal** is a *different projection of the same rows* (`attempt_kind != initial`), not an additional set of rows.

### A02.4 Retry/review double-counting is excluded by definition

There is **one row per actual invocation**, tagged with one `purpose` and one `attempt_kind`. A review that happens during a `retry` attempt is `purpose=review`, `attempt_kind=retry`. It appears in the retry subtotal **and** in the review breakdown, but only once in the total. It is therefore never added to the total twice. Reporting code must never add the retry subtotal to the purpose breakdown to form a total; the total comes from the base rows only.

### A02.5 Lifetime budget does not reset

`Task.budget_epoch` and `ProviderBudgetReservation.budget_epoch` currently exist. This ADR fixes their meaning: a retry epoch or a new Task does **not** reset the **horizon lifetime budget** (report §27 D02). Lifetime budget lives on the Horizon and spans epochs. A retry may open a new attempt, but it draws from the same lifetime budget. Exhausting it stops new attempts and preserves a checkpoint; it does not silently zero a counter.

### A02.6 Evidence is bound to exact hashes

Review/approval confirms an **exact** result and evidence (report §27 D08, invariant 42):

- A verdict binds `{candidate_hash, base_hash, evidence_hash, policy_revision, acceptance_revision}`.
- Any change to `candidate_hash` (rebase/merge/new commit) invalidates the prior PASS; revalidation is required.
- Re-review of an unchanged candidate under unchanged policy is not repeated.
- Machine checks (hash/schema/gates) are L0; they do not by themselves prove goal completeness.

### A02.7 Ledger coverage

The ledger must cover every model/tool invocation across `intake`, `planning`, `coding`, `review`, `research`, `setup`, plus horizon-level aggregates, extending the existing `UsageRecord`/reservation path **additively** (report §21, WP01). Interpreter usage that currently lands only in the trace outbox must reconcile into the same ledger before any "workflow cost = X" claim is made.

### A02.8 No credentials in accounting/evidence

Accounting, evidence and decision artifacts contain references only. Raw secrets, full prompts and credential values are excluded by default (report §21); redaction happens before durable storage.

## Consequences

- Cost claims become comparable across runs and providers.
- WP01 exposes a SQL/CLI breakdown by `purpose` and a separate retry slice without double counting.
- Unknown usage stays visible; dashboards/CLI must show it separately.
- Review revalidation after integration is mandatory for high-risk results.

## Invariants preserved

Invariants 1, 4, 42, 48; ADR-0001, ADR-0007, ADR-0008. Missing-usage-as-unknown and lifetime budget are consistent with the report's stable boundaries §5.

## Not decided here (follow-ups, do not implement under WP00)

- Exact numeric caps and defaults (versioned policy, report §28).
- Whether `PricingRevision` is a TOML registry or SQL table (v1 may be one registry).
- Learned/bandit routing (requires a stable ledger and eval corpus first).
- FX/conversion source for multi-currency reporting.
