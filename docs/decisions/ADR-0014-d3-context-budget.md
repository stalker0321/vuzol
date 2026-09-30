# ADR-0014 — D3 context/budget: source-backed research, materializer, scout, lifetime

Status: accepted (D3 writer, base 1d42f85 D2-PASS). Implements DELTA §D3.

## 1. Lifetime owner (Q1)

The lifetime owner appears on ingress as scope project plus intake identity
and travels as `AccountingContext.horizon_id` through all 4 production
callers (`routing`, `handlers` via row inheritance, `independent` reserve,
intake observer). Pre-Task calls write the ingress scope (intake row id)
straight into the usage row. `WorkPackage.owner` keeps its actor meaning —
it is not redefined. Settle inherits the owner from the reservation row
(sticky); reconcile contexts never wipe it back to NULL.

## 2. One lifetime math (Q2)

Canonical lifetime = settled + outstanding, no epoch filter (like
`_lifetime_spend`). `budget_epoch` keeps resetting only task and step caps
(`test_routing_concurrency.py:137` stays green by design). Retry, goal
revision and epoch changes never erase lifetime; the package lifetime
budget (`max_cost`/`max_attempts`) is enforced at reserve time against the
canonical totals.

## 3. Step-less reserve (Q3)

Nullable additive refs (`task_id`/`run_id`/`step_id` may be NULL) plus
`invocation_id` on reservations and usage (partial unique while present).
Idempotency keys on the invocation, never on NULL steps. No fake Step,
ever. Intake/planning/scout calls reserve before (scout) or around
(intake observer: reserve→settle in one transaction) their spend; an
accounting failure there is logged AND persisted as a `budgeting_failed`
event — never logging-only — without breaking the caller.

## 4. Review suballocation (Q4)

The `enforce_task_token_limits=False` bypass is removed. Review reserves
are subject to task caps like everything else, with a deductible allowance
pool per task (`HardLimits.review_allowance_*`) recorded on the reservation
inside the shared ledger. Only the overage part consumes the pool; an
exhausted pool refuses. No second ledger exists.

## 5. Item contract fields (Q5)

Three independent additive `PlanRevisionItem` fields — `work_kind` from the
closed set {coding, research, scout}, `capability`, `effect_intent` — plus
`item_contract_version` (the D1 unified hash, pinned at revision creation).
Legacy NULL rows keep the previous coding mapping. Unknown kinds, unknown
capabilities and read-only/write conflicts fail closed before any Task
exists. Research/scout items select the read-only `research.v1` workflow
without write caps.

## 6. ScoutPacket (Q6)

Typed immutable Artifact bytes plus InputBinding; observed revision (content
defined) and time travel inside the packet JSON. No new truth table.
Partial packets persist on probe failure with `packet_partial` events;
retry re-runs only missing probes and supersedes the binding (documented
refresh). Repository-execution probe kinds are explicitly refused; the
sandbox/egress policy union is a follow-up.

## 7. Freshness anchor (Q7)

`InputBinding.source_retrieved_at` carries the oldest source retrieval
time; the resolver anchors on the oldest credible timestamp
(`min(retrieved_at, created_at)`), so a repack carrying the propagated
anchor never rejuvenates. Producers must propagate; the resolver cannot
invent provenance (documented limitation, fails toward the artifact time).
