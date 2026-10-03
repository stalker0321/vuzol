# ADR-0018 — DecisionBinding and the budgeted decision chain

Status: accepted (J1, base 2ef7d22). Implements `IMPLEMENTATION_PLAN.md` §J1.
Extends ADR-0014 (D3 context budget) and ADR-0015 (D4 semantic planning)
without replacing their ledgers or tables.

## 1. Packet vs binding

A semantic decision is made against a snapshot, not against live rows. J1
introduces a compact decision-context module (`context/decision_binding.py`):

- `DecisionPacket` is the model-visible context: opaque `DecisionRef`s (kind,
  id, revision, content hash), a coverage marker and allowed options. It carries
  no vendor names and grants no authority.
- `DecisionBinding` is runtime-only: the exact `sha256` of the request payload,
  schema text, prompt text and packet, plus the refs the decision was made
  against. It is never sent to the model.

`build_binding` / `assert_binding_matches` reject drift between the packet and
the request/schema/prompt it was bound to. `decision.v1`/`decision.v2` readers
are untouched; this is a new namespace (`decision-packet.v1`,
`decision-binding.v1`, `decision-output.v1`).

## 2. Dynamic validation after JSON Schema

`parse_decision_output` runs after schema parsing and fails closed: unknown keys,
missing keys, wrong kind, a target or support ref outside the binding, a decided
output without support, and an abstain with a target are all rejected. Unknown
keys of the new contract fail closed; legacy readers keep their own parsers.

## 3. Apply-time snapshot checks

`assert_applicable(binding, snapshot)` runs in the point where the existing
transition applies. It rejects an active kill switch, a changed candidate set, a
ref that is missing (forged), consumed, or whose revision/content hash drifted
(stale). The model's echoed revisions are never trusted; only the persisted
snapshot is.

## 4. Pre-reserve and one shared limiter

`interpretation/decision_chain.py` reserves *before* provider I/O using the
existing step-less budget owner (`reserve_invocation_budget`), then calls, then
settles with `settle_invocation_budget`. No new ledger, no fake `Step`, no
migration. `DecisionChainState` is an in-process admission guard shared across
initial/recontext/retry/fallback: counters are monotonic and a `decision_kind`
change never resets them. DB-level caps (task/daily/lifetime) stay atomic under
the existing advisory lock, so concurrent calls cannot exceed one limit.

A provider crash settles the reservation with unknown usage, which the existing
ledger records as a conservative floor with `cost_known=false` — never zero.

## 5. Kill switch and late results

The kill switch is checked before the provider call (`execute_decision_step`)
and before applying a transition (`apply_decision`). A late shadow result is
recorded as an `Event` (`record_late_decision`) and never advances execution
state, mirroring D6's defer-not-lose doctrine.

## 6. Artifact pins

Decision input/evidence artifacts are pinned by extending the existing
`Artifact.retention_until` (`ops/retention.pin_artifacts_for_audit`); the pin
only moves the deadline forward and needs no new table. This reuses the
retention sweeper's existing column.

## Consequences

- J3's first production producer can bind an INTAKE/WORK_SHAPE decision to a
  snapshot and reserve before spending, without inventing a second ledger.
- The primitives are not yet wired into the interpreter's existing
  `_attempt_observer` path; that rewiring touches every interpreter/transcriber
  call and is deferred (dossier timing risk) until a producer needs it.
