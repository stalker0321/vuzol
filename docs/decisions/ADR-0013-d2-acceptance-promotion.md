# ADR-0013 — D2 acceptance/promotion: evidence gate before the real target

Status: accepted (D2 writer, base 760323b D1-PASS). Implements DELTA §D2.

## 1. Placement (Q1) and authority (Q2)

- The final acceptance gate stands BEFORE promotion of the last item into
  the real target (E05 confirmed: a post-queue-end gate cannot protect the
  target). The last item's apply into `integration_target_branch` requires
  acceptance evidence (or a waiver); intermediate items keep auto-approving
  into the integration branch. Enforced twice: the materialized `acceptance`
  step (coding.v4, after `approve_result`) assembles evidence, and
  `result_apply._load` re-checks it at apply time (race-proof).
- Authority: accepted = package-level (`WorkPackage.accepted_at` + evidence
  artifact); applied = per-apply `Approval` (→ `CONSUMED`); per-step =
  `ReviewVerdict` (D1 history refs flow into evidence). Evidence references
  approvals and verdicts; it never replaces them.

## 2. Evidence, waiver, compat (Q4)

- `acceptance-evidence.v1` (frozen for D3/D5) + tables `acceptance_evidence`
  (unique within `(package_id, evidence_hash)`) and `acceptance_waivers`
  (separate type with principal/reason, unique per head). One migration,
  additive, nullable; existing rows untouched.
- Old approvals without evidence read exactly as before (TTL 7 days);
  evidence is required only for pinned-horizon promotion applies and for
  `record_acceptance` on pinned packages. `Approval.step_id` keeps its
  non-unique index: the `ensure_result_approval` SELECT-then-insert race is
  documented, not schema-closed (changing it could strand legacy rows — Q4).

## 3. Corrective tail, reconcile (Q3, Q5)

- Owner: discussion service. Bounds: existing bounded repair + lifetime
  budget; no new scheduler. Durable trace: `correction_required` Event +
  projection outbox row + the persisted `should_notify` decision.
- Reject acceptance, workflow BLOCKED and final-approval reject all record
  the trace; "evaluating without a job" is impossible to lose.
- Reconciliation stays startup-scoped (applier); dev parity via the new
  `applier` compose profile (compose file only, running contour untouched).
  No periodic reconciler in D2 (follow-up).

## 4. Pin, UI, key (Q6)

- Single pin mechanic inherited from D0 (`pinned_horizon_enabled`); no
  second pin. `record_acceptance`, `revise_draft` guard, approval gate and
  promotion gate all read the pin (NULL → legacy fallback).
- ACCEPT/REJECT enter `PackageControlAction` + telegram callbacks (production
  caller for `record_acceptance`); goal/criteria enter through the draft
  create path (ready plumbing) and the `SET_GOAL` control (usable today).
- Acceptance records are content-unique per package; history rows never
  rewrite.
