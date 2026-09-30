# ADR-A03 — Autonomy boundary and approvals (decision log)

> Status: frozen, base 9be9054. Immutable frozen copy (WP00 baseline) — do not edit semantics.

- **Status:** Proposed (WP00 contracts baseline). Not yet accepted.
- **Scope:** which approvals remain mandatory; explicit refusal of silent weakening; deferred expansion proposal.
- **Base commit:** `79d7326e3e490647cb8a0e50c2c25ae23daa6409`.
- **Relation to existing decisions:** preserves invariants 18, 23, 24, 30, 35, 36 and ADR-0007, ADR-0008, ADR-0009. It changes **nothing** in the current approval behavior.

## Decision (explicit)

**Vuzol keeps its current approval requirements.** WP00 does not introduce preauthorization, does not introduce model-granted permissions, and does not relax invariant 36. Any future relaxation is a separate ADR with its own policy revision; it must not be smuggled in as an implementation detail of autonomy.

Report §13 and §27 D05 are explicit: capability availability is not permission, and the current setup approval is preserved. The report's "bounded preauthorization" idea is recorded below as **deferred and not adopted**.

## Current approvals that remain (unchanged)

| # | Approval | Bound to | Authority | Invariant / ADR |
|---|---|---|---|---|
| 1 | Plan approval | Immutable `PlanRevision` (`content_hash`, `approval_token_hash`, approver) | human principal | ADR-0005; plan revision approval provenance |
| 2 | Final local apply approval | Immutable action envelope: `action_envelope_hash`, exact `result_commit`/`base_commit`/`diff_hash`, target branch | human principal | invariant 35; `Approval`, `ResultApplyHandler` |
| 3 | Toolchain / dependency installation | source + hash, per installation | human principal | **invariant 36** (each install needs its own source- and hash-bound approval) |
| 4 | Host-privileged operation | action-specific envelope | human principal | invariant 30; ADR-0008 |
| 5 | Project provisioning / `/import` | allowlisted intake message in the dedicated topic | allowlisted human | ADR-0009, invariant 24 |
| 6 | Secret ingress | scoped request/ref, never a value in prompt | human/operator | ADR-0008; invariant 28 |
| 7 | New egress / new target beyond envelope | PermissionEnvelope extension | human principal | invariants 33, 35 |

Rules that remain in force:

- Approval callbacks are **single-use** and tied to one persisted step (invariant 23).
- An approval covers an **immutable** envelope; any material change invalidates it (invariant 35).
- Natural-language text and model output **cannot** approve a privileged/destructive action (invariant 18).
- A model may recommend risk/tier but may not grant permissions (invariants 14, 15, 16).
- Already-approved immutable operations are not re-approved for technical retries when target/hash/permissions are unchanged; conversely, a plan approval does not authorize a new target or a new irreversible operation (report §20).

## What WP00 explicitly does NOT do

- It does **not** remove or weaken the plan approval.
- It does **not** remove or weaken the final local apply approval.
- It does **not** remove or weaken the per-installation source/hash-bound approval (invariant 36).
- It does **not** give the model authority to grant secrets, egress or host privileges.
- It does **not** turn "setup" into a blanket privilege escalation.

## Deferred proposal (not adopted)

For future multi-hour horizons, report §13/§4 sketches **bounded preauthorization**: a policy that pre-approves specific catalogue IDs/versions, bounded downloads, and isolated installation on named nodes, so that a long horizon does not stop for each individual setup hash.

Status: **deferred**. It requires:

1. a separate ADR (e.g. a future ADR-A03 extension) — not this decision log;
2. a new explicit `policy_revision` with an upgrade step — no silent backfill ("everything old is pre-approved" is forbidden, report §23);
3. an explicit immutable envelope (source/version/hash, destinations, node/environment, egress, secret refs, resource cap, verification and rollback);
4. proof that revocation, quarantine and reconciliation still work.

Until all four are satisfied, automation stops at the existing approval (invariant 36).

## Consequences

- Zero behavioral change to approvals in WP00.
- Dependent packages (WP03 setup, WP08 horizon, WP10 reusable procedure) may not assume preauthorization.
- The autonomy boundary is a recorded, reviewable decision rather than an implicit side effect.

## Invariants preserved

18, 23, 24, 30, 33, 35, 36; ADR-0005, ADR-0007, ADR-0008, ADR-0009.
