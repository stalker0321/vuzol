# ADR-0019 — Scoped context assembler and pending projections

Status: accepted (J2, base d0270b5). Implements `IMPLEMENTATION_PLAN.md` §J2.
Builds on ADR-0018 (DecisionBinding) without creating a second pending owner.

## 1. Pure projections, repository-backed assembly

`context/assembler.py` is pure: it projects *already loaded* durable state into
bounded packets and never reads/writes the database or grants authority. The
repository-backed assembly (`discussion/context_assembler.py`) reads existing
owners (work packages / edit sessions / accepted decisions) and reuses the
existing repository methods.

## 2. TargetCandidateProjection is its own type

`TargetCandidateProjection` (candidate_id, statement, source_ref, revision_hash,
relevance, recency_rank, delivered, delivered_ordinal) is deliberately separate
from the discussion `DecisionCandidate`; the discussion type keeps its own
name/lifecycle and is not reused as a target.

## 3. Recency window vs exact reference

`RecentWorkWindow` keeps all entries ordered by recency but exposes only the
last ``limit`` (default 5) as "recent". An exact reference is always resolvable
through ``exact(ref)`` even when the target sits outside the window. Failed,
cancelled and accepted items are distinguished by ``WorkOutcome``; artifact and
plan revisions carry their own revision/hash.

## 4. Delivered options are ordered by what was shown

`project_delivered_options` marks delivery from the *delivered order* the user
actually saw, never from the candidate array position, so a reversed candidate
array cannot change "the second option". Undelivered candidates are not "seen";
several open pending interactions (`PendingInteractionSet.requires_resolution`)
need an explicit choice.

## 5. Single consume on the existing owner

Pending interactions are backed by the existing durable `EditSession`
(project, principal, state, expiry, `session_generation`). Consuming an option
delegates to `WorkPackageService.apply_item_edit`, which re-checks owner,
expiry and generation under a row lock and closes other open sessions when a new
revision is created. No second pending owner and no new table are introduced
(lead decision). A foreign project/principal or a stale/expired option fails
closed before the domain service is touched.

## 6. Profile slice and invalidation

`ProfileSlice` is sourced from active `AcceptedDecision` rows (desired side) plus
an optional observed side. When desired and observed disagree, both sides are
kept and the key is listed in `unresolved_gaps` — the assembler never adopts a
generated value as normative. The slice carries an `accepted_revision_hash`
covering every active decision (id, key, statement), so changing an accepted
constraint changes the token and invalidates the slice.

## 7. Approvals are not consumed by affirmation

`is_explicit_approval` returns True only for an explicit approval control; a bare
"yes"/"да" never qualifies. The assembler only projects; approval consumption
stays in the existing `ApprovalRepository.consume` path.

## Consequences

- A semantic producer (J3) can assemble a deterministic context packet for a
  decision kind from existing owners and bind it with J1's `DecisionBinding`.
- Repository reads added in J2 are read-only (`WorkPackageRepository.
  open_edit_sessions`); no migrations, no new operational truth store.
