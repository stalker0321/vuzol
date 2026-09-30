# ADR-0016 — D5 derived memory: templates-first units beside owners

Status: accepted (D5 writer, base 7de1ab1 D4-PASS). Implements DELTA §D5.

## 1. Desired, observed, declared state

Three state kinds stay with their typed owners; memory never becomes a
second runtime truth:

- desired: approved deltas and policies (`apply_approved_environment_delta`,
  plan approvals, accepted design decisions);
- observed: detected environment, execution receipts, verified artifacts
  (`record_detected_environment`, `Artifact.verified_at`, acceptance
  evidence);
- declared: operator/user statements about intent.

`MemoryUnit` stores causal source refs (decision/event/artifact/evidence/
turn/summary), never copies that become authoritative. `established_by`
direction is one-way: units point at owners; owners never read units on any
operational path. Memory text is not evidence of external apply; valid-state
reads go through owners only.

## 2. Supersession authority

Only the explicitly authorized user may replace a decision
(`DiscussionMemoryService.accept/supersede/retract` — existing authority
boundary, unchanged). Writer units follow: a newer source revision
supersedes, an older delayed writer is a no-op (`should_supersede` on
`effective_at`, ties keep the first). Conflicting decisions of different
principals are conflicts, not "newer text wins". Chain columns
(`superseded_by`, `effective_at`) plus `DECISION_SUPERSEDED` events carry
validity; job completion order never decides.

## 3. Triggers and scope

Writer jobs enqueue in the source transaction: explicit decision
accept/supersede/retract (trigger = decision event) and goal acceptance
(trigger = `PACKAGE_ACCEPTED` event). Job key is
`(trigger_event_id, extractor_version, scope)`; unit key adds the stable
source key (decision key, package revision). Summary-based units are out of
scope: the summary layer has no producer (dossier R2). Intermediate results
are observations, never "goal delivered"; outcome templates reference the
package revision and evidence or waiver explicitly.

## 4. Retention and redaction

Artifacts referenced by any memory unit are pinned against the retention
sweep (`referenced_by_memory_provenance`), alongside acceptance and effect
provenance. Post-hoc hiding is tombstone/redaction, never deletion:
unit text becomes `[tombstoned]` with a tombstone event ref (row and
provenance survive); artifact redaction moves only `redaction_revision`
plus an `artifact.redacted` event — content bytes and hashes are untouched,
so operational history cannot be forged. FTS and recall read only
`observation`/`verified` statuses; hypotheses, superseded, retracted and
tombstoned rows are invisible to recall but addressable by id with
provenance.

## 5. State Committer

State Committer is a name for deterministic transactional owner functions
(CAS on revision, related keys atomically, intent/reconcile before observed
commit), not a microservice. The memory writer is a pure observer of those
commits: it reads revisions and writes derived units, and it never edits
config, Git, or environment.

## 6. Memory levels vs review levels (docs развод)

Memory L0/L1/L2 (`discussion/memory.py`: raw turns, summary, decisions) are
prompt-pack bounds, unrelated to review L0–L3 (`review/policy.py`:
mechanical/review depth). Names are frozen by lead decision (T045) and only
separated in docs (`docs/CONTRACTS.md`).
