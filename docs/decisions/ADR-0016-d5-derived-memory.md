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

Five skip-reasons pin live provenance against the retention sweep
(`ops/retention._artifact_skip_reason`, after the runtime/worktree gates):
`referenced_by_acceptance_evidence` (`AcceptanceEvidence.artifact_id`),
`referenced_by_package_acceptance` (`WorkPackage.acceptance_artifact_id`),
`referenced_by_worktree_diff` (`Worktree.patch_artifact_id` /
`changed_files_artifact_id`, including cleaned worktrees — live ones are
caught by the earlier `referenced_by_worktree` gate),
`referenced_by_validation_result` (`ValidationResult.artifact_id`), and
`referenced_by_memory_provenance` (`MemoryUnit.source_artifact_id`).
Post-hoc hiding is tombstone/redaction, never deletion:
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

## 7. Project-lead in-turn reads

The lead's decision log and explicit notebook notes share `memory_units` but have
an independent read path. The lead can request `read_status`, `read_result`, or
`recall` before returning its final commands. These are server-side structured
output rounds; provider-native tools remain disabled. A response contains either
final commands or read calls. Individual read calls are validated by the server,
so malformed arguments produce a bounded tool error rather than an effect.

Each read checks the current turn claim, leases, delegation and goal authority in
a PostgreSQL read-only transaction. Each following inference repeats the normal
send fence. Reads reuse the command service's pure projections and do not create
command receipts, increment goal versions, or grant authority to apply commands.
Tool results are explicitly untrusted data.

`recall` searches active `decision_entry` and `notebook_note` project rows with
`simple` FTS, ordered by `ts_rank`, creation time and id. Decision details are part
of the generated search vector, including existing rows after migration. Scope
`this` binds to the goal's project; `all` resolves the owner's projects through
managed goals and provisioning ownership. Topic mappings alone grant no ownership.
Memory with another owner's goal provenance is excluded. No match returns an empty
list; the discussion service's recent-row fallback is not used.

Every inference round has its own deterministic invocation id, reservation and
receipt, unique on `(turn_id, phase, round)`. Round N's read results and audit
(name, arguments, result bytes, hit count) are in round N+1's immutable request.
Recovery loads that request before reading tools again, reuses received results,
and preserves the existing unknown-send reconciliation rule. A separate `tool_audit`
receipt on the originating inference retains call metadata even if revocation
prevents the following inference; audit bookkeeping never grants authority. Turn cost is the sum
of measured rounds only when every round's cost is known.

Defaults allow three tool rounds, four calls per round, 8192 UTF-8 bytes per result
and 24576 result bytes per turn. Reaching a limit forces a final inference whose
schema disallows tools. Oversized results are explicitly truncated. Downgrade
preserves existing round-zero receipts; additional rounds and tool audits must be archived before
removing their identity, rather than silently deleting accounting history.

The OpenAI adapter sends root-union requests in JSON mode and still validates the
complete response schema locally; forced final rounds retain the existing strict
schema mode. OpenAI's strict Structured Outputs subset requires an object at the
root and does not support a root union (see the
[official schema restrictions](https://developers.openai.com/api/docs/guides/structured-outputs#supported-schemas)).
