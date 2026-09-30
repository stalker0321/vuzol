# ADR-0015 — D4 semantic controls: deterministic fast path beside the interpreter

Status: accepted (D4 writer, base 1889bdc D3-PASS). Implements DELTA §D4.
Successor of ADR-0003: the model-based interpreter stays mandatory for
ambiguous natural language; a deterministic fast path handles only explicit
task commands beside it, never instead of it.

## 1. Decision interface (W1)

Closed classes `EffectIntent` / `RelationHint` / `ContextHint`
(`interpretation/decisions.py`) bind a model proposal to the policy verdict.
Free strings are not allowed. Hints never grant authority: capabilities,
approvals, review floors and execution eligibility stay in deterministic
policy. Ownership follows ARCHITECTURE_REVIEW §5.1: explicit code and
ingress state own command/target/principal/project/IDs before any model;
the bounded classifier proposes effect/relation; Jev may only pick an opaque
candidate ID; context needs are hints plus mandatory policy minima.

## 2. Provenance (W2)

Plan items are marked `derived` with `source_turn_ref` / `source_spec_revision`
refs; the D1 `TaskSpecRevision.source_turn_id` links the spec to the
originating turn. Planner, worker and reviewer follow refs
(`interpretation/provenance.task_source_refs`); generated text never replaces
the original turn. `PlanRevision.immutable_body` carries per-item markers, so
no migration is needed. Legacy rows read as `derived=False` / `None` refs.

## 3. Explicit controls (W3)

`EXPLICIT_TASK` arms only on explicit user task commands (`/task`, `task:`,
Russian "zadacha:"-prefixed imperatives), the legacy direct-task create path, and
pre-model slash commands — all of which already bypass the slow LLM. The
deterministic `explicit_task_interpretation` builds a `TASK_REQUEST` with zero
provider calls. Everything else stays confirm-first; `should_create_task`
stays False without an explicit plan envelope. The dead
`EXPLICIT_TASK -> TASK_REQUEST` mapping is revived only for these paths.
Explicit stop stays synchronous and never waits for retrieval.

## 4. Recovery (W4)

Generative recovery is bounded: one schema-repair plus at most one fallback
interpreter (`RECOVERY_MAX_REPAIRS/FALLBACKS = 1`), sharing one schema,
evidence and budget contract with the primary path. Fallbacks validate
against the same `TaskDraft` schema and write the same envelope. Exhaustion
routes to clarification/attention, never to execute. Without Jev there is no
default execute.

## 5. Candidates (W5)

Dynamic candidates carry stable IDs (`DecisionCandidate.key == candidate_id`)
and revision linkage (`revision_hash`, `source_turn_ref`). `TaskDraft`
targets bind via `target_candidate_id`, validated against the allowed set;
stale or unknown candidates fail closed to clarification. The
`ambiguous_task_ids` card channel is unchanged. Duplicate candidate IDs are
rejected; `resolve_discussion_candidate` is the single resolver.

## 6. Tiers and plan graph (W6/W7-scope)

`PlanningTier` DIRECT/LIGHT/STRONG (`interpretation/planning.py`) is a
planning-spend policy hint selected from uncertainty, dependencies, effect
risk, result size and cost of a wrong strategy. The compiler reads
`draft.needs_planning` (the hardcoded False is removed), making the optional
plan step reachable. Tiers never extend scope or permissions and never move
the review floor or plan authority. Plan dependencies validate
deterministically against known `local_id` values: unknown targets and
cycles are rejected.

## 7. Producer authority

Allowed `Interpretation`/`TaskDraft` producers: the model interpreter, the
package materializer (`MATERIALIZER_PROFILE`), the experiment harness, and
the deterministic explicit-task builder. Removing the interpreter call does
not break compiler, risk, capability or clarification consumers: they read
the validated draft envelope, not the LLM. New producers require an ADR
amendment with corpus parity evidence.

## 8. Jev rollout

Shadow of exactly one new decision kind: `target_selection`
(`decision.v2`), parallel to `repair_triage` (`decision.v1`, untouched).
Same downstream runtime, same evidence/revision contract, shared budget,
bounded single repair, no auto-promotion by confidence. Held-out fixtures
gain wrong-target, correction and ambiguous cases
(`decision-corpus.v2.json`); OOD retained. Vendor names are configuration
roles only, never DB enums. `repair_triage` is not generalized without a
new schema version.
